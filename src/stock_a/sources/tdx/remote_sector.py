"""以有界通达信原生文件协议获取行业与概念成员来源。"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import io
import ipaddress
from pathlib import Path
import queue
import re
import ssl
import socket
import struct
import threading
import time
from typing import Any, Callable, Mapping
from urllib.error import URLError
from urllib.parse import urlsplit
from urllib.request import (
    HTTPRedirectHandler,
    HTTPSHandler,
    Request,
    build_opener,
)
import zipfile
import zlib

from stock_a.sources.tdx.sector_members import (
    SectorSourceInputs,
    SourceFileEvidence,
    parse_remote_sector_source_inputs_with_status,
)


DEFAULT_REMOTE_SECTOR_HOSTS = ("124.71.187.122", "122.51.120.217")
REMOTE_SECTOR_PORT = 7709
REMOTE_BLOCK_CHUNK_BYTES = 30_000
MAX_REMOTE_BLOCK_FILE_BYTES = 8 * 1024 * 1024
MAX_REMOTE_ZHB_BYTES = 32 * 1024 * 1024
MAX_REMOTE_SESSION_SECONDS = 30.0
REMOTE_SOCKET_TIMEOUT_SECONDS = 5.0
MAX_REMOTE_CANDIDATE_COUNT = 12
MAX_REMOTE_CANDIDATE_SCAN_SECONDS = 90.0
MAX_TDX_CONFIG_BYTES = 256 * 1024
MAX_PYTDX_LIST_BYTES = 256 * 1024
PYTDX_LIST_TIMEOUT_SECONDS = 5.0
TDX_CONFIG_DNS_TIMEOUT_SECONDS = 1.5
PYTDX_SERVER_LIST_URL = (
    "https://raw.githubusercontent.com/rainx/pytdx/master/pytdx/config/hosts.py"
)

_RESPONSE_PREFIX = bytes.fromhex("b1cb7400")
_TYPE_CONNECT = 0x000D
_TYPE_BLOCK_META = 0x02C5
_TYPE_BLOCK_INFO = 0x06B9
_TDXHY_PATH = "T0002/hq_cache/tdxhy.cfg"
_TDXZS_PATH = "T0002/hq_cache/tdxzs.cfg"
_INFOHARBOR_PATH = "T0002/hq_cache/infoharbor_block.dat"
_LOCAL_TDX_CONFIG_SOURCE = "LOCAL_TDX_CONFIG:connect.cfg#HQHOST"
_PYTDX_LIST_SOURCE = f"PYTDX_PUBLIC_LIST:{PYTDX_SERVER_LIST_URL}"
_LEGACY_FALLBACK_SOURCE = "LEGACY_FALLBACK:DEFAULT_REMOTE_SECTOR_HOSTS"
_REMOTE_CANDIDATE_VALIDATION_TOKEN = object()
_DOMAIN_NAME = re.compile(
    r"(?=.{1,253}\Z)(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)"
    r"(?:\.(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?))+\Z"
)
_PYTDX_SERVER_TUPLE = re.compile(
    r"""\(\s*(['"])[^'"\r\n]{0,160}\1\s*,\s*(['"])([A-Za-z0-9.-]{1,253})\2\s*,\s*7709\s*\)"""
)


class TdxRemoteSectorError(ValueError):
    """TDX远端文件协议或来源完整性不符合合同。"""

    def __init__(
        self, code: str, message: str, *, details: Mapping[str, Any] | None = None
    ):
        super().__init__(message)
        self.code = code
        self.details = dict(details or {})


@dataclass(frozen=True)
class _RawRemoteSectorCandidate:
    host: str
    sources: tuple[str, ...]


@dataclass(frozen=True)
class _RemoteSectorCandidate:
    host: str
    sources: tuple[str, ...]


@dataclass(frozen=True)
class _CandidateDiscovery:
    candidates: tuple[_RemoteSectorCandidate, ...]
    sources: tuple[dict[str, str], ...]
    total_unique_inputs: int
    skipped_candidate_count: int
    rejected_candidate_count: int


@dataclass(frozen=True)
class RemoteSectorFiles:
    """一次来自单一服务器的完整远端来源观察；raw只留在进程内存。"""

    host: str
    files: Mapping[str, SourceFileEvidence]
    methods: Mapping[str, str]
    archive_size_bytes: int | None
    archive_sha256: str | None
    archive_raw: bytes | None
    archive_member_crc32: str | None
    missing_files: tuple[str, ...]
    source_failures: tuple[dict[str, str], ...]
    candidate_attempts: tuple[dict[str, Any], ...]
    candidate_sources: tuple[str, ...]
    candidate_count: int
    skipped_candidate_count: int
    discovery_sources: tuple[dict[str, str], ...]
    _candidate_validation_token: object = field(repr=False, compare=False)

    @property
    def available_kind_count(self) -> int:
        industry = "tdxhy.cfg" in self.files and "tdxzs.cfg" in self.files
        concept = "infoharbor_block.dat" in self.files
        return int(industry) + int(concept)


@dataclass(frozen=True)
class _CandidateDownload:
    host: str
    files: Mapping[str, SourceFileEvidence]
    methods: Mapping[str, str]
    archive_size_bytes: int | None
    archive_sha256: str | None
    archive_raw: bytes | None
    archive_member_crc32: str | None
    missing_files: tuple[str, ...]
    source_failures: tuple[dict[str, str], ...]

    @property
    def available_kind_count(self) -> int:
        industry = "tdxhy.cfg" in self.files and "tdxzs.cfg" in self.files
        concept = "infoharbor_block.dat" in self.files
        return int(industry) + int(concept)


def _is_public_ipv4(host: str) -> bool:
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return False
    return (
        address.version == 4
        and address.is_global
        and not address.is_private
        and not address.is_loopback
        and not address.is_link_local
        and not address.is_multicast
        and not address.is_reserved
    )


def _normalize_candidate_host(host: str, *, allow_domain: bool) -> str:
    value = host.strip()
    if not value or len(value) > 253 or any(char in value for char in "/:@[]%"):
        raise ValueError("候选地址格式无效")
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        address = None
    if address is not None:
        if address.version != 4 or not _is_public_ipv4(str(address)):
            raise ValueError("候选必须是公网IPv4")
        return str(address)
    if (
        not allow_domain
        or value.count(".") == 3
        and all(part.isdecimal() for part in value.split("."))
        or _DOMAIN_NAME.fullmatch(value) is None
    ):
        raise ValueError("候选不是允许来源中的公网IPv4或合法域名")
    return value.rstrip(".").lower()


def parse_pytdx_server_candidates(text: str) -> tuple[str, ...]:
    """只从已知公开清单的字面三元组提取IPv4，不解析或执行Python。"""
    if not isinstance(text, str) or len(text.encode("utf-8")) > MAX_PYTDX_LIST_BYTES:
        raise TdxRemoteSectorError(
            "REMOTE_CANDIDATE_LIST_SIZE_INVALID", "Pytdx公开候选清单超过字节上限"
        )
    candidates: list[str] = []
    seen: set[str] = set()
    for match in _PYTDX_SERVER_TUPLE.finditer(text):
        host = match.group(3)
        try:
            normalized = _normalize_candidate_host(host, allow_domain=False)
        except ValueError:
            continue
        if normalized not in seen:
            seen.add(normalized)
            candidates.append(normalized)
    return tuple(candidates)


def _parse_tdx_connect_cfg(
    raw: bytes,
) -> tuple[str | None, tuple[_RawRemoteSectorCandidate, ...], int]:
    """解析connect.cfg的HQHOST白名单字段，不读取或返回其他配置项。"""
    if not raw or len(raw) > MAX_TDX_CONFIG_BYTES:
        raise TdxRemoteSectorError(
            "REMOTE_LOCAL_CONFIG_SIZE_INVALID", "TDX connect.cfg为空或超过字节上限"
        )
    try:
        text = raw.decode("gb18030", errors="strict")
    except UnicodeDecodeError as error:
        raise TdxRemoteSectorError(
            "REMOTE_LOCAL_CONFIG_FORMAT_INVALID", "TDX connect.cfg不能按GB18030解码"
        ) from error

    section = ""
    values: dict[str, str] = {}
    for source_line in text.splitlines():
        line = source_line.strip()
        if not line or line.startswith((";", "#")):
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1].strip().upper()
            continue
        if section != "HQHOST" or "=" not in line:
            continue
        key, value = line.split("=", 1)
        normalized_key = key.strip().upper()
        if normalized_key in values:
            raise TdxRemoteSectorError(
                "REMOTE_LOCAL_CONFIG_FORMAT_INVALID",
                "TDX connect.cfg的HQHOST字段重复",
            )
        values[normalized_key] = value.strip()

    host_num_text = values.get("HOSTNUM", "")
    if not host_num_text.isdecimal() or not 1 <= int(host_num_text) <= 256:
        raise TdxRemoteSectorError(
            "REMOTE_LOCAL_CONFIG_FORMAT_INVALID",
            "TDX connect.cfg的HQHOST.HostNum无效",
        )
    host_num = int(host_num_text)
    primary_text = values.get("PRIMARYHOST", "")
    primary_value = int(primary_text) if re.fullmatch(r"-?[0-9]+", primary_text) else -1
    primary_index = primary_value + 1
    candidates: list[_RawRemoteSectorCandidate] = []
    rejected_count = 0
    primary_host: str | None = None
    for index in range(1, host_num + 1):
        suffix = f"{index:02d}"
        host_text = values.get(f"IPADDRESS{suffix}")
        port_text = values.get(f"PORT{suffix}")
        if host_text is None or port_text is None:
            rejected_count += 1
            continue
        if port_text != str(REMOTE_SECTOR_PORT):
            rejected_count += 1
            continue
        try:
            host = _normalize_candidate_host(host_text, allow_domain=True)
        except ValueError:
            rejected_count += 1
            continue
        source = f"{_LOCAL_TDX_CONFIG_SOURCE}.IPAddress{suffix}"
        candidates.append(_RawRemoteSectorCandidate(host, (source,)))
        if index == primary_index:
            primary_host = host
    return primary_host, tuple(candidates), rejected_count


class _CandidateListRedirectHandler(HTTPRedirectHandler):
    """候选清单重定向仅允许保留在同一HTTPS主机。"""

    def redirect_request(self, request, file_pointer, code, message, headers, new_url):
        parsed = urlsplit(new_url)
        if (
            parsed.scheme != "https"
            or parsed.hostname != "raw.githubusercontent.com"
            or parsed.port not in (None, 443)
            or parsed.path != urlsplit(PYTDX_SERVER_LIST_URL).path
            or parsed.query
        ):
            raise URLError("候选清单重定向超出批准主机")
        return super().redirect_request(
            request, file_pointer, code, message, headers, new_url
        )


def _fetch_pytdx_candidate_list() -> bytes:
    context = ssl.create_default_context()
    opener = build_opener(
        _CandidateListRedirectHandler(), HTTPSHandler(context=context)
    )
    request = Request(
        PYTDX_SERVER_LIST_URL,
        headers={"User-Agent": "Stock-A-TDX-endpoint-discovery/1"},
    )
    with opener.open(request, timeout=PYTDX_LIST_TIMEOUT_SECONDS) as response:
        final_url = urlsplit(response.geturl())
        if (
            final_url.scheme != "https"
            or final_url.hostname != "raw.githubusercontent.com"
            or response.status != 200
        ):
            raise TdxRemoteSectorError(
                "REMOTE_CANDIDATE_LIST_RESPONSE_INVALID",
                "Pytdx公开候选清单响应来源或状态无效",
            )
        content_length = response.headers.get("Content-Length")
        if content_length is not None:
            if (
                not content_length.isdecimal()
                or int(content_length) > MAX_PYTDX_LIST_BYTES
            ):
                raise TdxRemoteSectorError(
                    "REMOTE_CANDIDATE_LIST_SIZE_INVALID",
                    "Pytdx公开候选清单声明长度超过字节上限",
                )
        payload = response.read(MAX_PYTDX_LIST_BYTES + 1)
    if not payload or len(payload) > MAX_PYTDX_LIST_BYTES:
        raise TdxRemoteSectorError(
            "REMOTE_CANDIDATE_LIST_SIZE_INVALID",
            "Pytdx公开候选清单为空或超过字节上限",
        )
    return payload


def _load_local_tdx_candidates(
    config: Mapping[str, Any] | None,
) -> tuple[str | None, tuple[_RawRemoteSectorCandidate, ...], dict[str, str]]:
    source_policy = config.get("source_policy") if isinstance(config, Mapping) else None
    roots = (
        source_policy.get("configured_tdx_source_roots")
        if isinstance(source_policy, Mapping)
        else None
    )
    if (
        not isinstance(source_policy, Mapping)
        or source_policy.get("all_source_roots_read_only") is not True
        or not isinstance(roots, list)
        or len(roots) != 1
        or not isinstance(roots[0], str)
        or not roots[0]
    ):
        return (
            None,
            (),
            {
                "source": _LOCAL_TDX_CONFIG_SOURCE,
                "status": "SKIPPED_UNBOUND_ROOT",
                "candidate_count": "0",
                "reason_code": "TDX_READ_ONLY_ROOT_NOT_BOUND",
            },
        )
    try:
        root = Path(roots[0]).resolve(strict=True)
        if not root.is_dir():
            raise OSError("TDX来源根不是目录")
        source_path = root / "connect.cfg"
        resolved_path = source_path.resolve(strict=True)
        resolved_path.relative_to(root)
        if not resolved_path.is_file():
            raise OSError("TDX connect.cfg不是普通文件")
        before = resolved_path.stat()
        if before.st_size > MAX_TDX_CONFIG_BYTES:
            raise TdxRemoteSectorError(
                "REMOTE_LOCAL_CONFIG_SIZE_INVALID",
                "TDX connect.cfg超过只读解析字节上限",
            )
        raw = resolved_path.read_bytes()
        after = resolved_path.stat()
        if (
            before.st_size != after.st_size
            or before.st_mtime_ns != after.st_mtime_ns
            or len(raw) != after.st_size
        ):
            raise TdxRemoteSectorError(
                "REMOTE_LOCAL_CONFIG_CHANGED",
                "TDX connect.cfg在只读读取期间发生变化",
            )
        primary, candidates, rejected = _parse_tdx_connect_cfg(raw)
    except TdxRemoteSectorError as error:
        return (
            None,
            (),
            {
                "source": _LOCAL_TDX_CONFIG_SOURCE,
                "status": "REJECTED",
                "candidate_count": "0",
                "reason_code": error.code,
            },
        )
    except (OSError, ValueError, PermissionError):
        return (
            None,
            (),
            {
                "source": _LOCAL_TDX_CONFIG_SOURCE,
                "status": "UNAVAILABLE",
                "candidate_count": "0",
                "reason_code": "TDX_CONFIG_UNAVAILABLE",
            },
        )
    return (
        primary,
        candidates,
        {
            "source": _LOCAL_TDX_CONFIG_SOURCE,
            "status": "AVAILABLE" if candidates else "EMPTY",
            "candidate_count": str(len(candidates)),
            "rejected_entry_count": str(rejected),
        },
    )


def _resolve_candidate_host(
    host: str, *, timeout_seconds: float = TDX_CONFIG_DNS_TIMEOUT_SECONDS
) -> tuple[str, ...]:
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        address = None
    if address is not None:
        return (str(address),) if _is_public_ipv4(str(address)) else ()
    result_queue: queue.Queue[list[tuple]] = queue.Queue(maxsize=1)

    def resolve() -> None:
        try:
            result_queue.put(
                socket.getaddrinfo(
                    host,
                    REMOTE_SECTOR_PORT,
                    family=socket.AF_INET,
                    type=socket.SOCK_STREAM,
                )
            )
        except OSError:
            result_queue.put([])

    worker = threading.Thread(target=resolve, name="tdx-candidate-dns", daemon=True)
    worker.start()
    try:
        results = result_queue.get(timeout=timeout_seconds)
    except queue.Empty:
        return ()
    resolved: list[str] = []
    for _family, _kind, _protocol, _canonical_name, sockaddr in results:
        address_text = str(sockaddr[0])
        if _is_public_ipv4(address_text) and address_text not in resolved:
            resolved.append(address_text)
    return tuple(resolved)


def _discover_remote_candidates(
    config: Mapping[str, Any] | None,
) -> _CandidateDiscovery:
    primary_host, local_candidates, local_status = _load_local_tdx_candidates(config)
    discovery_sources = [local_status]

    try:
        public_payload = _fetch_pytdx_candidate_list()
        public_text = public_payload.decode("utf-8", errors="strict")
        public_hosts = parse_pytdx_server_candidates(public_text)
        public_status = {
            "source": _PYTDX_LIST_SOURCE,
            "status": "AVAILABLE" if public_hosts else "EMPTY",
            "candidate_count": str(len(public_hosts)),
            "response_bytes": str(len(public_payload)),
        }
    except (OSError, TimeoutError, URLError, UnicodeDecodeError, TdxRemoteSectorError):
        public_hosts = ()
        public_status = {
            "source": _PYTDX_LIST_SOURCE,
            "status": "UNAVAILABLE",
            "candidate_count": "0",
            "reason_code": "PYTDX_LIST_UNAVAILABLE_OR_INVALID",
        }
    discovery_sources.append(public_status)

    local_by_host: dict[str, _RawRemoteSectorCandidate] = {}
    for candidate in local_candidates:
        existing = local_by_host.get(candidate.host)
        local_by_host[candidate.host] = _RawRemoteSectorCandidate(
            candidate.host,
            tuple(
                dict.fromkeys(
                    (
                        *(existing.sources if existing is not None else ()),
                        *candidate.sources,
                    )
                )
            ),
        )
    public_by_host = {
        host: _RawRemoteSectorCandidate(host, (_PYTDX_LIST_SOURCE,))
        for host in public_hosts
    }
    ordered_inputs: list[_RawRemoteSectorCandidate] = []

    def add_input(candidate: _RawRemoteSectorCandidate | None) -> None:
        if candidate is None:
            return
        existing_index = next(
            (
                index
                for index, item in enumerate(ordered_inputs)
                if item.host == candidate.host
            ),
            None,
        )
        if existing_index is None:
            ordered_inputs.append(candidate)
            return
        existing = ordered_inputs[existing_index]
        ordered_inputs[existing_index] = _RawRemoteSectorCandidate(
            candidate.host,
            tuple(dict.fromkeys((*existing.sources, *candidate.sources))),
        )

    add_input(local_by_host.get(primary_host) if primary_host else None)
    local_remaining = [
        candidate for candidate in local_candidates if candidate.host != primary_host
    ]
    public_order = list(public_by_host.values())
    for index in range(max(len(local_remaining), len(public_order))):
        if index < len(public_order):
            add_input(public_order[index])
        if index < len(local_remaining):
            add_input(local_remaining[index])

    fallback_inputs = [
        _RawRemoteSectorCandidate(host, (_LEGACY_FALLBACK_SOURCE,))
        for host in DEFAULT_REMOTE_SECTOR_HOSTS
    ]
    for candidate in fallback_inputs:
        add_input(candidate)

    fallback_hosts = set(DEFAULT_REMOTE_SECTOR_HOSTS)
    preferred_inputs = [
        item for item in ordered_inputs if item.host not in fallback_hosts
    ]
    fallback_candidates = [
        _RemoteSectorCandidate(item.host, item.sources)
        for host in DEFAULT_REMOTE_SECTOR_HOSTS
        for item in ordered_inputs
        if item.host == host
    ]

    resolved: list[_RemoteSectorCandidate] = []
    resolution_failures: list[dict[str, str]] = []
    processed_inputs = 0
    rejected_candidate_count = 0
    for candidate in preferred_inputs:
        if len(resolved) >= MAX_REMOTE_CANDIDATE_COUNT - len(fallback_candidates):
            break
        processed_inputs += 1
        addresses = _resolve_candidate_host(candidate.host)
        if not addresses:
            rejected_candidate_count += 1
            resolution_failures.append(
                {
                    "source": ";".join(candidate.sources),
                    "status": "REJECTED",
                    "reason_code": "REMOTE_HOST_UNRESOLVED_OR_NON_PUBLIC",
                }
            )
            continue
        for address in addresses:
            if address in fallback_hosts:
                fallback_index = DEFAULT_REMOTE_SECTOR_HOSTS.index(address)
                existing = fallback_candidates[fallback_index]
                fallback_candidates[fallback_index] = _RemoteSectorCandidate(
                    existing.host,
                    tuple(dict.fromkeys((*existing.sources, *candidate.sources))),
                )
                continue
            existing_index = next(
                (index for index, item in enumerate(resolved) if item.host == address),
                None,
            )
            if existing_index is not None:
                existing = resolved[existing_index]
                resolved[existing_index] = _RemoteSectorCandidate(
                    address,
                    tuple(dict.fromkeys((*existing.sources, *candidate.sources))),
                )
                continue
            if len(resolved) >= MAX_REMOTE_CANDIDATE_COUNT - len(fallback_candidates):
                rejected_candidate_count += 1
                continue
            resolved.append(_RemoteSectorCandidate(address, candidate.sources))

    for candidate in fallback_candidates:
        existing_index = next(
            (
                index
                for index, item in enumerate(resolved)
                if item.host == candidate.host
            ),
            None,
        )
        if existing_index is not None:
            existing = resolved.pop(existing_index)
            candidate = _RemoteSectorCandidate(
                candidate.host,
                tuple(dict.fromkeys((*existing.sources, *candidate.sources))),
            )
        resolved.append(candidate)
    resolved = resolved[:MAX_REMOTE_CANDIDATE_COUNT]

    skipped_candidate_count = max(0, len(preferred_inputs) - processed_inputs)
    discovery_sources.extend(resolution_failures)
    discovery_sources.append(
        {
            "source": _LEGACY_FALLBACK_SOURCE,
            "status": "AVAILABLE",
            "candidate_count": str(len(fallback_candidates)),
        }
    )
    total_unique_inputs = len(ordered_inputs)
    return _CandidateDiscovery(
        candidates=tuple(resolved),
        sources=tuple(discovery_sources),
        total_unique_inputs=total_unique_inputs,
        skipped_candidate_count=skipped_candidate_count,
        rejected_candidate_count=rejected_candidate_count,
    )


class _TdxRemoteSession:
    """单服务器7709会话；只请求本任务列明的文件名。"""

    def __init__(
        self,
        host: str,
        *,
        socket_factory: Callable = socket.create_connection,
        timeout_seconds: float = REMOTE_SOCKET_TIMEOUT_SECONDS,
        max_session_seconds: float = MAX_REMOTE_SESSION_SECONDS,
    ) -> None:
        try:
            address = ipaddress.ip_address(host)
        except ValueError as error:
            raise TdxRemoteSectorError(
                "REMOTE_HOST_INVALID", "TDX远端候选必须是已登记的数字IP地址"
            ) from error
        if (
            address.version != 4
            or not _is_public_ipv4(str(address))
            or timeout_seconds <= 0
            or max_session_seconds <= 0
        ):
            raise TdxRemoteSectorError(
                "REMOTE_HOST_INVALID", "TDX远端候选不是公网IPv4或网络时限无效"
            )
        self.host = str(address)
        self._socket_factory = socket_factory
        self._timeout_seconds = timeout_seconds
        self._max_session_seconds = max_session_seconds
        self._deadline = 0.0
        self._socket: socket.socket | None = None
        self._message_id = 0

    def __enter__(self) -> _TdxRemoteSession:
        self._deadline = time.monotonic() + self._max_session_seconds
        try:
            self._socket = self._socket_factory(
                (self.host, REMOTE_SECTOR_PORT), timeout=self._timeout_seconds
            )
            self._socket.settimeout(self._timeout_seconds)
            _control, payload = self._request(_TYPE_CONNECT, b"\x01")
            if len(payload) < 68:
                raise TdxRemoteSectorError(
                    "REMOTE_HANDSHAKE_INVALID", "TDX服务器握手响应长度不足"
                )
        except TdxRemoteSectorError:
            self.close()
            raise
        except (OSError, EOFError, TimeoutError, zlib.error) as error:
            self.close()
            raise TdxRemoteSectorError(
                "REMOTE_HANDSHAKE_FAILED", "连接或验证TDX远端文件服务失败"
            ) from error
        return self

    def __exit__(self, _kind: object, _value: object, _traceback: object) -> None:
        self.close()

    def close(self) -> None:
        connection = self._socket
        self._socket = None
        if connection is not None:
            try:
                connection.close()
            except OSError:
                pass

    def _remaining_timeout(self) -> float:
        remaining = self._deadline - time.monotonic()
        if remaining <= 0:
            raise TdxRemoteSectorError(
                "REMOTE_SESSION_TIMEOUT", "单个TDX服务器会话超过30秒上限"
            )
        return min(self._timeout_seconds, remaining)

    def _receive_exact(self, size: int) -> bytes:
        if self._socket is None:
            raise TdxRemoteSectorError("REMOTE_SESSION_CLOSED", "TDX远端文件会话已关闭")
        result = bytearray()
        while len(result) < size:
            self._socket.settimeout(self._remaining_timeout())
            part = self._socket.recv(size - len(result))
            if not part:
                raise EOFError("TDX远端文件连接提前关闭")
            result.extend(part)
        return bytes(result)

    def _request(self, request_type: int, data: bytes) -> tuple[int, bytes]:
        if self._socket is None:
            raise TdxRemoteSectorError("REMOTE_SESSION_CLOSED", "TDX远端文件会话已关闭")
        self._remaining_timeout()
        message_id = self._message_id
        self._message_id += 1
        length = len(data) + 2
        frame = (
            b"\x0c"
            + struct.pack("<I", message_id)
            + b"\x01"
            + struct.pack("<HHH", length, length, request_type)
            + data
        )
        self._socket.settimeout(self._remaining_timeout())
        self._socket.sendall(frame)
        header = self._receive_exact(16)
        if header[:4] != _RESPONSE_PREFIX:
            raise TdxRemoteSectorError(
                "REMOTE_FRAME_PREFIX_INVALID", "TDX远端响应帧头无效"
            )
        response_id = struct.unpack_from("<I", header, 5)[0]
        response_type = struct.unpack_from("<H", header, 10)[0]
        compressed_length, raw_length = struct.unpack_from("<HH", header, 12)
        if response_id != message_id or response_type != request_type:
            raise TdxRemoteSectorError(
                "REMOTE_FRAME_IDENTITY_INVALID", "TDX远端响应类型或消息序号不匹配"
            )
        body = self._receive_exact(compressed_length)
        if compressed_length != raw_length:
            decompressor = zlib.decompressobj()
            payload = decompressor.decompress(body, raw_length + 1)
            if (
                len(payload) != raw_length
                or not decompressor.eof
                or decompressor.unused_data
                or decompressor.unconsumed_tail
            ):
                raise TdxRemoteSectorError(
                    "REMOTE_FRAME_COMPRESSION_INVALID",
                    "TDX远端响应压缩流或展开长度无效",
                )
        else:
            payload = body
        if len(payload) != raw_length:
            raise TdxRemoteSectorError(
                "REMOTE_FRAME_LENGTH_INVALID", "TDX远端响应实际长度与帧头不匹配"
            )
        return header[4], payload

    def get_block_file_raw(
        self, file_name: str, *, max_bytes: int = MAX_REMOTE_BLOCK_FILE_BYTES
    ) -> tuple[bytes | None, int | None]:
        if file_name not in {"tdxhy.cfg", "infoharbor_block.dat"}:
            raise TdxRemoteSectorError(
                "REMOTE_FILE_NOT_ALLOWLISTED", "远端文件不在本任务成员来源白名单中"
            )
        _control, metadata = self._request(
            _TYPE_BLOCK_META, file_name.encode("ascii").ljust(40, b"\x00")
        )
        if len(metadata) < 4:
            raise TdxRemoteSectorError(
                "REMOTE_META_INVALID", "TDX板块文件元数据长度不足"
            )
        file_size = struct.unpack_from("<I", metadata)[0]
        if file_size == 0:
            return None, 0
        if file_size > max_bytes:
            raise TdxRemoteSectorError(
                "REMOTE_FILE_SIZE_LIMIT", "TDX板块成员原件超过单文件字节上限"
            )
        output = bytearray()
        offset = 0
        while offset < file_size:
            requested = min(REMOTE_BLOCK_CHUNK_BYTES, file_size - offset)
            request_data = struct.pack("<II", offset, requested) + file_name.encode(
                "ascii"
            ).ljust(100, b"\x00")
            _control, response = self._request(_TYPE_BLOCK_INFO, request_data)
            chunk = _decode_file_chunk(response, requested)
            if not chunk or len(chunk) < requested:
                raise TdxRemoteSectorError(
                    "REMOTE_FILE_TRUNCATED", "TDX板块成员原件分块短缺"
                )
            output.extend(chunk)
            offset += len(chunk)
        if len(output) != file_size:
            raise TdxRemoteSectorError(
                "REMOTE_FILE_LENGTH_MISMATCH", "TDX板块成员原件总长度与元数据不一致"
            )
        return bytes(output), file_size

    def get_report_file(
        self, file_name: str, *, max_bytes: int = MAX_REMOTE_ZHB_BYTES
    ) -> bytes | None:
        if file_name not in {"zhb.zip", "infoharbor_block.dat"}:
            raise TdxRemoteSectorError(
                "REMOTE_FILE_NOT_ALLOWLISTED", "远端报表文件不在本任务成员来源白名单中"
            )
        output = bytearray()
        offset = 0
        while True:
            self._remaining_timeout()
            request_data = struct.pack(
                "<II", offset, REMOTE_BLOCK_CHUNK_BYTES
            ) + file_name.encode("ascii").ljust(100, b"\x00")
            _control, response = self._request(_TYPE_BLOCK_INFO, request_data)
            chunk = _decode_file_chunk(response, REMOTE_BLOCK_CHUNK_BYTES)
            if not chunk:
                break
            if len(output) + len(chunk) > max_bytes:
                raise TdxRemoteSectorError(
                    "REMOTE_FILE_SIZE_LIMIT", "TDX报表成员来源超过单文件字节上限"
                )
            output.extend(chunk)
            offset += len(chunk)
            if len(chunk) < REMOTE_BLOCK_CHUNK_BYTES:
                break
        return bytes(output) if output else None


def _decode_file_chunk(response: bytes, requested: int) -> bytes:
    if len(response) < 4:
        raise TdxRemoteSectorError("REMOTE_CHUNK_INVALID", "TDX远端文件分块头长度不足")
    declared = struct.unpack_from("<I", response)[0]
    data = response[4:]
    if declared != len(data) or declared > requested:
        raise TdxRemoteSectorError(
            "REMOTE_CHUNK_INVALID", "TDX远端文件分块声明长度与原始字节不匹配"
        )
    return data


def _file_evidence(
    *,
    host: str,
    file_name: str,
    relative_path: str,
    raw: bytes,
    method: str,
    archive_sha256: str | None = None,
) -> SourceFileEvidence:
    return SourceFileEvidence(
        relative_path=relative_path,
        absolute_path="",
        raw=raw,
        sha256=hashlib.sha256(raw).hexdigest(),
        size_bytes=len(raw),
        last_write_time_utc="NOT_PROVIDED_BY_TDX_REMOTE_SOURCE",
        source_label=f"TDX_REMOTE_SERVER/{host}:{REMOTE_SECTOR_PORT}/{file_name}",
        source_protocol=method,
        source_archive_sha256=archive_sha256,
    )


def _extract_tdxzs(raw_zip: bytes) -> tuple[bytes, str]:
    try:
        with zipfile.ZipFile(io.BytesIO(raw_zip)) as archive:
            matches = [
                item for item in archive.infolist() if item.filename == "tdxzs.cfg"
            ]
            if len(matches) != 1:
                raise TdxRemoteSectorError(
                    "REMOTE_TDXZS_MEMBER_INVALID",
                    "zhb.zip必须恰有一个根目录tdxzs.cfg成员",
                )
            member = matches[0]
            if (
                member.flag_bits & 0x1
                or member.file_size > MAX_REMOTE_BLOCK_FILE_BYTES
                or member.compress_type
                not in {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}
            ):
                raise TdxRemoteSectorError(
                    "REMOTE_TDXZS_MEMBER_INVALID",
                    "zhb.zip内tdxzs.cfg成员加密、过大或压缩方式不受支持",
                )
            raw = archive.read(member)
            if len(raw) != member.file_size:
                raise TdxRemoteSectorError(
                    "REMOTE_TDXZS_MEMBER_INVALID",
                    "zhb.zip内tdxzs.cfg成员长度不匹配",
                )
            return raw, f"{member.CRC:08x}"
    except TdxRemoteSectorError:
        raise
    except (OSError, RuntimeError, ValueError, zipfile.BadZipFile, zlib.error) as error:
        raise TdxRemoteSectorError(
            "REMOTE_ZHB_INVALID", "TDX远端zhb.zip或目标成员校验失败"
        ) from error


def _download_candidate(
    host: str,
    *,
    timeout_seconds: float = REMOTE_SOCKET_TIMEOUT_SECONDS,
    max_session_seconds: float = MAX_REMOTE_SESSION_SECONDS,
) -> _CandidateDownload:
    files: dict[str, SourceFileEvidence] = {}
    methods: dict[str, str] = {}
    archive_size_bytes: int | None = None
    archive_sha256: str | None = None
    archive_raw: bytes | None = None
    archive_member_crc32: str | None = None
    source_failures: list[dict[str, str]] = []
    with _TdxRemoteSession(
        host,
        timeout_seconds=timeout_seconds,
        max_session_seconds=max_session_seconds,
    ) as session:
        tdxhy, tdxhy_size = session.get_block_file_raw("tdxhy.cfg")
        if tdxhy is not None:
            files["tdxhy.cfg"] = _file_evidence(
                host=host,
                file_name="tdxhy.cfg",
                relative_path=_TDXHY_PATH,
                raw=tdxhy,
                method="GET_BLOCK_FILE_RAW_0X02C5_0X06B9",
            )
            methods["tdxhy.cfg"] = "GET_BLOCK_FILE_RAW_0X02C5_0X06B9"

        archive = session.get_report_file("zhb.zip")
        if archive is not None:
            archive_raw = archive
            archive_size_bytes = len(archive)
            archive_sha256 = hashlib.sha256(archive).hexdigest()
            try:
                tdxzs, archive_member_crc32 = _extract_tdxzs(archive)
            except TdxRemoteSectorError as error:
                source_failures.append({"file": "tdxzs.cfg", "reason_code": error.code})
            else:
                files["tdxzs.cfg"] = _file_evidence(
                    host=host,
                    file_name="zhb.zip!tdxzs.cfg",
                    relative_path=_TDXZS_PATH,
                    raw=tdxzs,
                    method="GET_REPORT_FILE_0X06B9_ZHB_ZIP_MEMBER",
                    archive_sha256=archive_sha256,
                )
                methods["zhb.zip"] = "GET_REPORT_FILE_0X06B9"
                methods["tdxzs.cfg"] = "GET_REPORT_FILE_0X06B9_ZHB_ZIP_MEMBER"

        infoharbor, infoharbor_size = session.get_block_file_raw("infoharbor_block.dat")
        infoharbor_method = "GET_BLOCK_FILE_RAW_0X02C5_0X06B9"
        if infoharbor is None:
            infoharbor = session.get_report_file("infoharbor_block.dat")
            infoharbor_method = "GET_REPORT_FILE_0X06B9"
        elif infoharbor_size != len(infoharbor):
            raise TdxRemoteSectorError(
                "REMOTE_FILE_LENGTH_MISMATCH",
                "TDX远端infoharbor_block.dat大小元数据不匹配",
            )
        if infoharbor is not None:
            files["infoharbor_block.dat"] = _file_evidence(
                host=host,
                file_name="infoharbor_block.dat",
                relative_path=_INFOHARBOR_PATH,
                raw=infoharbor,
                method=infoharbor_method,
            )
            methods["infoharbor_block.dat"] = infoharbor_method

    missing: list[str] = []
    if "tdxhy.cfg" not in files:
        missing.append("tdxhy.cfg")
    if "tdxzs.cfg" not in files:
        missing.append("tdxzs.cfg")
    if "infoharbor_block.dat" not in files:
        missing.append("infoharbor_block.dat")
    return _CandidateDownload(
        host=host,
        files=files,
        methods=methods,
        archive_size_bytes=archive_size_bytes,
        archive_sha256=archive_sha256,
        archive_raw=archive_raw,
        archive_member_crc32=archive_member_crc32,
        missing_files=tuple(missing),
        source_failures=tuple(source_failures),
    )


def fetch_remote_sector_files(
    config: Mapping[str, Any] | None = None,
) -> RemoteSectorFiles:
    """从许可来源发现至多12个节点，以真实成员文件内容选择来源。"""
    discovery = _discover_remote_candidates(config)
    candidates = discovery.candidates
    if not candidates or len(candidates) > MAX_REMOTE_CANDIDATE_COUNT:
        raise TdxRemoteSectorError(
            "REMOTE_CANDIDATE_BUDGET_INVALID",
            "TDX远端候选为空或超过12个节点上限",
            details={
                "candidate_total": len(candidates),
                "discovery_sources": list(discovery.sources),
            },
        )
    attempts: list[dict[str, Any]] = []
    best: (
        tuple[
            tuple[int, int, int, int, int, int],
            _CandidateDownload,
            _RemoteSectorCandidate,
        ]
        | None
    ) = None
    deadline = time.monotonic() + MAX_REMOTE_CANDIDATE_SCAN_SECONDS
    for candidate_index, candidate_identity in enumerate(candidates):
        host = candidate_identity.host
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            for skipped in candidates[candidate_index:]:
                attempts.append(
                    {
                        "host": skipped.host,
                        "candidate_sources": list(skipped.sources),
                        "status": "SKIPPED_TIME_BUDGET",
                        "reason_code": "REMOTE_CANDIDATE_TIME_BUDGET_EXHAUSTED",
                    }
                )
            break
        try:
            candidate = _download_candidate(
                host,
                timeout_seconds=min(REMOTE_SOCKET_TIMEOUT_SECONDS, remaining),
                max_session_seconds=min(MAX_REMOTE_SESSION_SECONDS, remaining),
            )
        except TdxRemoteSectorError as error:
            attempts.append(
                {
                    "host": host,
                    "candidate_sources": list(candidate_identity.sources),
                    "status": "FAILED",
                    "reason_code": error.code,
                }
            )
            continue
        except (OSError, EOFError, TimeoutError, zlib.error):
            attempts.append(
                {
                    "host": host,
                    "candidate_sources": list(candidate_identity.sources),
                    "status": "FAILED",
                    "reason_code": "REMOTE_TRANSPORT_FAILED",
                }
            )
            continue
        parse_outcome = _parse_candidate_files(host, candidate.files)
        available_kinds = sorted(
            {
                "INDUSTRY" if item.source_kind == "TDX_TDXHY_CFG" else "CONCEPT"
                for item in parse_outcome.snapshots
            }
        )
        quality_fields = [
            field
            for snapshot in parse_outcome.snapshots
            for field in snapshot.source_quality.split(";")
        ]
        unmapped_count = sum(
            count
            for snapshot in parse_outcome.snapshots
            for _reason, count in snapshot.unresolved_rows
        )
        complete_files = {
            "tdxhy.cfg",
            "tdxzs.cfg",
            "infoharbor_block.dat",
        }.issubset(candidate.files)
        complete = (
            complete_files
            and len(available_kinds) == 2
            and not parse_outcome.rejected_sources
            and not candidate.missing_files
            and not candidate.source_failures
        )
        status = (
            "COMPLETE" if complete else "PARTIAL" if available_kinds else "REJECTED"
        )
        attempt: dict[str, Any] = {
            "host": host,
            "candidate_sources": list(candidate_identity.sources),
            "status": status,
            "available_kind_count": len(available_kinds),
            "available_kinds": available_kinds,
            "missing_files": list(candidate.missing_files),
            "source_failures": list(candidate.source_failures),
            "parse_rejections": list(parse_outcome.rejected_sources),
            "source_quality": quality_fields,
            "verified_file_sha256": {
                name: item.sha256 for name, item in sorted(candidate.files.items())
            },
            "archive_sha256": candidate.archive_sha256,
        }
        if status == "REJECTED":
            if parse_outcome.rejected_sources:
                attempt["reason_code"] = parse_outcome.rejected_sources[0]["code"]
            elif candidate.source_failures:
                attempt["reason_code"] = candidate.source_failures[0]["reason_code"]
            else:
                attempt["reason_code"] = "REMOTE_MEMBER_FILES_UNAVAILABLE"
        attempts.append(attempt)
        score = (
            len(available_kinds),
            int(complete_files),
            -len(parse_outcome.rejected_sources),
            -len(candidate.missing_files),
            -len(candidate.source_failures),
            -unmapped_count,
        )
        if best is None or score > best[0]:
            best = (score, candidate, candidate_identity)
        if complete:
            break
    if best is None or best[0][0] == 0:
        details = {
            "candidate_total": len(candidates),
            "candidate_input_total": discovery.total_unique_inputs,
            "candidate_verified_count": 0,
            "candidate_skipped_over_budget": discovery.skipped_candidate_count,
            "candidate_rejected_before_network": discovery.rejected_candidate_count,
            "candidate_attempts": attempts,
            "discovery_sources": list(discovery.sources),
        }
        raise TdxRemoteSectorError(
            "BLOCKED_REMOTE_SOURCE",
            "有限TDX远端候选均未提供可核实行业或概念成员原件",
            details=details,
        )
    _score, selected, selected_identity = best
    return RemoteSectorFiles(
        host=selected.host,
        files=selected.files,
        methods=selected.methods,
        archive_size_bytes=selected.archive_size_bytes,
        archive_sha256=selected.archive_sha256,
        archive_raw=selected.archive_raw,
        archive_member_crc32=selected.archive_member_crc32,
        missing_files=selected.missing_files,
        source_failures=selected.source_failures,
        candidate_attempts=tuple(attempts),
        candidate_sources=selected_identity.sources,
        candidate_count=len(candidates),
        skipped_candidate_count=discovery.skipped_candidate_count,
        discovery_sources=discovery.sources,
        _candidate_validation_token=_REMOTE_CANDIDATE_VALIDATION_TOKEN,
    )


def _parse_candidate_files(host: str, files: Mapping[str, SourceFileEvidence]):
    inputs = SectorSourceInputs(
        root=f"TDX_REMOTE_SERVER/{host}:{REMOTE_SECTOR_PORT}",
        files={item.relative_path: item for item in files.values()},
    )
    return parse_remote_sector_source_inputs_with_status(
        inputs, observed_at=datetime.now(timezone.utc)
    )


def _source_is_approved(source: str, host: str) -> bool:
    if source == _PYTDX_LIST_SOURCE:
        return True
    if source == _LEGACY_FALLBACK_SOURCE:
        return host in DEFAULT_REMOTE_SECTOR_HOSTS
    prefix = f"{_LOCAL_TDX_CONFIG_SOURCE}.IPAddress"
    return source.startswith(prefix) and source[len(prefix) :].isdecimal()


def validate_remote_sector_files(remote: RemoteSectorFiles) -> None:
    """核实候选来源、已验证节点、文件摘要及解析结果彼此绑定。"""
    if not _is_public_ipv4(remote.host):
        raise TdxRemoteSectorError(
            "REMOTE_HOST_INVALID", "TDX成员来源服务器不是经核实的公网IPv4"
        )
    if (
        remote._candidate_validation_token is not _REMOTE_CANDIDATE_VALIDATION_TOKEN
        or not remote.candidate_sources
        or any(
            not _source_is_approved(source, remote.host)
            for source in remote.candidate_sources
        )
    ):
        raise TdxRemoteSectorError(
            "REMOTE_CANDIDATE_SOURCE_INVALID",
            "TDX成员来源节点缺少受认可的候选来源身份",
        )
    selected_attempt = next(
        (
            item
            for item in remote.candidate_attempts
            if item.get("host") == remote.host
            and item.get("status") in {"COMPLETE", "PARTIAL"}
        ),
        None,
    )
    if (
        selected_attempt is None
        or not 1 <= remote.candidate_count <= MAX_REMOTE_CANDIDATE_COUNT
        or remote.skipped_candidate_count < 0
        or selected_attempt.get("candidate_sources") != list(remote.candidate_sources)
        or type(selected_attempt.get("available_kind_count")) is not int
        or not 1 <= selected_attempt["available_kind_count"] <= 2
    ):
        raise TdxRemoteSectorError(
            "REMOTE_CANDIDATE_IDENTITY_INVALID",
            "TDX成员来源地址未通过本次候选节点内容验证",
        )
    expected_paths = {
        "tdxhy.cfg": _TDXHY_PATH,
        "tdxzs.cfg": _TDXZS_PATH,
        "infoharbor_block.dat": _INFOHARBOR_PATH,
    }
    prefix = f"TDX_REMOTE_SERVER/{remote.host}:{REMOTE_SECTOR_PORT}/"
    allowed_methods = {
        "tdxhy.cfg": {"GET_BLOCK_FILE_RAW_0X02C5_0X06B9"},
        "tdxzs.cfg": {"GET_REPORT_FILE_0X06B9_ZHB_ZIP_MEMBER"},
        "infoharbor_block.dat": {
            "GET_BLOCK_FILE_RAW_0X02C5_0X06B9",
            "GET_REPORT_FILE_0X06B9",
        },
    }
    if len(remote.files) > len(expected_paths):
        raise TdxRemoteSectorError(
            "REMOTE_FILE_SET_INVALID", "TDX远端成员来源含白名单外文件"
        )
    for name, evidence in remote.files.items():
        if (
            name not in expected_paths
            or evidence.relative_path != expected_paths[name]
            or evidence.source_label
            != f"{prefix}{name if name != 'tdxzs.cfg' else 'zhb.zip!tdxzs.cfg'}"
            or evidence.size_bytes != len(evidence.raw)
            or evidence.size_bytes > MAX_REMOTE_BLOCK_FILE_BYTES
            or evidence.sha256 != hashlib.sha256(evidence.raw).hexdigest()
            or remote.methods.get(name) != evidence.source_protocol
            or evidence.source_protocol not in allowed_methods.get(name, set())
        ):
            raise TdxRemoteSectorError(
                "REMOTE_MIXED_SERVER_REVISION",
                "远端成员文件路径、完整字节摘要或单服务器来源身份不一致",
            )
    if remote.archive_raw is None:
        if (
            any(
                value is not None
                for value in (
                    remote.archive_size_bytes,
                    remote.archive_sha256,
                    remote.archive_member_crc32,
                )
            )
            or "tdxzs.cfg" in remote.files
        ):
            raise TdxRemoteSectorError(
                "REMOTE_ARCHIVE_EVIDENCE_INVALID", "zhb.zip归档证据缺少完整原始字节"
            )
    else:
        actual_archive_sha = hashlib.sha256(remote.archive_raw).hexdigest()
        if (
            len(remote.archive_raw) > MAX_REMOTE_ZHB_BYTES
            or remote.archive_size_bytes != len(remote.archive_raw)
            or remote.archive_sha256 != actual_archive_sha
            or "tdxzs.cfg" not in remote.files
            or remote.files["tdxzs.cfg"].source_archive_sha256 != actual_archive_sha
        ):
            raise TdxRemoteSectorError(
                "REMOTE_ARCHIVE_EVIDENCE_INVALID",
                "zhb.zip大小、SHA或tdxzs.cfg来源不匹配",
            )
        extracted, crc = _extract_tdxzs(remote.archive_raw)
        directory = remote.files["tdxzs.cfg"]
        if (
            extracted != directory.raw
            or crc != remote.archive_member_crc32
            or directory.size_bytes != len(extracted)
        ):
            raise TdxRemoteSectorError(
                "REMOTE_ARCHIVE_EVIDENCE_INVALID",
                "zhb.zip完整字节与tdxzs.cfg原件不匹配",
            )

    verified_files = {name: item.sha256 for name, item in sorted(remote.files.items())}
    expected_method_keys = set(remote.files)
    if remote.archive_raw is not None:
        expected_method_keys.add("zhb.zip")
    if (
        selected_attempt.get("verified_file_sha256") != verified_files
        or selected_attempt.get("archive_sha256") != remote.archive_sha256
        or set(remote.methods) != expected_method_keys
        or remote.archive_raw is not None
        and remote.methods.get("zhb.zip") != "GET_REPORT_FILE_0X06B9"
        or remote.archive_raw is None
        and "zhb.zip" in remote.methods
        or set(remote.missing_files) != set(expected_paths).difference(remote.files)
    ):
        raise TdxRemoteSectorError(
            "REMOTE_CANDIDATE_CONTENT_IDENTITY_INVALID",
            "候选通过记录与本次远端文件SHA不一致",
        )
    parse_outcome = _parse_candidate_files(remote.host, remote.files)
    parsed_kinds = {
        "INDUSTRY" if item.source_kind == "TDX_TDXHY_CFG" else "CONCEPT"
        for item in parse_outcome.snapshots
    }
    if (
        len(parsed_kinds) != selected_attempt.get("available_kind_count")
        or not parsed_kinds
        or selected_attempt.get("status")
        != (
            "COMPLETE"
            if len(parsed_kinds) == 2
            and not parse_outcome.rejected_sources
            and not remote.missing_files
            and not remote.source_failures
            and len(remote.files) == len(expected_paths)
            else "PARTIAL"
        )
    ):
        raise TdxRemoteSectorError(
            "REMOTE_CANDIDATE_CONTENT_NOT_COMPLETE",
            "已选TDX候选没有通过完整行业/概念字节解析",
        )


__all__ = [
    "DEFAULT_REMOTE_SECTOR_HOSTS",
    "MAX_REMOTE_CANDIDATE_COUNT",
    "MAX_REMOTE_CANDIDATE_SCAN_SECONDS",
    "MAX_REMOTE_BLOCK_FILE_BYTES",
    "MAX_REMOTE_SESSION_SECONDS",
    "MAX_REMOTE_ZHB_BYTES",
    "PYTDX_SERVER_LIST_URL",
    "REMOTE_SECTOR_PORT",
    "RemoteSectorFiles",
    "TdxRemoteSectorError",
    "fetch_remote_sector_files",
    "parse_pytdx_server_candidates",
    "validate_remote_sector_files",
]
