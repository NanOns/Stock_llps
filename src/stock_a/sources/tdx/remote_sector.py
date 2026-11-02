"""以有界通达信原生文件协议获取行业与概念成员来源。"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import io
import ipaddress
import socket
import struct
import time
from typing import Callable, Mapping, Sequence
import zipfile
import zlib

from stock_a.sources.tdx.sector_members import SourceFileEvidence


DEFAULT_REMOTE_SECTOR_HOSTS = ("124.71.187.122", "122.51.120.217")
REMOTE_SECTOR_PORT = 7709
REMOTE_BLOCK_CHUNK_BYTES = 30_000
MAX_REMOTE_BLOCK_FILE_BYTES = 8 * 1024 * 1024
MAX_REMOTE_ZHB_BYTES = 32 * 1024 * 1024
MAX_REMOTE_SESSION_SECONDS = 30.0
REMOTE_SOCKET_TIMEOUT_SECONDS = 5.0

_RESPONSE_PREFIX = bytes.fromhex("b1cb7400")
_TYPE_CONNECT = 0x000D
_TYPE_BLOCK_META = 0x02C5
_TYPE_BLOCK_INFO = 0x06B9
_TDXHY_PATH = "T0002/hq_cache/tdxhy.cfg"
_TDXZS_PATH = "T0002/hq_cache/tdxzs.cfg"
_INFOHARBOR_PATH = "T0002/hq_cache/infoharbor_block.dat"


class TdxRemoteSectorError(ValueError):
    """TDX远端文件协议或来源完整性不符合合同。"""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


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
    candidate_attempts: tuple[dict[str, str], ...]

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
        if address.version != 4 or timeout_seconds <= 0 or max_session_seconds <= 0:
            raise TdxRemoteSectorError(
                "REMOTE_HOST_INVALID", "TDX远端候选或网络时限无效"
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


def _download_candidate(host: str) -> _CandidateDownload:
    files: dict[str, SourceFileEvidence] = {}
    methods: dict[str, str] = {}
    archive_size_bytes: int | None = None
    archive_sha256: str | None = None
    archive_raw: bytes | None = None
    archive_member_crc32: str | None = None
    source_failures: list[dict[str, str]] = []
    with _TdxRemoteSession(host) as session:
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
    hosts: Sequence[str] = DEFAULT_REMOTE_SECTOR_HOSTS,
) -> RemoteSectorFiles:
    """从一个主站及最多一个明确备选站取完整板块成员来源。"""
    candidates = tuple(hosts)
    if not candidates or len(candidates) > 2 or len(set(candidates)) != len(candidates):
        raise TdxRemoteSectorError(
            "REMOTE_CANDIDATE_BUDGET_INVALID",
            "远端来源只允许一个主站和最多一个不重复的后备候选",
        )
    attempts: list[dict[str, str]] = []
    best: _CandidateDownload | None = None
    for index, host in enumerate(candidates):
        try:
            candidate = _download_candidate(host)
        except TdxRemoteSectorError as error:
            attempts.append(
                {"host": host, "status": "FAILED", "reason_code": error.code}
            )
            continue
        except (OSError, EOFError, TimeoutError, zlib.error):
            attempts.append(
                {
                    "host": host,
                    "status": "FAILED",
                    "reason_code": "REMOTE_TRANSPORT_FAILED",
                }
            )
            continue
        attempts.append(
            {
                "host": host,
                "status": "AVAILABLE"
                if candidate.available_kind_count
                else "NO_MEMBER_SOURCE",
                "available_kind_count": str(candidate.available_kind_count),
            }
        )
        if best is None or candidate.available_kind_count > best.available_kind_count:
            best = candidate
        if candidate.available_kind_count == 2:
            break
        if index == len(candidates) - 1:
            break
    if best is None or best.available_kind_count == 0:
        raise TdxRemoteSectorError(
            "BLOCKED_REMOTE_SOURCE", "有限TDX远端候选均未提供可核实行业或概念成员原件"
        )
    return RemoteSectorFiles(
        host=best.host,
        files=best.files,
        methods=best.methods,
        archive_size_bytes=best.archive_size_bytes,
        archive_sha256=best.archive_sha256,
        archive_raw=best.archive_raw,
        archive_member_crc32=best.archive_member_crc32,
        missing_files=best.missing_files,
        source_failures=best.source_failures,
        candidate_attempts=tuple(attempts),
    )


def validate_remote_sector_files(remote: RemoteSectorFiles) -> None:
    """核实返回文件、归档及单服务器来源身份彼此一致。"""
    if remote.host not in DEFAULT_REMOTE_SECTOR_HOSTS:
        raise TdxRemoteSectorError(
            "REMOTE_HOST_INVALID", "TDX成员来源服务器不在本批固定候选内"
        )
    expected_paths = {
        "tdxhy.cfg": _TDXHY_PATH,
        "tdxzs.cfg": _TDXZS_PATH,
        "infoharbor_block.dat": _INFOHARBOR_PATH,
    }
    prefix = f"TDX_REMOTE_SERVER/{remote.host}:{REMOTE_SECTOR_PORT}/"
    for name, evidence in remote.files.items():
        if (
            name not in expected_paths
            or evidence.relative_path != expected_paths[name]
            or evidence.source_label
            != f"{prefix}{name if name != 'tdxzs.cfg' else 'zhb.zip!tdxzs.cfg'}"
            or evidence.size_bytes != len(evidence.raw)
            or evidence.sha256 != hashlib.sha256(evidence.raw).hexdigest()
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
        return
    actual_archive_sha = hashlib.sha256(remote.archive_raw).hexdigest()
    if (
        remote.archive_size_bytes != len(remote.archive_raw)
        or remote.archive_sha256 != actual_archive_sha
        or "tdxzs.cfg" not in remote.files
        or remote.files["tdxzs.cfg"].source_archive_sha256 != actual_archive_sha
    ):
        raise TdxRemoteSectorError(
            "REMOTE_ARCHIVE_EVIDENCE_INVALID", "zhb.zip大小、SHA或tdxzs.cfg来源不匹配"
        )
    extracted, crc = _extract_tdxzs(remote.archive_raw)
    directory = remote.files["tdxzs.cfg"]
    if (
        extracted != directory.raw
        or crc != remote.archive_member_crc32
        or directory.size_bytes != len(extracted)
    ):
        raise TdxRemoteSectorError(
            "REMOTE_ARCHIVE_EVIDENCE_INVALID", "zhb.zip完整字节与tdxzs.cfg原件不匹配"
        )


__all__ = [
    "DEFAULT_REMOTE_SECTOR_HOSTS",
    "MAX_REMOTE_BLOCK_FILE_BYTES",
    "MAX_REMOTE_SESSION_SECONDS",
    "MAX_REMOTE_ZHB_BYTES",
    "REMOTE_SECTOR_PORT",
    "RemoteSectorFiles",
    "TdxRemoteSectorError",
    "fetch_remote_sector_files",
    "validate_remote_sector_files",
]
