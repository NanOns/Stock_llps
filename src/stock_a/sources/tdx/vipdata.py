"""通达信官网盘后完整包的有界发现、下载和ZIP校验。"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from html.parser import HTMLParser
import hashlib
import io
from itertools import islice
import math
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import time
from typing import Any, BinaryIO, Callable, Iterable, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener
import unicodedata
import uuid
import zipfile


OFFICIAL_PAGE_URL = "https://www.tdx.com.cn/article/vipdata.html"
OFFICIAL_PACKAGE_URL = "https://data.tdx.com.cn/vipdoc/hsjday.zip"
OFFICIAL_METADATA_URL = "https://data.tdx.com.cn/vipdoc/_hsjdayinfo.js"
_OFFICIAL_HOST = "tdx.com.cn"
_CHINA_TIMEZONE = timezone(timedelta(hours=8), name="Asia/Shanghai")
_G_TEMP_ROOT = Path("G:/codex_tmp")
_REDIRECT_STATUSES = {301, 302, 303, 307, 308}
_DAY_ENTRY = re.compile(
    r"^(?:vipdoc/)?(sh|sz|bj)/lday/(sh|sz|bj)([0-9A-Z]{6})\.day$", re.IGNORECASE
)
_DATE = re.compile(
    r"(?P<year>20[0-9]{2})\s*(?:[-/.]|年)\s*(?P<month>0?[1-9]|1[0-2])"
    r"\s*(?:[-/.]|月)\s*(?P<day>0?[1-9]|[12][0-9]|3[01])(?![0-9])\s*日?"
)
_DATE_LABEL = re.compile(
    r"更新(?:日期|至|时间)?|数据日期|数据更新|最后更新|update[_\s-]*(?:date|time)?|published[_\s-]*at",
    re.IGNORECASE,
)


class VipdataError(ValueError):
    """VIPDATA适配器拒绝条件和稳定原因码。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


@dataclass(frozen=True)
class VipdataBudgets:
    max_page_bytes: int = 2 * 1024 * 1024
    max_package_bytes: int = 2 * 1024 * 1024 * 1024
    max_zip_entries: int = 50000
    max_uncompressed_bytes: int = 4 * 1024 * 1024 * 1024
    max_entry_bytes: int = 32 * 1024 * 1024
    max_compression_ratio: int = 200
    timeout_seconds: float = 20.0
    max_total_seconds: float = 300.0
    max_redirects: int = 4
    reserve_free_bytes: int = 512 * 1024 * 1024

    def __post_init__(self) -> None:
        integer_limits = (
            "max_page_bytes",
            "max_package_bytes",
            "max_zip_entries",
            "max_uncompressed_bytes",
            "max_entry_bytes",
            "max_compression_ratio",
            "max_redirects",
            "reserve_free_bytes",
        )
        for name in integer_limits:
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise VipdataError("BUDGET_INVALID", f"{name}必须是正整数")
        for name in ("timeout_seconds", "max_total_seconds"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or value <= 0
            ):
                raise VipdataError("BUDGET_INVALID", f"{name}必须是有限正数")


@dataclass(frozen=True)
class VipdataSourceConfig:
    """登记在唯一runtime.defaults.json中的官网端点。"""

    page_url: str = OFFICIAL_PAGE_URL
    package_url: str = OFFICIAL_PACKAGE_URL
    metadata_url: str = OFFICIAL_METADATA_URL

    def __post_init__(self) -> None:
        values = (
            ("page_url", self.page_url, ".html"),
            ("package_url", self.package_url, ".zip"),
            ("metadata_url", self.metadata_url, ".js"),
        )
        for label, value, expected_suffix in values:
            if not isinstance(value, str) or not _is_official_https_url(value):
                raise VipdataError(
                    "SOURCE_CONFIG_INVALID", f"{label}必须是通达信官网HTTPS地址"
                )
            parsed = urlsplit(value)
            if (
                not parsed.path.lower().endswith(expected_suffix)
                or parsed.query
                or parsed.fragment
            ):
                raise VipdataError(
                    "SOURCE_CONFIG_INVALID", f"{label}与官网端点合同不匹配"
                )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "VipdataSourceConfig":
        if not isinstance(value, Mapping):
            raise VipdataError(
                "SOURCE_CONFIG_INVALID", "source_policy.vipdata_official必须是配置对象"
            )
        required = {"page_url", "package_url", "metadata_url"}
        if set(value) != required:
            raise VipdataError(
                "SOURCE_CONFIG_INVALID",
                "官网端点配置必须精确包含page_url、package_url、metadata_url",
            )
        return cls(**{key: value[key] for key in required})


@dataclass(frozen=True)
class VipdataPageObservation:
    source_page_url: str
    page_final_url: str
    page_observed_at: datetime
    http_status: int
    page_declared_update_date: date | None
    resolved_download_url: str | None
    status: str
    reason: str | None
    evidence: tuple[str, ...]
    metadata_url: str | None = None
    metadata_observed_at: datetime | None = None
    official_update_at: datetime | None = None
    metadata_sha256: str | None = None
    page_html_status: str | None = None
    page_html_reason: str | None = None
    download_url_source: str | None = None


@dataclass(frozen=True)
class VipdataDayEntry:
    path: str
    market: str
    symbol: str
    compressed_bytes: int
    uncompressed_bytes: int


@dataclass(frozen=True)
class VipdataPackage:
    file_path: Path
    file_name: str
    file_size: int
    sha256: str
    zip_entry_count: int
    zip_crc: str
    total_uncompressed_bytes: int
    entry_count_by_market: Mapping[str, int]
    day_entries: tuple[VipdataDayEntry, ...]
    download_final_url: str
    request_started_at: datetime
    download_finished_at: datetime
    download_attempt_count: int = 1
    official_cookie_challenge_solved: bool = False


@dataclass(frozen=True)
class VerifiedVipdataPackage:
    """一次本地重验产生的短期包句柄；不代表来源封口、PIT或PG接受。"""

    file_path: Path
    file_name: str
    file_size: int
    sha256: str
    zip_entry_count: int
    zip_crc: str
    total_uncompressed_bytes: int
    entry_count_by_market: Mapping[str, int]
    day_entries: tuple[VipdataDayEntry, ...]
    verified_at: datetime
    file_identity: tuple[int, int, int, int]


@dataclass(frozen=True)
class VipdataCandidatePool:
    """由ZIP目录动态派生的沪深主板/创业板研究代码池。"""

    candidates: tuple[dict[str, Any], ...]
    catalog: Mapping[str, Any]
    entry_classification_counts: Mapping[str, int]


@dataclass(frozen=True)
class VipdataRevisionComparison:
    """两个只读包的代码条目修订对照；不生成来源封口或PIT结论。"""

    old_package_sha256: str
    new_package_sha256: str
    target_session: date
    old_candidate_total: int
    new_candidate_total: int
    only_old: int
    only_new: int
    content_same: int
    content_changed: int
    not_evaluated: int
    target_bar_changes: Mapping[str, int]
    market_changes: Mapping[str, Mapping[str, int]]
    changed_samples: tuple[Mapping[str, Any], ...]
    not_evaluated_samples: tuple[Mapping[str, Any], ...]
    bytes_read: int
    records_read: int
    elapsed_seconds: float
    comparison_complete: bool
    capture_clock_role: str = "LOCAL_REVALIDATION_OBSERVED_AT_NOT_FIRST_AVAILABLE"


@dataclass(frozen=True)
class VipdataDayScan:
    entry: VipdataDayEntry
    record_count: int
    byte_count: int
    first_date: date
    last_date: date
    raw_byte_sha256: str
    target_session: date
    target_bar_present: bool
    parser_version: str


@dataclass(frozen=True)
class VipdataEntryCapture:
    """从已重验官网ZIP直接扫描的单个条目；Capture含包/条目来源身份。"""

    source_symbol: str
    market: str
    board: str
    target_session: date
    capture: Any
    scan_summary: Any
    rows: tuple[Any, ...]
    target_bar: Any | None
    future_source_rows_excluded: int


@dataclass(frozen=True)
class VipdataTargetScanBudget:
    """全候选目标日扫描的明确分页、读取量和耗时上限。"""

    page_size: int = 100
    max_total_bytes: int = 4 * 1024 * 1024 * 1024
    max_total_records: int = 125_000_000
    max_total_seconds: float = 1800.0
    max_candidate_bytes: int = 32 * 1024 * 1024
    max_candidate_records: int = 1_000_000
    max_candidate_seconds: float = 30.0
    chunk_records: int = 1024
    max_capture_bytes: int = 512 * 1024 * 1024

    def __post_init__(self) -> None:
        for name in (
            "page_size",
            "max_total_bytes",
            "max_total_records",
            "max_candidate_bytes",
            "max_candidate_records",
            "chunk_records",
            "max_capture_bytes",
        ):
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise VipdataError(
                    "TARGET_SCAN_BUDGET_INVALID", f"{name}必须是拒绝bool的正整数"
                )
        if self.page_size > 100:
            raise VipdataError(
                "TARGET_SCAN_BUDGET_INVALID", "目标日评价页大小不得超过100"
            )
        for name in ("max_total_seconds", "max_candidate_seconds"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value <= 0
            ):
                raise VipdataError(
                    "TARGET_SCAN_BUDGET_INVALID", f"{name}必须是有限正数"
                )


@dataclass(frozen=True)
class VipdataCandidateQuality:
    """一个来源候选在指定目标日的独立评价与逐字段组质量。"""

    source_index: int
    page_offset: int
    symbol: str
    market: str
    board: str
    status: str
    reason: str | None
    target_bar_present: bool
    record_count: int
    first_date: date | None
    last_date: date | None
    package_id: str
    package_sha256: str
    zip_entry_path: str
    target_session: date
    entry_sha256: str | None
    bytes_read: int
    future_rows_excluded: int
    observed_started_at: datetime | None
    observed_completed_at: datetime | None
    source_clock_role: str | None
    price_quality: str | None
    price_reason: str | None
    amount_quality: str | None
    amount_reason: str | None
    volume_quality: str | None
    volume_reason: str | None


@dataclass(frozen=True)
class VipdataTargetQualityPage:
    page_number: int
    offset: int
    candidate_count: int
    first_symbol: str | None
    last_symbol: str | None
    evaluated: int
    target_present: int
    target_missing: int
    read_failed: int
    not_evaluated: int
    bytes_read: int
    elapsed_seconds: float


@dataclass(frozen=True)
class VipdataTargetQualityReport:
    target_session: date
    package_id: str
    package_sha256: str
    package_verified_at: datetime
    candidate_total: int
    evaluated: int
    target_present: int
    target_missing: int
    read_failed: int
    not_evaluated: int
    bytes_read: int
    records_read: int
    elapsed_seconds: float
    by_market: Mapping[str, Mapping[str, int]]
    by_board: Mapping[str, Mapping[str, int]]
    field_quality: Mapping[str, Mapping[str, int]]
    pages: tuple[VipdataTargetQualityPage, ...]
    candidates: tuple[VipdataCandidateQuality, ...]
    budget: VipdataTargetScanBudget
    consumer_captures: Mapping[str, VipdataEntryCapture] = field(default_factory=dict)
    entry_read_elapsed_seconds: float = 0.0
    consumer_capture_read_elapsed_seconds: float = 0.0


@dataclass(frozen=True)
class _HttpOpened:
    response: Any
    status: int
    final_url: str
    headers: Mapping[str, str]


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _is_official_https_url(value: str) -> bool:
    try:
        parsed = urlsplit(value)
        host = (parsed.hostname or "").rstrip(".").lower()
        return (
            parsed.scheme.lower() == "https"
            and (host == _OFFICIAL_HOST or host.endswith("." + _OFFICIAL_HOST))
            and parsed.username is None
            and parsed.password is None
            and parsed.port in (None, 443)
        )
    except ValueError:
        return False


def _header(headers: Mapping[str, str], key: str) -> str | None:
    for name, value in headers.items():
        if str(name).lower() == key.lower():
            return str(value)
    return None


def _default_transport(
    url: str,
    timeout: float,
    *,
    challenge_cookie: str | None = None,
    referer_url: str = OFFICIAL_PAGE_URL,
) -> Any:
    opener = build_opener(_NoRedirect)
    request = Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0"
                if challenge_cookie
                else "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36"
            ),
            "Referer": referer_url,
            "Accept": "text/html,application/javascript,application/zip,application/octet-stream;q=0.9,*/*;q=0.5",
            "Cache-Control": "no-cache, max-age=0",
            "Pragma": "no-cache",
            **({"Cookie": challenge_cookie} if challenge_cookie else {}),
        },
        method="GET",
    )
    try:
        return opener.open(request, timeout=timeout)
    except HTTPError as error:
        return error
    except (URLError, TimeoutError, OSError) as error:
        raise VipdataError("SOURCE_HTTP_ERROR", "官网请求失败或超过连接超时") from error


def _open_get(
    url: str,
    *,
    budgets: VipdataBudgets,
    transport: Callable[[str, float], Any] | None,
    challenge_cookie: str | None = None,
    referer_url: str = OFFICIAL_PAGE_URL,
) -> _HttpOpened:
    if not _is_official_https_url(url):
        raise VipdataError(
            "DOWNLOAD_URL_UNTRUSTED", "只允许通达信官网域名上的HTTPS资源"
        )
    current = url
    get = transport or _default_transport
    for _redirect_index in range(budgets.max_redirects + 1):
        try:
            response = (
                _default_transport(
                    current,
                    float(budgets.timeout_seconds),
                    challenge_cookie=challenge_cookie,
                    referer_url=referer_url,
                )
                if transport is None
                else get(current, float(budgets.timeout_seconds))
            )
        except VipdataError:
            raise
        except Exception as error:
            raise VipdataError("SOURCE_HTTP_ERROR", "官网请求失败") from error
        status = getattr(response, "status", None)
        if status is None:
            status = response.getcode()
        final_url = (
            response.geturl()
            if callable(getattr(response, "geturl", None))
            else current
        )
        headers = getattr(response, "headers", {})
        if not _is_official_https_url(final_url):
            response.close()
            raise VipdataError(
                "DOWNLOAD_REDIRECT_UNTRUSTED", "官网请求重定向到非官方域名或非HTTPS地址"
            )
        if int(status) in _REDIRECT_STATUSES:
            location = _header(headers, "Location")
            response.close()
            if not location:
                raise VipdataError(
                    "REDIRECT_LOCATION_MISSING", "官网重定向响应缺少Location"
                )
            next_url = urljoin(current, location)
            if not _is_official_https_url(next_url):
                raise VipdataError(
                    "DOWNLOAD_REDIRECT_UNTRUSTED", "重定向目标不是通达信官网HTTPS资源"
                )
            current = next_url
            continue
        return _HttpOpened(response, int(status), final_url, headers)
    raise VipdataError("REDIRECT_LIMIT_EXCEEDED", "官网重定向次数超过预算")


def _read_limited(
    response: Any,
    *,
    limit: int,
    total_seconds: float,
    started: float,
    sink: BinaryIO | None = None,
    digest: Any | None = None,
    free_space_path: Path | None = None,
    reserve_free_bytes: int = 0,
) -> tuple[bytes | None, int]:
    body = io.BytesIO() if sink is None else None
    total = 0
    while True:
        if time.monotonic() - started > total_seconds:
            raise VipdataError("DOWNLOAD_TOTAL_TIMEOUT", "官网读取超过总耗时预算")
        try:
            chunk = response.read(min(64 * 1024, limit - total + 1))
        except TimeoutError as error:
            raise VipdataError(
                "SOURCE_READ_TIMEOUT", "HTTP读取超过socket超时"
            ) from error
        except OSError as error:
            raise VipdataError("SOURCE_HTTP_ERROR", "HTTP响应读取失败") from error
        if not chunk:
            break
        if total + len(chunk) > limit:
            raise VipdataError("DOWNLOAD_SIZE_LIMIT", "响应超过显式最大字节数")
        if free_space_path is not None and reserve_free_bytes:
            if shutil.disk_usage(free_space_path).free < reserve_free_bytes:
                raise VipdataError(
                    "DISK_SPACE_BUDGET", "G盘可用空间低于保留预算，停止下载"
                )
        if sink is not None:
            sink.write(chunk)
        else:
            body.write(chunk)
        if digest is not None:
            digest.update(chunk)
        total += len(chunk)
    return (body.getvalue() if body is not None else None), total


def _content_length(headers: Mapping[str, str]) -> int | None:
    raw = _header(headers, "Content-Length")
    if raw is None:
        return None
    try:
        value = int(raw.strip())
    except (AttributeError, ValueError) as error:
        raise VipdataError(
            "CONTENT_LENGTH_INVALID", "响应Content-Length不是非负整数"
        ) from error
    if value < 0:
        raise VipdataError("CONTENT_LENGTH_INVALID", "响应Content-Length不是非负整数")
    return value


def _official_download_challenge_cookie(body: bytes) -> str | None:
    """识别官网已观测的静态Cookie挑战；不执行或求值响应中的JavaScript。"""
    if not isinstance(body, bytes) or not body or len(body) > 64 * 1024:
        return None
    text = body.decode("ascii", "ignore")
    values: list[int] = []
    for key in ("WTKkN", "bOYDu", "wyeCN"):
        matches = re.findall(r"\b" + re.escape(key) + r":([0-9]{1,12})", text)
        if len(matches) != 1:
            return None
        values.append(int(matches[0]))
    ssid_matches = re.findall(r"\]\(t,([0-9]{1,12})\)", text)
    if len(ssid_matches) != 1:
        return None
    status = sum(values)
    return f"__tst_status={status}#; EO_Bot_Ssid={ssid_matches[0]}"


class _PageParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.fragments: list[str] = []
        self.attribute_values: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.attribute_values.extend(value for _name, value in attrs if value)

    def handle_data(self, data: str) -> None:
        if data.strip():
            self.fragments.append(data)


def _parse_calendar_dates(raw: str) -> tuple[date | None, tuple[str, ...], str | None]:
    found: dict[date, str] = {}
    labels = list(_DATE_LABEL.finditer(raw))
    for label in labels:
        window = raw[label.start() : min(len(raw), label.end() + 180)]
        for match in _DATE.finditer(window):
            try:
                parsed = date(
                    int(match.group("year")),
                    int(match.group("month")),
                    int(match.group("day")),
                )
            except ValueError:
                continue
            excerpt = re.sub(r"\s+", " ", window[:160]).strip()
            found.setdefault(parsed, excerpt[:180])
    if not found:
        return None, (), "页面没有可独立解析的更新日期"
    if len(found) != 1:
        return (
            None,
            tuple(sorted(value for value in found.values())),
            "页面更新日期存在歧义",
        )
    declared, excerpt = next(iter(found.items()))
    return declared, (excerpt,), None


def _page_download_urls(raw: str, attributes: list[str]) -> tuple[str, ...]:
    candidates: set[str] = set()
    fragments = list(attributes)
    fragments.extend(
        re.findall(
            r"(?:https?://|/)[^\s\"'<>\\]+?\.zip(?:\?[^\s\"'<>\\]*)?",
            raw,
            re.IGNORECASE,
        )
    )
    for fragment in fragments:
        cleaned = (
            fragment.strip().replace("\\/", "/").replace("&amp;", "&").strip("\"'` ")
        )
        for match in re.finditer(
            r"(?:https?://|/)[^\s\"'<>]*?\.zip(?:\?[^\s\"'<>]*)?",
            cleaned,
            re.IGNORECASE,
        ):
            candidates.add(match.group(0).replace("&amp;", "&"))
        if re.search(r"\.zip(?:\?|$)", cleaned, re.IGNORECASE):
            candidates.add(cleaned)
    return tuple(sorted(candidates))


def _validate_target_session(
    target_session: str,
    session_checker: Callable[[date], bool] | None,
) -> date:
    if (
        not isinstance(target_session, str)
        or re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", target_session) is None
    ):
        raise VipdataError("TARGET_SESSION_INVALID", "目标会话必须使用YYYY-MM-DD格式")
    try:
        target = date.fromisoformat(target_session)
    except ValueError as error:
        raise VipdataError(
            "TARGET_SESSION_INVALID", "目标会话不是有效公历日期"
        ) from error
    if target.isoformat() != target_session:
        raise VipdataError("TARGET_SESSION_INVALID", "目标会话日期格式不规范")
    if session_checker is None:
        from stock_a.research.entry import ResearchEntryError, _load_calendar

        try:
            _payload, _metadata, _revision, sessions = _load_calendar(target)
            is_session = target in sessions
        except ResearchEntryError as error:
            if error.code == "TARGET_NOT_TRADING_SESSION":
                raise VipdataError(
                    "TARGET_SESSION_NOT_TRADING_DAY", "目标日期不是已核验的正式交易会话"
                ) from error
            raise VipdataError(
                "TARGET_CALENDAR_UNVERIFIED", "项目交易日历无法独立验证目标会话"
            ) from error
    else:
        try:
            is_session = session_checker(target)
        except Exception as error:
            raise VipdataError(
                "TARGET_CALENDAR_UNVERIFIED", "目标会话交易日历校验失败"
            ) from error
    if is_session is not True:
        raise VipdataError(
            "TARGET_SESSION_NOT_TRADING_DAY", "目标日期不是已核验的正式交易会话"
        )
    return target


def _parse_official_update_time(body: bytes) -> datetime:
    try:
        text = body.decode("ascii")
    except UnicodeDecodeError as error:
        raise VipdataError(
            "SOURCE_METADATA_ENCODING_INVALID", "官方更新元数据不是ASCII JavaScript"
        ) from error
    assignments = re.findall(
        r"(?m)^\s*(?:window\.)?HSJDAY_SOFT_TIME\s*=\s*['\"]"
        r"(20[0-9]{2}-[0-9]{2}-[0-9]{2}(?:\s+[0-9]{2}:[0-9]{2}:[0-9]{2})?)['\"]\s*;?\s*$",
        text,
    )
    if len(assignments) != 1:
        raise VipdataError(
            "SOURCE_METADATA_DATE_INVALID", "官方元数据必须唯一声明HSJDAY_SOFT_TIME"
        )
    value = assignments[0]
    formats = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d")
    for format_string in formats:
        try:
            parsed = datetime.strptime(value, format_string)
            return parsed.replace(tzinfo=_CHINA_TIMEZONE)
        except ValueError:
            continue
    raise VipdataError(
        "SOURCE_METADATA_DATE_INVALID", "官方HSJDAY_SOFT_TIME不是有效日期时间"
    )


def parse_vipdata_page(
    html: str,
    *,
    source_page_url: str = OFFICIAL_PAGE_URL,
    page_final_url: str | None = None,
    page_observed_at: datetime | None = None,
    http_status: int = 200,
    target_session: str,
    session_checker: Callable[[date], bool] | None = None,
) -> VipdataPageObservation:
    target = _validate_target_session(target_session, session_checker)
    final_url = page_final_url or source_page_url
    observed_at = page_observed_at or datetime.now(timezone.utc)
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise VipdataError("OBSERVATION_TIME_INVALID", "官网观测时间必须含时区")
    if not _is_official_https_url(source_page_url) or not _is_official_https_url(
        final_url
    ):
        raise VipdataError(
            "SOURCE_PAGE_UNTRUSTED", "来源页及其最终响应必须位于通达信官网HTTPS域名"
        )
    if http_status != 200:
        return VipdataPageObservation(
            source_page_url,
            final_url,
            observed_at,
            http_status,
            None,
            None,
            "SOURCE_HTTP_ERROR",
            f"官网页面返回HTTP {http_status}",
            (),
        )
    parser = _PageParser()
    parser.feed(html)
    raw = "\n".join((*parser.fragments, *parser.attribute_values, html)).replace(
        "\\/", "/"
    )
    declared, evidence, date_error = _parse_calendar_dates(raw)
    raw_urls = _page_download_urls(raw, parser.attribute_values)
    resolved = tuple(dict.fromkeys(urljoin(final_url, value) for value in raw_urls))
    if date_error:
        return VipdataPageObservation(
            source_page_url,
            final_url,
            observed_at,
            http_status,
            None,
            None,
            "SOURCE_METADATA_UNAVAILABLE"
            if not declared
            else "SOURCE_METADATA_AMBIGUOUS",
            date_error,
            evidence,
        )
    if not resolved:
        return VipdataPageObservation(
            source_page_url,
            final_url,
            observed_at,
            http_status,
            declared,
            None,
            "SOURCE_DOWNLOAD_OBJECT_UNAVAILABLE",
            "页面没有可追溯的ZIP下载对象",
            evidence,
        )
    if len(resolved) != 1:
        return VipdataPageObservation(
            source_page_url,
            final_url,
            observed_at,
            http_status,
            declared,
            None,
            "SOURCE_DOWNLOAD_OBJECT_AMBIGUOUS",
            "页面发现多个ZIP对象，未自动选择",
            evidence,
        )
    resolved_url = resolved[0]
    if not _is_official_https_url(resolved_url):
        return VipdataPageObservation(
            source_page_url,
            final_url,
            observed_at,
            http_status,
            declared,
            None,
            "DOWNLOAD_URL_UNTRUSTED",
            "ZIP链接不在通达信官网HTTPS域名",
            evidence,
        )
    if declared < target:
        return VipdataPageObservation(
            source_page_url,
            final_url,
            observed_at,
            http_status,
            declared,
            resolved_url,
            "SOURCE_NOT_READY",
            "官网声明更新日期早于目标交易会话",
            evidence,
        )
    return VipdataPageObservation(
        source_page_url,
        final_url,
        observed_at,
        http_status,
        declared,
        resolved_url,
        "PACKAGE_CANDIDATE_READY",
        None,
        evidence,
    )


def inspect_vipdata_page(
    target_session: str,
    *,
    budgets: VipdataBudgets = VipdataBudgets(),
    source_config: VipdataSourceConfig = VipdataSourceConfig(),
    session_checker: Callable[[date], bool] | None = None,
    transport: Callable[[str, float], Any] | None = None,
    observed_at: datetime | None = None,
) -> VipdataPageObservation:
    target = _validate_target_session(target_session, session_checker)
    opened = _open_get(
        source_config.page_url,
        budgets=budgets,
        transport=transport,
        referer_url=source_config.page_url,
    )
    page_observed_at = observed_at or datetime.now(timezone.utc)
    try:
        if opened.status != 200:
            return VipdataPageObservation(
                source_config.page_url,
                opened.final_url,
                page_observed_at,
                opened.status,
                None,
                None,
                "SOURCE_HTTP_ERROR",
                f"官网页面返回HTTP {opened.status}",
                (),
                page_html_status="SOURCE_HTTP_ERROR",
            )
        length = _content_length(opened.headers)
        if length is not None and length > budgets.max_page_bytes:
            raise VipdataError("PAGE_SIZE_LIMIT", "官网页面超过读取预算")
        body, count = _read_limited(
            opened.response,
            limit=budgets.max_page_bytes,
            total_seconds=budgets.max_total_seconds,
            started=time.monotonic(),
        )
        if length is not None and count != length:
            raise VipdataError(
                "CONTENT_LENGTH_MISMATCH", "官网页面实际字节数与Content-Length不一致"
            )
        content = body or b""
        charset = "utf-8"
        content_type = _header(opened.headers, "Content-Type") or ""
        charset_match = re.search(
            r"charset\s*=\s*[\"']?([a-zA-Z0-9._-]+)", content_type, re.IGNORECASE
        )
        if charset_match:
            charset = charset_match.group(1)
        try:
            html = content.decode(charset)
        except (LookupError, UnicodeDecodeError):
            try:
                html = content.decode("gb18030")
            except UnicodeDecodeError as error:
                raise VipdataError(
                    "PAGE_ENCODING_INVALID", "官网页面编码无法安全解析"
                ) from error
        page_observation = parse_vipdata_page(
            html,
            source_page_url=source_config.page_url,
            page_final_url=opened.final_url,
            page_observed_at=page_observed_at,
            http_status=opened.status,
            target_session=target_session,
            session_checker=session_checker,
        )
    finally:
        opened.response.close()

    metadata_observed_at = datetime.now(timezone.utc)
    metadata_opened = _open_get(
        source_config.metadata_url,
        budgets=budgets,
        transport=transport,
        referer_url=source_config.page_url,
    )
    try:
        if metadata_opened.status != 200:
            return VipdataPageObservation(
                source_config.page_url,
                page_observation.page_final_url,
                page_observed_at,
                200,
                page_observation.page_declared_update_date,
                None,
                "SOURCE_METADATA_HTTP_ERROR",
                f"官方更新元数据返回HTTP {metadata_opened.status}",
                page_observation.evidence,
                source_config.metadata_url,
                metadata_observed_at,
                None,
                None,
                page_observation.status,
                page_observation.reason,
            )
        metadata_length = _content_length(metadata_opened.headers)
        metadata_body, metadata_bytes = _read_limited(
            metadata_opened.response,
            limit=min(budgets.max_page_bytes, 256 * 1024),
            total_seconds=budgets.max_total_seconds,
            started=time.monotonic(),
        )
        if metadata_length is not None and metadata_bytes != metadata_length:
            raise VipdataError(
                "CONTENT_LENGTH_MISMATCH",
                "官方更新元数据实际长度与Content-Length不一致",
            )
        official_update_at = _parse_official_update_time(metadata_body or b"")
    finally:
        metadata_opened.response.close()

    evidence = list(page_observation.evidence)
    evidence.append(f"官方日期来自{source_config.metadata_url}中的HSJDAY_SOFT_TIME")
    evidence.append(
        f"ZIP对象取自runtime.defaults.json配置：{source_config.package_url}"
    )
    if page_observation.status in {
        "SOURCE_DOWNLOAD_OBJECT_UNAVAILABLE",
        "SOURCE_METADATA_UNAVAILABLE",
    }:
        evidence.append(
            "官网页面HTML未提供完整ZIP链接或更新日期；按登记端点及官方脚本元数据继续核验"
        )
    package_url = source_config.package_url
    common = {
        "metadata_url": source_config.metadata_url,
        "metadata_observed_at": metadata_observed_at,
        "official_update_at": official_update_at,
        "metadata_sha256": hashlib.sha256(metadata_body or b"").hexdigest(),
        "page_html_status": page_observation.status,
        "page_html_reason": page_observation.reason,
        "download_url_source": "CONFIGURED_OFFICIAL_ENDPOINT",
    }
    if official_update_at.date() < target:
        return VipdataPageObservation(
            source_config.page_url,
            page_observation.page_final_url,
            page_observed_at,
            200,
            page_observation.page_declared_update_date,
            package_url,
            "SOURCE_NOT_READY",
            "官方HSJDAY_SOFT_TIME早于目标交易会话",
            tuple(evidence),
            **common,
        )
    return VipdataPageObservation(
        source_config.page_url,
        page_observation.page_final_url,
        page_observed_at,
        200,
        page_observation.page_declared_update_date,
        package_url,
        "PACKAGE_CANDIDATE_READY",
        None,
        tuple(evidence),
        **common,
    )


def _safe_temp_directory(path: str | Path) -> Path:
    try:
        root = Path(path)
        lexical_root = root.resolve(strict=False)
        allowed_root = _G_TEMP_ROOT.resolve(strict=True)
        if allowed_root.drive.upper() != "G:" or lexical_root.drive.upper() != "G:":
            raise VipdataError("TEMP_ROOT_INVALID", "临时输入必须位于G盘")
        lexical_root.relative_to(allowed_root)
        root_attributes = getattr(allowed_root.stat(), "st_file_attributes", 0)
        if root_attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400):
            raise VipdataError(
                "TEMP_ROOT_REPARSE_POINT", "G盘临时根不能是链接或重解析点"
            )
        root.mkdir(parents=True, exist_ok=True)
        original_attributes = getattr(root.lstat(), "st_file_attributes", 0)
        if original_attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400):
            raise VipdataError(
                "TEMP_ROOT_REPARSE_POINT", "G盘临时输入目录不能是链接或重解析点"
            )
        resolved = root.resolve(strict=True)
        resolved.relative_to(allowed_root)
        if not resolved.is_dir():
            raise VipdataError("TEMP_ROOT_INVALID", "临时输入目标不是目录")
        attributes = getattr(resolved.stat(), "st_file_attributes", 0)
        if attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400):
            raise VipdataError(
                "TEMP_ROOT_REPARSE_POINT", "G盘临时输入目录不能是链接或重解析点"
            )
        return resolved
    except VipdataError:
        raise
    except (OSError, ValueError) as error:
        raise VipdataError(
            "TEMP_ROOT_INVALID", "受控临时输入目录必须位于G:/codex_tmp"
        ) from error


def _safe_temp_file(path: str | Path) -> Path:
    try:
        original = Path(path)
        original_attributes = getattr(original.lstat(), "st_file_attributes", 0)
        if original_attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400):
            raise VipdataError(
                "TEMP_FILE_REPARSE_POINT", "临时输入文件不能是链接或重解析点"
            )
        resolved = original.resolve(strict=True)
        allowed_root = _G_TEMP_ROOT.resolve(strict=True)
        if allowed_root.drive.upper() != "G:" or resolved.drive.upper() != "G:":
            raise VipdataError("TEMP_FILE_INVALID", "临时输入文件必须位于G盘")
        resolved.relative_to(allowed_root)
        if original.is_symlink() or not resolved.is_file():
            raise VipdataError("TEMP_FILE_INVALID", "临时输入必须是G盘中的普通文件")
        attributes = getattr(resolved.stat(), "st_file_attributes", 0)
        if attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400):
            raise VipdataError(
                "TEMP_FILE_REPARSE_POINT", "临时输入文件不能是链接或重解析点"
            )
        return resolved
    except VipdataError:
        raise
    except (OSError, ValueError) as error:
        raise VipdataError(
            "TEMP_FILE_INVALID", "临时输入文件不在G:/codex_tmp受控根内"
        ) from error


def _safe_entry_path(name: str) -> tuple[str, ...]:
    if not isinstance(name, str) or not name or "\\" in name or "\x00" in name:
        raise VipdataError("ZIP_PATH_INVALID", "ZIP条目路径为空或使用不安全分隔符")
    normalized = unicodedata.normalize("NFC", name)
    if normalized.startswith("/") or normalized.startswith("//") or ":" in normalized:
        raise VipdataError("ZIP_PATH_INVALID", "ZIP条目不得是绝对路径、UNC或盘符路径")
    parts = PurePosixPath(normalized).parts
    if not parts or any(part in ("", ".", "..") for part in parts):
        raise VipdataError("ZIP_PATH_INVALID", "ZIP条目包含空路径段或目录穿越")
    if any(len(part) > 240 for part in parts) or len(normalized) > 1024:
        raise VipdataError("ZIP_PATH_TOO_LONG", "ZIP条目路径超过安全长度预算")
    return parts


def validate_vipdata_zip(
    zip_path: str | Path,
    *,
    budgets: VipdataBudgets = VipdataBudgets(),
) -> tuple[int, int, tuple[VipdataDayEntry, ...], dict[str, int]]:
    path = _safe_temp_file(zip_path)
    try:
        size = path.stat().st_size
        if size <= 0 or size > budgets.max_package_bytes:
            raise VipdataError(
                "PACKAGE_SIZE_INVALID", "ZIP文件为空或超过最大下载字节数"
            )
        if not zipfile.is_zipfile(path):
            raise VipdataError("PACKAGE_NOT_ZIP", "下载内容不是有效ZIP文件")
        with zipfile.ZipFile(path, "r") as archive:
            infos = archive.infolist()
            if not infos:
                raise VipdataError("ZIP_EMPTY", "ZIP中央目录没有条目")
            if len(infos) > budgets.max_zip_entries:
                raise VipdataError("ZIP_ENTRY_LIMIT", "ZIP条目数超过预算")
            total_uncompressed = 0
            seen: set[str] = set()
            entries: list[VipdataDayEntry] = []
            counts = {"sh": 0, "sz": 0, "bj": 0}
            for info in infos:
                parts = (
                    _safe_entry_path(info.filename.rstrip("/"))
                    if info.filename.rstrip("/")
                    else ()
                )
                if not parts:
                    raise VipdataError("ZIP_PATH_INVALID", "ZIP包含根目录条目")
                key = "/".join(parts).casefold()
                if key in seen:
                    raise VipdataError(
                        "ZIP_DUPLICATE_PATH", "ZIP包含规范化后重复的条目路径"
                    )
                seen.add(key)
                mode = (info.external_attr >> 16) & 0xFFFF
                if stat.S_ISLNK(mode):
                    raise VipdataError("ZIP_LINK_FORBIDDEN", "ZIP包含符号链接条目")
                if info.is_dir():
                    continue
                if mode and not (stat.S_ISREG(mode) or mode & 0o170000 == 0):
                    raise VipdataError("ZIP_FILE_TYPE_INVALID", "ZIP条目不是普通文件")
                if (
                    info.file_size < 0
                    or info.compress_size < 0
                    or info.file_size > budgets.max_entry_bytes
                ):
                    raise VipdataError(
                        "ZIP_ENTRY_SIZE_LIMIT", "ZIP单条目解压大小非法或超预算"
                    )
                total_uncompressed += info.file_size
                if total_uncompressed > budgets.max_uncompressed_bytes:
                    raise VipdataError("ZIP_TOTAL_SIZE_LIMIT", "ZIP总解压大小超过预算")
                if info.file_size and (
                    info.compress_size == 0
                    or info.file_size
                    > info.compress_size * budgets.max_compression_ratio
                ):
                    raise VipdataError(
                        "ZIP_COMPRESSION_RATIO_LIMIT", "ZIP条目压缩比超过预算"
                    )
                match = _DAY_ENTRY.fullmatch("/".join(parts))
                if not match:
                    raise VipdataError(
                        "ZIP_ENTRY_FAMILY_UNEXPECTED",
                        f"VIPDATA ZIP包含未登记的文件族：{info.filename}",
                    )
                folder_market, file_market, symbol = match.groups()
                market = folder_market.lower()
                if file_market.lower() != market:
                    raise VipdataError(
                        "ZIP_MARKET_PATH_MISMATCH", "ZIP日线文件市场与目录不一致"
                    )
                if info.file_size % 32 != 0:
                    raise VipdataError(
                        "ZIP_DAY_LENGTH_INVALID",
                        f"TDX .day文件必须为空或由32字节记录组成：{info.filename}",
                    )
                counts[market] += 1
                entries.append(
                    VipdataDayEntry(
                        "/".join(parts),
                        market.upper(),
                        symbol.upper(),
                        info.compress_size,
                        info.file_size,
                    )
                )
            if not entries:
                raise VipdataError("ZIP_NO_DAY_ENTRIES", "ZIP没有可用的TDX日线文件")
            bad_entry = archive.testzip()
            if bad_entry is not None:
                raise VipdataError(
                    "ZIP_CRC_INVALID", f"ZIP条目CRC校验失败：{bad_entry}"
                )
            return len(infos), total_uncompressed, tuple(entries), counts
    except VipdataError:
        raise
    except (OSError, zipfile.BadZipFile, RuntimeError, NotImplementedError) as error:
        raise VipdataError(
            "ZIP_INVALID", "ZIP中央目录、压缩方法或内容校验失败"
        ) from error


def verify_existing_vipdata_package(
    zip_path: str | Path,
    *,
    expected_sha256: str,
    budgets: VipdataBudgets = VipdataBudgets(),
) -> VerifiedVipdataPackage:
    """重验G盘现存包的字节身份、完整目录与CRC，不联网也不改包。"""
    if (
        not isinstance(expected_sha256, str)
        or re.fullmatch(r"[0-9a-fA-F]{64}", expected_sha256) is None
    ):
        raise VipdataError(
            "PACKAGE_SHA_INVALID", "显式VIPDATA包必须提供64位十六进制SHA-256"
        )
    path = _safe_temp_file(zip_path)
    before = path.stat()
    if before.st_size <= 0 or before.st_size > budgets.max_package_bytes:
        raise VipdataError("PACKAGE_SIZE_INVALID", "本地ZIP为空或超过最大包字节数")
    started = time.monotonic()
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            while True:
                if time.monotonic() - started > budgets.max_total_seconds:
                    raise VipdataError(
                        "PACKAGE_VERIFY_TIMEOUT", "本地ZIP SHA校验超过总耗时预算"
                    )
                chunk = source.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
        actual_sha256 = digest.hexdigest()
        if actual_sha256 != expected_sha256.lower():
            raise VipdataError(
                "PACKAGE_SHA_MISMATCH", "本地ZIP SHA-256与显式绑定值不一致"
            )
        entry_count, total_uncompressed, entries, by_market = validate_vipdata_zip(
            path, budgets=budgets
        )
        if time.monotonic() - started > budgets.max_total_seconds:
            raise VipdataError(
                "PACKAGE_VERIFY_TIMEOUT", "本地ZIP SHA及CRC校验超过总耗时预算"
            )
        after = path.stat()
        identity_before = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        )
        identity_after = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        if identity_before != identity_after:
            raise VipdataError(
                "PACKAGE_CHANGED", "完整校验期间ZIP文件身份或大小发生变化"
            )
    except VipdataError:
        raise
    except OSError as error:
        raise VipdataError("PACKAGE_VERIFY_IO_ERROR", "读取G盘本地ZIP失败") from error
    return VerifiedVipdataPackage(
        file_path=path,
        file_name=path.name,
        file_size=after.st_size,
        sha256=actual_sha256,
        zip_entry_count=entry_count,
        zip_crc="PASS",
        total_uncompressed_bytes=total_uncompressed,
        entry_count_by_market=by_market,
        day_entries=entries,
        verified_at=datetime.now(timezone.utc),
        file_identity=identity_after,
    )


def vipdata_research_candidate_pool(
    package: VerifiedVipdataPackage,
) -> VipdataCandidatePool:
    """动态按现役符号合同分出沪深主板/GEM候选，BJ和非目标代码保留分账。"""
    from stock_a.research.entry import ResearchEntryError, _normalize_symbol

    if not isinstance(package, VerifiedVipdataPackage):
        raise VipdataError(
            "PACKAGE_HANDLE_INVALID", "候选池必须来自本次本地完整校验的VIPDATA包"
        )
    started = time.monotonic()
    candidates: list[dict[str, Any]] = []
    classifications: dict[str, int] = {}
    seen: dict[str, str] = {}
    excluded_out_of_scope = 0
    invalid_names = 0
    for entry in package.day_entries:
        market = entry.market.upper()
        code = entry.symbol.upper()
        category: str
        if market == "BJ":
            category = "BJ_OPTIONAL_DEFERRED"
        elif market not in {"SH", "SZ"}:
            category = "OTHER_MARKET"
        elif re.fullmatch(r"[0-9]{6}", code) is None:
            category = "INVALID_SYMBOL_CODE"
            invalid_names += 1
        else:
            try:
                normalized, _directory, board = _normalize_symbol(f"{market}.{code}")
            except ResearchEntryError as error:
                if error.code != "SYMBOL_OUT_OF_SCOPE":
                    raise VipdataError(
                        "PACKAGE_SYMBOL_INVALID", "VIPDATA证券代码无法按现役规则核验"
                    ) from error
                excluded_out_of_scope += 1
                if market == "SH" and code.startswith(("688", "689")):
                    category = "SH_STAR"
                elif market == "SH" and code.startswith("900"):
                    category = "SH_B_SHARE"
                elif market == "SZ" and code.startswith("200"):
                    category = "SZ_B_SHARE"
                elif (
                    (market == "SZ" and code.startswith("399"))
                    or code.startswith("000")
                    and market == "SH"
                ):
                    category = "INDEX_OR_OTHER"
                else:
                    category = "SH_SZ_OTHER"
            else:
                category = f"{market}_{board}"
                if normalized in seen:
                    raise VipdataError(
                        "PACKAGE_SYMBOL_DUPLICATE", "ZIP同一市场代码出现多个日线条目"
                    )
                seen[normalized] = entry.path
                candidates.append(
                    {
                        "symbol": normalized,
                        "market": market,
                        "board": board,
                        "zip_entry_path": entry.path,
                        "zip_entry_bytes": entry.uncompressed_bytes,
                        "empty_placeholder": entry.uncompressed_bytes == 0,
                    }
                )
        classifications[category] = classifications.get(category, 0) + 1
    if sum(classifications.values()) != len(package.day_entries):
        raise VipdataError(
            "PACKAGE_CLASSIFICATION_INCOMPLETE", "VIPDATA目录条目未能互斥完整分区"
        )
    candidates.sort(key=lambda item: item["symbol"])
    by_market = {
        market: sum(item["market"] == market for item in candidates)
        for market in ("SH", "SZ")
    }
    by_board = {
        board: sum(item["board"] == board for item in candidates)
        for board in ("MAIN_BOARD", "GEM")
    }
    catalog = {
        "contract": "TDX_SOURCE_CANDIDATES_NON_PIT",
        "complete": True,
        "configured_root_count": 1,
        "directories": [
            {
                "directory": f"{market.lower()}/lday",
                "directory_entries": package.entry_count_by_market.get(
                    market.lower(), 0
                ),
            }
            for market in ("SH", "SZ", "BJ")
        ],
        "directory_entries_scanned": len(package.day_entries),
        "day_file_entries": len(package.day_entries),
        "candidate_count": len(candidates),
        "excluded_out_of_scope": excluded_out_of_scope,
        "invalid_day_filenames": invalid_names,
        "invalid_day_filename_samples": [],
        "invalid_day_filename_sample_limit": 100,
        "candidate_counts_by_market": by_market,
        "candidate_counts_by_board": by_board,
        "elapsed_seconds": round(time.monotonic() - started, 6),
        "source_package_id": f"TDX-HSJDAY-SHA256:{package.sha256}",
        "source_package_sha256": package.sha256,
        "entry_classification_counts": dict(sorted(classifications.items())),
        "empty_placeholder_count": sum(
            entry.uncompressed_bytes == 0 for entry in package.day_entries
        ),
        "empty_placeholder_paths": sorted(
            entry.path for entry in package.day_entries if entry.uncompressed_bytes == 0
        )[:100],
        "official_universe_denominator": "UNKNOWN",
        "identity_disclosure": "ZIP文件名仅形成来源代码候选；不证明稳定实体、正式成员、上市、当日可交易或ST状态",
    }
    return VipdataCandidatePool(
        tuple(candidates), catalog, dict(sorted(classifications.items()))
    )


def compare_vipdata_package_revisions(
    old_package: VerifiedVipdataPackage,
    new_package: VerifiedVipdataPackage,
    *,
    target_session: date,
    max_total_bytes: int = 4 * 1024 * 1024 * 1024,
    max_total_records: int = 125_000_000,
    max_total_seconds: float = 1800.0,
    sample_limit: int = 20,
) -> VipdataRevisionComparison:
    """按来源代码对照两个已验ZIP的沪深主板/GEM条目内容和目标日Bar。

    仅解压、摘要并轻量解码目标日记录，不调用Core/Profile，不保存包内容，
    也不把本机重验时钟解释成来源首次可得或严格PIT时钟。
    """
    from stock_a.sources.tdx.day import DAY_STRUCT, UnitContract, decode_day_record

    if not isinstance(old_package, VerifiedVipdataPackage) or not isinstance(
        new_package, VerifiedVipdataPackage
    ):
        raise VipdataError(
            "REVISION_PACKAGE_INVALID", "修订对照必须使用两份本次完整重验的ZIP"
        )
    if type(target_session) is not date:
        raise VipdataError(
            "REVISION_TARGET_INVALID", "修订对照目标必须是显式date交易会话"
        )
    for name, value in (
        ("max_total_bytes", max_total_bytes),
        ("max_total_records", max_total_records),
        ("sample_limit", sample_limit),
    ):
        if type(value) is not int or value <= 0:
            raise VipdataError(
                "REVISION_BUDGET_INVALID", f"{name}必须是拒绝bool的正整数"
            )
    if (
        isinstance(max_total_seconds, bool)
        or not isinstance(max_total_seconds, (int, float))
        or not math.isfinite(float(max_total_seconds))
        or max_total_seconds <= 0
    ):
        raise VipdataError("REVISION_BUDGET_INVALID", "max_total_seconds必须是有限正数")

    old_path = _safe_temp_file(old_package.file_path)
    new_path = _safe_temp_file(new_package.file_path)
    if (
        _file_identity(old_path) != old_package.file_identity
        or _file_identity(new_path) != new_package.file_identity
    ):
        raise VipdataError("PACKAGE_CHANGED", "修订对照开始前包文件身份已变化")
    old_pool = vipdata_research_candidate_pool(old_package)
    new_pool = vipdata_research_candidate_pool(new_package)
    old_by_symbol = {item["symbol"]: item for item in old_pool.candidates}
    new_by_symbol = {item["symbol"]: item for item in new_pool.candidates}
    old_entries_by_path = {item.path: item for item in old_package.day_entries}
    new_entries_by_path = {item.path: item for item in new_package.day_entries}
    all_symbols = tuple(sorted(set(old_by_symbol) | set(new_by_symbol)))
    only_old = only_new = content_same = content_changed = not_evaluated = 0
    bytes_read = records_read = 0
    started = time.monotonic()
    target_changes = {
        "comparable_entries": 0,
        "target_presence_changed": 0,
        "target_raw_record_changed": 0,
        "target_prices_changed": 0,
        "target_amount_raw_changed": 0,
        "target_volume_raw_changed": 0,
        "target_bar_added": 0,
        "target_bar_removed": 0,
        "invalid_target_price_group_old": 0,
        "invalid_target_price_group_new": 0,
    }
    market_changes: dict[str, dict[str, int]] = {}
    changed_samples: list[Mapping[str, Any]] = []
    not_evaluated_samples: list[Mapping[str, Any]] = []
    budget_exhausted = False
    raw_only_units = UnitContract("VIPDATA_REVISION_RAW_ONLY_R1", None, None)
    max_seconds = float(max_total_seconds)

    def scan_entry(
        archive: zipfile.ZipFile, candidate: Mapping[str, Any]
    ) -> dict[str, Any]:
        nonlocal bytes_read, records_read
        path = str(candidate["zip_entry_path"])
        try:
            info = archive.getinfo(path)
        except KeyError as error:
            raise VipdataError(
                "REVISION_ENTRY_MISSING", "已验证目录中的日线条目读取时不存在"
            ) from error
        expected_size = int(candidate["zip_entry_bytes"])
        if info.file_size != expected_size or expected_size % 32 != 0:
            raise VipdataError(
                "REVISION_ENTRY_INVALID", "修订条目大小与已验证目录或32字节记录合同不符"
            )
        if expected_size == 0:
            return {
                "entry_sha256": hashlib.sha256(b"").hexdigest(),
                "record_count": 0,
                "target_bar": None,
                "invalid_record_reason": None,
                "future_rows": 0,
            }
        digest = hashlib.sha256()
        target_record: bytes | None = None
        target_decoded = None
        record_count = 0
        future_rows = 0
        previous_day: int | None = None
        invalid_record_reason: str | None = None
        buffer = b""
        with archive.open(info, "r") as member:
            while True:
                if time.monotonic() - started >= max_seconds:
                    raise TimeoutError("REVISION_MAX_SECONDS_EXCEEDED")
                block = member.read(1024 * 1024)
                if not block:
                    break
                bytes_read += len(block)
                digest.update(block)
                buffer += block
                complete_bytes = len(buffer) - (len(buffer) % DAY_STRUCT.size)
                position = 0
                while position < complete_bytes:
                    raw_record = buffer[position : position + DAY_STRUCT.size]
                    day_number = DAY_STRUCT.unpack(raw_record)[0]
                    record_count += 1
                    records_read += 1
                    if previous_day is not None and day_number <= previous_day:
                        invalid_record_reason = "NON_INCREASING_OR_DUPLICATE_SESSION"
                    previous_day = day_number
                    try:
                        decoded = decode_day_record(raw_record, raw_only_units)
                    except (ValueError, OverflowError):
                        invalid_record_reason = (
                            invalid_record_reason or "INVALID_SESSION_DATE_OR_RECORD"
                        )
                        decoded = None
                    if decoded is not None:
                        if decoded.session_date == target_session:
                            if target_record is not None:
                                invalid_record_reason = "DUPLICATE_TARGET_SESSION"
                            else:
                                target_record = raw_record
                                target_decoded = decoded
                        if decoded.session_date > target_session:
                            future_rows += 1
                    position += DAY_STRUCT.size
                buffer = buffer[complete_bytes:]
        if buffer:
            raise VipdataError("REVISION_ENTRY_TAIL_INVALID", "日线条目尾部不足32字节")
        if record_count != expected_size // DAY_STRUCT.size:
            raise VipdataError(
                "REVISION_ENTRY_COUNT_MISMATCH", "日线条目记录数与验证目录不一致"
            )
        if target_decoded is None:
            rendered_target = None
        else:
            prices = target_decoded.prices
            ohlc = tuple(prices.value) if prices.quality == "OBSERVED" else None
            rendered_target = {
                "raw_record_hex": target_record.hex()
                if target_record is not None
                else None,
                "ohlc_cents": tuple(int(value * 100) for value in ohlc)
                if ohlc is not None
                else None,
                "price_quality": prices.quality,
                "price_reason": prices.reason,
                "amount_float32_hex": target_record[20:24].hex()
                if target_record is not None
                else None,
                "volume_raw": target_decoded.raw_volume.value
                if target_decoded.raw_volume
                else None,
                "reserved": target_decoded.reserved,
            }
        return {
            "entry_sha256": digest.hexdigest(),
            "record_count": record_count,
            "target_bar": rendered_target,
            "invalid_record_reason": invalid_record_reason,
            "future_rows": future_rows,
        }

    try:
        with (
            zipfile.ZipFile(old_path, "r") as old_archive,
            zipfile.ZipFile(new_path, "r") as new_archive,
        ):
            for symbol in all_symbols:
                old_candidate = old_by_symbol.get(symbol)
                new_candidate = new_by_symbol.get(symbol)
                market = str((old_candidate or new_candidate)["market"])
                bucket = market_changes.setdefault(
                    market,
                    {
                        "content_same": 0,
                        "content_changed": 0,
                        "not_evaluated": 0,
                        "only_old": 0,
                        "only_new": 0,
                    },
                )
                if old_candidate is None:
                    only_new += 1
                    bucket["only_new"] += 1
                    continue
                if new_candidate is None:
                    only_old += 1
                    bucket["only_old"] += 1
                    continue
                if budget_exhausted:
                    not_evaluated += 1
                    bucket["not_evaluated"] += 1
                    if len(not_evaluated_samples) < sample_limit:
                        not_evaluated_samples.append(
                            {"symbol": symbol, "reason": "REVISION_BUDGET_EXHAUSTED"}
                        )
                    continue
                old_entry = old_entries_by_path[old_candidate["zip_entry_path"]]
                new_entry = new_entries_by_path[new_candidate["zip_entry_path"]]
                required_bytes = (
                    old_entry.uncompressed_bytes + new_entry.uncompressed_bytes
                )
                required_records = required_bytes // 32
                if (
                    bytes_read + required_bytes > max_total_bytes
                    or records_read + required_records > max_total_records
                    or time.monotonic() - started >= max_seconds
                ):
                    budget_exhausted = True
                    not_evaluated += 1
                    bucket["not_evaluated"] += 1
                    if len(not_evaluated_samples) < sample_limit:
                        not_evaluated_samples.append(
                            {"symbol": symbol, "reason": "REVISION_BUDGET_EXHAUSTED"}
                        )
                    continue
                old_result = scan_entry(old_archive, old_candidate)
                new_result = scan_entry(new_archive, new_candidate)
                target_changes["comparable_entries"] += 1
                changed = old_result["entry_sha256"] != new_result["entry_sha256"]
                if changed:
                    content_changed += 1
                    bucket["content_changed"] += 1
                else:
                    content_same += 1
                    bucket["content_same"] += 1
                old_bar = old_result["target_bar"]
                new_bar = new_result["target_bar"]
                if (old_bar is None) != (new_bar is None):
                    target_changes["target_presence_changed"] += 1
                    if new_bar is None:
                        target_changes["target_bar_removed"] += 1
                    else:
                        target_changes["target_bar_added"] += 1
                if old_bar is not None and old_bar.get("price_quality") != "OBSERVED":
                    target_changes["invalid_target_price_group_old"] += 1
                if new_bar is not None and new_bar.get("price_quality") != "OBSERVED":
                    target_changes["invalid_target_price_group_new"] += 1
                if old_bar is not None and new_bar is not None:
                    if old_bar.get("raw_record_hex") != new_bar.get("raw_record_hex"):
                        target_changes["target_raw_record_changed"] += 1
                    if (
                        old_bar.get("ohlc_cents"),
                        old_bar.get("price_quality"),
                        old_bar.get("price_reason"),
                    ) != (
                        new_bar.get("ohlc_cents"),
                        new_bar.get("price_quality"),
                        new_bar.get("price_reason"),
                    ):
                        target_changes["target_prices_changed"] += 1
                    if old_bar.get("amount_float32_hex") != new_bar.get(
                        "amount_float32_hex"
                    ):
                        target_changes["target_amount_raw_changed"] += 1
                    if old_bar.get("volume_raw") != new_bar.get("volume_raw"):
                        target_changes["target_volume_raw_changed"] += 1
                if changed and len(changed_samples) < sample_limit:
                    changed_samples.append(
                        {
                            "symbol": symbol,
                            "market": market,
                            "board": old_candidate["board"],
                            "old_entry_path": old_candidate["zip_entry_path"],
                            "new_entry_path": new_candidate["zip_entry_path"],
                            "old_entry_sha256": old_result["entry_sha256"],
                            "new_entry_sha256": new_result["entry_sha256"],
                            "old_record_count": old_result["record_count"],
                            "new_record_count": new_result["record_count"],
                            "old_target_bar": old_bar,
                            "new_target_bar": new_bar,
                            "old_invalid_record_reason": old_result[
                                "invalid_record_reason"
                            ],
                            "new_invalid_record_reason": new_result[
                                "invalid_record_reason"
                            ],
                            "old_future_rows": old_result["future_rows"],
                            "new_future_rows": new_result["future_rows"],
                        }
                    )
    except TimeoutError as error:
        if str(error) != "REVISION_MAX_SECONDS_EXCEEDED":
            raise
        budget_exhausted = True
    except (OSError, zipfile.BadZipFile, RuntimeError, NotImplementedError) as error:
        raise VipdataError(
            "REVISION_ENTRY_READ_FAILED", "修订对照解压或读取已校验条目失败"
        ) from error
    if (
        _file_identity(old_path) != old_package.file_identity
        or _file_identity(new_path) != new_package.file_identity
    ):
        raise VipdataError("PACKAGE_CHANGED", "修订对照期间ZIP文件身份或元数据发生变化")

    # A timeout during a pair scan invalidates that partial pair and every following pair.
    classified_overlap = content_same + content_changed + not_evaluated
    expected_overlap = len(set(old_by_symbol) & set(new_by_symbol))
    if classified_overlap < expected_overlap:
        # A timeout may interrupt a pair before its digest is retained. Recheck the
        # classified counter using the explicit overlap budget; residual entries stay
        # untyped at the market level when only a partial pair was observed.
        remaining = expected_overlap - classified_overlap
        not_evaluated += remaining
        market_changes.setdefault(
            "UNKNOWN",
            {
                "content_same": 0,
                "content_changed": 0,
                "not_evaluated": 0,
                "only_old": 0,
                "only_new": 0,
            },
        )["not_evaluated"] += remaining
    complete = (
        only_old + only_new + content_same + content_changed + not_evaluated
        == len(all_symbols)
    )
    if not complete:
        raise VipdataError(
            "REVISION_PARTITION_INCOMPLETE", "来源修订候选分母未能互斥闭合"
        )
    return VipdataRevisionComparison(
        old_package_sha256=old_package.sha256,
        new_package_sha256=new_package.sha256,
        target_session=target_session,
        old_candidate_total=len(old_by_symbol),
        new_candidate_total=len(new_by_symbol),
        only_old=only_old,
        only_new=only_new,
        content_same=content_same,
        content_changed=content_changed,
        not_evaluated=not_evaluated,
        target_bar_changes=target_changes,
        market_changes=market_changes,
        changed_samples=tuple(changed_samples),
        not_evaluated_samples=tuple(not_evaluated_samples),
        bytes_read=bytes_read,
        records_read=records_read,
        elapsed_seconds=round(time.monotonic() - started, 6),
        comparison_complete=complete,
    )


def vipdata_candidate_pages(
    candidates: tuple[dict[str, Any], ...],
    *,
    page_size: int,
    offset: int = 0,
) -> tuple[tuple[dict[str, Any], ...], ...]:
    """按候选池既有稳定顺序分页；越界offset返回空页集合。"""
    if not isinstance(candidates, tuple):
        raise VipdataError(
            "CANDIDATE_PAGE_INVALID", "候选分页输入必须是稳定排序的tuple"
        )
    if (
        type(page_size) is not int
        or page_size <= 0
        or type(offset) is not int
        or offset < 0
    ):
        raise VipdataError(
            "CANDIDATE_PAGE_INVALID", "page_size须为正整数且offset须为非负整数"
        )
    if any(not isinstance(item, Mapping) for item in candidates):
        raise VipdataError("CANDIDATE_PAGE_INVALID", "候选分页含非法来源条目")
    selected = candidates[offset:]
    return tuple(
        tuple(selected[start : start + page_size])
        for start in range(0, len(selected), page_size)
    )


def _bound_vipdata_entry(
    package: VerifiedVipdataPackage,
    candidate: Mapping[str, Any],
) -> VipdataDayEntry:
    if not isinstance(package, VerifiedVipdataPackage):
        raise VipdataError(
            "PACKAGE_HANDLE_INVALID", "条目生产者必须使用本次完整校验的VIPDATA包句柄"
        )
    if _file_identity(_safe_temp_file(package.file_path)) != package.file_identity:
        raise VipdataError("PACKAGE_CHANGED", "包SHA/CRC校验后ZIP文件身份发生变化")
    symbol = candidate.get("symbol")
    market = candidate.get("market")
    path = candidate.get("zip_entry_path")
    entry_size = candidate.get("zip_entry_bytes")
    if (
        not isinstance(symbol, str)
        or not isinstance(market, str)
        or not isinstance(path, str)
        or type(entry_size) is not int
        or entry_size < 0
    ):
        raise VipdataError(
            "PACKAGE_ENTRY_INVALID", "候选缺少来源代码、市场、条目路径或大小"
        )
    match = re.fullmatch(r"(SH|SZ)\.([0-9]{6})", symbol)
    entry = next((item for item in package.day_entries if item.path == path), None)
    if (
        match is None
        or entry is None
        or match.group(1) != market
        or entry.market != market
        or entry.symbol != match.group(2)
        or entry.uncompressed_bytes != entry_size
    ):
        raise VipdataError(
            "PACKAGE_ENTRY_INVALID", "候选代码/市场/路径/大小与已验证ZIP目录不匹配"
        )
    return entry


def _read_vipdata_candidate(
    package: VerifiedVipdataPackage,
    candidate: Mapping[str, Any],
    *,
    target_session: date,
    units: Any,
    limits: Any,
    archive: zipfile.ZipFile,
    retain_rows: bool,
) -> VipdataEntryCapture:
    from stock_a.domain import ContractError
    from stock_a.sources.tdx.capture import (
        TdxCaptureRequest,
        TdxPackageEntryReference,
        build_tdx_capture_envelope,
    )
    from stock_a.sources.tdx.day import UnitContract
    from stock_a.sources.tdx.day_stream import DayReadError, DayRecordScan, ReadLimits
    from uuid import uuid4

    if (
        type(target_session) is not date
        or not isinstance(units, UnitContract)
        or not isinstance(limits, ReadLimits)
    ):
        raise VipdataError(
            "ENTRY_SCAN_ARGUMENT_INVALID",
            "條目掃描必須綁定date、UnitContract和ReadLimits",
        )
    entry = _bound_vipdata_entry(package, candidate)
    if entry.uncompressed_bytes == 0:
        raise VipdataError("ZIP_DAY_ENTRY_EMPTY", "零字节官方占位没有可评价的目标日K线")
    if (
        entry.uncompressed_bytes > limits.max_bytes
        or entry.uncompressed_bytes // 32 > limits.max_records
    ):
        wrapped = VipdataError("READ_LIMIT_EXCEEDED", "单条目大小超过显式日线读取预算")
        wrapped.context = {"bytes_read_actual": 0, "records_read_actual": 0}
        raise wrapped
    try:
        info = archive.getinfo(entry.path)
    except KeyError as error:
        raise VipdataError(
            "PACKAGE_ENTRY_MISSING", "已验证ZIP目录条目在读取时不存在"
        ) from error
    if info.file_size != entry.uncompressed_bytes:
        raise VipdataError("PACKAGE_CHANGED", "ZIP条目大小与验证后的中央目录不一致")

    rows: list[Any] = []
    target_bar = None
    future_rows = 0
    try:
        with archive.open(info, "r") as member:
            scan = DayRecordScan(member, units, limits)
            try:
                for located in scan:
                    decoded = located.decoded
                    if retain_rows:
                        rows.append(decoded)
                    if decoded.session_date == target_session:
                        target_bar = decoded
                    if decoded.session_date > target_session:
                        future_rows += 1
                summary = scan.summary
            except DayReadError as error:
                error.context.setdefault("bytes_read_actual", scan.progress[0])
                error.context.setdefault("records_read_actual", scan.progress[1])
                raise
    except VipdataError:
        raise
    except (
        DayReadError,
        OSError,
        KeyError,
        zipfile.BadZipFile,
        RuntimeError,
        NotImplementedError,
    ) as error:
        if isinstance(error, DayReadError):
            wrapped = VipdataError(error.code, str(error))
            wrapped.context = dict(error.context)
            raise wrapped from error
        raise VipdataError(
            "ZIP_ENTRY_READ_FAILED", "VIPDATA日线条目解压或读取失败"
        ) from error
    if _file_identity(_safe_temp_file(package.file_path)) != package.file_identity:
        raise VipdataError("PACKAGE_CHANGED", "读取VIPDATA条目期间ZIP文件身份发生变化")

    source_reference = TdxPackageEntryReference(
        package_id=f"TDX-HSJDAY-SHA256:{package.sha256}",
        package_sha256=package.sha256,
        official_package_url=OFFICIAL_PACKAGE_URL,
        package_verified_at=package.verified_at,
        package_file_size=package.file_size,
        zip_entry_count=package.zip_entry_count,
        zip_crc=package.zip_crc,
        source_symbol=str(candidate["symbol"]),
        zip_entry_path=entry.path,
        entry_sha256=summary.raw_byte_sha256,
        entry_byte_count=summary.byte_count,
        target_session=target_session,
        observed_started_at=summary.read_started_at,
        observed_completed_at=summary.read_completed_at,
    )
    request = TdxCaptureRequest(
        capture_id=str(uuid4()),
        dataset_id="TDX_DAILY_BARS",
        dataset_version="TDX_HSJDAY_ZIP_ENTRY_32B_R1",
        query_scope={
            "source_symbol": str(candidate["symbol"]),
            "target_session": target_session.isoformat(),
            "selection_mode": "OFFICIAL_SESSIONS_NOT_AFTER_TARGET",
            "research_mode": "NON_PIT_RETROSPECTIVE",
        },
        query_start=min(summary.first_date, target_session),
        query_end=max(summary.last_date, target_session),
        parser_version="stock_a.sources.tdx.day.decode_day_record",
        validation_contract="TDX_HSJDAY_ZIP_ENTRY_32_BYTE_LAYOUT_R1",
    )
    try:
        capture = build_tdx_capture_envelope(
            summary,
            request,
            source_package_entry=source_reference,
        )
    except (ContractError, ValueError) as error:
        raise VipdataError(
            "ENTRY_CAPTURE_INVALID", "VIPDATA条目无法形成结构化来源捕获"
        ) from error
    return VipdataEntryCapture(
        source_symbol=str(candidate["symbol"]),
        market=str(candidate["market"]),
        board=str(candidate["board"]),
        target_session=target_session,
        capture=capture,
        scan_summary=summary,
        rows=tuple(rows),
        target_bar=target_bar,
        future_source_rows_excluded=future_rows,
    )


def read_vipdata_candidate(
    package: VerifiedVipdataPackage,
    candidate: Mapping[str, Any],
    *,
    target_session: date,
    units: Any,
    limits: Any,
) -> VipdataEntryCapture:
    """从已校验官网ZIP直接构造Canonical可消费的条目Capture，不生成临时业务文件。"""
    path = _safe_temp_file(package.file_path)
    try:
        with zipfile.ZipFile(path, "r") as archive:
            return _read_vipdata_candidate(
                package,
                candidate,
                target_session=target_session,
                units=units,
                limits=limits,
                archive=archive,
                retain_rows=True,
            )
    except VipdataError:
        raise
    except (OSError, zipfile.BadZipFile, RuntimeError) as error:
        raise VipdataError(
            "ZIP_ENTRY_READ_FAILED", "VIPDATA ZIP在只读条目扫描时失败"
        ) from error


def build_vipdata_entry_capture_for_target(
    package: VerifiedVipdataPackage,
    candidate: Mapping[str, Any],
    *,
    scan_summary: Any,
    target_session: date,
    target_bar: Any | None,
    future_source_rows_excluded: int,
) -> VipdataEntryCapture:
    """为已单遍扫描的ZIP条目构造指定会话的精确来源捕获，不再读取ZIP。"""
    if (
        type(target_session) is not date
        or type(future_source_rows_excluded) is not int
        or future_source_rows_excluded < 0
    ):
        raise VipdataError("ENTRY_CAPTURE_INVALID", "历史捕获会话或未来行计数无效")
    if scan_summary is None or not getattr(scan_summary, "complete", False):
        raise VipdataError(
            "ENTRY_CAPTURE_INVALID", "历史捕获必须基于完整的单遍条目扫描"
        )
    entry = _bound_vipdata_entry(package, candidate)
    from stock_a.domain import ContractError
    from stock_a.sources.tdx.capture import (
        TdxCaptureRequest,
        TdxPackageEntryReference,
        build_tdx_capture_envelope,
    )

    source_reference = TdxPackageEntryReference(
        package_id=f"TDX-HSJDAY-SHA256:{package.sha256}",
        package_sha256=package.sha256,
        official_package_url=OFFICIAL_PACKAGE_URL,
        package_verified_at=package.verified_at,
        package_file_size=package.file_size,
        zip_entry_count=package.zip_entry_count,
        zip_crc=package.zip_crc,
        source_symbol=str(candidate["symbol"]),
        zip_entry_path=entry.path,
        entry_sha256=scan_summary.raw_byte_sha256,
        entry_byte_count=scan_summary.byte_count,
        target_session=target_session,
        observed_started_at=scan_summary.read_started_at,
        observed_completed_at=scan_summary.read_completed_at,
    )
    request = TdxCaptureRequest(
        capture_id=str(uuid.uuid4()),
        dataset_id="TDX_DAILY_BARS",
        dataset_version="TDX_HSJDAY_ZIP_ENTRY_32B_R1",
        query_scope={
            "source_symbol": str(candidate["symbol"]),
            "target_session": target_session.isoformat(),
            "selection_mode": "OFFICIAL_SESSIONS_NOT_AFTER_TARGET",
            "research_mode": "NON_PIT_RETROSPECTIVE",
        },
        query_start=min(scan_summary.first_date, target_session),
        query_end=max(scan_summary.last_date, target_session),
        parser_version="stock_a.sources.tdx.day.decode_day_record",
        validation_contract="TDX_HSJDAY_ZIP_ENTRY_32_BYTE_LAYOUT_R1",
    )
    try:
        envelope = build_tdx_capture_envelope(
            scan_summary,
            request,
            source_package_entry=source_reference,
        )
    except (ContractError, ValueError) as error:
        raise VipdataError(
            "ENTRY_CAPTURE_INVALID", "已扫描历史条目无法构造会话来源捕获"
        ) from error
    return VipdataEntryCapture(
        source_symbol=str(candidate["symbol"]),
        market=str(candidate["market"]),
        board=str(candidate["board"]),
        target_session=target_session,
        capture=envelope,
        scan_summary=scan_summary,
        rows=(),
        target_bar=target_bar,
        future_source_rows_excluded=future_source_rows_excluded,
    )


def _quality_bucket(field_value: Any) -> tuple[str, str | None]:
    quality = getattr(field_value, "quality", None)
    reason = getattr(field_value, "reason", None)
    if quality == "OBSERVED":
        return "OBSERVED", None
    if quality == "NOT_APPLICABLE":
        return "NOT_APPLICABLE", reason
    if isinstance(reason, str) and reason.startswith("DATA_INVALID_"):
        return "BAD_VALUE", reason
    return "UNKNOWN", reason


def _target_quality_row(
    candidate: Mapping[str, Any],
    *,
    package_sha256: str,
    target_session: date,
    source_index: int,
    page_offset: int,
    capture: VipdataEntryCapture | None,
    status: str,
    reason: str | None,
    bytes_read: int,
    record_count: int = 0,
) -> VipdataCandidateQuality:
    target = capture.target_bar if capture is not None else None
    source_ref = capture.capture.source_package_entry if capture is not None else None
    groups = {}
    if target is not None:
        groups = {
            name: _quality_bucket(getattr(target, name))
            for name in ("prices", "amount", "volume")
        }
    summary = capture.scan_summary if capture is not None else None
    return VipdataCandidateQuality(
        source_index=source_index,
        page_offset=page_offset,
        symbol=str(candidate["symbol"]),
        market=str(candidate["market"]),
        board=str(candidate["board"]),
        status=status,
        reason=reason,
        target_bar_present=target is not None,
        record_count=summary.record_count if summary is not None else record_count,
        first_date=summary.first_date if summary is not None else None,
        last_date=summary.last_date if summary is not None else None,
        package_id=f"TDX-HSJDAY-SHA256:{package_sha256}",
        package_sha256=package_sha256,
        zip_entry_path=str(candidate["zip_entry_path"]),
        target_session=target_session,
        entry_sha256=capture.capture.source_package_entry.entry_sha256
        if capture is not None
        else None,
        bytes_read=summary.byte_count if summary is not None else bytes_read,
        future_rows_excluded=capture.future_source_rows_excluded
        if capture is not None
        else 0,
        observed_started_at=source_ref.observed_started_at
        if source_ref is not None
        else None,
        observed_completed_at=source_ref.observed_completed_at
        if source_ref is not None
        else None,
        source_clock_role=source_ref.source_clock_role
        if source_ref is not None
        else None,
        price_quality=groups.get("prices", (None, None))[0],
        price_reason=groups.get("prices", (None, None))[1],
        amount_quality=groups.get("amount", (None, None))[0],
        amount_reason=groups.get("amount", (None, None))[1],
        volume_quality=groups.get("volume", (None, None))[0],
        volume_reason=groups.get("volume", (None, None))[1],
    )


class _VipdataConsumerCallbackError(Exception):
    """把消费者错误与ZIP/来源读取错误区分开，避免错误归类为来源故障。"""

    def __init__(self, original: Exception) -> None:
        self.original = original


def scan_vipdata_target_quality(
    package: VerifiedVipdataPackage,
    pool: VipdataCandidatePool,
    *,
    target_session: date,
    units: Any,
    budget: VipdataTargetScanBudget = VipdataTargetScanBudget(),
    market: str = "ALL",
    board: str = "ALL",
    evaluation_symbols: Iterable[str] | None = None,
    capture_symbols: Iterable[str] = (),
    on_candidate: Callable[[VipdataCandidateQuality, VipdataEntryCapture | None], None]
    | None = None,
    on_page: Callable[[VipdataTargetQualityPage], None] | None = None,
) -> VipdataTargetQualityReport:
    """在同一已校验ZIP句柄上评价全来源或明确指定的候选，不写文件或业务存储。"""
    from stock_a.sources.tdx.day import UnitContract
    from stock_a.sources.tdx.day_stream import DayReadError, ReadLimits

    if (
        not isinstance(package, VerifiedVipdataPackage)
        or not isinstance(pool, VipdataCandidatePool)
        or type(target_session) is not date
        or not isinstance(units, UnitContract)
        or not isinstance(budget, VipdataTargetScanBudget)
    ):
        raise VipdataError(
            "TARGET_SCAN_ARGUMENT_INVALID", "全范围质量扫描的包、日期、单位或预算无效"
        )
    if market not in {"ALL", "SH", "SZ"} or board not in {"ALL", "MAIN_BOARD", "GEM"}:
        raise VipdataError(
            "TARGET_SCAN_FILTER_INVALID", "市场或板别过滤不符合现役沪深研究范围"
        )
    candidates = tuple(
        item
        for item in pool.candidates
        if (market == "ALL" or item["market"] == market)
        and (board == "ALL" or item["board"] == board)
    )
    if isinstance(capture_symbols, (str, bytes)):
        raise VipdataError(
            "TARGET_SCAN_CAPTURE_INVALID", "消费者捕获代码必须是有限代码序列"
        )
    try:
        capture_symbol_list = tuple(islice(iter(capture_symbols), 101))
    except TypeError as error:
        raise VipdataError(
            "TARGET_SCAN_CAPTURE_INVALID", "消费者捕获代码必须是有限代码序列"
        ) from error
    if (
        len(capture_symbol_list) > 100
        or any(not isinstance(symbol, str) for symbol in capture_symbol_list)
        or len(capture_symbol_list) != len(set(capture_symbol_list))
    ):
        raise VipdataError(
            "TARGET_SCAN_CAPTURE_INVALID", "消费者捕获最多100个互异来源代码"
        )
    candidate_by_symbol = {item["symbol"]: item for item in candidates}
    if any(symbol not in candidate_by_symbol for symbol in capture_symbol_list):
        raise VipdataError(
            "TARGET_SCAN_CAPTURE_INVALID", "消费者捕获代码不属于本次过滤后的候选集合"
        )
    if evaluation_symbols is None:
        evaluation_symbol_set: set[str] | None = None
    else:
        if isinstance(evaluation_symbols, (str, bytes)):
            raise VipdataError(
                "TARGET_SCAN_SCOPE_INVALID", "目标评价范围必须是有限代码序列"
            )
        try:
            evaluation_symbol_list = tuple(islice(iter(evaluation_symbols), 101))
        except TypeError as error:
            raise VipdataError(
                "TARGET_SCAN_SCOPE_INVALID", "目标评价范围必须是有限代码序列"
            ) from error
        if (
            len(evaluation_symbol_list) > 100
            or any(not isinstance(symbol, str) for symbol in evaluation_symbol_list)
            or len(evaluation_symbol_list) != len(set(evaluation_symbol_list))
        ):
            raise VipdataError(
                "TARGET_SCAN_SCOPE_INVALID", "单次目标评价范围最多包含100个互异来源代码"
            )
        if any(symbol not in candidate_by_symbol for symbol in evaluation_symbol_list):
            raise VipdataError(
                "TARGET_SCAN_SCOPE_INVALID", "目标评价代码不属于本次过滤后的候选集合"
            )
        evaluation_symbol_set = set(evaluation_symbol_list)
        if not set(capture_symbol_list).issubset(evaluation_symbol_set):
            raise VipdataError(
                "TARGET_SCAN_SCOPE_INVALID", "消费者Capture必须属于本次实际评价范围"
            )
    capture_symbol_set = set(capture_symbol_list)
    capture_bytes_bound = sum(
        int(candidate_by_symbol[symbol]["zip_entry_bytes"])
        for symbol in capture_symbol_set
        if candidate_by_symbol[symbol].get("empty_placeholder") is not True
    )
    if capture_bytes_bound > budget.max_capture_bytes:
        raise VipdataError(
            "TARGET_SCAN_CAPTURE_BUDGET_EXCEEDED",
            "分页消费者所需原始行超过内存捕获预算",
        )
    pages = vipdata_candidate_pages(candidates, page_size=budget.page_size)
    evaluations: list[VipdataCandidateQuality] = []
    page_summaries: list[VipdataTargetQualityPage] = []
    consumer_captures: dict[str, VipdataEntryCapture] = {}
    by_market: dict[str, dict[str, int]] = {}
    by_board: dict[str, dict[str, int]] = {}
    field_quality = {
        name: {
            "OBSERVED": 0,
            "UNKNOWN": 0,
            "NOT_APPLICABLE": 0,
            "BAD_VALUE": 0,
            "denominator": 0,
        }
        for name in ("price", "amount", "volume")
    }
    started = time.monotonic()
    bytes_read = 0
    records_read = 0
    entry_read_elapsed = 0.0
    consumer_capture_read_elapsed = 0.0
    budget_exhausted_reason: str | None = None
    limits = ReadLimits(
        max_bytes=budget.max_candidate_bytes,
        max_records=budget.max_candidate_records,
        chunk_records=budget.chunk_records,
        max_seconds=budget.max_candidate_seconds,
    )
    path = _safe_temp_file(package.file_path)

    try:
        with zipfile.ZipFile(path, "r") as archive:
            for page_index, page in enumerate(pages):
                page_started = time.monotonic()
                page_begin = len(evaluations)
                for within_page, candidate in enumerate(page):
                    global_index = page_index * budget.page_size + within_page
                    consumer_capture: VipdataEntryCapture | None = None
                    if (
                        evaluation_symbol_set is not None
                        and candidate["symbol"] not in evaluation_symbol_set
                    ):
                        row = _target_quality_row(
                            candidate,
                            package_sha256=package.sha256,
                            target_session=target_session,
                            source_index=global_index,
                            page_offset=global_index,
                            capture=None,
                            status="NOT_EVALUATED",
                            reason="NOT_EVALUATED_FOR_FAST_PAGE",
                            bytes_read=0,
                        )
                    elif budget_exhausted_reason is None:
                        elapsed = time.monotonic() - started
                        remaining_bytes = budget.max_total_bytes - bytes_read
                        remaining_records = budget.max_total_records - records_read
                        candidate_size = int(candidate["zip_entry_bytes"])
                        candidate_records = candidate_size // 32
                        if elapsed >= budget.max_total_seconds:
                            budget_exhausted_reason = "TOTAL_TIME_BUDGET_EXHAUSTED"
                        elif candidate_size > remaining_bytes:
                            budget_exhausted_reason = "TOTAL_BYTE_BUDGET_EXHAUSTED"
                        elif candidate_records > remaining_records:
                            budget_exhausted_reason = "TOTAL_RECORD_BUDGET_EXHAUSTED"

                    if (
                        evaluation_symbol_set is not None
                        and candidate["symbol"] not in evaluation_symbol_set
                    ):
                        pass
                    elif budget_exhausted_reason is not None:
                        row = _target_quality_row(
                            candidate,
                            package_sha256=package.sha256,
                            target_session=target_session,
                            source_index=global_index,
                            page_offset=global_index,
                            capture=None,
                            status="NOT_EVALUATED",
                            reason=budget_exhausted_reason,
                            bytes_read=0,
                        )
                    elif bool(candidate.get("empty_placeholder")):
                        row = _target_quality_row(
                            candidate,
                            package_sha256=package.sha256,
                            target_session=target_session,
                            source_index=global_index,
                            page_offset=global_index,
                            capture=None,
                            status="TARGET_MISSING",
                            reason="EMPTY_PLACEHOLDER_NO_BAR",
                            bytes_read=0,
                        )
                    else:
                        entry_started = time.monotonic()
                        try:
                            capture = _read_vipdata_candidate(
                                package,
                                candidate,
                                target_session=target_session,
                                units=units,
                                limits=limits,
                                archive=archive,
                                retain_rows=(
                                    candidate["symbol"] in capture_symbol_set
                                    or on_candidate is not None
                                ),
                            )
                        except DayReadError as error:
                            actual_bytes = int(
                                error.context.get(
                                    "bytes_read_actual",
                                    error.context.get("byte_count", 0),
                                )
                            )
                            actual_records = int(
                                error.context.get(
                                    "records_read_actual",
                                    error.context.get("record_count", 0),
                                )
                            )
                            bytes_read += max(0, actual_bytes)
                            records_read += max(0, actual_records)
                            if error.code in {"READ_LIMIT_EXCEEDED", "READ_TIMEOUT"}:
                                row = _target_quality_row(
                                    candidate,
                                    package_sha256=package.sha256,
                                    target_session=target_session,
                                    source_index=global_index,
                                    page_offset=global_index,
                                    capture=None,
                                    status="NOT_EVALUATED",
                                    reason=error.code,
                                    bytes_read=actual_bytes,
                                    record_count=actual_records,
                                )
                                budget_exhausted_reason = error.code
                            else:
                                row = _target_quality_row(
                                    candidate,
                                    package_sha256=package.sha256,
                                    target_session=target_session,
                                    source_index=global_index,
                                    page_offset=global_index,
                                    capture=None,
                                    status="READ_FAILED",
                                    reason=error.code,
                                    bytes_read=actual_bytes,
                                    record_count=actual_records,
                                )
                        except VipdataError as error:
                            context = getattr(error, "context", {})
                            actual_bytes = int(
                                context.get(
                                    "bytes_read_actual", context.get("byte_count", 0)
                                )
                            )
                            actual_records = int(
                                context.get(
                                    "records_read_actual",
                                    context.get("record_count", 0),
                                )
                            )
                            bytes_read += max(0, actual_bytes)
                            records_read += max(0, actual_records)
                            is_budget = error.code in {
                                "READ_LIMIT_EXCEEDED",
                                "READ_TIMEOUT",
                            }
                            row = _target_quality_row(
                                candidate,
                                package_sha256=package.sha256,
                                target_session=target_session,
                                source_index=global_index,
                                page_offset=global_index,
                                capture=None,
                                status="NOT_EVALUATED" if is_budget else "READ_FAILED",
                                reason=error.code,
                                bytes_read=actual_bytes,
                                record_count=actual_records,
                            )
                            if is_budget:
                                budget_exhausted_reason = error.code
                        else:
                            consumer_capture = capture
                            if candidate["symbol"] in capture_symbol_set:
                                consumer_captures[candidate["symbol"]] = capture
                            row = _target_quality_row(
                                candidate,
                                package_sha256=package.sha256,
                                target_session=target_session,
                                source_index=global_index,
                                page_offset=global_index,
                                capture=capture,
                                status="TARGET_PRESENT"
                                if capture.target_bar is not None
                                else "TARGET_MISSING",
                                reason=None
                                if capture.target_bar is not None
                                else "TARGET_SESSION_BAR_MISSING",
                                bytes_read=capture.scan_summary.byte_count,
                                record_count=capture.scan_summary.record_count,
                            )
                            bytes_read += capture.scan_summary.byte_count
                            records_read += capture.scan_summary.record_count
                            if capture.target_bar is not None:
                                for name, quality, reason in (
                                    ("price", row.price_quality, row.price_reason),
                                    ("amount", row.amount_quality, row.amount_reason),
                                    ("volume", row.volume_quality, row.volume_reason),
                                ):
                                    field_quality[name]["denominator"] += 1
                                    field_quality[name][quality] += 1
                        finally:
                            read_elapsed = time.monotonic() - entry_started
                            entry_read_elapsed += read_elapsed
                            if candidate["symbol"] in capture_symbol_set:
                                consumer_capture_read_elapsed += read_elapsed
                    evaluations.append(row)
                    for counts in (
                        by_market.setdefault(row.market, {}),
                        by_board.setdefault(row.board, {}),
                    ):
                        counts[row.status] = counts.get(row.status, 0) + 1
                    if on_candidate is not None:
                        try:
                            on_candidate(row, consumer_capture)
                        except Exception as error:
                            raise _VipdataConsumerCallbackError(error) from error

                subset = evaluations[page_begin:]
                page_summary = VipdataTargetQualityPage(
                    page_number=page_index + 1,
                    offset=page_index * budget.page_size,
                    candidate_count=len(page),
                    first_symbol=page[0]["symbol"] if page else None,
                    last_symbol=page[-1]["symbol"] if page else None,
                    evaluated=sum(item.status != "NOT_EVALUATED" for item in subset),
                    target_present=sum(
                        item.status == "TARGET_PRESENT" for item in subset
                    ),
                    target_missing=sum(
                        item.status == "TARGET_MISSING" for item in subset
                    ),
                    read_failed=sum(item.status == "READ_FAILED" for item in subset),
                    not_evaluated=sum(
                        item.status == "NOT_EVALUATED" for item in subset
                    ),
                    bytes_read=sum(item.bytes_read for item in subset),
                    elapsed_seconds=round(time.monotonic() - page_started, 6),
                )
                page_summaries.append(page_summary)
                if on_page is not None:
                    on_page(page_summary)
    except _VipdataConsumerCallbackError as error:
        raise error.original
    except (OSError, zipfile.BadZipFile, RuntimeError) as error:
        raise VipdataError(
            "ZIP_TARGET_SCAN_FAILED", "VIPDATA目标日全范围扫描遇到包级读取错误"
        ) from error

    elapsed = time.monotonic() - started
    evaluated = sum(item.status != "NOT_EVALUATED" for item in evaluations)
    target_present = sum(item.status == "TARGET_PRESENT" for item in evaluations)
    target_missing = sum(item.status == "TARGET_MISSING" for item in evaluations)
    read_failed = sum(item.status == "READ_FAILED" for item in evaluations)
    not_evaluated = sum(item.status == "NOT_EVALUATED" for item in evaluations)
    total = len(candidates)
    if (
        total != evaluated + not_evaluated
        or evaluated != target_present + target_missing + read_failed
    ):
        raise VipdataError(
            "TARGET_SCAN_CLOSURE_INVALID", "目标日全范围候选、评价与bar状态分母未闭合"
        )
    if any(
        sum(
            values[key]
            for key in ("OBSERVED", "UNKNOWN", "NOT_APPLICABLE", "BAD_VALUE")
        )
        != values["denominator"]
        for values in field_quality.values()
    ):
        raise VipdataError(
            "TARGET_SCAN_QUALITY_CLOSURE_INVALID", "目标日逐字段质量计数与bar分母未闭合"
        )
    return VipdataTargetQualityReport(
        target_session=target_session,
        package_id=f"TDX-HSJDAY-SHA256:{package.sha256}",
        package_sha256=package.sha256,
        package_verified_at=package.verified_at,
        candidate_total=total,
        evaluated=evaluated,
        target_present=target_present,
        target_missing=target_missing,
        read_failed=read_failed,
        not_evaluated=not_evaluated,
        bytes_read=bytes_read,
        records_read=records_read,
        elapsed_seconds=round(elapsed, 6),
        by_market={key: dict(value) for key, value in sorted(by_market.items())},
        by_board={key: dict(value) for key, value in sorted(by_board.items())},
        field_quality={key: dict(value) for key, value in field_quality.items()},
        pages=tuple(page_summaries),
        candidates=tuple(evaluations),
        budget=budget,
        consumer_captures=dict(consumer_captures),
        entry_read_elapsed_seconds=round(entry_read_elapsed, 6),
        consumer_capture_read_elapsed_seconds=round(consumer_capture_read_elapsed, 6),
    )


def extract_vipdata_candidate_page(
    package: VerifiedVipdataPackage,
    candidates: tuple[dict[str, Any], ...],
    *,
    destination_parent: str | Path = Path("G:/codex_tmp/stock_a_vipdata"),
    max_extract_bytes: int = 512 * 1024 * 1024,
) -> tuple[Path, dict[str, Path]]:
    """一次ZIP身份校验后，仅解压本页条目；不为每只证券重算全包SHA。"""
    if not isinstance(package, VerifiedVipdataPackage):
        raise VipdataError(
            "PACKAGE_HANDLE_INVALID", "页面抽取必须使用本次完整校验包句柄"
        )
    if type(max_extract_bytes) is not int or max_extract_bytes <= 0:
        raise VipdataError("PACKAGE_EXTRACT_BUDGET_INVALID", "页面解压预算必须为正整数")
    total_bytes = sum(item.get("zip_entry_bytes", -1) for item in candidates)
    if any(
        type(item.get("zip_entry_bytes")) is not int or item["zip_entry_bytes"] < 0
        for item in candidates
    ):
        raise VipdataError("PACKAGE_ENTRY_INVALID", "页面含未绑定目录身份的ZIP条目")
    if total_bytes > max_extract_bytes:
        raise VipdataError(
            "PACKAGE_EXTRACT_BUDGET_EXCEEDED", "当前分页解压字节数超过显式页面预算"
        )
    path = _safe_temp_file(package.file_path)
    if _file_identity(path) != package.file_identity:
        raise VipdataError("PACKAGE_CHANGED", "SHA/CRC校验后VIPDATA文件身份发生变化")
    parent = _safe_temp_directory(destination_parent)
    if shutil.disk_usage(parent).free < total_bytes + 64 * 1024 * 1024:
        raise VipdataError(
            "PACKAGE_EXTRACT_DISK_BUDGET", "G盘空间不足以临时抽取当前页并保留64MiB"
        )
    extraction_root = parent / f"select-{uuid.uuid4().hex}"
    extraction_root.mkdir()
    extracted: dict[str, Path] = {}
    entries_by_path = {entry.path: entry for entry in package.day_entries}
    try:
        with zipfile.ZipFile(path, "r") as archive:
            for candidate in candidates:
                entry_path = candidate.get("zip_entry_path")
                entry = (
                    entries_by_path.get(entry_path)
                    if isinstance(entry_path, str)
                    else None
                )
                symbol = candidate.get("symbol")
                if entry is None or not isinstance(symbol, str):
                    raise VipdataError(
                        "PACKAGE_ENTRY_INVALID", "分页候选与已校验ZIP目录不匹配"
                    )
                normalized, market_directory = _candidate_entry_destination(symbol)
                if (
                    normalized != symbol
                    or candidate.get("market") != symbol.split(".", 1)[0]
                ):
                    raise VipdataError(
                        "PACKAGE_ENTRY_INVALID", "分页候选市场、代码与条目身份不匹配"
                    )
                if candidate.get("zip_entry_bytes") != entry.uncompressed_bytes:
                    raise VipdataError(
                        "PACKAGE_ENTRY_INVALID", "分页候选条目大小与已校验目录不匹配"
                    )
                relative = (
                    PurePosixPath("vipdoc")
                    / market_directory
                    / "lday"
                    / f"{market_directory}{symbol.split('.', 1)[1]}.day"
                )
                parts = _safe_entry_path(str(relative))
                output_path = extraction_root.joinpath(*parts)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.parent.resolve(strict=True).relative_to(extraction_root)
                info = archive.getinfo(entry.path)
                if info.file_size != entry.uncompressed_bytes:
                    raise VipdataError(
                        "PACKAGE_CHANGED", "ZIP条目大小与本次重验目录不一致"
                    )
                with (
                    archive.open(info, "r") as source,
                    output_path.open("xb") as target,
                ):
                    written = 0
                    while True:
                        chunk = source.read(64 * 1024)
                        if not chunk:
                            break
                        written += len(chunk)
                        if written > entry.uncompressed_bytes:
                            raise VipdataError(
                                "ZIP_ENTRY_SIZE_MISMATCH",
                                "分页条目解压长度超过中央目录",
                            )
                        target.write(chunk)
                    target.flush()
                    os.fsync(target.fileno())
                if written != entry.uncompressed_bytes or (
                    written and written % 32 != 0
                ):
                    raise VipdataError(
                        "ZIP_DAY_LENGTH_INVALID", "分页日线条目长度不符合32字节记录合同"
                    )
                extracted[symbol] = output_path
        if _file_identity(path) != package.file_identity:
            raise VipdataError("PACKAGE_CHANGED", "分页抽取期间VIPDATA文件身份发生变化")
    except VipdataError:
        if extraction_root.exists():
            shutil.rmtree(extraction_root)
        raise
    except (OSError, KeyError, zipfile.BadZipFile, RuntimeError) as error:
        if extraction_root.exists():
            shutil.rmtree(extraction_root)
        raise VipdataError("ZIP_EXTRACT_INVALID", "分页ZIP条目未能安全抽取") from error
    return extraction_root, extracted


def cleanup_vipdata_extraction(extraction_root: str | Path) -> None:
    """仅移除本调用创建、位于G盘临时根内的唯一分页抽取目录。"""
    root = Path(extraction_root)
    parent = _safe_temp_directory(root.parent)
    resolved = root.resolve(strict=True)
    if (
        resolved.parent != parent
        or not resolved.name.startswith("select-")
        or root.is_symlink()
    ):
        raise VipdataError(
            "EXTRACTION_CLEANUP_REFUSED", "拒绝清理非本次创建的VIPDATA临时目录"
        )
    shutil.rmtree(resolved)


def _candidate_entry_destination(symbol: str) -> tuple[str, str]:
    from stock_a.research.entry import _normalize_symbol

    normalized, directory, _board = _normalize_symbol(symbol)
    return normalized, directory


def _file_identity(path: Path) -> tuple[int, int, int, int]:
    info = path.stat()
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)


def download_vipdata_package(
    observation: VipdataPageObservation,
    *,
    destination_root: str | Path | None = None,
    budgets: VipdataBudgets = VipdataBudgets(),
    transport: Callable[[str, float], Any] | None = None,
) -> VipdataPackage:
    if (
        observation.status != "PACKAGE_CANDIDATE_READY"
        or not observation.resolved_download_url
    ):
        raise VipdataError(
            "SOURCE_NOT_READY",
            "官方HSJDAY_SOFT_TIME或已配置ZIP对象尚未通过候选资格检查",
        )
    if not _is_official_https_url(observation.resolved_download_url):
        raise VipdataError(
            "DOWNLOAD_URL_UNTRUSTED", "候选下载对象不是通达信官网HTTPS资源"
        )
    if destination_root is None:
        destination_root = (
            Path("G:/codex_tmp/stock_a_vipdata") / f"run-{uuid.uuid4().hex}"
        )
    root = _safe_temp_directory(destination_root)
    request_started_at = datetime.now(timezone.utc)
    opened = _open_get(
        observation.resolved_download_url,
        budgets=budgets,
        transport=transport,
        referer_url=observation.source_page_url,
    )
    download_attempt_count = 1
    challenge_cookie_used = False
    first_content_type = (_header(opened.headers, "Content-Type") or "").lower()
    if opened.status == 200 and "text/html" in first_content_type:
        try:
            challenge_body, _challenge_size = _read_limited(
                opened.response,
                limit=64 * 1024,
                total_seconds=budgets.max_total_seconds,
                started=time.monotonic(),
            )
        finally:
            opened.response.close()
        challenge_cookie = _official_download_challenge_cookie(challenge_body or b"")
        if challenge_cookie is None:
            raise VipdataError(
                "DOWNLOAD_RESPONSE_HTML",
                "ZIP地址返回非预期HTML，未识别为通达信官方静态挑战",
            )
        opened = _open_get(
            observation.resolved_download_url,
            budgets=budgets,
            transport=transport,
            challenge_cookie=challenge_cookie,
            referer_url=observation.source_page_url,
        )
        download_attempt_count = 2
        challenge_cookie_used = True
    monotonic_started = time.monotonic()
    partial = root / f"vipdata-{uuid.uuid4().hex}.zip.partial"
    try:
        if opened.status != 200:
            raise VipdataError(
                "DOWNLOAD_HTTP_ERROR", f"官网数据包返回HTTP {opened.status}"
            )
        content_type = (_header(opened.headers, "Content-Type") or "").lower()
        if "text/html" in content_type:
            raise VipdataError(
                "DOWNLOAD_RESPONSE_HTML", "ZIP地址返回HTML响应，拒绝将其当作数据包下载"
            )
        content_length = _content_length(opened.headers)
        if content_length is not None and content_length > budgets.max_package_bytes:
            raise VipdataError(
                "DOWNLOAD_SIZE_LIMIT", "官网数据包Content-Length超过预算"
            )
        free = shutil.disk_usage(root).free
        required = (content_length or 0) + budgets.reserve_free_bytes
        if content_length is None:
            required = budgets.reserve_free_bytes
        if free < required:
            raise VipdataError(
                "DISK_SPACE_BUDGET", "G盘可用空间不足以保留数据包及安全余量"
            )
        digest = hashlib.sha256()
        with partial.open("xb") as output:
            _body, written = _read_limited(
                opened.response,
                limit=budgets.max_package_bytes,
                total_seconds=budgets.max_total_seconds,
                started=monotonic_started,
                sink=output,
                digest=digest,
                free_space_path=root,
                reserve_free_bytes=budgets.reserve_free_bytes,
            )
            output.flush()
            os.fsync(output.fileno())
        if content_length is not None and written != content_length:
            raise VipdataError(
                "CONTENT_LENGTH_MISMATCH", "数据包实际字节数与Content-Length不一致"
            )
        sha256 = digest.hexdigest()
        final = root / f"vipdata-{sha256}.zip"
        if final.exists():
            raise VipdataError(
                "PACKAGE_ALREADY_EXISTS", "同SHA数据包已存在；拒绝覆盖或复用旧临时文件"
            )
        os.rename(partial, final)
        try:
            zip_count, expanded, day_entries, by_market = validate_vipdata_zip(
                final, budgets=budgets
            )
        except VipdataError as error:
            rejected = root / f"rejected-vipdata-{sha256}.zip"
            if rejected.exists():
                raise VipdataError(
                    "PACKAGE_ALREADY_EXISTS", "同SHA拒绝包已存在；拒绝覆盖"
                ) from error
            os.rename(final, rejected)
            raise VipdataError(
                error.code,
                f"{error}；完整原始ZIP保留于G盘临时目录，SHA-256={sha256}，文件={rejected}",
            ) from error
        return VipdataPackage(
            file_path=final,
            file_name=final.name,
            file_size=written,
            sha256=sha256,
            zip_entry_count=zip_count,
            zip_crc="PASS",
            total_uncompressed_bytes=expanded,
            entry_count_by_market=by_market,
            day_entries=day_entries,
            download_final_url=opened.final_url,
            request_started_at=request_started_at,
            download_finished_at=datetime.now(timezone.utc),
            download_attempt_count=download_attempt_count,
            official_cookie_challenge_solved=challenge_cookie_used,
        )
    finally:
        opened.response.close()
        if partial.exists():
            partial.unlink()


def _verify_package_file(
    package: VipdataPackage, *, max_total_seconds: float = 300.0
) -> Path:
    package_path = _safe_temp_file(package.file_path)
    if package_path.stat().st_size != package.file_size:
        raise VipdataError("PACKAGE_CHANGED", "下载后ZIP文件大小发生变化，拒绝读取")
    digest = hashlib.sha256()
    started = time.monotonic()
    with package_path.open("rb") as source:
        while True:
            if time.monotonic() - started > max_total_seconds:
                raise VipdataError(
                    "PACKAGE_VERIFY_TIMEOUT", "ZIP SHA复核超过总耗时预算"
                )
            chunk = source.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    if digest.hexdigest() != package.sha256:
        raise VipdataError("PACKAGE_CHANGED", "下载后ZIP SHA-256发生变化，拒绝读取")
    return package_path


def scan_vipdata_day_entry(
    package: VipdataPackage,
    *,
    market: str,
    symbol: str,
    target_session: str,
    units: Any,
    limits: Any,
    session_checker: Callable[[date], bool] | None = None,
    max_total_seconds: float = 300.0,
) -> VipdataDayScan:
    """使用现役32字节流式Scanner检查ZIP内指定证券的日期覆盖。"""
    from stock_a.sources.tdx.day import UnitContract
    from stock_a.sources.tdx.day_stream import DayReadError, iter_day_records

    if not isinstance(units, UnitContract):
        raise VipdataError("UNIT_CONTRACT_INVALID", "ZIP日线扫描必须提供显式单位合同")
    target = _validate_target_session(target_session, session_checker)
    market_value = market.upper() if isinstance(market, str) else ""
    code = symbol.strip() if isinstance(symbol, str) else ""
    entry = next(
        (
            item
            for item in package.day_entries
            if item.market == market_value and item.symbol == code
        ),
        None,
    )
    if entry is None:
        raise VipdataError(
            "ZIP_DAY_ENTRY_NOT_FOUND", "已核验数据包中没有请求的证券日线条目"
        )
    if entry.uncompressed_bytes == 0:
        raise VipdataError(
            "ZIP_DAY_ENTRY_EMPTY", "官方ZIP中的证券日线条目为空，不能作为行情记录读取"
        )
    package_path = _verify_package_file(package, max_total_seconds=max_total_seconds)
    found_target = False
    try:
        with zipfile.ZipFile(package_path, "r") as archive:
            info = archive.getinfo(entry.path)
            if (
                info.file_size != entry.uncompressed_bytes
                or info.file_size > package.file_size * 1000
            ):
                raise VipdataError(
                    "ZIP_CHANGED", "ZIP日线条目元数据与下载校验结果不一致"
                )
            with archive.open(info, "r") as stream:
                scan = iter_day_records(stream, units, limits)
                for located in scan:
                    if located.decoded.session_date == target:
                        found_target = True
                summary = scan.summary
    except VipdataError:
        raise
    except DayReadError as error:
        raise VipdataError(error.code, str(error)) from error
    except (OSError, KeyError, zipfile.BadZipFile, RuntimeError) as error:
        raise VipdataError(
            "ZIP_DAY_SCAN_FAILED", "TDX日线条目未通过现役流式扫描"
        ) from error
    return VipdataDayScan(
        entry=entry,
        record_count=summary.record_count,
        byte_count=summary.byte_count,
        first_date=summary.first_date,
        last_date=summary.last_date,
        raw_byte_sha256=summary.raw_byte_sha256,
        target_session=target,
        target_bar_present=found_target,
        parser_version="stock_a.sources.tdx.day.decode_day_record",
    )


def extract_vipdata_day_entry(
    package: VipdataPackage,
    *,
    market: str,
    symbol: str,
    destination_parent: str | Path = Path("G:/codex_tmp/stock_a_vipdata"),
) -> tuple[Path, Path]:
    market_value = market.upper() if isinstance(market, str) else ""
    code = symbol.strip() if isinstance(symbol, str) else ""
    entry = next(
        (
            item
            for item in package.day_entries
            if item.market == market_value and item.symbol == code
        ),
        None,
    )
    if entry is None:
        raise VipdataError(
            "ZIP_DAY_ENTRY_NOT_FOUND", "已核验数据包中没有请求的证券日线条目"
        )
    if entry.uncompressed_bytes == 0:
        raise VipdataError(
            "ZIP_DAY_ENTRY_EMPTY", "官方ZIP中的证券日线条目为空，不能解压为行情来源"
        )
    package_path = _verify_package_file(package)
    parent = _safe_temp_directory(destination_parent)
    extraction_root = parent / f"extract-{uuid.uuid4().hex}"
    extraction_root.mkdir()
    extraction_root = extraction_root.resolve(strict=True)
    extraction_root.relative_to(parent)
    parts = _safe_entry_path(entry.path)
    normalized_parts = parts if parts[0].casefold() == "vipdoc" else ("vipdoc", *parts)
    output_path = extraction_root.joinpath(*normalized_parts)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.parent.resolve(strict=True).relative_to(extraction_root)
    partial = output_path.with_name(output_path.name + ".partial")
    try:
        with zipfile.ZipFile(package_path, "r") as archive:
            info = archive.getinfo(entry.path)
            if info.file_size != entry.uncompressed_bytes:
                raise VipdataError("ZIP_CHANGED", "ZIP条目大小与下载后校验摘要不一致")
            written = 0
            with archive.open(info, "r") as source, partial.open("xb") as target:
                while True:
                    chunk = source.read(64 * 1024)
                    if not chunk:
                        break
                    written += len(chunk)
                    if written > entry.uncompressed_bytes:
                        raise VipdataError(
                            "ZIP_ENTRY_SIZE_MISMATCH", "日线条目解压后超过中央目录长度"
                        )
                    target.write(chunk)
                target.flush()
                os.fsync(target.fileno())
            if written != entry.uncompressed_bytes or written % 32 != 0:
                raise VipdataError(
                    "ZIP_DAY_LENGTH_INVALID", "日线条目解压后长度与32字节记录不一致"
                )
            os.rename(partial, output_path)
    except VipdataError:
        raise
    except (OSError, KeyError, zipfile.BadZipFile, RuntimeError) as error:
        raise VipdataError(
            "ZIP_EXTRACT_INVALID", "已校验日线条目无法安全读取"
        ) from error
    return extraction_root, output_path


def observation_dict(observation: VipdataPageObservation) -> dict[str, Any]:
    return {
        "source_page_url": observation.source_page_url,
        "page_final_url": observation.page_final_url,
        "page_observed_at": observation.page_observed_at.isoformat(),
        "http_status": observation.http_status,
        "page_declared_update_date": observation.page_declared_update_date.isoformat()
        if observation.page_declared_update_date
        else None,
        "resolved_download_url": observation.resolved_download_url,
        "download_url_source": observation.download_url_source,
        "metadata_url": observation.metadata_url,
        "metadata_observed_at": observation.metadata_observed_at.isoformat()
        if observation.metadata_observed_at
        else None,
        "official_update_at": observation.official_update_at.isoformat()
        if observation.official_update_at
        else None,
        "metadata_sha256": observation.metadata_sha256,
        "page_html_status": observation.page_html_status,
        "page_html_reason": observation.page_html_reason,
        "status": observation.status,
        "reason": observation.reason,
        "evidence": list(observation.evidence),
    }


def package_dict(package: VipdataPackage) -> dict[str, Any]:
    return {
        "file_name": package.file_name,
        "file_path": str(package.file_path),
        "file_size": package.file_size,
        "sha256": package.sha256,
        "zip_entry_count": package.zip_entry_count,
        "zip_crc": package.zip_crc,
        "total_uncompressed_bytes": package.total_uncompressed_bytes,
        "entry_count_by_market": dict(package.entry_count_by_market),
        "download_final_url": package.download_final_url,
        "download_attempt_count": package.download_attempt_count,
        "official_cookie_challenge_solved": package.official_cookie_challenge_solved,
        "request_started_at": package.request_started_at.isoformat(),
        "download_finished_at": package.download_finished_at.isoformat(),
    }
