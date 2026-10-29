"""通达信官网盘后完整包的有界发现、下载和ZIP校验。"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from html.parser import HTMLParser
import hashlib
import io
import math
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import time
from typing import Any, BinaryIO, Callable, Mapping
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
_DAY_ENTRY = re.compile(r"^(?:vipdoc/)?(sh|sz|bj)/lday/(sh|sz|bj)([0-9A-Z]{6})\.day$", re.IGNORECASE)
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
            "max_page_bytes", "max_package_bytes", "max_zip_entries",
            "max_uncompressed_bytes", "max_entry_bytes", "max_compression_ratio",
            "max_redirects", "reserve_free_bytes",
        )
        for name in integer_limits:
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise VipdataError("BUDGET_INVALID", f"{name}必须是正整数")
        for name in ("timeout_seconds", "max_total_seconds"):
            value = getattr(self, name)
            if (isinstance(value, bool) or not isinstance(value, (int, float))
                    or not math.isfinite(float(value)) or value <= 0):
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
                raise VipdataError("SOURCE_CONFIG_INVALID", f"{label}必须是通达信官网HTTPS地址")
            parsed = urlsplit(value)
            if not parsed.path.lower().endswith(expected_suffix) or parsed.query or parsed.fragment:
                raise VipdataError("SOURCE_CONFIG_INVALID", f"{label}与官网端点合同不匹配")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "VipdataSourceConfig":
        if not isinstance(value, Mapping):
            raise VipdataError("SOURCE_CONFIG_INVALID", "source_policy.vipdata_official必须是配置对象")
        required = {"page_url", "package_url", "metadata_url"}
        if set(value) != required:
            raise VipdataError("SOURCE_CONFIG_INVALID", "官网端点配置必须精确包含page_url、package_url、metadata_url")
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
    request = Request(url, headers={
        "User-Agent": (
            "Mozilla/5.0" if challenge_cookie else
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36"
        ),
        "Referer": referer_url,
        "Accept": "text/html,application/javascript,application/zip,application/octet-stream;q=0.9,*/*;q=0.5",
        "Cache-Control": "no-cache, max-age=0",
        "Pragma": "no-cache",
        **({"Cookie": challenge_cookie} if challenge_cookie else {}),
    }, method="GET")
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
        raise VipdataError("DOWNLOAD_URL_UNTRUSTED", "只允许通达信官网域名上的HTTPS资源")
    current = url
    get = transport or _default_transport
    for _redirect_index in range(budgets.max_redirects + 1):
        try:
            response = (
                _default_transport(
                    current, float(budgets.timeout_seconds), challenge_cookie=challenge_cookie,
                    referer_url=referer_url,
                )
                if transport is None else get(current, float(budgets.timeout_seconds))
            )
        except VipdataError:
            raise
        except Exception as error:
            raise VipdataError("SOURCE_HTTP_ERROR", "官网请求失败") from error
        status = getattr(response, "status", None)
        if status is None:
            status = response.getcode()
        final_url = response.geturl() if callable(getattr(response, "geturl", None)) else current
        headers = getattr(response, "headers", {})
        if not _is_official_https_url(final_url):
            response.close()
            raise VipdataError("DOWNLOAD_REDIRECT_UNTRUSTED", "官网请求重定向到非官方域名或非HTTPS地址")
        if int(status) in _REDIRECT_STATUSES:
            location = _header(headers, "Location")
            response.close()
            if not location:
                raise VipdataError("REDIRECT_LOCATION_MISSING", "官网重定向响应缺少Location")
            next_url = urljoin(current, location)
            if not _is_official_https_url(next_url):
                raise VipdataError("DOWNLOAD_REDIRECT_UNTRUSTED", "重定向目标不是通达信官网HTTPS资源")
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
            raise VipdataError("SOURCE_READ_TIMEOUT", "HTTP读取超过socket超时") from error
        except OSError as error:
            raise VipdataError("SOURCE_HTTP_ERROR", "HTTP响应读取失败") from error
        if not chunk:
            break
        if total + len(chunk) > limit:
            raise VipdataError("DOWNLOAD_SIZE_LIMIT", "响应超过显式最大字节数")
        if free_space_path is not None and reserve_free_bytes:
            if shutil.disk_usage(free_space_path).free < reserve_free_bytes:
                raise VipdataError("DISK_SPACE_BUDGET", "G盘可用空间低于保留预算，停止下载")
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
        raise VipdataError("CONTENT_LENGTH_INVALID", "响应Content-Length不是非负整数") from error
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
        window = raw[label.start():min(len(raw), label.end() + 180)]
        for match in _DATE.finditer(window):
            try:
                parsed = date(int(match.group("year")), int(match.group("month")), int(match.group("day")))
            except ValueError:
                continue
            excerpt = re.sub(r"\s+", " ", window[:160]).strip()
            found.setdefault(parsed, excerpt[:180])
    if not found:
        return None, (), "页面没有可独立解析的更新日期"
    if len(found) != 1:
        return None, tuple(sorted(value for value in found.values())), "页面更新日期存在歧义"
    declared, excerpt = next(iter(found.items()))
    return declared, (excerpt,), None


def _page_download_urls(raw: str, attributes: list[str]) -> tuple[str, ...]:
    candidates: set[str] = set()
    fragments = list(attributes)
    fragments.extend(re.findall(r"(?:https?://|/)[^\s\"'<>\\]+?\.zip(?:\?[^\s\"'<>\\]*)?", raw, re.IGNORECASE))
    for fragment in fragments:
        cleaned = fragment.strip().replace("\\/", "/").replace("&amp;", "&").strip("\"'` ")
        for match in re.finditer(r"(?:https?://|/)[^\s\"'<>]*?\.zip(?:\?[^\s\"'<>]*)?", cleaned, re.IGNORECASE):
            candidates.add(match.group(0).replace("&amp;", "&"))
        if re.search(r"\.zip(?:\?|$)", cleaned, re.IGNORECASE):
            candidates.add(cleaned)
    return tuple(sorted(candidates))


def _validate_target_session(
    target_session: str,
    session_checker: Callable[[date], bool] | None,
) -> date:
    if not isinstance(target_session, str) or re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", target_session) is None:
        raise VipdataError("TARGET_SESSION_INVALID", "目标会话必须使用YYYY-MM-DD格式")
    try:
        target = date.fromisoformat(target_session)
    except ValueError as error:
        raise VipdataError("TARGET_SESSION_INVALID", "目标会话不是有效公历日期") from error
    if target.isoformat() != target_session:
        raise VipdataError("TARGET_SESSION_INVALID", "目标会话日期格式不规范")
    if session_checker is None:
        from stock_a.research.entry import _load_calendar

        try:
            _load_calendar(target)
            is_session = True
        except Exception as error:
            raise VipdataError("TARGET_CALENDAR_UNVERIFIED", "项目交易日历无法独立验证目标会话") from error
    else:
        try:
            is_session = session_checker(target)
        except Exception as error:
            raise VipdataError("TARGET_CALENDAR_UNVERIFIED", "目标会话交易日历校验失败") from error
    if is_session is not True:
        raise VipdataError("TARGET_SESSION_NOT_TRADING_DAY", "目标日期不是已核验的正式交易会话")
    return target


def _parse_official_update_time(body: bytes) -> datetime:
    try:
        text = body.decode("ascii")
    except UnicodeDecodeError as error:
        raise VipdataError("SOURCE_METADATA_ENCODING_INVALID", "官方更新元数据不是ASCII JavaScript") from error
    assignments = re.findall(
        r"(?m)^\s*(?:window\.)?HSJDAY_SOFT_TIME\s*=\s*['\"]"
        r"(20[0-9]{2}-[0-9]{2}-[0-9]{2}(?:\s+[0-9]{2}:[0-9]{2}:[0-9]{2})?)['\"]\s*;?\s*$",
        text,
    )
    if len(assignments) != 1:
        raise VipdataError("SOURCE_METADATA_DATE_INVALID", "官方元数据必须唯一声明HSJDAY_SOFT_TIME")
    value = assignments[0]
    formats = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d")
    for format_string in formats:
        try:
            parsed = datetime.strptime(value, format_string)
            return parsed.replace(tzinfo=_CHINA_TIMEZONE)
        except ValueError:
            continue
    raise VipdataError("SOURCE_METADATA_DATE_INVALID", "官方HSJDAY_SOFT_TIME不是有效日期时间")


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
    if not _is_official_https_url(source_page_url) or not _is_official_https_url(final_url):
        raise VipdataError("SOURCE_PAGE_UNTRUSTED", "来源页及其最终响应必须位于通达信官网HTTPS域名")
    if http_status != 200:
        return VipdataPageObservation(source_page_url, final_url, observed_at, http_status, None, None,
                                      "SOURCE_HTTP_ERROR", f"官网页面返回HTTP {http_status}", ())
    parser = _PageParser()
    parser.feed(html)
    raw = "\n".join((*parser.fragments, *parser.attribute_values, html)).replace("\\/", "/")
    declared, evidence, date_error = _parse_calendar_dates(raw)
    raw_urls = _page_download_urls(raw, parser.attribute_values)
    resolved = tuple(dict.fromkeys(urljoin(final_url, value) for value in raw_urls))
    if date_error:
        return VipdataPageObservation(source_page_url, final_url, observed_at, http_status, None, None,
                                      "SOURCE_METADATA_UNAVAILABLE" if not declared else "SOURCE_METADATA_AMBIGUOUS",
                                      date_error, evidence)
    if not resolved:
        return VipdataPageObservation(source_page_url, final_url, observed_at, http_status, declared, None,
                                      "SOURCE_DOWNLOAD_OBJECT_UNAVAILABLE", "页面没有可追溯的ZIP下载对象", evidence)
    if len(resolved) != 1:
        return VipdataPageObservation(source_page_url, final_url, observed_at, http_status, declared, None,
                                      "SOURCE_DOWNLOAD_OBJECT_AMBIGUOUS", "页面发现多个ZIP对象，未自动选择", evidence)
    resolved_url = resolved[0]
    if not _is_official_https_url(resolved_url):
        return VipdataPageObservation(source_page_url, final_url, observed_at, http_status, declared, None,
                                      "DOWNLOAD_URL_UNTRUSTED", "ZIP链接不在通达信官网HTTPS域名", evidence)
    if declared < target:
        return VipdataPageObservation(source_page_url, final_url, observed_at, http_status, declared, resolved_url,
                                      "SOURCE_NOT_READY", "官网声明更新日期早于目标交易会话", evidence)
    return VipdataPageObservation(source_page_url, final_url, observed_at, http_status, declared, resolved_url,
                                  "PACKAGE_CANDIDATE_READY", None, evidence)


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
        source_config.page_url, budgets=budgets, transport=transport,
        referer_url=source_config.page_url,
    )
    page_observed_at = observed_at or datetime.now(timezone.utc)
    try:
        if opened.status != 200:
            return VipdataPageObservation(source_config.page_url, opened.final_url,
                                          page_observed_at, opened.status, None, None,
                                          "SOURCE_HTTP_ERROR", f"官网页面返回HTTP {opened.status}", (),
                                          page_html_status="SOURCE_HTTP_ERROR")
        length = _content_length(opened.headers)
        if length is not None and length > budgets.max_page_bytes:
            raise VipdataError("PAGE_SIZE_LIMIT", "官网页面超过读取预算")
        body, count = _read_limited(opened.response, limit=budgets.max_page_bytes,
                                    total_seconds=budgets.max_total_seconds, started=time.monotonic())
        if length is not None and count != length:
            raise VipdataError("CONTENT_LENGTH_MISMATCH", "官网页面实际字节数与Content-Length不一致")
        content = body or b""
        charset = "utf-8"
        content_type = _header(opened.headers, "Content-Type") or ""
        charset_match = re.search(r"charset\s*=\s*[\"']?([a-zA-Z0-9._-]+)", content_type, re.IGNORECASE)
        if charset_match:
            charset = charset_match.group(1)
        try:
            html = content.decode(charset)
        except (LookupError, UnicodeDecodeError):
            try:
                html = content.decode("gb18030")
            except UnicodeDecodeError as error:
                raise VipdataError("PAGE_ENCODING_INVALID", "官网页面编码无法安全解析") from error
        page_observation = parse_vipdata_page(
            html, source_page_url=source_config.page_url, page_final_url=opened.final_url,
            page_observed_at=page_observed_at, http_status=opened.status,
            target_session=target_session, session_checker=session_checker,
        )
    finally:
        opened.response.close()

    metadata_observed_at = datetime.now(timezone.utc)
    metadata_opened = _open_get(
        source_config.metadata_url, budgets=budgets, transport=transport,
        referer_url=source_config.page_url,
    )
    try:
        if metadata_opened.status != 200:
            return VipdataPageObservation(
                source_config.page_url, page_observation.page_final_url, page_observed_at, 200,
                page_observation.page_declared_update_date, None, "SOURCE_METADATA_HTTP_ERROR",
                f"官方更新元数据返回HTTP {metadata_opened.status}", page_observation.evidence,
                source_config.metadata_url, metadata_observed_at, None, None,
                page_observation.status, page_observation.reason,
            )
        metadata_length = _content_length(metadata_opened.headers)
        metadata_body, metadata_bytes = _read_limited(
            metadata_opened.response,
            limit=min(budgets.max_page_bytes, 256 * 1024),
            total_seconds=budgets.max_total_seconds,
            started=time.monotonic(),
        )
        if metadata_length is not None and metadata_bytes != metadata_length:
            raise VipdataError("CONTENT_LENGTH_MISMATCH", "官方更新元数据实际长度与Content-Length不一致")
        official_update_at = _parse_official_update_time(metadata_body or b"")
    finally:
        metadata_opened.response.close()

    evidence = list(page_observation.evidence)
    evidence.append(f"官方日期来自{source_config.metadata_url}中的HSJDAY_SOFT_TIME")
    evidence.append(f"ZIP对象取自runtime.defaults.json配置：{source_config.package_url}")
    if page_observation.status in {"SOURCE_DOWNLOAD_OBJECT_UNAVAILABLE", "SOURCE_METADATA_UNAVAILABLE"}:
        evidence.append("官网页面HTML未提供完整ZIP链接或更新日期；按登记端点及官方脚本元数据继续核验")
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
            source_config.page_url, page_observation.page_final_url, page_observed_at, 200,
            page_observation.page_declared_update_date, package_url, "SOURCE_NOT_READY",
            "官方HSJDAY_SOFT_TIME早于目标交易会话", tuple(evidence), **common,
        )
    return VipdataPageObservation(
        source_config.page_url, page_observation.page_final_url, page_observed_at, 200,
        page_observation.page_declared_update_date, package_url, "PACKAGE_CANDIDATE_READY",
        None, tuple(evidence), **common,
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
            raise VipdataError("TEMP_ROOT_REPARSE_POINT", "G盘临时根不能是链接或重解析点")
        root.mkdir(parents=True, exist_ok=True)
        original_attributes = getattr(root.lstat(), "st_file_attributes", 0)
        if original_attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400):
            raise VipdataError("TEMP_ROOT_REPARSE_POINT", "G盘临时输入目录不能是链接或重解析点")
        resolved = root.resolve(strict=True)
        resolved.relative_to(allowed_root)
        if not resolved.is_dir():
            raise VipdataError("TEMP_ROOT_INVALID", "临时输入目标不是目录")
        attributes = getattr(resolved.stat(), "st_file_attributes", 0)
        if attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400):
            raise VipdataError("TEMP_ROOT_REPARSE_POINT", "G盘临时输入目录不能是链接或重解析点")
        return resolved
    except VipdataError:
        raise
    except (OSError, ValueError) as error:
        raise VipdataError("TEMP_ROOT_INVALID", "受控临时输入目录必须位于G:/codex_tmp") from error


def _safe_temp_file(path: str | Path) -> Path:
    try:
        original = Path(path)
        original_attributes = getattr(original.lstat(), "st_file_attributes", 0)
        if original_attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400):
            raise VipdataError("TEMP_FILE_REPARSE_POINT", "临时输入文件不能是链接或重解析点")
        resolved = original.resolve(strict=True)
        allowed_root = _G_TEMP_ROOT.resolve(strict=True)
        if allowed_root.drive.upper() != "G:" or resolved.drive.upper() != "G:":
            raise VipdataError("TEMP_FILE_INVALID", "临时输入文件必须位于G盘")
        resolved.relative_to(allowed_root)
        if original.is_symlink() or not resolved.is_file():
            raise VipdataError("TEMP_FILE_INVALID", "临时输入必须是G盘中的普通文件")
        attributes = getattr(resolved.stat(), "st_file_attributes", 0)
        if attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400):
            raise VipdataError("TEMP_FILE_REPARSE_POINT", "临时输入文件不能是链接或重解析点")
        return resolved
    except VipdataError:
        raise
    except (OSError, ValueError) as error:
        raise VipdataError("TEMP_FILE_INVALID", "临时输入文件不在G:/codex_tmp受控根内") from error


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
            raise VipdataError("PACKAGE_SIZE_INVALID", "ZIP文件为空或超过最大下载字节数")
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
                parts = _safe_entry_path(info.filename.rstrip("/")) if info.filename.rstrip("/") else ()
                if not parts:
                    raise VipdataError("ZIP_PATH_INVALID", "ZIP包含根目录条目")
                key = "/".join(parts).casefold()
                if key in seen:
                    raise VipdataError("ZIP_DUPLICATE_PATH", "ZIP包含规范化后重复的条目路径")
                seen.add(key)
                mode = (info.external_attr >> 16) & 0xFFFF
                if stat.S_ISLNK(mode):
                    raise VipdataError("ZIP_LINK_FORBIDDEN", "ZIP包含符号链接条目")
                if info.is_dir():
                    continue
                if mode and not (stat.S_ISREG(mode) or mode & 0o170000 == 0):
                    raise VipdataError("ZIP_FILE_TYPE_INVALID", "ZIP条目不是普通文件")
                if info.file_size < 0 or info.compress_size < 0 or info.file_size > budgets.max_entry_bytes:
                    raise VipdataError("ZIP_ENTRY_SIZE_LIMIT", "ZIP单条目解压大小非法或超预算")
                total_uncompressed += info.file_size
                if total_uncompressed > budgets.max_uncompressed_bytes:
                    raise VipdataError("ZIP_TOTAL_SIZE_LIMIT", "ZIP总解压大小超过预算")
                if info.file_size and (info.compress_size == 0 or info.file_size > info.compress_size * budgets.max_compression_ratio):
                    raise VipdataError("ZIP_COMPRESSION_RATIO_LIMIT", "ZIP条目压缩比超过预算")
                match = _DAY_ENTRY.fullmatch("/".join(parts))
                if not match:
                    raise VipdataError("ZIP_ENTRY_FAMILY_UNEXPECTED", f"VIPDATA ZIP包含未登记的文件族：{info.filename}")
                folder_market, file_market, symbol = match.groups()
                market = folder_market.lower()
                if file_market.lower() != market:
                    raise VipdataError("ZIP_MARKET_PATH_MISMATCH", "ZIP日线文件市场与目录不一致")
                if info.file_size % 32 != 0:
                    raise VipdataError("ZIP_DAY_LENGTH_INVALID", f"TDX .day文件必须为空或由32字节记录组成：{info.filename}")
                counts[market] += 1
                entries.append(VipdataDayEntry("/".join(parts), market.upper(), symbol.upper(),
                                               info.compress_size, info.file_size))
            if not entries:
                raise VipdataError("ZIP_NO_DAY_ENTRIES", "ZIP没有可用的TDX日线文件")
            bad_entry = archive.testzip()
            if bad_entry is not None:
                raise VipdataError("ZIP_CRC_INVALID", f"ZIP条目CRC校验失败：{bad_entry}")
            return len(infos), total_uncompressed, tuple(entries), counts
    except VipdataError:
        raise
    except (OSError, zipfile.BadZipFile, RuntimeError, NotImplementedError) as error:
        raise VipdataError("ZIP_INVALID", "ZIP中央目录、压缩方法或内容校验失败") from error


def verify_existing_vipdata_package(
    zip_path: str | Path,
    *,
    expected_sha256: str,
    budgets: VipdataBudgets = VipdataBudgets(),
) -> VerifiedVipdataPackage:
    """重验G盘现存包的字节身份、完整目录与CRC，不联网也不改包。"""
    if not isinstance(expected_sha256, str) or re.fullmatch(r"[0-9a-fA-F]{64}", expected_sha256) is None:
        raise VipdataError("PACKAGE_SHA_INVALID", "显式VIPDATA包必须提供64位十六进制SHA-256")
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
                    raise VipdataError("PACKAGE_VERIFY_TIMEOUT", "本地ZIP SHA校验超过总耗时预算")
                chunk = source.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
        actual_sha256 = digest.hexdigest()
        if actual_sha256 != expected_sha256.lower():
            raise VipdataError("PACKAGE_SHA_MISMATCH", "本地ZIP SHA-256与显式绑定值不一致")
        entry_count, total_uncompressed, entries, by_market = validate_vipdata_zip(path, budgets=budgets)
        if time.monotonic() - started > budgets.max_total_seconds:
            raise VipdataError("PACKAGE_VERIFY_TIMEOUT", "本地ZIP SHA及CRC校验超过总耗时预算")
        after = path.stat()
        identity_before = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        identity_after = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        if identity_before != identity_after:
            raise VipdataError("PACKAGE_CHANGED", "完整校验期间ZIP文件身份或大小发生变化")
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


def vipdata_research_candidate_pool(package: VerifiedVipdataPackage) -> VipdataCandidatePool:
    """动态按现役符号合同分出沪深主板/GEM候选，BJ和非目标代码保留分账。"""
    from stock_a.research.entry import ResearchEntryError, _normalize_symbol

    if not isinstance(package, VerifiedVipdataPackage):
        raise VipdataError("PACKAGE_HANDLE_INVALID", "候选池必须来自本次本地完整校验的VIPDATA包")
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
                    raise VipdataError("PACKAGE_SYMBOL_INVALID", "VIPDATA证券代码无法按现役规则核验") from error
                excluded_out_of_scope += 1
                if market == "SH" and code.startswith(("688", "689")):
                    category = "SH_STAR"
                elif market == "SH" and code.startswith("900"):
                    category = "SH_B_SHARE"
                elif market == "SZ" and code.startswith("200"):
                    category = "SZ_B_SHARE"
                elif (market == "SZ" and code.startswith("399")) or code.startswith("000") and market == "SH":
                    category = "INDEX_OR_OTHER"
                else:
                    category = "SH_SZ_OTHER"
            else:
                category = f"{market}_{board}"
                if normalized in seen:
                    raise VipdataError("PACKAGE_SYMBOL_DUPLICATE", "ZIP同一市场代码出现多个日线条目")
                seen[normalized] = entry.path
                candidates.append({
                    "symbol": normalized,
                    "market": market,
                    "board": board,
                    "zip_entry_path": entry.path,
                    "zip_entry_bytes": entry.uncompressed_bytes,
                    "empty_placeholder": entry.uncompressed_bytes == 0,
                })
        classifications[category] = classifications.get(category, 0) + 1
    if sum(classifications.values()) != len(package.day_entries):
        raise VipdataError("PACKAGE_CLASSIFICATION_INCOMPLETE", "VIPDATA目录条目未能互斥完整分区")
    candidates.sort(key=lambda item: item["symbol"])
    by_market = {market: sum(item["market"] == market for item in candidates) for market in ("SH", "SZ")}
    by_board = {board: sum(item["board"] == board for item in candidates) for board in ("MAIN_BOARD", "GEM")}
    catalog = {
        "contract": "TDX_SOURCE_CANDIDATES_NON_PIT",
        "complete": True,
        "configured_root_count": 1,
        "directories": [
            {"directory": f"{market.lower()}/lday", "directory_entries": package.entry_count_by_market.get(market.lower(), 0)}
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
        "empty_placeholder_count": sum(entry.uncompressed_bytes == 0 for entry in package.day_entries),
        "empty_placeholder_paths": sorted(
            entry.path for entry in package.day_entries if entry.uncompressed_bytes == 0
        )[:100],
        "official_universe_denominator": "UNKNOWN",
        "identity_disclosure": "ZIP文件名仅形成来源代码候选；不证明稳定实体、正式成员、上市、当日可交易或ST状态",
    }
    return VipdataCandidatePool(tuple(candidates), catalog, dict(sorted(classifications.items())))


def extract_vipdata_candidate_page(
    package: VerifiedVipdataPackage,
    candidates: tuple[dict[str, Any], ...],
    *,
    destination_parent: str | Path = Path("G:/codex_tmp/stock_a_vipdata"),
    max_extract_bytes: int = 512 * 1024 * 1024,
) -> tuple[Path, dict[str, Path]]:
    """一次ZIP身份校验后，仅解压本页条目；不为每只证券重算全包SHA。"""
    if not isinstance(package, VerifiedVipdataPackage):
        raise VipdataError("PACKAGE_HANDLE_INVALID", "页面抽取必须使用本次完整校验包句柄")
    if type(max_extract_bytes) is not int or max_extract_bytes <= 0:
        raise VipdataError("PACKAGE_EXTRACT_BUDGET_INVALID", "页面解压预算必须为正整数")
    total_bytes = sum(item.get("zip_entry_bytes", -1) for item in candidates)
    if any(type(item.get("zip_entry_bytes")) is not int or item["zip_entry_bytes"] < 0 for item in candidates):
        raise VipdataError("PACKAGE_ENTRY_INVALID", "页面含未绑定目录身份的ZIP条目")
    if total_bytes > max_extract_bytes:
        raise VipdataError("PACKAGE_EXTRACT_BUDGET_EXCEEDED", "当前分页解压字节数超过显式页面预算")
    path = _safe_temp_file(package.file_path)
    if _file_identity(path) != package.file_identity:
        raise VipdataError("PACKAGE_CHANGED", "SHA/CRC校验后VIPDATA文件身份发生变化")
    parent = _safe_temp_directory(destination_parent)
    if shutil.disk_usage(parent).free < total_bytes + 64 * 1024 * 1024:
        raise VipdataError("PACKAGE_EXTRACT_DISK_BUDGET", "G盘空间不足以临时抽取当前页并保留64MiB")
    extraction_root = parent / f"select-{uuid.uuid4().hex}"
    extraction_root.mkdir()
    extracted: dict[str, Path] = {}
    entries_by_path = {entry.path: entry for entry in package.day_entries}
    try:
        with zipfile.ZipFile(path, "r") as archive:
            for candidate in candidates:
                entry_path = candidate.get("zip_entry_path")
                entry = entries_by_path.get(entry_path) if isinstance(entry_path, str) else None
                symbol = candidate.get("symbol")
                if entry is None or not isinstance(symbol, str):
                    raise VipdataError("PACKAGE_ENTRY_INVALID", "分页候选与已校验ZIP目录不匹配")
                normalized, market_directory = _candidate_entry_destination(symbol)
                if normalized != symbol or candidate.get("market") != symbol.split(".", 1)[0]:
                    raise VipdataError("PACKAGE_ENTRY_INVALID", "分页候选市场、代码与条目身份不匹配")
                if candidate.get("zip_entry_bytes") != entry.uncompressed_bytes:
                    raise VipdataError("PACKAGE_ENTRY_INVALID", "分页候选条目大小与已校验目录不匹配")
                relative = PurePosixPath("vipdoc") / market_directory / "lday" / f"{market_directory}{symbol.split('.', 1)[1]}.day"
                parts = _safe_entry_path(str(relative))
                output_path = extraction_root.joinpath(*parts)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.parent.resolve(strict=True).relative_to(extraction_root)
                info = archive.getinfo(entry.path)
                if info.file_size != entry.uncompressed_bytes:
                    raise VipdataError("PACKAGE_CHANGED", "ZIP条目大小与本次重验目录不一致")
                with archive.open(info, "r") as source, output_path.open("xb") as target:
                    written = 0
                    while True:
                        chunk = source.read(64 * 1024)
                        if not chunk:
                            break
                        written += len(chunk)
                        if written > entry.uncompressed_bytes:
                            raise VipdataError("ZIP_ENTRY_SIZE_MISMATCH", "分页条目解压长度超过中央目录")
                        target.write(chunk)
                    target.flush()
                    os.fsync(target.fileno())
                if written != entry.uncompressed_bytes or (written and written % 32 != 0):
                    raise VipdataError("ZIP_DAY_LENGTH_INVALID", "分页日线条目长度不符合32字节记录合同")
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
    if resolved.parent != parent or not resolved.name.startswith("select-") or root.is_symlink():
        raise VipdataError("EXTRACTION_CLEANUP_REFUSED", "拒绝清理非本次创建的VIPDATA临时目录")
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
    if observation.status != "PACKAGE_CANDIDATE_READY" or not observation.resolved_download_url:
        raise VipdataError("SOURCE_NOT_READY", "官方HSJDAY_SOFT_TIME或已配置ZIP对象尚未通过候选资格检查")
    if not _is_official_https_url(observation.resolved_download_url):
        raise VipdataError("DOWNLOAD_URL_UNTRUSTED", "候选下载对象不是通达信官网HTTPS资源")
    if destination_root is None:
        destination_root = Path("G:/codex_tmp/stock_a_vipdata") / f"run-{uuid.uuid4().hex}"
    root = _safe_temp_directory(destination_root)
    request_started_at = datetime.now(timezone.utc)
    opened = _open_get(
        observation.resolved_download_url, budgets=budgets, transport=transport,
        referer_url=observation.source_page_url,
    )
    download_attempt_count = 1
    challenge_cookie_used = False
    first_content_type = (_header(opened.headers, "Content-Type") or "").lower()
    if opened.status == 200 and "text/html" in first_content_type:
        try:
            challenge_body, _challenge_size = _read_limited(
                opened.response, limit=64 * 1024, total_seconds=budgets.max_total_seconds,
                started=time.monotonic(),
            )
        finally:
            opened.response.close()
        challenge_cookie = _official_download_challenge_cookie(challenge_body or b"")
        if challenge_cookie is None:
            raise VipdataError("DOWNLOAD_RESPONSE_HTML", "ZIP地址返回非预期HTML，未识别为通达信官方静态挑战")
        opened = _open_get(
            observation.resolved_download_url, budgets=budgets, transport=transport,
            challenge_cookie=challenge_cookie, referer_url=observation.source_page_url,
        )
        download_attempt_count = 2
        challenge_cookie_used = True
    monotonic_started = time.monotonic()
    partial = root / f"vipdata-{uuid.uuid4().hex}.zip.partial"
    try:
        if opened.status != 200:
            raise VipdataError("DOWNLOAD_HTTP_ERROR", f"官网数据包返回HTTP {opened.status}")
        content_type = (_header(opened.headers, "Content-Type") or "").lower()
        if "text/html" in content_type:
            raise VipdataError("DOWNLOAD_RESPONSE_HTML", "ZIP地址返回HTML响应，拒绝将其当作数据包下载")
        content_length = _content_length(opened.headers)
        if content_length is not None and content_length > budgets.max_package_bytes:
            raise VipdataError("DOWNLOAD_SIZE_LIMIT", "官网数据包Content-Length超过预算")
        free = shutil.disk_usage(root).free
        required = (content_length or 0) + budgets.reserve_free_bytes
        if content_length is None:
            required = budgets.reserve_free_bytes
        if free < required:
            raise VipdataError("DISK_SPACE_BUDGET", "G盘可用空间不足以保留数据包及安全余量")
        digest = hashlib.sha256()
        with partial.open("xb") as output:
            _body, written = _read_limited(
                opened.response, limit=budgets.max_package_bytes,
                total_seconds=budgets.max_total_seconds, started=monotonic_started,
                sink=output, digest=digest, free_space_path=root,
                reserve_free_bytes=budgets.reserve_free_bytes,
            )
            output.flush()
            os.fsync(output.fileno())
        if content_length is not None and written != content_length:
            raise VipdataError("CONTENT_LENGTH_MISMATCH", "数据包实际字节数与Content-Length不一致")
        sha256 = digest.hexdigest()
        final = root / f"vipdata-{sha256}.zip"
        if final.exists():
            raise VipdataError("PACKAGE_ALREADY_EXISTS", "同SHA数据包已存在；拒绝覆盖或复用旧临时文件")
        os.rename(partial, final)
        try:
            zip_count, expanded, day_entries, by_market = validate_vipdata_zip(final, budgets=budgets)
        except VipdataError as error:
            rejected = root / f"rejected-vipdata-{sha256}.zip"
            if rejected.exists():
                raise VipdataError("PACKAGE_ALREADY_EXISTS", "同SHA拒绝包已存在；拒绝覆盖") from error
            os.rename(final, rejected)
            raise VipdataError(
                error.code, f"{error}；完整原始ZIP保留于G盘临时目录，SHA-256={sha256}，文件={rejected}",
            ) from error
        return VipdataPackage(
            file_path=final, file_name=final.name, file_size=written, sha256=sha256,
            zip_entry_count=zip_count, zip_crc="PASS", total_uncompressed_bytes=expanded,
            entry_count_by_market=by_market, day_entries=day_entries,
            download_final_url=opened.final_url, request_started_at=request_started_at,
            download_finished_at=datetime.now(timezone.utc),
            download_attempt_count=download_attempt_count,
            official_cookie_challenge_solved=challenge_cookie_used,
        )
    finally:
        opened.response.close()
        if partial.exists():
            partial.unlink()


def _verify_package_file(package: VipdataPackage, *, max_total_seconds: float = 300.0) -> Path:
    package_path = _safe_temp_file(package.file_path)
    if package_path.stat().st_size != package.file_size:
        raise VipdataError("PACKAGE_CHANGED", "下载后ZIP文件大小发生变化，拒绝读取")
    digest = hashlib.sha256()
    started = time.monotonic()
    with package_path.open("rb") as source:
        while True:
            if time.monotonic() - started > max_total_seconds:
                raise VipdataError("PACKAGE_VERIFY_TIMEOUT", "ZIP SHA复核超过总耗时预算")
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
    entry = next((item for item in package.day_entries
                  if item.market == market_value and item.symbol == code), None)
    if entry is None:
        raise VipdataError("ZIP_DAY_ENTRY_NOT_FOUND", "已核验数据包中没有请求的证券日线条目")
    if entry.uncompressed_bytes == 0:
        raise VipdataError("ZIP_DAY_ENTRY_EMPTY", "官方ZIP中的证券日线条目为空，不能作为行情记录读取")
    package_path = _verify_package_file(package, max_total_seconds=max_total_seconds)
    found_target = False
    try:
        with zipfile.ZipFile(package_path, "r") as archive:
            info = archive.getinfo(entry.path)
            if info.file_size != entry.uncompressed_bytes or info.file_size > package.file_size * 1000:
                raise VipdataError("ZIP_CHANGED", "ZIP日线条目元数据与下载校验结果不一致")
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
        raise VipdataError("ZIP_DAY_SCAN_FAILED", "TDX日线条目未通过现役流式扫描") from error
    return VipdataDayScan(
        entry=entry, record_count=summary.record_count, byte_count=summary.byte_count,
        first_date=summary.first_date, last_date=summary.last_date,
        raw_byte_sha256=summary.raw_byte_sha256, target_session=target,
        target_bar_present=found_target, parser_version="stock_a.sources.tdx.day.decode_day_record",
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
    entry = next((item for item in package.day_entries
                  if item.market == market_value and item.symbol == code), None)
    if entry is None:
        raise VipdataError("ZIP_DAY_ENTRY_NOT_FOUND", "已核验数据包中没有请求的证券日线条目")
    if entry.uncompressed_bytes == 0:
        raise VipdataError("ZIP_DAY_ENTRY_EMPTY", "官方ZIP中的证券日线条目为空，不能解压为行情来源")
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
                        raise VipdataError("ZIP_ENTRY_SIZE_MISMATCH", "日线条目解压后超过中央目录长度")
                    target.write(chunk)
                target.flush()
                os.fsync(target.fileno())
            if written != entry.uncompressed_bytes or written % 32 != 0:
                raise VipdataError("ZIP_DAY_LENGTH_INVALID", "日线条目解压后长度与32字节记录不一致")
            os.rename(partial, output_path)
    except VipdataError:
        raise
    except (OSError, KeyError, zipfile.BadZipFile, RuntimeError) as error:
        raise VipdataError("ZIP_EXTRACT_INVALID", "已校验日线条目无法安全读取") from error
    return extraction_root, output_path


def observation_dict(observation: VipdataPageObservation) -> dict[str, Any]:
    return {
        "source_page_url": observation.source_page_url,
        "page_final_url": observation.page_final_url,
        "page_observed_at": observation.page_observed_at.isoformat(),
        "http_status": observation.http_status,
        "page_declared_update_date": observation.page_declared_update_date.isoformat()
        if observation.page_declared_update_date else None,
        "resolved_download_url": observation.resolved_download_url,
        "download_url_source": observation.download_url_source,
        "metadata_url": observation.metadata_url,
        "metadata_observed_at": observation.metadata_observed_at.isoformat()
        if observation.metadata_observed_at else None,
        "official_update_at": observation.official_update_at.isoformat()
        if observation.official_update_at else None,
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
