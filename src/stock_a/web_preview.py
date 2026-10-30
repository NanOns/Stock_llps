"""本机只读PG来源级研究预览HTTP服务。"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
import json
import re
import socket
import threading
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import parse_qs, urlsplit

from stock_a import diagnostics
from stock_a.contracts import DayChainContractError, load_day_chain_contract
from stock_a.postgres_source_raw import (
    RawSourcePgError,
    read_raw_daily_catalog,
    read_raw_daily_page,
    read_raw_daily_stock,
    read_raw_daily_history_stock,
)
from stock_a.research import core as research_core
from stock_a.research import entry as research_entry


_HOST = "127.0.0.1"
_PORT = 28766
_MAX_BODY_BYTES = 8192
_MAX_PAGE_SIZE = 50
_MAX_OFFSET = 2**31 - 1
_SYMBOL = re.compile(r"^(SH|SZ)\.[0-9]{6}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_DATE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
_PROFILE_CONTRACT_NAME = "STOCK_A_DAY_CHAIN_R10"


class PreviewHttpError(Exception):
    def __init__(self, status: int, code: str, message: str):
        self.status = status
        self.code = code
        self.message = message
        super().__init__(message)


def _json_value(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        if isinstance(value, float) and (value != value or value in (float("inf"), float("-inf"))):
            raise ValueError("JSON拒绝NaN和Infinity")
        return value
    raise TypeError(f"不支持的预览响应类型：{type(value).__name__}")


def _canonical_sha(value: Any) -> str:
    encoded = json.dumps(
        _json_value(value), ensure_ascii=False, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _iso_date(value: Any, field: str = "target_session") -> date:
    if not isinstance(value, str) or _DATE.fullmatch(value) is None:
        raise PreviewHttpError(400, "PREVIEW_DATE_INVALID", f"{field}必须使用YYYY-MM-DD格式")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise PreviewHttpError(400, "PREVIEW_DATE_INVALID", f"{field}不是有效日期") from error
    if parsed.isoformat() != value:
        raise PreviewHttpError(400, "PREVIEW_DATE_INVALID", f"{field}不是标准ISO日期")
    return parsed


def _sha(value: Any) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise PreviewHttpError(400, "PREVIEW_SHA_INVALID", "来源包SHA必须是64位小写十六进制值")
    return value


def _symbol(value: Any) -> str:
    if not isinstance(value, str) or _SYMBOL.fullmatch(value) is None:
        raise PreviewHttpError(400, "PREVIEW_SYMBOL_INVALID", "来源代码必须符合SH.六位数字或SZ.六位数字")
    return value


def _positive_int(value: Any, field: str, maximum: int) -> int:
    if type(value) is not int or not 1 <= value <= maximum:
        raise PreviewHttpError(400, "PREVIEW_PARAMETER_INVALID", f"{field}必须为1至{maximum}的整数")
    return value


def _nonnegative_int(value: str | None, field: str, maximum: int = _MAX_OFFSET) -> int:
    if value is None or not value.isdecimal():
        raise PreviewHttpError(400, "PREVIEW_PARAMETER_INVALID", f"{field}必须为非负整数")
    parsed = int(value)
    if parsed > maximum:
        raise PreviewHttpError(400, "PREVIEW_PARAMETER_INVALID", f"{field}超过允许上限")
    return parsed


def _query(path: str, allowed: set[str]) -> dict[str, str]:
    parsed = urlsplit(path)
    try:
        values = parse_qs(parsed.query, keep_blank_values=True, strict_parsing=True)
    except ValueError as error:
        raise PreviewHttpError(400, "PREVIEW_QUERY_INVALID", "查询字符串格式无效") from error
    if set(values) - allowed or any(len(items) != 1 for items in values.values()):
        raise PreviewHttpError(400, "PREVIEW_QUERY_INVALID", "查询参数重复或包含未登记字段")
    return {name: items[0] for name, items in values.items()}


def _raw_public_fields(row: Mapping[str, Any]) -> dict[str, Any]:
    raw_names = (
        "raw_open", "raw_high", "raw_low", "raw_close", "raw_amount", "raw_volume",
    )
    quality_names = (
        "bar_status", "bar_reason", "price_quality", "price_reason",
        "raw_amount_quality", "raw_amount_reason", "amount_quality", "amount_reason",
        "amount_unit_status", "raw_volume_quality", "raw_volume_reason", "volume_quality",
        "volume_reason", "volume_unit_status",
    )
    return {
        "session_date": row["session_date"].isoformat(),
        "source_symbol": row["source_symbol"],
        "market": row["market"],
        "board": row["board"],
        "bar_status": row["bar_status"],
        "bar_reason": row["bar_reason"],
        "values": {name.removeprefix("raw_"): _json_value(row.get(name)) for name in raw_names},
        "quality": {name: _json_value(row.get(name)) for name in quality_names},
        "source": {
            "dataset_id": row.get("dataset_id"),
            "package_sha256": row.get("package_sha256"),
            "source_entry_sha256": row.get("source_entry_sha256"),
            "source_record_count": row.get("source_record_count"),
            "first_source_date": _json_value(row.get("first_source_date")),
            "last_source_date": _json_value(row.get("last_source_date")),
            "source_clock_role": row.get("capture_clock_role"),
            "source_time_quality": row.get("capture_time_quality"),
            "observed_started_at": _json_value(row.get("capture_read_started_at")),
            "observed_validated_at": _json_value(row.get("capture_validated_at")),
            "source_available_at": _json_value(row.get("source_available_at")),
        },
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "price_coordinate": "RAW_UNADJUSTED",
    }


def _page_public_fields(page: Mapping[str, Any]) -> dict[str, Any]:
    candidates = [
        {
            "source_symbol": row["source_symbol"],
            "market": row["market"],
            "board": row["board"],
            "bar_status": row["bar_status"],
            "price_quality": row.get("price_quality"),
            "price_reason": row.get("price_reason"),
            "identity_status": "SOURCE_SYMBOL_ONLY",
        }
        for row in page.get("rows", ())
    ]
    actual = page.get("version_row_count")
    expected = page.get("version_expected_row_count")
    return {
        "target_session": page["target_session"],
        "package_sha256": page["package_sha256"],
        "offset": page["offset"],
        "limit": page["limit"],
        "page_count": page["page_count"],
        "filtered_total": page["total_count"],
        "next_offset": page["next_offset"],
        "market_filter": page["market_filter"],
        "board_filter": page["board_filter"],
        "bar_status_counts": page["bar_status_counts"],
        "source_version": {
            "status": page["version_status"],
            "stored_raw_rows": actual,
            "capture_expected_rows": expected,
            "all_stored_rows_classified": page["partition_closed"],
        },
        "official_listing_population": {
            "status": "UNKNOWN",
            "reason": "NOT_CONNECTED：来源候选不是官方上市总体",
        },
        "results": candidates,
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "source_zip_opened": False,
        "source_recalculated": False,
        "ddl_executed": False,
        "database_write": False,
    }


class _PreviewState:
    def __init__(self) -> None:
        self.config, self.config_path = diagnostics._load_runtime_config()
        services = self.config.get("services")
        development = services.get("development") if isinstance(services, Mapping) else None
        if (
            not isinstance(development, Mapping)
            or development.get("host") != _HOST
            or type(development.get("port")) is not int
            or development.get("port") != _PORT
        ):
            raise diagnostics.DiagnosticError(
                "CONFIG_INVALID", "本机研究预览要求配置development服务为127.0.0.1:28766"
            )
        self.profile_contract = load_day_chain_contract(diagnostics._CONFIG_ROOT).document
        self.profile_contract_sha256 = _canonical_sha(self.profile_contract)
        self.core_source_sha256 = hashlib.sha256(Path(research_core.__file__).read_bytes()).hexdigest()
        self.entry_source_sha256 = hashlib.sha256(Path(research_entry.__file__).read_bytes()).hexdigest()

    def snapshot_identity(
        self, *, namespace_id: str, target_session: str, package_sha256: str,
        session_count: int | None = None, session_start: str | None = None,
        calendar_revision: str | None = None,
    ) -> dict[str, Any]:
        fields: dict[str, Any] = {
            "identity_type": "PREVIEW_SNAPSHOT_IDENTITY_NON_PIT",
            "application_id": "STOCK_A",
            "namespace_id": namespace_id,
            "target_session": target_session,
            "package_sha256": package_sha256,
            "preview_mode": "SOURCE_SYMBOL_NON_PIT_PREVIEW",
            "price_coordinate": "RAW_UNADJUSTED",
            "core_source_sha256": self.core_source_sha256,
            "research_entry_source_sha256": self.entry_source_sha256,
            "profile_contract": _PROFILE_CONTRACT_NAME,
            "profile_contract_sha256": self.profile_contract_sha256,
        }
        if session_count is not None:
            fields["target_window"] = {
                "session_count": session_count,
                "session_start": session_start,
                "session_end": target_session,
                "calendar_revision": calendar_revision,
            }
        return {**fields, "identity_sha256": _canonical_sha(fields)}


def _duplicate_free_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("JSON对象含重复字段")
        result[key] = value
    return result


def _reject_constant(_value: str) -> None:
    raise ValueError("JSON拒绝NaN和Infinity")


class _BoundedHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = False
    daemon_threads = True
    block_on_close = True

    def __init__(self, server_address: tuple[str, int], state: _PreviewState):
        self.state = state
        self._request_slots = threading.BoundedSemaphore(4)
        super().__init__(server_address, _PreviewHandler)

    def process_request(self, request: socket.socket, client_address: Any) -> None:
        if not self._request_slots.acquire(blocking=False):
            body = json.dumps(
                {"ok": False, "code": "PREVIEW_CONCURRENCY_LIMIT", "message": "预览请求并发已达4个上限，请稍后重试"},
                ensure_ascii=False, separators=(",", ":"),
            ).encode("utf-8")
            response = (
                b"HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json; charset=utf-8\r\n"
                + f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode("ascii") + body
            )
            try:
                request.sendall(response)
            except OSError:
                pass
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self._request_slots.release()
            raise

    def process_request_thread(self, request: socket.socket, client_address: Any) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._request_slots.release()


class _PreviewHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "StockA-Preview"
    sys_version = ""

    @property
    def preview_server(self) -> _BoundedHTTPServer:
        return self.server  # type: ignore[return-value]

    def setup(self) -> None:
        self.request.settimeout(8.0)
        super().setup()

    def log_message(self, _format: str, *args: Any) -> None:
        return

    def _host_allowed(self) -> bool:
        return self.headers.get("Host", "").lower() == f"{_HOST}:{_PORT}"

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
        if content_type.startswith("text/html"):
            self.send_header(
                "Content-Security-Policy",
                "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; "
                "img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'",
            )
        self.end_headers()
        if self.command != "HEAD":
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass

    def _send_json(self, status: int, payload: Mapping[str, Any]) -> None:
        body = json.dumps(
            _json_value(payload), ensure_ascii=False, allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
        self._send(status, body, "application/json; charset=utf-8")

    def _error(self, error: PreviewHttpError) -> None:
        self._send_json(error.status, {"ok": False, "code": error.code, "message": error.message})

    def _ensure_local_host(self) -> bool:
        if self._host_allowed():
            return True
        self._error(PreviewHttpError(421, "PREVIEW_HOST_REJECTED", "请求Host不属于127.0.0.1:28766"))
        return False

    def _handle_error(self, error: Exception) -> None:
        if isinstance(error, PreviewHttpError):
            self._error(error)
            return
        if isinstance(error, RawSourcePgError):
            invalid = error.code.endswith("INVALID") or error.code in {
                "PG_QUERY_KEY_INVALID", "PG_PAGE_INVALID", "PG_FILTER_INVALID",
                "PG_CATALOG_PAGE_INVALID", "PG_CATALOG_YEAR_INVALID",
                "SOURCE_SYMBOL_INVALID", "HISTORY_SESSION_WINDOW_INVALID",
            }
            if error.code == "PG_SOURCE_NO_MATCH":
                status = 404
            elif error.code == "PG_HISTORY_SESSION_MISSING":
                status = 422
            elif invalid:
                status = 400
            else:
                status = 503
            self._error(PreviewHttpError(status, error.code, str(error)))
            return
        if isinstance(error, diagnostics.DiagnosticError):
            status = 400 if error.code.endswith("INVALID") else 503
            self._error(PreviewHttpError(status, error.code, str(error)))
            return
        if isinstance(error, (DayChainContractError, research_entry.ResearchEntryError)):
            code = getattr(error, "code", "PREVIEW_CONTRACT_UNAVAILABLE")
            status = 400 if code.endswith("INVALID") or code == "TARGET_NOT_TRADING_SESSION" else 503
            self._error(PreviewHttpError(status, code, str(error)))
            return
        self._error(PreviewHttpError(500, "PREVIEW_INTERNAL_ERROR", "预览请求未完成；请检查本机服务输出"))

    def do_HEAD(self) -> None:
        self.do_GET()

    def do_OPTIONS(self) -> None:
        self._send_json(405, {"ok": False, "code": "PREVIEW_METHOD_NOT_ALLOWED", "message": "此路径不接受OPTIONS请求"})

    def do_GET(self) -> None:
        if not self._ensure_local_host():
            return
        try:
            parsed = urlsplit(self.path)
            if parsed.path == "/":
                if parsed.query:
                    raise PreviewHttpError(400, "PREVIEW_QUERY_INVALID", "首页不接受查询参数")
                self._send(200, _INDEX_HTML.encode("utf-8"), "text/html; charset=utf-8")
                return
            if parsed.path == "/app.css":
                if parsed.query:
                    raise PreviewHttpError(400, "PREVIEW_QUERY_INVALID", "静态资源不接受查询参数")
                self._send(200, _APP_CSS.encode("utf-8"), "text/css; charset=utf-8")
                return
            if parsed.path == "/app.js":
                if parsed.query:
                    raise PreviewHttpError(400, "PREVIEW_QUERY_INVALID", "静态资源不接受查询参数")
                self._send(200, _APP_JS.encode("utf-8"), "text/javascript; charset=utf-8")
                return
            if parsed.path == "/api/preview/catalog":
                values = _query(self.path, {"offset", "limit", "year"})
                offset = _nonnegative_int(values.get("offset", "0"), "offset")
                limit = _positive_int(_nonnegative_int(values.get("limit", "100"), "limit", 100) or 0, "limit", 100)
                year = None
                if "year" in values:
                    year = _nonnegative_int(values["year"], "year", 9998)
                    if year < 1900:
                        raise PreviewHttpError(400, "PREVIEW_PARAMETER_INVALID", "year必须在1900至9998之间")
                catalog = read_raw_daily_catalog(self.preview_server.state.config, year=year, offset=offset, limit=limit)
                self._send_json(200, {
                    "ok": True,
                    "namespace_id": catalog["namespace_id"],
                    "offset": catalog["offset"],
                    "limit": catalog["limit"],
                    "page_count": catalog["page_count"],
                    "total_version_count": catalog["total_version_count"],
                    "next_offset": catalog["next_offset"],
                    "versions": catalog["versions"],
                    "identity_status": "SOURCE_SYMBOL_ONLY",
                    "pit_status": "NON_PIT_RETROSPECTIVE",
                    "database_write": False,
                    "ddl_executed": False,
                    "source_zip_opened": False,
                    "source_recalculated": False,
                })
                return
            if parsed.path == "/api/preview/raw":
                values = _query(self.path, {"target_session", "package_sha256", "symbol"})
                required = {"target_session", "package_sha256", "symbol"}
                if set(values) != required:
                    raise PreviewHttpError(400, "PREVIEW_QUERY_REQUIRED", "Raw查询必须显式提供日期、完整SHA和来源代码")
                target = _iso_date(values["target_session"])
                package_sha = _sha(values["package_sha256"])
                symbol = _symbol(values["symbol"])
                row = read_raw_daily_stock(
                    self.preview_server.state.config,
                    source_symbol=symbol, target_session=target, package_sha256=package_sha,
                )
                if row is None:
                    raise PreviewHttpError(404, "PG_SOURCE_NO_MATCH", "PG中没有该明确日期、来源包SHA与来源代码的Raw记录")
                raw = _raw_public_fields(row)
                identity = self.preview_server.state.snapshot_identity(
                    namespace_id=str(row["namespace_id"]), target_session=target.isoformat(),
                    package_sha256=package_sha,
                )
                self._send_json(200, {"ok": True, "snapshot_identity": identity, "raw": raw, "database_write": False})
                return
            if parsed.path == "/api/preview/source-page":
                values = _query(self.path, {"target_session", "package_sha256", "offset", "limit", "market", "board"})
                if not {"target_session", "package_sha256"}.issubset(values):
                    raise PreviewHttpError(400, "PREVIEW_QUERY_REQUIRED", "候选分页必须显式提供目标日期和来源包SHA")
                target = _iso_date(values["target_session"])
                package_sha = _sha(values["package_sha256"])
                offset = _nonnegative_int(values.get("offset", "0"), "offset")
                limit = _nonnegative_int(values.get("limit", "20"), "limit", _MAX_PAGE_SIZE)
                if limit < 1:
                    raise PreviewHttpError(400, "PREVIEW_PARAMETER_INVALID", "limit必须在1至50之间")
                market = values.get("market", "ALL")
                board = values.get("board", "ALL")
                if market not in {"ALL", "SH", "SZ"} or board not in {"ALL", "MAIN_BOARD", "GEM"}:
                    raise PreviewHttpError(400, "PREVIEW_FILTER_INVALID", "市场或板别筛选无效")
                page = read_raw_daily_page(
                    self.preview_server.state.config, target_session=target,
                    package_sha256=package_sha, offset=offset, limit=limit,
                    market=market, board=board,
                )
                if page["version_status"] == "EMPTY":
                    raise PreviewHttpError(404, "PG_SOURCE_NO_MATCH", "PG中没有该明确日期和来源包SHA的Raw来源版本")
                namespace_id = page["namespace_id"]
                identity = self.preview_server.state.snapshot_identity(
                    namespace_id=namespace_id, target_session=target.isoformat(), package_sha256=package_sha,
                )
                self._send_json(200, {
                    "ok": True,
                    "snapshot_identity": identity,
                    "page": _page_public_fields(page),
                })
                return
            if parsed.path == "/api/preview/stock":
                raise PreviewHttpError(405, "PREVIEW_METHOD_NOT_ALLOWED", "单股Core/Profile计算只接受用户明确提交的POST")
            raise PreviewHttpError(404, "PREVIEW_ROUTE_NOT_FOUND", "请求路径不存在")
        except Exception as error:
            self._handle_error(error)

    def do_POST(self) -> None:
        if not self._ensure_local_host():
            return
        if self.path.split("?", 1)[0] != "/api/preview/stock":
            self._error(PreviewHttpError(404, "PREVIEW_ROUTE_NOT_FOUND", "POST仅用于用户明确触发的单股研究预览"))
            return
        origin = self.headers.get("Origin")
        if origin is not None and origin != f"http://{_HOST}:{_PORT}":
            self._error(PreviewHttpError(403, "PREVIEW_ORIGIN_REJECTED", "请求来源不属于本机预览页面"))
            return
        if self.headers.get_content_type() != "application/json":
            self._error(PreviewHttpError(415, "PREVIEW_CONTENT_TYPE_INVALID", "预览请求必须使用application/json"))
            return
        length_text = self.headers.get("Content-Length")
        if length_text is None or not length_text.isdecimal():
            self._error(PreviewHttpError(411, "PREVIEW_CONTENT_LENGTH_REQUIRED", "必须提供有效Content-Length"))
            return
        length = int(length_text)
        if length > _MAX_BODY_BYTES:
            self.close_connection = True
            self._error(PreviewHttpError(413, "PREVIEW_BODY_TOO_LARGE", "预览JSON请求体最多8192字节"))
            return
        if length == 0:
            self._error(PreviewHttpError(400, "PREVIEW_BODY_EMPTY", "预览请求体不能为空"))
            return
        try:
            raw_body = self.rfile.read(length)
            if len(raw_body) != length:
                raise PreviewHttpError(400, "PREVIEW_BODY_TRUNCATED", "预览请求体未完整到达")
            try:
                body = json.loads(
                    raw_body.decode("utf-8", errors="strict"),
                    object_pairs_hook=_duplicate_free_object,
                    parse_constant=_reject_constant,
                )
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
                raise PreviewHttpError(400, "PREVIEW_JSON_INVALID", "请求体不是有效UTF-8 JSON或含重复字段") from error
            expected = {"symbol", "target_session", "package_sha256", "session_count"}
            if not isinstance(body, dict) or set(body) != expected:
                raise PreviewHttpError(400, "PREVIEW_BODY_FIELDS_INVALID", "请求体必须且只能包含symbol、target_session、package_sha256、session_count")
            symbol = _symbol(body["symbol"])
            target_text = body["target_session"]
            target = _iso_date(target_text)
            package_sha = _sha(body["package_sha256"])
            session_count = _positive_int(body["session_count"], "session_count", 75)
            resolved_target, sessions, calendar_revision = diagnostics._pg_history_window(target.isoformat(), session_count)
            if resolved_target != target:
                raise PreviewHttpError(400, "PREVIEW_DATE_INVALID", "目标日期未通过官方交易日历校验")
            history = read_raw_daily_history_stock(
                self.preview_server.state.config,
                source_symbol=symbol, target_session=target, package_sha256=package_sha,
                sessions=sessions,
            )
            result = research_entry.research_stock_pg_history_preview(
                symbol=symbol,
                target_session=target.isoformat(),
                package_sha256=package_sha,
                session_dates=sessions,
                history_rows=history["history_rows"],
                namespace_id=history["namespace_id"],
                profile_contract=self.preview_server.state.profile_contract,
            )
            identity = self.preview_server.state.snapshot_identity(
                namespace_id=history["namespace_id"],
                target_session=target.isoformat(), package_sha256=package_sha,
                session_count=session_count, session_start=sessions[0].isoformat(),
                calendar_revision=calendar_revision,
            )
            result["snapshot_identity"] = identity
            result["selection"] = {
                "source_symbol": symbol,
                "target_session": target.isoformat(),
                "package_sha256": package_sha,
                "session_count": session_count,
                "calendar_revision": calendar_revision,
            }
            result["population_status"] = {
                "source_candidates": "PG_SOURCE_CANDIDATES_NON_PIT",
                "official_listing_population": "UNKNOWN_NOT_CONNECTED",
                "selected_or_excluded": "NOT_EVALUATED",
            }
            self._send_json(200, result)
        except Exception as error:
            self._handle_error(error)


_INDEX_HTML = """<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Stock A · 来源级研究预览</title><link rel="stylesheet" href="/app.css"><script src="/app.js" defer></script></head>
<body><header class="top"><div><p class="eyebrow">STOCK A / 本机开发研究</p><h1>来源级研究预览</h1><p class="sub">只读读取已保存的 PG Raw；显式选择日期、来源包版本和研究窗口。</p></div><span class="badge">非正式发布 · NON_PIT</span></header>
<main><section class="notice"><strong>数据边界</strong><span>当前页面展示来源候选，不代表官方上市总体、稳定证券身份、交易状态或选股结论。入选、行业和名称能力尚未接通。</span></section>
<section class="panel"><div class="panel-head"><div><h2>预览范围</h2><p>日期和包SHA都来自真实PG目录；任何查询都不会回退到最新版本。</p></div><button id="reloadCatalog" class="quiet">刷新目录</button></div>
<div class="controls"><label>目标交易日与来源包版本<select id="versionSelect" disabled><option>正在读取PG目录…</option></select></label><label>市场筛选<select id="marketSelect"><option value="ALL">沪深全部来源候选</option><option value="SH">上海来源候选</option><option value="SZ">深圳来源候选</option></select></label><label>板别筛选<select id="boardSelect"><option value="ALL">主板与创业板</option><option value="MAIN_BOARD">主板</option><option value="GEM">创业板</option></select></label></div>
<div id="population" class="population" role="status">等待读取PG来源目录。</div></section>
<div class="columns"><section class="panel"><div class="panel-head"><div><h2>来源候选</h2><p>按代码稳定排序；每页最多50行。缺Bar只说明来源缺值，不推断停牌或退市。</p></div><span id="pageCount" class="count">—</span></div>
<div class="table-wrap"><table><thead><tr><th>来源代码</th><th>市场 / 板别</th><th>目标日Bar</th><th>身份</th><th></th></tr></thead><tbody id="candidateRows"><tr><td colspan="5" class="muted">等待选择PG版本。</td></tr></tbody></table></div>
<div class="pager"><button id="prevPage" class="quiet" disabled>上一页</button><span id="pageStatus">—</span><div class="pager-actions"><button id="nextPage" class="quiet" disabled>下一页</button><button id="lastPage" class="quiet" disabled>末页</button></div></div></section>
<section class="panel search-panel"><div class="panel-head"><div><h2>单股研究预览</h2><p>查询动作仅读取所选目标日的PG历史和运行现役内核，不保存计算结果。</p></div></div>
<form id="stockForm"><label>来源代码<input id="symbolInput" name="symbol" placeholder="SH.600000" autocomplete="off" maxlength="9" required></label><label>目标日前官方交易会话数<input id="windowInput" name="session_count" type="number" min="1" max="75" value="75" required></label><button id="queryStock" type="submit">查询研究预览</button></form>
<div id="stockError" class="message" hidden></div><div id="stockResult" hidden>
<div class="identity-box"><strong id="resultTitle"></strong><span id="resultMode"></span><code id="snapshotId"></code><span id="windowText"></span></div>
<h3>原始行情与来源质量</h3><div id="rawSummary" class="raw-grid"></div>
<h3>Core 价格观察</h3><div class="table-wrap"><table><thead><tr><th>字段</th><th>值</th><th>质量</th><th>原因</th></tr></thead><tbody id="coreRows"></tbody></table></div>
<details><summary>查看全部43项 Core 字段</summary><div class="table-wrap"><table><thead><tr><th>字段</th><th>值</th><th>质量</th><th>原因</th></tr></thead><tbody id="allCoreRows"></tbody></table></div></details>
<h3>Profile 组件状态（11项）</h3><div class="table-wrap"><table><thead><tr><th>组件</th><th>状态</th><th>值</th><th>原因</th></tr></thead><tbody id="profileRows"></tbody></table></div>
<div class="unavailable"><strong>当前未连接能力</strong><span>官方上市总体、行业、中文名称、入选 / 未入选与Near-Miss：<code>NOT_CONNECTED / NOT_EVALUATED</code>。本页不以搜索不到或缺Bar代表不合格。</span></div>
</div></section></div>
<footer>Stock A 本机来源级研究预览 · PostgreSQL只读 · RAW_UNADJUSTED · NON_PIT_RETROSPECTIVE</footer></main></body></html>"""


_APP_CSS = """*{box-sizing:border-box}body{margin:0;background:#f2f5f8;color:#162332;font:15px/1.55 "Segoe UI","Microsoft YaHei",sans-serif}.top{background:#132b3d;color:white;padding:28px max(22px,calc((100vw - 1440px)/2));display:flex;align-items:center;justify-content:space-between;gap:18px}.top h1{font-size:30px;margin:2px 0}.top p{margin:0}.eyebrow{font-size:12px;letter-spacing:.1em;color:#a9c3d4}.sub{color:#d3e1e9}.badge{border:1px solid #75b8a4;border-radius:20px;padding:7px 13px;color:#c7f5e6;white-space:nowrap}main{max-width:1440px;margin:22px auto;padding:0 18px}.notice,.unavailable{display:flex;gap:12px;align-items:flex-start;background:#fff6de;border:1px solid #ecd797;border-radius:10px;padding:13px 16px;margin-bottom:16px}.notice strong,.unavailable strong{white-space:nowrap}.panel{background:white;border:1px solid #dce4eb;border-radius:12px;padding:18px;margin-bottom:16px;min-width:0;box-shadow:0 2px 8px #17344a0b}.panel-head{display:flex;justify-content:space-between;align-items:flex-start;gap:12px;margin-bottom:14px}.panel h2{font-size:19px;margin:0 0 3px}.panel h3{font-size:16px;margin:20px 0 9px}.panel-head p{color:#617384;margin:0;font-size:13px}.controls{display:grid;grid-template-columns:minmax(260px,2fr) minmax(170px,1fr) minmax(170px,1fr);gap:12px}label{display:flex;flex-direction:column;gap:5px;font-weight:600;font-size:13px}input,select{width:100%;min-height:40px;padding:8px 10px;border:1px solid #bdcbd6;border-radius:7px;background:white;color:#172a39;font:inherit}button{border:0;border-radius:7px;background:#176c8a;color:white;padding:10px 14px;font:600 14px inherit;cursor:pointer}button:hover{background:#115672}button:disabled{opacity:.45;cursor:not-allowed}.quiet{background:#e8eef2;color:#263f50}.quiet:hover{background:#d9e4eb}.population{margin-top:14px;padding:11px 12px;border-radius:7px;background:#f1f6f8;color:#314d5e;font-size:13px}.columns{display:grid;grid-template-columns:minmax(420px,1.05fr) minmax(480px,1fr);gap:16px;align-items:start}.columns .panel{margin-bottom:0}.count{color:#5b7180;font-size:13px;white-space:nowrap}.table-wrap{overflow:auto;max-width:100%}table{border-collapse:collapse;width:100%;font-size:13px}th,td{text-align:left;padding:8px 9px;border-bottom:1px solid #e8edf1;vertical-align:top}th{color:#5e7080;font-weight:600;white-space:nowrap}td code,code{font:12px ui-monospace,Consolas,monospace;overflow-wrap:anywhere}.row-select{padding:5px 9px;font-size:12px}.muted{color:#718392}.pager{display:flex;align-items:center;justify-content:space-between;gap:10px;padding-top:12px;font-size:12px;color:#617384}#stockForm{display:grid;grid-template-columns:minmax(145px,1.2fr) minmax(140px,.8fr) auto;gap:10px;align-items:end}.message{margin-top:13px;padding:11px;border-radius:7px;background:#fff0ee;color:#8a3024}.identity-box{display:grid;gap:5px;background:#eef5f7;border-left:3px solid #3b879d;padding:12px;margin-top:14px;overflow-wrap:anywhere}.identity-box span{font-size:13px;color:#445d6b}.raw-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px}.raw-item{border:1px solid #e2e9ed;border-radius:7px;padding:8px;min-width:0}.raw-item span{display:block;color:#627888;font-size:11px}.raw-item strong{display:block;overflow-wrap:anywhere;font-size:13px}.raw-item small{color:#617384;overflow-wrap:anywhere}details{margin-top:12px}summary{cursor:pointer;color:#176c8a;font-weight:600}footer{text-align:center;color:#70818c;font-size:12px;padding:22px}.unavailable{margin-top:16px;margin-bottom:0;background:#f3f5f7;border-color:#e0e5e9;color:#425864}.unavailable span{font-size:13px}@media(max-width:980px){.columns{grid-template-columns:1fr}.controls{grid-template-columns:1fr 1fr}.controls label:first-child{grid-column:1/-1}}@media(max-width:620px){.top{align-items:flex-start;flex-direction:column;padding:20px 16px}.top h1{font-size:24px}main{margin:13px auto;padding:0 10px}.panel{padding:13px}.notice,.unavailable{display:block}.notice span,.unavailable span{display:block;margin-top:4px}.controls{grid-template-columns:1fr}.controls label:first-child{grid-column:auto}#stockForm{grid-template-columns:1fr 1fr}#stockForm button{grid-column:1/-1}.raw-grid{grid-template-columns:repeat(2,minmax(0,1fr))}table{min-width:540px}.search-panel table{min-width:500px}.badge{font-size:12px}}"""


_APP_CSS += ".message.success{background:#e9f7ef;color:#20683c}.pager-actions{display:flex;gap:7px}button{font:600 14px \"Segoe UI\",sans-serif}@media(max-width:1300px){.raw-grid{grid-template-columns:repeat(2,minmax(0,1fr))}}"

_APP_JS = """'use strict';
const $=id=>document.getElementById(id);
const state={versions:[],version:null,offset:0,limit:20,total:0,pageRequest:0,loading:false};
const labels={'TARGET_PRESENT':'有Bar','TARGET_MISSING':'来源无Bar','READ_FAILED':'读取失败','MAIN_BOARD':'主板','GEM':'创业板','OBSERVED':'可观察','UNKNOWN':'未知','UNKNOWN_DATA':'数据未知','NOT_IMPLEMENTED':'未实现','CONTRACT_UNBOUND':'合同未绑定','NOT_APPLICABLE':'不适用','NOT_EVALUATED':'未评价','UNIT_AUTHORITY_NOT_VERIFIED':'量额单位权威未核实','SHORT_HISTORY':'历史窗口不足','UNKNOWN_SESSION_GAP':'未知交易会话缺口','PG_HISTORY_SESSION_MISSING':'PG缺少所需历史会话','HISTORY_WINDOW_INCOMPLETE':'历史窗口不完整','EVALUATED':'已计算'};
const profileLabels={'profile.trend':'趋势','profile.position':'价格位置','profile.ma_structure':'均线结构','profile.relative':'相对强弱','profile.compression':'压缩状态','profile.participation':'成交参与','profile.extension':'延伸程度','profile.near_high20':'接近20会话高点','profile.drawdown60':'60会话回撤','profile.closed_week_trend':'闭合周趋势','profile.closed_month_trend':'闭合月趋势'};
const text=v=>{if(v===null||v===undefined)return '—';if(typeof v==='object'){if(v.exact!==undefined)return `${v.exact} (${v.decimal??''})`;return JSON.stringify(v)}return labels[String(v)]||String(v)};
const cell=(row,value)=>{const td=document.createElement('td');td.textContent=text(value);row.append(td);return td};
function tableRow(parent,values){const tr=document.createElement('tr');values.forEach(v=>cell(tr,v));parent.append(tr);return tr}
function current(){return state.version}
function setMessage(message,ok=false){const el=$('stockError');el.hidden=!message;el.textContent=message||'';el.classList.toggle('success',ok)}
async function api(url,options){const response=await fetch(url,{cache:'no-store',...options});let payload;try{payload=await response.json()}catch{throw new Error('服务器返回了无法读取的响应。')}if(!response.ok||payload.ok===false)throw new Error(`${payload.code||'请求失败'}：${payload.message||'请检查输入和PG目录。'}`);return payload}
function selectedVersion(){const option=$('versionSelect').selectedOptions[0];if(!option||!option.value.includes('|'))return null;const [target_session,package_sha256]=option.value.split('|');return {target_session,package_sha256}}
function clearResult(){ $('stockResult').hidden=true;$('coreRows').replaceChildren();$('allCoreRows').replaceChildren();$('profileRows').replaceChildren();$('rawSummary').replaceChildren();setMessage('') }
async function loadCatalog(){
 state.pageRequest++;
 $('versionSelect').disabled=true;$('versionSelect').replaceChildren(new Option('正在读取PG目录…',''));$('population').textContent='正在从PostgreSQL只读读取来源日期与版本目录。';
 try{
  const data=await api('/api/preview/catalog?offset=0&limit=100');state.versions=data.versions;$('versionSelect').replaceChildren(new Option('— 请选择目标日期与来源包版本 —',''));
  for(const v of data.versions){const title=`${v.session_date} · ${v.package_sha256.slice(0,12)}… · ${v.version_status} · Raw ${v.row_count}/${v.expected_row_count}（有Bar ${v.bar_status_counts.TARGET_PRESENT||0} / 缺Bar ${v.bar_status_counts.TARGET_MISSING||0}）`;const option=new Option(title,`${v.session_date}|${v.package_sha256}`);$('versionSelect').append(option)}
  $('versionSelect').disabled=state.versions.length===0;state.version=null;state.offset=0;$('candidateRows').replaceChildren();$('pageCount').textContent='—';$('pageStatus').textContent=state.versions.length?'请选择PG日期和版本':'PG目录为空';$('prevPage').disabled=true;$('nextPage').disabled=true;$('lastPage').disabled=true;
  $('population').textContent=state.versions.length?`已读取${data.total_version_count}个真实PG日期/版本。请选择明确版本；Raw来源候选不代表官方上市总体。`:'PG中没有可选的Raw来源版本。';
 }catch(error){$('population').textContent=`目录读取失败：${error.message}`;$('candidateRows').replaceChildren();tableRow($('candidateRows'),['PG目录不可用','','','','']);$('versionSelect').disabled=true}
}
function describePopulation(page){const version=page.source_version;const versionText=version.status==='CLOSED'?`来源包Raw记录闭合：${version.stored_raw_rows}/${version.capture_expected_rows}`:`来源包Raw部分入库：${version.stored_raw_rows}/${version.capture_expected_rows}；不得按全市场完成解释`;return `${page.target_session} · 包SHA ${page.package_sha256} · ${versionText}。当前筛选来源候选=${page.filtered_total}；有效上市总体=UNKNOWN（NOT_CONNECTED）。这只是该来源包的候选分母。`}
async function loadPage(){const v=selectedVersion();if(!v)return;const requestId=++state.pageRequest;state.version=v;clearResult();$('candidateRows').replaceChildren();$('pageStatus').textContent='正在读取PG分页…';const query=new URLSearchParams({target_session:v.target_session,package_sha256:v.package_sha256,offset:String(state.offset),limit:String(state.limit),market:$('marketSelect').value,board:$('boardSelect').value});try{const data=await api(`/api/preview/source-page?${query}`);if(requestId!==state.pageRequest)return;const p=data.page;state.total=p.filtered_total;$('population').textContent=describePopulation(p);$('pageCount').textContent=`本页 ${p.page_count} / 筛选分母 ${p.filtered_total}`;$('pageStatus').textContent=`偏移 ${p.offset}，后续 ${p.next_offset===null?0:p.filtered_total-p.next_offset}`;$('prevPage').disabled=p.offset===0;$('nextPage').disabled=p.next_offset===null;$('candidateRows').replaceChildren();if(!p.results.length){tableRow($('candidateRows'),['当前筛选没有来源记录','','','','']);return}for(const item of p.results){const tr=document.createElement('tr');cell(tr,item.source_symbol);cell(tr,`${item.market} / ${text(item.board)}`);cell(tr,text(item.bar_status));cell(tr,'SOURCE_SYMBOL_ONLY');const td=document.createElement('td');const button=document.createElement('button');button.className='row-select';button.type='button';button.textContent='查看';button.addEventListener('click',()=>{$('symbolInput').value=item.source_symbol;$('stockForm').requestSubmit()});td.append(button);tr.append(td);$('candidateRows').append(tr)}}catch(error){if(requestId!==state.pageRequest)return;$('population').textContent=`候选页读取失败：${error.message}`;$('pageStatus').textContent='分页不可用';$('prevPage').disabled=true;$('nextPage').disabled=true;tableRow($('candidateRows'),['PG分页失败','','','',''])}}
labels['TARGET_SESSION_BAR_MISSING']='目标日来源记录无Bar，不推断停牌或退市';labels['CURRENT_SESSION_UNKNOWN']='目标日状态未知';
function rawItems(raw){
 const pairs=[
  ['目标日Bar',`${text(raw.bar_status)} · ${text(raw.bar_reason)}`],
  ['原始OHLC',Object.entries(raw.values).filter(([key])=>['open','high','low','close'].includes(key)).map(([key,value])=>`${key}=${text(value)}`).join(' · ')],
  ['原始金额',`${text(raw.values.amount)} · 字段质量 ${text(raw.quality.amount_quality)} · 原值质量 ${text(raw.quality.raw_amount_quality)} · ${text(raw.quality.amount_unit_status)}`],
  ['原始数量',`${text(raw.values.volume)} · 字段质量 ${text(raw.quality.volume_quality)} · 原值质量 ${text(raw.quality.raw_volume_quality)} · ${text(raw.quality.volume_unit_status)}`],
  ['价格质量',`${text(raw.quality.price_quality)} · ${text(raw.quality.price_reason)}`],
  ['来源包SHA',text(raw.source.package_sha256)],
  ['来源时钟',`${text(raw.source.source_clock_role)} · ${text(raw.source.source_time_quality)}`],
  ['本机观察时钟',`${text(raw.source.observed_started_at)} → ${text(raw.source.observed_validated_at)}`],
  ['来源条目SHA',text(raw.source.source_entry_sha256)],
  ['身份与坐标',`${raw.identity_status} · ${raw.price_coordinate} · ${raw.pit_status}`]
 ];const root=$('rawSummary');root.replaceChildren();for(const [label,value] of pairs){const item=document.createElement('div');item.className='raw-item';const span=document.createElement('span');span.textContent=label;const strong=document.createElement('strong');strong.textContent=value;item.append(span,strong);root.append(item)}
}
function renderFields(parent,fields,select){parent.replaceChildren();for(const name of select){const field=fields[name]||{};tableRow(parent,[name,field.value,field.quality,field.reason])}}
const coreNames=['core.ma20','core.ma60','core.atr20','core.ret20','core.pos60'];
const loadPageOriginal=loadPage;loadPage=async function(){await loadPageOriginal();$('lastPage').disabled=state.total===0||state.offset+state.limit>=state.total};
$('lastPage').addEventListener('click',()=>{if(state.total){state.offset=Math.floor((state.total-1)/state.limit)*state.limit;loadPage()}});
async function queryStock(event){event.preventDefault();const v=selectedVersion();if(!v){setMessage('请先从真实PG目录选择目标日期和来源包版本。');return}const symbol=$('symbolInput').value.trim();const count=Number($('windowInput').value);clearResult();$('queryStock').disabled=true;setMessage('正在按显式日期、包SHA和窗口只读读取PG并计算现役Core/Profile…');try{const raw=await api(`/api/preview/raw?${new URLSearchParams({...v,symbol})}`);const result=await api('/api/preview/stock',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({symbol,target_session:v.target_session,package_sha256:v.package_sha256,session_count:count})});const a=raw.snapshot_identity,b=result.snapshot_identity;if(a.namespace_id!==b.namespace_id||a.target_session!==b.target_session||a.package_sha256!==b.package_sha256)throw new Error('RAW与Core/Profile预览身份不一致，已拒绝合并展示。');$('stockResult').hidden=false;setMessage('研究预览已返回；数值为RAW_UNADJUSTED / NON_PIT。',true);$('resultTitle').textContent=`${symbol} · ${v.target_session}`;$('resultMode').textContent=`${result.status} · ${result.price_coordinate} · ${result.identity_status} · 正式准入 ${result.formal_foundation_status}`;$('snapshotId').textContent=`预览身份SHA256：${result.snapshot_identity.identity_sha256}（非正式context token） · 来源包SHA：${v.package_sha256}`;$('windowText').textContent=`窗口 ${result.window.start_session} 至 ${result.window.end_session}：实际Bar ${result.window.real_bar_count}/${result.window.requested_sessions}，未知会话 ${result.window.gap_count}；未使用未来会话。`;$('resultMode').dataset.namespace=result.snapshot_identity.namespace_id;rawItems(raw.raw);renderFields($('coreRows'),result.core.fields,coreNames);renderFields($('allCoreRows'),result.core.fields,Object.keys(result.core.fields).sort());$('profileRows').replaceChildren();for(const component of result.profile_preview.components){const key=component.field_id||'';tableRow($('profileRows'),[`${profileLabels[key]||key} · ${key}`,component.status==='EVALUATED'?component.quality:component.status,component.value,component.reason])}}catch(error){$('stockResult').hidden=true;setMessage(error.message)}finally{$('queryStock').disabled=false}}
$('reloadCatalog').addEventListener('click',loadCatalog);$('versionSelect').addEventListener('change',()=>{state.offset=0;loadPage()});$('marketSelect').addEventListener('change',()=>{state.offset=0;loadPage()});$('boardSelect').addEventListener('change',()=>{state.offset=0;loadPage()});$('prevPage').addEventListener('click',()=>{state.offset=Math.max(0,state.offset-state.limit);loadPage()});$('nextPage').addEventListener('click',()=>{if(state.offset+state.limit<state.total){state.offset+=state.limit;loadPage()}});$('stockForm').addEventListener('submit',queryStock);loadCatalog();"""


def run_preview_server(*, host: str = _HOST, port: int = _PORT) -> int:
    """仅以前台显式运行方式启动127.0.0.1:28766。"""
    if host != _HOST or type(port) is not int or port != _PORT:
        print("拒绝启动：本机研究预览只允许绑定127.0.0.1:28766。", file=diagnostics.sys.stderr)
        return diagnostics._EXIT_ARGUMENT
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.settimeout(0.2)
        try:
            if probe.connect_ex((_HOST, _PORT)) == 0:
                print("拒绝启动：127.0.0.1:28766已有监听者；不会终止或接管任何进程。", file=diagnostics.sys.stderr)
                return diagnostics._EXIT_UNAVAILABLE
        except OSError:
            pass
    try:
        state = _PreviewState()
        server = _BoundedHTTPServer((_HOST, _PORT), state)
    except (OSError, diagnostics.DiagnosticError, DayChainContractError) as error:
        print(f"预览服务未启动：{type(error).__name__}。", file=diagnostics.sys.stderr)
        return diagnostics._EXIT_UNAVAILABLE
    print("Stock A 本机来源级研究预览已启动：http://127.0.0.1:28766/（只读PG；Ctrl+C安全停止）")
    try:
        server.serve_forever(poll_interval=0.25)
    except KeyboardInterrupt:
        print("正在停止本机研究预览服务。")
    finally:
        server.shutdown()
        server.server_close()
    return diagnostics._EXIT_OK
