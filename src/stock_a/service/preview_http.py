"""本机只读PG来源级研究预览HTTP服务。"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
import json
import re
import socket
import sys
import threading
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import parse_qs, urlsplit

from stock_a.contracts import DayChainContractError, load_day_chain_contract
from stock_a.application.runtime_config import (
    DiagnosticError,
    _CONFIG_ROOT,
    _EXIT_ARGUMENT,
    _EXIT_OK,
    _EXIT_UNAVAILABLE,
    load_runtime_config,
)
from stock_a.application.research_window import resolve_pg_history_window
from stock_a.application.pg_research import (
    PgResearchBindingError,
    read_sealed_source_context,
)
from stock_a.application.pg_price_profiles import (
    PgPriceProfileError,
    query_source_symbol_price_profiles,
    query_source_symbol_price_profile_changes,
    source_symbol_price_profile_snapshots,
)
from stock_a.application.source_ingress import build_source_ingress_plan
from stock_a.storage.postgres.source_raw import (
    RawSourcePgError,
    read_raw_daily_catalog,
    read_raw_daily_page,
    read_raw_daily_readiness,
    read_raw_daily_stock,
    read_raw_daily_history_stock,
)
from stock_a.storage.postgres.source_set_candidate import (
    SourceSetCandidatePgError,
    read_source_set_candidate,
)
from stock_a.research import core as research_core
from stock_a.research import entry as research_entry


from stock_a.service.readiness import build_chain_readiness_payload
from stock_a.service.serialization import _json_value, canonical_sha
from stock_a.service.static_assets import read_static_asset

_HOST = "127.0.0.1"
_PORT = 28766
_MAX_BODY_BYTES = 8192
_MAX_PAGE_SIZE = 50
_MAX_OFFSET = 2**31 - 1
_SYMBOL = re.compile(r"^(SH|SZ)\.[0-9]{6}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_DATE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
_PROFILE_CONTRACT_NAME = "STOCK_A_DAY_CHAIN_R11"


# 只登记运行代码当前实际提供的能力边界。阶段名称、输入、产物和消费者始终来自机器合同。


class PreviewHttpError(Exception):
    def __init__(self, status: int, code: str, message: str):
        self.status = status
        self.code = code
        self.message = message
        super().__init__(message)


def _require_research_cutoff(target_session: date, max_target_session: date) -> None:
    if target_session > max_target_session:
        raise PreviewHttpError(
            400,
            "RESEARCH_CUTOFF_EXCEEDED",
            f"研究日期不能晚于冻结截止日{max_target_session.isoformat()}",
        )


def _iso_date(value: Any, field: str = "target_session") -> date:
    if not isinstance(value, str) or _DATE.fullmatch(value) is None:
        raise PreviewHttpError(
            400, "PREVIEW_DATE_INVALID", f"{field}必须使用YYYY-MM-DD格式"
        )
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise PreviewHttpError(
            400, "PREVIEW_DATE_INVALID", f"{field}不是有效日期"
        ) from error
    if parsed.isoformat() != value:
        raise PreviewHttpError(400, "PREVIEW_DATE_INVALID", f"{field}不是标准ISO日期")
    return parsed


def _sha(value: Any) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise PreviewHttpError(
            400, "PREVIEW_SHA_INVALID", "来源包SHA必须是64位小写十六进制值"
        )
    return value


def _symbol(value: Any) -> str:
    if not isinstance(value, str) or _SYMBOL.fullmatch(value) is None:
        raise PreviewHttpError(
            400, "PREVIEW_SYMBOL_INVALID", "来源代码必须符合SH.六位数字或SZ.六位数字"
        )
    return value


def _positive_int(value: Any, field: str, maximum: int) -> int:
    if type(value) is not int or not 1 <= value <= maximum:
        raise PreviewHttpError(
            400, "PREVIEW_PARAMETER_INVALID", f"{field}必须为1至{maximum}的整数"
        )
    return value


def _nonnegative_int(value: str | None, field: str, maximum: int = _MAX_OFFSET) -> int:
    if value is None or not value.isdecimal():
        raise PreviewHttpError(
            400, "PREVIEW_PARAMETER_INVALID", f"{field}必须为非负整数"
        )
    parsed = int(value)
    if parsed > maximum:
        raise PreviewHttpError(400, "PREVIEW_PARAMETER_INVALID", f"{field}超过允许上限")
    return parsed


def _query(path: str, allowed: set[str]) -> dict[str, str]:
    parsed = urlsplit(path)
    try:
        values = parse_qs(parsed.query, keep_blank_values=True, strict_parsing=True)
    except ValueError as error:
        raise PreviewHttpError(
            400, "PREVIEW_QUERY_INVALID", "查询字符串格式无效"
        ) from error
    if set(values) - allowed or any(len(items) != 1 for items in values.values()):
        raise PreviewHttpError(
            400, "PREVIEW_QUERY_INVALID", "查询参数重复或包含未登记字段"
        )
    return {name: items[0] for name, items in values.items()}


def _raw_public_fields(row: Mapping[str, Any]) -> dict[str, Any]:
    raw_names = (
        "raw_open",
        "raw_high",
        "raw_low",
        "raw_close",
        "raw_amount",
        "raw_volume",
    )
    quality_names = (
        "bar_status",
        "bar_reason",
        "price_quality",
        "price_reason",
        "raw_amount_quality",
        "raw_amount_reason",
        "amount_quality",
        "amount_reason",
        "amount_unit_status",
        "raw_volume_quality",
        "raw_volume_reason",
        "volume_quality",
        "volume_reason",
        "volume_unit_status",
    )
    return {
        "session_date": row["session_date"].isoformat(),
        "source_symbol": row["source_symbol"],
        "market": row["market"],
        "board": row["board"],
        "bar_status": row["bar_status"],
        "bar_reason": row["bar_reason"],
        "values": {
            name.removeprefix("raw_"): _json_value(row.get(name)) for name in raw_names
        },
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
        "source_object_status": row.get(
            "source_object_status", "TDX_SOURCE_SYMBOL_OBSERVED"
        ),
        "target_bar_status": row.get("target_bar_status"),
        "stable_security_id_status": row.get(
            "stable_security_id_status", "STABLE_SECURITY_ID_UNVERIFIED"
        ),
        "listing_status": row.get("listing_status", "LISTING_STATUS_UNKNOWN"),
        "suspension_status": row.get("suspension_status", "SUSPENSION_STATUS_UNKNOWN"),
        "official_listing_universe_status": "UNKNOWN",
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
            "source_object_status": row.get(
                "source_object_status", "TDX_SOURCE_SYMBOL_OBSERVED"
            ),
            "target_bar_status": row.get("target_bar_status"),
            "stable_security_id_status": row.get(
                "stable_security_id_status", "STABLE_SECURITY_ID_UNVERIFIED"
            ),
            "listing_status": row.get("listing_status", "LISTING_STATUS_UNKNOWN"),
            "suspension_status": row.get(
                "suspension_status", "SUSPENSION_STATUS_UNKNOWN"
            ),
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
        "source_object_status": page.get(
            "source_object_status", "TDX_SOURCE_SYMBOL_OBSERVED"
        ),
        "stable_security_id_status": page.get(
            "stable_security_id_status", "STABLE_SECURITY_ID_UNVERIFIED"
        ),
        "listing_status": page.get("listing_status", "LISTING_STATUS_UNKNOWN"),
        "suspension_status": page.get("suspension_status", "SUSPENSION_STATUS_UNKNOWN"),
        "official_listing_universe_status": page.get(
            "official_listing_universe_status", "UNKNOWN"
        ),
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "price_coordinate": "RAW_UNADJUSTED",
        "source_zip_opened": False,
        "source_recalculated": False,
        "ddl_executed": False,
        "database_write": False,
    }


class _PreviewState:
    def __init__(self, *, research_cutoff: date) -> None:
        if type(research_cutoff) is not date:
            raise DiagnosticError("RESEARCH_CUTOFF_REQUIRED", "必须显式绑定研究截止日")
        self.research_cutoff = research_cutoff
        self.config, self.config_path = load_runtime_config()
        services = self.config.get("services")
        development = (
            services.get("development") if isinstance(services, Mapping) else None
        )
        if (
            not isinstance(development, Mapping)
            or development.get("host") != _HOST
            or type(development.get("port")) is not int
            or development.get("port") != _PORT
        ):
            raise DiagnosticError(
                "CONFIG_INVALID", "本机研究预览要求配置development服务为127.0.0.1:28766"
            )
        self.profile_contract = load_day_chain_contract(_CONFIG_ROOT).document
        self.profile_contract_sha256 = canonical_sha(self.profile_contract)
        self.core_source_sha256 = hashlib.sha256(
            Path(research_core.__file__).read_bytes()
        ).hexdigest()
        self.entry_source_sha256 = hashlib.sha256(
            Path(research_entry.__file__).read_bytes()
        ).hexdigest()

    def snapshot_identity(
        self,
        *,
        namespace_id: str,
        target_session: str,
        package_sha256: str,
        session_count: int | None = None,
        session_start: str | None = None,
        calendar_revision: str | None = None,
        source_set_id: str | None = None,
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
        if source_set_id is not None:
            fields["source_set_id"] = source_set_id
        return {**fields, "identity_sha256": canonical_sha(fields)}


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
                {
                    "ok": False,
                    "code": "PREVIEW_CONCURRENCY_LIMIT",
                    "message": "预览请求并发已达4个上限，请稍后重试",
                },
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
            response = (
                b"HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json; charset=utf-8\r\n"
                + f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode(
                    "ascii"
                )
                + body
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

    def process_request_thread(
        self, request: socket.socket, client_address: Any
    ) -> None:
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
        self.send_header(
            "Permissions-Policy", "camera=(), microphone=(), geolocation=()"
        )
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
            _json_value(payload),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
        self._send(status, body, "application/json; charset=utf-8")

    def _error(self, error: PreviewHttpError) -> None:
        self._send_json(
            error.status, {"ok": False, "code": error.code, "message": error.message}
        )

    def _ensure_local_host(self) -> bool:
        if self._host_allowed():
            return True
        self._error(
            PreviewHttpError(
                421, "PREVIEW_HOST_REJECTED", "请求Host不属于127.0.0.1:28766"
            )
        )
        return False

    def _handle_error(self, error: Exception) -> None:
        if isinstance(error, PreviewHttpError):
            self._error(error)
            return
        if isinstance(error, PgResearchBindingError):
            status = (
                409 if error.code == "PG_RESEARCH_P04_SOURCE_SET_UNAVAILABLE" else 503
            )
            self._error(PreviewHttpError(status, error.code, str(error)))
            return
        if isinstance(error, PgPriceProfileError):
            if error.code.endswith("INVALID") or error.code in {
                "PROFILE_PAGE_INVALID",
                "PROFILE_FILTER_INVALID",
                "PROFILE_SOURCE_SET_ID_INVALID",
                "PROFILE_REVISION_INVALID",
                "PROFILE_SYMBOL_INVALID",
                "PROFILE_DATE_AFTER_CUTOFF",
                "PROFILE_CUTOFF_REQUIRED",
                "PROFILE_CHANGE_DATE_INVALID",
            }:
                status = 400
            elif error.code in {
                "PROFILE_SOURCE_SET_NOT_FOUND",
                "PROFILE_SNAPSHOT_INCOMPLETE",
            }:
                status = 404
            else:
                status = 503
            self._error(PreviewHttpError(status, error.code, str(error)))
            return
        if isinstance(error, RawSourcePgError):
            invalid = error.code.endswith("INVALID") or error.code in {
                "PG_QUERY_KEY_INVALID",
                "PG_PAGE_INVALID",
                "PG_FILTER_INVALID",
                "PG_CATALOG_PAGE_INVALID",
                "PG_CATALOG_YEAR_INVALID",
                "SOURCE_SYMBOL_INVALID",
                "HISTORY_SESSION_WINDOW_INVALID",
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
        if isinstance(error, SourceSetCandidatePgError):
            self._error(PreviewHttpError(503, error.code, str(error)))
            return
        if isinstance(error, DiagnosticError):
            status = 400 if error.code.endswith("INVALID") else 503
            self._error(PreviewHttpError(status, error.code, str(error)))
            return
        if isinstance(
            error, (DayChainContractError, research_entry.ResearchEntryError)
        ):
            code = getattr(error, "code", "PREVIEW_CONTRACT_UNAVAILABLE")
            status = (
                400
                if code.endswith("INVALID") or code == "TARGET_NOT_TRADING_SESSION"
                else 503
            )
            self._error(PreviewHttpError(status, code, str(error)))
            return
        self._error(
            PreviewHttpError(
                500, "PREVIEW_INTERNAL_ERROR", "预览请求未完成；请检查本机服务输出"
            )
        )

    def do_HEAD(self) -> None:
        self.do_GET()

    def do_OPTIONS(self) -> None:
        self._send_json(
            405,
            {
                "ok": False,
                "code": "PREVIEW_METHOD_NOT_ALLOWED",
                "message": "此路径不接受OPTIONS请求",
            },
        )

    def do_GET(self) -> None:
        if not self._ensure_local_host():
            return
        try:
            parsed = urlsplit(self.path)
            if parsed.path == "/":
                if parsed.query:
                    raise PreviewHttpError(
                        400, "PREVIEW_QUERY_INVALID", "首页不接受查询参数"
                    )
                body, content_type = read_static_asset("index.html")
                self._send(200, body, content_type)
                return
            if parsed.path == "/app.css":
                if parsed.query:
                    raise PreviewHttpError(
                        400, "PREVIEW_QUERY_INVALID", "静态资源不接受查询参数"
                    )
                body, content_type = read_static_asset("app.css")
                self._send(200, body, content_type)
                return
            if parsed.path == "/app.js":
                if parsed.query:
                    raise PreviewHttpError(
                        400, "PREVIEW_QUERY_INVALID", "静态资源不接受查询参数"
                    )
                body, content_type = read_static_asset("app.js")
                self._send(200, body, content_type)
                return
            if parsed.path == "/api/preview/catalog":
                values = _query(self.path, {"offset", "limit", "year"})
                offset = _nonnegative_int(values.get("offset", "0"), "offset")
                limit = _positive_int(
                    _nonnegative_int(values.get("limit", "100"), "limit", 100) or 0,
                    "limit",
                    100,
                )
                year = None
                if "year" in values:
                    year = _nonnegative_int(values["year"], "year", 9998)
                    if year < 1900:
                        raise PreviewHttpError(
                            400, "PREVIEW_PARAMETER_INVALID", "year必须在1900至9998之间"
                        )
                catalog = read_raw_daily_catalog(
                    self.preview_server.state.config,
                    max_target_session=self.preview_server.state.research_cutoff,
                    year=year,
                    offset=offset,
                    limit=limit,
                )
                self._send_json(
                    200,
                    {
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
                    },
                )
                return
            if parsed.path == "/api/source-symbol-price-profiles/snapshots":
                if parsed.query:
                    raise PreviewHttpError(
                        400,
                        "PROFILE_QUERY_INVALID",
                        "画像版本目录不接受隐式筛选或latest参数",
                    )
                snapshots = source_symbol_price_profile_snapshots(
                    self.preview_server.state.config,
                    max_target_session=self.preview_server.state.research_cutoff,
                )
                self._send_json(
                    200,
                    {
                        "ok": True,
                        "research_mode": "SOURCE_SYMBOL_NON_PIT_PRICE_PROFILE",
                        "formal_listing_universe": "UNKNOWN_NOT_CONNECTED",
                        "research_cutoff": self.preview_server.state.research_cutoff.isoformat(),
                        "snapshots": snapshots["snapshots"],
                        "database_write": False,
                        "ddl_executed": False,
                    },
                )
                return
            if parsed.path == "/api/source-symbol-price-profile-changes":
                values = _query(
                    self.path,
                    {
                        "before_session",
                        "after_session",
                        "offset",
                        "limit",
                        "symbol",
                        "change_type",
                    },
                )
                if not {"before_session", "after_session"}.issubset(values):
                    raise PreviewHttpError(
                        400,
                        "PROFILE_CHANGE_QUERY_REQUIRED",
                        "变化查询必须显式提供前后两个画像日期",
                    )
                before_session = _iso_date(values["before_session"], "before_session")
                after_session = _iso_date(values["after_session"], "after_session")
                _require_research_cutoff(
                    before_session, self.preview_server.state.research_cutoff
                )
                _require_research_cutoff(
                    after_session, self.preview_server.state.research_cutoff
                )
                symbol = _symbol(values["symbol"]) if "symbol" in values else None
                result = query_source_symbol_price_profile_changes(
                    self.preview_server.state.config,
                    before_session=before_session,
                    after_session=after_session,
                    max_target_session=self.preview_server.state.research_cutoff,
                    offset=_nonnegative_int(values.get("offset", "0"), "offset"),
                    limit=_positive_int(
                        _nonnegative_int(values.get("limit", "30"), "limit", 30) or 0,
                        "limit",
                        30,
                    ),
                    source_symbol=symbol,
                    change_type=values.get("change_type", "ALL"),
                )
                result.update(
                    {
                        "ok": True,
                        "research_mode": "ACCEPTED_PG_PROFILE_CHANGE_OBSERVATION",
                        "database_write": False,
                        "ddl_executed": False,
                    }
                )
                self._send_json(200, result)
                return
            if parsed.path == "/api/source-symbol-price-profiles":
                values = _query(
                    self.path,
                    {
                        "target_session",
                        "source_set_id",
                        "package_sha256",
                        "profile_revision",
                        "offset",
                        "limit",
                        "market",
                        "board",
                        "target_bar_status",
                        "price_window_status",
                        "symbol",
                        "core_field",
                        "core_quality",
                    },
                )
                required = {
                    "target_session",
                    "source_set_id",
                    "package_sha256",
                    "profile_revision",
                }
                if not required.issubset(values):
                    raise PreviewHttpError(
                        400,
                        "PROFILE_QUERY_REQUIRED",
                        "画像查询必须显式绑定日期、source_set、包SHA和画像修订",
                    )
                target = _iso_date(values["target_session"])
                _require_research_cutoff(
                    target, self.preview_server.state.research_cutoff
                )
                package_sha = _sha(values["package_sha256"])
                offset = _nonnegative_int(values.get("offset", "0"), "offset")
                limit = _positive_int(
                    _nonnegative_int(values.get("limit", "100"), "limit", 100) or 0,
                    "limit",
                    100,
                )
                symbol = _symbol(values["symbol"]) if "symbol" in values else None
                result = query_source_symbol_price_profiles(
                    self.preview_server.state.config,
                    target_session=target,
                    max_target_session=self.preview_server.state.research_cutoff,
                    source_set_id=values["source_set_id"],
                    package_sha256=package_sha,
                    profile_revision=values["profile_revision"],
                    offset=offset,
                    limit=limit,
                    market=values.get("market", "ALL"),
                    board=values.get("board", "ALL"),
                    target_bar_status=values.get("target_bar_status", "ALL"),
                    price_window_status=values.get("price_window_status", "ALL"),
                    source_symbol=symbol,
                    core_field=values.get("core_field"),
                    core_quality=values.get("core_quality"),
                )
                if result["status"] == "NO_MATCH":
                    raise PreviewHttpError(
                        404,
                        "PROFILE_NO_MATCH",
                        "PG中没有满足这组精确来源版本与筛选条件的画像记录",
                    )
                result.update(
                    {
                        "ok": True,
                        "research_mode": "SOURCE_SYMBOL_NON_PIT_PRICE_PROFILE",
                        "database_write": False,
                        "ddl_executed": False,
                        "source_zip_opened": False,
                        "baostock_called": False,
                    }
                )
                self._send_json(200, result)
                return
            if parsed.path == "/api/preview/chain-readiness":
                values = _query(self.path, {"target_session", "package_sha256"})
                if set(values) != {"target_session", "package_sha256"}:
                    raise PreviewHttpError(
                        400,
                        "PREVIEW_QUERY_REQUIRED",
                        "数据链准备状态必须显式提供目标日期和完整来源包SHA",
                    )
                target = _iso_date(values["target_session"])
                _require_research_cutoff(
                    target, self.preview_server.state.research_cutoff
                )
                package_sha = _sha(values["package_sha256"])
                source_version = read_raw_daily_readiness(
                    self.preview_server.state.config,
                    target_session=target,
                    package_sha256=package_sha,
                )
                source_candidate = read_source_set_candidate(
                    self.preview_server.state.config,
                    target_session=target,
                    package_sha256=package_sha,
                )
                if source_version["status"] == "EMPTY":
                    raise PreviewHttpError(
                        404,
                        "PG_SOURCE_NO_MATCH",
                        "当前DEV namespace中没有该明确日期和来源包SHA的Raw来源版本",
                    )
                source_ingress_result = build_source_ingress_plan(
                    self.preview_server.state.config,
                    target_session=target,
                    package_sha256=package_sha,
                )
                checked_at = datetime.now().astimezone().isoformat(timespec="seconds")
                payload = build_chain_readiness_payload(
                    self.preview_server.state.profile_contract,
                    source_version,
                    checked_at=checked_at,
                    source_set_candidate=source_candidate,
                    source_ingress_result=source_ingress_result,
                )
                self._send_json(200, payload)
                return
            if parsed.path == "/api/preview/raw":
                values = _query(
                    self.path, {"target_session", "package_sha256", "symbol"}
                )
                required = {"target_session", "package_sha256", "symbol"}
                if set(values) != required:
                    raise PreviewHttpError(
                        400,
                        "PREVIEW_QUERY_REQUIRED",
                        "Raw查询必须显式提供日期、完整SHA和来源代码",
                    )
                target = _iso_date(values["target_session"])
                _require_research_cutoff(
                    target, self.preview_server.state.research_cutoff
                )
                package_sha = _sha(values["package_sha256"])
                symbol = _symbol(values["symbol"])
                row = read_raw_daily_stock(
                    self.preview_server.state.config,
                    source_symbol=symbol,
                    target_session=target,
                    package_sha256=package_sha,
                )
                if row is None:
                    raise PreviewHttpError(
                        404,
                        "PG_SOURCE_NO_MATCH",
                        "PG中没有该明确日期、来源包SHA与来源代码的Raw记录",
                    )
                source_context = read_sealed_source_context(
                    self.preview_server.state.config,
                    target_session=target,
                    package_sha256=package_sha,
                    namespace_id=str(row["namespace_id"]),
                    target_raw_rows=(row,),
                )
                raw = _raw_public_fields(row)
                identity = self.preview_server.state.snapshot_identity(
                    namespace_id=str(row["namespace_id"]),
                    target_session=target.isoformat(),
                    package_sha256=package_sha,
                    source_set_id=source_context["source_set_id"],
                )
                self._send_json(
                    200,
                    {
                        "ok": True,
                        "snapshot_identity": identity,
                        "raw": raw,
                        "formal_source_set": source_context,
                        "database_write": False,
                    },
                )
                return
            if parsed.path == "/api/preview/source-page":
                values = _query(
                    self.path,
                    {
                        "target_session",
                        "package_sha256",
                        "offset",
                        "limit",
                        "market",
                        "board",
                    },
                )
                if not {"target_session", "package_sha256"}.issubset(values):
                    raise PreviewHttpError(
                        400,
                        "PREVIEW_QUERY_REQUIRED",
                        "候选分页必须显式提供目标日期和来源包SHA",
                    )
                target = _iso_date(values["target_session"])
                _require_research_cutoff(
                    target, self.preview_server.state.research_cutoff
                )
                package_sha = _sha(values["package_sha256"])
                offset = _nonnegative_int(values.get("offset", "0"), "offset")
                limit = _nonnegative_int(
                    values.get("limit", "20"), "limit", _MAX_PAGE_SIZE
                )
                if limit < 1:
                    raise PreviewHttpError(
                        400, "PREVIEW_PARAMETER_INVALID", "limit必须在1至50之间"
                    )
                market = values.get("market", "ALL")
                board = values.get("board", "ALL")
                if market not in {"ALL", "SH", "SZ"} or board not in {
                    "ALL",
                    "MAIN_BOARD",
                    "GEM",
                }:
                    raise PreviewHttpError(
                        400, "PREVIEW_FILTER_INVALID", "市场或板别筛选无效"
                    )
                page = read_raw_daily_page(
                    self.preview_server.state.config,
                    target_session=target,
                    package_sha256=package_sha,
                    offset=offset,
                    limit=limit,
                    market=market,
                    board=board,
                )
                if page["version_status"] == "EMPTY":
                    raise PreviewHttpError(
                        404,
                        "PG_SOURCE_NO_MATCH",
                        "PG中没有该明确日期和来源包SHA的Raw来源版本",
                    )
                source_context = read_sealed_source_context(
                    self.preview_server.state.config,
                    target_session=target,
                    package_sha256=package_sha,
                    namespace_id=page["namespace_id"],
                    target_raw_rows=page["rows"],
                    expected_raw_row_count=page["version_row_count"],
                )
                namespace_id = page["namespace_id"]
                identity = self.preview_server.state.snapshot_identity(
                    namespace_id=namespace_id,
                    target_session=target.isoformat(),
                    package_sha256=package_sha,
                    source_set_id=source_context["source_set_id"],
                )
                self._send_json(
                    200,
                    {
                        "ok": True,
                        "snapshot_identity": identity,
                        "page": _page_public_fields(page),
                        "formal_source_set": source_context,
                    },
                )
                return
            if parsed.path == "/api/preview/stock":
                raise PreviewHttpError(
                    405,
                    "PREVIEW_METHOD_NOT_ALLOWED",
                    "单股Core/Profile计算只接受用户明确提交的POST",
                )
            raise PreviewHttpError(404, "PREVIEW_ROUTE_NOT_FOUND", "请求路径不存在")
        except Exception as error:
            self._handle_error(error)

    def do_POST(self) -> None:
        if not self._ensure_local_host():
            return
        if self.path.split("?", 1)[0] != "/api/preview/stock":
            self._error(
                PreviewHttpError(
                    404,
                    "PREVIEW_ROUTE_NOT_FOUND",
                    "POST仅用于用户明确触发的单股研究预览",
                )
            )
            return
        origin = self.headers.get("Origin")
        if origin is not None and origin != f"http://{_HOST}:{_PORT}":
            self._error(
                PreviewHttpError(
                    403, "PREVIEW_ORIGIN_REJECTED", "请求来源不属于本机预览页面"
                )
            )
            return
        if self.headers.get_content_type() != "application/json":
            self._error(
                PreviewHttpError(
                    415,
                    "PREVIEW_CONTENT_TYPE_INVALID",
                    "预览请求必须使用application/json",
                )
            )
            return
        length_text = self.headers.get("Content-Length")
        if length_text is None or not length_text.isdecimal():
            self._error(
                PreviewHttpError(
                    411, "PREVIEW_CONTENT_LENGTH_REQUIRED", "必须提供有效Content-Length"
                )
            )
            return
        length = int(length_text)
        if length > _MAX_BODY_BYTES:
            self.close_connection = True
            self._error(
                PreviewHttpError(
                    413, "PREVIEW_BODY_TOO_LARGE", "预览JSON请求体最多8192字节"
                )
            )
            return
        if length == 0:
            self._error(
                PreviewHttpError(400, "PREVIEW_BODY_EMPTY", "预览请求体不能为空")
            )
            return
        try:
            raw_body = self.rfile.read(length)
            if len(raw_body) != length:
                raise PreviewHttpError(
                    400, "PREVIEW_BODY_TRUNCATED", "预览请求体未完整到达"
                )
            try:
                body = json.loads(
                    raw_body.decode("utf-8", errors="strict"),
                    object_pairs_hook=_duplicate_free_object,
                    parse_constant=_reject_constant,
                )
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
                raise PreviewHttpError(
                    400, "PREVIEW_JSON_INVALID", "请求体不是有效UTF-8 JSON或含重复字段"
                ) from error
            expected = {"symbol", "target_session", "package_sha256", "session_count"}
            if not isinstance(body, dict) or set(body) != expected:
                raise PreviewHttpError(
                    400,
                    "PREVIEW_BODY_FIELDS_INVALID",
                    "请求体必须且只能包含symbol、target_session、package_sha256、session_count",
                )
            symbol = _symbol(body["symbol"])
            target_text = body["target_session"]
            target = _iso_date(target_text)
            _require_research_cutoff(target, self.preview_server.state.research_cutoff)
            package_sha = _sha(body["package_sha256"])
            session_count = _positive_int(body["session_count"], "session_count", 75)
            resolved_target, sessions, calendar_revision = resolve_pg_history_window(
                target.isoformat(), session_count
            )
            if resolved_target != target:
                raise PreviewHttpError(
                    400, "PREVIEW_DATE_INVALID", "目标日期未通过官方交易日历校验"
                )
            history = read_raw_daily_history_stock(
                self.preview_server.state.config,
                source_symbol=symbol,
                target_session=target,
                package_sha256=package_sha,
                sessions=sessions,
            )
            source_context = read_sealed_source_context(
                self.preview_server.state.config,
                target_session=target,
                package_sha256=package_sha,
                namespace_id=history["namespace_id"],
                target_raw_rows=history["history_rows"],
            )
            result = research_entry.research_stock_pg_history_preview(
                symbol=symbol,
                target_session=target.isoformat(),
                package_sha256=package_sha,
                session_dates=sessions,
                history_rows=history["history_rows"],
                missing_history_sessions=tuple(
                    date.fromisoformat(item)
                    for item in history["missing_history_sessions"]
                ),
                namespace_id=history["namespace_id"],
                profile_contract=self.preview_server.state.profile_contract,
            )
            identity = self.preview_server.state.snapshot_identity(
                namespace_id=history["namespace_id"],
                target_session=target.isoformat(),
                package_sha256=package_sha,
                session_count=session_count,
                session_start=sessions[0].isoformat(),
                calendar_revision=calendar_revision,
                source_set_id=source_context["source_set_id"],
            )
            result["snapshot_identity"] = identity
            result["formal_source_set"] = source_context
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


def run_preview_server(
    *, host: str = _HOST, port: int = _PORT, research_cutoff: date
) -> int:
    """仅以前台显式运行方式启动127.0.0.1:28766。"""
    if host != _HOST or type(port) is not int or port != _PORT:
        print("拒绝启动：本机研究预览只允许绑定127.0.0.1:28766。", file=sys.stderr)
        return _EXIT_ARGUMENT
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.settimeout(0.2)
        try:
            if probe.connect_ex((_HOST, _PORT)) == 0:
                print(
                    "拒绝启动：127.0.0.1:28766已有监听者；不会终止或接管任何进程。",
                    file=sys.stderr,
                )
                return _EXIT_UNAVAILABLE
        except OSError:
            pass
    try:
        state = _PreviewState(research_cutoff=research_cutoff)
        server = _BoundedHTTPServer((_HOST, _PORT), state)
    except (OSError, DiagnosticError, DayChainContractError) as error:
        print(f"预览服务未启动：{type(error).__name__}。", file=sys.stderr)
        return _EXIT_UNAVAILABLE
    print(
        "Stock A 本机来源级研究预览已启动：http://127.0.0.1:28766/（只读PG；Ctrl+C安全停止）"
    )
    try:
        server.serve_forever(poll_interval=0.25)
    except KeyboardInterrupt:
        print("正在停止本机研究预览服务。")
    finally:
        server.shutdown()
        server.server_close()
    return _EXIT_OK
