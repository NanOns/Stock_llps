"""本机只读PG来源级研究预览HTTP服务。"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
import json
import re
import socket
import sys
import threading
import time
import traceback
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import parse_qs, urlsplit
from uuid import UUID, uuid4

from stock_a.application.snapshot_reuse import (
    SnapshotChanged,
    SnapshotReuse,
    SnapshotWitnessUnavailable,
)

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
    bind_source_context_to_raw_rows,
    read_sealed_source_context,
)
from stock_a.application.pg_price_profiles import (
    PgPriceProfileError,
    read_source_symbol_price_profile_observation,
    query_source_symbol_price_profile_breadth,
    query_source_symbol_price_profile_breadth_members,
    query_source_symbol_price_profile_pair,
    query_source_symbol_price_profiles,
    query_source_symbol_price_profile_changes,
    query_source_symbol_price_profile_relation_screen,
    source_symbol_price_profile_snapshots,
)
from stock_a.application.tdx_sector_members import (
    TdxSectorMembersApplicationError,
    read_latest_tdx_sector_snapshot_observation,
)
from stock_a.application.source_stock_change_context import (
    query_source_stock_change_context,
)
from stock_a.application.source_d0_raw import (
    SourceD0RawError,
    query_source_d0_raw_launch,
)
from stock_a.application.source_tdx_sector_membership import (
    read_tdx_sector_symbol_membership,
)
from stock_a.application.source_ingress import (
    SourceIngressError,
    build_source_ingress_plan,
)
from stock_a.application.source_set_candidate import (
    SourceSetCandidateApplicationError,
    _official_calendar,
)
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
from stock_a.research import raw_periods as research_raw_periods
from stock_a.research.raw_periods import (
    build_source_symbol_raw_periods,
    evaluate_raw_period_trend,
    raw_period_payload,
    summarize_raw_history_rows,
)


from stock_a.service.readiness import build_chain_readiness_payload
from stock_a.service.serialization import _json_value, canonical_sha
from stock_a.service.static_assets import read_static_asset, static_asset_routes

_HOST = "127.0.0.1"
_PORT = 28766
_MAX_BODY_BYTES = 8192
_MAX_PAGE_SIZE = 50
_MAX_OFFSET = 2**31 - 1
_SYMBOL = re.compile(r"^(SH|SZ)\.[0-9]{6}$")
_SYMBOL_SEARCH = re.compile(r"^[A-Za-z0-9.]{1,9}$")
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


def _source_context_preview_error(error: Exception) -> PreviewHttpError | None:
    """保留来源上下文读者的稳定错误码，并映射为可解释的HTTP状态。"""
    code = getattr(error, "code", None)
    if not isinstance(code, str) or not code:
        return None
    category = getattr(error, "category", None)
    request_errors = {
        "SOURCE_SYMBOL_INVALID",
        "SECTOR_SNAPSHOT_UUID_INVALID",
        "SECTOR_SNAPSHOT_IDS_NOT_DISTINCT",
        "DEV_NAMESPACE_INVALID",
        "P04_NAMESPACE_ID_INVALID",
        "P04_SOURCE_SET_ID_INVALID",
    }
    if code in {
        "PG_RESEARCH_P04_SOURCE_SET_UNAVAILABLE",
        "PG_RESEARCH_P04_SOURCE_SET_INVALID",
    }:
        status = 409
    elif code.startswith("PG_"):
        status = 503
    elif category == "ARGUMENT" or code in request_errors:
        status = 400
    elif code.endswith("NOT_FOUND") or code.endswith("NOT_IN_PAIR"):
        status = 404
    elif any(
        marker in code
        for marker in (
            "MISMATCH",
            "NOT_DISTINCT",
            "KIND_MISMATCH",
            "MODE_REJECTED",
            "QUALITY_INVALID",
            "INCOMPLETE",
            "RESULT_INVALID",
            "SNAPSHOT_INVALID",
        )
    ):
        status = 409
    elif code.endswith("READ_FAILED"):
        status = 503
    else:
        status = 503
    return PreviewHttpError(status, code, str(error))


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


def _symbol_search(value: Any) -> str:
    if not isinstance(value, str) or _SYMBOL_SEARCH.fullmatch(value) is None:
        raise PreviewHttpError(
            400,
            "PREVIEW_SYMBOL_SEARCH_INVALID",
            "来源代码搜索只接受1至9位字母、数字和点号",
        )
    return value.upper()


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


def _ratio_decimal(value: str | None, field: str) -> Decimal | None:
    if value is None:
        return None
    if not value or len(value) > 128:
        raise PreviewHttpError(
            400, "PREVIEW_RATIO_INVALID", f"{field}必须是非负有限Decimal"
        )
    try:
        parsed = Decimal(value)
    except InvalidOperation as error:
        raise PreviewHttpError(
            400, "PREVIEW_RATIO_INVALID", f"{field}必须是非负有限Decimal"
        ) from error
    if not parsed.is_finite() or parsed < 0:
        raise PreviewHttpError(
            400, "PREVIEW_RATIO_INVALID", f"{field}必须是非负有限Decimal"
        )
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


def _preview_source_context(
    config: Mapping[str, Any],
    *,
    target_session: date,
    package_sha256: str,
    namespace_id: str,
    target_raw_rows: Sequence[Mapping[str, Any]],
    expected_raw_row_count: int | None = None,
) -> dict[str, Any]:
    """P03存在时精确绑定；不存在时只披露Raw查询身份并保留正式状态未知。"""
    unavailable_code = None
    try:
        return read_sealed_source_context(
            config,
            target_session=target_session,
            package_sha256=package_sha256,
            namespace_id=namespace_id,
            target_raw_rows=target_raw_rows,
            expected_raw_row_count=expected_raw_row_count,
        )
    except PgResearchBindingError as error:
        if error.code != "PG_RESEARCH_P04_SOURCE_SET_UNAVAILABLE":
            raise
        unavailable_code = error.code
    target_rows = [
        row
        for row in target_raw_rows
        if row.get("session_date") == target_session
        or row.get("session_date") == target_session.isoformat()
    ]
    if expected_raw_row_count is None and not target_rows:
        raise PgResearchBindingError(
            "PG_RESEARCH_TARGET_RAW_UNAVAILABLE",
            "缺少精确目标日Raw行；不能创建来源代码研究上下文。",
        )
    if any(
        row.get("package_sha256") != package_sha256
        or (
            row.get("namespace_id") is not None
            and str(row.get("namespace_id")) != namespace_id
        )
        for row in target_rows
    ):
        raise PgResearchBindingError(
            "PG_RESEARCH_RAW_IDENTITY_MISMATCH",
            "目标日Raw行与显式namespace或包SHA不匹配。",
        )
    return {
        "status": "UNKNOWN",
        "target_session": target_session.isoformat(),
        "package_sha256": package_sha256,
        "namespace_id": namespace_id,
        "source_set_id": None,
        "revision_id": None,
        "seal_status": "UNKNOWN_NO_P03_SOURCE_SET",
        "durability_status": "UNKNOWN_NO_P04_READBACK",
        "formal_source_set_status": "UNKNOWN",
        "formal_source_set_reason": unavailable_code,
        "source_content_id": None,
        "source_capture_id": None,
        "source_digest": None,
        "source_raw_digest": None,
        "raw_revalidation_proof_level": "UNKNOWN",
        "target_raw_binding_count": len(target_rows),
        "target_raw_binding_status": "EXACT_PG_RAW_QUERY_WITHOUT_P03_BINDING",
        "database_write": False,
        "ddl_executed": False,
        "candidate_fallback": False,
    }


class _PreviewState:
    def __init__(self, *, research_cutoff: date) -> None:
        if type(research_cutoff) is not date:
            raise DiagnosticError("RESEARCH_CUTOFF_REQUIRED", "必须显式绑定研究截止日")
        self.research_cutoff = research_cutoff
        self.config, self.config_path = load_runtime_config()
        self.source_watch_reuse = SnapshotReuse(self.config)
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
        self.entry_source_sha256 = research_entry.implementation_source_sha256()
        self.raw_period_source_sha256 = hashlib.sha256(
            Path(research_raw_periods.__file__).read_bytes()
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

    def server_close(self) -> None:
        try:
            super().server_close()
        finally:
            reuse = getattr(self.state, "source_watch_reuse", None)
            if reuse is not None:
                reuse.close()

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
        payload = {"ok": False, "code": error.code, "message": error.message}
        request_context = getattr(self, "_raw_period_request_context", None)
        if request_context is not None:
            payload["request_context"] = request_context
        self._send_json(error.status, payload)

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
            if error.code in {
                "PG_RESEARCH_P04_SOURCE_SET_UNAVAILABLE",
                "PG_RESEARCH_P04_SOURCE_SET_INVALID",
                "P04_NAMESPACE_MISMATCH",
                "P04_SOURCE_SET_KEY_MISMATCH",
            }:
                status = 409
            elif error.code in {
                "P04_NAMESPACE_ID_INVALID",
                "P04_SOURCE_SET_ID_INVALID",
            }:
                status = 400
            else:
                status = 503
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
                "PROFILE_CHANGE_SNAPSHOT_MISSING",
                "PROFILE_CHANGE_SNAPSHOT_AMBIGUOUS",
                "PROFILE_CHANGE_SNAPSHOT_INCOMPLETE",
            }:
                status = (
                    409 if error.code.startswith("PROFILE_CHANGE_SNAPSHOT_") else 404
                )
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
        # 仅记录调用位置，不记录异常文本、局部变量、SQL或请求体。
        frames = traceback.extract_tb(error.__traceback__)
        route = str(getattr(self, "path", "")).split("?", 1)[0]
        known_routes = {
            "/api/preview/catalog",
            "/api/preview/chain-readiness",
            "/api/preview/raw",
            "/api/preview/source-launch-watch",
            "/api/preview/source-page",
            "/api/preview/stock-timeline",
            "/api/preview/stock",
        }
        method = getattr(self, "command", "")
        diagnostic = {
            "time": datetime.now(timezone.utc).isoformat(),
            "request_id": uuid4().hex,
            "method": method if method in {"GET", "HEAD", "POST"} else "UNKNOWN",
            "route": route if route in known_routes else "UNKNOWN_ROUTE",
            "exception_type": type(error).__name__,
            "stack": [
                {
                    "file": Path(frame.filename).name,
                    "line": frame.lineno,
                    "function": frame.name,
                }
                for frame in frames
            ],
        }
        print(json.dumps(diagnostic, ensure_ascii=True), file=sys.stderr, flush=True)
        self._error(
            PreviewHttpError(
                500, "PREVIEW_INTERNAL_ERROR", "预览请求未完成；请检查本机服务输出"
            )
        )

    def _source_launch_watch(self, body: Any) -> dict[str, Any]:
        """读取完整D0新增集合和板块归属后，再按请求分页。"""
        request_started = time.perf_counter()
        request_cpu_started = time.process_time()
        required = {
            "before_session",
            "target_session",
            "research_cutoff",
            "before_source_set_id",
            "source_set_id",
            "package_sha256",
            "namespace_id",
            "industry_snapshot_id",
            "concept_snapshot_id",
            "offset",
            "limit",
        }
        board_fields = {"board_kind", "board_native_id"}
        if not isinstance(body, dict) or not required.issubset(body):
            raise PreviewHttpError(
                400,
                "SOURCE_LAUNCH_WATCH_FIELDS_INVALID",
                "请求必须显式绑定双日期、研究截止、来源版本、DEV命名空间、双板块快照和分页",
            )
        body_fields = frozenset(body)
        if body_fields not in {
            frozenset(required),
            frozenset(required | board_fields),
        }:
            raise PreviewHttpError(
                400,
                "SOURCE_LAUNCH_WATCH_FIELDS_INVALID",
                "请求含未登记字段，或板块筛选字段未成对提供",
            )

        before_session = _iso_date(body["before_session"], "before_session")
        target_session = _iso_date(body["target_session"], "target_session")
        research_cutoff = _iso_date(body["research_cutoff"], "research_cutoff")
        if research_cutoff > self.preview_server.state.research_cutoff:
            raise PreviewHttpError(
                400,
                "RESEARCH_CUTOFF_EXCEEDED",
                "请求研究截止日不能晚于服务已绑定的研究截止日",
            )
        _require_research_cutoff(target_session, research_cutoff)
        _require_research_cutoff(before_session, research_cutoff)
        if before_session >= target_session:
            raise PreviewHttpError(
                400,
                "SOURCE_LAUNCH_WATCH_DATE_INVALID",
                "before_session必须早于target_session",
            )

        try:
            source_set_before = str(UUID(body["before_source_set_id"]))
            source_set_after = str(UUID(body["source_set_id"]))
            namespace_id = str(UUID(body["namespace_id"]))
            industry_snapshot_id = str(UUID(body["industry_snapshot_id"]))
            concept_snapshot_id = str(UUID(body["concept_snapshot_id"]))
        except (ValueError, TypeError, AttributeError) as error:
            raise PreviewHttpError(
                400,
                "SOURCE_LAUNCH_WATCH_ID_INVALID",
                "source_set、namespace和板块快照须使用有效UUID",
            ) from error
        if source_set_before == source_set_after:
            raise PreviewHttpError(
                400,
                "SOURCE_LAUNCH_WATCH_SOURCE_SETS_NOT_DISTINCT",
                "前后会话必须绑定不同source_set",
            )
        if industry_snapshot_id == concept_snapshot_id:
            raise PreviewHttpError(
                400,
                "SOURCE_LAUNCH_WATCH_SNAPSHOTS_NOT_DISTINCT",
                "行业和GN概念必须绑定各自不同的TDX快照",
            )
        package_sha256 = _sha(body["package_sha256"])
        offset = body["offset"]
        if type(offset) is not int or not 0 <= offset <= _MAX_OFFSET:
            raise PreviewHttpError(
                400,
                "PREVIEW_PARAMETER_INVALID",
                "offset必须为0至允许上限之间的整数",
            )
        limit = _positive_int(body["limit"], "limit", 30)

        board_kind: str | None = None
        board_native_id: str | None = None
        if board_fields <= set(body):
            board_kind = body["board_kind"]
            board_native_id = body["board_native_id"]
            if not isinstance(board_kind, str) or board_kind not in {
                "INDUSTRY",
                "CONCEPT",
            }:
                raise PreviewHttpError(
                    400,
                    "SOURCE_LAUNCH_WATCH_BOARD_KIND_INVALID",
                    "board_kind只允许INDUSTRY或CONCEPT",
                )
            if (
                not isinstance(board_native_id, str)
                or not board_native_id.strip()
                or len(board_native_id) > 64
                or board_native_id != board_native_id.strip()
            ):
                raise PreviewHttpError(
                    400,
                    "SOURCE_LAUNCH_WATCH_BOARD_ID_INVALID",
                    "board_native_id必须是非空且不含首尾空格的精确板块原生ID",
                )

        # 交易日相邻性由本地正式日历先验验证，未来/错日请求不会触达PG行情读者。
        resolved_target, pair_sessions, calendar_revision = resolve_pg_history_window(
            target_session.isoformat(), 2
        )
        if (
            resolved_target != target_session
            or len(pair_sessions) != 2
            or pair_sessions[0] != before_session
            or pair_sessions[1] != target_session
        ):
            raise PreviewHttpError(
                409,
                "SOURCE_LAUNCH_WATCH_SESSIONS_NOT_ADJACENT",
                "前后日期必须是同一现役日历版本中的严格相邻官方会话",
            )

        from stock_a.application.source_d0_tdx_membership import (
            build_d0_launch_watch_membership_result,
            read_tdx_sector_memberships_for_symbols,
        )

        def build_full_watch():
            try:
                d0_started = time.perf_counter()
                changes = query_source_d0_raw_launch(
                    self.preview_server.state.config,
                    scenario="LAUNCH_CONFIRM",
                    action="changes",
                    before_session=before_session,
                    before_source_set_id=source_set_before,
                    before_package_sha256=package_sha256,
                    target_session=target_session,
                    research_cutoff=research_cutoff,
                    source_set_id=source_set_after,
                    package_sha256=package_sha256,
                    transition="ALL",
                    offset=0,
                    limit=10000,
                    expected_namespace_id=namespace_id,
                )
                d0_reader_wall_seconds = time.perf_counter() - d0_started
            except SourceD0RawError as error:
                mapped = _source_context_preview_error(error)
                if mapped is None:
                    raise
                raise mapped from error

            all_change_rows = changes.get("results")
            selection = changes.get("selection")
            transition_counts = changes.get("transition_counts")
            source_key_set = changes.get("source_key_set")
            if (
                not isinstance(all_change_rows, list)
                or not isinstance(selection, Mapping)
                or not isinstance(transition_counts, Mapping)
                or not isinstance(source_key_set, Mapping)
                or selection.get("transition") != "ALL"
                or len(all_change_rows) != source_key_set.get("source_union_count")
                or selection.get("matched_count") != len(all_change_rows)
                or selection.get("next_offset") is not None
                or selection.get("evaluated_before_filter_and_pagination") is not True
                or source_key_set.get("union_partition_closed") is not True
                or sum(transition_counts.values())
                != source_key_set.get("comparable_intersection_count")
            ):
                raise PreviewHttpError(
                    409,
                    "SOURCE_LAUNCH_WATCH_D0_RESULT_INCOMPLETE",
                    "D0完整来源并集、九态转移及动态分母未闭合，已拒绝返回部分名单",
                )
            ft_rows = [
                row
                for row in all_change_rows
                if isinstance(row, Mapping) and row.get("transition") == "FALSE_TO_TRUE"
            ]
            if len(ft_rows) != transition_counts.get("FALSE_TO_TRUE"):
                raise PreviewHttpError(
                    409,
                    "SOURCE_LAUNCH_WATCH_FT_PARTITION_OPEN",
                    "FALSE_TO_TRUE集合未闭合到完整九态转移计数",
                )
            if (
                changes.get("before", {}).get("source_set", {}).get("source_set_id")
                != source_set_before
                or changes.get("after", {}).get("source_set", {}).get("source_set_id")
                != source_set_after
                or changes.get("before", {}).get("source_set", {}).get("package_sha256")
                != package_sha256
                or changes.get("after", {}).get("source_set", {}).get("package_sha256")
                != package_sha256
            ):
                raise PreviewHttpError(
                    409,
                    "SOURCE_LAUNCH_WATCH_SOURCE_IDENTITY_MISMATCH",
                    "D0实际source_set或package与请求不一致",
                )
            if (
                changes.get("before", {}).get("namespace_id") != namespace_id
                or changes.get("after", {}).get("namespace_id") != namespace_id
            ):
                raise PreviewHttpError(
                    409,
                    "SOURCE_LAUNCH_WATCH_NAMESPACE_MISMATCH",
                    "D0读者绑定的DEV namespace与请求不一致",
                )

            source_symbols = [row["source_symbol"] for row in ft_rows]
            try:
                membership_started = time.perf_counter()
                membership_read = read_tdx_sector_memberships_for_symbols(
                    self.preview_server.state.config,
                    source_symbols=source_symbols,
                    target_session=target_session,
                    industry_snapshot_id=industry_snapshot_id,
                    concept_snapshot_id=concept_snapshot_id,
                    expected_namespace_id=namespace_id,
                )
                membership_read_wall_seconds = time.perf_counter() - membership_started
                aggregation_started = time.perf_counter()
                complete = build_d0_launch_watch_membership_result(
                    changes, membership_read
                )
                aggregation_wall_seconds = time.perf_counter() - aggregation_started
            except Exception as error:
                mapped = _source_context_preview_error(error)
                if mapped is None:
                    raise
                raise mapped from error

            stocks = complete.get("stocks")
            boards = complete.get("boards")
            if (
                not isinstance(stocks, list)
                or not isinstance(boards, list)
                or complete.get("global_new_stock_count") != len(stocks)
                or len(stocks) != len(ft_rows)
                or complete.get("namespace_id") != namespace_id
                or complete.get("package_sha256") != package_sha256
                or complete.get("target_session") != target_session.isoformat()
            ):
                raise PreviewHttpError(
                    409,
                    "SOURCE_LAUNCH_WATCH_MEMBERSHIP_RESULT_INCOMPLETE",
                    "板块关系联结没有闭合到完整D0新增来源股票集合",
                )
            ordered_symbols = [stock.get("source_symbol") for stock in stocks]
            if (
                any(not isinstance(symbol, str) for symbol in ordered_symbols)
                or ordered_symbols != sorted(ordered_symbols)
                or len(set(ordered_symbols)) != len(ordered_symbols)
            ):
                raise PreviewHttpError(
                    409,
                    "SOURCE_LAUNCH_WATCH_STOCK_ORDER_INVALID",
                    "完整D0新增来源股票列表必须按唯一source_symbol升序闭合",
                )
            membership_snapshots = complete.get("membership_snapshots")
            if not isinstance(membership_snapshots, list) or {
                item.get("snapshot_id")
                for item in membership_snapshots
                if isinstance(item, Mapping)
            } != {industry_snapshot_id, concept_snapshot_id}:
                raise PreviewHttpError(
                    409,
                    "SOURCE_LAUNCH_WATCH_SNAPSHOT_PAIR_INVALID",
                    "板块归属必须返回请求绑定的行业与GN概念双快照",
                )
            return (
                {key: value for key, value in changes.items() if key != "results"},
                complete,
                d0_reader_wall_seconds,
                membership_read_wall_seconds,
                aggregation_wall_seconds,
            )

        actual_calendar = _official_calendar(target_session)
        calendar_identity = {
            key: actual_calendar[key]
            for key in ("calendar_id", "calendar_revision", "calendar_sha256")
        }
        reuse_key = (
            namespace_id,
            before_session,
            target_session,
            research_cutoff,
            source_set_before,
            source_set_after,
            package_sha256,
            industry_snapshot_id,
            concept_snapshot_id,
            calendar_revision,
            tuple(sorted(calendar_identity.items())),
        )

        def resolved_watch_identity(result: Any) -> str:
            changes, complete = result[:2]
            endpoints = []
            expected = (
                ("before", before_session, source_set_before),
                ("after", target_session, source_set_after),
            )
            for side, session, expected_set in expected:
                endpoint = changes[side]
                source = endpoint["source_set"]
                calendar = endpoint["official_calendar"]
                if (
                    endpoint.get("target_session") != session.isoformat()
                    or endpoint.get("namespace_id") != namespace_id
                    or source.get("source_set_id") != expected_set
                    or source.get("package_sha256") != package_sha256
                    or type(source.get("revision_id")) is not int
                    or source["revision_id"] < 1
                    or source.get("seal_status") != "SEALED"
                    or endpoint.get("session_contract") != "D0_SESSION_R2"
                    or not endpoint.get("algorithm_contract")
                    or calendar != calendar_identity
                    or not re.fullmatch(
                        r"[0-9a-f]{64}", str(calendar.get("calendar_sha256", ""))
                    )
                    or source.get("calendar_id") != calendar.get("calendar_id")
                    or source.get("calendar_revision")
                    != calendar.get("calendar_revision")
                    or source.get("calendar_sha256") != calendar.get("calendar_sha256")
                    or not source.get("source_content_id")
                    or not source.get("source_capture_id")
                    or not source.get("parser_contract")
                    or not re.fullmatch(
                        r"[0-9a-f]{64}", str(source.get("source_raw_digest", ""))
                    )
                    or not re.fullmatch(
                        r"[0-9a-f]{64}", str(source.get("source_digest", ""))
                    )
                ):
                    raise ValueError("来源观察缓存缺少完整的端点版本身份")
                endpoints.append(
                    {
                        "source": source,
                        "calendar": calendar,
                        "session_contract": endpoint["session_contract"],
                        "algorithm_contract": endpoint["algorithm_contract"],
                    }
                )
            snapshots = complete["membership_snapshots"]
            if len(snapshots) != 2 or {
                (item.get("snapshot_id"), item.get("board_kind")) for item in snapshots
            } != {(industry_snapshot_id, "INDUSTRY"), (concept_snapshot_id, "CONCEPT")}:
                raise ValueError("来源观察缓存缺少两类精确板块快照")
            if any(
                not re.fullmatch(
                    r"[0-9a-f]{64}", str(item.get("source_file_sha256", ""))
                )
                for item in snapshots
            ):
                raise ValueError("来源观察缓存缺少板块来源摘要")
            if not changes.get("contract") or not changes.get("producer_version"):
                raise ValueError("来源观察缓存缺少D0合同或生产版本")
            return canonical_sha(
                {
                    "endpoints": endpoints,
                    "snapshots": snapshots,
                    "contract": changes["contract"],
                    "producer_version": changes["producer_version"],
                }
            )

        try:
            (
                (
                    changes,
                    complete,
                    d0_reader_wall_seconds,
                    membership_read_wall_seconds,
                    aggregation_wall_seconds,
                ),
                cache_hit,
            ) = self.preview_server.state.source_watch_reuse.get_or_build(
                reuse_key, namespace_id, build_full_watch, resolved_watch_identity
            )
        except SnapshotChanged as error:
            raise PreviewHttpError(
                409,
                "SOURCE_LAUNCH_WATCH_SNAPSHOT_CHANGED",
                "来源观察期间数据库版本发生变化，请重新请求",
            ) from error
        except SnapshotWitnessUnavailable as error:
            raise PreviewHttpError(
                503,
                "SOURCE_LAUNCH_WATCH_WITNESS_UNAVAILABLE",
                "来源观察无法校验数据库快照，请稍后重试",
            ) from error
        transition_counts = changes["transition_counts"]
        stocks = complete["stocks"]
        boards = complete["boards"]
        if cache_hit:
            d0_reader_wall_seconds = 0.0
            membership_read_wall_seconds = 0.0
            aggregation_wall_seconds = 0.0
            for endpoint in ("before", "after"):
                changes[endpoint]["execution"] = None
        filtered = stocks
        if board_kind is not None and board_native_id is not None:
            filtered = [
                stock
                for stock in stocks
                if any(
                    membership.get("board_kind") == board_kind
                    and membership.get("board_native_id") == board_native_id
                    and membership.get("snapshot_id")
                    == (
                        industry_snapshot_id
                        if board_kind == "INDUSTRY"
                        else concept_snapshot_id
                    )
                    for membership in stock.get("also_in_sectors", ())
                )
            ]
        filtered_total = len(filtered)
        page = filtered[offset : offset + limit]
        for stock in page:
            stock["detail_request"] = {
                "method": "POST",
                "path": "/api/preview/stock-timeline",
                "body": {
                    "symbol": stock["source_symbol"],
                    "target_session": target_session.isoformat(),
                    "package_sha256": package_sha256,
                    "session_count": 75,
                    "source_set_id": source_set_after,
                    "namespace_id": namespace_id,
                    "include_source_change_context": True,
                    "research_cutoff": research_cutoff.isoformat(),
                    "before_session": before_session.isoformat(),
                    "before_source_set_id": source_set_before,
                    "before_package_sha256": package_sha256,
                    "industry_snapshot_id": industry_snapshot_id,
                    "concept_snapshot_id": concept_snapshot_id,
                },
            }

        return {
            "ok": True,
            "contract": "STOCK_A_SOURCE_LAUNCH_WATCH_API_R1",
            "request_identity": {
                "before_session": before_session.isoformat(),
                "target_session": target_session.isoformat(),
                "research_cutoff": research_cutoff.isoformat(),
                "before_source_set_id": source_set_before,
                "source_set_id": source_set_after,
                "package_sha256": package_sha256,
                "namespace_id": namespace_id,
                "industry_snapshot_id": industry_snapshot_id,
                "concept_snapshot_id": concept_snapshot_id,
                "calendar_revision": calendar_revision,
            },
            "source_change_status": {
                "identity_status": "SOURCE_SYMBOL_ONLY",
                "scenario": "LAUNCH_CONFIRM",
                "transition": "FALSE_TO_TRUE",
                "scenario_role": "DIAGNOSTIC_ONLY",
                "price_coordinate": "RAW_UNADJUSTED",
                "pit_status": "NON_PIT_RETROSPECTIVE",
                "formal_admission_status": changes.get("formal_admission_status"),
                "formal_status": "FORMAL_NOT_EVALUATED",
                "formal_evaluation_status": "NOT_EVALUATED",
                "seed_status": "NOT_EVALUATED",
                "research_universe_status": "UNKNOWN",
                "official_listing_universe_status": "UNKNOWN_NOT_CONNECTED",
                "priority_status": "NOT_EVALUATED",
                "d0_contract": changes.get("contract"),
                "d0_producer_version": changes.get("producer_version"),
                "endpoints": {
                    "before": changes.get("before"),
                    "after": changes.get("after"),
                },
                "transition_counts": transition_counts,
                "source_key_set": changes.get("source_key_set"),
            },
            "membership_status": {
                "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
                "snapshots": complete.get("membership_snapshots"),
                "global_new_stock_count": complete["global_new_stock_count"],
                "mapped_distinct_source_symbol_count": complete[
                    "mapped_distinct_source_symbol_count"
                ],
                "unmapped_distinct_source_symbol_count": complete[
                    "unmapped_distinct_source_symbol_count"
                ],
                "board_membership_relation_count": complete[
                    "board_membership_relation_count"
                ],
            },
            "filter": {
                "board_kind": board_kind,
                "board_native_id": board_native_id,
                "global_total": complete["global_new_stock_count"],
                "filtered_total": filtered_total,
            },
            "pagination": {
                "offset": offset,
                "limit": limit,
                "page_count": len(page),
                "next_offset": offset + len(page)
                if offset + len(page) < filtered_total
                else None,
            },
            "boards": boards,
            "results": page,
            "execution": {
                "d0_full_pool_computed_before_selection": True,
                "board_membership_read": (
                    "REUSED_COMPLETE_SNAPSHOT"
                    if cache_hit
                    else "BATCHED_TWO_SNAPSHOT_READ_ONLY"
                ),
                "cache_hit": cache_hit,
                "d0_reader_wall_seconds": d0_reader_wall_seconds,
                "d0_reader_execution": None if cache_hit else changes.get("execution"),
                "membership_read_wall_seconds": membership_read_wall_seconds,
                "membership_read_execution": None
                if cache_hit
                else complete.get("membership_read_execution"),
                "membership_aggregation_wall_seconds": aggregation_wall_seconds,
                "api_server_processing_wall_seconds": (
                    time.perf_counter() - request_started
                ),
                "api_server_process_cpu_seconds": (
                    time.process_time() - request_cpu_started
                ),
                "d1_full_universe_scan": False,
                "database_write": False,
                "ddl_executed": False,
                "dml_executed": False,
            },
        }

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
            asset_name = static_asset_routes().get(parsed.path)
            if asset_name is not None:
                if parsed.query:
                    message = (
                        "首页不接受查询参数"
                        if asset_name == "index.html"
                        else "静态资源不接受查询参数"
                    )
                    raise PreviewHttpError(400, "PREVIEW_QUERY_INVALID", message)
                body, content_type = read_static_asset(asset_name)
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
                        "namespace_id": snapshots["namespace_id"],
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
                        "before_source_set_id",
                        "before_package_sha256",
                        "before_profile_revision",
                        "after_source_set_id",
                        "after_package_sha256",
                        "after_profile_revision",
                        "offset",
                        "limit",
                        "symbol",
                        "change_type",
                        "price_damage_transition",
                        "ma_slope20_transition",
                        "profile_field_id",
                        "profile_change_kind",
                        "before_bias20_atr_state",
                        "after_bias20_atr_state",
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
                symbol = (
                    _symbol_search(values["symbol"]) if values.get("symbol") else None
                )
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
                    price_damage_transition=values.get(
                        "price_damage_transition", "ALL"
                    ),
                    ma_slope20_transition=values.get("ma_slope20_transition", "ALL"),
                    profile_field_id=values.get("profile_field_id"),
                    profile_change_kind=values.get("profile_change_kind"),
                    before_bias20_atr_state=values.get(
                        "before_bias20_atr_state", "ALL"
                    ),
                    after_bias20_atr_state=values.get("after_bias20_atr_state", "ALL"),
                    before_source_set_id=values.get("before_source_set_id"),
                    before_package_sha256=values.get("before_package_sha256"),
                    before_profile_revision=values.get("before_profile_revision"),
                    after_source_set_id=values.get("after_source_set_id"),
                    after_package_sha256=values.get("after_package_sha256"),
                    after_profile_revision=values.get("after_profile_revision"),
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
            if parsed.path == "/api/source-symbol-price-profile-relation-screen":
                values = _query(
                    self.path,
                    {
                        "before_session",
                        "after_session",
                        "before_source_set_id",
                        "before_package_sha256",
                        "before_profile_revision",
                        "after_source_set_id",
                        "after_package_sha256",
                        "after_profile_revision",
                        "relation_field",
                        "transition_type",
                        "ma20_direction",
                        "end_position_1_field",
                        "end_position_1_status",
                        "end_position_2_field",
                        "end_position_2_status",
                        "offset",
                        "limit",
                        "symbol",
                        "after_amount_ratio20_min",
                        "after_volume_ratio20_min",
                    },
                )
                required = {
                    "before_session",
                    "after_session",
                    "before_source_set_id",
                    "before_package_sha256",
                    "before_profile_revision",
                    "after_source_set_id",
                    "after_package_sha256",
                    "after_profile_revision",
                    "relation_field",
                    "transition_type",
                    "offset",
                    "limit",
                }
                if not required.issubset(values):
                    raise PreviewHttpError(
                        400,
                        "PROFILE_RELATION_QUERY_REQUIRED",
                        "RAW关系查询必须绑定前后完整画像身份、关系字段、转移类别和分页参数",
                    )
                before_session = _iso_date(values["before_session"], "before_session")
                after_session = _iso_date(values["after_session"], "after_session")
                _require_research_cutoff(
                    before_session, self.preview_server.state.research_cutoff
                )
                _require_research_cutoff(
                    after_session, self.preview_server.state.research_cutoff
                )
                limit = _positive_int(
                    _nonnegative_int(values["limit"], "limit", 100) or 0,
                    "limit",
                    100,
                )
                result = query_source_symbol_price_profile_relation_screen(
                    self.preview_server.state.config,
                    before_session=before_session,
                    after_session=after_session,
                    max_target_session=self.preview_server.state.research_cutoff,
                    offset=_nonnegative_int(values["offset"], "offset"),
                    limit=limit,
                    relation_field=values["relation_field"],
                    transition_type=values["transition_type"],
                    ma20_direction=values.get("ma20_direction", "ALL"),
                    after_amount_ratio20_min=values.get("after_amount_ratio20_min"),
                    after_volume_ratio20_min=values.get("after_volume_ratio20_min"),
                    end_position_1_field=values.get("end_position_1_field"),
                    end_position_1_status=values.get("end_position_1_status"),
                    end_position_2_field=values.get("end_position_2_field"),
                    end_position_2_status=values.get("end_position_2_status"),
                    source_symbol=(
                        _symbol_search(values["symbol"])
                        if values.get("symbol")
                        else None
                    ),
                    before_source_set_id=values["before_source_set_id"],
                    before_package_sha256=_sha(values["before_package_sha256"]),
                    before_profile_revision=_sha(values["before_profile_revision"]),
                    after_source_set_id=values["after_source_set_id"],
                    after_package_sha256=_sha(values["after_package_sha256"]),
                    after_profile_revision=_sha(values["after_profile_revision"]),
                )
                result.update(
                    {
                        "ok": True,
                        "database_write": False,
                        "ddl_executed": False,
                        "source_zip_opened": False,
                        "core_recalculated": False,
                    }
                )
                self._send_json(200, result)
                return
            if parsed.path == "/api/source-symbol-price-profile-breadth":
                values = _query(
                    self.path,
                    {
                        "before_session",
                        "after_session",
                        "before_source_set_id",
                        "before_package_sha256",
                        "before_profile_revision",
                        "after_source_set_id",
                        "after_package_sha256",
                        "after_profile_revision",
                    },
                )
                required = {
                    "before_session",
                    "after_session",
                    "before_source_set_id",
                    "before_package_sha256",
                    "before_profile_revision",
                    "after_source_set_id",
                    "after_package_sha256",
                    "after_profile_revision",
                }
                if set(values) != required:
                    raise PreviewHttpError(
                        400,
                        "PROFILE_BREADTH_QUERY_REQUIRED",
                        "来源池广度必须精确提供前后日期、source_set、包SHA和画像修订",
                    )
                before_session = _iso_date(values["before_session"], "before_session")
                after_session = _iso_date(values["after_session"], "after_session")
                _require_research_cutoff(
                    before_session, self.preview_server.state.research_cutoff
                )
                _require_research_cutoff(
                    after_session, self.preview_server.state.research_cutoff
                )
                result = query_source_symbol_price_profile_breadth(
                    self.preview_server.state.config,
                    before_session=before_session,
                    after_session=after_session,
                    max_target_session=self.preview_server.state.research_cutoff,
                    before_source_set_id=values["before_source_set_id"],
                    before_package_sha256=_sha(values["before_package_sha256"]),
                    before_profile_revision=_sha(values["before_profile_revision"]),
                    after_source_set_id=values["after_source_set_id"],
                    after_package_sha256=_sha(values["after_package_sha256"]),
                    after_profile_revision=_sha(values["after_profile_revision"]),
                )
                result.update(
                    {
                        "ok": True,
                        "research_mode": "SOURCE_SYMBOL_NON_PIT_PRICE_PROFILE_BREADTH",
                        "database_write": False,
                        "ddl_executed": False,
                        "source_zip_opened": False,
                        "core_recalculated": False,
                    }
                )
                self._send_json(200, result)
                return
            if parsed.path == "/api/source-symbol-price-profile-breadth-members":
                values = _query(
                    self.path,
                    {
                        "before_session",
                        "after_session",
                        "before_source_set_id",
                        "before_package_sha256",
                        "before_profile_revision",
                        "after_source_set_id",
                        "after_package_sha256",
                        "after_profile_revision",
                        "mode",
                        "side",
                        "relation",
                        "from",
                        "to",
                        "offset",
                        "limit",
                    },
                )
                base = {
                    "before_session",
                    "after_session",
                    "before_source_set_id",
                    "before_package_sha256",
                    "before_profile_revision",
                    "after_source_set_id",
                    "after_package_sha256",
                    "after_profile_revision",
                    "mode",
                }
                mode = values.get("mode")
                mode_parameters = {
                    "ENDPOINT": {"side", "relation"},
                    "TRANSITION": {"from", "to"},
                    "INCOMPARABLE": set(),
                }.get(mode)
                optional_page_parameters = {"offset", "limit"}
                if (
                    mode_parameters is None
                    or not base.issubset(values)
                    or set(values)
                    != base | mode_parameters | (set(values) & optional_page_parameters)
                    or set(values) - base - mode_parameters - optional_page_parameters
                ):
                    raise PreviewHttpError(
                        400,
                        "PROFILE_BREADTH_MEMBER_QUERY_INVALID",
                        "广度明细必须提供精确快照和与mode匹配的筛选参数",
                    )
                before_session = _iso_date(values["before_session"], "before_session")
                after_session = _iso_date(values["after_session"], "after_session")
                _require_research_cutoff(
                    before_session, self.preview_server.state.research_cutoff
                )
                _require_research_cutoff(
                    after_session, self.preview_server.state.research_cutoff
                )
                result = query_source_symbol_price_profile_breadth_members(
                    self.preview_server.state.config,
                    before_session=before_session,
                    after_session=after_session,
                    max_target_session=self.preview_server.state.research_cutoff,
                    before_source_set_id=values["before_source_set_id"],
                    before_package_sha256=_sha(values["before_package_sha256"]),
                    before_profile_revision=_sha(values["before_profile_revision"]),
                    after_source_set_id=values["after_source_set_id"],
                    after_package_sha256=_sha(values["after_package_sha256"]),
                    after_profile_revision=_sha(values["after_profile_revision"]),
                    mode=mode,
                    side=values.get("side"),
                    relation=values.get("relation"),
                    from_relation=values.get("from"),
                    to_relation=values.get("to"),
                    offset=_nonnegative_int(values.get("offset", "0"), "offset"),
                    limit=_positive_int(
                        _nonnegative_int(values.get("limit", "30"), "limit", 100) or 0,
                        "limit",
                        100,
                    ),
                )
                result.update(
                    {
                        "ok": True,
                        "research_mode": "SOURCE_SYMBOL_NON_PIT_PRICE_PROFILE_BREADTH_MEMBERS",
                        "database_write": False,
                        "ddl_executed": False,
                        "source_zip_opened": False,
                        "core_recalculated": False,
                    }
                )
                self._send_json(200, result)
                return
            if parsed.path == "/api/source-symbol-price-profile-pair":
                values = _query(
                    self.path,
                    {
                        "before_session",
                        "after_session",
                        "symbol",
                        "before_source_set_id",
                        "before_package_sha256",
                        "before_profile_revision",
                        "after_source_set_id",
                        "after_package_sha256",
                        "after_profile_revision",
                    },
                )
                required = {
                    "before_session",
                    "after_session",
                    "symbol",
                    "before_source_set_id",
                    "before_package_sha256",
                    "before_profile_revision",
                    "after_source_set_id",
                    "after_package_sha256",
                    "after_profile_revision",
                }
                if set(values) != required:
                    raise PreviewHttpError(
                        400,
                        "PROFILE_PAIR_QUERY_REQUIRED",
                        "双日代码对照必须精确提供前后日期、source_set、包SHA、画像修订和来源代码",
                    )
                before_session = _iso_date(values["before_session"], "before_session")
                after_session = _iso_date(values["after_session"], "after_session")
                _require_research_cutoff(
                    before_session, self.preview_server.state.research_cutoff
                )
                _require_research_cutoff(
                    after_session, self.preview_server.state.research_cutoff
                )
                result = query_source_symbol_price_profile_pair(
                    self.preview_server.state.config,
                    before_session=before_session,
                    after_session=after_session,
                    max_target_session=self.preview_server.state.research_cutoff,
                    source_symbol=_symbol(values["symbol"]),
                    before_source_set_id=values["before_source_set_id"],
                    before_package_sha256=_sha(values["before_package_sha256"]),
                    before_profile_revision=_sha(values["before_profile_revision"]),
                    after_source_set_id=values["after_source_set_id"],
                    after_package_sha256=_sha(values["after_package_sha256"]),
                    after_profile_revision=_sha(values["after_profile_revision"]),
                )
                result.update(
                    {
                        "ok": True,
                        "research_mode": "SOURCE_SYMBOL_NON_PIT_PRICE_PROFILE_PAIR",
                        "formal_listing_universe": "UNKNOWN_NOT_CONNECTED",
                        "database_write": False,
                        "ddl_executed": False,
                        "source_zip_opened": False,
                        "core_recalculated": False,
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
                        "price_damage_state",
                        "ma_slope20_sign",
                        "amount_ratio20_min",
                        "amount_ratio20_max",
                        "volume_ratio20_min",
                        "volume_ratio20_max",
                        "profile_field_id",
                        "profile_quality",
                        "profile_status",
                        "position_1_field",
                        "position_1_status",
                        "position_2_field",
                        "position_2_status",
                        "bias20_atr_band",
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
                    price_damage_state=values.get("price_damage_state", "ALL"),
                    ma_slope20_sign=values.get("ma_slope20_sign", "ALL"),
                    amount_ratio20_min=_ratio_decimal(
                        values.get("amount_ratio20_min"), "amount_ratio20_min"
                    ),
                    amount_ratio20_max=_ratio_decimal(
                        values.get("amount_ratio20_max"), "amount_ratio20_max"
                    ),
                    volume_ratio20_min=_ratio_decimal(
                        values.get("volume_ratio20_min"), "volume_ratio20_min"
                    ),
                    volume_ratio20_max=_ratio_decimal(
                        values.get("volume_ratio20_max"), "volume_ratio20_max"
                    ),
                    profile_field_id=values.get("profile_field_id"),
                    profile_quality=values.get("profile_quality"),
                    profile_status=values.get("profile_status"),
                    position_1_field=values.get("position_1_field"),
                    position_1_status=values.get("position_1_status"),
                    position_2_field=values.get("position_2_field"),
                    position_2_status=values.get("position_2_status"),
                    bias20_atr_band=values.get("bias20_atr_band", "ALL"),
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
                try:
                    source_ingress_result = build_source_ingress_plan(
                        self.preview_server.state.config,
                        target_session=target,
                        package_sha256=package_sha,
                    )
                except (
                    SourceIngressError,
                    SourceSetCandidateApplicationError,
                ) as error:
                    raise PreviewHttpError(
                        409,
                        "PREVIEW_SOURCE_INGRESS_UNAVAILABLE",
                        f"来源准备状态无法核验：{error}",
                    ) from error
                p04_input = source_ingress_result.get("p01_capture", {}).get(
                    "p04_input", {}
                )
                try:
                    source_profile_observation = (
                        read_source_symbol_price_profile_observation(
                            self.preview_server.state.config,
                            self.preview_server.state.profile_contract,
                            target_session=target,
                            package_sha256=package_sha,
                            source_set=p04_input,
                        )
                    )
                except PgPriceProfileError as error:
                    source_profile_observation = {
                        "status": "READ_FAILED",
                        "reason_code": error.code,
                        "target_session": target.isoformat(),
                        "package_sha256": package_sha,
                        "database_write": False,
                        "ddl_executed": False,
                    }
                try:
                    tdx_sector_observation = (
                        read_latest_tdx_sector_snapshot_observation(
                            self.preview_server.state.config,
                            target_session=target,
                            package_sha256=package_sha,
                            source_profile=source_profile_observation,
                        )
                    )
                except TdxSectorMembersApplicationError as error:
                    tdx_sector_observation = {
                        "status": "READ_FAILED",
                        "reason_code": error.code,
                        "target_session": target.isoformat(),
                        "package_sha256": package_sha,
                        "membership_mode": ("CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT"),
                        "snapshots": [],
                        "database_write": False,
                        "ddl_executed": False,
                    }
                checked_at = datetime.now().astimezone().isoformat(timespec="seconds")
                payload = build_chain_readiness_payload(
                    self.preview_server.state.profile_contract,
                    source_version,
                    checked_at=checked_at,
                    source_set_candidate=source_candidate,
                    source_ingress_result=source_ingress_result,
                    source_profile_observation=source_profile_observation,
                    tdx_sector_observation=tdx_sector_observation,
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
                source_context = _preview_source_context(
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
            if parsed.path == "/api/source-symbol/raw-periods":
                values = _query(
                    self.path,
                    {
                        "target_session",
                        "package_sha256",
                        "symbol",
                        "namespace_id",
                        "source_set_id",
                    },
                )
                self._raw_period_request_context = {
                    "target_session": values.get("target_session"),
                    "symbol": values.get("symbol"),
                    "package_sha256": values.get("package_sha256"),
                    "price_coordinate": "RAW_UNADJUSTED",
                    "namespace_id": values.get("namespace_id"),
                    "source_set_id": values.get("source_set_id"),
                    "research_cutoff": self.preview_server.state.research_cutoff.isoformat(),
                    "identity_status": "REQUEST_NOT_RESOLVED",
                }
                required = {"target_session", "package_sha256", "symbol"}
                if set(values) != required:
                    if not required.issubset(values):
                        raise PreviewHttpError(
                            400,
                            "RAW_PERIOD_QUERY_REQUIRED",
                            "RAW周期查询必须显式提供日期、完整SHA和来源代码",
                        )
                expected_namespace_id = None
                expected_source_set_id = None
                expected_source_set_bound = "source_set_id" in values
                identity_fields = set(values) & {"namespace_id", "source_set_id"}
                if identity_fields and identity_fields != {
                    "namespace_id",
                    "source_set_id",
                }:
                    raise PreviewHttpError(
                        400,
                        "PREVIEW_SOURCE_IDENTITY_INVALID",
                        "周期查询的namespace_id与source_set_id必须同时提供",
                    )
                if "namespace_id" in values:
                    try:
                        expected_namespace_id = str(UUID(values["namespace_id"]))
                        if (
                            expected_source_set_bound
                            and values["source_set_id"] != "NONE"
                        ):
                            expected_source_set_id = str(UUID(values["source_set_id"]))
                    except (ValueError, TypeError, AttributeError) as error:
                        raise PreviewHttpError(
                            400,
                            "PREVIEW_SOURCE_IDENTITY_INVALID",
                            "周期来源身份须使用有效的namespace_id和source_set_id",
                        ) from error
                elif "source_set_id" in values:
                    raise PreviewHttpError(
                        400,
                        "PREVIEW_SOURCE_IDENTITY_INVALID",
                        "周期source_set_id必须与namespace_id一并提供",
                    )
                target = _iso_date(values["target_session"])
                _require_research_cutoff(
                    target, self.preview_server.state.research_cutoff
                )
                package_sha = _sha(values["package_sha256"])
                symbol = _symbol(values["symbol"])
                resolved_target, sessions, calendar_revision = (
                    resolve_pg_history_window(target.isoformat(), 75)
                )
                if resolved_target != target:
                    raise PreviewHttpError(
                        400,
                        "PREVIEW_DATE_INVALID",
                        "目标日期未通过官方交易日历校验",
                    )
                history = read_raw_daily_history_stock(
                    self.preview_server.state.config,
                    source_symbol=symbol,
                    target_session=target,
                    package_sha256=package_sha,
                    sessions=sessions,
                )
                source_context = _preview_source_context(
                    self.preview_server.state.config,
                    target_session=target,
                    package_sha256=package_sha,
                    namespace_id=history["namespace_id"],
                    target_raw_rows=history["history_rows"],
                )
                if expected_namespace_id is not None and (
                    expected_namespace_id != history["namespace_id"]
                    or (
                        expected_source_set_bound
                        and expected_source_set_id != source_context["source_set_id"]
                    )
                    or (
                        expected_source_set_id is not None
                        and source_context["status"] != "AVAILABLE"
                    )
                ):
                    self._raw_period_request_context.update(
                        {
                            "resolved_namespace_id": history["namespace_id"],
                            "resolved_source_set_id": source_context["source_set_id"],
                            "identity_status": "REQUEST_IDENTITY_MISMATCH",
                        }
                    )
                    raise PreviewHttpError(
                        409,
                        "PREVIEW_SOURCE_IDENTITY_MISMATCH",
                        "周期返回的namespace或SEALED source_set与所选来源上下文不一致",
                    )
                calendar_payload, _metadata, _year_revision, calendar_sessions = (
                    research_entry._load_calendar(target)
                )
                period_bars = build_source_symbol_raw_periods(
                    target_session=target,
                    session_dates=sessions,
                    calendar_sessions=calendar_sessions,
                    history_rows=history["history_rows"],
                )
                history_summary = summarize_raw_history_rows(
                    session_dates=sessions,
                    history_rows=history["history_rows"],
                )
                foundation = self.preview_server.state.profile_contract.get(
                    "scope_bindings", {}
                ).get("FOUNDATION_EOD", {})
                profile_rules = foundation.get("profile_expression_registry", {})
                field_registry = foundation.get("field_registry", [])
                trends = {
                    kind.lower(): evaluate_raw_period_trend(
                        kind=kind,
                        periods=period_bars[kind],
                        target_session=target,
                        calendar_id=calendar_payload["calendar_id"],
                        calendar_revision=calendar_revision,
                        producer_source_sha256=self.preview_server.state.raw_period_source_sha256,
                        profile_rules=profile_rules,
                        field_registry=field_registry,
                    )
                    for kind in ("WEEK", "MONTH")
                }

                def trend_payload(item):
                    return {
                        "kind": item.kind,
                        "rule_id": item.rule_id,
                        "status": item.status,
                        "value": item.value,
                        "reason": item.reason,
                        "required_period_count": item.required_period_count,
                        "complete_period_count": item.complete_period_count,
                        "input_period_keys": list(item.input_period_keys),
                    }

                rows = history["history_rows"]
                if history_summary["missing_history_count"] != len(
                    history["missing_history_sessions"]
                ):
                    raise PreviewHttpError(
                        500,
                        "RAW_HISTORY_SUMMARY_MISMATCH",
                        "Raw历史缺失会话汇总与PG读取结果不一致",
                    )
                identity = self.preview_server.state.snapshot_identity(
                    namespace_id=history["namespace_id"],
                    target_session=target.isoformat(),
                    package_sha256=package_sha,
                    session_count=len(sessions),
                    session_start=sessions[0].isoformat(),
                    calendar_revision=calendar_revision,
                    source_set_id=source_context["source_set_id"],
                )
                self._send_json(
                    200,
                    {
                        "ok": True,
                        "research_mode": "NON_PIT_RAW_DIAGNOSTIC",
                        "identity_status": "SOURCE_SYMBOL_ONLY",
                        "stable_security_id_status": "STABLE_SECURITY_ID_UNVERIFIED",
                        "listing_status": "LISTING_STATUS_UNKNOWN",
                        "suspension_status": "SUSPENSION_STATUS_UNKNOWN",
                        "official_listing_universe_status": "UNKNOWN",
                        "pit_status": "NON_PIT_RETROSPECTIVE",
                        "price_coordinate": "RAW_UNADJUSTED",
                        "adjustment_status": "UNKNOWN_NOT_ADJUSTED",
                        "corporate_action_status": "UNKNOWN_NOT_BOUND",
                        "target_session": target.isoformat(),
                        "source_symbol": symbol,
                        "symbol": symbol,
                        "package_sha256": package_sha,
                        "namespace_id": history["namespace_id"],
                        "snapshot_identity": identity,
                        "formal_source_set": source_context,
                        "selection": {
                            "source_symbol": symbol,
                            "target_session": target.isoformat(),
                            "package_sha256": package_sha,
                            "namespace_id": history["namespace_id"],
                            "source_set_id": source_context["source_set_id"],
                        },
                        "calendar_id": calendar_payload["calendar_id"],
                        "calendar_revision": calendar_revision,
                        "history_window": {
                            "start_session": sessions[0].isoformat(),
                            "end_session": sessions[-1].isoformat(),
                            **history_summary,
                            "missing_history_sessions": history[
                                "missing_history_sessions"
                            ],
                        },
                        "periods": {
                            "week": [
                                raw_period_payload(period)
                                for period in period_bars["WEEK"]
                            ],
                            "month": [
                                raw_period_payload(period)
                                for period in period_bars["MONTH"]
                            ],
                        },
                        "trends": {
                            name: trend_payload(item) for name, item in trends.items()
                        },
                        "formal_period_profile": {
                            "closed_week": "UNKNOWN",
                            "closed_month": "UNKNOWN",
                            "reason": "NON_PIT_RAW_SOURCE_SYMBOL_NOT_FORMAL_CANONICAL_SELECTION",
                        },
                        "source_zip_opened": False,
                        "source_recalculated": False,
                        "database_write": False,
                        "ddl_executed": False,
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
                source_context = _preview_source_context(
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
        post_path = self.path.split("?", 1)[0]
        if post_path not in {
            "/api/preview/stock",
            "/api/preview/stock-timeline",
            "/api/preview/source-launch-watch",
        }:
            self._error(
                PreviewHttpError(
                    404,
                    "PREVIEW_ROUTE_NOT_FOUND",
                    "POST仅用于用户明确触发的有界来源级或单股只读研究预览",
                )
            )
            return
        if post_path == "/api/preview/source-launch-watch" and self.path != post_path:
            self._error(
                PreviewHttpError(
                    400,
                    "SOURCE_LAUNCH_WATCH_QUERY_REJECTED",
                    "来源级变化观察POST只接受JSON请求体，不接受查询参数",
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
            if post_path == "/api/preview/source-launch-watch":
                self._send_json(200, self._source_launch_watch(body))
                return
            expected = {"symbol", "target_session", "package_sha256", "session_count"}
            allowed_timeline = expected | {"source_set_id", "namespace_id"}
            source_change_required = {
                "include_source_change_context",
                "before_session",
                "before_source_set_id",
                "before_package_sha256",
                "industry_snapshot_id",
                "concept_snapshot_id",
            }
            source_change_optional = {
                "before_profile_revision",
                "after_profile_revision",
            }
            if (
                post_path == "/api/preview/stock-timeline"
                and isinstance(body, dict)
                and bool(source_change_optional & set(body))
                and not source_change_optional <= set(body)
            ):
                raise PreviewHttpError(
                    400,
                    "PREVIEW_PROFILE_REVISIONS_MUST_BE_PAIRED",
                    "before_profile_revision与after_profile_revision必须同时提供",
                )
            source_change_body = allowed_timeline | source_change_required
            source_change_bodies = {
                frozenset(source_change_body),
                frozenset(source_change_body | source_change_optional),
                frozenset(source_change_body | {"research_cutoff"}),
                frozenset(
                    source_change_body | source_change_optional | {"research_cutoff"}
                ),
            }
            valid_body = isinstance(body, dict) and (
                set(body) == expected
                or (
                    post_path == "/api/preview/stock-timeline"
                    and set(body) == allowed_timeline
                )
                or (
                    post_path == "/api/preview/stock-timeline"
                    and frozenset(body) in source_change_bodies
                )
            )
            if not valid_body:
                raise PreviewHttpError(
                    400,
                    "PREVIEW_BODY_FIELDS_INVALID",
                    "请求体须包含symbol、target_session、package_sha256、session_count；轨迹可绑定source_set_id与namespace_id，并可显式启用单股变化上下文",
                )
            symbol = _symbol(body["symbol"])
            target_text = body["target_session"]
            target = _iso_date(target_text)
            _require_research_cutoff(target, self.preview_server.state.research_cutoff)
            package_sha = _sha(body["package_sha256"])
            session_count = _positive_int(body["session_count"], "session_count", 75)
            source_change_opt_in = frozenset(body) in source_change_bodies
            research_cutoff = self.preview_server.state.research_cutoff
            if source_change_opt_in and "research_cutoff" in body:
                research_cutoff = _iso_date(body["research_cutoff"], "research_cutoff")
                if research_cutoff > self.preview_server.state.research_cutoff:
                    raise PreviewHttpError(
                        400,
                        "RESEARCH_CUTOFF_EXCEEDED",
                        "请求研究截止日不能晚于服务已绑定的研究截止日",
                    )
                _require_research_cutoff(target, research_cutoff)
            before_target: date | None = None
            before_source_set_id: str | None = None
            before_package_sha: str | None = None
            industry_snapshot_id: str | None = None
            concept_snapshot_id: str | None = None
            before_profile_revision: str | None = None
            after_profile_revision: str | None = None
            if source_change_opt_in:
                if body["include_source_change_context"] is not True:
                    raise PreviewHttpError(
                        400,
                        "PREVIEW_SOURCE_CHANGE_OPT_IN_INVALID",
                        "include_source_change_context必须显式为true",
                    )
                before_target = _iso_date(body["before_session"], "before_session")
                _require_research_cutoff(before_target, research_cutoff)
                if before_target >= target:
                    raise PreviewHttpError(
                        400,
                        "PREVIEW_SOURCE_CHANGE_DATE_INVALID",
                        "before_session必须早于target_session",
                    )
                before_package_sha = _sha(body["before_package_sha256"])
                if before_package_sha != package_sha:
                    raise PreviewHttpError(
                        409,
                        "PREVIEW_SOURCE_CHANGE_PACKAGE_MISMATCH",
                        "前后来源包SHA必须相同",
                    )
                try:
                    before_source_set_id = str(UUID(body["before_source_set_id"]))
                    industry_snapshot_id = str(UUID(body["industry_snapshot_id"]))
                    concept_snapshot_id = str(UUID(body["concept_snapshot_id"]))
                except (ValueError, TypeError, AttributeError) as error:
                    raise PreviewHttpError(
                        400,
                        "PREVIEW_SOURCE_CHANGE_ID_INVALID",
                        "前日source_set及行业/概念快照ID须为有效UUID",
                    ) from error
                if source_change_optional <= set(body):
                    before_profile_revision = _sha(body["before_profile_revision"])
                    after_profile_revision = _sha(body["after_profile_revision"])
            expected_source_set_id = None
            expected_namespace_id = None
            if allowed_timeline <= set(body):
                try:
                    expected_source_set_id = str(UUID(body["source_set_id"]))
                    expected_namespace_id = str(UUID(body["namespace_id"]))
                except (ValueError, TypeError, AttributeError) as error:
                    raise PreviewHttpError(
                        400,
                        "PREVIEW_SOURCE_IDENTITY_INVALID",
                        "轨迹来源身份须使用有效的source_set_id和namespace_id",
                    ) from error
            resolved_target, sessions, calendar_revision = resolve_pg_history_window(
                target.isoformat(), session_count
            )
            if resolved_target != target:
                raise PreviewHttpError(
                    400, "PREVIEW_DATE_INVALID", "目标日期未通过官方交易日历校验"
                )
            if source_change_opt_in:
                resolved_before, _before_sessions, before_calendar_revision = (
                    resolve_pg_history_window(before_target.isoformat(), 1)
                )
                _resolved_pair_target, adjacent_sessions, pair_calendar_revision = (
                    resolve_pg_history_window(target.isoformat(), 2)
                )
                if resolved_before != before_target:
                    raise PreviewHttpError(
                        400,
                        "PREVIEW_BEFORE_DATE_INVALID",
                        "前日日期未通过官方交易日历校验",
                    )
                if (
                    len(adjacent_sessions) != 2
                    or adjacent_sessions[0] != before_target
                    or adjacent_sessions[1] != target
                    or pair_calendar_revision != calendar_revision
                    or before_calendar_revision != calendar_revision
                ):
                    raise PreviewHttpError(
                        409,
                        "PREVIEW_SOURCE_CHANGE_SESSIONS_NOT_ADJACENT",
                        "前后日期必须是同一日历版本中的相邻官方会话",
                    )
            prevalidated_source_context = None
            if (
                post_path == "/api/preview/stock-timeline"
                and expected_source_set_id is not None
                and expected_namespace_id is not None
            ):
                try:
                    prevalidated_source_context = read_sealed_source_context(
                        self.preview_server.state.config,
                        target_session=target,
                        package_sha256=package_sha,
                        namespace_id=expected_namespace_id,
                        expected_source_set_id=expected_source_set_id,
                    )
                except PgResearchBindingError as error:
                    if error.code == "PG_RESEARCH_P04_SOURCE_SET_UNAVAILABLE":
                        raise PreviewHttpError(
                            409,
                            "PREVIEW_SOURCE_IDENTITY_MISMATCH",
                            "请求的source_set未通过精确P04封口核验；未读取单股行情",
                        ) from error
                    mapped = _source_context_preview_error(error)
                    if mapped is None:
                        raise
                    raise mapped from error
            history = read_raw_daily_history_stock(
                self.preview_server.state.config,
                source_symbol=symbol,
                target_session=target,
                package_sha256=package_sha,
                sessions=sessions,
                expected_namespace_id=expected_namespace_id,
            )
            if post_path == "/api/preview/stock-timeline":
                result = research_entry.research_stock_pg_history_timeline(
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
                if prevalidated_source_context is not None:
                    source_context = bind_source_context_to_raw_rows(
                        prevalidated_source_context,
                        target_session=target,
                        package_sha256=package_sha,
                        namespace_id=history["namespace_id"],
                        source_set_id=expected_source_set_id,
                        target_raw_rows=history["history_rows"],
                    )
                else:
                    source_context = _preview_source_context(
                        self.preview_server.state.config,
                        target_session=target,
                        package_sha256=package_sha,
                        namespace_id=history["namespace_id"],
                        target_raw_rows=history["history_rows"],
                    )
                if expected_namespace_id is not None and (
                    expected_namespace_id != history["namespace_id"]
                    or expected_source_set_id != source_context["source_set_id"]
                    or source_context["status"] != "AVAILABLE"
                ):
                    raise PreviewHttpError(
                        409,
                        "PREVIEW_SOURCE_IDENTITY_MISMATCH",
                        "轨迹返回的namespace或SEALED source_set与变化快照不一致",
                    )
                if source_change_opt_in:
                    try:
                        membership = read_tdx_sector_symbol_membership(
                            self.preview_server.state.config,
                            source_symbol=symbol,
                            target_session=target,
                            industry_snapshot_id=industry_snapshot_id,
                            concept_snapshot_id=concept_snapshot_id,
                            expected_namespace_id=history["namespace_id"],
                        )
                        source_change = query_source_stock_change_context(
                            self.preview_server.state.config,
                            namespace_id=history["namespace_id"],
                            before_session=before_target,
                            target_session=target,
                            research_cutoff=research_cutoff,
                            before_source_set_id=before_source_set_id,
                            before_package_sha256=before_package_sha,
                            source_set_id=source_context["source_set_id"],
                            package_sha256=package_sha,
                            symbol=symbol,
                            before_profile_revision=before_profile_revision,
                            after_profile_revision=after_profile_revision,
                        )
                    except Exception as error:
                        preview_error = _source_context_preview_error(error)
                        if preview_error is None:
                            raise
                        raise preview_error from error
                    result["source_change_context"] = {
                        **source_change,
                        "current_tdx_membership": membership,
                    }
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
                    "namespace_id": history["namespace_id"],
                    "source_set_id": source_context["source_set_id"],
                }
                relation_history = result.get("price_relation_history")
                if isinstance(relation_history, dict):
                    identity_ref = identity["identity_sha256"]
                    relation_history["identity_ref"] = identity_ref
                    for collection_name in ("observations", "events"):
                        for item in relation_history.get(collection_name, ()):
                            if isinstance(item, dict):
                                item["identity_ref"] = identity_ref
                direction_history = result.get("ma20_direction_history")
                if isinstance(direction_history, dict):
                    direction_history["identity_ref"] = identity["identity_sha256"]
                    binding = direction_history.get("binding")
                    if isinstance(binding, dict):
                        binding.update(
                            {
                                "source_set_id": source_context["source_set_id"],
                                "source_set_revision": source_context["revision_id"],
                                "source_set_status": source_context["status"],
                            }
                        )
                self._send_json(200, result)
                return
            source_context = _preview_source_context(
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
