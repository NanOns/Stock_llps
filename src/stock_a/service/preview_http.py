"""本机只读PG来源级研究预览HTTP服务。"""

from __future__ import annotations

from stock_a.contracts.profile_revision import PROFILE_REVISION_PATTERN

from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from collections import Counter, defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
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

from stock_a.application.source_market_context import (
    SourceMarketContextError,
    read_source_market_context,
    source_market_context_payload,
)
from stock_a.application.source_market_participation import (
    read_source_market_participation,
)
from stock_a.application.source_market_observation import (
    SourceObservationError,
    prepare_saved_d1_material_observation,
    read_d1_material_observation,
)
from stock_a.application.source_d1_structures import (
    ALL_STRUCTURE_TYPES,
    MATERIAL_CHANGE_REASONS,
    STRUCTURE_TYPES,
)
from stock_a.application.source_persisted_research import (
    ResearchSymbolNotFound,
    read_persisted_cohort,
    read_persisted_cohort_keys,
)
from stock_a.application.source_enrollment_cohort import read_source_enrollment_cohort
from stock_a.application.source_symbol_display_names import (
    decorate_api_source_symbols,
)
from stock_a.storage.postgres.research_facts import (
    ResearchFactConflict,
    ResearchFactNotFound,
)
from stock_a.storage.postgres.market_participation import MarketParticipationPgError
from stock_a.storage.postgres.research_day_facts import (
    ResearchDayPgError,
    read_source_research_day,
    read_source_board_comparison,
    read_source_research_day_scope,
)
from stock_a.contracts import PriceProfileContractError, load_price_profile_contract
from stock_a.application.runtime_config import (
    DiagnosticError,
    _CONFIG_ROOT,
    _EXIT_ARGUMENT,
    _EXIT_OK,
    _EXIT_UNAVAILABLE,
    load_runtime_config,
)
from stock_a.application.research_window import resolve_pg_history_window
from stock_a.research.entry_context.errors import ResearchEntryError
from stock_a.research.entry_context.sessions import load_period_calendar
from stock_a.application.pg_research import (
    PgResearchBindingError,
    read_source_version_context,
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
    read_tdx_sector_members,
    read_tdx_sector_source_snapshot_catalog,
)
from stock_a.application.source_sector_change_research import (
    SourceSectorChangeInputs,
    prepare_source_sector_change_inputs,
    build_source_sector_change_view,
    validate_source_d1_discovery_fact,
)
from stock_a.application.source_d1_support_changes import (
    discover_source_d1_support_changes,
)
from stock_a.research.entry_history import history_preview as research_history_preview
from stock_a.application.tdx_sector_change_stocks import _member_detail
from stock_a.application.source_stock_change_context import (
    SourceStockChangeContextError,
    query_source_stock_change_context,
)
from stock_a.application.source_stock_seed_context import (
    query_source_stock_seed_context,
)
from stock_a.application.source_d0_raw import (
    SourceD0RawError,
    _official_calendar,
    query_source_d0_raw_launch,
)
from stock_a.application.data_jobs import DataJobCoordinator, DataJobError
from stock_a.application.source_formal_admission import (
    present_source_formal_admission,
)
from stock_a.application.source_pool_factors import read_source_pool_rps
from stock_a.application.source_tdx_sector_membership import (
    read_tdx_sector_symbol_membership,
)
from stock_a.storage.postgres.source_raw import (
    RawSourcePgError,
)
from stock_a.storage.postgres.baostock_price_facts import (
    BaoStockPriceFactsError,
    read_baostock_price_catalog,
    read_baostock_price_detail,
    read_baostock_price_history,
    read_baostock_price_page,
    resolve_baostock_stock_code,
)
from stock_a.storage.postgres.data_jobs import DataJobPgError
from stock_a.research import core as research_core
from stock_a.research import entry as research_entry
from stock_a.research import raw_periods as research_raw_periods
from stock_a.research.entry_history.history_timeline import post_signal_price_facts
from stock_a.research.raw_periods import (
    build_source_symbol_raw_periods,
    evaluate_raw_period_trend,
    raw_period_payload,
    summarize_raw_history_rows,
)


from stock_a.service.serialization import _json_value
from stock_a.service.static_assets import read_static_asset, static_asset_routes

_HOST = "127.0.0.1"
_PORT = 28766
_MAX_BODY_BYTES = 8192
_MAX_PAGE_SIZE = 50
_MAX_OFFSET = 2**31 - 1
_D1_MATERIAL_FACTS_CACHE_TTL_SECONDS = 30
_SYMBOL = re.compile(r"^(SH|SZ)\.[0-9]{6}$")
_SYMBOL_SEARCH = re.compile(r"^[A-Za-z0-9.]{1,9}$")
_DATE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
_PRICE_COORDINATES = frozenset({"RAW_UNADJUSTED", "FORWARD_ADJUSTED"})
_DIAGNOSTIC_ROUTES = frozenset(
    {
        "/api/preview/catalog",
        "/api/preview/raw",
        "/api/preview/sector-change-context",
        "/api/preview/sector-change-stocks",
        "/api/preview/source-launch-watch",
        "/api/preview/t0-early-observations",
        "/api/preview/source-page",
        "/api/preview/stock-timeline",
        "/api/preview/stock",
        "/api/product/cohort-keys",
        "/api/product/cohort",
        "/api/product/cohort-enrollment-lifecycle",
        "/api/product/cohort-date-comparison",
        "/api/product/cohort-symbol-forward",
        "/api/product/sector-pulse-history",
        "/api/product/sector-pulse-directory",
        "/api/product/resolve-stock-code",
        "/api/product/market",
        "/api/product/d1-material",
        "/api/product/board-comparison",
        "/api/source-symbol-price-profile-breadth-members",
        "/api/source-symbol-price-profile-breadth",
        "/api/source-symbol-price-profile-changes",
        "/api/source-symbol-price-profile-pair",
        "/api/source-symbol-price-profile-relation-screen",
        "/api/source-symbol-price-profiles",
        "/api/source-symbol-price-profiles/snapshots",
        "/api/source-symbol/raw-periods",
    }
)
# 只登记运行代码当前实际提供的能力边界。阶段名称、输入、产物和消费者始终来自机器合同。


class PreviewHttpError(Exception):
    def __init__(self, status: int, code: str, message: str):
        self.status = status
        self.code = code
        self.message = message
        super().__init__(message)


def _attach_t0_early_contexts(
    config: Mapping[str, Any], entries: Sequence[dict[str, Any]]
) -> None:
    """一次性将页面登记ID批量联接到已存板内观察。"""
    enrollment_ids = [entry["id"] for entry in entries]
    from stock_a.storage.postgres.t0_early_observations import (
        read_source_t0_early_observations_for_enrollments,
    )

    contexts_by_enrollment = {}
    for start in range(0, len(enrollment_ids), 300):
        contexts_by_enrollment.update(
            read_source_t0_early_observations_for_enrollments(
                config, enrollment_ids=enrollment_ids[start : start + 300]
            )
        )
    for entry in entries:
        entry["early_contexts"] = contexts_by_enrollment.get(entry["id"], [])
        entry["early_status"] = (
            "AVAILABLE" if entry["early_contexts"] else "NO_STORED_FACT"
        )
        entry["early_reason"] = (
            None if entry["early_contexts"] else "该目标日没有已存板内观察事实"
        )


def _bind_t0_followup_status(
    summary: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    stored_status: str | None = None,
    stored_reason: str | None = None,
) -> dict[str, Any]:
    """将已知后继状态绑定到当前请求截止，不读取截止之后的行情。"""
    result = dict(summary)
    if result.get("status") != "AVAILABLE":
        return result
    if stored_status and stored_status not in {"NOT_COMPUTED", "UNKNOWN"}:
        status = stored_status
        reason = stored_reason
    elif target_session >= research_cutoff:
        status = "NOT_DUE"
        reason = "NO_SUCCESSOR_SESSION_WITHIN_RESEARCH_CUTOFF"
    else:
        status = "NOT_COMPUTED"
        reason = stored_reason or "SUCCESSOR_FACT_NOT_STORED_FOR_EXACT_SCOPE"
    result["followup_status"] = status
    result["followup_reason"] = reason
    driver = dict(result.get("t0_driver_and_earliness") or {})
    driver["followup_status"] = status
    driver["followup_reason"] = reason
    result["t0_driver_and_earliness"] = driver
    return result


def _t0_list_summary(summary: Mapping[str, Any]) -> dict[str, Any]:
    """只向列表返回紧凑分型与已选类别样例；完整事实留给单股展开。"""
    result = {
        "status": summary.get("status", "NO_STORED_FACT"),
        "triage_state": summary.get("triage_state", "NO_T0_FACT"),
        "followup_status": summary.get("followup_status", "NOT_COMPUTED"),
        "followup_reason": summary.get("followup_reason"),
    }
    driver = summary.get("t0_driver_and_earliness")
    if not isinstance(driver, Mapping):
        return result
    result["t0_driver_and_earliness"] = {
        key: driver[key]
        for key in (
            "version",
            "registration_count",
            "relation_count",
            "same_direction_candidate_relation_count",
            "same_direction_candidate_board_count",
            "same_direction_candidate_examples",
            "other_background_relation_count",
            "other_background_board_count",
            "background_pulse_false_relation_count",
            "background_pulse_false_board_count",
            "other_background_examples",
            "named_conflict_relation_count",
            "named_conflict_board_count",
            "named_conflict_codes",
            "named_conflict_examples",
            "unknown_relation_count",
            "unknown_board_count",
            "unknown_reason_codes",
            "pulse_transition_not_computed_relation_count",
            "next_check_codes",
            "price_at_t0",
            "scope",
            "price_coordinate",
            "followup_status",
            "followup_reason",
        )
        if key in driver
    }
    return result


def _membership_only_board_observation(
    membership_page: Mapping[str, Any], *, board_kind: str, board_native_id: str
) -> dict[str, Any]:
    """把精确当前成员分页呈现为成员身份已知、D0/Seed/D1未计算。"""
    total = int(membership_page.get("source_member_count", 0))
    inside = int(membership_page.get("matched_b_t", 0))
    outside = int(membership_page.get("unmatched_b_t", 0))
    members = []
    for item in membership_page.get("rows", []):
        in_pool = item.get("b_t_membership_status") == "MATCHED_B_T"
        members.append(
            {
                "source_symbol": item["source_symbol"],
                "membership_quality": item.get("membership_quality"),
                "source_pool_status": "IN_TARGET_SOURCE_POOL"
                if in_pool
                else "OUTSIDE_TARGET_SOURCE_POOL",
                "d0": {
                    "before_state": "UNKNOWN",
                    "after_state": "UNKNOWN",
                    "transition": "NOT_COMPUTED",
                },
                "seed": {
                    "source_seed": "UNKNOWN",
                    "s1": "UNKNOWN",
                    "s2": "UNKNOWN",
                    "safety": "UNKNOWN",
                    "rps5_board_rank": None,
                    "rps5_board_rank_denominator": None,
                    "rps5_board_tie_count": None,
                    "rps20_board_rank": None,
                    "rps20_board_rank_denominator": None,
                    "rps20_board_tie_count": None,
                    "rps5": None,
                    "rps20": None,
                },
                "d1": {
                    "target_price_quality": "UNKNOWN",
                    "material_anchor_count": None,
                },
                "t0_early_triage_summary": {"status": "NO_STORED_FACT"},
            }
        )
    return {
        "status": "AVAILABLE",
        "membership_only": True,
        "research_scope_status": "MEMBERSHIP_ONLY",
        "reason": "当前成员关系可读；D0、Seed和D1精确研究事实未保存。",
        "board_kind": board_kind,
        "board_native_id": board_native_id,
        "membership_scope": "NON_PIT_CURRENT_SNAPSHOT",
        "snapshot_member_count": total,
        "source_pool": {"inside": inside, "outside": outside},
        "d0": {
            "true": None,
            "false": None,
            "unknown": None,
            "missing": inside,
            "target_source_pool_denominator": inside,
            "outside_target_source_pool": outside,
            "transitions": {},
        },
        "seed": {
            "scope_status": "NOT_COMPUTED",
            "target_source_pool_denominator": inside,
            "true": None,
            "false": None,
            "unknown": None,
            "missing": inside,
        },
        "d1": {
            "scope_status": "NOT_COMPUTED",
            "material_change_members": None,
            "material_anchor_count": None,
            "target_price_quality_counts": {},
        },
        "cross_kind_overlap": {"kind": None, "member_count": None, "top_boards": []},
        "pagination": {
            "offset": membership_page.get("offset", 0),
            "limit": membership_page.get("limit", len(members)),
            "total_count": total,
            "next_offset": (
                int(membership_page["offset"]) + len(members)
                if membership_page.get("empty_page_out_of_range") is not True
                and int(membership_page.get("offset", 0)) + len(members) < total
                else None
            ),
        },
        "members": members,
    }


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
        "PG_RESEARCH_PROFILE_KEY_INVALID",
        "SECTOR_CHANGE_IDENTITY_INVALID",
        "SECTOR_CHANGE_SOURCE_ID_INVALID",
        "SECTOR_CHANGE_EVENT_WINDOW_INVALID",
        "D0_BEFORE_SESSION_INVALID",
    }
    if code.startswith("PG_"):
        status = 503
    elif (
        category == "ARGUMENT"
        or code in request_errors
        or (code.startswith("D0_") and code.endswith(("INVALID", "_REJECTED")))
    ):
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


def _verified_sector_snapshot_pair(
    config: Mapping[str, Any], *, industry_snapshot_id: int, concept_snapshot_id: int
) -> dict[str, Any]:
    """校验行业与GN快照来自同一观察，并返回其非PIT来源身份。"""
    if industry_snapshot_id == concept_snapshot_id:
        raise PreviewHttpError(
            409,
            "SECTOR_SNAPSHOT_PAIR_NOT_DISTINCT",
            "行业与GN概念必须使用不同的来源快照",
        )
    catalog = read_tdx_sector_source_snapshot_catalog(dict(config))
    rows = catalog.get("snapshots", ()) if catalog.get("status") == "AVAILABLE" else ()
    by_id = {
        int(row["snapshot_id"]): row
        for row in rows
        if isinstance(row, Mapping) and row.get("snapshot_id") is not None
    }
    selected = {
        "INDUSTRY": by_id.get(industry_snapshot_id),
        "CONCEPT": by_id.get(concept_snapshot_id),
    }
    expected_sources = {
        "INDUSTRY": "TDX_TDXHY_CFG",
        "CONCEPT": "TDX_INFOHARBOR_BLOCK_DAT",
    }
    normalized_times: dict[str, datetime] = {}
    for kind, row in selected.items():
        if row is None or row.get("source_kind") != expected_sources[kind]:
            raise PreviewHttpError(
                409,
                "SECTOR_SNAPSHOT_PAIR_IDENTITY_INVALID",
                f"{kind}快照ID不存在或来源类型不匹配",
            )
        try:
            observed_at = datetime.fromisoformat(str(row["observed_at"]))
            catalog_count = int(row.get(f"{kind.lower()}_catalog_rows") or 0)
            member_count = int(row.get(f"{kind.lower()}_member_rows") or 0)
            invalid_members = int(row.get("invalid_membership_quality_rows") or 0)
        except (KeyError, TypeError, ValueError) as error:
            raise PreviewHttpError(
                409,
                "SECTOR_SNAPSHOT_PAIR_METADATA_INVALID",
                f"{kind}快照观察时间或目录/成员统计无效",
            ) from error
        if (
            observed_at.tzinfo is None
            or row.get("research_mode") != "NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT"
            or row.get("clock_quality") != "UNKNOWN"
            or row.get("source_valid_on") is not None
            or row.get("source_known_at") is not None
            or row.get("source_available_at") is not None
            or catalog_count <= 0
            or member_count <= 0
            or invalid_members != 0
        ):
            raise PreviewHttpError(
                409,
                "SECTOR_SNAPSHOT_PAIR_METADATA_INVALID",
                f"{kind}快照不满足非PIT目录、成员及未知来源时钟合同",
            )
        normalized_times[kind] = observed_at.astimezone(timezone.utc)
    if normalized_times["INDUSTRY"] != normalized_times["CONCEPT"]:
        raise PreviewHttpError(
            409,
            "SECTOR_SNAPSHOT_PAIR_OBSERVATION_MISMATCH",
            "行业与GN概念快照不是同一次来源观察",
        )

    industry = selected["INDUSTRY"]
    concept = selected["CONCEPT"]
    observed_at = datetime.fromisoformat(str(industry["observed_at"]))
    return {
        "status": "AVAILABLE",
        "selection_rule": "EXPLICIT_IDS_VALIDATED_SAME_OBSERVATION",
        "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
        "research_mode": "NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT",
        "clock_quality": "UNKNOWN",
        "observed_at": observed_at.isoformat(),
        "snapshot_ids": {
            "INDUSTRY": industry_snapshot_id,
            "CONCEPT": concept_snapshot_id,
        },
        "snapshots": [
            {
                "snapshot_id": industry_snapshot_id,
                "board_kind": "INDUSTRY",
                "source_kind": industry["source_kind"],
                "observed_at": industry["observed_at"],
                "source_quality": industry.get("source_quality"),
                "catalog_rows": int(industry["industry_catalog_rows"]),
                "member_rows": int(industry["industry_member_rows"]),
            },
            {
                "snapshot_id": concept_snapshot_id,
                "board_kind": "CONCEPT",
                "source_kind": concept["source_kind"],
                "observed_at": concept["observed_at"],
                "source_quality": concept.get("source_quality"),
                "catalog_rows": int(concept["concept_catalog_rows"]),
                "member_rows": int(concept["concept_member_rows"]),
            },
        ],
    }


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


def _positive_id(value: Any, code: str, label: str) -> int:
    if (type(value) is int and value > 0) or (
        isinstance(value, str)
        and value.isdecimal()
        and str(int(value)) == value
        and int(value) > 0
    ):
        return int(value)
    raise PreviewHttpError(400, code, f"{label}必须是正数BIGINT")


def _capture_id(value: Any) -> int:
    return _positive_id(value, "PREVIEW_CAPTURE_ID_INVALID", "来源捕获ID")


def _profile_revision(value: Any) -> str:
    if (
        not isinstance(value, str)
        or re.fullmatch(PROFILE_REVISION_PATTERN, value) is None
    ):
        raise PreviewHttpError(
            400, "PREVIEW_PROFILE_REVISION_INVALID", "画像修订须为完整画像修订标签"
        )
    return value


def _selected_profile_history_session_count(
    profile_observation: Mapping[str, Any] | None,
    *,
    requested_history_session_count: int | None,
    requested_profile_revision: str | None,
) -> int | None:
    """只以完整唯一Profile的请求窗口选会话数，不要求每行都有全历史。"""
    if not isinstance(profile_observation, Mapping):
        return None
    selection = profile_observation.get("profile_revision_selection")
    selected = profile_observation.get("selected_revision")
    if not isinstance(selection, Mapping) or not isinstance(selected, Mapping):
        return None
    if (
        selection.get("current_version_match_count") != 1
        or selection.get("selected_version_match") is not True
    ):
        return None
    requested_min = selected.get("min_history_requested_sessions")
    requested_max = selected.get("max_history_requested_sessions")
    if (
        type(requested_min) is not int
        or requested_min != requested_max
        or not 1 <= requested_min <= 250
        or requested_history_session_count is not None
        and requested_min != requested_history_session_count
        or requested_profile_revision is not None
        and selected.get("profile_revision") != requested_profile_revision
    ):
        return None
    return requested_min


def _price_coordinate(value: Any) -> str:
    if not isinstance(value, str) or value not in _PRICE_COORDINATES:
        raise PreviewHttpError(
            400,
            "PRICE_COORDINATE_INVALID",
            "价格坐标只能是RAW_UNADJUSTED或FORWARD_ADJUSTED",
        )
    return value


def _snapshot_id(value: Any) -> int:
    return _positive_id(value, "PREVIEW_SNAPSHOT_ID_INVALID", "板块快照ID")


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


def _d1_material_int(value: Any, field: str, minimum: int, maximum: int) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise PreviewHttpError(
            400,
            "D1_MATERIAL_PARAMETER_INVALID",
            f"{field}必须为{minimum}至{maximum}的整数",
        )
    return value


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


def _raw_public_fields(
    row: Mapping[str, Any], *, price_coordinate: str
) -> dict[str, Any]:
    if price_coordinate not in _PRICE_COORDINATES:
        raise PreviewHttpError(
            400, "PRICE_COORDINATE_INVALID", "价格坐标不在现役坐标集合中"
        )
    values = row.get("ohlc")
    if not isinstance(values, Mapping):
        prefix = "raw_" if price_coordinate == "RAW_UNADJUSTED" else "qfq_"
        values = {
            name: row.get(f"{prefix}{name}")
            for name in ("open", "high", "low", "close")
        }
    quality_key = (
        "price_quality" if price_coordinate == "RAW_UNADJUSTED" else "qfq_price_quality"
    )
    reason_key = (
        "price_reason" if price_coordinate == "RAW_UNADJUSTED" else "qfq_price_reason"
    )
    field_prefix = "raw" if price_coordinate == "RAW_UNADJUSTED" else "qfq"
    quality = row.get(quality_key)
    price_reason = row.get(reason_key)
    values_public = {
        name: _json_value(values.get(name)) for name in ("open", "high", "low", "close")
    }
    field_qualities = {
        name: _json_value(
            row.get(
                f"{field_prefix}_{name}_quality",
                quality,
            )
        )
        for name in ("open", "high", "low", "close")
    }
    field_reasons = {
        name: _json_value(
            row.get(
                f"{field_prefix}_{name}_reason",
                price_reason,
            )
        )
        for name in ("open", "high", "low", "close")
    }
    coordinate_request_completed_at = row.get(
        "raw_request_completed_at"
        if price_coordinate == "RAW_UNADJUSTED"
        else "qfq_request_completed_at"
    )
    return {
        "session_date": row["session_date"].isoformat(),
        "source_symbol": row["source_symbol"],
        "market": row["market"],
        "board": row["board"],
        "bar_status": row["bar_status"],
        "bar_reason": row["bar_reason"],
        "values": {
            **values_public,
            "amount": _json_value(row.get("raw_amount")),
            "volume": _json_value(row.get("raw_volume")),
            "turn": _json_value(row.get("raw_turn_value")),
        },
        "quality": {
            "price_quality": _json_value(quality),
            "price_reason": _json_value(price_reason),
            "price_field_qualities": field_qualities,
            "price_field_reasons": field_reasons,
            "amount_quality": _json_value(row.get("amount_quality")),
            "amount_reason": _json_value(row.get("amount_reason")),
            "raw_amount_quality": _json_value(row.get("raw_amount_quality")),
            "raw_amount_reason": _json_value(row.get("raw_amount_reason")),
            "volume_quality": _json_value(row.get("volume_quality")),
            "volume_reason": _json_value(row.get("volume_reason")),
            "raw_volume_quality": _json_value(row.get("raw_volume_quality")),
            "raw_volume_reason": _json_value(row.get("raw_volume_reason")),
            "turn_quality": _json_value(row.get("turn_quality")),
            "turn_reason": _json_value(row.get("turn_reason")),
            "amount_unit_status": row.get("amount_unit_status"),
            "volume_unit_status": row.get("volume_unit_status"),
        },
        "source": {
            "provider": "BAOSTOCK",
            "source_capture_id": row.get("source_capture_id"),
            "captured_at": _json_value(coordinate_request_completed_at),
            "coordinate_request_completed_at": _json_value(
                coordinate_request_completed_at
            ),
            "raw_request_completed_at": _json_value(
                row.get("raw_request_completed_at")
            ),
            "qfq_request_completed_at": _json_value(
                row.get("qfq_request_completed_at")
            ),
            "capture_read_started_at": _json_value(row.get("capture_read_started_at")),
            "capture_clock_role": row.get("capture_clock_role"),
            "source_clock_role": row.get(
                "source_clock_role", row.get("capture_clock_role")
            ),
            "source_time_quality": row.get("source_time_quality"),
            "source_available_at": _json_value(row.get("source_available_at")),
            "unit_contract_version": row.get("unit_contract_version"),
        },
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "source_object_status": row.get(
            "source_object_status", "BAOSTOCK_SOURCE_SYMBOL_OBSERVED"
        ),
        "target_bar_status": row.get("target_bar_status", row.get("bar_status")),
        "stable_security_id_status": row.get(
            "stable_security_id_status", "STABLE_SECURITY_ID_UNVERIFIED"
        ),
        "listing_status": row.get("listing_status", "LISTING_STATUS_UNKNOWN"),
        "suspension_status": row.get("suspension_status", "SUSPENSION_STATUS_UNKNOWN"),
        "official_listing_universe_status": "UNKNOWN",
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "price_coordinate": price_coordinate,
        "source_capture_id": row.get("source_capture_id"),
    }


def _page_public_fields(
    page: Mapping[str, Any], *, price_coordinate: str
) -> dict[str, Any]:
    raw_selected = price_coordinate == "RAW_UNADJUSTED"
    candidates = [
        {
            "source_symbol": row["source_symbol"],
            "market": row["market"],
            "board": row["board"],
            "bar_status": row.get("bar_status")
            if raw_selected
            else row.get("qfq_bar_status"),
            "price_quality": row.get("price_quality")
            if raw_selected
            else row.get("qfq_price_quality"),
            "price_reason": row.get("price_reason")
            if raw_selected
            else row.get("qfq_price_reason"),
            "close_quality": row.get("raw_close_quality")
            if raw_selected
            else row.get("qfq_close_quality"),
            "close_reason": row.get("raw_close_reason")
            if raw_selected
            else row.get("qfq_close_reason"),
            "identity_status": "SOURCE_SYMBOL_ONLY",
            "source_object_status": row.get(
                "source_object_status", "BAOSTOCK_SOURCE_SYMBOL_OBSERVED"
            ),
            "target_bar_status": row.get("bar_status")
            if raw_selected
            else row.get("qfq_bar_status"),
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
        "source_capture_id": page["source_capture_id"],
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
            "stored_price_rows": actual,
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
            "source_object_status", "BAOSTOCK_SOURCE_SYMBOL_OBSERVED"
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
        "price_coordinate": price_coordinate,
        "ddl_executed": False,
        "database_write": False,
    }


def _baostock_source_context(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int,
    target_price_rows: Sequence[Mapping[str, Any]],
    price_coordinate: str,
) -> dict[str, Any]:
    """用BaoStock捕获目录核对RAW/QFQ共用捕获及目标行身份。"""
    catalog = read_baostock_price_catalog(
        config,
        cutoff=research_cutoff,
        source_capture_id=source_capture_id,
    )
    versions = [
        item
        for item in catalog["price_coordinate_versions"]
        if item["session_date"] == target_session.isoformat()
        and item["source_capture_id"] == source_capture_id
    ]
    by_coordinate = {item["price_coordinate"]: item for item in versions}
    raw_version = by_coordinate.get("RAW_UNADJUSTED")
    qfq_version = by_coordinate.get("FORWARD_ADJUSTED")
    if not raw_version or not qfq_version:
        raise PreviewHttpError(
            409,
            "BAOSTOCK_CAPTURE_COVERAGE_INCOMPLETE",
            "目标日缺少同一BaoStock捕获的RAW/QFQ双坐标价格目录事实",
        )
    supplied_rows = list(target_price_rows)
    if any(
        not isinstance(row, Mapping)
        or row.get("source_capture_id") != source_capture_id
        for row in supplied_rows
    ):
        raise PreviewHttpError(
            409,
            "BAOSTOCK_TARGET_ROW_CAPTURE_MISMATCH",
            "价格行捕获ID与所选BaoStock来源上下文不一致",
        )
    rows = [
        row
        for row in supplied_rows
        if row.get("session_date") in (target_session, target_session.isoformat())
    ]
    if any(
        not isinstance(row.get("source_symbol"), str)
        or _SYMBOL.fullmatch(row["source_symbol"]) is None
        for row in rows
    ):
        raise PreviewHttpError(
            409,
            "BAOSTOCK_TARGET_ROW_IDENTITY_INVALID",
            "BaoStock目标价格行缺少规范来源代码",
        )
    target_symbols = [row["source_symbol"] for row in rows]
    if len(target_symbols) != len(set(target_symbols)):
        raise PreviewHttpError(
            409,
            "BAOSTOCK_TARGET_ROW_IDENTITY_DUPLICATE",
            "BaoStock目标日期来源代码重复",
        )
    first = rows[0] if rows else {}
    complete = (
        raw_version.get("coverage_status") == "COMPLETE"
        and qfq_version.get("coverage_status") == "COMPLETE"
    )
    from stock_a.sources.baostock.identity import eligible_research_board
    from stock_a.storage.postgres.baostock_price_facts import (
        read_baostock_capture_roster,
    )

    roster = read_baostock_capture_roster(
        config,
        source_capture_id=source_capture_id,
        cutoff=research_cutoff,
    )
    target_iso = target_session.isoformat()
    research_expected_row_count = sum(
        1
        for item in roster.get("symbols", [])
        if eligible_research_board(item.get("source_symbol")) is not None
        and item.get("eligible_start_session")
        and item.get("eligible_end_session")
        and item["eligible_start_session"] <= target_iso <= item["eligible_end_session"]
    )
    return {
        "status": "AVAILABLE",
        "coverage_status": "COMPLETE" if complete else "INCOMPLETE",
        "target_session": target_session.isoformat(),
        "source_capture_id": source_capture_id,
        "provider": "BAOSTOCK",
        "price_coordinate": price_coordinate,
        "price_coordinates": ["RAW_UNADJUSTED", "FORWARD_ADJUSTED"],
        "source_capture_expected_row_count": raw_version.get("expected_row_count"),
        "research_expected_row_count": research_expected_row_count,
        "target_rows_status": "AVAILABLE" if rows else "EMPTY",
        "target_row_count": len(rows),
        "raw_price_row_count": raw_version.get("row_count"),
        "qfq_price_row_count": qfq_version.get("row_count"),
        "raw_coverage_status": raw_version.get("coverage_status"),
        "qfq_coverage_status": qfq_version.get("coverage_status"),
        "source_time_quality": first.get("source_time_quality"),
        "source_clock_role": first.get("capture_clock_role"),
        "capture_started_at": _json_value(first.get("capture_read_started_at")),
        "capture_validated_at": _json_value(first.get("capture_validated_at")),
        "source_available_at": _json_value(first.get("capture_available_at")),
        "time_role": "NON_PIT_RETROSPECTIVE",
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "database_write": False,
        "ddl_executed": False,
        "source_io_attempted": False,
    }


def _baostock_history_payload(
    history: Mapping[str, Any],
    *,
    sessions: Sequence[date],
    price_coordinate: str,
) -> dict[str, Any]:
    rows = list(history.get("rows") or [])
    present_dates = {row["session_date"] for row in rows}
    missing = [
        session.isoformat() for session in sessions if session not in present_dates
    ]
    return {
        **history,
        "history_rows": rows,
        "missing_history_sessions": missing,
        "price_coordinate": price_coordinate,
        "saved_session_range": [
            min(present_dates).isoformat() if present_dates else None,
            max(present_dates).isoformat() if present_dates else None,
        ],
        "history_complete": not missing,
        "history_status": "COMPLETE" if not missing else "UNKNOWN_HISTORY",
        "source_io_attempted": False,
        "database_write_attempted": False,
        "ddl_executed": False,
    }


def _read_baostock_history(
    config: Mapping[str, Any],
    *,
    source_symbol: str,
    target_session: date,
    sessions: Sequence[date],
    cutoff: date,
    price_coordinate: str,
    source_capture_id: int | None,
    starts_at_target: bool = False,
) -> dict[str, Any]:
    if source_capture_id is None:
        detail = read_baostock_price_detail(
            config,
            source_symbol=source_symbol,
            target_session=target_session,
            research_cutoff=cutoff,
        )
        if detail.get("status") == "AMBIGUOUS_CAPTURE":
            raise PreviewHttpError(
                409,
                "PRICE_CAPTURE_ID_REQUIRED",
                "目标日存在多个BaoStock捕获；请求必须指定source_capture_id",
            )
        if detail.get("status") != "AVAILABLE":
            raise PreviewHttpError(
                404,
                "PG_SOURCE_NO_MATCH",
                "PG中没有所选目标日与来源代码的BaoStock价格事实",
            )
        source_capture_id = _capture_id(detail.get("source_capture_id"))
    history = read_baostock_price_history(
        config,
        source_symbol=source_symbol,
        target_session=target_session,
        sessions=sessions,
        cutoff=cutoff,
        source_capture_id=source_capture_id,
        price_coordinate=price_coordinate,
        starts_at_target=starts_at_target,
    )
    return _baostock_history_payload(
        history,
        sessions=sessions,
        price_coordinate=price_coordinate,
    )


class _PreviewState:
    def __init__(
        self, *, research_cutoff: date, data_jobs_authorized: bool = False
    ) -> None:
        if type(research_cutoff) is not date:
            raise DiagnosticError("RESEARCH_CUTOFF_REQUIRED", "必须显式绑定研究截止日")
        self.research_cutoff = research_cutoff
        # 仅保存本服务进程最近一次实际响应错误，重启即清空。
        self.last_service_error: dict[str, Any] | None = None
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
        self.profile_contract = load_price_profile_contract(_CONFIG_ROOT).document
        self.profile_contract_revision = (
            self.profile_contract["contract_id"],
            self.profile_contract["version"],
        )
        self.data_jobs = DataJobCoordinator(
            config=self.config,
            research_cutoff=research_cutoff,
            execution_authorized=data_jobs_authorized,
        )
        self.source_d1_algorithm_version = "SOURCE_D1_PREVIEW_R1"
        self.source_d1_cache_lock = threading.Lock()
        self.source_d1_cache_key: tuple[Any, ...] | None = None
        self.source_d1_cache_result: dict[str, Any] | None = None
        self.source_d1_cache_expires_at = 0.0
        self.source_d1_cache_generation = 0
        self.source_d1_cache_result_generation = 0
        self.source_d1_inflight: dict[tuple[Any, ...], threading.Event] = {}
        self.source_sector_change_cache_lock = threading.Lock()
        self.source_sector_change_cache_key: tuple[Any, ...] | None = None
        self.source_sector_change_cache_inputs: SourceSectorChangeInputs | None = None
        self.source_sector_change_cache_expires_at = 0.0
        self.source_sector_change_cache_generation = 0
        self.source_sector_change_cache_result_generation = 0
        self.source_sector_change_inflight: dict[tuple[Any, ...], threading.Event] = {}
        self.source_pool_rps_cache_lock = threading.Lock()
        self.source_pool_rps_cache_key: tuple[Any, ...] | None = None
        self.source_pool_rps_cache_result: dict[str, Any] | None = None
        self.source_pool_rps_cache_expires_at = 0.0
        self.source_pool_rps_inflight: dict[tuple[Any, ...], threading.Event] = {}
        self.d1_material_cache_lock = threading.Lock()
        self.d1_material_cache_key: tuple[Any, ...] | None = None
        self.d1_material_cache_facts: dict[str, Any] | None = None
        self.d1_material_cache_expires_at = 0.0
        self.d1_material_inflight: threading.Event | None = None
        self.d0_watch_cache_lock = threading.Lock()
        self.d0_watch_cache_key: tuple[Any, ...] | None = None
        self.d0_watch_cache_result: tuple[Any, ...] | None = None
        self.d0_watch_cache_expires_at = 0.0
        self.d0_watch_inflight: dict[tuple[Any, ...], threading.Event] = {}

    def source_d0_watch(
        self, cache_key: tuple[Any, ...], factory: Any
    ) -> tuple[tuple[Any, ...], bool]:
        """复用同一双日来源及成员快照的D0全池事实，最多保留十分钟。"""
        while True:
            with self.d0_watch_cache_lock:
                if (
                    self.d0_watch_cache_key == cache_key
                    and self.d0_watch_cache_result is not None
                    and time.monotonic() < self.d0_watch_cache_expires_at
                ):
                    return self.d0_watch_cache_result, True
                event = self.d0_watch_inflight.get(cache_key)
                if event is None:
                    event = threading.Event()
                    self.d0_watch_inflight[cache_key] = event
                    break
            event.wait()
        try:
            result = factory()
        except BaseException:
            with self.d0_watch_cache_lock:
                self.d0_watch_inflight.pop(cache_key, None)
                event.set()
            raise
        with self.d0_watch_cache_lock:
            self.d0_watch_cache_key = cache_key
            self.d0_watch_cache_result = result
            self.d0_watch_cache_expires_at = time.monotonic() + 600
            self.d0_watch_inflight.pop(cache_key, None)
            event.set()
        return result, False

    def source_d1_material_facts(
        self, cache_key: tuple[Any, ...], factory: Any
    ) -> tuple[dict[str, Any], bool]:
        """串行复用精确绑定的D1全池材料事实，并定期刷新PG投影。"""
        while True:
            with self.d1_material_cache_lock:
                if (
                    self.d1_material_cache_key == cache_key
                    and self.d1_material_cache_facts is not None
                    and time.monotonic() < self.d1_material_cache_expires_at
                ):
                    return self.d1_material_cache_facts, True
                event = self.d1_material_inflight
                if event is None:
                    event = threading.Event()
                    self.d1_material_inflight = event
                    owner = True
                else:
                    owner = False
            if not owner:
                raise PreviewHttpError(
                    503,
                    "D1_MATERIAL_BUSY",
                    "另一项完整来源池研究正在计算，请稍候自动重试",
                )
            try:
                facts = factory()
            except BaseException:
                with self.d1_material_cache_lock:
                    if self.d1_material_inflight is event:
                        self.d1_material_inflight = None
                    event.set()
                raise
            with self.d1_material_cache_lock:
                self.d1_material_cache_key = cache_key
                self.d1_material_cache_facts = facts
                self.d1_material_cache_expires_at = (
                    time.monotonic() + _D1_MATERIAL_FACTS_CACHE_TTL_SECONDS
                )
                if self.d1_material_inflight is event:
                    self.d1_material_inflight = None
                event.set()
            return facts, False

    def source_pool_rps(
        self, cache_key: tuple[Any, ...], factory: Any
    ) -> tuple[dict[str, Any], bool]:
        """复用一个精确来源身份的完整RPS横截面，最多保留10分钟。"""
        while True:
            with self.source_pool_rps_cache_lock:
                if (
                    self.source_pool_rps_cache_key == cache_key
                    and self.source_pool_rps_cache_result is not None
                    and time.monotonic() < self.source_pool_rps_cache_expires_at
                ):
                    return self.source_pool_rps_cache_result, True
                event = self.source_pool_rps_inflight.get(cache_key)
                if event is None:
                    event = threading.Event()
                    self.source_pool_rps_inflight[cache_key] = event
                    break
            event.wait()
        try:
            result = factory()
        except BaseException:
            with self.source_pool_rps_cache_lock:
                if self.source_pool_rps_inflight.get(cache_key) is event:
                    del self.source_pool_rps_inflight[cache_key]
                event.set()
            raise
        with self.source_pool_rps_cache_lock:
            self.source_pool_rps_cache_key = cache_key
            self.source_pool_rps_cache_result = result
            self.source_pool_rps_cache_expires_at = time.monotonic() + 600
            if self.source_pool_rps_inflight.get(cache_key) is event:
                del self.source_pool_rps_inflight[cache_key]
            event.set()
        return result, False

    def source_d1_discovery(
        self, cache_key: tuple[Any, ...], factory: Any
    ) -> tuple[dict[str, Any], bool]:
        """复用同一Raw捕获、算法和会话窗的完整D1事实，最多保留10分钟。"""
        while True:
            with self.source_d1_cache_lock:
                now = time.monotonic()
                if (
                    self.source_d1_cache_key == cache_key
                    and self.source_d1_cache_result is not None
                    and now < self.source_d1_cache_expires_at
                ):
                    return self.source_d1_cache_result, True
                event = self.source_d1_inflight.get(cache_key)
                if event is None:
                    event = threading.Event()
                    self.source_d1_inflight[cache_key] = event
                    self.source_d1_cache_generation += 1
                    generation = self.source_d1_cache_generation
                    break
            event.wait()
        try:
            result = factory()
        except BaseException:
            with self.source_d1_cache_lock:
                if self.source_d1_inflight.get(cache_key) is event:
                    del self.source_d1_inflight[cache_key]
                event.set()
            raise
        with self.source_d1_cache_lock:
            if generation >= self.source_d1_cache_result_generation:
                self.source_d1_cache_key = cache_key
                self.source_d1_cache_result = result
                self.source_d1_cache_expires_at = time.monotonic() + 600
                self.source_d1_cache_result_generation = generation
            if self.source_d1_inflight.get(cache_key) is event:
                del self.source_d1_inflight[cache_key]
            event.set()
        return result, False

    def source_sector_change_inputs(
        self,
        cache_key: tuple[Any, ...],
        factory: Any,
    ) -> tuple[SourceSectorChangeInputs, bool]:
        """为连续页面筛选复用一份精确绑定的全池只读输入，最多保留10分钟。"""
        while True:
            with self.source_sector_change_cache_lock:
                now = time.monotonic()
                if (
                    self.source_sector_change_cache_key == cache_key
                    and self.source_sector_change_cache_inputs is not None
                    and now < self.source_sector_change_cache_expires_at
                ):
                    return self.source_sector_change_cache_inputs, True
                event = self.source_sector_change_inflight.get(cache_key)
                if event is None:
                    event = threading.Event()
                    self.source_sector_change_inflight[cache_key] = event
                    self.source_sector_change_cache_generation += 1
                    generation = self.source_sector_change_cache_generation
                    break
            event.wait()
        try:
            inputs = factory()
        except BaseException:
            with self.source_sector_change_cache_lock:
                if self.source_sector_change_inflight.get(cache_key) is event:
                    del self.source_sector_change_inflight[cache_key]
                event.set()
            raise
        with self.source_sector_change_cache_lock:
            if generation >= self.source_sector_change_cache_result_generation:
                self.source_sector_change_cache_key = cache_key
                self.source_sector_change_cache_inputs = inputs
                self.source_sector_change_cache_expires_at = time.monotonic() + 600
                self.source_sector_change_cache_result_generation = generation
            if self.source_sector_change_inflight.get(cache_key) is event:
                del self.source_sector_change_inflight[cache_key]
            event.set()
        return inputs, False

    def completed_source_sector_change_inputs(
        self, cache_key: tuple[Any, ...]
    ) -> SourceSectorChangeInputs | None:
        """只读读取已完成的精确全池输入，不等待或启动额外全池任务。"""
        with self.source_sector_change_cache_lock:
            if (
                self.source_sector_change_cache_key != cache_key
                or self.source_sector_change_cache_inputs is None
                or time.monotonic() >= self.source_sector_change_cache_expires_at
            ):
                return None
            return self.source_sector_change_cache_inputs

    def snapshot_identity(
        self,
        *,
        target_session: str,
        source_capture_id: str,
        price_coordinate: str = "FORWARD_ADJUSTED",
        session_count: int | None = None,
        session_start: str | None = None,
        calendar_revision: str | None = None,
    ) -> dict[str, Any]:
        fields: dict[str, Any] = {
            "identity_type": "PREVIEW_SNAPSHOT_IDENTITY_NON_PIT",
            "application_id": "STOCK_A",
            "target_session": target_session,
            "source_capture_id": source_capture_id,
            "preview_mode": "SOURCE_SYMBOL_NON_PIT_PREVIEW",
            "price_coordinate": price_coordinate,
            "core_algorithm_version": "STOCK_A_CORE_R1",
            "research_entry_version": "STOCK_A_RESEARCH_ENTRY_R1",
            "profile_contract": (
                f"{self.profile_contract['contract_id']}/"
                f"{self.profile_contract['version']}"
            ),
            "profile_contract_revision": self.profile_contract_revision,
        }
        if session_count is not None:
            fields["target_window"] = {
                "session_count": session_count,
                "session_start": session_start,
                "session_end": target_session,
                "calendar_revision": calendar_revision,
            }
        return fields


def _duplicate_free_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("JSON对象含重复字段")
        result[key] = value
    return result


def _legacy_price_identity(value: Any, path: str = "$") -> str | None:
    """拒绝重新出现的TDX行情包身份或文件来源语义。"""
    retired_keys = {
        "source_package_id",
        "before_source_package_id",
        "after_source_package_id",
        "package_sha256",
    }
    if isinstance(value, Mapping):
        for key, child in value.items():
            key_text = str(key)
            child_path = f"{path}.{key_text}"
            if key_text in retired_keys:
                return child_path
            if (
                key_text in {"dataset_id", "source_dataset_id"}
                and child == "TDX_VIPDATA_PACKAGE"
            ):
                return child_path
            found = _legacy_price_identity(child, child_path)
            if found:
                return found
    elif isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            found = _legacy_price_identity(child, f"{path}[{index}]")
            if found:
                return found
    elif isinstance(value, str) and value.startswith("TDX_PACKAGE:"):
        return path
    return None


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
        super().server_close()

    def process_request(self, request: socket.socket, client_address: Any) -> None:
        if not self._request_slots.acquire(timeout=8):
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
        self.send_header("Connection", "close")
        self.close_connection = True
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
        if payload.get("ok") is True:
            payload = decorate_api_source_symbols(
                self.preview_server.state.config, payload
            )
        retired_path = _legacy_price_identity(payload)
        if retired_path:
            status = 500
            payload = {
                "ok": False,
                "code": "RETIRED_PRICE_IDENTITY_REJECTED",
                "message": f"响应含已退役的TDX行情包身份（{retired_path}）；需要BaoStock捕获身份。",
            }
        body = json.dumps(
            _json_value(payload),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
        self._send(status, body, "application/json; charset=utf-8")

    def _error(self, error: PreviewHttpError) -> None:
        route = str(getattr(self, "path", "")).split("?", 1)[0]
        if self._host_allowed() and route in _DIAGNOSTIC_ROUTES:
            self.preview_server.state.last_service_error = {
                "observed_at": datetime.now(timezone.utc).isoformat(),
                "route": route,
                "status": error.status,
                "code": error.code,
            }
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
        if isinstance(error, DataJobError):
            self._error(PreviewHttpError(error.status, error.code, str(error)))
            return
        if isinstance(error, DataJobPgError):
            status = (
                409
                if error.code
                in {
                    "DATA_JOB_BUSY",
                    "DATA_JOB_STATE_CONFLICT",
                    "DATA_JOB_SOURCE_REVISION_CONFLICT",
                    "DATA_JOB_PROGRESS_CONFLICT",
                }
                else 503
            )
            self._error(
                PreviewHttpError(status, error.code, "数据任务PG状态读取或写入失败。")
            )
            return
        if isinstance(error, PgResearchBindingError):
            if error.code.endswith("MISMATCH") or error.code.endswith("AMBIGUOUS"):
                status = 409
            elif error.code.endswith("INVALID"):
                status = 400
            elif error.code.endswith("UNAVAILABLE") or error.code.endswith("MISSING"):
                status = 404
            else:
                status = 503
            self._error(PreviewHttpError(status, error.code, str(error)))
            return
        if isinstance(error, PgPriceProfileError):
            if error.code.endswith("INVALID") or error.code in {
                "PROFILE_PAGE_INVALID",
                "PROFILE_FILTER_INVALID",
                "PROFILE_REVISION_INVALID",
                "PROFILE_SYMBOL_INVALID",
                "PROFILE_DATE_AFTER_CUTOFF",
                "PROFILE_CUTOFF_REQUIRED",
                "PROFILE_CHANGE_DATE_INVALID",
            }:
                status = 400
            elif error.code in {
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
        if isinstance(error, DiagnosticError):
            status = 400 if error.code.endswith("INVALID") else 503
            self._error(PreviewHttpError(status, error.code, str(error)))
            return
        if isinstance(
            error, (PriceProfileContractError, research_entry.ResearchEntryError)
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
        method = getattr(self, "command", "")
        diagnostic = {
            "time": datetime.now(timezone.utc).isoformat(),
            "request_id": uuid4().hex,
            "method": method if method in {"GET", "HEAD", "POST"} else "UNKNOWN",
            "route": route if route in _DIAGNOSTIC_ROUTES else "UNKNOWN_ROUTE",
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

    def _source_launch_watch_focus_saved_page(
        self,
        *,
        before_session: date,
        target_session: date,
        research_cutoff: date,
        before_source_capture_id: int | None,
        source_capture_id: int,
        industry_snapshot_id: int | None,
        concept_snapshot_id: int | None,
        offset: int,
        limit: int,
        watch_group: str,
        d1_history_session_count: int | None,
        history_session_count: int | None,
        profile_revision: str | None,
        board_kind: str | None,
        board_native_id: str | None,
    ) -> dict[str, Any]:
        """从已保存事实按组分页，范围限于当前页和选中板块。"""
        request_started = time.perf_counter()
        request_cpu_started = time.process_time()
        config = self.preview_server.state.config
        before_source = read_source_version_context(
            config,
            target_session=before_session,
            source_capture_id=before_source_capture_id,
            research_cutoff=research_cutoff,
        )
        after_source = read_source_version_context(
            config,
            target_session=target_session,
            source_capture_id=source_capture_id,
            research_cutoff=research_cutoff,
        )
        if (
            before_source.get("status") != "AVAILABLE"
            or after_source.get("status") != "AVAILABLE"
            or (
                before_source_capture_id is not None
                and before_source.get("source_capture_id") != before_source_capture_id
            )
            or after_source.get("source_capture_id") != source_capture_id
        ):
            raise PreviewHttpError(
                409,
                "SOURCE_LAUNCH_WATCH_SOURCE_IDENTITY_MISMATCH",
                "相邻BaoStock价格捕获不完整，不能读取来源观察分页",
            )
        board_snapshot_id = (
            industry_snapshot_id
            if board_kind == "INDUSTRY"
            else concept_snapshot_id
            if board_kind == "CONCEPT"
            else None
        )
        if board_kind is not None and board_snapshot_id is None:
            raise PreviewHttpError(
                409,
                "SOURCE_LAUNCH_WATCH_BOARD_SNAPSHOT_UNAVAILABLE",
                "所选板块类型没有精确绑定的成员快照",
            )

        from stock_a.storage.postgres.source_launch_watch_evidence import (
            read_source_launch_watch_board_rps_comparison,
            read_source_launch_watch_d0_boards,
            read_source_launch_watch_evidence,
            read_source_launch_watch_page,
        )

        try:
            page_result = read_source_launch_watch_page(
                config,
                before_session=before_session,
                target_session=target_session,
                source_capture_id=source_capture_id,
                before_capture_id=before_source["source_capture_id"],
                target_capture_id=after_source["source_capture_id"],
                expected_source_count=after_source["research_expected_row_count"],
                watch_group=watch_group,
                offset=offset,
                limit=limit,
                d1_history_session_count=d1_history_session_count,
                selected_history_session_count=history_session_count,
                selected_profile_revision=profile_revision,
                board_kind=board_kind,
                board_native_id=board_native_id,
                board_snapshot_id=board_snapshot_id,
            )
        except ValueError as error:
            if not str(error).startswith("来源观察PG分页键无效："):
                raise
            raise PreviewHttpError(
                400,
                "SOURCE_LAUNCH_WATCH_PAGE_ARGUMENT_INVALID",
                str(error),
            ) from error
        if page_result.get("status") != "AVAILABLE" or not page_result.get(
            "partition_closed"
        ):
            raise PreviewHttpError(
                409,
                "SOURCE_LAUNCH_WATCH_PARTITION_INCOMPLETE",
                "目标BaoStock来源池或互斥观察分组未闭合，已拒绝显示局部总数",
            )

        page_rows = page_result["results"]
        page_symbols = [item["source_symbol"] for item in page_rows]
        from stock_a.storage.postgres.formal_admission import (
            read_source_formal_admission_day,
        )

        d0_scope = page_result.get("d0_scope")
        if not isinstance(d0_scope, Mapping):
            formal_admission_day = {
                "status": "NOT_COMPUTED",
                "reason_code": "FORMAL_ADMISSION_D0_VERSION_NOT_AVAILABLE",
                "by_symbol": {},
                "counts": None,
            }
        else:
            seed_scope = page_result.get("seed_scope") or {}
            formal_admission_day = read_source_formal_admission_day(
                config,
                target_session=target_session,
                source_capture_id=after_source["source_capture_id"],
                expected_source_count=after_source["research_expected_row_count"],
                algorithm_contract=d0_scope["algorithm_contract"],
                session_contract=d0_scope["session_contract"],
                calendar_revision=d0_scope["calendar_revision"],
                seed_producer_version=seed_scope.get("producer_version"),
                source_symbols=sorted(page_symbols),
                profile_history_session_count=history_session_count,
                profile_revision=profile_revision,
            )
        t0_summary_query_started = time.perf_counter()
        from stock_a.storage.postgres.t0_early_observations import (
            read_source_t0_early_triage_summaries_for_symbols,
        )

        t0_summaries = (
            read_source_t0_early_triage_summaries_for_symbols(
                config,
                target_session=target_session,
                source_capture_id=after_source["source_capture_id"],
                source_symbols=page_symbols,
                selected_snapshot_ids={
                    kind: snapshot_id
                    for kind, snapshot_id in (
                        ("INDUSTRY", industry_snapshot_id),
                        ("CONCEPT", concept_snapshot_id),
                    )
                    if type(snapshot_id) is int and snapshot_id > 0
                },
            )
            if page_symbols
            else {}
        )
        t0_summary_query_seconds = time.perf_counter() - t0_summary_query_started
        if page_symbols:
            evidence = read_source_launch_watch_evidence(
                config,
                target_session=target_session,
                source_capture_id=after_source["source_capture_id"],
                expected_source_count=after_source["research_expected_row_count"],
                source_symbols=page_symbols,
                d1_history_session_count=d1_history_session_count,
                selected_history_session_count=history_session_count,
                selected_profile_revision=profile_revision,
            )
            from stock_a.application.source_d0_tdx_membership import (
                read_tdx_sector_memberships_for_symbols,
            )

            membership_read = read_tdx_sector_memberships_for_symbols(
                config,
                source_symbols=page_symbols,
                target_session=target_session,
                industry_snapshot_id=industry_snapshot_id,
                concept_snapshot_id=concept_snapshot_id,
            )
        else:
            evidence = {
                "status": "AVAILABLE_EMPTY",
                "seed_status": page_result["scope_status"]["seed"],
                "d1_status": page_result["scope_status"]["d1"],
                "rules_status": page_result["scope_status"]["rules"],
                "seed_scope": page_result.get("seed_scope"),
                "rule_scope": None,
                "d1_scope": None,
                "by_symbol": {},
                "execution": {
                    "transaction": "REPEATABLE READ READ ONLY",
                    "database_write_attempted": False,
                    "ddl_executed": False,
                    "dml_executed": False,
                },
            }
            membership_read = {
                "status": "AVAILABLE_EMPTY",
                "memberships": [],
                "snapshots": [],
                "missing_membership_kinds": [
                    kind
                    for kind, value in (
                        ("INDUSTRY", industry_snapshot_id),
                        ("CONCEPT", concept_snapshot_id),
                    )
                    if value is None
                ],
                "execution": {
                    "transaction": "REPEATABLE READ READ ONLY",
                    "database_write_attempted": False,
                    "select_statement_count": 0,
                    "select_rows_returned": 0,
                },
            }

        memberships_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for membership in membership_read.get("memberships", []):
            memberships_by_symbol[membership["source_symbol"]].append(membership)
        home_board_result = {"status": "NOT_REQUESTED", "boards": []}
        if watch_group == "D0_NEW" and board_kind is None and limit == 30:
            d0_scope_status = page_result["scope_status"]["d0"]
            if d0_scope_status == "AVAILABLE":
                home_board_result = read_source_launch_watch_d0_boards(
                    config,
                    before_session=before_session,
                    target_session=target_session,
                    source_capture_id=source_capture_id,
                    before_capture_id=before_source["source_capture_id"],
                    target_capture_id=after_source["source_capture_id"],
                    expected_source_count=after_source["research_expected_row_count"],
                    industry_snapshot_id=industry_snapshot_id,
                    concept_snapshot_id=concept_snapshot_id,
                )
                if home_board_result.get("status") != "AVAILABLE":
                    raise PreviewHttpError(
                        409,
                        "SOURCE_LAUNCH_WATCH_BOARD_SCOPE_INCOMPLETE",
                        "完整D0事实可用时，首页板块摘要未能绑定同一范围",
                    )
            else:
                home_board_result = {
                    "status": d0_scope_status,
                    "total_board_count": None,
                    "boards": [],
                    "execution": {
                        "database_write_attempted": False,
                        "d0_board_summary_query_executed": False,
                    },
                }
        page_boards = {
            (
                membership["snapshot_id"],
                membership["board_kind"],
                membership["board_native_id"],
            ): {
                "snapshot_id": membership["snapshot_id"],
                "board_kind": membership["board_kind"],
                "board_native_id": membership["board_native_id"],
            }
            for membership in membership_read.get("memberships", [])
        }
        for board in home_board_result.get("boards", []):
            key = (
                board["snapshot_id"],
                board["board_kind"],
                board["board_native_id"],
            )
            page_boards[key] = {
                "snapshot_id": board["snapshot_id"],
                "board_kind": board["board_kind"],
                "board_native_id": board["board_native_id"],
            }
        if (
            board_kind is not None
            and board_native_id is not None
            and board_snapshot_id is not None
        ):
            page_boards[(board_snapshot_id, board_kind, board_native_id)] = {
                "snapshot_id": board_snapshot_id,
                "board_kind": board_kind,
                "board_native_id": board_native_id,
            }
        from stock_a.storage.postgres.sector_pulse_facts import (
            read_source_sector_pulse_for_boards,
        )

        pulse = read_source_sector_pulse_for_boards(
            config,
            target_session=target_session,
            source_capture_id=after_source["source_capture_id"],
            source_pool_count=after_source["capture_expected_row_count"],
            boards=list(page_boards.values()),
        )
        pulse_by_board = pulse.get("by_board", {})
        pulse_transition_by_board: dict[str, dict[str, Any]] = {}
        if page_boards:
            previous_pulse = read_source_sector_pulse_for_boards(
                config,
                target_session=before_session,
                source_capture_id=before_source["source_capture_id"],
                source_pool_count=before_source["capture_expected_row_count"],
                boards=list(page_boards.values()),
            )
            previous_by_board = previous_pulse.get("by_board", {})
            previous_revisions = {
                item.get("profile_revision")
                for item in previous_by_board.values()
                if item.get("status") == "AVAILABLE"
                and isinstance(item.get("profile_revision"), str)
            }
            target_revisions = {
                item.get("profile_revision")
                for item in pulse_by_board.values()
                if item.get("status") == "AVAILABLE"
                and isinstance(item.get("profile_revision"), str)
            }
            if len(previous_revisions) == 1 and len(target_revisions) == 1:
                target_ids = []
                for board_key, target_fact in pulse_by_board.items():
                    previous_fact = previous_by_board.get(board_key, {})
                    pulse_fact_id = target_fact.get("pulse_fact_id")
                    if (
                        target_fact.get("status") == "AVAILABLE"
                        and previous_fact.get("status") == "AVAILABLE"
                        and previous_fact.get("snapshot_id")
                        == target_fact.get("snapshot_id")
                        and type(pulse_fact_id) is int
                    ):
                        target_ids.append(pulse_fact_id)
                if target_ids:
                    from stock_a.storage.postgres.t0_early_observations import (
                        read_source_t0_pulse_transition_facts,
                    )

                    transitions = read_source_t0_pulse_transition_facts(
                        config,
                        target_pulse_ids=target_ids,
                        previous_session=before_session,
                        target_session=target_session,
                        previous_source_capture_id=before_source["source_capture_id"],
                        target_source_capture_id=after_source["source_capture_id"],
                        previous_profile_revision=next(iter(previous_revisions)),
                        target_profile_revision=next(iter(target_revisions)),
                    )
                    for board_key, target_fact in pulse_by_board.items():
                        pulse_fact_id = target_fact.get("pulse_fact_id")
                        transition = transitions.get(pulse_fact_id)
                        if transition is None:
                            continue
                        pulse_transition_by_board[board_key] = {
                            **transition,
                            "snapshot_id": target_fact.get("snapshot_id"),
                            "research_mode": "NON_PIT_RETROSPECTIVE",
                            "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
                            "price_coordinate": "FORWARD_ADJUSTED",
                            "formal_rotation_state": "NOT_EVALUATED",
                            "previous_profile_revision": next(iter(previous_revisions)),
                            "target_profile_revision": next(iter(target_revisions)),
                        }
            elif previous_revisions and target_revisions:
                for board_key, target_fact in pulse_by_board.items():
                    previous_fact = previous_by_board.get(board_key, {})
                    if (
                        target_fact.get("status") == "AVAILABLE"
                        and previous_fact.get("status") == "AVAILABLE"
                    ):
                        pulse_transition_by_board[board_key] = {
                            "status": "UNKNOWN",
                            "transition_state": "PULSE_TRANSITION_UNKNOWN",
                            "reason_codes": [
                                "PULSE_TRANSITION_PROFILE_REVISION_AMBIGUOUS"
                            ],
                            "snapshot_id": target_fact.get("snapshot_id"),
                            "research_mode": "NON_PIT_RETROSPECTIVE",
                            "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
                            "price_coordinate": "FORWARD_ADJUSTED",
                            "formal_rotation_state": "NOT_EVALUATED",
                        }
        pulse_followup_by_board: dict[str, dict[str, Any]] = {}
        pulse_followup_comparison: dict[str, dict[str, Any]] = {}
        pulse_followup_execution = {
            "reader_executed": False,
            "read_only": True,
            "raw_history_rows_returned_to_python": 0,
        }
        pulse_followup_directory: dict[str, Any] = {
            "status": "NOT_REQUESTED",
            "reason_codes": [],
            "total_board_count": 0,
            "boards": [],
        }
        selected_pulse_board = (
            board_kind is not None
            and board_native_id is not None
            and board_snapshot_id is not None
        )
        pulse_directory_fallback = (
            board_kind is None
            and watch_group == "D0_NEW"
            and page_result["scope_status"].get("d0") == "NOT_COMPUTED"
            and industry_snapshot_id is not None
            and concept_snapshot_id is not None
        )
        if selected_pulse_board or pulse_directory_fallback:
            from stock_a.storage.postgres.sector_pulse_facts import (
                read_source_sector_pulse_day,
            )
            from stock_a.storage.postgres.sector_pulse_followups import (
                read_source_sector_pulse_followups,
            )

            pulse_day = read_source_sector_pulse_day(
                config,
                target_session=target_session,
                source_capture_id=after_source["source_capture_id"],
                source_pool_count=after_source["capture_expected_row_count"],
                snapshot_ids={
                    "INDUSTRY": industry_snapshot_id,
                    "CONCEPT": concept_snapshot_id,
                },
            )
            pulse_followup_execution["pulse_board_count_read"] = sum(
                group.get("read_board_count", 0)
                for group in pulse_day.get("by_kind", {}).values()
            )
            if pulse_day.get("status") == "AVAILABLE":
                reader_pulse_day = {
                    "status": "AVAILABLE",
                    "target_session": target_session.isoformat(),
                    "profile_revision": pulse_day["profile_revision"],
                    "source_version": {
                        "source_capture_id": after_source["source_capture_id"],
                    },
                    "by_kind": {},
                }
                for kind, group in pulse_day["by_kind"].items():
                    reader_pulse_day["by_kind"][kind] = {
                        "status": group.get("status"),
                        "reason_codes": group.get("reason_codes", []),
                        "snapshot_id": group.get("snapshot_id"),
                        "expected_board_count": group.get("expected_board_count"),
                        "read_board_count": group.get("read_board_count"),
                        "facts": {
                            native_id: {
                                "state": fact.get("state"),
                                "board_native_id": fact.get("board_native_id"),
                                "board_name": fact.get("board_name"),
                                "member_count": fact.get("member_count"),
                                "pulse_fact_id": fact.get("pulse_fact_id"),
                                "catalog_id": fact.get("catalog_id"),
                            }
                            for native_id, fact in group.get("facts", {}).items()
                        },
                    }
                calendar = _official_calendar(research_cutoff)
                calendar_sessions = tuple(calendar["sessions"])
                try:
                    target_index = calendar_sessions.index(target_session)
                except ValueError as error:
                    raise PreviewHttpError(
                        409,
                        "PULSE_FOLLOWUP_CALENDAR_TARGET_MISSING",
                        "所选板块PULSE目标日不在绑定的官方日历中",
                    ) from error
                horizon_targets = {
                    horizon: (
                        calendar_sessions[target_index + horizon]
                        if target_index + horizon < len(calendar_sessions)
                        else None
                    )
                    for horizon in (1, 3, 5)
                }
                followup_read = read_source_sector_pulse_followups(
                    config,
                    pulse_day=reader_pulse_day,
                    research_cutoff=research_cutoff,
                    horizon_targets=horizon_targets,
                    calendar_identity={
                        "calendar_id": calendar["calendar_id"],
                        "calendar_revision": calendar["calendar_revision"],
                    },
                )
                pulse_followup_execution.update(
                    {
                        "reader_executed": True,
                        "reader_status": followup_read.get("status"),
                        "t0_session": target_session.isoformat(),
                        "source_capture_id": after_source["source_capture_id"],
                        "calendar_revision": calendar["calendar_revision"],
                    }
                )
                if selected_pulse_board:
                    selected_group = followup_read.get("by_kind", {}).get(
                        board_kind, {}
                    )
                    selected_fact = selected_group.get("facts", {}).get(board_native_id)
                    if isinstance(selected_fact, Mapping):
                        selected_followup = dict(selected_fact)
                        t1_result = next(
                            (
                                item
                                for item in selected_fact.get("horizons", ())
                                if item.get("horizon_sessions") == 1
                            ),
                            None,
                        )
                        selected_followup["status"] = (
                            t1_result.get("status")
                            if isinstance(t1_result, Mapping)
                            else "NOT_COMPUTED"
                        )
                        selected_followup["reason_codes"] = (
                            list(t1_result.get("reason_codes", []))
                            if isinstance(t1_result, Mapping)
                            else ["FOLLOWUP_T1_RESULT_NOT_AVAILABLE"]
                        )
                        selected_followup.update(
                            {
                                "research_mode": "NON_PIT_RETROSPECTIVE",
                                "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
                                "price_coordinate": "FORWARD_ADJUSTED",
                            }
                        )
                        pulse_followup_by_board[f"{board_kind}:{board_native_id}"] = (
                            selected_followup
                        )
                    else:
                        pulse_followup_by_board[f"{board_kind}:{board_native_id}"] = {
                            "status": selected_group.get("status", "NOT_COMPUTED"),
                            "reason_codes": selected_group.get(
                                "reason_codes",
                                ["PULSE_FOLLOWUP_BOARD_NOT_IN_SNAPSHOT"],
                            ),
                            "horizons": [],
                            "research_mode": "NON_PIT_RETROSPECTIVE",
                            "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
                            "price_coordinate": "FORWARD_ADJUSTED",
                        }
                    if isinstance(selected_group.get("comparison"), Mapping):
                        pulse_followup_comparison[board_kind] = selected_group[
                            "comparison"
                        ]
                else:
                    snapshot_catalog = read_tdx_sector_source_snapshot_catalog(config)
                    snapshot_by_id = {
                        item["snapshot_id"]: item
                        for item in snapshot_catalog.get("snapshots", [])
                    }
                    pulse_state_counts: Counter[str] = Counter()
                    t1_status_counts: Counter[str] = Counter()
                    fallback_boards = []
                    for kind in ("INDUSTRY", "CONCEPT"):
                        pulse_group = pulse_day["by_kind"].get(kind, {})
                        outcome_group = followup_read.get("by_kind", {}).get(kind, {})
                        pulse_followup_comparison[kind] = outcome_group.get(
                            "comparison", {}
                        )
                        snapshot_id = pulse_group.get("snapshot_id")
                        snapshot = snapshot_by_id.get(snapshot_id, {})
                        for board_id, pulse_fact in pulse_group.get(
                            "facts", {}
                        ).items():
                            pulse_state_counts[pulse_fact.get("state", "UNKNOWN")] += 1
                            source_quality_parts = str(
                                pulse_fact.get("snapshot_source_quality", "UNKNOWN")
                            ).split(";")
                            unmapped_value = next(
                                (
                                    part.partition("=")[2]
                                    for part in source_quality_parts
                                    if part.startswith("UNMAPPED_SOURCE_ROWS=")
                                ),
                                None,
                            )
                            outcome = outcome_group.get("facts", {}).get(board_id, {})
                            horizons = outcome.get("horizons", [])
                            t1_result = next(
                                (
                                    item
                                    for item in horizons
                                    if item.get("horizon_sessions") == 1
                                ),
                                None,
                            )
                            t1_status = (
                                t1_result.get("status")
                                if isinstance(t1_result, Mapping)
                                else "NOT_COMPUTED"
                            )
                            t1_status_counts[t1_status] += 1
                            followup = dict(outcome)
                            followup.update(
                                {
                                    "status": t1_status,
                                    "reason_codes": list(
                                        t1_result.get("reason_codes", [])
                                        if isinstance(t1_result, Mapping)
                                        else ["FOLLOWUP_T1_RESULT_NOT_AVAILABLE"]
                                    ),
                                    "research_mode": "NON_PIT_RETROSPECTIVE",
                                    "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
                                    "price_coordinate": "FORWARD_ADJUSTED",
                                }
                            )
                            fallback_boards.append(
                                {
                                    "board_kind": kind,
                                    "board_native_id": board_id,
                                    "board_name": pulse_fact.get(
                                        "board_name", board_id
                                    ),
                                    "snapshot_id": snapshot_id,
                                    "observed_at": snapshot.get("observed_at"),
                                    "membership_source_quality": {
                                        "quality": source_quality_parts[0],
                                        "source_unmapped_rows": (
                                            int(unmapped_value)
                                            if unmapped_value
                                            and unmapped_value.isdecimal()
                                            else None
                                        ),
                                    },
                                    "cross_period_pulse": {
                                        key: pulse_fact.get(key)
                                        for key in (
                                            "status",
                                            "state",
                                            "dq5_pp",
                                            "breadth_delta1",
                                            "reason_codes",
                                            "snapshot_id",
                                            "member_count",
                                            "snapshot_source_quality",
                                            "research_mode",
                                            "membership_mode",
                                            "price_coordinate",
                                            "formal_rotation_state",
                                        )
                                    },
                                    "cross_period_pulse_transition": {
                                        "status": "NOT_COMPUTED",
                                        "transition_state": "NOT_COMPUTED",
                                        "reason_codes": [
                                            "PULSE_TRANSITION_NOT_READ_FOR_FOLLOWUP_DIRECTORY"
                                        ],
                                        "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
                                    },
                                    "cross_period_pulse_followup": followup,
                                    "pulse_followup_only": True,
                                    "home_featured": False,
                                    "observation_basis": "只读已存T0 PULSE与后继结果；D0保持NOT_COMPUTED。",
                                    "observation_rank": len(fallback_boards) + 1,
                                }
                            )
                    fallback_boards.sort(
                        key=lambda item: (
                            item["board_kind"],
                            item["board_name"],
                            item["board_native_id"],
                        )
                    )
                    for index, board in enumerate(fallback_boards, start=1):
                        board["observation_rank"] = index
                    pulse_followup_directory = {
                        "status": "AVAILABLE",
                        "target_session": target_session.isoformat(),
                        "research_cutoff": research_cutoff.isoformat(),
                        "research_mode": "NON_PIT_RETROSPECTIVE",
                        "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
                        "price_coordinate": "FORWARD_ADJUSTED",
                        "total_board_count": len(fallback_boards),
                        "pulse_state_counts": dict(pulse_state_counts),
                        "t1_status_counts": dict(t1_status_counts),
                        "comparison": pulse_followup_comparison,
                        "boards": fallback_boards,
                    }
                    pulse_followup_execution[
                        "pulse_fallback_directory_rows_returned_to_python"
                    ] = len(fallback_boards)
            else:
                if selected_pulse_board:
                    pulse_followup_by_board[f"{board_kind}:{board_native_id}"] = {
                        "status": pulse_day.get("status", "NOT_COMPUTED"),
                        "reason_codes": pulse_day.get(
                            "reason_codes", ["PULSE_DAY_NOT_AVAILABLE"]
                        ),
                        "horizons": [],
                        "research_mode": "NON_PIT_RETROSPECTIVE",
                        "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
                        "price_coordinate": "FORWARD_ADJUSTED",
                    }
                elif pulse_directory_fallback:
                    pulse_followup_directory = {
                        "status": pulse_day.get("status", "NOT_COMPUTED"),
                        "reason_codes": pulse_day.get(
                            "reason_codes", ["PULSE_DAY_NOT_AVAILABLE"]
                        ),
                        "total_board_count": None,
                        "boards": [],
                    }
                pulse_followup_execution["reader_status"] = pulse_day.get(
                    "status", "NOT_COMPUTED"
                )
        quality_by_source = {
            snapshot.get("source_kind"): snapshot.get("source_quality_counts") or {}
            for snapshot in membership_read.get("snapshots", [])
        }
        quality_by_kind = {
            "INDUSTRY": quality_by_source.get("TDX_TDXHY_CFG", {}),
            "CONCEPT": quality_by_source.get("TDX_INFOHARBOR_BLOCK_DAT", {}),
        }
        for index, board in enumerate(home_board_result.get("boards", []), start=1):
            key = f"{board['board_kind']}:{board['board_native_id']}"
            board["cross_period_pulse"] = pulse_by_board.get(
                key,
                {
                    "status": "NOT_COMPUTED",
                    "reason_codes": ["PULSE_FACTS_NOT_SAVED_FOR_EXACT_BOARD"],
                    "snapshot_id": board["snapshot_id"],
                    "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
                },
            )
            board["cross_period_pulse_transition"] = pulse_transition_by_board.get(
                key,
                {
                    "status": "NOT_COMPUTED",
                    "transition_state": "NOT_COMPUTED",
                    "reason_codes": [
                        "PULSE_TRANSITION_FACTS_NOT_SAVED_FOR_EXACT_INPUT"
                    ],
                    "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
                },
            )
            quality_counts = quality_by_kind.get(board["board_kind"], {})
            board["membership_source_quality"] = {
                "quality": quality_counts.get("quality", "UNKNOWN"),
                "source_unmapped_rows": quality_counts.get("unmapped_source_rows"),
            }
            board["home_selection_reason"] = (
                "首页按已证D0新增成员数降序；数量相同按行业、概念、板块名称和编号稳定排序。"
                if board["home_featured"]
                else (
                    "首页最多展示15个板块；完整列表仍可搜索和分页，本板按同一D0成员数顺序"
                    f"排第{board['observation_rank']}位。"
                )
            )
            board["observation_basis"] = (
                "仅按已存D0新增成员数排列；PULSE只作为同日上下文。"
            )
            board["observation_rank"] = board.get("observation_rank") or index
        board_comparison: dict[str, dict[str, Any]] = {}
        if board_kind is not None and page_symbols and page_result.get("seed_scope"):
            board_comparison = read_source_launch_watch_board_rps_comparison(
                config,
                target_session=target_session,
                source_capture_id=after_source["source_capture_id"],
                source_symbols=page_symbols,
                seed_scope=page_result["seed_scope"],
                board_kind=board_kind,
                board_native_id=board_native_id,
                board_snapshot_id=board_snapshot_id,
            )

        evidence_by_symbol = evidence.get("by_symbol", {})
        from stock_a.storage.postgres.research_enrollment import (
            read_enrollment_followups,
            read_enrollments_for_symbols,
        )

        enrollment_by_symbol = read_enrollments_for_symbols(
            config,
            target=target_session,
            source_capture_id=source_capture_id,
            target_capture_id=after_source["source_capture_id"],
            before=before_session,
            before_capture=before_source["source_capture_id"],
            include_d0=page_result["scope_status"]["d0"] == "AVAILABLE",
            symbols=page_symbols,
            rule_scope=(
                evidence.get("rule_scope")
                if isinstance(evidence.get("rule_scope"), Mapping)
                else None
            ),
        )
        calendar_sessions = _official_calendar(research_cutoff)["sessions"]
        successor_sessions = tuple(
            day for day in calendar_sessions if target_session < day <= research_cutoff
        )
        horizon_due_sessions = tuple(
            day for day in calendar_sessions if day > target_session
        )[:20]
        if target_session not in calendar_sessions:
            raise PreviewHttpError(
                409,
                "SOURCE_LAUNCH_WATCH_TARGET_CALENDAR_UNAVAILABLE",
                "目标会话不在当前官方日历范围内，不能读取个股后继结果",
            )
        from stock_a.storage.postgres.source_launch_watch_evidence import (
            read_source_seed_t1_outcomes,
        )

        seed_t1_by_symbol = read_source_seed_t1_outcomes(
            config,
            target_session=target_session,
            research_cutoff=research_cutoff,
            source_capture_id=after_source["source_capture_id"],
            expected_source_count=after_source["research_expected_row_count"],
            source_symbols=page_symbols,
            successor_sessions=successor_sessions,
        )
        for seed_t1 in seed_t1_by_symbol.values():
            seed_t1["d0_transition_status"] = page_result["scope_status"]["d0"]
            seed_t1["rule_status"] = page_result["scope_status"]["rules"]
            seed_t1["d1_status"] = page_result["scope_status"]["d1"]
        rule_enrollment_ids = [
            item["id"]
            for entries in enrollment_by_symbol.values()
            for item in entries
            if item.get("fact_type") == "RULE"
        ]
        d0_enrollment_ids = [
            item["id"]
            for entries in enrollment_by_symbol.values()
            for item in entries
            if item.get("fact_type") == "D0"
        ]
        followups = {}
        if rule_enrollment_ids:
            from stock_a.storage.postgres.source_observation_segments import (
                read_source_observation_segment_followups,
            )

            followups.update(
                read_source_observation_segment_followups(
                    config,
                    target=target_session,
                    cutoff=research_cutoff,
                    enrollment_ids=rule_enrollment_ids,
                    horizon_due_sessions=horizon_due_sessions,
                )
            )
        if d0_enrollment_ids:
            followups.update(
                read_enrollment_followups(
                    config,
                    target=target_session,
                    cutoff=research_cutoff,
                    enrollment_ids=d0_enrollment_ids,
                    successor_sessions=successor_sessions,
                    horizon_due_sessions=horizon_due_sessions,
                )
                if target_session in calendar_sessions
                else {
                    item: {
                        "status": "UNKNOWN",
                        "reason": "CALENDAR_WINDOW_UNAVAILABLE",
                    }
                    for item in d0_enrollment_ids
                }
            )
        group_counts = page_result["group_counts"]
        global_total = (
            group_counts.get("D0_NEW", 0)
            if page_result["scope_status"]["d0"] == "AVAILABLE"
            else None
        )
        results = []
        for rank, row in enumerate(page_rows, start=offset + 1):
            symbol = row["source_symbol"]
            item_evidence = evidence_by_symbol.get(symbol, {})
            if evidence.get("rule_scope") is not None:
                item_evidence = {
                    **item_evidence,
                    "rules": {
                        **item_evidence.get("rules", {}),
                        "scope": evidence["rule_scope"],
                    },
                }
            d0_change = {
                "transition": row.get("transition"),
                "before_state": row.get("before_state"),
                "after_state": row.get("after_state"),
            }
            group = row["watch_group"]
            reasons = set(row.get("unknown_reason_codes") or [])
            for scope in ("seed", "d1", "rules"):
                detail = item_evidence.get(scope, {})
                reasons.update(detail.get("reason_codes", ()))
                if scope == "seed":
                    for name in ("source_seed", "s1", "s2", "safety"):
                        reasons.update(detail.get(name, {}).get("reasons", ()))
                elif scope == "d1":
                    reasons.update(detail.get("target_price_reasons", ()))
                else:
                    for fact in detail.get("facts", {}).values():
                        reasons.update(fact.get("reasons", ()))
            if group == "UNKNOWN" and not reasons:
                reasons.add("UNKNOWN_REASON_NOT_PERSISTED")
            member_rows = memberships_by_symbol.get(symbol, [])
            enriched_memberships = []
            for membership in member_rows:
                key = f"{membership['board_kind']}:{membership['board_native_id']}"
                enriched_memberships.append(
                    {
                        **membership,
                        "pulse_context": pulse_by_board.get(
                            key,
                            {
                                "status": "NOT_COMPUTED",
                                "reason_codes": [
                                    "PULSE_FACTS_NOT_SAVED_FOR_EXACT_BOARD"
                                ],
                                "snapshot_id": membership["snapshot_id"],
                                "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
                            },
                        ),
                    }
                )
            enrollments = enrollment_by_symbol.get(symbol, [])
            for enrollment in enrollments:
                enrollment["followup"] = followups[enrollment["id"]]
                enrollment["early_status"] = (
                    "AVAILABLE"
                    if row.get("t0_triage_summary", {}).get("status") == "AVAILABLE"
                    else "NO_STORED_FACT"
                )
            followup_states = {item["followup"]["status"] for item in enrollments}
            followup_reasons = {item["followup"]["reason"] for item in enrollments}
            followup_status = (
                next(iter(followup_states))
                if len(followup_states) == 1
                else "MULTIPLE"
                if followup_states
                else "NOT_COMPUTED"
            )
            followup_reason = (
                next(iter(followup_reasons))
                if len(followup_reasons) == 1
                else "SEE_RULE_DETAILS"
                if followup_reasons
                else "NO_STORED_FACT"
            )
            t0_triage_summary = _t0_list_summary(
                _bind_t0_followup_status(
                    t0_summaries.get(symbol, row.get("t0_triage_summary") or {}),
                    target_session=target_session,
                    research_cutoff=research_cutoff,
                    stored_status=followup_status,
                    stored_reason=followup_reason,
                )
            )
            t0_status = (
                "ENTRY"
                if any(item["kind"] == "ENTRY" for item in enrollments)
                else "DAILY_LANDMARK"
                if any(item["kind"] == "DAILY_LANDMARK" for item in enrollments)
                else "FIRST_OBSERVED_LEFT_CENSORED"
                if enrollments
                else "UNKNOWN"
                if group == "UNKNOWN"
                else "KNOWN_NO_SIGNAL"
                if group == "KNOWN_NO_SIGNAL"
                else "NOT_COMPUTED"
            )
            source_version = {
                "source_capture_id": after_source["source_capture_id"],
                "target_session": target_session.isoformat(),
                "raw_row_count": after_source["raw_row_count"],
                "source_time_quality": after_source["source_time_quality"],
                "source_clock_role": after_source["source_clock_role"],
                "provider": after_source["provider"],
                "price_coordinates": after_source["price_coordinates"],
            }
            formal_d0_admission = present_source_formal_admission(
                stored_day=formal_admission_day,
                source_symbol=symbol,
                target_session=target_session,
                source_capture_id=after_source["source_capture_id"],
                d0_evidence=item_evidence.get("d0"),
            )
            item_evidence = {
                **item_evidence,
                "formal_d0_admission": formal_d0_admission,
            }
            results.append(
                {
                    **row,
                    "d0_change": d0_change,
                    "observation_reason": {
                        "D0_NEW": "已保存D0 FALSE→TRUE相邻会话事实",
                        "SEED_RULE": "已保存Seed、S1/S2或冻结规则TRUE事实",
                        "D1_MATERIAL": "已保存D1材料变化锚事实",
                        "UNKNOWN": "目标条件存在缺失或不能判定的证据",
                        "KNOWN_NO_SIGNAL": "完整已保存条件未证任何观察线索",
                    }[group],
                    "unknown_reason_codes": sorted(reasons),
                    "research_evidence": item_evidence,
                    "seed_t1_outcome": seed_t1_by_symbol.get(symbol),
                    "source_version": source_version,
                    "t0_observation": {
                        "status": t0_status,
                        "enrollments": enrollments,
                        "triage_summary": t0_triage_summary,
                        "reason": "NO_STORED_FACT"
                        if not enrollments and t0_status == "NOT_COMPUTED"
                        else None,
                        "followup_status": followup_status,
                        "followup_reason": followup_reason,
                        "target_session": target_session.isoformat(),
                        "source_capture_id": after_source["source_capture_id"],
                        "profile_revision": (page_result.get("seed_scope") or {}).get(
                            "profile_revision"
                        ),
                        "rule_version": (evidence.get("rule_scope") or {}).get(
                            "rule_version"
                        ),
                        "scope": "NON_PIT_EXPLORATORY",
                        "price_coordinate": "FORWARD_ADJUSTED",
                        "research_only": True,
                        "formal_d0_admission": formal_d0_admission,
                    },
                    "also_in_sectors": enriched_memberships,
                    "membership_status": membership_read.get("status", "UNKNOWN"),
                    "board_comparison": board_comparison.get(symbol),
                    "research_reading_order": {
                        "rank": rank,
                        "contract": "T0_RESEARCH_TRIAGE_R1",
                        "triage_state": row.get("t0_triage_state", "NO_T0_FACT"),
                        "triage_rank": row.get("t0_triage_rank", 5),
                        "home_featured": watch_group == "D0_NEW" and rank <= 30,
                        "tie_break": "SOURCE_SYMBOL_ASC",
                    },
                    "home_selection_reason": (
                        "按已存T0证据分型排序；同层按来源代码升序。"
                        "此顺序只供研究阅读，不改变D0、五组资格或正式Priority。"
                    ),
                }
            )

        selected_total = page_result["filtered_count"]
        next_offset = (
            offset + len(results) if offset + len(results) < selected_total else None
        )
        response_boards = home_board_result.get("boards", [])
        if board_kind is not None and board_native_id is not None:
            board_key = f"{board_kind}:{board_native_id}"
            response_boards = [
                {
                    "board_kind": board_kind,
                    "board_native_id": board_native_id,
                    "cross_period_pulse": pulse_by_board.get(board_key),
                    "cross_period_pulse_transition": pulse_transition_by_board.get(
                        board_key,
                        {
                            "status": "NOT_COMPUTED",
                            "transition_state": "NOT_COMPUTED",
                            "reason_codes": [
                                "PULSE_TRANSITION_FACTS_NOT_SAVED_FOR_EXACT_INPUT"
                            ],
                            "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
                        },
                    ),
                    "cross_period_pulse_followup": pulse_followup_by_board.get(
                        board_key,
                        {
                            "status": "NOT_COMPUTED",
                            "reason_codes": [
                                "PULSE_FOLLOWUP_NOT_READ_FOR_BOARD_DETAIL"
                            ],
                            "horizons": [],
                            "research_mode": "NON_PIT_RETROSPECTIVE",
                            "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
                            "price_coordinate": "FORWARD_ADJUSTED",
                        },
                    ),
                }
            ]
        response = {
            "ok": True,
            "contract": "STOCK_A_SOURCE_LAUNCH_WATCH_API_R1",
            "request_identity": {
                "before_session": before_session.isoformat(),
                "target_session": target_session.isoformat(),
                "research_cutoff": research_cutoff.isoformat(),
                "source_capture_id": source_capture_id,
                "industry_snapshot_id": industry_snapshot_id,
                "concept_snapshot_id": concept_snapshot_id,
            },
            "source_change_status": {
                "transition_scope": "SAVED_SOURCE_D0_ADJACENT_FACTS",
                "status": page_result["scope_status"]["d0"],
                "price_coordinate": "FORWARD_ADJUSTED",
                "pit_status": "NON_PIT_RETROSPECTIVE",
                "source_key_set": {
                    "after_count": page_result["target_source_count"],
                    "after_distinct_count": page_result["target_distinct_source_count"],
                    "partition_closed": page_result["partition_closed"],
                },
            },
            "membership_status": {
                "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
                "snapshots": membership_read.get("snapshots", []),
                "missing_membership_kinds": membership_read.get(
                    "missing_membership_kinds", []
                ),
                "page_symbol_count": len(page_symbols),
                "mapped_distinct_source_symbol_count": membership_read.get(
                    "matched_source_symbol_count", 0
                ),
                "membership_relation_count": membership_read.get(
                    "membership_relation_count", 0
                ),
            },
            "filter": {
                "board_kind": board_kind,
                "board_native_id": board_native_id,
                "global_total": global_total,
                "filtered_total": selected_total,
                "watch_group": watch_group,
            },
            "watch_groups": {
                "counts": group_counts,
                "target_source_pool_denominator": page_result["target_source_count"],
                "partition_closed": page_result["partition_closed"],
                "unassigned_known_false_count": 0,
                "grouping": "D0_NEW > SEED_RULE > D1_MATERIAL > UNKNOWN > KNOWN_NO_SIGNAL",
                "d0_scope_status": page_result["scope_status"]["d0"],
            },
            "formal_admission": {
                "status": formal_admission_day.get("status", "NOT_COMPUTED"),
                "reason_code": formal_admission_day.get("reason_code"),
                "source_count": formal_admission_day.get("source_count"),
                "stored_rows": formal_admission_day.get("stored_rows"),
                "distinct_source_count": formal_admission_day.get(
                    "distinct_source_count"
                ),
                "counts": formal_admission_day.get("counts"),
                "identity_scope": "SOURCE_SYMBOL_ONLY",
                "research_mode": "NON_PIT_EXPLORATORY",
                "price_coordinate": "FORWARD_ADJUSTED",
            },
            "pagination": {
                "offset": offset,
                "limit": limit,
                "page_count": len(results),
                "total_count": selected_total,
                "next_offset": next_offset,
            },
            "boards": response_boards,
            "pulse_followup_directory": pulse_followup_directory,
            "board_summary": {
                "status": home_board_result.get("status", "NOT_REQUESTED"),
                "total_board_count": home_board_result.get("total_board_count", 0),
                "returned_board_count": len(home_board_result.get("boards", [])),
            },
            "observation_ordering": {
                "candidate_count": group_counts.get(watch_group, 0),
                "ordering": "T0_RESEARCH_TRIAGE_R1",
                "precedence": [
                    "CURRENT_SAME_DIRECTION",
                    "MIXED",
                    "CURRENT_COUNTER",
                    "UNKNOWN",
                    "NO_PROVED_CASE",
                    "NO_T0_FACT",
                ],
                "tie_break": "SOURCE_SYMBOL_ASC",
                "meaning": "仅研究阅读顺序；不改变五组资格、D0顺序、正式Priority或收益判断。",
                "page_scope": True,
            },
            "board_selection": {
                "status": "SELECTED_BOARD_ONLY" if board_kind else "NOT_FILTERED"
            },
            "stored_evidence_status": {
                "status": "AVAILABLE"
                if page_result["scope_status"]["rules_match_seed"]
                else "UNKNOWN",
                "d0": page_result["scope_status"]["d0"],
                "seed": page_result["scope_status"]["seed"],
                "d1": page_result["scope_status"]["d1"],
                "rules": page_result["scope_status"]["rules"],
                "seed_scope": page_result.get("seed_scope"),
                "rule_scope": evidence.get("rule_scope"),
                "d1_scope": evidence.get("d1_scope"),
            },
            "sector_pulse_status": pulse.get("status", "NOT_COMPUTED"),
            "sector_pulse_followup_comparison": pulse_followup_comparison,
            "results": results,
            "execution": {
                **page_result["execution"],
                "d0_full_pool_computed_before_selection": False,
                "formal_admission_get_recalculated": False,
                "cache_hit": False,
                "membership_read_scope": "CURRENT_PAGE_SYMBOLS_ONLY",
                "pulse_read_scope": "CURRENT_PAGE_MEMBER_BOARDS_ONLY",
                "sector_pulse_cross_period_recalculated": False,
                "sector_pulse_followup": pulse_followup_execution,
                "evidence_symbol_count": len(page_symbols),
                "rule_rows_returned_to_python": sum(
                    len(item.get("rules", {}).get("facts", {}))
                    for item in evidence_by_symbol.values()
                ),
                "t0_early_summary_rows_returned_to_python": sum(
                    item.get("t0_triage_summary", {}).get("status") == "AVAILABLE"
                    for item in page_rows
                ),
                "t0_driver_summary_rows_returned_to_python": sum(
                    item.get("status") == "AVAILABLE" for item in t0_summaries.values()
                ),
                "t0_driver_summary_query_seconds": t0_summary_query_seconds,
                "t0_early_full_context_rows_returned_to_python": 0,
                "t0_early_full_contexts_loaded": False,
                "page_selection_and_triage_order_executed_in_postgresql": True,
                "home_board_summary_rows_returned_to_python": len(
                    home_board_result.get("boards", [])
                ),
                "d0_full_pool_python_materialization": False,
                "api_server_processing_wall_seconds": time.perf_counter()
                - request_started,
                "api_server_process_cpu_seconds": time.process_time()
                - request_cpu_started,
                "database_write": False,
                "ddl_executed": False,
                "dml_executed": False,
            },
        }
        from stock_a.storage.postgres.source_s2_t1_cohort import (
            SourceS2T1CohortReadError,
            read_source_s2_t1_cohort_for_focus,
        )

        try:
            response["s2_t1_cohort"] = read_source_s2_t1_cohort_for_focus(
                config,
                target_session=target_session,
                endpoint_session=successor_sessions[0] if successor_sessions else None,
                research_cutoff=research_cutoff,
                source_capture_id=after_source["source_capture_id"],
            )
        except SourceS2T1CohortReadError as error:
            raise PreviewHttpError(
                409, "S2_T1_COHORT_READ_REJECTED", str(error)
            ) from error
        response["execution"]["s2_t1_cohort_summary_rows_returned_to_python"] = (
            12 if response["s2_t1_cohort"].get("status") == "AVAILABLE" else 0
        )
        response["execution"]["s2_t1_cohort_full_pool_materialized"] = False
        return response

    def _source_t0_early_observations(self, body: Any) -> dict[str, Any]:
        """用户展开单股时，按捕获和板块快照读取已存T0板内事实。"""
        required = {
            "symbol",
            "target_session",
            "source_capture_id",
        }
        history_fields = {
            "board_kind",
            "board_native_id",
            "membership_snapshot_id",
        }
        if not isinstance(body, dict) or frozenset(body) not in {
            frozenset(required),
            frozenset(required | history_fields),
        }:
            raise PreviewHttpError(
                400,
                "T0_EARLY_DETAIL_FIELDS_INVALID",
                "T0板内详情必须绑定来源代码、目标日和捕获ID；跨会话读取还须提供板类、板块ID和真实成员快照ID",
            )
        symbol = _symbol(body["symbol"])
        target = _iso_date(body["target_session"])
        _require_research_cutoff(target, self.preview_server.state.research_cutoff)
        source_capture_id = _capture_id(body["source_capture_id"])
        source = read_source_version_context(
            self.preview_server.state.config,
            target_session=target,
            source_capture_id=source_capture_id,
            research_cutoff=self.preview_server.state.research_cutoff,
        )
        if (
            source.get("status") != "AVAILABLE"
            or source.get("source_capture_id") != source_capture_id
        ):
            raise PreviewHttpError(
                409,
                "T0_EARLY_DETAIL_SOURCE_MISMATCH",
                "T0板内详情捕获与已存BaoStock价格版本不一致",
            )
        from stock_a.storage.postgres.t0_early_observations import (
            read_source_t0_early_observations_for_symbols,
            read_source_t0_early_triage_summaries_for_symbols,
        )

        contexts = read_source_t0_early_observations_for_symbols(
            self.preview_server.state.config,
            target_session=target,
            source_capture_id=source_capture_id,
            source_symbols=[symbol],
        ).get(symbol, [])
        triage_summary = _bind_t0_followup_status(
            read_source_t0_early_triage_summaries_for_symbols(
                self.preview_server.state.config,
                target_session=target,
                source_capture_id=source_capture_id,
                source_symbols=[symbol],
                selected_snapshot_ids=(
                    {body["board_kind"]: _snapshot_id(body["membership_snapshot_id"])}
                    if history_fields.issubset(body)
                    and body.get("board_kind") in {"INDUSTRY", "CONCEPT"}
                    else {}
                ),
            )[symbol],
            target_session=target,
            research_cutoff=self.preview_server.state.research_cutoff,
        )
        if any(
            context.get("target_session") != target
            or context.get("source_capture_id") != source_capture_id
            for context in contexts
        ):
            raise PreviewHttpError(
                409,
                "T0_EARLY_DETAIL_READBACK_SCOPE_MISMATCH",
                "T0板内详情读回超出请求的精确日期或来源版本",
            )
        by_enrollment: dict[str, list[dict[str, Any]]] = {}
        for context in contexts:
            key = str(context["enrollment_id"])
            by_enrollment.setdefault(key, []).append(context)
        response = {
            "ok": True,
            "symbol": symbol,
            "target_session": target.isoformat(),
            "source_capture_id": source_capture_id,
            "status": "AVAILABLE" if contexts else "NO_STORED_FACT",
            "triage_summary": triage_summary,
            "registration_count": len(by_enrollment),
            "relation_count": len(contexts),
            "by_enrollment": by_enrollment,
            "scope": "NON_PIT_EXPLORATORY",
            "price_coordinate": "FORWARD_ADJUSTED",
            "execution": {
                "database_write_attempted": False,
                "ddl_executed": False,
                "dml_executed": False,
                "full_t0_pool_materialized": False,
                "single_symbol_only": True,
            },
        }
        if history_fields.issubset(body):
            board_kind = body["board_kind"]
            board_native_id = body["board_native_id"]
            membership_snapshot_id = _snapshot_id(body["membership_snapshot_id"])
            if (
                not isinstance(board_kind, str)
                or board_kind not in {"INDUSTRY", "CONCEPT"}
                or not isinstance(board_native_id, str)
                or not board_native_id
                or len(board_native_id) > 64
            ):
                raise PreviewHttpError(
                    400,
                    "T0_EARLY_HISTORY_BOARD_INVALID",
                    "板内跨会话读取的板类或板块ID无效",
                )
            from stock_a.application.source_board_relative_history import (
                read_source_board_relative_history,
            )

            try:
                response["relative_position_history"] = (
                    read_source_board_relative_history(
                        self.preview_server.state.config,
                        target_session=target,
                        research_cutoff=self.preview_server.state.research_cutoff,
                        source_capture_id=source_capture_id,
                        source_symbol=symbol,
                        membership_snapshot_id=membership_snapshot_id,
                        board_kind=board_kind,
                        board_native_id=board_native_id,
                    )
                )
            except ValueError as error:
                raise PreviewHttpError(
                    409, "T0_EARLY_HISTORY_NOT_COMPARABLE", str(error)
                ) from error
        return response

    def _source_launch_watch(self, body: Any) -> dict[str, Any]:
        """从PG按研究分型排序后读取有界D0或Focus页面。"""
        request_started = time.perf_counter()
        request_cpu_started = time.process_time()
        required = {
            "before_session",
            "target_session",
            "research_cutoff",
            "source_capture_id",
            "industry_snapshot_id",
            "concept_snapshot_id",
            "offset",
            "limit",
        }
        board_fields = {"board_kind", "board_native_id"}
        evidence_fields = {"d1_history_session_count"}
        profile_fields = {"history_session_count", "profile_revision"}
        watch_fields = {"watch_group"}
        before_capture_field = {"before_source_capture_id"}
        if not isinstance(body, dict) or not required.issubset(body):
            raise PreviewHttpError(
                400,
                "SOURCE_LAUNCH_WATCH_FIELDS_INVALID",
                "请求必须显式绑定双日期、研究截止、目标捕获、双板块快照和分页",
            )
        body_fields = frozenset(body)
        allowed_body_fields = {
            frozenset(
                required
                | (board_fields if has_board else set())
                | (evidence_fields if has_evidence else set())
                | (profile_fields if has_profile else set())
                | (watch_fields if has_watch else set())
                | (before_capture_field if has_before_capture else set())
            )
            for has_board in (False, True)
            for has_evidence in (False, True)
            for has_profile in (False, True)
            for has_watch in (False, True)
            for has_before_capture in (False, True)
        }
        if body_fields not in allowed_body_fields:
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

        source_capture_id = _capture_id(body["source_capture_id"])
        before_source_capture_id = (
            _capture_id(body["before_source_capture_id"])
            if body.get("before_source_capture_id") is not None
            else None
        )
        industry_snapshot_id = (
            _snapshot_id(body["industry_snapshot_id"])
            if body["industry_snapshot_id"] is not None
            else None
        )
        concept_snapshot_id = (
            _snapshot_id(body["concept_snapshot_id"])
            if body["concept_snapshot_id"] is not None
            else None
        )
        if (
            industry_snapshot_id is not None
            and industry_snapshot_id == concept_snapshot_id
        ):
            raise PreviewHttpError(
                400,
                "SOURCE_LAUNCH_WATCH_SNAPSHOTS_NOT_DISTINCT",
                "行业和GN概念必须绑定各自不同的TDX快照",
            )
        offset = body["offset"]
        if type(offset) is not int or not 0 <= offset <= _MAX_OFFSET:
            raise PreviewHttpError(
                400,
                "PREVIEW_PARAMETER_INVALID",
                "offset必须为0至允许上限之间的整数",
            )
        limit = _positive_int(body["limit"], "limit", 30)
        watch_group = body.get("watch_group", "D0_NEW")
        if watch_group not in {
            "D0_NEW",
            "SEED_RULE",
            "D1_MATERIAL",
            "UNKNOWN",
            "KNOWN_NO_SIGNAL",
        }:
            raise PreviewHttpError(
                400, "SOURCE_LAUNCH_WATCH_GROUP_INVALID", "来源观察分组无效"
            )
        d1_history_session_count = (
            _positive_int(
                body["d1_history_session_count"], "d1_history_session_count", 60
            )
            if "d1_history_session_count" in body
            else None
        )
        if d1_history_session_count is not None and d1_history_session_count < 2:
            raise PreviewHttpError(
                400,
                "SOURCE_LAUNCH_WATCH_D1_WINDOW_INVALID",
                "D1历史窗口须为2至60个官方会话",
            )
        history_session_count = (
            _positive_int(body["history_session_count"], "history_session_count", 250)
            if "history_session_count" in body
            else None
        )
        profile_revision = (
            _profile_revision(body["profile_revision"])
            if "profile_revision" in body
            else None
        )
        if (history_session_count is None) != (profile_revision is None):
            raise PreviewHttpError(
                400,
                "SOURCE_LAUNCH_WATCH_PROFILE_SCOPE_INVALID",
                "Profile历史会话数与画像修订须成对提供",
            )

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

        return self._source_launch_watch_focus_saved_page(
            before_session=before_session,
            target_session=target_session,
            research_cutoff=research_cutoff,
            before_source_capture_id=before_source_capture_id,
            source_capture_id=source_capture_id,
            industry_snapshot_id=industry_snapshot_id,
            concept_snapshot_id=concept_snapshot_id,
            offset=offset,
            limit=limit,
            watch_group=watch_group,
            d1_history_session_count=d1_history_session_count,
            history_session_count=history_session_count,
            profile_revision=profile_revision,
            board_kind=board_kind,
            board_native_id=board_native_id,
        )

    def _board_comparison(self, body: Any) -> dict[str, Any]:
        required = {
            "target_session",
            "research_cutoff",
            "source_capture_id",
            "d1_history_session_count",
            "industry_snapshot_id",
            "concept_snapshot_id",
            "board_a",
            "board_b",
        }
        optional = {"history_session_count", "profile_revision"}
        if (
            not isinstance(body, dict)
            or set(body) - required - optional
            or not required <= set(body)
            or ("history_session_count" in body) != ("profile_revision" in body)
        ):
            raise PreviewHttpError(
                400,
                "BOARD_COMPARISON_FIELDS_INVALID",
                "双板块比较须提供完整字段；Profile会话数与修订须成对提供",
            )
        target = _iso_date(body["target_session"])
        cutoff = _iso_date(body["research_cutoff"], "research_cutoff")
        _require_research_cutoff(cutoff, self.preview_server.state.research_cutoff)
        _require_research_cutoff(target, cutoff)
        source_capture_id = _capture_id(body["source_capture_id"])
        history_count = _positive_int(
            body["d1_history_session_count"], "d1_history_session_count", 32767
        )
        profile_history_count = (
            _positive_int(body["history_session_count"], "history_session_count", 250)
            if "history_session_count" in body
            else None
        )
        profile_revision = (
            _profile_revision(body["profile_revision"])
            if "profile_revision" in body
            else None
        )
        snapshots = {
            "INDUSTRY": (
                _snapshot_id(body["industry_snapshot_id"])
                if body["industry_snapshot_id"] is not None
                else None
            ),
            "CONCEPT": (
                _snapshot_id(body["concept_snapshot_id"])
                if body["concept_snapshot_id"] is not None
                else None
            ),
        }
        if (
            snapshots["INDUSTRY"] is not None
            and snapshots["INDUSTRY"] == snapshots["CONCEPT"]
        ):
            raise PreviewHttpError(
                400,
                "BOARD_COMPARISON_SNAPSHOTS_NOT_DISTINCT",
                "行业与GN比较必须绑定不同的成员快照",
            )

        def parse_board(value: Any, label: str) -> tuple[str, str]:
            if (
                not isinstance(value, dict)
                or set(value) != {"board_kind", "board_native_id"}
                or value["board_kind"] not in {"INDUSTRY", "CONCEPT"}
                or not isinstance(value["board_native_id"], str)
                or not value["board_native_id"].strip()
                or value["board_native_id"] != value["board_native_id"].strip()
                or len(value["board_native_id"]) > 128
            ):
                raise PreviewHttpError(
                    400,
                    "BOARD_COMPARISON_BOARD_INVALID",
                    f"{label}必须是有效的精确板块类型和原生编号",
                )
            return value["board_kind"], value["board_native_id"]

        board_a = parse_board(body["board_a"], "board_a")
        board_b = parse_board(body["board_b"], "board_b")
        try:
            result = read_source_board_comparison(
                self.preview_server.state.config,
                target_session=target,
                source_capture_id=source_capture_id,
                d1_history_session_count=history_count,
                history_session_count=profile_history_count,
                profile_revision=profile_revision,
                snapshot_ids=snapshots,
                board_a_kind=board_a[0],
                board_a_native_id=board_a[1],
                board_b_kind=board_b[0],
                board_b_native_id=board_b[1],
            )
        except ResearchDayPgError as error:
            raise PreviewHttpError(409, error.code, str(error)) from error
        return {"ok": True, **result}

    def _d1_material(self, body: Any) -> dict[str, Any]:
        required = {
            "target_session",
            "research_cutoff",
            "source_capture_id",
            "d1_history_session_count",
            "industry_snapshot_id",
            "concept_snapshot_id",
        }
        optional = {
            "structure_type",
            "material_change_reason",
            "board_kind",
            "board_native_id",
            "symbol",
            "board_offset",
            "member_offset",
            "stock_offset",
            "board_limit",
            "member_limit",
            "stock_limit",
            "board_search",
            "member_search",
            "member_group",
        }
        if (
            not isinstance(body, dict)
            or set(body) - required - optional
            or not (required <= set(body))
        ):
            raise PreviewHttpError(
                400,
                "D1_MATERIAL_REQUEST_FIELDS_INVALID",
                "D1材料请求字段不完整或含未登记字段",
            )
        target = _iso_date(body["target_session"])
        cutoff = _iso_date(body["research_cutoff"], "research_cutoff")
        _require_research_cutoff(cutoff, self.preview_server.state.research_cutoff)
        _require_research_cutoff(target, cutoff)
        source_capture_id = _capture_id(body["source_capture_id"])
        d1_history_session_count = _d1_material_int(
            body["d1_history_session_count"],
            "d1_history_session_count",
            2,
            60,
        )
        snapshots = {}
        for field in ("industry_snapshot_id", "concept_snapshot_id"):
            value = body[field]
            if value is None:
                snapshots[field] = None
                continue
            snapshots[field] = _snapshot_id(value)
        if snapshots["industry_snapshot_id"] is not None and (
            snapshots["industry_snapshot_id"] == snapshots["concept_snapshot_id"]
        ):
            raise PreviewHttpError(
                400,
                "D1_MATERIAL_SNAPSHOT_IDS_NOT_DISTINCT",
                "行业与GN成员快照必须是两个不同的记录",
            )
        if (
            snapshots["industry_snapshot_id"] is not None
            and snapshots["concept_snapshot_id"] is not None
        ):
            snapshot_pair_provenance = _verified_sector_snapshot_pair(
                self.preview_server.state.config,
                industry_snapshot_id=snapshots["industry_snapshot_id"],
                concept_snapshot_id=snapshots["concept_snapshot_id"],
            )
        else:
            snapshot_pair_provenance = {
                "status": "NOT_AVAILABLE",
                "reason_code": "SECTOR_SNAPSHOT_PAIR_NOT_BOUND",
                "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
                "snapshot_ids": {
                    "INDUSTRY": snapshots["industry_snapshot_id"],
                    "CONCEPT": snapshots["concept_snapshot_id"],
                },
            }

        structure_type = body.get("structure_type") or None
        if structure_type == "ALL":
            structure_type = None
        if structure_type is not None and (
            not isinstance(structure_type, str)
            or structure_type not in ALL_STRUCTURE_TYPES
        ):
            raise PreviewHttpError(
                400, "D1_MATERIAL_STRUCTURE_TYPE_INVALID", "D1结构类型筛选值无效"
            )
        material_change_reason = body.get("material_change_reason") or None
        if material_change_reason == "ALL":
            material_change_reason = None
        if material_change_reason is not None and (
            not isinstance(material_change_reason, str)
            or material_change_reason not in MATERIAL_CHANGE_REASONS
        ):
            raise PreviewHttpError(
                400, "D1_MATERIAL_REASON_INVALID", "D1材料变化原因筛选值无效"
            )
        board_kind = body.get("board_kind") or None
        if board_kind == "ALL":
            board_kind = None
        if board_kind is not None and (
            not isinstance(board_kind, str) or board_kind not in {"INDUSTRY", "CONCEPT"}
        ):
            raise PreviewHttpError(
                400, "D1_MATERIAL_BOARD_KIND_INVALID", "板块类型筛选值无效"
            )
        board_native_id = body.get("board_native_id") or None
        if board_native_id is not None:
            board_native_id = str(board_native_id).strip()
            if not board_kind or not board_native_id or len(board_native_id) > 128:
                raise PreviewHttpError(
                    400,
                    "D1_MATERIAL_BOARD_ID_INVALID",
                    "精确板块下钻必须同时指定板块类型和编号",
                )
        symbol = body.get("symbol") or None
        if symbol is not None:
            symbol = _symbol(symbol)
        page_values = {}
        for name, default in (
            ("board_offset", 0),
            ("member_offset", 0),
            ("stock_offset", 0),
        ):
            page_values[name] = _d1_material_int(
                body.get(name, default), name, 0, _MAX_OFFSET
            )
        for name, default in (
            ("board_limit", 15),
            ("member_limit", 30),
            ("stock_limit", 30),
        ):
            page_values[name] = _d1_material_int(
                body.get(name, default), name, 1, _MAX_PAGE_SIZE
            )
        text_values = {}
        for name in ("board_search", "member_search"):
            value = body.get(name)
            if value is not None and not isinstance(value, str):
                raise PreviewHttpError(
                    400, "D1_MATERIAL_SEARCH_INVALID", "D1搜索条件必须是文本"
                )
            value = value.strip() if value else ""
            if len(value) > 120:
                raise PreviewHttpError(
                    400, "D1_MATERIAL_SEARCH_INVALID", "D1搜索文本最多120个字符"
                )
            text_values[name] = value or None
        member_group = body.get("member_group") or None
        if member_group == "ALL":
            member_group = None
        allowed_groups = {
            "MATERIAL_CHANGE",
            "NO_SELECTED_REASON",
            "KNOWN_NON_MATERIAL",
            "UNKNOWN",
            "NOT_COMPARABLE",
            "NOT_EVALUABLE",
            "NOT_APPLICABLE",
            "NO_PROVED_ANCHOR",
            "NOT_IN_SOURCE_POOL",
        }
        if member_group is not None and (
            not isinstance(member_group, str) or member_group not in allowed_groups
        ):
            raise PreviewHttpError(
                400, "D1_MATERIAL_MEMBER_GROUP_INVALID", "板块成员状态筛选值无效"
            )

        cache_key = (
            target.isoformat(),
            cutoff.isoformat(),
            source_capture_id,
            d1_history_session_count,
            snapshots["industry_snapshot_id"],
            snapshots["concept_snapshot_id"],
            "SOURCE_D1_STRUCTURE_MATERIAL_CHANGE_R1",
        )
        request_started = time.perf_counter()
        try:
            prepared, cache_hit = self.preview_server.state.source_d1_material_facts(
                cache_key,
                lambda: prepare_saved_d1_material_observation(
                    self.preview_server.state.config,
                    target_session=target,
                    research_cutoff=cutoff,
                    source_capture_id=source_capture_id,
                    d1_history_session_count=d1_history_session_count,
                    industry_snapshot_id=snapshots["industry_snapshot_id"],
                    concept_snapshot_id=snapshots["concept_snapshot_id"],
                ),
            )
            result = read_d1_material_observation(
                self.preview_server.state.config,
                target_session=target,
                research_cutoff=cutoff,
                source_capture_id=source_capture_id,
                d1_history_session_count=d1_history_session_count,
                industry_snapshot_id=snapshots["industry_snapshot_id"],
                concept_snapshot_id=snapshots["concept_snapshot_id"],
                structure_type=structure_type,
                material_change_reason=material_change_reason,
                board_kind=board_kind,
                board_native_id=board_native_id,
                symbol=symbol,
                offset=0,
                limit=30,
                prepared_facts=prepared,
                board_offset=page_values["board_offset"],
                member_offset=page_values["member_offset"],
                stock_offset=page_values["stock_offset"],
                board_limit=page_values["board_limit"],
                member_limit=page_values["member_limit"],
                stock_limit=page_values["stock_limit"],
                board_search=text_values["board_search"],
                member_search=text_values["member_search"],
                member_group=member_group,
            )
        except SourceObservationError as error:
            raise PreviewHttpError(409, error.code, str(error)) from error

        d1 = prepared["d1"]
        full_scan_passes = (
            0 if cache_hit else d1["execution"].get("full_pool_scan_passes", 1)
        )
        payload = {
            "ok": True,
            "view": result["view"],
            "scope": result["scope"],
            "qualification_scope": result["qualification_scope"],
            "target_session": result["target_session"],
            "research_cutoff": result["research_cutoff"],
            "source_capture_id": result["source_capture_id"],
            "profile_revision": result["profile_revision"],
            "d1_history_session_count": result["d1_history_session_count"],
            "d1_history_start": result["d1_history_start"],
            "d1_history_end": result["d1_history_end"],
            "source": {
                key: result["source"].get(key)
                for key in (
                    "source_capture_id",
                    "raw_row_count",
                    "capture_expected_row_count",
                    "capture_read_started_at",
                    "capture_validated_at",
                    "source_time_quality",
                    "source_clock_role",
                )
            },
            "snapshots": result["snapshots"],
            "snapshot_pair_provenance": snapshot_pair_provenance,
            "market": result["market"],
            "boards": {
                key: value
                for key, value in result["boards"].items()
                if key not in {"selected_material_stocks"}
            },
            "stocks": result["stocks"],
            "selected_structure_type": result["selected_structure_type"],
            "selected_material_change_reason": result[
                "selected_material_change_reason"
            ],
            "selection_scope": result["selection_scope"],
            "selected_stock": result["selected_stock"],
            "d1": {
                "status": result["d1"]["status"],
                "contract": result["d1"]["contract"],
                "research_mode": result["d1"]["research_mode"],
                "price_coordinate": result["d1"]["price_coordinate"],
                "formal_downstream": result["d1"]["formal_downstream"],
            },
            "outcome_comparison": result["outcome_comparison"],
            "execution": {
                **result["execution"],
                "d1_full_pool_scan_passes": full_scan_passes,
                "d1_pg_saved_fact_read": True,
                "d1_full_pool_scan_wall_seconds": d1["execution"].get("wall_seconds"),
                "reused_process_d1_material_facts": cache_hit,
                "http_request_wall_seconds": round(
                    time.perf_counter() - request_started, 6
                ),
            },
        }
        return payload

    def _sector_change_stocks(self, body: Any) -> dict[str, Any]:
        required = {
            "target_session",
            "research_cutoff",
            "source_capture_id",
            "price_coordinate",
            "profile_revision",
            "industry_snapshot_id",
            "concept_snapshot_id",
            "event_window_start",
            "pg_history_session_count",
            "board_kind",
            "changed_only",
            "sort_by",
            "direction",
            "offset",
            "limit",
            "detail_board_kind",
            "detail_board_native_id",
            "member_state",
            "member_search",
            "member_offset",
            "member_limit",
            "include_changed_stock_page",
            "stock_offset",
            "stock_limit",
        }
        if not isinstance(body, dict) or not required <= set(body) <= (
            required
            | {
                "observation_mode",
                "explain_symbol",
                "board_search",
                "d1_only_explanation",
            }
        ):
            raise PreviewHttpError(
                400,
                "SECTOR_CHANGE_BODY_INVALID",
                "行业/GN变化查询请求字段不完整或含未登记字段",
            )

        observation_mode = body.get("observation_mode", "D1")
        if not isinstance(observation_mode, str) or observation_mode not in {
            "D1",
            "D0",
        }:
            raise PreviewHttpError(
                400,
                "SECTOR_CHANGE_MODE_INVALID",
                "观察模式只能为D1或D0",
            )
        include_d0_launch_new = observation_mode == "D0"
        explain_symbol = (
            _symbol(body["explain_symbol"]) if "explain_symbol" in body else None
        )
        d1_only = body.get("d1_only_explanation", False)
        if type(d1_only) is not bool or (
            d1_only and (include_d0_launch_new or explain_symbol is None)
        ):
            raise PreviewHttpError(
                400, "SECTOR_CHANGE_EXPLANATION_INVALID", "仅D1解释须指定代码与D1模式"
            )
        read_d0 = include_d0_launch_new or (explain_symbol is not None and not d1_only)
        target = _iso_date(body["target_session"])
        cutoff = _iso_date(body["research_cutoff"], "research_cutoff")
        _require_research_cutoff(target, cutoff)
        _require_research_cutoff(cutoff, self.preview_server.state.research_cutoff)
        source_capture_id = _capture_id(body["source_capture_id"])
        price_coordinate = _price_coordinate(body["price_coordinate"])
        if price_coordinate != "FORWARD_ADJUSTED":
            raise PreviewHttpError(
                400,
                "SECTOR_CHANGE_PRICE_COORDINATE_INVALID",
                "D0/D1及跨会话结构观察固定使用BaoStock FORWARD_ADJUSTED坐标",
            )
        profile_revision = (
            None if d1_only else _profile_revision(body["profile_revision"])
        )
        event_window_start = _iso_date(body["event_window_start"], "event_window_start")
        source_policy = self.preview_server.state.config.get("source_policy", {})
        baostock_policy = (
            source_policy.get("baostock_primary", {})
            if isinstance(source_policy, Mapping)
            else {}
        )
        request_budget = (
            baostock_policy.get("request_budget", {})
            if isinstance(baostock_policy, Mapping)
            else {}
        )
        history_limit = (
            request_budget.get("max_history_session_count")
            if isinstance(request_budget, Mapping)
            else None
        )
        if type(history_limit) is not int or not 1 <= history_limit <= 250:
            history_limit = 250
        history_session_count = _positive_int(
            body["pg_history_session_count"], "pg_history_session_count", history_limit
        )
        d0_before_session = None
        if read_d0:
            try:
                resolved_target, adjacent_sessions, _ = resolve_pg_history_window(
                    target.isoformat(), 2
                )
            except ResearchEntryError as error:
                raise PreviewHttpError(
                    400, "D0_OFFICIAL_CALENDAR_INVALID", str(error)
                ) from error
            if (
                resolved_target != target
                or len(adjacent_sessions) != 2
                or adjacent_sessions[-1] != target
            ):
                raise PreviewHttpError(
                    400,
                    "D0_BEFORE_SESSION_INVALID",
                    "D0目标日没有可用的前一官方交易会话",
                )
            d0_before_session = adjacent_sessions[-2]
            _require_research_cutoff(d0_before_session, cutoff)

        industry_snapshot_id = concept_snapshot_id = None
        if not d1_only:
            industry_snapshot_id = _snapshot_id(body["industry_snapshot_id"])
            concept_snapshot_id = _snapshot_id(body["concept_snapshot_id"])
            if (
                industry_snapshot_id != body["industry_snapshot_id"]
                or concept_snapshot_id != body["concept_snapshot_id"]
                or industry_snapshot_id == concept_snapshot_id
            ):
                raise PreviewHttpError(
                    400,
                    "SECTOR_CHANGE_SNAPSHOT_ID_INVALID",
                    "行业与GN快照编号必须规范且互不相同",
                )

        board_kind = body["board_kind"]
        if board_kind not in {"ALL", "INDUSTRY", "CONCEPT"}:
            raise PreviewHttpError(
                400,
                "SECTOR_CHANGE_BOARD_KIND_INVALID",
                "板块类型只能为ALL、INDUSTRY或GN",
            )
        changed_only = body["changed_only"]
        include_changed_stock_page = body["include_changed_stock_page"]
        if type(changed_only) is not bool or include_changed_stock_page is not True:
            raise PreviewHttpError(
                400,
                "SECTOR_CHANGE_FILTER_INVALID",
                "changed_only必须为布尔值且必须读取全局去重变化个股页",
            )
        if include_d0_launch_new and changed_only:
            raise PreviewHttpError(
                400,
                "SECTOR_CHANGE_MODE_FILTER_INVALID",
                "D0观察模式不能叠加D1 changed_only筛选",
            )
        sort_by = body["sort_by"]
        valid_sorts = {"changed_stocks", "delta20", "delta5", "ret20", "ret5"}
        if include_d0_launch_new:
            valid_sorts.add("d0_launch_new")
        if sort_by not in valid_sorts or (
            include_d0_launch_new and sort_by != "d0_launch_new"
        ):
            raise PreviewHttpError(
                400, "SECTOR_CHANGE_SORT_INVALID", "板块排序字段不在现役合同中"
            )
        direction = body["direction"]
        if direction not in {"STRONGER", "WEAKER"}:
            raise PreviewHttpError(
                400, "SECTOR_CHANGE_DIRECTION_INVALID", "方向只能为STRONGER或WEAKER"
            )
        detail_board_kind = body["detail_board_kind"]
        detail_board_native_id = body["detail_board_native_id"]
        if (detail_board_kind is None) != (detail_board_native_id is None):
            raise PreviewHttpError(
                400,
                "SECTOR_CHANGE_DETAIL_INVALID",
                "板块明细必须同时提供类型和原生编号",
            )
        if detail_board_kind not in {None, "INDUSTRY", "CONCEPT"}:
            raise PreviewHttpError(
                400,
                "SECTOR_CHANGE_DETAIL_INVALID",
                "板块明细类型只能是INDUSTRY或GN概念",
            )
        if detail_board_native_id is not None and (
            not isinstance(detail_board_native_id, str)
            or not detail_board_native_id.strip()
            or len(detail_board_native_id) > 128
        ):
            raise PreviewHttpError(
                400,
                "SECTOR_CHANGE_DETAIL_INVALID",
                "原生板块编号必须是1至128个字符",
            )
        member_state = body["member_state"]
        if member_state not in {
            "ALL",
            "CHANGED",
            "UNCHANGED",
            "UNKNOWN",
            "NOT_COMPARABLE",
            "NOT_EVALUABLE",
        }:
            raise PreviewHttpError(
                400, "SECTOR_CHANGE_MEMBER_STATE_INVALID", "板内状态筛选值无效"
            )
        member_search = body["member_search"]
        if member_search is not None:
            member_search = _symbol_search(member_search)
        board_search = body.get("board_search")
        if board_search is not None:
            if (
                not isinstance(board_search, str)
                or not 1 <= len(board_search.strip()) <= 64
            ):
                raise PreviewHttpError(
                    400, "SECTOR_BOARD_SEARCH_INVALID", "板块搜索须为1至64个字符"
                )
            board_search = board_search.strip()
        offset = _nonnegative_int(str(body["offset"]), "offset")
        limit = _positive_int(body["limit"], "limit", 100)
        member_offset = _nonnegative_int(str(body["member_offset"]), "member_offset")
        member_limit = body["member_limit"]
        if detail_board_kind is None:
            if member_offset != 0 or member_limit != 0 or member_search is not None:
                raise PreviewHttpError(
                    400,
                    "SECTOR_CHANGE_MEMBER_PAGE_INVALID",
                    "未选择板块时不能提交板内分页或股票搜索",
                )
        else:
            member_limit = _positive_int(member_limit, "member_limit", 100)
        stock_offset = _nonnegative_int(str(body["stock_offset"]), "stock_offset")
        stock_limit = _positive_int(body["stock_limit"], "stock_limit", 30)

        config = self.preview_server.state.config
        try:
            profile_catalog = None
            if not d1_only:
                profile_catalog = source_symbol_price_profile_snapshots(
                    config,
                    max_target_session=cutoff,
                )
                exact_profiles = [
                    item
                    for item in profile_catalog["snapshots"]
                    if item.get("target_session") == target.isoformat()
                    and item.get("source_capture_id") == source_capture_id
                    and item.get("profile_revision") == profile_revision
                    and item.get("complete") is True
                ]
                if len(exact_profiles) != 1:
                    raise PreviewHttpError(
                        409,
                        "SECTOR_CHANGE_PROFILE_REVISION_UNAVAILABLE",
                        "所选日期、来源捕获和画像修订没有唯一完整的PG版本",
                    )
                catalog = read_tdx_sector_source_snapshot_catalog(config)
                snapshots = {item["snapshot_id"]: item for item in catalog["snapshots"]}
                industry = snapshots.get(industry_snapshot_id)
                concept = snapshots.get(concept_snapshot_id)
                if industry is None or concept is None:
                    raise PreviewHttpError(
                        404,
                        "SECTOR_CHANGE_SNAPSHOT_NOT_FOUND",
                        "所选行业或GN快照不在PG真实目录中",
                    )
                if (
                    industry.get("source_kind") != "TDX_TDXHY_CFG"
                    or concept.get("source_kind") != "TDX_INFOHARBOR_BLOCK_DAT"
                ):
                    raise PreviewHttpError(
                        409,
                        "SECTOR_CHANGE_SNAPSHOT_KIND_MISMATCH",
                        "所选快照原生来源类型与行业/GN角色不匹配",
                    )

            cache_key = (
                target.isoformat(),
                cutoff.isoformat(),
                source_capture_id,
                price_coordinate,
                profile_revision,
                industry_snapshot_id,
                concept_snapshot_id,
                event_window_start.isoformat(),
                history_session_count,
                read_d0,
                d0_before_session.isoformat() if d0_before_session else None,
            )
            inputs = (
                self.preview_server.state.completed_source_sector_change_inputs(
                    cache_key
                )
                if d1_only
                else None
            )
            cache_hit = inputs is not None
            source_d1_cache_hit = False
            if d1_only and inputs is None:
                _resolved_target, d1_sessions, d1_calendar_revision = (
                    resolve_pg_history_window(target.isoformat(), history_session_count)
                )
                discovery = discover_source_d1_support_changes(
                    config,
                    str(self.preview_server.state.config_path),
                    target,
                    cutoff,
                    d1_sessions,
                    d1_calendar_revision,
                    source_capture_id,
                    price_coordinate,
                    event_window_start,
                    0,
                    1,
                    include_symbol_details=True,
                    source_symbol=explain_symbol,
                )
                exact_details = discovery["complete_symbol_details"]
                detail = (
                    exact_details[0]
                    if exact_details
                    and exact_details[0].get("source_symbol") == explain_symbol
                    else None
                )
                symbol_d1 = _member_detail(explain_symbol, detail)
                return {
                    "ok": True,
                    "scope": "SOURCE_SYMBOL_ONLY_D1_SUPPORT_EXPLANATION",
                    "target_session": target.isoformat(),
                    "research_cutoff": cutoff.isoformat(),
                    "source_capture_id": source_capture_id,
                    "price_coordinate": price_coordinate,
                    "source_pool_count": discovery["source_pool_count"],
                    "evaluated_source_symbol_count": discovery[
                        "evaluated_source_symbol_count"
                    ],
                    "symbol_observation": {
                        "source_symbol": explain_symbol,
                        "in_target_raw_pool": detail is not None,
                        "d1": symbol_d1,
                        "d0_change": None,
                        "memberships": [],
                        "missing_membership_kinds": ["INDUSTRY", "CONCEPT"],
                        "membership_status": "NOT_EVALUATED",
                        "formal_evaluation_status": "NOT_EVALUATED",
                    },
                    "d1_discovery": {
                        "scope": discovery["scope"],
                        "identity_status": discovery["identity_status"],
                        "price_coordinate": discovery["price_coordinate"],
                        "source": discovery["source"],
                        "raw_d1_counts_by_session": discovery[
                            "raw_d1_counts_by_session"
                        ],
                        "evaluated_support_event_counts": discovery[
                            "evaluated_support_event_counts"
                        ],
                        "source_pool_partition_closed": discovery[
                            "source_pool_partition_closed"
                        ],
                    },
                    "execution": {
                        **discovery["execution"],
                        "d1_full_universe_scan": False,
                        "database_write_attempted": False,
                        "ddl_executed": False,
                        "dml_executed": False,
                    },
                    "cache": {
                        "reused_exact_inputs": False,
                        "reused_completed_full_inputs": False,
                        "reused_source_d1_discovery": False,
                    },
                }
            if not d1_only:
                source_version_context = read_source_version_context(
                    config,
                    target_session=target,
                    source_capture_id=source_capture_id,
                    research_cutoff=cutoff,
                    price_coordinate=price_coordinate,
                )
                resolved_target, d1_sessions, d1_calendar_revision = (
                    resolve_pg_history_window(target.isoformat(), history_session_count)
                )
                if resolved_target != target:
                    raise PgResearchBindingError(
                        "D1_SUPPORT_DISCOVERY_SESSION_INVALID",
                        "D1上下文日期没有解析为请求的目标官方会话",
                    )
                if inputs is not None:
                    try:
                        validate_source_d1_discovery_fact(
                            inputs.discovery,
                            source_version_context=source_version_context,
                            target_session=target,
                            research_cutoff=cutoff,
                            source_capture_id=source_capture_id,
                            sessions=d1_sessions,
                            calendar_revision=d1_calendar_revision,
                            event_window_start=event_window_start,
                            price_coordinate=price_coordinate,
                        )
                    except PgResearchBindingError:
                        inputs = None
                        cache_hit = False
            if inputs is None:
                source_d1_cache_key = (
                    target.isoformat(),
                    cutoff.isoformat(),
                    source_capture_id,
                    source_version_context["source_capture_id"],
                    str(source_version_context["source_capture_id"]),
                    source_version_context["raw_row_count"],
                    source_version_context["capture_expected_row_count"],
                    d1_calendar_revision,
                    tuple(day.isoformat() for day in d1_sessions),
                    event_window_start.isoformat(),
                    history_session_count,
                    "SOURCE_SYMBOL_ONLY",
                    price_coordinate,
                    "TARGET_SESSION_RAW_SOURCE_POOL_EVENT_WINDOW_DISCOVERY",
                    self.preview_server.state.source_d1_algorithm_version,
                )

                def discover_exact_source_d1() -> dict[str, Any]:
                    discovery = discover_source_d1_support_changes(
                        config,
                        str(self.preview_server.state.config_path),
                        target,
                        cutoff,
                        d1_sessions,
                        d1_calendar_revision,
                        source_capture_id,
                        price_coordinate,
                        event_window_start,
                        0,
                        1,
                        include_symbol_details=True,
                        source_version_context=source_version_context,
                    )
                    validate_source_d1_discovery_fact(
                        discovery,
                        source_version_context=source_version_context,
                        target_session=target,
                        research_cutoff=cutoff,
                        source_capture_id=source_capture_id,
                        sessions=d1_sessions,
                        calendar_revision=d1_calendar_revision,
                        event_window_start=event_window_start,
                        price_coordinate=price_coordinate,
                    )
                    return discovery

                source_d1_discovery, source_d1_cache_hit = (
                    self.preview_server.state.source_d1_discovery(
                        source_d1_cache_key,
                        discover_exact_source_d1,
                    )
                )
                validate_source_d1_discovery_fact(
                    source_d1_discovery,
                    source_version_context=source_version_context,
                    target_session=target,
                    research_cutoff=cutoff,
                    source_capture_id=source_capture_id,
                    sessions=d1_sessions,
                    calendar_revision=d1_calendar_revision,
                    event_window_start=event_window_start,
                    price_coordinate=price_coordinate,
                )
                inputs, cache_hit = (
                    self.preview_server.state.source_sector_change_inputs(
                        cache_key,
                        lambda: prepare_source_sector_change_inputs(
                            config,
                            str(self.preview_server.state.config_path),
                            target_session=target,
                            research_cutoff=cutoff,
                            industry_snapshot_id=industry_snapshot_id,
                            concept_snapshot_id=concept_snapshot_id,
                            source_capture_id=source_capture_id,
                            profile_revision=profile_revision,
                            event_window_start=event_window_start,
                            pg_history_session_count=history_session_count,
                            include_d0_launch_new=read_d0,
                            d0_before_session=d0_before_session,
                            source_d1_discovery=source_d1_discovery,
                            source_version_context=source_version_context,
                            price_coordinate=price_coordinate,
                        ),
                    )
                )
        except PreviewHttpError:
            raise
        except SourceD0RawError as error:
            mapped = _source_context_preview_error(error)
            if mapped is None:
                raise
            raise mapped from error
        except TdxSectorMembersApplicationError as error:
            if "MISMATCH" in error.code or "INCOMPLETE" in error.code:
                status = 409
            elif error.code.endswith("NOT_FOUND"):
                status = 404
            elif "INVALID" in error.code:
                status = 400
            else:
                status = 503
            raise PreviewHttpError(status, error.code, str(error)) from error

        try:
            result = build_source_sector_change_view(
                config,
                inputs,
                board_kind_filter=None if board_kind == "ALL" else board_kind,
                detail_board_kind=detail_board_kind,
                detail_board_native_id=detail_board_native_id,
                changed_only=changed_only,
                sort_by=sort_by,
                direction=direction,
                offset=offset,
                limit=limit,
                member_state=member_state,
                member_offset=member_offset,
                member_limit=member_limit,
                include_changed_stock_page=True,
                stock_offset=stock_offset,
                stock_limit=stock_limit,
                include_source_market_context=False,
                member_search=member_search,
                board_search=board_search,
                explain_symbol=explain_symbol,
                d0_new_only=include_d0_launch_new,
            )
        except TdxSectorMembersApplicationError as error:
            if "MISMATCH" in error.code or "INCOMPLETE" in error.code:
                status = 409
            elif error.code.endswith("NOT_FOUND"):
                status = 404
            elif "INVALID" in error.code:
                status = 400
            else:
                status = 503
            raise PreviewHttpError(status, error.code, str(error)) from error
        result["observation_mode"] = observation_mode
        result["price_coordinate"] = price_coordinate
        result["time_role"] = "NON_PIT_RETROSPECTIVE"
        result["cache"] = {
            "reused_exact_inputs": cache_hit,
            "reused_source_d1_discovery": source_d1_cache_hit,
        }
        if profile_revision is not None:
            from stock_a.application.r03_formal_sector import R03_CONTRACT_VERSION
            from stock_a.storage.postgres.r03_formal_sector import (
                read_r03_loo_support_fact,
                read_r03_native_day_facts,
                read_r03_overlap_day,
            )

            r03_scope = f"BAOSTOCK_CAP_{source_capture_id}:PROF_{profile_revision}:NON_PIT"
            r03_member_ver = f"IND_{body['industry_snapshot_id']}:CON_{body['concept_snapshot_id']}"
            native_res = read_r03_native_day_facts(
                config,
                trade_date=target,
                scope_key=r03_scope,
                member_version=r03_member_ver,
                contract_version=R03_CONTRACT_VERSION,
            )
            ov_res = read_r03_overlap_day(
                config,
                trade_date=target,
                scope_key=r03_scope,
                member_version=r03_member_ver,
                contract_version=R03_CONTRACT_VERSION,
            )
            result["r03_formal_sector_context"] = {
                "scope_key": r03_scope,
                "member_version": r03_member_ver,
                "contract_version": R03_CONTRACT_VERSION,
                "native_status": native_res.get("status"),
                "overlap_status": ov_res.get("status"),
                "clusters": ov_res.get("clusters", []),
                "pairs": [
                    {
                        **p,
                        "jaccard_value": (
                            str(p["jaccard_value"])
                            if p.get("jaccard_value") is not None
                            else None
                        ),
                    }
                    for p in ov_res.get("pairs", ())
                ],
            }
            if detail_board_kind and detail_board_native_id:
                n_item = next(
                    (
                        item
                        for item in native_res.get("items", ())
                        if item.get("board_kind") == detail_board_kind
                        and item.get("board_id") == detail_board_native_id
                    ),
                    None,
                )
                o_item = next(
                    (
                        item
                        for item in ov_res.get("board_facts", ())
                        if item.get("board_kind") == detail_board_kind
                        and item.get("board_id") == detail_board_native_id
                    ),
                    None,
                )
                result["r03_formal_board_detail"] = {
                    "native": n_item,
                    "overlap_board": o_item,
                }
            if explain_symbol is not None:
                result["r03_formal_loo"] = {
                    "scope_key": r03_scope,
                    "member_version": r03_member_ver,
                    "contract_version": R03_CONTRACT_VERSION,
                    "by_board_kind": {
                        bk: read_r03_loo_support_fact(
                            config,
                            trade_date=target,
                            target_entity_id=explain_symbol,
                            board_kind=bk,
                            scope_key=r03_scope,
                            member_version=r03_member_ver,
                            contract_version=R03_CONTRACT_VERSION,
                        )
                        for bk in ("INDUSTRY", "CONCEPT")
                    },
                }
        return result

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
            if parsed.path == "/api/product/data-jobs/capabilities":
                if parsed.query:
                    raise PreviewHttpError(
                        400, "DATA_JOB_QUERY_INVALID", "能力读取不接受查询参数。"
                    )
                self._send_json(
                    200,
                    {"ok": True, **self.preview_server.state.data_jobs.capabilities()},
                )
                return
            if parsed.path == "/api/product/data-jobs/latest":
                if parsed.query:
                    raise PreviewHttpError(
                        400, "DATA_JOB_QUERY_INVALID", "最近任务读取不接受查询参数。"
                    )
                self._send_json(
                    200,
                    {
                        "ok": True,
                        "job": self.preview_server.state.data_jobs.read_job(),
                    },
                )
                return
            if parsed.path == "/api/product/data-jobs":
                values = _query(self.path, {"id"})
                if set(values) != {"id"}:
                    raise PreviewHttpError(
                        400, "DATA_JOB_ID_REQUIRED", "任务进度查询必须提供任务ID。"
                    )
                try:
                    job_id = UUID(values["id"])
                except (ValueError, AttributeError) as error:
                    raise PreviewHttpError(
                        400, "DATA_JOB_ID_INVALID", "任务ID格式无效。"
                    ) from error
                job = self.preview_server.state.data_jobs.read_job(job_id)
                if job is None:
                    raise PreviewHttpError(
                        404, "DATA_JOB_NOT_FOUND", "PostgreSQL中没有该数据任务。"
                    )
                self._send_json(200, {"ok": True, "job": job})
                return
            if parsed.path == "/api/product/research-day":
                values = _query(
                    self.path,
                    {
                        "target_session",
                        "source_capture_id",
                        "d1_history_session_count",
                        "offset",
                        "limit",
                        "symbol",
                        "industry_snapshot_id",
                        "concept_snapshot_id",
                        "history_session_count",
                        "profile_revision",
                        "board_kind",
                        "board_native_id",
                        "structure_type",
                        "material_change_reason",
                    },
                )
                required = {
                    "target_session",
                    "source_capture_id",
                    "d1_history_session_count",
                }
                if not required.issubset(values):
                    raise PreviewHttpError(
                        400,
                        "QUERY_REQUIRED",
                        "须提供目标会话、来源捕获和D1窗口会话数",
                    )
                if ("history_session_count" in values) != (
                    "profile_revision" in values
                ):
                    raise PreviewHttpError(
                        400,
                        "PROFILE_SCOPE_INVALID",
                        "Profile历史会话数与修订须成对提供",
                    )
                target = _iso_date(values["target_session"])
                _require_research_cutoff(
                    target, self.preview_server.state.research_cutoff
                )
                symbol = values.get("symbol") or None
                if symbol is not None and not _SYMBOL.fullmatch(symbol):
                    raise PreviewHttpError(400, "SYMBOL_INVALID", "来源代码格式无效")
                if values.get("board_kind") not in {None, "INDUSTRY", "CONCEPT"}:
                    raise PreviewHttpError(
                        400, "BOARD_KIND_INVALID", "板块类型须为INDUSTRY或CONCEPT"
                    )
                if values.get("structure_type") not in {None, *ALL_STRUCTURE_TYPES}:
                    raise PreviewHttpError(
                        400, "STRUCTURE_TYPE_INVALID", "D1结构类型不在现役合同枚举中"
                    )
                if values.get("material_change_reason") not in {
                    None,
                    *MATERIAL_CHANGE_REASONS,
                }:
                    raise PreviewHttpError(
                        400,
                        "MATERIAL_CHANGE_REASON_INVALID",
                        "D1材料变化原因不在现役枚举中",
                    )
                try:
                    result = read_source_research_day(
                        self.preview_server.state.config,
                        target_session=target,
                        source_capture_id=_capture_id(values["source_capture_id"]),
                        d1_history_session_count=_positive_int(
                            _nonnegative_int(
                                values["d1_history_session_count"],
                                "d1_history_session_count",
                                32767,
                            ),
                            "d1_history_session_count",
                            32767,
                        ),
                        history_session_count=(
                            _positive_int(
                                _nonnegative_int(
                                    values["history_session_count"],
                                    "history_session_count",
                                    250,
                                ),
                                "history_session_count",
                                250,
                            )
                            if "history_session_count" in values
                            else None
                        ),
                        profile_revision=(
                            _profile_revision(values["profile_revision"])
                            if "profile_revision" in values
                            else None
                        ),
                        offset=_nonnegative_int(values.get("offset", "0"), "offset"),
                        limit=_positive_int(
                            _nonnegative_int(values.get("limit", "30"), "limit", 50),
                            "limit",
                            50,
                        ),
                        symbol=symbol,
                        industry_snapshot_id=(
                            _snapshot_id(values["industry_snapshot_id"])
                            if "industry_snapshot_id" in values
                            else None
                        ),
                        concept_snapshot_id=(
                            _snapshot_id(values["concept_snapshot_id"])
                            if "concept_snapshot_id" in values
                            else None
                        ),
                        board_kind=values.get("board_kind"),
                        board_native_id=values.get("board_native_id"),
                        structure_type=values.get("structure_type"),
                        material_change_reason=values.get("material_change_reason"),
                    )
                except ResearchDayPgError as error:
                    raise PreviewHttpError(409, error.code, str(error)) from error
                selected_board = result.get("selected_board_observation")
                members = (
                    selected_board.get("members")
                    if isinstance(selected_board, Mapping)
                    else None
                )
                if (
                    values.get("board_kind") in {"INDUSTRY", "CONCEPT"}
                    and values.get("board_native_id")
                    and (
                        not isinstance(members, list)
                        or not isinstance(selected_board, Mapping)
                        or selected_board.get("status") != "AVAILABLE"
                    )
                ):
                    board_kind = values["board_kind"]
                    source_capture_id = _capture_id(values["source_capture_id"])
                    snapshot_value = values.get(
                        "industry_snapshot_id"
                        if board_kind == "INDUSTRY"
                        else "concept_snapshot_id"
                    )
                    if snapshot_value is not None:
                        try:
                            membership_page = read_tdx_sector_members(
                                self.preview_server.state.config,
                                snapshot_id=_snapshot_id(snapshot_value),
                                board_kind=board_kind,
                                board_native_id=values["board_native_id"],
                                target_session=target,
                                research_cutoff=self.preview_server.state.research_cutoff,
                                source_capture_id=source_capture_id,
                                profile_revision=values.get("profile_revision"),
                                offset=_nonnegative_int(
                                    values.get("offset", "0"), "offset"
                                ),
                                limit=_positive_int(
                                    _nonnegative_int(
                                        values.get("limit", "30"), "limit", 50
                                    ),
                                    "limit",
                                    50,
                                ),
                            )
                            result["selected_board_observation"] = (
                                _membership_only_board_observation(
                                    membership_page,
                                    board_kind=board_kind,
                                    board_native_id=values["board_native_id"],
                                )
                            )
                        except TdxSectorMembersApplicationError as error:
                            result["selected_board_observation"] = {
                                "status": "NOT_COMPUTED",
                                "reason": error.code,
                                "research_scope_status": "MEMBERSHIP_FALLBACK_UNAVAILABLE",
                            }
                        selected_board = result.get("selected_board_observation")
                        members = (
                            selected_board.get("members")
                            if isinstance(selected_board, Mapping)
                            else None
                        )
                if (
                    isinstance(members, list)
                    and values.get("board_kind") in {"INDUSTRY", "CONCEPT"}
                    and values.get("board_native_id")
                    and not (
                        isinstance(selected_board, Mapping)
                        and selected_board.get("membership_only") is True
                    )
                ):
                    source_capture_id = _capture_id(values["source_capture_id"])
                    source_context = read_source_version_context(
                        self.preview_server.state.config,
                        target_session=target,
                        source_capture_id=source_capture_id,
                        research_cutoff=self.preview_server.state.research_cutoff,
                    )
                    if source_context.get("status") == "AVAILABLE":
                        from stock_a.storage.postgres.t0_early_observations import (
                            read_source_t0_early_observations_for_symbols,
                            read_source_t0_early_triage_summaries_for_symbols,
                        )

                        symbols = [
                            member["source_symbol"]
                            for member in members
                            if isinstance(member, Mapping)
                            and isinstance(member.get("source_symbol"), str)
                        ]
                        contexts_by_symbol = (
                            read_source_t0_early_observations_for_symbols(
                                self.preview_server.state.config,
                                target_session=target,
                                source_capture_id=source_context["source_capture_id"],
                                source_symbols=sorted(set(symbols)),
                                board_kind=values["board_kind"],
                                board_native_id=values["board_native_id"],
                            )
                            if symbols
                            else {}
                        )
                        triage_by_symbol = (
                            read_source_t0_early_triage_summaries_for_symbols(
                                self.preview_server.state.config,
                                target_session=target,
                                source_capture_id=source_context["source_capture_id"],
                                source_symbols=sorted(set(symbols)),
                                selected_snapshot_ids=(
                                    {
                                        values["board_kind"]: _snapshot_id(
                                            values[
                                                "industry_snapshot_id"
                                                if values["board_kind"] == "INDUSTRY"
                                                else "concept_snapshot_id"
                                            ]
                                        )
                                    }
                                    if values.get(
                                        "industry_snapshot_id"
                                        if values.get("board_kind") == "INDUSTRY"
                                        else "concept_snapshot_id"
                                    )
                                    is not None
                                    else {}
                                ),
                            )
                            if symbols
                            else {}
                        )
                        for member in members:
                            if isinstance(member, dict):
                                member["t0_early_observations"] = (
                                    contexts_by_symbol.get(
                                        member.get("source_symbol"), []
                                    )
                                )
                                member["t0_early_status"] = (
                                    "AVAILABLE"
                                    if member["t0_early_observations"]
                                    else "NO_STORED_FACT"
                                )
                                member["t0_early_triage_summary"] = _t0_list_summary(
                                    _bind_t0_followup_status(
                                        triage_by_symbol.get(
                                            member.get("source_symbol"),
                                            {
                                                "status": "NO_STORED_FACT",
                                                "triage_state": "NO_T0_FACT",
                                            },
                                        ),
                                        target_session=target,
                                        research_cutoff=(
                                            self.preview_server.state.research_cutoff
                                        ),
                                    )
                                )
                self._send_json(200, {"ok": True, **result})
                return
            if parsed.path == "/api/product/market":
                values = _query(self.path, {"target_session", "source_capture_id"})
                if set(values) != {"target_session", "source_capture_id"}:
                    raise PreviewHttpError(
                        400, "QUERY_REQUIRED", "须提供目标日与来源捕获"
                    )
                target = _iso_date(values["target_session"])
                cutoff = self.preview_server.state.research_cutoff
                _require_research_cutoff(target, cutoff)
                breadth_error_payload: dict[str, Any] | None = None
                try:
                    market = read_source_market_context(
                        self.preview_server.state.config,
                        target_session=target,
                        research_cutoff=cutoff,
                        source_capture_id=_capture_id(values["source_capture_id"]),
                    )
                except SourceMarketContextError as error:
                    if error.code not in {
                        "MARKET_BREADTH_NOT_COMPUTED",
                        "MARKET_BREADTH_INCOMPLETE",
                    }:
                        raise
                    market = None
                    breadth_error_payload = {
                        "status": error.code.removeprefix("MARKET_BREADTH_"),
                        "target_session": target.isoformat(),
                        "reason": str(error),
                    }
                if market is not None:
                    try:
                        participation = read_source_market_participation(
                            self.preview_server.state.config, market
                        )
                    except (MarketParticipationPgError, RawSourcePgError) as error:
                        raise PreviewHttpError(
                            503,
                            getattr(error, "code", "MARKET_PARTICIPATION_READ_FAILED"),
                            str(error),
                        ) from error
                    cal_rev = market.calendar["calendar_revision"]
                    breadth_contract_ver = market.calculation.contract_version
                else:
                    from stock_a.application.source_market_context import (
                        SOURCE_MARKET_BREADTH_CONTRACT,
                        _load_calendar,
                    )
                    from stock_a.storage.postgres.market_participation import (
                        read_market_participation_standalone,
                    )

                    cal_payload, _meta, _revs, _sessions = _load_calendar(target)
                    cal_id = str(cal_payload["calendar_id"])
                    cal_rev = str(cal_payload["revision"])
                    breadth_contract_ver = SOURCE_MARKET_BREADTH_CONTRACT
                    try:
                        participation = read_market_participation_standalone(
                            self.preview_server.state.config,
                            target_session=target,
                            source_capture_id=_capture_id(values["source_capture_id"]),
                            calendar_id=cal_id,
                            calendar_revision=cal_rev,
                        )
                    except (MarketParticipationPgError, RawSourcePgError) as error:
                        raise PreviewHttpError(
                            503,
                            getattr(error, "code", "MARKET_PARTICIPATION_READ_FAILED"),
                            str(error),
                        ) from error
                from stock_a.contracts.loader import (
                    load_market_reference_indices_contract,
                )
                from stock_a.research.sector_native import (
                    compute_market_risk_projection,
                )
                from stock_a.storage.postgres.limit_up_ladder import (
                    read_market_limit_up_ladder_summary,
                )
                from stock_a.storage.postgres.market_breadth import (
                    read_market_breadth_change,
                )
                from stock_a.storage.postgres.market_indices import (
                    read_market_environment_display,
                    read_market_indices_and_style_summary,
                )

                capture_id_val = _capture_id(values["source_capture_id"])
                breadth_change = read_market_breadth_change(
                    self.preview_server.state.config,
                    target_session=target,
                    source_capture_id=capture_id_val,
                    calendar_revision=cal_rev,
                    contract_version=breadth_contract_ver,
                )
                limit_up_ladder = read_market_limit_up_ladder_summary(
                    self.preview_server.state.config,
                    target_session=target,
                    source_capture_id=capture_id_val,
                    calendar_revision=cal_rev,
                )
                idx_contract = load_market_reference_indices_contract(
                    Path(
                        self.preview_server.state.config["paths"]["configuration_root"]
                    )
                )
                primary_idx_sym = next(
                    item["source_symbol"]
                    for item in idx_contract["indices"]
                    if item["role"] == "PRIMARY_MARKET"
                )
                environment_display = read_market_environment_display(
                    self.preview_server.state.config,
                    target_session=target,
                    stock_capture_id=capture_id_val,
                    primary_index_symbol=primary_idx_sym,
                    calendar_revision=cal_rev,
                )
                pinned_idx_capture_id = (
                    environment_display.get("index_capture_id")
                    if environment_display.get("status") == "AVAILABLE"
                    else None
                )
                pinned_idx_config_ver = (
                    environment_display.get("index_config_version")
                    if environment_display.get("status") == "AVAILABLE"
                    else None
                )
                index_observations = read_market_indices_and_style_summary(
                    self.preview_server.state.config,
                    target_session=target,
                    calendar_revision=cal_rev,
                    index_contract=idx_contract,
                    index_capture_id=pinned_idx_capture_id,
                    config_version=pinned_idx_config_ver,
                )
                if market is not None:
                    base_payload = source_market_context_payload(
                        market,
                        market_participation=participation,
                        breadth_change=breadth_change,
                        environment_display=environment_display,
                    )
                    axes_map = base_payload.get("market_axes") or {}
                    pressure_for_risk = axes_map.get("pressure")
                    breadth_for_risk = axes_map.get("breadth")
                else:
                    from stock_a.application.source_market_context import (
                        market_environment_label,
                    )
                    from stock_a.storage.postgres.price_limits import (
                        read_market_pressure_summary,
                    )

                    standalone_pressure = read_market_pressure_summary(
                        self.preview_server.state.config,
                        target_session=target,
                        source_capture_id=capture_id_val,
                        calendar_revision=cal_rev,
                    )
                    p_cls = (
                        standalone_pressure.get("classification")
                        if standalone_pressure.get("status") == "OBSERVED"
                        else None
                    )
                    part_cls = (
                        (participation.get("metric") or {}).get(
                            "classification", participation.get("classification")
                        )
                        if participation.get("status")
                        not in {"NOT_COMPUTED", "NOT_COMPARABLE", "INCOMPLETE"}
                        else None
                    )
                    trend_cls = (
                        (environment_display.get("axes") or {}).get("trend")
                        if environment_display.get("status") == "AVAILABLE"
                        else None
                    )
                    if trend_cls not in {"STRONG", "NEUTRAL", "WEAK"}:
                        trend_cls = None
                    if trend_cls is None and isinstance(index_observations, Mapping):
                        for idx_item in index_observations.get("items") or ():
                            if isinstance(idx_item, Mapping):
                                cand = idx_item.get("trend_classification") or idx_item.get("trend")
                                if cand in {"STRONG", "NEUTRAL", "WEAK"}:
                                    trend_cls = cand
                                    break
                    composite = market_environment_label(
                        trend_cls,
                        None,
                        part_cls,
                        p_cls,
                    )
                    trend_axis = {
                        "status": "OBSERVED" if trend_cls else "NOT_COMPUTED",
                        "classification": trend_cls or "UNKNOWN",
                    }
                    breadth_axis = {
                        **(breadth_error_payload or {}),
                        "classification": "UNKNOWN",
                    }
                    base_payload = {
                        **(breadth_error_payload or {}),
                        "breadth": breadth_axis,
                        "participation": participation,
                        "breadth_change": breadth_change,
                        "market_axes": {
                            "breadth": breadth_axis,
                            "participation": participation,
                            "trend": trend_axis,
                            "pressure": standalone_pressure,
                            "composite": composite,
                        },
                    }
                    pressure_for_risk = standalone_pressure
                    breadth_for_risk = breadth_axis
                from stock_a.storage.postgres import source_raw
                from stock_a.storage.postgres.r03_formal_sector import (
                    read_r03_formal_market_breadth,
                )

                mkt_scope_hint: str | None = None
                req_profile_rev = values.get("profile_revision")
                req_u_t = values.get("universe_version_t")
                req_u_t3 = values.get("universe_version_t3")
                if capture_id_val is not None:
                    with source_raw._transaction(
                        self.preview_server.state.config, write=False, statement_timeout_ms=5000
                    ) as (_c, _cur, _):
                        scope_sql = """
                            SELECT DISTINCT scope_key
                            FROM stock.r03_formal_market_breadth_facts
                            WHERE trade_date=%s AND scope_key LIKE %s
                        """
                        scope_params: list[Any] = [target, f"BAOSTOCK_CAP_{capture_id_val}:%"]
                        if req_profile_rev:
                            scope_sql += " AND scope_key=%s"
                            scope_params.append(f"BAOSTOCK_CAP_{capture_id_val}:PROF_{req_profile_rev}:NON_PIT")
                        _cur.execute(scope_sql, tuple(scope_params))
                        matched_scopes = [str(r[0]) for r in _cur.fetchall()]
                    if not matched_scopes:
                        formal_breadth = {
                            "status": "NOT_READY_HISTORICAL_UNIVERSE",
                            "reason": f"捕获 {capture_id_val} 在 {target} 尚无已保存的正式市场宽度事实（禁止串读其他捕获）。",
                        }
                    elif len(matched_scopes) > 1:
                        formal_breadth = {
                            "status": "AMBIGUOUS_SCOPE",
                            "reason": f"捕获 {capture_id_val} 在 {target} 匹配到多个正式市场宽度口径({matched_scopes})，须指定精确 profile_revision。",
                        }
                    else:
                        mkt_scope_hint = matched_scopes[0]
                        formal_breadth = read_r03_formal_market_breadth(
                            self.preview_server.state.config,
                            trade_date=target,
                            scope_key=mkt_scope_hint,
                            universe_version_t=req_u_t,
                            universe_version_t3=req_u_t3,
                        )
                else:
                    formal_breadth = read_r03_formal_market_breadth(
                        self.preview_server.state.config,
                        trade_date=target,
                        universe_version_t=req_u_t,
                        universe_version_t3=req_u_t3,
                    )
                if formal_breadth.get("status") in {
                    "OBSERVED",
                    "UNKNOWN",
                    "AMBIGUOUS_SCOPE",
                    "NOT_READY_HISTORICAL_UNIVERSE",
                }:
                    base_payload["formal_market_breadth"] = formal_breadth
                market_risk = compute_market_risk_projection(
                    pressure_axis=pressure_for_risk,
                    breadth_axis=breadth_for_risk,
                    participation_axis=participation,
                )
                self._send_json(
                    200,
                    {
                        "ok": True,
                        **base_payload,
                        "limit_up_ladder": limit_up_ladder,
                        "index_observations": index_observations,
                        "environment_display": environment_display,
                        "market_risk": market_risk,
                    },
                )
                return
            if parsed.path == "/api/product/r03-sectors":
                allowed = {
                    "trade_date",
                    "scope_key",
                    "member_version",
                    "contract_version",
                    "board_kind",
                    "limit",
                    "offset",
                }
                required = {"trade_date", "scope_key", "member_version", "contract_version"}
                values = _query(self.path, allowed)
                if not required.issubset(set(values)):
                    raise PreviewHttpError(
                        400, "QUERY_REQUIRED", "须提供trade_date、scope_key、member_version与contract_version"
                    )
                trade_d = _iso_date(values["trade_date"])
                if trade_d > self.preview_server.state.research_cutoff:
                    raise PreviewHttpError(
                        400,
                        "RESEARCH_CUTOFF_EXCEEDED",
                        f"查询日期 {trade_d.isoformat()} 超过研究截止日 {self.preview_server.state.research_cutoff.isoformat()}。",
                    )
                b_kind = values.get("board_kind")
                if b_kind is not None and b_kind not in {"INDUSTRY", "CONCEPT"}:
                    raise PreviewHttpError(
                        400, "INVALID_BOARD_KIND", "board_kind 仅允许 INDUSTRY 或 CONCEPT。"
                    )
                try:
                    limit_val = int(values["limit"]) if "limit" in values else 50
                    offset_val = int(values["offset"]) if "offset" in values else 0
                except ValueError as err:
                    raise PreviewHttpError(
                        400, "INVALID_PAGINATION", "limit/offset 必须为整数。"
                    ) from err
                if limit_val <= 0 or limit_val > 500 or offset_val < 0:
                    raise PreviewHttpError(
                        400, "INVALID_PAGINATION", "limit 须在 [1, 500] 且 offset >= 0。"
                    )
                from stock_a.storage.postgres.r03_formal_sector import (
                    read_r03_native_day_facts,
                    read_r03_overlap_day,
                )

                native_res = read_r03_native_day_facts(
                    self.preview_server.state.config,
                    trade_date=trade_d,
                    scope_key=values["scope_key"],
                    member_version=values["member_version"],
                    contract_version=values["contract_version"],
                    board_kind=b_kind,
                    limit=limit_val,
                    offset=offset_val,
                )
                overlap_res = read_r03_overlap_day(
                    self.preview_server.state.config,
                    trade_date=trade_d,
                    scope_key=values["scope_key"],
                    member_version=values["member_version"],
                    contract_version=values["contract_version"],
                    board_kind=b_kind,
                )
                self._send_json(
                    200,
                    {
                        "ok": True,
                        "trade_date": trade_d.isoformat(),
                        "scope_key": values["scope_key"],
                        "native": native_res,
                        "overlap": overlap_res,
                    },
                )
                return
            if parsed.path == "/api/product/r03-sector-rotation":
                required = {"board_kind", "board_id", "scope_key", "contract_version"}
                allowed = required | {"as_of_date"}
                values = _query(self.path, allowed)
                if not required.issubset(set(values)):
                    raise PreviewHttpError(
                        400, "QUERY_REQUIRED", "须提供board_kind、board_id、scope_key与contract_version"
                    )
                if values["board_kind"] not in {"INDUSTRY", "CONCEPT"}:
                    raise PreviewHttpError(
                        400, "INVALID_BOARD_KIND", "board_kind 仅允许 INDUSTRY 或 CONCEPT。"
                    )
                as_of = (
                    _iso_date(values["as_of_date"])
                    if "as_of_date" in values
                    else self.preview_server.state.research_cutoff
                )
                if as_of > self.preview_server.state.research_cutoff:
                    raise PreviewHttpError(
                        400,
                        "RESEARCH_CUTOFF_EXCEEDED",
                        f"Rotation 截至日 {as_of.isoformat()} 超过研究截止日 {self.preview_server.state.research_cutoff.isoformat()}。",
                    )
                from stock_a.storage.postgres.r03_formal_sector import (
                    read_r03_rotation_timeline,
                )

                rot_res = read_r03_rotation_timeline(
                    self.preview_server.state.config,
                    board_kind=values["board_kind"],
                    board_id=values["board_id"],
                    scope_key=values["scope_key"],
                    contract_version=values["contract_version"],
                    as_of_date=as_of,
                )
                self._send_json(200, {"ok": True, "as_of_date": as_of.isoformat(), **rot_res})
                return
            if parsed.path == "/api/product/r03-sector-loo":
                required = {
                    "trade_date",
                    "target_entity_id",
                    "board_kind",
                    "scope_key",
                    "member_version",
                    "contract_version",
                }
                values = _query(self.path, required)
                if set(values) != required:
                    raise PreviewHttpError(
                        400, "QUERY_REQUIRED", "须提供完整LOO查询参数"
                    )
                if values["board_kind"] not in {"INDUSTRY", "CONCEPT"}:
                    raise PreviewHttpError(
                        400, "INVALID_BOARD_KIND", "board_kind 仅允许 INDUSTRY 或 CONCEPT。"
                    )
                trade_d = _iso_date(values["trade_date"])
                if trade_d > self.preview_server.state.research_cutoff:
                    raise PreviewHttpError(
                        400,
                        "RESEARCH_CUTOFF_EXCEEDED",
                        f"查询日期 {trade_d.isoformat()} 超过研究截止日 {self.preview_server.state.research_cutoff.isoformat()}。",
                    )
                from stock_a.storage.postgres.r03_formal_sector import (
                    read_r03_loo_support_fact,
                )

                loo_res = read_r03_loo_support_fact(
                    self.preview_server.state.config,
                    trade_date=trade_d,
                    target_entity_id=values["target_entity_id"],
                    board_kind=values["board_kind"],
                    scope_key=values["scope_key"],
                    member_version=values["member_version"],
                    contract_version=values["contract_version"],
                )
                self._send_json(200, {"ok": True, **loo_res})
                return
            if parsed.path == "/api/product/sector-pulse-history":
                allowed = {
                    "target_session",
                    "research_cutoff",
                    "source_capture_id",
                    "profile_revision",
                    "snapshot_id",
                    "board_kind",
                    "board_native_id",
                }
                values = _query(self.path, allowed)
                if set(values) != allowed:
                    raise PreviewHttpError(
                        400,
                        "SECTOR_PULSE_HISTORY_QUERY_INVALID",
                        "须提供目标会话、来源捕获、画像修订、快照与板块身份",
                    )
                target = _iso_date(values["target_session"])
                cutoff = _iso_date(values["research_cutoff"])
                configured_cutoff = self.preview_server.state.research_cutoff
                if cutoff != configured_cutoff:
                    raise PreviewHttpError(
                        409,
                        "SECTOR_PULSE_HISTORY_CUTOFF_MISMATCH",
                        "历史PULSE研究截止必须与当前DEV配置一致",
                    )
                _require_research_cutoff(target, cutoff)
                board_kind = values["board_kind"]
                if board_kind not in {"INDUSTRY", "CONCEPT"}:
                    raise PreviewHttpError(
                        400,
                        "SECTOR_PULSE_HISTORY_BOARD_KIND_INVALID",
                        "板块类型必须为INDUSTRY或CONCEPT",
                    )
                board_native_id = values["board_native_id"]
                if not board_native_id or len(board_native_id) > 64:
                    raise PreviewHttpError(
                        400,
                        "SECTOR_PULSE_HISTORY_BOARD_ID_INVALID",
                        "板块原生编号为空或超出长度范围",
                    )
                calendar = _official_calendar(target)
                if target not in calendar["sessions"]:
                    raise PreviewHttpError(
                        409,
                        "SECTOR_PULSE_HISTORY_TARGET_NOT_OFFICIAL",
                        "目标会话不在当前官方交易日历中",
                    )
                calendar_sessions = tuple(calendar["sessions"])
                target_index = calendar_sessions.index(target)
                first_index = max(0, target_index - 6)
                history_sessions = calendar_sessions[first_index : target_index + 1]
                calendar_window = calendar_sessions[first_index : target_index + 6]
                try:
                    from stock_a.storage.postgres.sector_pulse_history import (
                        read_source_sector_pulse_history,
                    )

                    result = read_source_sector_pulse_history(
                        self.preview_server.state.config,
                        target_session=target,
                        research_cutoff=cutoff,
                        target_source_capture_id=_capture_id(
                            values["source_capture_id"]
                        ),
                        profile_revision=_profile_revision(values["profile_revision"]),
                        snapshot_id=_positive_int(
                            _nonnegative_int(
                                values["snapshot_id"], "snapshot_id", 2**31 - 1
                            ),
                            "snapshot_id",
                            2**31 - 1,
                        ),
                        board_kind=board_kind,
                        board_native_id=board_native_id,
                        calendar_id=calendar["calendar_id"],
                        calendar_revision=calendar["calendar_revision"],
                        calendar_sessions=calendar_window,
                        history_sessions=history_sessions,
                    )
                except ValueError as error:
                    raise PreviewHttpError(
                        409,
                        getattr(error, "code", "SECTOR_PULSE_HISTORY_UNAVAILABLE"),
                        str(error),
                    ) from error
                self._send_json(200, {"ok": True, **result})
                return
            if parsed.path == "/api/product/sector-pulse-directory":
                allowed = {
                    "target_session",
                    "research_cutoff",
                    "source_capture_id",
                    "profile_revision",
                    "industry_snapshot_id",
                    "concept_snapshot_id",
                    "board_kind",
                    "search",
                    "offset",
                    "limit",
                }
                values = _query(self.path, allowed)
                required = allowed - {"search", "offset", "limit"}
                if not required.issubset(values):
                    raise PreviewHttpError(
                        400,
                        "SECTOR_PULSE_DIRECTORY_QUERY_INVALID",
                        "须提供日期、来源捕获、画像修订、双快照与板块类型",
                    )
                target = _iso_date(values["target_session"])
                cutoff = _iso_date(values["research_cutoff"])
                if cutoff != self.preview_server.state.research_cutoff:
                    raise PreviewHttpError(
                        409,
                        "SECTOR_PULSE_DIRECTORY_CUTOFF_MISMATCH",
                        "PULSE目录截止必须与当前DEV配置一致",
                    )
                _require_research_cutoff(target, cutoff)
                board_kind = values["board_kind"]
                if board_kind not in {"ALL", "INDUSTRY", "CONCEPT"}:
                    raise PreviewHttpError(
                        400,
                        "SECTOR_PULSE_DIRECTORY_BOARD_KIND_INVALID",
                        "板块类型须为ALL、INDUSTRY或CONCEPT",
                    )
                search = values.get("search", "").strip()
                if len(search) > 80:
                    raise PreviewHttpError(
                        400,
                        "SECTOR_PULSE_DIRECTORY_SEARCH_INVALID",
                        "板块搜索最多80个字符",
                    )
                offset = _nonnegative_int(values.get("offset", "0"), "offset")
                limit = _positive_int(
                    _nonnegative_int(values.get("limit", "15"), "limit", 50),
                    "limit",
                    50,
                )
                industry_snapshot_id = _positive_int(
                    _nonnegative_int(
                        values["industry_snapshot_id"],
                        "industry_snapshot_id",
                        2**31 - 1,
                    ),
                    "industry_snapshot_id",
                    2**31 - 1,
                )
                concept_snapshot_id = _positive_int(
                    _nonnegative_int(
                        values["concept_snapshot_id"],
                        "concept_snapshot_id",
                        2**31 - 1,
                    ),
                    "concept_snapshot_id",
                    2**31 - 1,
                )
                snapshot_pair_provenance = _verified_sector_snapshot_pair(
                    self.preview_server.state.config,
                    industry_snapshot_id=industry_snapshot_id,
                    concept_snapshot_id=concept_snapshot_id,
                )
                calendar = _official_calendar(target)
                if target not in calendar["sessions"]:
                    raise PreviewHttpError(
                        409,
                        "SECTOR_PULSE_DIRECTORY_TARGET_NOT_OFFICIAL",
                        "目标会话不在当前官方交易日历中",
                    )
                calendar_sessions = tuple(calendar["sessions"])
                target_index = calendar_sessions.index(target)
                horizon_targets = {
                    horizon: (
                        calendar_sessions[target_index + horizon]
                        if target_index + horizon < len(calendar_sessions)
                        else None
                    )
                    for horizon in (1, 3, 5)
                }
                try:
                    source = read_source_version_context(
                        self.preview_server.state.config,
                        target_session=target,
                        source_capture_id=_capture_id(values["source_capture_id"]),
                        research_cutoff=self.preview_server.state.research_cutoff,
                    )
                    if (
                        source.get("status") != "AVAILABLE"
                        or source.get("version_status") != "COMPLETE"
                        or source.get("raw_row_count")
                        != source.get("capture_expected_row_count")
                    ):
                        raise PreviewHttpError(
                            409,
                            "SECTOR_PULSE_DIRECTORY_SOURCE_NOT_CLOSED",
                            "目标会话没有唯一且闭合的精确Raw来源",
                        )
                    from stock_a.storage.postgres.sector_pulse_directory import (
                        read_source_sector_pulse_directory_page,
                    )

                    prof_rev = _profile_revision(values["profile_revision"])
                    result = read_source_sector_pulse_directory_page(
                        self.preview_server.state.config,
                        target_session=target,
                        research_cutoff=cutoff,
                        source_capture_id=source["source_capture_id"],
                        source_pool_count=source["raw_row_count"],
                        profile_revision=prof_rev,
                        snapshot_ids={
                            "INDUSTRY": industry_snapshot_id,
                            "CONCEPT": concept_snapshot_id,
                        },
                        calendar_id=calendar["calendar_id"],
                        calendar_revision=calendar["calendar_revision"],
                        horizon_targets=horizon_targets,
                        board_kind=board_kind,
                        search=search,
                        offset=offset,
                        limit=limit,
                    )
                    from stock_a.application.r03_formal_sector import (
                        R03_CONTRACT_VERSION,
                    )
                    from stock_a.storage.postgres import source_raw as _sr_dir
                    from stock_a.storage.postgres.r03_formal_sector import (
                        read_r03_native_day_facts,
                        read_r03_overlap_day,
                    )

                    r03_scope = (
                        f"BAOSTOCK_CAP_{source['source_capture_id']}:PROF_{prof_rev}:NON_PIT"
                    )
                    r03_member_ver = f"IND_{industry_snapshot_id}:CON_{concept_snapshot_id}"
                    r03_native = read_r03_native_day_facts(
                        self.preview_server.state.config,
                        trade_date=target,
                        scope_key=r03_scope,
                        member_version=r03_member_ver,
                        contract_version=R03_CONTRACT_VERSION,
                    )
                    r03_overlap = read_r03_overlap_day(
                        self.preview_server.state.config,
                        trade_date=target,
                        scope_key=r03_scope,
                        member_version=r03_member_ver,
                        contract_version=R03_CONTRACT_VERSION,
                    )
                    rot_by_key: dict[tuple[str, str], dict[str, Any]] = {}
                    with _sr_dir._transaction(
                        self.preview_server.state.config,
                        write=False,
                        statement_timeout_ms=5000,
                    ) as (_dc, _dcur, _):
                        _dcur.execute(
                            """
                            SELECT board_kind, board_id, rotation_state, last_known_state, reason_code
                            FROM stock.r03_sector_rotation_board_day_facts
                            WHERE trade_date=%s AND scope_key=%s AND member_version=%s AND contract_version=%s
                            """,
                            (target, r03_scope, r03_member_ver, R03_CONTRACT_VERSION),
                        )
                        for bk, bid, r_st, lk_st, rsn in _dcur.fetchall():
                            rot_by_key[(str(bk), str(bid))] = {
                                "rotation_state": str(r_st),
                                "last_known_state": lk_st,
                                "reason_code": rsn,
                            }
                    native_by_key = {
                        (str(it["board_kind"]), str(it["board_id"])): it
                        for it in r03_native.get("items", ())
                    }
                    ov_by_key = {
                        (str(it["board_kind"]), str(it["board_id"])): it
                        for it in r03_overlap.get("board_facts", ())
                    }
                    for dir_item in result.get("items", ()):
                        dk = (
                            str(dir_item.get("board_kind", "")),
                            str(dir_item.get("board_native_id", "")),
                        )
                        n_it = native_by_key.get(dk)
                        o_it = ov_by_key.get(dk)
                        rt_it = rot_by_key.get(dk)
                        if n_it or o_it or rt_it:
                            dir_item["r03_formal"] = {
                                "effective_maturity": n_it.get("effective_maturity") if n_it else None,
                                "display_maturity": n_it.get("display_maturity") if n_it else None,
                                "health_state": n_it.get("health_state") if n_it else None,
                                "exhaustion_level": n_it.get("exhaustion_level") if n_it else None,
                                "emergence_level": n_it.get("emergence_level") if n_it else None,
                                "priority_status": n_it.get("priority_status") if n_it else None,
                                "priority_rank": n_it.get("priority_rank") if n_it else None,
                                "unique_share_status": o_it.get("unique_share_status") if o_it else None,
                                "unique_share": str(o_it["unique_share"]) if o_it and o_it.get("unique_share") is not None else None,
                                "cluster_id": o_it.get("cluster_id") if o_it else None,
                                "rotation_state": rt_it.get("rotation_state") if rt_it else None,
                            }
                    result["r03_formal_sector_context"] = {
                        "scope_key": r03_scope,
                        "member_version": r03_member_ver,
                        "contract_version": R03_CONTRACT_VERSION,
                        "native_status": r03_native.get("status"),
                        "overlap_status": r03_overlap.get("status"),
                        "clusters": r03_overlap.get("clusters", []),
                        "pairs": [
                            {
                                **p,
                                "jaccard_value": (
                                    str(p["jaccard_value"])
                                    if p.get("jaccard_value") is not None
                                    else None
                                ),
                            }
                            for p in r03_overlap.get("pairs", ())
                        ],
                    }
                except PreviewHttpError:
                    raise
                except ValueError as error:
                    raise PreviewHttpError(
                        409,
                        getattr(error, "code", "SECTOR_PULSE_DIRECTORY_UNAVAILABLE"),
                        str(error),
                    ) from error
                result["snapshot_pair_provenance"] = snapshot_pair_provenance
                self._send_json(200, {"ok": True, **result})
                return
            if parsed.path == "/api/product/resolve-stock-code":
                allowed = {"target_session", "source_capture_id", "stock_code"}
                values = _query(self.path, allowed)
                if not {"target_session", "stock_code"}.issubset(values):
                    raise PreviewHttpError(
                        400,
                        "QUERY_REQUIRED",
                        "须提供目标交易日、来源捕获和六位股票代码",
                    )
                target = _iso_date(values["target_session"])
                _require_research_cutoff(
                    target, self.preview_server.state.research_cutoff
                )
                try:
                    resolved = resolve_baostock_stock_code(
                        self.preview_server.state.config,
                        stock_code=values["stock_code"],
                        target_session=target,
                        research_cutoff=self.preview_server.state.research_cutoff,
                        source_capture_id=(
                            _capture_id(values["source_capture_id"])
                            if values.get("source_capture_id")
                            else None
                        ),
                    )
                except (RawSourcePgError, BaoStockPriceFactsError) as error:
                    if error.code == "STOCK_CODE_INVALID":
                        status = 400
                    elif error.code == "STOCK_CODE_NOT_FOUND":
                        status = 404
                    elif error.code in {
                        "SOURCE_CODE_POOL_INCOMPLETE",
                        "STOCK_CODE_AMBIGUOUS",
                    }:
                        status = 409
                    else:
                        raise
                    raise PreviewHttpError(status, error.code, str(error)) from error
                self._send_json(200, {"ok": True, "status": "AVAILABLE", **resolved})
                return
            if parsed.path == "/api/product/cohort-keys":
                values = _query(self.path, {"target_session", "source_capture_id"})
                if set(values) != {"target_session", "source_capture_id"}:
                    raise PreviewHttpError(
                        400, "QUERY_REQUIRED", "须提供目标日和来源捕获"
                    )
                target = _iso_date(values["target_session"])
                _require_research_cutoff(
                    target, self.preview_server.state.research_cutoff
                )
                source_capture_id = _capture_id(values["source_capture_id"])
                keys = read_persisted_cohort_keys(
                    self.preview_server.state.config,
                    target=target,
                    research_cutoff=self.preview_server.state.research_cutoff,
                    source_capture_id=source_capture_id,
                )
                if any(key["capture_count"] != 1 for key in keys):
                    raise PreviewHttpError(
                        409,
                        "COHORT_CAPTURE_AMBIGUOUS",
                        "精确规则键含多个来源捕获，不能合并分母",
                    )
                if len(keys) > 100:
                    raise PreviewHttpError(
                        409,
                        "COHORT_KEYS_TOO_MANY",
                        "冻结研究键超过读取上限，请缩小范围",
                    )
                self._send_json(
                    200,
                    {
                        "ok": True,
                        "keys": keys,
                        "status": "SAVED" if keys else "NO_STORED_FACT",
                    },
                )
                return
            if parsed.path == "/api/product/cohort":
                allowed = {
                    "target_session",
                    "source_capture_id",
                    "profile_revision",
                    "rule",
                    "history",
                    "d1_history",
                    "window",
                    "horizon",
                    "offset",
                    "symbol",
                    "rps_board_kind",
                    "rps_scenario",
                    "rps_rule_state",
                    "include_rps_pulse_groups",
                }
                values = _query(self.path, allowed)
                required = allowed - {
                    "offset",
                    "symbol",
                    "rps_board_kind",
                    "rps_scenario",
                    "rps_rule_state",
                    "include_rps_pulse_groups",
                }
                if not required.issubset(values):
                    raise PreviewHttpError(
                        400, "QUERY_REQUIRED", "须提供完整冻结研究键"
                    )
                target = _iso_date(values["target_session"])
                cutoff = self.preview_server.state.research_cutoff
                _require_research_cutoff(target, cutoff)
                symbol = values.get("symbol") or None
                if symbol is not None and not _SYMBOL.fullmatch(symbol):
                    raise PreviewHttpError(400, "SYMBOL_INVALID", "来源代码格式无效")
                if values["horizon"] not in {"1", "3", "5", "10", "20"}:
                    raise PreviewHttpError(
                        400, "HORIZON_INVALID", "后继会话数须为1、3、5、10或20"
                    )
                rps_board_kind = values.get("rps_board_kind", "ALL")
                if rps_board_kind not in {"ALL", "INDUSTRY", "CONCEPT"}:
                    raise PreviewHttpError(
                        400,
                        "COHORT_RPS_BOARD_KIND_INVALID",
                        "RPS×PULSE分组板块类型须为全部、行业或概念",
                    )
                rps_scenario = values.get("rps_scenario", "ALL")
                if rps_scenario not in {
                    "ALL",
                    "DOUBLE_IMPROVED_PULSE_FALSE",
                    "DOUBLE_IMPROVED_PULSE_TRUE",
                    "RPS5_UP_RPS20_DOWN_PULSE_TRUE",
                    "DOUBLE_WORSENED_PULSE_TRUE",
                    "QUALITY_UNKNOWN",
                }:
                    raise PreviewHttpError(
                        400,
                        "COHORT_RPS_SCENARIO_INVALID",
                        "RPS×PULSE分组筛选无效",
                    )
                rps_rule_state = values.get("rps_rule_state", "ALL")
                if rps_rule_state not in {"ALL", "TRUE", "FALSE", "UNKNOWN"}:
                    raise PreviewHttpError(
                        400,
                        "COHORT_RPS_RULE_STATE_INVALID",
                        "原来源规则状态筛选无效",
                    )
                include_rps_pulse_groups_value = values.get(
                    "include_rps_pulse_groups", "false"
                )
                if include_rps_pulse_groups_value not in {"true", "false"}:
                    raise PreviewHttpError(
                        400,
                        "COHORT_RPS_GROUPS_FLAG_INVALID",
                        "RPS×PULSE分组读取标记须为true或false",
                    )
                try:
                    result = read_persisted_cohort(
                        self.preview_server.state.config,
                        target=target,
                        research_cutoff=cutoff,
                        source_capture_id=_capture_id(values["source_capture_id"]),
                        profile_revision=_profile_revision(values["profile_revision"]),
                        history_session_count=_positive_int(
                            _nonnegative_int(values["history"], "history", 500),
                            "history",
                            500,
                        ),
                        d1_history_session_count=_positive_int(
                            _nonnegative_int(values["d1_history"], "d1_history", 500),
                            "d1_history",
                            500,
                        ),
                        event_window_start=_iso_date(values["window"]),
                        rule=values["rule"],
                        horizon=_positive_int(
                            _nonnegative_int(values["horizon"], "horizon", 120),
                            "horizon",
                            120,
                        ),
                        offset=_nonnegative_int(values.get("offset", "0"), "offset"),
                        limit=30,
                        symbol=symbol,
                        include_rps_pulse_groups=(
                            include_rps_pulse_groups_value == "true"
                        ),
                        rps_board_kind=rps_board_kind,
                        rps_scenario=rps_scenario,
                        rps_rule_state=rps_rule_state,
                    )
                except ResearchFactConflict as error:
                    raise PreviewHttpError(
                        409, "COHORT_CAPTURE_AMBIGUOUS", str(error)
                    ) from error
                except ResearchSymbolNotFound as error:
                    raise PreviewHttpError(
                        404, "COHORT_SYMBOL_NOT_FOUND", str(error)
                    ) from error
                except ResearchFactNotFound as error:
                    self._send_json(
                        200,
                        {
                            "ok": True,
                            "storage_status": "NO_STORED_FACT",
                            "source_pool_count": None,
                            "groups": [],
                            "results": [],
                            "reason": str(error),
                        },
                    )
                    return
                result["storage_status"] = (
                    "SAVED" if result["source_pool_count"] else "NO_STORED_FACT"
                )
                self._send_json(200, {"ok": True, **result})
                return
            if parsed.path == "/api/product/cohort-enrollment-lifecycle":
                allowed = {
                    "target_session",
                    "source_capture_id",
                    "profile_revision",
                    "history",
                    "d1_history",
                    "window",
                    "rule",
                }
                values = _query(self.path, allowed)
                if set(values) != allowed:
                    raise PreviewHttpError(
                        400,
                        "COHORT_ENROLLMENT_QUERY_INVALID",
                        "须提供完整T0、来源捕获、画像窗口及冻结规则键",
                    )
                target = _iso_date(values["target_session"])
                cutoff = self.preview_server.state.research_cutoff
                _require_research_cutoff(target, cutoff)
                try:
                    result = read_source_enrollment_cohort(
                        self.preview_server.state.config,
                        target=target,
                        research_cutoff=cutoff,
                        source_capture_id=_capture_id(values["source_capture_id"]),
                        profile_revision=_profile_revision(values["profile_revision"]),
                        history_session_count=_positive_int(
                            _nonnegative_int(values["history"], "history", 500),
                            "history",
                            500,
                        ),
                        d1_history_session_count=_positive_int(
                            _nonnegative_int(values["d1_history"], "d1_history", 500),
                            "d1_history",
                            500,
                        ),
                        event_window_start=_iso_date(values["window"]),
                        rule=values["rule"],
                    )
                except ResearchFactConflict as error:
                    raise PreviewHttpError(
                        409, "COHORT_ENROLLMENT_FACT_CONFLICT", str(error)
                    ) from error
                except ValueError as error:
                    raise PreviewHttpError(
                        400, "COHORT_ENROLLMENT_QUERY_INVALID", str(error)
                    ) from error
                self._send_json(200, {"ok": True, **result})
                return
            if parsed.path == "/api/product/source-observation-segments":
                allowed = {
                    "as_of",
                    "research_cutoff",
                    "source_capture_id",
                    "rule",
                    "status",
                    "event",
                    "offset",
                    "limit",
                    "event_offset",
                    "event_limit",
                    "outcome_horizon",
                }
                values = _query(self.path, allowed)
                if not {
                    "as_of",
                    "research_cutoff",
                    "source_capture_id",
                }.issubset(values):
                    raise PreviewHttpError(
                        400,
                        "SOURCE_OBSERVATION_SEGMENT_QUERY_INVALID",
                        "须提供明确观察日期、研究截止和来源捕获ID",
                    )
                as_of = _iso_date(values["as_of"])
                cutoff = _iso_date(values["research_cutoff"])
                configured_cutoff = self.preview_server.state.research_cutoff
                if cutoff != configured_cutoff:
                    raise PreviewHttpError(
                        409,
                        "SOURCE_OBSERVATION_SEGMENT_CUTOFF_MISMATCH",
                        "请求研究截止必须与本机DEV固定截止一致",
                    )
                _require_research_cutoff(as_of, cutoff)
                rule_id = values.get("rule")
                if rule_id in {None, "", "ALL"}:
                    rule_id = None
                status = values.get("status")
                if status in {None, "", "ALL"}:
                    status = None
                event_code = values.get("event")
                if event_code in {None, "", "ALL"}:
                    event_code = None
                from stock_a.storage.postgres.source_observation_segments import (
                    read_source_observation_segments,
                )

                try:
                    result = read_source_observation_segments(
                        self.preview_server.state.config,
                        as_of=as_of,
                        research_cutoff=cutoff,
                        source_capture_id=_capture_id(values["source_capture_id"]),
                        rule_id=rule_id,
                        status=status,
                        event_code=event_code,
                        offset=_nonnegative_int(values.get("offset", "0"), "offset"),
                        limit=_positive_int(
                            _nonnegative_int(values.get("limit", "20"), "limit", 100),
                            "limit",
                            100,
                        ),
                        event_offset=_nonnegative_int(
                            values.get("event_offset", "0"), "event_offset"
                        ),
                        event_limit=_positive_int(
                            _nonnegative_int(
                                values.get("event_limit", "20"), "event_limit", 100
                            ),
                            "event_limit",
                            100,
                        ),
                        outcome_horizon=_positive_int(
                            _nonnegative_int(
                                values.get("outcome_horizon", "1"),
                                "outcome_horizon",
                                5,
                            ),
                            "outcome_horizon",
                            5,
                        ),
                    )
                except ValueError as error:
                    raise PreviewHttpError(
                        400,
                        "SOURCE_OBSERVATION_SEGMENT_QUERY_INVALID",
                        str(error),
                    ) from error
                self._send_json(200, {"ok": True, **result})
                return
            if parsed.path == "/api/product/cohort-date-comparison":
                values = _query(
                    self.path,
                    {"source_capture_id", "rule", "horizon"},
                )
                if set(values) != {"source_capture_id", "rule", "horizon"}:
                    raise PreviewHttpError(
                        400,
                        "COHORT_DATE_COMPARISON_QUERY_INVALID",
                        "须提供来源捕获、冻结规则和期限",
                    )
                if values["horizon"] not in {"1", "3", "5"}:
                    raise PreviewHttpError(
                        400,
                        "COHORT_DATE_COMPARISON_HORIZON_INVALID",
                        "跨日期研究对照只读取T+1、T+3或T+5",
                    )
                try:
                    from stock_a.storage.postgres.cohort_stats import (
                        read_cohort_date_comparison,
                    )

                    result = read_cohort_date_comparison(
                        self.preview_server.state.config,
                        research_cutoff=self.preview_server.state.research_cutoff,
                        source_capture_id=_capture_id(values["source_capture_id"]),
                        rule=values["rule"],
                        horizon=int(values["horizon"]),
                    )
                except ValueError as error:
                    raise PreviewHttpError(
                        400, "COHORT_DATE_COMPARISON_QUERY_INVALID", str(error)
                    ) from error
                self._send_json(200, {"ok": True, **result})
                return
            if parsed.path == "/api/product/fep-baseline":
                values = _query(
                    self.path,
                    {"source_capture_id", "rule", "horizon", "lane"},
                )
                if set(values) != {"source_capture_id", "rule", "horizon", "lane"}:
                    raise PreviewHttpError(
                        400,
                        "FEP_BASELINE_QUERY_INVALID",
                        "须提供来源捕获、冻结规则、期限及ENTRY、DAILY_LANDMARK或REENTRY lane",
                    )
                if values["horizon"] not in {"1", "3", "5"}:
                    raise PreviewHttpError(
                        400,
                        "FEP_BASELINE_HORIZON_INVALID",
                        "FEP描述统计仅读取T+1、T+3或T+5已存事实",
                    )
                if values["lane"] not in {"ENTRY", "DAILY_LANDMARK", "REENTRY"}:
                    raise PreviewHttpError(
                        400,
                        "FEP_BASELINE_LANE_INVALID",
                        "FEP lane只接受ENTRY、DAILY_LANDMARK或REENTRY",
                    )
                try:
                    from stock_a.storage.postgres.fep_baseline import (
                        read_fep_baseline,
                    )

                    result = read_fep_baseline(
                        self.preview_server.state.config,
                        research_cutoff=self.preview_server.state.research_cutoff,
                        source_capture_id=_capture_id(values["source_capture_id"]),
                        rule_id=values["rule"],
                        horizon=int(values["horizon"]),
                        lane=values["lane"],
                    )
                except ValueError as error:
                    raise PreviewHttpError(
                        400, "FEP_BASELINE_QUERY_INVALID", str(error)
                    ) from error
                self._send_json(200, {"ok": True, **result})
                return
            if parsed.path == "/api/product/cohort-symbol-forward":
                allowed = {
                    "target_session",
                    "source_capture_id",
                    "profile_revision",
                    "history",
                    "d1_history",
                    "window",
                    "rule",
                    "symbol",
                }
                values = _query(self.path, allowed)
                if set(values) != allowed:
                    raise PreviewHttpError(
                        400,
                        "COHORT_SYMBOL_FORWARD_QUERY_INVALID",
                        "须提供完整T0规则键和来源代码",
                    )
                target = _iso_date(values["target_session"])
                cutoff = self.preview_server.state.research_cutoff
                _require_research_cutoff(target, cutoff)
                symbol = values["symbol"]
                if not _SYMBOL.fullmatch(symbol):
                    raise PreviewHttpError(400, "SYMBOL_INVALID", "来源代码格式无效")
                calendar = _official_calendar(cutoff)
                if target not in calendar["sessions"]:
                    raise PreviewHttpError(
                        409,
                        "COHORT_SYMBOL_FORWARD_CALENDAR_UNAVAILABLE",
                        "目标日不在官方日历范围内",
                    )
                try:
                    from stock_a.storage.postgres.cohort_stats import (
                        read_cohort_symbol_forward_paths,
                    )

                    result = read_cohort_symbol_forward_paths(
                        self.preview_server.state.config,
                        target=target,
                        research_cutoff=cutoff,
                        source_capture_id=_capture_id(values["source_capture_id"]),
                        profile_revision=_profile_revision(values["profile_revision"]),
                        history_session_count=_positive_int(
                            _nonnegative_int(values["history"], "history", 500),
                            "history",
                            500,
                        ),
                        d1_history_session_count=_positive_int(
                            _nonnegative_int(values["d1_history"], "d1_history", 500),
                            "d1_history",
                            500,
                        ),
                        event_window_start=_iso_date(values["window"]),
                        rule=values["rule"],
                        symbol=symbol,
                        horizon_due_sessions=tuple(
                            day for day in calendar["sessions"] if day > target
                        )[:20],
                    )
                except ValueError as error:
                    raise PreviewHttpError(
                        400, "COHORT_SYMBOL_FORWARD_QUERY_INVALID", str(error)
                    ) from error
                self._send_json(200, {"ok": True, **result})
                return
            if parsed.path == "/api/preview/sector-change-context":
                values = _query(
                    self.path,
                    {
                        "target_session",
                        "source_capture_id",
                        "price_coordinate",
                        "history_session_count",
                        "profile_revision",
                    },
                )
                if not {
                    "target_session",
                    "source_capture_id",
                    "price_coordinate",
                }.issubset(values):
                    raise PreviewHttpError(
                        400,
                        "SECTOR_CHANGE_CONTEXT_QUERY_INVALID",
                        "板块变化目录须精确提供target_session、source_capture_id和price_coordinate",
                    )
                target = _iso_date(values["target_session"])
                _require_research_cutoff(
                    target, self.preview_server.state.research_cutoff
                )
                price_coordinate = _price_coordinate(values["price_coordinate"])
                if price_coordinate != "FORWARD_ADJUSTED":
                    raise PreviewHttpError(
                        400,
                        "SECTOR_CHANGE_PRICE_COORDINATE_INVALID",
                        "板块变化及跨会话画像固定使用BaoStock FORWARD_ADJUSTED坐标",
                    )
                requested_source_capture_id = _capture_id(values["source_capture_id"])
                requested_history_session_count = (
                    _positive_int(
                        _nonnegative_int(
                            values["history_session_count"],
                            "history_session_count",
                            250,
                        ),
                        "history_session_count",
                        250,
                    )
                    if "history_session_count" in values
                    else None
                )
                requested_profile_revision = (
                    _profile_revision(values["profile_revision"])
                    if "profile_revision" in values
                    else None
                )
                if requested_profile_revision is not None and (
                    requested_history_session_count is None
                ):
                    raise PreviewHttpError(
                        400,
                        "SECTOR_CHANGE_PROFILE_SCOPE_INVALID",
                        "精确画像修订必须同时提供history_session_count",
                    )
                catalog = read_tdx_sector_source_snapshot_catalog(
                    self.preview_server.state.config
                )
                profile_observation = None
                history_session_count = None
                d1_history_session_count = None
                if "source_capture_id" in values:
                    source_capture_id = requested_source_capture_id
                    source_context = read_source_version_context(
                        self.preview_server.state.config,
                        target_session=target,
                        source_capture_id=source_capture_id,
                        research_cutoff=self.preview_server.state.research_cutoff,
                        price_coordinate=price_coordinate,
                    )
                    try:
                        profile_observation = read_source_symbol_price_profile_observation(
                            self.preview_server.state.config,
                            self.preview_server.state.profile_contract,
                            target_session=target,
                            source_capture_id=source_context["source_capture_id"],
                            research_cutoff=self.preview_server.state.research_cutoff,
                            history_session_count=requested_history_session_count,
                            profile_revision=requested_profile_revision,
                        )
                    except PgPriceProfileError as error:
                        profile_observation = {
                            "status": "NOT_COMPUTED",
                            "reason_code": error.code,
                            "reason": str(error),
                        }
                    history_session_count = _selected_profile_history_session_count(
                        profile_observation,
                        requested_history_session_count=requested_history_session_count,
                        requested_profile_revision=requested_profile_revision,
                    )
                    try:
                        d1_scope = read_source_research_day_scope(
                            self.preview_server.state.config,
                            target_session=target,
                            source_capture_id=source_capture_id,
                        )
                    except ResearchDayPgError as error:
                        raise PreviewHttpError(409, error.code, str(error)) from error
                    if d1_scope.get("status") == "SAVED_SCOPE_UNIQUE":
                        d1_history_session_count = d1_scope["d1_history_session_count"]
                sessions = ()
                calendar_revision = None
                if history_session_count is not None:
                    try:
                        _resolved_target, sessions, calendar_revision = (
                            resolve_pg_history_window(
                                target.isoformat(), history_session_count
                            )
                        )
                    except ResearchEntryError as error:
                        raise PreviewHttpError(409, error.code, str(error)) from error
                snapshots = []
                for snapshot in catalog["snapshots"]:
                    observed = datetime.fromisoformat(snapshot["observed_at"])
                    observed_after = observed.date() > target
                    snapshots.append(
                        {
                            **snapshot,
                            "observed_after_target": observed_after,
                            "temporal_relationship": (
                                "OBSERVED_AFTER_TARGET"
                                if observed_after
                                else "OBSERVED_ON_OR_BEFORE_TARGET"
                            ),
                        }
                    )
                window_options = []
                if history_session_count is not None:
                    for event_session_count in (1, 5, 10):
                        if len(sessions) < event_session_count + 1:
                            continue
                        window_options.append(
                            {
                                "event_session_count": event_session_count,
                                "event_window_start": sessions[
                                    -(event_session_count + 1)
                                ].isoformat(),
                                "target_session": target.isoformat(),
                                "pg_history_session_count": min(
                                    history_session_count,
                                    event_session_count + 23,
                                ),
                            }
                        )
                self._send_json(
                    200,
                    {
                        "ok": True,
                        "target_session": target.isoformat(),
                        "research_cutoff": self.preview_server.state.research_cutoff.isoformat(),
                        "source_capture_id": requested_source_capture_id,
                        "price_coordinate": price_coordinate,
                        "time_role": "NON_PIT_RETROSPECTIVE",
                        "official_calendar_revision": calendar_revision,
                        "previous_session": sessions[-2].isoformat()
                        if len(sessions) >= 2
                        else None,
                        "source_history_start": sessions[0].isoformat()
                        if sessions
                        else None,
                        "source_history_session_count": history_session_count,
                        "d1_history_session_count": d1_history_session_count,
                        "profile_observation": profile_observation,
                        "last_service_error": self.preview_server.state.last_service_error,
                        "window_options": window_options,
                        "snapshots": snapshots,
                        "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
                        "identity_status": "SOURCE_SYMBOL_ONLY",
                        "pit_status": "NON_PIT_RETROSPECTIVE",
                        "database_write": False,
                        "ddl_executed": False,
                        "dml_executed": False,
                    },
                )
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
                catalog = read_baostock_price_catalog(
                    self.preview_server.state.config,
                    cutoff=self.preview_server.state.research_cutoff,
                )
                grouped: dict[tuple[str, int], dict[str, Mapping[str, Any]]] = {}
                for item in catalog.get("price_coordinate_versions", []):
                    session_text = item.get("session_date")
                    if not isinstance(session_text, str):
                        continue
                    if year is not None and int(session_text[:4]) != year:
                        continue
                    key = (session_text, int(item["source_capture_id"]))
                    grouped.setdefault(key, {})[item["price_coordinate"]] = item
                all_versions = []
                for (session_text, capture_id), coordinates in sorted(grouped.items()):
                    raw_version = coordinates.get("RAW_UNADJUSTED", {})
                    qfq_version = coordinates.get("FORWARD_ADJUSTED", {})
                    expected_rows = raw_version.get("expected_row_count")
                    all_versions.append(
                        {
                            "session_date": session_text,
                            "source_capture_id": capture_id,
                            "price_coordinate": "RAW_UNADJUSTED",
                            "price_coordinates": [
                                "RAW_UNADJUSTED",
                                "FORWARD_ADJUSTED",
                            ],
                            "version_status": (
                                "CLOSED"
                                if raw_version.get("coverage_status") == "COMPLETE"
                                and qfq_version.get("coverage_status") == "COMPLETE"
                                else "INCOMPLETE"
                            ),
                            "row_count": raw_version.get("row_count", 0),
                            "expected_row_count": expected_rows or 0,
                            "qfq_row_count": qfq_version.get("row_count", 0),
                            "qfq_expected_row_count": qfq_version.get(
                                "expected_row_count", 0
                            ),
                            "raw_observed_value_row_count": raw_version.get(
                                "observed_value_row_count", 0
                            ),
                            "qfq_observed_value_row_count": qfq_version.get(
                                "observed_value_row_count", 0
                            ),
                            "bar_status_counts": {
                                "TARGET_PRESENT": raw_version.get("row_count", 0),
                                "TARGET_MISSING": 0,
                            },
                        }
                    )
                total_version_count = len(all_versions)
                versions = all_versions[offset : offset + limit]
                for version in versions:
                    if (
                        not isinstance(version, Mapping)
                        or version.get("price_coordinates")
                        != ["RAW_UNADJUSTED", "FORWARD_ADJUSTED"]
                        or version.get("source_capture_id") is None
                    ):
                        raise PreviewHttpError(
                            503,
                            "PRICE_CAPTURE_CATALOG_IDENTITY_INCOMPLETE",
                            "BaoStock价格目录缺少真实捕获ID或坐标",
                        )
                    _capture_id(version["source_capture_id"])
                self._send_json(
                    200,
                    {
                        "ok": True,
                        "offset": offset,
                        "limit": limit,
                        "page_count": len(versions),
                        "total_version_count": total_version_count,
                        "next_offset": (
                            offset + len(versions)
                            if offset + len(versions) < total_version_count
                            else None
                        ),
                        "versions": versions,
                        "identity_status": "SOURCE_SYMBOL_ONLY",
                        "pit_status": "NON_PIT_RETROSPECTIVE",
                        "database_write": False,
                        "ddl_executed": False,
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
                        "price_coordinate": "FORWARD_ADJUSTED",
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
                        "before_source_capture_id",
                        "before_profile_revision",
                        "after_source_capture_id",
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
                        "price_coordinate",
                    },
                )
                if not {
                    "before_session",
                    "after_session",
                    "price_coordinate",
                }.issubset(values):
                    raise PreviewHttpError(
                        400,
                        "PROFILE_CHANGE_QUERY_REQUIRED",
                        "变化查询必须显式提供前后两个画像日期",
                    )
                price_coordinate = _price_coordinate(
                    values.get("price_coordinate", "FORWARD_ADJUSTED")
                )
                if price_coordinate != "FORWARD_ADJUSTED":
                    raise PreviewHttpError(
                        400,
                        "PROFILE_PRICE_COORDINATE_INVALID",
                        "Core/Profile变化查询固定使用BaoStock FORWARD_ADJUSTED坐标",
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
                    before_source_capture_id=values.get("before_source_capture_id"),
                    before_profile_revision=values.get("before_profile_revision"),
                    after_source_capture_id=values.get("after_source_capture_id"),
                    after_profile_revision=values.get("after_profile_revision"),
                    price_coordinate=price_coordinate,
                )
                result.update(
                    {
                        "ok": True,
                        "research_mode": "ACCEPTED_PG_PROFILE_CHANGE_OBSERVATION",
                        "price_coordinate": price_coordinate,
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
                        "before_source_capture_id",
                        "before_profile_revision",
                        "after_source_capture_id",
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
                        "price_coordinate",
                    },
                )
                required = {
                    "before_session",
                    "after_session",
                    "before_source_capture_id",
                    "before_profile_revision",
                    "after_source_capture_id",
                    "after_profile_revision",
                    "relation_field",
                    "price_coordinate",
                }
                if not required.issubset(values):
                    raise PreviewHttpError(
                        400,
                        "PROFILE_RELATION_QUERY_REQUIRED",
                        "RAW关系查询必须绑定前后日期、来源捕获ID、画像修订和关系字段",
                    )
                price_coordinate = _price_coordinate(values["price_coordinate"])
                if price_coordinate != "FORWARD_ADJUSTED":
                    raise PreviewHttpError(
                        400,
                        "PROFILE_PRICE_COORDINATE_INVALID",
                        "Core价格关系筛选固定使用BaoStock FORWARD_ADJUSTED坐标",
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
                    _nonnegative_int(values.get("limit", "50"), "limit", 100) or 0,
                    "limit",
                    100,
                )
                result = query_source_symbol_price_profile_relation_screen(
                    self.preview_server.state.config,
                    before_session=before_session,
                    after_session=after_session,
                    max_target_session=self.preview_server.state.research_cutoff,
                    offset=_nonnegative_int(values.get("offset", "0"), "offset"),
                    limit=limit,
                    relation_field=values["relation_field"],
                    transition_type=values.get("transition_type", "ALL"),
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
                    before_source_capture_id=_capture_id(
                        values["before_source_capture_id"]
                    ),
                    before_profile_revision=_profile_revision(
                        values["before_profile_revision"]
                    ),
                    after_source_capture_id=_capture_id(
                        values["after_source_capture_id"]
                    ),
                    after_profile_revision=_profile_revision(
                        values["after_profile_revision"]
                    ),
                    price_coordinate=price_coordinate,
                )
                result.update(
                    {
                        "ok": True,
                        "price_coordinate": price_coordinate,
                        "database_write": False,
                        "ddl_executed": False,
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
                        "before_source_capture_id",
                        "before_profile_revision",
                        "after_source_capture_id",
                        "after_profile_revision",
                        "price_coordinate",
                    },
                )
                required = {
                    "before_session",
                    "after_session",
                    "before_source_capture_id",
                    "before_profile_revision",
                    "after_source_capture_id",
                    "after_profile_revision",
                    "price_coordinate",
                }
                if not required.issubset(values):
                    raise PreviewHttpError(
                        400,
                        "PROFILE_BREADTH_QUERY_REQUIRED",
                        "来源池广度必须精确提供前后日期、来源捕获ID和画像修订",
                    )
                price_coordinate = _price_coordinate(
                    values.get("price_coordinate", "FORWARD_ADJUSTED")
                )
                if price_coordinate != "FORWARD_ADJUSTED":
                    raise PreviewHttpError(
                        400,
                        "PROFILE_PRICE_COORDINATE_INVALID",
                        "来源池广度固定使用BaoStock FORWARD_ADJUSTED坐标",
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
                    before_source_capture_id=_capture_id(
                        values["before_source_capture_id"]
                    ),
                    before_profile_revision=_profile_revision(
                        values["before_profile_revision"]
                    ),
                    after_source_capture_id=_capture_id(
                        values["after_source_capture_id"]
                    ),
                    after_profile_revision=_profile_revision(
                        values["after_profile_revision"]
                    ),
                    price_coordinate=price_coordinate,
                )
                result.update(
                    {
                        "ok": True,
                        "research_mode": "SOURCE_SYMBOL_NON_PIT_PRICE_PROFILE_BREADTH",
                        "price_coordinate": price_coordinate,
                        "database_write": False,
                        "ddl_executed": False,
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
                        "before_source_capture_id",
                        "before_profile_revision",
                        "after_source_capture_id",
                        "after_profile_revision",
                        "price_coordinate",
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
                    "before_source_capture_id",
                    "before_profile_revision",
                    "after_source_capture_id",
                    "after_profile_revision",
                    "price_coordinate",
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
                price_coordinate = _price_coordinate(values["price_coordinate"])
                if price_coordinate != "FORWARD_ADJUSTED":
                    raise PreviewHttpError(
                        400,
                        "PROFILE_PRICE_COORDINATE_INVALID",
                        "来源池广度成员固定使用BaoStock FORWARD_ADJUSTED坐标",
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
                    before_source_capture_id=_capture_id(
                        values["before_source_capture_id"]
                    ),
                    before_profile_revision=_profile_revision(
                        values["before_profile_revision"]
                    ),
                    after_source_capture_id=_capture_id(
                        values["after_source_capture_id"]
                    ),
                    after_profile_revision=_profile_revision(
                        values["after_profile_revision"]
                    ),
                    price_coordinate=price_coordinate,
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
                        "price_coordinate": price_coordinate,
                        "database_write": False,
                        "ddl_executed": False,
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
                        "before_source_capture_id",
                        "before_profile_revision",
                        "after_source_capture_id",
                        "after_profile_revision",
                        "price_coordinate",
                    },
                )
                required = {
                    "before_session",
                    "after_session",
                    "symbol",
                    "before_source_capture_id",
                    "before_profile_revision",
                    "after_source_capture_id",
                    "after_profile_revision",
                    "price_coordinate",
                }
                if set(values) != required:
                    raise PreviewHttpError(
                        400,
                        "PROFILE_PAIR_QUERY_REQUIRED",
                        "双日代码对照必须精确提供前后日期、来源捕获ID、画像修订和来源代码",
                    )
                price_coordinate = _price_coordinate(values["price_coordinate"])
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
                    before_source_capture_id=_capture_id(
                        values["before_source_capture_id"]
                    ),
                    before_profile_revision=_profile_revision(
                        values["before_profile_revision"]
                    ),
                    after_source_capture_id=_capture_id(
                        values["after_source_capture_id"]
                    ),
                    after_profile_revision=_profile_revision(
                        values["after_profile_revision"]
                    ),
                    price_coordinate=price_coordinate,
                )
                result.update(
                    {
                        "ok": True,
                        "research_mode": "SOURCE_SYMBOL_NON_PIT_PRICE_PROFILE_PAIR",
                        "price_coordinate": price_coordinate,
                        "formal_listing_universe": "UNKNOWN_NOT_CONNECTED",
                        "database_write": False,
                        "ddl_executed": False,
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
                        "source_capture_id",
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
                        "price_coordinate",
                    },
                )
                required = {
                    "target_session",
                    "profile_revision",
                    "price_coordinate",
                }
                if not required.issubset(values):
                    raise PreviewHttpError(
                        400,
                        "PROFILE_QUERY_REQUIRED",
                        "画像查询必须显式绑定日期和画像修订；capture可按日期唯一解析",
                    )
                price_coordinate = _price_coordinate(
                    values.get("price_coordinate", "FORWARD_ADJUSTED")
                )
                if price_coordinate != "FORWARD_ADJUSTED":
                    raise PreviewHttpError(
                        400,
                        "PROFILE_PRICE_COORDINATE_INVALID",
                        "价格画像固定使用BaoStock FORWARD_ADJUSTED坐标",
                    )
                target = _iso_date(values["target_session"])
                _require_research_cutoff(
                    target, self.preview_server.state.research_cutoff
                )
                source_capture_id = (
                    _capture_id(values["source_capture_id"])
                    if values.get("source_capture_id")
                    else None
                )
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
                    source_capture_id=source_capture_id,
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
                    price_coordinate=price_coordinate,
                )
                result.update(
                    {
                        "ok": True,
                        "research_mode": "SOURCE_SYMBOL_NON_PIT_PRICE_PROFILE",
                        "price_coordinate": price_coordinate,
                        "database_write": False,
                        "ddl_executed": False,
                        "baostock_called": False,
                    }
                )
                self._send_json(200, result)
                return
            if parsed.path == "/api/preview/raw":
                values = _query(
                    self.path, {"target_session", "source_capture_id", "symbol"}
                )
                required = {"target_session", "symbol"}
                if not required.issubset(values):
                    raise PreviewHttpError(
                        400,
                        "PREVIEW_QUERY_REQUIRED",
                        "未复权行情查询必须提供日期和来源代码；捕获ID可选",
                    )
                target = _iso_date(values["target_session"])
                _require_research_cutoff(
                    target, self.preview_server.state.research_cutoff
                )
                requested_capture_id = (
                    _capture_id(values["source_capture_id"])
                    if values.get("source_capture_id")
                    else None
                )
                symbol = _symbol(values["symbol"])
                detail = read_baostock_price_detail(
                    self.preview_server.state.config,
                    source_symbol=symbol,
                    target_session=target,
                    research_cutoff=self.preview_server.state.research_cutoff,
                    source_capture_id=requested_capture_id,
                )
                if detail.get("status") == "AMBIGUOUS_CAPTURE":
                    raise PreviewHttpError(
                        409,
                        "PRICE_CAPTURE_ID_REQUIRED",
                        "目标日存在多个BaoStock捕获；请求必须明确指定source_capture_id",
                    )
                row = detail.get("row")
                if detail.get("status") != "AVAILABLE" or row is None:
                    raise PreviewHttpError(
                        404,
                        "PG_SOURCE_NO_MATCH",
                        "PG中没有该明确日期、来源捕获ID与来源代码的BaoStock双坐标价格事实",
                    )
                source_capture_id = _capture_id(row.get("source_capture_id"))
                if (
                    requested_capture_id is not None
                    and source_capture_id != requested_capture_id
                ):
                    raise PreviewHttpError(
                        409,
                        "PRICE_CAPTURE_IDENTITY_MISMATCH",
                        "未复权行情捕获与请求不一致",
                    )
                source_context = _baostock_source_context(
                    self.preview_server.state.config,
                    target_session=target,
                    research_cutoff=self.preview_server.state.research_cutoff,
                    source_capture_id=source_capture_id,
                    target_price_rows=(row,),
                    price_coordinate="RAW_UNADJUSTED",
                )
                raw = _raw_public_fields(row, price_coordinate="RAW_UNADJUSTED")
                identity = self.preview_server.state.snapshot_identity(
                    target_session=target.isoformat(),
                    source_capture_id=source_capture_id,
                    price_coordinate="RAW_UNADJUSTED",
                )
                self._send_json(
                    200,
                    {
                        "ok": True,
                        "symbol": symbol,
                        "target_session": target.isoformat(),
                        "source_capture_id": source_capture_id,
                        "snapshot_identity": identity,
                        "raw": raw,
                        "source_version": source_context,
                        "database_write": False,
                    },
                )
                return
            if parsed.path == "/api/source-symbol/raw-periods":
                values = _query(
                    self.path,
                    {
                        "target_session",
                        "source_capture_id",
                        "symbol",
                        "session_count",
                        "price_coordinate",
                    },
                )
                self._raw_period_request_context = {
                    "target_session": values.get("target_session"),
                    "symbol": values.get("symbol"),
                    "source_capture_id": values.get("source_capture_id"),
                    "price_coordinate": values.get("price_coordinate"),
                    "research_cutoff": self.preview_server.state.research_cutoff.isoformat(),
                    "identity_status": "REQUEST_NOT_RESOLVED",
                }
                required = {"target_session", "symbol", "price_coordinate"}
                if not required.issubset(values) or set(values) - required - {
                    "source_capture_id",
                    "session_count",
                }:
                    raise PreviewHttpError(
                        400,
                        "PRICE_PERIOD_QUERY_REQUIRED",
                        "周期查询须提供日期、来源代码和价格坐标",
                    )
                target = _iso_date(values["target_session"])
                _require_research_cutoff(
                    target, self.preview_server.state.research_cutoff
                )
                requested_capture_id = (
                    _capture_id(values["source_capture_id"])
                    if values.get("source_capture_id")
                    else None
                )
                price_coordinate = _price_coordinate(values["price_coordinate"])
                symbol = _symbol(values["symbol"])
                session_count = _positive_int(
                    _nonnegative_int(
                        values.get("session_count", "75"), "session_count", 75
                    )
                    or 0,
                    "session_count",
                    75,
                )
                resolved_target, sessions, calendar_revision = (
                    resolve_pg_history_window(target.isoformat(), session_count)
                )
                if resolved_target != target:
                    raise PreviewHttpError(
                        400,
                        "PREVIEW_DATE_INVALID",
                        "目标日期未通过官方交易日历校验",
                    )
                history = _read_baostock_history(
                    self.preview_server.state.config,
                    source_symbol=symbol,
                    target_session=target,
                    source_capture_id=requested_capture_id,
                    price_coordinate=price_coordinate,
                    sessions=sessions,
                    cutoff=self.preview_server.state.research_cutoff,
                )
                source_capture_id = _capture_id(history.get("source_capture_id"))
                if (
                    requested_capture_id is not None
                    and source_capture_id != requested_capture_id
                ) or history.get("price_coordinate") != price_coordinate:
                    self._raw_period_request_context["identity_status"] = (
                        "REQUEST_IDENTITY_MISMATCH"
                    )
                    raise PreviewHttpError(
                        409,
                        "PRICE_PERIOD_IDENTITY_MISMATCH",
                        "BaoStock周期捕获或价格坐标与请求不一致",
                    )
                source_context = _baostock_source_context(
                    self.preview_server.state.config,
                    target_session=target,
                    research_cutoff=self.preview_server.state.research_cutoff,
                    source_capture_id=source_capture_id,
                    target_price_rows=history["history_rows"],
                    price_coordinate=price_coordinate,
                )
                if source_context["status"] != "AVAILABLE":
                    self._raw_period_request_context.update(
                        {
                            "identity_status": "REQUEST_IDENTITY_MISMATCH",
                        }
                    )
                    raise PreviewHttpError(
                        409,
                        "PREVIEW_SOURCE_IDENTITY_MISMATCH",
                        "周期返回的BaoStock捕获与请求不一致",
                    )
                calendar_payload, _metadata, _year_revision, _ = (
                    research_entry._load_calendar(target)
                )
                calendar_sessions, coverage_start, coverage_end = load_period_calendar(
                    target
                )
                period_bars = build_source_symbol_raw_periods(
                    target_session=target,
                    session_dates=sessions,
                    calendar_sessions=calendar_sessions,
                    history_rows=history["history_rows"],
                    price_coordinate=price_coordinate,
                    calendar_coverage_start=coverage_start,
                    calendar_coverage_end=coverage_end,
                )
                qfq_period_bars = (
                    period_bars
                    if price_coordinate == "FORWARD_ADJUSTED"
                    else build_source_symbol_raw_periods(
                        target_session=target,
                        session_dates=sessions,
                        calendar_sessions=calendar_sessions,
                        history_rows=tuple(
                            {**row, "price_coordinate": "FORWARD_ADJUSTED"}
                            for row in history["history_rows"]
                        ),
                        price_coordinate="FORWARD_ADJUSTED",
                        calendar_coverage_start=coverage_start,
                        calendar_coverage_end=coverage_end,
                    )
                )
                history_summary = summarize_raw_history_rows(
                    session_dates=sessions,
                    history_rows=history["history_rows"],
                    price_coordinate=price_coordinate,
                )
                profile_contract = self.preview_server.state.profile_contract
                profile_rules = profile_contract.get("profile_expression_registry", {})
                field_registry = profile_contract.get("field_registry", [])
                trends = {
                    kind.lower(): evaluate_raw_period_trend(
                        kind=kind,
                        periods=qfq_period_bars[kind],
                        target_session=target,
                        calendar_id=calendar_payload["calendar_id"],
                        calendar_revision=calendar_revision,
                        source_cutoff=(
                            f"BAOSTOCK_CAPTURE:{source_capture_id}:FORWARD_ADJUSTED:THROUGH:{target.isoformat()}"
                        ),
                        profile_rules=profile_rules,
                        field_registry=field_registry,
                        price_coordinate="FORWARD_ADJUSTED",
                    )
                    for kind in ("WEEK", "MONTH")
                }

                def trend_payload(item):
                    item_capture_id = _capture_id(item.source_capture_id)
                    item_coordinate = _price_coordinate(item.price_coordinate)
                    time_role = item.time_role
                    if (
                        item_capture_id != source_capture_id
                        or item_coordinate != "FORWARD_ADJUSTED"
                        or not isinstance(time_role, str)
                        or not time_role.strip()
                    ):
                        raise PreviewHttpError(
                            409,
                            "PRICE_PERIOD_TREND_IDENTITY_MISMATCH",
                            "周月趋势必须绑定同一paired capture的QFQ坐标和有效time_role",
                        )
                    return {
                        "kind": item.kind,
                        "rule_id": item.rule_id,
                        "status": item.status,
                        "value": item.value,
                        "reason": item.reason,
                        "source_capture_id": item_capture_id,
                        "price_coordinate": item_coordinate,
                        "time_role": time_role,
                        "required_period_count": item.required_period_count,
                        "complete_period_count": item.complete_period_count,
                        "input_period_keys": list(item.input_period_keys),
                    }

                def period_payload(item, kind: str):
                    payload = raw_period_payload(item)
                    if not isinstance(payload, dict):
                        raise PreviewHttpError(
                            500,
                            "PRICE_PERIOD_PAYLOAD_INVALID",
                            "周期价格Reader未返回对象",
                        )
                    returned_coordinate = payload.get("price_coordinate")
                    returned_capture = payload.get("source_capture_id")
                    returned_time_role = payload.get("time_role")
                    trend = trends[kind.lower()]
                    time_role = trend.time_role
                    if (
                        (
                            returned_coordinate is not None
                            and returned_coordinate != price_coordinate
                        )
                        or (
                            returned_capture is not None
                            and _capture_id(returned_capture) != source_capture_id
                        )
                        or (
                            returned_time_role is not None
                            and returned_time_role != time_role
                        )
                    ):
                        raise PreviewHttpError(
                            409,
                            "PRICE_PERIOD_IDENTITY_MISMATCH",
                            "周期价格条目的捕获或坐标与请求不一致",
                        )
                    payload["price_coordinate"] = price_coordinate
                    payload["source_capture_id"] = source_capture_id
                    payload["time_role"] = time_role
                    return payload

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
                    target_session=target.isoformat(),
                    source_capture_id=source_capture_id,
                    price_coordinate=price_coordinate,
                    session_count=len(sessions),
                    session_start=sessions[0].isoformat(),
                    calendar_revision=calendar_revision,
                )
                self._send_json(
                    200,
                    {
                        "ok": True,
                        "research_mode": "NON_PIT_RETROSPECTIVE",
                        "identity_status": "SOURCE_SYMBOL_ONLY",
                        "stable_security_id_status": "STABLE_SECURITY_ID_UNVERIFIED",
                        "listing_status": "LISTING_STATUS_UNKNOWN",
                        "suspension_status": "SUSPENSION_STATUS_UNKNOWN",
                        "official_listing_universe_status": "UNKNOWN",
                        "pit_status": "NON_PIT_RETROSPECTIVE",
                        "price_coordinate": price_coordinate,
                        "time_role": "NON_PIT_RETROSPECTIVE",
                        "target_session": target.isoformat(),
                        "source_symbol": symbol,
                        "symbol": symbol,
                        "source_capture_id": source_capture_id,
                        "snapshot_identity": identity,
                        "source_version": source_context,
                        "selection": {
                            "source_symbol": symbol,
                            "target_session": target.isoformat(),
                            "source_capture_id": source_capture_id,
                            "price_coordinate": price_coordinate,
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
                                period_payload(period, "WEEK")
                                for period in period_bars["WEEK"]
                            ],
                            "month": [
                                period_payload(period, "MONTH")
                                for period in period_bars["MONTH"]
                            ],
                        },
                        "trends": {
                            name: trend_payload(item) for name, item in trends.items()
                        },
                        "formal_period_profile": {
                            "closed_week": "UNKNOWN",
                            "closed_month": "UNKNOWN",
                            "reason": "PERIOD_PROFILE_NOT_COMPUTED_FOR_EXACT_CAPTURE",
                        },
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
                        "source_capture_id",
                        "price_coordinate",
                        "offset",
                        "limit",
                        "market",
                        "board",
                    },
                )
                if not {"target_session", "price_coordinate"}.issubset(values):
                    raise PreviewHttpError(
                        400,
                        "PREVIEW_QUERY_REQUIRED",
                        "候选分页必须显式提供目标日期和价格坐标",
                    )
                target = _iso_date(values["target_session"])
                _require_research_cutoff(
                    target, self.preview_server.state.research_cutoff
                )
                price_coordinate = _price_coordinate(values["price_coordinate"])
                requested_capture_id = (
                    _capture_id(values["source_capture_id"])
                    if values.get("source_capture_id")
                    else None
                )
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
                if market not in {"ALL", "SH", "SZ", "BJ"} or board not in {
                    "ALL",
                    "MAIN_BOARD",
                    "GEM",
                    "STAR_BOARD",
                    "BEIJING_BOARD",
                }:
                    raise PreviewHttpError(
                        400, "PREVIEW_FILTER_INVALID", "市场或板别筛选无效"
                    )
                page = read_baostock_price_page(
                    self.preview_server.state.config,
                    target_session=target,
                    cutoff=self.preview_server.state.research_cutoff,
                    source_capture_id=requested_capture_id,
                    price_coordinate=price_coordinate,
                    offset=offset,
                    limit=limit,
                    market=market,
                    board=board,
                )
                if page.get("status") == "AMBIGUOUS_CAPTURE":
                    raise PreviewHttpError(
                        409,
                        "PRICE_CAPTURE_ID_REQUIRED",
                        "该目标日存在多个BaoStock捕获；候选分页必须明确source_capture_id",
                    )
                if page["version_status"] == "EMPTY":
                    raise PreviewHttpError(
                        404,
                        "PG_SOURCE_NO_MATCH",
                        "PG中没有该日期和价格坐标对应的BaoStock来源版本",
                    )
                source_capture_id = _capture_id(page.get("source_capture_id"))
                if (
                    requested_capture_id is not None
                    and source_capture_id != requested_capture_id
                ) or page.get("price_coordinate") != price_coordinate:
                    raise PreviewHttpError(
                        409,
                        "PRICE_PAGE_IDENTITY_MISMATCH",
                        "候选页捕获或坐标与请求不一致",
                    )
                source_context = _baostock_source_context(
                    self.preview_server.state.config,
                    target_session=target,
                    research_cutoff=self.preview_server.state.research_cutoff,
                    source_capture_id=source_capture_id,
                    target_price_rows=page["rows"],
                    price_coordinate=price_coordinate,
                )
                identity = self.preview_server.state.snapshot_identity(
                    target_session=target.isoformat(),
                    source_capture_id=source_capture_id,
                    price_coordinate=price_coordinate,
                )
                self._send_json(
                    200,
                    {
                        "ok": True,
                        "snapshot_identity": identity,
                        "page": _page_public_fields(
                            page, price_coordinate=price_coordinate
                        ),
                        "source_version": source_context,
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
            "/api/preview/t0-early-observations",
            "/api/preview/sector-change-stocks",
            "/api/product/d1-material",
            "/api/product/board-comparison",
            "/api/product/data-jobs/plan",
            "/api/product/data-jobs/start",
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
        if post_path == "/api/preview/t0-early-observations" and self.path != post_path:
            self._error(
                PreviewHttpError(
                    400,
                    "T0_EARLY_DETAIL_QUERY_REJECTED",
                    "T0板内详情POST只接受JSON请求体，不接受查询参数",
                )
            )
            return
        if post_path == "/api/product/d1-material" and self.path != post_path:
            self._error(
                PreviewHttpError(
                    400,
                    "D1_MATERIAL_QUERY_REJECTED",
                    "D1材料观察POST只接受JSON请求体，不接受查询参数",
                )
            )
            return
        if post_path == "/api/product/board-comparison" and self.path != post_path:
            self._error(
                PreviewHttpError(
                    400,
                    "BOARD_COMPARISON_QUERY_REJECTED",
                    "双板块比较POST只接受JSON请求体",
                )
            )
            return
        if (
            post_path
            in {
                "/api/product/data-jobs/plan",
                "/api/product/data-jobs/start",
            }
            and self.path != post_path
        ):
            self._error(
                PreviewHttpError(
                    400,
                    "DATA_JOB_QUERY_INVALID",
                    "数据任务计划与启动只接受JSON请求体。",
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
            retired_path = _legacy_price_identity(body)
            if retired_path:
                raise PreviewHttpError(
                    400,
                    "RETIRED_PRICE_IDENTITY_REJECTED",
                    f"请求含已退役的TDX行情包身份（{retired_path}）；请使用BaoStock捕获ID和板块快照ID。",
                )
            if post_path == "/api/preview/source-launch-watch":
                self._send_json(200, self._source_launch_watch(body))
                return
            if post_path == "/api/preview/t0-early-observations":
                self._send_json(200, self._source_t0_early_observations(body))
                return
            if post_path == "/api/product/d1-material":
                self._send_json(200, self._d1_material(body))
                return
            if post_path == "/api/product/board-comparison":
                self._send_json(200, self._board_comparison(body))
                return
            if post_path == "/api/product/data-jobs/plan":
                plan = self.preview_server.state.data_jobs.plan(body)
                self._send_json(200, {"ok": True, "plan": plan})
                return
            if post_path == "/api/product/data-jobs/start":
                result = self.preview_server.state.data_jobs.start(body)
                if not result.get("accepted"):
                    plan = result.get("plan", {})
                    code = str(plan.get("status") or "DATA_JOB_NOT_EXECUTABLE")
                    message = (
                        "当前数据边界或计划条件不允许启动；未发起来源读取或行情写入。"
                    )
                    self._send_json(
                        409,
                        {
                            "ok": False,
                            "code": code,
                            "message": message,
                            "plan": plan,
                        },
                    )
                    return
                self._send_json(202, {"ok": True, **result})
                return
            if post_path == "/api/preview/sector-change-stocks":
                self._send_json(200, self._sector_change_stocks(body))
                return
            if (
                post_path == "/api/preview/stock-timeline"
                and isinstance(body, dict)
                and body.get("post_signal_price_facts") is True
            ):
                required = {
                    "post_signal_price_facts",
                    "price_coordinate",
                    "symbol",
                    "target_session",
                    "source_capture_id",
                    "research_cutoff",
                    "session_count",
                }
                if set(body) != required:
                    raise PreviewHttpError(
                        400,
                        "PREVIEW_BODY_FIELDS_INVALID",
                        "后续价格事实请求字段不完整或含额外字段",
                    )
                symbol = _symbol(body["symbol"])
                post_signal_coordinate = _price_coordinate(body["price_coordinate"])
                if post_signal_coordinate != "FORWARD_ADJUSTED":
                    raise PreviewHttpError(
                        400,
                        "PRICE_COORDINATE_INVALID",
                        "跨会话后续价格观察固定使用BaoStock FORWARD_ADJUSTED坐标",
                    )
                target = _iso_date(body["target_session"])
                cutoff = _iso_date(body["research_cutoff"], "research_cutoff")
                _require_research_cutoff(target, cutoff)
                _require_research_cutoff(
                    cutoff, self.preview_server.state.research_cutoff
                )
                source_capture_id = _capture_id(body["source_capture_id"])
                count = _positive_int(body["session_count"], "session_count", 10)
                calendar = _official_calendar(cutoff)
                all_sessions = calendar["sessions"]
                if target not in all_sessions:
                    raise PreviewHttpError(
                        400, "TARGET_DATE_INVALID", "目标日不在官方交易日历中"
                    )
                sessions = tuple(
                    day for day in all_sessions if target <= day <= cutoff
                )[: count + 1]
                history = _read_baostock_history(
                    self.preview_server.state.config,
                    source_symbol=symbol,
                    target_session=target,
                    source_capture_id=source_capture_id,
                    sessions=sessions,
                    cutoff=cutoff,
                    price_coordinate="FORWARD_ADJUSTED",
                    starts_at_target=True,
                )
                result = post_signal_price_facts(
                    sessions, history["history_rows"], count
                )
                result.update(
                    {
                        "symbol": symbol,
                        "target_session": target.isoformat(),
                        "research_cutoff": cutoff.isoformat(),
                        "source_capture_id": source_capture_id,
                        "price_coordinate": post_signal_coordinate,
                        "calendar_revision": calendar["calendar_revision_chain"],
                        "saved_session_range": history["saved_session_range"],
                        "database_write": False,
                        "ddl_executed": False,
                    }
                )
                self._send_json(200, result)
                return
            if (
                post_path == "/api/preview/stock-timeline"
                and isinstance(body, dict)
                and body.get("source_observation_only") is True
            ):
                required_observation_fields = {
                    "source_observation_only",
                    "symbol",
                    "target_session",
                    "source_capture_id",
                    "price_coordinate",
                    "research_cutoff",
                }
                membership_fields = {"industry_snapshot_id", "concept_snapshot_id"}
                optional_observation_fields = membership_fields | {
                    "diagnostic_scope",
                    "before_source_capture_id",
                }
                if not required_observation_fields <= set(body) <= (
                    required_observation_fields | optional_observation_fields
                ) or (
                    bool(membership_fields & set(body))
                    and not membership_fields <= set(body)
                ):
                    raise PreviewHttpError(
                        400,
                        "PREVIEW_BODY_FIELDS_INVALID",
                        "单股来源观察请求字段不完整或含未登记字段",
                    )
                symbol = _symbol(body["symbol"])
                target = _iso_date(body["target_session"])
                cutoff = _iso_date(body["research_cutoff"], "research_cutoff")
                _require_research_cutoff(target, cutoff)
                _require_research_cutoff(
                    cutoff, self.preview_server.state.research_cutoff
                )
                source_capture_id = _capture_id(body["source_capture_id"])
                price_coordinate = _price_coordinate(body["price_coordinate"])
                if price_coordinate != "FORWARD_ADJUSTED":
                    raise PreviewHttpError(
                        400,
                        "PRICE_COORDINATE_INVALID",
                        "D0跨会话观察必须绑定BaoStock FORWARD_ADJUSTED坐标",
                    )
                before_source_capture_id = (
                    _capture_id(body["before_source_capture_id"])
                    if body.get("before_source_capture_id") is not None
                    else None
                )
                diagnostic_scope = body.get("diagnostic_scope", "LAUNCH")
                if not isinstance(diagnostic_scope, str) or diagnostic_scope not in {
                    "BOTH",
                    "LAUNCH",
                    "RECOVERY",
                }:
                    raise PreviewHttpError(
                        400, "SOURCE_DIAGNOSTIC_SCOPE_INVALID", "来源诊断范围无效"
                    )
                launch_change = None
                if diagnostic_scope != "RECOVERY":
                    _, pair, _ = resolve_pg_history_window(target.isoformat(), 2)
                    try:
                        changes = query_source_d0_raw_launch(
                            self.preview_server.state.config,
                            scenario="LAUNCH_CONFIRM",
                            action="changes",
                            before_session=pair[0],
                            target_session=target,
                            research_cutoff=cutoff,
                            before_source_capture_id=before_source_capture_id,
                            source_capture_id=source_capture_id,
                            price_coordinate=price_coordinate,
                            symbol=symbol,
                            transition="ALL",
                            offset=0,
                            limit=1,
                            _single_symbol_only=True,
                        )
                        launch_change = next(iter(changes["results"]), None)
                    except (SourceD0RawError, SourceStockChangeContextError) as error:
                        mapped = _source_context_preview_error(error)
                        if mapped is None:
                            raise
                        raise mapped from error
                recovery = None
                if diagnostic_scope != "LAUNCH":
                    rps_error = None
                    try:
                        rps, _ = self.preview_server.state.source_pool_rps(
                            (target, cutoff, source_capture_id),
                            lambda: read_source_pool_rps(
                                self.preview_server.state.config,
                                target_session=target,
                                research_cutoff=cutoff,
                                source_capture_id=source_capture_id,
                            ),
                        )
                    except RawSourcePgError as error:
                        if error.code != "PG_SOURCE_NO_MATCH":
                            raise
                        rps = None
                        rps_error = error.code
                    recovery = query_source_d0_raw_launch(
                        self.preview_server.state.config,
                        scenario="RECOVERY_TURN",
                        action="current",
                        target_session=target,
                        research_cutoff=cutoff,
                        source_capture_id=source_capture_id,
                        price_coordinate=price_coordinate,
                        symbol=symbol,
                        offset=0,
                        limit=1,
                        _single_symbol_only=True,
                        _rps_result=rps,
                        _rps_result_error=rps_error,
                    )
                membership = {
                    "status": "NOT_EVALUATED",
                    "reason": "未同时选择行业与GN快照",
                }
                if membership_fields <= set(body):
                    try:
                        if (
                            body["industry_snapshot_id"] is None
                            or body["concept_snapshot_id"] is None
                        ):
                            from stock_a.application.source_d0_tdx_membership import (
                                read_tdx_sector_memberships_for_symbols,
                            )

                            membership = read_tdx_sector_memberships_for_symbols(
                                self.preview_server.state.config,
                                source_symbols=[symbol],
                                target_session=target,
                                industry_snapshot_id=body["industry_snapshot_id"],
                                concept_snapshot_id=body["concept_snapshot_id"],
                            )
                            missing = [
                                kind
                                for kind, field in (
                                    ("INDUSTRY", "industry_snapshot_id"),
                                    ("CONCEPT", "concept_snapshot_id"),
                                )
                                if body[field] is None
                            ]
                            membership["status"] = "PARTIAL_MEMBERSHIP_UNKNOWN"
                            membership["missing_membership_kinds"] = missing
                            membership["reason"] = "缺少成员快照：" + "、".join(missing)
                        else:
                            membership = read_tdx_sector_symbol_membership(
                                self.preview_server.state.config,
                                source_symbol=symbol,
                                target_session=target,
                                industry_snapshot_id=body["industry_snapshot_id"],
                                concept_snapshot_id=body["concept_snapshot_id"],
                            )
                    except Exception as error:
                        if getattr(error, "code", None) != "SECTOR_SNAPSHOT_NOT_FOUND":
                            mapped = _source_context_preview_error(error)
                            if mapped is None:
                                raise
                            raise mapped from error
                        membership = {
                            "status": "NOT_EVALUATED",
                            "reason": "所选行业或GN快照不存在",
                        }
                self._send_json(
                    200,
                    {
                        "source_symbol": symbol,
                        "target_session": target.isoformat(),
                        "source_capture_id": source_capture_id,
                        "price_coordinate": price_coordinate,
                        "time_role": "NON_PIT_RETROSPECTIVE",
                        "d0_change": launch_change,
                        "recovery": next(iter(recovery["results"]), None)
                        if recovery
                        else None,
                        "diagnostic_scope": diagnostic_scope,
                        "d1_status": "NOT_EVALUATED",
                        "membership_status": membership["status"],
                        "current_tdx_membership": membership,
                        "formal_evaluation_status": "NOT_EVALUATED",
                    },
                )
                return
            expected = {
                "symbol",
                "target_session",
                "session_count",
            }
            capture_field = {"source_capture_id"}
            coordinate_field = {"price_coordinate"}
            timeline_fields = expected | coordinate_field
            source_change_required = {
                "include_source_change_context",
                "before_session",
                "industry_snapshot_id",
                "concept_snapshot_id",
            }
            source_change_optional = {
                "research_cutoff",
                "before_source_capture_id",
                "before_profile_revision",
                "after_profile_revision",
            }
            has_before_profile_revision = (
                isinstance(body, dict) and "before_profile_revision" in body
            )
            has_after_profile_revision = (
                isinstance(body, dict) and "after_profile_revision" in body
            )
            if (
                post_path == "/api/preview/stock-timeline"
                and has_before_profile_revision != has_after_profile_revision
            ):
                raise PreviewHttpError(
                    400,
                    "PREVIEW_PROFILE_REVISIONS_MUST_BE_PAIRED",
                    "before_profile_revision与after_profile_revision必须同时提供",
                )
            source_change_body = timeline_fields | source_change_required
            source_change_allowed = (
                source_change_body | source_change_optional | capture_field
            )
            stock_seed_fields = {"profile_revision"}
            stock_evidence_fields = {"industry_snapshot_id", "concept_snapshot_id"}
            stock_d1_evidence_field = {"d1_history_session_count"}
            valid_body = isinstance(body, dict)
            if valid_body and post_path == "/api/preview/stock":
                allowed_stock_bodies = {
                    frozenset(expected),
                    frozenset(expected | capture_field),
                    frozenset(expected | stock_seed_fields),
                    frozenset(expected | stock_seed_fields | capture_field),
                    frozenset(expected | stock_evidence_fields),
                    frozenset(expected | stock_evidence_fields | capture_field),
                    frozenset(expected | stock_seed_fields | stock_evidence_fields),
                    frozenset(
                        expected
                        | stock_seed_fields
                        | stock_evidence_fields
                        | capture_field
                    ),
                    frozenset(
                        expected | stock_evidence_fields | stock_d1_evidence_field
                    ),
                    frozenset(
                        expected
                        | stock_evidence_fields
                        | stock_d1_evidence_field
                        | capture_field
                    ),
                    frozenset(
                        expected
                        | stock_seed_fields
                        | stock_evidence_fields
                        | stock_d1_evidence_field
                    ),
                    frozenset(
                        expected
                        | stock_seed_fields
                        | stock_evidence_fields
                        | stock_d1_evidence_field
                        | capture_field
                    ),
                }
                valid_body = frozenset(body) in allowed_stock_bodies
            elif valid_body:
                valid_body = set(body) in {
                    frozenset(timeline_fields),
                    frozenset(timeline_fields | capture_field),
                } or (
                    post_path == "/api/preview/stock-timeline"
                    and source_change_body <= set(body) <= source_change_allowed
                )
            if not valid_body:
                raise PreviewHttpError(
                    400,
                    "PREVIEW_BODY_FIELDS_INVALID",
                    "请求体须包含symbol、target_session、session_count；日线轨迹还须指定price_coordinate，可选提供source_capture_id",
                )
            symbol = _symbol(body["symbol"])
            target_text = body["target_session"]
            target = _iso_date(target_text)
            _require_research_cutoff(target, self.preview_server.state.research_cutoff)
            requested_source_capture_id = (
                _capture_id(body["source_capture_id"])
                if body.get("source_capture_id") is not None
                else None
            )
            price_coordinate = (
                _price_coordinate(body["price_coordinate"])
                if post_path == "/api/preview/stock-timeline"
                else "FORWARD_ADJUSTED"
            )
            session_count = _positive_int(body["session_count"], "session_count", 75)
            source_change_opt_in = source_change_required <= set(body)
            if source_change_opt_in and price_coordinate != "FORWARD_ADJUSTED":
                raise PreviewHttpError(
                    400,
                    "SOURCE_CHANGE_PRICE_COORDINATE_INVALID",
                    "板块/个股D0/D1跨会话来源上下文必须使用FORWARD_ADJUSTED坐标",
                )
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
            before_source_capture_id: int | None = None
            industry_snapshot_id: int | None = None
            concept_snapshot_id: int | None = None
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
                before_source_capture_id = (
                    _capture_id(body["before_source_capture_id"])
                    if body.get("before_source_capture_id") is not None
                    else None
                )
                industry_snapshot_id = _snapshot_id(body["industry_snapshot_id"])
                concept_snapshot_id = _snapshot_id(body["concept_snapshot_id"])
                if source_change_optional <= set(body):
                    before_profile_revision = _profile_revision(
                        body["before_profile_revision"]
                    )
                    after_profile_revision = _profile_revision(
                        body["after_profile_revision"]
                    )
            profile_revision = None
            d1_history_session_count = None
            membership_snapshot_ids = None
            if (
                post_path == "/api/preview/stock"
                and body.get("profile_revision") is not None
            ):
                profile_revision = _profile_revision(body["profile_revision"])
            if post_path == "/api/preview/stock" and stock_evidence_fields <= set(body):
                if body.get("d1_history_session_count") is not None:
                    d1_history_session_count = _positive_int(
                        body["d1_history_session_count"],
                        "d1_history_session_count",
                        75,
                    )
                membership_snapshot_ids = {
                    "INDUSTRY": (
                        _snapshot_id(body["industry_snapshot_id"])
                        if body["industry_snapshot_id"] is not None
                        else None
                    ),
                    "CONCEPT": (
                        _snapshot_id(body["concept_snapshot_id"])
                        if body["concept_snapshot_id"] is not None
                        else None
                    ),
                }
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
            history = _read_baostock_history(
                self.preview_server.state.config,
                source_symbol=symbol,
                target_session=target,
                source_capture_id=requested_source_capture_id,
                price_coordinate=price_coordinate,
                sessions=sessions,
                cutoff=self.preview_server.state.research_cutoff,
            )
            source_capture_id = _capture_id(history.get("source_capture_id"))
            if (
                requested_source_capture_id is not None
                and source_capture_id != requested_source_capture_id
            ):
                raise PreviewHttpError(
                    409,
                    "PRICE_HISTORY_CAPTURE_MISMATCH",
                    "个股价格历史捕获与请求不一致",
                )
            if (
                post_path == "/api/preview/stock-timeline"
                and history.get("price_coordinate") != price_coordinate
            ):
                raise PreviewHttpError(
                    409,
                    "PRICE_HISTORY_COORDINATE_MISMATCH",
                    "个股价格历史坐标与请求不一致",
                )
            if post_path == "/api/preview/stock-timeline":
                result = research_entry.research_stock_pg_history_timeline(
                    symbol=symbol,
                    target_session=target.isoformat(),
                    source_capture_id=source_capture_id,
                    price_coordinate=price_coordinate,
                    session_dates=sessions,
                    history_rows=history["history_rows"],
                    missing_history_sessions=tuple(
                        date.fromisoformat(item)
                        for item in history["missing_history_sessions"]
                    ),
                    profile_contract=self.preview_server.state.profile_contract,
                )
                source_context = _baostock_source_context(
                    self.preview_server.state.config,
                    target_session=target,
                    research_cutoff=self.preview_server.state.research_cutoff,
                    source_capture_id=source_capture_id,
                    target_price_rows=history["history_rows"],
                    price_coordinate=price_coordinate,
                )
                if source_context["status"] != "AVAILABLE":
                    raise PreviewHttpError(
                        409,
                        "PREVIEW_SOURCE_IDENTITY_MISMATCH",
                        "轨迹返回的价格捕获ID或捕获记录与变化快照不一致",
                    )
                if (
                    result.get("price_coordinate") != price_coordinate
                    or result.get("source_capture_id") != source_capture_id
                    or result.get("time_role") != "NON_PIT_RETROSPECTIVE"
                ):
                    raise PreviewHttpError(
                        409,
                        "PRICE_HISTORY_COORDINATE_MISMATCH",
                        "日线Reader返回坐标与请求不一致",
                    )
                points = result.get("points")
                if not isinstance(points, list) or any(
                    not isinstance(point, Mapping) for point in points
                ):
                    raise PreviewHttpError(
                        500,
                        "PRICE_TIMELINE_POINTS_INVALID",
                        "日线Reader未返回坐标明确的价格点列表",
                    )
                normalized_points = []
                for point in points:
                    point = dict(point)
                    if (
                        point.get("price_coordinate") != price_coordinate
                        or _capture_id(point.get("source_capture_id"))
                        != source_capture_id
                        or point.get("time_role") != "NON_PIT_RETROSPECTIVE"
                    ):
                        raise PreviewHttpError(
                            409,
                            "PRICE_HISTORY_IDENTITY_MISMATCH",
                            "日线价格点捕获或坐标与请求不一致",
                        )
                    normalized_points.append(point)
                result["points"] = normalized_points
                result["price_coordinate"] = price_coordinate
                result["source_capture_id"] = source_capture_id
                if source_change_opt_in:
                    try:
                        membership = read_tdx_sector_symbol_membership(
                            self.preview_server.state.config,
                            source_symbol=symbol,
                            target_session=target,
                            industry_snapshot_id=industry_snapshot_id,
                            concept_snapshot_id=concept_snapshot_id,
                        )
                        source_change = query_source_stock_change_context(
                            self.preview_server.state.config,
                            before_session=before_target,
                            target_session=target,
                            research_cutoff=research_cutoff,
                            before_source_capture_id=before_source_capture_id,
                            source_capture_id=source_capture_id,
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
                    target_session=target.isoformat(),
                    source_capture_id=source_capture_id,
                    price_coordinate=price_coordinate,
                    session_count=session_count,
                    session_start=sessions[0].isoformat(),
                    calendar_revision=calendar_revision,
                )
                result["snapshot_identity"] = identity
                result["source_version"] = source_context
                result["selection"] = {
                    "source_symbol": symbol,
                    "target_session": target.isoformat(),
                    "source_capture_id": source_capture_id,
                    "session_count": session_count,
                    "calendar_revision": calendar_revision,
                    "price_coordinate": price_coordinate,
                }
                relation_history = result.get("price_relation_history")
                if isinstance(relation_history, dict):
                    relation_history["source_capture_id"] = source_capture_id
                direction_history = result.get("ma20_direction_history")
                if isinstance(direction_history, dict):
                    binding = direction_history.get("binding")
                    if isinstance(binding, dict):
                        binding.update(
                            {
                                "source_capture_id": source_context[
                                    "source_capture_id"
                                ],
                            }
                        )
                self._send_json(200, result)
                return
            source_context = _baostock_source_context(
                self.preview_server.state.config,
                target_session=target,
                research_cutoff=self.preview_server.state.research_cutoff,
                source_capture_id=source_capture_id,
                target_price_rows=history["history_rows"],
                price_coordinate=price_coordinate,
            )
            result = research_entry.research_stock_pg_history_preview(
                symbol=symbol,
                target_session=target.isoformat(),
                source_capture_id=source_capture_id,
                price_coordinate=price_coordinate,
                session_dates=sessions,
                history_rows=history["history_rows"],
                missing_history_sessions=tuple(
                    date.fromisoformat(item)
                    for item in history["missing_history_sessions"]
                ),
                profile_contract=self.preview_server.state.profile_contract,
            )
            identity = self.preview_server.state.snapshot_identity(
                target_session=target.isoformat(),
                source_capture_id=source_capture_id,
                price_coordinate="FORWARD_ADJUSTED",
                session_count=session_count,
                session_start=sessions[0].isoformat(),
                calendar_revision=calendar_revision,
            )
            result["snapshot_identity"] = identity
            result["source_version"] = source_context
            result["source_capture_id"] = source_capture_id
            result["price_coordinate"] = "FORWARD_ADJUSTED"
            result["time_role"] = "NON_PIT_RETROSPECTIVE"
            result["price_coordinates"] = {
                "absolute_spot": "RAW_UNADJUSTED",
                "cross_session_research": "FORWARD_ADJUSTED",
            }
            result["selection"] = {
                "source_symbol": symbol,
                "target_session": target.isoformat(),
                "source_capture_id": source_capture_id,
                "session_count": session_count,
                "calendar_revision": calendar_revision,
                "price_coordinate": "FORWARD_ADJUSTED",
            }
            result["population_status"] = {
                "source_candidates": "PG_SOURCE_CANDIDATES_NON_PIT",
                "official_listing_population": "UNKNOWN_NOT_CONNECTED",
                "selected_or_excluded": "NOT_EVALUATED",
            }
            from stock_a.storage.postgres.price_limits import (
                read_stock_price_limit_fact,
            )

            result["price_limit"] = read_stock_price_limit_fact(
                self.preview_server.state.config,
                target_session=target,
                source_symbol=symbol,
                source_capture_id=source_context["source_capture_id"],
                calendar_revision=calendar_revision,
            )
            from stock_a.sources.baostock.identity import eligible_research_board

            if eligible_research_board(symbol) is None:
                result["source_seed"] = {
                    "status": "OUT_OF_FORMAL_RESEARCH_SCOPE",
                    "source_symbol": symbol,
                    "reason": "OUT_OF_FORMAL_RESEARCH_MARKET_OR_BOARD",
                    "message": "该证券属于科创板或非正式研究板别，仅保留底层来源检索与基础价格画像。",
                }
                if post_path == "/api/preview/stock":
                    result["seed_t1_outcome"] = {
                        "status": "NOT_IN_FORMAL_RESEARCH_UNIVERSE",
                        "reason": "OUT_OF_FORMAL_RESEARCH_MARKET_OR_BOARD",
                    }
                    result["formal_d0_admission"] = {
                        "status": "NOT_IN_FORMAL_RESEARCH_UNIVERSE",
                        "reason": "OUT_OF_FORMAL_RESEARCH_MARKET_OR_BOARD",
                    }
                    result["t0_observation"] = {
                        "status": "NOT_IN_FORMAL_RESEARCH_UNIVERSE",
                        "reason": "OUT_OF_FORMAL_RESEARCH_MARKET_OR_BOARD",
                        "formal_d0_admission": result["formal_d0_admission"],
                    }
            else:
                result["source_seed"] = query_source_stock_seed_context(
                    self.preview_server.state.config,
                    target_session=target,
                    source_capture_id=source_context["source_capture_id"],
                    source_symbol=symbol,
                    profile_revision=profile_revision,
                    expected_source_count=source_context["research_expected_row_count"],
                    source_time_quality=source_context["source_time_quality"],
                    source_clock_role=source_context["source_clock_role"],
                    source_available_at=source_context["source_available_at"],
                )
                if post_path == "/api/preview/stock":
                    from stock_a.storage.postgres.source_launch_watch_evidence import (
                        read_source_launch_watch_evidence,
                    )

                    saved_evidence = read_source_launch_watch_evidence(
                        self.preview_server.state.config,
                        target_session=target,
                        source_capture_id=source_context["source_capture_id"],
                        expected_source_count=source_context[
                            "research_expected_row_count"
                        ],
                        source_symbols=(symbol,),
                        d1_history_session_count=d1_history_session_count,
                        single_symbol_only=True,
                        expected_before_session=sessions[-2]
                        if len(sessions) > 1
                        else None,
                        selected_profile_revision=profile_revision,
                        selected_history_session_count=session_count,
                        membership_snapshot_ids=membership_snapshot_ids,
                    )
                    single_evidence = saved_evidence["by_symbol"][symbol]
                    d0_transition_status = single_evidence.get("d0", {}).get(
                        "status", "UNKNOWN"
                    )
                    calendar_sessions = _official_calendar(research_cutoff)["sessions"]
                    if target not in calendar_sessions:
                        raise PreviewHttpError(
                            409,
                            "STOCK_SEED_T1_TARGET_CALENDAR_UNAVAILABLE",
                            "目标会话不在当前官方日历范围内，不能读取个股后继结果",
                        )
                    successor_sessions = tuple(
                        day
                        for day in calendar_sessions
                        if target < day <= research_cutoff
                    )
                    horizon_due_sessions = tuple(
                        day for day in calendar_sessions if day > target
                    )[:20]
                    from stock_a.storage.postgres.source_launch_watch_evidence import (
                        read_source_seed_t1_outcomes,
                    )

                    seed_t1_outcome = read_source_seed_t1_outcomes(
                        self.preview_server.state.config,
                        target_session=target,
                        research_cutoff=research_cutoff,
                        source_capture_id=source_context["source_capture_id"],
                        expected_source_count=source_context[
                            "research_expected_row_count"
                        ],
                        source_symbols=(symbol,),
                        successor_sessions=successor_sessions,
                    )[symbol]
                    seed_t1_outcome["d0_transition_status"] = d0_transition_status
                    seed_t1_outcome["rule_status"] = single_evidence["rules"]["status"]
                    seed_t1_outcome["d1_status"] = single_evidence["d1"]["status"]
                    result["seed_t1_outcome"] = seed_t1_outcome
                result["stored_research_evidence"] = single_evidence
                result["stored_research_evidence_execution"] = saved_evidence[
                    "execution"
                ]
                result["stored_research_evidence_status"] = saved_evidence["status"]
                from stock_a.storage.postgres.research_enrollment import (
                    read_enrollment_followups,
                    read_enrollments_for_symbols,
                )

                config = self.preview_server.state.config
                _, pair, _ = resolve_pg_history_window(target.isoformat(), 2)
                before = pair[-2] if len(pair) == 2 else None
                before_source = (
                    read_source_version_context(
                        config,
                        target_session=before,
                        source_capture_id=None,
                        research_cutoff=self.preview_server.state.research_cutoff,
                    )
                    if before is not None
                    else {}
                )
                entries = []
                if before_source.get("status") == "AVAILABLE":
                    entries = read_enrollments_for_symbols(
                        config,
                        target=target,
                        source_capture_id=source_context["source_capture_id"],
                        target_capture_id=source_context["source_capture_id"],
                        before=before,
                        before_capture=before_source["source_capture_id"],
                        include_d0=True,
                        symbols=[symbol],
                        rule_scope=(
                            saved_evidence.get("rule_scope")
                            if isinstance(saved_evidence.get("rule_scope"), Mapping)
                            else None
                        ),
                    ).get(symbol, [])
                calendar = calendar_sessions
                rule_ids = [
                    entry["id"] for entry in entries if entry.get("fact_type") == "RULE"
                ]
                d0_ids = [
                    entry["id"] for entry in entries if entry.get("fact_type") == "D0"
                ]
                followups = {}
                if rule_ids:
                    from stock_a.storage.postgres.source_observation_segments import (
                        read_source_observation_segment_followups,
                    )

                    followups.update(
                        read_source_observation_segment_followups(
                            config,
                            target=target,
                            cutoff=research_cutoff,
                            enrollment_ids=rule_ids,
                            horizon_due_sessions=tuple(
                                day for day in calendar if day > target
                            )[:20],
                        )
                    )
                if d0_ids:
                    followups.update(
                        read_enrollment_followups(
                            config,
                            target=target,
                            cutoff=research_cutoff,
                            enrollment_ids=d0_ids,
                            successor_sessions=tuple(
                                day
                                for day in calendar
                                if target < day <= research_cutoff
                            ),
                            horizon_due_sessions=tuple(
                                day for day in calendar if day > target
                            )[:20],
                        )
                        if target in calendar
                        else {
                            item: {
                                "status": "UNKNOWN",
                                "reason": "CALENDAR_WINDOW_UNAVAILABLE",
                            }
                            for item in d0_ids
                        }
                    )
                for entry in entries:
                    entry["followup"] = followups[entry["id"]]
                _attach_t0_early_contexts(config, entries)
                from stock_a.storage.postgres.t0_early_observations import (
                    read_source_t0_early_triage_summaries_for_symbols,
                )

                triage_summary = read_source_t0_early_triage_summaries_for_symbols(
                    config,
                    target_session=target,
                    source_capture_id=source_context["source_capture_id"],
                    source_symbols=[symbol],
                    selected_snapshot_ids={
                        kind: snapshot_id
                        for kind, snapshot_id in (membership_snapshot_ids or {}).items()
                        if type(snapshot_id) is int and snapshot_id > 0
                    },
                )[symbol]
                followup_states = {entry["followup"]["status"] for entry in entries}
                followup_reasons = {entry["followup"]["reason"] for entry in entries}
                followup_status = (
                    next(iter(followup_states))
                    if len(followup_states) == 1
                    else "MULTIPLE"
                    if followup_states
                    else "NOT_COMPUTED"
                )
                followup_reason = (
                    next(iter(followup_reasons))
                    if len(followup_reasons) == 1
                    else "SEE_RULE_DETAILS"
                    if followup_reasons
                    else "NO_STORED_LANDMARK"
                )
                triage_summary = _bind_t0_followup_status(
                    triage_summary,
                    target_session=target,
                    research_cutoff=research_cutoff,
                    stored_status=followup_status,
                    stored_reason=followup_reason,
                )
                registration_status = (
                    "AVAILABLE"
                    if entries
                    else "NOT_COMPUTED"
                    if d0_transition_status == "NOT_COMPUTED"
                    or single_evidence["rules"]["status"] == "NOT_COMPUTED"
                    else "NO_STORED_FACT"
                    if before_source.get("status") == "AVAILABLE"
                    and single_evidence["rules"]["status"] == "AVAILABLE"
                    else "UNKNOWN"
                )
                registration_reason = (
                    None
                    if entries
                    else "D0_ADJACENT_LAUNCH_FACT_NOT_SAVED_FOR_EXACT_INPUT"
                    if d0_transition_status == "NOT_COMPUTED"
                    else "RULE_FACTS_NOT_COMPUTED_FOR_EXACT_INPUT"
                    if single_evidence["rules"]["status"] == "NOT_COMPUTED"
                    else "NO_STORED_FACT"
                    if registration_status == "NO_STORED_FACT"
                    else "EXACT_SCOPE_UNAVAILABLE"
                )
                result["t0_observation"] = {
                    "status": registration_status,
                    "reason": registration_reason,
                    "d0_transition_status": d0_transition_status,
                    "rule_status": single_evidence["rules"]["status"],
                    "d1_status": single_evidence["d1"]["status"],
                    "d0_context": single_evidence.get("d0"),
                    "enrollments": entries,
                    "triage_summary": triage_summary,
                    "followup_status": followup_status,
                    "followup_reason": followup_reason,
                    "target_session": target.isoformat(),
                    "source_capture_id": source_context["source_capture_id"],
                    "profile_revision": (saved_evidence.get("rule_scope") or {}).get(
                        "profile_revision"
                    ),
                    "price_coordinate": "FORWARD_ADJUSTED",
                    "scope": "NON_PIT_EXPLORATORY",
                }
                from stock_a.storage.postgres.formal_admission import (
                    read_source_formal_admission_day,
                )

                d0_contract = single_evidence.get("d0") or {}
                if not all(
                    isinstance(d0_contract.get(key), str) and d0_contract[key]
                    for key in (
                        "algorithm_contract",
                        "session_contract",
                        "calendar_revision",
                    )
                ):
                    formal_admission_day = {
                        "status": "NOT_COMPUTED",
                        "reason_code": "FORMAL_ADMISSION_D0_VERSION_NOT_AVAILABLE",
                        "by_symbol": {},
                        "counts": None,
                    }
                else:
                    saved_rule_scope = saved_evidence.get("rule_scope")
                    admission_profile_revision = (
                        profile_revision
                        if profile_revision is not None
                        else saved_rule_scope.get("profile_revision")
                        if isinstance(saved_rule_scope, Mapping)
                        else None
                    )
                    admission_history_session_count = None
                    if admission_profile_revision is not None:
                        from stock_a.contracts.profile_revision import (
                            profile_revision_history_session_count,
                        )

                        admission_history_session_count = (
                            profile_revision_history_session_count(
                                admission_profile_revision
                            )
                        )
                    formal_admission_day = read_source_formal_admission_day(
                        config,
                        target_session=target,
                        source_capture_id=source_context["source_capture_id"],
                        expected_source_count=source_context[
                            "research_expected_row_count"
                        ],
                        algorithm_contract=d0_contract["algorithm_contract"],
                        session_contract=d0_contract["session_contract"],
                        calendar_revision=d0_contract["calendar_revision"],
                        seed_producer_version=(
                            single_evidence.get("seed_scope") or {}
                        ).get("producer_version"),
                        source_symbols=[symbol],
                        profile_history_session_count=admission_history_session_count,
                        profile_revision=admission_profile_revision,
                    )
                formal_d0_admission = present_source_formal_admission(
                    stored_day=formal_admission_day,
                    source_symbol=symbol,
                    target_session=target,
                    source_capture_id=source_context["source_capture_id"],
                    d0_evidence=single_evidence.get("d0"),
                )
                result["formal_d0_admission"] = formal_d0_admission
                result["t0_observation"]["formal_d0_admission"] = formal_d0_admission
                result["stored_research_evidence"]["formal_d0_admission"] = (
                    formal_d0_admission
                )
                try:
                    from stock_a.storage.postgres.baostock_price_facts import (
                        read_baostock_price_detail,
                    )

                    result["baostock_price_source"] = read_baostock_price_detail(
                        config,
                        source_symbol=symbol,
                        target_session=target,
                        research_cutoff=research_cutoff,
                        source_capture_id=source_context["source_capture_id"],
                    )
                except Exception as error:
                    result["baostock_price_source"] = {
                        "status": "UNKNOWN",
                        "reason_code": getattr(error, "code", type(error).__name__),
                        "message": "BaoStock价格来源读回失败；没有替换为其它价格来源。",
                        "source": "BAOSTOCK",
                        "source_symbol": symbol,
                        "target_session": target.isoformat(),
                        "research_cutoff": research_cutoff.isoformat(),
                        "price_coordinate": "RAW_UNADJUSTED",
                        "source_capture_id": source_capture_id,
                        "pit_status": "NON_PIT",
                        "source_network_called": False,
                        "database_write_attempted": False,
                    }
            self._send_json(200, result)
        except Exception as error:
            self._handle_error(error)


def run_preview_server(
    *,
    host: str = _HOST,
    port: int = _PORT,
    research_cutoff: date,
    data_jobs_authorized: bool = False,
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
        state = _PreviewState(
            research_cutoff=research_cutoff,
            data_jobs_authorized=data_jobs_authorized,
        )
        server = _BoundedHTTPServer((_HOST, _PORT), state)
    except (OSError, DiagnosticError, PriceProfileContractError) as error:
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
