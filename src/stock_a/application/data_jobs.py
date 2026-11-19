"""数据初始化与手动日更的共享计划器和单后台执行协调器。"""

from __future__ import annotations

import threading
import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any
from uuid import UUID, uuid4, uuid5

from stock_a.research.entry import _load_calendar
from stock_a.research.entry_context.sessions import CORE_MAX_ACTUAL_LOOKBACK
from stock_a.storage.postgres import data_jobs as data_job_pg
from stock_a.storage.postgres import business_reset as business_reset_pg


class DataJobError(Exception):
    """数据任务计划或后台执行错误。"""

    def __init__(self, code: str, message: str, *, status: int = 400):
        self.code = code
        self.status = status
        super().__init__(message)


_MAX_SESSION_COUNT = 32767
_STAGE_TABLES = {
    "SOURCE_RAW": (
        "stock.source_packages",
        "stock.source_captures",
        "stock.source_raw_daily",
    ),
    "SOURCE_BAOSTOCK": ("stock.source_baostock_daily",),
    "SOURCE_SECTOR_MEMBERS": (
        "stock.tdx_sector_source_snapshots",
        "stock.tdx_sector_catalog",
        "stock.tdx_sector_members",
    ),
    "CORE_PROFILE": ("stock.source_symbol_price_profiles",),
    "MARKET_BREADTH": (
        "stock.source_market_breadth_days",
        "stock.source_market_breadth_symbols",
    ),
    "MARKET_PARTICIPATION": (
        "stock.source_market_participation_days",
        "stock.source_market_participation_unknown_reasons",
    ),
    "STATIC_RESEARCH": (
        "stock.source_d0_scenario_day_facts",
        "stock.source_pool_seed_day_facts",
    ),
    "D0_D1": (
        "stock.source_d0_launch_day_facts",
        "stock.source_d1_structure_day_facts",
        "stock.source_d1_material_anchor_day_facts",
        "stock.source_research_enrollments",
    ),
    "FOCUS_FORMAL": ("stock.source_d0_formal_admission_day_facts",),
    "SECTOR_PULSE": ("stock.source_sector_cross_period_pulse_facts",),
    "SECTOR_TRANSITION": ("stock.source_sector_pulse_adjacent_transitions",),
    "SECTOR_FOLLOWUP": (
        "stock.source_sector_pulse_followup_facts",
        "stock.source_sector_pulse_cohort_group_facts",
        "stock.source_sector_pulse_cohort_reason_facts",
    ),
    "ADJUSTED_PRICE": ("stock.source_adjusted_daily_prices",),
}
_STAGE_ORDER = {
    "BUSINESS_RESET": 1,
    "SOURCE_RAW": 1,
    "SOURCE_SECTOR_MEMBERS": 2,
    "SOURCE_BAOSTOCK": 3,
    "MARKET_BREADTH": 3,
    "CORE_PROFILE": 4,
    "MARKET_PARTICIPATION": 5,
    "D0_D1": 6,
    "STATIC_RESEARCH": 7,
    "FOCUS_FORMAL": 8,
    "SECTOR_PULSE": 9,
    "SECTOR_TRANSITION": 10,
    "SECTOR_FOLLOWUP": 11,
    "ADJUSTED_PRICE": 12,
}
_RAW_JOB_SCOPE = "STOCK_A_R08_BIZ08_DATA_JOB_RAW_IMPORT_R1"

_MODULES = {
    "TDX_RAW": ("SOURCE", "TDX官方Raw来源", True, True),
    "TDX_SECTOR_MEMBERS": ("SOURCE", "行业与GN概念成员来源", True, True),
    "BAOSTOCK": ("SOURCE", "BaoStock补充来源", True, True),
    "CORE_PROFILE": ("BASE", "Core / Profile", True, True),
    "MARKET_BREADTH": ("BASE", "市场宽度", True, True),
    "MARKET_PARTICIPATION": ("BASE", "成交参与", True, True),
    "D0": ("RESEARCH", "D0 / Seed研究", True, True),
    "D1": ("RESEARCH", "D1结构与材料变化", True, True),
    "FOCUS_FORMAL": ("RESEARCH", "Focus正式准入", True, True),
    "SECTOR_PULSE": ("RESEARCH", "行业 / GN概念PULSE", True, True),
    "SECTOR_TRANSITION": ("RESEARCH", "行业 / GN概念相邻变化", True, True),
    "SECTOR_FOLLOWUP": ("RESEARCH", "板块跨日跟踪", True, True),
    "ADJUSTED_PRICE": ("RESEARCH", "仿射复权价格", True, True),
}
_UNCOORDINATED_RESEARCH_CONSUMERS = (
    (
        "source_rule_evaluations",
        "规则评估",
        "当前菜单协调器没有该现役生产者，清理后依赖它的规则与观察读取会保留未计算状态。",
    ),
    (
        "source_cohort_group_stats",
        "Cohort分组统计",
        "当前菜单协调器没有该现役生产者，清理后Cohort统计读取会保留未计算状态。",
    ),
    (
        "source_fep_baseline_facts",
        "FEP条件基线",
        "当前菜单协调器没有该现役生产者，清理后FEP基线读取会保留未计算状态。",
    ),
    (
        "source_forward_path_facts",
        "Forward路径",
        "当前菜单协调器没有该现役生产者，清理后Forward路径读取会保留未计算状态。",
    ),
    (
        "source_research_observation_segment_days",
        "观察持续分段",
        "当前菜单协调器没有该现役生产者，清理后观察分段读取会保留未计算状态。",
    ),
    (
        "source_t0_early_observation_facts",
        "T0早期观察",
        "当前菜单协调器没有该现役生产者，清理后T0早期观察读取会保留未计算状态。",
    ),
)
_CLEANUP_OPERATIONS = {"CLEAR_DERIVED", "CLEAR_AND_REGENERATE"}
_OPERATIONS = {
    "FILL_MISSING",
    "RECOMPUTE",
    "CLEAR_DERIVED",
    "CLEAR_AND_REGENERATE",
}


def _positive_integer(value: Any, name: str, *, allow_zero: bool = False) -> int:
    minimum = 0 if allow_zero else 1
    if type(value) is not int or not minimum <= value <= _MAX_SESSION_COUNT:
        raise DataJobError(
            "DATA_JOB_INPUT_INVALID",
            f"{name}必须是{minimum}至{_MAX_SESSION_COUNT}之间的整数。",
        )
    return value


def _request_fields(body: Any) -> dict[str, Any]:
    if not isinstance(body, dict):
        raise DataJobError("DATA_JOB_INPUT_INVALID", "任务参数必须是JSON对象。")
    allowed = {
        "mode",
        "target_session",
        "history_session_count",
        "profile_history_session_count",
        "d1_history_session_count",
        "d0_output_session_count",
        "d1_output_session_count",
        "source_modules",
        "base_modules",
        "research_modules",
        "module_output_session_counts",
        "output_session_count",
        "operation",
        "initialization_mode",
        "daily_mode",
        "reset_confirmed",
        "reset_preview_fingerprint",
    }
    if set(body) - allowed:
        raise DataJobError("DATA_JOB_INPUT_INVALID", "任务参数包含未登记字段。")
    mode = body.get("mode")
    if mode not in {"INITIALIZE", "MANUAL_DAILY"}:
        raise DataJobError(
            "DATA_JOB_MODE_INVALID", "任务模式须为INITIALIZE或MANUAL_DAILY。"
        )
    initialization_mode = body.get("initialization_mode", "REUSE_RAW")
    daily_mode = body.get("daily_mode", "CUSTOM")
    if mode == "INITIALIZE":
        if initialization_mode not in {"REUSE_RAW", "RESET_ALL_BUSINESS_FACTS"}:
            raise DataJobError(
                "DATA_JOB_INITIALIZATION_MODE_INVALID", "初始化方式无效。"
            )
        if daily_mode != "CUSTOM":
            raise DataJobError(
                "DATA_JOB_DAILY_MODE_INVALID", "初始化任务不能使用日更预设。"
            )
    elif initialization_mode != "REUSE_RAW" or daily_mode not in {
        "FULL_THREE_LAYERS",
        "SOURCE_ONLY",
        "CUSTOM",
    }:
        raise DataJobError("DATA_JOB_DAILY_MODE_INVALID", "手动日更方式无效。")
    source_modules = body.get("source_modules")
    base_modules = body.get("base_modules")
    research_modules = body.get("research_modules")
    has_module_selection = any(
        key in body for key in ("source_modules", "base_modules", "research_modules")
    )
    selections = {
        "source": source_modules if source_modules is not None else [],
        "base": base_modules if base_modules is not None else [],
        "research": research_modules if research_modules is not None else [],
    }
    if any(
        not isinstance(values, list)
        or any(not isinstance(value, str) or value not in _MODULES for value in values)
        or len(values) != len(set(values))
        for values in selections.values()
    ):
        raise DataJobError("DATA_JOB_MODULE_INVALID", "任务模块选择无效。")
    modules = {value for values in selections.values() for value in values}
    for layer, values in selections.items():
        expected_layer = {"source": "SOURCE", "base": "BASE", "research": "RESEARCH"}[
            layer
        ]
        if any(_MODULES[value][0] != expected_layer for value in values):
            raise DataJobError(
                "DATA_JOB_MODULE_LAYER_INVALID", "模块被放入错误的数据层。"
            )
    counts = body.get("module_output_session_counts", {})
    if not isinstance(counts, dict) or any(
        not isinstance(key, str)
        or key not in modules
        or type(value) is not int
        or not 1 <= value <= _MAX_SESSION_COUNT
        for key, value in counts.items()
    ):
        raise DataJobError("DATA_JOB_OUTPUT_COUNT_INVALID", "模块输出会话配置无效。")
    operation = body.get("operation", "FILL_MISSING")
    if operation not in _OPERATIONS:
        raise DataJobError("DATA_JOB_OPERATION_INVALID", "任务执行方式无效。")
    if operation in _CLEANUP_OPERATIONS and (
        selections["source"]
        or len(selections["base"]) + len(selections["research"]) != 1
    ):
        raise DataJobError(
            "DATA_JOB_CLEANUP_SCOPE_INVALID",
            "派生清理只允许单个基础或研究模块，不能包含基础来源。",
        )
    legacy = not has_module_selection
    if legacy:
        # BIZ08请求保持兼容；新界面始终显式发送模块列表。
        selections = {
            "source": ["TDX_RAW"],
            "base": ["CORE_PROFILE", "MARKET_BREADTH", "MARKET_PARTICIPATION"],
            "research": [],
        }
        if body.get("d0_output_session_count", 0):
            selections["research"].append("D0")
        if body.get("d1_output_session_count", 0):
            selections["research"].append("D1")
        modules = {value for values in selections.values() for value in values}
        counts = {}
    if not modules:
        raise DataJobError("DATA_JOB_MODULE_SELECTION_EMPTY", "至少选择一个实际模块。")
    profile_history_count = body.get("profile_history_session_count")
    if profile_history_count is not None:
        profile_history_count = _positive_integer(
            profile_history_count, "profile_history_session_count"
        )
    elif legacy and mode == "INITIALIZE":
        # BIZ08/BIZ09请求曾把Raw历史数同时作为Profile窗口；旧请求仍按原语义解析。
        profile_history_count = body.get("history_session_count")
    d1_history_count = body.get("d1_history_session_count")
    if d1_history_count is not None:
        d1_history_count = _positive_integer(
            d1_history_count, "d1_history_session_count"
        )
        if d1_history_count < 2:
            raise DataJobError(
                "DATA_JOB_D1_WINDOW_INVALID", "D1历史窗口至少需要两个官方会话。"
            )
    if mode == "INITIALIZE" and initialization_mode == "RESET_ALL_BUSINESS_FACTS":
        if not {"TDX_RAW", "TDX_SECTOR_MEMBERS"}.issubset(set(selections["source"])):
            raise DataJobError(
                "DATA_JOB_RESET_SOURCE_SELECTION_INVALID",
                "清空全部业务事实后重建必须包含TDX Raw与行业/GN成员来源。",
            )
    if mode == "MANUAL_DAILY" and daily_mode == "SOURCE_ONLY":
        if selections["base"] or selections["research"] or not selections["source"]:
            raise DataJobError(
                "DATA_JOB_SOURCE_ONLY_SCOPE_INVALID",
                "仅第一层日更必须选择来源模块，且不得选择任何基础画像或研究模块。",
            )
    if mode == "MANUAL_DAILY" and daily_mode == "FULL_THREE_LAYERS":
        required_sources = {"TDX_RAW", "TDX_SECTOR_MEMBERS"}
        required_base = {
            code
            for code, (layer, _label, implemented, available) in _MODULES.items()
            if layer == "BASE" and implemented and available
        }
        required_research = {
            code
            for code, (layer, _label, implemented, available) in _MODULES.items()
            if layer == "RESEARCH" and implemented and available
        }
        if (
            not required_sources.issubset(set(selections["source"]))
            or set(selections["base"]) != required_base
            or set(selections["research"]) != required_research
        ):
            raise DataJobError(
                "DATA_JOB_FULL_THREE_LAYERS_SCOPE_INVALID",
                "完整三层日更必须选择全部现役基础画像与研究模块，以及TDX Raw和行业/GN来源。",
            )
    return {
        "mode": mode,
        "target_session": body.get("target_session"),
        "history_session_count": body.get("history_session_count"),
        "profile_history_session_count": profile_history_count,
        "d1_history_session_count": d1_history_count,
        "d0_output_session_count": body.get("d0_output_session_count", 0),
        "d1_output_session_count": body.get("d1_output_session_count", 0),
        "source_modules": selections["source"],
        "base_modules": selections["base"],
        "research_modules": selections["research"],
        "selected_modules": sorted(modules),
        "module_output_session_counts": counts,
        "output_session_count": body.get("output_session_count"),
        "operation": operation,
        "initialization_mode": initialization_mode,
        "daily_mode": daily_mode,
        "reset_confirmed": body.get("reset_confirmed") is True,
        "reset_preview_fingerprint": body.get("reset_preview_fingerprint"),
        "has_module_selection": has_module_selection,
        "legacy_request": legacy,
    }


def _parse_session(value: Any) -> date:
    if not isinstance(value, str):
        raise DataJobError("DATA_JOB_TARGET_INVALID", "目标会话必须是YYYY-MM-DD日期。")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise DataJobError(
            "DATA_JOB_TARGET_INVALID", "目标会话必须是YYYY-MM-DD有效日期。"
        ) from error
    if parsed.isoformat() != value:
        raise DataJobError("DATA_JOB_TARGET_INVALID", "目标会话日期格式无效。")
    return parsed


def _load_official_sessions(target: date) -> tuple[str, str, tuple[date, ...]]:
    try:
        payload, _metadata, _revision_chain, sessions = _load_calendar(target)
    except Exception as error:
        raise DataJobError(
            "OFFICIAL_CALENDAR_UNAVAILABLE",
            "没有已核验官方日历覆盖所选目标会话。",
            status=503,
        ) from error
    calendar_id = payload.get("calendar_id")
    calendar_revision = payload.get("revision")
    if not isinstance(calendar_id, str) or not isinstance(calendar_revision, str):
        raise DataJobError(
            "OFFICIAL_CALENDAR_INVALID", "官方日历身份或修订缺失。", status=503
        )
    return calendar_id, calendar_revision, tuple(sessions)


def recent_official_sessions(
    official_sessions: Sequence[date], *, through: date, count: int
) -> tuple[date, ...]:
    """按正式日历选择最近N个会话，不用自然日偏移推算交易日。"""
    if (
        type(through) is not date
        or type(count) is not int
        or count < 0
        or any(type(session) is not date for session in official_sessions)
        or tuple(sorted(set(official_sessions))) != tuple(official_sessions)
    ):
        raise DataJobError("OFFICIAL_SESSION_WINDOW_INVALID", "官方会话窗口参数无效。")
    if count == 0:
        return ()
    eligible = tuple(session for session in official_sessions if session <= through)
    if len(eligible) < count:
        raise DataJobError(
            "OFFICIAL_SESSION_WINDOW_INSUFFICIENT",
            "已核验官方日历会话不足以形成请求窗口。",
        )
    return eligible[-count:]


def select_session_windows(
    official_sessions: Sequence[date],
    *,
    through: date,
    raw_history_count: int,
    d0_output_count: int,
    d1_output_count: int,
) -> dict[str, tuple[date, ...]]:
    """分别解析Raw、D0、D1最近官方会话窗口，所有窗口使用同一官方日历。"""
    raw_count = _positive_integer(raw_history_count, "raw_history_count")
    d0_count = _positive_integer(d0_output_count, "d0_output_count", allow_zero=True)
    d1_count = _positive_integer(d1_output_count, "d1_output_count", allow_zero=True)
    return {
        "raw": recent_official_sessions(
            official_sessions, through=through, count=raw_count
        ),
        "d0": recent_official_sessions(
            official_sessions, through=through, count=d0_count
        ),
        "d1": recent_official_sessions(
            official_sessions, through=through, count=d1_count
        ),
    }


def _module_output_sessions(
    *,
    module: str,
    requested_count: int,
    sessions: Sequence[date],
    target: date,
    operation: str,
    raw_coverage: Mapping[str, Any],
) -> tuple[date, ...]:
    requested = _positive_integer(requested_count, f"{module}输出会话数")
    chosen = recent_official_sessions(sessions, through=target, count=requested)
    # FILL_MISSING候选不能按日期水位先行剔除。是否可跳过由任务完整请求键、
    # 来源包/捕获、算法/窗口身份以及实际PG表读回共同决定。
    return chosen


def _unit(
    units: list[dict[str, Any]],
    *,
    stage: str,
    target_session: date,
    business_table: str,
    status: str = "NOT_STARTED",
    error_code: str | None = None,
    error_message: str | None = None,
) -> None:
    units.append(
        {
            "stage_order": _STAGE_ORDER[stage],
            "stage_code": stage,
            "target_session": target_session,
            "business_table": business_table,
            "planned_units": 1,
            "status": status,
            "error_code": error_code,
            "error_message": error_message,
        }
    )


def build_data_job_plan(
    body: Any,
    *,
    cutoff: date,
    raw_coverage: Mapping[str, Any],
    execution_authorized: bool,
    adjusted_action_sessions: Sequence[date] = (),
    adjusted_action_symbols_by_session: Mapping[str, Sequence[str]] | None = None,
) -> dict[str, Any]:
    """按官方日历与截止日内闭合PG Raw目录生成不写数据的任务计划。"""
    request = _request_fields(body)
    if type(cutoff) is not date:
        raise DataJobError("DATA_JOB_CUTOFF_REQUIRED", "服务必须绑定明确研究截止日。")
    mode = request["mode"]
    latest_raw = raw_coverage.get("last_stored_session")
    raw_history_count = request["history_session_count"]
    profile_history = request["profile_history_session_count"]
    retrying_daily = mode == "MANUAL_DAILY" and request["target_session"] is not None
    if mode == "INITIALIZE":
        raw_history_count = _positive_integer(
            raw_history_count, "history_session_count"
        )
        if profile_history is None:
            profile_history = raw_coverage.get("current_profile_history_session_count")
        if profile_history is None:
            profile_history = CORE_MAX_ACTUAL_LOOKBACK
        profile_history = _positive_integer(
            profile_history, "profile_history_session_count"
        )
    else:
        needs_profile_window = bool(
            {"CORE_PROFILE", "MARKET_PARTICIPATION", "D0", "FOCUS_FORMAL"}
            & set(request["selected_modules"])
        )
        if profile_history is None:
            profile_history = raw_coverage.get("current_profile_history_session_count")
        if profile_history is None:
            profile_history = CORE_MAX_ACTUAL_LOOKBACK if needs_profile_window else 1
        profile_history = _positive_integer(
            profile_history, "profile_history_session_count"
        )

    output_counts = request["module_output_session_counts"]
    default_output_count = request["output_session_count"]
    if default_output_count is not None:
        default_output_count = _positive_integer(
            default_output_count, "output_session_count"
        )
    selected_modules = set(request["selected_modules"])
    output_count_fallback = (
        default_output_count if default_output_count is not None else 5
    )
    if request["legacy_request"]:
        d0_requested = _positive_integer(
            request["d0_output_session_count"],
            "d0_output_session_count",
            allow_zero=True,
        )
        d1_requested = _positive_integer(
            request["d1_output_session_count"],
            "d1_output_session_count",
            allow_zero=True,
        )
    else:
        d0_requested = output_counts.get(
            "D0", output_count_fallback if "D0" in selected_modules else 0
        )
        d1_requested = output_counts.get(
            "D1", output_count_fallback if "D1" in selected_modules else 0
        )
        d0_requested = _positive_integer(d0_requested, "D0输出会话数", allow_zero=True)
        d1_requested = _positive_integer(d1_requested, "D1输出会话数", allow_zero=True)
    selected_source_modules = set(request["source_modules"])
    selected_base_modules = set(request["base_modules"])
    selected_research_modules = set(request["research_modules"])
    operation = request["operation"]
    active_package_id = raw_coverage.get("latest_profile_source_package_id")
    if active_package_id is None:
        raw_sessions_in_pg = set(raw_coverage.get("closed_sessions", ()))
    else:
        raw_sessions_in_pg = {
            item["session_date"]
            for item in raw_coverage.get("closed_versions", ())
            if item["source_package_id"] == active_package_id
        }
    if mode == "INITIALIZE":
        target = (
            _parse_session(request["target_session"])
            if request["target_session"] is not None
            else _parse_session(latest_raw)
            if latest_raw
            else cutoff
        )
        if target > cutoff:
            return _blocked_plan(
                mode,
                cutoff,
                raw_coverage,
                "BLOCKED_NOT_AUTHORIZED",
                ["TARGET_AFTER_RESEARCH_CUTOFF"],
                target_session=target.isoformat(),
            )
        calendar_id, calendar_revision, all_sessions = _load_official_sessions(cutoff)
        if target not in all_sessions:
            raise DataJobError(
                "DATA_JOB_TARGET_NOT_TRADING_SESSION",
                "目标日期不是官方交易会话。",
            )
        eligible = tuple(day for day in all_sessions if day <= target and day <= cutoff)
        daily_sessions = ()
    else:
        calendar_id, calendar_revision, all_sessions = _load_official_sessions(cutoff)
        if retrying_daily:
            target = _parse_session(request["target_session"])
            if target > cutoff:
                return _blocked_plan(
                    mode,
                    cutoff,
                    raw_coverage,
                    "BLOCKED_NOT_AUTHORIZED",
                    ["TARGET_AFTER_RESEARCH_CUTOFF"],
                    target_session=target.isoformat(),
                    calendar_id=calendar_id,
                    calendar_revision=calendar_revision,
                )
            if target not in all_sessions:
                raise DataJobError(
                    "DATA_JOB_TARGET_NOT_TRADING_SESSION",
                    "待恢复任务的目标日期不是官方交易会话。",
                )
            eligible = tuple(
                day for day in all_sessions if day <= target and day <= cutoff
            )
            daily_sessions = ()
        else:
            eligible = tuple(day for day in all_sessions if day <= cutoff)
            if not eligible:
                return _blocked_plan(
                    mode,
                    cutoff,
                    raw_coverage,
                    "OFFICIAL_CALENDAR_UNAVAILABLE",
                    ["NO_OFFICIAL_SESSION_THROUGH_CUTOFF"],
                    target_session=None,
                    calendar_id=calendar_id,
                    calendar_revision=calendar_revision,
                )
            if request["legacy_request"]:
                last_completed = raw_coverage.get("last_completed_research_session")
                last = (
                    _parse_session(last_completed)
                    if last_completed
                    else _parse_session(latest_raw)
                    if latest_raw
                    else None
                )
                daily_sessions = tuple(
                    day for day in eligible if last is None or day > last
                )
                if not daily_sessions:
                    if operation in {"RECOMPUTE", *_CLEANUP_OPERATIONS}:
                        daily_sessions = (eligible[-1],)
                    else:
                        return _blocked_plan(
                            mode,
                            cutoff,
                            raw_coverage,
                            "BLOCKED_NOT_AUTHORIZED",
                            ["NO_AUTHORIZED_NEW_OFFICIAL_SESSION"],
                            target_session=None,
                            calendar_id=calendar_id,
                            calendar_revision=calendar_revision,
                        )
                target = daily_sessions[-1]
            else:
                # 新版逐模块水位负责决定补算日期；全局研究水位不能遮住落后的单域。
                target = eligible[-1]
                daily_sessions = (target,)

    raw_request_count = (
        output_counts.get("TDX_RAW", raw_history_count)
        if mode == "INITIALIZE"
        else output_counts.get("TDX_RAW", 1)
    )
    if "TDX_RAW" not in selected_source_modules:
        raw_request_count = 1
    raw_request_count = _positive_integer(raw_request_count, "TDX Raw输出会话数")
    if mode == "MANUAL_DAILY":
        raw_history_count = raw_request_count
    tdx_raw_output_sessions = (
        recent_official_sessions(eligible, through=target, count=raw_request_count)
        if "TDX_RAW" in selected_source_modules
        else ()
    )
    if "BAOSTOCK" in selected_source_modules:
        if "TDX_RAW" in selected_source_modules:
            baostock_output_sessions = tdx_raw_output_sessions
        else:
            baostock_count = _positive_integer(
                output_counts.get("BAOSTOCK", 1), "BAOSTOCK输出会话数"
            )
            baostock_output_sessions = recent_official_sessions(
                eligible, through=target, count=baostock_count
            )
    else:
        baostock_output_sessions = ()
    if mode == "INITIALIZE":
        requested_target_count = raw_request_count
        if len(eligible) < requested_target_count:
            return _blocked_plan(
                mode,
                cutoff,
                raw_coverage,
                "HISTORY_INSUFFICIENT",
                ["OFFICIAL_CALENDAR_WINDOW_TOO_SHORT"],
                target_session=target.isoformat(),
                requested_history_session_count=requested_target_count,
                official_sessions_available=len(eligible),
                calendar_id=calendar_id,
                calendar_revision=calendar_revision,
            )
        windows = select_session_windows(
            eligible,
            through=target,
            raw_history_count=requested_target_count,
            d0_output_count=d0_requested,
            d1_output_count=d1_requested,
        )
        raw_window = windows["raw"] if "TDX_RAW" in selected_source_modules else ()
        d0_count = d0_requested
        d1_count = d1_requested
    else:
        if len(eligible) < profile_history:
            return _blocked_plan(
                mode,
                cutoff,
                raw_coverage,
                "HISTORY_INSUFFICIENT",
                ["OFFICIAL_CALENDAR_WINDOW_TOO_SHORT"],
                target_session=target.isoformat(),
                requested_history_session_count=profile_history,
                official_sessions_available=len(eligible),
                calendar_id=calendar_id,
                calendar_revision=calendar_revision,
            )
        raw_window = (
            recent_official_sessions(eligible, through=target, count=raw_request_count)
            if "TDX_RAW" in selected_source_modules
            else ()
        )
        d0_count = (
            len(daily_sessions)
            if request["legacy_request"] and not retrying_daily
            else d0_requested
        )
        d1_count = (
            len(daily_sessions)
            if request["legacy_request"] and not retrying_daily
            else d1_requested
        )

    if d0_count > len(eligible) or d1_count > len(eligible):
        return _blocked_plan(
            mode,
            cutoff,
            raw_coverage,
            "HISTORY_INSUFFICIENT",
            ["REQUESTED_OUTPUT_SESSION_COUNT_EXCEEDS_CALENDAR"],
            target_session=target.isoformat(),
            requested_history_session_count=profile_history,
            official_sessions_available=len(eligible),
            d0_output_session_count=d0_count,
            d1_output_session_count=d1_count,
            calendar_id=calendar_id,
            calendar_revision=calendar_revision,
        )

    module_counts = request["module_output_session_counts"]
    module_output_sessions: dict[str, tuple[date, ...]] = {}
    for module in sorted(selected_base_modules | selected_research_modules):
        if module == "D0":
            count = d0_count
        elif module == "D1":
            count = d1_count
        else:
            fallback = (
                output_count_fallback
                if module
                in {
                    "CORE_PROFILE",
                    "MARKET_BREADTH",
                    "MARKET_PARTICIPATION",
                    "SECTOR_PULSE",
                    "SECTOR_TRANSITION",
                    "SECTOR_FOLLOWUP",
                    "ADJUSTED_PRICE",
                    "FOCUS_FORMAL",
                    "D0",
                    "D1",
                }
                else 5
            )
            count = module_counts.get(module, default_output_count or fallback)
        if count == 0:
            module_output_sessions[module] = ()
            continue
        module_output_sessions[module] = _module_output_sessions(
            module=module,
            requested_count=count,
            sessions=eligible,
            target=target,
            operation=operation,
            raw_coverage=raw_coverage,
        )

    adjusted_action_session_set = {
        session
        for session in adjusted_action_sessions
        if type(session) is date and session <= target
    }
    adjusted_symbols = adjusted_action_symbols_by_session or {}
    adjusted_source_windows: dict[str, tuple[date, ...]] = {}
    if "ADJUSTED_PRICE" in selected_research_modules:
        for day in module_output_sessions.get("ADJUSTED_PRICE", ()):
            day_text = day.isoformat()
            symbols = tuple(
                sorted(
                    {
                        symbol
                        for symbol in adjusted_symbols.get(day_text, ())
                        if isinstance(symbol, str) and symbol
                    }
                )
            )
            if day not in adjusted_action_session_set or not symbols:
                continue
            source_window = recent_official_sessions(eligible, through=day, count=2)
            if len(source_window) == 2 and source_window[-1] == day:
                adjusted_source_windows[day_text] = source_window
        if "TDX_RAW" in selected_source_modules and adjusted_source_windows:
            raw_window = tuple(
                sorted(
                    set(raw_window).union(
                        session
                        for window in adjusted_source_windows.values()
                        for session in window
                    )
                )
            )

    sector_followup_due: dict[str, dict[int, date]] = {}
    if "SECTOR_FOLLOWUP" in selected_research_modules:
        official_through_cutoff = tuple(
            session for session in all_sessions if session <= cutoff
        )
        for t0 in module_output_sessions.get("SECTOR_FOLLOWUP", ()):
            index = official_through_cutoff.index(t0)
            due = {}
            for horizon in (1, 3, 5):
                endpoint_index = index + horizon
                if endpoint_index < len(official_through_cutoff):
                    due[horizon] = official_through_cutoff[endpoint_index]
            sector_followup_due[t0.isoformat()] = due

    sector_transition_pairs: dict[str, str | None] = {}
    if "SECTOR_TRANSITION" in selected_research_modules:
        transition_targets = module_output_sessions.get("SECTOR_TRANSITION", ())
        if not transition_targets:
            transition_targets = module_output_sessions.get("SECTOR_PULSE", ())
        for target_day in transition_targets:
            previous = tuple(session for session in eligible if session < target_day)
            sector_transition_pairs[target_day.isoformat()] = (
                previous[-1].isoformat() if previous else None
            )

    d0_sessions = module_output_sessions.get("D0", ())
    d1_sessions = module_output_sessions.get("D1", ())
    if request["legacy_request"] and mode == "MANUAL_DAILY" and not retrying_daily:
        d0_sessions = daily_sessions if d0_count else ()
        d1_sessions = daily_sessions if d1_count else ()
        module_output_sessions["D0"] = d0_sessions
        module_output_sessions["D1"] = d1_sessions
    focus_sessions = (
        module_output_sessions.get("FOCUS_FORMAL", ())
        if not request["legacy_request"]
        else d0_sessions
    )
    profile_dates = (
        tuple(sorted(module_output_sessions.get("CORE_PROFILE", ())))
        if not request["legacy_request"]
        else tuple(
            sorted(
                set(d0_sessions)
                | set(d1_sessions)
                | ({target} if mode == "INITIALIZE" else set())
            )
        )
    )
    market_sessions = (
        tuple(sorted(module_output_sessions.get("MARKET_BREADTH", ())))
        if not request["legacy_request"]
        else raw_window
        if mode == "INITIALIZE"
        else daily_sessions
    )
    participation_sessions = (
        tuple(sorted(module_output_sessions.get("MARKET_PARTICIPATION", ())))
        if not request["legacy_request"]
        else profile_dates
    )

    stored_d1_window = raw_coverage.get("current_d1_history_session_count")
    d1_window_ambiguous = raw_coverage.get("d1_history_window_ambiguous") is True
    current_d1_window = request["d1_history_session_count"]
    if current_d1_window is None:
        current_d1_window = stored_d1_window
    if current_d1_window is None and not d1_window_ambiguous:
        from stock_a.application.source_research_day import (
            D1_DEFAULT_HISTORY_SESSION_COUNT,
        )

        current_d1_window = D1_DEFAULT_HISTORY_SESSION_COUNT
    required_raw_sessions: set[date] = set(raw_window)
    required_raw_sessions.update(tdx_raw_output_sessions)
    profile_inputs: dict[date, tuple[date, ...]] = {}
    d0_inputs: dict[date, tuple[date, ...]] = {}
    d0_profile_inputs: dict[date, tuple[date, ...]] = {}
    market_inputs: dict[date, tuple[date, ...]] = {}
    d1_inputs: dict[date, tuple[date, ...]] = {}
    for day in profile_dates:
        try:
            profile_inputs[day] = recent_official_sessions(
                eligible, through=day, count=profile_history
            )
            required_raw_sessions.update(profile_inputs[day])
        except DataJobError:
            profile_inputs[day] = ()
    for day in market_sessions:
        try:
            market_inputs[day] = recent_official_sessions(
                eligible, through=day, count=5
            )
            required_raw_sessions.update(market_inputs[day])
        except DataJobError:
            market_inputs[day] = ()
    for day in d0_sessions:
        previous = tuple(session for session in eligible if session < day)
        if previous:
            d0_inputs[day] = (previous[-1], day)
            required_raw_sessions.update(d0_inputs[day])
            try:
                d0_profile_inputs[day] = recent_official_sessions(
                    eligible, through=day, count=profile_history
                )
                required_raw_sessions.update(d0_profile_inputs[day])
            except DataJobError:
                d0_profile_inputs[day] = ()
        else:
            d0_inputs[day] = ()
    if d1_sessions and current_d1_window is not None:
        for day in d1_sessions:
            try:
                d1_inputs[day] = recent_official_sessions(
                    eligible, through=day, count=max(2, current_d1_window)
                )
                required_raw_sessions.update(d1_inputs[day])
            except DataJobError:
                d1_inputs[day] = ()
    if "SECTOR_PULSE" in selected_research_modules:
        required_raw_sessions.update(module_output_sessions.get("SECTOR_PULSE", ()))
    if "SECTOR_TRANSITION" in selected_research_modules:
        for target_text, previous_text in sector_transition_pairs.items():
            required_raw_sessions.add(date.fromisoformat(target_text))
            if previous_text is not None:
                required_raw_sessions.add(date.fromisoformat(previous_text))
    if "SECTOR_FOLLOWUP" in selected_research_modules:
        for t0_text, due_targets in sector_followup_due.items():
            required_raw_sessions.add(date.fromisoformat(t0_text))
            required_raw_sessions.update(due_targets.values())
    if "MARKET_PARTICIPATION" in selected_base_modules:
        required_raw_sessions.update(
            module_output_sessions.get("MARKET_PARTICIPATION", ())
        )
    raw_window = tuple(sorted(required_raw_sessions))
    if operation == "CLEAR_DERIVED":
        raw_window = ()

    closed_versions = tuple(raw_coverage.get("closed_versions", ()))
    package_sessions: dict[int, dict[str, Mapping[str, Any]]] = {}
    for item in closed_versions:
        package_sessions.setdefault(int(item["source_package_id"]), {})[
            str(item["session_date"])
        ] = item
    baostock_raw_session_inputs: list[Mapping[str, Any]] = []
    baostock_source_package_id = None
    if baostock_output_sessions and "TDX_RAW" not in selected_source_modules:
        resolved_versions = []
        for session in baostock_output_sessions:
            matches = [
                item
                for item in closed_versions
                if item.get("session_date") == session.isoformat()
            ]
            preferred = [
                item
                for item in matches
                if item.get("source_package_id") == active_package_id
            ]
            candidates = preferred or matches
            if len(candidates) != 1:
                resolved_versions = []
                break
            resolved_versions.append(candidates[0])
        package_ids = {int(item["source_package_id"]) for item in resolved_versions}
        if (
            len(resolved_versions) == len(baostock_output_sessions)
            and len(package_ids) == 1
            and all(
                int(item["row_count"]) == int(item["expected_row_count"])
                for item in resolved_versions
            )
        ):
            baostock_source_package_id = next(iter(package_ids))
            baostock_raw_session_inputs = sorted(
                resolved_versions, key=lambda item: item["session_date"]
            )
    package_order = sorted(
        package_sessions,
        key=lambda package_id: (
            package_id == active_package_id,
            len(package_sessions[package_id]),
            package_id,
        ),
        reverse=True,
    )
    required_iso = {day.isoformat() for day in raw_window}
    if "ADJUSTED_PRICE" in selected_research_modules:
        required_iso.update(
            session.isoformat()
            for window in adjusted_source_windows.values()
            for session in window
        )
    reusable_packages = [
        package_id
        for package_id in package_order
        if required_iso.issubset(package_sessions[package_id])
    ]
    reuse_raw_package_id = reusable_packages[0] if reusable_packages else None
    if request["initialization_mode"] == "RESET_ALL_BUSINESS_FACTS":
        # 清库重建不能把将被删除的PG Raw包误当作可复用输入。
        reuse_raw_package_id = None
    raw_version_by_date = (
        package_sessions.get(reuse_raw_package_id, {})
        if reuse_raw_package_id is not None
        else {}
    )
    raw_sessions_in_pg = set(raw_version_by_date)
    missing_raw = [
        day.isoformat()
        for day in raw_window
        if day.isoformat() not in raw_sessions_in_pg
    ]
    source_will_fill_raw = "TDX_RAW" in selected_source_modules
    raw_window_set = set(raw_window)
    raw_input_unavailable = set(missing_raw) if not source_will_fill_raw else set()
    d0_insufficient = [
        day.isoformat()
        for day in d0_sessions
        if not d0_inputs.get(day)
        or (
            not source_will_fill_raw
            and any(
                session.isoformat() in raw_input_unavailable
                for session in (
                    *d0_inputs.get(day, ()),
                    *d0_profile_inputs.get(day, ()),
                )
            )
        )
    ]
    d1_insufficient = [
        day.isoformat()
        for day in d1_sessions
        if current_d1_window is None
        or len(d1_inputs.get(day, ())) < max(2, current_d1_window)
        or (
            not source_will_fill_raw
            and any(
                item.isoformat() in raw_input_unavailable
                for item in d1_inputs.get(day, ())
            )
        )
    ]
    profile_window_by_date = {
        day: len(profile_inputs.get(day, ())) for day in profile_dates
    }
    profile_insufficient = [
        day.isoformat()
        for day in profile_dates
        if profile_window_by_date.get(day, 0)
        < min(CORE_MAX_ACTUAL_LOOKBACK, profile_history)
        or (
            not source_will_fill_raw
            and any(
                item.isoformat() in raw_input_unavailable
                for item in profile_inputs.get(day, ())
            )
        )
    ]
    completion_units = raw_coverage.get("completed_units_by_module", {})
    if not isinstance(completion_units, Mapping):
        completion_units = {}

    def completed_for_selected_raw(
        module: str,
        session: date,
        *,
        profile_window: int | None = None,
        d1_window: int | None = None,
        expected_calendar_revision: str | None = None,
    ) -> bool:
        version = raw_version_by_date.get(session.isoformat())
        if not isinstance(version, Mapping):
            return False
        package_id = version.get("source_package_id")
        capture_id = version.get("source_capture_id")
        if type(package_id) is not int or type(capture_id) is not int:
            return False
        if profile_window is not None and profile_window < 1:
            return False
        candidates = completion_units.get(module, ())
        if not isinstance(candidates, Sequence):
            return False
        expected_profile_revision = None
        revision_calendar = expected_calendar_revision
        if profile_window is not None:
            from stock_a.application.pg_price_profiles import (
                SOURCE_SYMBOL_PROFILE_SCOPE,
                _profile_revision,
            )
            from stock_a.application.research_window import resolve_pg_history_window

            _target, _window, revision_calendar = resolve_pg_history_window(
                session.isoformat(), profile_window
            )
            expected_profile_revision = _profile_revision(
                scope=SOURCE_SYMBOL_PROFILE_SCOPE,
                calendar_id=calendar_id,
                calendar_revision=revision_calendar,
                history_session_count=profile_window,
            )
        if module == "CORE_PROFILE":
            for profile in raw_coverage.get("profile_completed_versions", ()):
                if (
                    profile.get("target_session") == session.isoformat()
                    and profile.get("source_package_id") == package_id
                    and profile.get("source_capture_id") == capture_id
                    and profile.get("history_session_count") == profile_window
                    and profile.get("profile_revision") == expected_profile_revision
                ):
                    return True
        if module == "MARKET_BREADTH":
            for breadth in raw_coverage.get("market_breadth_completed_versions", ()):
                if (
                    breadth.get("target_session") == session.isoformat()
                    and breadth.get("source_package_id") == package_id
                    and breadth.get("source_capture_id") == capture_id
                    and breadth.get("contract_version")
                    == "MKT-02_RAW_POSITIVE_RETURN_STATE_DELTA_R1"
                    and breadth.get("calendar_revision") == expected_calendar_revision
                ):
                    return True
        for candidate in candidates:
            if (
                candidate.get("target_session") != session.isoformat()
                or candidate.get("source_package_id") != package_id
                or candidate.get("source_capture_id") != capture_id
            ):
                continue
            tables = candidate.get("business_tables", ())
            if not isinstance(tables, Sequence):
                continue
            if profile_window is not None and any(
                row.get("profile_history_session_count") != profile_window
                or row.get("profile_revision") != expected_profile_revision
                for row in tables
            ):
                continue
            if d1_window is not None and any(
                row.get("d1_history_session_count") != d1_window for row in tables
            ):
                continue
            if revision_calendar is not None and any(
                row.get("calendar_revision") != revision_calendar for row in tables
            ):
                continue
            return True
        return False

    planned_profile_ready = {
        item.isoformat()
        for item in profile_dates
        if item.isoformat() not in profile_insufficient
    }
    stored_profile_ready = {
        day.isoformat()
        for day in set(d0_sessions) | set(focus_sessions) | set(participation_sessions)
        if completed_for_selected_raw(
            "CORE_PROFILE",
            day,
            profile_window=len(profile_inputs.get(day, ()))
            or len(d0_profile_inputs.get(day, ()))
            or profile_history,
            expected_calendar_revision=calendar_revision,
        )
    }
    profile_ready_sessions = planned_profile_ready | stored_profile_ready
    sector_pulse_insufficient = {
        day.isoformat()
        for day in module_output_sessions.get("SECTOR_PULSE", ())
        if not source_will_fill_raw and day.isoformat() in raw_input_unavailable
    }
    d0_ready_sessions = {
        day.isoformat() for day in d0_sessions if day.isoformat() not in d0_insufficient
    } | {
        day.isoformat()
        for day in focus_sessions
        if completed_for_selected_raw(
            "D0", day, expected_calendar_revision=calendar_revision
        )
    }
    planned_breadth_ready = {
        day.isoformat()
        for day in market_sessions
        if len(market_inputs.get(day, ())) == 5
        and (
            source_will_fill_raw
            or not any(
                item.isoformat() in raw_input_unavailable
                for item in market_inputs.get(day, ())
            )
        )
        and operation != "CLEAR_DERIVED"
    }
    stored_breadth_ready = {
        day.isoformat()
        for day in participation_sessions
        if completed_for_selected_raw(
            "MARKET_BREADTH",
            day,
            expected_calendar_revision=calendar_revision,
        )
    }
    stored_breadth_ready.update(
        str(breadth["target_session"])
        for breadth in raw_coverage.get("market_breadth_completed_versions", ())
        if isinstance(breadth, Mapping)
        and breadth.get("calendar_revision") == calendar_revision
        and (version := raw_version_by_date.get(str(breadth.get("target_session"))))
        is not None
        and breadth.get("source_package_id") == version.get("source_package_id")
        and breadth.get("source_capture_id") == version.get("source_capture_id")
    )
    breadth_ready_sessions = planned_breadth_ready | stored_breadth_ready
    if "D0" in selected_research_modules:
        profile_insufficient.extend(
            day.isoformat()
            for day in d0_sessions
            if day.isoformat() not in profile_ready_sessions
        )
    if "FOCUS_FORMAL" in selected_research_modules:
        profile_insufficient.extend(
            day.isoformat()
            for day in focus_sessions
            if day.isoformat() not in profile_ready_sessions
        )
    profile_insufficient = sorted(set(profile_insufficient))
    sector_transition_insufficient = {
        target_text
        for target_text, previous_text in sector_transition_pairs.items()
        if previous_text is None
        or (
            not source_will_fill_raw
            and any(
                session.isoformat() in raw_input_unavailable
                for session in (
                    date.fromisoformat(target_text),
                    date.fromisoformat(previous_text),
                )
            )
        )
    }
    planned_seed_sessions = {
        day
        for day in d0_sessions
        if day.isoformat() not in d0_insufficient
        and day.isoformat() not in profile_insufficient
    }

    def seed_available(session: date) -> bool:
        if session in planned_seed_sessions:
            return True
        profile_window = (
            len(d0_profile_inputs.get(session, ()))
            or len(profile_inputs.get(session, ()))
            or profile_history
        )
        return completed_for_selected_raw(
            "SEED",
            session,
            profile_window=profile_window,
            expected_calendar_revision=calendar_revision,
        )

    sector_pulse_insufficient = sorted(
        {
            *sector_pulse_insufficient,
            *(
                day.isoformat()
                for day in module_output_sessions.get("SECTOR_PULSE", ())
                if not seed_available(day)
            ),
        }
    )
    sector_transition_insufficient.update(
        target_text
        for target_text, previous_text in sector_transition_pairs.items()
        if previous_text is None
        or not seed_available(date.fromisoformat(target_text))
        or not seed_available(date.fromisoformat(previous_text))
    )
    focus_insufficient = {
        day.isoformat()
        for day in focus_sessions
        if day.isoformat() in d0_insufficient
        or day.isoformat() in profile_insufficient
        or day.isoformat() not in d0_ready_sessions
    }
    market_insufficient = {
        day.isoformat()
        for day in market_sessions
        if len(market_inputs.get(day, ())) < 5
        or (
            not source_will_fill_raw
            and any(
                item.isoformat() in raw_input_unavailable
                for item in market_inputs.get(day, ())
            )
        )
    }
    market_participation_insufficient = {
        day.isoformat()
        for day in participation_sessions
        if day.isoformat() in profile_insufficient
        or day.isoformat() in market_insufficient
        or day.isoformat() not in profile_ready_sessions
        or day.isoformat() not in breadth_ready_sessions
        or (not source_will_fill_raw and day.isoformat() in raw_input_unavailable)
    }
    sector_followup_insufficient = {
        t0
        for t0, due in sector_followup_due.items()
        if not source_will_fill_raw
        and any(
            session.isoformat() in raw_input_unavailable
            for session in (date.fromisoformat(t0), *due.values())
        )
    }
    units: list[dict[str, Any]] = []
    if "TDX_RAW" in selected_source_modules:
        _unit(
            units,
            stage="SOURCE_RAW",
            target_session=target,
            business_table="stock.source_packages",
        )
        for day in raw_window:
            _unit(
                units,
                stage="SOURCE_RAW",
                target_session=day,
                business_table="stock.source_captures",
            )
            _unit(
                units,
                stage="SOURCE_RAW",
                target_session=day,
                business_table="stock.source_raw_daily",
            )
    if "BAOSTOCK" in selected_source_modules:
        for day in baostock_output_sessions:
            _unit(
                units,
                stage="SOURCE_BAOSTOCK",
                target_session=day,
                business_table="stock.source_baostock_daily",
                status=(
                    "NOT_COMPUTED"
                    if "TDX_RAW" not in selected_source_modules
                    and baostock_source_package_id is None
                    else "NOT_STARTED"
                ),
                error_code=(
                    "BAOSTOCK_RAW_INPUT_UNAVAILABLE"
                    if "TDX_RAW" not in selected_source_modules
                    and baostock_source_package_id is None
                    else None
                ),
                error_message=(
                    "目标会话没有唯一、完整且同包绑定的PG TDX Raw输入；不启动TDX采集。"
                    if "TDX_RAW" not in selected_source_modules
                    and baostock_source_package_id is None
                    else None
                ),
            )
    if "TDX_SECTOR_MEMBERS" in selected_source_modules:
        for table in _STAGE_TABLES["SOURCE_SECTOR_MEMBERS"]:
            _unit(
                units,
                stage="SOURCE_SECTOR_MEMBERS",
                target_session=target,
                business_table=table,
            )
    for day in market_sessions:
        status = (
            "NOT_COMPUTED" if day.isoformat() in market_insufficient else "NOT_STARTED"
        )
        for table in _STAGE_TABLES["MARKET_BREADTH"]:
            _unit(
                units,
                stage="MARKET_BREADTH",
                target_session=day,
                business_table=table,
                status=status,
                error_code="MARKET_BREADTH_HISTORY_INSUFFICIENT"
                if status == "NOT_COMPUTED"
                else None,
                error_message="市场宽度要求的前序官方Raw会话不在本任务范围内。"
                if status == "NOT_COMPUTED"
                else None,
            )
    for day in profile_dates:
        status = (
            "NOT_COMPUTED" if day.isoformat() in profile_insufficient else "NOT_STARTED"
        )
        _unit(
            units,
            stage="CORE_PROFILE",
            target_session=day,
            business_table="stock.source_symbol_price_profiles",
            status=status,
            error_code="PROFILE_HISTORY_INSUFFICIENT"
            if status == "NOT_COMPUTED"
            else None,
            error_message="请求的历史窗口不在本任务Raw范围内，保留未计算状态。"
            if status == "NOT_COMPUTED"
            else None,
        )
    for day in participation_sessions:
        status = (
            "NOT_COMPUTED"
            if day.isoformat() in market_participation_insufficient
            else "NOT_STARTED"
        )
        for table in _STAGE_TABLES["MARKET_PARTICIPATION"]:
            _unit(
                units,
                stage="MARKET_PARTICIPATION",
                target_session=day,
                business_table=table,
                status=status,
                error_code="MARKET_PARTICIPATION_INPUT_UNAVAILABLE"
                if status == "NOT_COMPUTED"
                else None,
                error_message="市场宽度或该日Core/Profile窗口未闭合。"
                if status == "NOT_COMPUTED"
                else None,
            )
    for day in d0_sessions:
        insufficient_output = day.isoformat() in d0_insufficient
        insufficient_profile = day.isoformat() in profile_insufficient
        status = (
            "NOT_COMPUTED"
            if insufficient_output or insufficient_profile
            else "NOT_STARTED"
        )
        code = (
            "D0_OUTPUT_HISTORY_INSUFFICIENT"
            if insufficient_output
            else "PROFILE_HISTORY_INSUFFICIENT"
            if insufficient_profile
            else None
        )
        message = (
            "输出目标日或其前一官方会话不在本任务Raw范围内。"
            if insufficient_output
            else "静态研究所需Core/Profile历史窗口不可用。"
            if insufficient_profile
            else None
        )
        for table in _STAGE_TABLES["STATIC_RESEARCH"]:
            _unit(
                units,
                stage="STATIC_RESEARCH",
                target_session=day,
                business_table=table,
                status=status,
                error_code=code,
                error_message=message,
            )
    for day in d0_sessions:
        status = "NOT_COMPUTED" if day.isoformat() in d0_insufficient else "NOT_STARTED"
        for table in (
            "stock.source_d0_launch_day_facts",
            "stock.source_research_enrollments",
        ):
            _unit(
                units,
                stage="D0_D1",
                target_session=day,
                business_table=table,
                status=status,
                error_code="D0_OUTPUT_HISTORY_INSUFFICIENT"
                if status == "NOT_COMPUTED"
                else None,
                error_message="输出目标日或其前一官方会话不在本任务Raw范围内。"
                if status == "NOT_COMPUTED"
                else None,
            )
    for day in focus_sessions:
        formal_status = (
            "NOT_COMPUTED" if day.isoformat() in focus_insufficient else "NOT_STARTED"
        )
        _unit(
            units,
            stage="FOCUS_FORMAL",
            target_session=day,
            business_table="stock.source_d0_formal_admission_day_facts",
            status=formal_status,
            error_code="FORMAL_ADMISSION_INPUT_INSUFFICIENT"
            if formal_status == "NOT_COMPUTED"
            else None,
            error_message="D0或Core/Profile前置事实未闭合，正式Focus准入保留未计算。"
            if formal_status == "NOT_COMPUTED"
            else None,
        )
    for day in d1_sessions:
        status = "NOT_COMPUTED" if day.isoformat() in d1_insufficient else "NOT_STARTED"
        for table in (
            "stock.source_d1_structure_day_facts",
            "stock.source_d1_material_anchor_day_facts",
        ):
            _unit(
                units,
                stage="D0_D1",
                target_session=day,
                business_table=table,
                status=status,
                error_code="D1_HISTORY_WINDOW_UNAVAILABLE"
                if status == "NOT_COMPUTED"
                else None,
                error_message="现行D1算法窗口不在本任务Raw范围内或窗口配置不唯一。"
                if status == "NOT_COMPUTED"
                else None,
            )
    if d0_count == 0 and request["legacy_request"]:
        for stage, table in (
            ("STATIC_RESEARCH", "stock.source_d0_scenario_day_facts"),
            ("STATIC_RESEARCH", "stock.source_pool_seed_day_facts"),
            ("D0_D1", "stock.source_d0_launch_day_facts"),
            ("D0_D1", "stock.source_research_enrollments"),
        ):
            _unit(
                units,
                stage=stage,
                target_session=target,
                business_table=table,
                status="SKIPPED_BY_USER",
                error_code="SKIPPED_BY_USER",
                error_message="用户关闭D0输出。",
            )
        _unit(
            units,
            stage="FOCUS_FORMAL",
            target_session=target,
            business_table="stock.source_d0_formal_admission_day_facts",
            status="SKIPPED_BY_USER",
            error_code="SKIPPED_BY_USER",
            error_message="用户关闭D0输出，正式Focus准入随之跳过。",
        )
    if d1_count == 0 and request["legacy_request"]:
        for table in (
            "stock.source_d1_structure_day_facts",
            "stock.source_d1_material_anchor_day_facts",
        ):
            _unit(
                units,
                stage="D0_D1",
                target_session=target,
                business_table=table,
                status="SKIPPED_BY_USER",
                error_code="SKIPPED_BY_USER",
                error_message="用户关闭D1输出。",
            )

    if operation in _CLEANUP_OPERATIONS:
        for unit in units:
            unit["status"] = "NOT_STARTED"
            unit["error_code"] = None
            unit["error_message"] = None

    blockers: list[str] = []
    source_io_required = (
        bool(missing_raw and "TDX_RAW" in selected_source_modules)
        or "TDX_SECTOR_MEMBERS" in selected_source_modules
        or "BAOSTOCK" in selected_source_modules
    )
    if not execution_authorized and source_io_required:
        blockers.append("DATA_EXECUTION_NOT_AUTHORIZED")
    if d1_sessions and current_d1_window is None:
        blockers.append(
            "D1_HISTORY_WINDOW_AMBIGUOUS"
            if d1_window_ambiguous
            else "D1_HISTORY_WINDOW_NOT_ESTABLISHED"
        )
    if any(item["status"] == "NOT_COMPUTED" for item in units):
        blockers.append("HISTORY_INSUFFICIENT_FOR_SOME_OUTPUT_DATES")

    executable = execution_authorized or not source_io_required
    if not executable:
        status = "BLOCKED_NOT_AUTHORIZED"
    elif any(item["status"] == "NOT_COMPUTED" for item in units):
        status = "READY_WITH_INCOMPLETE_UNITS"
    elif missing_raw:
        status = "READY_SOURCE_CHECK_REQUIRED"
    else:
        status = "READY"

    return {
        "mode": mode,
        "status": status,
        "executable": executable,
        "blocked_reasons": blockers,
        "target_session": target.isoformat(),
        "research_cutoff": cutoff.isoformat(),
        "history_session_count": max(raw_request_count, len(raw_window)),
        "profile_history_session_count": profile_history,
        "d0_output_session_count": d0_count,
        "d1_output_session_count": d1_count,
        "d1_calculation_session_count": current_d1_window,
        "calendar_id": calendar_id,
        "calendar_revision": calendar_revision,
        "raw_history_sessions": [day.isoformat() for day in raw_window],
        "tdx_raw_output_sessions": [day.isoformat() for day in tdx_raw_output_sessions],
        "baostock_output_sessions": [
            day.isoformat() for day in baostock_output_sessions
        ],
        "baostock_source_package_id": baostock_source_package_id,
        "baostock_raw_session_inputs": [
            dict(item) for item in baostock_raw_session_inputs
        ],
        "raw_history_session_count_available": len(raw_window),
        "raw_history_sessions_in_pg": len(raw_window) - len(missing_raw),
        "missing_raw_sessions": missing_raw,
        "reuse_raw_package_id": reuse_raw_package_id,
        "raw_session_inputs": [
            raw_version_by_date[day.isoformat()]
            for day in raw_window
            if day.isoformat() in raw_version_by_date
        ]
        if reuse_raw_package_id is not None
        else [],
        "d0_output_sessions": [day.isoformat() for day in d0_sessions],
        "d1_output_sessions": [day.isoformat() for day in d1_sessions],
        "profile_output_sessions": [day.isoformat() for day in profile_dates],
        "profile_history_session_counts_by_session": {
            day.isoformat(): len(
                profile_inputs.get(day, ()) or d0_profile_inputs.get(day, ())
            )
            for day in sorted(set(profile_dates) | set(d0_sessions))
        },
        "d0_profile_history_session_counts_by_session": {
            day.isoformat(): len(d0_profile_inputs.get(day, ())) for day in d0_sessions
        },
        "d1_history_session_counts_by_session": {
            day.isoformat(): len(d1_inputs.get(day, ())) for day in d1_sessions
        },
        "market_participation_sessions": [
            day.isoformat() for day in participation_sessions
        ],
        "market_breadth_sessions": [day.isoformat() for day in market_sessions],
        "sector_pulse_sessions": [
            day.isoformat() for day in module_output_sessions.get("SECTOR_PULSE", ())
        ],
        "sector_transition_pairs": sector_transition_pairs,
        "sector_transition_sessions": sorted(sector_transition_pairs),
        "sector_transition_insufficient_sessions": sorted(
            sector_transition_insufficient
        ),
        "sector_followup_sessions": [
            day.isoformat() for day in module_output_sessions.get("SECTOR_FOLLOWUP", ())
        ],
        "sector_followup_due_targets": {
            t0: {
                str(horizon): endpoint.isoformat() for horizon, endpoint in due.items()
            }
            for t0, due in sector_followup_due.items()
        },
        "sector_pulse_insufficient_sessions": sorted(sector_pulse_insufficient),
        "sector_followup_insufficient_sessions": sorted(sector_followup_insufficient),
        "module_output_sessions": {
            **{
                module: [day.isoformat() for day in sessions]
                for module, sessions in module_output_sessions.items()
            },
            **(
                {"BAOSTOCK": [day.isoformat() for day in baostock_output_sessions]}
                if "BAOSTOCK" in selected_source_modules
                else {}
            ),
            **(
                {"TDX_SECTOR_MEMBERS": [target.isoformat()]}
                if "TDX_SECTOR_MEMBERS" in selected_source_modules
                else {}
            ),
            **(
                {"TDX_RAW": [day.isoformat() for day in tdx_raw_output_sessions]}
                if "TDX_RAW" in selected_source_modules
                else {}
            ),
        },
        "adjusted_price_source_windows": {
            target_session: [session.isoformat() for session in window]
            for target_session, window in adjusted_source_windows.items()
        },
        "adjusted_price_symbols_by_session": {
            target_session: list(adjusted_symbols.get(target_session, ()))
            for target_session in adjusted_source_windows
        },
        "market_breadth_insufficient_history_sessions": sorted(market_insufficient),
        "market_participation_insufficient_sessions": sorted(
            market_participation_insufficient
        ),
        "unplanned_producers": [],
        "d0_insufficient_previous_sessions": d0_insufficient,
        "d1_insufficient_history_sessions": d1_insufficient,
        "profile_insufficient_history_sessions": profile_insufficient,
        "source_coverage": {
            "status": "PG_SOURCE_REUSE_AVAILABLE"
            if reuse_raw_package_id is not None and not missing_raw
            else "NOT_EXECUTED_DATA_FREEZE"
            if source_io_required and not execution_authorized
            else "CHECKED_AFTER_EXPLICIT_START"
            if source_io_required
            else "PG_SOURCE_INPUT_UNAVAILABLE"
            if missing_raw
            else "NO_SOURCE_IO_REQUIRED",
            "verified_external_sessions": None,
            "source_request_performed": False,
            "pg_closed_raw_session_count": raw_coverage.get("stored_session_count", 0),
            "pg_first_session": raw_coverage.get("first_stored_session"),
            "pg_last_session": raw_coverage.get("last_stored_session"),
            "active_pg_source_package_id": active_package_id,
        },
        "stage_units": units,
        "planned_unit_count": len(units),
        "database_write_attempted": False,
        "source_io_attempted": False,
    }


def _blocked_plan(
    mode: str,
    cutoff: date,
    raw_coverage: Mapping[str, Any],
    status: str,
    reasons: list[str],
    **extra: Any,
) -> dict[str, Any]:
    return {
        "mode": mode,
        "status": status,
        "executable": False,
        "blocked_reasons": reasons,
        "research_cutoff": cutoff.isoformat(),
        "source_coverage": {
            "status": "NOT_CHECKED_DATA_FREEZE",
            "verified_external_sessions": None,
            "source_request_performed": False,
            "pg_closed_raw_session_count": raw_coverage.get("stored_session_count", 0),
            "pg_first_session": raw_coverage.get("first_stored_session"),
            "pg_last_session": raw_coverage.get("last_stored_session"),
            "active_pg_source_package_id": raw_coverage.get(
                "latest_profile_source_package_id"
            ),
        },
        "stage_units": [],
        "planned_unit_count": 0,
        "database_write_attempted": False,
        "source_io_attempted": False,
        **extra,
    }


def _job_request_key(plan: Mapping[str, Any]) -> str:
    identity = {
        "mode": plan["mode"],
        "target_session": plan["target_session"],
        "history_session_count": plan["history_session_count"],
        "profile_history_session_count": plan.get("profile_history_session_count"),
        "d0_output_session_count": plan["d0_output_session_count"],
        "d1_output_session_count": plan["d1_output_session_count"],
        "calendar_id": plan["calendar_id"],
        "calendar_revision": plan["calendar_revision"],
        "research_cutoff": plan["research_cutoff"],
        "selected_modules": plan.get("selected_modules", []),
        "module_output_session_counts": plan.get("module_output_session_counts", {}),
        "operation": plan.get("operation", "FILL_MISSING"),
        "initialization_mode": plan.get("initialization_mode", "REUSE_RAW"),
        "daily_mode": plan.get("daily_mode", "CUSTOM"),
        "reset_preview_fingerprint": plan.get("reset_preview_fingerprint"),
        "source_version_fingerprint": plan.get("source_version_fingerprint"),
        "sector_snapshot_ids": plan.get("sector_snapshot_ids"),
        "cleanup_preview_key": plan.get("cleanup_preview", {}).get("preview_key"),
    }
    canonical = json.dumps(
        identity, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class DataJobCoordinator:
    """在单个预览服务进程中运行一项后台任务并持久化读回进度。"""

    def __init__(
        self,
        *,
        config: Mapping[str, Any],
        research_cutoff: date,
        execution_authorized: bool = False,
    ) -> None:
        self.config = config
        self.research_cutoff = research_cutoff
        self.execution_authorized = execution_authorized is True
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self.startup_recovery = data_job_pg.recover_interrupted_jobs(config)

    def _coverage(self) -> dict[str, Any]:
        return data_job_pg.read_raw_coverage(self.config, cutoff=self.research_cutoff)

    def capabilities(self) -> dict[str, Any]:
        coverage = self._coverage()
        latest = data_job_pg.read_recent_jobs(self.config, limit=1)
        from stock_a.application.source_sector_pulse import (
            resolve_source_sector_snapshot_pair,
        )

        sector_pair = resolve_source_sector_snapshot_pair(self.config)
        try:
            _calendar_id, _calendar_revision, all_sessions = _load_official_sessions(
                self.research_cutoff
            )
            target_sessions = [
                item.isoformat()
                for item in all_sessions
                if item <= self.research_cutoff
            ]
        except DataJobError:
            target_sessions = list(coverage.get("closed_sessions", ()))
        modules = []
        sector_source_can_be_planned = True
        for code, (layer, label, implemented, available) in _MODULES.items():
            reason = None
            if (
                code in {"SECTOR_PULSE", "SECTOR_TRANSITION"}
                and sector_pair.get("status") != "AVAILABLE"
            ):
                reason = (
                    "PG当前没有已配对快照；在本次任务中选择行业/GN成员来源后，"
                    "可先生成同次快照再计算。"
                    if sector_source_can_be_planned
                    else sector_pair.get("reason_code") or "行业与概念快照对不可用。"
                )
            cleanup_allowed = _MODULES[code][0] != "SOURCE"
            modules.append(
                {
                    "code": code,
                    "label": label,
                    "layer": layer,
                    "implemented": implemented,
                    "available": available,
                    "reason": reason,
                    "cleanup_allowed": cleanup_allowed,
                    "default_selected": (
                        code in {"TDX_RAW", "TDX_SECTOR_MEMBERS"}
                        if layer == "SOURCE"
                        else True
                    ),
                    "default_output_session_count": 5,
                }
            )
        return {
            "status": "AVAILABLE",
            "execution_authorized": self.execution_authorized,
            "execution_reason": None
            if self.execution_authorized
            else "BLOCKED_NOT_AUTHORIZED",
            "research_cutoff": self.research_cutoff.isoformat(),
            "raw_coverage": coverage,
            "default_history_session_count": 200,
            "current_profile_history_session_count": coverage.get(
                "current_profile_history_session_count"
            ),
            "core_max_actual_lookback": CORE_MAX_ACTUAL_LOOKBACK,
            "modules": modules,
            "available_target_sessions": target_sessions,
            "sector_snapshot_pair": sector_pair,
            "latest_job": latest[0] if latest else None,
            "recent_jobs": data_job_pg.read_recent_jobs(self.config, limit=8),
            "source_request_performed": False,
            "database_write_attempted": False,
        }

    def plan(self, body: Any) -> dict[str, Any]:
        coverage = self._coverage()
        request = _request_fields(body)
        adjusted_action_sessions: tuple[date, ...] = ()
        adjusted_symbols_by_session: dict[str, tuple[str, ...]] = {}
        adjusted_input_status_by_session: dict[str, dict[str, str]] = {}
        adjusted_event_revisions_by_session: dict[str, list[dict[str, str]]] = {}
        adjusted_capture_by_session: dict[str, int] = {}
        adjusted_raw_identities_by_session: dict[str, dict[str, Any]] = {}
        if "ADJUSTED_PRICE" in request["selected_modules"]:
            latest_raw = coverage.get("last_stored_session")
            if request.get("target_session") is not None:
                adjusted_target = _parse_session(request["target_session"])
            elif request["mode"] == "INITIALIZE" and latest_raw:
                adjusted_target = _parse_session(latest_raw)
            else:
                _calendar_id, _calendar_revision, sessions = _load_official_sessions(
                    self.research_cutoff
                )
                valid_sessions = tuple(
                    session for session in sessions if session <= self.research_cutoff
                )
                adjusted_target = valid_sessions[-1] if valid_sessions else None

            if adjusted_target is not None and adjusted_target <= self.research_cutoff:
                _calendar_id, _calendar_revision, sessions = _load_official_sessions(
                    self.research_cutoff
                )
                if adjusted_target in sessions:
                    output_count = request["module_output_session_counts"].get(
                        "ADJUSTED_PRICE",
                        request["output_session_count"] or 5,
                    )
                    output_count = _positive_integer(
                        output_count, "ADJUSTED_PRICE输出会话数"
                    )
                    adjusted_output_sessions = recent_official_sessions(
                        sessions,
                        through=adjusted_target,
                        count=output_count,
                    )
                    from stock_a.storage.postgres.adjusted_prices import (
                        read_corporate_action_events,
                        read_verified_action_sessions,
                        read_verified_action_symbols_for_session,
                    )

                    adjusted_action_sessions = read_verified_action_sessions(
                        self.config, through_session=adjusted_target
                    )
                    action_session_set = set(adjusted_action_sessions)
                    for action_session in adjusted_output_sessions:
                        day_text = action_session.isoformat()
                        if action_session not in action_session_set:
                            adjusted_input_status_by_session[day_text] = {
                                "status": "NOT_COMPUTED",
                                "code": "ADJUSTMENT_ACTION_NOT_PROVEN",
                                "message": "该目标会话没有已验证的公司行为公告范围；不推断零调整。",
                            }
                            continue
                        symbols = read_verified_action_symbols_for_session(
                            self.config, target_session=action_session
                        )
                        if not symbols:
                            adjusted_input_status_by_session[day_text] = {
                                "status": "NOT_COMPUTED",
                                "code": "ADJUSTMENT_ACTION_SYMBOL_NOT_PROVEN",
                                "message": "目标公告没有可唯一绑定的已验证来源代码。",
                            }
                            continue
                        revisions: list[dict[str, str]] = []
                        invalid_reason = None
                        for symbol in symbols:
                            events = read_corporate_action_events(
                                self.config,
                                source_symbol=symbol,
                                through_session=action_session,
                            )
                            if (
                                len(events) != 1
                                or events[0].get("effective_session") != action_session
                                or events[0].get("source_quality")
                                != "VERIFIED_OFFICIAL_NOTICE"
                                or events[0].get("action_kind")
                                != "CAPITAL_RESERVE_TO_STOCK"
                            ):
                                invalid_reason = (
                                    "该来源代码存在多事件、非目标生效日或未验证事件；"
                                    "本公告能力不能闭合此目标窗口。"
                                )
                                break
                            revisions.append(
                                {
                                    "source_symbol": symbol,
                                    "event_ref": str(events[0]["event_ref"]),
                                    "semantic_revision": str(
                                        events[0]["semantic_revision"]
                                    ).strip(),
                                }
                            )
                        if invalid_reason:
                            adjusted_input_status_by_session[day_text] = {
                                "status": "INCOMPLETE",
                                "code": "ADJUSTMENT_EVENT_COVERAGE_INCOMPLETE",
                                "message": invalid_reason,
                            }
                            continue
                        adjusted_symbols_by_session[day_text] = tuple(symbols)
                        adjusted_event_revisions_by_session[day_text] = revisions

        sector_pair = {"status": "NOT_REQUESTED", "snapshot_ids": None}
        if {
            "SECTOR_PULSE",
            "SECTOR_TRANSITION",
            "SECTOR_FOLLOWUP",
        } & set(request["selected_modules"]):
            from stock_a.application.source_sector_pulse import (
                resolve_source_sector_snapshot_pair,
            )

            sector_pair = resolve_source_sector_snapshot_pair(self.config)
        plan = build_data_job_plan(
            body,
            cutoff=self.research_cutoff,
            raw_coverage=coverage,
            execution_authorized=self.execution_authorized,
            adjusted_action_sessions=adjusted_action_sessions,
            adjusted_action_symbols_by_session=adjusted_symbols_by_session,
        )
        if "ADJUSTED_PRICE" in request["selected_modules"]:
            plan["adjusted_price_input_status_by_session"] = (
                adjusted_input_status_by_session
            )
            plan["adjusted_price_event_revisions"] = adjusted_event_revisions_by_session
            plan["adjusted_price_capture_by_session"] = adjusted_capture_by_session
            plan["adjusted_price_raw_identities_by_session"] = (
                adjusted_raw_identities_by_session
            )
            if "TDX_RAW" not in request["source_modules"]:
                package_id = plan.get("reuse_raw_package_id")
                windows = plan.get("adjusted_price_source_windows", {})
                symbols_by_session = plan.get("adjusted_price_symbols_by_session", {})
                if type(package_id) is int:
                    from stock_a.storage.postgres.adjusted_prices import (
                        read_adjustment_raw_window,
                    )

                    for day_text, window_text in windows.items():
                        symbols = symbols_by_session.get(day_text, ())
                        expected_window = tuple(
                            date.fromisoformat(value) for value in window_text
                        )
                        capture_ids: set[int] = set()
                        capture_ids_by_source_session: dict[str, int] = {}
                        window_complete = len(expected_window) == 2
                        for symbol in symbols:
                            rows = read_adjustment_raw_window(
                                self.config,
                                source_symbol=symbol,
                                source_package_id=package_id,
                                target_session=expected_window[-1],
                                session_count=2,
                            )
                            if tuple(
                                row["session_date"] for row in rows
                            ) != expected_window or any(
                                row.get("source_package_id") != package_id
                                or row.get("price_quality") != "OBSERVED"
                                or row.get("bar_status") != "TARGET_PRESENT"
                                for row in rows
                            ):
                                window_complete = False
                                break
                            symbol_capture_ids = {
                                row["session_date"].isoformat(): int(
                                    row["source_capture_id"]
                                )
                                for row in rows
                            }
                            if len(symbol_capture_ids) != 2 or (
                                capture_ids_by_source_session
                                and symbol_capture_ids != capture_ids_by_source_session
                            ):
                                window_complete = False
                                break
                            capture_ids_by_source_session = symbol_capture_ids
                            target_rows = [
                                row
                                for row in rows
                                if row["session_date"] == expected_window[-1]
                            ]
                            capture_ids.update(
                                int(row["source_capture_id"]) for row in target_rows
                            )
                        if window_complete and len(capture_ids) == 1:
                            adjusted_capture_by_session[day_text] = next(
                                iter(capture_ids)
                            )
                            adjusted_raw_identities_by_session[day_text] = {
                                "source_package_id": package_id,
                                "source_capture_ids_by_session": (
                                    capture_ids_by_source_session
                                ),
                                "source_symbols": list(symbols),
                            }
                        else:
                            plan["adjusted_price_input_status_by_session"][day_text] = {
                                "status": "NOT_COMPUTED",
                                "code": "ADJUSTMENT_RAW_WINDOW_UNAVAILABLE",
                                "message": "同一来源包没有完整的前一官方会话与公告生效会话Raw窗口。",
                            }
                else:
                    for day_text in plan.get("adjusted_price_source_windows", {}):
                        plan["adjusted_price_input_status_by_session"][day_text] = {
                            "status": "NOT_COMPUTED",
                            "code": "ADJUSTMENT_RAW_PACKAGE_UNAVAILABLE",
                            "message": "没有绑定包含两会话输入窗口的已存Raw来源包；未启动来源采集。",
                        }
            for day_text in plan.get("module_output_sessions", {}).get(
                "ADJUSTED_PRICE", ()
            ):
                if (
                    day_text not in plan.get("adjusted_price_source_windows", {})
                    and day_text not in plan["adjusted_price_input_status_by_session"]
                ):
                    plan["adjusted_price_input_status_by_session"][day_text] = {
                        "status": "NOT_COMPUTED",
                        "code": "ADJUSTMENT_TWO_SESSION_WINDOW_UNAVAILABLE",
                        "message": "没有完整的两个截止内官方输入会话；不触发调整生产。",
                    }
        if "BAOSTOCK" in request["source_modules"]:
            bao_policy = self.config.get("source_policy", {}).get(
                "baostock_supplement", {}
            )
            plan["baostock_request_status"] = {
                "source_availability": "NOT_CHECKED_NO_NETWORK",
                "request_policy": bao_policy.get("request_policy", "UNKNOWN"),
                "execution_authorized": self.execution_authorized,
                "network_requests_performed": False,
            }
            if "TDX_RAW" not in request["source_modules"]:
                from stock_a.application.baostock_daily_jobs import (
                    _request_budget,
                    plan_baostock_capture_batches,
                )
                from stock_a.storage.postgres.data_jobs import (
                    read_raw_symbols_by_session,
                )

                package_id = plan.get("baostock_source_package_id")
                sessions = tuple(
                    date.fromisoformat(item)
                    for item in plan.get("baostock_output_sessions", ())
                )
                units = [
                    item
                    for item in plan.get("stage_units", ())
                    if item.get("stage_code") == "SOURCE_BAOSTOCK"
                ]
                blockers = plan.setdefault("blocked_reasons", [])

                def block_baostock(code: str, message: str) -> None:
                    blockers.append(code)
                    plan["executable"] = False
                    plan["status"] = "BLOCKED_INPUT_UNAVAILABLE"
                    for unit in units:
                        unit.update(
                            {
                                "status": "BLOCKED",
                                "error_code": code,
                                "error_message": message,
                            }
                        )
                    plan.setdefault("unplanned_producers", []).append(
                        {
                            "stage_code": "BAOSTOCK",
                            "status": "BLOCKED",
                            "reason_code": code,
                            "message": message,
                        }
                    )

                if type(package_id) is not int or not sessions:
                    block_baostock(
                        "BAOSTOCK_RAW_INPUT_UNAVAILABLE",
                        "目标日期没有唯一完整的PG TDX Raw来源包；未启动TDX重新采集。",
                    )
                else:
                    raw_readback = read_raw_symbols_by_session(
                        self.config,
                        target_sessions=sessions,
                        source_package_id=package_id,
                        cutoff=self.research_cutoff,
                    )
                    expected_counts = {
                        item["session_date"]: int(item["expected_row_count"])
                        for item in plan.get("baostock_raw_session_inputs", ())
                    }
                    actual_counts = raw_readback["symbol_counts_by_session"]
                    if raw_readback["missing_sessions"] or any(
                        actual_counts.get(day.isoformat())
                        != expected_counts.get(day.isoformat())
                        for day in sessions
                    ):
                        block_baostock(
                            "BAOSTOCK_RAW_READBACK_INCOMPLETE",
                            "独立PG读回的TDX代码分母与闭合Raw目录不一致；未发起来源请求。",
                        )
                    else:
                        max_logins, max_queries, _timeout = _request_budget(self.config)
                        budget_plan = plan_baostock_capture_batches(
                            sessions,
                            raw_readback["symbols_by_session"],
                            max_login_batches=max_logins,
                            max_queries_per_target_session=max_queries,
                        )
                        plan["baostock_request_budget"] = {
                            key: budget_plan[key]
                            for key in (
                                "status",
                                "target_session_count",
                                "symbol_session_units",
                                "required_login_batches",
                                "allowed_login_batches",
                                "max_queries_per_target_session",
                            )
                        }
                        if budget_plan["status"] != "READY":
                            block_baostock(
                                "BAOSTOCK_REQUEST_BUDGET_EXCEEDED",
                                "所需BaoStock登录批次超过本机预算；未抽样、未登录且未发送请求。",
                            )
                if bao_policy.get("request_policy") != "REQUESTED":
                    block_baostock(
                        "BAOSTOCK_REQUEST_NOT_AUTHORIZED",
                        "BaoStock来源策略为未请求；仅核对PG代码池和预算，不发送网络请求。",
                    )
        plan["sector_snapshot_pair"] = sector_pair
        plan["sector_snapshot_ids"] = sector_pair.get("snapshot_ids")
        completed = self._complete_plan(plan, request, coverage, sector_pair)
        if (
            request["mode"] == "INITIALIZE"
            and request["initialization_mode"] == "RESET_ALL_BUSINESS_FACTS"
            and isinstance(request.get("reset_preview_fingerprint"), str)
        ):
            for recent in data_job_pg.read_recent_jobs(self.config, limit=20):
                if (
                    recent.get("mode") != "INITIALIZE"
                    or recent.get("initialization_mode") != "RESET_ALL_BUSINESS_FACTS"
                    or recent.get("reset_preview_fingerprint")
                    != request["reset_preview_fingerprint"]
                    or recent.get("status")
                    not in {
                        "RUNNING",
                        "FAILED",
                        "INTERRUPTED",
                        "INCOMPLETE",
                        "BLOCKED",
                    }
                ):
                    continue
                if (
                    recent.get("target_session").isoformat()
                    != completed.get("target_session")
                    or recent.get("history_session_count")
                    != completed.get("history_session_count")
                    or recent.get("profile_history_session_count")
                    != completed.get("profile_history_session_count")
                    or recent.get("selected_modules")
                    != completed.get("selected_modules")
                    or recent.get("module_output_session_counts")
                    != [
                        completed["module_output_session_counts"][module]
                        for module in completed["selected_modules"]
                    ]
                ):
                    continue
                saved = data_job_pg.read_job(self.config, job_id=recent["job_id"])
                saved_reset_units = [
                    unit
                    for unit in (saved or {}).get("progress", ())
                    if unit.get("stage_code") == "BUSINESS_RESET"
                ]
                reset_already_cleared = bool(saved_reset_units) and all(
                    unit.get("status") == "CLEARED" for unit in saved_reset_units
                )
                if not (
                    reset_already_cleared
                    or recent.get("status") == "RUNNING"
                    or completed.get("reset_preview_fingerprint")
                    == request["reset_preview_fingerprint"]
                ):
                    continue
                completed["resume_existing_job"] = True
                completed["resume_request_key"] = recent["request_key"]
                completed["resume_job_id"] = str(recent["job_id"])
                completed["source_version_fingerprint"] = recent[
                    "source_version_fingerprint"
                ]
                completed["reset_preview_fingerprint"] = request[
                    "reset_preview_fingerprint"
                ]
                preview = completed.get("reset_preview")
                if isinstance(preview, dict):
                    preview["reset_preview_fingerprint"] = request[
                        "reset_preview_fingerprint"
                    ]
                    preview["resume_existing_reset"] = True
                    preview["resume_job_id"] = str(recent["job_id"])
                    preview["source_preflight_status"] = "REQUIRED_BEFORE_CONTINUE"
                    preview["source_preflight_message"] = (
                        "上次任务已提交清理事务；本次只会恢复尚未完成的来源/算法单元，不会再次清空业务事实。"
                    )
                    preview["irreversible_warning"] = (
                        "当前为既有任务恢复；不会再次执行全库清理。"
                    )
                completed["executable"] = self.execution_authorized
                completed["status"] = (
                    "RESET_RESUME_READY"
                    if self.execution_authorized
                    else "BLOCKED_NOT_AUTHORIZED"
                )
                if self.execution_authorized:
                    completed["blocked_reasons"] = [
                        reason
                        for reason in completed.get("blocked_reasons", ())
                        if reason != "DATA_EXECUTION_NOT_AUTHORIZED"
                    ]
                break
        return completed

    def _complete_plan(
        self,
        plan: dict[str, Any],
        request: Mapping[str, Any],
        coverage: Mapping[str, Any],
        sector_pair: Mapping[str, Any],
    ) -> dict[str, Any]:
        plan["selected_modules"] = request["selected_modules"]
        plan["source_modules"] = request["source_modules"]
        plan["base_modules"] = request["base_modules"]
        plan["research_modules"] = request["research_modules"]
        plan["initialization_mode"] = request["initialization_mode"]
        plan["daily_mode"] = request["daily_mode"]
        output_counts = dict(request["module_output_session_counts"])
        for module in request["selected_modules"]:
            if module in output_counts:
                continue
            if module == "TDX_RAW":
                output_counts[module] = max(
                    1, int(plan.get("history_session_count") or 1)
                )
            elif module == "BAOSTOCK":
                output_counts[module] = max(
                    1, len(plan.get("baostock_output_sessions", ()))
                )
            elif module == "TDX_SECTOR_MEMBERS":
                output_counts[module] = 1
            elif module == "D0":
                output_counts[module] = max(
                    1, int(plan.get("d0_output_session_count") or 1)
                )
            elif module == "D1":
                output_counts[module] = max(
                    1, int(plan.get("d1_output_session_count") or 1)
                )
            else:
                output_counts[module] = max(
                    1,
                    int(request.get("output_session_count") or 5),
                )
        plan["module_output_session_counts"] = output_counts
        plan["operation"] = request["operation"]
        version_rows = list(plan.get("raw_session_inputs", ()))
        source_identity = [
            {
                "session_date": item.get("session_date"),
                "source_package_id": item.get("source_package_id"),
                "source_capture_id": item.get("source_capture_id"),
            }
            for item in sorted(version_rows, key=lambda value: value["session_date"])
        ]
        if "BAOSTOCK" in request["selected_modules"]:
            source_identity.extend(
                {
                    "source": "TDX_RAW_REUSE_FOR_BAOSTOCK",
                    "session_date": item.get("session_date"),
                    "source_package_id": item.get("source_package_id"),
                    "source_capture_id": item.get("source_capture_id"),
                    "row_count": item.get("row_count"),
                    "expected_row_count": item.get("expected_row_count"),
                }
                for item in plan.get("baostock_raw_session_inputs", ())
            )
            bao_units = (
                coverage.get("completed_units_by_module", {}).get("BAOSTOCK", [])
                if isinstance(coverage.get("completed_units_by_module"), Mapping)
                else []
            )
            source_identity.extend(
                {
                    "source": "BAOSTOCK",
                    "target_session": item.get("target_session"),
                    "capture_id": item.get("baostock_capture_id"),
                    "verified_rows": item.get("verified_rows"),
                }
                for item in bao_units
                if item.get("target_session")
                in set(plan.get("baostock_output_sessions", ()))
            )
        if "ADJUSTED_PRICE" in request["selected_modules"]:
            source_identity.append(
                {
                    "source": "VERIFIED_CORPORATE_ACTION_EVENTS",
                    "sessions": plan.get("adjusted_price_event_revisions", {}),
                }
            )
            source_identity.append(
                {
                    "source": "ADJUSTED_PRICE_RAW_INPUTS",
                    "sessions": plan.get(
                        "adjusted_price_raw_identities_by_session", {}
                    ),
                }
            )
        request_identity = {
            "algorithm_revision": "STOCK_A_DATA_JOB_ALGORITHM_R3",
            "profile_semantics_contract": "STOCK-A-PRICE-PROFILE-SEMANTICS/R1",
            "source_versions": source_identity,
            "mode": request["mode"],
            "initialization_mode": request["initialization_mode"],
            "daily_mode": request["daily_mode"],
            "source_modules": request["source_modules"],
            "base_modules": request["base_modules"],
            "research_modules": request["research_modules"],
            "history_session_count": plan.get("history_session_count"),
            "profile_history_session_count": plan.get("profile_history_session_count"),
            "module_output_session_counts": output_counts,
            "d1_history_session_count": plan.get("d1_calculation_session_count"),
            "calendar_id": plan.get("calendar_id"),
            "calendar_revision": plan.get("calendar_revision"),
            "operation": request["operation"],
        }
        plan["source_version_fingerprint"] = hashlib.sha256(
            json.dumps(request_identity, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
        ).hexdigest()
        if "ADJUSTED_PRICE" in request["selected_modules"]:
            input_statuses = plan.get("adjusted_price_input_status_by_session", {})
            for day_text in plan.get("module_output_sessions", {}).get(
                "ADJUSTED_PRICE", ()
            ):
                input_status = input_statuses.get(day_text, {})
                status = input_status.get("status", "NOT_STARTED")
                _unit(
                    plan["stage_units"],
                    stage="ADJUSTED_PRICE",
                    target_session=date.fromisoformat(day_text),
                    business_table="stock.source_adjusted_daily_prices",
                    status=status,
                    error_code=input_status.get("code"),
                    error_message=input_status.get("message"),
                )
        if "SECTOR_PULSE" in request["selected_modules"]:
            sector_source_will_generate = (
                "TDX_SECTOR_MEMBERS" in request["source_modules"]
            )
            pulse_days = plan.get("sector_pulse_sessions", ())
            unavailable_days = set(plan.get("sector_pulse_insufficient_sessions", ()))
            for day in pulse_days:
                day_status = (
                    "NOT_COMPUTED"
                    if (
                        sector_pair.get("status") != "AVAILABLE"
                        and not sector_source_will_generate
                    )
                    or day in unavailable_days
                    else "NOT_STARTED"
                )
                _unit(
                    plan["stage_units"],
                    stage="SECTOR_PULSE",
                    target_session=date.fromisoformat(day),
                    business_table="stock.source_sector_cross_period_pulse_facts",
                    status=day_status,
                    error_code=(
                        "SECTOR_RAW_INPUT_UNAVAILABLE"
                        if day in unavailable_days
                        else sector_pair.get("reason_code")
                    )
                    if day_status == "NOT_COMPUTED"
                    else None,
                    error_message=(
                        "行业与GN概念来源快照对不存在；保留未计算。"
                        if (
                            sector_pair.get("status") != "AVAILABLE"
                            and not sector_source_will_generate
                        )
                        else "目标日Raw不在已存完整来源包中；保留未计算。"
                    )
                    if day_status == "NOT_COMPUTED"
                    else None,
                )
            plan["planned_unit_count"] = len(plan["stage_units"])
            if (
                sector_pair.get("status") != "AVAILABLE"
                and not sector_source_will_generate
            ):
                plan.setdefault("unplanned_producers", []).append(
                    {
                        "stage_code": "SECTOR_PULSE",
                        "status": "NOT_COMPUTED",
                        "reason_code": sector_pair.get("reason_code"),
                        "message": "行业与GN概念来源快照对不可用。",
                    }
                )
        if "SECTOR_FOLLOWUP" in request["selected_modules"]:
            due_by_session = plan.get("sector_followup_due_targets", {})
            insufficient = set(plan.get("sector_followup_insufficient_sessions", ()))
            for day in plan.get("sector_followup_sessions", ()):
                due = due_by_session.get(day, {})
                status = (
                    "NOT_COMPUTED"
                    if day in insufficient
                    else "NOT_STARTED"
                    if due
                    else "NOT_DUE"
                )
                for table in _STAGE_TABLES["SECTOR_FOLLOWUP"]:
                    _unit(
                        plan["stage_units"],
                        stage="SECTOR_FOLLOWUP",
                        target_session=date.fromisoformat(day),
                        business_table=table,
                        status=status,
                        error_code=(
                            "FOLLOWUP_RAW_INPUT_UNAVAILABLE"
                            if day in insufficient
                            else None
                            if due
                            else "FOLLOWUP_NOT_DUE"
                        ),
                        error_message=None
                        if due and day not in insufficient
                        else "后继目标Raw没有绑定到同一完整来源包；保留未计算。"
                        if day in insufficient
                        else "研究截止内尚无到期后继会话；不读取截止后的行情。",
                    )
        if "SECTOR_TRANSITION" in request["selected_modules"]:
            sector_source_will_generate = (
                "TDX_SECTOR_MEMBERS" in request["source_modules"]
            )
            unavailable = set(plan.get("sector_transition_insufficient_sessions", ()))
            for day in plan.get("sector_transition_sessions", ()):
                status = (
                    "NOT_COMPUTED"
                    if day in unavailable
                    or (
                        sector_pair.get("status") != "AVAILABLE"
                        and not sector_source_will_generate
                    )
                    else "NOT_STARTED"
                )
                _unit(
                    plan["stage_units"],
                    stage="SECTOR_TRANSITION",
                    target_session=date.fromisoformat(day),
                    business_table="stock.source_sector_pulse_adjacent_transitions",
                    status=status,
                    error_code=(
                        "SECTOR_TRANSITION_INPUT_UNAVAILABLE"
                        if status == "NOT_COMPUTED"
                        else None
                    ),
                    error_message=(
                        "精确前序Raw或行业/GN快照输入不可用；相邻变化保留未计算。"
                        if status == "NOT_COMPUTED"
                        else None
                    ),
                )
        if request["daily_mode"] == "FULL_THREE_LAYERS" or (
            request["mode"] == "INITIALIZE"
            and request["initialization_mode"] == "RESET_ALL_BUSINESS_FACTS"
        ):
            for table, label, message in _UNCOORDINATED_RESEARCH_CONSUMERS:
                plan.setdefault("unplanned_producers", []).append(
                    {
                        "stage_code": label,
                        "business_table": f"stock.{table}",
                        "status": "SKIPPED_NOT_IMPLEMENTED",
                        "reason_code": "PRODUCER_NOT_IN_JOB_COORDINATOR",
                        "message": message,
                    }
                )
        if (
            request["mode"] == "INITIALIZE"
            and request["initialization_mode"] == "RESET_ALL_BUSINESS_FACTS"
        ):
            target_sessions = tuple(plan.get("raw_history_sessions", ()))
            reset_preview = business_reset_pg.preview_business_fact_reset(
                self.config,
                target_sessions=target_sessions,
                source_modules=tuple(request["source_modules"]),
                history_session_count=int(plan["history_session_count"]),
                module_output_session_counts=output_counts,
            )
            reset_preview.update(
                {
                    "source_ready": False,
                    "sources_ready": False,
                    "source_preflight_status": "REQUIRED_BEFORE_DELETE",
                    "source_preflight_required_before_delete": True,
                    "source_preflight_message": "任务启动后将先验证官方Raw来源及行业/GN成员来源；任一必需来源不完整时保留现有事实，不执行清理。",
                    "irreversible_warning": "业务事实清理提交后不能直接撤销；只有删除前的全部来源与窗口预检通过才会清理。",
                }
            )
            plan["reset_preview"] = reset_preview
            plan["reset_preview_fingerprint"] = reset_preview[
                "reset_preview_fingerprint"
            ]
            reset_target = date.fromisoformat(plan["target_session"])
            for row in reset_preview["affected_tables"]:
                _unit(
                    plan["stage_units"],
                    stage="BUSINESS_RESET",
                    target_session=reset_target,
                    business_table=row["table"],
                )
            plan["source_version_fingerprint"] = hashlib.sha256(
                json.dumps(
                    {
                        "request_fingerprint": plan["source_version_fingerprint"],
                        "reset_preview_fingerprint": plan["reset_preview_fingerprint"],
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
            if not self.execution_authorized:
                plan["executable"] = False
                plan["status"] = "BLOCKED_NOT_AUTHORIZED"
                plan["blocked_reasons"] = list(
                    dict.fromkeys(
                        [
                            *plan.get("blocked_reasons", ()),
                            "DATA_EXECUTION_NOT_AUTHORIZED",
                        ]
                    )
                )
        if request["operation"] in _CLEANUP_OPERATIONS:
            cleanup_module = next(
                iter(request["base_modules"] + request["research_modules"])
            )
            cleanup_days = plan.get("module_output_sessions", {}).get(
                cleanup_module, []
            )
            plan["cleanup_preview"] = data_job_pg.preview_derived_cleanup(
                self.config,
                module_code=cleanup_module,
                target_sessions=tuple(date.fromisoformat(day) for day in cleanup_days),
                cutoff=self.research_cutoff,
            )
            plan["cleanup_preview"]["module_label"] = _MODULES[cleanup_module][1]
            cleanup_identity = {
                "input_source_fingerprint": plan["source_version_fingerprint"],
                "cleanup_preview_key": plan["cleanup_preview"].get("preview_key"),
            }
            plan["source_version_fingerprint"] = hashlib.sha256(
                json.dumps(
                    cleanup_identity,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
            if plan["cleanup_preview"].get("complete") is not True:
                plan["executable"] = False
                plan["status"] = "CLEANUP_SCOPE_UNAVAILABLE"
            elif not request["source_modules"]:
                plan["executable"] = True
                plan["status"] = (
                    "READY_WITH_INCOMPLETE_UNITS"
                    if any(
                        unit.get("status") == "NOT_COMPUTED"
                        for unit in plan["stage_units"]
                    )
                    else "READY"
                )
                plan["blocked_reasons"] = [
                    reason
                    for reason in plan.get("blocked_reasons", [])
                    if reason != "DATA_EXECUTION_NOT_AUTHORIZED"
                ]
        if request["operation"] == "CLEAR_DERIVED" and not request["source_modules"]:
            plan["executable"] = plan.get("cleanup_preview", {}).get("complete") is True
        plan["planned_unit_count"] = len(plan["stage_units"])
        plan["source_io_attempted"] = False
        plan["database_write_attempted"] = False
        return plan

    def read_job(self, job_id: UUID | None = None) -> dict[str, Any] | None:
        return data_job_pg.read_job(self.config, job_id=job_id)

    def recent_jobs(self) -> list[dict[str, Any]]:
        return data_job_pg.read_recent_jobs(self.config, limit=8)

    def start(self, body: Any) -> dict[str, Any]:
        plan = self.plan(body)
        request = _request_fields(body)
        if (
            request["mode"] == "INITIALIZE"
            and request["initialization_mode"] == "RESET_ALL_BUSINESS_FACTS"
        ):
            preview = plan.get("reset_preview")
            if request["reset_confirmed"] is not True:
                raise DataJobError(
                    "RESET_CONFIRMATION_REQUIRED",
                    "清理本项目全部业务事实前需要在菜单再次确认。",
                    status=409,
                )
            if not isinstance(preview, Mapping) or request[
                "reset_preview_fingerprint"
            ] != preview.get("reset_preview_fingerprint"):
                raise DataJobError(
                    "RESET_PREVIEW_STALE",
                    "重置影响范围自预览后发生变化；未创建任务，也未清理数据。请重新读取预览。",
                    status=409,
                )
        if not plan.get("executable"):
            return {"accepted": False, "plan": plan}
        request_key = plan.get("resume_request_key") or _job_request_key(plan)
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                latest = data_job_pg.read_job(self.config)
                if latest is not None and latest.get("request_key") == request_key:
                    return {
                        "accepted": True,
                        "status": "RUNNING",
                        "job": latest,
                        "reused": True,
                    }
                raise DataJobError(
                    "DATA_JOB_BUSY",
                    "已有不同模块、来源版本或操作范围的任务运行中。",
                    status=409,
                )
            task_id = uuid4()
            job = {
                "job_id": task_id,
                "request_key": request_key,
                "mode": plan["mode"],
                "target_session": date.fromisoformat(plan["target_session"]),
                "research_cutoff": self.research_cutoff,
                "history_session_count": plan["history_session_count"],
                "profile_history_session_count": plan.get(
                    "profile_history_session_count"
                ),
                "d0_output_session_count": plan["d0_output_session_count"],
                "d1_output_session_count": plan["d1_output_session_count"],
                "operation": plan["operation"],
                "initialization_mode": plan["initialization_mode"],
                "daily_mode": plan["daily_mode"],
                "reset_preview_fingerprint": plan.get("reset_preview_fingerprint"),
                "selected_modules": plan["selected_modules"],
                "module_output_session_counts": [
                    plan["module_output_session_counts"][module]
                    for module in plan["selected_modules"]
                ],
                "source_version_fingerprint": plan["source_version_fingerprint"],
                "calendar_id": plan["calendar_id"],
                "calendar_revision": plan["calendar_revision"],
                "planned_units": plan["planned_unit_count"],
            }
            result = data_job_pg.create_or_resume_job(
                self.config, job=job, progress=plan["stage_units"]
            )
            if result["status"] == "ALREADY_COMPLETE":
                return {"accepted": True, **result}
            if result.get("active") and result["status"] == "RUNNING":
                return {
                    "accepted": True,
                    **result,
                    "job": data_job_pg.read_job(self.config, job_id=result["job_id"]),
                }
            self._thread = threading.Thread(
                target=self._execute_job,
                args=(result["job_id"], plan),
                name="stock-a-data-job",
                daemon=True,
            )
            self._thread.start()
        return {
            "accepted": True,
            "status": "RUNNING",
            "job_id": result["job_id"],
            "reused": result.get("reused", False),
        }

    def _execute_job(self, job_id: UUID, plan: Mapping[str, Any]) -> None:
        prepared_package = None
        prepared_package_owner = None
        reset_input_bound = False
        job_finished = False
        try:
            execution_plan = dict(plan)
            if (
                plan.get("mode") == "INITIALIZE"
                and plan.get("initialization_mode") == "RESET_ALL_BUSINESS_FACTS"
            ):
                progress_job = data_job_pg.read_job(self.config, job_id=job_id)
                reset_progress = [
                    item
                    for item in (progress_job or {}).get("progress", ())
                    if item.get("stage_code") == "BUSINESS_RESET"
                ]
                reset_already_completed = bool(reset_progress) and all(
                    item.get("status") == "CLEARED" for item in reset_progress
                )
                expected_input_fingerprint = (
                    data_job_pg.read_reset_preflight_fingerprint(
                        self.config, job_id=job_id
                    )
                )
                prepared = _prepare_reset_source_inputs(
                    self.config,
                    plan,
                    job_id=job_id,
                    expected_preflight_fingerprint=expected_input_fingerprint,
                )
                prepared_package = prepared["vipdata_package"]
                package_owner = prepared["package_owner"]
                prepared_package_owner = package_owner
                data_job_pg.bind_reset_preflight_fingerprint(
                    self.config,
                    job_id=job_id,
                    fingerprint=prepared["preflight_fingerprint"],
                )
                reset_input_bound = True
                execution_plan["_prepared_vipdata_package"] = prepared_package
                execution_plan["_prepared_sector_snapshots"] = prepared[
                    "sector_snapshots"
                ]
                from stock_a.sources.tdx.vipdata import vipdata_package_entry_sha256

                if (
                    vipdata_package_entry_sha256(prepared_package)
                    != prepared["package_sha256"]
                ):
                    raise DataJobError(
                        "RESET_SOURCE_PACKAGE_CHANGED",
                        "Raw包在预检完成后发生变化；未开始清理。",
                        status=409,
                    )
                if not reset_already_completed:
                    reset_units = [
                        unit
                        for unit in plan.get("stage_units", ())
                        if unit.get("stage_code") == "BUSINESS_RESET"
                    ]
                    begin_callback = self._begin_units
                    begin_callback(job_id=job_id, units=reset_units)
                    preview = plan.get("reset_preview")
                    if not isinstance(preview, Mapping):
                        raise DataJobError(
                            "RESET_PREVIEW_REQUIRED",
                            "清理前缺少完整业务事实影响预览。",
                            status=409,
                        )
                    expected_counts = {
                        str(item["table"]): int(item["row_count"])
                        for item in preview.get("affected_tables", ())
                    }
                    reset_result = business_reset_pg.clear_business_facts(
                        self.config,
                        job_id=job_id,
                        expected_preview_fingerprint=str(
                            preview.get("reset_preview_fingerprint") or ""
                        ),
                        expected_table_counts=expected_counts,
                        target_sessions=tuple(preview.get("target_sessions", ())),
                        source_modules=tuple(preview.get("source_modules", ())),
                        history_session_count=int(
                            preview.get("history_session_count") or 0
                        ),
                        module_output_session_counts=preview.get(
                            "module_output_session_counts", {}
                        ),
                    )
                    deleted_rows = reset_result.get("deleted_rows_by_table", {})
                    for unit in reset_units:
                        data_job_pg.mark_business_reset_progress(
                            self.config,
                            job_id=job_id,
                            target_session=unit["target_session"],
                            business_table=unit["business_table"],
                            deleted_rows=int(
                                deleted_rows.get(unit["business_table"], 0)
                            ),
                        )
            if plan.get("operation") in _CLEANUP_OPERATIONS:
                cleanup_package = plan.get("reuse_raw_package_id")
                if cleanup_package is not None:
                    data_job_pg.set_job_source_package(
                        self.config,
                        job_id=job_id,
                        source_package_id=int(cleanup_package),
                    )
                _execute_derived_cleanup(
                    config=self.config,
                    job_id=job_id,
                    plan=plan,
                    begin_callback=self._begin_units,
                )
                if plan.get("operation") == "CLEAR_DERIVED":
                    data_job_pg.finish_job(self.config, job_id=job_id)
                    job_finished = True
                    return
            _execute_shared_production_chain(
                config=self.config,
                job_id=job_id,
                plan=execution_plan,
                progress_callback=self._record_verified_unit,
                begin_callback=self._begin_units,
            )
            data_job_pg.finish_job(self.config, job_id=job_id)
            job_finished = True
        except Exception as error:  # noqa: BLE001 - 后台任务必须把任意阶段异常持久化为失败状态。
            error_code = getattr(error, "code", type(error).__name__)
            error_message = str(error)[:1000]
            status = (
                "BLOCKED"
                if str(error_code).startswith(("BLOCKED_", "NOT_AUTHORIZED"))
                else "FAILED"
            )
            data_job_pg.fail_job(
                self.config,
                job_id=job_id,
                status=status,
                error_code=error_code,
                error_message=error_message,
            )
        finally:
            if prepared_package_owner is not None and (
                job_finished or not reset_input_bound
            ):
                from stock_a.sources.tdx.vipdata import cleanup_vipdata_package

                cleanup_vipdata_package(prepared_package_owner)

    def _begin_units(self, *, job_id: UUID, units: Sequence[Mapping[str, Any]]) -> None:
        data_job_pg.mark_progress_running_batch(self.config, job_id=job_id, units=units)

    def _record_verified_unit(
        self,
        *,
        job_id: UUID,
        stage_code: str,
        target_session: date,
        business_table: str,
        source_package_id: int | None,
        source_capture_id: int | None,
        profile_history_session_count: int | None,
        d1_history_session_count: int | None,
        expected_rows: int | None,
        inserted_rows: int,
        quality_status: str | None,
        baostock_capture_id: UUID | None = None,
        no_change: bool = False,
        profile_revision: str | None = None,
        producer_version: str | Sequence[str] | None = None,
        calendar_revision: str | None = None,
        sector_snapshot_ids: Mapping[str, int] | None = None,
        followup_target_sessions: Sequence[date] | None = None,
    ) -> None:
        running = data_job_pg.mark_progress_running(
            self.config,
            job_id=job_id,
            stage_code=stage_code,
            target_session=target_session,
            business_table=business_table,
        )
        actual_rows = data_job_pg.read_business_table_count(
            self.config,
            business_table=business_table,
            target_session=target_session,
            source_package_id=source_package_id,
            source_capture_id=source_capture_id,
            baostock_capture_id=baostock_capture_id,
            profile_history_session_count=profile_history_session_count,
            d1_history_session_count=d1_history_session_count,
            profile_revision=profile_revision,
            producer_version=producer_version,
            calendar_revision=calendar_revision,
            sector_snapshot_ids=sector_snapshot_ids,
            followup_target_sessions=followup_target_sessions,
            research_cutoff=self.research_cutoff,
        )
        stored_producer_version = (
            "|".join(str(value) for value in producer_version)
            if isinstance(producer_version, Sequence)
            and not isinstance(producer_version, (str, bytes))
            else producer_version
        )
        if not running:
            previous = data_job_pg.read_progress_unit(
                self.config,
                job_id=job_id,
                stage_code=stage_code,
                target_session=target_session,
                business_table=business_table,
            )
            expected_identity = {
                "source_package_id": source_package_id,
                "source_capture_id": source_capture_id,
                "baostock_capture_id": baostock_capture_id,
                "profile_revision": profile_revision,
                "producer_version": stored_producer_version,
                "profile_history_session_count": profile_history_session_count,
                "d1_history_session_count": d1_history_session_count,
                "calendar_revision": calendar_revision,
                "industry_snapshot_id": (
                    sector_snapshot_ids.get("INDUSTRY")
                    if isinstance(sector_snapshot_ids, Mapping)
                    else None
                ),
                "concept_snapshot_id": (
                    sector_snapshot_ids.get("CONCEPT")
                    if isinstance(sector_snapshot_ids, Mapping)
                    else None
                ),
            }
            identity_matches = previous is not None and all(
                previous.get(key) == value for key, value in expected_identity.items()
            )
            if (
                previous is None
                or previous.get("verified_rows") != actual_rows
                or not identity_matches
            ):
                raise DataJobError(
                    "DATA_JOB_READBACK_IDENTITY_CHANGED",
                    "已核验进度对应的PG来源、算法、窗口或板块快照身份发生变化；任务需要重新计算。",
                    status=409,
                )
            return
        data_job_pg.mark_progress_verified(
            self.config,
            job_id=job_id,
            stage_code=stage_code,
            target_session=target_session,
            business_table=business_table,
            verified_rows=actual_rows,
            inserted_rows=inserted_rows,
            updated_rows=0,
            expected_rows=expected_rows,
            quality_status=quality_status,
            source_package_id=source_package_id,
            source_capture_id=source_capture_id,
            baostock_capture_id=baostock_capture_id,
            profile_revision=profile_revision,
            producer_version=stored_producer_version,
            profile_history_session_count=profile_history_session_count,
            d1_history_session_count=d1_history_session_count,
            calendar_revision=calendar_revision,
            industry_snapshot_id=(
                sector_snapshot_ids.get("INDUSTRY")
                if isinstance(sector_snapshot_ids, Mapping)
                else None
            ),
            concept_snapshot_id=(
                sector_snapshot_ids.get("CONCEPT")
                if isinstance(sector_snapshot_ids, Mapping)
                else None
            ),
            no_change=no_change,
        )


def _prepare_reset_source_inputs(
    config: Mapping[str, Any],
    plan: Mapping[str, Any],
    *,
    job_id: UUID,
    expected_preflight_fingerprint: str | None = None,
) -> dict[str, Any]:
    """清理前逐会话验证Raw并绑定可恢复的任务级输入。"""
    from dataclasses import replace
    from datetime import datetime
    from pathlib import Path
    from zoneinfo import ZoneInfo

    from stock_a.sources.tdx.sector_members import (
        parse_sector_source_inputs,
        read_sector_source_inputs,
    )
    from stock_a.sources.tdx.vipdata import (
        VipdataBudgets,
        VipdataPackage,
        VipdataSourceConfig,
        VipdataTargetScanBudget,
        cleanup_vipdata_package,
        download_vipdata_package,
        inspect_vipdata_page,
        verify_existing_vipdata_package,
        vipdata_package_entry_sha256,
    )
    from stock_a.storage.postgres import source_raw
    from stock_a.storage.postgres.tdx_sector_members import _validate_limits

    sessions = tuple(
        date.fromisoformat(value) for value in plan.get("raw_history_sessions", ())
    )
    cutoff = date.fromisoformat(plan["research_cutoff"])
    if (
        not isinstance(job_id, UUID)
        or not sessions
        or tuple(sorted(set(sessions))) != sessions
        or sessions[-1] > cutoff
        or any(session > cutoff for session in sessions)
    ):
        raise DataJobError(
            "RESET_RAW_WINDOW_INVALID",
            "清理预检要求完整、严格递增且不晚于研究截止的Raw窗口。",
            status=409,
        )
    _calendar_id, _calendar_revision, official_sessions = _load_official_sessions(
        cutoff
    )
    if any(session not in official_sessions for session in sessions):
        raise DataJobError(
            "RESET_RAW_WINDOW_NOT_OFFICIAL",
            "清理预检窗口包含不在现行官方日历中的会话；现有事实保持不变。",
            status=409,
        )

    temporary_parent = Path("G:/codex_tmp/stock_a_vipdata").resolve()
    temporary_parent.mkdir(parents=True, exist_ok=True)
    job_root = temporary_parent / f"reset-job-{job_id.hex}"
    if job_root.parent != temporary_parent:
        raise DataJobError(
            "RESET_TEMP_ROOT_REJECTED", "重置来源临时目录越出G盘授权根。", status=409
        )
    source_config = VipdataSourceConfig.from_mapping(
        config["source_policy"]["vipdata_official"]
    )
    budgets = VipdataBudgets()
    package_owner = None
    verified_package = None
    package_downloaded = False
    try:
        cached_packages = tuple(job_root.glob("*.zip")) if job_root.exists() else ()
        if expected_preflight_fingerprint is not None and len(cached_packages) != 1:
            raise DataJobError(
                "RESET_SOURCE_CACHE_MISSING",
                "重置任务已经清理过业务事实，但原始已核验包缓存缺失；拒绝使用新来源续跑。",
                status=409,
            )
        if cached_packages:
            if len(cached_packages) != 1:
                raise DataJobError(
                    "RESET_SOURCE_CACHE_AMBIGUOUS",
                    "任务级Raw缓存不唯一；未清理或继续写入。",
                    status=409,
                )
            verified_package = verify_existing_vipdata_package(
                cached_packages[0], budgets=budgets
            )
            if verified_package.file_path.resolve().parent != job_root.resolve():
                raise DataJobError(
                    "RESET_SOURCE_CACHE_PATH_INVALID",
                    "重置任务缓存文件越出其G盘任务目录。",
                    status=409,
                )
            package_sha256 = vipdata_package_entry_sha256(verified_package)
            package_owner = VipdataPackage(
                file_path=verified_package.file_path,
                file_name=verified_package.file_name,
                file_size=verified_package.file_size,
                sha256=package_sha256,
                zip_entry_count=verified_package.zip_entry_count,
                zip_crc=verified_package.zip_crc,
                total_uncompressed_bytes=verified_package.total_uncompressed_bytes,
                entry_count_by_market=verified_package.entry_count_by_market,
                day_entries=verified_package.day_entries,
                download_final_url=source_config.package_url,
                request_started_at=verified_package.verified_at,
                download_finished_at=verified_package.verified_at,
                temporary_root=job_root,
            )
        else:
            if job_root.exists():
                if any(job_root.iterdir()):
                    raise DataJobError(
                        "RESET_SOURCE_CACHE_DIRTY",
                        "重置任务缓存目录包含未登记文件；拒绝覆盖。",
                        status=409,
                    )
                job_root.rmdir()
            if expected_preflight_fingerprint is not None:
                raise DataJobError(
                    "RESET_SOURCE_CACHE_MISSING",
                    "重置任务缺少原始输入缓存；拒绝重新下载不同来源续跑。",
                    status=409,
                )
            try:
                observation = inspect_vipdata_page(
                    sessions[-1].isoformat(),
                    budgets=budgets,
                    source_config=source_config,
                )
            except Exception as error:
                raise DataJobError(
                    "RESET_OFFICIAL_SOURCE_PREFLIGHT_FAILED",
                    "官方Raw来源在清理前不可验证；现有业务事实保持不变。",
                    status=503,
                ) from error
            if observation.status != "PACKAGE_CANDIDATE_READY":
                raise DataJobError(
                    "RESET_OFFICIAL_SOURCE_NOT_READY",
                    observation.reason or "官方Raw来源当前不可验证；现有事实保持不变。",
                    status=503,
                )
            update_at = getattr(observation, "official_update_at", None)
            if update_at is None or update_at.date() > cutoff:
                raise DataJobError(
                    "BLOCKED_SOURCE_AFTER_CUTOFF"
                    if update_at is not None
                    else "RESET_SOURCE_CLOCK_UNKNOWN",
                    "官方来源时钟晚于研究截止或不可验证；未下载行情包，也未清理事实。",
                    status=409,
                )
            job_root.mkdir()
            package_owner = replace(
                download_vipdata_package(
                    observation,
                    destination_root=job_root,
                    budgets=budgets,
                ),
                temporary_root=job_root,
            )
            package_downloaded = True
            verified_package = verify_existing_vipdata_package(
                package_owner.file_path, budgets=budgets
            )
            package_sha256 = vipdata_package_entry_sha256(verified_package)

        source_raw._validate_source_input(
            verified_package, sessions[-1], task_scope=_RAW_JOB_SCOPE
        )
        raw_preflight = source_raw.preflight_verified_vipdata_history(
            config,
            verified_package,
            sessions,
            task_scope=_RAW_JOB_SCOPE,
            scan_budget=VipdataTargetScanBudget(),
            max_raw_rows=2_000_000,
        )
        if raw_preflight["package_sha256"] != package_sha256:
            raise DataJobError(
                "RESET_SOURCE_PACKAGE_CHANGED",
                "Raw包在逐会话预检期间发生变化；现有事实保持不变。",
                status=409,
            )

        roots = config.get("source_policy", {}).get("configured_tdx_source_roots")
        if (
            config.get("source_policy", {}).get("all_source_roots_read_only")
            is not True
            or not isinstance(roots, list)
            or len(roots) != 1
            or not isinstance(roots[0], str)
        ):
            raise DataJobError(
                "RESET_SECTOR_SOURCE_NOT_BOUND",
                "清理前没有唯一只读行业/GN来源根；现有事实保持不变。",
                status=409,
            )
        inputs = read_sector_source_inputs(roots[0])
        snapshots = parse_sector_source_inputs(
            inputs, observed_at=datetime.now(ZoneInfo("Asia/Shanghai"))
        )
        if {item.source_kind for item in snapshots} != {
            "TDX_TDXHY_CFG",
            "TDX_INFOHARBOR_BLOCK_DAT",
        }:
            raise DataJobError(
                "RESET_SECTOR_SOURCE_INCOMPLETE",
                "清理前无法同时验证行业与GN概念成员来源；现有事实保持不变。",
                status=409,
            )
        _validate_limits(snapshots)
        sector_file_fingerprint = {
            relative: {
                "size_bytes": evidence.size_bytes,
                "sha256": hashlib.sha256(evidence.raw).hexdigest(),
            }
            for relative, evidence in sorted(inputs.files.items())
        }
        fingerprint_material = {
            "contract": "STOCK_A_RESET_INPUT_PREFLIGHT_R1",
            "target_sessions": [session.isoformat() for session in sessions],
            "raw_package_sha256": package_sha256,
            "raw_candidate_total": raw_preflight["candidate_total"],
            "sector_parse_contract": snapshots[0].parse_contract_version,
            "sector_sources": sector_file_fingerprint,
        }
        preflight_fingerprint = hashlib.sha256(
            json.dumps(
                fingerprint_material, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
        ).hexdigest()
        if (
            expected_preflight_fingerprint is not None
            and preflight_fingerprint != expected_preflight_fingerprint
        ):
            raise DataJobError(
                "RESET_INPUT_IDENTITY_CHANGED",
                "重置恢复时Raw包、候选范围或行业/GN来源与已核验输入不一致；拒绝混用。",
                status=409,
            )
        return {
            "vipdata_package": verified_package,
            "package_owner": package_owner,
            "package_sha256": package_sha256,
            "candidate_count": raw_preflight["candidate_total"],
            "planned_raw_rows": raw_preflight["planned_raw_rows"],
            "raw_preflight": raw_preflight,
            "sector_snapshots": tuple(snapshots),
            "sector_source_root": inputs.root,
            "sector_source_fingerprint": sector_file_fingerprint,
            "preflight_fingerprint": preflight_fingerprint,
            "preflight_status": "COMPLETE_BEFORE_DELETE",
            "source_request_performed": package_downloaded,
        }
    except Exception:
        if package_owner is not None and expected_preflight_fingerprint is None:
            try:
                cleanup_vipdata_package(package_owner)
            except Exception:
                pass
        raise


def _execute_derived_cleanup(
    *,
    config: Mapping[str, Any],
    job_id: UUID,
    plan: Mapping[str, Any],
    begin_callback: Any,
) -> dict[str, Any]:
    module = next(iter(plan["base_modules"] + plan["research_modules"]))
    targets = tuple(
        date.fromisoformat(item)
        for item in plan.get("module_output_sessions", {}).get(module, ())
    )
    preview = plan.get("cleanup_preview")
    if not isinstance(preview, Mapping) or preview.get("complete") is not True:
        raise DataJobError(
            "DATA_JOB_CLEANUP_PREVIEW_REQUIRED",
            "精确清理预检缺失或不完整；未执行清理。",
            status=409,
        )
    affected_tables = set(preview.get("affected_tables", ()))
    target_set = set(targets)
    units = [
        unit
        for unit in plan["stage_units"]
        if unit["target_session"] in target_set
        and unit["business_table"] in affected_tables
    ]
    if (
        not targets
        or not affected_tables
        or len(units) != len(targets) * len(affected_tables)
    ):
        raise DataJobError(
            "DATA_JOB_CLEANUP_PLAN_MISMATCH",
            "清理预览与当前任务进度单元不一致；未执行清理。",
            status=409,
        )
    begin_callback(job_id=job_id, units=units)
    result = data_job_pg.clear_derived_module(
        config,
        module,
        targets,
        date.fromisoformat(plan["research_cutoff"]),
        expected_preview_key=preview.get("preview_key"),
        current_job_id=job_id,
    )
    deleted = result.get("deleted_rows_by_session", {})
    for unit in units:
        session = unit["target_session"].isoformat()
        table = unit["business_table"]
        deleted_rows = int(deleted.get(session, {}).get(table, 0))
        data_job_pg.mark_progress_cleared(
            config,
            job_id=job_id,
            stage_code=unit["stage_code"],
            target_session=unit["target_session"],
            business_table=table,
            deleted_rows=deleted_rows,
        )
    return result


def _persist_sector_membership_stage(
    *,
    config: Mapping[str, Any],
    job_id: UUID,
    plan: Mapping[str, Any],
    progress_callback: Any,
    begin_callback: Any,
    prepared_snapshots: Sequence[Any] | None = None,
) -> dict[str, Any] | None:
    if "TDX_SECTOR_MEMBERS" not in set(plan.get("source_modules", ())):
        return None
    units = [
        unit
        for unit in plan.get("stage_units", ())
        if unit.get("stage_code") == "SOURCE_SECTOR_MEMBERS"
    ]
    if not units:
        return None
    begin_callback(job_id=job_id, units=units)
    from stock_a.application.tdx_sector_members import ingest_tdx_sector_members
    from stock_a.storage.postgres.tdx_sector_members import (
        persist_sector_source_snapshots,
    )

    if prepared_snapshots is None:
        result = ingest_tdx_sector_members(dict(config))
    else:
        persisted = persist_sector_source_snapshots(dict(config), prepared_snapshots)
        result = {
            **persisted,
            "source_root": "PREVALIDATED_READ_ONLY_TDX_ROOT",
            "source_modified": False,
            "source_downloaded": False,
        }
    snapshots = result.get("snapshots", ())
    snapshot_ids = {
        "INDUSTRY": next(
            (
                int(item["snapshot_id"])
                for item in snapshots
                if item.get("source_kind") == "TDX_TDXHY_CFG"
            ),
            None,
        ),
        "CONCEPT": next(
            (
                int(item["snapshot_id"])
                for item in snapshots
                if item.get("source_kind") == "TDX_INFOHARBOR_BLOCK_DAT"
            ),
            None,
        ),
    }
    if any(value is None for value in snapshot_ids.values()):
        raise DataJobError(
            "SECTOR_SOURCE_SNAPSHOT_PAIR_INCOMPLETE",
            "行业与GN概念成员源快照没有在同一生产批次中闭合。",
            status=409,
        )
    if isinstance(plan, dict):
        old_sector_reason = plan.get("sector_snapshot_pair", {}).get("reason_code")
        plan["sector_snapshot_ids"] = snapshot_ids
        plan["sector_snapshot_pair"] = {
            "status": "AVAILABLE",
            "snapshot_ids": snapshot_ids,
            "source_kinds": {
                "INDUSTRY": "TDX_TDXHY_CFG",
                "CONCEPT": "TDX_INFOHARBOR_BLOCK_DAT",
            },
            "research_mode": "NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT",
        }
        for unit in plan.get("stage_units", ()):
            if (
                unit.get("stage_code") in {"SECTOR_PULSE", "SECTOR_TRANSITION"}
                and unit.get("status") == "NOT_COMPUTED"
                and (
                    unit.get("error_code") == old_sector_reason
                    or str(unit.get("error_code") or "").startswith("SECTOR_SNAPSHOT")
                )
            ):
                unit.update(
                    {"status": "NOT_STARTED", "error_code": None, "error_message": None}
                )
        plan["unplanned_producers"] = [
            item
            for item in plan.get("unplanned_producers", ())
            if item.get("stage_code") != "SECTOR_PULSE"
        ]
    readback = result.get("independent_readback", {})
    if not isinstance(readback, Mapping):
        raise DataJobError(
            "SECTOR_SOURCE_READBACK_UNAVAILABLE",
            "行业与GN概念成员源缺少独立PG读回。",
            status=409,
        )
    catalog_rows = 0
    member_rows = 0
    for snapshot_id in snapshot_ids.values():
        counts = readback.get(str(snapshot_id), {})
        if not isinstance(counts, Mapping):
            raise DataJobError(
                "SECTOR_SOURCE_READBACK_INCOMPLETE",
                "行业或GN概念成员源缺少精确快照读回计数。",
                status=409,
            )
        catalog_rows += int(counts.get("catalog_rows", 0))
        member_rows += int(counts.get("member_rows", 0))
    table_counts = {
        "stock.tdx_sector_source_snapshots": len(snapshot_ids),
        "stock.tdx_sector_catalog": catalog_rows,
        "stock.tdx_sector_members": member_rows,
    }
    persistence = result
    for unit in units:
        table = unit["business_table"]
        progress_callback(
            job_id=job_id,
            stage_code="SOURCE_SECTOR_MEMBERS",
            target_session=unit["target_session"],
            business_table=table,
            source_package_id=None,
            source_capture_id=None,
            profile_history_session_count=None,
            d1_history_session_count=None,
            expected_rows=table_counts[table],
            inserted_rows=int(
                persistence.get(
                    {
                        "stock.tdx_sector_source_snapshots": "inserted_snapshot_rows",
                        "stock.tdx_sector_catalog": "inserted_catalog_rows",
                        "stock.tdx_sector_members": "inserted_member_rows",
                    }[table],
                    0,
                )
            ),
            quality_status="COMPLETE_NON_PIT_CURRENT_MEMBERSHIP",
            no_change=persistence.get("status") == "IDEMPOTENT_READBACK",
            producer_version="STOCK_A_TDX_SECTOR_SOURCE_MEMBERS_R1",
            sector_snapshot_ids=snapshot_ids,
        )
    return {**result, "sector_snapshot_ids": snapshot_ids}


def _execute_baostock_source_stage(
    *,
    config: Mapping[str, Any],
    job_id: UUID,
    plan: Mapping[str, Any],
    source_package_id: int,
    official_sessions: Sequence[date],
    progress_callback: Any,
    begin_callback: Any,
) -> None:
    if "BAOSTOCK" not in set(plan.get("source_modules", ())):
        return

    from stock_a.application.baostock_daily_jobs import (
        _request_budget,
        plan_baostock_capture_batches,
        run_manual_baostock_daily_capture,
    )
    from stock_a.sources.baostock.evidence import _UNITS as documented_units
    from stock_a.storage.postgres.data_jobs import read_raw_symbols_by_session

    target_sessions = tuple(
        date.fromisoformat(item) for item in plan.get("baostock_output_sessions", ())
    )
    if not target_sessions:
        return
    cutoff = date.fromisoformat(plan["research_cutoff"])
    raw_coverage = read_raw_symbols_by_session(
        config,
        target_sessions=target_sessions,
        source_package_id=source_package_id,
        cutoff=cutoff,
    )
    symbols_by_session = raw_coverage["symbols_by_session"]
    units = [
        item
        for item in plan.get("stage_units", ())
        if item.get("stage_code") == "SOURCE_BAOSTOCK"
    ]
    max_logins, max_queries, _timeout = _request_budget(config)
    batch_plan = plan_baostock_capture_batches(
        target_sessions,
        symbols_by_session,
        max_login_batches=max_logins,
        max_queries_per_target_session=max_queries,
    )
    if batch_plan["status"] != "READY":
        reason = (
            "BaoStock本次范围需要"
            f"{batch_plan['required_login_batches']}个有界来源登录批次，"
            f"当前配置仅允许{batch_plan['allowed_login_batches']}个；"
            "未抽样、未登录且未发送来源请求。"
        )
        for unit in units:
            data_job_pg.mark_progress_unavailable(
                config,
                job_id=job_id,
                stage_code="SOURCE_BAOSTOCK",
                target_session=unit["target_session"],
                business_table=unit["business_table"],
                status="BLOCKED",
                error_code="BAOSTOCK_REQUEST_BUDGET_EXCEEDED",
                error_message=reason,
            )
        return

    range_capture_ids: dict[tuple[date, date], UUID] = {}
    capture_by_session: dict[date, UUID] = {}
    for batch in batch_plan["batches"]:
        range_key = (batch["range_start"], batch["range_end"])
        capture_id = range_capture_ids.setdefault(
            range_key,
            uuid5(
                job_id,
                f"BAOSTOCK_RANGE:{range_key[0].isoformat()}:{range_key[1].isoformat()}",
            ),
        )
        for session in batch["target_sessions"]:
            capture_by_session[session] = capture_id

    begin_callback(job_id=job_id, units=units)
    inserted_by_session: dict[str, int] = {}
    statuses_by_session: dict[str, set[str]] = {}
    errors_by_session: dict[str, list[str]] = {}

    for batch in batch_plan["batches"]:
        range_key = (batch["range_start"], batch["range_end"])
        capture_id = range_capture_ids[range_key]
        run_sessions = tuple(batch["target_sessions"])
        run_symbols = tuple(batch["source_symbols"])
        try:
            outcome = run_manual_baostock_daily_capture(
                config,
                manual_start=True,
                capture_id=capture_id,
                source_symbols=run_symbols,
                target_sessions=run_sessions,
                official_sessions=official_sessions,
                cutoff=cutoff,
                units=documented_units,
            )
        except Exception as error:  # noqa: BLE001 - BaoStock是可选来源，批次失败不撤销其它来源。
            for session in run_sessions:
                errors_by_session.setdefault(session.isoformat(), []).append(
                    f"{getattr(error, 'code', type(error).__name__)}: {str(error)[:300]}"
                )
            continue

        if outcome.get("pg_readback_verified") is True:
            if UUID(str(outcome.get("capture_id"))) != capture_id:
                raise DataJobError(
                    "BAOSTOCK_CAPTURE_ID_MISMATCH",
                    "BaoStock生产者读回的捕获UUID与本任务确定性请求身份不一致。",
                    status=409,
                )
            for session_text, inserted in outcome.get(
                "inserted_rows_by_session", {}
            ).items():
                inserted_by_session[session_text] = inserted_by_session.get(
                    session_text, 0
                ) + int(inserted)
        else:
            reason_code = (
                "BAOSTOCK_REQUEST_BUDGET_EXCEEDED"
                if outcome.get("status") == "NOT_ATTEMPTED_BUDGET"
                else outcome.get("persistence_error_code")
                or outcome.get("readback_error_code")
                or "BAOSTOCK_SOURCE_NOT_COMPLETE"
            )
            reason = str(
                outcome.get("reason")
                or "BaoStock本批没有形成完整typed PG读回；TDX及算法阶段继续。"
            )
            for session in run_sessions:
                errors_by_session.setdefault(session.isoformat(), []).append(
                    f"{reason_code}: {reason[:300]}"
                )
        for session in run_sessions:
            statuses_by_session.setdefault(session.isoformat(), set()).add(
                str(outcome.get("status") or "UNKNOWN")
            )

    for unit in units:
        day = unit["target_session"]
        session_text = day.isoformat()
        requested_count = len(symbols_by_session.get(session_text, ()))
        if requested_count == 0:
            data_job_pg.mark_progress_unavailable(
                config,
                job_id=job_id,
                stage_code="SOURCE_BAOSTOCK",
                target_session=day,
                business_table=unit["business_table"],
                status="NOT_COMPUTED",
                error_code="BAOSTOCK_RAW_SYMBOL_SCOPE_EMPTY",
                error_message="该官方会话没有精确绑定的TDX来源代码；BaoStock不单独推断证券范围。",
            )
            continue
        failures = errors_by_session.get(session_text, [])
        outcome_statuses = statuses_by_session.get(session_text, set())
        quality_status = (
            "INCOMPLETE"
            if failures
            else "COMPLETE"
            if outcome_statuses and outcome_statuses == {"COMPLETED"}
            else "PARTIAL_WITH_UNKNOWN"
        )
        capture_id = capture_by_session.get(day)
        if capture_id is None:
            data_job_pg.mark_progress_unavailable(
                config,
                job_id=job_id,
                stage_code="SOURCE_BAOSTOCK",
                target_session=day,
                business_table=unit["business_table"],
                status="INCOMPLETE",
                error_code="BAOSTOCK_BATCH_NOT_PLANNED",
                error_message="TDX Raw来源代码没有生成对应的BaoStock批次。",
            )
            continue
        progress_callback(
            job_id=job_id,
            stage_code="SOURCE_BAOSTOCK",
            target_session=day,
            business_table=unit["business_table"],
            source_package_id=None,
            source_capture_id=None,
            baostock_capture_id=capture_id,
            profile_history_session_count=None,
            d1_history_session_count=None,
            expected_rows=requested_count,
            inserted_rows=inserted_by_session.get(session_text, 0),
            quality_status=quality_status,
            no_change=(not failures and inserted_by_session.get(session_text, 0) == 0),
        )


def _execute_adjusted_price_stage(
    *,
    config: Mapping[str, Any],
    job_id: UUID,
    plan: Mapping[str, Any],
    source_package_id: int,
    capture_by_session: Mapping[str, int],
    progress_callback: Any,
    begin_callback: Any,
) -> None:
    """复用现役复权生产者，仅处理公告生效日的精确两会话窗口。"""
    from stock_a.application.adjusted_prices import produce_adjusted_price_window
    from stock_a.canonical.adjustment import PRODUCER_VERSION

    units = [
        unit
        for unit in plan.get("stage_units", ())
        if unit.get("stage_code") == "ADJUSTED_PRICE"
        and unit.get("status") == "NOT_STARTED"
    ]
    symbols_by_session = plan.get("adjusted_price_symbols_by_session", {})
    windows_by_session = plan.get("adjusted_price_source_windows", {})
    planned_captures = plan.get("adjusted_price_capture_by_session", {})
    cutoff = date.fromisoformat(plan["research_cutoff"])
    for unit in units:
        target = unit["target_session"]
        target_text = target.isoformat()
        symbols = tuple(symbols_by_session.get(target_text, ()))
        source_window = tuple(
            date.fromisoformat(value)
            for value in windows_by_session.get(target_text, ())
        )
        if (
            len(source_window) != 2
            or source_window[-1] != target
            or source_window[-1] > cutoff
            or not symbols
        ):
            data_job_pg.mark_progress_unavailable(
                config,
                job_id=job_id,
                stage_code="ADJUSTED_PRICE",
                target_session=target,
                business_table="stock.source_adjusted_daily_prices",
                status="NOT_COMPUTED",
                error_code="ADJUSTMENT_TWO_SESSION_WINDOW_UNAVAILABLE",
                error_message="公告目标未绑定截止内两个相邻官方Raw会话；不触发调整生产。",
            )
            continue
        begin_callback(job_id=job_id, units=[unit])
        expected_rows = 0
        inserted_rows = 0
        statuses: list[str] = []
        try:
            for symbol in symbols:
                result = produce_adjusted_price_window(
                    config,
                    source_symbol=symbol,
                    source_package_id=source_package_id,
                    target_session=target,
                    source_window_sessions=source_window,
                    research_cutoff=cutoff,
                    calendar_id=str(plan["calendar_id"]),
                    calendar_revision=str(plan["calendar_revision"]),
                )
                if result.get("status") != "COMMITTED_SINGLE_NOTICE_WINDOW":
                    raise DataJobError(
                        str(
                            result.get("reason_code") or "ADJUSTED_PRICE_NOT_COMMITTED"
                        ),
                        "复权生产者未提交完整的单公告两会话结果。",
                        status=409,
                    )
                expected_rows += int(result.get("expected_rows", 0) or 0)
                inserted_rows += int(result.get("inserted_rows", 0) or 0)
                statuses.append(str(result["status"]))
            if expected_rows != 2 * len(symbols):
                raise DataJobError(
                    "ADJUSTED_PRICE_EXPECTED_ROW_COUNT_MISMATCH",
                    "复权生产结果没有为每个公告代码返回恰好两会话事实。",
                    status=409,
                )
            target_capture_id = capture_by_session.get(target_text)
            if target_capture_id is None:
                target_capture_id = planned_captures.get(target_text)
            if type(target_capture_id) is not int or target_capture_id <= 0:
                raise DataJobError(
                    "ADJUSTED_PRICE_TARGET_CAPTURE_UNAVAILABLE",
                    "公告目标会话没有唯一的同包Raw捕获身份；调整进度保留未完成。",
                    status=409,
                )
            progress_callback(
                job_id=job_id,
                stage_code="ADJUSTED_PRICE",
                target_session=target,
                business_table="stock.source_adjusted_daily_prices",
                source_package_id=source_package_id,
                source_capture_id=target_capture_id,
                profile_history_session_count=None,
                d1_history_session_count=None,
                expected_rows=expected_rows,
                inserted_rows=inserted_rows,
                quality_status=";".join(sorted(set(statuses))),
                no_change=inserted_rows == 0,
                producer_version=PRODUCER_VERSION,
                calendar_revision=str(plan["calendar_revision"]),
            )
        except Exception as error:  # noqa: BLE001 - 局部能力缺口保留具体阶段状态。
            code = str(getattr(error, "code", type(error).__name__))
            incomplete = inserted_rows > 0 or code in {
                "ADJUSTMENT_EVENT_COVERAGE_INCOMPLETE",
                "ADJUSTMENT_EVENT_WINDOW_NOT_PROVEN",
                "ADJUSTMENT_EVENT_SYMBOL_MISMATCH",
                "ADJUSTMENT_EVENT_SESSION_MISMATCH",
                "ADJUSTED_PRICE_READBACK_MISMATCH",
                "ADJUSTED_PRICE_EXPECTED_ROW_COUNT_MISMATCH",
            }
            data_job_pg.mark_progress_unavailable(
                config,
                job_id=job_id,
                stage_code="ADJUSTED_PRICE",
                target_session=target,
                business_table="stock.source_adjusted_daily_prices",
                status="INCOMPLETE" if incomplete else "NOT_COMPUTED",
                error_code=code,
                error_message=f"调整价格单股生产未闭合：{str(error)[:500]}",
            )


def _execute_shared_production_chain(
    *,
    config: Mapping[str, Any],
    job_id: UUID,
    plan: Mapping[str, Any],
    progress_callback: Any,
    begin_callback: Any,
) -> None:
    """复用唯一VIPDATA Raw、Core/Profile、D0/D1及静态研究生产者。"""
    from stock_a.application.pg_price_profiles import (
        SOURCE_SYMBOL_PROFILE_SCOPE,
        run_source_symbol_price_profile,
    )
    from stock_a.application.runtime_config import _CONFIG_ROOT
    from stock_a.application.source_formal_admission_production import (
        SourceFormalAdmissionProductionError,
        run_source_formal_admission_range_from_saved,
    )
    from stock_a.application.source_market_context import (
        SourceMarketContextError,
        compute_source_market_context,
        read_source_market_context,
    )
    from stock_a.application.source_market_participation import (
        SourceMarketParticipationError,
        produce_market_participation,
    )
    from stock_a.application.source_research_day import (
        run_source_research_day,
        run_source_static_research_day,
    )
    from stock_a.contracts import load_price_profile_contract
    from stock_a.sources.tdx.vipdata import (
        VipdataBudgets,
        VipdataSourceConfig,
        VipdataTargetScanBudget,
        cleanup_vipdata_package,
        download_vipdata_package,
        inspect_vipdata_page,
    )
    from stock_a.storage.postgres.market_breadth import (
        MarketBreadthPgError,
        persist_market_breadth,
    )
    from stock_a.storage.postgres.source_raw import import_verified_vipdata_history

    target = date.fromisoformat(plan["target_session"])
    profile_history = int(plan.get("profile_history_session_count") or 1)
    raw_sessions = tuple(
        date.fromisoformat(day) for day in plan["raw_history_sessions"]
    )
    selected_source_modules = set(plan.get("source_modules", ()))
    if "TDX_RAW" in selected_source_modules:
        package_unit = next(
            unit
            for unit in plan["stage_units"]
            if unit["business_table"] == "stock.source_packages"
        )
        begin_callback(job_id=job_id, units=[package_unit])
    cutoff = date.fromisoformat(plan["research_cutoff"])
    package_id_holder: list[int] = []

    def package_verified(package_id: int, inserted: bool) -> None:
        progress_callback(
            job_id=job_id,
            stage_code="SOURCE_RAW",
            target_session=target,
            business_table="stock.source_packages",
            source_package_id=package_id,
            source_capture_id=None,
            profile_history_session_count=None,
            d1_history_session_count=None,
            expected_rows=1,
            inserted_rows=int(inserted),
            quality_status="PACKAGE_VERIFIED",
            no_change=not inserted,
        )

    def session_verified(result: Mapping[str, Any]) -> None:
        day = date.fromisoformat(result["target_session"])
        capture_id = int(result["source_capture_id"])
        for table, expected, inserted in (
            ("stock.source_captures", 1, int(result.get("capture_inserted") is True)),
            (
                "stock.source_raw_daily",
                int(result["candidate_total"]),
                int(result["new_rows_committed"]),
            ),
        ):
            progress_callback(
                job_id=job_id,
                stage_code="SOURCE_RAW",
                target_session=day,
                business_table=table,
                source_package_id=package_id_holder[0],
                source_capture_id=capture_id,
                profile_history_session_count=None,
                d1_history_session_count=None,
                expected_rows=expected,
                inserted_rows=inserted,
                quality_status=result["status"],
                no_change=result["status"] == "ALREADY_COMPLETE",
            )

    def register_package_id(package_id: int, inserted: bool) -> None:
        package_id_holder.append(package_id)
        data_job_pg.set_job_source_package(
            config, job_id=job_id, source_package_id=package_id
        )
        package_verified(package_id, inserted)

    def sessions_started(sessions: Sequence[date]) -> None:
        selected = set(sessions)
        units = [
            unit
            for unit in plan["stage_units"]
            if unit["stage_code"] == "SOURCE_RAW"
            and unit["business_table"]
            in {"stock.source_captures", "stock.source_raw_daily"}
            and unit["target_session"] in selected
        ]
        begin_callback(job_id=job_id, units=units)

    _persist_sector_membership_stage(
        config=config,
        job_id=job_id,
        plan=plan,
        progress_callback=progress_callback,
        begin_callback=begin_callback,
        prepared_snapshots=plan.get("_prepared_sector_snapshots"),
    )

    if (
        selected_source_modules == {"TDX_SECTOR_MEMBERS"}
        and not plan.get("base_modules")
        and not plan.get("research_modules")
    ):
        return

    if (
        not selected_source_modules
        and not plan.get("base_modules")
        and set(plan.get("research_modules", ())) <= {"ADJUSTED_PRICE"}
        and not any(
            unit.get("stage_code") == "ADJUSTED_PRICE"
            and unit.get("status") == "NOT_STARTED"
            for unit in plan.get("stage_units", ())
        )
    ):
        return

    reuse_package_id = plan.get("reuse_raw_package_id")
    raw_session_inputs = plan.get("raw_session_inputs", [])
    reuse_saved_baostock_raw = (
        "BAOSTOCK" in selected_source_modules
        and "TDX_RAW" not in selected_source_modules
        and not raw_sessions
        and type(plan.get("baostock_source_package_id")) is int
    )
    if reuse_saved_baostock_raw:
        source_package_id = int(plan["baostock_source_package_id"])
        package_id_holder.append(source_package_id)
        saved_inputs = plan.get("baostock_raw_session_inputs", ())
        data_job_pg.set_job_source_package(
            config, job_id=job_id, source_package_id=source_package_id
        )
        raw_result = {
            "status": "ALREADY_COMPLETE",
            "source_package_id": source_package_id,
            "session_results": [
                {
                    "target_session": item["session_date"],
                    "candidate_total": int(item["expected_row_count"]),
                    "row_count_before": int(item["row_count"]),
                    "row_count_after": int(item["row_count"]),
                    "new_rows_committed": 0,
                    "source_capture_id": int(item["source_capture_id"]),
                    "capture_inserted": False,
                    "status": "ALREADY_COMPLETE",
                }
                for item in saved_inputs
            ],
        }
    elif reuse_package_id is not None and len(raw_session_inputs) == len(raw_sessions):
        package_id_holder.append(int(reuse_package_id))
        data_job_pg.set_job_source_package(
            config, job_id=job_id, source_package_id=int(reuse_package_id)
        )
        if "TDX_RAW" in selected_source_modules:
            package_verified(int(reuse_package_id), False)
            sessions_started(raw_sessions)
        reused_session_results = [
            {
                "target_session": item["session_date"],
                "candidate_total": int(item["expected_row_count"]),
                "row_count_before": int(item["row_count"]),
                "row_count_after": int(item["row_count"]),
                "new_rows_committed": 0,
                "source_capture_id": int(item["source_capture_id"]),
                "capture_inserted": False,
                "status": "ALREADY_COMPLETE",
            }
            for item in raw_session_inputs
        ]
        raw_result = {
            "status": "ALREADY_COMPLETE",
            "source_package_id": int(reuse_package_id),
            "session_results": reused_session_results,
        }
    elif "TDX_RAW" not in selected_source_modules:
        raise DataJobError(
            "RAW_INPUT_WINDOW_UNAVAILABLE",
            "所选算法需要的全部Raw输入没有绑定同一完整来源包；未触发外部来源读取。",
            status=409,
        )
    else:
        prepared_package = plan.get("_prepared_vipdata_package")
        owns_package = prepared_package is None
        package_owner = None
        try:
            if prepared_package is None:
                source_config = VipdataSourceConfig.from_mapping(
                    config["source_policy"]["vipdata_official"]
                )
                download_budgets = VipdataBudgets()
                observation = inspect_vipdata_page(
                    target.isoformat(),
                    budgets=download_budgets,
                    source_config=source_config,
                )
                if observation.status != "PACKAGE_CANDIDATE_READY":
                    raise DataJobError(
                        "BLOCKED_SOURCE_NOT_AVAILABLE",
                        observation.reason or "VIPDATA来源当前不可验证。",
                        status=503,
                    )
                update_at = getattr(observation, "official_update_at", None)
                if update_at is None:
                    raise DataJobError(
                        "BLOCKED_SOURCE_TIME_UNKNOWN",
                        "官方来源更新时钟未知；未下载或读取行情包。",
                        status=409,
                    )
                if update_at.date() > cutoff:
                    raise DataJobError(
                        "BLOCKED_SOURCE_AFTER_CUTOFF",
                        "官方来源更新时钟晚于任务截止；未下载或读取行情包。",
                        status=409,
                    )
                package_owner = download_vipdata_package(
                    observation, budgets=download_budgets
                )
                prepared_package = verify_existing_vipdata_package(
                    package_owner.file_path, budgets=download_budgets
                )
            raw_result = import_verified_vipdata_history(
                config,
                prepared_package,
                raw_sessions,
                task_scope=_RAW_JOB_SCOPE,
                scan_budget=VipdataTargetScanBudget(),
                chunk_size=100,
                max_raw_rows=2_000_000,
                on_source_package_verified=register_package_id,
                on_sessions_started=sessions_started,
                on_session_verified=session_verified,
            )
        finally:
            if owns_package and package_owner is not None:
                cleanup_vipdata_package(package_owner)

    source_package_id = int(raw_result["source_package_id"])
    capture_by_session = {
        item["target_session"]: int(item["source_capture_id"])
        for item in raw_result["session_results"]
    }
    raw_count_by_session = {
        item["target_session"]: int(item["candidate_total"])
        for item in raw_result["session_results"]
    }
    if "ADJUSTED_PRICE" in plan.get("research_modules", ()):
        _execute_adjusted_price_stage(
            config=config,
            job_id=job_id,
            plan=plan,
            source_package_id=source_package_id,
            capture_by_session=capture_by_session,
            progress_callback=progress_callback,
            begin_callback=begin_callback,
        )
    if "BAOSTOCK" in selected_source_modules:
        try:
            _calendar_id, _calendar_revision, official_sessions = (
                _load_official_sessions(cutoff)
            )
            _execute_baostock_source_stage(
                config=config,
                job_id=job_id,
                plan=plan,
                source_package_id=source_package_id,
                official_sessions=official_sessions,
                progress_callback=progress_callback,
                begin_callback=begin_callback,
            )
        except Exception as error:
            # BaoStock是TDX之外的可选第一层来源；其失败不回滚已提交TDX Raw。
            for unit in plan["stage_units"]:
                if unit.get("stage_code") != "SOURCE_BAOSTOCK":
                    continue
                data_job_pg.mark_progress_unavailable(
                    config,
                    job_id=job_id,
                    stage_code="SOURCE_BAOSTOCK",
                    target_session=unit["target_session"],
                    business_table=unit["business_table"],
                    status="INCOMPLETE",
                    error_code=getattr(error, "code", type(error).__name__),
                    error_message=f"BaoStock补充来源阶段未完成：{str(error)[:500]}",
                )
    market_blocked = set(plan["market_breadth_insufficient_history_sessions"])
    for day_text in plan["market_breadth_sessions"]:
        day = date.fromisoformat(day_text)
        if day_text in market_blocked:
            continue
        market_units = [
            unit
            for unit in plan["stage_units"]
            if unit["stage_code"] == "MARKET_BREADTH"
            and unit["target_session"] == day
            and unit["status"] == "NOT_STARTED"
        ]
        begin_callback(job_id=job_id, units=market_units)
        try:
            market = compute_source_market_context(
                config,
                target_session=day,
                research_cutoff=cutoff,
                source_package_id=source_package_id,
            )
            market_persistence = persist_market_breadth(config, market)
        except (SourceMarketContextError, MarketBreadthPgError) as error:
            unavailable_status = (
                "INCOMPLETE"
                if getattr(error, "database_write_attempted", False)
                else "NOT_COMPUTED"
            )
            for unit in market_units:
                data_job_pg.mark_progress_unavailable(
                    config,
                    job_id=job_id,
                    stage_code="MARKET_BREADTH",
                    target_session=day,
                    business_table=unit["business_table"],
                    status=unavailable_status,
                    error_code=getattr(error, "code", type(error).__name__),
                    error_message=str(error),
                )
            continue
        breadth_inserted = int(market_persistence.get("inserted_rows", 0) or 0)
        breadth_quality = "SOURCE_UNKNOWN_COUNT=" + str(
            market.calculation.counts.get("UNKNOWN", 0)
        )
        for table, expected_rows, inserted_rows in (
            ("stock.source_market_breadth_days", 1, int(breadth_inserted > 0)),
            (
                "stock.source_market_breadth_symbols",
                int(market.calculation.n_source),
                breadth_inserted,
            ),
        ):
            progress_callback(
                job_id=job_id,
                stage_code="MARKET_BREADTH",
                target_session=day,
                business_table=table,
                source_package_id=source_package_id,
                source_capture_id=capture_by_session[day_text],
                profile_history_session_count=None,
                d1_history_session_count=None,
                expected_rows=expected_rows,
                inserted_rows=inserted_rows,
                quality_status=breadth_quality,
                no_change=market_persistence.get("status")
                == "IDEMPOTENT_ALREADY_PRESENT",
                calendar_revision=market.calendar["calendar_revision"],
            )

    profile_contract = load_price_profile_contract(_CONFIG_ROOT).document
    d0_requested = set(plan["d0_output_sessions"])
    d1_requested = set(plan["d1_output_sessions"])
    d0_days = d0_requested - set(plan["d0_insufficient_previous_sessions"])
    d1_days = d1_requested - set(plan["d1_insufficient_history_sessions"])
    profile_blocked = set(plan["profile_insufficient_history_sessions"])
    profile_days = set(plan["profile_output_sessions"]) - profile_blocked
    profile_window_counts = plan.get("profile_history_session_counts_by_session", {})
    d0_profile_window_counts = plan.get(
        "d0_profile_history_session_counts_by_session", {}
    )
    d1_window_counts = plan.get("d1_history_session_counts_by_session", {})
    output_days = sorted(d0_days | d1_days | profile_days)
    d1_window = int(plan.get("d1_calculation_session_count") or 1)
    effective_d1_window = max(2, d1_window)
    participation_blocked = set(
        plan.get("market_participation_insufficient_sessions", ())
    )
    participation_completed: set[str] = set()

    def produce_participation(day_text: str, history_count: int) -> None:
        if day_text in participation_blocked or day_text in participation_completed:
            return
        day = date.fromisoformat(day_text)
        market_units = [
            unit
            for unit in plan["stage_units"]
            if unit["stage_code"] == "MARKET_PARTICIPATION"
            and unit["target_session"] == day
        ]
        if not market_units:
            return
        begin_callback(job_id=job_id, units=market_units)
        try:
            market = read_source_market_context(
                config,
                target_session=day,
                research_cutoff=cutoff,
                source_package_id=source_package_id,
                include_symbols=True,
            )
            participation = produce_market_participation(config, market)
        except (SourceMarketContextError, SourceMarketParticipationError) as error:
            unavailable_status = (
                "INCOMPLETE"
                if getattr(error, "database_write_attempted", False)
                else "NOT_COMPUTED"
            )
            for unit in market_units:
                data_job_pg.mark_progress_unavailable(
                    config,
                    job_id=job_id,
                    stage_code="MARKET_PARTICIPATION",
                    target_session=day,
                    business_table=unit["business_table"],
                    status=unavailable_status,
                    error_code=getattr(error, "code", type(error).__name__),
                    error_message=str(error),
                )
            participation_completed.add(day_text)
            return
        persistence = participation["persistence"]
        readback = participation["readback"]
        unknown_reasons = readback.get("denominator", {}).get(
            "unknown_reason_counts", {}
        )
        inserted_reasons = int(persistence.get("inserted_reason_rows", 0) or 0)
        for table, expected_rows, inserted_rows in (
            (
                "stock.source_market_participation_days",
                1,
                int(persistence.get("status") == "WRITTEN"),
            ),
            (
                "stock.source_market_participation_unknown_reasons",
                len(unknown_reasons),
                inserted_reasons,
            ),
        ):
            progress_callback(
                job_id=job_id,
                stage_code="MARKET_PARTICIPATION",
                target_session=day,
                business_table=table,
                source_package_id=source_package_id,
                source_capture_id=capture_by_session[day_text],
                profile_history_session_count=history_count,
                d1_history_session_count=None,
                expected_rows=expected_rows,
                inserted_rows=inserted_rows,
                quality_status=readback.get("status"),
                no_change=persistence.get("status") == "IDEMPOTENT_ALREADY_PRESENT",
                profile_revision=participation["profile_revision"],
                producer_version=participation["producer_version"],
                calendar_revision=participation["calendar_revision"],
            )
        participation_completed.add(day_text)

    for day_text in output_days:
        day = date.fromisoformat(day_text)
        profile_input_count = int(
            profile_window_counts.get(
                day_text,
                d0_profile_window_counts.get(day_text, profile_history),
            )
        )
        d1_input_count = int(d1_window_counts.get(day_text, effective_d1_window))
        history_count = (
            profile_input_count if profile_input_count > 0 else d1_input_count
        )
        if history_count < 1:
            continue
        if day_text in profile_days:
            history_count = int(profile_window_counts.get(day_text, profile_history))
            profile_unit = next(
                unit
                for unit in plan["stage_units"]
                if unit["stage_code"] == "CORE_PROFILE"
                and unit["target_session"] == day
            )
            begin_callback(job_id=job_id, units=[profile_unit])
            profile = run_source_symbol_price_profile(
                config,
                target_session=day,
                source_package_id=source_package_id,
                scope=SOURCE_SYMBOL_PROFILE_SCOPE,
                session_count=history_count,
                page_size=100,
                profile_contract=profile_contract,
            )
            profile_inserted = int(profile.get("inserted_count", 0) or 0)
            progress_callback(
                job_id=job_id,
                stage_code="CORE_PROFILE",
                target_session=day,
                business_table="stock.source_symbol_price_profiles",
                source_package_id=source_package_id,
                source_capture_id=capture_by_session[day_text],
                profile_history_session_count=history_count,
                d1_history_session_count=None,
                expected_rows=raw_count_by_session[day_text],
                inserted_rows=profile_inserted,
                quality_status=profile.get("status"),
                no_change=profile.get("status") == "IDEMPOTENT_READBACK",
                profile_revision=profile.get("profile_revision"),
                calendar_revision=profile.get("calendar_revision"),
            )
            produce_participation(day_text, history_count)

        if day_text not in d0_days and day_text not in d1_days:
            continue

        research_units = [
            unit
            for unit in plan["stage_units"]
            if unit["stage_code"] == "D0_D1"
            and unit["target_session"] == day
            and unit["status"] == "NOT_STARTED"
        ]
        begin_callback(job_id=job_id, units=research_units)
        run_d1_window = (
            d1_input_count
            if day_text in d1_days
            else min(effective_d1_window, history_count)
        )
        report = run_source_research_day(
            config,
            target_session=day,
            research_cutoff=cutoff,
            source_package_id=source_package_id,
            d1_history_session_count=run_d1_window,
            persist=True,
            include_d0=day_text in d0_days,
            include_d1=day_text in d1_days,
            include_static=False,
        )
        persistence = report.get("persistence", {})
        expected = persistence.get("rows_expected", {})
        inserted = persistence.get("rows_inserted", {})
        research_tables: list[str] = []
        if day_text in d0_days:
            research_tables.extend(
                (
                    "stock.source_d0_launch_day_facts",
                    "stock.source_research_enrollments",
                )
            )
        if day_text in d1_days:
            research_tables.extend(
                (
                    "stock.source_d1_structure_day_facts",
                    "stock.source_d1_material_anchor_day_facts",
                )
            )
        for table in research_tables:
            key = table.removeprefix("stock.")
            if table == "stock.source_research_enrollments":
                inserted_rows = int(
                    persistence.get("research_enrollments", {}).get("inserted", 0) or 0
                )
                expected_rows = persistence.get("research_enrollments", {}).get(
                    "expected"
                )
                if type(expected_rows) is not int or expected_rows < 0:
                    raise DataJobError(
                        "D0_ENROLLMENT_EXPECTED_ROWS_UNKNOWN",
                        "D0入组事务未返回可独立核验的精确预期行数。",
                        status=409,
                    )
            else:
                inserted_rows = int(inserted.get(key, 0) or 0)
                expected_rows = expected.get(key)
            producer_version = persistence.get("producer_versions", {}).get(key)
            progress_callback(
                job_id=job_id,
                stage_code="D0_D1",
                target_session=day,
                business_table=table,
                source_package_id=source_package_id,
                source_capture_id=capture_by_session[day_text],
                profile_history_session_count=history_count,
                d1_history_session_count=effective_d1_window,
                expected_rows=expected_rows,
                inserted_rows=inserted_rows,
                quality_status=report.get("status"),
                no_change=persistence.get("status") == "IDEMPOTENT_ALREADY_PRESENT",
                producer_version=producer_version,
                calendar_revision=persistence.get(
                    "calendar_revision",
                    report.get("calendar", {}).get("calendar_revision"),
                ),
            )

    for day_text in plan.get("market_participation_sessions", ()):
        if day_text in profile_days:
            continue
        day = date.fromisoformat(day_text)
        history_count = int(
            profile_window_counts.get(
                day_text,
                d0_profile_window_counts.get(day_text, profile_history),
            )
        )
        if history_count > 0:
            produce_participation(day_text, history_count)

    for day_text in sorted(d0_days):
        if day_text in profile_blocked:
            continue
        day = date.fromisoformat(day_text)
        history_count = int(d0_profile_window_counts.get(day_text, 0))
        if history_count < 1:
            continue
        static_units = [
            unit
            for unit in plan["stage_units"]
            if unit["stage_code"] == "STATIC_RESEARCH" and unit["target_session"] == day
        ]
        begin_callback(job_id=job_id, units=static_units)
        static = run_source_static_research_day(
            config,
            target_session=day,
            research_cutoff=cutoff,
            source_package_id=source_package_id,
            persist=True,
            history_session_count=history_count,
        )
        static_persistence = static.get("persistence", {})
        static_expected = static_persistence.get("rows_expected", {})
        static_inserted = static_persistence.get("rows_inserted", {})
        for table in (
            "stock.source_d0_scenario_day_facts",
            "stock.source_pool_seed_day_facts",
        ):
            key = table.removeprefix("stock.")
            inserted_rows = int(static_inserted.get(key, 0) or 0)
            producer_versions = static_persistence.get("producer_versions", {}).get(
                key, [None]
            )
            producer_version = (
                producer_versions
                if key == "source_d0_scenario_day_facts"
                else producer_versions[0]
            )
            progress_callback(
                job_id=job_id,
                stage_code="STATIC_RESEARCH",
                target_session=day,
                business_table=table,
                source_package_id=source_package_id,
                source_capture_id=capture_by_session[day_text],
                profile_history_session_count=history_count,
                d1_history_session_count=effective_d1_window,
                expected_rows=static_expected.get(key),
                inserted_rows=inserted_rows,
                quality_status=static.get("status"),
                no_change=static_persistence.get("status")
                == "IDEMPOTENT_ALREADY_PRESENT",
                profile_revision=static_persistence.get("profile_revision"),
                producer_version=producer_version,
                calendar_revision=static_persistence.get("calendar_revisions", {}).get(
                    key, [None]
                )[0],
            )

    focus_days = set(plan.get("module_output_sessions", {}).get("FOCUS_FORMAL", ()))
    for day_text in sorted(focus_days):
        day = date.fromisoformat(day_text)
        formal_unit = next(
            unit
            for unit in plan["stage_units"]
            if unit["stage_code"] == "FOCUS_FORMAL" and unit["target_session"] == day
        )
        begin_callback(job_id=job_id, units=[formal_unit])
        try:
            formal_range = run_source_formal_admission_range_from_saved(
                config,
                start_session=day,
                target_session=day,
                research_cutoff=cutoff,
                source_package_id=source_package_id,
            )
        except SourceFormalAdmissionProductionError as error:
            data_job_pg.mark_progress_unavailable(
                config,
                job_id=job_id,
                stage_code="FOCUS_FORMAL",
                target_session=day,
                business_table="stock.source_d0_formal_admission_day_facts",
                status=(
                    "INCOMPLETE"
                    if "READBACK_MISMATCH" in error.code
                    else "NOT_COMPUTED"
                ),
                error_code=error.code,
                error_message=str(error),
            )
            continue
        formal_reports = formal_range.get("reports", ())
        if formal_range.get("status") != "PERSISTED" or len(formal_reports) != 1:
            data_job_pg.mark_progress_unavailable(
                config,
                job_id=job_id,
                stage_code="FOCUS_FORMAL",
                target_session=day,
                business_table="stock.source_d0_formal_admission_day_facts",
                status="NOT_COMPUTED",
                error_code=str(
                    formal_range.get("reason_code") or "FORMAL_ADMISSION_NOT_COMPUTED"
                ),
                error_message="已保存D0准入输入未闭合，正式Focus准入保留未计算。",
            )
            continue
        formal = formal_reports[0]
        progress_callback(
            job_id=job_id,
            stage_code="FOCUS_FORMAL",
            target_session=day,
            business_table="stock.source_d0_formal_admission_day_facts",
            source_package_id=source_package_id,
            source_capture_id=int(formal.get("source_capture_id") or 0)
            or capture_by_session[day_text],
            profile_history_session_count=None,
            d1_history_session_count=None,
            expected_rows=int(formal["source_count"]),
            inserted_rows=int(formal["inserted_rows"]),
            quality_status=formal.get("status"),
            no_change=formal.get("idempotent_rerun") is True,
            profile_revision=formal.get("profile_revision"),
            producer_version=formal.get("producer_version"),
            calendar_revision=formal.get("calendar_revision"),
        )

    sector_pair = plan.get("sector_snapshot_pair", {})
    sector_snapshot_ids = sector_pair.get("snapshot_ids")
    if "SECTOR_PULSE" in set(plan.get("research_modules", ())):
        from stock_a.application.source_sector_pulse import run_source_sector_pulse_day
        from stock_a.storage.postgres.sector_pulse_facts import (
            read_source_sector_pulse_day,
        )
        from stock_a.storage.postgres.static_research_facts import (
            read_source_pool_seed_forward_scope,
        )

        for day_text in plan.get("sector_pulse_sessions", ()):
            day = date.fromisoformat(day_text)
            units = [
                unit
                for unit in plan["stage_units"]
                if unit["stage_code"] == "SECTOR_PULSE"
                and unit["target_session"] == day
            ]
            if not units or units[0].get("status") == "NOT_COMPUTED":
                continue
            if sector_pair.get("status") != "AVAILABLE" or not isinstance(
                sector_snapshot_ids, Mapping
            ):
                for unit in units:
                    data_job_pg.mark_progress_unavailable(
                        config,
                        job_id=job_id,
                        stage_code="SECTOR_PULSE",
                        target_session=day,
                        business_table=unit["business_table"],
                        status="NOT_COMPUTED",
                        error_code=sector_pair.get("reason_code")
                        or "SECTOR_SNAPSHOT_PAIR_NOT_AVAILABLE",
                        error_message="行业与GN概念快照对不可用；板块结果保留未计算。",
                    )
                continue
            begin_callback(job_id=job_id, units=units)
            capture_id = capture_by_session[day_text]
            expected_source_count = raw_count_by_session[day_text]
            seed_scope = read_source_pool_seed_forward_scope(
                config,
                target_session=day,
                source_package_id=source_package_id,
                source_capture_id=capture_id,
                expected_source_count=expected_source_count,
            )
            pulse = run_source_sector_pulse_day(
                config,
                target_session=day,
                research_cutoff=cutoff,
                source_package_id=source_package_id,
                profile_revision=seed_scope["profile_revision"],
                industry_snapshot_id=int(sector_snapshot_ids["INDUSTRY"]),
                concept_snapshot_id=int(sector_snapshot_ids["CONCEPT"]),
            )
            readback = read_source_sector_pulse_day(
                config,
                target_session=day,
                source_package_id=source_package_id,
                source_capture_id=capture_id,
                source_pool_count=expected_source_count,
                snapshot_ids=sector_snapshot_ids,
            )
            if readback.get("status") != "AVAILABLE":
                raise DataJobError(
                    "SECTOR_PULSE_READBACK_INCOMPLETE",
                    "行业与GN概念PULSE没有在独立事务中闭合读回。",
                    status=409,
                )
            expected_rows = sum(
                int(group.get("expected_board_count", 0))
                for group in readback.get("by_kind", {}).values()
            )
            verified_rows = sum(
                int(group.get("read_board_count", 0))
                for group in readback.get("by_kind", {}).values()
            )
            if verified_rows != expected_rows:
                raise DataJobError(
                    "SECTOR_PULSE_READBACK_COUNT_MISMATCH",
                    "行业与GN概念PULSE目录总数与读回行数不一致。",
                    status=409,
                )
            inserted_rows = int(
                pulse.get("persistence", {})
                .get("rows_inserted", {})
                .get("source_sector_cross_period_pulse_facts", 0)
            )
            progress_callback(
                job_id=job_id,
                stage_code="SECTOR_PULSE",
                target_session=day,
                business_table="stock.source_sector_cross_period_pulse_facts",
                source_package_id=source_package_id,
                source_capture_id=capture_id,
                profile_history_session_count=None,
                d1_history_session_count=None,
                expected_rows=expected_rows,
                inserted_rows=inserted_rows,
                quality_status="COMPLETE_NON_PIT_CURRENT_MEMBERSHIP",
                no_change=pulse.get("persistence", {}).get("status")
                == "IDEMPOTENT_ALREADY_PRESENT",
                profile_revision=seed_scope["profile_revision"],
                producer_version="SOURCE_CROSS_PERIOD_PULSE_R1",
                sector_snapshot_ids=sector_snapshot_ids,
            )

    if "SECTOR_TRANSITION" in set(plan.get("research_modules", ())):
        from stock_a.application.source_sector_pulse import (
            SourceSectorPulseError,
            run_source_sector_pulse_transition_day,
        )
        from stock_a.storage.postgres.static_research_facts import (
            StaticResearchPgError,
            read_source_pool_seed_forward_scope,
        )

        transition_pairs = plan.get("sector_transition_pairs", {})
        for day_text in plan.get("sector_transition_sessions", ()):
            target_day = date.fromisoformat(day_text)
            unit = next(
                (
                    item
                    for item in plan["stage_units"]
                    if item.get("stage_code") == "SECTOR_TRANSITION"
                    and item.get("target_session") == target_day
                ),
                None,
            )
            if unit is None or unit.get("status") in {"NOT_COMPUTED", "BLOCKED"}:
                continue
            previous_text = transition_pairs.get(day_text)
            if not isinstance(previous_text, str):
                data_job_pg.mark_progress_unavailable(
                    config,
                    job_id=job_id,
                    stage_code="SECTOR_TRANSITION",
                    target_session=target_day,
                    business_table=unit["business_table"],
                    status="NOT_COMPUTED",
                    error_code="SECTOR_TRANSITION_PREVIOUS_SESSION_MISSING",
                    error_message="相邻变化缺少前序官方会话；保留未计算。",
                )
                continue
            previous_day = date.fromisoformat(previous_text)
            previous_capture = capture_by_session.get(previous_text)
            target_capture = capture_by_session.get(day_text)
            if previous_capture is None or target_capture is None:
                data_job_pg.mark_progress_unavailable(
                    config,
                    job_id=job_id,
                    stage_code="SECTOR_TRANSITION",
                    target_session=target_day,
                    business_table=unit["business_table"],
                    status="NOT_COMPUTED",
                    error_code="SECTOR_TRANSITION_RAW_CAPTURE_UNAVAILABLE",
                    error_message="相邻两端缺少精确Raw捕获；不读取其它版本替代。",
                )
                continue
            if sector_pair.get("status") != "AVAILABLE" or not isinstance(
                sector_snapshot_ids, Mapping
            ):
                data_job_pg.mark_progress_unavailable(
                    config,
                    job_id=job_id,
                    stage_code="SECTOR_TRANSITION",
                    target_session=target_day,
                    business_table=unit["business_table"],
                    status="NOT_COMPUTED",
                    error_code=sector_pair.get("reason_code")
                    or "SECTOR_SNAPSHOT_PAIR_NOT_AVAILABLE",
                    error_message="相邻变化缺少同一有效行业/GN快照对。",
                )
                continue
            try:
                previous_seed = read_source_pool_seed_forward_scope(
                    config,
                    target_session=previous_day,
                    source_package_id=source_package_id,
                    source_capture_id=previous_capture,
                    expected_source_count=raw_count_by_session[previous_text],
                )
                target_seed = read_source_pool_seed_forward_scope(
                    config,
                    target_session=target_day,
                    source_package_id=source_package_id,
                    source_capture_id=target_capture,
                    expected_source_count=raw_count_by_session[day_text],
                )
                begin_callback(job_id=job_id, units=[unit])
                transition = run_source_sector_pulse_transition_day(
                    config,
                    previous_session=previous_day,
                    target_session=target_day,
                    research_cutoff=cutoff,
                    previous_source_package_id=source_package_id,
                    target_source_package_id=source_package_id,
                    previous_profile_revision=previous_seed["profile_revision"],
                    target_profile_revision=target_seed["profile_revision"],
                    industry_snapshot_id=int(sector_snapshot_ids["INDUSTRY"]),
                    concept_snapshot_id=int(sector_snapshot_ids["CONCEPT"]),
                )
            except (SourceSectorPulseError, StaticResearchPgError) as error:
                code = getattr(error, "code", type(error).__name__)
                if str(code).startswith(
                    (
                        "SECTOR_PULSE_TRANSITION_RAW_",
                        "SECTOR_PULSE_TRANSITION_PROFILE_",
                        "STATIC_RESEARCH_",
                        "SOURCE_POOL_SEED_",
                    )
                ):
                    data_job_pg.mark_progress_unavailable(
                        config,
                        job_id=job_id,
                        stage_code="SECTOR_TRANSITION",
                        target_session=target_day,
                        business_table=unit["business_table"],
                        status="NOT_COMPUTED",
                        error_code=str(code),
                        error_message=str(error),
                    )
                    continue
                raise
            if transition.get("readback_status") != "AVAILABLE":
                raise DataJobError(
                    "SECTOR_TRANSITION_READBACK_INCOMPLETE",
                    "行业/GN相邻变化没有在独立事务中闭合读回。",
                    status=409,
                )
            transition_calendar = transition.get("calendar_identity", {})
            transition_inserted = transition.get("persistence", {}).get(
                "rows_inserted", 0
            )
            progress_callback(
                job_id=job_id,
                stage_code="SECTOR_TRANSITION",
                target_session=target_day,
                business_table=unit["business_table"],
                source_package_id=source_package_id,
                source_capture_id=target_capture,
                profile_history_session_count=None,
                d1_history_session_count=None,
                expected_rows=int(transition.get("transition_rows_expected", 0)),
                inserted_rows=int(transition_inserted),
                quality_status="COMPLETE_NON_PIT_ADJACENT_TRANSITION",
                no_change=transition.get("persistence", {}).get("status")
                == "IDEMPOTENT_ALREADY_PRESENT",
                profile_revision=target_seed["profile_revision"],
                producer_version="SOURCE_CROSS_PERIOD_PULSE_TRANSITION_R1",
                calendar_revision=transition_calendar.get("calendar_revision"),
                sector_snapshot_ids=sector_snapshot_ids,
                followup_target_sessions=(previous_day,),
            )

    if "SECTOR_FOLLOWUP" in set(plan.get("research_modules", ())):
        from stock_a.application.source_sector_pulse_followup import (
            run_source_sector_pulse_followup_day,
        )
        from stock_a.storage.postgres.sector_pulse_facts import (
            read_source_sector_pulse_day,
        )

        due_map = plan.get("sector_followup_due_targets", {})
        for day_text in plan.get("sector_followup_sessions", ()):
            day = date.fromisoformat(day_text)
            units = [
                unit
                for unit in plan["stage_units"]
                if unit["stage_code"] == "SECTOR_FOLLOWUP"
                and unit["target_session"] == day
            ]
            if not units or not due_map.get(day_text):
                continue
            if any(unit.get("status") == "NOT_COMPUTED" for unit in units):
                continue
            if sector_pair.get("status") != "AVAILABLE" or not isinstance(
                sector_snapshot_ids, Mapping
            ):
                for unit in units:
                    data_job_pg.mark_progress_unavailable(
                        config,
                        job_id=job_id,
                        stage_code="SECTOR_FOLLOWUP",
                        target_session=day,
                        business_table=unit["business_table"],
                        status="NOT_COMPUTED",
                        error_code="SECTOR_SNAPSHOT_PAIR_NOT_AVAILABLE",
                        error_message="后继研究缺少行业与GN概念的有效同次观察快照。",
                    )
                continue
            capture_id = capture_by_session.get(day_text)
            if capture_id is None:
                raise DataJobError(
                    "SECTOR_FOLLOWUP_T0_RAW_UNAVAILABLE",
                    "板块后继T0没有精确Raw捕获；不读取其它会话替代。",
                    status=409,
                )
            pulse_readback = read_source_sector_pulse_day(
                config,
                target_session=day,
                source_package_id=source_package_id,
                source_capture_id=capture_id,
                source_pool_count=raw_count_by_session[day_text],
                snapshot_ids=sector_snapshot_ids,
            )
            if pulse_readback.get("status") != "AVAILABLE":
                for unit in units:
                    data_job_pg.mark_progress_unavailable(
                        config,
                        job_id=job_id,
                        stage_code="SECTOR_FOLLOWUP",
                        target_session=day,
                        business_table=unit["business_table"],
                        status="NOT_COMPUTED",
                        error_code="SECTOR_FOLLOWUP_PULSE_INPUT_UNAVAILABLE",
                        error_message="T0行业与GN概念PULSE没有完整精确读回。",
                    )
                continue
            due_targets = {
                int(horizon): date.fromisoformat(target_date)
                for horizon, target_date in due_map[day_text].items()
            }
            target_package_ids = {horizon: source_package_id for horizon in due_targets}
            begin_callback(job_id=job_id, units=units)
            followup = run_source_sector_pulse_followup_day(
                config,
                target_session=day,
                research_cutoff=cutoff,
                source_package_id=source_package_id,
                target_package_ids=target_package_ids,
            )
            persistence = followup.get("persistence", {})
            if (
                followup.get("status") != "PERSISTED"
                or followup.get("independent_transaction_readback") != "PASS"
            ):
                raise DataJobError(
                    "SECTOR_FOLLOWUP_READBACK_INCOMPLETE",
                    "行业与GN概念后继结果未在独立事务中闭合读回。",
                    status=409,
                )
            inserted_by_table = {
                "stock.source_sector_pulse_followup_facts": int(
                    persistence.get("rows_inserted", 0) or 0
                ),
                "stock.source_sector_pulse_cohort_group_facts": int(
                    persistence.get("cohort_group_rows_inserted", 0) or 0
                ),
                "stock.source_sector_pulse_cohort_reason_facts": int(
                    persistence.get("cohort_reason_rows_inserted", 0) or 0
                ),
            }
            expected_by_table = {
                "stock.source_sector_pulse_followup_facts": int(
                    persistence.get("rows_expected", 0) or 0
                ),
                "stock.source_sector_pulse_cohort_group_facts": int(
                    persistence.get("cohort_group_rows_expected", 0) or 0
                ),
                "stock.source_sector_pulse_cohort_reason_facts": int(
                    persistence.get("cohort_reason_rows_expected", 0) or 0
                ),
            }
            for unit in units:
                table = unit["business_table"]
                progress_callback(
                    job_id=job_id,
                    stage_code="SECTOR_FOLLOWUP",
                    target_session=day,
                    business_table=table,
                    source_package_id=source_package_id,
                    source_capture_id=capture_id,
                    profile_history_session_count=None,
                    d1_history_session_count=None,
                    expected_rows=expected_by_table[table],
                    inserted_rows=inserted_by_table[table],
                    quality_status=followup.get("readback_status"),
                    no_change=persistence.get("status") == "IDEMPOTENT_ALREADY_PRESENT",
                    profile_revision=followup.get("input", {}).get("profile_revision"),
                    producer_version="SOURCE_SECTOR_PULSE_FOLLOWUP_CLOSE_R1",
                    calendar_revision=followup.get("input", {}).get(
                        "calendar_revision"
                    ),
                    sector_snapshot_ids=followup.get("input", {}).get(
                        "snapshot_ids", sector_snapshot_ids
                    ),
                    followup_target_sessions=tuple(due_targets.values()),
                )


__all__ = [
    "DataJobCoordinator",
    "DataJobError",
    "build_data_job_plan",
    "recent_official_sessions",
    "select_session_windows",
]
