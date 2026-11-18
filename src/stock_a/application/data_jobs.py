"""数据初始化与手动日更的共享计划器和单后台执行协调器。"""

from __future__ import annotations

import threading
import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any
from uuid import UUID, uuid4

from stock_a.research.entry import _load_calendar
from stock_a.research.entry_context.sessions import CORE_MAX_ACTUAL_LOOKBACK
from stock_a.storage.postgres import data_jobs as data_job_pg


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
    "SECTOR_FOLLOWUP": (
        "stock.source_sector_pulse_followup_facts",
        "stock.source_sector_pulse_cohort_group_facts",
        "stock.source_sector_pulse_cohort_reason_facts",
    ),
}
_STAGE_ORDER = {
    "SOURCE_RAW": 1,
    "SOURCE_BAOSTOCK": 2,
    "MARKET_BREADTH": 3,
    "CORE_PROFILE": 4,
    "MARKET_PARTICIPATION": 5,
    "D0_D1": 6,
    "STATIC_RESEARCH": 7,
    "FOCUS_FORMAL": 8,
    "SECTOR_PULSE": 9,
    "SECTOR_FOLLOWUP": 10,
}
_RAW_JOB_SCOPE = "STOCK_A_R08_BIZ08_DATA_JOB_RAW_IMPORT_R1"

_MODULES = {
    "TDX_RAW": ("SOURCE", "TDX官方Raw来源", True, True),
    "BAOSTOCK": ("SOURCE", "BaoStock补充来源", True, False),
    "CORE_PROFILE": ("BASE", "Core / Profile", True, True),
    "MARKET_BREADTH": ("BASE", "市场宽度", True, True),
    "MARKET_PARTICIPATION": ("BASE", "成交参与", True, True),
    "D0": ("RESEARCH", "D0 / Seed研究", True, True),
    "D1": ("RESEARCH", "D1结构与材料变化", True, True),
    "FOCUS_FORMAL": ("RESEARCH", "Focus正式准入", True, True),
    "SECTOR_PULSE": ("RESEARCH", "行业 / GN概念PULSE", True, True),
    "SECTOR_FOLLOWUP": ("RESEARCH", "板块跨日跟踪", True, True),
}
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
        "d0_output_session_count",
        "d1_output_session_count",
        "source_modules",
        "base_modules",
        "research_modules",
        "module_output_session_counts",
        "output_session_count",
        "operation",
    }
    if set(body) - allowed:
        raise DataJobError("DATA_JOB_INPUT_INVALID", "任务参数包含未登记字段。")
    mode = body.get("mode")
    if mode not in {"INITIALIZE", "MANUAL_DAILY"}:
        raise DataJobError(
            "DATA_JOB_MODE_INVALID", "任务模式须为INITIALIZE或MANUAL_DAILY。"
        )
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
    if "BAOSTOCK" in modules:
        raise DataJobError(
            "DATA_JOB_MODULE_UNAVAILABLE",
            "BaoStock单次手动采集入口受NOT_REQUESTED策略与请求预算限制，当前全市场作业不可启动。",
            status=409,
        )
    return {
        "mode": mode,
        "target_session": body.get("target_session"),
        "history_session_count": body.get("history_session_count"),
        "d0_output_session_count": body.get("d0_output_session_count", 0),
        "d1_output_session_count": body.get("d1_output_session_count", 0),
        "source_modules": selections["source"],
        "base_modules": selections["base"],
        "research_modules": selections["research"],
        "selected_modules": sorted(modules),
        "module_output_session_counts": counts,
        "output_session_count": body.get("output_session_count"),
        "operation": operation,
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
    if operation not in {"FILL_MISSING"}:
        return chosen
    completed = raw_coverage.get("completed_sessions_by_module", {})
    if not isinstance(completed, Mapping):
        completed = {}
    completed_dates = {str(item) for item in completed.get(module, ())}
    return tuple(
        session for session in chosen if session.isoformat() not in completed_dates
    )


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
) -> dict[str, Any]:
    """按官方日历与截止日内闭合PG Raw目录生成不写数据的任务计划。"""
    request = _request_fields(body)
    if type(cutoff) is not date:
        raise DataJobError("DATA_JOB_CUTOFF_REQUIRED", "服务必须绑定明确研究截止日。")
    mode = request["mode"]
    latest_raw = raw_coverage.get("last_stored_session")
    profile_history = request["history_session_count"]
    retrying_daily = mode == "MANUAL_DAILY" and request["target_session"] is not None
    if mode == "INITIALIZE":
        profile_history = _positive_integer(profile_history, "history_session_count")
    else:
        needs_profile_window = bool(
            {"CORE_PROFILE", "MARKET_PARTICIPATION", "D0", "FOCUS_FORMAL"}
            & set(request["selected_modules"])
        )
        if profile_history is None:
            profile_history = raw_coverage.get("current_profile_history_session_count")
        if profile_history is None and not needs_profile_window:
            profile_history = 1
        if profile_history is None:
            raise DataJobError(
                "DATA_JOB_PROFILE_WINDOW_UNKNOWN",
                "没有唯一已存Core/Profile历史窗口，手动日更暂不可规划。",
                status=409,
            )
        profile_history = _positive_integer(profile_history, "history_session_count")

    output_counts = request["module_output_session_counts"]
    default_output_count = request["output_session_count"]
    if default_output_count is not None:
        default_output_count = _positive_integer(
            default_output_count, "output_session_count"
        )
    selected_modules = set(request["selected_modules"])
    output_count_fallback = (
        default_output_count if default_output_count is not None else 1
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
        output_counts.get("TDX_RAW", profile_history)
        if mode == "INITIALIZE"
        else output_counts.get(
            "TDX_RAW", default_output_count if default_output_count is not None else 1
        )
    )
    if "TDX_RAW" not in selected_source_modules:
        raw_request_count = 1
    raw_request_count = _positive_integer(raw_request_count, "TDX Raw输出会话数")
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
                profile_history
                if module in {"CORE_PROFILE", "MARKET_BREADTH", "MARKET_PARTICIPATION"}
                else d0_count or d1_count or 1
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

    current_d1_window = raw_coverage.get("current_d1_history_session_count")
    d1_window_ambiguous = raw_coverage.get("d1_history_window_ambiguous") is True
    required_raw_sessions: set[date] = set(raw_window)
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
    reusable_packages = [
        package_id
        for package_id in package_order
        if required_iso.issubset(package_sessions[package_id])
    ]
    reuse_raw_package_id = reusable_packages[0] if reusable_packages else None
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
    completed_by_module = raw_coverage.get("completed_sessions_by_module", {})
    if not isinstance(completed_by_module, Mapping):
        completed_by_module = {}
    profile_ready_sessions = {
        str(item) for item in raw_coverage.get("profile_completed_sessions", ())
    } | {item.isoformat() for item in profile_dates}
    d0_ready_sessions = set(completed_by_module.get("D0", ())) | {
        item.isoformat() for item in d0_sessions
    }
    breadth_ready_sessions = (
        set(completed_by_module.get("MARKET_BREADTH", ()))
        | {item.isoformat() for item in market_sessions}
        | {
            str(item)
            for item in raw_coverage.get("market_breadth_completed_sessions", ())
        }
    )
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
    sector_pulse_insufficient = [
        day.isoformat()
        for day in module_output_sessions.get("SECTOR_PULSE", ())
        if not source_will_fill_raw and day.isoformat() in raw_input_unavailable
    ]
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
        "history_session_count": profile_history,
        "d0_output_session_count": d0_count,
        "d1_output_session_count": d1_count,
        "d1_calculation_session_count": current_d1_window,
        "calendar_id": calendar_id,
        "calendar_revision": calendar_revision,
        "raw_history_sessions": [day.isoformat() for day in raw_window],
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
        "market_participation_sessions": [
            day.isoformat() for day in participation_sessions
        ],
        "market_breadth_sessions": [day.isoformat() for day in market_sessions],
        "sector_pulse_sessions": [
            day.isoformat() for day in module_output_sessions.get("SECTOR_PULSE", ())
        ],
        "sector_followup_sessions": [
            day.isoformat() for day in module_output_sessions.get("SECTOR_FOLLOWUP", ())
        ],
        "sector_followup_due_targets": {
            t0: {
                str(horizon): endpoint.isoformat() for horizon, endpoint in due.items()
            }
            for t0, due in sector_followup_due.items()
        },
        "sector_pulse_insufficient_sessions": sector_pulse_insufficient,
        "sector_followup_insufficient_sessions": sorted(sector_followup_insufficient),
        "module_output_sessions": {
            **{
                module: [day.isoformat() for day in sessions]
                for module, sessions in module_output_sessions.items()
            },
            **(
                {"TDX_RAW": [day.isoformat() for day in raw_window]}
                if "TDX_RAW" in selected_source_modules
                else {}
            ),
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
        "d0_output_session_count": plan["d0_output_session_count"],
        "d1_output_session_count": plan["d1_output_session_count"],
        "calendar_id": plan["calendar_id"],
        "calendar_revision": plan["calendar_revision"],
        "research_cutoff": plan["research_cutoff"],
        "selected_modules": plan.get("selected_modules", []),
        "module_output_session_counts": plan.get("module_output_session_counts", {}),
        "operation": plan.get("operation", "FILL_MISSING"),
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
            ][-60:]
        except DataJobError:
            target_sessions = list(coverage.get("closed_sessions", ()))
        modules = []
        for code, (layer, label, implemented, available) in _MODULES.items():
            reason = None
            if code == "BAOSTOCK":
                reason = "请求策略为NOT_REQUESTED，当前批量来源采集不允许启动。"
            elif code == "SECTOR_PULSE" and sector_pair.get("status") != "AVAILABLE":
                reason = sector_pair.get("reason_code") or "行业与概念快照对不可用。"
            cleanup_allowed = code in {
                "MARKET_BREADTH",
                "D1",
                "MARKET_PARTICIPATION",
            }
            modules.append(
                {
                    "code": code,
                    "label": label,
                    "layer": layer,
                    "implemented": implemented,
                    "available": available
                    and (
                        code != "SECTOR_PULSE"
                        or sector_pair.get("status") == "AVAILABLE"
                    ),
                    "reason": reason,
                    "cleanup_allowed": cleanup_allowed,
                    "default_selected": code
                    in {"TDX_RAW", "CORE_PROFILE", "MARKET_BREADTH"},
                    "default_output_session_count": 1,
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
        sector_pair = {"status": "NOT_REQUESTED", "snapshot_ids": None}
        if {"SECTOR_PULSE", "SECTOR_FOLLOWUP"} & set(request["selected_modules"]):
            from stock_a.application.source_sector_pulse import (
                resolve_source_sector_snapshot_pair,
            )

            sector_pair = resolve_source_sector_snapshot_pair(self.config)
        plan = build_data_job_plan(
            body,
            cutoff=self.research_cutoff,
            raw_coverage=coverage,
            execution_authorized=self.execution_authorized,
        )
        plan["sector_snapshot_pair"] = sector_pair
        plan["sector_snapshot_ids"] = sector_pair.get("snapshot_ids")
        return self._complete_plan(plan, request, coverage, sector_pair)

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
        output_counts = dict(request["module_output_session_counts"])
        for module in request["selected_modules"]:
            if module in output_counts:
                continue
            if module == "TDX_RAW":
                output_counts[module] = max(
                    1, int(plan.get("history_session_count") or 1)
                )
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
                    int(
                        request.get("output_session_count")
                        or plan.get("history_session_count")
                        or 1
                    ),
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
        plan["source_version_fingerprint"] = hashlib.sha256(
            json.dumps(source_identity, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
        ).hexdigest()
        if "SECTOR_PULSE" in request["selected_modules"]:
            pulse_days = plan.get("sector_pulse_sessions", ())
            unavailable_days = set(plan.get("sector_pulse_insufficient_sessions", ()))
            for day in pulse_days:
                day_status = (
                    "NOT_COMPUTED"
                    if sector_pair.get("status") != "AVAILABLE"
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
                        if sector_pair.get("status") != "AVAILABLE"
                        else "目标日Raw不在已存完整来源包中；保留未计算。"
                    )
                    if day_status == "NOT_COMPUTED"
                    else None,
                )
            plan["planned_unit_count"] = len(plan["stage_units"])
            if sector_pair.get("status") != "AVAILABLE":
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
        if not plan.get("executable"):
            return {"accepted": False, "plan": plan}
        request_key = _job_request_key(plan)
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
                "d0_output_session_count": plan["d0_output_session_count"],
                "d1_output_session_count": plan["d1_output_session_count"],
                "operation": plan["operation"],
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
        try:
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
                    return
            _execute_shared_production_chain(
                config=self.config,
                job_id=job_id,
                plan=plan,
                progress_callback=self._record_verified_unit,
                begin_callback=self._begin_units,
            )
            data_job_pg.finish_job(self.config, job_id=job_id)
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

    def _begin_units(self, *, job_id: UUID, units: Sequence[Mapping[str, Any]]) -> None:
        data_job_pg.mark_progress_running_batch(self.config, job_id=job_id, units=units)

    def _record_verified_unit(
        self,
        *,
        job_id: UUID,
        stage_code: str,
        target_session: date,
        business_table: str,
        source_package_id: int,
        source_capture_id: int | None,
        profile_history_session_count: int | None,
        d1_history_session_count: int | None,
        expected_rows: int | None,
        inserted_rows: int,
        quality_status: str | None,
        no_change: bool = False,
        profile_revision: str | None = None,
        producer_version: str | None = None,
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
            profile_history_session_count=profile_history_session_count,
            d1_history_session_count=d1_history_session_count,
            profile_revision=profile_revision,
            producer_version=producer_version,
            calendar_revision=calendar_revision,
            sector_snapshot_ids=sector_snapshot_ids,
            followup_target_sessions=followup_target_sessions,
            research_cutoff=self.research_cutoff,
        )
        if not running:
            previous = data_job_pg.read_progress_unit(
                self.config,
                job_id=job_id,
                stage_code=stage_code,
                target_session=target_session,
                business_table=business_table,
            )
            if previous is None or previous.get("verified_rows") != actual_rows:
                raise DataJobError(
                    "DATA_JOB_READBACK_CHANGED",
                    "已核验进度对应的PG行数发生变化；任务需要复核。",
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
            no_change=no_change,
        )


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

    reuse_package_id = plan.get("reuse_raw_package_id")
    raw_session_inputs = plan.get("raw_session_inputs", [])
    if reuse_package_id is not None and len(raw_session_inputs) == len(raw_sessions):
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
        package = download_vipdata_package(observation, budgets=download_budgets)
        try:
            raw_result = import_verified_vipdata_history(
                config,
                package,
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
            cleanup_vipdata_package(package)

    source_package_id = int(raw_result["source_package_id"])
    capture_by_session = {
        item["target_session"]: int(item["source_capture_id"])
        for item in raw_result["session_results"]
    }
    raw_count_by_session = {
        item["target_session"]: int(item["candidate_total"])
        for item in raw_result["session_results"]
    }
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
        history_count = sum(session <= day for session in raw_sessions)
        if history_count < 1:
            continue
        if day_text in profile_days:
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
            effective_d1_window
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
                expected_rows = None
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
        history_count = sum(session <= day for session in raw_sessions)
        if history_count > 0:
            produce_participation(day_text, history_count)

    for day_text in sorted(d0_days):
        if day_text in profile_blocked:
            continue
        day = date.fromisoformat(day_text)
        history_count = sum(session <= day for session in raw_sessions)
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
