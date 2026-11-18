"""数据初始化与手动日更的共享计划器和单后台执行协调器。"""

from __future__ import annotations

import threading
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
}
_STAGE_ORDER = {
    "SOURCE_RAW": 1,
    "MARKET_BREADTH": 2,
    "CORE_PROFILE": 3,
    "MARKET_PARTICIPATION": 4,
    "D0_D1": 5,
    "STATIC_RESEARCH": 6,
    "FOCUS_FORMAL": 7,
}
_RAW_JOB_SCOPE = "STOCK_A_R08_BIZ08_DATA_JOB_RAW_IMPORT_R1"


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
    }
    if set(body) - allowed:
        raise DataJobError("DATA_JOB_INPUT_INVALID", "任务参数包含未登记字段。")
    mode = body.get("mode")
    if mode not in {"INITIALIZE", "MANUAL_DAILY"}:
        raise DataJobError(
            "DATA_JOB_MODE_INVALID", "任务模式须为INITIALIZE或MANUAL_DAILY。"
        )
    return {
        "mode": mode,
        "target_session": body.get("target_session"),
        "history_session_count": body.get("history_session_count"),
        "d0_output_session_count": body.get("d0_output_session_count", 0),
        "d1_output_session_count": body.get("d1_output_session_count", 0),
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
        if profile_history is None:
            profile_history = raw_coverage.get("current_profile_history_session_count")
        if profile_history is None:
            raise DataJobError(
                "DATA_JOB_PROFILE_WINDOW_UNKNOWN",
                "没有唯一已存Core/Profile历史窗口，手动日更暂不可规划。",
                status=409,
            )
        profile_history = _positive_integer(profile_history, "history_session_count")

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
        calendar_id, calendar_revision, all_sessions = _load_official_sessions(target)
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

    if mode == "INITIALIZE":
        requested_target_count = profile_history
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
        raw_window = windows["raw"]
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
        raw_window = recent_official_sessions(
            eligible, through=target, count=profile_history
        )
        d0_count = d0_requested if retrying_daily else len(daily_sessions)
        d1_count = d1_requested if retrying_daily else len(daily_sessions)

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

    d0_sessions = recent_official_sessions(eligible, through=target, count=d0_count)
    d1_sessions = recent_official_sessions(eligible, through=target, count=d1_count)
    if mode == "MANUAL_DAILY" and not retrying_daily:
        d0_sessions = daily_sessions
        d1_sessions = daily_sessions
    raw_window_set = set(raw_window)
    missing_raw = [
        day.isoformat()
        for day in raw_window
        if day.isoformat() not in raw_sessions_in_pg
    ]
    raw_version_by_date = {
        item["session_date"]: item
        for item in raw_coverage.get("closed_versions", ())
        if item["source_package_id"] == active_package_id
    }
    reuse_raw_package_id = (
        active_package_id if active_package_id is not None and not missing_raw else None
    )
    current_d1_window = raw_coverage.get("current_d1_history_session_count")
    d1_window_ambiguous = raw_coverage.get("d1_history_window_ambiguous") is True
    d0_insufficient = [
        day.isoformat()
        for day in d0_sessions
        if day not in raw_window_set
        or not any(previous < day for previous in raw_window_set)
    ]
    d1_insufficient: list[str] = []
    if d1_sessions and current_d1_window is None:
        d1_insufficient = [day.isoformat() for day in d1_sessions]
    elif d1_sessions:
        d1_insufficient = [
            day.isoformat()
            for day in d1_sessions
            if day not in raw_window_set
            or sum(session <= day for session in raw_window) < current_d1_window
        ]

    output_dates = tuple(sorted(set(d0_sessions) | set(d1_sessions)))
    profile_dates = set(output_dates)
    if mode == "INITIALIZE":
        profile_dates.add(target)
    profile_dates = tuple(sorted(profile_dates))
    profile_window_by_date = {
        day: min(profile_history, sum(session <= day for session in raw_window))
        for day in profile_dates
    }
    profile_insufficient = [
        day.isoformat()
        for day, count in profile_window_by_date.items()
        if count < min(CORE_MAX_ACTUAL_LOOKBACK, profile_history)
    ]
    market_sessions = raw_window if mode == "INITIALIZE" else daily_sessions
    market_insufficient = {
        day.isoformat()
        for day in market_sessions
        if sum(session <= day for session in raw_window) < 5
    }
    market_participation_insufficient = {
        day.isoformat()
        for day in profile_dates
        if day.isoformat() in profile_insufficient
        or day.isoformat() in market_insufficient
    }

    units: list[dict[str, Any]] = []
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
    for day in profile_dates:
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
        formal_status = (
            "NOT_COMPUTED"
            if day.isoformat() in d0_insufficient
            or day.isoformat() in profile_insufficient
            else "NOT_STARTED"
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
    if d0_count == 0:
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
    if d1_count == 0:
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

    blockers: list[str] = []
    if not execution_authorized:
        blockers.append("DATA_EXECUTION_NOT_AUTHORIZED")
    if d1_sessions and current_d1_window is None:
        blockers.append(
            "D1_HISTORY_WINDOW_AMBIGUOUS"
            if d1_window_ambiguous
            else "D1_HISTORY_WINDOW_NOT_ESTABLISHED"
        )
    if any(item["status"] == "NOT_COMPUTED" for item in units):
        blockers.append("HISTORY_INSUFFICIENT_FOR_SOME_OUTPUT_DATES")

    executable = execution_authorized
    if not execution_authorized:
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
        "market_breadth_sessions": [day.isoformat() for day in market_sessions],
        "market_breadth_insufficient_history_sessions": sorted(market_insufficient),
        "market_participation_sessions": [day.isoformat() for day in profile_dates],
        "market_participation_insufficient_sessions": sorted(
            market_participation_insufficient
        ),
        "unplanned_producers": [
            {
                "stage_code": "SECTOR_PULSE",
                "status": "NOT_COMPUTED",
                "reason_code": "SECTOR_SNAPSHOT_PAIR_NOT_BOUND",
                "message": "行业与概念脉冲生产需要明确且不同的双快照ID；当前任务表单未绑定该输入。",
            }
        ],
        "d0_insufficient_previous_sessions": d0_insufficient,
        "d1_insufficient_history_sessions": d1_insufficient,
        "profile_insufficient_history_sessions": profile_insufficient,
        "source_coverage": {
            "status": "NOT_CHECKED_DATA_FREEZE"
            if not execution_authorized
            else "PG_SOURCE_REUSE_AVAILABLE"
            if reuse_raw_package_id is not None
            else "CHECKED_AFTER_EXPLICIT_START",
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
    return "|".join(
        (
            plan["mode"],
            plan["target_session"],
            str(plan["history_session_count"]),
            str(plan["d0_output_session_count"]),
            str(plan["d1_output_session_count"]),
            plan["calendar_id"],
            plan["calendar_revision"],
            plan["research_cutoff"],
        )
    )


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
        return {
            "status": "AVAILABLE",
            "execution_authorized": self.execution_authorized,
            "execution_reason": None
            if self.execution_authorized
            else "BLOCKED_NOT_AUTHORIZED",
            "research_cutoff": self.research_cutoff.isoformat(),
            "raw_coverage": coverage,
            "latest_job": latest[0] if latest else None,
            "recent_jobs": data_job_pg.read_recent_jobs(self.config, limit=8),
            "source_request_performed": False,
            "database_write_attempted": False,
        }

    def plan(self, body: Any) -> dict[str, Any]:
        coverage = self._coverage()
        request = _request_fields(body)
        latest_jobs = data_job_pg.read_recent_jobs(self.config, limit=1)
        latest = latest_jobs[0] if latest_jobs else None
        retry_statuses = {"FAILED", "INTERRUPTED", "INCOMPLETE", "BLOCKED"}
        retrying_existing = False
        if (
            latest is not None
            and latest["mode"] == request["mode"]
            and latest["status"] in retry_statuses
        ):
            if request["mode"] == "MANUAL_DAILY":
                retrying_existing = True
            else:
                requested_target = (
                    _parse_session(request["target_session"])
                    if request["target_session"] is not None
                    else _parse_session(coverage["last_stored_session"])
                    if coverage.get("last_stored_session")
                    else self.research_cutoff
                )
                retrying_existing = (
                    request["history_session_count"] == latest["history_session_count"]
                    and request["d0_output_session_count"]
                    == latest["d0_output_session_count"]
                    and request["d1_output_session_count"]
                    == latest["d1_output_session_count"]
                    and requested_target == latest["target_session"]
                )
        if retrying_existing and latest is not None:
            retry_body = {
                "mode": request["mode"],
                "target_session": latest["target_session"].isoformat(),
                "history_session_count": latest["history_session_count"],
                "d0_output_session_count": latest["d0_output_session_count"],
                "d1_output_session_count": latest["d1_output_session_count"],
            }
            plan = build_data_job_plan(
                retry_body,
                cutoff=self.research_cutoff,
                raw_coverage=coverage,
                execution_authorized=self.execution_authorized,
            )
            plan["resume_existing_job"] = True
            plan["resume_job_id"] = str(latest["job_id"])
            retry_package_id = latest.get("source_package_id")
            plan["retry_bound_source_package_id"] = (
                int(retry_package_id) if retry_package_id is not None else None
            )
            if retry_package_id is not None:
                existing = {
                    item["session_date"]: item
                    for item in coverage.get("closed_versions", ())
                    if item["source_package_id"] == int(retry_package_id)
                }
                retry_sessions = plan.get("raw_history_sessions", ())
                retry_inputs = [
                    existing[day] for day in retry_sessions if day in existing
                ]
                if len(retry_inputs) == len(retry_sessions):
                    plan["reuse_raw_package_id"] = int(retry_package_id)
                    plan["raw_session_inputs"] = retry_inputs
                    plan["missing_raw_sessions"] = []
                    plan["raw_history_sessions_in_pg"] = len(retry_sessions)
                    plan["source_coverage"]["active_pg_source_package_id"] = int(
                        retry_package_id
                    )
                    plan["source_coverage"]["status"] = (
                        "NOT_CHECKED_DATA_FREEZE"
                        if not self.execution_authorized
                        else "PG_SOURCE_REUSE_AVAILABLE"
                    )
                else:
                    plan["source_coverage"]["active_pg_source_package_id"] = int(
                        retry_package_id
                    )
                    plan["source_coverage"]["status"] = (
                        "NOT_CHECKED_DATA_FREEZE"
                        if not self.execution_authorized
                        else "RETRY_BOUND_SOURCE_VERSION_REQUIRED"
                    )
            return plan
        return build_data_job_plan(
            body,
            cutoff=self.research_cutoff,
            raw_coverage=coverage,
            execution_authorized=self.execution_authorized,
        )

    def read_job(self, job_id: UUID | None = None) -> dict[str, Any] | None:
        return data_job_pg.read_job(self.config, job_id=job_id)

    def recent_jobs(self) -> list[dict[str, Any]]:
        return data_job_pg.read_recent_jobs(self.config, limit=8)

    def start(self, body: Any) -> dict[str, Any]:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                latest = data_job_pg.read_job(self.config)
                if latest is not None and latest.get("status") == "RUNNING":
                    requested_mode = (
                        body.get("mode") if isinstance(body, dict) else None
                    )
                    if requested_mode != latest["mode"]:
                        raise DataJobError(
                            "DATA_JOB_BUSY",
                            "已有不同类型的数据生产任务运行中；请先查看该任务进度。",
                            status=409,
                        )
                    return {
                        "accepted": True,
                        "status": "RUNNING",
                        "job": latest,
                        "reused": True,
                        "requested_mode_matches": requested_mode == latest["mode"],
                    }
        plan = self.plan(body)
        if not plan.get("executable"):
            return {"accepted": False, "plan": plan}
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                latest = data_job_pg.read_job(self.config)
                if latest is not None:
                    requested_mode = (
                        body.get("mode") if isinstance(body, dict) else None
                    )
                    if requested_mode != latest["mode"]:
                        raise DataJobError(
                            "DATA_JOB_BUSY",
                            "已有不同类型的数据生产任务运行中；请先查看该任务进度。",
                            status=409,
                        )
                    return {
                        "accepted": True,
                        "status": "RUNNING",
                        "job": latest,
                        "reused": True,
                        "requested_mode_matches": requested_mode == latest["mode"],
                    }
            task_id = uuid4()
            job = {
                "job_id": task_id,
                "request_key": _job_request_key(plan),
                "mode": plan["mode"],
                "target_session": date.fromisoformat(plan["target_session"]),
                "research_cutoff": self.research_cutoff,
                "history_session_count": plan["history_session_count"],
                "d0_output_session_count": plan["d0_output_session_count"],
                "d1_output_session_count": plan["d1_output_session_count"],
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
            if day_text not in set(plan["market_participation_insufficient_sessions"]):
                market_units = [
                    unit
                    for unit in plan["stage_units"]
                    if unit["stage_code"] == "MARKET_PARTICIPATION"
                    and unit["target_session"] == day
                    and unit["status"] == "NOT_STARTED"
                ]
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
                except (
                    SourceMarketContextError,
                    SourceMarketParticipationError,
                ) as error:
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
                else:
                    participation_persistence = participation["persistence"]
                    participation_readback = participation["readback"]
                    unknown_reasons = participation_readback.get("denominator", {}).get(
                        "unknown_reason_counts", {}
                    )
                    participation_inserted = int(
                        participation_persistence.get("inserted_reason_rows", 0) or 0
                    )
                    for table, expected_rows, inserted_rows in (
                        (
                            "stock.source_market_participation_days",
                            1,
                            int(participation_persistence.get("status") == "WRITTEN"),
                        ),
                        (
                            "stock.source_market_participation_unknown_reasons",
                            len(unknown_reasons),
                            participation_inserted,
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
                            quality_status=participation_readback.get("status"),
                            no_change=participation_persistence.get("status")
                            == "IDEMPOTENT_ALREADY_PRESENT",
                            profile_revision=participation["profile_revision"],
                            producer_version=participation["producer_version"],
                            calendar_revision=participation["calendar_revision"],
                        )

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

        if day_text in d0_days and day_text not in profile_blocked:
            static_units = [
                unit
                for unit in plan["stage_units"]
                if unit["stage_code"] == "STATIC_RESEARCH"
                and unit["target_session"] == day
                and unit["status"] == "NOT_STARTED"
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
                    calendar_revision=static_persistence.get(
                        "calendar_revisions", {}
                    ).get(key, [None])[0],
                )

            formal_unit = next(
                unit
                for unit in plan["stage_units"]
                if unit["stage_code"] == "FOCUS_FORMAL"
                and unit["target_session"] == day
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
                        formal_range.get("reason_code")
                        or "FORMAL_ADMISSION_NOT_COMPUTED"
                    ),
                    error_message="已保存D0准入输入未闭合，Focus准入结果保持未计算。",
                )
                continue
            formal = formal_reports[0]
            progress_callback(
                job_id=job_id,
                stage_code="FOCUS_FORMAL",
                target_session=day,
                business_table="stock.source_d0_formal_admission_day_facts",
                source_package_id=source_package_id,
                source_capture_id=capture_by_session[day_text],
                profile_history_session_count=history_count,
                d1_history_session_count=effective_d1_window,
                expected_rows=int(formal["source_count"]),
                inserted_rows=int(formal["inserted_rows"]),
                quality_status=formal.get("status"),
                no_change=formal.get("idempotent_rerun") is True,
                profile_revision=formal.get("profile_revision"),
                producer_version=report["d0"]["producer_version"],
                calendar_revision=report["calendar"]["calendar_revision"],
            )


__all__ = [
    "DataJobCoordinator",
    "DataJobError",
    "build_data_job_plan",
    "recent_official_sessions",
    "select_session_windows",
]
