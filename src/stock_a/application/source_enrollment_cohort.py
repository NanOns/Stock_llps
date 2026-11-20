"""为Focus/Cohort提供同T0来源登记类别的已存后继结果。"""

from __future__ import annotations

from datetime import date
from typing import Any, Mapping

from stock_a.research.entry_context.errors import ResearchEntryError
from stock_a.research.entry_context.sessions import _load_calendar
from stock_a.storage.postgres.cohort_enrollment import (
    read_cohort_enrollment_lifecycle_groups,
)
from stock_a.storage.postgres.research_facts import RULES


def read_source_enrollment_cohort(
    config: Mapping[str, Any],
    *,
    target: date,
    research_cutoff: date,
    source_capture_id: int,
    profile_revision: str,
    history_session_count: int,
    d1_history_session_count: int,
    event_window_start: date,
    rule: str,
) -> dict[str, Any]:
    """只接受精确规则键，并把价格读取限制在显式截止内官方会话。"""
    if (
        type(target) is not date
        or type(research_cutoff) is not date
        or target > research_cutoff
        or rule not in RULES
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or type(history_session_count) is not int
        or type(d1_history_session_count) is not int
        or type(event_window_start) is not date
    ):
        raise ValueError("来源登记Cohort请求键无效")
    _, _, calendar_revision, all_sessions = _load_calendar(research_cutoff)
    saved_sessions = tuple(
        session for session in all_sessions if session <= research_cutoff
    )
    if target not in saved_sessions:
        raise ResearchEntryError(
            "TARGET_NOT_TRADING_SESSION", "目标日不在截止内官方会话范围"
        )
    index = saved_sessions.index(target)
    horizon_sessions = saved_sessions[index + 1 : index + 6]
    previous_session = saved_sessions[index - 1] if index else None
    result = read_cohort_enrollment_lifecycle_groups(
        config,
        target=target,
        research_cutoff=research_cutoff,
        source_capture_id=source_capture_id,
        profile_revision=profile_revision,
        history_session_count=history_session_count,
        d1_history_session_count=d1_history_session_count,
        event_window_start=event_window_start,
        calendar_revision=calendar_revision,
        rule=rule,
        horizon_sessions=horizon_sessions,
        previous_session=previous_session,
    )
    result["official_calendar_revision"] = calendar_revision
    result["independent_date_status"] = "INSUFFICIENT_INDEPENDENT_DATES"
    return result
