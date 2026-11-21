"""T0 RPS位次变化分组的已存事实离线生产入口。"""

from __future__ import annotations

from datetime import date
import re
from typing import Any, Mapping

from stock_a.contracts.profile_revision import PROFILE_REVISION_PATTERN
from stock_a.research.entry_context.sessions import _load_calendar
from stock_a.storage.postgres.source_rps_pulse_groups import (
    persist_source_rps_pulse_t0_change_facts,
)


def persist_source_rps_pulse_t0_groups_from_saved(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int,
    history_session_count: int | None = None,
    profile_revision: str | None = None,
) -> dict[str, Any]:
    """仅用精确T0、前态和板块关系PG事实生产变化标签，不采集行情。"""
    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or history_session_count is not None
        and (
            type(history_session_count) is not int
            or not 1 <= history_session_count <= 250
        )
        or profile_revision is not None
        and (
            history_session_count is None
            or not isinstance(profile_revision, str)
            or re.fullmatch(PROFILE_REVISION_PATTERN, profile_revision) is None
        )
    ):
        raise ValueError("RPS×PULSE目标日、截止日或BaoStock来源捕获ID无效")
    _, _, _, official_sessions = _load_calendar(research_cutoff)
    if target_session not in official_sessions:
        raise ValueError("RPS×PULSE目标日不是内置官方交易会话")
    index = official_sessions.index(target_session)
    if index == 0:
        raise ValueError("目标T0没有可比较的前一官方交易会话")
    previous_session = official_sessions[index - 1]
    result = persist_source_rps_pulse_t0_change_facts(
        config,
        target_session=target_session,
        previous_session=previous_session,
        research_cutoff=research_cutoff,
        source_capture_id=source_capture_id,
        official_sessions=official_sessions,
        history_session_count=history_session_count,
        profile_revision=profile_revision,
    )
    result["official_previous_session"] = previous_session.isoformat()
    result["source_read_policy"] = "PG_TYPED_FACTS_ONLY_NO_SOURCE_FILE_READ"
    return result
