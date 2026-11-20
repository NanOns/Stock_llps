"""组合单股单板块的已存跨会话相对位置与并列背景。"""

from __future__ import annotations

from datetime import date
from typing import Any, Mapping

from stock_a.application.source_t0_early_observation import _metric_position
from stock_a.research.entry_context.sessions import _load_calendar
from stock_a.storage.postgres.source_board_relative_history import (
    read_source_board_relative_history_inputs,
)
from stock_a.storage.postgres.source_launch_watch_evidence import (
    read_source_launch_watch_board_rps_comparison,
)


_MAX_HISTORY_SESSIONS = 7


def read_source_board_relative_history(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int,
    source_symbol: str,
    board_kind: str,
    board_native_id: str,
) -> dict[str, Any]:
    """读取前一官方会话至T0后最多五个会话，单独标出T0前态变化。"""
    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or not isinstance(source_symbol, str)
        or len(source_symbol) != 9
        or source_symbol[:3] not in {"SH.", "SZ."}
        or not source_symbol[3:].isdigit()
        or not isinstance(board_kind, str)
        or board_kind not in {"INDUSTRY", "CONCEPT"}
        or not isinstance(board_native_id, str)
        or not board_native_id
        or len(board_native_id) > 64
    ):
        raise ValueError("板内相对位置历史查询键无效")

    _calendar, _metadata, calendar_revision, official_sessions = _load_calendar(
        research_cutoff
    )
    if target_session not in official_sessions:
        raise ValueError("所选T0日期不是内置核验日历中的官方会话")
    target_index = official_sessions.index(target_session)
    first_index = max(0, target_index - 1)
    first_session = official_sessions[first_index]
    sessions = tuple(
        session
        for session in official_sessions[first_index:]
        if session <= research_cutoff
    )[:_MAX_HISTORY_SESSIONS]
    if not sessions or target_session not in sessions or sessions[0] != first_session:
        raise ValueError("所选T0日期没有可读取的官方会话范围")

    inputs = read_source_board_relative_history_inputs(
        config,
        target_session=target_session,
        source_capture_id=source_capture_id,
        source_symbol=source_symbol,
        board_kind=board_kind,
        board_native_id=board_native_id,
        sessions=sessions,
    )
    base = {
        "symbol": source_symbol,
        "target_session": target_session.isoformat(),
        "board_kind": board_kind,
        "board_native_id": board_native_id,
        "membership_mode": inputs.get(
            "membership_mode", "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT"
        ),
        "research_mode": inputs.get("research_mode", "NON_PIT_EXPLORATORY"),
        "price_coordinate": inputs.get("price_coordinate", "FORWARD_ADJUSTED"),
        "calendar_revision": calendar_revision,
        "sessions": [],
        "comparisons": [],
        "t0_comparison": None,
        "t0_comparison_status": "NOT_COMPUTED",
        "t0_comparison_reason": "尚未读取到前一官方会话与目标T0的可比事实",
        "t0_label_policy": "OFFICIAL_PREVIOUS_SESSION_TO_TARGET_ONLY",
        "post_target_comparisons_are_t0_inputs": False,
        "execution": inputs.get(
            "execution",
            {
                "transaction": "REPEATABLE READ READ ONLY",
                "database_write_attempted": False,
                "ddl_executed": False,
                "dml_executed": False,
                "official_sessions_requested": len(sessions),
                "single_symbol_only": True,
                "single_board_only": True,
            },
        ),
    }
    if inputs.get("status") != "AVAILABLE":
        return {
            **base,
            "status": inputs.get("status", "NOT_COMPUTED"),
            "reason": inputs.get("reason", "没有唯一可用的板内成员上下文"),
            "t0_comparison_status": inputs.get("status", "NOT_COMPUTED"),
            "t0_comparison_reason": inputs.get(
                "reason", "没有唯一可用的板内成员上下文"
            ),
            "membership_snapshot_id": inputs.get("membership_snapshot_id"),
            "catalog_id": inputs.get("catalog_id"),
        }

    enriched_days = []
    for day in inputs["days"]:
        current = dict(day)
        current["target_session"] = day["target_session"].isoformat()
        current["relative_time_role"] = (
            "PRIOR_T0_BASELINE"
            if day["target_session"] < target_session
            else "T0_LABEL"
            if day["target_session"] == target_session
            else "POST_T0_CONTEXT"
        )
        current["rps5"] = _unknown_metric("当日RPS5输入没有闭合Seed事实")
        current["rps20"] = _unknown_metric("当日RPS20输入没有闭合Seed事实")
        scope = day.get("seed_scope")
        if day.get("source_status") == "AVAILABLE" and scope is not None:
            comparison = read_source_launch_watch_board_rps_comparison(
                config,
                target_session=day["target_session"],
                source_capture_id=day["source_capture_id"],
                source_symbols=[source_symbol],
                seed_scope=scope,
                board_kind=board_kind,
                board_native_id=board_native_id,
                board_snapshot_id=inputs["membership_snapshot_id"],
            ).get(source_symbol, {})
            for metric_name, result_key in (("RPS5", "rps5"), ("RPS20", "rps20")):
                metric = comparison.get(metric_name)
                if isinstance(metric, Mapping):
                    current[result_key] = {
                        **metric,
                        "position": _metric_position(metric),
                    }
                else:
                    current[result_key] = _unknown_metric(
                        f"{metric_name}板内比较没有返回目标证券"
                    )
        current["source_pool_member_count"] = len(day["source_pool_symbols"])
        current["snapshot_member_count"] = inputs["snapshot_member_count"]
        current.pop("source_pool_symbols", None)
        current.pop("snapshot_members", None)
        current.pop("rps5_member_symbols", None)
        current.pop("rps20_member_symbols", None)
        current.pop("seed_scope", None)
        current.pop("pulse_context", None)
        enriched_days.append(current)

    comparisons = _compare_adjacent_days(
        inputs["days"], enriched_days, official_sessions=official_sessions
    )
    target_iso = target_session.isoformat()
    for comparison in comparisons:
        comparison["relative_time_role"] = (
            "T0_LABEL_INPUT"
            if comparison["to_session"] == target_iso
            else "POST_T0_CONTEXT"
        )
    t0_comparison = next(
        (
            comparison
            for comparison in comparisons
            if comparison["to_session"] == target_iso
        ),
        None,
    )
    return {
        **base,
        "status": "AVAILABLE",
        "membership_snapshot_id": inputs["membership_snapshot_id"],
        "catalog_id": inputs["catalog_id"],
        "hypothesis_version": inputs["hypothesis_version"],
        "snapshot_member_count": inputs["snapshot_member_count"],
        "sessions": enriched_days,
        "comparisons": comparisons,
        "t0_comparison": t0_comparison,
        "t0_comparison_status": (
            "AVAILABLE" if t0_comparison is not None else "NOT_COMPUTED"
        ),
        "t0_comparison_reason": (
            None
            if t0_comparison is not None
            else "前一官方会话或目标T0缺少相邻比较事实"
        ),
    }


def _unknown_metric(reason: str) -> dict[str, Any]:
    return {
        "status": "NOT_COMPUTED",
        "rank_first": None,
        "rank_last": None,
        "valid_member_count": 0,
        "unknown_member_count": None,
        "out_of_source_pool_member_count": None,
        "snapshot_member_count": None,
        "numerator": None,
        "denominator": None,
        "position": "UNKNOWN",
        "reason": reason,
    }


def _scope_reasons(
    previous: Mapping[str, Any],
    current: Mapping[str, Any],
    official_sessions: tuple[date, ...],
) -> list[str]:
    reasons = []
    if (
        previous.get("source_status") != "AVAILABLE"
        or current.get("source_status") != "AVAILABLE"
    ):
        reasons.append("相邻会话没有唯一且闭合的来源捕获")
    previous_scope = previous.get("seed_scope")
    current_scope = current.get("seed_scope")
    if not isinstance(previous_scope, Mapping) or not isinstance(
        current_scope, Mapping
    ):
        reasons.append("相邻会话缺少唯一Seed输入范围")
        return reasons
    for field in (
        "producer_contract",
        "producer_version",
        "calendar_id",
        "calendar_revision",
        "research_mode",
        "price_coordinate",
        "source_time_quality",
        "source_clock_role",
    ):
        if previous_scope.get(field) != current_scope.get(field):
            reasons.append(f"Seed输入{field}发生变化")
    if previous_scope.get("price_coordinate") != "FORWARD_ADJUSTED":
        reasons.append("Seed价格坐标不是FORWARD_ADJUSTED")
    if previous_scope.get("research_mode") != "NON_PIT_RETROSPECTIVE":
        reasons.append("Seed研究模式不是NON_PIT_RETROSPECTIVE")
    if (
        previous.get("membership_snapshot_id") != current.get("membership_snapshot_id")
        or previous.get("catalog_id") != current.get("catalog_id")
        or previous.get("snapshot_members") != current.get("snapshot_members")
    ):
        reasons.append("板块成员快照或完整成员集合发生变化")
    if previous.get("source_pool_symbols") != current.get("source_pool_symbols"):
        reasons.append("来源池证券集合发生变化")
    if not _history_window_contiguous(previous, current, official_sessions):
        reasons.append("Profile历史窗口没有按官方会话连续延长或滚动")
    return list(dict.fromkeys(reasons))


def _history_window_contiguous(
    previous: Mapping[str, Any],
    current: Mapping[str, Any],
    official_sessions: tuple[date, ...],
) -> bool:
    before = previous.get("seed_scope")
    after = current.get("seed_scope")
    if not isinstance(before, Mapping) or not isinstance(after, Mapping):
        return False
    before_count = before.get("history_session_count")
    after_count = after.get("history_session_count")
    before_start = before.get("history_window_start")
    after_start = after.get("history_window_start")
    before_end = before.get("history_window_end")
    after_end = after.get("history_window_end")
    before_session = previous.get("target_session")
    after_session = current.get("target_session")
    if not all(
        (
            type(before_count) is int,
            type(after_count) is int,
            isinstance(before_start, date),
            isinstance(after_start, date),
            before_end == before_session,
            after_end == after_session,
        )
    ):
        return False
    if after_count == before_count + 1 and after_start == before_start:
        return True
    if after_count == before_count:
        session_index = {
            session: index for index, session in enumerate(official_sessions)
        }
        if (
            before_start in session_index
            and after_start in session_index
            and session_index[after_start] == session_index[before_start] + 1
        ):
            return True
    return False


def _compare_adjacent_days(
    raw_days: list[dict[str, Any]],
    display_days: list[dict[str, Any]],
    *,
    official_sessions: tuple[date, ...],
) -> list[dict[str, Any]]:
    comparisons = []
    for index in range(1, len(raw_days)):
        previous = raw_days[index - 1]
        current = raw_days[index]
        global_reasons = _scope_reasons(previous, current, official_sessions)
        result = {
            "from_session": previous["target_session"].isoformat(),
            "to_session": current["target_session"].isoformat(),
            "source_pool_comparable": not global_reasons,
            "reason": "；".join(global_reasons) if global_reasons else None,
        }
        for metric_name, key in (("RPS5", "rps5"), ("RPS20", "rps20")):
            before_metric = display_days[index - 1][key]
            after_metric = display_days[index][key]
            member_key = f"{key}_member_symbols"
            state, reason = _metric_change(
                before_metric,
                after_metric,
                previous.get(member_key, []),
                current.get(member_key, []),
                global_reasons,
            )
            result[key] = {"state": state, "reason": reason}
        comparisons.append(result)
    return comparisons


def _metric_change(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    before_members: list[str],
    after_members: list[str],
    global_reasons: list[str],
) -> tuple[str, str | None]:
    if global_reasons:
        return "NOT_COMPARABLE", "；".join(global_reasons)
    if before_members != after_members:
        return "NOT_COMPARABLE", "该RPS指标的有效板内成员集合发生变化"
    if before.get("status") != "AVAILABLE" or after.get("status") != "AVAILABLE":
        return "UNKNOWN", "相邻会话缺少可评价的RPS值或排名"
    before_first = before.get("rank_first")
    before_last = before.get("rank_last")
    after_first = after.get("rank_first")
    after_last = after.get("rank_last")
    if any(
        type(value) is not int
        for value in (before_first, before_last, after_first, after_last)
    ):
        return "UNKNOWN", "排名区间缺失"
    if after_last < before_first:
        return "IMPROVED", None
    if after_first > before_last:
        return "WORSENED", None
    if before_first == after_first and before_last == after_last:
        return "UNCHANGED", None
    return "UNKNOWN", "前后并列排名区间重叠，不能确定前移或后退"
