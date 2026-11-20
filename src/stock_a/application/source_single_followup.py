"""对已冻结来源候选，按单股BaoStock QFQ历史逐日观察启动谓词。"""

from stock_a.research.d0_launch_raw import (
    calculate_d0_launch_confirm_raw,
    calculate_d0_recovery_turn_raw,
)
from stock_a.storage.postgres.baostock_price_facts import (
    QFQ_COORDINATE,
    read_baostock_capture_roster,
    read_baostock_price_history_batch,
)


def observe_frozen_scenario(
    config,
    *,
    symbol,
    target,
    cutoff,
    source_capture_id,
    target_capture_id,
    calendar,
    horizon,
    scenario="LAUNCH_CONFIRM",
):
    """只读同一paired BaoStock捕获；恢复谓词缺少横截面RPS时保留未知。"""
    if scenario not in {"LAUNCH_CONFIRM", "RECOVERY_TURN"}:
        raise ValueError("单股后继只支持已冻结的启动或恢复来源诊断")
    if source_capture_id != target_capture_id:
        raise ValueError("单股后继与冻结T0必须绑定同一BaoStock捕获")

    start = calendar.index(target)
    end = min(start + horizon, len(calendar) - 1)
    future = tuple(day for day in calendar[start + 1 : end + 1] if day <= cutoff)
    if not future:
        return {
            "observations": [],
            "first_false": None,
            "first_false_upper_bound": None,
            "first_recovery_after_false": None,
            "first_recovery_after_false_upper_bound": None,
            "status": "NO_FOLLOWUP_SESSION",
            "formal_episode": "NOT_CREATED",
        }

    lookback = 20 if scenario == "LAUNCH_CONFIRM" else 24
    history_start = max(0, start - lookback)
    read_end = calendar.index(future[-1])
    read_sessions = tuple(calendar[history_start : read_end + 1])
    roster = read_baostock_capture_roster(
        config, source_capture_id=source_capture_id, cutoff=cutoff
    )
    roster_symbol = next(
        (
            row
            for row in roster.get("symbols", ())
            if row.get("source_symbol") == symbol
        ),
        None,
    )
    if roster.get("status") != "AVAILABLE" or roster_symbol is None:
        return {
            "observations": [],
            "first_false": None,
            "first_false_upper_bound": None,
            "first_recovery_after_false": None,
            "first_recovery_after_false_upper_bound": None,
            "status": "UNKNOWN_T0_SOURCE_IDENTITY",
            "formal_episode": "NOT_CREATED",
        }
    if roster_symbol.get("request_complete") is not True:
        return {
            "observations": [],
            "first_false": None,
            "first_false_upper_bound": None,
            "first_recovery_after_false": None,
            "first_recovery_after_false_upper_bound": None,
            "status": "UNKNOWN_SOURCE_CAPTURE_INCOMPLETE",
            "formal_episode": "NOT_CREATED",
        }

    forward = read_baostock_price_history_batch(
        config,
        source_symbols=(symbol,),
        sessions=read_sessions,
        source_capture_id=source_capture_id,
        price_coordinate=QFQ_COORDINATE,
        cutoff=cutoff,
        offset=0,
        limit=len(read_sessions),
    )
    if (
        forward.get("status") != "AVAILABLE"
        or forward.get("source_capture_id") != source_capture_id
        or forward.get("price_coordinate") != QFQ_COORDINATE
        or forward.get("page", {}).get("next_offset") is not None
    ):
        raise ValueError("单股后继QFQ价格未从PG一次完整读回")

    rows = {
        row["session_date"]: row
        for row in forward.get("rows", ())
        if row.get("source_symbol") == symbol
    }
    birth = rows.get(target)
    if birth is None or birth.get("source_capture_id") != target_capture_id:
        return {
            "observations": [],
            "first_false": None,
            "first_false_upper_bound": None,
            "first_recovery_after_false": None,
            "first_recovery_after_false_upper_bound": None,
            "status": "UNKNOWN_T0_SOURCE_IDENTITY",
            "formal_episode": "NOT_CREATED",
        }

    unknown_before = False
    first = upper = recovery = recovery_upper = None
    unknown_before_recovery = False
    observations = []
    for day in future:
        position = calendar.index(day)
        window = tuple(calendar[max(0, position - lookback) : position + 1])
        day_row = rows.get(day)
        capture = day_row.get("source_capture_id") if day_row else None
        if capture is None:
            state, reason = "UNKNOWN", "TARGET_SOURCE_CAPTURE_MISSING"
        else:
            calculate = (
                calculate_d0_launch_confirm_raw
                if scenario == "LAUNCH_CONFIRM"
                else calculate_d0_recovery_turn_raw
            )
            extra = {} if scenario == "LAUNCH_CONFIRM" else {"rps5_delta3": None}
            result = calculate(
                target_session=day,
                official_sessions=window,
                history_rows_by_session={
                    session: rows.get(session) for session in window
                },
                source_symbol=symbol,
                source_capture_id=source_capture_id,
                price_coordinate=QFQ_COORDINATE,
                **extra,
            )
            state, reason = result["scenario_raw"], result["main_reason"]
        if upper is None and state == "UNKNOWN":
            unknown_before = True
        if recovery_upper is None and state == "UNKNOWN":
            unknown_before_recovery = True
        if state == "FALSE" and upper is None:
            upper = day.isoformat()
            if not unknown_before:
                first = upper
        if upper is not None and recovery_upper is None and state == "TRUE":
            recovery_upper = day.isoformat()
            if not unknown_before_recovery:
                recovery = recovery_upper
        observations.append(
            {"session": day.isoformat(), "state": state, "reason": reason}
        )
    return {
        "scope": "SOURCE_DIAGNOSTIC_NON_PIT_NOT_FORMAL_INVALIDATION",
        "scenario": scenario,
        "input_limit": []
        if scenario == "LAUNCH_CONFIRM"
        else ["PER_SESSION_CROSS_SECTION_RPS_NOT_READ"],
        "observations": observations,
        "first_false": first,
        "first_false_upper_bound": upper,
        "status": "OBSERVED"
        if first
        else "UNKNOWN"
        if unknown_before
        else "NOT_OBSERVED",
        "first_recovery_after_false": recovery,
        "first_recovery_after_false_upper_bound": recovery_upper,
        "formal_episode": "NOT_CREATED",
    }
