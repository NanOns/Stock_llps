"""对已冻结来源候选，按单股PG历史逐日观察启动谓词。"""

from stock_a.research.d0_launch_raw import (
    calculate_d0_launch_confirm_raw,
    calculate_d0_recovery_turn_raw,
)
from stock_a.storage.postgres.source_raw import read_raw_daily_history_stock


def observe_frozen_scenario(
    config,
    *,
    symbol,
    target,
    cutoff,
    package_sha256,
    namespace_id,
    target_capture_id,
    calendar,
    horizon,
    scenario="LAUNCH_CONFIRM",
):
    """不扫描全池；恢复谓词缺少横截面RPS时只输出可证明的FALSE或UNKNOWN。"""
    if scenario not in {"LAUNCH_CONFIRM", "RECOVERY_TURN"}:
        raise ValueError("单股后继只支持已冻结的启动或恢复来源诊断")
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
    history_sessions = tuple(calendar[history_start : start + 1])
    history = read_raw_daily_history_stock(
        config,
        source_symbol=symbol,
        target_session=target,
        package_sha256=package_sha256,
        sessions=history_sessions,
        expected_namespace_id=namespace_id,
        research_cutoff=cutoff,
        include_parser_contract=True,
    )
    forward = read_raw_daily_history_stock(
        config,
        source_symbol=symbol,
        target_session=target,
        package_sha256=package_sha256,
        sessions=(target, *future),
        expected_namespace_id=namespace_id,
        research_cutoff=cutoff,
        include_parser_contract=True,
        starts_at_target=True,
    )
    rows = {row["session_date"]: row for row in history["history_rows"]}
    forward_rows = {row["session_date"]: row for row in forward["history_rows"]}
    if rows.get(target) != forward_rows.get(target):
        raise ValueError("两次单股读取的T0来源事实不一致")
    rows.update(forward_rows)
    birth = rows.get(target)
    if birth is None or str(birth["source_capture_id"]) != target_capture_id:
        raise ValueError("单股后继与冻结T0捕获不一致")
    content = birth.get("source_content_id")
    entry = birth.get("source_entry_sha256")
    parser = birth.get("parser_contract")
    if not content or not entry or not parser:
        return {
            "observations": [],
            "first_false": None,
            "first_false_upper_bound": None,
            "status": "UNKNOWN_T0_SOURCE_IDENTITY",
            "formal_episode": "NOT_CREATED",
        }
    safe = {
        day: row
        if row.get("source_content_id") == content
        and row.get("source_entry_sha256") == entry
        else {**row, "reference_quality": False}
        for day, row in rows.items()
    }
    unknown_before = False
    first = upper = recovery = recovery_upper = None
    unknown_before_recovery = False
    observations = []
    for day in future:
        position = calendar.index(day)
        window = tuple(calendar[max(0, position - lookback) : position + 1])
        day_row = safe.get(day)
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
                history_rows_by_session={d: safe.get(d) for d in window},
                source_symbol=symbol,
                package_sha256=package_sha256,
                namespace_id=namespace_id,
                source_content_id=content,
                target_capture_id=capture,
                parser_contract=parser,
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
