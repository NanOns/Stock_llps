"""严格前态来源观察；诊断父事实永不构成正式事件。"""

from fractions import Fraction

from stock_a.research.d0_launch_raw import (
    _raw_amount,
    _window_row,
    calculate_d0_launch_confirm_raw,
    calculate_d0_recovery_turn_raw,
    fraction_payload,
)


def observe_prior_shapes(
    *, sessions, rows, symbol, package, namespace, content, parser, historical_rps=None
):
    """逐个过去启动观察计算旧峰；目标日及后续行情不参与父或峰的选择。"""
    sessions = tuple(sessions)
    if not sessions or tuple(sorted(set(sessions))) != sessions:
        raise ValueError("历史观察需要有序唯一的官方会话")
    target = sessions[-1]
    if any(row.get("session_date") not in sessions for row in rows):
        raise ValueError("历史行不得超出所选官方会话")
    by_day = {row["session_date"]: row for row in rows}
    if len(by_day) != len(rows):
        raise ValueError("同一历史会话不得存在重复来源行")
    entries = {row.get("source_entry_sha256") for row in rows}
    same_entry = len(entries) == 1 and all(entries)
    checked = []
    for day in sessions:
        row = by_day.get(day)
        _, values, error = _window_row(
            day,
            by_day,
            source_symbol=symbol,
            package_sha256=package,
            namespace_id=namespace,
            source_content_id=content,
            target_capture_id=row.get("source_capture_id") if row else "",
            parser_contract=parser,
        )
        if not same_entry:
            values, error = None, "RAW_SOURCE_ENTRY_MISMATCH"
        checked.append((row, values, error))

    def pred(value, formula, **inputs):
        return {
            "state": "UNKNOWN" if value is None else "TRUE" if value else "FALSE",
            "formula": formula,
            "inputs": {
                k: fraction_payload(v) if isinstance(v, Fraction) else v
                for k, v in inputs.items()
            },
        }

    historical = []
    candidates = []
    last = len(sessions) - 1
    for index in range(max(20, last - 20), last):
        if any(error for _, _, error in checked[index - 20 : index + 1]):
            historical.append(
                {
                    "session": sessions[index].isoformat(),
                    "launch": "UNKNOWN",
                    "recovery": "UNKNOWN",
                    "reason": "STRICT_INPUT_UNAVAILABLE",
                }
            )
            continue
        args = dict(
            target_session=sessions[index],
            official_sessions=sessions[index - 20 : index + 1],
            history_rows_by_session=by_day,
            source_symbol=symbol,
            package_sha256=package,
            namespace_id=namespace,
            source_content_id=content,
            target_capture_id=checked[index][0]["source_capture_id"],
            parser_contract=parser,
        )
        launch = calculate_d0_launch_confirm_raw(**args)
        recovery_args = dict(args)
        recovery_args["official_sessions"] = sessions[max(0, index - 24) : index + 1]
        rps = (historical_rps or {}).get(sessions[index])
        recovery = calculate_d0_recovery_turn_raw(**recovery_args, rps5_delta3=rps)
        needs_rps = rps is None and all(
            item["state"] == "TRUE"
            for key, item in recovery["predicates"].items()
            if key != "rps5_delta3"
        )
        historical.append(
            {
                "session": sessions[index].isoformat(),
                "launch": launch["scenario_raw"],
                "recovery": recovery["scenario_raw"],
                "recovery_reason": recovery["main_reason"],
                "recovery_needs_rps": needs_rps,
                "rps5_delta3": {
                    "quality": rps.quality if rps is not None else "UNKNOWN",
                    "value": fraction_payload(Fraction(rps.value))
                    if rps is not None and rps.quality == "OBSERVED"
                    else None,
                    "reason": rps.reason
                    if rps is not None
                    else "NOT_REQUIRED_OR_NOT_LOADED",
                },
            }
        )
        births = [
            name
            for name, result in (
                ("LAUNCH_CONFIRM", launch),
                ("RECOVERY_TURN", recovery),
            )
            if result["scenario_raw"] == "TRUE"
        ]
        birth_results = {"LAUNCH_CONFIRM": launch, "RECOVERY_TURN": recovery}
        if not births:
            continue
        history = checked[index:last]
        if any(error for _, _, error in history):
            candidates.extend(
                {
                    "birth_session": sessions[index].isoformat(),
                    "birth_scenario": name,
                    "diagnostic_failure_line": birth_results[name][
                        "failure_line_candidate"
                    ],
                    "state": "UNKNOWN",
                    "reason": "PEAK_HISTORY_INCOMPLETE",
                }
                for name in births
            )
            continue
        # max只取首次出现的同高，峰值严格截止前一会话。
        peak = max(range(index, last), key=lambda j: checked[j][1]["high"])
        high, peak_close = checked[peak][1]["high"], checked[peak][1]["close"]
        current = checked[last][1]
        previous = checked[last - 1][1]
        depth = 1 - current["close"] / high if current else None
        parts = {
            "birth_age": pred(1 <= last - index <= 20, "1<=i-b<=20", age=last - index),
            "peak_age": pred(2 <= last - peak <= 10, "2<=i-h<=10", age=last - peak),
            "no_new_high": pred(
                current["high"] <= high if current else None,
                "H_i<=peak_high",
                peak_high=high,
                current_high=current["high"] if current else None,
            ),
            "drawdown": pred(
                Fraction(3, 100) <= depth <= Fraction(12, 100)
                if depth is not None
                else None,
                "0.03<=1-C_i/peak_high<=0.12",
                depth=depth,
            ),
            "previous_below_peak_close": pred(
                previous["close"] < peak_close if previous else None,
                "C_i-1<peak_close",
                peak_close=peak_close,
                previous_close=previous["close"] if previous else None,
            ),
        }
        controls = []
        control_inputs = []
        for j in range(peak + 1, last + 1):
            window = checked[j - 19 : j + 1] if j >= 19 else []
            if len(window) != 20 or any(v is None for _, v, _ in window):
                controls.append(None)
                control_inputs.append(
                    {"session": sessions[j].isoformat(), "ratio": None}
                )
            else:
                ma = sum((v["close"] for _, v, _ in window), Fraction()) / 20
                ratio = checked[j][1]["close"] / ma
                controls.append(ratio >= Fraction(97, 100))
                control_inputs.append(
                    {
                        "session": sessions[j].isoformat(),
                        "ratio": fraction_payload(ratio),
                    }
                )
        control = (
            False
            if False in controls
            else None
            if not controls or None in controls
            else True
        )
        parts["ma20_control"] = pred(control, "all C[p..i]/MA20>=0.97")
        parts["ma20_control"]["window"] = control_inputs
        if control is None:
            parts["ma20_control"]["reason"] = "STRICT_MA20_WINDOW_UNAVAILABLE"
        amount_indices = (
            list(range(peak - 4, peak + 1)) + list(range(peak + 1, last)) + [last]
        )
        amount_values = []
        amount_reason = "PULLBACK_WINDOW_EMPTY_OR_BASE_TRUNCATED"
        if peak >= 4 and peak + 1 < last:
            for j in amount_indices:
                row, _, error = checked[j]
                value, amount_error = _raw_amount(
                    sessions[j], row, parser_contract=parser
                )
                if (
                    error
                    or amount_error
                    or row.get("amount_unit_status") != "UNIT_UNVERIFIED"
                ):
                    amount_reason = error or amount_error or "AMOUNT_UNIT_MISMATCH"
                    amount_values = []
                    break
                amount_values.append(value)
        if amount_values:
            base = sum(amount_values[:5], Fraction()) / 5
            pull = sum(amount_values[5:-1], Fraction()) / len(amount_values[5:-1])
            ratio = pull / base if base > 0 else None
            expansion = amount_values[-1] / pull if pull > 0 else None
        else:
            base = pull = ratio = expansion = None
        parts["contraction"] = pred(
            ratio <= Fraction(17, 20) if ratio is not None else None,
            "mean(A[p..i-1])/mean(A[h-4..h])<=0.85",
            ratio=ratio,
            base_mean=base,
            pullback_mean=pull,
        )
        parts["confirmation_amount"] = pred(
            expansion >= 1 if expansion is not None else None,
            "A[i]>=mean(A[p..i-1]), mean>0",
            ratio=expansion,
        )
        if ratio is None:
            parts["contraction"]["reason"] = (
                "BASE_AMOUNT_MEAN_ZERO" if amount_values else amount_reason
            )
        if expansion is None:
            parts["confirmation_amount"]["reason"] = (
                "PULLBACK_AMOUNT_MEAN_ZERO" if amount_values else amount_reason
            )
        parts["contraction"]["reference_sessions"] = [
            day.isoformat() for day in sessions[max(0, peak - 4) : peak + 1]
        ]
        parts["contraction"]["pullback_sessions"] = [
            day.isoformat() for day in sessions[peak + 1 : last]
        ]
        parts["contraction"]["amount_unit"] = "SAME_SOURCE_NATIVE_UNIT_UNVERIFIED"
        candidates.extend(
            {
                "birth_session": sessions[index].isoformat(),
                "birth_scenario": name,
                "diagnostic_failure_line": birth_results[name][
                    "failure_line_candidate"
                ],
                "peak_session": sessions[peak].isoformat(),
                "peak_high": fraction_payload(high),
                "peak_close": fraction_payload(peak_close),
                "predicates": parts,
                "state": "DIAGNOSTIC_COMPONENTS_ONLY",
            }
            for name in births
        )
    return {
        "role": "EXPLORATORY_DIAGNOSTIC_PARENT_NON_PIT",
        "formal_parent": "UNKNOWN_NOT_PROVEN_BY_SOURCE_DIAGNOSTIC",
        "formal_pb_continue": "NOT_EVALUATED",
        "target_session": target.isoformat(),
        "prior_through": sessions[-2].isoformat() if len(sessions) > 1 else None,
        "historical_states": historical,
        "diagnostic_parent_candidates": candidates,
        "recovery_parent_limitation": "UNKNOWN_WHERE_HISTORICAL_RPS_UNAVAILABLE",
    }
