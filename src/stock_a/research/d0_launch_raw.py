"""D0来源级启动与恢复诊断的严格会话计算。"""

from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation, localcontext
from fractions import Fraction
from typing import Any, Mapping, Sequence

from stock_a.research.d0_price_facts import calculate_d0_first_day_damage


_SCENARIO = "LAUNCH_CONFIRM"
_SESSION_CONTRACT = "D0_SESSION_R2"
_REASON_TEXT = {
    "OFFICIAL_WINDOW_INVALID": "官方会话必须是有序、唯一且以目标日结束的窗口。",
    "OFFICIAL_WINDOW_LEFT_TRUNCATED": "目标日前不足20个连续官方会话。",
    "OFFICIAL_SESSION_ROW_MISSING": "严格官方会话窗口没有该来源代码的RAW行。",
    "OFFICIAL_SESSION_BAR_MISSING": "官方会话未取得有效实际Bar；停复牌状态未知，不能补零或跳日。",
    "RAW_SOURCE_IDENTITY_MISMATCH": "RAW行与命名空间、来源代码、日期、来源包或来源内容不一致。",
    "RAW_TARGET_CAPTURE_MISMATCH": "目标日RAW行与来源捕获身份不一致。",
    "RAW_COORDINATE_MISMATCH": "价格坐标不是同一RAW_UNADJUSTED坐标。",
    "RAW_PARSER_CONTRACT_MISMATCH": "窗口RAW历史使用了不同的来源解析合同。",
    "RAW_PRICE_QUALITY_UNKNOWN": "严格会话价格质量未知。",
    "RAW_PRICE_QUALITY_BAD": "严格会话价格值不合法。",
    "INVALID_PRICE_OHLC": "原始OHLC缺失、非有限、非正数或内部关系非法。",
    "CLV_RANGE_ZERO": "目标日最高价等于最低价，CLV分母为零。",
    "RAW_AMOUNT_QUALITY_UNKNOWN": "原生金额值质量未知或不适用。",
    "RAW_AMOUNT_QUALITY_BAD": "原生金额值非法。",
    "RAW_AMOUNT_NATIVE_UNIT_MISMATCH": "严格窗口金额单位状态不一致，不能证明同尺度。",
    "AMOUNT_MEAN_DENOMINATOR_ZERO": "前20个严格会话原生金额均值为零，不能计算比率。",
    "RAW_REFERENCE_QUALITY_UNKNOWN": "RAW捕获或内容引用未能通过来源存储核验。",
    "C_ABOVE_PHC20_TRUE": "目标收盘价严格高于此前20个严格会话的最高收盘价。",
    "C_ABOVE_PHC20_FALSE": "目标收盘价未严格高于此前20个严格会话的最高收盘价。",
    "RET1_POSITIVE_TRUE": "目标收盘价高于紧邻前一严格官方会话收盘价。",
    "RET1_POSITIVE_FALSE": "目标收盘价未高于紧邻前一严格官方会话收盘价。",
    "CLV_AT_LEAST_060_TRUE": "目标日CLV大于或等于3/5。",
    "CLV_AT_LEAST_060_FALSE": "目标日CLV低于3/5。",
    "AMR20_MEAN_PRIOR_AT_LEAST_120_TRUE": "目标日原生金额与此前20个严格会话均额之比大于或等于6/5。",
    "AMR20_MEAN_PRIOR_AT_LEAST_120_FALSE": "目标日原生金额与此前20个严格会话均额之比低于6/5。",
    "PHH20_SHADOW_REJECTION_TRUE": "目标日不满足PHH20日线冲高回落拒绝条件。",
    "PHH20_SHADOW_REJECTION_FALSE": "目标日触发PHH20日线冲高回落拒绝条件。",
}


def _fraction(value: Any) -> Fraction | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, Fraction):
        return value
    if isinstance(value, Decimal):
        return Fraction(value) if value.is_finite() else None
    if isinstance(value, int):
        return Fraction(value)
    if isinstance(value, str):
        try:
            decimal_value = Decimal(value)
        except InvalidOperation:
            return None
        return Fraction(decimal_value) if decimal_value.is_finite() else None
    return None


def fraction_payload(value: Fraction | None) -> dict[str, str] | None:
    if value is None:
        return None
    return {"numerator": str(value.numerator), "denominator": str(value.denominator)}


def _predicate(
    state: str,
    *,
    reason_code: str,
    value: Fraction | None = None,
    inputs: Mapping[str, Any] | None = None,
    formula: str,
) -> dict[str, Any]:
    quality = "OBSERVED" if state in {"TRUE", "FALSE"} else "UNKNOWN"
    return {
        "state": state,
        "quality": quality,
        "reason_code": reason_code,
        "reason": _REASON_TEXT.get(reason_code, "严格原始输入不能确定该谓词。"),
        "formula": formula,
        "value": fraction_payload(value),
        "inputs": {
            key: fraction_payload(item) if isinstance(item, Fraction) else item
            for key, item in (inputs or {}).items()
        },
    }


def _window_row(
    session: date,
    rows: Mapping[date, Mapping[str, Any] | None],
    *,
    source_symbol: str,
    package_sha256: str,
    namespace_id: str,
    source_content_id: str,
    target_capture_id: str,
    parser_contract: str,
) -> tuple[Mapping[str, Any] | None, dict[str, Fraction] | None, str | None]:
    row = rows.get(session)
    if row is None:
        return None, None, "OFFICIAL_SESSION_ROW_MISSING"
    if (
        row.get("session_date") != session
        or row.get("source_symbol") != source_symbol
        or row.get("namespace_id") != namespace_id
        or str(row.get("package_sha256", "")).strip() != package_sha256
        or row.get("source_content_id") != source_content_id
    ):
        return row, None, "RAW_SOURCE_IDENTITY_MISMATCH"
    if (
        session == rows.get("_target_session")
        and row.get("source_capture_id") != target_capture_id
    ):
        return row, None, "RAW_TARGET_CAPTURE_MISMATCH"
    if row.get("price_coordinate") != "RAW_UNADJUSTED":
        return row, None, "RAW_COORDINATE_MISMATCH"
    if row.get("reference_quality") is False:
        return row, None, "RAW_REFERENCE_QUALITY_UNKNOWN"
    if row.get("parser_contract") != parser_contract:
        return row, None, "RAW_PARSER_CONTRACT_MISMATCH"
    if row.get("bar_status") != "TARGET_PRESENT":
        return row, None, "OFFICIAL_SESSION_BAR_MISSING"
    if row.get("price_quality") == "BAD_VALUE":
        return row, None, "RAW_PRICE_QUALITY_BAD"
    if row.get("price_quality") != "OBSERVED":
        return row, None, "RAW_PRICE_QUALITY_UNKNOWN"
    values = {
        name: _fraction(row.get(column))
        for name, column in (
            ("open", "raw_open"),
            ("high", "raw_high"),
            ("low", "raw_low"),
            ("close", "raw_close"),
        )
    }
    if any(value is None for value in values.values()):
        return row, None, "INVALID_PRICE_OHLC"
    opening, high, low, close = (
        values["open"],
        values["high"],
        values["low"],
        values["close"],
    )
    if (
        min(opening, high, low, close) <= 0
        or low > high
        or not low <= opening <= high
        or not low <= close <= high
    ):
        return row, None, "INVALID_PRICE_OHLC"
    return row, values, None


def _unknown_predicate(reason_code: str, formula: str) -> dict[str, Any]:
    return _predicate("UNKNOWN", reason_code=reason_code, formula=formula)


def _tri_not(state: str) -> str:
    return {"TRUE": "FALSE", "FALSE": "TRUE", "UNKNOWN": "UNKNOWN"}[state]


def _tri_and(predicates: Mapping[str, Mapping[str, Any]]) -> str:
    states = [predicate["state"] for predicate in predicates.values()]
    if "FALSE" in states:
        return "FALSE"
    if "UNKNOWN" in states:
        return "UNKNOWN"
    return "TRUE"


def _raw_amount(
    session: date,
    row: Mapping[str, Any] | None,
    *,
    parser_contract: str,
) -> tuple[Fraction | None, str | None]:
    if row is None or row.get("bar_status") != "TARGET_PRESENT":
        return None, "OFFICIAL_SESSION_BAR_MISSING"
    if row.get("reference_quality") is False:
        return None, "RAW_REFERENCE_QUALITY_UNKNOWN"
    if row.get("parser_contract") != parser_contract:
        return None, "RAW_PARSER_CONTRACT_MISMATCH"
    if row.get("raw_amount_quality") != "RAW_SOURCE_VALUE_OBSERVED":
        if row.get("raw_amount_quality") == "BAD_VALUE":
            return None, "RAW_AMOUNT_QUALITY_BAD"
        return None, "RAW_AMOUNT_QUALITY_UNKNOWN"
    value = _fraction(row.get("raw_amount"))
    if value is None or value < 0:
        return None, "RAW_AMOUNT_QUALITY_BAD"
    return value, None


def _window_payload(
    official_sessions: Sequence[date],
    rows_by_session: Mapping[date, Mapping[str, Any] | None],
) -> list[dict[str, Any]]:
    fields = (
        "bar_status",
        "bar_reason",
        "price_quality",
        "price_reason",
        "raw_amount_quality",
        "raw_amount_reason",
        "amount_unit_status",
        "source_capture_id",
        "source_content_id",
        "package_sha256",
    )
    result = []
    for session in official_sessions:
        row = rows_by_session.get(session)
        payload = {
            "session_date": session.isoformat(),
            "status": "ROW_NOT_STORED" if row is None else "ROW_STORED",
        }
        if row is not None:
            payload.update({key: row.get(key) for key in fields})
            payload["raw_ohlc"] = {
                key: fraction_payload(_fraction(row.get(column)))
                for key, column in (
                    ("open", "raw_open"),
                    ("high", "raw_high"),
                    ("low", "raw_low"),
                    ("close", "raw_close"),
                )
            }
            payload["raw_amount"] = fraction_payload(_fraction(row.get("raw_amount")))
        result.append(payload)
    return result


def calculate_d0_launch_confirm_raw(
    *,
    target_session: date,
    official_sessions: Sequence[date],
    history_rows_by_session: Mapping[date, Mapping[str, Any] | None],
    source_symbol: str,
    package_sha256: str,
    namespace_id: str,
    source_content_id: str,
    target_capture_id: str,
    parser_contract: str,
    include_history_window: bool = False,
) -> dict[str, Any]:
    """独立按D0_SESSION_R2严格20个前态会话和目标日计算LAUNCH_CONFIRM raw。"""
    sessions = tuple(official_sessions)
    valid_window = (
        type(target_session) is date
        and len(sessions) == 21
        and all(type(session) is date for session in sessions)
        and tuple(sorted(set(sessions))) == sessions
        and sessions[-1] == target_session
    )
    window = sessions if valid_window else sessions[-21:]
    rows = dict(history_rows_by_session)
    rows["_target_session"] = target_session
    formulas = {
        "close_above_phc20": "C[t] > max(C[t-20..t-1])",
        "positive_ret1": "C[t] / C[t-1] - 1 > 0",
        "clv_at_least_060": "(C[t]-L[t]) / (H[t]-L[t]) >= 3/5",
        "amr20_mean_prior_at_least_120": "A[t] / mean(A[t-20..t-1]) >= 6/5",
        "not_phh20_shadow_rejection": "NOT(H[t] > max(H[t-20..t-1]) AND C[t] <= max(H[t-20..t-1]))",
    }
    predicates: dict[str, dict[str, Any]] = {}
    if not valid_window:
        reason = (
            "OFFICIAL_WINDOW_LEFT_TRUNCATED"
            if len(sessions) < 21
            else "OFFICIAL_WINDOW_INVALID"
        )
        predicates = {
            key: _unknown_predicate(reason, formula)
            for key, formula in formulas.items()
        }
    else:
        checked: dict[
            date,
            tuple[Mapping[str, Any] | None, dict[str, Fraction] | None, str | None],
        ] = {
            session: _window_row(
                session,
                rows,
                source_symbol=source_symbol,
                package_sha256=package_sha256,
                namespace_id=namespace_id,
                source_content_id=source_content_id,
                target_capture_id=target_capture_id,
                parser_contract=parser_contract,
            )
            for session in sessions
        }
        prior = sessions[:20]
        current_session = sessions[-1]
        previous_session = sessions[-2]
        current_row, current_ohlc, current_error = checked[current_session]

        prior_close_errors = [checked[session][2] for session in prior]
        if any(prior_close_errors):
            first_error = next(error for error in prior_close_errors if error)
            predicates["close_above_phc20"] = _unknown_predicate(
                first_error or "OFFICIAL_SESSION_BAR_MISSING",
                formulas["close_above_phc20"],
            )
        elif current_error:
            predicates["close_above_phc20"] = _unknown_predicate(
                current_error, formulas["close_above_phc20"]
            )
        else:
            phc20 = max(checked[session][1]["close"] for session in prior)
            close = current_ohlc["close"]
            predicates["close_above_phc20"] = _predicate(
                "TRUE" if close > phc20 else "FALSE",
                reason_code="C_ABOVE_PHC20_TRUE"
                if close > phc20
                else "C_ABOVE_PHC20_FALSE",
                value=close - phc20,
                inputs={"C_t": close, "PHC20": phc20},
                formula=formulas["close_above_phc20"],
            )

        prior_error = checked[previous_session][2]
        if prior_error:
            predicates["positive_ret1"] = _unknown_predicate(
                prior_error, formulas["positive_ret1"]
            )
        elif current_error:
            predicates["positive_ret1"] = _unknown_predicate(
                current_error, formulas["positive_ret1"]
            )
        else:
            prior_close = checked[previous_session][1]["close"]
            close = current_ohlc["close"]
            ret1 = close / prior_close - 1
            predicates["positive_ret1"] = _predicate(
                "TRUE" if ret1 > 0 else "FALSE",
                reason_code="RET1_POSITIVE_TRUE" if ret1 > 0 else "RET1_POSITIVE_FALSE",
                value=ret1,
                inputs={"C_t": close, "C_t_minus_1": prior_close},
                formula=formulas["positive_ret1"],
            )

        if current_error:
            predicates["clv_at_least_060"] = _unknown_predicate(
                current_error, formulas["clv_at_least_060"]
            )
        else:
            high, low, close = (
                current_ohlc["high"],
                current_ohlc["low"],
                current_ohlc["close"],
            )
            if high == low:
                predicates["clv_at_least_060"] = _unknown_predicate(
                    "CLV_RANGE_ZERO", formulas["clv_at_least_060"]
                )
            else:
                clv = (close - low) / (high - low)
                predicates["clv_at_least_060"] = _predicate(
                    "TRUE" if clv >= Fraction(3, 5) else "FALSE",
                    reason_code="CLV_AT_LEAST_060_TRUE"
                    if clv >= Fraction(3, 5)
                    else "CLV_AT_LEAST_060_FALSE",
                    value=clv,
                    inputs={"C_t": close, "L_t": low, "H_t": high},
                    formula=formulas["clv_at_least_060"],
                )

        amount_values: dict[date, Fraction | None] = {}
        amount_errors: dict[date, str | None] = {}
        for session in (*prior, current_session):
            row = checked[session][0]
            strict_session_error = checked[session][2]
            if strict_session_error:
                value, error = None, strict_session_error
            elif row is not None and (
                row.get("source_symbol") != source_symbol
                or row.get("namespace_id") != namespace_id
                or row.get("session_date") != session
                or str(row.get("package_sha256", "")).strip() != package_sha256
                or row.get("source_content_id") != source_content_id
                or session == current_session
                and row.get("source_capture_id") != target_capture_id
            ):
                value, error = None, "RAW_SOURCE_IDENTITY_MISMATCH"
            else:
                value, error = _raw_amount(
                    session, row, parser_contract=parser_contract
                )
            amount_values[session] = value
            amount_errors[session] = error
        unit_statuses = {
            checked[session][0].get("amount_unit_status")
            if checked[session][0] is not None
            else None
            for session in (*prior, current_session)
        }
        if any(amount_errors.values()):
            first_error = next(error for error in amount_errors.values() if error)
            predicates["amr20_mean_prior_at_least_120"] = _unknown_predicate(
                first_error or "RAW_AMOUNT_QUALITY_UNKNOWN",
                formulas["amr20_mean_prior_at_least_120"],
            )
        elif len(unit_statuses) != 1 or unit_statuses != {"UNIT_UNVERIFIED"}:
            predicates["amr20_mean_prior_at_least_120"] = _unknown_predicate(
                "RAW_AMOUNT_NATIVE_UNIT_MISMATCH",
                formulas["amr20_mean_prior_at_least_120"],
            )
        else:
            current_amount = amount_values[current_session]
            prior_amount_mean = (
                sum((amount_values[session] for session in prior), Fraction(0)) / 20
            )
            if prior_amount_mean == 0:
                predicates["amr20_mean_prior_at_least_120"] = _unknown_predicate(
                    "AMOUNT_MEAN_DENOMINATOR_ZERO",
                    formulas["amr20_mean_prior_at_least_120"],
                )
            else:
                amr = current_amount / prior_amount_mean
                predicates["amr20_mean_prior_at_least_120"] = _predicate(
                    "TRUE" if amr >= Fraction(6, 5) else "FALSE",
                    reason_code="AMR20_MEAN_PRIOR_AT_LEAST_120_TRUE"
                    if amr >= Fraction(6, 5)
                    else "AMR20_MEAN_PRIOR_AT_LEAST_120_FALSE",
                    value=amr,
                    inputs={
                        "A_t": current_amount,
                        "mean_A_t_minus_20_to_t_minus_1": prior_amount_mean,
                    },
                    formula=formulas["amr20_mean_prior_at_least_120"],
                )

        prior_high_errors = [checked[session][2] for session in prior]
        phh20_error = next((error for error in prior_high_errors if error), None)
        if phh20_error:
            predicates["not_phh20_shadow_rejection"] = _unknown_predicate(
                phh20_error, formulas["not_phh20_shadow_rejection"]
            )
        elif current_error:
            predicates["not_phh20_shadow_rejection"] = _unknown_predicate(
                current_error, formulas["not_phh20_shadow_rejection"]
            )
        else:
            phh20 = max(checked[session][1]["high"] for session in prior)
            high = current_ohlc["high"]
            close = current_ohlc["close"]
            new_high = high > phh20
            close_not_above_phh = close <= phh20
            rejected = new_high and close_not_above_phh
            state = _tri_not("TRUE" if rejected else "FALSE")
            predicates["not_phh20_shadow_rejection"] = _predicate(
                state,
                reason_code="PHH20_SHADOW_REJECTION_TRUE"
                if state == "TRUE"
                else "PHH20_SHADOW_REJECTION_FALSE",
                value=None,
                inputs={
                    "H_t": high,
                    "C_t": close,
                    "PHH20": phh20,
                    "H_t_gt_PHH20": new_high,
                    "C_t_le_PHH20": close_not_above_phh,
                    "shadow_rejection": rejected,
                },
                formula=formulas["not_phh20_shadow_rejection"],
            )

    state = _tri_and(predicates)
    main_reason = None
    if state == "FALSE":
        false_key = next(
            key for key, value in predicates.items() if value["state"] == "FALSE"
        )
        main_reason = f"KNOWN_FALSE:{false_key}"
    elif state == "UNKNOWN":
        unknown_key = next(
            key for key, value in predicates.items() if value["state"] == "UNKNOWN"
        )
        main_reason = f"UNKNOWN:{unknown_key}:{predicates[unknown_key]['reason_code']}"
    return {
        "source_symbol": source_symbol,
        "target_session": target_session.isoformat(),
        "scenario": _SCENARIO,
        "scenario_raw": state,
        "quality": "OBSERVED" if state != "UNKNOWN" else "UNKNOWN",
        "main_reason": main_reason or "ALL_FIVE_PREDICATES_TRUE",
        "predicates": predicates,
        "failure_line_candidate": {
            "value": predicates["close_above_phc20"].get("inputs", {}).get("PHC20")
            if state == "TRUE"
            else None,
            "source": "T0_PHC20" if state == "TRUE" else None,
            "evaluation_starts": "NEXT_OFFICIAL_SESSION",
            "role": "DIAGNOSTIC_ONLY_NOT_D2",
        },
        "session_contract": _SESSION_CONTRACT,
        "strict_official_sessions": [session.isoformat() for session in window],
        "formal_admission_status": "NOT_AUTHORIZED_SOURCE_SYMBOL_DIAGNOSTIC",
        "research_universe_status": "UNKNOWN",
        "absolute_liquidity_cny_status": "UNKNOWN_UNIT_UNVERIFIED",
        "turn_status": "UNKNOWN_NOT_PRODUCED",
        "stable_security_id_status": "STABLE_SECURITY_ID_UNVERIFIED",
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "episode_status": "NOT_ISSUED",
        "other_scenarios": {
            "RECOVERY_TURN": "AVAILABLE_VIA_SEPARATE_REQUEST",
            "STRONG_PULLBACK": "NOT_IMPLEMENTED",
            "TREND_CONTINUE": "NOT_IMPLEMENTED",
        },
        **(
            {"history_window": _window_payload(window, rows)}
            if include_history_window
            else {}
        ),
    }


def calculate_d0_recovery_turn_raw(
    *,
    target_session,
    official_sessions,
    history_rows_by_session,
    source_symbol,
    package_sha256,
    namespace_id,
    source_content_id,
    target_capture_id,
    parser_contract,
    rps5_delta3,
    rps20=None,
    include_prior_ols=False,
):
    """按严格市场会话计算恢复结构；各子条件独立保留未知。"""
    sessions = tuple(official_sessions)
    if (
        not sessions
        or sessions[-1] != target_session
        or tuple(sorted(set(sessions))) != sessions
        or len(sessions) > 25
    ):
        raise ValueError("恢复诊断的官方会话窗口无效")
    rows = dict(history_rows_by_session)
    rows["_target_session"] = target_session
    checked = [
        _window_row(
            day,
            rows,
            source_symbol=source_symbol,
            package_sha256=package_sha256,
            namespace_id=namespace_id,
            source_content_id=source_content_id,
            target_capture_id=target_capture_id,
            parser_contract=parser_contract,
        )
        for day in sessions
    ]
    entry_shas = {
        row.get("source_entry_sha256")
        for row, _, _ in checked
        if row is not None and row.get("source_entry_sha256")
    }
    if len(entry_shas) > 1:
        checked = [
            (row, None, "RAW_SOURCE_ENTRY_SHA_CONFLICT") for row, _, _ in checked
        ]

    def price(index, field):
        if index < -len(checked) or index >= len(checked):
            return None
        values = checked[index][1]
        return None if values is None else values[field]

    def ma(end, length):
        index = len(sessions) + end if end < 0 else end
        if index - length + 1 < 0:
            return None
        values = [price(i, "close") for i in range(index - length + 1, index + 1)]
        return (
            sum(values, Fraction()) / length
            if all(v is not None for v in values)
            else None
        )

    def predicate(name, value, formula, inputs=None):
        state = "UNKNOWN" if value is None else "TRUE" if value else "FALSE"
        result = _predicate(
            state, reason_code=f"{name.upper()}_{state}", formula=formula, inputs=inputs
        )
        result["reason"] = (
            "所需严格会话或来源字段不足。" if value is None else "已按所列公式计算。"
        )
        return result

    close, previous = price(-1, "close"), price(-2, "close")
    high, low = price(-1, "high"), price(-1, "low")
    ma5, previous_ma5 = ma(-1, 5), ma(-2, 5)
    ma20, previous_ma20, ma20_minus3 = ma(-1, 20), ma(-2, 20), ma(-4, 20)
    prior_pairs = [(price(i, "close"), ma(i, 20)) for i in range(-6, -1)]
    below = (
        sum(c <= avg for c, avg in prior_pairs)
        if all(c is not None and avg is not None for c, avg in prior_pairs)
        else None
    )
    r5 = (
        previous <= previous_ma5 and close > ma5
        if all(v is not None for v in (previous, previous_ma5, close, ma5))
        else None
    )
    r20_parts = [
        predicate(
            "r20_cross",
            previous <= previous_ma20 and close > ma20
            if all(v is not None for v in (previous, previous_ma20, close, ma20))
            else None,
            "C[t-1]<=MA20[t-1] AND C[t]>MA20[t]",
        ),
        predicate(
            "prior5_below",
            below >= 2 if below is not None else None,
            "prior5_below>=2",
            {"prior5_below": below},
        ),
        predicate(
            "ma20_slope",
            ma20 >= ma20_minus3
            if ma20 is not None and ma20_minus3 is not None
            else None,
            "MA20[t]>=MA20[t-3]",
            {"MA20_t": ma20, "MA20_t_minus_3": ma20_minus3},
        ),
    ]
    r20_state = _tri_and({str(i): item for i, item in enumerate(r20_parts)})
    r5_state = "UNKNOWN" if r5 is None else "TRUE" if r5 else "FALSE"
    trigger = (
        "TRUE"
        if "TRUE" in (r5_state, r20_state)
        else "UNKNOWN"
        if "UNKNOWN" in (r5_state, r20_state)
        else "FALSE"
    )
    clv = (
        (close - low) / (high - low)
        if all(v is not None for v in (close, high, low)) and high != low
        else None
    )
    amount_rows = checked[-21:]
    amount_identity_errors = {
        "RAW_SOURCE_IDENTITY_MISMATCH",
        "RAW_TARGET_CAPTURE_MISMATCH",
        "RAW_REFERENCE_QUALITY_UNKNOWN",
        "RAW_PARSER_CONTRACT_MISMATCH",
        "RAW_SOURCE_ENTRY_SHA_CONFLICT",
        "OFFICIAL_SESSION_ROW_MISSING",
        "OFFICIAL_SESSION_BAR_MISSING",
    }
    amounts = [
        _raw_amount(
            sessions[len(sessions) - len(amount_rows) + i],
            row,
            parser_contract=parser_contract,
        )[0]
        if error not in amount_identity_errors
        else None
        for i, (row, _, error) in enumerate(amount_rows)
    ]
    units = {
        row.get("amount_unit_status") if row else None for row, _, _ in amount_rows
    }
    amr = None
    if (
        len(amounts) == 21
        and all(v is not None for v in amounts)
        and units == {"UNIT_UNVERIFIED"}
    ):
        mean = sum(amounts[:-1], Fraction()) / 20
        if mean > 0:
            amr = amounts[-1] / mean
    delta = (
        Fraction(rps5_delta3.value)
        if rps5_delta3 is not None and rps5_delta3.quality == "OBSERVED"
        else None
    )
    strength20 = (
        Fraction(rps20.value)
        if rps20 is not None and rps20.quality == "OBSERVED"
        else None
    )
    predicates = {
        "trigger": _predicate(
            trigger,
            reason_code=f"RECOVERY_TRIGGER_{trigger}",
            formula="R5 OR R20",
            inputs={"R5": r5_state, "R20": r20_state},
        ),
        "positive_ret1": predicate(
            "positive_ret1",
            close > previous if close is not None and previous is not None else None,
            "C[t]>C[t-1]",
            {"C_t": close, "C_previous": previous},
        ),
        "clv": predicate(
            "clv",
            clv >= Fraction(11, 20) if clv is not None else None,
            "CLV>=0.55",
            {"CLV": clv},
        ),
        "rps5_delta3": predicate(
            "rps5_delta3",
            delta > 0 if delta is not None else None,
            "rps5_delta3_pp>0",
            {"delta_pp": delta},
        ),
        "amr": predicate(
            "amr",
            amr >= Fraction(21, 20) if amr is not None else None,
            "A[t]/mean(A[t-20..t-1])>=1.05",
            {"amr": amr},
        ),
        "close_ma20": predicate(
            "close_ma20",
            close / ma20 >= Fraction(49, 50)
            if close is not None and ma20 is not None
            else None,
            "C[t]/MA20[t]>=0.98",
            {"C_t": close, "MA20_t": ma20},
        ),
    }
    predicates["trigger"]["reason"] = (
        "R5或R20的输入不足。" if trigger == "UNKNOWN" else "已按R5或R20归约。"
    )
    damage = calculate_d0_first_day_damage(
        target_session=target_session,
        official_sessions=sessions,
        history_rows_by_session=history_rows_by_session,
        source_symbol=source_symbol,
        package_sha256=package_sha256,
        namespace_id=namespace_id,
        source_content_id=source_content_id,
        target_capture_id=target_capture_id,
    )
    damage_input_error = next((error for _, _, error in checked[-20:] if error), None)
    damage_state = damage.state if damage_input_error is None else "UNKNOWN"
    ret1 = close / previous - 1 if close is not None and previous is not None else None
    severe = (
        "UNKNOWN"
        if ret1 is None
        else "TRUE"
        if ret1 <= Fraction(-2, 25)
        else "FALSE"
        if ret1 > Fraction(-1, 25)
        else "UNKNOWN"
    )
    state = _tri_and(predicates)
    first = (
        next((key for key, item in predicates.items() if item["state"] == state), None)
        if state != "TRUE"
        else None
    )
    previous_ma10 = ma(-2, 10)
    prior_highs = (
        [price(i, "high") for i in range(-21, -1)] if len(sessions) >= 21 else []
    )
    phh20 = (
        max(prior_highs)
        if len(prior_highs) == 20 and all(v is not None for v in prior_highs)
        else None
    )
    shadow_clear = (
        not (high > phh20 and close <= phh20)
        if high is not None and close is not None and phh20 is not None
        else None
    )
    retest = (
        low <= Fraction(101, 100) * previous_ma10 and close >= previous_ma10
        if low is not None and close is not None and previous_ma10 is not None
        else None
    )
    prior_closes = (
        [price(i, "close") for i in range(-21, -1)] if len(sessions) >= 21 else []
    )
    ols_slope = ols_r2 = None
    if (
        include_prior_ols
        and len(prior_closes) == 20
        and all(value is not None for value in prior_closes)
    ):
        if len(set(prior_closes)) == 1:
            ols_slope = Decimal(0)
        else:
            with localcontext() as context:
                context.prec = 34
                logs = [
                    (Decimal(value.numerator) / Decimal(value.denominator)).ln()
                    for value in prior_closes
                ]
                mean_y = sum(logs, Decimal(0)) / Decimal(20)
                mean_x = Decimal(19) / 2
                sxx = sum(((Decimal(i) - mean_x) ** 2 for i in range(20)), Decimal(0))
                sxy = sum(
                    (
                        (Decimal(i) - mean_x) * (value - mean_y)
                        for i, value in enumerate(logs)
                    ),
                    Decimal(0),
                )
                syy = sum(((value - mean_y) ** 2 for value in logs), Decimal(0))
                ols_slope = sxy / sxx
                if syy != 0:
                    ols_r2 = sxy * sxy / (sxx * syy)
    partial = {
        "STRONG_PULLBACK": {
            "prior_ols_status": "EVALUATED"
            if include_prior_ols
            else "NOT_REQUESTED_SELECT_EXACT_SOURCE_SYMBOL",
            "prior_ols_slope_positive": predicate(
                "prior_ols_slope_positive",
                ols_slope > 0 if ols_slope is not None else None,
                "OLS20(log C)[t-1].slope>0",
                {"slope": str(ols_slope) if ols_slope is not None else None},
            ),
            "prior_ols_r2_at_least_040": predicate(
                "prior_ols_r2_at_least_040",
                ols_r2 >= Decimal("0.40") if ols_r2 is not None else None,
                "OLS20(log C)[t-1].R2>=0.40",
                {"r2": str(ols_r2) if ols_r2 is not None else None},
            ),
            "reclaim_ma5": predicate(
                "reclaim_ma5", r5, "C[t-1]<=MA5[t-1] AND C[t]>MA5[t]"
            ),
            "retest_previous_ma10": predicate(
                "retest_previous_ma10",
                retest,
                "L[t]<=1.01*MA10[t-1] AND C[t]>=MA10[t-1]",
                {"MA10_previous": previous_ma10},
            ),
            "close_not_below_previous": predicate(
                "close_not_below_previous",
                close >= previous
                if close is not None and previous is not None
                else None,
                "C[t]>=C[t-1]",
            ),
            "clv": predicates["clv"],
            "no_shadow_rejection": predicate(
                "no_shadow_rejection",
                shadow_clear,
                "NOT(H[t]>PHH20 AND C[t]<=PHH20)",
                {"PHH20": phh20},
            ),
        },
        "TREND_CONTINUE": {
            "ma_order": predicate(
                "ma_order",
                close >= ma5 >= ma20
                if all(v is not None for v in (close, ma5, ma20))
                else None,
                "C[t]>=MA5[t]>=MA20[t]",
            ),
            "positive_ret1": predicates["positive_ret1"],
            "clv": predicates["clv"],
            "rps20_at_least_70": predicate(
                "rps20_at_least_70",
                strength20 >= 70 if strength20 is not None else None,
                "RPS20>=70",
                {"rps20": strength20},
            ),
            "amr_range": predicate(
                "amr_range",
                Fraction(4, 5) <= amr <= Fraction(5, 2) if amr is not None else None,
                "0.8<=amr20<=2.5",
                {"amr": amr},
            ),
        },
    }
    if not include_prior_ols:
        for key in ("prior_ols_slope_positive", "prior_ols_r2_at_least_040"):
            partial["STRONG_PULLBACK"][key]["reason"] = "未请求指定来源代码的前态OLS。"
    elif ols_slope is not None and ols_r2 is None:
        partial["STRONG_PULLBACK"]["prior_ols_r2_at_least_040"]["reason"] = (
            "前态log收盘价总平方差为零。"
        )
    line = (
        ma20
        if r20_state == "TRUE"
        else ma5
        if r20_state == "FALSE" and r5_state == "TRUE"
        else None
    )
    return {
        "source_symbol": source_symbol,
        "target_session": target_session.isoformat(),
        "scenario": "RECOVERY_TURN",
        "scenario_raw": state,
        "quality": "UNKNOWN" if state == "UNKNOWN" else "OBSERVED",
        "main_reason": f"{state}:{first}" if first else "ALL_PREDICATES_TRUE",
        "predicates": predicates,
        "trigger_detail": {
            "r5": r5_state,
            "r20": r20_state,
            "r20_components": r20_parts,
            "ma5": fraction_payload(ma5),
            "ma20": fraction_payload(ma20),
            "prior5_below": below,
        },
        "other_scenario_partial_predicates": partial,
        "other_scenario_status": "NOT_EVALUATED_FORMAL_PARENT_AND_OTHER_INPUTS_UNPROVED",
        "other_scenario_missing_dependencies": {
            "formal_t_minus_1_parent": "UNKNOWN",
            "pullback_frozen_peak_and_window": "UNKNOWN",
            "pullback_volume_windows": "UNKNOWN_FROZEN_PEAK_AND_WINDOW_UNPROVED",
            "continuation_formal_loo_confirmed_raw": "NOT_EVALUATED",
            "diagnostic_d1_reference": "NOT_FORMAL_PARENT",
        },
        "common_gate_diagnostics": {
            "first_day_damage": {
                "state": damage_state,
                "quality": damage.quality if damage_state != "UNKNOWN" else "UNKNOWN",
                "reason": damage_input_error or damage.reason_code,
                "close_to_ma20": fraction_payload(damage.close_to_ma20)
                if damage_state != "UNKNOWN"
                else None,
            },
            "severe_drop": {
                "state": severe,
                "reason": "PRIOR_SIGMA20_NOT_PRODUCED"
                if severe == "UNKNOWN"
                else "DETERMINED_BY_EXACT_RET1_BOUND",
                "ret1": fraction_payload(ret1),
            },
            "liquidity_cny": "UNKNOWN_UNIT_UNVERIFIED",
            "research_universe": "UNKNOWN",
            "corporate_action_coordinate": "UNKNOWN",
            "formal_admission": "NOT_AUTHORIZED",
        },
        "failure_line_candidate": {
            "value": fraction_payload(line) if state == "TRUE" else None,
            "source": ("R20_MA20" if r20_state == "TRUE" else "R5_MA5")
            if state == "TRUE" and line is not None
            else None,
            "evaluation_starts": "NEXT_OFFICIAL_SESSION",
            "role": "DIAGNOSTIC_ONLY_NOT_D2",
        },
        "session_contract": _SESSION_CONTRACT,
        "formal_admission_status": "NOT_AUTHORIZED_SOURCE_SYMBOL_DIAGNOSTIC",
        "absolute_liquidity_cny_status": "UNKNOWN_UNIT_UNVERIFIED",
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "episode_status": "NOT_ISSUED",
    }


__all__ = [
    "calculate_d0_launch_confirm_raw",
    "calculate_d0_recovery_turn_raw",
    "fraction_payload",
]
