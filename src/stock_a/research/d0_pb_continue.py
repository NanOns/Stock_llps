"""D0 STRONG_PULLBACK 与 TREND_CONTINUE 纯核及父峰评估（ALG-28/§11.6, ALG-30/§10.3）。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, localcontext
from fractions import Fraction
from typing import Any, Mapping, Sequence

from stock_a.domain import (
    ContractError,
    FieldValue,
    exact_number,
    unknown,
)
from stock_a.research.admission import Admission, SCENARIOS, admit
from stock_a.research.d0_launch_raw import _raw_amount, _window_row, fraction_payload
from stock_a.research.facts import boolean, capability, number

D0_PB_CONTINUE_CONTRACT = "D0_SCENARIO_FACTS_R3"
_QFQ_COORDINATE = "FORWARD_ADJUSTED"
_DEFAULT_CAPABILITY = "READY"

_DRAWDOWN_MIN = Fraction(3, 100)
_DRAWDOWN_MAX = Fraction(12, 100)
_MA20_CONTROL_MIN = Fraction(97, 100)
_OLS_R2_MIN = Fraction(2, 5)
_CONTRACTION_MAX = Fraction(17, 20)
_CLV_MIN = Fraction(11, 20)
_MA10_TOLERANCE = Fraction(101, 100)
_RPS20_MIN = Fraction(70)
_AMR20_MIN = Fraction(4, 5)
_AMR20_MAX = Fraction(5, 2)


@dataclass(frozen=True)
class PeakEvent:
    """父 episode 的冻结峰值事件（§11.6）；今日新峰仅从 t+1 起供 D0 使用。"""

    peak_event_id: str
    entity_id: str
    parent_episode_id: str
    peak_session: date
    peak_ordinal: int
    peak_seq: int
    peak_high: Fraction
    peak_close: Fraction
    coordinate_id: str
    source_ref: str
    created_from: str  # BIRTH_INITIAL / NEW_HIGH


@dataclass(frozen=True)
class FormalParentContext:
    """t-1 合法 FORMAL 父 episode 及其冻结峰与关联旧 support 绑定。"""

    episode_id: str
    entity_id: str
    scenario: str
    birth_ordinal: int
    prior_peak: PeakEvent | None
    support_anchor_id: str | None
    support_lower: FieldValue
    support_valid: FieldValue


@dataclass(frozen=True)
class PullbackParentDetail:
    """STRONG_PULLBACK 单父/峰评估明细与准入结果。"""

    parent_episode_id: str
    peak_event_id: str | None
    peak_ordinal: int | None
    raw: FieldValue
    reason_code: str
    predicates: dict[str, dict[str, Any]]
    admission: Admission


@dataclass(frozen=True)
class ContinueParentDetail:
    """TREND_CONTINUE 单父评估明细与准入结果。"""

    parent_episode_id: str
    raw: FieldValue
    reason_code: str
    predicates: dict[str, dict[str, Any]]
    admission: Admission


@dataclass(frozen=True)
class ScenarioBatchResult:
    """单场景逐父明细与证券级归约结果。"""

    scenario: str
    raw_summary: FieldValue
    summary_reason: str
    details: tuple[PullbackParentDetail | ContinueParentDetail, ...]
    admitted_events: tuple[Any, ...]


def make_peak_event_id(
    *,
    parent_episode_id: str,
    peak_ordinal: int,
    peak_high: Fraction,
    peak_close: Fraction,
    coordinate_id: str,
) -> str:
    """按普通业务键生成确定的峰值事件标识。"""
    return repr(
        (
            D0_PB_CONTINUE_CONTRACT,
            "PEAK_EVENT",
            parent_episode_id,
            peak_ordinal,
            peak_high,
            peak_close,
            coordinate_id,
        )
    )


def create_birth_peak_event(
    *,
    entity_id: str,
    parent_episode_id: str,
    birth_session: date,
    birth_ordinal: int,
    high: Fraction,
    close: Fraction,
    coordinate_id: str,
    source_ref: str,
) -> PeakEvent:
    """为新出生的 FORMAL 股票 episode 创建初始峰值事件（§11.6）。"""
    if high <= 0 or close <= 0 or close > high:
        raise ValueError("出生峰值 OHLC 非法")
    peak_id = make_peak_event_id(
        parent_episode_id=parent_episode_id,
        peak_ordinal=birth_ordinal,
        peak_high=high,
        peak_close=close,
        coordinate_id=coordinate_id,
    )
    return PeakEvent(
        peak_event_id=peak_id,
        entity_id=entity_id,
        parent_episode_id=parent_episode_id,
        peak_session=birth_session,
        peak_ordinal=birth_ordinal,
        peak_seq=1,
        peak_high=high,
        peak_close=close,
        coordinate_id=coordinate_id,
        source_ref=source_ref,
        created_from="BIRTH_INITIAL",
    )


def evaluate_new_peak_event(
    *,
    entity_id: str,
    parent_episode_id: str,
    prior_peak: PeakEvent,
    session: date,
    ordinal: int,
    actual: FieldValue,
    high: FieldValue,
    close: FieldValue,
    coordinate_id: str,
    source_ref: str,
    comparable: FieldValue | None = None,
) -> PeakEvent | None:
    """按 D1 实际 high 严格创新高（H > 旧 peak_high，同高取最早，同经济坐标才比较）生成新 peak_event。"""
    if prior_peak.parent_episode_id != parent_episode_id:
        raise ValueError("旧峰值与父 episode 不匹配")
    if ordinal <= prior_peak.peak_ordinal:
        raise ValueError("新峰会话序号必须严格晚于旧峰")
    if prior_peak.coordinate_id != coordinate_id:
        return None
    if comparable is not None and boolean(comparable) is not True:
        return None
    if boolean(actual) is not True:
        return None
    h_val = number(high, positive=True)
    c_val = number(close, positive=True)
    if h_val is None or c_val is None:
        return None
    if c_val > h_val:
        raise ValueError("当日 close 不得大于 high")
    if h_val <= prior_peak.peak_high:
        return None
    peak_id = make_peak_event_id(
        parent_episode_id=parent_episode_id,
        peak_ordinal=ordinal,
        peak_high=h_val,
        peak_close=c_val,
        coordinate_id=coordinate_id,
    )
    return PeakEvent(
        peak_event_id=peak_id,
        entity_id=entity_id,
        parent_episode_id=parent_episode_id,
        peak_session=session,
        peak_ordinal=ordinal,
        peak_seq=prior_peak.peak_seq + 1,
        peak_high=h_val,
        peak_close=c_val,
        coordinate_id=coordinate_id,
        source_ref=source_ref,
        created_from="NEW_HIGH",
    )


def _pred(
    field: FieldValue,
    formula: str,
    **inputs: Any,
) -> dict[str, Any]:
    state = (
        "TRUE"
        if field.quality == "OBSERVED" and field.value is True
        else "FALSE"
        if field.quality == "OBSERVED" and field.value is False
        else "UNKNOWN"
    )
    return {
        "state": state,
        "reason": field.reason,
        "formula": formula,
        "inputs": {
            k: fraction_payload(v) if isinstance(v, Fraction) else v
            for k, v in inputs.items()
        },
    }


def _tri_and_fields(fields: Sequence[FieldValue]) -> FieldValue:
    vals = [boolean(f) for f in fields]
    if any(v is False for v in vals):
        return FieldValue(False)
    unk = next((f for f, v in zip(fields, vals) if v is None), None)
    if unk is not None:
        return unk
    return FieldValue(True)


def _tri_or_fields(fields: Sequence[FieldValue]) -> FieldValue:
    vals = [boolean(f) for f in fields]
    if any(v is True for v in vals):
        return FieldValue(True)
    unk = next((f for f, v in zip(fields, vals) if v is None), None)
    if unk is not None:
        return unk
    return FieldValue(False)


def _bool_field(
    value: bool | None, true_reason: str, false_reason: str, unknown_reason: str
) -> FieldValue:
    if value is True:
        return FieldValue(True)
    if value is False:
        return FieldValue(False)
    return unknown(unknown_reason)


def compute_ols_log20(
    closes: Sequence[Fraction],
) -> tuple[FieldValue, FieldValue, str | None]:
    """对连续 20 个正收盘价的 ln(close) 做 OLS 拟合，返回 (slope, r2, reason)。"""
    if len(closes) != 20 or any(c <= 0 for c in closes):
        return (
            unknown("OLS_WINDOW_INVALID"),
            unknown("OLS_WINDOW_INVALID"),
            "OLS_WINDOW_INVALID",
        )
    if len(set(closes)) == 1:
        return (
            FieldValue(Fraction(0)),
            unknown("OLS_SST_ZERO"),
            "OLS_SST_ZERO",
        )
    with localcontext() as ctx:
        ctx.prec = 50
        ys = [(Decimal(c.numerator) / Decimal(c.denominator)).ln() for c in closes]
        y_mean = sum(ys, Decimal(0)) / Decimal(20)
        x_mean = Decimal("9.5")
        sxx = Decimal(665)  # sum((k - 9.5)^2 for k in range(20)) == 665
        sxy = sum(
            ((Decimal(idx) - x_mean) * (y - y_mean) for idx, y in enumerate(ys)),
            Decimal(0),
        )
        sst = sum(((y - y_mean) ** 2 for y in ys), Decimal(0))
        slope = sxy / sxx
        if sst == 0:
            return (
                FieldValue(Fraction(slope)),
                unknown("OLS_SST_ZERO"),
                "OLS_SST_ZERO",
            )
        ssr = (sxy * sxy) / sxx
        r2 = ssr / sst
        if r2 < 0:
            r2 = Decimal(0)
        elif r2 > 1:
            r2 = Decimal(1)
        return (
            FieldValue(Fraction(slope)),
            FieldValue(Fraction(r2)),
            None,
        )


def _validate_strict_history(
    *,
    sessions: Sequence[date],
    rows_by_session: Mapping[date, Mapping[str, Any] | None],
    source_symbol: str,
    source_capture_id: int,
    price_coordinate: str,
) -> tuple[
    tuple[date, ...],
    list[tuple[Mapping[str, Any] | None, dict[str, Fraction] | None, str | None]],
]:
    session_tuple = tuple(sessions)
    if (
        not session_tuple
        or any(type(day) is not date for day in session_tuple)
        or tuple(sorted(set(session_tuple))) != session_tuple
    ):
        raise ValueError("官方会话必须有序且唯一")
    if price_coordinate != _QFQ_COORDINATE:
        raise ValueError("D0 严格会话计算仅支持 FORWARD_ADJUSTED 坐标")
    checked: list[
        tuple[Mapping[str, Any] | None, dict[str, Fraction] | None, str | None]
    ] = []
    for day in session_tuple:
        row, values, error = _window_row(
            day,
            rows_by_session,
            source_symbol=source_symbol,
            source_capture_id=source_capture_id,
            price_coordinate=price_coordinate,
        )
        checked.append((row, values, error))
    return session_tuple, checked


def _d0_ma(
    checked: Sequence[
        tuple[Mapping[str, Any] | None, dict[str, Fraction] | None, str | None]
    ],
    end_index: int,
    period: int,
) -> tuple[Fraction | None, str | None]:
    if end_index < period - 1:
        return None, "OFFICIAL_WINDOW_LEFT_TRUNCATED"
    window = checked[end_index - period + 1 : end_index + 1]
    if len(window) != period or any(vals is None for _, vals, _ in window):
        first_err = next(
            (err for _, vals, err in window if vals is None and err),
            "STRICT_MA_WINDOW_UNAVAILABLE",
        )
        return None, first_err
    return sum(
        (vals["close"] for _, vals, _ in window if vals is not None), Fraction(0)
    ) / period, None


def _d0_phh20(
    checked: Sequence[
        tuple[Mapping[str, Any] | None, dict[str, Fraction] | None, str | None]
    ],
    end_index: int,
) -> tuple[Fraction | None, str | None]:
    if end_index < 20:
        return None, "OFFICIAL_WINDOW_LEFT_TRUNCATED"
    window = checked[end_index - 20 : end_index]
    if len(window) != 20 or any(vals is None for _, vals, _ in window):
        first_err = next(
            (err for _, vals, err in window if vals is None and err),
            "STRICT_PHH20_WINDOW_UNAVAILABLE",
        )
        return None, first_err
    return max(vals["high"] for _, vals, _ in window if vals is not None), None


def _d0_amr20(
    sessions: Sequence[date],
    checked: Sequence[
        tuple[Mapping[str, Any] | None, dict[str, Fraction] | None, str | None]
    ],
    end_index: int,
    *,
    source_capture_id: int,
) -> tuple[Fraction | None, str | None]:
    if end_index < 20:
        return None, "OFFICIAL_WINDOW_LEFT_TRUNCATED"
    amounts: list[Fraction] = []
    for idx in range(end_index - 20, end_index + 1):
        row, _, err = checked[idx]
        amt, amt_err = _raw_amount(
            sessions[idx], row, source_capture_id=source_capture_id
        )
        if (
            err
            or amt_err
            or row is None
            or row.get("amount_unit_status") != "CNY_VERIFIED"
        ):
            return None, err or amt_err or "AMOUNT_UNIT_MISMATCH"
        amounts.append(amt)
    prior_mean = sum(amounts[:-1], Fraction(0)) / 20
    if prior_mean <= 0:
        return None, "AMOUNT_MEAN_DENOMINATOR_ZERO"
    return amounts[-1] / prior_mean, None


def _reduce_parent_raws(
    raws: Sequence[tuple[FieldValue, str]],
    *,
    empty_reason: str,
    lineage_unknown: bool,
) -> tuple[FieldValue, str]:
    if not raws:
        if lineage_unknown:
            return unknown(
                "PRIOR_PARENT_LINEAGE_UNKNOWN"
            ), "PRIOR_PARENT_LINEAGE_UNKNOWN"
        return FieldValue(False), empty_reason
    # 多匹配 OR 归约：TRUE 支配 UNKNOWN 支配 FALSE
    if any(r.quality == "OBSERVED" and r.value is True for r, _ in raws):
        return FieldValue(True), "PARENT_MATCH_TRUE"
    unknown_item = next(
        ((r, code) for r, code in raws if r.quality != "OBSERVED"), None
    )
    if unknown_item is not None:
        return unknown_item[0], unknown_item[1]
    return FieldValue(False), raws[0][1]


def evaluate_strong_pullback_day(
    *,
    entity_id: str,
    target_session: date,
    target_ordinal: int,
    sessions: Sequence[date],
    session_ordinals: Mapping[date, int],
    rows_by_session: Mapping[date, Mapping[str, Any] | None],
    source_symbol: str,
    source_capture_id: int,
    coordinate_id: str,
    parents: Sequence[FormalParentContext],
    prior_lineage_unknown: bool,
    gates: tuple[FieldValue, ...],
    evidence_ref_prefix: str,
    capability_state: str = _DEFAULT_CAPABILITY,
) -> ScenarioBatchResult:
    """逐父评估 STRONG_PULLBACK（§11.6）并绑定旧 support 锚执行 admission.admit。"""
    capability(capability_state)
    session_tuple, checked = _validate_strict_history(
        sessions=sessions,
        rows_by_session=rows_by_session,
        source_symbol=source_symbol,
        source_capture_id=source_capture_id,
        price_coordinate=_QFQ_COORDINATE,
    )
    if (
        session_tuple[-1] != target_session
        or session_ordinals.get(target_session) != target_ordinal
    ):
        raise ValueError("STRONG_PULLBACK 目标会话与日历序号不一致")
    ordinal_to_idx = {
        session_ordinals[day]: idx
        for idx, day in enumerate(session_tuple)
        if day in session_ordinals
    }
    i = len(session_tuple) - 1
    cur_row, cur_vals, cur_err = checked[i]
    prev_vals = checked[i - 1][1] if i >= 1 else None
    prev_err = checked[i - 1][2] if i >= 1 else "OFFICIAL_WINDOW_LEFT_TRUNCATED"

    # 预先计算全证券可复用的 t-1 OLS、MA5[t], MA5[t-1], MA10[t-1], PHH20[t]
    ols_closes: list[Fraction] = []
    ols_err: str | None = None
    if i >= 20:
        for j in range(i - 20, i):
            if checked[j][1] is None:
                ols_err = checked[j][2] or "STRICT_OLS_WINDOW_UNAVAILABLE"
                break
            ols_closes.append(checked[j][1]["close"])
    else:
        ols_err = "OFFICIAL_WINDOW_LEFT_TRUNCATED"

    if ols_err is None:
        slope_field, r2_field, ols_calc_err = compute_ols_log20(ols_closes)
        slope_ok = (
            slope_field.quality == "OBSERVED"
            and r2_field.quality == "OBSERVED"
            and exact_number(slope_field.value) > 0
            and exact_number(r2_field.value) >= _OLS_R2_MIN
        )
        if r2_field.quality != "OBSERVED":
            ols_pred_field = unknown(ols_calc_err or "OLS_R2_UNKNOWN")
        else:
            ols_pred_field = _bool_field(
                slope_ok, "OLS_TREND_TRUE", "OLS_TREND_FALSE", "OLS_UNKNOWN"
            )
    else:
        slope_field, r2_field = unknown(ols_err), unknown(ols_err)
        ols_pred_field = unknown(ols_err)

    ma5_cur, ma5_cur_err = _d0_ma(checked, i, 5)
    ma5_prev, ma5_prev_err = (
        _d0_ma(checked, i - 1, 5)
        if i >= 1
        else (None, "OFFICIAL_WINDOW_LEFT_TRUNCATED")
    )
    ma10_prev, ma10_prev_err = (
        _d0_ma(checked, i - 1, 10)
        if i >= 1
        else (None, "OFFICIAL_WINDOW_LEFT_TRUNCATED")
    )
    phh20_cur, phh20_err = _d0_phh20(checked, i)

    close_field = (
        FieldValue(cur_vals["close"])
        if cur_vals is not None
        else unknown(cur_err or "CLOSE_UNKNOWN")
    )

    if not parents:
        raw_summary, summary_reason = _reduce_parent_raws(
            (),
            empty_reason="NO_ELIGIBLE_FORMAL_PARENT",
            lineage_unknown=prior_lineage_unknown,
        )
        adm = admit(
            entity_id=entity_id,
            ordinal=target_ordinal,
            next_ordinal=target_ordinal + 1,
            coordinate_id=coordinate_id,
            scenario="STRONG_PULLBACK",
            role="FORMAL",
            raw=raw_summary,
            gates=gates,
            close=close_field,
            phc20=unknown("PB_PHC20_NOT_APPLICABLE"),
            ma5=unknown("PB_MA5_NOT_APPLICABLE"),
            ma20=unknown("PB_MA20_NOT_APPLICABLE"),
            r5=unknown("PB_R5_NOT_APPLICABLE"),
            r20=unknown("PB_R20_NOT_APPLICABLE"),
            prior_episode_id=None,
            prior_episode_valid=unknown("NO_ELIGIBLE_FORMAL_PARENT"),
            support_anchor_id=None,
            support_lower=unknown("PB_SUPPORT_NOT_APPLICABLE"),
            support_valid=unknown("PB_SUPPORT_NOT_APPLICABLE"),
            evidence_ref=f"{evidence_ref_prefix}:NO_PARENT",
            window_ref=f"{session_tuple[0].isoformat()}..{target_session.isoformat()}:PB_NO_PARENT",
            capability_state=capability_state,
        )
        return ScenarioBatchResult(
            scenario="STRONG_PULLBACK",
            raw_summary=raw_summary,
            summary_reason=summary_reason,
            details=(),
            admitted_events=(adm.event,) if adm.event is not None else (),
        )

    details: list[PullbackParentDetail] = []
    raw_pairs: list[tuple[FieldValue, str]] = []
    admitted: list[Any] = []

    for parent in parents:
        peak = parent.prior_peak
        if peak is None:
            raw_f = unknown("PRIOR_PEAK_MISSING")
            adm = admit(
                entity_id=entity_id,
                ordinal=target_ordinal,
                next_ordinal=target_ordinal + 1,
                coordinate_id=coordinate_id,
                scenario="STRONG_PULLBACK",
                role="FORMAL",
                raw=raw_f,
                gates=gates,
                close=close_field,
                phc20=unknown("PB_PHC20_NOT_APPLICABLE"),
                ma5=unknown("PB_MA5_NOT_APPLICABLE"),
                ma20=unknown("PB_MA20_NOT_APPLICABLE"),
                r5=unknown("PB_R5_NOT_APPLICABLE"),
                r20=unknown("PB_R20_NOT_APPLICABLE"),
                prior_episode_id=parent.episode_id,
                prior_episode_valid=FieldValue(True),
                support_anchor_id=parent.support_anchor_id,
                support_lower=parent.support_lower,
                support_valid=parent.support_valid,
                evidence_ref=f"{evidence_ref_prefix}:{parent.episode_id}:NO_PEAK",
                window_ref=f"{session_tuple[0].isoformat()}..{target_session.isoformat()}:PB_NO_PEAK",
                capability_state=capability_state,
            )
            details.append(
                PullbackParentDetail(
                    parent_episode_id=parent.episode_id,
                    peak_event_id=None,
                    peak_ordinal=None,
                    raw=raw_f,
                    reason_code="PRIOR_PEAK_MISSING",
                    predicates={},
                    admission=adm,
                )
            )
            raw_pairs.append((raw_f, "PRIOR_PEAK_MISSING"))
            continue

        b_ord = parent.birth_ordinal
        h_ord = peak.peak_ordinal
        p_ord = h_ord + 1
        birth_age = target_ordinal - b_ord
        peak_age = target_ordinal - h_ord

        # 1-2. 窗口年龄门
        birth_age_f = FieldValue(1 <= birth_age <= 20)
        peak_age_f = FieldValue(2 <= peak_age <= 10 and h_ord <= target_ordinal - 2)

        # 3. H_i <= peak_high（严格高于时记 RESET_NEW_HIGH）
        if cur_vals is None:
            no_new_high_f = unknown(cur_err or "CURRENT_HIGH_UNKNOWN")
        elif cur_vals["high"] > peak.peak_high:
            no_new_high_f = FieldValue(False)
        else:
            no_new_high_f = FieldValue(True)

        # 4. drawdown in [0.03, 0.12]
        if cur_vals is None:
            drawdown_f = unknown(cur_err or "CURRENT_CLOSE_UNKNOWN")
            depth = None
        else:
            depth = Fraction(1) - cur_vals["close"] / peak.peak_high
            drawdown_f = FieldValue(_DRAWDOWN_MIN <= depth <= _DRAWDOWN_MAX)

        # 5. 昨 C < peak_close
        if prev_vals is None:
            prev_below_f = unknown(prev_err or "PREV_CLOSE_UNKNOWN")
        else:
            prev_below_f = FieldValue(prev_vals["close"] < peak.peak_close)

        # 6. [p, i] 每日 C / d0.MA20 >= 0.97
        p_idx = ordinal_to_idx.get(p_ord)
        ma20_controls: list[bool | None] = []
        if p_idx is None:
            ma20_control_f = unknown("PULLBACK_WINDOW_LEFT_TRUNCATED")
        else:
            for j in range(p_idx, i + 1):
                ma20_j, ma20_j_err = _d0_ma(checked, j, 20)
                if checked[j][1] is None or ma20_j is None or ma20_j <= 0:
                    ma20_controls.append(None)
                else:
                    ma20_controls.append(
                        checked[j][1]["close"] / ma20_j >= _MA20_CONTROL_MIN
                    )
            if False in ma20_controls:
                ma20_control_f = FieldValue(False)
            elif not ma20_controls or None in ma20_controls:
                ma20_control_f = unknown("STRICT_MA20_WINDOW_UNAVAILABLE")
            else:
                ma20_control_f = FieldValue(True)

        # 7. 缩量与放量：mean(A[p..i-1]) / mean(A[h-4..h]) <= 0.85 且 A[i] >= mean(A[p..i-1])
        h_idx = ordinal_to_idx.get(h_ord)
        contraction_f = unknown("PULLBACK_AMOUNT_WINDOW_UNAVAILABLE")
        confirm_amt_f = unknown("PULLBACK_AMOUNT_WINDOW_UNAVAILABLE")
        base_mean = pull_mean = amt_ratio = expansion = None
        if h_idx is not None and p_idx is not None and h_idx >= 4 and p_idx <= i - 1:
            base_vals: list[Fraction] = []
            pull_vals: list[Fraction] = []
            amt_err_found: str | None = None
            for j in range(h_idx - 4, h_idx + 1):
                row, _, err = checked[j]
                amt, amt_err = _raw_amount(
                    session_tuple[j], row, source_capture_id=source_capture_id
                )
                if (
                    err
                    or amt_err
                    or row is None
                    or row.get("amount_unit_status") != "CNY_VERIFIED"
                ):
                    amt_err_found = err or amt_err or "AMOUNT_UNIT_MISMATCH"
                    break
                base_vals.append(amt)
            if amt_err_found is None:
                for j in range(p_idx, i):
                    row, _, err = checked[j]
                    amt, amt_err = _raw_amount(
                        session_tuple[j], row, source_capture_id=source_capture_id
                    )
                    if (
                        err
                        or amt_err
                        or row is None
                        or row.get("amount_unit_status") != "CNY_VERIFIED"
                    ):
                        amt_err_found = err or amt_err or "AMOUNT_UNIT_MISMATCH"
                        break
                    pull_vals.append(amt)
            cur_amt = None
            if amt_err_found is None:
                cur_amt, cur_amt_err = _raw_amount(
                    session_tuple[i], cur_row, source_capture_id=source_capture_id
                )
                if (
                    cur_err
                    or cur_amt_err
                    or cur_row is None
                    or cur_row.get("amount_unit_status") != "CNY_VERIFIED"
                ):
                    amt_err_found = cur_err or cur_amt_err or "AMOUNT_UNIT_MISMATCH"
            if amt_err_found is not None:
                contraction_f = unknown(amt_err_found)
                confirm_amt_f = unknown(amt_err_found)
            else:
                base_mean = sum(base_vals, Fraction(0)) / 5
                pull_mean = sum(pull_vals, Fraction(0)) / len(pull_vals)
                if base_mean <= 0:
                    contraction_f = unknown("BASE_AMOUNT_MEAN_ZERO")
                else:
                    amt_ratio = pull_mean / base_mean
                    contraction_f = FieldValue(amt_ratio <= _CONTRACTION_MAX)
                if pull_mean <= 0 or cur_amt is None:
                    confirm_amt_f = unknown("PULLBACK_AMOUNT_MEAN_ZERO")
                else:
                    expansion = cur_amt / pull_mean
                    confirm_amt_f = FieldValue(cur_amt >= pull_mean)
        elif h_idx is None or p_idx is None or h_idx < 4:
            contraction_f = unknown("AMOUNT_WINDOW_LEFT_TRUNCATED")
            confirm_amt_f = unknown("AMOUNT_WINDOW_LEFT_TRUNCATED")

        # 8. C >= 昨 C
        if cur_vals is None or prev_vals is None:
            close_ge_prev_f = unknown(cur_err or prev_err or "CLOSE_HISTORY_UNKNOWN")
        else:
            close_ge_prev_f = FieldValue(cur_vals["close"] >= prev_vals["close"])

        # 9. CLV >= 0.55
        if cur_vals is None:
            clv_f = unknown(cur_err or "CLV_INPUT_UNKNOWN")
            clv_val = None
        elif cur_vals["high"] == cur_vals["low"]:
            clv_f = unknown("CLV_RANGE_ZERO")
            clv_val = None
        else:
            clv_val = (cur_vals["close"] - cur_vals["low"]) / (
                cur_vals["high"] - cur_vals["low"]
            )
            clv_f = FieldValue(clv_val >= _CLV_MIN)

        # 10. reclaim MA5 或 (L_i <= 1.01 * 昨MA10 and C_i >= 昨MA10)
        if prev_vals is None or cur_vals is None or ma5_prev is None or ma5_cur is None:
            reclaim_ma5_f = unknown(
                cur_err
                or prev_err
                or ma5_prev_err
                or ma5_cur_err
                or "MA5_RECLAIM_UNKNOWN"
            )
        else:
            reclaim_ma5_f = FieldValue(
                prev_vals["close"] <= ma5_prev and cur_vals["close"] > ma5_cur
            )
        if cur_vals is None or ma10_prev is None:
            ma10_test_f = unknown(cur_err or ma10_prev_err or "MA10_TEST_UNKNOWN")
        else:
            ma10_test_f = FieldValue(
                cur_vals["low"] <= _MA10_TOLERANCE * ma10_prev
                and cur_vals["close"] >= ma10_prev
            )
        trigger_f = _tri_or_fields((reclaim_ma5_f, ma10_test_f))

        # 11. 无日线冲高回落: NOT(H_i > d0.PHH20_i AND C_i <= d0.PHH20_i)
        if cur_vals is None or phh20_cur is None:
            no_shadow_f = unknown(cur_err or phh20_err or "PHH20_UNKNOWN")
        else:
            rejected = cur_vals["high"] > phh20_cur and cur_vals["close"] <= phh20_cur
            no_shadow_f = FieldValue(not rejected)

        predicates = {
            "birth_age": _pred(birth_age_f, "1<=i-b<=20", birth_age=birth_age),
            "peak_age": _pred(peak_age_f, "2<=i-h<=10", peak_age=peak_age),
            "no_new_high": _pred(
                no_new_high_f,
                "H_i<=peak_high",
                peak_high=peak.peak_high,
                current_high=cur_vals["high"] if cur_vals else None,
            ),
            "drawdown": _pred(drawdown_f, "0.03<=1-C_i/peak_high<=0.12", depth=depth),
            "previous_below_peak_close": _pred(
                prev_below_f, "C_{i-1}<peak_close", peak_close=peak.peak_close
            ),
            "ma20_control": _pred(ma20_control_f, "all C[p..i]/d0.MA20>=0.97"),
            "prior_ols": _pred(
                ols_pred_field,
                "OLS_slope(t-1)>0 AND OLS_R2(t-1)>=0.40",
                slope=slope_field.value if slope_field.quality == "OBSERVED" else None,
                r2=r2_field.value if r2_field.quality == "OBSERVED" else None,
            ),
            "contraction": _pred(
                contraction_f,
                "mean(A[p..i-1])/mean(A[h-4..h])<=0.85",
                ratio=amt_ratio,
                base_mean=base_mean,
                pullback_mean=pull_mean,
            ),
            "confirmation_amount": _pred(
                confirm_amt_f, "A[i]>=mean(A[p..i-1])", ratio=expansion
            ),
            "close_ge_prev": _pred(close_ge_prev_f, "C[i]>=C[i-1]"),
            "clv": _pred(clv_f, "CLV[i]>=0.55", clv=clv_val),
            "reclaim_or_ma10": _pred(
                trigger_f,
                "(C[i-1]<=MA5[i-1] AND C[i]>MA5[i]) OR (L[i]<=1.01*MA10[i-1] AND C[i]>=MA10[i-1])",
            ),
            "no_shadow_rejection": _pred(
                no_shadow_f,
                "NOT(H[i]>d0.PHH20[i] AND C[i]<=d0.PHH20[i])",
                phh20=phh20_cur,
            ),
        }

        ordered_fields = (
            birth_age_f,
            peak_age_f,
            no_new_high_f,
            drawdown_f,
            prev_below_f,
            ma20_control_f,
            ols_pred_field,
            contraction_f,
            confirm_amt_f,
            close_ge_prev_f,
            clv_f,
            trigger_f,
            no_shadow_f,
        )
        raw_combined = _tri_and_fields(ordered_fields)
        if no_new_high_f.quality == "OBSERVED" and no_new_high_f.value is False:
            reason_code = "RESET_NEW_HIGH"
            raw_combined = FieldValue(False)
        elif raw_combined.quality == "OBSERVED" and raw_combined.value is True:
            reason_code = "STRONG_PULLBACK_RAW_TRUE"
        elif raw_combined.quality == "OBSERVED" and raw_combined.value is False:
            reason_code = "STRONG_PULLBACK_RAW_FALSE"
        else:
            first_unk = next(
                (
                    f.reason
                    for f in ordered_fields
                    if f.quality != "OBSERVED" and f.reason
                ),
                "PB_INPUT_UNKNOWN",
            )
            reason_code = first_unk

        evidence_ref = f"{evidence_ref_prefix}:{parent.episode_id}:{peak.peak_event_id}"
        window_ref = f"{session_tuple[0].isoformat()}..{target_session.isoformat()}:PB_P{parent.birth_ordinal}_H{peak.peak_ordinal}"
        adm = admit(
            entity_id=entity_id,
            ordinal=target_ordinal,
            next_ordinal=target_ordinal + 1,
            coordinate_id=coordinate_id,
            scenario="STRONG_PULLBACK",
            role="FORMAL",
            raw=raw_combined,
            gates=gates,
            close=close_field,
            phc20=unknown("PB_PHC20_NOT_APPLICABLE"),
            ma5=unknown("PB_MA5_NOT_APPLICABLE"),
            ma20=unknown("PB_MA20_NOT_APPLICABLE"),
            r5=unknown("PB_R5_NOT_APPLICABLE"),
            r20=unknown("PB_R20_NOT_APPLICABLE"),
            prior_episode_id=parent.episode_id,
            prior_episode_valid=FieldValue(True),
            support_anchor_id=parent.support_anchor_id,
            support_lower=parent.support_lower,
            support_valid=parent.support_valid,
            evidence_ref=evidence_ref,
            window_ref=window_ref,
            capability_state=capability_state,
        )
        details.append(
            PullbackParentDetail(
                parent_episode_id=parent.episode_id,
                peak_event_id=peak.peak_event_id,
                peak_ordinal=peak.peak_ordinal,
                raw=raw_combined,
                reason_code=reason_code,
                predicates=predicates,
                admission=adm,
            )
        )
        raw_pairs.append((raw_combined, reason_code))
        if adm.event is not None:
            admitted.append(adm.event)

    raw_summary, summary_reason = _reduce_parent_raws(
        raw_pairs,
        empty_reason="NO_ELIGIBLE_FORMAL_PARENT",
        lineage_unknown=prior_lineage_unknown,
    )
    return ScenarioBatchResult(
        scenario="STRONG_PULLBACK",
        raw_summary=raw_summary,
        summary_reason=summary_reason,
        details=tuple(details),
        admitted_events=tuple(admitted),
    )


def _eval_loo_confirmed_raw(
    loo_confirmed_raws: Sequence[FieldValue] | None,
) -> FieldValue:
    """至少一个目标剔除后板块 loo_confirmed_raw = TRUE；不读板块 D2[t]。"""
    if loo_confirmed_raws is None:
        return unknown("LOO_CONFIRMED_RAW_UNAVAILABLE")
    if not loo_confirmed_raws:
        return FieldValue(False)
    return _tri_or_fields(tuple(loo_confirmed_raws))


def evaluate_trend_continue_day(
    *,
    entity_id: str,
    target_session: date,
    target_ordinal: int,
    sessions: Sequence[date],
    session_ordinals: Mapping[date, int],
    rows_by_session: Mapping[date, Mapping[str, Any] | None],
    source_symbol: str,
    source_capture_id: int,
    coordinate_id: str,
    parents: Sequence[FormalParentContext],
    prior_lineage_unknown: bool,
    gates: tuple[FieldValue, ...],
    core_ma_slope20_atr: FieldValue,
    core_prior_high20: FieldValue,
    rps20: FieldValue,
    loo_confirmed_raws: Sequence[FieldValue] | None,
    evidence_ref_prefix: str,
    capability_state: str = _DEFAULT_CAPABILITY,
) -> ScenarioBatchResult:
    """逐父评估 TREND_CONTINUE（§11.6）并调用 admission.admit（支持无旧锚时 T0_MA20_FALLBACK）。"""
    capability(capability_state)
    session_tuple, checked = _validate_strict_history(
        sessions=sessions,
        rows_by_session=rows_by_session,
        source_symbol=source_symbol,
        source_capture_id=source_capture_id,
        price_coordinate=_QFQ_COORDINATE,
    )
    if (
        session_tuple[-1] != target_session
        or session_ordinals.get(target_session) != target_ordinal
    ):
        raise ValueError("TREND_CONTINUE 目标会话与日历序号不一致")
    i = len(session_tuple) - 1
    _, cur_vals, cur_err = checked[i]
    prev_vals = checked[i - 1][1] if i >= 1 else None
    prev_err = checked[i - 1][2] if i >= 1 else "OFFICIAL_WINDOW_LEFT_TRUNCATED"

    ma5_cur, ma5_err = _d0_ma(checked, i, 5)
    ma20_cur, ma20_err = _d0_ma(checked, i, 20)
    amr20_cur, amr20_err = _d0_amr20(
        session_tuple, checked, i, source_capture_id=source_capture_id
    )

    close_field = (
        FieldValue(cur_vals["close"])
        if cur_vals is not None
        else unknown(cur_err or "CLOSE_UNKNOWN")
    )
    ma20_field = (
        FieldValue(ma20_cur)
        if ma20_cur is not None
        else unknown(ma20_err or "D0_MA20_UNKNOWN")
    )

    # 1. C >= MA5 >= MA20
    if cur_vals is None or ma5_cur is None or ma20_cur is None:
        ma_align_f = unknown(cur_err or ma5_err or ma20_err or "MA_ALIGNMENT_UNKNOWN")
    else:
        ma_align_f = FieldValue(cur_vals["close"] >= ma5_cur >= ma20_cur)

    # 2. core.ma_slope20_atr > 0
    slope_num = number(core_ma_slope20_atr)
    if slope_num is None:
        slope_f = unknown(core_ma_slope20_atr.reason or "CORE_MA_SLOPE20_ATR_UNKNOWN")
    else:
        slope_f = FieldValue(slope_num > 0)

    # 3. RPS20 >= 70
    rps_num = number(rps20)
    if rps_num is None:
        rps_f = unknown(rps20.reason or "RPS20_UNKNOWN")
    else:
        rps_f = FieldValue(rps_num >= _RPS20_MIN)

    # 4. ret1 > 0
    if cur_vals is None or prev_vals is None:
        ret1_f = unknown(cur_err or prev_err or "RET1_UNKNOWN")
    else:
        ret1_f = FieldValue(cur_vals["close"] > prev_vals["close"])

    # 5. C > core.prior_high20
    ph20_num = number(core_prior_high20, positive=True)
    if cur_vals is None or ph20_num is None:
        ph20_f = unknown(
            cur_err or core_prior_high20.reason or "CORE_PRIOR_HIGH20_UNKNOWN"
        )
    else:
        ph20_f = FieldValue(cur_vals["close"] > ph20_num)

    # 6. CLV >= 0.55
    if cur_vals is None:
        clv_f = unknown(cur_err or "CLV_UNKNOWN")
        clv_val = None
    elif cur_vals["high"] == cur_vals["low"]:
        clv_f = unknown("CLV_RANGE_ZERO")
        clv_val = None
    else:
        clv_val = (cur_vals["close"] - cur_vals["low"]) / (
            cur_vals["high"] - cur_vals["low"]
        )
        clv_f = FieldValue(clv_val >= _CLV_MIN)

    # 7. 0.8 <= amr20 <= 2.5
    if amr20_cur is None:
        amr_f = unknown(amr20_err or "AMR20_UNKNOWN")
    else:
        amr_f = FieldValue(_AMR20_MIN <= amr20_cur <= _AMR20_MAX)

    # 8. 至少一个目标剔除后板块 loo_confirmed_raw = TRUE
    loo_f = _eval_loo_confirmed_raw(loo_confirmed_raws)

    predicates = {
        "ma_alignment": _pred(
            ma_align_f, "C>=d0.MA5>=d0.MA20", ma5=ma5_cur, ma20=ma20_cur
        ),
        "core_ma_slope20_atr": _pred(slope_f, "core.ma_slope20_atr>0", value=slope_num),
        "rps20": _pred(rps_f, "RPS20>=70", value=rps_num),
        "positive_ret1": _pred(ret1_f, "C[i]>C[i-1]"),
        "above_core_prior_high20": _pred(
            ph20_f, "C[i]>core.prior_high20", prior_high20=ph20_num
        ),
        "clv": _pred(clv_f, "CLV[i]>=0.55", clv=clv_val),
        "amr20_band": _pred(amr_f, "0.8<=d0.amr20<=2.5", amr20=amr20_cur),
        "loo_confirmed_raw": _pred(loo_f, "ANY(loo_sector_confirmed_raw==TRUE)"),
    }
    ordered_fields = (ma_align_f, slope_f, rps_f, ret1_f, ph20_f, clv_f, amr_f, loo_f)
    base_raw = _tri_and_fields(ordered_fields)

    if not parents:
        raw_summary, summary_reason = _reduce_parent_raws(
            (),
            empty_reason="NO_ELIGIBLE_FORMAL_PARENT",
            lineage_unknown=prior_lineage_unknown,
        )
        adm = admit(
            entity_id=entity_id,
            ordinal=target_ordinal,
            next_ordinal=target_ordinal + 1,
            coordinate_id=coordinate_id,
            scenario="TREND_CONTINUE",
            role="FORMAL",
            raw=raw_summary,
            gates=gates,
            close=close_field,
            phc20=unknown("CONTINUE_PHC20_NOT_APPLICABLE"),
            ma5=unknown("CONTINUE_MA5_NOT_APPLICABLE"),
            ma20=ma20_field,
            r5=unknown("CONTINUE_R5_NOT_APPLICABLE"),
            r20=unknown("CONTINUE_R20_NOT_APPLICABLE"),
            prior_episode_id=None,
            prior_episode_valid=unknown("NO_ELIGIBLE_FORMAL_PARENT"),
            support_anchor_id=None,
            support_lower=unknown("CONTINUE_SUPPORT_NOT_APPLICABLE"),
            support_valid=unknown("CONTINUE_SUPPORT_NOT_APPLICABLE"),
            evidence_ref=f"{evidence_ref_prefix}:NO_PARENT",
            window_ref=f"{session_tuple[0].isoformat()}..{target_session.isoformat()}:CONTINUE_NO_PARENT",
            capability_state=capability_state,
        )
        return ScenarioBatchResult(
            scenario="TREND_CONTINUE",
            raw_summary=raw_summary,
            summary_reason=summary_reason,
            details=(),
            admitted_events=(adm.event,) if adm.event is not None else (),
        )

    if base_raw.quality == "OBSERVED" and base_raw.value is True:
        reason_code = "TREND_CONTINUE_RAW_TRUE"
    elif base_raw.quality == "OBSERVED" and base_raw.value is False:
        reason_code = "TREND_CONTINUE_RAW_FALSE"
    else:
        reason_code = next(
            (f.reason for f in ordered_fields if f.quality != "OBSERVED" and f.reason),
            "CONTINUE_INPUT_UNKNOWN",
        )

    details: list[ContinueParentDetail] = []
    raw_pairs: list[tuple[FieldValue, str]] = []
    admitted: list[Any] = []
    for parent in parents:
        evidence_ref = f"{evidence_ref_prefix}:{parent.episode_id}"
        window_ref = f"{session_tuple[0].isoformat()}..{target_session.isoformat()}:CONTINUE_P{parent.birth_ordinal}"
        adm = admit(
            entity_id=entity_id,
            ordinal=target_ordinal,
            next_ordinal=target_ordinal + 1,
            coordinate_id=coordinate_id,
            scenario="TREND_CONTINUE",
            role="FORMAL",
            raw=base_raw,
            gates=gates,
            close=close_field,
            phc20=unknown("CONTINUE_PHC20_NOT_APPLICABLE"),
            ma5=unknown("CONTINUE_MA5_NOT_APPLICABLE"),
            ma20=ma20_field,
            r5=unknown("CONTINUE_R5_NOT_APPLICABLE"),
            r20=unknown("CONTINUE_R20_NOT_APPLICABLE"),
            prior_episode_id=parent.episode_id,
            prior_episode_valid=FieldValue(True),
            support_anchor_id=parent.support_anchor_id,
            support_lower=parent.support_lower,
            support_valid=parent.support_valid,
            evidence_ref=evidence_ref,
            window_ref=window_ref,
            capability_state=capability_state,
        )
        details.append(
            ContinueParentDetail(
                parent_episode_id=parent.episode_id,
                raw=base_raw,
                reason_code=reason_code,
                predicates=predicates,
                admission=adm,
            )
        )
        raw_pairs.append((base_raw, reason_code))
        if adm.event is not None:
            admitted.append(adm.event)

    raw_summary, summary_reason = _reduce_parent_raws(
        raw_pairs,
        empty_reason="NO_ELIGIBLE_FORMAL_PARENT",
        lineage_unknown=prior_lineage_unknown,
    )
    return ScenarioBatchResult(
        scenario="TREND_CONTINUE",
        raw_summary=raw_summary,
        summary_reason=summary_reason,
        details=tuple(details),
        admitted_events=tuple(admitted),
    )


__all__ = [
    "ContinueParentDetail",
    "D0_PB_CONTINUE_CONTRACT",
    "FormalParentContext",
    "PeakEvent",
    "PullbackParentDetail",
    "ScenarioBatchResult",
    "compute_ols_log20",
    "create_birth_peak_event",
    "evaluate_new_peak_event",
    "evaluate_strong_pullback_day",
    "evaluate_trend_continue_day",
    "make_peak_event_id",
]
