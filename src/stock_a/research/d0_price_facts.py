"""D0首日价格损伤与ALG-27辅助原语：严格使用连续官方市场会话。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation, localcontext
from fractions import Fraction
from typing import Any, Mapping, Sequence

from stock_a.research.entry_history.support import trade_status_state_from_pg_row


@dataclass(frozen=True)
class D0FirstDayDamage:
    """D0首日损伤三态事实及其精确计算值。"""

    state: str
    quality: str
    reason_code: str
    reason: str
    current_close: Fraction | None
    ma20: Fraction | None
    close_to_ma20: Fraction | None
    official_sessions: tuple[date, ...]
    missing_sessions: tuple[date, ...]
    source_capture_id: int | None = None
    price_coordinate: str = "FORWARD_ADJUSTED"
    time_role: str = "NON_PIT_RETROSPECTIVE"


def _unknown(
    reason_code: str,
    *,
    sessions: tuple[date, ...],
    missing: tuple[date, ...] = (),
    source_capture_id: int | None = None,
    price_coordinate: str = "FORWARD_ADJUSTED",
) -> D0FirstDayDamage:
    return D0FirstDayDamage(
        state="UNKNOWN",
        quality="UNKNOWN",
        reason_code=reason_code,
        reason=_REASONS.get(reason_code, "原始价格窗口不满足计算质量要求。"),
        current_close=None,
        ma20=None,
        close_to_ma20=None,
        official_sessions=sessions,
        missing_sessions=missing,
        source_capture_id=source_capture_id,
        price_coordinate=price_coordinate,
    )


def calculate_d0_first_day_damage(
    *,
    target_session: date,
    official_sessions: Sequence[date],
    history_rows_by_session: Mapping[date, Mapping[str, object]],
    source_symbol: str,
    source_capture_id: int,
    price_coordinate: str = "FORWARD_ADJUSTED",
) -> D0FirstDayDamage:
    """按目标日及其前19个会话以同一QFQ坐标计算C/MA20<97/100。"""
    if type(source_capture_id) is not int or source_capture_id <= 0:
        raise ValueError("D0损伤必须绑定paired source_capture_id")
    if price_coordinate != "FORWARD_ADJUSTED":
        return _unknown(
            "QFQ_COORDINATE_REQUIRED",
            sessions=tuple(official_sessions),
            source_capture_id=source_capture_id,
            price_coordinate=price_coordinate,
        )
    sessions = tuple(official_sessions)
    if (
        type(target_session) is not date
        or not sessions
        or any(type(session) is not date for session in sessions)
        or tuple(sorted(set(sessions))) != sessions
        or sessions[-1] != target_session
    ):
        return _unknown(
            "OFFICIAL_WINDOW_INVALID",
            sessions=sessions,
            source_capture_id=source_capture_id,
            price_coordinate=price_coordinate,
        )

    window = sessions[-20:]
    if len(window) != 20:
        return _unknown(
            "OFFICIAL_WINDOW_LEFT_TRUNCATED",
            sessions=window,
            source_capture_id=source_capture_id,
            price_coordinate=price_coordinate,
        )

    closes: list[Fraction] = []
    missing: list[date] = []
    for session in window:
        row = history_rows_by_session.get(session)
        if row is None:
            missing.append(session)
            continue
        if (
            row.get("session_date") != session
            or row.get("source_symbol") != source_symbol
            or row.get("source_capture_id") != source_capture_id
        ):
            return _unknown(
                "SOURCE_CAPTURE_IDENTITY_MISMATCH",
                sessions=window,
                source_capture_id=source_capture_id,
                price_coordinate=price_coordinate,
            )
        row_coordinate = row.get(
            "qfq_price_coordinate"
            if price_coordinate == "FORWARD_ADJUSTED"
            else "raw_price_coordinate",
            row.get("price_coordinate"),
        )
        if row_coordinate != price_coordinate:
            return _unknown(
                "PRICE_COORDINATE_MISMATCH",
                sessions=window,
                source_capture_id=source_capture_id,
                price_coordinate=price_coordinate,
            )
        response_fields = row.get("qfq_response_fields")
        if (
            row.get("qfq_adjustflag") not in (2, "2")
            or row.get("qfq_response_session_date")
            not in (
                session,
                session.isoformat(),
            )
            or str(row.get("qfq_response_source_code") or "").upper()
            != source_symbol.upper()
            or not isinstance(response_fields, (list, tuple, set, frozenset))
            or not {"date", "code", "close"}.issubset(set(response_fields))
        ):
            return _unknown(
                "QFQ_RESPONSE_IDENTITY_MISMATCH",
                sessions=window,
                source_capture_id=source_capture_id,
                price_coordinate=price_coordinate,
            )
        if row.get("qfq_bar_status") != "TARGET_PRESENT":
            return _unknown(
                "OFFICIAL_SESSION_BAR_MISSING",
                sessions=window,
                missing=(session,),
                source_capture_id=source_capture_id,
                price_coordinate=price_coordinate,
            )
        trade_state, trade_reason = trade_status_state_from_pg_row(
            row, price_coordinate="FORWARD_ADJUSTED"
        )
        if trade_state != "ACTUAL":
            return _unknown(
                trade_reason or "TRADE_STATUS_UNKNOWN",
                sessions=window,
                missing=(session,),
                source_capture_id=source_capture_id,
                price_coordinate=price_coordinate,
            )
        if row.get("qfq_close_quality") != "OBSERVED":
            reason_code = row.get("qfq_close_reason") or "QFQ_CLOSE_QUALITY_UNKNOWN"
            return _unknown(
                str(reason_code),
                sessions=window,
                source_capture_id=source_capture_id,
                price_coordinate=price_coordinate,
            )
        close_value = row.get("qfq_close")
        if (
            isinstance(close_value, bool)
            or not isinstance(close_value, (Decimal, int, Fraction))
            or isinstance(close_value, Decimal)
            and not close_value.is_finite()
        ):
            return _unknown(
                "INVALID_PRICE_CLOSE",
                sessions=window,
                source_capture_id=source_capture_id,
                price_coordinate=price_coordinate,
            )
        close_value = Fraction(close_value)
        if close_value <= 0:
            return _unknown(
                "INVALID_PRICE_CLOSE",
                sessions=window,
                source_capture_id=source_capture_id,
                price_coordinate=price_coordinate,
            )
        closes.append(close_value)

    if missing:
        return _unknown(
            "OFFICIAL_SESSION_BAR_MISSING",
            sessions=window,
            missing=tuple(missing),
            source_capture_id=source_capture_id,
            price_coordinate=price_coordinate,
        )
    current_close = closes[-1]
    ma20 = sum(closes, Fraction(0)) / 20
    close_to_ma20 = current_close / ma20
    damaged = close_to_ma20 < Fraction(97, 100)
    reason_code = "DAMAGE_THRESHOLD_TRUE" if damaged else "DAMAGE_THRESHOLD_FALSE"
    reason = (
        "目标收盘价与严格20会话均线之比低于97/100。"
        if damaged
        else "目标收盘价与严格20会话均线之比未低于97/100。"
    )
    return D0FirstDayDamage(
        state="TRUE" if damaged else "FALSE",
        quality="OBSERVED",
        reason_code=reason_code,
        reason=reason,
        current_close=current_close,
        ma20=ma20,
        close_to_ma20=close_to_ma20,
        official_sessions=window,
        missing_sessions=(),
        source_capture_id=source_capture_id,
        price_coordinate=price_coordinate,
    )


_REASONS = {
    "OFFICIAL_WINDOW_INVALID": "官方会话窗口缺少目标日或顺序不唯一。",
    "OFFICIAL_WINDOW_LEFT_TRUNCATED": "目标日前不足所需连续官方会话。",
    "SOURCE_CAPTURE_IDENTITY_MISMATCH": "历史行与目标来源代码、日期或paired capture不一致。",
    "PRICE_COORDINATE_MISMATCH": "官方会话窗口内的价格坐标不是QFQ。",
    "MIXED_PRICE_COORDINATE": "严格窗口内QFQ价格来自不同捕获且缺少同basis可比证据。",
    "OFFICIAL_SESSION_BAR_MISSING": "严格官方窗口至少一个会话缺少有效QFQ日线。",
    "CONFIRMED_SUSPENSION_NO_ACTUAL_BAR": "官方会话已证停牌，不存在实际成交Bar。",
    "INVALID_PRICE_CLOSE": "官方会话QFQ收盘价缺失、非有限或非正数。",
    "QFQ_PRICE_QUALITY_UNKNOWN": "官方会话QFQ价格质量未知。",
    "QFQ_RESPONSE_IDENTITY_MISMATCH": "QFQ响应日期或证券代码与目标会话不一致。",
    "INVALID_PRICE_OHLC": "官方会话QFQ OHLC缺失、非法或不满足价格范围关系。",
    "OLS20_OBSERVED": "严格前20个官方会话对数收盘价OLS斜率与R2已计算。",
    "OLS20_SST_ZERO": "严格前20个官方会话对数收盘价总平方差为零，R2不可确定（UNKNOWN）。",
    "PRIOR5_BELOW_TRUE": "前5个历史端点中各自C<=自身MA20的天数达到至少2天。",
    "PRIOR5_BELOW_FALSE": "前5个历史端点中各自C<=自身MA20的天数不足2天。",
    "LIQ20_THRESHOLD_TRUE": "严格前20个官方会话RAW CNY成交额中位数达到20,000,000元。",
    "LIQ20_THRESHOLD_FALSE": "严格前20个官方会话RAW CNY成交额中位数低于20,000,000元。",
    "RAW_AMOUNT_QUALITY_UNKNOWN": "严格会话RAW成交额质量未知。",
    "RAW_AMOUNT_QUALITY_BAD": "严格会话RAW成交额值非法。",
    "RAW_AMOUNT_UNIT_UNVERIFIED": "严格会话RAW成交额未绑定CNY_VERIFIED单位合同。",
    "RAW_COORDINATE_MISMATCH": "严格会话RAW成交额不是RAW_UNADJUSTED未复权坐标。",
    "SEVERE_DROP_RET1_TRUE": "目标日日收益率ret1<=-0.08，触发严重下跌。",
    "SEVERE_DROP_LOGRET_SIGMA_TRUE": "目标日对数收益率触发-max(-log(0.96), 2*prior_sigma20)严重下跌。",
    "SEVERE_DROP_EXCLUDED_BY_POSITIVE_RET1": "目标日ret1>0，直接排除严重下跌。",
    "SEVERE_DROP_FALSE": "目标日未触发ret1<=-0.08或2*prior_sigma20严重下跌阈值。",
}

_D0_LIQ20_THRESHOLD_CNY = Fraction(20_000_000)


def _to_fraction(value: Any) -> Fraction | None:
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
            dec = Decimal(value)
        except InvalidOperation:
            return None
        return Fraction(dec) if dec.is_finite() else None
    return None


def _has_qfq_comparable_evidence(
    row: Mapping[str, Any], target_row: Mapping[str, Any] | None
) -> bool:
    """判断跨捕获QFQ行是否携带明确的同basis可比证据。"""
    if row.get("qfq_basis_comparable") is True or row.get("qfq_comparable") is True:
        return True
    for key in ("qfq_basis_id", "qfq_coordinate_basis", "qfq_price_basis"):
        val = row.get(key)
        if val is not None and target_row is not None and target_row.get(key) == val:
            return True
    return False


def _validate_window_sessions(
    target_session: date,
    official_sessions: Sequence[date],
    required_len: int,
) -> tuple[tuple[date, ...] | None, str | None]:
    sessions = tuple(official_sessions)
    if (
        type(target_session) is not date
        or not sessions
        or any(type(s) is not date for s in sessions)
        or tuple(sorted(set(sessions))) != sessions
        or sessions[-1] != target_session
    ):
        return None, "OFFICIAL_WINDOW_INVALID"
    if len(sessions) < required_len:
        return sessions, "OFFICIAL_WINDOW_LEFT_TRUNCATED"
    return sessions[-required_len:], None


def _extract_strict_qfq_close(
    *,
    session: date,
    row: Mapping[str, Any] | None,
    source_symbol: str,
    target_qfq_capture_id: int | None,
    target_row: Mapping[str, Any] | None,
) -> tuple[Fraction | None, str | None]:
    """按F02规则提取单日严格QFQ收盘价：支持同basis跨捕获放行，无证跨捕获标MIXED_PRICE_COORDINATE。"""
    if row is None:
        return None, "OFFICIAL_SESSION_BAR_MISSING"
    if row.get("session_date") != session or row.get("source_symbol") != source_symbol:
        return None, "SOURCE_CAPTURE_IDENTITY_MISMATCH"
    row_qfq_cap = row.get("qfq_source_capture_id")
    if row_qfq_cap is None:
        row_qfq_cap = row.get("source_capture_id")
    if type(row_qfq_cap) is not int or row_qfq_cap <= 0:
        return None, "SOURCE_CAPTURE_IDENTITY_MISMATCH"
    if (
        target_qfq_capture_id is not None
        and row_qfq_cap != target_qfq_capture_id
        and not _has_qfq_comparable_evidence(row, target_row)
    ):
        return None, "MIXED_PRICE_COORDINATE"
    row_coord = row.get("qfq_price_coordinate", row.get("price_coordinate"))
    if row_coord != "FORWARD_ADJUSTED":
        return None, "PRICE_COORDINATE_MISMATCH"
    if row.get("qfq_adjustflag") not in (2, "2"):
        return None, "QFQ_RESPONSE_IDENTITY_MISMATCH"
    response_fields = row.get("qfq_response_fields")
    if (
        row.get("qfq_response_session_date") not in (session, session.isoformat())
        or str(row.get("qfq_response_source_code") or "").upper()
        != source_symbol.upper()
        or not isinstance(response_fields, (list, tuple, set, frozenset))
        or not {"date", "code", "close"}.issubset(set(response_fields))
    ):
        return None, "QFQ_RESPONSE_IDENTITY_MISMATCH"
    trade_state, trade_reason = trade_status_state_from_pg_row(
        row, price_coordinate="FORWARD_ADJUSTED"
    )
    if trade_state == "CONFIRMED_SUSPENSION":
        return None, "CONFIRMED_SUSPENSION_NO_ACTUAL_BAR"
    if trade_state != "ACTUAL":
        return None, trade_reason or "TRADE_STATUS_UNKNOWN"
    if row.get("qfq_bar_status") != "TARGET_PRESENT":
        return None, "OFFICIAL_SESSION_BAR_MISSING"
    if row.get("qfq_close_quality") != "OBSERVED":
        return None, str(row.get("qfq_close_reason") or "QFQ_PRICE_QUALITY_UNKNOWN")
    close_val = _to_fraction(row.get("qfq_close"))
    if close_val is None or close_val <= 0:
        return None, "INVALID_PRICE_CLOSE"
    return close_val, None


def _extract_strict_raw_amount(
    *,
    session: date,
    row: Mapping[str, Any] | None,
    source_symbol: str,
) -> tuple[Fraction | None, str | None]:
    """独立按RAW_UNADJUSTED坐标与raw_source_capture_id提取严格RAW CNY成交额。"""
    if row is None:
        return None, "OFFICIAL_SESSION_BAR_MISSING"
    if row.get("session_date") != session or row.get("source_symbol") != source_symbol:
        return None, "SOURCE_CAPTURE_IDENTITY_MISMATCH"
    row_raw_cap = row.get("raw_source_capture_id")
    if row_raw_cap is None:
        row_raw_cap = row.get("source_capture_id")
    if type(row_raw_cap) is not int or row_raw_cap <= 0:
        return None, "SOURCE_CAPTURE_IDENTITY_MISMATCH"
    if row.get("raw_adjustflag") not in (None, 3, "3"):
        return None, "RAW_COORDINATE_MISMATCH"
    trade_state, trade_reason = trade_status_state_from_pg_row(
        row, price_coordinate="RAW_UNADJUSTED"
    )
    if trade_state == "CONFIRMED_SUSPENSION":
        return None, "CONFIRMED_SUSPENSION_NO_ACTUAL_BAR"
    if trade_state != "ACTUAL":
        return None, trade_reason or "TRADE_STATUS_UNKNOWN"
    if row.get("bar_status") != "TARGET_PRESENT":
        return None, "OFFICIAL_SESSION_BAR_MISSING"
    if row.get("raw_amount_quality") == "BAD_VALUE":
        return None, "RAW_AMOUNT_QUALITY_BAD"
    if row.get("raw_amount_quality") != "RAW_SOURCE_VALUE_OBSERVED":
        return None, str(row.get("raw_amount_reason") or "RAW_AMOUNT_QUALITY_UNKNOWN")
    if row.get("amount_unit_status") != "CNY_VERIFIED":
        return None, "RAW_AMOUNT_UNIT_UNVERIFIED"
    amount = _to_fraction(row.get("raw_amount"))
    if amount is None or amount < 0:
        return None, "RAW_AMOUNT_QUALITY_BAD"
    return amount, None


def _resolve_target_qfq_capture_id(
    target_row: Mapping[str, Any] | None,
    explicit_capture_id: int | None,
) -> int | None:
    if isinstance(target_row, Mapping):
        qfq_cap = target_row.get("qfq_source_capture_id")
        if type(qfq_cap) is int and qfq_cap > 0:
            return qfq_cap
    return explicit_capture_id


@dataclass(frozen=True)
class D0Ols20Result:
    """ALG-27：前20个严格官方会话对数收盘价OLS斜率与R2。"""

    state: str
    quality: str
    reason_code: str
    reason: str
    slope: Decimal | None
    r2: Decimal | None
    slope_quality: str
    r2_quality: str
    official_sessions: tuple[date, ...]
    missing_sessions: tuple[date, ...]
    price_coordinate: str = "FORWARD_ADJUSTED"
    time_role: str = "NON_PIT_RETROSPECTIVE"


def calculate_d0_ols20(
    *,
    target_session: date,
    official_sessions: Sequence[date],
    history_rows_by_session: Mapping[date, Mapping[str, Any] | None],
    source_symbol: str,
    source_capture_id: int | None = None,
    price_coordinate: str = "FORWARD_ADJUSTED",
) -> D0Ols20Result:
    """按目标日前20个严格官方会话计算log(close)线性回归斜率与R2；SST=0时R2为UNKNOWN。"""
    if price_coordinate != "FORWARD_ADJUSTED":
        return D0Ols20Result(
            state="UNKNOWN",
            quality="UNKNOWN",
            reason_code="PRICE_COORDINATE_MISMATCH",
            reason=_REASONS["PRICE_COORDINATE_MISMATCH"],
            slope=None,
            r2=None,
            slope_quality="UNKNOWN",
            r2_quality="UNKNOWN",
            official_sessions=tuple(official_sessions),
            missing_sessions=(),
            price_coordinate=price_coordinate,
        )
    window, win_err = _validate_window_sessions(target_session, official_sessions, 21)
    if win_err or window is None:
        return D0Ols20Result(
            state="UNKNOWN",
            quality="UNKNOWN",
            reason_code=win_err or "OFFICIAL_WINDOW_INVALID",
            reason=_REASONS.get(win_err or "OFFICIAL_WINDOW_INVALID", "窗口无效。"),
            slope=None,
            r2=None,
            slope_quality="UNKNOWN",
            r2_quality="UNKNOWN",
            official_sessions=window or tuple(official_sessions),
            missing_sessions=(),
        )
    prior_20 = window[:20]
    target_row = history_rows_by_session.get(target_session)
    target_qfq_cap = _resolve_target_qfq_capture_id(target_row, source_capture_id)
    closes: list[Fraction] = []
    missing: list[date] = []
    first_err: str | None = None
    for session in prior_20:
        val, err = _extract_strict_qfq_close(
            session=session,
            row=history_rows_by_session.get(session),
            source_symbol=source_symbol,
            target_qfq_capture_id=target_qfq_cap,
            target_row=target_row,
        )
        if err is not None or val is None:
            missing.append(session)
            if first_err is None:
                first_err = err or "OFFICIAL_SESSION_BAR_MISSING"
        else:
            closes.append(val)
    if missing or len(closes) != 20:
        code = first_err or "OFFICIAL_SESSION_BAR_MISSING"
        return D0Ols20Result(
            state="UNKNOWN",
            quality="UNKNOWN",
            reason_code=code,
            reason=_REASONS.get(code, "严格官方窗口缺少有效QFQ收盘价。"),
            slope=None,
            r2=None,
            slope_quality="UNKNOWN",
            r2_quality="UNKNOWN",
            official_sessions=prior_20,
            missing_sessions=tuple(missing),
        )
    if len(set(closes)) == 1:
        return D0Ols20Result(
            state="UNKNOWN",
            quality="UNKNOWN",
            reason_code="OLS20_SST_ZERO",
            reason=_REASONS["OLS20_SST_ZERO"],
            slope=Decimal(0),
            r2=None,
            slope_quality="OBSERVED",
            r2_quality="UNKNOWN",
            official_sessions=prior_20,
            missing_sessions=(),
        )
    with localcontext() as ctx:
        ctx.prec = 34
        logs = [(Decimal(c.numerator) / Decimal(c.denominator)).ln() for c in closes]
        mean_y = sum(logs, Decimal(0)) / Decimal(20)
        mean_x = Decimal(19) / Decimal(2)
        sxx = sum(((Decimal(i) - mean_x) ** 2 for i in range(20)), Decimal(0))
        sxy = sum(
            ((Decimal(i) - mean_x) * (y - mean_y) for i, y in enumerate(logs)),
            Decimal(0),
        )
        syy = sum(((y - mean_y) ** 2 for y in logs), Decimal(0))
        slope = sxy / sxx
        if syy == 0:
            return D0Ols20Result(
                state="UNKNOWN",
                quality="UNKNOWN",
                reason_code="OLS20_SST_ZERO",
                reason=_REASONS["OLS20_SST_ZERO"],
                slope=slope,
                r2=None,
                slope_quality="OBSERVED",
                r2_quality="UNKNOWN",
                official_sessions=prior_20,
                missing_sessions=(),
            )
        r2 = (sxy * sxy) / (sxx * syy)
    return D0Ols20Result(
        state="OBSERVED",
        quality="OBSERVED",
        reason_code="OLS20_OBSERVED",
        reason=_REASONS["OLS20_OBSERVED"],
        slope=slope,
        r2=r2,
        slope_quality="OBSERVED",
        r2_quality="OBSERVED",
        official_sessions=prior_20,
        missing_sessions=(),
    )


@dataclass(frozen=True)
class D0Prior5BelowResult:
    """ALG-27：目标日前5个历史端点各自C<=自身MA20的统计。"""

    state: str
    quality: str
    reason_code: str
    reason: str
    below_count: int | None
    official_sessions: tuple[date, ...]
    missing_sessions: tuple[date, ...]
    price_coordinate: str = "FORWARD_ADJUSTED"
    time_role: str = "NON_PIT_RETROSPECTIVE"


def calculate_d0_prior5_below(
    *,
    target_session: date,
    official_sessions: Sequence[date],
    history_rows_by_session: Mapping[date, Mapping[str, Any] | None],
    source_symbol: str,
    source_capture_id: int | None = None,
    price_coordinate: str = "FORWARD_ADJUSTED",
) -> D0Prior5BelowResult:
    """统计目标日前5个端点(t-5..t-1)各自收盘价<=其自身20会话MA20的天数(>=2为TRUE)。"""
    if price_coordinate != "FORWARD_ADJUSTED":
        return D0Prior5BelowResult(
            state="UNKNOWN",
            quality="UNKNOWN",
            reason_code="PRICE_COORDINATE_MISMATCH",
            reason=_REASONS["PRICE_COORDINATE_MISMATCH"],
            below_count=None,
            official_sessions=tuple(official_sessions),
            missing_sessions=(),
            price_coordinate=price_coordinate,
        )
    window, win_err = _validate_window_sessions(target_session, official_sessions, 25)
    if win_err or window is None:
        return D0Prior5BelowResult(
            state="UNKNOWN",
            quality="UNKNOWN",
            reason_code=win_err or "OFFICIAL_WINDOW_INVALID",
            reason=_REASONS.get(win_err or "OFFICIAL_WINDOW_INVALID", "窗口无效。"),
            below_count=None,
            official_sessions=window or tuple(official_sessions),
            missing_sessions=(),
        )
    prior_24 = window[:24]
    target_row = history_rows_by_session.get(target_session)
    target_qfq_cap = _resolve_target_qfq_capture_id(target_row, source_capture_id)
    closes: list[Fraction] = []
    missing: list[date] = []
    first_err: str | None = None
    for session in prior_24:
        val, err = _extract_strict_qfq_close(
            session=session,
            row=history_rows_by_session.get(session),
            source_symbol=source_symbol,
            target_qfq_capture_id=target_qfq_cap,
            target_row=target_row,
        )
        if err is not None or val is None:
            missing.append(session)
            if first_err is None:
                first_err = err or "OFFICIAL_SESSION_BAR_MISSING"
        else:
            closes.append(val)
    if missing or len(closes) != 24:
        code = first_err or "OFFICIAL_SESSION_BAR_MISSING"
        return D0Prior5BelowResult(
            state="UNKNOWN",
            quality="UNKNOWN",
            reason_code=code,
            reason=_REASONS.get(code, "严格官方窗口缺少有效QFQ收盘价。"),
            below_count=None,
            official_sessions=prior_24,
            missing_sessions=tuple(missing),
        )
    below_count = 0
    # closes索引0..23对应t-24..t-1；前5个历史端点为索引19..23(t-5..t-1)
    for idx in range(19, 24):
        ma20_k = sum(closes[idx - 19 : idx + 1], Fraction(0)) / 20
        if closes[idx] <= ma20_k:
            below_count += 1
    passed = below_count >= 2
    code = "PRIOR5_BELOW_TRUE" if passed else "PRIOR5_BELOW_FALSE"
    return D0Prior5BelowResult(
        state="TRUE" if passed else "FALSE",
        quality="OBSERVED",
        reason_code=code,
        reason=_REASONS[code],
        below_count=below_count,
        official_sessions=prior_24,
        missing_sessions=(),
    )


@dataclass(frozen=True)
class D0Liq20Result:
    """ALG-27：前20个严格官方会话RAW CNY成交额中位数>=20,000,000。"""

    state: str
    quality: str
    reason_code: str
    reason: str
    median_amount_cny: Fraction | None
    threshold_cny: Fraction
    official_sessions: tuple[date, ...]
    missing_sessions: tuple[date, ...]
    price_coordinate: str = "RAW_UNADJUSTED"
    time_role: str = "NON_PIT_RETROSPECTIVE"


def calculate_d0_liq20(
    *,
    target_session: date,
    official_sessions: Sequence[date],
    history_rows_by_session: Mapping[date, Mapping[str, Any] | None],
    source_symbol: str,
    threshold_cny: Fraction = _D0_LIQ20_THRESHOLD_CNY,
) -> D0Liq20Result:
    """独立按RAW未复权CNY金额计算严格前20个官方会话成交额中位数>=20,000,000。"""
    window, win_err = _validate_window_sessions(target_session, official_sessions, 21)
    if win_err or window is None:
        return D0Liq20Result(
            state="UNKNOWN",
            quality="UNKNOWN",
            reason_code=win_err or "OFFICIAL_WINDOW_INVALID",
            reason=_REASONS.get(win_err or "OFFICIAL_WINDOW_INVALID", "窗口无效。"),
            median_amount_cny=None,
            threshold_cny=threshold_cny,
            official_sessions=window or tuple(official_sessions),
            missing_sessions=(),
        )
    prior_20 = window[:20]
    amounts: list[Fraction] = []
    missing: list[date] = []
    first_err: str | None = None
    for session in prior_20:
        val, err = _extract_strict_raw_amount(
            session=session,
            row=history_rows_by_session.get(session),
            source_symbol=source_symbol,
        )
        if err is not None or val is None:
            missing.append(session)
            if first_err is None:
                first_err = err or "RAW_AMOUNT_QUALITY_UNKNOWN"
        else:
            amounts.append(val)
    if missing or len(amounts) != 20:
        code = first_err or "RAW_AMOUNT_QUALITY_UNKNOWN"
        return D0Liq20Result(
            state="UNKNOWN",
            quality="UNKNOWN",
            reason_code=code,
            reason=_REASONS.get(code, "严格前20会话RAW成交额不足或单位未验证。"),
            median_amount_cny=None,
            threshold_cny=threshold_cny,
            official_sessions=prior_20,
            missing_sessions=tuple(missing),
        )
    ordered = sorted(amounts)
    median_cny = (ordered[9] + ordered[10]) / 2
    passed = median_cny >= threshold_cny
    code = "LIQ20_THRESHOLD_TRUE" if passed else "LIQ20_THRESHOLD_FALSE"
    return D0Liq20Result(
        state="TRUE" if passed else "FALSE",
        quality="OBSERVED",
        reason_code=code,
        reason=_REASONS[code],
        median_amount_cny=median_cny,
        threshold_cny=threshold_cny,
        official_sessions=prior_20,
        missing_sessions=(),
    )


@dataclass(frozen=True)
class D0SevereDropResult:
    """ALG-27：严重下跌三态判定。"""

    state: str
    quality: str
    reason_code: str
    reason: str
    ret1: Fraction | None
    logret1: Decimal | None
    prior_sigma20: Decimal | None
    threshold_logret: Decimal | None
    official_sessions: tuple[date, ...]
    missing_sessions: tuple[date, ...]
    price_coordinate: str = "FORWARD_ADJUSTED"
    time_role: str = "NON_PIT_RETROSPECTIVE"


def calculate_d0_severe_drop(
    *,
    target_session: date,
    official_sessions: Sequence[date],
    history_rows_by_session: Mapping[date, Mapping[str, Any] | None],
    source_symbol: str,
    source_capture_id: int | None = None,
    price_coordinate: str = "FORWARD_ADJUSTED",
) -> D0SevereDropResult:
    """计算ret1<=-0.08 OR logret1<=-max(-log(0.96), 2*prior_sigma20)；ret1>0直接排除，否则真实计算prior_sigma20。"""
    if price_coordinate != "FORWARD_ADJUSTED":
        return D0SevereDropResult(
            state="UNKNOWN",
            quality="UNKNOWN",
            reason_code="PRICE_COORDINATE_MISMATCH",
            reason=_REASONS["PRICE_COORDINATE_MISMATCH"],
            ret1=None,
            logret1=None,
            prior_sigma20=None,
            threshold_logret=None,
            official_sessions=tuple(official_sessions),
            missing_sessions=(),
            price_coordinate=price_coordinate,
        )
    sessions = tuple(official_sessions)
    if (
        type(target_session) is not date
        or not sessions
        or any(type(s) is not date for s in sessions)
        or tuple(sorted(set(sessions))) != sessions
        or sessions[-1] != target_session
    ):
        return D0SevereDropResult(
            state="UNKNOWN",
            quality="UNKNOWN",
            reason_code="OFFICIAL_WINDOW_INVALID",
            reason=_REASONS["OFFICIAL_WINDOW_INVALID"],
            ret1=None,
            logret1=None,
            prior_sigma20=None,
            threshold_logret=None,
            official_sessions=sessions,
            missing_sessions=(),
        )
    if len(sessions) < 2:
        return D0SevereDropResult(
            state="UNKNOWN",
            quality="UNKNOWN",
            reason_code="OFFICIAL_WINDOW_LEFT_TRUNCATED",
            reason=_REASONS["OFFICIAL_WINDOW_LEFT_TRUNCATED"],
            ret1=None,
            logret1=None,
            prior_sigma20=None,
            threshold_logret=None,
            official_sessions=sessions,
            missing_sessions=(),
        )
    target_row = history_rows_by_session.get(target_session)
    target_qfq_cap = _resolve_target_qfq_capture_id(target_row, source_capture_id)
    prev_session = sessions[-2]
    c_prev, err_prev = _extract_strict_qfq_close(
        session=prev_session,
        row=history_rows_by_session.get(prev_session),
        source_symbol=source_symbol,
        target_qfq_capture_id=target_qfq_cap,
        target_row=target_row,
    )
    if err_prev is not None or c_prev is None:
        code = err_prev or "OFFICIAL_SESSION_BAR_MISSING"
        return D0SevereDropResult(
            state="UNKNOWN",
            quality="UNKNOWN",
            reason_code=code,
            reason=_REASONS.get(code, "前一官方会话QFQ收盘价不可用。"),
            ret1=None,
            logret1=None,
            prior_sigma20=None,
            threshold_logret=None,
            official_sessions=sessions[-2:],
            missing_sessions=(prev_session,),
        )
    c_curr, err_curr = _extract_strict_qfq_close(
        session=target_session,
        row=target_row,
        source_symbol=source_symbol,
        target_qfq_capture_id=target_qfq_cap,
        target_row=target_row,
    )
    if err_curr is not None or c_curr is None:
        code = err_curr or "OFFICIAL_SESSION_BAR_MISSING"
        return D0SevereDropResult(
            state="UNKNOWN",
            quality="UNKNOWN",
            reason_code=code,
            reason=_REASONS.get(code, "目标官方会话QFQ收盘价不可用。"),
            ret1=None,
            logret1=None,
            prior_sigma20=None,
            threshold_logret=None,
            official_sessions=sessions[-2:],
            missing_sessions=(target_session,),
        )
    ret1 = c_curr / c_prev - 1
    with localcontext() as ctx:
        ctx.prec = 34
        logret1 = (
            (Decimal(c_curr.numerator) / Decimal(c_curr.denominator))
            / (Decimal(c_prev.numerator) / Decimal(c_prev.denominator))
        ).ln()
    if ret1 <= Fraction(-2, 25):
        return D0SevereDropResult(
            state="TRUE",
            quality="OBSERVED",
            reason_code="SEVERE_DROP_RET1_TRUE",
            reason=_REASONS["SEVERE_DROP_RET1_TRUE"],
            ret1=ret1,
            logret1=logret1,
            prior_sigma20=None,
            threshold_logret=None,
            official_sessions=sessions[-2:],
            missing_sessions=(),
        )
    if ret1 > 0:
        return D0SevereDropResult(
            state="FALSE",
            quality="OBSERVED",
            reason_code="SEVERE_DROP_EXCLUDED_BY_POSITIVE_RET1",
            reason=_REASONS["SEVERE_DROP_EXCLUDED_BY_POSITIVE_RET1"],
            ret1=ret1,
            logret1=logret1,
            prior_sigma20=None,
            threshold_logret=None,
            official_sessions=sessions[-2:],
            missing_sessions=(),
        )
    if len(sessions) < 22:
        return D0SevereDropResult(
            state="UNKNOWN",
            quality="UNKNOWN",
            reason_code="OFFICIAL_WINDOW_LEFT_TRUNCATED",
            reason=_REASONS["OFFICIAL_WINDOW_LEFT_TRUNCATED"],
            ret1=ret1,
            logret1=logret1,
            prior_sigma20=None,
            threshold_logret=None,
            official_sessions=sessions,
            missing_sessions=(),
        )
    window_22 = sessions[-22:]
    prior_21 = window_22[:21]
    prior_closes: list[Fraction] = []
    missing: list[date] = []
    first_err: str | None = None
    for session in prior_21:
        val, err = _extract_strict_qfq_close(
            session=session,
            row=history_rows_by_session.get(session),
            source_symbol=source_symbol,
            target_qfq_capture_id=target_qfq_cap,
            target_row=target_row,
        )
        if err is not None or val is None:
            missing.append(session)
            if first_err is None:
                first_err = err or "OFFICIAL_SESSION_BAR_MISSING"
        else:
            prior_closes.append(val)
    if missing or len(prior_closes) != 21:
        code = first_err or "OFFICIAL_SESSION_BAR_MISSING"
        return D0SevereDropResult(
            state="UNKNOWN",
            quality="UNKNOWN",
            reason_code=code,
            reason=_REASONS.get(code, "计算prior_sigma20缺少连续官方会话收盘价。"),
            ret1=ret1,
            logret1=logret1,
            prior_sigma20=None,
            threshold_logret=None,
            official_sessions=window_22,
            missing_sessions=tuple(missing),
        )
    with localcontext() as ctx:
        ctx.prec = 34
        prior_logrets = [
            (
                (Decimal(curr.numerator) / Decimal(curr.denominator))
                / (Decimal(prev.numerator) / Decimal(prev.denominator))
            ).ln()
            for prev, curr in zip(prior_closes, prior_closes[1:])
        ]
        mean_lr = sum(prior_logrets, Decimal(0)) / Decimal(20)
        variance = sum(
            ((lr - mean_lr) ** 2 for lr in prior_logrets), Decimal(0)
        ) / Decimal(19)
        sigma20 = variance.sqrt()
        floor_bound = -Decimal("0.96").ln()
        threshold = -max(floor_bound, Decimal(2) * sigma20)
        triggered = logret1 <= threshold
    code = "SEVERE_DROP_LOGRET_SIGMA_TRUE" if triggered else "SEVERE_DROP_FALSE"
    return D0SevereDropResult(
        state="TRUE" if triggered else "FALSE",
        quality="OBSERVED",
        reason_code=code,
        reason=_REASONS[code],
        ret1=ret1,
        logret1=logret1,
        prior_sigma20=sigma20,
        threshold_logret=threshold,
        official_sessions=window_22,
        missing_sessions=(),
    )


__all__ = [
    "D0FirstDayDamage",
    "D0Liq20Result",
    "D0Ols20Result",
    "D0Prior5BelowResult",
    "D0SevereDropResult",
    "calculate_d0_first_day_damage",
    "calculate_d0_liq20",
    "calculate_d0_ols20",
    "calculate_d0_prior5_below",
    "calculate_d0_severe_drop",
]
