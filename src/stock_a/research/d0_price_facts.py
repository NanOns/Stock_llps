"""D0首日价格损伤原语：严格使用连续20个官方市场会话。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from fractions import Fraction
from typing import Mapping, Sequence

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
    "OFFICIAL_WINDOW_LEFT_TRUNCATED": "目标日前不足20个连续官方会话。",
    "SOURCE_CAPTURE_IDENTITY_MISMATCH": "历史行与目标来源代码、日期或paired capture不一致。",
    "PRICE_COORDINATE_MISMATCH": "官方会话窗口内的价格坐标不是QFQ。",
    "OFFICIAL_SESSION_BAR_MISSING": "严格官方窗口至少一个会话缺少有效QFQ日线。",
    "INVALID_PRICE_CLOSE": "官方会话QFQ收盘价缺失、非有限或非正数。",
    "QFQ_PRICE_QUALITY_UNKNOWN": "官方会话QFQ价格质量未知。",
    "INVALID_PRICE_CLOSE": "官方会话收盘价缺失、非有限或非正数。",
    "INVALID_PRICE_OHLC": "官方会话QFQ OHLC缺失、非法或不满足价格范围关系。",
}


__all__ = ["D0FirstDayDamage", "calculate_d0_first_day_damage"]
