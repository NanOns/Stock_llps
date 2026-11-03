"""D0首日价格损伤原语：严格使用连续20个官方市场会话。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from fractions import Fraction
from typing import Mapping, Sequence


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


def _unknown(
    reason_code: str,
    *,
    sessions: tuple[date, ...],
    missing: tuple[date, ...] = (),
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
    )


def calculate_d0_first_day_damage(
    *,
    target_session: date,
    official_sessions: Sequence[date],
    history_rows_by_session: Mapping[date, Mapping[str, object]],
    source_symbol: str,
    package_sha256: str,
    namespace_id: str,
    source_content_id: str,
    target_capture_id: str,
) -> D0FirstDayDamage:
    """按目标日及其前19个紧邻官方会话计算C/MA20<97/100。"""
    sessions = tuple(official_sessions)
    if (
        type(target_session) is not date
        or not sessions
        or any(type(session) is not date for session in sessions)
        or tuple(sorted(set(sessions))) != sessions
        or sessions[-1] != target_session
    ):
        return _unknown("OFFICIAL_WINDOW_INVALID", sessions=sessions)

    window = sessions[-20:]
    if len(window) != 20:
        return _unknown("OFFICIAL_WINDOW_LEFT_TRUNCATED", sessions=window)

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
            or row.get("namespace_id") != namespace_id
        ):
            return _unknown("RAW_SOURCE_IDENTITY_MISMATCH", sessions=window)
        if row.get("package_sha256") != package_sha256:
            return _unknown("RAW_PACKAGE_REVISION_MISMATCH", sessions=window)
        if row.get("source_content_id") != source_content_id:
            return _unknown("RAW_SOURCE_CONTENT_MISMATCH", sessions=window)
        if (
            session == target_session
            and row.get("source_capture_id") != target_capture_id
        ):
            return _unknown("RAW_TARGET_CAPTURE_MISMATCH", sessions=window)
        if row.get("price_coordinate") != "RAW_UNADJUSTED":
            return _unknown("PRICE_COORDINATE_MISMATCH", sessions=window)
        if row.get("bar_status") != "TARGET_PRESENT":
            return _unknown(
                "OFFICIAL_SESSION_BAR_MISSING",
                sessions=window,
                missing=(session,),
            )
        if row.get("price_quality") != "OBSERVED":
            reason_code = row.get("price_reason") or "RAW_PRICE_QUALITY_UNKNOWN"
            return _unknown(str(reason_code), sessions=window)
        values = [
            row.get(name) for name in ("raw_open", "raw_high", "raw_low", "raw_close")
        ]
        if any(
            isinstance(value, bool)
            or not isinstance(value, (Decimal, int, Fraction))
            or isinstance(value, Decimal)
            and not value.is_finite()
            for value in values
        ):
            return _unknown("INVALID_PRICE_OHLC", sessions=window)
        opening, high, low, close_value = (Fraction(value) for value in values)
        if (
            min(opening, high, low, close_value) <= 0
            or low > high
            or not low <= opening <= high
            or not low <= close_value <= high
        ):
            return _unknown("INVALID_PRICE_OHLC", sessions=window)
        closes.append(close_value)

    if missing:
        return _unknown(
            "OFFICIAL_SESSION_BAR_MISSING",
            sessions=window,
            missing=tuple(missing),
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
    )


_REASONS = {
    "OFFICIAL_WINDOW_INVALID": "官方会话窗口缺少目标日或顺序不唯一。",
    "OFFICIAL_WINDOW_LEFT_TRUNCATED": "目标日前不足20个连续官方会话。",
    "RAW_SOURCE_IDENTITY_MISMATCH": "历史行与目标来源代码、命名空间或日期不一致。",
    "RAW_PACKAGE_REVISION_MISMATCH": "官方会话窗口内的RAW包修订不一致。",
    "RAW_SOURCE_CONTENT_MISMATCH": "官方会话窗口内的RAW来源内容摘要不一致。",
    "RAW_TARGET_CAPTURE_MISMATCH": "目标RAW行与SEALED来源捕获身份不一致。",
    "PRICE_COORDINATE_MISMATCH": "官方会话窗口内的价格坐标不是RAW_UNADJUSTED。",
    "OFFICIAL_SESSION_BAR_MISSING": "严格官方窗口至少一个会话缺少有效RAW日线。",
    "RAW_PRICE_QUALITY_UNKNOWN": "官方会话RAW价格质量未知。",
    "INVALID_PRICE_CLOSE": "官方会话收盘价缺失、非有限或非正数。",
    "INVALID_PRICE_OHLC": "官方会话原始OHLC缺失、非法或不满足价格范围关系。",
}


__all__ = ["D0FirstDayDamage", "calculate_d0_first_day_damage"]
