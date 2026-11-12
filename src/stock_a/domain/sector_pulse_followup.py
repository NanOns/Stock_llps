"""板块PULSE冻结成员篮子的来源级后继收盘结果。"""

from __future__ import annotations

from datetime import date
from fractions import Fraction
from typing import Any, Mapping, Sequence

from stock_a.domain import exact_number


def _usable_close(
    row: Mapping[str, Any] | None,
    *,
    expected_session: date,
    source_package_id: int,
    label: str,
) -> tuple[Fraction | None, str | None]:
    if row is None:
        return None, f"{label}_RAW_ROW_MISSING"
    if row.get("session_date") != expected_session:
        return None, f"{label}_SESSION_MISMATCH"
    if row.get("bar_status") != "TARGET_PRESENT":
        return None, f"{label}_BAR_NOT_PRESENT"
    if row.get("price_quality") != "OBSERVED":
        return None, f"{label}_PRICE_NOT_OBSERVED"
    if row.get("price_coordinate") != "RAW_UNADJUSTED":
        return None, f"{label}_PRICE_COORDINATE_UNKNOWN"
    capture_id = row.get("source_capture_id")
    if (
        row.get("source_package_id") != source_package_id
        or type(capture_id) is not int
        or capture_id <= 0
    ):
        return None, f"{label}_SOURCE_IDENTITY_MISMATCH"
    values = [row.get(f"raw_{field}") for field in ("open", "high", "low", "close")]
    if any(value is None for value in values):
        return None, f"{label}_OHLC_MISSING"
    try:
        opening, high, low, close = (exact_number(value) for value in values)
    except (TypeError, ValueError, ZeroDivisionError):
        return None, f"{label}_OHLC_INVALID"
    if (
        min(opening, high, low, close) <= 0
        or low > min(opening, close)
        or high < max(opening, close)
    ):
        return None, f"{label}_OHLC_INVALID"
    return close, None


def source_sector_pulse_followup_outcome(
    *,
    member_symbols: Sequence[str],
    rows_by_symbol: Mapping[str, Sequence[Mapping[str, Any]]],
    t0_session: date,
    target_session: date,
    source_package_id: int,
) -> dict[str, Any]:
    """按现役post_R规则计算T0收盘至一个官方后继会话的固定等权篮子。"""
    if (
        type(t0_session) is not date
        or type(target_session) is not date
        or target_session <= t0_session
        or type(source_package_id) is not int
        or source_package_id <= 0
        or len(set(member_symbols)) != len(member_symbols)
        or any(not isinstance(symbol, str) or not symbol for symbol in member_symbols)
    ):
        raise ValueError("板块PULSE后继计算输入身份或成员篮子无效")

    denominator = len(member_symbols)
    observed_returns: list[Fraction] = []
    reasons: set[str] = set()
    for symbol in member_symbols:
        rows = rows_by_symbol.get(symbol, ())
        by_session = {row.get("session_date"): row for row in rows}
        if len(by_session) != len(rows):
            reasons.add("DUPLICATE_RAW_MEMBER_SESSION")
            continue
        t0_close, t0_reason = _usable_close(
            by_session.get(t0_session),
            expected_session=t0_session,
            source_package_id=source_package_id,
            label="T0",
        )
        target_close, target_reason = _usable_close(
            by_session.get(target_session),
            expected_session=target_session,
            source_package_id=source_package_id,
            label="TARGET",
        )
        if t0_reason:
            reasons.add(t0_reason)
        if target_reason:
            reasons.add(target_reason)
        if t0_close is None or target_close is None:
            continue
        observed_returns.append(target_close / t0_close - 1)

    observed_count = len(observed_returns)
    unknown_count = denominator - observed_count
    if denominator == 0:
        reasons.add("EMPTY_FIXED_MEMBER_BASKET")
    mean_return = (
        sum(observed_returns, Fraction()) / denominator
        if denominator > 0 and observed_count == denominator
        else None
    )
    return {
        "outcome_status": "OBSERVED" if mean_return is not None else "UNKNOWN",
        "fixed_member_denominator": denominator,
        "observed_member_count": observed_count,
        "unknown_member_count": unknown_count,
        "equal_weight_mean_return": mean_return,
        "reason_codes": sorted(reasons),
        "research_mode": "NON_PIT_RETROSPECTIVE",
        "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
        "price_coordinate": "RAW_UNADJUSTED",
    }


__all__ = ["source_sector_pulse_followup_outcome"]
