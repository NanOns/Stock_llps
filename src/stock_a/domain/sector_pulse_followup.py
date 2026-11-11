"""板块PULSE冻结成员篮子的来源级后继收盘结果。"""

from __future__ import annotations

from datetime import date
from fractions import Fraction
import re
from typing import Any, Mapping, Sequence

from stock_a.domain import exact_number

_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def _usable_close(
    row: Mapping[str, Any] | None,
    *,
    expected_session: date,
    namespace_id: str,
    package_sha256: str,
    source_content_id: str,
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
    if (
        row.get("namespace_id") != namespace_id
        or row.get("package_sha256") != package_sha256
        or row.get("source_content_id") != source_content_id
    ):
        return None, f"{label}_SOURCE_IDENTITY_MISMATCH"
    entry_sha = row.get("source_entry_sha256")
    if not isinstance(entry_sha, str) or not _SHA256.fullmatch(entry_sha):
        return None, f"{label}_SOURCE_ENTRY_IDENTITY_MISSING"
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
    namespace_id: str,
    package_sha256: str,
    source_content_id: str,
) -> dict[str, Any]:
    """按现役post_R规则计算T0收盘至一个官方后继会话的固定等权篮子。"""
    if (
        type(t0_session) is not date
        or type(target_session) is not date
        or target_session <= t0_session
        or not isinstance(namespace_id, str)
        or not namespace_id
        or not _SHA256.fullmatch(package_sha256 or "")
        or not _SHA256.fullmatch(source_content_id or "")
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
        t0_row = by_session.get(t0_session)
        target_row = by_session.get(target_session)
        t0_close, t0_reason = _usable_close(
            t0_row,
            expected_session=t0_session,
            namespace_id=namespace_id,
            package_sha256=package_sha256,
            source_content_id=source_content_id,
            label="T0",
        )
        target_close, target_reason = _usable_close(
            target_row,
            expected_session=target_session,
            namespace_id=namespace_id,
            package_sha256=package_sha256,
            source_content_id=source_content_id,
            label="TARGET",
        )
        if t0_reason:
            reasons.add(t0_reason)
        if target_reason:
            reasons.add(target_reason)
        if t0_close is None or target_close is None:
            continue
        if t0_row.get("source_entry_sha256") != target_row.get("source_entry_sha256"):
            reasons.add("SOURCE_ENTRY_COORDINATE_MISMATCH")
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
