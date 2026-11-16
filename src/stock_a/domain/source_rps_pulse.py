"""RPS板内并列名次变化的三态分类。"""

from __future__ import annotations

from typing import Any


_CHANGE_STATES = {
    "IMPROVED",
    "WORSENED",
    "UNCHANGED",
    "UNKNOWN",
    "NOT_COMPARABLE",
    "NOT_COMPUTED",
}


def classify_rank_change(
    *,
    source_pool_status: str,
    member_sets_equal: bool,
    order_exact: bool,
    previous_quality: str | None,
    previous_rank_first: int | None,
    previous_rank_last: int | None,
    current_quality: str | None,
    current_rank_first: int | None,
    current_rank_last: int | None,
    current_rank_matches: bool,
) -> tuple[str, list[str]]:
    """按与单股RPS历史Reader相同的并列区间规则生成状态和原因码。"""
    if source_pool_status != "AVAILABLE":
        reason = {
            "UNKNOWN": "SOURCE_CONTEXT_UNKNOWN",
            "NOT_COMPUTED": "SOURCE_CONTEXT_NOT_COMPUTED",
        }.get(source_pool_status, "SOURCE_CONTEXT_NOT_COMPARABLE")
        return source_pool_status, [reason]
    if not member_sets_equal:
        return "NOT_COMPARABLE", ["VALID_MEMBER_SET_CHANGED"]
    if not order_exact:
        return "UNKNOWN", ["RPS_ORDER_NOT_PROVEN_EXACT"]
    qualities = (previous_quality, current_quality)
    if "NOT_COMPUTED" in qualities:
        return "NOT_COMPUTED", ["ADJACENT_RPS_VALUE_NOT_COMPUTED"]
    if any(quality != "OBSERVED" for quality in qualities):
        return "UNKNOWN", ["ADJACENT_RPS_VALUE_UNKNOWN"]
    if not current_rank_matches:
        return "UNKNOWN", ["T0_RANK_FACT_MISMATCH"]
    ranks = (
        previous_rank_first,
        previous_rank_last,
        current_rank_first,
        current_rank_last,
    )
    if any(type(value) is not int or value <= 0 for value in ranks):
        return "UNKNOWN", ["RANK_INTERVAL_MISSING"]
    before_first, before_last, after_first, after_last = ranks
    if before_last < before_first or after_last < after_first:
        return "UNKNOWN", ["RANK_INTERVAL_INVALID"]
    if after_last < before_first:
        return "IMPROVED", []
    if after_first > before_last:
        return "WORSENED", []
    if before_first == after_first and before_last == after_last:
        return "UNCHANGED", []
    return "UNKNOWN", ["TIED_RANK_INTERVALS_OVERLAP"]


def validate_change_state(value: Any) -> str:
    """供存储边界复用的有限状态校验。"""
    if value not in _CHANGE_STATES:
        raise ValueError("RPS变化状态不在现行有限枚举内")
    return value
