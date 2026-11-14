"""S2固定T0分组与已保存T+1端点的精确描述统计。"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import date
from fractions import Fraction
from typing import Any

COHORT_VERSION = "SOURCE_S2_T1_COHORT_R1"
S2_STATES = ("TRUE", "FALSE", "UNKNOWN")
T1_STATES = ("ALL", "TRUE", "FALSE", "UNKNOWN")

COHORT_COLUMNS = (
    "target_session",
    "endpoint_session",
    "research_cutoff",
    "source_package_id",
    "source_capture_id",
    "endpoint_source_capture_id",
    "t0_profile_revision",
    "endpoint_profile_revision",
    "producer_contract",
    "producer_version",
    "t0_history_session_count",
    "endpoint_history_session_count",
    "calendar_id",
    "calendar_revision",
    "path_rule_version",
    "cohort_version",
    "research_mode",
    "price_coordinate",
    "t0_s2_state",
    "t1_s2_state",
    "source_pool_count",
    "member_count",
    "endpoint_observed_count",
    "endpoint_unknown_count",
    "endpoint_other_count",
    "mean_return_numerator",
    "mean_return_denominator",
    "median_return_numerator",
    "median_return_denominator",
    "min_return_numerator",
    "min_return_denominator",
    "max_return_numerator",
    "max_return_denominator",
    "positive_count",
    "non_positive_count",
    "failure_reason_subject_count",
    "failure_reason_codes",
    "failure_reason_counts",
    "endpoint_unknown_reason_codes",
    "endpoint_unknown_reason_counts",
)

COHORT_KEY = (
    "target_session",
    "endpoint_session",
    "research_cutoff",
    "source_package_id",
    "source_capture_id",
    "endpoint_source_capture_id",
    "t0_profile_revision",
    "endpoint_profile_revision",
    "producer_contract",
    "producer_version",
    "t0_history_session_count",
    "endpoint_history_session_count",
    "calendar_id",
    "calendar_revision",
    "path_rule_version",
    "cohort_version",
    "t0_s2_state",
    "t1_s2_state",
)


class SourceS2T1CohortError(ValueError):
    """S2/T+1统计输入无法证明唯一、完整或同源。"""

    preserve_transaction_error = True


def _pair(value: Fraction | None) -> tuple[int | None, int | None]:
    return (None, None) if value is None else (value.numerator, value.denominator)


def _median(values: Sequence[Fraction]) -> Fraction | None:
    if not values:
        return None
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2


def prepare_source_s2_t1_cohort_rows(
    *,
    target_session: date,
    endpoint_session: date,
    research_cutoff: date,
    source_package_id: int,
    source_capture_id: int,
    endpoint_source_capture_id: int,
    t0_scope: Mapping[str, Any],
    endpoint_scope: Mapping[str, Any],
    calendar_revision: str,
    path_rule_version: str,
    outcomes: Sequence[Mapping[str, Any]],
    expected_source_count: int,
) -> tuple[tuple[Any, ...], ...]:
    """验证完整输入后生成三态固定组及T+1事后分层的12个统计行。"""
    if (
        type(target_session) is not date
        or type(endpoint_session) is not date
        or type(research_cutoff) is not date
        or not target_session < endpoint_session <= research_cutoff
        or type(source_package_id) is not int
        or source_package_id <= 0
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or type(endpoint_source_capture_id) is not int
        or endpoint_source_capture_id <= 0
        or type(expected_source_count) is not int
        or expected_source_count <= 0
        or not calendar_revision
        or not path_rule_version
        or len(outcomes) != expected_source_count
    ):
        raise SourceS2T1CohortError("S2/T+1范围或完整来源池分母无效")

    required_scope_keys = (
        "profile_revision",
        "producer_contract",
        "producer_version",
        "history_session_count",
        "calendar_id",
        "calendar_revision",
        "research_mode",
        "price_coordinate",
        "source_count",
        "distinct_source_count",
        "source_time_quality",
        "source_clock_role",
        "source_available_at",
    )
    if any(
        key not in scope
        for scope in (t0_scope, endpoint_scope)
        for key in required_scope_keys
    ):
        raise SourceS2T1CohortError("T0或T+1 Seed输入上下文不完整")
    shared_keys = (
        "producer_contract",
        "producer_version",
        "calendar_id",
        "calendar_revision",
        "research_mode",
        "price_coordinate",
        "source_time_quality",
        "source_clock_role",
        "source_available_at",
    )
    if (
        any(t0_scope[key] != endpoint_scope[key] for key in shared_keys)
        or t0_scope["research_mode"] != "NON_PIT_RETROSPECTIVE"
        or t0_scope["price_coordinate"] != "RAW_UNADJUSTED"
        or t0_scope["source_time_quality"] != "NON_PIT_OBSERVED"
        or t0_scope["source_clock_role"] != "LOCAL_VERIFIED_VIPDATA_TARGET_SCAN_NON_PIT"
        or t0_scope["source_available_at"] is not None
        or t0_scope["calendar_revision"] != calendar_revision
        or t0_scope["source_count"] != expected_source_count
        or endpoint_scope["source_count"] != expected_source_count
        or t0_scope["distinct_source_count"] != expected_source_count
        or endpoint_scope["distinct_source_count"] != expected_source_count
    ):
        raise SourceS2T1CohortError("T0与T+1 Seed合同、时钟、日历或完整分母不一致")

    by_symbol: dict[str, Mapping[str, Any]] = {}
    for item in outcomes:
        symbol = item.get("source_symbol")
        t0_state = item.get("t0_s2_state")
        t1_state = item.get("t1_s2_state")
        if (
            not isinstance(symbol, str)
            or not symbol
            or symbol in by_symbol
            or t0_state not in S2_STATES
            or t1_state not in S2_STATES
        ):
            raise SourceS2T1CohortError("T0/T+1 S2来源代码或三态输入重复、缺失或无效")
        for key in ("t1_s2_reasons",):
            reasons = item.get(key)
            if not isinstance(reasons, (list, tuple)) or any(
                not isinstance(reason, str) or not reason for reason in reasons
            ):
                raise SourceS2T1CohortError("T+1 S2原因必须是非空原因码数组")
        quality = item.get("endpoint_quality")
        numerator = item.get("endpoint_numerator")
        denominator = item.get("endpoint_denominator")
        if quality == "OBSERVED":
            if (
                type(numerator) is not int
                or type(denominator) is not int
                or denominator <= 0
            ):
                raise SourceS2T1CohortError("可评价T+1收益必须有正分母的精确有理数")
        elif numerator is not None or denominator is not None:
            raise SourceS2T1CohortError("不可评价T+1端点不能携带数值收益")
        if not isinstance(quality, str) or not quality:
            raise SourceS2T1CohortError("T+1端点质量码无效")
        if item.get("price_coordinate") != "RAW_UNADJUSTED":
            raise SourceS2T1CohortError("T+1端点价格坐标不是RAW_UNADJUSTED")
        by_symbol[symbol] = item
    if len(by_symbol) != expected_source_count:
        raise SourceS2T1CohortError("T0与T+1输入代码集合未闭合")

    context = (
        target_session,
        endpoint_session,
        research_cutoff,
        source_package_id,
        source_capture_id,
        endpoint_source_capture_id,
        t0_scope["profile_revision"],
        endpoint_scope["profile_revision"],
        t0_scope["producer_contract"],
        t0_scope["producer_version"],
        t0_scope["history_session_count"],
        endpoint_scope["history_session_count"],
        t0_scope["calendar_id"],
        calendar_revision,
        path_rule_version,
        COHORT_VERSION,
        "NON_PIT_EXPLORATORY",
        "RAW_UNADJUSTED",
    )

    output: list[tuple[Any, ...]] = []
    for t0_state in S2_STATES:
        for t1_state in T1_STATES:
            members = [
                item
                for item in by_symbol.values()
                if item["t0_s2_state"] == t0_state
                and (t1_state == "ALL" or item["t1_s2_state"] == t1_state)
            ]
            observed = [
                Fraction(item["endpoint_numerator"], item["endpoint_denominator"])
                for item in members
                if item["endpoint_quality"] == "OBSERVED"
            ]
            unknown = [
                item for item in members if item["endpoint_quality"] == "UNKNOWN"
            ]
            other = [
                item
                for item in members
                if item["endpoint_quality"] not in {"OBSERVED", "UNKNOWN"}
            ]
            if len(observed) + len(unknown) + len(other) != len(members):
                raise SourceS2T1CohortError("T+1端点质量分母未闭合")

            failure_subjects: set[str] = set()
            failure_reasons: Counter[str] = Counter()
            if t1_state != "TRUE":
                for item in members:
                    if item["t1_s2_state"] == "TRUE":
                        continue
                    reasons = set(item["t1_s2_reasons"])
                    if reasons:
                        failure_subjects.add(item["source_symbol"])
                        failure_reasons.update(reasons)
            endpoint_reasons: Counter[str] = Counter(
                item.get("endpoint_reason") or "UNKNOWN_REASON_UNSPECIFIED"
                for item in unknown
            )

            mean = sum(observed, Fraction()) / len(observed) if observed else None
            median = _median(observed)
            minimum = min(observed) if observed else None
            maximum = max(observed) if observed else None
            pairs = tuple(
                value
                for fraction in (mean, median, minimum, maximum)
                for value in _pair(fraction)
            )
            output.append(
                context
                + (
                    t0_state,
                    t1_state,
                    expected_source_count,
                    len(members),
                    len(observed),
                    len(unknown),
                    len(other),
                    *pairs,
                    sum(value > 0 for value in observed),
                    sum(value <= 0 for value in observed),
                    len(failure_subjects),
                    sorted(failure_reasons),
                    [failure_reasons[key] for key in sorted(failure_reasons)],
                    sorted(endpoint_reasons),
                    [endpoint_reasons[key] for key in sorted(endpoint_reasons)],
                )
            )
    if len(output) != len(S2_STATES) * len(T1_STATES):
        raise SourceS2T1CohortError("固定T0/T+1三态统计矩阵未完整生成")
    return tuple(output)
