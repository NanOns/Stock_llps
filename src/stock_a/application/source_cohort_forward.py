"""来源Cohort已存QFQ后继展示适配器。"""

from fractions import Fraction
from typing import Any, Mapping


def _frozen_line_followup(
    item: Mapping[str, Any], facts: Mapping[str, Any]
) -> dict[str, Any]:
    """以paired BaoStock QFQ收盘观察已冻结T0线，不创建正式失效事实。"""
    candidate = item.get("failure_line_candidate", {})
    value = candidate.get("value")
    state = item.get("state") or item.get("scenario_raw") or item.get("rule_state")
    role = "SOURCE_PRICE_DIAGNOSTIC_NOT_FORMAL_INVALIDATION"
    if state == "FALSE":
        return {
            "status": "NOT_APPLICABLE",
            "reason_code": "RULE_STATE_FALSE_NO_T0_SIGNAL",
            "role": role,
        }
    if state == "UNKNOWN":
        return {
            "status": "UNKNOWN",
            "reason_code": "RULE_STATE_UNKNOWN_NO_PROVEN_T0_SIGNAL",
            "role": role,
        }
    try:
        line = (
            Fraction(int(value["numerator"]), int(value["denominator"]))
            if value
            else None
        )
    except (KeyError, TypeError, ValueError, ZeroDivisionError):
        line = None
    source = candidate.get("source")
    coordinate = candidate.get("price_coordinate") or item.get(
        "price_coordinate", "FORWARD_ADJUSTED"
    )
    scope = candidate.get("scope") or item.get("scope", "NON_PIT_EXPLORATORY")
    if line is None or line <= 0 or not isinstance(source, str) or not source:
        return {
            "status": "NOT_COMPUTED",
            "reason_code": "T0_CANDIDATE_LINE_NOT_SAVED_OR_INVALID",
            "role": role,
        }
    if coordinate != "FORWARD_ADJUSTED" or scope != "NON_PIT_EXPLORATORY":
        return {
            "status": "UNKNOWN",
            "reason_code": "T0_CANDIDATE_LINE_COORDINATE_OR_SCOPE_MISMATCH",
            "role": role,
        }

    first = upper = None
    unknown_before = False
    recovery = recovery_upper = None
    recovery_unknown = False
    touched = False
    observations = []
    for point in facts.get("points", ()):
        close = point.get("qfq_close") or point.get("ohlc", {}).get("close", {})
        session = point.get("session")
        if session is None and close.get("quality") == "NOT_AVAILABLE":
            observations.append(
                {
                    "offset": point.get("offset"),
                    "session": None,
                    "state": "NOT_DUE",
                    "close": {"value": None, "quality": "NOT_AVAILABLE"},
                    "reason": close.get("reason")
                    or "NO_FUTURE_SESSION_IN_SAVED_WINDOW",
                }
            )
            continue
        if close.get("quality") != "OBSERVED":
            state = "UNKNOWN"
            if upper is None:
                unknown_before = True
            if recovery_upper is None:
                recovery_unknown = True
            close_result = {
                "value": None,
                "quality": close.get("quality") or "UNKNOWN",
                "reason": close.get("reason") or "QFQ_CLOSE_UNKNOWN",
            }
        else:
            amount = close["value"]
            try:
                close_value = Fraction(
                    amount["exact"] if isinstance(amount, dict) else amount
                )
            except (TypeError, ValueError, ZeroDivisionError):
                close_value = None
            if close_value is None or close_value <= 0:
                state = "UNKNOWN"
                if upper is None:
                    unknown_before = True
                if recovery_upper is None:
                    recovery_unknown = True
                close_result = {
                    "value": None,
                    "quality": "BAD_VALUE",
                    "reason": "QFQ_CLOSE_VALUE_INVALID",
                }
            else:
                close_result = {
                    "value": str(close_value),
                    "numerator": str(close_value.numerator),
                    "denominator": str(close_value.denominator),
                    "quality": "OBSERVED",
                    "reason": None,
                }
                state = (
                    "TOUCHED"
                    if close_value <= line
                    else "RETURNED_ABOVE"
                    if touched
                    else "ABOVE"
                )
            if state == "TOUCHED" and upper is None:
                upper = session
                if not unknown_before:
                    first = upper
            if state == "TOUCHED":
                touched = True
            elif (
                state in {"ABOVE", "RETURNED_ABOVE"}
                and touched
                and recovery_upper is None
            ):
                recovery_upper = session
                if not recovery_unknown:
                    recovery = recovery_upper
        observations.append(
            {
                "offset": point.get("offset"),
                "session": session,
                "state": state,
                "close": close_result,
                "reason": close_result.get("reason"),
            }
        )
    not_due = facts.get("due") is False
    return {
        "status": "AVAILABLE",
        "role": role,
        "line": {
            "value": {
                "numerator": str(line.numerator),
                "denominator": str(line.denominator),
            },
            "source": source,
            "price_coordinate": coordinate,
            "scope": scope,
        },
        "observations": observations,
        "first_proved_touch": first,
        "first_proved_touch_upper_bound": upper,
        "first_touch_status": "OBSERVED"
        if first
        else "UNKNOWN"
        if unknown_before
        else "NOT_DUE"
        if not_due
        else "NOT_OBSERVED",
        "first_proved_return_above": recovery,
        "first_proved_return_above_upper_bound": recovery_upper,
        "first_return_above_status": "OBSERVED"
        if recovery
        else "UNKNOWN"
        if recovery_unknown
        else "NOT_DUE"
        if not_due
        else "NOT_OBSERVED",
        "return_above_scope": "SOURCE_PRICE_DIAGNOSTIC_NOT_FORMAL_REENTRY",
        "maturity": facts.get("maturity"),
        "due": facts.get("due"),
        "path_quality": facts.get("path_quality", "UNKNOWN"),
        "path_reasons": list(facts.get("path_reasons", [])),
    }


def read_source_cohort_forward(*args, **kwargs) -> dict[str, Any]:
    """旧离线整池研究入口已退役，避免回到package键或RAW技术路径。"""
    return {
        "status": "NOT_COMPUTED",
        "reason_code": "LEGACY_COHORT_FORWARD_RETIRED",
        "reason": "整池后继结果由PG来源捕获读服务提供；旧package级离线入口已退役。",
        "database_write_attempted": False,
        "source_io_attempted": False,
    }
