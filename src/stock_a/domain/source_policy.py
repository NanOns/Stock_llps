from __future__ import annotations

import re
from typing import Any, Mapping


_BAOSTOCK_POLICY_REQUIRED_KEYS = frozenset(
    {"contract", "request_policy", "required_for_core", "request_budget"}
)
_BAOSTOCK_POLICY_OPTIONAL_KEYS = frozenset({"policy_revision"})
_BAOSTOCK_POLICY_REVISION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
_BAOSTOCK_POLICY_BUDGET_KEYS = frozenset(
    {
        "max_login_batches",
        "max_api_calls_per_batch",
        "query_timeout_seconds",
        "max_identity_total_seconds",
        "max_history_session_count",
        "max_retry_attempts",
        "retry_backoff_seconds",
    }
)


def validate_baostock_primary_policy(value: Any) -> dict[str, Any]:
    """校验唯一股票价格来源及其显式请求预算。"""
    if not isinstance(value, Mapping):
        raise ValueError("BaoStock策略必须是对象")
    policy = dict(value)
    keys = set(policy)
    budget = policy.get("request_budget")
    if (
        not _BAOSTOCK_POLICY_REQUIRED_KEYS.issubset(keys)
        or not keys.issubset(
            _BAOSTOCK_POLICY_REQUIRED_KEYS | _BAOSTOCK_POLICY_OPTIONAL_KEYS
        )
        or policy.get("contract") != "STOCK-A-BAOSTOCK-PRIMARY-PRICE-SOURCE/R1"
        or policy.get("request_policy") != "MANUAL_BOUNDED_JOB"
        or policy.get("required_for_core") is not True
        or not isinstance(budget, Mapping)
        or set(budget) != _BAOSTOCK_POLICY_BUDGET_KEYS
        or type(budget.get("max_login_batches")) is not int
        or not 1 <= budget["max_login_batches"] <= 1000
        or type(budget.get("max_api_calls_per_batch")) is not int
        or not 1 <= budget["max_api_calls_per_batch"] <= 100
        or type(budget.get("query_timeout_seconds")) is not int
        or not 1 <= budget["query_timeout_seconds"] <= 30
        or type(budget.get("max_identity_total_seconds")) is not int
        or not 30 <= budget["max_identity_total_seconds"] <= 120
        or type(budget.get("max_history_session_count")) is not int
        or not 1 <= budget["max_history_session_count"] <= 250
        or type(budget.get("max_retry_attempts")) is not int
        or not 0 <= budget["max_retry_attempts"] <= 1
        or type(budget.get("retry_backoff_seconds")) is not int
        or not 0 <= budget["retry_backoff_seconds"] <= 10
    ):
        raise ValueError("BaoStock主价格源策略或请求预算不符合现行typed合同")
    revision = policy.get("policy_revision")
    if "policy_revision" in policy and (
        not isinstance(revision, str)
        or _BAOSTOCK_POLICY_REVISION.fullmatch(revision) is None
    ):
        raise ValueError("BaoStock policy_revision须为1至64位稳定ASCII标识")
    return policy
