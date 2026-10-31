from __future__ import annotations

import re
from typing import Any, Mapping


_BAOSTOCK_POLICY_REQUIRED_KEYS = frozenset(
    {
        "contract",
        "request_policy",
        "reason_code",
        "required_for_core",
        "request_budget",
    }
)
_BAOSTOCK_POLICY_OPTIONAL_KEYS = frozenset({"policy_revision"})
_BAOSTOCK_POLICY_REVISION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
_BAOSTOCK_POLICY_BUDGET = {
    "max_logins": 1,
    "max_queries_per_target_session": 10,
    "query_timeout_seconds": 30,
}


def validate_optional_baostock_policy(value: Any) -> dict[str, Any]:
    """校验当前Bao补充来源策略，保留可独立演进的显式policy_revision。"""
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
        or policy.get("contract") != "STOCK-A-OPTIONAL-BAOSTOCK-SUPPLEMENT/R1"
        or policy.get("request_policy") != "NOT_REQUESTED"
        or policy.get("reason_code") != "OPTIONAL_SOURCE_NOT_REQUESTED"
        or policy.get("required_for_core") is not False
        or not isinstance(budget, Mapping)
        or set(budget) != set(_BAOSTOCK_POLICY_BUDGET)
        or any(
            type(budget[key]) is not int or budget[key] != expected
            for key, expected in _BAOSTOCK_POLICY_BUDGET.items()
        )
    ):
        raise ValueError("BaoStock策略未知或预算不符合现行typed合同")
    revision = policy.get("policy_revision")
    if "policy_revision" in policy and (
        not isinstance(revision, str)
        or _BAOSTOCK_POLICY_REVISION.fullmatch(revision) is None
    ):
        raise ValueError("BaoStock policy_revision须为1至64位稳定ASCII标识")
    return policy
