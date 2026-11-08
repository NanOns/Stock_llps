"""来源级条件统计：仅描述已冻结T0与后继价格，不生成预测。"""

from collections import Counter
from fractions import Fraction
import json


def _quantile(values, numerator, denominator):
    """离散最近秩：第ceil(n*p)个值，空集合无值。"""
    if not values:
        return None
    ordered = sorted(values)
    rank = (len(ordered) * numerator + denominator - 1) // denominator
    return str(ordered[max(rank, 1) - 1])


def _distribution(values):
    return {
        "count": len(values),
        "p25": _quantile(values, 1, 4),
        "median": _quantile(values, 1, 2),
        "p75": _quantile(values, 3, 4),
    }


def summarize_day(
    *,
    target,
    source,
    namespace_id,
    calendar_revision,
    session_versions,
    frozen,
    outcomes,
    rules,
    horizons,
    market,
):
    """读取调用方在后继读取前冻结的三态及同日市场条件。"""
    symbols = sorted(frozen)
    if set(symbols) != set(outcomes):
        raise ValueError("冻结来源池与后继来源池不一致")
    unknown_rules = (
        set(rules) - set(next(iter(frozen.values()))["rules"]) if symbols else set()
    )
    if unknown_rules:
        raise ValueError("规则不在冻结来源池中")
    if any(
        frozen[s]["rules"][rule]["state"] not in {"TRUE", "FALSE", "UNKNOWN"}
        for s in symbols
        for rule in rules
    ):
        raise ValueError("规则三态无效，拒绝丢失来源分母")
    d1_identity = (
        next(iter(frozen.values())).get("d1_t0_predicate", {}) if symbols else {}
    )
    rows = []
    examples = []
    for rule in rules:
        for horizon in horizons:
            for state, label in (
                ("TRUE", "MATCH"),
                ("FALSE", "NO_MATCH"),
                ("UNKNOWN", "UNKNOWN"),
            ):
                members = [
                    s for s in symbols if frozen[s]["rules"][rule]["state"] == state
                ]
                maturity, quality, endpoint_reasons, path_reasons = (
                    Counter(),
                    Counter(),
                    Counter(),
                    Counter(),
                )
                endpoint_values = []
                event_values = []
                event_maturity, event_quality = Counter(), Counter()
                rule_reasons = Counter()
                paths = {key: [] for key in ("mfe", "mae", "mdd")}
                weights = 0
                due_count = 0
                for symbol in members:
                    fact = outcomes[symbol][horizon]
                    due_count += int(fact.get("due", False))
                    if state == "UNKNOWN" and not any(
                        e["kind"] == "T0_RULE_UNKNOWN"
                        and e["rule"] == rule
                        and e["horizon"] == horizon
                        for e in examples
                    ):
                        examples.append(
                            {
                                "kind": "T0_RULE_UNKNOWN",
                                "rule": rule,
                                "horizon": horizon,
                                "source_symbol": symbol,
                                "t0_rule_reasons": frozen[symbol]["rules"][rule].get(
                                    "reasons", []
                                ),
                            }
                        )
                    rule_reasons.update(
                        frozen[symbol]["rules"][rule].get("reasons", [])
                    )
                    weight = (
                        len(frozen[symbol]["anchors"])
                        if rule == "D1_ANCHOR_WINDOW" and state == "TRUE"
                        else 1
                    )
                    maturity[fact["maturity"]] += 1
                    quality[fact["path_quality"]] += 1
                    event_maturity[fact["maturity"]] += weight
                    event_quality[fact["path_quality"]] += weight
                    value = fact["endpoint"]["raw_close_relative_change"]
                    if value["quality"] == "OBSERVED":
                        amount = Fraction(value["value"]["exact"])
                        endpoint_values.append(amount)
                        event_values.extend([amount] * weight)
                        kind = (
                            "MATCH_NON_POSITIVE"
                            if state == "TRUE" and amount <= 0
                            else "NO_MATCH_POSITIVE"
                            if state == "FALSE" and amount > 0
                            else None
                        )
                        kinds = ([kind] if kind else []) + (
                            ["ENDPOINT_WITH_UNKNOWN_PATH"]
                            if fact["path_quality"] != "OBSERVED"
                            else []
                        )
                        for kind in kinds:
                            if any(
                                e["kind"] == kind
                                and e["rule"] == rule
                                and e["horizon"] == horizon
                                for e in examples
                            ):
                                continue
                            examples.append(
                                {
                                    "kind": kind,
                                    "rule": rule,
                                    "horizon": horizon,
                                    "source_symbol": symbol,
                                    "endpoint": str(amount),
                                    "t0_rule_state": state,
                                    "t0_rule_reasons": frozen[symbol]["rules"][
                                        rule
                                    ].get("reasons", []),
                                    "t0_failure_line": frozen[symbol]["rules"][
                                        rule
                                    ].get("frozen_line"),
                                    "path_reasons": fact["path_reasons"],
                                }
                            )
                    else:
                        endpoint_reasons[value.get("reason") or fact["maturity"]] += 1
                        if not any(
                            e["kind"] == "UNEVALUABLE"
                            and e["rule"] == rule
                            and e["horizon"] == horizon
                            and e["group"] == label
                            for e in examples
                        ):
                            examples.append(
                                {
                                    "kind": "UNEVALUABLE",
                                    "rule": rule,
                                    "horizon": horizon,
                                    "group": label,
                                    "source_symbol": symbol,
                                    "maturity": fact["maturity"],
                                    "endpoint_reason": value.get("reason"),
                                    "rule_reasons": frozen[symbol]["rules"][rule].get(
                                        "reasons", []
                                    ),
                                }
                            )
                    for reason in fact["path_reasons"]:
                        path_reasons[reason] += 1
                    if fact["path_quality"] == "OBSERVED" and not fact["path_values"]:
                        raise ValueError("完整路径缺少路径数值")
                    if fact["path_quality"] == "OBSERVED":
                        for key in paths:
                            paths[key].append(Fraction(fact["path_values"][key]))
                    weights += (
                        len(frozen[symbol]["anchors"])
                        if rule == "D1_ANCHOR_WINDOW" and state == "TRUE"
                        else 1
                    )
                positive = sum(v > 0 for v in endpoint_values)
                rows.append(
                    {
                        "rule": rule,
                        "horizon": horizon,
                        "group": label,
                        "source_count": len(symbols),
                        "count": len(members),
                        "due_count": due_count,
                        "not_due_count": len(members) - due_count,
                        "d1_event_count": weights
                        if rule == "D1_ANCHOR_WINDOW" and state == "TRUE"
                        else None,
                        "maturity": dict(maturity),
                        "path_quality": dict(quality),
                        "endpoint_reasons": dict(endpoint_reasons),
                        "path_reasons": dict(path_reasons),
                        "rule_reasons": dict(rule_reasons),
                        "endpoint": {
                            **_distribution(endpoint_values),
                            "positive": positive,
                            "non_positive": len(endpoint_values) - positive,
                        },
                        "path": {
                            key: _distribution(values) for key, values in paths.items()
                        },
                        "d1_event_endpoint": (
                            {
                                **_distribution(event_values),
                                "positive": sum(v > 0 for v in event_values),
                                "non_positive": sum(v <= 0 for v in event_values),
                            }
                            if rule == "D1_ANCHOR_WINDOW" and state == "TRUE"
                            else None
                        ),
                        "d1_event_maturity": dict(event_maturity)
                        if rule == "D1_ANCHOR_WINDOW" and state == "TRUE"
                        else None,
                        "d1_event_path_quality": dict(event_quality)
                        if rule == "D1_ANCHOR_WINDOW" and state == "TRUE"
                        else None,
                    }
                )
    comparisons = []
    for rule in rules:
        for horizon in horizons:
            selected = {
                r["group"]: r
                for r in rows
                if r["rule"] == rule and r["horizon"] == horizon
            }
            a, b = selected["MATCH"]["endpoint"], selected["NO_MATCH"]["endpoint"]
            comparable = bool(a["count"] and b["count"])
            comparisons.append(
                {
                    "rule": rule,
                    "horizon": horizon,
                    "status": "COMPUTED" if comparable else "NOT_EVALUABLE",
                    "median_difference": str(
                        Fraction(a["median"]) - Fraction(b["median"])
                    )
                    if comparable
                    else None,
                    "positive_fraction_difference": str(
                        Fraction(a["positive"], a["count"])
                        - Fraction(b["positive"], b["count"])
                    )
                    if comparable
                    else None,
                }
            )
    return {
        "target_session": target.isoformat(),
        "namespace_id": namespace_id,
        "source_version": source,
        "calendar_revision": calendar_revision,
        "forward_session_versions": session_versions,
        "d1_producer_identity": {
            key: d1_identity.get(key)
            for key in (
                "core_producer_symbol",
                "core_producer_source_sha256",
                "execution_adapter_symbol",
                "execution_adapter_source_sha256",
            )
        },
        "market": market,
        "groups": rows,
        "comparisons": comparisons,
        "examples": examples,
        "example_availability": [
            {
                "rule": rule,
                "horizon": horizon,
                "kind": kind,
                "status": "OBSERVED"
                if any(
                    e["rule"] == rule and e["horizon"] == horizon and e["kind"] == kind
                    for e in examples
                )
                else "NOT_OBSERVED_IN_WINDOW",
            }
            for rule in rules
            for horizon in horizons
            for kind in (
                "MATCH_NON_POSITIVE",
                "NO_MATCH_POSITIVE",
                "ENDPOINT_WITH_UNKNOWN_PATH",
                "T0_RULE_UNKNOWN",
            )
        ],
        "weighting": "股票按同日来源代码去重；D1匹配锚事件单独计权；不同规则允许重叠",
        "reason_counts_note": "原因可重叠；原因计数之和不是独立样本数",
    }


def aggregate_days(days):
    """对每个同日对照等权；市场宽度正负是预先固定的条件分层。"""
    identities = {
        (
            d["namespace_id"],
            d["source_version"].get("package_sha256"),
            d["source_version"].get("source_content_id"),
            d["source_version"].get("parser_contract"),
            d["source_version"].get("source_time_quality"),
            d["calendar_revision"],
            d.get("runtime_profile_revision"),
            json.dumps(d.get("rule_versions"), sort_keys=True, ensure_ascii=False),
            json.dumps(
                d.get("d1_producer_identity"), sort_keys=True, ensure_ascii=False
            ),
        )
        for d in days
    }
    if len(identities) > 1:
        raise ValueError("跨日来源、画像、日历或市场质量不同，拒绝混合")
    keys = sorted({(c["rule"], c["horizon"]) for d in days for c in d["comparisons"]})
    output = []
    for rule, horizon in keys:
        conditions = [("ALL", None)] + [
            (condition, quality)
            for condition in (
                "B_FIXED_POSITIVE",
                "B_FIXED_NON_POSITIVE",
                "B_FIXED_UNKNOWN",
            )
            for quality in sorted(
                {(d.get("market") or {}).get("status", "UNKNOWN") for d in days}
            )
        ]
        for condition, market_quality in conditions:
            selected = []
            for day in days:
                market = day.get("market") or {}
                metric = (market.get("metric") or {}).get("exact")
                value = (
                    Fraction(metric["fraction"])
                    if metric
                    and metric.get("fraction") is not None
                    and market.get("status") not in {"UNKNOWN", "UNKNOWN_DATA"}
                    else None
                )
                label = (
                    "B_FIXED_UNKNOWN"
                    if value is None
                    else "B_FIXED_POSITIVE"
                    if value > 0
                    else "B_FIXED_NON_POSITIVE"
                )
                if condition != "ALL" and condition != label:
                    continue
                if (
                    market_quality is not None
                    and market.get("status", "UNKNOWN") != market_quality
                ):
                    continue
                comparison = next(
                    c
                    for c in day["comparisons"]
                    if c["rule"] == rule and c["horizon"] == horizon
                )
                if condition == "B_FIXED_UNKNOWN":
                    comparison = {
                        **comparison,
                        "status": "NOT_EVALUABLE",
                        "median_difference": None,
                        "positive_fraction_difference": None,
                    }
                selected.append({"session": day["target_session"], **comparison})
            valid = [
                Fraction(c["median_difference"])
                for c in selected
                if c["status"] == "COMPUTED"
            ]
            valid_positive = [
                Fraction(c["positive_fraction_difference"])
                for c in selected
                if c["status"] == "COMPUTED"
            ]
            if condition == "B_FIXED_UNKNOWN":
                # 未知条件可展示逐日基础统计，但不能作为已知环境比较。
                valid = []
                valid_positive = []
            direction_counts = Counter(
                "POSITIVE" if value > 0 else "NEGATIVE" if value < 0 else "ZERO"
                for value in valid
            )
            output.append(
                {
                    "rule": rule,
                    "horizon": horizon,
                    "condition": condition,
                    "condition_scope": "完整来源描述基线，包含市场条件未知日；不代表已知市场环境"
                    if condition == "ALL"
                    else "按T0市场条件和质量分层",
                    "market_quality_date_counts": dict(
                        Counter(
                            (d.get("market") or {}).get("status", "UNKNOWN")
                            for d in days
                        )
                    )
                    if condition == "ALL"
                    else None,
                    "market_quality": market_quality,
                    "dates": selected,
                    "requested_date_count": len(days),
                    "condition_date_count": len(selected),
                    "condition_not_selected_date_count": len(days) - len(selected),
                    "unevaluable_condition_date_count": len(selected) - len(valid),
                    "valid_date_count": len(valid),
                    "median_difference_direction_counts": dict(direction_counts),
                    "median_difference_direction_consistency": (
                        "CONSISTENT" if len(direction_counts) == 1 else "MIXED"
                    )
                    if len(valid) > 1
                    else "INSUFFICIENT_INDEPENDENT_DATES"
                    if valid
                    else "NOT_EVALUABLE",
                    "equal_date_mean_median_difference": str(
                        sum(valid, Fraction()) / len(valid)
                    )
                    if valid
                    else None,
                    "equal_date_mean_positive_fraction_difference": str(
                        sum(valid_positive, Fraction()) / len(valid_positive)
                    )
                    if valid_positive
                    else None,
                    "status": "DESCRIPTIVE"
                    if len(valid) > 1
                    else "INSUFFICIENT_INDEPENDENT_DATES"
                    if valid
                    else "NOT_EVALUABLE",
                }
            )
    return output
