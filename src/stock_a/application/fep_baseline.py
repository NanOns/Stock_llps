"""从已存typed研究事实生成并持久化描述性FEP Baseline。"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from datetime import date
from fractions import Fraction
from typing import Any

from stock_a.application.research_window import resolve_pg_history_window
from stock_a.research.entry_context.sessions import _load_calendar
from stock_a.research.source_fep_baseline import aggregate_days, summarize_day
from stock_a.storage.postgres.fep_baseline import (
    persist_fep_baseline_rows,
    read_fep_source_contexts,
)
from stock_a.storage.postgres.research_facts import (
    PATH_VERSION,
    ResearchFactConflict,
)

_LANES = ("ENTRY", "DAILY_LANDMARK", "REENTRY")
_METRICS = (
    ("RAW_CLOSE_RETURN", "endpoint"),
    ("RAW_MFE", "mfe"),
    ("RAW_MAE", "mae"),
    ("RAW_MDD_CLOSE", "mdd"),
)


def _fraction_pair(value: str | None) -> tuple[int | None, int | None]:
    if value is None:
        return None, None
    fraction = Fraction(value)
    return fraction.numerator, fraction.denominator


def _stored_fraction(numerator: Any, denominator: Any) -> str | None:
    if numerator is None and denominator is None:
        return None
    if numerator is None or denominator is None or int(denominator) <= 0:
        raise ResearchFactConflict("Forward精确路径分数不完整")
    return str(Fraction(int(numerator), int(denominator)))


def _reason_arrays(values: Mapping[str, int]) -> tuple[list[str], list[int]]:
    pairs = sorted((str(code), int(count)) for code, count in values.items() if count)
    return [code for code, _ in pairs], [count for _, count in pairs]


def _base_row(
    *,
    context: Mapping[str, Any],
    research_cutoff: date,
    lane: str,
    horizon: int,
    summary_scope: str,
    group_code: str,
    metric_code: str,
    condition_code: str = "ALL",
    status: str = "NOT_COMPUTED",
) -> dict[str, Any]:
    return {
        "research_cutoff": research_cutoff,
        "source_capture_id": context["source_capture_id"],
        "target_session": context["target_session"] if summary_scope == "T0" else None,
        "period_start_session": None,
        "period_end_session": None,
        "rule_id": context["rule_id"],
        "rule_version": context["rule_version"],
        "profile_family": context["profile_family"],
        "profile_revision": context["profile_revision"]
        if summary_scope == "T0"
        else None,
        "history_session_count": context["history_session_count"]
        if summary_scope == "T0"
        else None,
        "d1_history_session_count": context["d1_history_session_count"]
        if summary_scope == "T0"
        else None,
        "event_window_start": context["event_window_start"]
        if summary_scope == "T0"
        else None,
        "calendar_revision": context["calendar_revision"],
        "path_rule_version": PATH_VERSION,
        "source_time_quality": context["source_time_quality"],
        "parser_contract": context["parser_contract"],
        "lane": lane,
        "horizon": horizon,
        "summary_scope": summary_scope,
        "group_code": group_code,
        "metric_code": metric_code,
        "condition_code": condition_code,
        "market_condition_code": None,
        "status": status,
        "capability_code": "DESCRIPTIVE_ONLY",
        "entity_code": "SOURCE_STOCK",
        "target_code": "RAW_CLOSE_RETURN",
        "scope": "NON_PIT_EXPLORATORY",
        "price_coordinate": "FORWARD_ADJUSTED",
        "reason_codes": [],
        "reason_counts": [],
    }


def _endpoint_fact(forward: Mapping[str, Any] | None, *, due: bool) -> dict[str, Any]:
    if not due:
        return {
            "maturity": "NOT_DUE",
            "due": False,
            "endpoint": {
                "raw_close_relative_change": {
                    "quality": "NOT_DUE",
                    "reason": "OFFICIAL_ENDPOINT_AFTER_RESEARCH_CUTOFF",
                }
            },
            "path_quality": "NOT_DUE",
            "path_reasons": ["OFFICIAL_ENDPOINT_AFTER_RESEARCH_CUTOFF"],
            "path_values": {},
        }
    if forward is None:
        return {
            "maturity": "NOT_COMPUTED",
            "due": True,
            "endpoint": {
                "raw_close_relative_change": {
                    "quality": "NOT_COMPUTED",
                    "reason": "DUE_FORWARD_FACT_MISSING",
                }
            },
            "path_quality": "NOT_COMPUTED",
            "path_reasons": ["DUE_FORWARD_FACT_MISSING"],
            "path_values": {},
        }
    endpoint_quality = forward["endpoint_quality"]
    endpoint_value = None
    if endpoint_quality == "OBSERVED":
        numerator = forward["endpoint_numerator"]
        denominator = forward["endpoint_denominator"]
        if numerator is None or denominator is None or int(denominator) <= 0:
            raise ValueError("OBSERVED Forward端点缺少精确分数")
        endpoint_value = {"exact": str(Fraction(int(numerator), int(denominator)))}
    endpoint_reason = forward["endpoint_reason"] or endpoint_quality
    if forward["path_quality"] not in {"OBSERVED", "UNKNOWN", "NOT_COMPUTED"}:
        raise ResearchFactConflict("Forward路径质量状态不在现役合同内")
    path_values = {
        "mfe": _stored_fraction(forward["mfe_numerator"], forward["mfe_denominator"]),
        "mae": _stored_fraction(forward["mae_numerator"], forward["mae_denominator"]),
        "mdd": _stored_fraction(forward["mdd_numerator"], forward["mdd_denominator"]),
    }
    if forward["path_quality"] == "OBSERVED" and any(
        value is None for value in path_values.values()
    ):
        raise ResearchFactConflict("OBSERVED Forward路径缺少MFE/MAE/MDD数值")
    if forward["path_quality"] != "OBSERVED" and any(
        value is not None for value in path_values.values()
    ):
        raise ResearchFactConflict("非OBSERVED Forward路径携带可评价路径数值")
    return {
        "maturity": forward["maturity"],
        "due": True,
        "endpoint": {
            "raw_close_relative_change": {
                "quality": endpoint_quality,
                "value": {"exact": endpoint_value["exact"]} if endpoint_value else None,
                "reason": endpoint_reason,
            }
        },
        "path_quality": forward["path_quality"],
        "path_reasons": list(forward["path_reasons"] or ()),
        "path_values": path_values if forward["path_quality"] == "OBSERVED" else {},
    }


def _status_for_group(
    *,
    member_count: int,
    due_count: int,
    not_due_count: int,
    observed_count: int,
    unknown_count: int,
    not_computed_count: int,
) -> str:
    if observed_count:
        return "OBSERVED"
    if member_count and not_due_count == member_count:
        return "NOT_DUE"
    if unknown_count:
        return "UNKNOWN"
    if not_computed_count or due_count:
        return "NOT_COMPUTED"
    return "NOT_COMPUTED"


def _daily_rows(
    *,
    context: Mapping[str, Any],
    day_summary: Mapping[str, Any],
    research_cutoff: date,
    horizon: int,
    lane: str,
    due: bool,
) -> list[dict[str, Any]]:
    market = context["market"]
    market_condition = (
        "B_FIXED_UNKNOWN"
        if market is None
        or not market["complete_for_condition"]
        or market["metric"] is None
        else "B_FIXED_POSITIVE"
        if market["metric"] > 0
        else "B_FIXED_NON_POSITIVE"
    )
    rows = []
    for group in day_summary["groups"]:
        if group["rule"] != context["rule_id"] or group["horizon"] != horizon:
            continue
        group_code = group["group"]
        for metric_code, key in _METRICS:
            metric = group["endpoint"] if key == "endpoint" else group["path"][key]
            observed_count = int(metric["count"])
            member_count = int(group["count"])
            not_due_count = int(group["not_due_count"])
            due_count = int(group["due_count"])
            if key == "endpoint":
                reasons = group["endpoint_reasons"]
                not_computed_count = sum(
                    count
                    for quality, count in group["endpoint_quality"].items()
                    if quality in {"NOT_COMPUTED", "INCOMPLETE"}
                )
                unknown_count = max(
                    0,
                    member_count - observed_count - not_due_count - not_computed_count,
                )
                positive_count = int(metric["positive"])
                non_positive_count = int(metric["non_positive"])
            else:
                reasons = group["path_reasons"]
                not_computed_count = sum(
                    count
                    for quality, count in group["path_quality"].items()
                    if quality in {"NOT_COMPUTED", "INCOMPLETE"}
                )
                unknown_count = max(
                    0,
                    member_count - observed_count - not_due_count - not_computed_count,
                )
                positive_count = None
                non_positive_count = None
            status = (
                "NOT_EVALUABLE"
                if member_count == 0
                else _status_for_group(
                    member_count=member_count,
                    due_count=due_count,
                    not_due_count=not_due_count,
                    observed_count=observed_count,
                    unknown_count=unknown_count,
                    not_computed_count=not_computed_count,
                )
            )
            row = _base_row(
                context=context,
                research_cutoff=research_cutoff,
                lane=lane,
                horizon=horizon,
                summary_scope="T0",
                group_code=group_code,
                metric_code=metric_code,
                status=status,
            )
            row.update(
                {
                    "market_condition_code": market_condition,
                    "source_pool_count": group["source_count"],
                    "t0_match_count": group["rule_true_count"],
                    "t0_no_match_count": next(
                        item["count"]
                        for item in day_summary["groups"]
                        if item["rule"] == group["rule"]
                        and item["horizon"] == horizon
                        and item["group"] == "NO_MATCH"
                    ),
                    "t0_unknown_count": next(
                        item["count"]
                        for item in day_summary["groups"]
                        if item["rule"] == group["rule"]
                        and item["horizon"] == horizon
                        and item["group"] == "UNKNOWN"
                    ),
                    "entry_count": group["lane_enrollment_counts"].get("ENTRY", 0),
                    "daily_landmark_count": group["lane_enrollment_counts"].get(
                        "DAILY_LANDMARK", 0
                    ),
                    "reentry_count": group["lane_enrollment_counts"].get("REENTRY", 0),
                    "left_censored_count": group["lane_enrollment_counts"].get(
                        "FIRST_OBSERVED_LEFT_CENSORED", 0
                    ),
                    "member_count": member_count,
                    "due_count": due_count,
                    "not_due_count": not_due_count,
                    "observed_count": observed_count,
                    "unknown_count": unknown_count,
                    "not_computed_count": not_computed_count,
                    "positive_count": positive_count,
                    "non_positive_count": non_positive_count,
                    "market_time_quality": market["time_quality"]
                    if market
                    else "UNKNOWN",
                    "market_source_count": market["source_count"] if market else None,
                    "market_eligible_count": market["eligible_count"]
                    if market
                    else None,
                }
            )
            for quantile in ("p25", "median", "p75"):
                numerator, denominator = _fraction_pair(metric[quantile])
                prefix = "p50" if quantile == "median" else quantile
                row[f"{prefix}_numerator"] = numerator
                row[f"{prefix}_denominator"] = denominator
            reason_codes, reason_counts = _reason_arrays(reasons)
            row["reason_codes"] = reason_codes
            row["reason_counts"] = reason_counts
            if key == "endpoint" and market and market["metric"] is not None:
                row["market_metric_numerator"] = market["metric"].numerator
                row["market_metric_denominator"] = market["metric"].denominator
            rows.append(row)

    comparison = next(
        item
        for item in day_summary["comparisons"]
        if item["rule"] == context["rule_id"] and item["horizon"] == horizon
    )
    comparison_groups = [
        item
        for item in day_summary["groups"]
        if item["rule"] == context["rule_id"]
        and item["horizon"] == horizon
        and item["group"] in {"MATCH", "NO_MATCH"}
    ]
    comparison_quality = Counter(
        quality
        for group in comparison_groups
        for quality, count in group["endpoint_quality"].items()
        for _ in range(count)
    )
    comparison_status = (
        "OBSERVED"
        if comparison["status"] == "COMPUTED"
        else "NOT_DUE"
        if not due
        else "INCOMPLETE"
        if day_summary["input_status_by_horizon"][horizon] == "INCOMPLETE"
        else "NOT_COMPUTED"
        if comparison_quality["NOT_COMPUTED"] or comparison_quality["INCOMPLETE"]
        else "UNKNOWN"
        if any(
            quality not in {"OBSERVED", "NOT_DUE", "NOT_COMPUTED", "INCOMPLETE"}
            for quality in comparison_quality
        )
        else "NOT_EVALUABLE"
    )
    row = _base_row(
        context=context,
        research_cutoff=research_cutoff,
        lane=lane,
        horizon=horizon,
        summary_scope="T0",
        group_code="MATCH_VS_NO_MATCH",
        metric_code="RAW_CLOSE_RETURN",
        status=comparison_status,
    )
    all_groups = {
        item["group"]: item
        for item in day_summary["groups"]
        if item["rule"] == context["rule_id"] and item["horizon"] == horizon
    }
    row.update(
        {
            "market_condition_code": market_condition,
            "source_pool_count": all_groups["MATCH"]["source_count"],
            "t0_match_count": all_groups["MATCH"]["rule_true_count"],
            "t0_no_match_count": all_groups["NO_MATCH"]["count"],
            "t0_unknown_count": all_groups["UNKNOWN"]["count"],
            "entry_count": all_groups["MATCH"]["lane_enrollment_counts"].get(
                "ENTRY", 0
            ),
            "daily_landmark_count": all_groups["MATCH"]["lane_enrollment_counts"].get(
                "DAILY_LANDMARK", 0
            ),
            "reentry_count": all_groups["MATCH"]["lane_enrollment_counts"].get(
                "REENTRY", 0
            ),
            "left_censored_count": all_groups["MATCH"]["lane_enrollment_counts"].get(
                "FIRST_OBSERVED_LEFT_CENSORED", 0
            ),
            "median_difference_numerator": _fraction_pair(
                comparison["median_difference"]
            )[0],
            "median_difference_denominator": _fraction_pair(
                comparison["median_difference"]
            )[1],
            "positive_fraction_difference_numerator": _fraction_pair(
                comparison["positive_fraction_difference"]
            )[0],
            "positive_fraction_difference_denominator": _fraction_pair(
                comparison["positive_fraction_difference"]
            )[1],
            "reason_codes": []
            if comparison_status == "OBSERVED"
            else [comparison_status],
            "reason_counts": [] if comparison_status == "OBSERVED" else [1],
            "market_time_quality": market["time_quality"] if market else "UNKNOWN",
            "market_source_count": market["source_count"] if market else None,
            "market_eligible_count": market["eligible_count"] if market else None,
        }
    )
    if market and market["metric"] is not None:
        row["market_metric_numerator"] = market["metric"].numerator
        row["market_metric_denominator"] = market["metric"].denominator
    rows.append(row)
    return rows


def _equal_date_rows(
    *,
    contexts: list[Mapping[str, Any]],
    summaries: list[Mapping[str, Any]],
    research_cutoff: date,
    lane: str,
    horizon: int,
) -> list[dict[str, Any]]:
    aggregated = aggregate_days(summaries)
    rows = []
    for result in aggregated:
        if result["rule"] != contexts[0]["rule_id"] or result["horizon"] != horizon:
            continue
        condition = result["condition"]
        selected = result["dates"]
        status = (
            "DESCRIPTIVE_ONLY"
            if result["status"] == "DESCRIPTIVE"
            else "INSUFFICIENT_INDEPENDENT_DATES"
            if result["status"] == "INSUFFICIENT_INDEPENDENT_DATES"
            else "UNKNOWN"
            if condition == "B_FIXED_UNKNOWN"
            else "NOT_COMPUTED"
        )
        if contexts[0]["source_time_quality"] == "UNKNOWN":
            status = "UNKNOWN"
        elif result["status"] == "NOT_EVALUABLE" and condition != "B_FIXED_UNKNOWN":
            status = "NOT_EVALUABLE"
        base = _base_row(
            context=contexts[0],
            research_cutoff=research_cutoff,
            lane=lane,
            horizon=horizon,
            summary_scope="EQUAL_DATE",
            group_code="MATCH_VS_NO_MATCH",
            metric_code="RAW_CLOSE_RETURN",
            condition_code=condition,
            status=status,
        )
        context_by_session = {
            context["target_session"].isoformat(): context for context in contexts
        }
        summary_by_session = {
            summary["target_session"]: summary for summary in summaries
        }
        not_due_count = 0
        unknown_date_count = 0
        not_computed_date_count = 0
        for item in selected:
            session = item["session"]
            summary = summary_by_session[session]
            context = context_by_session[session]
            if summary["input_status_by_horizon"][horizon] == "NOT_DUE":
                not_due_count += 1
                continue
            comparison = next(
                value
                for value in summary["comparisons"]
                if value["rule"] == result["rule"] and value["horizon"] == horizon
            )
            match_groups = [
                group
                for group in summary["groups"]
                if group["rule"] == result["rule"]
                and group["horizon"] == horizon
                and group["group"] in {"MATCH", "NO_MATCH"}
            ]
            has_unknown_metric = any(
                any(
                    quality not in {"OBSERVED", "NOT_DUE", "NOT_COMPUTED", "INCOMPLETE"}
                    and count > 0
                    for quality, count in group["endpoint_quality"].items()
                )
                for group in match_groups
            )
            has_not_computed_metric = any(
                any(
                    quality in {"NOT_COMPUTED", "INCOMPLETE"} and count > 0
                    for quality, count in group["endpoint_quality"].items()
                )
                for group in match_groups
            )
            if has_unknown_metric or context["source_time_quality"] == "UNKNOWN":
                unknown_date_count += 1
            elif (
                has_not_computed_metric
                or summary["input_status_by_horizon"][horizon] == "INCOMPLETE"
            ):
                not_computed_date_count += 1
        direction_counts = result["median_difference_direction_counts"]
        base.update(
            {
                "period_start_session": min(
                    item["target_session"] for item in contexts
                ),
                "period_end_session": max(item["target_session"] for item in contexts),
                "requested_date_count": result["requested_date_count"],
                "condition_date_count": result["condition_date_count"],
                "valid_date_count": result["valid_date_count"],
                "not_due_date_count": not_due_count,
                "unknown_date_count": unknown_date_count,
                "not_computed_date_count": not_computed_date_count,
                "equal_date_mean_median_difference_numerator": _fraction_pair(
                    result["equal_date_mean_median_difference"]
                )[0],
                "equal_date_mean_median_difference_denominator": _fraction_pair(
                    result["equal_date_mean_median_difference"]
                )[1],
                "equal_date_mean_positive_fraction_difference_numerator": _fraction_pair(
                    result["equal_date_mean_positive_fraction_difference"]
                )[0],
                "equal_date_mean_positive_fraction_difference_denominator": _fraction_pair(
                    result["equal_date_mean_positive_fraction_difference"]
                )[1],
                "positive_direction_date_count": direction_counts.get("POSITIVE", 0),
                "negative_direction_date_count": direction_counts.get("NEGATIVE", 0),
                "zero_direction_date_count": direction_counts.get("ZERO", 0),
                "reason_codes": [] if status == "DESCRIPTIVE_ONLY" else [status],
                "reason_counts": [] if status == "DESCRIPTIVE_ONLY" else [1],
            }
        )
        rows.append(base)
    return rows


def prepare_fep_baseline(
    config: Mapping[str, Any],
    *,
    research_cutoff: date,
    source_capture_id: int,
    rule_ids: tuple[str, ...],
    horizons: tuple[int, ...],
    lanes: tuple[str, ...] = _LANES,
) -> dict[str, Any]:
    """只读准备typed行；不写PG，供生产前核对与同一生产链复用。"""
    if (
        type(research_cutoff) is not date
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or not rule_ids
        or not horizons
        or not lanes
        or any(lane not in _LANES for lane in lanes)
    ):
        raise ValueError("FEP生产范围无效")
    contexts = read_fep_source_contexts(
        config,
        research_cutoff=research_cutoff,
        source_capture_id=source_capture_id,
        rule_ids=rule_ids,
        horizons=horizons,
    )
    return _prepare_fep_from_contexts(
        contexts=contexts,
        research_cutoff=research_cutoff,
        source_capture_id=source_capture_id,
        rule_ids=rule_ids,
        horizons=horizons,
        lanes=lanes,
    )


def _prepare_fep_from_contexts(
    *,
    contexts: list[dict[str, Any]],
    research_cutoff: date,
    source_capture_id: int,
    rule_ids: tuple[str, ...],
    horizons: tuple[int, ...],
    lanes: tuple[str, ...],
) -> dict[str, Any]:
    """按已读取的同一事实快照计算FEP，供常规生产及受控纠错复用。"""
    if not contexts:
        return {
            "status": "NOT_COMPUTED",
            "reason_codes": ["FEP_SOURCE_RULE_FACTS_NOT_SAVED"],
            "research_cutoff": research_cutoff.isoformat(),
            "source_capture_id": source_capture_id,
            "rule_ids": list(rule_ids),
            "horizons": list(horizons),
            "lanes": list(lanes),
            "input_date_count": 0,
            "typed_row_count": 0,
            "capability": "DESCRIPTIVE_ONLY",
            "scope": "NON_PIT_EXPLORATORY",
            "price_coordinate": "FORWARD_ADJUSTED",
            "typed_rows": [],
            "coverage": [],
            "database_write_attempted": False,
            "source_io_attempted": False,
            "ddl_executed": False,
        }
    _, calendar_metadata, _calendar_revision_chain, calendar_sessions = _load_calendar(
        research_cutoff
    )
    annual_revisions = {
        int(item["year"]): item["revision"] for item in calendar_metadata
    }
    session_index = {session: index for index, session in enumerate(calendar_sessions)}
    for context in contexts:
        target = context["target_session"]
        target_calendar_data, _, _, _ = _load_calendar(target)
        target_year_revision = annual_revisions.get(target.year)
        resolved, _window, rule_calendar_revision = resolve_pg_history_window(
            target.isoformat(), context["d1_history_session_count"]
        )
        annual_component = f"{target.year}:{target_year_revision}"
        if (
            resolved != target
            or target not in session_index
            or target_year_revision != target_calendar_data.get("revision")
            or rule_calendar_revision != context["calendar_revision"]
            or annual_component not in context["calendar_revision"].split("+")
        ):
            raise ValueError("T0规则与Forward不在同一官方日历修订链，拒绝生产FEP")
    by_aggregate_identity: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for context in contexts:
        identity = (
            context["source_capture_id"],
            context["rule_id"],
            context["rule_version"],
            context["profile_family"],
            context["calendar_revision"],
            PATH_VERSION,
            context["source_time_quality"],
            context["parser_contract"],
        )
        by_aggregate_identity.setdefault(identity, []).append(context)

    typed_rows = []
    coverage = []
    for identity_contexts in by_aggregate_identity.values():
        dates = [context["target_session"] for context in identity_contexts]
        if len(dates) != len(set(dates)):
            raise ValueError("同一FEP分区存在重复T0日期，拒绝伪造独立样本数")
        lane_summaries = {lane: [] for lane in lanes}
        for context in identity_contexts:
            target = context["target_session"]
            if target > research_cutoff or target not in session_index:
                raise ValueError("FEP T0不在截止内官方会话中")
            symbols = set(context["rule_facts"])
            if (
                context["raw_count"] != len(symbols)
                or context["raw_symbols"] != symbols
                or not context["enrollment_complete"]
            ):
                raise ValueError("FEP T0 Raw、三态来源分母或lane登记未完整闭合")
            state_counts = Counter(
                fact["state"] for fact in context["rule_facts"].values()
            )
            if sum(state_counts.values()) != len(symbols) or not set(
                state_counts
            ).issubset({"TRUE", "FALSE", "UNKNOWN"}):
                raise ValueError("FEP T0的MATCH/NO_MATCH/UNKNOWN分母未闭合")
            frozen = {
                symbol: {
                    "rules": {
                        context["rule_id"]: {
                            "state": fact["state"],
                            "reasons": fact["reasons"],
                        }
                    },
                    "anchors": [],
                }
                for symbol, fact in context["rule_facts"].items()
            }
            outcomes = {symbol: {} for symbol in symbols}
            due_by_horizon = {}
            forward_complete_by_horizon = {}
            endpoint_by_horizon = {}
            for horizon in horizons:
                endpoint_index = session_index[target] + horizon
                if endpoint_index >= len(calendar_sessions):
                    raise ValueError("官方日历不足以判断FEP后继到期状态")
                endpoint_session = calendar_sessions[endpoint_index]
                if endpoint_session.year not in annual_revisions:
                    raise ValueError("FEP端点年度缺少已核验官方日历资源")
                due = endpoint_session <= research_cutoff
                due_by_horizon[horizon] = due
                forward = context["forward_by_horizon"][horizon]
                expected_symbols = set(forward)
                complete = (
                    not due
                    or expected_symbols == symbols
                    and all(item["valid_forward_identity"] for item in forward.values())
                    and all(
                        item["endpoint_session"] == endpoint_session
                        for item in forward.values()
                    )
                )
                forward_complete_by_horizon[horizon] = complete
                endpoint_by_horizon[horizon] = endpoint_session
                for symbol in symbols:
                    outcomes[symbol][horizon] = _endpoint_fact(
                        forward.get(symbol) if due else None,
                        due=due and complete,
                    )
                    if due and not complete:
                        outcomes[symbol][horizon] = {
                            "maturity": "INCOMPLETE",
                            "due": True,
                            "endpoint": {
                                "raw_close_relative_change": {
                                    "quality": "NOT_COMPUTED",
                                    "reason": "DUE_FORWARD_SOURCE_POOL_INCOMPLETE",
                                }
                            },
                            "path_quality": "NOT_COMPUTED",
                            "path_reasons": ["DUE_FORWARD_SOURCE_POOL_INCOMPLETE"],
                            "path_values": {},
                        }

            market = context["market"]
            market_metric = (
                market["metric"]
                if market and market["complete_for_condition"]
                else None
            )
            market_input = {
                "status": "OBSERVED" if market_metric is not None else "UNKNOWN",
                "metric": {
                    "exact": {"fraction": str(market_metric)}
                    if market_metric is not None
                    else {"fraction": None}
                },
            }
            day_by_lane = {}
            for lane in lanes:
                summary = summarize_day(
                    target=target,
                    source={
                        "source_capture_id": context["source_capture_id"],
                        "parser_contract": context["parser_contract"],
                        "source_time_quality": context["source_time_quality"],
                    },
                    calendar_revision=context["calendar_revision"],
                    session_versions={"path_rule_version": PATH_VERSION},
                    frozen=frozen,
                    outcomes=outcomes,
                    rules=(context["rule_id"],),
                    horizons=horizons,
                    market=market_input,
                    lane=lane,
                    enrollment_kinds={context["rule_id"]: context["enrollments"]},
                )
                summary["runtime_profile_revision"] = context["profile_family"]
                summary["rule_versions"] = {
                    context["rule_id"]: {
                        "algorithm_contract": context["rule_version"],
                        "producer_version": context["profile_family"],
                    }
                }
                summary["profile_revision"] = context["profile_revision"]
                summary["history_session_count"] = context["history_session_count"]
                summary["d1_history_session_count"] = context[
                    "d1_history_session_count"
                ]
                summary["endpoint_sessions"] = {
                    str(horizon): endpoint_by_horizon[horizon].isoformat()
                    for horizon in horizons
                }
                summary["input_status_by_horizon"] = {
                    horizon: "NOT_DUE"
                    if not due_by_horizon[horizon]
                    else "OBSERVED"
                    if forward_complete_by_horizon[horizon]
                    else "INCOMPLETE"
                    for horizon in horizons
                }
                day_by_lane[lane] = summary
                lane_summaries[lane].append(summary)
                for horizon in horizons:
                    typed_rows.extend(
                        _daily_rows(
                            context=context,
                            day_summary=summary,
                            research_cutoff=research_cutoff,
                            horizon=horizon,
                            lane=lane,
                            due=due_by_horizon[horizon],
                        )
                    )
            coverage.append(
                {
                    "target_session": target.isoformat(),
                    "source_pool_count": len(symbols),
                    "rule_state_counts": dict(state_counts),
                    "enrollment_counts": dict(Counter(context["enrollments"].values())),
                    "horizons": {
                        str(horizon): {
                            "endpoint_session": endpoint_by_horizon[
                                horizon
                            ].isoformat(),
                            "status": "NOT_DUE"
                            if not due_by_horizon[horizon]
                            else "OBSERVED"
                            if forward_complete_by_horizon[horizon]
                            else "INCOMPLETE",
                            "forward_row_count": len(
                                context["forward_by_horizon"][horizon]
                            ),
                        }
                        for horizon in horizons
                    },
                }
            )

        for lane, summaries in lane_summaries.items():
            if not summaries:
                continue
            for horizon in horizons:
                equal_rows = _equal_date_rows(
                    contexts=identity_contexts,
                    summaries=summaries,
                    research_cutoff=research_cutoff,
                    lane=lane,
                    horizon=horizon,
                )
                typed_rows.extend(equal_rows)

    if not typed_rows:
        raise ValueError("FEP没有形成可持久化的目标分区")
    return {
        "status": "PREPARED",
        "research_cutoff": research_cutoff.isoformat(),
        "source_capture_id": source_capture_id,
        "rule_ids": list(rule_ids),
        "horizons": list(horizons),
        "lanes": list(lanes),
        "input_date_count": len(contexts),
        "typed_row_count": len(typed_rows),
        "capability": "DESCRIPTIVE_ONLY",
        "scope": "NON_PIT_EXPLORATORY",
        "price_coordinate": "FORWARD_ADJUSTED",
        "typed_rows": typed_rows,
        "coverage": coverage,
    }


def produce_fep_baseline(
    config: Mapping[str, Any],
    *,
    research_cutoff: date,
    source_capture_id: int,
    rule_ids: tuple[str, ...],
    horizons: tuple[int, ...],
    lanes: tuple[str, ...] = _LANES,
) -> dict[str, Any]:
    """只汇总已存BaoStock来源事实；不重算Forward价格路径。"""
    prepared = prepare_fep_baseline(
        config,
        research_cutoff=research_cutoff,
        source_capture_id=source_capture_id,
        rule_ids=rule_ids,
        horizons=horizons,
        lanes=lanes,
    )
    if prepared["status"] == "NOT_COMPUTED":
        return prepared
    write_result = persist_fep_baseline_rows(
        config, prepared["typed_rows"], allow_statistical_restatement=False
    )
    return {key: value for key, value in prepared.items() if key != "typed_rows"} | {
        "status": "PERSISTED",
        "write": write_result,
    }
