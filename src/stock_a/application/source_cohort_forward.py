"""按T0完整来源池计算独立研究组及后验原始价格路径。"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from datetime import date
from fractions import Fraction
from statistics import median
from typing import Any

from stock_a.application.research_window import resolve_pg_history_window
from stock_a.application.source_d0_raw import (
    attach_shape_state_followup,
    query_source_d0_raw_launch,
    read_source_shape_cohort,
)
from stock_a.application.source_market_observation import SourceObservationError
from stock_a.research.entry_context.sessions import _load_calendar
from stock_a.research.entry_history.history_preview import (
    prepare_source_d1_preview_context,
    research_stock_pg_d1_history_previews,
)
from stock_a.research.source_forward_path import exploratory_raw_path
from stock_a.storage.postgres.source_raw import (
    read_raw_daily_forward_pool,
    read_raw_daily_history_page,
)


def _read_t0_d1(config, day, package_sha256, history_count):
    target = day["session"]
    _, sessions, _ = resolve_pg_history_window(target.isoformat(), history_count)
    context = prepare_source_d1_preview_context((target,))
    states = {}
    offset = 0
    expected = set(day["states"])
    source = day["scenarios"]["LAUNCH_CONFIRM"]["source_version"]
    while True:
        page = read_raw_daily_history_page(
            config,
            target_session=target,
            package_sha256=package_sha256,
            sessions=sessions,
            offset=offset,
            limit=100,
            market="ALL",
            board="ALL",
        )
        if (
            page["namespace_id"] != day["namespace_id"]
            or page["total_count"] != len(expected)
            or page["version_status"] != "CLOSED"
            or not page["partition_closed"]
        ):
            raise SourceObservationError("D1来源池或版本不完整")
        for candidate in page["candidates"]:
            symbol = candidate["source_symbol"]
            rows = candidate["history_rows"]
            birth = next((row for row in rows if row["session_date"] == target), None)
            if (
                symbol not in expected
                or symbol in states
                or birth is None
                or birth["source_content_id"] != source["source_content_id"]
                or birth["source_capture_id"] != source["source_capture_id"]
            ):
                raise SourceObservationError("D1来源代码或T0捕获不一致")
            mismatches = []
            safe_rows = []
            for row in rows:
                if row.get("bar_status") == "TARGET_PRESENT" and not birth.get(
                    "source_entry_sha256"
                ):
                    safe_rows.append(
                        {
                            **row,
                            "bar_status": "READ_FAILED",
                            "bar_reason": "T0_SOURCE_IDENTITY_UNPROVED",
                        }
                    )
                elif (
                    row["session_date"] != target
                    and row.get("bar_status") == "TARGET_PRESENT"
                    and birth.get("source_entry_sha256")
                    and (
                        row.get("source_entry_sha256") != birth["source_entry_sha256"]
                        or row.get("source_content_id") != birth["source_content_id"]
                    )
                ):
                    mismatches.append(row["session_date"].isoformat())
                    safe_rows.append(
                        {
                            **row,
                            "bar_status": "READ_FAILED",
                            "bar_reason": "SOURCE_IDENTITY_MISMATCH",
                        }
                    )
                else:
                    safe_rows.append(row)
            preview = research_stock_pg_d1_history_previews(
                symbol=symbol,
                event_sessions=(target,),
                followup_session=target,
                session_dates=sessions,
                package_sha256=package_sha256,
                history_rows=safe_rows,
                namespace_id=day["namespace_id"],
                context=context,
            )[target]["raw_d1_breakout_predicate"]
            predicate = preview["predicates"]["result"]
            state = predicate["state"]
            if state not in {"TRUE", "FALSE", "UNKNOWN"}:
                raise SourceObservationError("D1谓词状态无效")
            states[symbol] = {
                "state": state,
                "reasons": predicate.get("dependency_reasons", []),
                "predicate": preview,
                "source_mismatch_sessions": mismatches,
                "source_identity_bound": bool(birth.get("source_entry_sha256")),
            }
        offset = page["next_offset"]
        if offset is None:
            break
    if set(states) != expected:
        raise SourceObservationError("D1与T0来源池代码集合不一致")
    return states


def _summary(symbols, outcomes, weights):
    values = []
    reasons = Counter()
    maturity = Counter()
    path_quality = Counter()
    for symbol in symbols:
        fact = outcomes[symbol]
        weight = weights.get(symbol, 1)
        maturity[fact["maturity"]] += weight
        path_quality[fact["path_quality"]] += weight
        endpoint = fact["endpoint"]["raw_close_relative_change"]
        if endpoint["quality"] == "OBSERVED":
            values.extend([Fraction(endpoint["value"]["exact"])] * weight)
        else:
            reasons[endpoint.get("reason") or fact["maturity"]] += weight
    total = sum(weights.get(symbol, 1) for symbol in symbols)
    positive = sum(value > 0 for value in values)
    return {
        "count": total,
        "evaluable": len(values),
        "unevaluable": total - len(values),
        "positive": positive,
        "non_positive": len(values) - positive,
        "median": str(median(values)) if values else None,
        "unevaluable_reasons": dict(reasons),
        "maturity": dict(maturity),
        "path_quality": dict(path_quality),
        "status": "COMPUTED" if values else "NOT_EVALUABLE",
    }


def _frozen_line_followup(item, facts):
    candidate = item.get("failure_line_candidate", {})
    value = candidate.get("value")
    line = (
        Fraction(int(value["numerator"]), int(value["denominator"])) if value else None
    )
    first = upper = None
    unknown_before = False
    observations = []
    for point in facts["points"]:
        close = point["ohlc"]["close"]
        if line is None or close["quality"] != "OBSERVED":
            state = "UNKNOWN"
            if upper is None:
                unknown_before = True
        else:
            amount = close["value"]
            close_value = Fraction(
                amount["exact"] if isinstance(amount, dict) else amount
            )
            state = "TRUE" if close_value <= line else "FALSE"
            if state == "TRUE" and upper is None:
                upper = point["session"]
                if not unknown_before:
                    first = upper
        observations.append({"session": point["session"], "state": state})
    return {
        "role": "FROZEN_T0_CANDIDATE_DIAGNOSTIC_NOT_FORMAL_INVALIDATION",
        "line": candidate,
        "observations": observations,
        "first_proved_touch": first,
        "first_proved_touch_upper_bound": upper,
        "first_touch_status": "OBSERVED"
        if first
        else "UNKNOWN"
        if unknown_before
        else "NOT_OBSERVED",
    }


def read_source_cohort_forward(
    config: Mapping[str, Any],
    day: Mapping[str, Any],
    d1_discovery: Mapping[str, Any],
    *,
    research_cutoff: date,
    package_sha256: str,
    horizons: tuple[int, ...],
    d1_history_count: int,
    symbol: str | None = None,
    offset: int = 0,
    limit: int = 30,
) -> dict[str, Any]:
    """先冻结全部T0规则，再对同一代码池逐期限描述后验事实。"""
    if not horizons or any(
        type(n) is not int or n not in {1, 3, 5, 10, 20} for n in horizons
    ):
        raise ValueError("期限须从1、3、5、10、20中选择")
    if tuple(sorted(set(horizons))) != horizons or offset < 0 or not 1 <= limit <= 100:
        raise ValueError("期限或分页参数无效")
    target = day["session"]
    if target > research_cutoff:
        raise ValueError("T0不得晚于研究截止日")
    base_states = day["states"]
    symbols = tuple(sorted(base_states))
    source = day["scenarios"]["LAUNCH_CONFIRM"]["source_version"]
    if (
        d1_discovery["source_pool_count"] != len(symbols)
        or d1_discovery["source"]["namespace_id"] != day["namespace_id"]
        or d1_discovery["source"]["source_content_id"] != source["source_content_id"]
        or d1_discovery["source"]["source_capture_id"] != source["source_capture_id"]
    ):
        raise SourceObservationError("D1历史锚与T0来源池不一致")
    details = {
        item["source_symbol"]: item for item in d1_discovery["complete_symbol_details"]
    }
    if set(details) != set(symbols):
        raise SourceObservationError("D1历史锚来源代码集合不一致")
    d1 = _read_t0_d1(config, day, package_sha256, d1_history_count)
    diagnostic_shapes = read_source_shape_cohort(
        config, day, research_cutoff=research_cutoff, package_sha256=package_sha256
    )
    indexes = {
        rule: {item["source_symbol"]: item for item in page["results"]}
        for rule, page in day["scenarios"].items()
    }
    _, _, _, prior_calendar = _load_calendar(target)
    frozen = {}
    for code in symbols:
        rules = {}
        for name, state in base_states[code].items():
            if state not in {"TRUE", "FALSE", "UNKNOWN"}:
                raise SourceObservationError("T0来源规则状态无效")
            if name in indexes:
                reasons = [indexes[name][code].get("main_reason")]
            else:
                reasons = day["seed"]["facts"][code][name].get("reasons", [])
            rules[name] = {
                "state": state,
                "reasons": [reason for reason in reasons if reason],
            }
        rules["D1_T0"] = {
            key: d1[code][key]
            for key in ("state", "reasons", "source_mismatch_sessions")
        }
        anchors = []
        unknowns = list(details[code]["raw_d1_unknown_sessions"])
        mismatch_dates = set(d1[code]["source_mismatch_sessions"])
        for anchor in details[code]["anchor_events"]:
            anchor_day = date.fromisoformat(anchor["support_anchor_session"])
            anchor_index = prior_calendar.index(anchor_day)
            dependency_days = {
                session.isoformat()
                for session in prior_calendar[
                    max(0, anchor_index - 20) : anchor_index + 1
                ]
            }
            identity = anchor.get("source", {})
            compatible = (
                d1[code]["source_identity_bound"]
                and identity.get("anchor_entry_sha256")
                and identity.get("anchor_entry_sha256")
                == identity.get("target_entry_sha256")
                and identity.get("source_content_id") == source["source_content_id"]
                and not (dependency_days & mismatch_dates)
            )
            if compatible:
                anchors.append(anchor)
            else:
                unknowns.append(
                    {
                        "session": anchor_day.isoformat(),
                        "dependency_reasons": ["ANCHOR_SOURCE_IDENTITY_UNPROVED"],
                    }
                )
        if not d1[code]["source_identity_bound"]:
            unknowns.append(
                {
                    "session": target.isoformat(),
                    "dependency_reasons": ["T0_SOURCE_IDENTITY_UNPROVED"],
                }
            )
        rules["D1_ANCHOR_WINDOW"] = {
            "state": "TRUE" if anchors else "UNKNOWN" if unknowns else "FALSE",
            "reasons": ["ANCHOR_WINDOW_HAS_UNKNOWN"]
            if unknowns and not anchors
            else [],
        }
        shape_fact = diagnostic_shapes["facts"][code]
        for rule in ("STRONG_PULLBACK_DIAGNOSTIC", "TREND_CONTINUE_DIAGNOSTIC"):
            rules[rule] = shape_fact[rule]
        frozen[code] = {
            "rules": rules,
            "anchors": anchors,
            "d1_anchor_unknown_sessions": unknowns,
            "d1_t0_predicate": d1[code]["predicate"],
            "diagnostic_shape_summary": shape_fact,
        }
    shape = None
    if symbol in frozen:
        diagnostic = query_source_d0_raw_launch(
            config,
            scenario="RECOVERY_TURN",
            target_session=target,
            research_cutoff=research_cutoff,
            package_sha256=package_sha256,
            symbol=symbol,
            limit=1,
            expected_namespace_id=day["namespace_id"],
            _include_shape_observation=True,
        )
        if (
            len(diagnostic["results"]) != 1
            or diagnostic["source_version"]["source_capture_id"]
            != source["source_capture_id"]
            or diagnostic["source_version"]["source_content_id"]
            != source["source_content_id"]
        ):
            raise SourceObservationError("T0诊断形态与冻结来源捕获不一致")
        shape = diagnostic["results"][0]["prior_shape_observation"]
    # 此处以后才读取后继，不能反向改变frozen。
    _, _, calendar_revision, calendar = _load_calendar(research_cutoff)
    if target not in calendar:
        raise ValueError("T0不在当前官方日历中")
    start = calendar.index(target)
    available = tuple(
        day
        for day in calendar[start : start + max(horizons) + 1]
        if day <= research_cutoff
    )
    if shape is not None and len(available) > 1:
        attach_shape_state_followup(
            config,
            diagnostic,
            target=target,
            followup=available[min(10, len(available) - 1)],
            cutoff=research_cutoff,
            package_sha256=package_sha256,
        )
        shape["rolling_followup_limit"] = "最多10个官方会话；逐日峰值滚动重算"
    forward_pool = read_raw_daily_forward_pool(
        config,
        source_symbols=symbols,
        sessions=available,
        research_cutoff=research_cutoff,
        package_sha256=package_sha256,
        expected_namespace_id=day["namespace_id"],
    )
    outcomes = {}
    for code in symbols:
        rows = forward_pool["rows_by_symbol"][code]
        birth = next((row for row in rows if row["session_date"] == target), None)
        if birth is None or birth["source_capture_id"] != source["source_capture_id"]:
            raise SourceObservationError("后继读取与冻结T0捕获不一致")
        outcomes[code] = {}
        for n in horizons:
            full_target = calendar[start + n] if start + n < len(calendar) else None
            window = available[: n + 1]
            window_set = set(window)
            outcomes[code][n] = exploratory_raw_path(
                window,
                [row for row in rows if row["session_date"] in window_set],
                n,
                target_session=full_target,
                saved_through=forward_pool["saved_through"],
            )
    groups = []
    rules = tuple(next(iter(frozen.values()))["rules"])
    for rule in rules:
        for raw_state, label in (
            ("TRUE", "MATCH"),
            ("FALSE", "NO_MATCH"),
            ("UNKNOWN", "UNKNOWN"),
        ):
            members = [
                code
                for code in symbols
                if frozen[code]["rules"][rule]["state"] == raw_state
            ]
            for n in horizons:
                per_stock = {code: outcomes[code][n] for code in members}
                if rule == "D1_ANCHOR_WINDOW" and raw_state == "TRUE":
                    weights = {code: len(frozen[code]["anchors"]) for code in members}
                elif rule == "STRONG_PULLBACK_DIAGNOSTIC" and raw_state == "TRUE":
                    weights = {
                        code: frozen[code]["diagnostic_shape_summary"][
                            "matched_parent_count"
                        ]
                        for code in members
                    }
                else:
                    weights = {}
                groups.append(
                    {
                        "rule": rule,
                        "group": label,
                        "horizon": n,
                        "event_weighted": _summary(members, per_stock, weights),
                        "stock_weighted": _summary(members, per_stock, {}),
                    }
                )
    selected = [code for code in symbols if symbol is None or code == symbol]
    page = []
    for code in selected[offset : offset + limit]:
        followups = {
            rule: _frozen_line_followup(
                indexes[rule][code], outcomes[code][max(horizons)]
            )
            for rule in indexes
            if frozen[code]["rules"][rule]["state"] == "TRUE"
        }
        frozen_shapes = []
        if shape is not None and code == symbol:
            for parent in shape["diagnostic_parent_candidates"]:
                frozen_shapes.append(
                    {
                        "birth_session": parent["birth_session"],
                        "birth_scenario": parent["birth_scenario"],
                        "t0_peak_session": parent.get("peak_session"),
                        "t0_peak_high": parent.get("peak_high"),
                        "t0_source_shape_state": parent.get(
                            "source_shape_state", "UNKNOWN"
                        ),
                        "frozen_line_followup": _frozen_line_followup(
                            {
                                "failure_line_candidate": parent.get(
                                    "diagnostic_failure_line", {}
                                )
                            },
                            outcomes[code][max(horizons)],
                        ),
                    }
                )
        page.append(
            {
                "source_symbol": code,
                **frozen[code],
                "forward": outcomes[code],
                "frozen_candidate_followup": followups,
                "t0_prior_shape_observation": shape if code == symbol else None,
                "frozen_diagnostic_parent_followup": frozen_shapes,
            }
        )
    partitions = {}
    for rule in rules:
        counts = Counter(frozen[code]["rules"][rule]["state"] for code in symbols)
        reasons = Counter(
            reason
            for code in symbols
            if frozen[code]["rules"][rule]["state"] == "UNKNOWN"
            for reason in frozen[code]["rules"][rule]["reasons"]
        )
        partitions[rule] = {
            "n_source": len(symbols),
            "calculated": counts["TRUE"] + counts["FALSE"],
            "uncalculated": counts["UNKNOWN"],
            "match": counts["TRUE"],
            "no_match": counts["FALSE"],
            "unknown": counts["UNKNOWN"],
            "unknown_reasons": dict(reasons),
            "partition_closed": sum(counts.values()) == len(symbols),
        }
    matched_rule_counts = [
        sum(value["state"] == "TRUE" for value in frozen[code]["rules"].values())
        for code in symbols
    ]
    source_classes = Counter(
        (
            indexes["LAUNCH_CONFIRM"][code].get("market"),
            indexes["LAUNCH_CONFIRM"][code].get("board"),
        )
        for code in symbols
    )
    return {
        "ok": True,
        "scope": "NON_PIT_EXPLORATORY",
        "target_session": target.isoformat(),
        "research_cutoff": research_cutoff.isoformat(),
        "source_version": source,
        "calendar_revision": calendar_revision,
        "source_saved_through": forward_pool["saved_through"],
        "forward_session_versions": forward_pool["session_versions"],
        "source_pool_count": len(symbols),
        "formal_eligibility": "UNKNOWN",
        "formal_episode": "NOT_CREATED",
        "diagnostic_shape_scope": "全来源池诊断形态，不混入正式D0；指定代码可读取冻结父峰明细",
        "diagnostic_shape_historical_rps_days": diagnostic_shapes[
            "historical_rps_days"
        ],
        "diagnostic_shape_target_rps_error": diagnostic_shapes["target_rps_error"],
        "diagnostic_shape_historical_rps_errors": diagnostic_shapes[
            "historical_rps_errors"
        ],
        "selection": "T0完整来源池先冻结；后继价格和成熟状态不参与入组",
        "universe": "来源代码池；沪深主板及创业板为研究目标，不剔除ST。来源分类非权威，历史上市及板别资格未知，非正式Universe",
        "source_classification_counts": [
            {"market": market, "board": board, "count": count}
            for (market, board), count in sorted(
                source_classes.items(), key=lambda item: str(item[0])
            )
        ],
        "d1_anchor_window": {
            "event_count": sum(len(frozen[code]["anchors"]) for code in symbols),
            "distinct_stock_count": sum(
                bool(frozen[code]["anchors"]) for code in symbols
            ),
            "matched_stocks_with_unknown_sessions": sum(
                bool(frozen[code]["anchors"])
                and bool(frozen[code]["d1_anchor_unknown_sessions"])
                for code in symbols
            ),
            "window_start": d1_discovery["event_window_start"],
            "window_end_exclusive": d1_discovery["event_window_end_exclusive"],
        },
        "partitions": partitions,
        "groups": groups,
        "overlap": "同一规则三态互斥；不同规则可重复匹配。D1历史锚按事件计权并另给同日股票去重。",
        "cross_rule_overlap": {
            "distinct_source_symbols_matching_any_rule": sum(
                n > 0 for n in matched_rule_counts
            ),
            "distinct_source_symbols_matching_multiple_rules": sum(
                n > 1 for n in matched_rule_counts
            ),
            "sum_of_rule_stock_matches": sum(matched_rule_counts),
        },
        "page": {
            "offset": offset,
            "limit": limit,
            "selected_count": len(selected),
            "next_offset": offset + limit if offset + limit < len(selected) else None,
        },
        "results": page,
        "database_transaction": "READ_ONLY",
        "database_write_attempted": False,
    }
