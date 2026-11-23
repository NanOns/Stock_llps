"""冻结Cohort的完整统计和有界规则事实只读投影。"""

from fractions import Fraction

from stock_a.storage.postgres.research_facts import (
    PATH_VERSION,
    RULE_VERSION,
    ResearchFactConflict,
    ResearchFactNotFound,
    _read_research_rows_cursor,
)
from stock_a.storage.postgres.source_raw import _transaction

_MINIMUM_INDEPENDENT_T0_DATES = 3


def read_cohort_page(
    config,
    *,
    target,
    research_cutoff,
    source_capture_id,
    profile_revision,
    history_session_count,
    d1_history_session_count,
    event_window_start,
    calendar_revision,
    rule,
    horizon,
    offset,
    limit,
    symbol,
    rps_board_kind="ALL",
    rps_scenario="ALL",
    rps_rule_state="ALL",
    include_rps_pulse_groups=False,
):
    """同一只读快照读取六组完整统计和最多一页的明细。"""
    with _transaction(config, write=False, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT source_capture_id,state,weighting,source_pool_count,member_count,
                      evaluable_count,positive_count,median_numerator,median_denominator,
                      maturity_codes,maturity_counts,path_quality_codes,path_quality_counts,
                      reason_codes,reason_counts
               FROM source_cohort_group_stats
               WHERE target_session=%s AND research_cutoff=%s AND source_capture_id=%s
                 AND rule_id=%s AND rule_version=%s AND profile_revision=%s
                 AND history_session_count=%s AND d1_history_session_count=%s
                 AND event_window_start=%s AND calendar_revision=%s
                 AND path_rule_version=%s AND horizon=%s
               ORDER BY source_capture_id,state,weighting LIMIT 13""",
            (
                target,
                research_cutoff,
                source_capture_id,
                rule,
                RULE_VERSION,
                profile_revision,
                history_session_count,
                d1_history_session_count,
                event_window_start,
                calendar_revision,
                PATH_VERSION,
                horizon,
            ),
        )
        stored = cursor.fetchall()
        if not stored:
            raise ResearchFactNotFound("精确版本的离线Cohort统计尚未计算")
        captures = {row[0] for row in stored}
        pairs = {(row[1], row[2]) for row in stored}
        expected = {
            (state, weight)
            for state in ("TRUE", "FALSE", "UNKNOWN")
            for weight in ("stock", "event")
        }
        counts = {row[3] for row in stored}
        if (
            len(stored) != 6
            or len(captures) != 1
            or pairs != expected
            or len(counts) != 1
        ):
            raise ResearchFactConflict("Cohort统计不完整或包含多个来源捕获")
        total = next(iter(counts))
        if sum(row[4] for row in stored if row[2] == "stock") != total:
            raise ResearchFactConflict("Cohort三态代码分母不一致")
        groups = []
        for row in stored:
            (
                _,
                state,
                weight,
                _,
                count,
                evaluable,
                positive,
                numerator,
                denominator,
                *arrays,
            ) = row
            if any(len(arrays[i]) != len(arrays[i + 1]) for i in (0, 2, 4)):
                raise ResearchFactConflict("Cohort统计质量分组不完整")
            groups.append(
                {
                    "group": {
                        "TRUE": "MATCH",
                        "FALSE": "NO_MATCH",
                        "UNKNOWN": "UNKNOWN",
                    }[state],
                    "weighting": weight,
                    "count": count,
                    "evaluable": evaluable,
                    "unknown": count - evaluable,
                    "positive": positive,
                    "non_positive": evaluable - positive,
                    "median": str(Fraction(int(numerator), int(denominator)))
                    if numerator is not None
                    else None,
                    "maturity": dict(zip(arrays[0], arrays[1])),
                    "path_quality": dict(zip(arrays[2], arrays[3])),
                    "unevaluable_reasons": dict(zip(arrays[4], arrays[5])),
                }
            )
        groups.sort(
            key=lambda x: (
                ("MATCH", "NO_MATCH", "UNKNOWN").index(x["group"]),
                ("stock", "event").index(x["weighting"]),
            )
        )
        rows = _read_research_rows_cursor(
            cursor,
            target=target,
            source_capture_id=source_capture_id,
            profile_revision=profile_revision,
            history_session_count=history_session_count,
            d1_history_session_count=d1_history_session_count,
            event_window_start=event_window_start,
            calendar_revision=calendar_revision,
            rule=rule,
            horizon=horizon,
            research_cutoff=research_cutoff,
            symbol=symbol,
            offset=0 if symbol else offset,
            limit=limit,
        )
        if any(row[21] != next(iter(captures)) for row in rows):
            raise ResearchFactConflict("规则明细和Cohort统计的来源捕获不一致")
        rules = []
        if symbol:
            cursor.execute(
                """SELECT r.id,r.rule_id,r.state,r.reasons,e.id,e.enrollment_kind,
                          e.reason_code,e.parent_enrollment_id
                   FROM source_rule_evaluations r
                   LEFT JOIN source_research_enrollments e ON e.rule_fact_id=r.id
                   WHERE r.target_session=%s AND r.source_capture_id=%s
                     AND r.source_symbol=%s
                     AND r.rule_version=%s AND r.profile_revision=%s
                     AND r.history_session_count=%s AND r.d1_history_session_count=%s
                     AND r.event_window_start=%s AND r.calendar_revision=%s
                   ORDER BY r.rule_id LIMIT 10""",
                (
                    target,
                    source_capture_id,
                    symbol,
                    RULE_VERSION,
                    profile_revision,
                    history_session_count,
                    d1_history_session_count,
                    event_window_start,
                    calendar_revision,
                ),
            )
            rules = [
                {
                    "fact_id": fact_id,
                    "rule_id": rule_id,
                    "state": state,
                    "reasons": reasons,
                    "enrollment": (
                        {
                            "id": enrollment_id,
                            "kind": kind,
                            "reason": reason,
                            "parent_enrollment_id": parent_id,
                        }
                        if enrollment_id is not None
                        else None
                    ),
                }
                for fact_id, rule_id, state, reasons, enrollment_id, kind, reason, parent_id in cursor.fetchall()
            ]
            if len(rules) > 9:
                raise ResearchFactConflict("所选代码的规则事实超过现行九规则范围")
        rps_pulse_groups = None
        if include_rps_pulse_groups:
            from stock_a.storage.postgres.source_rps_pulse_groups import (
                read_rps_pulse_forward_groups_cursor,
            )

            rps_pulse_groups = read_rps_pulse_forward_groups_cursor(
                cursor,
                target=target,
                research_cutoff=research_cutoff,
                source_capture_id=next(iter(captures)),
                profile_revision=profile_revision,
                history_session_count=history_session_count,
                d1_history_session_count=d1_history_session_count,
                event_window_start=event_window_start,
                calendar_revision=calendar_revision,
                rule_id=rule,
                board_kind=rps_board_kind,
                scenario_id=rps_scenario,
                rule_state_filter=rps_rule_state,
            )
        return total, groups, rows, rules, rps_pulse_groups


def read_cohort_date_comparison(
    config,
    *,
    research_cutoff,
    source_capture_id,
    rule,
    horizon,
    limit=20,
):
    """只读最多20个已保存T0日的冻结三态Cohort逐日统计。"""
    from stock_a.storage.postgres.research_facts import RULES

    if (
        rule not in RULES
        or horizon not in (1, 3, 5)
        or type(limit) is not int
        or not 1 <= limit <= 20
        or type(source_capture_id) is not int
        or source_capture_id <= 0
    ):
        raise ValueError("Cohort跨日读取的规则、期限、分页或来源包参数无效")

    with _transaction(config, write=False, statement_timeout_ms=15000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """WITH selected_sessions AS (
                   SELECT DISTINCT target_session
                   FROM source_cohort_group_stats
                   WHERE research_cutoff=%s AND source_capture_id=%s
                     AND rule_id=%s AND rule_version=%s
                     AND path_rule_version=%s AND horizon=%s
                   ORDER BY target_session DESC LIMIT %s
               )
               SELECT s.target_session,s.source_capture_id,s.profile_revision,
                      s.history_session_count,s.d1_history_session_count,
                      s.event_window_start,s.calendar_revision,s.state,s.weighting,
                      s.source_pool_count,s.member_count,s.evaluable_count,
                      s.positive_count,s.median_numerator,s.median_denominator,
                      s.maturity_codes,s.maturity_counts,s.path_quality_codes,
                      s.path_quality_counts,s.reason_codes,s.reason_counts
               FROM source_cohort_group_stats s
               JOIN selected_sessions x USING (target_session)
               WHERE s.research_cutoff=%s AND s.source_capture_id=%s
                 AND s.rule_id=%s AND s.rule_version=%s
                 AND s.path_rule_version=%s AND s.horizon=%s
               ORDER BY s.target_session DESC,s.source_capture_id,
                        s.profile_revision,s.history_session_count,
                        s.d1_history_session_count,s.event_window_start,
                        s.calendar_revision,s.state,s.weighting""",
            (
                research_cutoff,
                source_capture_id,
                rule,
                RULE_VERSION,
                PATH_VERSION,
                horizon,
                limit,
                research_cutoff,
                source_capture_id,
                rule,
                RULE_VERSION,
                PATH_VERSION,
                horizon,
            ),
        )
        stored = cursor.fetchall()

    by_session = {}
    for row in stored:
        (
            session,
            capture,
            profile,
            history,
            d1_history,
            window,
            calendar,
            state,
            weighting,
            source_pool_count,
            member_count,
            evaluable_count,
            positive_count,
            median_numerator,
            median_denominator,
            maturity_codes,
            maturity_counts,
            path_quality_codes,
            path_quality_counts,
            reason_codes,
            reason_counts,
        ) = row
        scope = (
            capture,
            profile,
            history,
            d1_history,
            window,
            calendar,
        )
        scope_rows = by_session.setdefault(session, {}).setdefault(scope, [])
        arrays_valid = (
            len(maturity_codes) == len(maturity_counts)
            and len(path_quality_codes) == len(path_quality_counts)
            and len(reason_codes) == len(reason_counts)
        )
        scope_rows.append(
            {
                "state": state,
                "weighting": weighting,
                "source_pool_count": source_pool_count,
                "member_count": member_count,
                "evaluable_count": evaluable_count,
                "positive_count": positive_count,
                "median": (
                    str(Fraction(int(median_numerator), int(median_denominator)))
                    if median_numerator is not None
                    else None
                ),
                "maturity": dict(zip(maturity_codes, maturity_counts)),
                "path_quality": dict(zip(path_quality_codes, path_quality_counts)),
                "unevaluable_reasons": dict(zip(reason_codes, reason_counts)),
                "arrays_valid": arrays_valid,
            }
        )

    dates = []
    expected_pairs = {
        (state, weighting)
        for state in ("TRUE", "FALSE", "UNKNOWN")
        for weighting in ("stock", "event")
    }
    for session, scopes in sorted(by_session.items(), reverse=True):
        if len(scopes) != 1:
            dates.append(
                {
                    "target_session": session.isoformat(),
                    "status": "AMBIGUOUS",
                    "reason_codes": ["MULTIPLE_STORED_T0_SCOPES"],
                    "scope_variant_count": len(scopes),
                    "groups": [],
                }
            )
            continue
        scope, rows = next(iter(scopes.items()))
        capture, profile, history, d1_history, window, calendar = scope
        pairs = {(row["state"], row["weighting"]) for row in rows}
        totals = {row["source_pool_count"] for row in rows}
        total = next(iter(totals)) if len(totals) == 1 else None
        stock_rows = [row for row in rows if row["weighting"] == "stock"]
        complete = (
            len(rows) == 6
            and pairs == expected_pairs
            and len(totals) == 1
            and total is not None
            and sum(row["member_count"] for row in stock_rows) == total
            and all(row["arrays_valid"] for row in rows)
        )
        if not complete:
            dates.append(
                {
                    "target_session": session.isoformat(),
                    "status": "INCOMPLETE",
                    "reason_codes": ["COHORT_GROUP_SCOPE_NOT_CLOSED"],
                    "scope_variant_count": 1,
                    "source_pool_count": total,
                    "groups": [],
                }
            )
            continue

        not_due_count = sum(row["maturity"].get("NOT_DUE", 0) for row in stock_rows)
        due = not_due_count != total
        groups = []
        for row in sorted(
            stock_rows,
            key=lambda item: ("TRUE", "FALSE", "UNKNOWN").index(item["state"]),
        ):
            state = row["state"]
            groups.append(
                {
                    "group": {
                        "TRUE": "MATCH",
                        "FALSE": "NO_MATCH",
                        "UNKNOWN": "UNKNOWN",
                    }[state],
                    "member_count": row["member_count"],
                    "evaluable_count": row["evaluable_count"],
                    "unknown_count": row["member_count"] - row["evaluable_count"],
                    "positive_count": row["positive_count"],
                    "non_positive_count": row["evaluable_count"]
                    - row["positive_count"],
                    "median": row["median"],
                    "maturity": row["maturity"],
                    "path_quality": row["path_quality"],
                    "unevaluable_reasons": row["unevaluable_reasons"],
                }
            )
        dates.append(
            {
                "target_session": session.isoformat(),
                "status": "AVAILABLE" if due else "NOT_DUE",
                "reason_codes": [] if due else ["WAITING_FOR_FUTURE_OFFICIAL_SESSION"],
                "scope_variant_count": 1,
                "source_capture_id": capture,
                "source_pool_count": total,
                "profile_revision": profile,
                "history_session_count": history,
                "d1_history_session_count": d1_history,
                "event_window_start": window.isoformat(),
                "calendar_revision": calendar,
                "groups": groups,
            }
        )

    due_date_count = sum(item["status"] == "AVAILABLE" for item in dates)
    from stock_a.storage.postgres.cohort_enrollment import (
        read_cohort_enrollment_datewise_comparison,
    )

    enrollment_datewise = read_cohort_enrollment_datewise_comparison(
        config,
        dates=dates,
        research_cutoff=research_cutoff,
        source_capture_id=source_capture_id,
        rule=rule,
    )
    return {
        "status": "AVAILABLE" if dates else "NO_STORED_FACT",
        "scope": "NON_PIT_EXPLORATORY",
        "price_coordinate": "FORWARD_ADJUSTED",
        "source_capture_id": source_capture_id,
        "rule": rule,
        "rule_version": RULE_VERSION,
        "horizon": horizon,
        "path_rule_version": PATH_VERSION,
        "research_cutoff": research_cutoff.isoformat(),
        "independent_t0_date_count": due_date_count,
        "comparison_status": (
            "INSUFFICIENT_INDEPENDENT_DATES"
            if due_date_count < _MINIMUM_INDEPENDENT_T0_DATES
            else "DESCRIPTIVE_BY_T0_DATE"
        ),
        "aggregation": "NO_CROSS_DATE_AGGREGATE; each eligible T0 session is one descriptive row",
        "dates": dates,
        "enrollment_datewise": enrollment_datewise,
    }


def read_cohort_symbol_forward_paths(
    config,
    *,
    target,
    research_cutoff,
    source_capture_id,
    profile_revision,
    history_session_count,
    d1_history_session_count,
    event_window_start,
    rule,
    symbol,
    horizon_due_sessions,
):
    """从PG按精确规则T0键读取单股T+1/T+3/T+5路径，不重算价格。"""
    from stock_a.research.entry_context.sessions import _load_calendar
    from stock_a.research.source_forward_path import (
        forward_calendar_revision_compatible,
    )
    from stock_a.storage.postgres.research_facts import RULES

    if (
        target > research_cutoff
        or rule not in RULES
        or not isinstance(symbol, str)
        or len(symbol) != 9
        or symbol[2] != "."
        or symbol[:2] not in {"SH", "SZ"}
        or not symbol[3:].isdecimal()
        or len(horizon_due_sessions) > 20
        or any(
            type(day) is not type(target) or day <= target
            for day in horizon_due_sessions
        )
        or tuple(sorted(set(horizon_due_sessions))) != tuple(horizon_due_sessions)
    ):
        raise ValueError("单股多期限读取范围或规则键无效")
    _, _, official_revision_chain, _ = _load_calendar(research_cutoff)

    due_by_horizon = {
        horizon: (
            horizon_due_sessions[horizon - 1]
            if len(horizon_due_sessions) >= horizon
            else None
        )
        for horizon in (1, 3, 5)
    }
    due_horizons = [
        horizon
        for horizon, due_session in due_by_horizon.items()
        if due_session is not None and due_session <= research_cutoff
    ]
    with _transaction(config, write=False, statement_timeout_ms=10000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT id,source_capture_id,calendar_revision,scope,state
               FROM source_rule_evaluations
               WHERE target_session=%s AND source_capture_id=%s
                 AND source_symbol=%s AND rule_id=%s AND rule_version=%s
                 AND profile_revision=%s AND history_session_count=%s
                 AND d1_history_session_count=%s AND event_window_start=%s
               ORDER BY source_capture_id,id LIMIT 2""",
            (
                target,
                source_capture_id,
                symbol,
                rule,
                RULE_VERSION,
                profile_revision,
                history_session_count,
                d1_history_session_count,
                event_window_start,
            ),
        )
        rule_rows = cursor.fetchall()
        if len(rule_rows) != 1:
            return {
                "status": "NO_STORED_FACT" if not rule_rows else "AMBIGUOUS",
                "target_session": target.isoformat(),
                "source_capture_id": source_capture_id,
                "source_symbol": symbol,
                "rule": rule,
                "paths": [],
                "reason_codes": [
                    "NO_EXACT_T0_RULE_FACT"
                    if not rule_rows
                    else "MULTIPLE_T0_RULE_FACTS"
                ],
            }
        fact_id, capture_id, calendar_revision, scope, state = rule_rows[0]
        if capture_id != source_capture_id:
            raise ResearchFactConflict("规则事实来源捕获与请求捕获不一致")
        stored_paths = []
        if due_horizons:
            cursor.execute(
                """SELECT horizon,path_rule_version,calendar_revision,endpoint_session,
                          maturity,endpoint_quality,endpoint_reason,endpoint_numerator,
                          endpoint_denominator,path_quality,path_reasons,price_coordinate,scope
                   FROM source_forward_path_facts
                   WHERE target_session=%s AND source_capture_id=%s
                     AND source_symbol=%s AND horizon=ANY(%s)
                   ORDER BY horizon,path_rule_version,calendar_revision""",
                (target, capture_id, symbol, due_horizons),
            )
            stored_paths = cursor.fetchall()

    paths = []
    for horizon in (1, 3, 5):
        due_session = due_by_horizon[horizon]
        item = {
            "horizon": horizon,
            "due_session": due_session.isoformat() if due_session else None,
            "status": "UNKNOWN",
            "reason_codes": [],
            "maturity": None,
            "endpoint_quality": "UNKNOWN",
            "endpoint_reason": None,
            "endpoint_return": None,
            "endpoint_numerator": None,
            "endpoint_denominator": None,
            "path_quality": "UNKNOWN",
            "path_reasons": [],
            "path_rule_version": PATH_VERSION,
            "calendar_revision": calendar_revision,
            "price_coordinate": "FORWARD_ADJUSTED",
            "scope": "NON_PIT_EXPLORATORY",
        }
        if due_session is None:
            item["reason_codes"] = ["OFFICIAL_HORIZON_SESSION_UNAVAILABLE"]
        elif due_session > research_cutoff:
            item.update(
                status="NOT_DUE",
                reason_codes=["WAITING_FOR_FUTURE_OFFICIAL_SESSION"],
            )
        else:
            candidates = [row for row in stored_paths if row[0] == horizon]
            exact = [
                row
                for row in candidates
                if row[1] == PATH_VERSION
                and row[3] == due_session
                and row[11] == "FORWARD_ADJUSTED"
                and row[12] == "NON_PIT_EXPLORATORY"
                and forward_calendar_revision_compatible(
                    target,
                    row[3],
                    rule_revision=calendar_revision,
                    path_revision=row[2],
                    official_revision_chain=official_revision_chain,
                )
            ]
            if len(exact) > 1:
                item.update(
                    status="INCOMPLETE",
                    reason_codes=["FORWARD_PATH_KEY_AMBIGUOUS"],
                )
            elif not exact:
                item.update(
                    status="NOT_COMPUTED",
                    reason_codes=[
                        "FORWARD_PATH_VERSION_OR_CALENDAR_MISMATCH"
                        if candidates
                        else "FORWARD_PATH_FACT_NOT_SAVED"
                    ],
                )
            else:
                (
                    _horizon,
                    path_version,
                    path_calendar,
                    endpoint_session,
                    maturity,
                    endpoint_quality,
                    endpoint_reason,
                    numerator,
                    denominator,
                    path_quality,
                    path_reasons,
                    coordinate,
                    path_scope,
                ) = exact[0]
                reason_codes = [
                    item for item in (endpoint_reason, *(path_reasons or [])) if item
                ]
                valid = (
                    endpoint_session == due_session
                    and coordinate == "FORWARD_ADJUSTED"
                    and path_scope == "NON_PIT_EXPLORATORY"
                )
                observed = (
                    valid
                    and maturity == "ENDPOINT_OBSERVED_NON_PIT"
                    and endpoint_quality == "OBSERVED"
                    and numerator is not None
                    and denominator is not None
                    and denominator > 0
                )
                item.update(
                    status="OBSERVED" if observed else "UNKNOWN",
                    reason_codes=(
                        list(dict.fromkeys(reason_codes))
                        if valid
                        else ["FORWARD_PATH_SCOPE_MISMATCH"]
                    ),
                    maturity=maturity,
                    endpoint_quality=endpoint_quality,
                    endpoint_reason=endpoint_reason,
                    endpoint_numerator=str(numerator)
                    if numerator is not None
                    else None,
                    endpoint_denominator=str(denominator)
                    if denominator is not None
                    else None,
                    endpoint_return=(
                        str(Fraction(int(numerator), int(denominator)))
                        if observed
                        else None
                    ),
                    path_quality=path_quality,
                    path_reasons=list(path_reasons or []),
                    path_rule_version=path_version,
                    calendar_revision=path_calendar,
                    price_coordinate=coordinate,
                    scope=path_scope,
                )
                if not observed and not item["reason_codes"]:
                    item["reason_codes"] = ["FORWARD_ENDPOINT_NOT_OBSERVED"]
        paths.append(item)

    return {
        "status": "AVAILABLE",
        "target_session": target.isoformat(),
        "research_cutoff": research_cutoff.isoformat(),
        "source_capture_id": source_capture_id,
        "source_symbol": symbol,
        "rule": rule,
        "rule_fact_id": fact_id,
        "rule_state": state,
        "rule_scope": scope,
        "rule_version": RULE_VERSION,
        "calendar_revision": calendar_revision,
        "price_coordinate": "FORWARD_ADJUSTED",
        "scope": "NON_PIT_EXPLORATORY",
        "paths": paths,
    }
