"""Focus/Cohort读取来源登记类别与已保存后继结果。"""

from __future__ import annotations

from collections import Counter
from datetime import date
from fractions import Fraction
from statistics import median
from typing import Any

from stock_a.storage.postgres.research_facts import (
    PATH_VERSION,
    RULES,
    RULE_VERSION,
    SCOPE,
    ResearchFactConflict,
)
from stock_a.storage.postgres.source_raw import (
    _page_version_status,
    _transaction,
)

_KINDS = (
    "ENTRY",
    "REENTRY",
    "FIRST_OBSERVED_LEFT_CENSORED",
    "DAILY_LANDMARK",
)
_HORIZONS = (1, 3, 5)
_ACTIVE_KINDS = (
    "ENTRY",
    "FIRST_OBSERVED_LEFT_CENSORED",
    "DAILY_LANDMARK",
    "REENTRY",
)


def read_cohort_enrollment_lifecycle_groups(
    config,
    *,
    target: date,
    research_cutoff: date,
    source_package_id: int,
    profile_revision: str,
    history_session_count: int,
    d1_history_session_count: int,
    event_window_start: date,
    calendar_revision: str,
    rule: str,
    horizon_sessions: tuple[date, ...],
    previous_session: date | None,
) -> dict[str, Any]:
    """在一个精确T0×规则内分组，GET只读PG且只读截止内会话。"""
    if (
        type(target) is not date
        or type(research_cutoff) is not date
        or target > research_cutoff
        or type(source_package_id) is not int
        or source_package_id <= 0
        or rule not in RULES
        or not isinstance(profile_revision, str)
        or not profile_revision
        or type(history_session_count) is not int
        or type(d1_history_session_count) is not int
        or type(event_window_start) is not date
        or not isinstance(calendar_revision, str)
        or not calendar_revision
        or len(horizon_sessions) > 5
        or tuple(sorted(set(horizon_sessions))) != horizon_sessions
        or any(
            type(session) is not date or session <= target or session > research_cutoff
            for session in horizon_sessions
        )
        or (
            previous_session is not None
            and (
                type(previous_session) is not date
                or previous_session >= target
                or previous_session > research_cutoff
            )
        )
    ):
        raise ValueError("来源登记分组的T0、版本或有界后继范围无效")

    key = (
        target,
        source_package_id,
        rule,
        RULE_VERSION,
        profile_revision,
        history_session_count,
        d1_history_session_count,
        event_window_start,
        calendar_revision,
        SCOPE,
    )
    registered: dict[str, list[dict[str, Any]]] = {kind: [] for kind in _KINDS}
    with _transaction(config, write=False, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT count(*)::bigint,count(DISTINCT source_symbol)::bigint,
                      count(DISTINCT source_capture_id)::bigint,
                      min(source_capture_id),max(source_capture_id)
               FROM source_rule_evaluations
               WHERE target_session=%s AND source_package_id=%s AND rule_id=%s
                 AND rule_version=%s AND profile_revision=%s
                 AND history_session_count=%s AND d1_history_session_count=%s
                 AND event_window_start=%s AND calendar_revision=%s AND scope=%s""",
            key,
        )
        pool_count, symbol_count, capture_count, capture_min, capture_max = (
            cursor.fetchone()
        )
        pool_count = int(pool_count)
        if pool_count == 0:
            return {
                "status": "NO_STORED_FACT",
                "target_session": target.isoformat(),
                "rule": rule,
                "source_pool_count": 0,
                "groups": [],
                "reason_codes": ["NO_EXACT_T0_RULE_FACT"],
                "database_transaction": "READ_ONLY",
                "database_write_attempted": False,
            }
        if (
            symbol_count != pool_count
            or capture_count != 1
            or capture_min != capture_max
        ):
            raise ResearchFactConflict("同一T0规则键不是唯一捕获或来源代码分母不闭合")
        source_capture_id = int(capture_min)

        cursor.execute(
            """SELECT r.state,count(*)::bigint,count(e.id)::bigint
               FROM source_rule_evaluations r
               LEFT JOIN source_research_enrollments e ON e.rule_fact_id=r.id
               WHERE r.target_session=%s AND r.source_package_id=%s AND r.rule_id=%s
                 AND r.rule_version=%s AND r.profile_revision=%s
                 AND r.history_session_count=%s AND r.d1_history_session_count=%s
                 AND r.event_window_start=%s AND r.calendar_revision=%s AND r.scope=%s
               GROUP BY r.state ORDER BY r.state""",
            key,
        )
        state_rows = cursor.fetchall()
        state_counts = {state: int(count) for state, count, _ in state_rows}
        enrolled_by_state = {state: int(enrolled) for state, _, enrolled in state_rows}
        if sum(state_counts.values()) != pool_count:
            raise ResearchFactConflict("规则三态分母与精确来源池不一致")

        cursor.execute(
            """SELECT e.id,r.source_symbol,e.enrollment_kind,e.reason_code,
                      e.parent_enrollment_id,e.previous_rule_fact_id,r.state,
                      r.source_capture_id,parent_e.id,parent_e.enrollment_kind,
                      parent_r.target_session,parent_r.source_symbol,parent_r.rule_id,
                      parent_r.state,parent_r.rule_version,parent_r.source_package_id,
                      previous_r.target_session,previous_r.source_symbol,
                      previous_r.rule_id,previous_r.state,previous_r.rule_version,
                      previous_r.source_package_id
               FROM source_rule_evaluations r
               JOIN source_research_enrollments e ON e.rule_fact_id=r.id
               LEFT JOIN source_research_enrollments parent_e
                 ON parent_e.id=e.parent_enrollment_id
               LEFT JOIN source_rule_evaluations parent_r
                 ON parent_r.id=parent_e.rule_fact_id
               LEFT JOIN source_rule_evaluations previous_r
                 ON previous_r.id=e.previous_rule_fact_id
               WHERE r.target_session=%s AND r.source_package_id=%s AND r.rule_id=%s
                 AND r.rule_version=%s AND r.profile_revision=%s
                 AND r.history_session_count=%s AND r.d1_history_session_count=%s
                 AND r.event_window_start=%s AND r.calendar_revision=%s AND r.scope=%s
               ORDER BY e.enrollment_kind,r.source_symbol,e.id""",
            key,
        )
        enrollment_rows = cursor.fetchall()
        enrollment_ids: set[int] = set()
        symbols_by_kind: dict[str, set[str]] = {kind: set() for kind in _KINDS}
        for row in enrollment_rows:
            (
                enrollment_id,
                symbol,
                kind,
                reason_code,
                parent_id,
                previous_rule_fact_id,
                state,
                capture_id,
                parent_record_id,
                parent_kind,
                parent_session,
                parent_symbol,
                parent_rule,
                parent_state,
                parent_version,
                parent_package,
                previous_target,
                previous_symbol,
                previous_rule,
                previous_state,
                previous_version,
                previous_package,
            ) = row
            if kind not in _KINDS or state != "TRUE" or capture_id != source_capture_id:
                raise ResearchFactConflict(
                    "规则登记含非TRUE事实、未登记类别或来源捕获错配"
                )
            if enrollment_id in enrollment_ids or symbol in symbols_by_kind[kind]:
                raise ResearchFactConflict("同一T0规则登记关系出现重复来源代码")
            enrollment_ids.add(enrollment_id)
            symbols_by_kind[kind].add(symbol)
            if kind == "REENTRY":
                proved = (
                    parent_id is not None
                    and parent_record_id == parent_id
                    and parent_kind
                    in {"ENTRY", "FIRST_OBSERVED_LEFT_CENSORED", "REENTRY"}
                    and parent_session is not None
                    and parent_session < target
                    and parent_symbol == symbol
                    and parent_rule == rule
                    and parent_state == "TRUE"
                    and parent_version == RULE_VERSION
                    and parent_package == source_package_id
                    and reason_code == "MATCH_AFTER_PROVED_EXIT"
                    and previous_rule_fact_id is not None
                    and previous_session is not None
                    and previous_target == previous_session
                    and previous_symbol == symbol
                    and previous_rule == rule
                    and previous_state == "FALSE"
                    and previous_version == RULE_VERSION
                    and previous_package == source_package_id
                    and parent_session < previous_session < target
                )
                if not proved:
                    raise ResearchFactConflict(
                        "REENTRY缺少同证券同规则的父登记或已证FALSE退出前态"
                    )
            registered[kind].append(
                {"id": int(enrollment_id), "symbol": symbol, "parent_id": parent_id}
            )

        enrollment_counts = {kind: len(registered[kind]) for kind in _KINDS}
        if sum(enrollment_counts.values()) != sum(enrolled_by_state.values()):
            raise ResearchFactConflict("登记类别分母与规则事实关联数不一致")
        if enrolled_by_state.get("FALSE", 0) or enrolled_by_state.get("UNKNOWN", 0):
            raise ResearchFactConflict("非TRUE规则事实被持久化为来源登记")

        cursor.execute(
            """SELECT count(DISTINCT r.id)::bigint
               FROM source_rule_evaluations r
               JOIN LATERAL (
                 SELECT h.id,h.state
                 FROM source_rule_evaluations h
                 WHERE h.source_package_id=r.source_package_id
                   AND h.source_symbol=r.source_symbol AND h.rule_id=r.rule_id
                   AND h.rule_version=r.rule_version AND h.scope=r.scope
                   AND h.target_session<r.target_session
                   AND h.state IN ('TRUE','FALSE')
                 ORDER BY h.target_session DESC,h.id DESC LIMIT 1
               ) latest ON TRUE
               JOIN source_research_enrollments active_e
                 ON active_e.rule_fact_id=latest.id
               WHERE r.target_session=%s AND r.source_package_id=%s
                 AND r.rule_id=%s AND r.rule_version=%s
                 AND r.profile_revision=%s AND r.history_session_count=%s
                 AND r.d1_history_session_count=%s
                 AND r.event_window_start=%s AND r.calendar_revision=%s
                 AND r.scope=%s AND r.state='UNKNOWN'
                 AND latest.state='TRUE'
                 AND active_e.enrollment_kind=ANY(%s::text[])""",
            (*key, list(_ACTIVE_KINDS)),
        )
        paused_unknown_count = int(cursor.fetchone()[0])

        risk_set = {
            "source_pool_count": pool_count,
            "unique_source_symbol_count": int(symbol_count),
            "rule_state_counts": state_counts,
            "enrollment_counts": enrollment_counts,
            "unregistered_counts": {
                "FALSE_NO_ENROLLMENT": max(
                    0,
                    state_counts.get("FALSE", 0) - enrolled_by_state.get("FALSE", 0),
                ),
                "PAUSED_UNKNOWN": paused_unknown_count,
                "UNKNOWN_LIFECYCLE_UNPROVED": max(
                    0,
                    state_counts.get("UNKNOWN", 0)
                    - enrolled_by_state.get("UNKNOWN", 0)
                    - paused_unknown_count,
                ),
                "TRUE_WITHOUT_ENROLLMENT": max(
                    0,
                    state_counts.get("TRUE", 0) - enrolled_by_state.get("TRUE", 0),
                ),
            },
            "paused_unknown_basis": "LATEST_KNOWN_TRUE_WITH_ACTIVE_ENROLLMENT",
        }

        cursor.execute(
            """SELECT e.id,r.source_symbol,e.enrollment_kind,
                      f.horizon,f.endpoint_session,f.maturity,f.endpoint_quality,
                      f.endpoint_reason,f.endpoint_numerator::text,
                      f.endpoint_denominator::text,f.path_quality,f.path_reasons,
                      f.price_coordinate,f.scope
               FROM source_rule_evaluations r
               JOIN source_research_enrollments e ON e.rule_fact_id=r.id
               LEFT JOIN source_forward_path_facts f
                 ON f.target_session=r.target_session
                AND f.source_package_id=r.source_package_id
                AND f.source_capture_id=r.source_capture_id
                AND f.source_symbol=r.source_symbol
                AND f.horizon=ANY(%s::smallint[])
                AND f.path_rule_version=%s
                AND f.calendar_revision=r.calendar_revision
                AND f.endpoint_session<=%s
                AND f.price_coordinate='RAW_UNADJUSTED' AND f.scope=%s
               WHERE r.target_session=%s AND r.source_package_id=%s AND r.rule_id=%s
                 AND r.rule_version=%s AND r.profile_revision=%s
                 AND r.history_session_count=%s AND r.d1_history_session_count=%s
                 AND r.event_window_start=%s AND r.calendar_revision=%s AND r.scope=%s
               ORDER BY e.enrollment_kind,r.source_symbol,f.horizon""",
            (list(_HORIZONS), PATH_VERSION, research_cutoff, SCOPE, *key),
        )
        forward_rows = cursor.fetchall()
        forward_by_enrollment: dict[tuple[int, int], tuple[Any, ...]] = {}
        for row in forward_rows:
            if row[3] is None:
                continue
            identity = (int(row[0]), int(row[3]))
            if identity in forward_by_enrollment:
                raise ResearchFactConflict("同一登记与期限命中多条Forward事实")
            forward_by_enrollment[identity] = row

        group_results = []
        for kind in _KINDS:
            entries = registered[kind]
            forward_groups = []
            for horizon in _HORIZONS:
                endpoint_session = (
                    horizon_sessions[horizon - 1]
                    if len(horizon_sessions) >= horizon
                    else None
                )
                outcomes = Counter()
                maturity_counts = Counter()
                endpoint_quality_counts = Counter()
                path_quality_counts = Counter()
                unevaluable_reasons = Counter()
                values: list[tuple[Fraction, str, int]] = []
                for entry in entries:
                    if endpoint_session is None:
                        outcomes["NOT_DUE"] += 1
                        maturity_counts["NOT_DUE"] += 1
                        unevaluable_reasons["WAITING_FOR_FUTURE_OFFICIAL_SESSION"] += 1
                        continue
                    row = forward_by_enrollment.get((entry["id"], horizon))
                    if row is None:
                        outcomes["NOT_COMPUTED"] += 1
                        maturity_counts["MISSING_FACT"] += 1
                        unevaluable_reasons["MISSING_FORWARD_FACT"] += 1
                        continue
                    (
                        _,
                        symbol,
                        _,
                        _,
                        stored_endpoint,
                        maturity,
                        endpoint_quality,
                        endpoint_reason,
                        numerator_text,
                        denominator_text,
                        path_quality,
                        path_reasons,
                        coordinate,
                        scope,
                    ) = row
                    if stored_endpoint != endpoint_session:
                        raise ResearchFactConflict("Forward端点与官方会话期限不一致")
                    maturity_counts[maturity or "UNKNOWN"] += 1
                    endpoint_quality_counts[endpoint_quality or "UNKNOWN"] += 1
                    path_quality_counts[path_quality or "UNKNOWN"] += 1
                    if (
                        maturity == "ENDPOINT_OBSERVED"
                        and endpoint_quality == "OBSERVED"
                        and numerator_text is not None
                        and denominator_text is not None
                        and int(denominator_text) > 0
                        and coordinate == "RAW_UNADJUSTED"
                        and scope == SCOPE
                    ):
                        values.append(
                            (
                                Fraction(int(numerator_text), int(denominator_text)),
                                symbol,
                                entry["id"],
                            )
                        )
                        outcomes["OBSERVED"] += 1
                    else:
                        outcomes["UNKNOWN"] += 1
                        reason = (
                            endpoint_reason or maturity or endpoint_quality or "UNKNOWN"
                        )
                        unevaluable_reasons[str(reason)] += 1
                        for path_reason in path_reasons or ():
                            unevaluable_reasons[str(path_reason)] += 1

                values.sort(key=lambda item: (item[0], item[1]))
                positive = [value for value in values if value[0] > 0]
                non_positive = [value for value in values if value[0] <= 0]

                def example(item):
                    if item is None:
                        return None
                    return {
                        "source_symbol": item[1],
                        "enrollment_id": item[2],
                        "exact_return": str(item[0]),
                    }

                if endpoint_session is None:
                    status = "NOT_DUE"
                elif outcomes["NOT_COMPUTED"]:
                    status = "INCOMPLETE"
                elif outcomes["UNKNOWN"]:
                    status = "AVAILABLE_WITH_UNKNOWN"
                else:
                    status = "AVAILABLE"
                forward_groups.append(
                    {
                        "horizon": horizon,
                        "endpoint_session": (
                            endpoint_session.isoformat()
                            if endpoint_session is not None
                            else None
                        ),
                        "status": status,
                        "risk_denominator": len(entries),
                        "observed_count": outcomes["OBSERVED"],
                        "unknown_count": outcomes["UNKNOWN"],
                        "not_computed_count": outcomes["NOT_COMPUTED"],
                        "not_due_count": outcomes["NOT_DUE"],
                        "positive_count": len(positive),
                        "non_positive_count": len(non_positive),
                        "median_exact_return": (
                            str(median([value[0] for value in values]))
                            if values
                            else None
                        ),
                        "maturity_counts": dict(sorted(maturity_counts.items())),
                        "endpoint_quality_counts": dict(
                            sorted(endpoint_quality_counts.items())
                        ),
                        "path_quality_counts": dict(
                            sorted(path_quality_counts.items())
                        ),
                        "unevaluable_reasons": dict(
                            sorted(unevaluable_reasons.items())
                        ),
                        "examples": {
                            "positive": example(positive[-1] if positive else None),
                            "non_positive": example(
                                non_positive[0] if non_positive else None
                            ),
                        },
                    }
                )

            group_result = {
                "kind": kind,
                "enrollment_count": len(entries),
                "independent_security_count": len(symbols_by_kind[kind]),
                "parent_proved_count": len(entries) if kind == "REENTRY" else 0,
                "forward": forward_groups,
            }
            if (
                group_result["enrollment_count"]
                != group_result["independent_security_count"]
            ):
                raise ResearchFactConflict("登记关系数与去重来源证券数不一致")
            for outcome in forward_groups:
                if (
                    sum(
                        outcome[field]
                        for field in (
                            "observed_count",
                            "unknown_count",
                            "not_computed_count",
                            "not_due_count",
                        )
                    )
                    != group_result["enrollment_count"]
                ):
                    raise ResearchFactConflict("Forward质量状态未闭合登记分母")
            group_results.append(group_result)

        line_scope = (
            "SOURCE_FROZEN_FAILURE_LINE"
            if rule in {"LAUNCH_CONFIRM", "RECOVERY_TURN"}
            else "NOT_APPLICABLE"
        )
        line_counts = {kind: {h: Counter() for h in _HORIZONS} for kind in _KINDS}
        raw_session_versions = []
        if line_scope != "NOT_APPLICABLE":
            closed_sessions = []
            for session in horizon_sessions:
                version_status, actual, expected = _page_version_status(
                    cursor, session, source_package_id
                )
                raw_session_versions.append(
                    {
                        "session": session.isoformat(),
                        "status": version_status,
                        "row_count": actual,
                        "expected_count": expected,
                    }
                )
                if version_status == "CLOSED":
                    closed_sessions.append(session)
            cursor.execute(
                """WITH horizons AS (
                     SELECT unnest(ARRAY[1,3,5]::smallint[]) AS horizon
                   ), line_candidates AS (
                     SELECT e.id AS enrollment_id,e.enrollment_kind,r.source_symbol,
                            r.frozen_line_numerator,r.frozen_line_denominator
                     FROM source_rule_evaluations r
                     JOIN source_research_enrollments e ON e.rule_fact_id=r.id
                     WHERE r.target_session=%s AND r.source_package_id=%s
                       AND r.rule_id=%s AND r.rule_version=%s
                       AND r.profile_revision=%s AND r.history_session_count=%s
                       AND r.d1_history_session_count=%s
                       AND r.event_window_start=%s AND r.calendar_revision=%s
                       AND r.scope=%s AND r.state='TRUE'
                       AND r.frozen_line_numerator>0
                       AND r.frozen_line_denominator>0
                       AND r.frozen_line_source IS NOT NULL
                   ), points AS (
                     SELECT l.enrollment_id,l.enrollment_kind,h.horizon,
                            s.session_date,
                            CASE
                              WHEN NOT (s.session_date=ANY(%s::date[]))
                                THEN 'UNKNOWN'
                              WHEN p.source_symbol IS NULL OR c.capture_id IS NULL
                                OR p.bar_status<>'TARGET_PRESENT'
                                OR p.price_quality<>'OBSERVED'
                                OR p.raw_close IS NULL OR p.raw_close<=0
                                THEN 'UNKNOWN'
                              WHEN p.raw_close*l.frozen_line_denominator
                                   <=l.frozen_line_numerator
                                THEN 'TOUCHED'
                              ELSE 'ABOVE'
                            END AS point_state
                     FROM line_candidates l
                     CROSS JOIN horizons h
                     JOIN LATERAL (
                       SELECT session_date
                       FROM unnest(%s::date[]) WITH ORDINALITY AS s(session_date,ordinal)
                       WHERE s.ordinal<=h.horizon
                     ) s ON TRUE
                     LEFT JOIN source_raw_daily p
                       ON p.source_symbol=l.source_symbol
                      AND p.session_date=s.session_date AND p.source_package_id=%s
                     LEFT JOIN source_captures c
                       ON c.capture_id=p.source_capture_id
                      AND c.source_package_id=p.source_package_id
                      AND c.target_session=p.session_date
                   ), touch_points AS (
                     SELECT p.*,
                            min(session_date) FILTER (WHERE point_state='TOUCHED')
                              OVER (PARTITION BY enrollment_id,horizon) AS first_touch
                     FROM points p
                   ), return_points AS (
                     SELECT p.*,
                            min(session_date) FILTER (
                              WHERE point_state='ABOVE' AND first_touch IS NOT NULL
                                AND session_date>first_touch
                            ) OVER (PARTITION BY enrollment_id,horizon) AS first_return
                     FROM touch_points p
                   ), candidate_horizons AS (
                     SELECT l.enrollment_id,l.enrollment_kind,h.horizon,
                            min(p.first_touch) AS first_touch,
                            min(p.first_return) AS first_return,
                            COALESCE(bool_or(
                              p.point_state='UNKNOWN'
                              AND (p.first_touch IS NULL OR p.session_date<p.first_touch)
                            ),FALSE) AS unknown_before_touch,
                            COALESCE(bool_or(
                              p.point_state='UNKNOWN'
                              AND (p.first_return IS NULL OR p.session_date<p.first_return)
                            ),FALSE) AS unknown_before_return
                     FROM line_candidates l
                     CROSS JOIN horizons h
                     LEFT JOIN return_points p
                       ON p.enrollment_id=l.enrollment_id AND p.horizon=h.horizon
                     GROUP BY l.enrollment_id,l.enrollment_kind,h.horizon
                   ), statuses AS (
                     SELECT enrollment_kind,horizon,
                       CASE
                         WHEN first_touch IS NOT NULL AND NOT unknown_before_touch
                           THEN 'OBSERVED'
                         WHEN unknown_before_touch THEN 'UNKNOWN'
                         WHEN horizon<=%s THEN 'NOT_OBSERVED'
                         ELSE 'NOT_DUE'
                       END AS touch_status,
                       CASE
                         WHEN first_return IS NOT NULL AND NOT unknown_before_return
                           THEN 'OBSERVED'
                         WHEN unknown_before_return THEN 'UNKNOWN'
                         WHEN horizon<=%s THEN 'NOT_OBSERVED'
                         ELSE 'NOT_DUE'
                       END AS return_status
                     FROM candidate_horizons
                   )
                   SELECT enrollment_kind,horizon,touch_status,return_status,
                          count(*)::bigint
                   FROM statuses
                   GROUP BY enrollment_kind,horizon,touch_status,return_status
                   ORDER BY enrollment_kind,horizon,touch_status,return_status""",
                (
                    *key,
                    closed_sessions,
                    list(horizon_sessions),
                    source_package_id,
                    len(horizon_sessions),
                    len(horizon_sessions),
                ),
            )
            for kind, horizon, touch_status, return_status, count in cursor.fetchall():
                line_counts[kind][int(horizon)][touch_status] += int(count)
                line_counts[kind][int(horizon)][f"RETURN_{return_status}"] += int(count)

        for group_result in group_results:
            kind = group_result["kind"]
            followup = []
            for horizon in _HORIZONS:
                counts = line_counts[kind][horizon]
                touch = {
                    status: counts[status]
                    for status in ("OBSERVED", "UNKNOWN", "NOT_OBSERVED", "NOT_DUE")
                }
                returned = {
                    status: counts[f"RETURN_{status}"]
                    for status in ("OBSERVED", "UNKNOWN", "NOT_OBSERVED", "NOT_DUE")
                }
                followup.append(
                    {
                        "horizon": horizon,
                        "candidate_line_count": sum(touch.values()),
                        "touch_status_counts": touch,
                        "return_status_counts": returned,
                    }
                )
            group_result["line_scope"] = line_scope
            group_result["candidate_line_followup"] = followup

        return {
            "status": "AVAILABLE",
            "scope": SCOPE,
            "identity_status": "SOURCE_SYMBOL_ONLY",
            "price_coordinate": "RAW_UNADJUSTED",
            "target_session": target.isoformat(),
            "research_cutoff": research_cutoff.isoformat(),
            "rule": rule,
            "rule_version": RULE_VERSION,
            "path_rule_version": PATH_VERSION,
            "source_package_id": source_package_id,
            "source_capture_id": source_capture_id,
            "profile_revision": profile_revision,
            "history_session_count": history_session_count,
            "d1_history_session_count": d1_history_session_count,
            "event_window_start": event_window_start.isoformat(),
            "calendar_revision": calendar_revision,
            "risk_set": risk_set,
            "groups": group_results,
            "comparison_status": (
                "COMPARABLE_DESCRIPTIVE_SINGLE_T0"
                if enrollment_counts["ENTRY"] and enrollment_counts["REENTRY"]
                else "NO_VALID_COMPARABLE_SCOPE"
            ),
            "independent_t0_date_count": 1,
            "raw_session_versions": raw_session_versions,
            "database_transaction": "REPEATABLE_READ_READ_ONLY",
            "database_write_attempted": False,
        }
