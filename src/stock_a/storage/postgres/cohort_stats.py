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


def read_cohort_page(
    config,
    *,
    target,
    research_cutoff,
    source_package_id,
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
):
    """同一只读快照读取六组完整统计和最多一页的明细。"""
    with _transaction(config, write=False) as (_, cursor, _):
        cursor.execute(
            """SELECT source_capture_id,state,weighting,source_pool_count,member_count,
                      evaluable_count,positive_count,median_numerator,median_denominator,
                      maturity_codes,maturity_counts,path_quality_codes,path_quality_counts,
                      reason_codes,reason_counts
               FROM source_cohort_group_stats
               WHERE target_session=%s AND research_cutoff=%s AND source_package_id=%s
                 AND rule_id=%s AND rule_version=%s AND profile_revision=%s
                 AND history_session_count=%s AND d1_history_session_count=%s
                 AND event_window_start=%s AND calendar_revision=%s
                 AND path_rule_version=%s AND horizon=%s
               ORDER BY source_capture_id,state,weighting LIMIT 13""",
            (
                target,
                research_cutoff,
                source_package_id,
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
            source_package_id=source_package_id,
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
                """SELECT r.id,r.rule_id,r.state,r.reasons,e.id,e.enrollment_kind,e.reason_code
                   FROM source_rule_evaluations r
                   LEFT JOIN source_research_enrollments e ON e.rule_fact_id=r.id
                   WHERE r.target_session=%s AND r.source_package_id=%s
                     AND r.source_capture_id=%s AND r.source_symbol=%s
                     AND r.rule_version=%s AND r.profile_revision=%s
                     AND r.history_session_count=%s AND r.d1_history_session_count=%s
                     AND r.event_window_start=%s AND r.calendar_revision=%s
                   ORDER BY r.rule_id LIMIT 10""",
                (
                    target,
                    source_package_id,
                    next(iter(captures)),
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
                        {"id": enrollment_id, "kind": kind, "reason": reason}
                        if enrollment_id is not None
                        else None
                    ),
                }
                for fact_id, rule_id, state, reasons, enrollment_id, kind, reason in cursor.fetchall()
            ]
            if len(rules) > 9:
                raise ResearchFactConflict("所选代码的规则事实超过现行九规则范围")
        return total, groups, rows, rules
