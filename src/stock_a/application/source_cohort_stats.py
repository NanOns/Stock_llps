"""从已保存的冻结规则与路径事实离线生成Cohort统计。"""

from fractions import Fraction

from stock_a.application.source_persisted_research import summarize_cohort_rows
from stock_a.research.entry_context.sessions import _load_calendar
from stock_a.storage.postgres.research_facts import (
    PATH_VERSION,
    RULE_VERSION,
    _read_research_rows_cursor,
)
from stock_a.storage.postgres.source_raw import _transaction

_COHORT_RULE_IDS = ("LAUNCH_CONFIRM", "RECOVERY_TURN", "s1", "s2", "seed")


def persist_cohort_stats(config, *, target, research_cutoff, source_capture_id):
    """在单事务内读取完整规则并保存六组typed统计；不读取来源文件。"""
    if target > research_cutoff:
        raise ValueError("目标日不得晚于研究截止日")
    _, _, revision, calendar = _load_calendar(research_cutoff)
    if target not in calendar:
        raise ValueError("目标日不在官方日历中")
    with _transaction(config, write=True, statement_timeout_ms=30000) as (_, cursor, _):
        cursor.execute(
            """SELECT rule_id,profile_revision,history_session_count,d1_history_session_count,
                      event_window_start,count(*),count(DISTINCT source_symbol)
               FROM source_rule_evaluations
               WHERE target_session=%s AND source_capture_id=%s
                 AND rule_version=%s AND calendar_revision=%s
                 AND rule_id=ANY(%s)
               GROUP BY 1,2,3,4,5 ORDER BY 1,2,3,4,5""",
            (target, source_capture_id, RULE_VERSION, revision, list(_COHORT_RULE_IDS)),
        )
        keys = cursor.fetchall()
        if not keys:
            raise ValueError("目标日没有可汇总的精确规则事实")
        total = 0
        for (
            rule,
            profile,
            history,
            d1_history,
            window,
            count,
            distinct,
        ) in keys:
            if count != distinct:
                raise ValueError("规则代码不唯一")
            cursor.execute(
                """SELECT count(*),count(DISTINCT source_symbol) FROM source_raw_daily
                   WHERE session_date=%s AND source_capture_id=%s
                     AND board IN ('MAIN_BOARD', 'GEM')""",
                (target, source_capture_id),
            )
            if cursor.fetchone() != (count, count):
                raise ValueError("规则分母与来源研究范围分母不一致")
            cursor.execute(
                """SELECT count(*) FROM source_rule_evaluations r
                   LEFT JOIN source_raw_daily d ON d.session_date=r.target_session
                     AND d.source_capture_id=r.source_capture_id
                     AND d.source_symbol=r.source_symbol
                     AND d.board IN ('MAIN_BOARD', 'GEM')
                   WHERE r.target_session=%s AND r.source_capture_id=%s
                     AND r.rule_id=%s AND r.rule_version=%s AND r.profile_revision=%s
                     AND r.history_session_count=%s AND r.d1_history_session_count=%s
                     AND r.event_window_start=%s AND r.calendar_revision=%s AND d.source_symbol IS NULL""",
                (
                    target,
                    source_capture_id,
                    rule,
                    RULE_VERSION,
                    profile,
                    history,
                    d1_history,
                    window,
                    revision,
                ),
            )
            if cursor.fetchone()[0]:
                raise ValueError("规则代码集合与来源捕获不一致")
            for horizon in (1, 3, 5, 10, 20):
                idx = calendar.index(target) + horizon
                due = idx < len(calendar) and calendar[idx] <= research_cutoff
                rows = _read_research_rows_cursor(
                    cursor,
                    target=target,
                    source_capture_id=source_capture_id,
                    profile_revision=profile,
                    history_session_count=history,
                    d1_history_session_count=d1_history,
                    event_window_start=window,
                    calendar_revision=revision,
                    rule=rule,
                    horizon=horizon,
                    research_cutoff=research_cutoff,
                )
                groups = summarize_cohort_rows(rows, rule=rule, due=due)
                for group in groups:
                    state = {
                        "MATCH": "TRUE",
                        "NO_MATCH": "FALSE",
                        "UNKNOWN": "UNKNOWN",
                    }[group["group"]]
                    median = (
                        Fraction(group["median"])
                        if group["median"] is not None
                        else None
                    )
                    pairs = []
                    for field in ("maturity", "path_quality", "unevaluable_reasons"):
                        items = sorted(group[field].items())
                        pairs.extend(
                            ([item[0] for item in items], [item[1] for item in items])
                        )
                    cursor.execute(
                        """INSERT INTO source_cohort_group_stats (
                            target_session, research_cutoff, source_capture_id,
                            rule_id, rule_version, profile_revision,
                            history_session_count, d1_history_session_count,
                            event_window_start, calendar_revision, path_rule_version,
                            horizon, state, weighting,
                            source_pool_count, member_count, evaluable_count, positive_count,
                            median_numerator, median_denominator,
                            maturity_codes, maturity_counts,
                            path_quality_codes, path_quality_counts,
                            reason_codes, reason_counts
                           ) VALUES (
                            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                           ON CONFLICT (target_session,research_cutoff,rule_id,horizon,state,weighting)
                           DO UPDATE SET source_capture_id=EXCLUDED.source_capture_id,
                            rule_version=EXCLUDED.rule_version,
                            profile_revision=EXCLUDED.profile_revision,
                            history_session_count=EXCLUDED.history_session_count,
                            d1_history_session_count=EXCLUDED.d1_history_session_count,
                            event_window_start=EXCLUDED.event_window_start,
                            calendar_revision=EXCLUDED.calendar_revision,
                            path_rule_version=EXCLUDED.path_rule_version,
                            source_pool_count=EXCLUDED.source_pool_count,
                            member_count=EXCLUDED.member_count,evaluable_count=EXCLUDED.evaluable_count,
                            positive_count=EXCLUDED.positive_count,median_numerator=EXCLUDED.median_numerator,
                            median_denominator=EXCLUDED.median_denominator,maturity_codes=EXCLUDED.maturity_codes,
                            maturity_counts=EXCLUDED.maturity_counts,path_quality_codes=EXCLUDED.path_quality_codes,
                            path_quality_counts=EXCLUDED.path_quality_counts,reason_codes=EXCLUDED.reason_codes,
                            reason_counts=EXCLUDED.reason_counts
                           WHERE (source_cohort_group_stats.source_capture_id,
                                  source_cohort_group_stats.rule_version,
                                  source_cohort_group_stats.profile_revision,
                                  source_cohort_group_stats.history_session_count,
                                  source_cohort_group_stats.d1_history_session_count,
                                  source_cohort_group_stats.event_window_start,
                                  source_cohort_group_stats.calendar_revision,
                                  source_cohort_group_stats.path_rule_version,
                                  source_cohort_group_stats.source_pool_count,
                                  source_cohort_group_stats.member_count,
                                  source_cohort_group_stats.evaluable_count,
                                  source_cohort_group_stats.positive_count,
                                  source_cohort_group_stats.median_numerator,
                                  source_cohort_group_stats.median_denominator,
                                  source_cohort_group_stats.maturity_codes,
                                  source_cohort_group_stats.maturity_counts,
                                  source_cohort_group_stats.path_quality_codes,
                                  source_cohort_group_stats.path_quality_counts,
                                  source_cohort_group_stats.reason_codes,
                                  source_cohort_group_stats.reason_counts)
                             IS DISTINCT FROM
                                 (EXCLUDED.source_capture_id,EXCLUDED.rule_version,
                                  EXCLUDED.profile_revision,EXCLUDED.history_session_count,
                                  EXCLUDED.d1_history_session_count,EXCLUDED.event_window_start,
                                  EXCLUDED.calendar_revision,EXCLUDED.path_rule_version,
                                  EXCLUDED.source_pool_count,EXCLUDED.member_count,
                                  EXCLUDED.evaluable_count,EXCLUDED.positive_count,
                                  EXCLUDED.median_numerator,EXCLUDED.median_denominator,
                                  EXCLUDED.maturity_codes,EXCLUDED.maturity_counts,
                                  EXCLUDED.path_quality_codes,EXCLUDED.path_quality_counts,
                                  EXCLUDED.reason_codes,EXCLUDED.reason_counts)""",
                        (
                            target,
                            research_cutoff,
                            source_capture_id,
                            rule,
                            RULE_VERSION,
                            profile,
                            history,
                            d1_history,
                            window,
                            revision,
                            PATH_VERSION,
                            horizon,
                            state,
                            group["weighting"],
                            count,
                            group["count"],
                            group["evaluable"],
                            group["positive"],
                            median.numerator if median is not None else None,
                            median.denominator if median is not None else None,
                            *pairs,
                        ),
                    )
                    total += 1
    return {"status": "PERSISTED", "groups": total, "rule_keys": len(keys)}
