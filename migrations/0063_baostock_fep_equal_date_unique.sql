-- 跨日期FEP统计没有唯一来源捕获；空捕获仍属于同一业务键。
DROP INDEX stock.source_fep_baseline_equal_date_business_key;

CREATE UNIQUE INDEX source_fep_baseline_equal_date_business_key
    ON stock.source_fep_baseline_facts (
        research_cutoff, source_capture_id, period_start_session, period_end_session,
        rule_id, rule_version, profile_family, calendar_revision, path_rule_version,
        source_time_quality, parser_contract, lane, horizon, group_code, metric_code,
        condition_code
    ) NULLS NOT DISTINCT WHERE summary_scope = 'EQUAL_DATE';

COMMENT ON INDEX stock.source_fep_baseline_equal_date_business_key IS
    '跨日期FEP统计的空来源捕获按同一业务键处理，防止重复生成相同汇总。';
