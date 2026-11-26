-- 全项目单一有效业务事实专项集中收尾（0088）。
-- 目标：将剩余后继事实表（forward_path / cohort_group_stats / fep_baseline / s2_t1_cohort_stats / t0_early_observation_facts）
-- 收敛为按真实业务对象、有效交易日与真实业务维度的最短唯一业务键；
-- 保留来源捕获、规则/路径/日历修订等非键元数据列。

-- ============================================================================
-- 1. stock.source_forward_path_facts
-- ============================================================================
ALTER TABLE stock.source_forward_path_facts
    DROP CONSTRAINT IF EXISTS source_forward_business_uq,
    ADD CONSTRAINT source_forward_business_uq UNIQUE (target_session, source_symbol, horizon);

COMMENT ON CONSTRAINT source_forward_business_uq ON stock.source_forward_path_facts
    IS '前向路径唯一业务键：每个交易日、标的与观察周期仅存一份有效前向路径事实';

-- ============================================================================
-- 2. stock.source_cohort_group_stats
-- ============================================================================
ALTER TABLE stock.source_cohort_group_stats
    DROP CONSTRAINT IF EXISTS source_cohort_group_stats_pkey,
    ADD CONSTRAINT source_cohort_group_stats_pkey PRIMARY KEY (
        target_session,
        research_cutoff,
        rule_id,
        horizon,
        state,
        weighting
    );

COMMENT ON CONSTRAINT source_cohort_group_stats_pkey ON stock.source_cohort_group_stats
    IS '队列分组统计唯一主键：按交易日、研究截止日、规则、周期、状态与权重唯一';

-- ============================================================================
-- 3. stock.source_fep_baseline_facts
-- ============================================================================
DROP INDEX IF EXISTS stock.source_fep_baseline_t0_business_key;
DROP INDEX IF EXISTS stock.source_fep_baseline_equal_date_business_key;

CREATE UNIQUE INDEX source_fep_baseline_t0_business_key
    ON stock.source_fep_baseline_facts (
        research_cutoff,
        target_session,
        rule_id,
        lane,
        horizon,
        group_code,
        metric_code,
        condition_code
    )
    WHERE (summary_scope = 'T0'::text);

CREATE UNIQUE INDEX source_fep_baseline_equal_date_business_key
    ON stock.source_fep_baseline_facts (
        research_cutoff,
        period_start_session,
        period_end_session,
        rule_id,
        lane,
        horizon,
        group_code,
        metric_code,
        condition_code
    )
    NULLS NOT DISTINCT
    WHERE (summary_scope = 'EQUAL_DATE'::text);

-- ============================================================================
-- 4. stock.source_s2_t1_cohort_stats
-- ============================================================================
ALTER TABLE stock.source_s2_t1_cohort_stats
    DROP CONSTRAINT IF EXISTS source_s2_t1_cohort_stats_pkey,
    ADD CONSTRAINT source_s2_t1_cohort_stats_pkey PRIMARY KEY (
        target_session,
        endpoint_session,
        research_cutoff,
        t0_s2_state,
        t1_s2_state
    );

COMMENT ON CONSTRAINT source_s2_t1_cohort_stats_pkey ON stock.source_s2_t1_cohort_stats
    IS 'S2 T+1 队列统计唯一主键：按 T0 日、终点日、研究截止日与 T0/T1 S2 状态唯一';

-- ============================================================================
-- 5. stock.source_t0_early_observation_facts
-- ============================================================================
ALTER TABLE stock.source_t0_early_observation_facts
    DROP CONSTRAINT IF EXISTS source_t0_early_context_revision_uq,
    ADD CONSTRAINT source_t0_early_context_revision_uq UNIQUE (
        enrollment_id,
        hypothesis_version,
        board_context_key
    );

COMMENT ON CONSTRAINT source_t0_early_context_revision_uq ON stock.source_t0_early_observation_facts
    IS 'T0 早期观察唯一业务键：按入组ID、假设版本与板块上下文键唯一';
