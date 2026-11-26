-- 全项目单一有效业务事实：彻底删除多版本机制，简化业务唯一键与任务进度表（0087）。
-- 原则：同一业务对象、同一交易日、同一真实业务维度仅存在一份正式有效结果；
-- 保留真实来源捕获元数据、双价格坐标（RAW/QFQ）、历史成员、冻结篮子及真实生命周期字段。

-- ============================================================================
-- 1. R03 正式市场宽度与市场环境
-- ============================================================================

-- 1.1 stock.r03_formal_market_breadth_facts
ALTER TABLE stock.r03_formal_market_breadth_facts
    DROP CONSTRAINT IF EXISTS r03_formal_market_breadth_fac_trade_date_scope_key_universe_key,
    DROP COLUMN IF EXISTS scope_key,
    DROP COLUMN IF EXISTS universe_version_t,
    DROP COLUMN IF EXISTS universe_version_t3,
    DROP COLUMN IF EXISTS contract_version,
    ADD CONSTRAINT r03_formal_market_breadth_facts_trade_date_uq UNIQUE (trade_date);

COMMENT ON CONSTRAINT r03_formal_market_breadth_facts_trade_date_uq ON stock.r03_formal_market_breadth_facts
    IS '正式市场宽度唯一业务键：每个有效交易日仅存一份正式市场宽度事实';

-- 1.2 stock.source_market_environment_day_facts
UPDATE stock.source_market_environment_day_facts
SET breadth_source_kind = 'UNKNOWN'
WHERE breadth_source_kind = 'SOURCE_LEGACY';

ALTER TABLE stock.source_market_environment_day_facts
    DROP CONSTRAINT IF EXISTS source_market_environment_day_facts_uq,
    DROP CONSTRAINT IF EXISTS source_market_environment_day_facts_breadth_source_kind_check,
    DROP COLUMN IF EXISTS formal_scope_key,
    DROP COLUMN IF EXISTS profile_revision,
    DROP COLUMN IF EXISTS universe_version_t,
    DROP COLUMN IF EXISTS universe_version_t3,
    DROP COLUMN IF EXISTS prev_formal_scope_key,
    ADD CONSTRAINT source_market_environment_day_facts_breadth_source_kind_check
        CHECK (breadth_source_kind IN ('FORMAL_UNIVERSE', 'UNKNOWN')),
    ADD CONSTRAINT source_market_environment_day_facts_session_uq UNIQUE (target_session);

COMMENT ON CONSTRAINT source_market_environment_day_facts_session_uq ON stock.source_market_environment_day_facts
    IS '正式市场环境唯一业务键：每个有效交易日仅存一份正式市场环境事实';

-- ============================================================================
-- 2. R03 正式板块事实表群（Native / Rotation / Overlap / LOO）
-- ============================================================================

-- 2.1 stock.r03_sector_native_day_facts
ALTER TABLE stock.r03_sector_native_day_facts
    DROP CONSTRAINT IF EXISTS r03_sector_native_day_facts_uq,
    DROP COLUMN IF EXISTS scope_key,
    DROP COLUMN IF EXISTS member_version,
    DROP COLUMN IF EXISTS contract_version,
    ADD CONSTRAINT r03_sector_native_day_facts_uq UNIQUE (trade_date, board_kind, board_id);

-- 2.2 stock.r03_sector_rotation_episodes
ALTER TABLE stock.r03_sector_rotation_episodes
    DROP CONSTRAINT IF EXISTS r03_sector_rotation_episodes_uq,
    DROP COLUMN IF EXISTS scope_key,
    DROP COLUMN IF EXISTS member_version,
    DROP COLUMN IF EXISTS contract_version,
    ADD CONSTRAINT r03_sector_rotation_episodes_uq UNIQUE (episode_key);

-- 2.3 stock.r03_sector_rotation_board_day_facts
ALTER TABLE stock.r03_sector_rotation_board_day_facts
    DROP CONSTRAINT IF EXISTS r03_sector_rotation_board_day_trade_date_board_kind_board_i_key,
    DROP COLUMN IF EXISTS scope_key,
    DROP COLUMN IF EXISTS member_version,
    DROP COLUMN IF EXISTS contract_version,
    ADD CONSTRAINT r03_sector_rotation_board_day_facts_uq UNIQUE (trade_date, board_kind, board_id);

-- 2.4 stock.r03_sector_overlap_clusters
ALTER TABLE stock.r03_sector_overlap_clusters
    DROP CONSTRAINT IF EXISTS r03_sector_overlap_clusters_uq,
    DROP COLUMN IF EXISTS scope_key,
    DROP COLUMN IF EXISTS member_version,
    DROP COLUMN IF EXISTS contract_version,
    ADD CONSTRAINT r03_sector_overlap_clusters_uq UNIQUE (trade_date, board_kind, cluster_id);

-- 2.5 stock.r03_sector_overlap_board_facts
ALTER TABLE stock.r03_sector_overlap_board_facts
    DROP CONSTRAINT IF EXISTS r03_sector_overlap_board_facts_uq,
    DROP COLUMN IF EXISTS scope_key,
    DROP COLUMN IF EXISTS member_version,
    DROP COLUMN IF EXISTS contract_version,
    ADD CONSTRAINT r03_sector_overlap_board_facts_uq UNIQUE (trade_date, board_kind, board_id);

-- 2.6 stock.r03_sector_overlap_pair_facts
ALTER TABLE stock.r03_sector_overlap_pair_facts
    DROP CONSTRAINT IF EXISTS r03_sector_overlap_pair_facts_uq,
    DROP COLUMN IF EXISTS scope_key,
    DROP COLUMN IF EXISTS member_version,
    DROP COLUMN IF EXISTS contract_version,
    ADD CONSTRAINT r03_sector_overlap_pair_facts_uq UNIQUE (trade_date, board_kind, board_id_a, board_id_b);

-- 2.7 stock.r03_sector_loo_support_facts
ALTER TABLE stock.r03_sector_loo_support_facts
    DROP CONSTRAINT IF EXISTS r03_sector_loo_support_facts_uq,
    DROP COLUMN IF EXISTS scope_key,
    DROP COLUMN IF EXISTS member_version,
    DROP COLUMN IF EXISTS contract_version,
    ADD CONSTRAINT r03_sector_loo_support_facts_uq UNIQUE (trade_date, target_entity_id, board_kind);

-- ============================================================================
-- 3. 任务进度表（删除 0084/0085 多版本端点与前态链列及索引）
-- ============================================================================

DROP INDEX IF EXISTS stock.data_job_progress_r03_market_lookup_ix;
DROP INDEX IF EXISTS stock.data_job_progress_r03_prev_chain_ix;

ALTER TABLE stock.data_job_progress
    DROP COLUMN IF EXISTS universe_version_t,
    DROP COLUMN IF EXISTS universe_version_t1,
    DROP COLUMN IF EXISTS universe_version_t3,
    DROP COLUMN IF EXISTS source_capture_id_t1,
    DROP COLUMN IF EXISTS source_capture_id_t3,
    DROP COLUMN IF EXISTS profile_revision_t1,
    DROP COLUMN IF EXISTS profile_revision_t3,
    DROP COLUMN IF EXISTS prev_scope_key,
    DROP COLUMN IF EXISTS prev_member_version,
    DROP COLUMN IF EXISTS prev_trade_date,
    DROP COLUMN IF EXISTS industry_snapshot_id_t1,
    DROP COLUMN IF EXISTS concept_snapshot_id_t1,
    DROP COLUMN IF EXISTS industry_snapshot_id_t3,
    DROP COLUMN IF EXISTS concept_snapshot_id_t3,
    DROP COLUMN IF EXISTS source_capture_id_t4,
    DROP COLUMN IF EXISTS prev_session_t,
    DROP COLUMN IF EXISTS prev_session_t3;

-- ============================================================================
-- 4. R00–R02 来源事实与上游衍生表（移除多版本宽唯一键与版本列）
-- ============================================================================

-- 4.1 stock.source_raw_daily（保留 source_capture_id 作普通溯源列，表内分列 RAW/QFQ 双坐标）
DROP INDEX IF EXISTS stock.source_raw_daily_session_package_symbol_idx;
ALTER TABLE stock.source_raw_daily
    DROP CONSTRAINT IF EXISTS source_raw_daily_business_uq,
    ADD CONSTRAINT source_raw_daily_business_uq UNIQUE (source_symbol, session_date);
CREATE INDEX IF NOT EXISTS source_raw_daily_session_symbol_idx
    ON stock.source_raw_daily (session_date, source_symbol);

-- 4.2 stock.source_baostock_stock_basic
DROP INDEX IF EXISTS stock.idx_baostock_stock_basic_symbol;
ALTER TABLE stock.source_baostock_stock_basic
    DROP CONSTRAINT IF EXISTS source_baostock_stock_basic_uq,
    ADD CONSTRAINT source_baostock_stock_basic_uq UNIQUE (source_symbol);

-- 4.3 stock.source_baostock_session_directory
DROP INDEX IF EXISTS stock.idx_baostock_session_dir_date_board;
ALTER TABLE stock.source_baostock_session_directory
    DROP CONSTRAINT IF EXISTS source_baostock_session_directory_uq,
    ADD CONSTRAINT source_baostock_session_directory_uq UNIQUE (session_date, source_symbol);
CREATE INDEX IF NOT EXISTS idx_baostock_session_dir_date_board
    ON stock.source_baostock_session_directory (session_date, board);

-- 4.4 stock.source_market_index_daily
DROP INDEX IF EXISTS stock.idx_market_index_daily_symbol_date;
ALTER TABLE stock.source_market_index_daily
    DROP CONSTRAINT IF EXISTS source_market_index_daily_uq,
    ADD CONSTRAINT source_market_index_daily_uq UNIQUE (source_symbol, session_date);

-- 4.5 stock.source_market_index_day_facts
ALTER TABLE stock.source_market_index_day_facts
    DROP CONSTRAINT IF EXISTS source_market_index_day_facts_uq,
    ADD CONSTRAINT source_market_index_day_facts_uq UNIQUE (target_session, source_symbol);

-- 4.6 stock.source_cross_section_day_facts
ALTER TABLE stock.source_cross_section_day_facts
    DROP CONSTRAINT IF EXISTS source_cross_section_day_facts_uq,
    DROP COLUMN IF EXISTS universe_version,
    ADD CONSTRAINT source_cross_section_day_facts_uq UNIQUE (target_session, source_symbol);

-- 4.7 stock.source_cross_section_endpoints
DROP INDEX IF EXISTS stock.source_cross_section_endpoints_session_idx;
ALTER TABLE stock.source_cross_section_endpoints
    DROP CONSTRAINT IF EXISTS source_cross_section_endpoints_business_uq,
    DROP COLUMN IF EXISTS universe_version,
    DROP COLUMN IF EXISTS universe_revision,
    ADD CONSTRAINT source_cross_section_endpoints_business_uq UNIQUE (target_session, endpoint_key);

-- 4.8 stock.source_symbol_price_profiles
DROP INDEX IF EXISTS stock.source_symbol_price_profiles_session_package_symbol_idx;
ALTER TABLE stock.source_symbol_price_profiles
    DROP CONSTRAINT IF EXISTS source_symbol_price_profiles_business_uq,
    DROP COLUMN IF EXISTS profile_revision,
    DROP COLUMN IF EXISTS cross_section_universe_capture_id,
    DROP COLUMN IF EXISTS cross_section_universe_version,
    DROP COLUMN IF EXISTS cross_section_contract_version,
    ADD CONSTRAINT source_symbol_price_profiles_business_uq UNIQUE (target_session, source_symbol);
CREATE INDEX IF NOT EXISTS source_symbol_price_profiles_session_symbol_idx
    ON stock.source_symbol_price_profiles (target_session, source_symbol);

-- 4.9 stock.source_market_breadth_days & symbols & change_days
DROP INDEX IF EXISTS stock.source_market_breadth_session_package_idx;
ALTER TABLE stock.source_market_breadth_days
    DROP CONSTRAINT IF EXISTS source_market_breadth_days_baostock_key_uq,
    ADD CONSTRAINT source_market_breadth_days_session_uq UNIQUE (target_session);

ALTER TABLE stock.source_market_breadth_symbols
    DROP CONSTRAINT IF EXISTS source_market_breadth_symbols_baostock_key_uq,
    ADD CONSTRAINT source_market_breadth_symbols_session_uq UNIQUE (target_session, source_symbol);

ALTER TABLE stock.source_market_breadth_change_days
    DROP CONSTRAINT IF EXISTS source_market_breadth_change_pair_uq,
    ADD CONSTRAINT source_market_breadth_change_session_uq UNIQUE (target_session);

-- 4.10 stock.source_market_participation_days
ALTER TABLE stock.source_market_participation_days
    DROP CONSTRAINT IF EXISTS source_market_participation_business_key,
    DROP COLUMN IF EXISTS profile_revision,
    ADD CONSTRAINT source_market_participation_business_key UNIQUE (target_session);

-- 4.11 stock.source_price_limit_day_facts & source_limit_up_streak_day_facts
DROP INDEX IF EXISTS stock.source_price_limit_day_facts_market_idx;
ALTER TABLE stock.source_price_limit_day_facts
    DROP CONSTRAINT IF EXISTS source_price_limit_day_facts_uq,
    ADD CONSTRAINT source_price_limit_day_facts_uq UNIQUE (target_session, source_symbol);
CREATE INDEX IF NOT EXISTS source_price_limit_day_facts_market_idx
    ON stock.source_price_limit_day_facts (target_session, research_eligible);

DROP INDEX IF EXISTS stock.idx_source_limit_up_streak_session_eligible;
ALTER TABLE stock.source_limit_up_streak_day_facts
    DROP CONSTRAINT IF EXISTS source_limit_up_streak_day_facts_uq,
    ADD CONSTRAINT source_limit_up_streak_day_facts_uq UNIQUE (target_session, source_symbol);
CREATE INDEX IF NOT EXISTS idx_source_limit_up_streak_session_eligible
    ON stock.source_limit_up_streak_day_facts (target_session, research_eligible, streak_status);

-- 4.12 stock.source_pool_seed_day_facts
DROP INDEX IF EXISTS stock.source_pool_seed_session_package_profile_idx;
ALTER TABLE stock.source_pool_seed_day_facts
    DROP CONSTRAINT IF EXISTS source_pool_seed_business_uq,
    DROP COLUMN IF EXISTS profile_revision,
    DROP COLUMN IF EXISTS cross_section_universe_capture_id,
    DROP COLUMN IF EXISTS cross_section_universe_version,
    DROP COLUMN IF EXISTS cross_section_contract_version,
    DROP COLUMN IF EXISTS universe_match_status,
    ADD CONSTRAINT source_pool_seed_business_uq UNIQUE (target_session, source_symbol);

-- 4.13 stock.source_d0_launch_day_facts（合并重复 calendar_revision 行并重指向外键后建最短唯一键）
CREATE TEMP TABLE _d0_launch_dedup_map ON COMMIT DROP AS
WITH ranked_d0 AS (
    SELECT
        d.id,
        FIRST_VALUE(d.id) OVER (
            PARTITION BY d.target_session, d.source_symbol
            ORDER BY
                (d.calendar_revision LIKE '2025:%') DESC,
                d.id DESC
        ) AS keep_id
    FROM stock.source_d0_launch_day_facts d
)
SELECT id AS old_id, keep_id
FROM ranked_d0
WHERE id <> keep_id;

UPDATE stock.source_research_enrollments e
SET d0_fact_id = m.keep_id
FROM _d0_launch_dedup_map m
WHERE e.d0_fact_id = m.old_id;

UPDATE stock.source_research_enrollments e
SET adjacent_d0_fact_id = m.keep_id
FROM _d0_launch_dedup_map m
WHERE e.adjacent_d0_fact_id = m.old_id;

UPDATE stock.source_t0_early_observation_facts t
SET d0_fact_id = m.keep_id
FROM _d0_launch_dedup_map m
WHERE t.d0_fact_id = m.old_id;

DELETE FROM stock.source_d0_launch_day_facts d
USING _d0_launch_dedup_map m
WHERE d.id = m.old_id;

DROP INDEX IF EXISTS stock.source_d0_launch_session_package_symbol_idx;
ALTER TABLE stock.source_d0_launch_day_facts
    DROP CONSTRAINT IF EXISTS source_d0_launch_business_uq,
    ADD CONSTRAINT source_d0_launch_business_uq UNIQUE (target_session, source_symbol);

-- 4.14 stock.source_d0_scenario_day_facts & source_d0_formal_admission_day_facts
DROP INDEX IF EXISTS stock.source_d0_scenario_session_package_scenario_idx;
ALTER TABLE stock.source_d0_scenario_day_facts
    DROP CONSTRAINT IF EXISTS source_d0_scenario_business_uq,
    ADD CONSTRAINT source_d0_scenario_business_uq UNIQUE (target_session, source_symbol, scenario);

DROP INDEX IF EXISTS stock.source_d0_formal_admission_day_status_idx;
DROP INDEX IF EXISTS stock.source_d0_formal_admission_symbol_day_idx;
ALTER TABLE stock.source_d0_formal_admission_day_facts
    DROP CONSTRAINT IF EXISTS source_d0_formal_admission_unique_input,
    ADD CONSTRAINT source_d0_formal_admission_unique_input UNIQUE (target_session, source_symbol, scenario);
CREATE INDEX IF NOT EXISTS source_d0_formal_admission_day_status_idx
    ON stock.source_d0_formal_admission_day_facts (target_session, admission_status, source_symbol);
CREATE INDEX IF NOT EXISTS source_d0_formal_admission_symbol_day_idx
    ON stock.source_d0_formal_admission_day_facts (source_symbol, target_session DESC, admission_status);

-- 4.15 stock.source_d1_structure_day_facts
DROP INDEX IF EXISTS stock.source_d1_structure_session_package_symbol_idx;
ALTER TABLE stock.source_d1_structure_day_facts
    DROP CONSTRAINT IF EXISTS source_d1_structure_business_uq,
    DROP COLUMN IF EXISTS cross_section_universe_capture_id,
    DROP COLUMN IF EXISTS cross_section_universe_version,
    DROP COLUMN IF EXISTS cross_section_contract_version,
    DROP COLUMN IF EXISTS universe_match_status,
    ADD CONSTRAINT source_d1_structure_business_uq UNIQUE (target_session, source_symbol, structure_type);

-- 4.16 stock.source_rule_evaluations
DROP INDEX IF EXISTS stock.source_rule_evaluations_adjacent_lookup_idx;
DROP INDEX IF EXISTS stock.source_rule_evaluations_session_package_rule_idx;
ALTER TABLE stock.source_rule_evaluations
    DROP CONSTRAINT IF EXISTS source_rule_evaluations_business_uq,
    ADD CONSTRAINT source_rule_evaluations_business_uq UNIQUE (target_session, source_symbol, rule_id);
CREATE INDEX IF NOT EXISTS source_rule_evaluations_adjacent_lookup_idx
    ON stock.source_rule_evaluations (target_session, source_symbol, rule_id);
CREATE INDEX IF NOT EXISTS source_rule_evaluations_session_rule_idx
    ON stock.source_rule_evaluations (target_session, rule_id, state);

-- 4.17 stock.source_sector_cross_period_pulse_facts & followup & cohort_group
ALTER TABLE stock.source_sector_cross_period_pulse_facts
    DROP CONSTRAINT IF EXISTS source_sector_cross_period_pulse_business_uq,
    DROP COLUMN IF EXISTS profile_revision,
    ADD CONSTRAINT source_sector_cross_period_pulse_business_uq UNIQUE (target_session, catalog_id);

ALTER TABLE stock.source_sector_pulse_followup_facts
    DROP CONSTRAINT IF EXISTS source_sector_pulse_followup_business_uq,
    ADD CONSTRAINT source_sector_pulse_followup_business_uq UNIQUE (pulse_fact_id, horizon_sessions);

DROP INDEX IF EXISTS stock.source_sector_pulse_cohort_group_read_idx;
ALTER TABLE stock.source_sector_pulse_cohort_group_facts
    DROP CONSTRAINT IF EXISTS source_sector_pulse_cohort_group_business_uq,
    DROP COLUMN IF EXISTS t0_profile_revision,
    ADD CONSTRAINT source_sector_pulse_cohort_group_business_uq
        UNIQUE (t0_session, research_cutoff, board_kind, cohort_axis, cohort_state, horizon_sessions);
CREATE INDEX IF NOT EXISTS source_sector_pulse_cohort_group_read_idx
    ON stock.source_sector_pulse_cohort_group_facts (t0_session, research_cutoff, board_kind, cohort_axis, horizon_sessions);
