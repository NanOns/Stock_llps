-- R03 全阶段第一轮集中收尾：补齐 Native/Rotation/Overlap/LOO 必要解释与质量列及任务进度精确依赖引用（0085）。

-- 1. stock.r03_sector_native_day_facts（C02 / C04 / C08）
ALTER TABLE stock.r03_sector_native_day_facts
    ADD COLUMN IF NOT EXISTS membership_complete boolean NOT NULL DEFAULT true,
    ALTER COLUMN joined_count DROP NOT NULL,
    ALTER COLUMN joined_count DROP DEFAULT,
    ALTER COLUMN exited_count DROP NOT NULL,
    ALTER COLUMN exited_count DROP DEFAULT,
    ADD COLUMN IF NOT EXISTS membership_delta_status text NOT NULL DEFAULT 'UNKNOWN',
    ADD COLUMN IF NOT EXISTS rs5_peer_count integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS rs20_peer_count integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS seed_k integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS seed_f integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS seed_u integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS quote_coverage numeric NULL,
    ADD COLUMN IF NOT EXISTS breadth_delta1_common_count integer NULL,
    ADD COLUMN IF NOT EXISTS breadth_delta3_common_count integer NULL,
    ADD COLUMN IF NOT EXISTS ma20_delta3_common_count integer NULL,
    ADD COLUMN IF NOT EXISTS concentration_status text NOT NULL DEFAULT 'UNKNOWN',
    ADD COLUMN IF NOT EXISTS concentration_reason text NULL,
    ADD COLUMN IF NOT EXISTS safety_reason text NULL,
    ADD COLUMN IF NOT EXISTS prewatch_raw_reason text NULL,
    ADD COLUMN IF NOT EXISTS warm_raw_reason text NULL,
    ADD COLUMN IF NOT EXISTS confirmed_raw_reason text NULL,
    ADD COLUMN IF NOT EXISTS exhaustion_reason text NULL,
    ADD COLUMN IF NOT EXISTS emergence_reason text NULL,
    ADD COLUMN IF NOT EXISTS rs5_reason text NULL,
    ADD COLUMN IF NOT EXISTS rs20_reason text NULL,
    ADD COLUMN IF NOT EXISTS breadth_reason text NULL,
    ADD COLUMN IF NOT EXISTS ma20_reason text NULL,
    ADD COLUMN IF NOT EXISTS amr20_reason text NULL,
    ADD COLUMN IF NOT EXISTS prev_trade_date date NULL,
    ADD COLUMN IF NOT EXISTS session_gap integer NULL,
    ADD COLUMN IF NOT EXISTS prev_exhaustion_level text NULL;

ALTER TABLE stock.r03_sector_native_day_facts
    DROP CONSTRAINT IF EXISTS r03_sector_native_day_facts_membership_delta_status_check,
    ADD CONSTRAINT r03_sector_native_day_facts_membership_delta_status_check
        CHECK (membership_delta_status IN ('OBSERVED', 'UNKNOWN')),
    DROP CONSTRAINT IF EXISTS r03_sector_native_day_facts_rs5_peer_count_check,
    ADD CONSTRAINT r03_sector_native_day_facts_rs5_peer_count_check
        CHECK (rs5_peer_count >= 0),
    DROP CONSTRAINT IF EXISTS r03_sector_native_day_facts_rs20_peer_count_check,
    ADD CONSTRAINT r03_sector_native_day_facts_rs20_peer_count_check
        CHECK (rs20_peer_count >= 0),
    DROP CONSTRAINT IF EXISTS r03_sector_native_day_facts_seed_k_check,
    ADD CONSTRAINT r03_sector_native_day_facts_seed_k_check CHECK (seed_k >= 0),
    DROP CONSTRAINT IF EXISTS r03_sector_native_day_facts_seed_f_check,
    ADD CONSTRAINT r03_sector_native_day_facts_seed_f_check CHECK (seed_f >= 0),
    DROP CONSTRAINT IF EXISTS r03_sector_native_day_facts_seed_u_check,
    ADD CONSTRAINT r03_sector_native_day_facts_seed_u_check CHECK (seed_u >= 0),
    DROP CONSTRAINT IF EXISTS r03_sector_native_day_facts_quote_coverage_check,
    ADD CONSTRAINT r03_sector_native_day_facts_quote_coverage_check
        CHECK (quote_coverage IS NULL OR (quote_coverage >= 0 AND quote_coverage <= 1)),
    DROP CONSTRAINT IF EXISTS r03_sector_native_day_facts_bd1_common_check,
    ADD CONSTRAINT r03_sector_native_day_facts_bd1_common_check
        CHECK (breadth_delta1_common_count IS NULL OR breadth_delta1_common_count >= 0),
    DROP CONSTRAINT IF EXISTS r03_sector_native_day_facts_bd3_common_check,
    ADD CONSTRAINT r03_sector_native_day_facts_bd3_common_check
        CHECK (breadth_delta3_common_count IS NULL OR breadth_delta3_common_count >= 0),
    DROP CONSTRAINT IF EXISTS r03_sector_native_day_facts_ma20d3_common_check,
    ADD CONSTRAINT r03_sector_native_day_facts_ma20d3_common_check
        CHECK (ma20_delta3_common_count IS NULL OR ma20_delta3_common_count >= 0),
    DROP CONSTRAINT IF EXISTS r03_sector_native_day_facts_concentration_status_check,
    ADD CONSTRAINT r03_sector_native_day_facts_concentration_status_check
        CHECK (concentration_status IN ('OBSERVED', 'NA', 'UNKNOWN')),
    DROP CONSTRAINT IF EXISTS r03_sector_native_day_facts_session_gap_check,
    ADD CONSTRAINT r03_sector_native_day_facts_session_gap_check
        CHECK (session_gap IS NULL OR session_gap >= 1),
    DROP CONSTRAINT IF EXISTS r03_sector_native_day_facts_prev_exhaustion_level_check,
    ADD CONSTRAINT r03_sector_native_day_facts_prev_exhaustion_level_check
        CHECK (prev_exhaustion_level IS NULL OR prev_exhaustion_level IN ('HIGH', 'MEDIUM', 'LOW', 'UNKNOWN'));

COMMENT ON COLUMN stock.r03_sector_native_day_facts.membership_complete IS '当日同类型同来源声明成员清单是否完整可证（true/false）。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.membership_delta_status IS '跨t-3成员进出比较状态（OBSERVED=双端完整可比，UNKNOWN=任一端缺失/未映射/未知）。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.rs5_peer_count IS 'RS5 同类百分位实际可评价 peer 分母数量（>=0）。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.rs20_peer_count IS 'RS20 同类百分位实际可评价 peer 分母数量（>=0）。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.seed_k IS '完整声明成员中已知 Seed TRUE 数量（k）。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.seed_f IS '完整声明成员中已知 Seed FALSE 数量（f）。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.seed_u IS '完整声明成员中 Seed 状态未知或未映射槽位数量（u）。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.quote_coverage IS '同目标坐标合法 ACTUAL OHLC 成员数占完整声明成员数比例 [0,1]，不可证时为 NULL。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.breadth_delta1_common_count IS 't 与 t-1 共同可评价成员集合 E_delta1 数量。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.breadth_delta3_common_count IS 't 与 t-3 共同可评价成员集合 E_delta3 数量。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.ma20_delta3_common_count IS 't 与 t-3 共同可评价 MA20 成员集合 E_ma20_delta3 数量。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.concentration_status IS '成交额集中度（top1/top3）状态：OBSERVED/NA（零总额）/UNKNOWN。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.concentration_reason IS '成交额集中度状态或不可用原因码。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.safety_reason IS '板块安全门（safety_state）完整判定原因码。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.prewatch_raw_reason IS 'PREWATCH 原始资格谓词原因码（含未通过项）。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.warm_raw_reason IS 'WARM 原始资格谓词原因码（含未通过项）。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.confirmed_raw_reason IS 'CONFIRMED 原始资格谓词原因码（含未通过项）。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.exhaustion_reason IS '板块衰竭风险（exhaustion_level）原因码。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.emergence_reason IS '板块涌现等级（emergence_level）原因码。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.rs5_reason IS 'RS5 原语质量/缺口原因码。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.rs20_reason IS 'RS20 原语质量/缺口原因码。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.breadth_reason IS '收益宽度原语质量/缺口原因码。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.ma20_reason IS 'MA20 宽度原语质量/缺口原因码。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.amr20_reason IS '量比中位数原语质量/缺口原因码。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.prev_trade_date IS '成熟度/健康/风险变化实际消费的上一官方交易日；首次观察为 NULL。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.session_gap IS '相对 prev_trade_date 的官方交易日间隔（>=1）。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.prev_exhaustion_level IS '上一可比日衰竭风险等级，用于表达风险变化（RISK_UP/DOWN/UNCHANGED/FIRST_OBSERVED）。';

-- 2. stock.r03_sector_rotation_day_states（C05 / C06）
ALTER TABLE stock.r03_sector_rotation_day_states
    ADD COLUMN IF NOT EXISTS prev_trade_date date NULL,
    ADD COLUMN IF NOT EXISTS session_gap integer NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS price_retention numeric NULL,
    ADD COLUMN IF NOT EXISTS price_retention_state text NOT NULL DEFAULT 'UNKNOWN',
    ADD COLUMN IF NOT EXISTS price_retention_reason text NULL,
    ADD COLUMN IF NOT EXISTS valuation_reason text NULL,
    ADD COLUMN IF NOT EXISTS seed_retention_state text NOT NULL DEFAULT 'UNKNOWN',
    ADD COLUMN IF NOT EXISTS seed_retention_reason text NULL,
    ADD COLUMN IF NOT EXISTS breadth_retention_state text NOT NULL DEFAULT 'UNKNOWN',
    ADD COLUMN IF NOT EXISTS breadth_retention_reason text NULL,
    ADD COLUMN IF NOT EXISTS strong_retention_state text NOT NULL DEFAULT 'UNKNOWN',
    ADD COLUMN IF NOT EXISTS strong_retention_reason text NULL,
    ADD COLUMN IF NOT EXISTS entered_count integer NULL,
    ADD COLUMN IF NOT EXISTS exited_count integer NULL,
    ADD COLUMN IF NOT EXISTS diffusion_reason text NULL,
    ADD COLUMN IF NOT EXISTS early_reason text NULL,
    ADD COLUMN IF NOT EXISTS mature_reason text NULL;

ALTER TABLE stock.r03_sector_rotation_day_states
    DROP CONSTRAINT IF EXISTS r03_sector_rotation_day_states_session_gap_check,
    ADD CONSTRAINT r03_sector_rotation_day_states_session_gap_check CHECK (session_gap >= 1),
    DROP CONSTRAINT IF EXISTS r03_sector_rotation_day_states_price_retention_state_check,
    ADD CONSTRAINT r03_sector_rotation_day_states_price_retention_state_check
        CHECK (price_retention_state IN ('OBSERVED', 'NA', 'UNKNOWN')),
    DROP CONSTRAINT IF EXISTS r03_sector_rotation_day_states_valuation_quality_check,
    ADD CONSTRAINT r03_sector_rotation_day_states_valuation_quality_check
        CHECK (valuation_quality IN ('OBSERVED', 'MARKED_ESTIMATE', 'NA', 'UNKNOWN')),
    DROP CONSTRAINT IF EXISTS r03_sector_rotation_day_states_seed_retention_state_check,
    ADD CONSTRAINT r03_sector_rotation_day_states_seed_retention_state_check
        CHECK (seed_retention_state IN ('OBSERVED', 'NA', 'UNKNOWN')),
    DROP CONSTRAINT IF EXISTS r03_sector_rotation_day_states_breadth_retention_state_check,
    ADD CONSTRAINT r03_sector_rotation_day_states_breadth_retention_state_check
        CHECK (breadth_retention_state IN ('OBSERVED', 'NA', 'UNKNOWN')),
    DROP CONSTRAINT IF EXISTS r03_sector_rotation_day_states_strong_retention_state_check,
    ADD CONSTRAINT r03_sector_rotation_day_states_strong_retention_state_check
        CHECK (strong_retention_state IN ('OBSERVED', 'NA', 'UNKNOWN')),
    DROP CONSTRAINT IF EXISTS r03_sector_rotation_day_states_entered_count_check,
    ADD CONSTRAINT r03_sector_rotation_day_states_entered_count_check
        CHECK (entered_count IS NULL OR entered_count >= 0),
    DROP CONSTRAINT IF EXISTS r03_sector_rotation_day_states_exited_count_check,
    ADD CONSTRAINT r03_sector_rotation_day_states_exited_count_check
        CHECK (exited_count IS NULL OR exited_count >= 0);

COMMENT ON COLUMN stock.r03_sector_rotation_day_states.prev_trade_date IS '当前轮动状态实际恢复的前态官方交易日。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.session_gap IS '相对 prev_trade_date 的官方市场交易日间隔（>=1）。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.price_retention IS '价格留存比 formation_R(u)/formation_R(t0)；出生形成收益<=0 时为 NA(NULL)。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.price_retention_state IS '价格留存状态：OBSERVED/NA/UNKNOWN。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.price_retention_reason IS '价格留存状态或不可用原因码。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.valuation_reason IS '篮子估值质量（如停牌估值、超期、缺价）原因码。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.seed_retention_state IS 'Seed 留存状态：OBSERVED/NA(S0空集)/UNKNOWN。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.seed_retention_reason IS 'Seed 留存原因码。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.breadth_retention_state IS 'Breadth 留存状态：OBSERVED/NA(U0空集)/UNKNOWN。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.breadth_retention_reason IS 'Breadth 留存原因码。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.strong_retention_state IS 'Strong 留存状态：OBSERVED/NA(H0空集)/UNKNOWN。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.strong_retention_reason IS 'Strong 留存原因码。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.entered_count IS '固定篮子 B 相邻可评估会话 Seed 新进数量；未知时为 NULL。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.exited_count IS '固定篮子 B 相邻可评估会话 Seed 退出数量；未知时为 NULL。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.diffusion_reason IS '扩散判定（diffusion_state）原因码。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.early_reason IS '早期确认（early_state）原因码。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.mature_reason IS '成熟确认（mature_state）原因码。';

-- 3. stock.r03_sector_rotation_board_day_facts（C06）
ALTER TABLE stock.r03_sector_rotation_board_day_facts
    ADD COLUMN IF NOT EXISTS prev_trade_date date NULL,
    ADD COLUMN IF NOT EXISTS session_gap integer NULL;

ALTER TABLE stock.r03_sector_rotation_board_day_facts
    DROP CONSTRAINT IF EXISTS r03_sector_rotation_board_day_facts_rotation_state_check,
    ADD CONSTRAINT r03_sector_rotation_board_day_facts_rotation_state_check
        CHECK (rotation_state IN ('NONE', 'PULSE', 'IN', 'ACCEPTED', 'EXPANDING', 'REACCELERATING', 'FAILED', 'OUT', 'UNKNOWN', 'REJECTED')),
    DROP CONSTRAINT IF EXISTS r03_sector_rotation_board_day_facts_last_known_state_check,
    ADD CONSTRAINT r03_sector_rotation_board_day_facts_last_known_state_check
        CHECK (last_known_state IS NULL OR last_known_state IN ('NONE', 'PULSE', 'IN', 'ACCEPTED', 'EXPANDING', 'REACCELERATING', 'FAILED', 'OUT')),
    DROP CONSTRAINT IF EXISTS r03_sector_rotation_board_day_facts_session_gap_check,
    ADD CONSTRAINT r03_sector_rotation_board_day_facts_session_gap_check
        CHECK (session_gap IS NULL OR session_gap >= 1);

COMMENT ON COLUMN stock.r03_sector_rotation_board_day_facts.prev_trade_date IS '板块日级轮动事实实际恢复的前态官方交易日。';
COMMENT ON COLUMN stock.r03_sector_rotation_board_day_facts.session_gap IS '相对 prev_trade_date 的官方交易日间隔（>=1）。';

-- 4. stock.r03_sector_overlap_pair_facts & r03_sector_overlap_clusters（C08）
ALTER TABLE stock.r03_sector_overlap_pair_facts
    DROP CONSTRAINT IF EXISTS r03_sector_overlap_pair_facts_jaccard_status_check,
    ADD CONSTRAINT r03_sector_overlap_pair_facts_jaccard_status_check
        CHECK (jaccard_status IN ('OBSERVED', 'NA', 'UNKNOWN'));

ALTER TABLE stock.r03_sector_overlap_clusters
    ADD COLUMN IF NOT EXISTS cluster_reason text NULL,
    ADD COLUMN IF NOT EXISTS representative_reason text NULL,
    ADD COLUMN IF NOT EXISTS unknown_edge_count integer NOT NULL DEFAULT 0;

ALTER TABLE stock.r03_sector_overlap_clusters
    DROP CONSTRAINT IF EXISTS r03_sector_overlap_clusters_unknown_edge_count_check,
    ADD CONSTRAINT r03_sector_overlap_clusters_unknown_edge_count_check
        CHECK (unknown_edge_count >= 0);

COMMENT ON COLUMN stock.r03_sector_overlap_clusters.cluster_reason IS '概念簇形成或未知原因码（含跨分量未知边阻塞）。';
COMMENT ON COLUMN stock.r03_sector_overlap_clusters.representative_reason IS '簇代表选择或未知原因码。';
COMMENT ON COLUMN stock.r03_sector_overlap_clusters.unknown_edge_count IS '与本簇相关的未知 Jaccard 边数量。';

-- 5. stock.r03_sector_loo_support_facts & r03_sector_loo_candidate_facts（C07）
ALTER TABLE stock.r03_sector_loo_support_facts
    ADD COLUMN IF NOT EXISTS candidate_count integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS qualified_candidate_count integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS matches_primary_industry boolean NULL;

ALTER TABLE stock.r03_sector_loo_support_facts
    DROP CONSTRAINT IF EXISTS r03_sector_loo_support_facts_candidate_count_check,
    ADD CONSTRAINT r03_sector_loo_support_facts_candidate_count_check CHECK (candidate_count >= 0),
    DROP CONSTRAINT IF EXISTS r03_sector_loo_support_facts_qualified_candidate_count_check,
    ADD CONSTRAINT r03_sector_loo_support_facts_qualified_candidate_count_check CHECK (qualified_candidate_count >= 0);

ALTER TABLE stock.r03_sector_loo_candidate_facts
    ADD COLUMN IF NOT EXISTS is_selected_support boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS breadth_delta3 numeric NULL,
    ADD COLUMN IF NOT EXISTS ma20_delta3 numeric NULL,
    ADD COLUMN IF NOT EXISTS top1_concentration numeric NULL;

COMMENT ON COLUMN stock.r03_sector_loo_support_facts.candidate_count IS '目标证券所属同类型板块候选总数。';
COMMENT ON COLUMN stock.r03_sector_loo_support_facts.qualified_candidate_count IS '剔除目标后仍满足安全门与合格条件的候选板块数。';
COMMENT ON COLUMN stock.r03_sector_loo_support_facts.matches_primary_industry IS '行业域算法支持板块是否与当前主行业一致。';
COMMENT ON COLUMN stock.r03_sector_loo_candidate_facts.is_selected_support IS '该候选板块是否为本目标证券最终选中的算法支持板块。';
COMMENT ON COLUMN stock.r03_sector_loo_candidate_facts.breadth_delta3 IS '剔除目标后候选板块3日收益宽度变化。';
COMMENT ON COLUMN stock.r03_sector_loo_candidate_facts.ma20_delta3 IS '剔除目标后候选板块3日MA20宽度变化。';
COMMENT ON COLUMN stock.r03_sector_loo_candidate_facts.top1_concentration IS '剔除目标后候选板块最大单股成交额占比。';

-- 6. stock.data_job_progress（C09 / C10 / C11）
ALTER TABLE stock.data_job_progress
    ADD COLUMN IF NOT EXISTS prev_trade_date date NULL,
    ADD COLUMN IF NOT EXISTS industry_snapshot_id_t1 bigint NULL,
    ADD COLUMN IF NOT EXISTS concept_snapshot_id_t1 bigint NULL,
    ADD COLUMN IF NOT EXISTS industry_snapshot_id_t3 bigint NULL,
    ADD COLUMN IF NOT EXISTS concept_snapshot_id_t3 bigint NULL,
    ADD COLUMN IF NOT EXISTS source_capture_id_t4 bigint NULL,
    ADD COLUMN IF NOT EXISTS prev_session_t date NULL,
    ADD COLUMN IF NOT EXISTS prev_session_t3 date NULL;

CREATE INDEX IF NOT EXISTS data_job_progress_r03_prev_chain_ix
    ON stock.data_job_progress (
        business_table,
        prev_trade_date,
        prev_scope_key,
        prev_member_version,
        producer_version
    )
    WHERE prev_trade_date IS NOT NULL;

COMMENT ON COLUMN stock.data_job_progress.prev_trade_date IS '当前日级 R03 分区实际消费的前态官方交易日；用于前态依赖链精确递推。';
COMMENT ON COLUMN stock.data_job_progress.industry_snapshot_id_t1 IS 't-1 端点实际消费的 TDX 行业快照 ID。';
COMMENT ON COLUMN stock.data_job_progress.concept_snapshot_id_t1 IS 't-1 端点实际消费的 TDX 概念快照 ID。';
COMMENT ON COLUMN stock.data_job_progress.industry_snapshot_id_t3 IS 't-3 端点实际消费的 TDX 行业快照 ID；用于跨版成员依赖精确失效。';
COMMENT ON COLUMN stock.data_job_progress.concept_snapshot_id_t3 IS 't-3 端点实际消费的 TDX 概念快照 ID；用于跨版成员依赖精确失效。';
COMMENT ON COLUMN stock.data_job_progress.source_capture_id_t4 IS '正式市场宽度缺 Profile 回退 raw ret1 时，t-4 端点实际消费的来源捕获 ID。';
COMMENT ON COLUMN stock.data_job_progress.prev_session_t IS '正式市场宽度计算 t 日 ret1 实际消费的前一官方交易日。';
COMMENT ON COLUMN stock.data_job_progress.prev_session_t3 IS '正式市场宽度计算 t-3 日 ret1 实际消费的前一官方交易日。';
