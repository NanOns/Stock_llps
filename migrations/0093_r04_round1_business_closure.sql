-- R04 第一轮综合业务闭环前向迁移（0093）：
-- 1. 扩展 source_pool_seed_day_facts，增加安全门后正式 Seed 与 PREWATCH 字段；
-- 2. 扩展 source_d0_scenario_day_facts 与 source_d0_formal_admission_day_facts 至四场景及冻结线来源；
-- 3. 扩展 source_d1_structure_day_facts 至十类结构，并在 source_d1_material_anchor_day_facts 增加中阳留存率；
-- 4. 新建正式 D2 生命周期表群（d2_episodes、d2_episode_day_states、d2_primary_episode_day_facts、d2_parent_peak_events）；
-- 5. 新建股票级风险、Priority、Radar 与 Near-Miss 最小 typed 事实表群。

-- ============================================================================
-- 1. stock.source_pool_seed_day_facts 正式 Seed / PREWATCH 扩展
-- ============================================================================
ALTER TABLE stock.source_pool_seed_day_facts
    ADD COLUMN formal_universe_state text NOT NULL DEFAULT 'UNKNOWN'
        CHECK (formal_universe_state IN ('TRUE', 'FALSE', 'UNKNOWN')),
    ADD COLUMN formal_universe_reason text,
    ADD COLUMN formal_seed_state text NOT NULL DEFAULT 'UNKNOWN'
        CHECK (formal_seed_state IN ('TRUE', 'FALSE', 'UNKNOWN')),
    ADD COLUMN formal_seed_reasons text[] NOT NULL DEFAULT ARRAY[]::text[],
    ADD COLUMN formal_prewatch_state text NOT NULL DEFAULT 'UNKNOWN'
        CHECK (formal_prewatch_state IN ('TRUE', 'FALSE', 'UNKNOWN')),
    ADD COLUMN formal_prewatch_reasons text[] NOT NULL DEFAULT ARRAY[]::text[];

COMMENT ON COLUMN stock.source_pool_seed_day_facts.formal_universe_state IS '正式研究总体门三态（TRUE/FALSE/UNKNOWN）；主板与创业板含ST合法进入，科创北交排除。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.formal_universe_reason IS '正式研究总体门原因码；NULL表示门已明确为TRUE无阻断原因。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.formal_seed_state IS '安全门后正式BaseSeed三态（TRUE/FALSE/UNKNOWN）；区别于仅反映形态的source_seed_state。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.formal_seed_reasons IS '正式BaseSeed判定原因码数组；空数组表示无阻断。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.formal_prewatch_state IS '正式PREWATCH（Seed AND mandatory_core_quality_ready）三态（TRUE/FALSE/UNKNOWN）。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.formal_prewatch_reasons IS '正式PREWATCH判定原因码数组；空数组表示无阻断。';

-- ============================================================================
-- 2. stock.source_d0_scenario_day_facts 四场景支持
-- ============================================================================
ALTER TABLE stock.source_d0_scenario_day_facts
    DROP CONSTRAINT IF EXISTS source_d0_scenario_day_facts_scenario_ck,
    DROP CONSTRAINT IF EXISTS source_d0_scenario_day_facts_vector_ck;

ALTER TABLE stock.source_d0_scenario_day_facts
    ADD CONSTRAINT source_d0_scenario_day_facts_scenario_ck
        CHECK (scenario IN ('LAUNCH_CONFIRM', 'RECOVERY_TURN', 'STRONG_PULLBACK', 'TREND_CONTINUE')),
    ADD CONSTRAINT source_d0_scenario_day_facts_vector_ck
        CHECK (
            cardinality(predicate_ids) = cardinality(predicate_states)
            AND cardinality(predicate_ids) = cardinality(predicate_qualities)
            AND cardinality(predicate_ids) = cardinality(predicate_reason_codes)
            AND cardinality(predicate_ids) = cardinality(predicate_value_numerators)
            AND cardinality(predicate_ids) = cardinality(predicate_value_denominators)
            AND array_position(predicate_ids, NULL::text) IS NULL
            AND array_position(predicate_states, NULL::text) IS NULL
            AND array_position(predicate_qualities, NULL::text) IS NULL
            AND array_position(predicate_reason_codes, NULL::text) IS NULL
            AND cardinality(predicate_ids) >= 1
        );

COMMENT ON COLUMN stock.source_d0_scenario_day_facts.scenario IS 'D0场景类型：LAUNCH_CONFIRM/RECOVERY_TURN/STRONG_PULLBACK/TREND_CONTINUE。';

-- ============================================================================
-- 3. stock.source_d0_formal_admission_day_facts 四场景与冻结线支持
-- ============================================================================
ALTER TABLE stock.source_d0_formal_admission_day_facts
    DROP CONSTRAINT IF EXISTS source_d0_formal_admission_day_facts_scenario_check,
    DROP CONSTRAINT IF EXISTS source_d0_formal_admission_line_ck,
    ADD COLUMN prior_episode_id text,
    ADD COLUMN linked_anchor_id text;

ALTER TABLE stock.source_d0_formal_admission_day_facts
    ADD CONSTRAINT source_d0_formal_admission_day_facts_scenario_check
        CHECK (scenario IN ('LAUNCH_CONFIRM', 'RECOVERY_TURN', 'STRONG_PULLBACK', 'TREND_CONTINUE')),
    ADD CONSTRAINT source_d0_formal_admission_line_ck
        CHECK (
            (
                admission_status = 'READY'
                AND raw_role = 'FORMAL'
                AND raw_state = 'TRUE'
                AND failure_level_numerator IS NOT NULL
                AND failure_level_denominator IS NOT NULL
                AND failure_level_denominator > 0
                AND failure_coordinate IN ('RAW_UNADJUSTED', 'FORWARD_ADJUSTED')
                AND failure_source IN ('T0_PHC20', 'T0_MA20', 'T0_MA5', 'PRIOR_SUPPORT_LOWER', 'T0_MA20_FALLBACK')
                AND failure_window_start IS NOT NULL
                AND failure_window_session_count >= 1
                AND failure_source_event_ref IS NOT NULL
                AND event_id IS NOT NULL
                AND effective_from_session > target_session
                AND research_universe_state = 'TRUE'
                AND actual_quote_state = 'TRUE'
                AND coordinate_window_state = 'TRUE'
                AND d0_liquidity_state = 'TRUE'
                AND non_core_damage_state = 'TRUE'
                AND non_extreme_extension_state = 'TRUE'
                AND non_first_day_damage_state = 'TRUE'
                AND non_severe_drop_state = 'TRUE'
            )
            OR
            (
                admission_status <> 'READY'
                AND event_id IS NULL
                AND failure_level_numerator IS NULL
                AND failure_level_denominator IS NULL
                AND failure_coordinate IS NULL
                AND failure_source IS NULL
                AND failure_window_start IS NULL
                AND failure_window_session_count IS NULL
                AND failure_source_event_ref IS NULL
                AND effective_from_session IS NULL
            )
        );

COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.scenario IS '正式准入D0场景：LAUNCH_CONFIRM/RECOVERY_TURN/STRONG_PULLBACK/TREND_CONTINUE。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.prior_episode_id IS 'PB/CONTINUE关联的t-1合法FORMAL父episode业务ID；LAUNCH/RECOVERY为NULL。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.linked_anchor_id IS 'PB/CONTINUE冻结关联的D1旧支撑锚ID；无关联旧锚时为NULL。';

-- ============================================================================
-- 4. stock.source_d1_structure_day_facts 十类结构与材料锚留存扩展
-- ============================================================================
ALTER TABLE stock.source_d1_structure_day_facts
    DROP CONSTRAINT IF EXISTS source_d1_structure_day_facts_structure_type_check;

ALTER TABLE stock.source_d1_structure_day_facts
    ADD CONSTRAINT source_d1_structure_day_facts_structure_type_check
        CHECK (
            structure_type IN (
                'PIVOT_LOW',
                'PIVOT_HIGH',
                'RANGE_FLOOR',
                'RANGE_CEILING',
                'GAP_ZONE',
                'IMPULSE_BODY',
                'IMPULSE_LOW',
                'BREAKOUT_PLATFORM',
                'DYNAMIC_MA5',
                'DYNAMIC_MA20'
            )
        );

COMMENT ON COLUMN stock.source_d1_structure_day_facts.structure_type IS 'D1十类结构锚点类型：PIVOT_LOW/HIGH、RANGE_FLOOR/CEILING、GAP_ZONE、IMPULSE_BODY/LOW、BREAKOUT_PLATFORM、DYNAMIC_MA5/20。';

ALTER TABLE stock.source_d1_material_anchor_day_facts
    ADD COLUMN impulse_retention_1_numerator numeric,
    ADD COLUMN impulse_retention_1_denominator numeric,
    ADD COLUMN impulse_retention_1_quality text NOT NULL DEFAULT 'UNKNOWN'
        CHECK (impulse_retention_1_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_APPLICABLE', 'NOT_DUE')),
    ADD COLUMN impulse_retention_1_reason text,
    ADD COLUMN impulse_retention_3_numerator numeric,
    ADD COLUMN impulse_retention_3_denominator numeric,
    ADD COLUMN impulse_retention_3_quality text NOT NULL DEFAULT 'UNKNOWN'
        CHECK (impulse_retention_3_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_APPLICABLE', 'NOT_DUE')),
    ADD COLUMN impulse_retention_3_reason text,
    ADD CONSTRAINT source_d1_material_anchor_ret1_pair_ck
        CHECK ((impulse_retention_1_numerator IS NULL) = (impulse_retention_1_denominator IS NULL) AND (impulse_retention_1_denominator IS NULL OR impulse_retention_1_denominator > 0)),
    ADD CONSTRAINT source_d1_material_anchor_ret3_pair_ck
        CHECK ((impulse_retention_3_numerator IS NULL) = (impulse_retention_3_denominator IS NULL) AND (impulse_retention_3_denominator IS NULL OR impulse_retention_3_denominator > 0));

COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.impulse_retention_1_numerator IS '中阳线t0+1收盘留存率分子：(C[t0+1]-base)；非中阳或未到期/未知时NULL。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.impulse_retention_1_denominator IS '中阳线t0+1收盘留存率分母：(C[t0]-base)>0。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.impulse_retention_1_quality IS '中阳线t0+1留存率质量：OBSERVED/UNKNOWN/NOT_APPLICABLE/NOT_DUE。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.impulse_retention_1_reason IS '中阳线t0+1留存率不可观测原因码。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.impulse_retention_3_numerator IS '中阳线t0+3收盘留存率分子：(C[t0+3]-base)；非中阳或未到期/未知时NULL。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.impulse_retention_3_denominator IS '中阳线t0+3收盘留存率分母：(C[t0]-base)>0。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.impulse_retention_3_quality IS '中阳线t0+3留存率质量：OBSERVED/UNKNOWN/NOT_APPLICABLE/NOT_DUE。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.impulse_retention_3_reason IS '中阳线t0+3留存率不可观测原因码。';

-- ============================================================================
-- 5. D2 正式生命周期表群（episode、逐日状态、主选择、父峰）
-- ============================================================================
CREATE TABLE stock.d2_episodes (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    episode_id text NOT NULL UNIQUE,
    entity_type text NOT NULL CHECK (entity_type IN ('STOCK', 'SECTOR')),
    entity_id text NOT NULL,
    calendar_id text NOT NULL,
    rule_identity text NOT NULL,
    scenario text NOT NULL CHECK (scenario IN ('LAUNCH_CONFIRM', 'RECOVERY_TURN', 'STRONG_PULLBACK', 'TREND_CONTINUE', 'CORE_EARLY')),
    kind text NOT NULL CHECK (kind IN ('CORE', 'FORMAL')),
    birth_session date NOT NULL,
    birth_ordinal integer NOT NULL CHECK (birth_ordinal >= 0),
    parent_episode_id text,
    event_id text,
    failure_level_numerator numeric,
    failure_level_denominator numeric,
    failure_coordinate text,
    failure_source text,
    effective_from_session date,
    linked_anchor_id text,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT d2_episodes_failure_pair_ck
        CHECK ((failure_level_numerator IS NULL) = (failure_level_denominator IS NULL) AND (failure_level_denominator IS NULL OR failure_level_denominator > 0))
);

COMMENT ON TABLE stock.d2_episodes IS 'D2正式与核心研究episode主档表：保存唯一episode_id、T0冻结失效线及父子血缘。';
COMMENT ON COLUMN stock.d2_episodes.id IS 'episode内部自增主键ID。';
COMMENT ON COLUMN stock.d2_episodes.episode_id IS '短业务唯一episode标识符。';
COMMENT ON COLUMN stock.d2_episodes.entity_type IS '实体类型：STOCK或SECTOR。';
COMMENT ON COLUMN stock.d2_episodes.entity_id IS '实体代码（如SH.600000）。';
COMMENT ON COLUMN stock.d2_episodes.calendar_id IS '交易日历标识。';
COMMENT ON COLUMN stock.d2_episodes.rule_identity IS 'D2状态机规则合同标识（仅溯源）。';
COMMENT ON COLUMN stock.d2_episodes.scenario IS '触发场景：LAUNCH_CONFIRM/RECOVERY_TURN/STRONG_PULLBACK/TREND_CONTINUE/CORE_EARLY。';
COMMENT ON COLUMN stock.d2_episodes.kind IS 'episode性质：FORMAL正式或CORE早期。';
COMMENT ON COLUMN stock.d2_episodes.birth_session IS 'episode出生交易日。';
COMMENT ON COLUMN stock.d2_episodes.birth_ordinal IS '出生交易日在日历中的序号。';
COMMENT ON COLUMN stock.d2_episodes.parent_episode_id IS '父episode业务ID（分叉/重入子代填写，首代为NULL）。';
COMMENT ON COLUMN stock.d2_episodes.event_id IS '关联正式D0事件ID（CORE早期为NULL）。';
COMMENT ON COLUMN stock.d2_episodes.failure_level_numerator IS 'T0冻结硬失效线分子。';
COMMENT ON COLUMN stock.d2_episodes.failure_level_denominator IS 'T0冻结硬失效线分母（>0）。';
COMMENT ON COLUMN stock.d2_episodes.failure_coordinate IS 'T0冻结失效线价格坐标（RAW_UNADJUSTED/FORWARD_ADJUSTED）。';
COMMENT ON COLUMN stock.d2_episodes.failure_source IS 'T0冻结失效线来源（T0_PHC20/T0_MA20/T0_MA5/PRIOR_SUPPORT_LOWER/T0_MA20_FALLBACK）。';
COMMENT ON COLUMN stock.d2_episodes.effective_from_session IS 'T0冻结失效线生效交易日（严格大于出生日）。';
COMMENT ON COLUMN stock.d2_episodes.linked_anchor_id IS '关联D1旧支撑锚ID。';
COMMENT ON COLUMN stock.d2_episodes.created_at IS '记录创建时间（UTC/含时区）。';

CREATE TABLE stock.d2_episode_day_states (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    entity_type text NOT NULL CHECK (entity_type IN ('STOCK', 'SECTOR')),
    entity_id text NOT NULL,
    calendar_id text NOT NULL,
    session_date date NOT NULL,
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    rule_identity text NOT NULL,
    episode_id text NOT NULL REFERENCES stock.d2_episodes(episode_id) ON DELETE CASCADE,
    coordinate_id text NOT NULL,
    input_ref text NOT NULL,
    prior_mode text NOT NULL CHECK (prior_mode IN ('FIRST_OBSERVATION', 'PREVIOUS_ACCEPTED', 'MODEL_BOUNDARY')),
    event_id text,
    scenario text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('CORE', 'FORMAL')),
    birth_ordinal integer NOT NULL CHECK (birth_ordinal >= 0),
    parent_episode_id text,
    exact_maturity text,
    lower_bound_maturity text NOT NULL,
    display_maturity text NOT NULL,
    effective_eligibility_value boolean,
    effective_eligibility_quality text NOT NULL,
    effective_eligibility_reason text,
    effective_bucket integer,
    validity_value text,
    validity_quality text NOT NULL,
    validity_reason text,
    freshness text NOT NULL,
    health_value text,
    health_quality text NOT NULL,
    health_reason text,
    tracking text NOT NULL,
    downgrade_candidate text,
    downgrade_count integer NOT NULL CHECK (downgrade_count >= 0),
    expiry_count integer NOT NULL CHECK (expiry_count >= 0),
    improvement_baseline_numerator numeric,
    improvement_baseline_denominator numeric,
    exit_ordinal integer,
    exit_reason text,
    reentry_armed boolean NOT NULL,
    transitions text[] NOT NULL DEFAULT ARRAY[]::text[],
    preserved_episode_ids text[] NOT NULL DEFAULT ARRAY[]::text[],
    CONSTRAINT d2_episode_day_states_uq UNIQUE (entity_type, entity_id, calendar_id, session_date, rule_identity, episode_id),
    CONSTRAINT d2_episode_day_states_baseline_ck CHECK ((improvement_baseline_numerator IS NULL) = (improvement_baseline_denominator IS NULL) AND (improvement_baseline_denominator IS NULL OR improvement_baseline_denominator > 0))
);

CREATE INDEX d2_episode_day_states_session_idx
    ON stock.d2_episode_day_states (session_date, entity_type, entity_id);

COMMENT ON TABLE stock.d2_episode_day_states IS 'D2逐episode逐日状态表：保存成熟度、有效性、健康度、降级/过期计数及状态跃迁。';
COMMENT ON COLUMN stock.d2_episode_day_states.id IS '自增主键ID。';
COMMENT ON COLUMN stock.d2_episode_day_states.entity_type IS '实体类型：STOCK或SECTOR。';
COMMENT ON COLUMN stock.d2_episode_day_states.entity_id IS '实体代码。';
COMMENT ON COLUMN stock.d2_episode_day_states.calendar_id IS '交易日历标识。';
COMMENT ON COLUMN stock.d2_episode_day_states.session_date IS '业务交易日。';
COMMENT ON COLUMN stock.d2_episode_day_states.ordinal IS '交易日在日历中的序号。';
COMMENT ON COLUMN stock.d2_episode_day_states.rule_identity IS 'D2状态机规则合同标识。';
COMMENT ON COLUMN stock.d2_episode_day_states.episode_id IS '关联episode业务ID。';
COMMENT ON COLUMN stock.d2_episode_day_states.coordinate_id IS '价格坐标标识。';
COMMENT ON COLUMN stock.d2_episode_day_states.input_ref IS '当日输入溯源引用。';
COMMENT ON COLUMN stock.d2_episode_day_states.prior_mode IS '前态模式：FIRST_OBSERVATION/PREVIOUS_ACCEPTED/MODEL_BOUNDARY。';
COMMENT ON COLUMN stock.d2_episode_day_states.event_id IS '正式事件ID。';
COMMENT ON COLUMN stock.d2_episode_day_states.scenario IS '场景类型。';
COMMENT ON COLUMN stock.d2_episode_day_states.kind IS 'CORE或FORMAL。';
COMMENT ON COLUMN stock.d2_episode_day_states.birth_ordinal IS '出生日序号。';
COMMENT ON COLUMN stock.d2_episode_day_states.parent_episode_id IS '父episode ID。';
COMMENT ON COLUMN stock.d2_episode_day_states.exact_maturity IS '精确成熟度（高阶未知时为NULL）。';
COMMENT ON COLUMN stock.d2_episode_day_states.lower_bound_maturity IS '已证最低成熟度下界。';
COMMENT ON COLUMN stock.d2_episode_day_states.display_maturity IS '展示成熟度（含降级迟滞）。';
COMMENT ON COLUMN stock.d2_episode_day_states.effective_eligibility_value IS '今日有效资格布尔值（未知时NULL）。';
COMMENT ON COLUMN stock.d2_episode_day_states.effective_eligibility_quality IS '今日有效资格质量（OBSERVED/UNKNOWN/NOT_APPLICABLE）。';
COMMENT ON COLUMN stock.d2_episode_day_states.effective_eligibility_reason IS '今日有效资格原因码。';
COMMENT ON COLUMN stock.d2_episode_day_states.effective_bucket IS '今日有效排序分桶（0=CONFIRMED..3=SEED，无效或未知时NULL）。';
COMMENT ON COLUMN stock.d2_episode_day_states.validity_value IS '有效性状态值（VALID/INVALIDATED等）。';
COMMENT ON COLUMN stock.d2_episode_day_states.validity_quality IS '有效性质量。';
COMMENT ON COLUMN stock.d2_episode_day_states.validity_reason IS '有效性原因码。';
COMMENT ON COLUMN stock.d2_episode_day_states.freshness IS '新鲜度（FRESH/STALE）。';
COMMENT ON COLUMN stock.d2_episode_day_states.health_value IS '健康度（DAMAGED/EXHAUSTED/IMPROVING/WEAKENING/STABLE）。';
COMMENT ON COLUMN stock.d2_episode_day_states.health_quality IS '健康度质量。';
COMMENT ON COLUMN stock.d2_episode_day_states.health_reason IS '健康度原因码。';
COMMENT ON COLUMN stock.d2_episode_day_states.tracking IS '跟踪状态（ACTIVE/EXITED等）。';
COMMENT ON COLUMN stock.d2_episode_day_states.downgrade_candidate IS '连续降级候选目标档。';
COMMENT ON COLUMN stock.d2_episode_day_states.downgrade_count IS '连续降级累计会话数。';
COMMENT ON COLUMN stock.d2_episode_day_states.expiry_count IS '无改善累计会话数（满10退）。';
COMMENT ON COLUMN stock.d2_episode_day_states.improvement_baseline_numerator IS '最近改善基准值分子。';
COMMENT ON COLUMN stock.d2_episode_day_states.improvement_baseline_denominator IS '最近改善基准值分母。';
COMMENT ON COLUMN stock.d2_episode_day_states.exit_ordinal IS '退出日序号（活跃为NULL）。';
COMMENT ON COLUMN stock.d2_episode_day_states.exit_reason IS '退出原因码。';
COMMENT ON COLUMN stock.d2_episode_day_states.reentry_armed IS '退出后是否已武装重入（需经一次无匹配后）。';
COMMENT ON COLUMN stock.d2_episode_day_states.transitions IS '当日状态跃迁事件数组。';
COMMENT ON COLUMN stock.d2_episode_day_states.preserved_episode_ids IS '同日分叉保留推进的旧episode ID数组。';

CREATE TABLE stock.d2_primary_episode_day_facts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    entity_type text NOT NULL CHECK (entity_type IN ('STOCK', 'SECTOR')),
    entity_id text NOT NULL,
    calendar_id text NOT NULL,
    session_date date NOT NULL,
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    rule_identity text NOT NULL,
    primary_status text NOT NULL CHECK (primary_status IN ('SELECTED', 'NOT_APPLICABLE', 'UNKNOWN')),
    primary_reason text,
    primary_episode_id text REFERENCES stock.d2_episodes(episode_id) ON DELETE SET NULL,
    candidate_count integer NOT NULL CHECK (candidate_count >= 0),
    CONSTRAINT d2_primary_episode_day_facts_uq UNIQUE (entity_type, entity_id, calendar_id, session_date, rule_identity),
    CONSTRAINT d2_primary_episode_day_facts_sel_ck CHECK ((primary_status = 'SELECTED' AND primary_episode_id IS NOT NULL) OR (primary_status <> 'SELECTED' AND primary_episode_id IS NULL))
);

COMMENT ON TABLE stock.d2_primary_episode_day_facts IS '每日每实体唯一主episode投影表：供风险、Priority及Reader直接读取。';
COMMENT ON COLUMN stock.d2_primary_episode_day_facts.id IS '自增主键ID。';
COMMENT ON COLUMN stock.d2_primary_episode_day_facts.entity_type IS '实体类型：STOCK或SECTOR。';
COMMENT ON COLUMN stock.d2_primary_episode_day_facts.entity_id IS '实体代码。';
COMMENT ON COLUMN stock.d2_primary_episode_day_facts.calendar_id IS '交易日历标识。';
COMMENT ON COLUMN stock.d2_primary_episode_day_facts.session_date IS '业务交易日。';
COMMENT ON COLUMN stock.d2_primary_episode_day_facts.ordinal IS '交易日序号。';
COMMENT ON COLUMN stock.d2_primary_episode_day_facts.rule_identity IS 'D2规则合同标识。';
COMMENT ON COLUMN stock.d2_primary_episode_day_facts.primary_status IS '选主状态：SELECTED/NOT_APPLICABLE/UNKNOWN。';
COMMENT ON COLUMN stock.d2_primary_episode_day_facts.primary_reason IS '选主或不可选原因码。';
COMMENT ON COLUMN stock.d2_primary_episode_day_facts.primary_episode_id IS '选中的主episode ID（仅SELECTED时非空）。';
COMMENT ON COLUMN stock.d2_primary_episode_day_facts.candidate_count IS '参与选主的候选episode总数。';

CREATE TABLE stock.d2_parent_peak_events (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    parent_episode_id text NOT NULL REFERENCES stock.d2_episodes(episode_id) ON DELETE CASCADE,
    entity_id text NOT NULL,
    peak_session date NOT NULL,
    peak_ordinal integer NOT NULL CHECK (peak_ordinal >= 0),
    coordinate_id text NOT NULL,
    peak_high_numerator numeric NOT NULL,
    peak_high_denominator numeric NOT NULL CHECK (peak_high_denominator > 0),
    peak_close_numerator numeric NOT NULL,
    peak_close_denominator numeric NOT NULL CHECK (peak_close_denominator > 0),
    CONSTRAINT d2_parent_peak_events_uq UNIQUE (parent_episode_id, peak_session)
);

CREATE INDEX d2_parent_peak_events_entity_idx
    ON stock.d2_parent_peak_events (entity_id, peak_ordinal);

COMMENT ON TABLE stock.d2_parent_peak_events IS '合法FORMAL父episode冻结峰事件表：仅记录严格高于旧峰的最高价，供次日及以后PB使用。';
COMMENT ON COLUMN stock.d2_parent_peak_events.id IS '自增主键ID。';
COMMENT ON COLUMN stock.d2_parent_peak_events.parent_episode_id IS '关联FORMAL父episode ID。';
COMMENT ON COLUMN stock.d2_parent_peak_events.entity_id IS '证券代码。';
COMMENT ON COLUMN stock.d2_parent_peak_events.peak_session IS '创出该冻结峰的交易日。';
COMMENT ON COLUMN stock.d2_parent_peak_events.peak_ordinal IS '峰交易日序号。';
COMMENT ON COLUMN stock.d2_parent_peak_events.coordinate_id IS '价格坐标标识。';
COMMENT ON COLUMN stock.d2_parent_peak_events.peak_high_numerator IS '冻结峰最高价分子。';
COMMENT ON COLUMN stock.d2_parent_peak_events.peak_high_denominator IS '冻结峰最高价分母。';
COMMENT ON COLUMN stock.d2_parent_peak_events.peak_close_numerator IS '冻结峰收盘价分子。';
COMMENT ON COLUMN stock.d2_parent_peak_events.peak_close_denominator IS '冻结峰收盘价分母。';

-- ============================================================================
-- 6. 股票综合风险、Priority、Radar 与 Near-Miss 持久化表群
-- ============================================================================
CREATE TABLE stock.stock_risk_day_facts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    target_session date NOT NULL,
    source_symbol text NOT NULL CHECK (source_symbol ~ '^(SH|SZ)\.[0-9]{6}$'),
    coordinate_id text NOT NULL,
    selected_episode_id text,
    structure_applicability text NOT NULL CHECK (structure_applicability IN ('ABSENT', 'LINKED', 'UNKNOWN')),
    support_applicability text NOT NULL CHECK (support_applicability IN ('ABSENT', 'LINKED', 'UNKNOWN')),
    risk_level text CHECK (risk_level IN ('LOW', 'MEDIUM', 'HIGH', 'EXTREME')),
    risk_quality text NOT NULL CHECK (risk_quality IN ('OBSERVED', 'UNKNOWN')),
    risk_reason text,
    lower_bound text CHECK (lower_bound IN ('LOW', 'MEDIUM', 'HIGH', 'EXTREME')),
    upper_bound text CHECK (upper_bound IN ('LOW', 'MEDIUM', 'HIGH', 'EXTREME')),
    primary_reason text,
    confirmed_reasons text[] NOT NULL DEFAULT ARRAY[]::text[],
    possible_reasons text[] NOT NULL DEFAULT ARRAY[]::text[],
    damage_value boolean,
    damage_quality text NOT NULL,
    extension_value text,
    extension_quality text NOT NULL,
    invalidated_value boolean,
    invalidated_quality text NOT NULL,
    shallow_breach_value boolean,
    shallow_breach_quality text NOT NULL,
    CONSTRAINT stock_risk_day_facts_uq UNIQUE (target_session, source_symbol)
);

COMMENT ON TABLE stock.stock_risk_day_facts IS '全市场股票日度综合风险事实表（§10.1 / ALG-31）。';
COMMENT ON COLUMN stock.stock_risk_day_facts.id IS '自增主键ID。';
COMMENT ON COLUMN stock.stock_risk_day_facts.target_session IS '业务交易日。';
COMMENT ON COLUMN stock.stock_risk_day_facts.source_symbol IS '证券代码（SH/SZ.xxxxxx）。';
COMMENT ON COLUMN stock.stock_risk_day_facts.coordinate_id IS '价格坐标标识。';
COMMENT ON COLUMN stock.stock_risk_day_facts.selected_episode_id IS '当前选中的主episode ID（无适用或未知时NULL）。';
COMMENT ON COLUMN stock.stock_risk_day_facts.structure_applicability IS '结构适用性：ABSENT/LINKED/UNKNOWN。';
COMMENT ON COLUMN stock.stock_risk_day_facts.support_applicability IS '关联支撑适用性：ABSENT/LINKED/UNKNOWN。';
COMMENT ON COLUMN stock.stock_risk_day_facts.risk_level IS '综合风险等级：LOW/MEDIUM/HIGH/EXTREME（未知时NULL）。';
COMMENT ON COLUMN stock.stock_risk_day_facts.risk_quality IS '风险质量：OBSERVED或UNKNOWN。';
COMMENT ON COLUMN stock.stock_risk_day_facts.risk_reason IS '风险未知或不可评估原因码。';
COMMENT ON COLUMN stock.stock_risk_day_facts.lower_bound IS '未知归并下界等级。';
COMMENT ON COLUMN stock.stock_risk_day_facts.upper_bound IS '未知归并上界等级。';
COMMENT ON COLUMN stock.stock_risk_day_facts.primary_reason IS '唯一主风险原因（不唯一时NULL）。';
COMMENT ON COLUMN stock.stock_risk_day_facts.confirmed_reasons IS '已确认风险原因列表。';
COMMENT ON COLUMN stock.stock_risk_day_facts.possible_reasons IS '候选可能风险原因列表。';
COMMENT ON COLUMN stock.stock_risk_day_facts.damage_value IS 'P分项：价格损伤布尔值。';
COMMENT ON COLUMN stock.stock_risk_day_facts.damage_quality IS 'P分项质量。';
COMMENT ON COLUMN stock.stock_risk_day_facts.extension_value IS 'E分项：延伸度枚举。';
COMMENT ON COLUMN stock.stock_risk_day_facts.extension_quality IS 'E分项质量。';
COMMENT ON COLUMN stock.stock_risk_day_facts.invalidated_value IS 'I分项：关联episode或支持硬破布尔值。';
COMMENT ON COLUMN stock.stock_risk_day_facts.invalidated_quality IS 'I分项质量。';
COMMENT ON COLUMN stock.stock_risk_day_facts.shallow_breach_value IS 'W分项：关联支持浅破布尔值。';
COMMENT ON COLUMN stock.stock_risk_day_facts.shallow_breach_quality IS 'W分项质量。';

CREATE TABLE stock.stock_priority_day_facts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    target_session date NOT NULL,
    source_symbol text NOT NULL CHECK (source_symbol ~ '^(SH|SZ)\.[0-9]{6}$'),
    priority_status text NOT NULL CHECK (priority_status IN ('RANKABLE', 'EXCLUDED', 'UNKNOWN')),
    exclusion_reasons text[] NOT NULL DEFAULT ARRAY[]::text[],
    bucket integer,
    emergence_rank integer,
    structure_rank integer,
    rps5_delta3_pp_numerator numeric,
    rps5_delta3_pp_denominator numeric,
    risk_rank integer,
    improvement_age integer,
    improvement_age_category text NOT NULL CHECK (improvement_age_category IN ('KNOWN_AGE', 'NO_PROVEN_IMPROVEMENT', 'UNKNOWN', 'NOT_APPLICABLE')),
    prior20_amount_mean_numerator numeric,
    prior20_amount_mean_denominator numeric,
    stable_id text NOT NULL,
    priority_rank integer,
    CONSTRAINT stock_priority_day_facts_uq UNIQUE (target_session, source_symbol),
    CONSTRAINT stock_priority_day_facts_rps_pair_ck CHECK ((rps5_delta3_pp_numerator IS NULL) = (rps5_delta3_pp_denominator IS NULL) AND (rps5_delta3_pp_denominator IS NULL OR rps5_delta3_pp_denominator > 0)),
    CONSTRAINT stock_priority_day_facts_amt_pair_ck CHECK ((prior20_amount_mean_numerator IS NULL) = (prior20_amount_mean_denominator IS NULL) AND (prior20_amount_mean_denominator IS NULL OR prior20_amount_mean_denominator > 0))
);

CREATE INDEX stock_priority_day_facts_rank_idx
    ON stock.stock_priority_day_facts (target_session, priority_status, priority_rank);

COMMENT ON TABLE stock.stock_priority_day_facts IS '全市场股票日度正式Priority排序事实表（ALG-32 / §11.7）。';
COMMENT ON COLUMN stock.stock_priority_day_facts.id IS '自增主键ID。';
COMMENT ON COLUMN stock.stock_priority_day_facts.target_session IS '业务交易日。';
COMMENT ON COLUMN stock.stock_priority_day_facts.source_symbol IS '证券代码。';
COMMENT ON COLUMN stock.stock_priority_day_facts.priority_status IS '排序资格状态：RANKABLE/EXCLUDED/UNKNOWN。';
COMMENT ON COLUMN stock.stock_priority_day_facts.exclusion_reasons IS '排除或未知原因数组。';
COMMENT ON COLUMN stock.stock_priority_day_facts.bucket IS '今日有效成熟度分桶（0=CONFIRMED..3=SEED）。';
COMMENT ON COLUMN stock.stock_priority_day_facts.emergence_rank IS '异动强度等级（3=HIGH,2=MEDIUM,1=LOW,0=NONE）。';
COMMENT ON COLUMN stock.stock_priority_day_facts.structure_rank IS '关联支撑结构等级（4=ACCEPTED..0=NONE）。';
COMMENT ON COLUMN stock.stock_priority_day_facts.rps5_delta3_pp_numerator IS 'rps5_delta3_pp分子。';
COMMENT ON COLUMN stock.stock_priority_day_facts.rps5_delta3_pp_denominator IS 'rps5_delta3_pp分母。';
COMMENT ON COLUMN stock.stock_priority_day_facts.risk_rank IS '综合风险等级数值（0=LOW..3=EXTREME）。';
COMMENT ON COLUMN stock.stock_priority_day_facts.improvement_age IS '距最近真实改善的市场会话数（KNOWN_AGE时非空）。';
COMMENT ON COLUMN stock.stock_priority_day_facts.improvement_age_category IS '改善年龄类别：KNOWN_AGE/NO_PROVEN_IMPROVEMENT/UNKNOWN/NOT_APPLICABLE。';
COMMENT ON COLUMN stock.stock_priority_day_facts.prior20_amount_mean_numerator IS '前20日均成交额分子（CNY）。';
COMMENT ON COLUMN stock.stock_priority_day_facts.prior20_amount_mean_denominator IS '前20日均成交额分母。';
COMMENT ON COLUMN stock.stock_priority_day_facts.stable_id IS '稳定业务ID（用于tie-break）。';
COMMENT ON COLUMN stock.stock_priority_day_facts.priority_rank IS '当日RANKABLE集合内的1-based全局稳定排序序号（非RANKABLE为NULL）。';

CREATE TABLE stock.stock_radar_day_events (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    target_session date NOT NULL,
    source_symbol text NOT NULL CHECK (source_symbol ~ '^(SH|SZ)\.[0-9]{6}$'),
    event_type text NOT NULL CHECK (event_type IN ('INVALIDATED', 'EXPIRED', 'EXITED', 'WEAKENED', 'UPGRADED', 'REENTERED', 'FIRST_ENROLLED', 'RISK_WORSENED', 'RISK_IMPROVED')),
    episode_id text NOT NULL,
    display_order integer NOT NULL CHECK (display_order >= 1),
    is_primary_event boolean NOT NULL,
    why_now text NOT NULL,
    waiting_for text,
    invalid_if text,
    annotation_flags text[] NOT NULL DEFAULT ARRAY[]::text[],
    evidence_summary text NOT NULL,
    CONSTRAINT stock_radar_day_events_uq UNIQUE (target_session, source_symbol, event_type, episode_id)
);

CREATE INDEX stock_radar_day_events_order_idx
    ON stock.stock_radar_day_events (target_session, display_order);

COMMENT ON TABLE stock.stock_radar_day_events IS '股票日度Radar真实状态与风险变化事件表（ST-08 / §11.7）。';
COMMENT ON COLUMN stock.stock_radar_day_events.id IS '自增主键ID。';
COMMENT ON COLUMN stock.stock_radar_day_events.target_session IS '业务交易日。';
COMMENT ON COLUMN stock.stock_radar_day_events.source_symbol IS '证券代码。';
COMMENT ON COLUMN stock.stock_radar_day_events.event_type IS 'Radar事件类型（按合同9类枚举）。';
COMMENT ON COLUMN stock.stock_radar_day_events.episode_id IS '关联episode或风险对象标识。';
COMMENT ON COLUMN stock.stock_radar_day_events.display_order IS '当日Radar展示排序序号（1-based）。';
COMMENT ON COLUMN stock.stock_radar_day_events.is_primary_event IS '是否为该股当日主展示事件。';
COMMENT ON COLUMN stock.stock_radar_day_events.why_now IS '触发原因解释文本。';
COMMENT ON COLUMN stock.stock_radar_day_events.waiting_for IS '等待条件说明。';
COMMENT ON COLUMN stock.stock_radar_day_events.invalid_if IS '失效条件说明。';
COMMENT ON COLUMN stock.stock_radar_day_events.annotation_flags IS '独立状态注记（RULE_CHANGED/SELECTION_CHANGED/FIRST_OBSERVED/BECAME_KNOWN）。';
COMMENT ON COLUMN stock.stock_radar_day_events.evidence_summary IS '原谓词与变化证据摘要。';

CREATE TABLE stock.stock_near_miss_day_facts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    target_session date NOT NULL,
    source_symbol text NOT NULL CHECK (source_symbol ~ '^(SH|SZ)\.[0-9]{6}$'),
    scenario text NOT NULL CHECK (scenario IN ('LAUNCH_CONFIRM', 'RECOVERY_TURN', 'STRONG_PULLBACK', 'TREND_CONTINUE')),
    path_id text NOT NULL,
    failed_predicate text NOT NULL,
    gap_numerator numeric NOT NULL,
    gap_denominator numeric NOT NULL CHECK (gap_denominator > 0),
    tolerance_numerator numeric NOT NULL,
    tolerance_denominator numeric NOT NULL CHECK (tolerance_denominator > 0),
    gap_ratio_numerator numeric NOT NULL,
    gap_ratio_denominator numeric NOT NULL CHECK (gap_ratio_denominator > 0),
    emergence_rank integer NOT NULL,
    delta3_numerator numeric NOT NULL,
    delta3_denominator numeric NOT NULL CHECK (delta3_denominator > 0),
    stable_id text NOT NULL,
    display_order integer NOT NULL CHECK (display_order >= 1),
    hard_gates_summary text NOT NULL,
    CONSTRAINT stock_near_miss_day_facts_uq UNIQUE (target_session, source_symbol, scenario, path_id)
);

CREATE INDEX stock_near_miss_day_facts_order_idx
    ON stock.stock_near_miss_day_facts (target_session, display_order);

COMMENT ON TABLE stock.stock_near_miss_day_facts IS '股票日度Near-Miss近失解释表：仅记录硬门全真且恰有一项软数值谓词落入容差者（§11.7）。';
COMMENT ON COLUMN stock.stock_near_miss_day_facts.id IS '自增主键ID。';
COMMENT ON COLUMN stock.stock_near_miss_day_facts.target_session IS '业务交易日。';
COMMENT ON COLUMN stock.stock_near_miss_day_facts.source_symbol IS '证券代码。';
COMMENT ON COLUMN stock.stock_near_miss_day_facts.scenario IS '场景类型。';
COMMENT ON COLUMN stock.stock_near_miss_day_facts.path_id IS '场景具名AND路径或父标识。';
COMMENT ON COLUMN stock.stock_near_miss_day_facts.failed_predicate IS '唯一失败的软数值谓词名称。';
COMMENT ON COLUMN stock.stock_near_miss_day_facts.gap_numerator IS '距通过阈值差值分子。';
COMMENT ON COLUMN stock.stock_near_miss_day_facts.gap_denominator IS '距通过阈值差值分母。';
COMMENT ON COLUMN stock.stock_near_miss_day_facts.tolerance_numerator IS '合同容差分子。';
COMMENT ON COLUMN stock.stock_near_miss_day_facts.tolerance_denominator IS '合同容差分母。';
COMMENT ON COLUMN stock.stock_near_miss_day_facts.gap_ratio_numerator IS 'g/容差比值分子。';
COMMENT ON COLUMN stock.stock_near_miss_day_facts.gap_ratio_denominator IS 'g/容差比值分母。';
COMMENT ON COLUMN stock.stock_near_miss_day_facts.emergence_rank IS '异动强度等级。';
COMMENT ON COLUMN stock.stock_near_miss_day_facts.delta3_numerator IS 'rps5_delta3_pp分子。';
COMMENT ON COLUMN stock.stock_near_miss_day_facts.delta3_denominator IS 'rps5_delta3_pp分母。';
COMMENT ON COLUMN stock.stock_near_miss_day_facts.stable_id IS '稳定业务ID。';
COMMENT ON COLUMN stock.stock_near_miss_day_facts.display_order IS '当日Near-Miss展示排序序号（1-based）。';
COMMENT ON COLUMN stock.stock_near_miss_day_facts.hard_gates_summary IS '已通过硬门摘要。';
