-- R05 综合业务前向迁移（0099）：
-- 建立正式 Focus 每日队列、13 规则 Cohort 评估及子行、冻结 T0 观察登记、
-- 固定基准与相对收益、M5 匹配对照及 ITT 结果、以及原 T0 结构首事件 7 张核心表。

-- 1. 正式 Focus 每日跟踪队列（ALG-33 / §11.8）
CREATE TABLE stock.formal_focus_episode_day_facts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    target_session date NOT NULL,
    entity_id text NOT NULL,
    source_symbol text NOT NULL,
    episode_id text NOT NULL,
    parent_episode_id text,
    scenario text NOT NULL,
    display_maturity text NOT NULL CHECK (display_maturity IN ('WARM', 'CONFIRMED', 'EXITED', 'INVALIDATED', 'EXPIRED')),
    tracking_status text NOT NULL CHECK (tracking_status IN ('ACTIVE', 'EXITED', 'INVALIDATED', 'EXPIRED')),
    birth_session date NOT NULL,
    last_verified_event_date date NOT NULL,
    last_verified_event_type text NOT NULL,
    exit_session date,
    exit_reason text,
    linked_anchor_id text,
    failure_level_numerator numeric,
    failure_level_denominator numeric,
    risk_tier text,
    is_archived boolean NOT NULL DEFAULT false,
    is_pinned boolean NOT NULL DEFAULT false,
    research_mode text NOT NULL DEFAULT 'NON_PIT_RETROSPECTIVE',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT formal_focus_episode_day_facts_uq UNIQUE (target_session, episode_id)
);
CREATE INDEX idx_formal_focus_day_order
    ON stock.formal_focus_episode_day_facts (target_session, last_verified_event_date DESC, entity_id ASC, episode_id ASC);
COMMENT ON TABLE stock.formal_focus_episode_day_facts IS '正式Focus自动跟踪每日队列事实表（消费R04有效WARM/CONFIRMED与episode身份，按FOCUS_RECENCY_ORDER_R1排序）。';
COMMENT ON COLUMN stock.formal_focus_episode_day_facts.id IS '自增代理主键。';
COMMENT ON COLUMN stock.formal_focus_episode_day_facts.target_session IS '所属研究交易日（历史日不可被未来事件篡改）。';
COMMENT ON COLUMN stock.formal_focus_episode_day_facts.entity_id IS '证券实体稳定标识。';
COMMENT ON COLUMN stock.formal_focus_episode_day_facts.source_symbol IS '证券来源代码。';
COMMENT ON COLUMN stock.formal_focus_episode_day_facts.episode_id IS 'D2正式Episode唯一业务标识（父子不合并）。';
COMMENT ON COLUMN stock.formal_focus_episode_day_facts.parent_episode_id IS '重入时的父Episode标识。';
COMMENT ON COLUMN stock.formal_focus_episode_day_facts.scenario IS '触发场景。';
COMMENT ON COLUMN stock.formal_focus_episode_day_facts.display_maturity IS '当日展示成熟度（WARM/CONFIRMED或退出后终态）。';
COMMENT ON COLUMN stock.formal_focus_episode_day_facts.tracking_status IS '跟踪状态（ACTIVE/EXITED/INVALIDATED/EXPIRED）。';
COMMENT ON COLUMN stock.formal_focus_episode_day_facts.birth_session IS '该Episode出生交易日。';
COMMENT ON COLUMN stock.formal_focus_episode_day_facts.last_verified_event_date IS '最后已验证研究事件交易日（仅白名单事件更新，普通波动/UNKNOWN/打开页面不更新）。';
COMMENT ON COLUMN stock.formal_focus_episode_day_facts.last_verified_event_type IS '最后已验证研究事件类型。';
COMMENT ON COLUMN stock.formal_focus_episode_day_facts.exit_session IS '退出/失效/到期交易日。';
COMMENT ON COLUMN stock.formal_focus_episode_day_facts.exit_reason IS '退出/失效/到期原因码。';
COMMENT ON COLUMN stock.formal_focus_episode_day_facts.linked_anchor_id IS '关联D1支撑锚ID。';
COMMENT ON COLUMN stock.formal_focus_episode_day_facts.failure_level_numerator IS '冻结失效线分子。';
COMMENT ON COLUMN stock.formal_focus_episode_day_facts.failure_level_denominator IS '冻结失效线分母。';
COMMENT ON COLUMN stock.formal_focus_episode_day_facts.risk_tier IS '当日股票综合风险等级。';
COMMENT ON COLUMN stock.formal_focus_episode_day_facts.is_archived IS '是否归档（仅影响显示，不停止Forward，不阻断重入child）。';
COMMENT ON COLUMN stock.formal_focus_episode_day_facts.is_pinned IS '独立用户置顶视图标记（不改变研究排序或资格）。';
COMMENT ON COLUMN stock.formal_focus_episode_day_facts.research_mode IS '时间语义模式（NON_PIT_RETROSPECTIVE）。';
COMMENT ON COLUMN stock.formal_focus_episode_day_facts.created_at IS '创建时间。';
COMMENT ON COLUMN stock.formal_focus_episode_day_facts.updated_at IS '更新时间。';

-- 2. 13 类正式 Cohort 每日总评价（ALG-34 / §11.8）
CREATE TABLE stock.formal_cohort_evaluations (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    target_session date NOT NULL,
    rule_id text NOT NULL,
    entity_type text NOT NULL CHECK (entity_type IN ('STOCK', 'INDUSTRY', 'CONCEPT')),
    entity_id text NOT NULL,
    source_symbol text,
    board_kind text,
    is_st boolean,
    universe_status text NOT NULL CHECK (universe_status IN ('ELIGIBLE', 'EXCLUDED', 'UNMAPPED_UNKNOWN')),
    state text NOT NULL CHECK (state IN ('TRUE', 'FALSE', 'UNKNOWN')),
    reasons text[] NOT NULL DEFAULT ARRAY[]::text[],
    event_count integer NOT NULL DEFAULT 0 CHECK (event_count >= 0),
    source_capture_id bigint,
    calendar_revision text NOT NULL,
    research_mode text NOT NULL DEFAULT 'NON_PIT_RETROSPECTIVE',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT formal_cohort_evaluations_uq UNIQUE (target_session, rule_id, entity_type, entity_id)
);
CREATE INDEX idx_formal_cohort_eval_rule ON stock.formal_cohort_evaluations (target_session, rule_id, state);
COMMENT ON TABLE stock.formal_cohort_evaluations IS '13类正式Cohort规则每日全总体三态评价表（保留MATCH/NO_MATCH/UNKNOWN及未映射隔离分母）。';
COMMENT ON COLUMN stock.formal_cohort_evaluations.id IS '自增代理主键。';
COMMENT ON COLUMN stock.formal_cohort_evaluations.target_session IS '评估交易日。';
COMMENT ON COLUMN stock.formal_cohort_evaluations.rule_id IS '13类正式规则ID（STOCK_*/SECTOR_*/ROTATION_*）。';
COMMENT ON COLUMN stock.formal_cohort_evaluations.entity_type IS '对象类型：STOCK、INDUSTRY或CONCEPT。';
COMMENT ON COLUMN stock.formal_cohort_evaluations.entity_id IS '稳定实体ID或板块原生ID（未映射槽位使用UNMAPPED:<symbol>）。';
COMMENT ON COLUMN stock.formal_cohort_evaluations.source_symbol IS '股票来源代码（板块时为NULL）。';
COMMENT ON COLUMN stock.formal_cohort_evaluations.board_kind IS '股票板别或板块类型。';
COMMENT ON COLUMN stock.formal_cohort_evaluations.is_st IS '股票当日是否ST（板块时为NULL）。';
COMMENT ON COLUMN stock.formal_cohort_evaluations.universe_status IS '总体准入状态：ELIGIBLE/EXCLUDED/UNMAPPED_UNKNOWN。';
COMMENT ON COLUMN stock.formal_cohort_evaluations.state IS '规则三态评价：TRUE(MATCH)/FALSE(NO_MATCH)/UNKNOWN。';
COMMENT ON COLUMN stock.formal_cohort_evaluations.reasons IS '评价原因或排除原因码数组。';
COMMENT ON COLUMN stock.formal_cohort_evaluations.event_count IS '当日命中独立事件/子行数（无事件为0）。';
COMMENT ON COLUMN stock.formal_cohort_evaluations.source_capture_id IS '关联BaoStock来源捕获ID（溯源元数据）。';
COMMENT ON COLUMN stock.formal_cohort_evaluations.calendar_revision IS '官方日历修订标识。';
COMMENT ON COLUMN stock.formal_cohort_evaluations.research_mode IS '时间语义模式。';
COMMENT ON COLUMN stock.formal_cohort_evaluations.created_at IS '创建时间。';
COMMENT ON COLUMN stock.formal_cohort_evaluations.updated_at IS '更新时间。';

-- 3. D0 多父/峰事件子行明细（不合并事件身份）
CREATE TABLE stock.formal_cohort_event_subrows (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    evaluation_id bigint NOT NULL REFERENCES stock.formal_cohort_evaluations(id) ON DELETE CASCADE,
    target_session date NOT NULL,
    rule_id text NOT NULL,
    entity_id text NOT NULL,
    sub_event_key text NOT NULL,
    parent_episode_id text,
    peak_event_id text,
    event_id text,
    linked_anchor_id text,
    sub_state text NOT NULL CHECK (sub_state IN ('TRUE', 'FALSE', 'UNKNOWN')),
    admission_status text NOT NULL,
    reasons text[] NOT NULL DEFAULT ARRAY[]::text[],
    failure_level_numerator numeric,
    failure_level_denominator numeric,
    failure_coordinate text,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT formal_cohort_event_subrows_uq UNIQUE (target_session, rule_id, entity_id, sub_event_key)
);
COMMENT ON TABLE stock.formal_cohort_event_subrows IS 'D0多父/峰/事件子行明细表（保留各自事件身份与冻结线，总评价按OR归约）。';
COMMENT ON COLUMN stock.formal_cohort_event_subrows.id IS '自增代理主键。';
COMMENT ON COLUMN stock.formal_cohort_event_subrows.evaluation_id IS '关联formal_cohort_evaluations主键。';
COMMENT ON COLUMN stock.formal_cohort_event_subrows.target_session IS '评估交易日。';
COMMENT ON COLUMN stock.formal_cohort_event_subrows.rule_id IS '规则ID。';
COMMENT ON COLUMN stock.formal_cohort_event_subrows.entity_id IS '实体ID。';
COMMENT ON COLUMN stock.formal_cohort_event_subrows.sub_event_key IS '子事件唯一键（如parent_episode_id:peak_ordinal）。';
COMMENT ON COLUMN stock.formal_cohort_event_subrows.parent_episode_id IS '关联父Episode ID。';
COMMENT ON COLUMN stock.formal_cohort_event_subrows.peak_event_id IS '关联峰事件ID。';
COMMENT ON COLUMN stock.formal_cohort_event_subrows.event_id IS '正式事件ID。';
COMMENT ON COLUMN stock.formal_cohort_event_subrows.linked_anchor_id IS '关联D1支撑锚ID。';
COMMENT ON COLUMN stock.formal_cohort_event_subrows.sub_state IS '子行三态评价（TRUE/FALSE/UNKNOWN）。';
COMMENT ON COLUMN stock.formal_cohort_event_subrows.admission_status IS '正式准入状态。';
COMMENT ON COLUMN stock.formal_cohort_event_subrows.reasons IS '子行原因码数组。';
COMMENT ON COLUMN stock.formal_cohort_event_subrows.failure_level_numerator IS '冻结失效线分子。';
COMMENT ON COLUMN stock.formal_cohort_event_subrows.failure_level_denominator IS '冻结失效线分母。';
COMMENT ON COLUMN stock.formal_cohort_event_subrows.failure_coordinate IS '冻结失效线价格坐标。';
COMMENT ON COLUMN stock.formal_cohort_event_subrows.created_at IS '创建时间。';

-- 4. 正式研究观察与冻结 T0 登记（ALG-34 / §11.8）
CREATE TABLE stock.formal_research_observations (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    t0_session date NOT NULL,
    time_lane text NOT NULL CHECK (time_lane IN ('H', 'L', 'LATE')),
    rule_id text NOT NULL,
    entity_type text NOT NULL CHECK (entity_type IN ('STOCK', 'INDUSTRY', 'CONCEPT')),
    entity_id text NOT NULL,
    observation_sub_key text NOT NULL DEFAULT 'PRIMARY',
    source_symbol text,
    enrollment_kind text NOT NULL CHECK (enrollment_kind IN ('ENTRY', 'DAILY_LANDMARK', 'REENTRY', 'FIRST_OBSERVED_LEFT_CENSORED', 'UNKNOWN_PAUSED', 'EXIT_PROVED')),
    parent_observation_id bigint REFERENCES stock.formal_research_observations(id) ON DELETE SET NULL,
    episode_id text,
    event_id text,
    linked_anchor_id text,
    t0_p0_numerator numeric,
    t0_p0_denominator numeric,
    t0_qfq_coordinate_id text,
    t0_rps20_num numeric,
    t0_rps20_den numeric,
    t0_pos60_num numeric,
    t0_pos60_den numeric,
    t0_amt20_num numeric,
    t0_amt20_den numeric,
    t0_industry_id text,
    t0_board_type text,
    t0_is_st boolean,
    t0_safety_gate_passed boolean,
    source_available_at timestamptz,
    first_captured_at timestamptz,
    reason_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT formal_research_observations_uq UNIQUE (t0_session, time_lane, rule_id, entity_type, entity_id, observation_sub_key)
);
CREATE INDEX idx_formal_obs_entity ON stock.formal_research_observations (rule_id, entity_id, t0_session);
COMMENT ON TABLE stock.formal_research_observations IS '正式研究观察与T0冻结登记表（区分ENTRY/DAILY_LANDMARK/REENTRY/左截断/暂停/退出，冻结T0特征与时钟）。';
COMMENT ON COLUMN stock.formal_research_observations.id IS '自增代理主键。';
COMMENT ON COLUMN stock.formal_research_observations.t0_session IS '观察T0交易日。';
COMMENT ON COLUMN stock.formal_research_observations.time_lane IS '时间通道：H(历史后验)/L(实时首捕)/LATE(迟采)。';
COMMENT ON COLUMN stock.formal_research_observations.rule_id IS '正式规则ID。';
COMMENT ON COLUMN stock.formal_research_observations.entity_type IS '对象类型。';
COMMENT ON COLUMN stock.formal_research_observations.entity_id IS '稳定实体ID或板块ID。';
COMMENT ON COLUMN stock.formal_research_observations.observation_sub_key IS '同日多事件区分键（普通登记为PRIMARY，多事件为event_id/episode_id）。';
COMMENT ON COLUMN stock.formal_research_observations.source_symbol IS '来源代码。';
COMMENT ON COLUMN stock.formal_research_observations.enrollment_kind IS '登记类型：ENTRY/DAILY_LANDMARK/REENTRY/FIRST_OBSERVED_LEFT_CENSORED/UNKNOWN_PAUSED/EXIT_PROVED。';
COMMENT ON COLUMN stock.formal_research_observations.parent_observation_id IS '直接父观察/登记ID（用于REENTRY与连续跟踪）。';
COMMENT ON COLUMN stock.formal_research_observations.episode_id IS '关联Episode ID。';
COMMENT ON COLUMN stock.formal_research_observations.event_id IS '关联正式事件ID。';
COMMENT ON COLUMN stock.formal_research_observations.linked_anchor_id IS '关联锚ID。';
COMMENT ON COLUMN stock.formal_research_observations.t0_p0_numerator IS 'T0冻结QFQ收盘价分子。';
COMMENT ON COLUMN stock.formal_research_observations.t0_p0_denominator IS 'T0冻结QFQ收盘价分母。';
COMMENT ON COLUMN stock.formal_research_observations.t0_qfq_coordinate_id IS 'T0冻结QFQ经济坐标标识。';
COMMENT ON COLUMN stock.formal_research_observations.t0_rps20_num IS 'T0冻结RPS20分子。';
COMMENT ON COLUMN stock.formal_research_observations.t0_rps20_den IS 'T0冻结RPS20分母。';
COMMENT ON COLUMN stock.formal_research_observations.t0_pos60_num IS 'T0冻结pos60分子。';
COMMENT ON COLUMN stock.formal_research_observations.t0_pos60_den IS 'T0冻结pos60分母。';
COMMENT ON COLUMN stock.formal_research_observations.t0_amt20_num IS 'T0冻结前20日均额分子。';
COMMENT ON COLUMN stock.formal_research_observations.t0_amt20_den IS 'T0冻结前20日均额分母。';
COMMENT ON COLUMN stock.formal_research_observations.t0_industry_id IS 'T0冻结官方主行业ID。';
COMMENT ON COLUMN stock.formal_research_observations.t0_board_type IS 'T0冻结板别。';
COMMENT ON COLUMN stock.formal_research_observations.t0_is_st IS 'T0冻结ST标记。';
COMMENT ON COLUMN stock.formal_research_observations.t0_safety_gate_passed IS 'T0普通安全门是否通过。';
COMMENT ON COLUMN stock.formal_research_observations.source_available_at IS '来源可用时间（有验证证据时记录）。';
COMMENT ON COLUMN stock.formal_research_observations.first_captured_at IS '首次实际捕获时钟。';
COMMENT ON COLUMN stock.formal_research_observations.reason_codes IS '登记原因码数组。';
COMMENT ON COLUMN stock.formal_research_observations.created_at IS '创建时间。';
COMMENT ON COLUMN stock.formal_research_observations.updated_at IS '更新时间。';

-- 5. 固定基准与相对收益（D01/D02, ALG-36 / §11.9）
CREATE TABLE stock.formal_forward_benchmarks (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    t0_session date NOT NULL,
    target_key text NOT NULL,
    horizon smallint NOT NULL CHECK (horizon IN (1, 3, 5, 10, 20)),
    benchmark_kind text NOT NULL CHECK (benchmark_kind IN ('MARKET', 'PRIMARY_INDUSTRY', 'SUPPORT_CONCEPT', 'SELF_BASKET')),
    benchmark_ref_id text NOT NULL,
    endpoint_session date,
    basket_member_count integer NOT NULL CHECK (basket_member_count >= 0),
    evaluable_member_count integer NOT NULL CHECK (evaluable_member_count >= 0),
    coverage_ratio_num numeric,
    coverage_ratio_den numeric,
    benchmark_return_num numeric,
    benchmark_return_den numeric,
    relative_return_num numeric,
    relative_return_den numeric,
    mfe_close_num numeric,
    mfe_close_den numeric,
    mae_close_num numeric,
    mae_close_den numeric,
    mdd_close_num numeric,
    mdd_close_den numeric,
    outcome_quality text NOT NULL CHECK (outcome_quality IN ('OBSERVED', 'MARKED_ESTIMATE', 'PARTIAL_UNVALUED', 'UNKNOWN', 'NOT_DUE')),
    reason_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT formal_forward_benchmarks_uq UNIQUE (t0_session, target_key, horizon, benchmark_kind)
);
COMMENT ON TABLE stock.formal_forward_benchmarks IS 'T0固定篮子基准、五期限收盘路径及相对收益表（冻结成员与原权重，不因缺终值删成员重权）。';
COMMENT ON COLUMN stock.formal_forward_benchmarks.id IS '自增代理主键。';
COMMENT ON COLUMN stock.formal_forward_benchmarks.t0_session IS 'T0交易日。';
COMMENT ON COLUMN stock.formal_forward_benchmarks.target_key IS '目标对象键（如STOCK:SZ.003990或INDUSTRY:IND_A）。';
COMMENT ON COLUMN stock.formal_forward_benchmarks.horizon IS '后继交易会话期限（1/3/5/10/20）。';
COMMENT ON COLUMN stock.formal_forward_benchmarks.benchmark_kind IS '基准类型：MARKET/PRIMARY_INDUSTRY/SUPPORT_CONCEPT/SELF_BASKET。';
COMMENT ON COLUMN stock.formal_forward_benchmarks.benchmark_ref_id IS 'T0冻结的基准参考ID（如主行业ID、概念ID或MARKET_UNIVERSE）。';
COMMENT ON COLUMN stock.formal_forward_benchmarks.endpoint_session IS '实际到期交易日。';
COMMENT ON COLUMN stock.formal_forward_benchmarks.basket_member_count IS 'T0冻结篮子成员总数（固定分母）。';
COMMENT ON COLUMN stock.formal_forward_benchmarks.evaluable_member_count IS '终点可估值成员数。';
COMMENT ON COLUMN stock.formal_forward_benchmarks.coverage_ratio_num IS 'T0初始覆盖率分子。';
COMMENT ON COLUMN stock.formal_forward_benchmarks.coverage_ratio_den IS 'T0初始覆盖率分母。';
COMMENT ON COLUMN stock.formal_forward_benchmarks.benchmark_return_num IS '固定篮子等权收益分子。';
COMMENT ON COLUMN stock.formal_forward_benchmarks.benchmark_return_den IS '固定篮子等权收益分母。';
COMMENT ON COLUMN stock.formal_forward_benchmarks.relative_return_num IS '目标相对该基准的超额收益分子。';
COMMENT ON COLUMN stock.formal_forward_benchmarks.relative_return_den IS '目标相对该基准的超额收益分母。';
COMMENT ON COLUMN stock.formal_forward_benchmarks.mfe_close_num IS '固定篮子每日收盘路径MFE_CLOSE分子。';
COMMENT ON COLUMN stock.formal_forward_benchmarks.mfe_close_den IS '固定篮子每日收盘路径MFE_CLOSE分母。';
COMMENT ON COLUMN stock.formal_forward_benchmarks.mae_close_num IS '固定篮子每日收盘路径MAE_CLOSE分子。';
COMMENT ON COLUMN stock.formal_forward_benchmarks.mae_close_den IS '固定篮子每日收盘路径MAE_CLOSE分母。';
COMMENT ON COLUMN stock.formal_forward_benchmarks.mdd_close_num IS '固定篮子每日收盘路径MDD_CLOSE分子。';
COMMENT ON COLUMN stock.formal_forward_benchmarks.mdd_close_den IS '固定篮子每日收盘路径MDD_CLOSE分母。';
COMMENT ON COLUMN stock.formal_forward_benchmarks.outcome_quality IS '结果质量：OBSERVED/MARKED_ESTIMATE/PARTIAL_UNVALUED/UNKNOWN/NOT_DUE。';
COMMENT ON COLUMN stock.formal_forward_benchmarks.reason_codes IS '原因码数组。';
COMMENT ON COLUMN stock.formal_forward_benchmarks.created_at IS '创建时间。';
COMMENT ON COLUMN stock.formal_forward_benchmarks.updated_at IS '更新时间。';

-- 6. M5 匹配对照关系与 ITT/Clean 结果（D03, §11.9）
CREATE TABLE stock.formal_m5_matched_controls (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    observation_id bigint NOT NULL REFERENCES stock.formal_research_observations(id) ON DELETE CASCADE,
    t0_session date NOT NULL,
    rule_id text NOT NULL,
    target_entity_id text NOT NULL,
    control_rank smallint NOT NULL CHECK (control_rank >= 0),
    control_entity_id text NOT NULL,
    control_symbol text NOT NULL,
    match_status text NOT NULL CHECK (match_status IN ('MATCHED', 'INSUFFICIENT_MATCH', 'UNKNOWN')),
    match_k smallint NOT NULL CHECK (match_k >= 0 AND match_k <= 5),
    distance_num numeric,
    distance_den numeric,
    weight_num numeric,
    weight_den numeric,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT formal_m5_matched_controls_uq UNIQUE (observation_id, control_rank)
);
COMMENT ON TABLE stock.formal_m5_matched_controls IS 'M5_R1 T0冻结匹配对照关系表（记录匹配排名、精确距离、固定权重及不足诊断）。';
COMMENT ON COLUMN stock.formal_m5_matched_controls.id IS '自增代理主键。';
COMMENT ON COLUMN stock.formal_m5_matched_controls.observation_id IS '关联T0观察主键。';
COMMENT ON COLUMN stock.formal_m5_matched_controls.t0_session IS 'T0交易日。';
COMMENT ON COLUMN stock.formal_m5_matched_controls.rule_id IS '规则ID。';
COMMENT ON COLUMN stock.formal_m5_matched_controls.target_entity_id IS '目标证券实体ID。';
COMMENT ON COLUMN stock.formal_m5_matched_controls.control_rank IS '对照排名（1..5；0表示INSUFFICIENT_MATCH/UNKNOWN占位）。';
COMMENT ON COLUMN stock.formal_m5_matched_controls.control_entity_id IS '对照证券实体ID（无有效对照时为NONE）。';
COMMENT ON COLUMN stock.formal_m5_matched_controls.control_symbol IS '对照证券代码。';
COMMENT ON COLUMN stock.formal_m5_matched_controls.match_status IS '匹配状态：MATCHED(3..5)/INSUFFICIENT_MATCH(<3)/UNKNOWN。';
COMMENT ON COLUMN stock.formal_m5_matched_controls.match_k IS '匹配对照数量k。';
COMMENT ON COLUMN stock.formal_m5_matched_controls.distance_num IS 'M5精确距离分子。';
COMMENT ON COLUMN stock.formal_m5_matched_controls.distance_den IS 'M5精确距离分母。';
COMMENT ON COLUMN stock.formal_m5_matched_controls.weight_num IS 'T0冻结权重分子（1/k）。';
COMMENT ON COLUMN stock.formal_m5_matched_controls.weight_den IS 'T0冻结权重分母。';
COMMENT ON COLUMN stock.formal_m5_matched_controls.created_at IS '创建时间。';

CREATE TABLE stock.formal_m5_control_outcomes (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    observation_id bigint NOT NULL REFERENCES stock.formal_research_observations(id) ON DELETE CASCADE,
    horizon smallint NOT NULL CHECK (horizon IN (1, 3, 5, 10, 20)),
    variant text NOT NULL CHECK (variant IN ('ITT', 'CLEAN_SENSITIVITY')),
    match_status text NOT NULL CHECK (match_status IN ('MATCHED', 'INSUFFICIENT_MATCH', 'UNKNOWN')),
    match_k smallint NOT NULL CHECK (match_k >= 0 AND match_k <= 5),
    control_return_num numeric,
    control_return_den numeric,
    excess_return_num numeric,
    excess_return_den numeric,
    outcome_quality text NOT NULL CHECK (outcome_quality IN ('OBSERVED', 'PARTIAL_UNVALUED', 'UNKNOWN', 'NOT_DUE')),
    reason_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT formal_m5_control_outcomes_uq UNIQUE (observation_id, horizon, variant)
);
COMMENT ON TABLE stock.formal_m5_control_outcomes IS 'M5匹配对照组五期限ITT及Clean敏感性收益表（T0固定权重不因缺终值重权）。';
COMMENT ON COLUMN stock.formal_m5_control_outcomes.id IS '自增代理主键。';
COMMENT ON COLUMN stock.formal_m5_control_outcomes.observation_id IS '关联T0观察主键。';
COMMENT ON COLUMN stock.formal_m5_control_outcomes.horizon IS '后继期限（1/3/5/10/20）。';
COMMENT ON COLUMN stock.formal_m5_control_outcomes.variant IS '口径变体：ITT（主口径）或CLEAN_SENSITIVITY（敏感性）。';
COMMENT ON COLUMN stock.formal_m5_control_outcomes.match_status IS 'T0匹配状态。';
COMMENT ON COLUMN stock.formal_m5_control_outcomes.match_k IS '对照数k。';
COMMENT ON COLUMN stock.formal_m5_control_outcomes.control_return_num IS '对照组等权收益分子。';
COMMENT ON COLUMN stock.formal_m5_control_outcomes.control_return_den IS '对照组等权收益分母。';
COMMENT ON COLUMN stock.formal_m5_control_outcomes.excess_return_num IS '目标减对照超额收益分子。';
COMMENT ON COLUMN stock.formal_m5_control_outcomes.excess_return_den IS '目标减对照超额收益分母。';
COMMENT ON COLUMN stock.formal_m5_control_outcomes.outcome_quality IS '结果质量。';
COMMENT ON COLUMN stock.formal_m5_control_outcomes.reason_codes IS '原因码数组。';
COMMENT ON COLUMN stock.formal_m5_control_outcomes.created_at IS '创建时间。';
COMMENT ON COLUMN stock.formal_m5_control_outcomes.updated_at IS '更新时间。';

-- 7. 原 T0 结构首事件（E01, §11.9）
CREATE TABLE stock.formal_structure_first_events (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    observation_id bigint NOT NULL REFERENCES stock.formal_research_observations(id) ON DELETE CASCADE,
    t0_session date NOT NULL,
    entity_id text NOT NULL,
    episode_id text NOT NULL,
    horizon smallint NOT NULL CHECK (horizon IN (1, 3, 5, 10, 20)),
    first_exit_type text CHECK (first_exit_type IN ('INVALIDATED', 'EXPIRED', 'EXITED', 'NONE', 'UNKNOWN')),
    first_exit_session date,
    first_exit_quality text NOT NULL CHECK (first_exit_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_DUE')),
    known_upper_bound_session date,
    known_upper_bound_type text,
    first_structure_event_type text,
    first_structure_event_session date,
    any_events text[] NOT NULL DEFAULT ARRAY[]::text[],
    reason_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT formal_structure_first_events_uq UNIQUE (observation_id, horizon)
);
COMMENT ON TABLE stock.formal_structure_first_events IS '原T0事件/锚在后继1..N会话内的结构首事件与首退出表（同日按INVALIDATED>EXPIRED>EXITED优先，更早UNKNOWN保上界）。';
COMMENT ON COLUMN stock.formal_structure_first_events.id IS '自增代理主键。';
COMMENT ON COLUMN stock.formal_structure_first_events.observation_id IS '关联T0观察主键。';
COMMENT ON COLUMN stock.formal_structure_first_events.t0_session IS 'T0交易日。';
COMMENT ON COLUMN stock.formal_structure_first_events.entity_id IS '实体ID。';
COMMENT ON COLUMN stock.formal_structure_first_events.episode_id IS '风险集所属Episode ID。';
COMMENT ON COLUMN stock.formal_structure_first_events.horizon IS '观察期限（1/3/5/10/20）。';
COMMENT ON COLUMN stock.formal_structure_first_events.first_exit_type IS '首退出类型：INVALIDATED/EXPIRED/EXITED/NONE/UNKNOWN（升级不计为退出）。';
COMMENT ON COLUMN stock.formal_structure_first_events.first_exit_session IS '首退出交易日（UNKNOWN或NONE时为NULL）。';
COMMENT ON COLUMN stock.formal_structure_first_events.first_exit_quality IS '首退出质量：OBSERVED/UNKNOWN/NOT_DUE。';
COMMENT ON COLUMN stock.formal_structure_first_events.known_upper_bound_session IS '更早存在UNKNOWN时保留的已知退出上界交易日。';
COMMENT ON COLUMN stock.formal_structure_first_events.known_upper_bound_type IS '已知退出上界事件类型。';
COMMENT ON COLUMN stock.formal_structure_first_events.first_structure_event_type IS '独立结构首事件类型（含升级等非退出结构事件）。';
COMMENT ON COLUMN stock.formal_structure_first_events.first_structure_event_session IS '独立结构首事件交易日。';
COMMENT ON COLUMN stock.formal_structure_first_events.any_events IS '窗口内全部发生的结构事件类型集合。';
COMMENT ON COLUMN stock.formal_structure_first_events.reason_codes IS '原因码数组。';
COMMENT ON COLUMN stock.formal_structure_first_events.created_at IS '创建时间。';
COMMENT ON COLUMN stock.formal_structure_first_events.updated_at IS '更新时间。';
