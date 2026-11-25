-- R03 正式市场、板块、轮动、LOO 与 Overlap 持久化表及协调器模块注册

ALTER TABLE stock.data_jobs
    DROP CONSTRAINT data_jobs_selected_modules_check,
    ADD CONSTRAINT data_jobs_selected_modules_check CHECK (
        selected_modules <@ ARRAY[
            'BAOSTOCK_PRICES'::text,
            'BAOSTOCK_UNIVERSE'::text,
            'BAOSTOCK_MARKET_INDICES'::text,
            'TDX_SECTOR_MEMBERS'::text,
            'CORE_PROFILE'::text,
            'CROSS_SECTION'::text,
            'PRICE_LIMITS'::text,
            'LIMIT_UP_LADDER'::text,
            'MARKET_BREADTH'::text,
            'MARKET_PARTICIPATION'::text,
            'MARKET_INDICES_ENV'::text,
            'D0'::text,
            'D1'::text,
            'FOCUS_FORMAL'::text,
            'SECTOR_PULSE'::text,
            'SECTOR_TRANSITION'::text,
            'SECTOR_FOLLOWUP'::text,
            'R03_SECTOR_NATIVE'::text,
            'R03_SECTOR_ROTATION'::text,
            'R03_SECTOR_OVERLAP'::text,
            'R03_SECTOR_LOO'::text,
            'FORWARD_PATH'::text,
            'COHORT_STATS'::text,
            'FEP_BASELINE'::text,
            'OBSERVATION_SEGMENTS'::text
        ]
    );

COMMENT ON CONSTRAINT data_jobs_selected_modules_check ON stock.data_jobs IS
    '允许现役来源、基础研究与 R03 正式板块/轮动/Overlap/LOO 模块枚举。';

-- 1. R03 正式板块 Native 日级事实、三档 raw 资格、状态/健康、exhaustion 风险与 Priority
CREATE TABLE stock.r03_sector_native_day_facts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    trade_date date NOT NULL,
    board_kind text NOT NULL CHECK (board_kind IN ('INDUSTRY', 'CONCEPT')),
    board_id text NOT NULL,
    scope_key text NOT NULL,
    member_version text NOT NULL,
    contract_version text NOT NULL,
    member_count integer NOT NULL CHECK (member_count >= 0),
    actual_quote_count integer NOT NULL CHECK (actual_quote_count >= 0),
    suspended_count integer NOT NULL CHECK (suspended_count >= 0),
    unknown_quote_count integer NOT NULL CHECK (unknown_quote_count >= 0),
    rs5 numeric,
    rs20 numeric,
    rs5_pct numeric,
    rs20_pct numeric,
    dq5 numeric,
    breadth numeric,
    ma20_width numeric,
    amr20_median numeric,
    breadth_delta1 numeric,
    breadth_delta3 numeric,
    ma20_delta3 numeric,
    seed_wilson_point numeric,
    seed_wilson_lower numeric,
    seed_wilson_upper numeric,
    top1_concentration numeric,
    top3_concentration numeric,
    safety_state text NOT NULL CHECK (safety_state IN ('TRUE', 'FALSE', 'UNKNOWN')),
    prewatch_raw_state text NOT NULL CHECK (prewatch_raw_state IN ('TRUE', 'FALSE', 'UNKNOWN')),
    warm_raw_state text NOT NULL CHECK (warm_raw_state IN ('TRUE', 'FALSE', 'UNKNOWN')),
    confirmed_raw_state text NOT NULL CHECK (confirmed_raw_state IN ('TRUE', 'FALSE', 'UNKNOWN')),
    effective_maturity text NOT NULL CHECK (effective_maturity IN ('NONE', 'PREWATCH', 'WARM', 'CONFIRMED', 'UNKNOWN')),
    display_maturity text NOT NULL CHECK (display_maturity IN ('NONE', 'PREWATCH', 'WARM', 'CONFIRMED', 'UNKNOWN')),
    downgrade_streak integer NOT NULL DEFAULT 0 CHECK (downgrade_streak >= 0),
    prewatch_no_improve_count integer NOT NULL DEFAULT 0 CHECK (prewatch_no_improve_count >= 0),
    health_state text NOT NULL CHECK (health_state IN ('IMPROVING', 'STABLE', 'WEAKENING', 'DAMAGED', 'UNKNOWN')),
    exhaustion_level text NOT NULL CHECK (exhaustion_level IN ('HIGH', 'MEDIUM', 'LOW', 'UNKNOWN')),
    emergence_level text NOT NULL CHECK (emergence_level IN ('HIGH', 'MEDIUM', 'LOW', 'NONE', 'UNKNOWN')),
    priority_status text NOT NULL CHECK (priority_status IN ('RANKABLE', 'EXCLUDED', 'UNKNOWN')),
    priority_rank integer,
    reason_code text,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT r03_sector_native_day_facts_uq UNIQUE (trade_date, board_kind, board_id, scope_key, member_version, contract_version)
);

ALTER TABLE stock.r03_sector_native_day_facts OWNER TO stock_a;
COMMENT ON TABLE stock.r03_sector_native_day_facts IS '保存 R03 正式板块日级 Native 原语、端点中秩排名、Wilson 宽度、集中度、三档 raw 资格、成熟/健康状态、exhaustion 风险与 Priority 排序结果。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.id IS '自增主键。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.trade_date IS '目标官方交易日。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.board_kind IS '板块类型枚举：INDUSTRY 行业或 CONCEPT 概念。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.board_id IS '规范板块稳定标识。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.scope_key IS '数据捕获与研究口径标识（区分正式 PIT 与 NON_PIT）。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.member_version IS '日期有效板块成员版本号。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.contract_version IS '算法合同版本号。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.member_count IS '当日完整声明成员分母 n。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.actual_quote_count IS '当日合法 ACTUAL OHLC 成员数。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.suspended_count IS '当日已证停牌成员数。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.unknown_quote_count IS '当日行情未知成员数。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.rs5 IS '5日收益中位数（fraction）。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.rs20 IS '20日收益中位数（fraction）。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.rs5_pct IS '同类型合格全集 RS5 中秩百分位（0..100）。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.rs20_pct IS '同类型合格全集 RS20 中秩百分位（0..100）。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.dq5 IS 'RS5_pct_t 与 RS5_pct_{t-3} 各自端点独立排名之差（pp）。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.breadth IS '当日正 ret1 比例（fraction）。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.ma20_width IS '当日 C > MA20 比例（fraction）。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.amr20_median IS '成员 amr20 中位数（无量纲）。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.breadth_delta1 IS '1日共同可评估成员宽度差（fraction）。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.breadth_delta3 IS '3日共同可评估成员宽度差（fraction）。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.ma20_delta3 IS '3日共同可评估成员 MA20 宽度差（fraction）。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.seed_wilson_point IS 'u=0 时的 Seed Wilson 点值，u>0 时为 NULL。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.seed_wilson_lower IS 'Seed Wilson 下界。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.seed_wilson_upper IS 'Seed Wilson 上界。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.top1_concentration IS '完整成员 RAW 成交额最大单股占比。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.top3_concentration IS '完整成员 RAW 成交额前三股占比之和。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.safety_state IS '板块安全门三态结果。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.prewatch_raw_state IS 'PREWATCH 原始资格三态结果。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.warm_raw_state IS 'WARM 原始资格三态结果。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.confirmed_raw_state IS 'CONFIRMED 原始资格三态结果。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.effective_maturity IS '当日有效成熟档位。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.display_maturity IS '含 2 日滞后保护的展示成熟档位。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.downgrade_streak IS '连续有效降级计数。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.prewatch_no_improve_count IS 'PREWATCH 无改善连续有效会话计数（达 10 退出）。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.health_state IS '板块健康状态（仅依独立 dq5 判定）。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.exhaustion_level IS '板块 exhaustion 风险等级。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.emergence_level IS '板块 emergence 等级。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.priority_status IS '板块 Priority 排序准入状态。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.priority_rank IS '同日同口径 RANKABLE 板块有序名次（1 起）。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.reason_code IS '降级、排除或未知原因码。';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.recorded_at IS '写入 PG 的带时区时间戳。';

-- 2. R03 正式 Rotation Episode 出生与冻结篮子
CREATE TABLE stock.r03_sector_rotation_episodes (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    episode_key text NOT NULL,
    board_kind text NOT NULL CHECK (board_kind IN ('INDUSTRY', 'CONCEPT')),
    board_id text NOT NULL,
    t0_session date NOT NULL,
    scope_key text NOT NULL,
    contract_version text NOT NULL,
    parent_episode_id bigint REFERENCES stock.r03_sector_rotation_episodes(id),
    frozen_member_count integer NOT NULL CHECK (frozen_member_count >= 5),
    s0_count integer NOT NULL CHECK (s0_count >= 0),
    u0_count integer NOT NULL CHECK (u0_count >= 0),
    h0_count integer NOT NULL CHECK (h0_count >= 0),
    left_censored boolean NOT NULL DEFAULT false,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT r03_sector_rotation_episodes_uq UNIQUE (episode_key, scope_key, contract_version)
);

ALTER TABLE stock.r03_sector_rotation_episodes OWNER TO stock_a;
COMMENT ON TABLE stock.r03_sector_rotation_episodes IS '保存正式 Rotation Episode 的 t0 出生身份与冻结篮子 B=M[t0-1]、S0/U0/H0 基数。';
COMMENT ON COLUMN stock.r03_sector_rotation_episodes.id IS '自增主键。';
COMMENT ON COLUMN stock.r03_sector_rotation_episodes.episode_key IS '确定性 Episode 业务标识。';
COMMENT ON COLUMN stock.r03_sector_rotation_episodes.board_kind IS '板块类型（INDUSTRY/CONCEPT）。';
COMMENT ON COLUMN stock.r03_sector_rotation_episodes.board_id IS '板块稳定标识。';
COMMENT ON COLUMN stock.r03_sector_rotation_episodes.t0_session IS '出生信号官方交易日 t0。';
COMMENT ON COLUMN stock.r03_sector_rotation_episodes.scope_key IS '数据捕获与研究口径标识。';
COMMENT ON COLUMN stock.r03_sector_rotation_episodes.contract_version IS '算法合同版本号。';
COMMENT ON COLUMN stock.r03_sector_rotation_episodes.parent_episode_id IS '重入或分叉时指向前序终态 Episode 的父外键。';
COMMENT ON COLUMN stock.r03_sector_rotation_episodes.frozen_member_count IS '冻结篮子 B=M[t0-1] 成员数。';
COMMENT ON COLUMN stock.r03_sector_rotation_episodes.s0_count IS 't0 已知 Seed 集合 S0 成员数。';
COMMENT ON COLUMN stock.r03_sector_rotation_episodes.u0_count IS 't0 正 ret1 集合 U0 成员数。';
COMMENT ON COLUMN stock.r03_sector_rotation_episodes.h0_count IS 't0-1 RPS20>=80 集合 H0 成员数。';
COMMENT ON COLUMN stock.r03_sector_rotation_episodes.left_censored IS '是否因历史左截断出生。';
COMMENT ON COLUMN stock.r03_sector_rotation_episodes.recorded_at IS '写入 PG 的带时区时间戳。';

-- 3. R03 正式 Rotation 逐日状态推进事实
CREATE TABLE stock.r03_sector_rotation_day_states (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    episode_id bigint NOT NULL REFERENCES stock.r03_sector_rotation_episodes(id) ON DELETE CASCADE,
    trade_date date NOT NULL,
    rotation_state text NOT NULL CHECK (rotation_state IN ('NONE', 'PULSE', 'IN', 'ACCEPTED', 'EXPANDING', 'REACCELERATING', 'FAILED', 'OUT', 'UNKNOWN', 'REJECTED')),
    eval_age integer NOT NULL CHECK (eval_age >= 0),
    accepted_once boolean NOT NULL DEFAULT false,
    armed boolean NOT NULL DEFAULT false,
    out_neg_streak integer NOT NULL DEFAULT 0 CHECK (out_neg_streak >= 0),
    formation_r numeric,
    post_r numeric,
    seed_retention numeric,
    breadth_retention numeric,
    strong_retention numeric,
    diffusion_state text NOT NULL CHECK (diffusion_state IN ('TRUE', 'FALSE', 'UNKNOWN')),
    early_state text NOT NULL CHECK (early_state IN ('TRUE', 'FALSE', 'UNKNOWN')),
    mature_state text NOT NULL CHECK (mature_state IN ('TRUE', 'FALSE', 'UNKNOWN', 'NA')),
    reason_code text,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT r03_sector_rotation_day_states_uq UNIQUE (episode_id, trade_date)
);

ALTER TABLE stock.r03_sector_rotation_day_states OWNER TO stock_a;
COMMENT ON TABLE stock.r03_sector_rotation_day_states IS '保存正式 Rotation Episode 按官方交易日推进的 10 步有序状态机结果、留存率、扩散与双基准累计收益。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.id IS '自增主键。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.episode_id IS '所属 Rotation Episode 主键。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.trade_date IS '推进目标官方交易日。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.rotation_state IS '§11.3 状态机当日输出状态。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.eval_age IS '可评估后继有效会话计数（UNKNOWN 暂停）。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.accepted_once IS '该 Episode 是否曾达到 ACCEPTED。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.armed IS '终态后是否已由已知 FALSE pulse 重新武装。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.out_neg_streak IS 'OUT 条件连续负会话计数。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.formation_r IS '相对 t0-1 基准 b_j 的固定篮子累计收益。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.post_r IS '相对 t0 基准 p_j 的固定篮子后验收益（t0=0）。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.seed_retention IS 'S0 Seed 留存比例。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.breadth_retention IS 'U0 价格留存比例。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.strong_retention IS 'H0 强势（RPS20>=80）留存比例。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.diffusion_state IS '固定篮子扩散三态。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.early_state IS 'early_retained 三态。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.mature_state IS 'mature_retained 四态（含 NA）。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.reason_code IS '状态转换、超时、拒绝或未知原因码。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.recorded_at IS '写入 PG 的带时区时间戳。';

-- 4. R03 正式 Overlap 主题簇与代表事实
CREATE TABLE stock.r03_sector_overlap_clusters (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    trade_date date NOT NULL,
    board_kind text NOT NULL CHECK (board_kind IN ('INDUSTRY', 'CONCEPT')),
    cluster_id text NOT NULL,
    scope_key text NOT NULL,
    contract_version text NOT NULL,
    cluster_status text NOT NULL CHECK (cluster_status IN ('KNOWN', 'UNKNOWN')),
    member_board_ids text[] NOT NULL,
    parent_cluster_ids text[] NOT NULL DEFAULT '{}',
    representative_board_id text,
    representative_status text NOT NULL CHECK (representative_status IN ('SELECTED', 'FALLBACK_ID', 'UNKNOWN')),
    recorded_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT r03_sector_overlap_clusters_uq UNIQUE (trade_date, board_kind, cluster_id, scope_key, contract_version)
);

ALTER TABLE stock.r03_sector_overlap_clusters OWNER TO stock_a;
COMMENT ON TABLE stock.r03_sector_overlap_clusters IS '保存 ALG-25 同类型合格板块 Overlap 连通分量簇、parent 分裂合并追溯与 Priority 代表。';
COMMENT ON COLUMN stock.r03_sector_overlap_clusters.id IS '自增主键。';
COMMENT ON COLUMN stock.r03_sector_overlap_clusters.trade_date IS '目标官方交易日。';
COMMENT ON COLUMN stock.r03_sector_overlap_clusters.board_kind IS '板块类型（行业/概念绝不混簇）。';
COMMENT ON COLUMN stock.r03_sector_overlap_clusters.cluster_id IS '按成员板块集合生成的确定性簇标识。';
COMMENT ON COLUMN stock.r03_sector_overlap_clusters.scope_key IS '数据捕获与研究口径标识。';
COMMENT ON COLUMN stock.r03_sector_overlap_clusters.contract_version IS '算法合同版本号。';
COMMENT ON COLUMN stock.r03_sector_overlap_clusters.cluster_status IS '簇边界是否因未知边跨分量而未知。';
COMMENT ON COLUMN stock.r03_sector_overlap_clusters.member_board_ids IS '簇内成员板块稳定标识数组。';
COMMENT ON COLUMN stock.r03_sector_overlap_clusters.parent_cluster_ids IS '上一交易日相交父簇标识数组。';
COMMENT ON COLUMN stock.r03_sector_overlap_clusters.representative_board_id IS '按板块 Priority 选出的代表板块标识。';
COMMENT ON COLUMN stock.r03_sector_overlap_clusters.representative_status IS '代表选择状态（SELECTED/FALLBACK_ID/UNKNOWN）。';
COMMENT ON COLUMN stock.r03_sector_overlap_clusters.recorded_at IS '写入 PG 的带时区时间戳。';

-- 5. R03 正式 LOO 与算法支持板块事实
CREATE TABLE stock.r03_sector_loo_support_facts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    trade_date date NOT NULL,
    target_entity_id text NOT NULL,
    board_kind text NOT NULL CHECK (board_kind IN ('INDUSTRY', 'CONCEPT')),
    scope_key text NOT NULL,
    contract_version text NOT NULL,
    primary_industry_id text,
    algorithm_support_board_id text,
    support_status text NOT NULL CHECK (support_status IN ('SELECTED', 'NA', 'UNKNOWN')),
    reason_code text,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT r03_sector_loo_support_facts_uq UNIQUE (trade_date, target_entity_id, board_kind, scope_key, contract_version)
);

ALTER TABLE stock.r03_sector_loo_support_facts OWNER TO stock_a;
COMMENT ON TABLE stock.r03_sector_loo_support_facts IS '保存 ALG-25 剔除目标证券后完整重算得出的确定性算法支持板块及与主行业分离的关系事实。';
COMMENT ON COLUMN stock.r03_sector_loo_support_facts.id IS '自增主键。';
COMMENT ON COLUMN stock.r03_sector_loo_support_facts.trade_date IS '目标官方交易日。';
COMMENT ON COLUMN stock.r03_sector_loo_support_facts.target_entity_id IS '被剔除重算的目标稳定证券实体标识。';
COMMENT ON COLUMN stock.r03_sector_loo_support_facts.board_kind IS 'LOO 比较所属板块类型。';
COMMENT ON COLUMN stock.r03_sector_loo_support_facts.scope_key IS '数据捕获与研究口径标识。';
COMMENT ON COLUMN stock.r03_sector_loo_support_facts.contract_version IS '算法合同版本号。';
COMMENT ON COLUMN stock.r03_sector_loo_support_facts.primary_industry_id IS '日期有效主行业标识（独立于算法支持板块）。';
COMMENT ON COLUMN stock.r03_sector_loo_support_facts.algorithm_support_board_id IS 'LOO 重算后确定性选出的算法支持板块标识。';
COMMENT ON COLUMN stock.r03_sector_loo_support_facts.support_status IS '支持板块选择状态（SELECTED/NA/UNKNOWN）。';
COMMENT ON COLUMN stock.r03_sector_loo_support_facts.reason_code IS '无支持板块或未知时的原因码。';
COMMENT ON COLUMN stock.r03_sector_loo_support_facts.recorded_at IS '写入 PG 的带时区时间戳。';
