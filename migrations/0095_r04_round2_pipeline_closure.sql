-- R04 第二轮正式生产链闭环前向迁移（0095）：
-- 1. 移除 d2_episode_day_states 与 d2_primary_episode_day_facts 唯一业务键中的 rule_identity，确保同日每业务对象单一有效事实；
-- 2. 在 d2_episodes 补充 failure_window 与 evidence_ref，无损保留 T0 冻结证据；
-- 3. 新建 stock.source_d1_anchors 与 stock.source_d1_anchor_day_observations，提供跨日稳定活跃锚身份、全量日观察与留存读回。

-- ============================================================================
-- 1. D2 单一有效业务键与 T0 冻结证据补充
-- ============================================================================
ALTER TABLE stock.d2_episode_day_states
    DROP CONSTRAINT IF EXISTS d2_episode_day_states_uq;

ALTER TABLE stock.d2_episode_day_states
    ADD CONSTRAINT d2_episode_day_states_uq
        UNIQUE (entity_type, entity_id, calendar_id, session_date, episode_id);

ALTER TABLE stock.d2_primary_episode_day_facts
    DROP CONSTRAINT IF EXISTS d2_primary_episode_day_facts_uq;

ALTER TABLE stock.d2_primary_episode_day_facts
    ADD CONSTRAINT d2_primary_episode_day_facts_uq
        UNIQUE (entity_type, entity_id, calendar_id, session_date);

ALTER TABLE stock.d2_episodes
    ADD COLUMN IF NOT EXISTS failure_window text,
    ADD COLUMN IF NOT EXISTS evidence_ref text;

COMMENT ON COLUMN stock.d2_episodes.failure_window IS 'T0冻结失效线来源窗口文本（如出生日起止窗口），日推进时不覆盖。';
COMMENT ON COLUMN stock.d2_episodes.evidence_ref IS 'T0原始准入事件证据引用，日推进时不以当日input_ref替代。';

-- ============================================================================
-- 2. D1 跨日稳定出生锚实体表与全量日观察表
-- ============================================================================
CREATE TABLE stock.source_d1_anchors (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    anchor_uid text NOT NULL UNIQUE,
    source_symbol text NOT NULL CHECK (source_symbol ~ '^(SH|SZ)\.[0-9]{6}$'),
    structure_type text NOT NULL CHECK (
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
    ),
    relation text NOT NULL CHECK (relation IN ('SUPPORT', 'RESISTANCE')),
    created_session date NOT NULL,
    available_from_session date NOT NULL,
    test_from_session date,
    coordinate_id text NOT NULL,
    lower_numerator numeric NOT NULL,
    lower_denominator numeric NOT NULL CHECK (lower_denominator > 0),
    upper_numerator numeric NOT NULL,
    upper_denominator numeric NOT NULL CHECK (upper_denominator > 0),
    source_event_id text,
    source_ref text,
    birth_capture_id bigint
);

CREATE INDEX source_d1_anchors_symbol_session_idx
    ON stock.source_d1_anchors (source_symbol, created_session);

COMMENT ON TABLE stock.source_d1_anchors IS 'D1稳定出生锚实体表：保存跨日不变的锚数字主键、短业务唯一键及冻结价格带。';
COMMENT ON COLUMN stock.source_d1_anchors.id IS '跨日稳定D1锚自增主键ID。';
COMMENT ON COLUMN stock.source_d1_anchors.anchor_uid IS '跨日稳定短业务唯一键（symbol:type:created_session:event_key）。';
COMMENT ON COLUMN stock.source_d1_anchors.source_symbol IS '证券代码（SH/SZ.XXXXXX）。';
COMMENT ON COLUMN stock.source_d1_anchors.structure_type IS 'D1十类结构锚点类型。';
COMMENT ON COLUMN stock.source_d1_anchors.relation IS '锚点方向：SUPPORT支撑或RESISTANCE阻力。';
COMMENT ON COLUMN stock.source_d1_anchors.created_session IS '锚点出生官方交易日。';
COMMENT ON COLUMN stock.source_d1_anchors.available_from_session IS '锚点最早可用交易日。';
COMMENT ON COLUMN stock.source_d1_anchors.test_from_session IS '锚点最早可测交易日（出生次日起测）。';
COMMENT ON COLUMN stock.source_d1_anchors.coordinate_id IS '出生价格坐标标识。';
COMMENT ON COLUMN stock.source_d1_anchors.lower_numerator IS '价格带下沿分子。';
COMMENT ON COLUMN stock.source_d1_anchors.lower_denominator IS '价格带下沿分母（>0）。';
COMMENT ON COLUMN stock.source_d1_anchors.upper_numerator IS '价格带上沿分子。';
COMMENT ON COLUMN stock.source_d1_anchors.upper_denominator IS '价格带上沿分母（>0）。';
COMMENT ON COLUMN stock.source_d1_anchors.source_event_id IS '动态MA等关联的正式D0事件ID（非事件锚为NULL）。';
COMMENT ON COLUMN stock.source_d1_anchors.source_ref IS '锚点来源说明（如T0_MA5/STRUCT）。';
COMMENT ON COLUMN stock.source_d1_anchors.birth_capture_id IS '出生日来源捕获ID（仅溯源）。';

CREATE TABLE stock.source_d1_anchor_day_observations (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    anchor_id bigint NOT NULL REFERENCES stock.source_d1_anchors(id) ON DELETE CASCADE,
    anchor_uid text NOT NULL,
    target_session date NOT NULL,
    source_symbol text NOT NULL CHECK (source_symbol ~ '^(SH|SZ)\.[0-9]{6}$'),
    structure_type text NOT NULL,
    observation_status text NOT NULL CHECK (observation_status IN ('BIRTH_IDLE', 'OBSERVED', 'UNKNOWN', 'NOT_COMPARABLE')),
    state text NOT NULL,
    quality text NOT NULL CHECK (quality IN ('OBSERVED', 'UNKNOWN')),
    acceptance text NOT NULL,
    freshness text NOT NULL,
    breach_streak integer NOT NULL CHECK (breach_streak >= 0),
    acceptance_streak integer NOT NULL CHECK (acceptance_streak >= 0),
    test_count integer NOT NULL CHECK (test_count >= 0),
    separated_sessions integer NOT NULL CHECK (separated_sessions >= 0),
    retest_qualified boolean NOT NULL,
    is_active boolean NOT NULL,
    reason text,
    comparability_status text NOT NULL CHECK (comparability_status IN ('COMPARABLE', 'NOT_COMPARABLE')),
    observation_capture_id bigint,
    impulse_retention_1_numerator numeric,
    impulse_retention_1_denominator numeric,
    impulse_retention_1_quality text NOT NULL DEFAULT 'NOT_APPLICABLE'
        CHECK (impulse_retention_1_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_APPLICABLE', 'NOT_DUE')),
    impulse_retention_1_reason text,
    impulse_retention_3_numerator numeric,
    impulse_retention_3_denominator numeric,
    impulse_retention_3_quality text NOT NULL DEFAULT 'NOT_APPLICABLE'
        CHECK (impulse_retention_3_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_APPLICABLE', 'NOT_DUE')),
    impulse_retention_3_reason text,
    CONSTRAINT source_d1_anchor_day_obs_uq UNIQUE (anchor_id, target_session),
    CONSTRAINT source_d1_anchor_day_obs_ret1_ck CHECK ((impulse_retention_1_numerator IS NULL) = (impulse_retention_1_denominator IS NULL) AND (impulse_retention_1_denominator IS NULL OR impulse_retention_1_denominator > 0)),
    CONSTRAINT source_d1_anchor_day_obs_ret3_ck CHECK ((impulse_retention_3_numerator IS NULL) = (impulse_retention_3_denominator IS NULL) AND (impulse_retention_3_denominator IS NULL OR impulse_retention_3_denominator > 0))
);

CREATE INDEX source_d1_anchor_day_obs_session_sym_idx
    ON stock.source_d1_anchor_day_observations (target_session, source_symbol, is_active);

COMMENT ON TABLE stock.source_d1_anchor_day_observations IS 'D1支撑锚全量逐日观察表：保存每日所有活跃及当日失效支撑锚状态与中阳留存率，含未变化锚。';
COMMENT ON COLUMN stock.source_d1_anchor_day_observations.id IS '自增主键ID。';
COMMENT ON COLUMN stock.source_d1_anchor_day_observations.anchor_id IS '关联稳定出生锚主键ID。';
COMMENT ON COLUMN stock.source_d1_anchor_day_observations.anchor_uid IS '稳定出生锚业务唯一键。';
COMMENT ON COLUMN stock.source_d1_anchor_day_observations.target_session IS '观察交易日。';
COMMENT ON COLUMN stock.source_d1_anchor_day_observations.source_symbol IS '证券代码。';
COMMENT ON COLUMN stock.source_d1_anchor_day_observations.structure_type IS '结构锚点类型。';
COMMENT ON COLUMN stock.source_d1_anchor_day_observations.observation_status IS '日观察状态：BIRTH_IDLE/OBSERVED/UNKNOWN/NOT_COMPARABLE。';
COMMENT ON COLUMN stock.source_d1_anchor_day_observations.state IS '支撑锚当日状态（IDLE/TESTING/HELD_CONFIRMED/BREACHED_SHALLOW/BROKEN等）。';
COMMENT ON COLUMN stock.source_d1_anchor_day_observations.quality IS '观察质量：OBSERVED/UNKNOWN。';
COMMENT ON COLUMN stock.source_d1_anchor_day_observations.acceptance IS '接受状态。';
COMMENT ON COLUMN stock.source_d1_anchor_day_observations.freshness IS '新鲜度（FRESH/STALE）。';
COMMENT ON COLUMN stock.source_d1_anchor_day_observations.breach_streak IS '连续跌破天数。';
COMMENT ON COLUMN stock.source_d1_anchor_day_observations.acceptance_streak IS '连续接受天数。';
COMMENT ON COLUMN stock.source_d1_anchor_day_observations.test_count IS '累计有效测试次数。';
COMMENT ON COLUMN stock.source_d1_anchor_day_observations.separated_sessions IS '分离复测间隔会话数。';
COMMENT ON COLUMN stock.source_d1_anchor_day_observations.retest_qualified IS '是否满足分离复测资格。';
COMMENT ON COLUMN stock.source_d1_anchor_day_observations.is_active IS '当日观察后是否仍为活跃支撑（BROKEN/过期为FALSE）。';
COMMENT ON COLUMN stock.source_d1_anchor_day_observations.reason IS '观察质量或状态原因码。';
COMMENT ON COLUMN stock.source_d1_anchor_day_observations.comparability_status IS '经济坐标可比性状态：COMPARABLE/NOT_COMPARABLE。';
COMMENT ON COLUMN stock.source_d1_anchor_day_observations.observation_capture_id IS '观察日来源捕获ID（仅溯源）。';
COMMENT ON COLUMN stock.source_d1_anchor_day_observations.impulse_retention_1_numerator IS '中阳T+1留存率分子。';
COMMENT ON COLUMN stock.source_d1_anchor_day_observations.impulse_retention_1_denominator IS '中阳T+1留存率分母（>0）。';
COMMENT ON COLUMN stock.source_d1_anchor_day_observations.impulse_retention_1_quality IS '中阳T+1留存率质量（OBSERVED/UNKNOWN/NOT_APPLICABLE/NOT_DUE）。';
COMMENT ON COLUMN stock.source_d1_anchor_day_observations.impulse_retention_1_reason IS '中阳T+1留存率原因码。';
COMMENT ON COLUMN stock.source_d1_anchor_day_observations.impulse_retention_3_numerator IS '中阳T+3留存率分子。';
COMMENT ON COLUMN stock.source_d1_anchor_day_observations.impulse_retention_3_denominator IS '中阳T+3留存率分母（>0）。';
COMMENT ON COLUMN stock.source_d1_anchor_day_observations.impulse_retention_3_quality IS '中阳T+3留存率质量（OBSERVED/UNKNOWN/NOT_APPLICABLE/NOT_DUE）。';
COMMENT ON COLUMN stock.source_d1_anchor_day_observations.impulse_retention_3_reason IS '中阳T+3留存率原因码。';
