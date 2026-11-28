-- R04 第三轮续修前向迁移（0097）：
-- 建立 PB/CONTINUE 逐父/峰情景评估明细表 stock.d0_parent_scenario_detail_facts，
-- 用于按真实业务维度持久化多父/峰的原始三态、具名谓词、正式准入状态与冻结线证据。

CREATE TABLE stock.d0_parent_scenario_detail_facts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    target_session date NOT NULL,
    source_symbol text NOT NULL,
    scenario text NOT NULL CHECK (scenario IN ('STRONG_PULLBACK', 'TREND_CONTINUE')),
    parent_episode_id text NOT NULL,
    peak_event_id text,
    peak_ordinal_key integer NOT NULL DEFAULT -1,
    raw_state text NOT NULL CHECK (raw_state IN ('TRUE', 'FALSE', 'UNKNOWN')),
    raw_reason text NOT NULL,
    predicate_ids text[] NOT NULL,
    predicate_states text[] NOT NULL CHECK (predicate_states <@ ARRAY['TRUE', 'FALSE', 'UNKNOWN']::text[]),
    predicate_qualities text[] NOT NULL CHECK (predicate_qualities <@ ARRAY['OBSERVED', 'UNKNOWN']::text[]),
    predicate_reason_codes text[] NOT NULL,
    predicate_value_numerators numeric[],
    predicate_value_denominators numeric[],
    admission_status text NOT NULL CHECK (admission_status IN ('READY', 'BLOCKED', 'NOT_READY', 'NOT_REQUIRED')),
    admission_reason text NOT NULL,
    reason_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
    event_id text,
    linked_anchor_id text,
    failure_level_numerator numeric,
    failure_level_denominator numeric,
    failure_coordinate text CHECK (failure_coordinate IS NULL OR failure_coordinate IN ('RAW_UNADJUSTED', 'FORWARD_ADJUSTED')),
    failure_source text CHECK (failure_source IS NULL OR failure_source IN ('T0_PHC20', 'T0_MA20', 'T0_MA5', 'PRIOR_SUPPORT_LOWER', 'T0_MA20_FALLBACK')),
    failure_window_start date,
    failure_window_session_count integer,
    effective_from_session date,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT d0_parent_scenario_detail_facts_uq
        UNIQUE (target_session, source_symbol, scenario, parent_episode_id, peak_ordinal_key),
    CONSTRAINT d0_parent_scenario_detail_facts_vector_ck
        CHECK (
            cardinality(predicate_ids) = cardinality(predicate_states)
            AND cardinality(predicate_ids) = cardinality(predicate_qualities)
            AND cardinality(predicate_ids) = cardinality(predicate_reason_codes)
            AND (
                predicate_value_numerators IS NULL
                OR cardinality(predicate_ids) = cardinality(predicate_value_numerators)
            )
            AND (
                predicate_value_denominators IS NULL
                OR cardinality(predicate_ids) = cardinality(predicate_value_denominators)
            )
        ),
    CONSTRAINT d0_parent_scenario_detail_facts_line_ck
        CHECK (
            (
                admission_status = 'READY'
                AND raw_state = 'TRUE'
                AND event_id IS NOT NULL
                AND failure_level_numerator IS NOT NULL
                AND failure_level_denominator IS NOT NULL
                AND failure_level_denominator > 0
                AND failure_coordinate IS NOT NULL
                AND failure_source IS NOT NULL
                AND effective_from_session > target_session
            )
            OR
            (
                admission_status <> 'READY'
                AND event_id IS NULL
                AND failure_level_numerator IS NULL
                AND failure_level_denominator IS NULL
            )
        )
);

CREATE INDEX idx_d0_parent_scenario_detail_facts_session_symbol
    ON stock.d0_parent_scenario_detail_facts (target_session, source_symbol, scenario);

COMMENT ON TABLE stock.d0_parent_scenario_detail_facts IS 'PB/CONTINUE逐父episode与峰事件评估及正式准入明细事实表（按单一有效业务键存储，不设多版本）。';
COMMENT ON COLUMN stock.d0_parent_scenario_detail_facts.id IS '自增代理主键。';
COMMENT ON COLUMN stock.d0_parent_scenario_detail_facts.target_session IS '评估目标交易日（YYYY-MM-DD）。';
COMMENT ON COLUMN stock.d0_parent_scenario_detail_facts.source_symbol IS '证券代码（如SZ.003990）。';
COMMENT ON COLUMN stock.d0_parent_scenario_detail_facts.scenario IS '延续场景类型：STRONG_PULLBACK或TREND_CONTINUE。';
COMMENT ON COLUMN stock.d0_parent_scenario_detail_facts.parent_episode_id IS 't-1合法FORMAL父episode业务ID。';
COMMENT ON COLUMN stock.d0_parent_scenario_detail_facts.peak_event_id IS 'STRONG_PULLBACK关联的t-1冻结峰事件ID；TREND_CONTINUE或无峰时为NULL。';
COMMENT ON COLUMN stock.d0_parent_scenario_detail_facts.peak_ordinal_key IS '峰值会话序号键（无峰或TREND_CONTINUE固定为-1，用于唯一业务键）。';
COMMENT ON COLUMN stock.d0_parent_scenario_detail_facts.raw_state IS '该父/峰组合的D0原始场景三态（TRUE/FALSE/UNKNOWN）。';
COMMENT ON COLUMN stock.d0_parent_scenario_detail_facts.raw_reason IS '该父/峰组合的原始场景判定原因码。';
COMMENT ON COLUMN stock.d0_parent_scenario_detail_facts.predicate_ids IS '具名谓词标识数组。';
COMMENT ON COLUMN stock.d0_parent_scenario_detail_facts.predicate_states IS '具名谓词三态数组（TRUE/FALSE/UNKNOWN）。';
COMMENT ON COLUMN stock.d0_parent_scenario_detail_facts.predicate_qualities IS '具名谓词质量数组（OBSERVED/UNKNOWN）。';
COMMENT ON COLUMN stock.d0_parent_scenario_detail_facts.predicate_reason_codes IS '具名谓词原因码数组。';
COMMENT ON COLUMN stock.d0_parent_scenario_detail_facts.predicate_value_numerators IS '具名谓词精确有理数值分子数组（无数值时为NULL）。';
COMMENT ON COLUMN stock.d0_parent_scenario_detail_facts.predicate_value_denominators IS '具名谓词精确有理数值分母数组（无数值时为NULL）。';
COMMENT ON COLUMN stock.d0_parent_scenario_detail_facts.admission_status IS '正式准入状态：READY/BLOCKED/NOT_READY/NOT_REQUIRED。';
COMMENT ON COLUMN stock.d0_parent_scenario_detail_facts.admission_reason IS '正式准入主原因码。';
COMMENT ON COLUMN stock.d0_parent_scenario_detail_facts.reason_codes IS '正式准入完整原因码数组。';
COMMENT ON COLUMN stock.d0_parent_scenario_detail_facts.event_id IS '准入READY时生成的正式事件ID；未准入时为NULL。';
COMMENT ON COLUMN stock.d0_parent_scenario_detail_facts.linked_anchor_id IS '该父/峰事件冻结关联的D1旧支撑锚UID（可为NULL）。';
COMMENT ON COLUMN stock.d0_parent_scenario_detail_facts.failure_level_numerator IS '冻结失效线精确有理数分子。';
COMMENT ON COLUMN stock.d0_parent_scenario_detail_facts.failure_level_denominator IS '冻结失效线精确有理数分母（正数）。';
COMMENT ON COLUMN stock.d0_parent_scenario_detail_facts.failure_coordinate IS '冻结失效线价格坐标（RAW_UNADJUSTED/FORWARD_ADJUSTED）。';
COMMENT ON COLUMN stock.d0_parent_scenario_detail_facts.failure_source IS '冻结失效线来源（PRIOR_SUPPORT_LOWER/T0_MA20_FALLBACK等）。';
COMMENT ON COLUMN stock.d0_parent_scenario_detail_facts.failure_window_start IS '冻结失效线窗口起始交易日。';
COMMENT ON COLUMN stock.d0_parent_scenario_detail_facts.failure_window_session_count IS '冻结失效线窗口会话数。';
COMMENT ON COLUMN stock.d0_parent_scenario_detail_facts.effective_from_session IS '冻结失效线生效起始交易日（严格晚于target_session）。';
COMMENT ON COLUMN stock.d0_parent_scenario_detail_facts.created_at IS '记录创建时间。';
COMMENT ON COLUMN stock.d0_parent_scenario_detail_facts.updated_at IS '记录最近更新时间。';
