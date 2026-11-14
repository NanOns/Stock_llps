CREATE TABLE stock.source_s2_t1_cohort_stats (
    target_session date NOT NULL,
    endpoint_session date NOT NULL,
    research_cutoff date NOT NULL,
    source_package_id bigint NOT NULL,
    source_capture_id bigint NOT NULL,
    endpoint_source_capture_id bigint NOT NULL,
    t0_profile_revision text NOT NULL,
    endpoint_profile_revision text NOT NULL,
    producer_contract text NOT NULL,
    producer_version text NOT NULL,
    t0_history_session_count smallint NOT NULL,
    endpoint_history_session_count smallint NOT NULL,
    calendar_id text NOT NULL,
    calendar_revision text NOT NULL,
    path_rule_version text NOT NULL,
    cohort_version text NOT NULL,
    research_mode text NOT NULL,
    price_coordinate text NOT NULL,
    t0_s2_state text NOT NULL,
    t1_s2_state text NOT NULL,
    source_pool_count bigint NOT NULL,
    member_count bigint NOT NULL,
    endpoint_observed_count bigint NOT NULL,
    endpoint_unknown_count bigint NOT NULL,
    endpoint_other_count bigint NOT NULL,
    mean_return_numerator numeric,
    mean_return_denominator numeric,
    median_return_numerator numeric,
    median_return_denominator numeric,
    min_return_numerator numeric,
    min_return_denominator numeric,
    max_return_numerator numeric,
    max_return_denominator numeric,
    positive_count bigint NOT NULL,
    non_positive_count bigint NOT NULL,
    failure_reason_subject_count bigint NOT NULL,
    failure_reason_codes text[] NOT NULL,
    failure_reason_counts bigint[] NOT NULL,
    endpoint_unknown_reason_codes text[] NOT NULL,
    endpoint_unknown_reason_counts bigint[] NOT NULL,
    CONSTRAINT source_s2_t1_cohort_stats_pkey PRIMARY KEY (
        target_session,
        endpoint_session,
        research_cutoff,
        source_package_id,
        source_capture_id,
        endpoint_source_capture_id,
        t0_profile_revision,
        endpoint_profile_revision,
        producer_contract,
        producer_version,
        t0_history_session_count,
        endpoint_history_session_count,
        calendar_id,
        calendar_revision,
        path_rule_version,
        cohort_version,
        t0_s2_state,
        t1_s2_state
    ),
    CONSTRAINT source_s2_t1_cohort_captures_fk FOREIGN KEY (source_capture_id)
        REFERENCES stock.source_captures(capture_id),
    CONSTRAINT source_s2_t1_cohort_endpoint_capture_fk FOREIGN KEY (endpoint_source_capture_id)
        REFERENCES stock.source_captures(capture_id),
    CONSTRAINT source_s2_t1_cohort_package_fk FOREIGN KEY (source_package_id)
        REFERENCES stock.source_packages(source_package_id),
    CONSTRAINT source_s2_t1_cohort_sessions_ck CHECK (
        target_session < endpoint_session AND endpoint_session <= research_cutoff
    ),
    CONSTRAINT source_s2_t1_cohort_source_scope_ck CHECK (
        source_pool_count > 0
        AND member_count BETWEEN 0 AND source_pool_count
        AND t0_history_session_count BETWEEN 1 AND 75
        AND endpoint_history_session_count BETWEEN 1 AND 75
    ),
    CONSTRAINT source_s2_t1_cohort_state_ck CHECK (
        t0_s2_state IN ('TRUE', 'FALSE', 'UNKNOWN')
        AND t1_s2_state IN ('ALL', 'TRUE', 'FALSE', 'UNKNOWN')
        AND cohort_version = 'SOURCE_S2_T1_COHORT_R1'
        AND research_mode = 'NON_PIT_EXPLORATORY'
        AND price_coordinate = 'RAW_UNADJUSTED'
    ),
    CONSTRAINT source_s2_t1_cohort_counts_ck CHECK (
        endpoint_observed_count >= 0
        AND endpoint_unknown_count >= 0
        AND endpoint_other_count >= 0
        AND endpoint_observed_count + endpoint_unknown_count + endpoint_other_count = member_count
        AND positive_count >= 0
        AND non_positive_count >= 0
        AND positive_count + non_positive_count = endpoint_observed_count
        AND failure_reason_subject_count BETWEEN 0 AND member_count
    ),
    CONSTRAINT source_s2_t1_cohort_mean_pair_ck CHECK (
        (mean_return_numerator IS NULL) = (mean_return_denominator IS NULL)
        AND (mean_return_denominator IS NULL OR mean_return_denominator > 0)
    ),
    CONSTRAINT source_s2_t1_cohort_median_pair_ck CHECK (
        (median_return_numerator IS NULL) = (median_return_denominator IS NULL)
        AND (median_return_denominator IS NULL OR median_return_denominator > 0)
    ),
    CONSTRAINT source_s2_t1_cohort_min_pair_ck CHECK (
        (min_return_numerator IS NULL) = (min_return_denominator IS NULL)
        AND (min_return_denominator IS NULL OR min_return_denominator > 0)
    ),
    CONSTRAINT source_s2_t1_cohort_max_pair_ck CHECK (
        (max_return_numerator IS NULL) = (max_return_denominator IS NULL)
        AND (max_return_denominator IS NULL OR max_return_denominator > 0)
    ),
    CONSTRAINT source_s2_t1_cohort_return_summary_ck CHECK (
        (endpoint_observed_count = 0
            AND mean_return_numerator IS NULL AND median_return_numerator IS NULL
            AND min_return_numerator IS NULL AND max_return_numerator IS NULL)
        OR
        (endpoint_observed_count > 0
            AND mean_return_numerator IS NOT NULL AND median_return_numerator IS NOT NULL
            AND min_return_numerator IS NOT NULL AND max_return_numerator IS NOT NULL)
    ),
    CONSTRAINT source_s2_t1_cohort_reason_arrays_ck CHECK (
        cardinality(failure_reason_codes) = cardinality(failure_reason_counts)
        AND cardinality(endpoint_unknown_reason_codes) = cardinality(endpoint_unknown_reason_counts)
    )
);

CREATE INDEX source_s2_t1_cohort_session_idx
    ON stock.source_s2_t1_cohort_stats
    (target_session, source_package_id, source_capture_id, endpoint_session, endpoint_source_capture_id);

COMMENT ON TABLE stock.source_s2_t1_cohort_stats IS '保存来源形态S2固定T0三态分组的同源T+1收盘收益描述统计及后继谓词原因；收益为NON_PIT、RAW_UNADJUSTED研究，不表示正式入选或交易有效性。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.target_session IS '固定T0 S2分组所用的官方交易会话日期。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.endpoint_session IS 'T0之后唯一已存官方T+1收益端点会话。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.research_cutoff IS '允许读取已存来源事实的研究截止会话。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.source_package_id IS 'T0与T+1共用的Raw来源包数字ID。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.source_capture_id IS '固定T0来源事实的唯一捕获数字ID。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.endpoint_source_capture_id IS 'T+1来源事实的唯一捕获数字ID。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.t0_profile_revision IS '冻结T0 S2状态所绑定的画像修订。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.endpoint_profile_revision IS 'T+1事后S2状态所绑定的画像修订。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.producer_contract IS '两端Seed使用的来源资格与算法合同标识。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.producer_version IS '两端Seed生产者版本标识。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.t0_history_session_count IS 'T0画像实际使用的官方会话窗口数。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.endpoint_history_session_count IS 'T+1画像实际使用的官方会话窗口数。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.calendar_id IS '两端使用的官方交易日历标识。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.calendar_revision IS 'T0、T+1及收益端点共同使用的官方日历修订。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.path_rule_version IS 'RAW_UNADJUSTED T+1收盘收益路径算法版本。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.cohort_version IS '固定T0 S2组收益摘要算法版本。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.research_mode IS '统计研究模式；固定为NON_PIT_EXPLORATORY。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.price_coordinate IS '收益价格坐标；固定为RAW_UNADJUSTED原始未复权。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.t0_s2_state IS '事前固定的T0来源形态S2三态分组。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.t1_s2_state IS 'T+1事后分层；ALL表示T0组全体，其他值为后验S2三态。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.source_pool_count IS '本次T0固定来源池总分母；所有统计行均绑定同一完整池。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.member_count IS '本T0状态及可选T+1后验状态组合的股票分母。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.endpoint_observed_count IS '具有合法T+1端点收盘收益的股票数；均值和中位数仅以此为分母。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.endpoint_unknown_count IS 'T+1收盘收益质量为UNKNOWN的股票数，不以零收益代替。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.endpoint_other_count IS '端点质量既非OBSERVED也非UNKNOWN的不可评价股票数。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.mean_return_numerator IS 'OBSERVED_ONLY等权平均收益精确有理数分子；无可评价值时为空。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.mean_return_denominator IS 'OBSERVED_ONLY等权平均收益精确有理数正分母；无可评价值时为空。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.median_return_numerator IS 'OBSERVED_ONLY收益精确中位数分子；偶数样本取中间两值均值。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.median_return_denominator IS 'OBSERVED_ONLY收益精确中位数正分母。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.min_return_numerator IS 'OBSERVED_ONLY样本最小收益精确分子；无可评价值时为空。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.min_return_denominator IS 'OBSERVED_ONLY样本最小收益精确正分母。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.max_return_numerator IS 'OBSERVED_ONLY样本最大收益精确分子；无可评价值时为空。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.max_return_denominator IS 'OBSERVED_ONLY样本最大收益精确正分母。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.positive_count IS 'OBSERVED_ONLY中收益大于零的股票数。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.non_positive_count IS 'OBSERVED_ONLY中收益小于或等于零的股票数。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.failure_reason_subject_count IS '具有至少一个T+1 S2 FALSE或UNKNOWN原因码的独立股票数。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.failure_reason_codes IS 'T+1 S2未为TRUE的冻结谓词原因码；各原因可在同一股票共现。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.failure_reason_counts IS '按原因码数组位置配对的独立股票出现频数，不可求和解释为独立股票数。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.endpoint_unknown_reason_codes IS 'T+1端点收益UNKNOWN的原因码，不包含S2状态原因。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.endpoint_unknown_reason_counts IS '按端点UNKNOWN原因码数组位置配对的股票频数。';

ALTER TABLE stock.source_s2_t1_cohort_stats OWNER TO stock_a;
