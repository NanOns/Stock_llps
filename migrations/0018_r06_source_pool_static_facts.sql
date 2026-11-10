CREATE TABLE stock.source_d0_scenario_day_facts (
    namespace_id uuid NOT NULL,
    target_session date NOT NULL,
    source_symbol text NOT NULL,
    package_sha256 character(64) NOT NULL,
    source_capture_id uuid NOT NULL,
    source_raw_digest character(64) NOT NULL,
    parser_contract text NOT NULL,
    source_content_id character(64) NOT NULL,
    scenario text NOT NULL,
    producer_version text NOT NULL,
    algorithm_contract text NOT NULL,
    session_contract text NOT NULL,
    history_window_start date NOT NULL,
    history_session_count smallint NOT NULL,
    source_count integer NOT NULL,
    scenario_state text NOT NULL,
    scenario_reason text NOT NULL,
    predicate_ids text[] NOT NULL,
    predicate_states text[] NOT NULL,
    predicate_qualities text[] NOT NULL,
    predicate_reason_codes text[] NOT NULL,
    predicate_value_numerators numeric[] NOT NULL,
    predicate_value_denominators numeric[] NOT NULL,
    source_time_quality text NOT NULL,
    source_clock_role text NOT NULL,
    source_available_at timestamp with time zone,
    calendar_id text NOT NULL,
    calendar_revision text NOT NULL,
    calendar_sha256 character(64) NOT NULL,
    research_mode text NOT NULL,
    price_coordinate text NOT NULL,
    CONSTRAINT source_d0_scenario_day_facts_pkey PRIMARY KEY (
        namespace_id,
        target_session,
        source_symbol,
        package_sha256,
        source_capture_id,
        scenario,
        producer_version,
        calendar_revision,
        calendar_sha256
    ),
    CONSTRAINT source_d0_scenario_day_facts_scenario_ck CHECK (
        scenario IN ('LAUNCH_CONFIRM', 'RECOVERY_TURN')
    ),
    CONSTRAINT source_d0_scenario_day_facts_state_ck CHECK (
        scenario_state IN ('TRUE', 'FALSE', 'UNKNOWN')
    ),
    CONSTRAINT source_d0_scenario_day_facts_key_ck CHECK (
        source_symbol ~ '^(SH|SZ)\.[0-9]{6}$'
        AND left(source_symbol, 2) IN ('SH', 'SZ')
        AND package_sha256 ~ '^[0-9a-f]{64}$'
        AND source_raw_digest ~ '^[0-9a-f]{64}$'
        AND parser_contract <> ''
        AND source_content_id ~ '^[0-9a-f]{64}$'
        AND calendar_sha256 ~ '^[0-9a-f]{64}$'
        AND target_session >= history_window_start
        AND history_session_count BETWEEN 1 AND 75
        AND source_count > 0
        AND scenario_reason <> ''
        AND source_time_quality <> ''
        AND source_clock_role <> ''
        AND research_mode = 'NON_PIT_RETROSPECTIVE'
        AND price_coordinate = 'RAW_UNADJUSTED'
    ),
    CONSTRAINT source_d0_scenario_day_facts_vector_ck CHECK (
        cardinality(predicate_ids) = cardinality(predicate_states)
        AND cardinality(predicate_ids) = cardinality(predicate_qualities)
        AND cardinality(predicate_ids) = cardinality(predicate_reason_codes)
        AND cardinality(predicate_ids) = cardinality(predicate_value_numerators)
        AND cardinality(predicate_ids) = cardinality(predicate_value_denominators)
        AND array_position(predicate_ids, NULL) IS NULL
        AND array_position(predicate_states, NULL) IS NULL
        AND array_position(predicate_qualities, NULL) IS NULL
        AND array_position(predicate_reason_codes, NULL) IS NULL
        AND cardinality(predicate_ids) = CASE scenario
            WHEN 'LAUNCH_CONFIRM' THEN 5
            WHEN 'RECOVERY_TURN' THEN 6
        END
    ),
    CONSTRAINT source_d0_scenario_day_facts_predicate_state_ck CHECK (
        predicate_states <@ ARRAY['TRUE', 'FALSE', 'UNKNOWN']::text[]
    ),
    CONSTRAINT source_d0_scenario_day_facts_predicate_quality_ck CHECK (
        predicate_qualities <@ ARRAY['OBSERVED', 'UNKNOWN']::text[]
    )
);

CREATE INDEX source_d0_scenario_day_lookup_idx
    ON stock.source_d0_scenario_day_facts
    (namespace_id, target_session, package_sha256, scenario, source_symbol);

CREATE TABLE stock.source_pool_seed_day_facts (
    namespace_id uuid NOT NULL,
    target_session date NOT NULL,
    source_symbol text NOT NULL,
    market text NOT NULL,
    board text NOT NULL,
    package_sha256 character(64) NOT NULL,
    source_content_id character(64) NOT NULL,
    source_capture_id uuid NOT NULL,
    profile_revision character(64) NOT NULL,
    producer_contract text NOT NULL,
    producer_version text NOT NULL,
    producer_semantics_sha256 character(64) NOT NULL,
    history_window_start date NOT NULL,
    history_window_end date NOT NULL,
    history_session_count smallint NOT NULL,
    calendar_id text NOT NULL,
    calendar_revision text NOT NULL,
    calendar_sha256 character(64) NOT NULL,
    source_time_quality text NOT NULL,
    source_clock_role text NOT NULL,
    source_available_at timestamp with time zone,
    research_mode text NOT NULL,
    price_coordinate text NOT NULL,
    core_bias20_atr_numerator numeric,
    core_bias20_atr_denominator numeric,
    core_bias20_atr_quality text NOT NULL,
    core_bias20_atr_reason text,
    core_liquidity_mean20_numerator numeric,
    core_liquidity_mean20_denominator numeric,
    core_liquidity_mean20_quality text NOT NULL,
    core_liquidity_mean20_reason text,
    core_price_damage boolean,
    core_price_damage_quality text NOT NULL,
    core_price_damage_reason text,
    core_ma20_numerator numeric,
    core_ma20_denominator numeric,
    core_ma20_quality text NOT NULL,
    core_ma20_reason text,
    core_ret1_numerator numeric,
    core_ret1_denominator numeric,
    core_ret1_quality text NOT NULL,
    core_ret1_reason text,
    core_ret5_numerator numeric,
    core_ret5_denominator numeric,
    core_ret5_quality text NOT NULL,
    core_ret5_reason text,
    core_ret20_numerator numeric,
    core_ret20_denominator numeric,
    core_ret20_quality text NOT NULL,
    core_ret20_reason text,
    core_range_ratio_numerator numeric,
    core_range_ratio_denominator numeric,
    core_range_ratio_quality text NOT NULL,
    core_range_ratio_reason text,
    core_atr_ratio_numerator numeric,
    core_atr_ratio_denominator numeric,
    core_atr_ratio_quality text NOT NULL,
    core_atr_ratio_reason text,
    core_vol_ratio_numerator numeric,
    core_vol_ratio_denominator numeric,
    core_vol_ratio_quality text NOT NULL,
    core_vol_ratio_reason text,
    target_actual_quote boolean NOT NULL,
    target_close_numerator numeric,
    target_close_denominator numeric,
    target_close_quality text NOT NULL,
    target_close_reason text,
    profile_compression_value text,
    profile_compression_quality text NOT NULL,
    profile_compression_reason text,
    profile_extension_value text,
    profile_extension_quality text NOT NULL,
    profile_extension_reason text,
    profile_ma_structure_value text,
    profile_ma_structure_quality text NOT NULL,
    profile_ma_structure_reason text,
    rps5_numerator numeric,
    rps5_denominator numeric,
    rps5_quality text NOT NULL,
    rps5_reason text,
    rps5_delta3_numerator numeric,
    rps5_delta3_denominator numeric,
    rps5_delta3_quality text NOT NULL,
    rps5_delta3_reason text,
    rps20_numerator numeric,
    rps20_denominator numeric,
    rps20_quality text NOT NULL,
    rps20_reason text,
    rps20_delta3_numerator numeric,
    rps20_delta3_denominator numeric,
    rps20_delta3_quality text NOT NULL,
    rps20_delta3_reason text,
    previous_ma20_cross_state text NOT NULL,
    previous_ma20_cross_reason text,
    predicate_ids text[] NOT NULL,
    predicate_states text[] NOT NULL,
    predicate_reasons text[] NOT NULL,
    safety_state text NOT NULL,
    safety_reasons text[] NOT NULL,
    s1_state text NOT NULL,
    s1_reasons text[] NOT NULL,
    s2_state text NOT NULL,
    s2_reasons text[] NOT NULL,
    source_seed_state text NOT NULL,
    source_seed_reasons text[] NOT NULL,
    raw_prewatch_state text NOT NULL,
    raw_prewatch_reasons text[] NOT NULL,
    CONSTRAINT source_pool_seed_day_facts_pkey PRIMARY KEY (
        namespace_id,
        target_session,
        source_symbol,
        package_sha256,
        source_capture_id,
        profile_revision,
        producer_version
    ),
    CONSTRAINT source_pool_seed_day_facts_source_ck CHECK (
        source_symbol ~ '^(SH|SZ)\.[0-9]{6}$'
        AND market = left(source_symbol, 2)
        AND board IN ('MAIN_BOARD', 'GEM')
        AND package_sha256 ~ '^[0-9a-f]{64}$'
        AND source_content_id ~ '^[0-9a-f]{64}$'
        AND profile_revision ~ '^[0-9a-f]{64}$'
        AND producer_semantics_sha256 ~ '^[0-9a-f]{64}$'
        AND calendar_sha256 ~ '^[0-9a-f]{64}$'
        AND history_window_end = target_session
        AND history_window_start <= target_session
        AND history_session_count BETWEEN 1 AND 75
        AND source_time_quality <> ''
        AND source_clock_role <> ''
        AND research_mode = 'NON_PIT_RETROSPECTIVE'
        AND price_coordinate = 'RAW_UNADJUSTED'
    ),
    CONSTRAINT source_pool_seed_day_facts_predicate_vector_ck CHECK (
        predicate_ids = ARRAY[
            'research_universe', 'actual_bar', 'non_st', 'normal_listing_phase',
            'limited_price_rule', 'coordinate_ready', 'liquidity_mean20',
            'no_damage', 'no_severe_extension', 'bias_below_3', 'compression',
            'rps5_delta3_at_least_3', 'rps5_delta3_at_least_10',
            'bull_transition', 'previous_ma20_cross'
        ]::text[]
        AND cardinality(predicate_states) = cardinality(predicate_ids)
        AND cardinality(predicate_reasons) = cardinality(predicate_ids)
        AND array_position(predicate_states, NULL) IS NULL
    ),
    CONSTRAINT source_pool_seed_day_facts_state_ck CHECK (
        safety_state IN ('TRUE', 'FALSE', 'UNKNOWN')
        AND s1_state IN ('TRUE', 'FALSE', 'UNKNOWN')
        AND s2_state IN ('TRUE', 'FALSE', 'UNKNOWN')
        AND source_seed_state IN ('TRUE', 'FALSE', 'UNKNOWN')
        AND raw_prewatch_state IN ('TRUE', 'FALSE', 'UNKNOWN')
        AND previous_ma20_cross_state IN ('TRUE', 'FALSE', 'UNKNOWN')
        AND predicate_states <@ ARRAY['TRUE', 'FALSE', 'UNKNOWN']::text[]
    )
);

CREATE INDEX source_pool_seed_day_lookup_idx
    ON stock.source_pool_seed_day_facts
    (namespace_id, target_session, package_sha256, profile_revision, source_symbol);

COMMENT ON TABLE stock.source_d0_scenario_day_facts IS '逐来源股票保存现役已实现D0启动与恢复场景的单日三态及具名谓词；历史回看为NON_PIT研究。';
COMMENT ON COLUMN stock.source_d0_scenario_day_facts.namespace_id IS 'PG来源数据命名空间UUID。';
COMMENT ON COLUMN stock.source_d0_scenario_day_facts.target_session IS 'D0当前状态对应的官方交易会话。';
COMMENT ON COLUMN stock.source_d0_scenario_day_facts.source_symbol IS 'TDX来源股票代码，不代表稳定证券身份。';
COMMENT ON COLUMN stock.source_d0_scenario_day_facts.package_sha256 IS '精确Raw来源包SHA-256。';
COMMENT ON COLUMN stock.source_d0_scenario_day_facts.source_capture_id IS '本目标会话的唯一Raw捕获UUID。';
COMMENT ON COLUMN stock.source_d0_scenario_day_facts.source_raw_digest IS '本目标捕获的Raw逐股内容摘要。';
COMMENT ON COLUMN stock.source_d0_scenario_day_facts.parser_contract IS '原始来源解析合同；同一目标捕获计算使用同一解析版本。';
COMMENT ON COLUMN stock.source_d0_scenario_day_facts.source_content_id IS '本目标会话绑定的规范来源内容摘要。';
COMMENT ON COLUMN stock.source_d0_scenario_day_facts.scenario IS '当前已实现D0诊断场景：LAUNCH_CONFIRM或RECOVERY_TURN。';
COMMENT ON COLUMN stock.source_d0_scenario_day_facts.producer_version IS 'D0单日状态生产者版本。';
COMMENT ON COLUMN stock.source_d0_scenario_day_facts.algorithm_contract IS 'D0场景算法合同标识。';
COMMENT ON COLUMN stock.source_d0_scenario_day_facts.session_contract IS 'D0官方会话和历史窗口合同标识。';
COMMENT ON COLUMN stock.source_d0_scenario_day_facts.history_window_start IS '该场景读取历史窗口的首个官方会话。';
COMMENT ON COLUMN stock.source_d0_scenario_day_facts.history_session_count IS '实际请求的官方会话窗口数，单位为会话。';
COMMENT ON COLUMN stock.source_d0_scenario_day_facts.source_count IS '该目标日完整来源池分母，单位为来源股票只数。';
COMMENT ON COLUMN stock.source_d0_scenario_day_facts.scenario_state IS '当前场景三态：TRUE、FALSE或UNKNOWN。';
COMMENT ON COLUMN stock.source_d0_scenario_day_facts.scenario_reason IS '当前场景现役主原因码。';
COMMENT ON COLUMN stock.source_d0_scenario_day_facts.predicate_ids IS '按D0合同顺序排列的具名谓词标识数组，与其余谓词数组逐项对应。';
COMMENT ON COLUMN stock.source_d0_scenario_day_facts.predicate_states IS '具名谓词三态数组：TRUE、FALSE或UNKNOWN。';
COMMENT ON COLUMN stock.source_d0_scenario_day_facts.predicate_qualities IS '具名谓词质量数组：OBSERVED或UNKNOWN。';
COMMENT ON COLUMN stock.source_d0_scenario_day_facts.predicate_reason_codes IS '具名谓词原因码数组；位置与predicate_ids一一对应。';
COMMENT ON COLUMN stock.source_d0_scenario_day_facts.predicate_value_numerators IS '具名谓词精确值整数分子数组；无标量值时该位置为空。';
COMMENT ON COLUMN stock.source_d0_scenario_day_facts.predicate_value_denominators IS '具名谓词精确值正整数分母数组；无标量值时该位置为空。';
COMMENT ON COLUMN stock.source_d0_scenario_day_facts.source_time_quality IS '目标Raw来源时钟质量。';
COMMENT ON COLUMN stock.source_d0_scenario_day_facts.source_clock_role IS '目标Raw来源时钟角色。';
COMMENT ON COLUMN stock.source_d0_scenario_day_facts.source_available_at IS '目标数值实际可用时刻及其时区；未知时为空。';
COMMENT ON COLUMN stock.source_d0_scenario_day_facts.calendar_id IS '使用的正式交易日历标识。';
COMMENT ON COLUMN stock.source_d0_scenario_day_facts.calendar_revision IS '正式交易日历修订键。';
COMMENT ON COLUMN stock.source_d0_scenario_day_facts.calendar_sha256 IS '对应正式日历资源SHA-256。';
COMMENT ON COLUMN stock.source_d0_scenario_day_facts.research_mode IS '固定NON_PIT_RETROSPECTIVE后验研究语义。';
COMMENT ON COLUMN stock.source_d0_scenario_day_facts.price_coordinate IS '输入价格坐标，固定RAW_UNADJUSTED。';

COMMENT ON TABLE stock.source_pool_seed_day_facts IS '按来源股票和目标日保存现役来源池RPS、Core/Profile实际消费字段及Seed三态结果；NON_PIT描述事实。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.namespace_id IS 'PG来源数据命名空间UUID。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.target_session IS 'Seed与横截面RPS对应的官方目标会话。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.source_symbol IS 'TDX来源股票代码，不代表稳定证券身份。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.market IS '来源市场SH或SZ，必须与代码前缀一致。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.board IS '现役来源范围中的板别：MAIN_BOARD或GEM。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.package_sha256 IS '精确Raw来源包SHA-256。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.source_content_id IS '目标会话绑定的规范来源内容摘要。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.source_capture_id IS '目标会话的唯一Raw捕获UUID。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.profile_revision IS '用于该Seed输入的精确Core/Profile修订。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.producer_contract IS 'Seed与来源池RPS生产合同标识。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.producer_version IS 'Seed与来源池RPS生产者版本。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.producer_semantics_sha256 IS 'Core/Profile与Seed语义合同摘要。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.history_window_start IS '参与Seed计算的实际历史窗口首个官方会话。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.history_window_end IS '参与Seed计算的历史窗口末个官方会话，必须等于目标会话。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.history_session_count IS '请求的正式历史窗口数，单位为会话。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.calendar_id IS '使用的正式交易日历标识。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.calendar_revision IS '正式交易日历修订键。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.calendar_sha256 IS '对应正式日历资源SHA-256。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.source_time_quality IS '目标Raw来源时钟质量。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.source_clock_role IS '目标Raw来源时钟角色。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.source_available_at IS '目标数值可用时刻及其时区；未知时为空。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.research_mode IS '固定NON_PIT_RETROSPECTIVE后验研究语义。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.price_coordinate IS '输入价格坐标，固定RAW_UNADJUSTED。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_bias20_atr_numerator IS 'core.bias20_atr精确整数分子；未知时为空。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_bias20_atr_denominator IS 'core.bias20_atr精确正整数分母；未知时为空。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_bias20_atr_quality IS 'core.bias20_atr质量状态：OBSERVED或UNKNOWN。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_bias20_atr_reason IS 'core.bias20_atr未知原因码；已观察时为空。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_liquidity_mean20_numerator IS 'core.liquidity_mean20精确整数分子；未知时为空。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_liquidity_mean20_denominator IS 'core.liquidity_mean20精确正整数分母；未知时为空。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_liquidity_mean20_quality IS 'core.liquidity_mean20质量状态。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_liquidity_mean20_reason IS 'core.liquidity_mean20质量或单位未知原因。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_price_damage IS 'core.price_damage原生布尔值；UNKNOWN时为空。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_price_damage_quality IS 'core.price_damage质量状态。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_price_damage_reason IS 'core.price_damage未知原因码。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_ma20_numerator IS 'core.ma20精确整数分子；未知时为空。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_ma20_denominator IS 'core.ma20精确正整数分母；未知时为空。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_ma20_quality IS 'core.ma20质量状态。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_ma20_reason IS 'core.ma20未知原因码。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_ret1_numerator IS 'core.ret1精确整数分子；未知时为空。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_ret1_denominator IS 'core.ret1精确正整数分母；未知时为空。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_ret1_quality IS 'core.ret1质量状态。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_ret1_reason IS 'core.ret1未知原因码。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_ret5_numerator IS 'core.ret5精确整数分子；未知时为空。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_ret5_denominator IS 'core.ret5精确正整数分母；未知时为空。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_ret5_quality IS 'core.ret5质量状态。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_ret5_reason IS 'core.ret5未知原因码。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_ret20_numerator IS 'core.ret20精确整数分子；未知时为空。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_ret20_denominator IS 'core.ret20精确正整数分母；未知时为空。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_ret20_quality IS 'core.ret20质量状态。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_ret20_reason IS 'core.ret20未知原因码。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_range_ratio_numerator IS 'Profile压缩消费者输入core.range_ratio精确整数分子。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_range_ratio_denominator IS 'Profile压缩消费者输入core.range_ratio精确正整数分母。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_range_ratio_quality IS 'core.range_ratio质量状态。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_range_ratio_reason IS 'core.range_ratio未知原因码。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_atr_ratio_numerator IS 'Profile压缩消费者输入core.atr_ratio精确整数分子。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_atr_ratio_denominator IS 'Profile压缩消费者输入core.atr_ratio精确正整数分母。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_atr_ratio_quality IS 'core.atr_ratio质量状态。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_atr_ratio_reason IS 'core.atr_ratio未知原因码。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_vol_ratio_numerator IS 'Profile压缩消费者输入core.vol_ratio精确整数分子。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_vol_ratio_denominator IS 'Profile压缩消费者输入core.vol_ratio精确正整数分母。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_vol_ratio_quality IS 'core.vol_ratio质量状态。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.core_vol_ratio_reason IS 'core.vol_ratio未知原因码。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.target_actual_quote IS '目标日OHLC价格真实有效时为TRUE；否则为FALSE。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.target_close_numerator IS '目标RAW收盘价精确整数分子；未知时为空。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.target_close_denominator IS '目标RAW收盘价精确正整数分母；未知时为空。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.target_close_quality IS '目标RAW收盘价质量状态。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.target_close_reason IS '目标RAW收盘价未知原因码。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.profile_compression_value IS 'profile.compression原生枚举值；未知时为空。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.profile_compression_quality IS 'profile.compression质量状态。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.profile_compression_reason IS 'profile.compression未知原因码。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.profile_extension_value IS 'profile.extension原生枚举值；未知时为空。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.profile_extension_quality IS 'profile.extension质量状态。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.profile_extension_reason IS 'profile.extension未知原因码。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.profile_ma_structure_value IS 'profile.ma_structure原生枚举值；未知时为空。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.profile_ma_structure_quality IS 'profile.ma_structure质量状态。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.profile_ma_structure_reason IS 'profile.ma_structure未知原因码。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.rps5_numerator IS '来源池横截面RPS5精确整数分子；未知时为空。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.rps5_denominator IS '来源池横截面RPS5精确正整数分母；未知时为空。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.rps5_quality IS '来源池横截面RPS5质量状态。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.rps5_reason IS 'RPS5未知原因码。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.rps5_delta3_numerator IS 'RPS5相对三会话前百分位变化的精确整数分子。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.rps5_delta3_denominator IS 'RPS5相对三会话前变化的精确正整数分母。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.rps5_delta3_quality IS 'RPS5三会话相对变化质量状态。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.rps5_delta3_reason IS 'RPS5三会话相对变化未知原因码。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.rps20_numerator IS '来源池横截面RPS20精确整数分子；未知时为空。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.rps20_denominator IS '来源池横截面RPS20精确正整数分母；未知时为空。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.rps20_quality IS '来源池横截面RPS20质量状态。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.rps20_reason IS 'RPS20未知原因码。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.rps20_delta3_numerator IS 'RPS20相对三会话前变化的精确整数分子。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.rps20_delta3_denominator IS 'RPS20相对三会话前变化的精确正整数分母。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.rps20_delta3_quality IS 'RPS20三会话相对变化质量状态。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.rps20_delta3_reason IS 'RPS20三会话相对变化未知原因码。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.previous_ma20_cross_state IS '相邻目标会话前收盘低于或等于前MA20且目标收盘高于目标MA20的三态结果。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.previous_ma20_cross_reason IS '前一MA20交叉谓词未知或未满足原因。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.predicate_ids IS '按ALG-19/20固定顺序排列的Seed具名谓词标识。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.predicate_states IS 'Seed具名谓词三态数组。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.predicate_reasons IS 'Seed具名谓词原因数组；位置与predicate_ids一一对应。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.safety_state IS 'Seed安全门合取的三态结果。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.safety_reasons IS 'Seed安全门原因码数组。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.s1_state IS '现役来源Seed路线S1三态结果。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.s1_reasons IS 'S1路线原因码数组。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.s2_state IS '现役来源Seed路线S2三态结果。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.s2_reasons IS 'S2路线原因码数组。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.source_seed_state IS 'S1或S2来源形态结果三态。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.source_seed_reasons IS '来源Seed原因码数组。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.raw_prewatch_state IS '未实现正式Prewatch的来源侧三态输入，不代表正式候选。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.raw_prewatch_reasons IS '未实现Prewatch的来源侧原因码数组。';
