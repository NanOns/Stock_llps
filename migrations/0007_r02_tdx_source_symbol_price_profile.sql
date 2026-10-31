-- R02：为精确P03来源版本保存有界、NON_PIT来源代码级基础价格画像。
-- 不保存Raw K线副本，不生成正式证券身份、上市Universe或P05结果。
CREATE TABLE stock.source_symbol_price_profiles (
    namespace_id uuid NOT NULL,
    source_set_id uuid NOT NULL,
    source_set_revision integer NOT NULL CHECK (source_set_revision > 0),
    source_capture_id uuid NOT NULL,
    source_symbol text NOT NULL CHECK (source_symbol ~ '^(SH|SZ)\.[0-9]{6}$'),
    market text NOT NULL CHECK (market IN ('SH', 'SZ')),
    board text NOT NULL CHECK (board IN ('MAIN_BOARD', 'GEM')),
    target_session date NOT NULL,
    package_sha256 char(64) NOT NULL CHECK (package_sha256 ~ '^[0-9a-f]{64}$'),
    profile_revision char(64) NOT NULL CHECK (profile_revision ~ '^[0-9a-f]{64}$'),
    record_content_sha256 char(64) NOT NULL CHECK (record_content_sha256 ~ '^[0-9a-f]{64}$'),
    calculation_scope text NOT NULL CHECK (calculation_scope = 'SOURCE_SYMBOL_NON_PIT_PRICE_PROFILE'),
    producer_contract text NOT NULL CHECK (producer_contract = 'STOCK_A_PG_SOURCE_SYMBOL_CORE_PROFILE_PREVIEW_R1'),
    producer_version text NOT NULL,
    producer_contract_sha256 char(64) NOT NULL CHECK (producer_contract_sha256 ~ '^[0-9a-f]{64}$'),
    price_coordinate text NOT NULL CHECK (price_coordinate = 'RAW_UNADJUSTED'),
    identity_status text NOT NULL CHECK (identity_status = 'SOURCE_SYMBOL_ONLY'),
    stable_security_id_status text NOT NULL CHECK (stable_security_id_status = 'STABLE_SECURITY_ID_UNVERIFIED'),
    listing_status text NOT NULL CHECK (listing_status = 'LISTING_STATUS_UNKNOWN'),
    suspension_status text NOT NULL CHECK (suspension_status = 'SUSPENSION_STATUS_UNKNOWN'),
    pit_status text NOT NULL CHECK (pit_status = 'NON_PIT_RETROSPECTIVE'),
    source_time_quality text NOT NULL CHECK (source_time_quality IN ('UNKNOWN', 'NON_PIT_OBSERVED', 'VERIFIED_SOURCE_TIME')),
    source_available_at timestamptz,
    history_calendar_id text NOT NULL,
    history_calendar_revision text NOT NULL,
    history_window_start date NOT NULL,
    history_window_end date NOT NULL,
    history_requested_session_count smallint NOT NULL CHECK (history_requested_session_count BETWEEN 1 AND 75),
    history_stored_session_count smallint NOT NULL CHECK (history_stored_session_count BETWEEN 0 AND 75),
    history_records_complete boolean NOT NULL,
    history_records_status text NOT NULL CHECK (history_records_status IN ('RECORDS_COMPLETE', 'HISTORY_ROW_NOT_STORED')),
    history_actual_price_bar_count smallint NOT NULL CHECK (history_actual_price_bar_count BETWEEN 0 AND 75),
    target_bar_status text NOT NULL CHECK (target_bar_status IN ('TDX_TARGET_BAR_PRESENT', 'TDX_TARGET_BAR_MISSING', 'TDX_READ_FAILED')),
    target_price_quality text NOT NULL CHECK (target_price_quality IN ('OBSERVED', 'UNKNOWN', 'BAD_VALUE', 'NOT_APPLICABLE')),
    ma20_window_valid_bar_count smallint NOT NULL CHECK (ma20_window_valid_bar_count BETWEEN 0 AND 20),
    ma20_window_expected_bar_count smallint NOT NULL CHECK (ma20_window_expected_bar_count BETWEEN 1 AND 20),
    atr20_window_valid_bar_count smallint NOT NULL CHECK (atr20_window_valid_bar_count BETWEEN 0 AND 21),
    atr20_window_expected_bar_count smallint NOT NULL CHECK (atr20_window_expected_bar_count BETWEEN 1 AND 21),
    price_window_usable boolean NOT NULL,
    price_window_status text NOT NULL CHECK (price_window_status IN ('USABLE', 'TARGET_MISSING', 'READ_FAILED', 'HISTORY_ROW_NOT_STORED', 'INSUFFICIENT_VALID_BARS', 'PRICE_FIELDS_UNKNOWN')),
    price_window_reason text,
    history_window_sha256 char(64) NOT NULL CHECK (history_window_sha256 ~ '^[0-9a-f]{64}$'),
    amount_unit_status text NOT NULL CHECK (amount_unit_status = 'UNIT_UNVERIFIED'),
    volume_unit_status text NOT NULL CHECK (volume_unit_status = 'UNIT_UNVERIFIED'),

    ma5 numeric, ma5_exact_numerator numeric(100,0), ma5_exact_denominator numeric(100,0), ma5_quality text NOT NULL, ma5_reason text,
    ma10 numeric, ma10_exact_numerator numeric(100,0), ma10_exact_denominator numeric(100,0), ma10_quality text NOT NULL, ma10_reason text,
    ma20 numeric, ma20_exact_numerator numeric(100,0), ma20_exact_denominator numeric(100,0), ma20_quality text NOT NULL, ma20_reason text,
    ma60 numeric, ma60_exact_numerator numeric(100,0), ma60_exact_denominator numeric(100,0), ma60_quality text NOT NULL, ma60_reason text,
    atr20 numeric, atr20_exact_numerator numeric(100,0), atr20_exact_denominator numeric(100,0), atr20_quality text NOT NULL, atr20_reason text,
    clv numeric, clv_exact_numerator numeric(100,0), clv_exact_denominator numeric(100,0), clv_quality text NOT NULL, clv_reason text,
    prior_high20 numeric, prior_high20_exact_numerator numeric(100,0), prior_high20_exact_denominator numeric(100,0), prior_high20_quality text NOT NULL, prior_high20_reason text,
    ret20 numeric, ret20_exact_numerator numeric(100,0), ret20_exact_denominator numeric(100,0), ret20_quality text NOT NULL, ret20_reason text,
    pos60 numeric, pos60_exact_numerator numeric(100,0), pos60_exact_denominator numeric(100,0), pos60_quality text NOT NULL, pos60_reason text,
    bias20_atr numeric, bias20_atr_exact_numerator numeric(100,0), bias20_atr_exact_denominator numeric(100,0), bias20_atr_quality text NOT NULL, bias20_atr_reason text,

    profile_field_ids text[] NOT NULL CHECK (cardinality(profile_field_ids) = 11),
    profile_statuses text[] NOT NULL CHECK (cardinality(profile_statuses) = 11),
    profile_qualities text[] NOT NULL CHECK (cardinality(profile_qualities) = 11),
    profile_reasons text[] NOT NULL CHECK (cardinality(profile_reasons) = 11),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),

    PRIMARY KEY (namespace_id, source_set_id, target_session, source_symbol, profile_revision),
    UNIQUE (namespace_id, source_set_id, package_sha256, target_session, source_symbol, profile_revision),
    FOREIGN KEY (namespace_id, source_set_id)
        REFERENCES stock.source_sets(namespace_id, source_set_id),
    FOREIGN KEY (namespace_id, target_session, package_sha256, source_set_revision)
        REFERENCES stock.source_sets(namespace_id, target_session, package_sha256, revision_id),
    FOREIGN KEY (namespace_id, source_capture_id)
        REFERENCES stock.source_captures(namespace_id, capture_id),
    FOREIGN KEY (namespace_id, source_symbol, target_session, package_sha256)
        REFERENCES stock.source_raw_daily(namespace_id, source_symbol, session_date, package_sha256),
    CHECK ((market = 'SH' AND source_symbol LIKE 'SH.%') OR (market = 'SZ' AND source_symbol LIKE 'SZ.%')),
    CHECK (history_window_start <= history_window_end AND history_window_end = target_session),
    CHECK (history_stored_session_count <= history_requested_session_count),
    CHECK (history_actual_price_bar_count <= history_stored_session_count),
    CHECK (history_records_complete = (history_stored_session_count = history_requested_session_count)),
    CHECK ((history_records_complete AND history_records_status = 'RECORDS_COMPLETE') OR (NOT history_records_complete AND history_records_status = 'HISTORY_ROW_NOT_STORED')),
    CHECK ((price_window_usable AND price_window_status = 'USABLE' AND price_window_reason IS NULL) OR (NOT price_window_usable AND price_window_status <> 'USABLE' AND price_window_reason IS NOT NULL)),
    CHECK (source_available_at IS NULL OR source_time_quality = 'VERIFIED_SOURCE_TIME'),
    CHECK (ma5_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_APPLICABLE')),
    CHECK (ma10_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_APPLICABLE')),
    CHECK (ma20_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_APPLICABLE')),
    CHECK (ma60_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_APPLICABLE')),
    CHECK (atr20_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_APPLICABLE')),
    CHECK (clv_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_APPLICABLE')),
    CHECK (prior_high20_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_APPLICABLE')),
    CHECK (ret20_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_APPLICABLE')),
    CHECK (pos60_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_APPLICABLE')),
    CHECK (bias20_atr_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_APPLICABLE')),
    CHECK ((ma5_quality = 'OBSERVED' AND ma5 IS NOT NULL AND ma5_reason IS NULL AND ma5_exact_numerator IS NOT NULL AND ma5_exact_denominator > 0) OR (ma5_quality <> 'OBSERVED' AND ma5 IS NULL AND ma5_reason IS NOT NULL AND ma5_exact_numerator IS NULL AND ma5_exact_denominator IS NULL)),
    CHECK ((ma10_quality = 'OBSERVED' AND ma10 IS NOT NULL AND ma10_reason IS NULL AND ma10_exact_numerator IS NOT NULL AND ma10_exact_denominator > 0) OR (ma10_quality <> 'OBSERVED' AND ma10 IS NULL AND ma10_reason IS NOT NULL AND ma10_exact_numerator IS NULL AND ma10_exact_denominator IS NULL)),
    CHECK ((ma20_quality = 'OBSERVED' AND ma20 IS NOT NULL AND ma20_reason IS NULL AND ma20_exact_numerator IS NOT NULL AND ma20_exact_denominator > 0) OR (ma20_quality <> 'OBSERVED' AND ma20 IS NULL AND ma20_reason IS NOT NULL AND ma20_exact_numerator IS NULL AND ma20_exact_denominator IS NULL)),
    CHECK ((ma60_quality = 'OBSERVED' AND ma60 IS NOT NULL AND ma60_reason IS NULL AND ma60_exact_numerator IS NOT NULL AND ma60_exact_denominator > 0) OR (ma60_quality <> 'OBSERVED' AND ma60 IS NULL AND ma60_reason IS NOT NULL AND ma60_exact_numerator IS NULL AND ma60_exact_denominator IS NULL)),
    CHECK ((atr20_quality = 'OBSERVED' AND atr20 IS NOT NULL AND atr20_reason IS NULL AND atr20_exact_numerator IS NOT NULL AND atr20_exact_denominator > 0) OR (atr20_quality <> 'OBSERVED' AND atr20 IS NULL AND atr20_reason IS NOT NULL AND atr20_exact_numerator IS NULL AND atr20_exact_denominator IS NULL)),
    CHECK ((clv_quality = 'OBSERVED' AND clv IS NOT NULL AND clv_reason IS NULL AND clv_exact_numerator IS NOT NULL AND clv_exact_denominator > 0) OR (clv_quality <> 'OBSERVED' AND clv IS NULL AND clv_reason IS NOT NULL AND clv_exact_numerator IS NULL AND clv_exact_denominator IS NULL)),
    CHECK ((prior_high20_quality = 'OBSERVED' AND prior_high20 IS NOT NULL AND prior_high20_reason IS NULL AND prior_high20_exact_numerator IS NOT NULL AND prior_high20_exact_denominator > 0) OR (prior_high20_quality <> 'OBSERVED' AND prior_high20 IS NULL AND prior_high20_reason IS NOT NULL AND prior_high20_exact_numerator IS NULL AND prior_high20_exact_denominator IS NULL)),
    CHECK ((ret20_quality = 'OBSERVED' AND ret20 IS NOT NULL AND ret20_reason IS NULL AND ret20_exact_numerator IS NOT NULL AND ret20_exact_denominator > 0) OR (ret20_quality <> 'OBSERVED' AND ret20 IS NULL AND ret20_reason IS NOT NULL AND ret20_exact_numerator IS NULL AND ret20_exact_denominator IS NULL)),
    CHECK ((pos60_quality = 'OBSERVED' AND pos60 IS NOT NULL AND pos60_reason IS NULL AND pos60_exact_numerator IS NOT NULL AND pos60_exact_denominator > 0) OR (pos60_quality <> 'OBSERVED' AND pos60 IS NULL AND pos60_reason IS NOT NULL AND pos60_exact_numerator IS NULL AND pos60_exact_denominator IS NULL)),
    CHECK ((bias20_atr_quality = 'OBSERVED' AND bias20_atr IS NOT NULL AND bias20_atr_reason IS NULL AND bias20_atr_exact_numerator IS NOT NULL AND bias20_atr_exact_denominator > 0) OR (bias20_atr_quality <> 'OBSERVED' AND bias20_atr IS NULL AND bias20_atr_reason IS NOT NULL AND bias20_atr_exact_numerator IS NULL AND bias20_atr_exact_denominator IS NULL))
);

CREATE INDEX source_symbol_price_profiles_session_market_idx
    ON stock.source_symbol_price_profiles (target_session, market, board, source_symbol);
CREATE INDEX source_symbol_price_profiles_price_status_idx
    ON stock.source_symbol_price_profiles (target_session, price_window_status, source_symbol);

COMMENT ON TABLE stock.source_symbol_price_profiles IS '按P03精确封口版本保存TDX来源代码级NON_PIT原始坐标基础价格画像，供PG搜索和分页读取；不代表正式上市Universe、P05 Canonical或发布Head。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.namespace_id IS 'Stock A固定DEV运行命名空间UUID。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.source_set_id IS '精确引用SEALED的stock.source_sets.source_set_id；不回退候选或其它版本。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.source_set_revision IS '被引用P03封口来源修订号，与目标会话和来源包SHA共同外键绑定。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.source_capture_id IS '画像目标日Raw实际引用的来源捕获UUID。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.source_symbol IS 'TDX来源代码；仅为来源符号，不等同稳定证券身份。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.market IS '来源市场枚举SH或SZ；从同一目标日Raw行读取。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.board IS '来源板别枚举MAIN_BOARD或GEM；不代表官方上市资格。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.target_session IS '经版本化交易所日历确认的画像目标交易会话。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.package_sha256 IS '被P03来源集合引用的完整TDX原始包SHA-256。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.profile_revision IS '画像修订SHA-256，绑定scope、Core/Profile合同、窗口参数及日历修订；改变参数时产生新键。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.record_content_sha256 IS '画像所有持久字段的规范内容SHA-256；同键重跑逐行相同内容才幂等，异内容拒绝。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.calculation_scope IS '唯一允许的计算范围SOURCE_SYMBOL_NON_PIT_PRICE_PROFILE。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.producer_contract IS '实际调用的现役PG来源代码Core/Profile研究合同标识。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.producer_version IS '写入该画像的正式程序包版本。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.producer_contract_sha256 IS '本次使用的装配式day-chain合同SHA-256。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.price_coordinate IS '价格坐标RAW_UNADJUSTED；不表示复权价格。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.identity_status IS '身份状态固定SOURCE_SYMBOL_ONLY，未声称稳定实体身份。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.stable_security_id_status IS '稳定证券身份验证状态；当前固定为未验证。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.listing_status IS '正式上市状态；当前未知，不由来源代码首次出现推断。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.suspension_status IS '停复牌状态；当前未知，不由缺Bar推断。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.pit_status IS '时间资格固定NON_PIT_RETROSPECTIVE，不进入严格PIT绩效。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.source_time_quality IS 'TDX原件供应方可得时钟资格；未经证明时为UNKNOWN或NON_PIT_OBSERVED。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.source_available_at IS '来源真实可得时刻；未证实时必须为空，不以捕获或封口时间代替。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.history_calendar_id IS '确定该价格画像窗口的正式交易日历身份。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.history_calendar_revision IS '确定该价格画像窗口的正式日历修订。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.history_window_start IS '实际读取的最早官方会话日期。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.history_window_end IS '实际读取的最晚会话日期，必须等于目标会话。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.history_requested_session_count IS '按显式窗口请求的官方交易会话数，范围1至75。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.history_stored_session_count IS 'PG中实际读到的窗口Raw行数；小于请求数表示存在未持久会话。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.history_records_complete IS '是否每个请求会话均有PG Raw记录；不代表每行都有有效Bar。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.history_records_status IS 'Raw记录覆盖状态：RECORDS_COMPLETE或HISTORY_ROW_NOT_STORED。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.history_actual_price_bar_count IS '窗口内bar_status为TARGET_PRESENT且价格质量有效的Bar数。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.target_bar_status IS '目标日Bar状态；缺Bar时保留缺失状态且不伪造OHLC。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.target_price_quality IS '目标日OHLC价格质量枚举OBSERVED/UNKNOWN/BAD_VALUE/NOT_APPLICABLE。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ma20_window_valid_bar_count IS 'MA20末20个连续官方会话中有效实际Bar数量。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ma20_window_expected_bar_count IS 'MA20窗口应有的官方会话数，最多20。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.atr20_window_valid_bar_count IS 'ATR20计算依赖的末21个会话中有效Bar数量；首Bar提供前收。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.atr20_window_expected_bar_count IS 'ATR20计算所需会话数，最多21。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.price_window_usable IS 'MA20与ATR20均按现役严格ACTUAL窗口观测时为真；不代表其它Core/Profile全可用。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.price_window_status IS '基础价格窗口状态，区分可用、目标缺Bar、历史行缺失和有效Bar不足。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.price_window_reason IS '价格窗口不可用的具名原因；可用时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.history_window_sha256 IS '对精确读取窗口会话、Raw状态、来源捕获和OHLC输入的规范摘要SHA-256。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.amount_unit_status IS '成交额单位资格；当前固定UNIT_UNVERIFIED且不得推导金额画像值。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.volume_unit_status IS '成交量单位资格；当前固定UNIT_UNVERIFIED且不得推导量额画像值。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ma5 IS 'Core MA5原始价格坐标简单均值；仅OBSERVED时有值，单位沿用来源价格坐标。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ma5_exact_numerator IS 'Core MA5精确有理数的整数分子；未观测时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ma5_exact_denominator IS 'Core MA5精确有理数的正整数分母；未观测时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ma5_quality IS 'Core MA5字段质量枚举；未知时不存数值。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ma5_reason IS 'Core MA5未知原因码；OBSERVED时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ma10_exact_numerator IS 'Core MA10精确有理数的整数分子；未观测时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ma10_exact_denominator IS 'Core MA10精确有理数的正整数分母；未观测时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ma10_quality IS 'Core MA10字段质量枚举。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ma10_reason IS 'Core MA10未知原因码；OBSERVED时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ma20 IS 'Core MA20原始价格坐标末20个合法ACTUAL Bar收盘均值。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ma20_exact_numerator IS 'Core MA20精确有理数的整数分子；未观测时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ma20_exact_denominator IS 'Core MA20精确有理数的正整数分母；未观测时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ma20_quality IS 'Core MA20字段质量枚举。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ma20_reason IS 'Core MA20未知原因码；OBSERVED时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ma60 IS 'Core MA60原始价格坐标末60个合法ACTUAL Bar收盘均值。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ma60_exact_numerator IS 'Core MA60精确有理数的整数分子；未观测时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ma60_exact_denominator IS 'Core MA60精确有理数的正整数分母；未观测时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ma60_quality IS 'Core MA60字段质量枚举。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ma60_reason IS 'Core MA60未知原因码；OBSERVED时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.atr20 IS 'Core ATR20原始价格坐标20个真实True Range算术均值。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.atr20_exact_numerator IS 'Core ATR20精确有理数的整数分子；未观测时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.atr20_exact_denominator IS 'Core ATR20精确有理数的正整数分母；未观测时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.atr20_quality IS 'Core ATR20字段质量枚举。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.atr20_reason IS 'Core ATR20未知原因码；OBSERVED时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.clv IS 'Core CLV=(收盘-最低)/(最高-最低)，零价格区间为UNKNOWN。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.clv_exact_numerator IS 'Core CLV精确有理数的整数分子；未观测时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.clv_exact_denominator IS 'Core CLV精确有理数的正整数分母；未观测时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.clv_quality IS 'Core CLV字段质量枚举。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.clv_reason IS 'Core CLV未知原因码；OBSERVED时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.prior_high20 IS 'Core prior_high20前20个ACTUAL Bar最高价，不含目标日。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.prior_high20_exact_numerator IS 'Core prior_high20精确有理数的整数分子；未观测时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.prior_high20_exact_denominator IS 'Core prior_high20精确有理数的正整数分母；未观测时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.prior_high20_quality IS 'Core prior_high20字段质量枚举。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.prior_high20_reason IS 'Core prior_high20未知原因码；OBSERVED时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ret20 IS 'Core ret20按现役严格窗口计算的20会话价格收益率。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ret20_exact_numerator IS 'Core ret20精确有理数的整数分子；未观测时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ret20_exact_denominator IS 'Core ret20精确有理数的正整数分母；未观测时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ret20_quality IS 'Core ret20字段质量枚举。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ret20_reason IS 'Core ret20未知原因码；OBSERVED时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.pos60 IS 'Core pos60目标价格在60会话高低区间中的相对位置。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.pos60_exact_numerator IS 'Core pos60精确有理数的整数分子；未观测时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.pos60_exact_denominator IS 'Core pos60精确有理数的正整数分母；未观测时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.pos60_quality IS 'Core pos60字段质量枚举。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.pos60_reason IS 'Core pos60未知原因码；OBSERVED时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.bias20_atr IS 'Core bias20_atr按现役合同计算的MA20相对ATR20偏离。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.bias20_atr_exact_numerator IS 'Core bias20_atr精确有理数的整数分子；未观测时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.bias20_atr_exact_denominator IS 'Core bias20_atr精确有理数的正整数分母；未观测时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.bias20_atr_quality IS 'Core bias20_atr字段质量枚举。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.bias20_atr_reason IS 'Core bias20_atr未知原因码；OBSERVED时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.profile_field_ids IS '与Profile状态数组逐位对应的11个现役字段标识，按field_id升序。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.profile_statuses IS '与profile_field_ids对齐的11项Profile评估状态；不保存整套响应JSON。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.profile_qualities IS '与profile_field_ids对齐的11项Profile字段质量。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.profile_reasons IS '与profile_field_ids对齐的11项Profile未观测原因；无原因位置为空字符串。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.created_at IS '首次插入该画像的数据库时钟；不是来源可得时间。';
