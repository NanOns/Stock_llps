-- 同源离线成交参与中位数及逐原因UNKNOWN计数；不保存逐股行情副本。
CREATE TABLE stock.source_market_participation_days (
    id bigserial PRIMARY KEY,
    target_session date NOT NULL,
    source_package_id bigint NOT NULL,
    source_capture_id bigint NOT NULL,
    contract_version text NOT NULL CHECK (contract_version = 'MARKET_PARTICIPATION_R1'),
    profile_revision text NOT NULL CHECK (btrim(profile_revision) <> ''),
    producer_contract text NOT NULL CHECK (btrim(producer_contract) <> ''),
    producer_version text NOT NULL CHECK (btrim(producer_version) <> ''),
    source_dataset_id text NOT NULL CHECK (btrim(source_dataset_id) <> ''),
    source_time_quality text NOT NULL CHECK (source_time_quality = 'NON_PIT_OBSERVED'),
    source_available_at timestamptz,
    source_clock_role text NOT NULL CHECK (btrim(source_clock_role) <> ''),
    calendar_id text NOT NULL CHECK (btrim(calendar_id) <> ''),
    calendar_revision text NOT NULL CHECK (btrim(calendar_revision) <> ''),
    profile_history_calendar_id text NOT NULL CHECK (btrim(profile_history_calendar_id) <> ''),
    profile_history_calendar_revision text NOT NULL CHECK (btrim(profile_history_calendar_revision) <> ''),
    history_window_start date NOT NULL,
    history_window_end date NOT NULL,
    history_requested_session_count smallint NOT NULL CHECK (history_requested_session_count > 0),
    history_stored_session_count smallint NOT NULL CHECK (history_stored_session_count > 0),
    amount_unit_status text NOT NULL CHECK (amount_unit_status = 'UNIT_UNVERIFIED'),
    price_coordinate text NOT NULL CHECK (price_coordinate = 'RAW_UNADJUSTED'),
    pit_status text NOT NULL CHECK (pit_status = 'NON_PIT_RETROSPECTIVE'),
    source_pool_count integer NOT NULL CHECK (source_pool_count > 0),
    profile_count integer NOT NULL CHECK (profile_count > 0),
    assessable_count integer NOT NULL CHECK (assessable_count >= 0),
    profile_unknown_count integer NOT NULL CHECK (profile_unknown_count >= 0),
    profile_missing_count integer NOT NULL CHECK (profile_missing_count >= 0),
    median_numerator numeric(100, 0),
    median_denominator numeric(100, 0),
    median_value numeric,
    classification text NOT NULL CHECK (classification IN ('EXPANDING', 'NORMAL', 'THIN', 'UNKNOWN')),
    status text NOT NULL CHECK (status IN ('COMPLETE', 'PARTIAL_WITH_UNKNOWN', 'UNKNOWN_ZERO_EVALUABLE')),
    CONSTRAINT source_market_participation_business_key
        UNIQUE (target_session, source_package_id, source_capture_id, contract_version, profile_revision, producer_version),
    CONSTRAINT source_market_participation_package_fk
        FOREIGN KEY (source_package_id) REFERENCES stock.source_packages(source_package_id),
    CONSTRAINT source_market_participation_capture_fk
        FOREIGN KEY (source_capture_id) REFERENCES stock.source_captures(capture_id),
    CONSTRAINT source_market_participation_counts_check
        CHECK (
            history_window_start <= history_window_end
            AND history_window_end = target_session
            AND history_requested_session_count = history_stored_session_count
            AND profile_count + profile_missing_count = source_pool_count
            AND assessable_count + profile_unknown_count = profile_count
            AND assessable_count <= profile_count
        ),
    CONSTRAINT source_market_participation_median_check
        CHECK (
            (
                assessable_count = 0
                AND median_numerator IS NULL
                AND median_denominator IS NULL
                AND median_value IS NULL
                AND classification = 'UNKNOWN'
            )
            OR
            (
                assessable_count > 0
                AND median_numerator IS NOT NULL
                AND median_numerator >= 0
                AND median_denominator IS NOT NULL
                AND median_denominator > 0
                AND median_value IS NOT NULL
                AND median_value::text NOT IN ('NaN', 'Infinity', '-Infinity')
                AND median_value >= 0
                AND classification = CASE
                    WHEN median_numerator * 5 >= median_denominator * 6 THEN 'EXPANDING'
                    WHEN median_numerator * 5 < median_denominator * 4 THEN 'THIN'
                    ELSE 'NORMAL'
                END
            )
        ),
    CONSTRAINT source_market_participation_status_check
        CHECK (
            status = CASE
                WHEN assessable_count = 0 THEN 'UNKNOWN_ZERO_EVALUABLE'
                WHEN profile_unknown_count > 0 OR profile_missing_count > 0 THEN 'PARTIAL_WITH_UNKNOWN'
                ELSE 'COMPLETE'
            END
        )
);

CREATE TABLE stock.source_market_participation_unknown_reasons (
    participation_day_id bigint NOT NULL,
    reason_code text NOT NULL CHECK (btrim(reason_code) <> ''),
    unknown_count integer NOT NULL CHECK (unknown_count > 0),
    PRIMARY KEY (participation_day_id, reason_code),
    FOREIGN KEY (participation_day_id)
        REFERENCES stock.source_market_participation_days(id) ON DELETE CASCADE
);

COMMENT ON TABLE stock.source_market_participation_days IS '同日、同来源捕获的amr20中位数及分母摘要；仅描述来源池成交参与，不代表正式上市总体或事前预测。';
COMMENT ON COLUMN stock.source_market_participation_days.id IS '成交参与日事实的数据库内部标识。';
COMMENT ON COLUMN stock.source_market_participation_days.target_session IS '成交参与事实所对应的官方目标交易会话。';
COMMENT ON COLUMN stock.source_market_participation_days.source_package_id IS '参与画像所属的数字来源包身份；与市场宽度使用同一来源包。';
COMMENT ON COLUMN stock.source_market_participation_days.source_capture_id IS '参与画像所属的数字来源捕获身份；与市场宽度使用同一捕获。';
COMMENT ON COLUMN stock.source_market_participation_days.contract_version IS '市场成交参与中位数与分类规则版本。';
COMMENT ON COLUMN stock.source_market_participation_days.profile_revision IS '唯一适用的来源价格画像修订，含历史窗口与日历修订身份。';
COMMENT ON COLUMN stock.source_market_participation_days.producer_contract IS '来源价格画像Core/Profile生产合同标识。';
COMMENT ON COLUMN stock.source_market_participation_days.producer_version IS '来源价格画像Core/Profile生产语义版本。';
COMMENT ON COLUMN stock.source_market_participation_days.source_dataset_id IS '底层来源数据集标识。';
COMMENT ON COLUMN stock.source_market_participation_days.source_time_quality IS '输入来源时间质量；本结果限定为非PIT实测来源。';
COMMENT ON COLUMN stock.source_market_participation_days.source_available_at IS '来源数据实际可得时间；来源未提供时为空。';
COMMENT ON COLUMN stock.source_market_participation_days.source_clock_role IS '来源时钟角色，不等同于画像观察或结果写入时间。';
COMMENT ON COLUMN stock.source_market_participation_days.calendar_id IS '与市场宽度相同的目标日官方交易日历身份。';
COMMENT ON COLUMN stock.source_market_participation_days.calendar_revision IS '与市场宽度相同的目标日官方交易日历修订。';
COMMENT ON COLUMN stock.source_market_participation_days.profile_history_calendar_id IS '来源价格画像历史窗口使用的官方交易日历身份。';
COMMENT ON COLUMN stock.source_market_participation_days.profile_history_calendar_revision IS '来源价格画像历史窗口所覆盖年份的完整日历修订链。';
COMMENT ON COLUMN stock.source_market_participation_days.history_window_start IS '来源价格画像历史官方会话窗口的起始日期。';
COMMENT ON COLUMN stock.source_market_participation_days.history_window_end IS '来源价格画像历史官方会话窗口的结束日期，须等于目标日。';
COMMENT ON COLUMN stock.source_market_participation_days.history_requested_session_count IS '画像生产请求的官方会话窗口长度。';
COMMENT ON COLUMN stock.source_market_participation_days.history_stored_session_count IS '画像实际保存的官方会话窗口长度，必须与请求长度一致。';
COMMENT ON COLUMN stock.source_market_participation_days.amount_unit_status IS '原生金额单位可信状态；UNIT_UNVERIFIED表示不解释为人民币，成交比值仍为无量纲。';
COMMENT ON COLUMN stock.source_market_participation_days.price_coordinate IS '画像价格坐标；本结果使用RAW_UNADJUSTED原始未复权口径。';
COMMENT ON COLUMN stock.source_market_participation_days.pit_status IS '研究时态；NON_PIT_RETROSPECTIVE表示回看研究，不代表当时可得预测。';
COMMENT ON COLUMN stock.source_market_participation_days.source_pool_count IS '同日市场宽度精确来源代码池分母；不代表官方上市证券总体。';
COMMENT ON COLUMN stock.source_market_participation_days.profile_count IS '与该来源包和捕获匹配的已存来源价格画像数。';
COMMENT ON COLUMN stock.source_market_participation_days.assessable_count IS 'amount_ratio20质量为OBSERVED且具有合法精确分数的画像数。';
COMMENT ON COLUMN stock.source_market_participation_days.profile_unknown_count IS '已存画像中amount_ratio20为UNKNOWN的代码数。';
COMMENT ON COLUMN stock.source_market_participation_days.profile_missing_count IS '来源池中没有同版本已存画像的代码数；不填零、不当作可评价。';
COMMENT ON COLUMN stock.source_market_participation_days.median_numerator IS '可评价amount_ratio20中位数的精确非负整数分子；无可评价值时为空。';
COMMENT ON COLUMN stock.source_market_participation_days.median_denominator IS '可评价amount_ratio20中位数的精确正整数分母；无可评价值时为空。';
COMMENT ON COLUMN stock.source_market_participation_days.median_value IS '精确有理数中位数的NUMERIC近似显示值；金融比较以分子和分母为准。';
COMMENT ON COLUMN stock.source_market_participation_days.classification IS '参与分类：EXPANDING为大于等于1.2，THIN为小于0.8，其余为NORMAL；无可评价值为UNKNOWN。';
COMMENT ON COLUMN stock.source_market_participation_days.status IS '分母质量状态：COMPLETE、PARTIAL_WITH_UNKNOWN或UNKNOWN_ZERO_EVALUABLE。';

COMMENT ON TABLE stock.source_market_participation_unknown_reasons IS '市场成交参与画像UNKNOWN与缺失画像的原因代码计数；不保存逐股行情或整对象载荷。';
COMMENT ON COLUMN stock.source_market_participation_unknown_reasons.participation_day_id IS '所归属的成交参与日事实标识。';
COMMENT ON COLUMN stock.source_market_participation_unknown_reasons.reason_code IS '画像或来源池代码不可评价的明确原因；缺失画像使用PROFILE_NOT_STORED。';
COMMENT ON COLUMN stock.source_market_participation_unknown_reasons.unknown_count IS '该原因对应的正整数来源代码数。';
