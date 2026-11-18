-- R08/业务09：将已规范化的BaoStock未复权来源观察按捕获与单位合同版本持久化。
-- 本迁移只创建typed来源表，不采集网络来源、不改写TDX Raw或现有派生事实。
CREATE TABLE stock.source_baostock_unit_contracts (
    unit_contract_version text PRIMARY KEY
        CHECK (length(unit_contract_version) BETWEEN 1 AND 128 AND btrim(unit_contract_version) <> ''),
    close_input_unit text NOT NULL
        CHECK (length(close_input_unit) BETWEEN 1 AND 64 AND btrim(close_input_unit) <> ''),
    close_to_cny stock.finite_numeric NOT NULL CHECK (close_to_cny > 0),
    amount_to_cny stock.finite_numeric NOT NULL CHECK (amount_to_cny > 0),
    volume_to_shares stock.finite_numeric NOT NULL CHECK (volume_to_shares > 0),
    turn_input_unit text NOT NULL CHECK (turn_input_unit = 'PERCENT')
);

COMMENT ON TABLE stock.source_baostock_unit_contracts IS '保存BaoStock规范值转换使用的显式单位合同版本；不代表第三方单位已独立权威验证。';
COMMENT ON COLUMN stock.source_baostock_unit_contracts.unit_contract_version IS '日线规范器声明的单位合同版本；合同内容不可变，新单位口径使用新版本。';
COMMENT ON COLUMN stock.source_baostock_unit_contracts.close_input_unit IS 'BaoStock价格输入单位原文，由调用方合同显式给出。';
COMMENT ON COLUMN stock.source_baostock_unit_contracts.close_to_cny IS '价格输入值转为人民币元/股的精确倍率，必须为正有限数。';
COMMENT ON COLUMN stock.source_baostock_unit_contracts.amount_to_cny IS '成交金额输入值转为人民币元的精确倍率，必须为正有限数。';
COMMENT ON COLUMN stock.source_baostock_unit_contracts.volume_to_shares IS '成交量输入值转为股的精确倍率，必须为正有限数。';
COMMENT ON COLUMN stock.source_baostock_unit_contracts.turn_input_unit IS '换手率输入单位；当前规范器只接受PERCENT并转换为比例值。';

CREATE TABLE stock.source_baostock_daily (
    capture_id uuid NOT NULL,
    source_code text NOT NULL CHECK (source_code ~* '^(sh|sz)\.[0-9]{6}$'),
    source_symbol text NOT NULL CHECK (source_symbol ~ '^(SH|SZ)\.[0-9]{6}$'),
    session_date date NOT NULL,
    captured_at timestamptz NOT NULL,
    capture_clock_role text NOT NULL
        CHECK (capture_clock_role = 'CAPTURED_AT_THIS_QUERY_NOT_HISTORICAL_FIRST_AVAILABLE'),
    adjustment_flag text NOT NULL CHECK (adjustment_flag = '3'),
    price_coordinate text NOT NULL CHECK (price_coordinate = 'RAW_UNADJUSTED'),
    unit_contract_version text NOT NULL
        REFERENCES stock.source_baostock_unit_contracts(unit_contract_version) ON DELETE RESTRICT,
    returned_fields text[] NOT NULL CHECK (cardinality(returned_fields) > 0),
    open_value stock.finite_numeric,
    open_quality text NOT NULL CHECK (open_quality IN ('OBSERVED', 'UNKNOWN_DATA', 'NOT_APPLICABLE')),
    open_reason text,
    high_value stock.finite_numeric,
    high_quality text NOT NULL CHECK (high_quality IN ('OBSERVED', 'UNKNOWN_DATA', 'NOT_APPLICABLE')),
    high_reason text,
    low_value stock.finite_numeric,
    low_quality text NOT NULL CHECK (low_quality IN ('OBSERVED', 'UNKNOWN_DATA', 'NOT_APPLICABLE')),
    low_reason text,
    close_value stock.finite_numeric,
    close_quality text NOT NULL CHECK (close_quality IN ('OBSERVED', 'UNKNOWN_DATA', 'NOT_APPLICABLE')),
    close_reason text,
    volume_value stock.finite_numeric,
    volume_quality text NOT NULL CHECK (volume_quality IN ('OBSERVED', 'UNKNOWN_DATA', 'NOT_APPLICABLE')),
    volume_reason text,
    amount_value stock.finite_numeric,
    amount_quality text NOT NULL CHECK (amount_quality IN ('OBSERVED', 'UNKNOWN_DATA', 'NOT_APPLICABLE')),
    amount_reason text,
    turn_value stock.finite_numeric,
    turn_quality text NOT NULL CHECK (turn_quality IN ('OBSERVED', 'UNKNOWN_DATA', 'NOT_APPLICABLE')),
    turn_reason text,
    tradestatus_value text,
    tradestatus_quality text NOT NULL CHECK (tradestatus_quality IN ('OBSERVED', 'UNKNOWN_DATA', 'NOT_APPLICABLE')),
    tradestatus_reason text,
    is_st_value text,
    is_st_quality text NOT NULL CHECK (is_st_quality IN ('OBSERVED', 'UNKNOWN_DATA', 'NOT_APPLICABLE')),
    is_st_reason text,
    normalized_sha256 char(64) NOT NULL CHECK (normalized_sha256 ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (capture_id, source_code, session_date),
    CHECK (upper(source_code) = source_symbol),
    CHECK (
        (open_quality = 'OBSERVED' AND open_value IS NOT NULL AND open_reason IS NULL AND open_value > 0)
        OR (open_quality <> 'OBSERVED' AND open_value IS NULL AND open_reason IS NOT NULL AND btrim(open_reason) <> '')
    ),
    CHECK (
        (high_quality = 'OBSERVED' AND high_value IS NOT NULL AND high_reason IS NULL AND high_value > 0)
        OR (high_quality <> 'OBSERVED' AND high_value IS NULL AND high_reason IS NOT NULL AND btrim(high_reason) <> '')
    ),
    CHECK (
        (low_quality = 'OBSERVED' AND low_value IS NOT NULL AND low_reason IS NULL AND low_value > 0)
        OR (low_quality <> 'OBSERVED' AND low_value IS NULL AND low_reason IS NOT NULL AND btrim(low_reason) <> '')
    ),
    CHECK (
        (close_quality = 'OBSERVED' AND close_value IS NOT NULL AND close_reason IS NULL AND close_value > 0)
        OR (close_quality <> 'OBSERVED' AND close_value IS NULL AND close_reason IS NOT NULL AND btrim(close_reason) <> '')
    ),
    CHECK (
        (volume_quality = 'OBSERVED' AND volume_value IS NOT NULL AND volume_reason IS NULL AND volume_value >= 0 AND volume_value = trunc(volume_value))
        OR (volume_quality <> 'OBSERVED' AND volume_value IS NULL AND volume_reason IS NOT NULL AND btrim(volume_reason) <> '')
    ),
    CHECK (
        (amount_quality = 'OBSERVED' AND amount_value IS NOT NULL AND amount_reason IS NULL AND amount_value >= 0)
        OR (amount_quality <> 'OBSERVED' AND amount_value IS NULL AND amount_reason IS NOT NULL AND btrim(amount_reason) <> '')
    ),
    CHECK (
        (turn_quality = 'OBSERVED' AND turn_value IS NOT NULL AND turn_reason IS NULL AND turn_value >= 0)
        OR (turn_quality <> 'OBSERVED' AND turn_value IS NULL AND turn_reason IS NOT NULL AND btrim(turn_reason) <> '')
    ),
    CHECK (
        (tradestatus_quality = 'OBSERVED' AND tradestatus_value IN ('0', '1') AND tradestatus_reason IS NULL)
        OR (tradestatus_quality <> 'OBSERVED' AND tradestatus_value IS NULL AND tradestatus_reason IS NOT NULL AND btrim(tradestatus_reason) <> '')
    ),
    CHECK (
        (is_st_quality = 'OBSERVED' AND is_st_value IN ('0', '1') AND is_st_reason IS NULL)
        OR (is_st_quality <> 'OBSERVED' AND is_st_value IS NULL AND is_st_reason IS NOT NULL AND btrim(is_st_reason) <> '')
    )
);

CREATE INDEX source_baostock_daily_symbol_session_ix
    ON stock.source_baostock_daily (source_symbol, session_date DESC, captured_at DESC);
COMMENT ON INDEX stock.source_baostock_daily_symbol_session_ix IS '支持按显式BaoStock捕获版本、证券代码和目标会话读取补充来源事实。';

COMMENT ON TABLE stock.source_baostock_daily IS '保存按BaoStock捕获UUID和单位合同版本隔离的未复权日线字段质量事实；不覆盖TDX Raw。';
COMMENT ON COLUMN stock.source_baostock_daily.capture_id IS 'BaoStock一次实际查询捕获的UUID；重试须复用原捕获身份，新的查询使用新UUID。';
COMMENT ON COLUMN stock.source_baostock_daily.source_code IS 'BaoStock原始沪深证券代码，格式sh.六位代码或sz.六位代码。';
COMMENT ON COLUMN stock.source_baostock_daily.source_symbol IS '按既有格式规范化的来源证券代码；不代表稳定security_id或官方同实体证明。';
COMMENT ON COLUMN stock.source_baostock_daily.session_date IS 'BaoStock响应中的交易日期；读取和写入均须受调用方显式截止日约束。';
COMMENT ON COLUMN stock.source_baostock_daily.captured_at IS '本机读取该条来源响应的捕获时刻，含时区；不代表历史available_at或published_at。';
COMMENT ON COLUMN stock.source_baostock_daily.capture_clock_role IS '固定时钟角色，说明该字段仅为本机查询捕获时间，不构成历史PIT。';
COMMENT ON COLUMN stock.source_baostock_daily.adjustment_flag IS 'BaoStock复权标记；本表仅接受3，即不复权日线。';
COMMENT ON COLUMN stock.source_baostock_daily.price_coordinate IS '价格坐标；固定RAW_UNADJUSTED，不能替代TDX主价格源或复权事实。';
COMMENT ON COLUMN stock.source_baostock_daily.unit_contract_version IS '外键绑定本行金额、价格、数量和换手率转换倍率及输入单位。';
COMMENT ON COLUMN stock.source_baostock_daily.returned_fields IS '来源响应实际返回的字段名清单；不保存原始响应对象或整包payload。';
COMMENT ON COLUMN stock.source_baostock_daily.open_value IS 'BaoStock规范化开盘价；质量非OBSERVED时为空，单位由合同版本确定。';
COMMENT ON COLUMN stock.source_baostock_daily.open_quality IS '开盘价字段质量：OBSERVED、UNKNOWN_DATA或NOT_APPLICABLE。';
COMMENT ON COLUMN stock.source_baostock_daily.open_reason IS '开盘价未知或不适用原因；OBSERVED时为空。';
COMMENT ON COLUMN stock.source_baostock_daily.high_value IS 'BaoStock规范化最高价；质量非OBSERVED时为空，单位由合同版本确定。';
COMMENT ON COLUMN stock.source_baostock_daily.high_quality IS '最高价字段质量：OBSERVED、UNKNOWN_DATA或NOT_APPLICABLE。';
COMMENT ON COLUMN stock.source_baostock_daily.high_reason IS '最高价未知或不适用原因；OBSERVED时为空。';
COMMENT ON COLUMN stock.source_baostock_daily.low_value IS 'BaoStock规范化最低价；质量非OBSERVED时为空，单位由合同版本确定。';
COMMENT ON COLUMN stock.source_baostock_daily.low_quality IS '最低价字段质量：OBSERVED、UNKNOWN_DATA或NOT_APPLICABLE。';
COMMENT ON COLUMN stock.source_baostock_daily.low_reason IS '最低价未知或不适用原因；OBSERVED时为空。';
COMMENT ON COLUMN stock.source_baostock_daily.close_value IS 'BaoStock规范化收盘价；质量非OBSERVED时为空，单位由合同版本确定。';
COMMENT ON COLUMN stock.source_baostock_daily.close_quality IS '收盘价字段质量：OBSERVED、UNKNOWN_DATA或NOT_APPLICABLE。';
COMMENT ON COLUMN stock.source_baostock_daily.close_reason IS '收盘价未知或不适用原因；OBSERVED时为空。';
COMMENT ON COLUMN stock.source_baostock_daily.volume_value IS 'BaoStock规范化成交数量，按合同转换为整数股；质量非OBSERVED时为空。';
COMMENT ON COLUMN stock.source_baostock_daily.volume_quality IS '成交数量字段质量：OBSERVED、UNKNOWN_DATA或NOT_APPLICABLE。';
COMMENT ON COLUMN stock.source_baostock_daily.volume_reason IS '成交数量未知或不适用原因；OBSERVED时为空。';
COMMENT ON COLUMN stock.source_baostock_daily.amount_value IS 'BaoStock规范化成交金额，按合同转换为人民币元；质量非OBSERVED时为空。';
COMMENT ON COLUMN stock.source_baostock_daily.amount_quality IS '成交金额字段质量：OBSERVED、UNKNOWN_DATA或NOT_APPLICABLE。';
COMMENT ON COLUMN stock.source_baostock_daily.amount_reason IS '成交金额未知或不适用原因；OBSERVED时为空。';
COMMENT ON COLUMN stock.source_baostock_daily.turn_value IS 'BaoStock换手率比例值，由百分数输入除以100；质量非OBSERVED时为空。';
COMMENT ON COLUMN stock.source_baostock_daily.turn_quality IS '换手率字段质量：OBSERVED、UNKNOWN_DATA或NOT_APPLICABLE。';
COMMENT ON COLUMN stock.source_baostock_daily.turn_reason IS '换手率未知或不适用原因；OBSERVED时为空。';
COMMENT ON COLUMN stock.source_baostock_daily.tradestatus_value IS 'BaoStock交易状态原值，仅保留0或1；不是官方停复牌身份事实。';
COMMENT ON COLUMN stock.source_baostock_daily.tradestatus_quality IS '交易状态字段质量；非法原值按UNKNOWN_DATA保留原因。';
COMMENT ON COLUMN stock.source_baostock_daily.tradestatus_reason IS '交易状态未知或不适用原因；OBSERVED时为空。';
COMMENT ON COLUMN stock.source_baostock_daily.is_st_value IS 'BaoStock ST字段原值，仅保留0或1；不是项目的官方ST身份证明。';
COMMENT ON COLUMN stock.source_baostock_daily.is_st_quality IS 'ST字段质量；非法原值按UNKNOWN_DATA保留原因。';
COMMENT ON COLUMN stock.source_baostock_daily.is_st_reason IS 'ST字段未知或不适用原因；OBSERVED时为空。';
COMMENT ON COLUMN stock.source_baostock_daily.normalized_sha256 IS '字段、质量、合同与捕获时钟规范表示的SHA-256，用于同捕获重试冲突检查。';

ALTER TABLE stock.source_baostock_unit_contracts OWNER TO stock_a;
ALTER TABLE stock.source_baostock_daily OWNER TO stock_a;
