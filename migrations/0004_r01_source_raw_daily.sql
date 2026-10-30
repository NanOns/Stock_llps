-- R01→R02：保存一日的TDX VIPDATA来源代码观察，不签发稳定证券身份。
-- 仅由受控集中迁移入口在既有stock schema中执行；不保存ZIP/历史payload。
CREATE TABLE stock.source_raw_daily (
    namespace_id uuid NOT NULL,
    source_symbol text NOT NULL CHECK (source_symbol ~ '^(SH|SZ)\.[0-9]{6}$'),
    market text NOT NULL CHECK (market IN ('SH', 'SZ')),
    board text NOT NULL CHECK (board IN ('MAIN_BOARD', 'GEM')),
    session_date date NOT NULL,
    package_sha256 char(64) NOT NULL CHECK (package_sha256 ~ '^[0-9a-f]{64}$'),
    source_content_id char(64) NOT NULL CHECK (source_content_id ~ '^[0-9a-f]{64}$'),
    source_capture_id uuid NOT NULL,
    source_entry_sha256 char(64) CHECK (source_entry_sha256 IS NULL OR source_entry_sha256 ~ '^[0-9a-f]{64}$'),
    source_entry_bytes bigint NOT NULL CHECK (source_entry_bytes >= 0),
    source_record_count integer NOT NULL CHECK (source_record_count >= 0),
    bar_status text NOT NULL CHECK (bar_status IN ('TARGET_PRESENT', 'TARGET_MISSING', 'READ_FAILED')),
    bar_reason text,
    raw_open numeric,
    raw_high numeric,
    raw_low numeric,
    raw_close numeric,
    price_quality text NOT NULL CHECK (price_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE')),
    price_reason text,
    raw_amount numeric,
    raw_amount_quality text NOT NULL CHECK (raw_amount_quality IN ('RAW_SOURCE_VALUE_OBSERVED', 'UNKNOWN', 'BAD_VALUE', 'NOT_APPLICABLE')),
    raw_amount_reason text,
    amount_quality text NOT NULL CHECK (amount_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE')),
    amount_reason text,
    amount_unit_status text NOT NULL CHECK (amount_unit_status = 'UNIT_UNVERIFIED'),
    raw_volume bigint,
    raw_volume_quality text NOT NULL CHECK (raw_volume_quality IN ('RAW_SOURCE_VALUE_OBSERVED', 'UNKNOWN', 'BAD_VALUE', 'NOT_APPLICABLE')),
    raw_volume_reason text,
    volume_quality text NOT NULL CHECK (volume_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE')),
    volume_reason text,
    volume_unit_status text NOT NULL CHECK (volume_unit_status = 'UNIT_UNVERIFIED'),
    first_source_date date,
    last_source_date date,
    future_source_rows_excluded integer NOT NULL CHECK (future_source_rows_excluded >= 0),
    observed_started_at timestamptz NOT NULL,
    observed_completed_at timestamptz NOT NULL CHECK (observed_completed_at >= observed_started_at),
    source_clock_role text NOT NULL CHECK (source_clock_role = 'LOCAL_VERIFIED_VIPDATA_TARGET_SCAN_NON_PIT'),
    source_available_at timestamptz,
    PRIMARY KEY (namespace_id, source_symbol, session_date, package_sha256),
    FOREIGN KEY (namespace_id, source_content_id)
        REFERENCES stock.source_contents(namespace_id, content_id),
    FOREIGN KEY (namespace_id, source_capture_id)
        REFERENCES stock.source_captures(namespace_id, capture_id),
    CHECK ((market = 'SH' AND source_symbol LIKE 'SH.%') OR (market = 'SZ' AND source_symbol LIKE 'SZ.%')),
    CHECK (first_source_date IS NULL OR last_source_date IS NULL OR first_source_date <= last_source_date),
    CHECK (source_available_at IS NULL),
    CHECK (
        (price_quality = 'OBSERVED' AND bar_status = 'TARGET_PRESENT'
         AND raw_open IS NOT NULL AND raw_high IS NOT NULL AND raw_low IS NOT NULL AND raw_close IS NOT NULL
         AND price_reason IS NULL)
        OR
        (price_quality <> 'OBSERVED'
         AND raw_open IS NULL AND raw_high IS NULL AND raw_low IS NULL AND raw_close IS NULL
         AND price_reason IS NOT NULL)
    ),
    CHECK ((raw_amount_quality = 'RAW_SOURCE_VALUE_OBSERVED' AND raw_amount IS NOT NULL AND raw_amount_reason IS NULL)
        OR (raw_amount_quality <> 'RAW_SOURCE_VALUE_OBSERVED' AND raw_amount IS NULL AND raw_amount_reason IS NOT NULL)),
    CHECK ((raw_volume_quality = 'RAW_SOURCE_VALUE_OBSERVED' AND raw_volume IS NOT NULL AND raw_volume_reason IS NULL)
        OR (raw_volume_quality <> 'RAW_SOURCE_VALUE_OBSERVED' AND raw_volume IS NULL AND raw_volume_reason IS NOT NULL)),
    CHECK ((amount_quality = 'OBSERVED' AND amount_reason IS NULL)
        OR (amount_quality <> 'OBSERVED' AND amount_reason IS NOT NULL)),
    CHECK ((volume_quality = 'OBSERVED' AND volume_reason IS NULL)
        OR (volume_quality <> 'OBSERVED' AND volume_reason IS NOT NULL))
);

COMMENT ON TABLE stock.source_raw_daily IS '保存通达信VIPDATA指定目标日的来源代码级Raw日线观察；不代表官方证券身份、Canonical或Foundation结果。';
COMMENT ON COLUMN stock.source_raw_daily.namespace_id IS 'DEV运行命名空间UUID，外键边界来自stock.namespaces，不是证券业务身份。';
COMMENT ON COLUMN stock.source_raw_daily.source_symbol IS '来源代码，格式SH.六位代码或SZ.六位代码；仅表示ZIP中的来源符号。';
COMMENT ON COLUMN stock.source_raw_daily.market IS '来源市场枚举SH或SZ；不推导官方上市状态。';
COMMENT ON COLUMN stock.source_raw_daily.board IS '来源代码池板别：MAIN_BOARD沪深主板或GEM创业板；不是经核验的有效证券总体。';
COMMENT ON COLUMN stock.source_raw_daily.session_date IS '目标交易会话日期；按显式输入和32字节记录读取，不推断停牌、退市或上市日期。';
COMMENT ON COLUMN stock.source_raw_daily.package_sha256 IS '完整官方VIPDATA ZIP原件SHA-256小写十六进制值，用于区分来源修订。';
COMMENT ON COLUMN stock.source_raw_daily.source_content_id IS '外键引用stock.source_contents的包内容摘要记录；不表示ZIP字节存入数据库。';
COMMENT ON COLUMN stock.source_raw_daily.source_capture_id IS '外键引用stock.source_captures的一次包与目标日捕获记录。';
COMMENT ON COLUMN stock.source_raw_daily.source_entry_sha256 IS '本证券32字节日线条目展开后的原始字节SHA-256；零字节占位或读取失败时为空。';
COMMENT ON COLUMN stock.source_raw_daily.source_entry_bytes IS 'ZIP目录登记的单证券条目解压字节数，非数据库payload大小。';
COMMENT ON COLUMN stock.source_raw_daily.source_record_count IS '本次单证券条目实际扫描的32字节日线记录数量。';
COMMENT ON COLUMN stock.source_raw_daily.bar_status IS '目标日观察枚举：TARGET_PRESENT有记录、TARGET_MISSING无记录、READ_FAILED条目读取失败。';
COMMENT ON COLUMN stock.source_raw_daily.bar_reason IS '目标日状态的稳定原因码；TARGET_MISSING不表示停牌、退市或上市前。';
COMMENT ON COLUMN stock.source_raw_daily.raw_open IS '目标日原生未复权开盘价，单位为TDX价格坐标；价格质量非OBSERVED时为空。';
COMMENT ON COLUMN stock.source_raw_daily.raw_high IS '目标日原生未复权最高价，单位为TDX价格坐标；价格质量非OBSERVED时为空。';
COMMENT ON COLUMN stock.source_raw_daily.raw_low IS '目标日原生未复权最低价，单位为TDX价格坐标；价格质量非OBSERVED时为空。';
COMMENT ON COLUMN stock.source_raw_daily.raw_close IS '目标日原生未复权收盘价，单位为TDX价格坐标；价格质量非OBSERVED时为空。';
COMMENT ON COLUMN stock.source_raw_daily.price_quality IS 'OHLC组字段质量：OBSERVED已验证数值、UNKNOWN未知、NOT_APPLICABLE不适用、BAD_VALUE非法值。';
COMMENT ON COLUMN stock.source_raw_daily.price_reason IS 'OHLC组未知或非法原因码；质量为OBSERVED时为空。';
COMMENT ON COLUMN stock.source_raw_daily.raw_amount IS 'TDX记录中的float32原始成交金额精确展开值；金额单位尚未核实，不标作人民币元。';
COMMENT ON COLUMN stock.source_raw_daily.raw_amount_quality IS '金额原生值质量：RAW_SOURCE_VALUE_OBSERVED保留合法来源数值，UNKNOWN、BAD_VALUE或NOT_APPLICABLE表示原因列所述状态。';
COMMENT ON COLUMN stock.source_raw_daily.raw_amount_reason IS '金额原生值缺失或非法原因码；原始数值合法时为空。';
COMMENT ON COLUMN stock.source_raw_daily.amount_quality IS '按当前未绑定单位的金额质量：不把原始值冒充标准金额；适用UNKNOWN需保留原因。';
COMMENT ON COLUMN stock.source_raw_daily.amount_reason IS '金额标准化质量原因，例如UNIT_AUTHORITY_NOT_VERIFIED；质量为OBSERVED时为空。';
COMMENT ON COLUMN stock.source_raw_daily.amount_unit_status IS '金额单位固定为UNIT_UNVERIFIED；未核验前禁止标作CNY。';
COMMENT ON COLUMN stock.source_raw_daily.raw_volume IS 'TDX记录中的无符号原始成交量数值，存为非负整数；数量单位尚未核实，不标作股。';
COMMENT ON COLUMN stock.source_raw_daily.raw_volume_quality IS '数量原生值质量：RAW_SOURCE_VALUE_OBSERVED保留合法来源数值，UNKNOWN、BAD_VALUE或NOT_APPLICABLE表示原因列所述状态。';
COMMENT ON COLUMN stock.source_raw_daily.raw_volume_reason IS '数量原生值缺失或非法原因码；原始数值合法时为空。';
COMMENT ON COLUMN stock.source_raw_daily.volume_quality IS '按当前未绑定单位的数量质量：不把原始值冒充标准股数；适用UNKNOWN需保留原因。';
COMMENT ON COLUMN stock.source_raw_daily.volume_reason IS '数量标准化质量原因，例如UNIT_AUTHORITY_NOT_VERIFIED；质量为OBSERVED时为空。';
COMMENT ON COLUMN stock.source_raw_daily.volume_unit_status IS '数量单位固定为UNIT_UNVERIFIED；未核验前禁止标作SHARES。';
COMMENT ON COLUMN stock.source_raw_daily.first_source_date IS '源条目扫描到的首条交易日期；空值表示未能从条目读取。';
COMMENT ON COLUMN stock.source_raw_daily.last_source_date IS '源条目扫描到的末条交易日期；空值表示未能从条目读取。';
COMMENT ON COLUMN stock.source_raw_daily.future_source_rows_excluded IS '根据目标日排除的未来记录数；仅记录解析器事实，不生成历史PIT结论。';
COMMENT ON COLUMN stock.source_raw_daily.observed_started_at IS '本机本次已验证ZIP目标日扫描开始时刻，含时区；不是来源发布时间或首次可得时间。';
COMMENT ON COLUMN stock.source_raw_daily.observed_completed_at IS '本机本次已验证ZIP目标日扫描完成时刻，含时区；不是历史available_at。';
COMMENT ON COLUMN stock.source_raw_daily.source_clock_role IS '固定时钟角色LOCAL_VERIFIED_VIPDATA_TARGET_SCAN_NON_PIT，表示本机后验读取观察。';
COMMENT ON COLUMN stock.source_raw_daily.source_available_at IS '来源历史可用时刻；官方VIPDATA不能证明时必须为空，本表约束禁止回填。';
CREATE INDEX source_raw_daily_session_revision_idx
    ON stock.source_raw_daily(namespace_id, session_date, package_sha256, market, board, source_symbol);
COMMENT ON INDEX stock.source_raw_daily_session_revision_idx IS '支持按命名空间、目标日、明确包版本和市场/板别进行稳定分页查询。';
ALTER TABLE stock.source_raw_daily OWNER TO stock_a;
