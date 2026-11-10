-- 现役来源固定篮市场宽度的逐日和逐股typed事实。

CREATE TABLE stock.source_market_breadth_days (
    namespace_id uuid NOT NULL,
    target_session date NOT NULL,
    package_sha256 char(64) NOT NULL,
    source_capture_id uuid NOT NULL,
    source_content_id char(64) NOT NULL,
    source_dataset_id text NOT NULL,
    source_byte_count bigint NOT NULL,
    parser_contract text NOT NULL,
    source_time_quality text NOT NULL,
    source_available_at timestamptz,
    capture_read_started_at timestamptz NOT NULL,
    capture_validated_at timestamptz NOT NULL,
    source_clock_role text NOT NULL,
    capture_digest char(64) NOT NULL,
    calendar_id text NOT NULL,
    calendar_revision text NOT NULL,
    calendar_sha256 char(64) NOT NULL,
    session_t4 date NOT NULL,
    session_t3 date NOT NULL,
    session_t2 date NOT NULL,
    session_t1 date NOT NULL,
    contract_version text NOT NULL,
    source_count integer NOT NULL CHECK (source_count > 0),
    eligible_count integer NOT NULL CHECK (eligible_count >= 0),
    target_present_count integer NOT NULL CHECK (target_present_count >= 0),
    target_missing_count integer NOT NULL CHECK (target_missing_count >= 0),
    read_failed_count integer NOT NULL CHECK (read_failed_count >= 0),
    history_row_count integer NOT NULL CHECK (history_row_count >= 0),
    PRIMARY KEY (namespace_id, target_session, package_sha256, source_capture_id, calendar_revision, calendar_sha256, contract_version),
    FOREIGN KEY (namespace_id, source_capture_id) REFERENCES stock.source_captures(namespace_id, capture_id),
    FOREIGN KEY (namespace_id, source_content_id) REFERENCES stock.source_contents(namespace_id, content_id),
    CHECK (eligible_count <= source_count AND target_present_count + target_missing_count + read_failed_count = source_count),
    CHECK (session_t4 < session_t3 AND session_t3 < session_t2 AND session_t2 < session_t1 AND session_t1 < target_session)
);

CREATE TABLE stock.source_market_breadth_symbols (
    namespace_id uuid NOT NULL,
    target_session date NOT NULL,
    package_sha256 char(64) NOT NULL,
    source_capture_id uuid NOT NULL,
    calendar_revision text NOT NULL,
    calendar_sha256 char(64) NOT NULL,
    contract_version text NOT NULL,
    source_symbol text NOT NULL,
    ret_t_numerator numeric(100,0),
    ret_t_denominator numeric(100,0),
    ret_t3_numerator numeric(100,0),
    ret_t3_denominator numeric(100,0),
    positive_t smallint,
    positive_t3 smallint,
    delta_indicator smallint,
    classification text NOT NULL CHECK (classification IN ('COMPARABLE_GAIN','COMPARABLE_LOSS','COMPARABLE_BOTH_POSITIVE','COMPARABLE_BOTH_NONPOSITIVE','UNKNOWN')),
    primary_unknown_reason text,
    unknown_reason_detail text,
    PRIMARY KEY (namespace_id, target_session, package_sha256, source_capture_id, calendar_revision, calendar_sha256, contract_version, source_symbol),
    FOREIGN KEY (namespace_id, target_session, package_sha256, source_capture_id, calendar_revision, calendar_sha256, contract_version) REFERENCES stock.source_market_breadth_days(namespace_id, target_session, package_sha256, source_capture_id, calendar_revision, calendar_sha256, contract_version),
    CHECK (source_symbol ~ '^(SH|SZ)\.[0-9]{6}$'),
    CHECK (coalesce((ret_t_numerator IS NULL AND ret_t_denominator IS NULL AND positive_t IS NULL) OR (ret_t_numerator IS NOT NULL AND ret_t_denominator > 0 AND positive_t IN (0,1) AND positive_t = CASE WHEN ret_t_numerator > 0 THEN 1 ELSE 0 END), false)),
    CHECK (coalesce((ret_t3_numerator IS NULL AND ret_t3_denominator IS NULL AND positive_t3 IS NULL) OR (ret_t3_numerator IS NOT NULL AND ret_t3_denominator > 0 AND positive_t3 IN (0,1) AND positive_t3 = CASE WHEN ret_t3_numerator > 0 THEN 1 ELSE 0 END), false)),
    CHECK (coalesce((classification = 'UNKNOWN' AND delta_indicator IS NULL AND primary_unknown_reason IS NOT NULL AND btrim(primary_unknown_reason) <> '' AND (positive_t IS NULL OR positive_t3 IS NULL)) OR (classification <> 'UNKNOWN' AND delta_indicator IN (-1,0,1) AND primary_unknown_reason IS NULL AND positive_t IS NOT NULL AND positive_t3 IS NOT NULL AND delta_indicator = positive_t - positive_t3 AND classification = CASE WHEN delta_indicator = 1 THEN 'COMPARABLE_GAIN' WHEN delta_indicator = -1 THEN 'COMPARABLE_LOSS' WHEN positive_t = 1 THEN 'COMPARABLE_BOTH_POSITIVE' ELSE 'COMPARABLE_BOTH_NONPOSITIVE' END), false))
);

COMMENT ON TABLE stock.source_market_breadth_days IS '目标日固定来源篮市场宽度事实及来源和日历上下文；NON_PIT后验研究。';

COMMENT ON COLUMN stock.source_market_breadth_days.namespace_id IS '项目命名空间。';

COMMENT ON COLUMN stock.source_market_breadth_days.target_session IS '目标官方交易会话。';

COMMENT ON COLUMN stock.source_market_breadth_days.package_sha256 IS '来源包SHA-256。';

COMMENT ON COLUMN stock.source_market_breadth_days.source_capture_id IS '目标来源捕获。';

COMMENT ON COLUMN stock.source_market_breadth_days.source_content_id IS '来源内容摘要。';

COMMENT ON COLUMN stock.source_market_breadth_days.source_dataset_id IS '来源数据集标识。';

COMMENT ON COLUMN stock.source_market_breadth_days.source_byte_count IS '来源内容字节数。';

COMMENT ON COLUMN stock.source_market_breadth_days.parser_contract IS '来源解析合同。';

COMMENT ON COLUMN stock.source_market_breadth_days.source_time_quality IS '来源时间质量。';

COMMENT ON COLUMN stock.source_market_breadth_days.source_available_at IS '来源实际可得时间，未知为空。';

COMMENT ON COLUMN stock.source_market_breadth_days.capture_read_started_at IS '来源读取开始时间。';

COMMENT ON COLUMN stock.source_market_breadth_days.capture_validated_at IS '来源核验时间。';

COMMENT ON COLUMN stock.source_market_breadth_days.source_clock_role IS '来源时间角色。';

COMMENT ON COLUMN stock.source_market_breadth_days.capture_digest IS '来源捕获行摘要。';

COMMENT ON COLUMN stock.source_market_breadth_days.calendar_id IS '官方日历标识。';

COMMENT ON COLUMN stock.source_market_breadth_days.calendar_revision IS '官方日历版本。';

COMMENT ON COLUMN stock.source_market_breadth_days.calendar_sha256 IS '官方日历资源摘要。';

COMMENT ON COLUMN stock.source_market_breadth_days.session_t4 IS '目标日前第四个官方会话。';

COMMENT ON COLUMN stock.source_market_breadth_days.session_t3 IS '目标日前第三个官方会话。';

COMMENT ON COLUMN stock.source_market_breadth_days.session_t2 IS '目标日前第二个官方会话。';

COMMENT ON COLUMN stock.source_market_breadth_days.session_t1 IS '目标日前第一个官方会话。';

COMMENT ON COLUMN stock.source_market_breadth_days.contract_version IS '市场宽度算法合同。';

COMMENT ON COLUMN stock.source_market_breadth_days.source_count IS '目标来源代码分母。';

COMMENT ON COLUMN stock.source_market_breadth_days.eligible_count IS '可比较来源代码分母。';

COMMENT ON COLUMN stock.source_market_breadth_days.target_present_count IS '目标日有价格的来源数。';

COMMENT ON COLUMN stock.source_market_breadth_days.target_missing_count IS '目标日缺价格的来源数。';

COMMENT ON COLUMN stock.source_market_breadth_days.read_failed_count IS '目标日读取失败来源数。';

COMMENT ON COLUMN stock.source_market_breadth_days.history_row_count IS '计算所读四个价格会话的Raw行数。';

COMMENT ON TABLE stock.source_market_breadth_symbols IS '固定来源篮逐股正收益状态差、质量及未知原因，供CLI和市场聚合读取。';

COMMENT ON COLUMN stock.source_market_breadth_symbols.namespace_id IS '项目命名空间。';

COMMENT ON COLUMN stock.source_market_breadth_symbols.target_session IS '目标官方交易会话。';

COMMENT ON COLUMN stock.source_market_breadth_symbols.package_sha256 IS '来源包SHA-256。';

COMMENT ON COLUMN stock.source_market_breadth_symbols.source_capture_id IS '目标来源捕获。';

COMMENT ON COLUMN stock.source_market_breadth_symbols.calendar_revision IS '官方日历版本。';

COMMENT ON COLUMN stock.source_market_breadth_symbols.calendar_sha256 IS '官方日历摘要。';

COMMENT ON COLUMN stock.source_market_breadth_symbols.contract_version IS '市场宽度算法合同。';

COMMENT ON COLUMN stock.source_market_breadth_symbols.source_symbol IS '来源股票代码。';

COMMENT ON COLUMN stock.source_market_breadth_symbols.ret_t_numerator IS '目标日收益精确分子；未知为空。';

COMMENT ON COLUMN stock.source_market_breadth_symbols.ret_t_denominator IS '目标日收益精确正分母；未知为空。';

COMMENT ON COLUMN stock.source_market_breadth_symbols.ret_t3_numerator IS '前三会话收益精确分子；未知为空。';

COMMENT ON COLUMN stock.source_market_breadth_symbols.ret_t3_denominator IS '前三会话收益精确正分母；未知为空。';

COMMENT ON COLUMN stock.source_market_breadth_symbols.positive_t IS '目标日收益是否为正：0/1，未知为空。';

COMMENT ON COLUMN stock.source_market_breadth_symbols.positive_t3 IS '前三会话收益是否为正：0/1，未知为空。';

COMMENT ON COLUMN stock.source_market_breadth_symbols.delta_indicator IS '正收益状态差：-1/0/1，未知为空。';

COMMENT ON COLUMN stock.source_market_breadth_symbols.classification IS '互斥分类；UNKNOWN不是零变化。';

COMMENT ON COLUMN stock.source_market_breadth_symbols.primary_unknown_reason IS '不可比较的具名主原因，已知时为空。';

COMMENT ON COLUMN stock.source_market_breadth_symbols.unknown_reason_detail IS '来源质量原因详情，可为空。';
