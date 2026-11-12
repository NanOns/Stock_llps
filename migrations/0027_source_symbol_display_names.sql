CREATE TABLE stock.source_symbol_display_names (
    source_symbol text PRIMARY KEY,
    market text NOT NULL,
    display_name text NOT NULL,
    source_file text NOT NULL,
    parser_contract text NOT NULL,
    source_authority text NOT NULL,
    source_time_quality text NOT NULL,
    source_clock_role text NOT NULL,
    source_available_at timestamp with time zone,
    captured_at timestamp with time zone NOT NULL,
    identity_status text NOT NULL,
    pit_status text NOT NULL,
    CONSTRAINT source_symbol_display_names_symbol_ck
        CHECK (source_symbol ~ '^(SH|SZ|BJ)\.[0-9]{6}$'),
    CONSTRAINT source_symbol_display_names_market_ck
        CHECK (market IN ('SH', 'SZ', 'BJ') AND market = split_part(source_symbol, '.', 1)),
    CONSTRAINT source_symbol_display_names_name_ck
        CHECK (display_name = btrim(display_name) AND char_length(display_name) BETWEEN 1 AND 40),
    CONSTRAINT source_symbol_display_names_file_ck
        CHECK (source_file IN ('shs.tnf', 'szs.tnf', 'bjs.tnf')),
    CONSTRAINT source_symbol_display_names_contract_ck
        CHECK (parser_contract = 'TDX_TNF_DISPLAY_NAME_R1'),
    CONSTRAINT source_symbol_display_names_authority_ck
        CHECK (source_authority = 'UNVERIFIED_LOCAL_TDX_INSTALLATION'),
    CONSTRAINT source_symbol_display_names_time_quality_ck
        CHECK (source_time_quality = 'NON_PIT_OBSERVED'),
    CONSTRAINT source_symbol_display_names_clock_role_ck
        CHECK (source_clock_role = 'OBSERVED_CAPTURED'),
    CONSTRAINT source_symbol_display_names_available_at_ck
        CHECK (source_available_at IS NULL),
    CONSTRAINT source_symbol_display_names_identity_ck
        CHECK (identity_status = 'SOURCE_SYMBOL_ONLY_NOT_STABLE_ENTITY'),
    CONSTRAINT source_symbol_display_names_pit_ck
        CHECK (pit_status = 'NON_PIT')
);

COMMENT ON TABLE stock.source_symbol_display_names IS '本机TDX .tnf 文件解析的当前中文显示标签，仅按来源代码映射，不证明官方证券身份、上市状态或PIT有效性。每次生产在单一事务内整体替换，只保留一份最新观察；captured_at为本机实际读取时钟，来源可得时钟未知。';
COMMENT ON COLUMN stock.source_symbol_display_names.source_symbol IS 'TDX 来源代码，市场前缀加六位代码；仅为来源键，不是稳定证券实体ID。';
COMMENT ON COLUMN stock.source_symbol_display_names.market IS '来源市场前缀，取值 SH、SZ、BJ，与source_symbol前缀一致。';
COMMENT ON COLUMN stock.source_symbol_display_names.display_name IS '本机TDX .tnf解析出的中文显示标签；仅用于页面展示，来源未独立验证。';
COMMENT ON COLUMN stock.source_symbol_display_names.source_file IS '标签所在的本机只读行情缓存文件名：shs.tnf、szs.tnf或bjs.tnf。';
COMMENT ON COLUMN stock.source_symbol_display_names.parser_contract IS '二进制名称字段解析合同版本；当前固定为TDX_TNF_DISPLAY_NAME_R1。';
COMMENT ON COLUMN stock.source_symbol_display_names.source_authority IS '来源权威状态；固定UNVERIFIED_LOCAL_TDX_INSTALLATION，表示本机安装未被独立认证。';
COMMENT ON COLUMN stock.source_symbol_display_names.source_time_quality IS '来源时间质量；固定NON_PIT_OBSERVED，不代表文件内容的实际可得时间。';
COMMENT ON COLUMN stock.source_symbol_display_names.source_clock_role IS '时钟角色；固定OBSERVED_CAPTURED，表示Codex读取本机文件的观察时钟。';
COMMENT ON COLUMN stock.source_symbol_display_names.source_available_at IS '来源内容可得时间；本机文件未提供可验证时钟，必须为空。';
COMMENT ON COLUMN stock.source_symbol_display_names.captured_at IS 'Codex读取该名称快照的实际带时区时间；仅作为观察时间，不回填历史。';
COMMENT ON COLUMN stock.source_symbol_display_names.identity_status IS '身份状态；固定SOURCE_SYMBOL_ONLY_NOT_STABLE_ENTITY，不提供security_id或实体合并证明。';
COMMENT ON COLUMN stock.source_symbol_display_names.pit_status IS 'PIT状态；固定NON_PIT，仅允许披露后的显示标签使用。';
