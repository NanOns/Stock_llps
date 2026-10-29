-- 前向迁移源码：准备R01来源首捕、证券身份、交易日日历与Canonical typed行。
-- 仅适用于既有stock_a数据库及stock schema；本文件不创建数据库或schema，不在R01任务中执行。

CREATE DOMAIN stock.finite_numeric AS numeric
    CHECK (VALUE::text NOT IN ('NaN', 'Infinity', '-Infinity'));
COMMENT ON DOMAIN stock.finite_numeric IS '拒绝PostgreSQL numeric的NaN与无穷值，用于规范价格和成交额。';
ALTER DOMAIN stock.finite_numeric OWNER TO stock_a;

CREATE TABLE stock.source_contents (
    namespace_id uuid NOT NULL REFERENCES stock.namespaces(namespace_id),
    content_id char(64) NOT NULL CHECK (content_id ~ '^[0-9a-f]{64}$'),
    dataset_id text NOT NULL CHECK (btrim(dataset_id) <> ''),
    byte_sha char(64) NOT NULL CHECK (byte_sha ~ '^[0-9a-f]{64}$'),
    byte_count bigint NOT NULL CHECK (byte_count > 0),
    PRIMARY KEY (namespace_id, content_id),
    UNIQUE (namespace_id, dataset_id, byte_sha)
);
COMMENT ON TABLE stock.source_contents IS '来源内容的摘要与大小引用；不保存行情原始字节、ZIP或响应payload。';
COMMENT ON COLUMN stock.source_contents.namespace_id IS 'Stock A运行命名空间UUID，外键绑定固定stock.namespaces。';
COMMENT ON COLUMN stock.source_contents.content_id IS '数据集与原始内容SHA组合摘要的SHA-256小写十六进制值。';
COMMENT ON COLUMN stock.source_contents.dataset_id IS '来源数据集稳定标识，例如TDX_DAILY_BARS或BAOSTOCK_IDENTITY_REFERENCE。';
COMMENT ON COLUMN stock.source_contents.byte_sha IS '实际读取来源内容或规范化响应的SHA-256小写十六进制摘要。';
COMMENT ON COLUMN stock.source_contents.byte_count IS '原件或规范化响应表示的字节数，必须为正整数；不代表数据库保存了源字节。';
ALTER TABLE stock.source_contents OWNER TO stock_a;

CREATE TABLE stock.source_captures (
    namespace_id uuid NOT NULL,
    capture_id uuid NOT NULL,
    content_id char(64) NOT NULL,
    parser_contract text NOT NULL CHECK (btrim(parser_contract) <> ''),
    read_started_at timestamptz NOT NULL,
    validated_at timestamptz NOT NULL,
    source_available_at timestamptz,
    source_time_quality text NOT NULL CHECK (source_time_quality IN ('UNKNOWN', 'NON_PIT_OBSERVED', 'VERIFIED_SOURCE_TIME')),
    source_clock_role text NOT NULL CHECK (btrim(source_clock_role) <> ''),
    canonical_digest char(64) NOT NULL CHECK (canonical_digest ~ '^[0-9a-f]{64}$'),
    normalized_count bigint NOT NULL CHECK (normalized_count > 0),
    PRIMARY KEY (namespace_id, capture_id),
    FOREIGN KEY (namespace_id, content_id) REFERENCES stock.source_contents(namespace_id, content_id),
    CHECK (validated_at >= read_started_at),
    CHECK (source_time_quality <> 'VERIFIED_SOURCE_TIME' OR source_available_at IS NOT NULL)
);
COMMENT ON TABLE stock.source_captures IS '记录一次来源读取的内容引用、解析版本、实际观察时钟和质量；不存源包或整响应。';
COMMENT ON COLUMN stock.source_captures.namespace_id IS 'Stock A运行命名空间UUID，组成来源首捕复合主键。';
COMMENT ON COLUMN stock.source_captures.capture_id IS '本次来源读取UUID；运行ID只定位首捕记录，不进入Canonical字段语义修订。';
COMMENT ON COLUMN stock.source_captures.content_id IS '外键引用stock.source_contents中的数据集内容摘要。';
COMMENT ON COLUMN stock.source_captures.parser_contract IS '解析器版本和验证合同组成的稳定业务合同文本。';
COMMENT ON COLUMN stock.source_captures.read_started_at IS '本机开始读取来源的实际时刻，含时区；不表示来源首次可用。';
COMMENT ON COLUMN stock.source_captures.validated_at IS '本机完成读取并验证摘要的实际时刻，含时区。';
COMMENT ON COLUMN stock.source_captures.source_available_at IS '来源实际available_at；来源未提供时必须为空，禁止用本机读取时刻代填。';
COMMENT ON COLUMN stock.source_captures.source_time_quality IS '时间质量枚举：UNKNOWN未知、NON_PIT_OBSERVED后验观测、VERIFIED_SOURCE_TIME已验证来源时钟。';
COMMENT ON COLUMN stock.source_captures.source_clock_role IS '来源时钟角色，例如本机ZIP条目读取NON_PIT；明确观察时间的含义。';
COMMENT ON COLUMN stock.source_captures.canonical_digest IS '解析后规范化来源行摘要SHA-256，不是原始数据payload。';
COMMENT ON COLUMN stock.source_captures.normalized_count IS '本次捕获成功规范化的记录数，必须为正整数。';
ALTER TABLE stock.source_captures OWNER TO stock_a;

CREATE TABLE stock.entities (
    namespace_id uuid NOT NULL REFERENCES stock.namespaces(namespace_id),
    entity_id text NOT NULL CHECK (btrim(entity_id) <> ''),
    identity_revision char(64) NOT NULL CHECK (identity_revision ~ '^[0-9a-f]{64}$'),
    source_capture_id uuid NOT NULL,
    identity_state text NOT NULL CHECK (identity_state = 'OFFICIAL_STABLE'),
    PRIMARY KEY (namespace_id, entity_id, identity_revision),
    FOREIGN KEY (namespace_id, source_capture_id) REFERENCES stock.source_captures(namespace_id, capture_id)
);
COMMENT ON TABLE stock.entities IS '只保存具真实官方稳定security_id证据的实体版本；来源代码不作为实体ID。';
COMMENT ON COLUMN stock.entities.namespace_id IS 'Stock A运行命名空间UUID，组成稳定实体复合主键。';
COMMENT ON COLUMN stock.entities.entity_id IS '稳定security_id，不得直接使用SH/SZ来源证券代码。';
COMMENT ON COLUMN stock.entities.identity_revision IS '稳定身份及有效期语义摘要SHA-256小写十六进制值。';
COMMENT ON COLUMN stock.entities.source_capture_id IS '签发稳定身份所依据的来源首捕UUID。';
COMMENT ON COLUMN stock.entities.identity_state IS '实体身份状态；正式实体表仅允许OFFICIAL_STABLE。';
ALTER TABLE stock.entities OWNER TO stock_a;

CREATE TABLE stock.identity_observations (
    namespace_id uuid NOT NULL REFERENCES stock.namespaces(namespace_id),
    source_symbol text NOT NULL CHECK (source_symbol ~ '^(SH|SZ)\.[0-9]{6}$'),
    identity_revision char(64) NOT NULL CHECK (identity_revision ~ '^[0-9a-f]{64}$'),
    entity_id text,
    identity_state text NOT NULL CHECK (identity_state IN ('OFFICIAL_STABLE', 'PROVISIONAL', 'UNMAPPED')),
    valid_from date,
    valid_to date,
    source_capture_id uuid NOT NULL,
    observed_at timestamptz NOT NULL,
    published_at timestamptz,
    available_at timestamptz,
    security_name text CHECK (security_name IS NULL OR octet_length(security_name) <= 128),
    security_type smallint CHECK (security_type IS NULL OR security_type IN (1, 2, 3, 4, 5)),
    listing_status smallint CHECK (listing_status IS NULL OR listing_status IN (0, 1)),
    target_session date NOT NULL,
    target_trade_status smallint CHECK (target_trade_status IS NULL OR target_trade_status IN (0, 1)),
    PRIMARY KEY (namespace_id, source_symbol, identity_revision),
    UNIQUE (namespace_id, entity_id, identity_revision),
    FOREIGN KEY (namespace_id, entity_id, identity_revision)
        REFERENCES stock.entities(namespace_id, entity_id, identity_revision),
    FOREIGN KEY (namespace_id, source_capture_id)
        REFERENCES stock.source_captures(namespace_id, capture_id),
    CHECK ((identity_state = 'OFFICIAL_STABLE' AND entity_id IS NOT NULL)
        OR (identity_state IN ('PROVISIONAL', 'UNMAPPED') AND entity_id IS NULL)),
    CHECK (valid_from IS NULL OR valid_to IS NULL OR valid_from <= valid_to)
);
COMMENT ON TABLE stock.identity_observations IS '按来源代码、有效期和观察时钟记录身份映射；未知与暂定映射保留，后验观察不冒充PIT。';
COMMENT ON COLUMN stock.identity_observations.namespace_id IS 'Stock A运行命名空间UUID，组成身份观察复合主键。';
COMMENT ON COLUMN stock.identity_observations.source_symbol IS '来源市场代码，例如SH.600000；代码本身不是稳定实体ID。';
COMMENT ON COLUMN stock.identity_observations.identity_revision IS '身份映射、有效区间和目标日观察内容的SHA-256语义版本。';
COMMENT ON COLUMN stock.identity_observations.entity_id IS '已验证稳定security_id；PROVISIONAL或UNMAPPED时为空。';
COMMENT ON COLUMN stock.identity_observations.identity_state IS '身份质量枚举：OFFICIAL_STABLE官方稳定、PROVISIONAL暂定、UNMAPPED未映射。';
COMMENT ON COLUMN stock.identity_observations.valid_from IS '身份映射有效起始日；未知时为空，不以首次观测代替上市日期。';
COMMENT ON COLUMN stock.identity_observations.valid_to IS '身份映射有效截止日；未知或仍有效时为空。';
COMMENT ON COLUMN stock.identity_observations.source_capture_id IS '本条身份观察所依据的来源首捕UUID。';
COMMENT ON COLUMN stock.identity_observations.observed_at IS '本机实际取得身份资料的时刻，含时区；不等于published_at或available_at。';
COMMENT ON COLUMN stock.identity_observations.published_at IS '来源实际发布时间；未提供时为空。';
COMMENT ON COLUMN stock.identity_observations.available_at IS '来源实际可用时刻；未提供时为空，禁止倒填本机观察时间。';
COMMENT ON COLUMN stock.identity_observations.security_name IS '来源证券名称；最多128 UTF-8字节，不据当前名称推断目标日ST。';
COMMENT ON COLUMN stock.identity_observations.security_type IS '来源证券类型枚举：1股票、2指数、3其他、4可转债、5 ETF；未提供时为空。';
COMMENT ON COLUMN stock.identity_observations.listing_status IS '来源当前上市状态枚举：1上市、0退市；不自动视为历史PIT状态。';
COMMENT ON COLUMN stock.identity_observations.target_session IS '本条身份与状态说明所针对的交易日期。';
COMMENT ON COLUMN stock.identity_observations.target_trade_status IS '来源报告的目标日交易状态枚举：1正常交易、0停牌；缺失时为空。';
ALTER TABLE stock.identity_observations OWNER TO stock_a;

CREATE TABLE stock.calendar_sessions (
    namespace_id uuid NOT NULL REFERENCES stock.namespaces(namespace_id),
    calendar_version char(64) NOT NULL CHECK (calendar_version ~ '^[0-9a-f]{64}$'),
    session_date date NOT NULL,
    is_open boolean NOT NULL,
    source_capture_id uuid NOT NULL,
    PRIMARY KEY (namespace_id, calendar_version, session_date),
    FOREIGN KEY (namespace_id, source_capture_id) REFERENCES stock.source_captures(namespace_id, capture_id)
);
COMMENT ON TABLE stock.calendar_sessions IS 'Canonical交易日外键依赖的版本化单日开市事实。';
COMMENT ON COLUMN stock.calendar_sessions.namespace_id IS 'Stock A运行命名空间UUID，组成日历事实复合主键。';
COMMENT ON COLUMN stock.calendar_sessions.calendar_version IS '日历来源与修订组合摘要SHA-256。';
COMMENT ON COLUMN stock.calendar_sessions.session_date IS '交易所本地时区的日历日期。';
COMMENT ON COLUMN stock.calendar_sessions.is_open IS '该日是否为适用市场交易日；TRUE为开市，FALSE为休市。';
COMMENT ON COLUMN stock.calendar_sessions.source_capture_id IS '日历事实所依据的来源首捕UUID。';
ALTER TABLE stock.calendar_sessions OWNER TO stock_a;

CREATE TABLE stock.canonical_daily (
    namespace_id uuid NOT NULL,
    entity_id text NOT NULL,
    identity_revision char(64) NOT NULL CHECK (identity_revision ~ '^[0-9a-f]{64}$'),
    session_date date NOT NULL,
    calendar_version char(64) NOT NULL CHECK (calendar_version ~ '^[0-9a-f]{64}$'),
    canonical_revision char(64) NOT NULL CHECK (canonical_revision ~ '^[0-9a-f]{64}$'),
    source_capture_id uuid NOT NULL,
    unit_version text NOT NULL CHECK (btrim(unit_version) <> ''),
    open stock.finite_numeric,
    high stock.finite_numeric,
    low stock.finite_numeric,
    close stock.finite_numeric,
    amount stock.finite_numeric,
    volume bigint,
    price_quality text NOT NULL CHECK (price_quality IN ('OBSERVED', 'UNKNOWN_DATA', 'NOT_APPLICABLE')),
    price_reason text,
    price_revision char(64) NOT NULL CHECK (price_revision ~ '^[0-9a-f]{64}$'),
    amount_quality text NOT NULL CHECK (amount_quality IN ('OBSERVED', 'UNKNOWN_DATA', 'NOT_APPLICABLE')),
    amount_reason text,
    amount_revision char(64) NOT NULL CHECK (amount_revision ~ '^[0-9a-f]{64}$'),
    volume_quality text NOT NULL CHECK (volume_quality IN ('OBSERVED', 'UNKNOWN_DATA', 'NOT_APPLICABLE')),
    volume_reason text,
    volume_revision char(64) NOT NULL CHECK (volume_revision ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (namespace_id, entity_id, session_date, canonical_revision),
    FOREIGN KEY (namespace_id, entity_id, identity_revision)
        REFERENCES stock.entities(namespace_id, entity_id, identity_revision),
    FOREIGN KEY (namespace_id, calendar_version, session_date)
        REFERENCES stock.calendar_sessions(namespace_id, calendar_version, session_date),
    FOREIGN KEY (namespace_id, source_capture_id)
        REFERENCES stock.source_captures(namespace_id, capture_id),
    CHECK ((price_quality = 'OBSERVED' AND price_reason IS NULL
            AND open IS NOT NULL AND high IS NOT NULL AND low IS NOT NULL AND close IS NOT NULL
            AND low > 0 AND low <= least(open, close) AND high >= greatest(open, close))
        OR (price_quality IN ('UNKNOWN_DATA', 'NOT_APPLICABLE')
            AND open IS NULL AND high IS NULL AND low IS NULL AND close IS NULL
            AND price_reason IS NOT NULL AND octet_length(price_reason) BETWEEN 1 AND 128)),
    CHECK ((amount_quality = 'OBSERVED' AND amount IS NOT NULL AND amount >= 0 AND amount_reason IS NULL)
        OR (amount_quality IN ('UNKNOWN_DATA', 'NOT_APPLICABLE') AND amount IS NULL
            AND amount_reason IS NOT NULL AND octet_length(amount_reason) BETWEEN 1 AND 128)),
    CHECK ((volume_quality = 'OBSERVED' AND volume IS NOT NULL AND volume >= 0 AND volume_reason IS NULL)
        OR (volume_quality IN ('UNKNOWN_DATA', 'NOT_APPLICABLE') AND volume IS NULL
            AND volume_reason IS NOT NULL AND octet_length(volume_reason) BETWEEN 1 AND 128))
);
COMMENT ON TABLE stock.canonical_daily IS '每个稳定实体和交易日的Canonical typed事实；价格、金额、数量质量及语义修订独立。';
COMMENT ON COLUMN stock.canonical_daily.namespace_id IS 'Stock A运行命名空间UUID，隔离固定数据库中的业务候选。';
COMMENT ON COLUMN stock.canonical_daily.entity_id IS '已验证稳定security_id，不允许使用来源代码替代。';
COMMENT ON COLUMN stock.canonical_daily.identity_revision IS '本行采用的稳定身份有效版本SHA-256。';
COMMENT ON COLUMN stock.canonical_daily.session_date IS 'Canonical行情所属交易日期。';
COMMENT ON COLUMN stock.canonical_daily.calendar_version IS '验证目标交易日所用日历版本SHA-256。';
COMMENT ON COLUMN stock.canonical_daily.canonical_revision IS '稳定身份、日历和三组字段语义修订组合SHA-256；不含运行时capture ID。';
COMMENT ON COLUMN stock.canonical_daily.source_capture_id IS '本行实际选用的来源首捕UUID，用于来源追溯及幂等读回。';
COMMENT ON COLUMN stock.canonical_daily.unit_version IS '本行成交额与成交量解码使用的单位合同版本；不表示未经核验的单位已获权威。';
COMMENT ON COLUMN stock.canonical_daily.open IS '未复权开盘价，单位人民币元/股；仅价格质量为OBSERVED时非空。';
COMMENT ON COLUMN stock.canonical_daily.high IS '未复权最高价，单位人民币元/股；仅价格质量为OBSERVED时非空。';
COMMENT ON COLUMN stock.canonical_daily.low IS '未复权最低价，单位人民币元/股；仅价格质量为OBSERVED时非空。';
COMMENT ON COLUMN stock.canonical_daily.close IS '未复权收盘价，单位人民币元/股；仅价格质量为OBSERVED时非空。';
COMMENT ON COLUMN stock.canonical_daily.amount IS '成交金额，单位人民币元；单位未验证时必须为空且质量未知。';
COMMENT ON COLUMN stock.canonical_daily.volume IS '成交数量，单位股；单位未验证时必须为空且质量未知，存储范围为非负bigint。';
COMMENT ON COLUMN stock.canonical_daily.price_quality IS 'OHLC四字段共同质量：OBSERVED已知、UNKNOWN_DATA未知、NOT_APPLICABLE不适用。';
COMMENT ON COLUMN stock.canonical_daily.price_reason IS '价格未知或不适用原因，最多128 UTF-8字节；OBSERVED时为空。';
COMMENT ON COLUMN stock.canonical_daily.price_revision IS 'OHLC值、质量、原因、单位与实际价格坐标的独立SHA-256语义版本。';
COMMENT ON COLUMN stock.canonical_daily.amount_quality IS '成交额字段质量：OBSERVED已知、UNKNOWN_DATA未知、NOT_APPLICABLE不适用；合法零值可为已知。';
COMMENT ON COLUMN stock.canonical_daily.amount_reason IS '成交额未知或不适用原因，最多128 UTF-8字节；OBSERVED时为空。';
COMMENT ON COLUMN stock.canonical_daily.amount_revision IS '成交额值、质量、原因与单位权威语义的独立SHA-256版本。';
COMMENT ON COLUMN stock.canonical_daily.volume_quality IS '成交量字段质量：OBSERVED已知、UNKNOWN_DATA未知、NOT_APPLICABLE不适用；合法零值可为已知。';
COMMENT ON COLUMN stock.canonical_daily.volume_reason IS '成交量未知或不适用原因，最多128 UTF-8字节；OBSERVED时为空。';
COMMENT ON COLUMN stock.canonical_daily.volume_revision IS '成交量值、质量、原因与单位权威语义的独立SHA-256版本。';
ALTER TABLE stock.canonical_daily OWNER TO stock_a;

CREATE INDEX canonical_daily_entity_session_idx
    ON stock.canonical_daily (namespace_id, entity_id, session_date);
COMMENT ON INDEX stock.canonical_daily_entity_session_idx IS '支持单实体按交易日读取Canonical行的有界范围索引。';
