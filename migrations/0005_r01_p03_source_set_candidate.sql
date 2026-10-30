-- R01→R02 P03来源集合候选；只持久化真实来源引用及有界摘要，不签发正式source_set。
CREATE TABLE stock.source_set_candidates (
    namespace_id uuid NOT NULL,
    target_session date NOT NULL,
    package_sha256 char(64) NOT NULL CHECK (package_sha256 ~ '^[0-9a-f]{64}$'),
    source_content_id char(64) NOT NULL CHECK (source_content_id ~ '^[0-9a-f]{64}$'),
    source_capture_id uuid NOT NULL,
    source_byte_sha256 char(64) NOT NULL CHECK (source_byte_sha256 ~ '^[0-9a-f]{64}$'),
    source_dataset_id text NOT NULL CHECK (source_dataset_id = 'TDX_VIPDATA_PACKAGE'),
    source_byte_count bigint NOT NULL CHECK (source_byte_count > 0),
    parser_contract text NOT NULL,
    capture_expected_row_count integer NOT NULL CHECK (capture_expected_row_count > 0),
    raw_row_count integer NOT NULL CHECK (raw_row_count > 0),
    target_present_count integer NOT NULL CHECK (target_present_count >= 0),
    target_missing_count integer NOT NULL CHECK (target_missing_count >= 0),
    read_failed_count integer NOT NULL CHECK (read_failed_count = 0),
    raw_source_digest char(64) NOT NULL CHECK (raw_source_digest ~ '^[0-9a-f]{64}$'),
    capture_canonical_digest char(64) NOT NULL CHECK (capture_canonical_digest ~ '^[0-9a-f]{64}$'),
    capture_read_started_at timestamptz NOT NULL,
    capture_validated_at timestamptz NOT NULL CHECK (capture_validated_at >= capture_read_started_at),
    source_available_at timestamptz,
    source_time_quality text NOT NULL CHECK (source_time_quality IN ('UNKNOWN', 'NON_PIT_OBSERVED', 'VERIFIED_SOURCE_TIME')),
    source_clock_role text NOT NULL CHECK (length(source_clock_role) > 0),
    calendar_id text NOT NULL CHECK (calendar_id = 'CN_SSE_SZSE_EQUITY'),
    calendar_revision text NOT NULL CHECK (length(calendar_revision) > 0),
    calendar_sha256 char(64) NOT NULL CHECK (calendar_sha256 ~ '^[0-9a-f]{64}$'),
    candidate_sha256 char(64) NOT NULL CHECK (candidate_sha256 ~ '^[0-9a-f]{64}$'),
    candidate_status text NOT NULL CHECK (candidate_status = 'SOURCE_SET_CANDIDATE_PERSISTED_NON_PIT'),
    formal_p03_status text NOT NULL CHECK (formal_p03_status = 'PREREQUISITE_NOT_CONNECTED'),
    pit_status text NOT NULL CHECK (pit_status = 'NON_PIT_RETROSPECTIVE'),
    identity_status text NOT NULL CHECK (identity_status = 'SOURCE_SYMBOL_ONLY'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (namespace_id, target_session, package_sha256),
    FOREIGN KEY (namespace_id, source_content_id)
        REFERENCES stock.source_contents(namespace_id, content_id),
    FOREIGN KEY (namespace_id, source_capture_id)
        REFERENCES stock.source_captures(namespace_id, capture_id),
    CHECK (raw_row_count = capture_expected_row_count),
    CHECK (raw_row_count = target_present_count + target_missing_count + read_failed_count),
    CHECK (source_byte_sha256 = package_sha256),
    CHECK (source_time_quality <> 'VERIFIED_SOURCE_TIME' OR source_available_at IS NOT NULL),
    CHECK (source_time_quality <> 'UNKNOWN' OR source_available_at IS NULL)
);

COMMENT ON TABLE stock.source_set_candidates IS 'P03正式来源集合之前的有界研究候选，精确绑定TDX包、内容、捕获、Raw分母和版本化日历；不代表正式P03封口、稳定身份或PIT。';
COMMENT ON COLUMN stock.source_set_candidates.namespace_id IS 'DEV来源命名空间UUID，必须引用当前Stock A部署的唯一DEV命名空间。';
COMMENT ON COLUMN stock.source_set_candidates.target_session IS '候选目标交易会话日期，须由版本化官方交易日历确认；不允许自动改用其它日期。';
COMMENT ON COLUMN stock.source_set_candidates.package_sha256 IS '官方TDX VIPDATA ZIP原始字节SHA-256小写十六进制值，是精确来源版本键。';
COMMENT ON COLUMN stock.source_set_candidates.source_content_id IS '引用stock.source_contents中的原始内容身份摘要；不保存来源包字节。';
COMMENT ON COLUMN stock.source_set_candidates.source_capture_id IS '引用stock.source_captures中该内容的真实捕获记录UUID。';
COMMENT ON COLUMN stock.source_set_candidates.source_byte_sha256 IS 'source_contents记录的原始包字节SHA-256，必须与package_sha256相同。';
COMMENT ON COLUMN stock.source_set_candidates.source_dataset_id IS '来源数据集固定为TDX_VIPDATA_PACKAGE，标明官方通达信日线ZIP来源。';
COMMENT ON COLUMN stock.source_set_candidates.source_byte_count IS '来源ZIP原始字节数，仅记录大小，不保存ZIP内容。';
COMMENT ON COLUMN stock.source_set_candidates.parser_contract IS '创建捕获记录所用的解析合同身份；用于证明输入来源，不代替P03生产者。';
COMMENT ON COLUMN stock.source_set_candidates.capture_expected_row_count IS 'source_captures声明的规范化记录分母，单位为来源代码观察行。';
COMMENT ON COLUMN stock.source_set_candidates.raw_row_count IS '目标日PG Raw实际行数；候选只接受与捕获分母相等且状态分区闭合的版本。';
COMMENT ON COLUMN stock.source_set_candidates.target_present_count IS '状态为TARGET_PRESENT的目标日来源代码数，不等同官方上市总体。';
COMMENT ON COLUMN stock.source_set_candidates.target_missing_count IS '状态为TARGET_MISSING的目标日来源代码数；不推断停牌、退市或未上市。';
COMMENT ON COLUMN stock.source_set_candidates.read_failed_count IS '状态为READ_FAILED的目标日来源代码数；候选要求该值为0。';
COMMENT ON COLUMN stock.source_set_candidates.raw_source_digest IS '按固定字段顺序对目标日完整Raw行集计算的SHA-256，不存储OHLC或行payload。';
COMMENT ON COLUMN stock.source_set_candidates.capture_canonical_digest IS '捕获时对规范化记录计算的独立摘要SHA-256，必须与Raw行集摘要相同。';
COMMENT ON COLUMN stock.source_set_candidates.capture_read_started_at IS '本机开始读取来源的捕获时刻，含时区；不代表来源首次可得。';
COMMENT ON COLUMN stock.source_set_candidates.capture_validated_at IS '本机完成来源读取和摘要验证的捕获时刻，含时区。';
COMMENT ON COLUMN stock.source_set_candidates.source_available_at IS '来源真实可得时间；来源未提供或未验证时保持NULL，不以捕获、插入或查询时间代填。';
COMMENT ON COLUMN stock.source_set_candidates.source_time_quality IS '来源时钟质量：UNKNOWN未知、NON_PIT_OBSERVED后验观测、VERIFIED_SOURCE_TIME已验证来源时钟。';
COMMENT ON COLUMN stock.source_set_candidates.source_clock_role IS '捕获记录声明的时钟角色；必须与实际来源捕获身份一致。';
COMMENT ON COLUMN stock.source_set_candidates.calendar_id IS '确认目标日的版本化交易日历身份，固定为CN_SSE_SZSE_EQUITY。';
COMMENT ON COLUMN stock.source_set_candidates.calendar_revision IS '确认目标日的官方交易日历版本标识。';
COMMENT ON COLUMN stock.source_set_candidates.calendar_sha256 IS '交易日历资源原始字节SHA-256，用于绑定候选使用的日历版本。';
COMMENT ON COLUMN stock.source_set_candidates.candidate_sha256 IS '候选身份及全部摘要输入的稳定SHA-256；相同精确输入幂等，不同输入冲突。';
COMMENT ON COLUMN stock.source_set_candidates.candidate_status IS '候选状态固定为SOURCE_SET_CANDIDATE_PERSISTED_NON_PIT；不代表正式P03完成。';
COMMENT ON COLUMN stock.source_set_candidates.formal_p03_status IS '正式P03状态；当前固定PREREQUISITE_NOT_CONNECTED，直至required输入真实接通。';
COMMENT ON COLUMN stock.source_set_candidates.pit_status IS '时间准入状态固定为NON_PIT_RETROSPECTIVE，不得用于当时预测表现。';
COMMENT ON COLUMN stock.source_set_candidates.identity_status IS '身份状态固定为SOURCE_SYMBOL_ONLY，不代表稳定证券实体身份。';
COMMENT ON COLUMN stock.source_set_candidates.created_at IS '候选写入PG的事务时间，含时区；不是来源available_at或PIT时间。';

CREATE TABLE stock.source_set_candidate_inputs (
    namespace_id uuid NOT NULL,
    target_session date NOT NULL,
    package_sha256 char(64) NOT NULL CHECK (package_sha256 ~ '^[0-9a-f]{64}$'),
    input_ordinal smallint NOT NULL CHECK (input_ordinal BETWEEN 1 AND 7),
    input_role text NOT NULL CHECK (input_role IN ('TDX_CONTENT', 'TDX_CAPTURE', 'TDX_RAW_DAILY', 'FIELD_QUALITY', 'BAOSTOCK_CAPABILITY')),
    field_family text CHECK (field_family IS NULL OR field_family IN ('PRICE', 'AMOUNT', 'VOLUME')),
    input_status text NOT NULL CHECK (input_status IN ('VERIFIED', 'NOT_REQUESTED')),
    source_content_id char(64),
    source_capture_id uuid,
    evidence_sha256 char(64) CHECK (evidence_sha256 IS NULL OR evidence_sha256 ~ '^[0-9a-f]{64}$'),
    observed_row_count integer CHECK (observed_row_count IS NULL OR observed_row_count >= 0),
    expected_row_count integer CHECK (expected_row_count IS NULL OR expected_row_count >= 0),
    raw_observed_count integer CHECK (raw_observed_count IS NULL OR raw_observed_count >= 0),
    raw_unknown_count integer CHECK (raw_unknown_count IS NULL OR raw_unknown_count >= 0),
    raw_not_applicable_count integer CHECK (raw_not_applicable_count IS NULL OR raw_not_applicable_count >= 0),
    raw_bad_value_count integer CHECK (raw_bad_value_count IS NULL OR raw_bad_value_count >= 0),
    normalized_observed_count integer CHECK (normalized_observed_count IS NULL OR normalized_observed_count >= 0),
    normalized_unknown_count integer CHECK (normalized_unknown_count IS NULL OR normalized_unknown_count >= 0),
    normalized_not_applicable_count integer CHECK (normalized_not_applicable_count IS NULL OR normalized_not_applicable_count >= 0),
    normalized_bad_value_count integer CHECK (normalized_bad_value_count IS NULL OR normalized_bad_value_count >= 0),
    unit_status text CHECK (unit_status IS NULL OR unit_status IN ('RAW_UNADJUSTED', 'UNIT_UNVERIFIED')),
    source_available_at timestamptz,
    source_time_quality text CHECK (source_time_quality IS NULL OR source_time_quality IN ('UNKNOWN', 'NON_PIT_OBSERVED', 'VERIFIED_SOURCE_TIME')),
    source_clock_role text,
    reason_code text,
    PRIMARY KEY (namespace_id, target_session, package_sha256, input_ordinal),
    FOREIGN KEY (namespace_id, target_session, package_sha256)
        REFERENCES stock.source_set_candidates(namespace_id, target_session, package_sha256),
    FOREIGN KEY (namespace_id, source_content_id)
        REFERENCES stock.source_contents(namespace_id, content_id),
    FOREIGN KEY (namespace_id, source_capture_id)
        REFERENCES stock.source_captures(namespace_id, capture_id),
    CHECK ((input_role = 'FIELD_QUALITY') = (field_family IS NOT NULL)),
    CHECK (
        (input_role = 'BAOSTOCK_CAPABILITY' AND input_status = 'NOT_REQUESTED'
         AND source_content_id IS NULL AND source_capture_id IS NULL AND evidence_sha256 IS NULL
         AND observed_row_count IS NULL AND expected_row_count IS NULL
         AND reason_code = 'OPTIONAL_SOURCE_NOT_REQUESTED')
        OR
        (input_role <> 'BAOSTOCK_CAPABILITY' AND input_status = 'VERIFIED' AND reason_code IS NULL)
    ),
    CHECK (
        input_role <> 'FIELD_QUALITY'
        OR (observed_row_count IS NOT NULL AND expected_row_count IS NOT NULL
            AND observed_row_count = expected_row_count AND unit_status IS NOT NULL)
    ),
    CHECK (
        input_role <> 'TDX_RAW_DAILY'
        OR (evidence_sha256 IS NOT NULL AND observed_row_count IS NOT NULL
            AND expected_row_count IS NOT NULL AND observed_row_count = expected_row_count)
    ),
    CHECK (
        input_role <> 'TDX_CAPTURE'
        OR (source_content_id IS NOT NULL AND source_capture_id IS NOT NULL
            AND evidence_sha256 IS NOT NULL AND source_time_quality IS NOT NULL
            AND source_clock_role IS NOT NULL)
    ),
    CHECK (
        input_role <> 'TDX_CONTENT'
        OR (source_content_id IS NOT NULL AND source_capture_id IS NULL
            AND evidence_sha256 = package_sha256)
    )
);

COMMENT ON TABLE stock.source_set_candidate_inputs IS '来源集合候选的至多七条类型化输入引用：TDX内容、捕获、Raw摘要、三类字段质量和可选Bao请求状态；不存整对象JSON或来源数据。';
COMMENT ON COLUMN stock.source_set_candidate_inputs.namespace_id IS 'DEV来源命名空间UUID，必须与候选主行和真实来源引用相同。';
COMMENT ON COLUMN stock.source_set_candidate_inputs.target_session IS '候选目标交易会话日期，继承候选主行的精确日期。';
COMMENT ON COLUMN stock.source_set_candidate_inputs.package_sha256 IS '官方TDX来源包SHA-256，继承候选主行的精确版本。';
COMMENT ON COLUMN stock.source_set_candidate_inputs.input_ordinal IS '候选内输入顺序号，1至7；同一候选不得重复输入位置。';
COMMENT ON COLUMN stock.source_set_candidate_inputs.input_role IS '类型化输入角色：TDX内容、TDX捕获、TDX Raw、字段质量或Bao请求状态。';
COMMENT ON COLUMN stock.source_set_candidate_inputs.field_family IS '字段质量族：PRICE价格、AMOUNT金额或VOLUME数量；非字段质量输入时为空。';
COMMENT ON COLUMN stock.source_set_candidate_inputs.input_status IS '输入状态：VERIFIED已核验真实引用、NOT_REQUESTED本任务未请求该可选来源。';
COMMENT ON COLUMN stock.source_set_candidate_inputs.source_content_id IS '真实来源内容身份摘要；Bao未请求时为空，不得填占位内容ID。';
COMMENT ON COLUMN stock.source_set_candidate_inputs.source_capture_id IS '真实来源捕获UUID；未捕获/未请求时为空，不得构造虚假捕获。';
COMMENT ON COLUMN stock.source_set_candidate_inputs.evidence_sha256 IS '该输入的内容、捕获、Raw行集或字段质量证据摘要；没有真实摘要时为空。';
COMMENT ON COLUMN stock.source_set_candidate_inputs.observed_row_count IS '该输入实际观察行数；字段质量计数以来源代码行作为分母。';
COMMENT ON COLUMN stock.source_set_candidate_inputs.expected_row_count IS '该输入声明的预期行数；字段质量须覆盖完整候选Raw分母。';
COMMENT ON COLUMN stock.source_set_candidate_inputs.raw_observed_count IS '来源原始值质量为OBSERVED或RAW_SOURCE_VALUE_OBSERVED的行数。';
COMMENT ON COLUMN stock.source_set_candidate_inputs.raw_unknown_count IS '来源原始值质量为UNKNOWN的行数；未知不按零替换。';
COMMENT ON COLUMN stock.source_set_candidate_inputs.raw_not_applicable_count IS '来源原始值质量为NOT_APPLICABLE的行数。';
COMMENT ON COLUMN stock.source_set_candidate_inputs.raw_bad_value_count IS '来源原始值质量为BAD_VALUE的行数。';
COMMENT ON COLUMN stock.source_set_candidate_inputs.normalized_observed_count IS '规范字段质量为OBSERVED的来源代码行数。';
COMMENT ON COLUMN stock.source_set_candidate_inputs.normalized_unknown_count IS '规范字段质量为UNKNOWN的来源代码行数。';
COMMENT ON COLUMN stock.source_set_candidate_inputs.normalized_not_applicable_count IS '规范字段质量为NOT_APPLICABLE的来源代码行数。';
COMMENT ON COLUMN stock.source_set_candidate_inputs.normalized_bad_value_count IS '规范字段质量为BAD_VALUE的来源代码行数。';
COMMENT ON COLUMN stock.source_set_candidate_inputs.unit_status IS '字段单位/坐标状态：RAW_UNADJUSTED表示原始OHLC坐标，UNIT_UNVERIFIED表示量额单位尚未验证。';
COMMENT ON COLUMN stock.source_set_candidate_inputs.source_available_at IS '来源真实可得时间；未知或未提供时保持NULL，不由读取或写入时刻推断。';
COMMENT ON COLUMN stock.source_set_candidate_inputs.source_time_quality IS '真实来源捕获时间质量；Bao未请求等无捕获输入保持NULL。';
COMMENT ON COLUMN stock.source_set_candidate_inputs.source_clock_role IS '真实来源捕获时钟角色；未捕获时为空，不合成观察时钟。';
COMMENT ON COLUMN stock.source_set_candidate_inputs.reason_code IS '非请求输入的具名原因；当前Bao状态使用OPTIONAL_SOURCE_NOT_REQUESTED。';

ALTER TABLE stock.source_set_candidates OWNER TO stock_a;
ALTER TABLE stock.source_set_candidate_inputs OWNER TO stock_a;
