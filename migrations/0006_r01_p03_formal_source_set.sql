-- R01 P03正式来源集合：尝试行、P02能力和提交确认均为有界typed记录。
CREATE TABLE stock.source_sets (
    namespace_id uuid NOT NULL,
    ingress_attempt_id uuid NOT NULL,
    target_session date NOT NULL,
    package_sha256 char(64) NOT NULL CHECK (package_sha256 ~ '^[0-9a-f]{64}$'),
    source_set_id uuid,
    revision_id integer NOT NULL CHECK (revision_id >= 1),
    source_content_id char(64) NOT NULL CHECK (source_content_id ~ '^[0-9a-f]{64}$'),
    source_capture_id uuid NOT NULL,
    source_digest char(64) NOT NULL CHECK (source_digest ~ '^[0-9a-f]{64}$'),
    source_raw_digest char(64) NOT NULL CHECK (source_raw_digest ~ '^[0-9a-f]{64}$'),
    source_dataset_id text NOT NULL CHECK (source_dataset_id = 'TDX_VIPDATA_PACKAGE'),
    source_byte_count bigint NOT NULL CHECK (source_byte_count > 0),
    parser_contract text NOT NULL CHECK (length(parser_contract) > 0),
    capture_expected_row_count integer NOT NULL CHECK (capture_expected_row_count > 0),
    raw_row_count integer NOT NULL CHECK (raw_row_count > 0),
    target_present_count integer NOT NULL CHECK (target_present_count >= 0),
    target_missing_count integer NOT NULL CHECK (target_missing_count >= 0),
    read_failed_count integer NOT NULL CHECK (read_failed_count = 0),
    capture_read_started_at timestamptz NOT NULL,
    capture_validated_at timestamptz NOT NULL CHECK (capture_validated_at >= capture_read_started_at),
    source_available_at timestamptz,
    source_time_quality text NOT NULL CHECK (source_time_quality IN ('UNKNOWN', 'NON_PIT_OBSERVED', 'VERIFIED_SOURCE_TIME')),
    source_clock_role text NOT NULL CHECK (length(source_clock_role) > 0),
    price_unit_status text NOT NULL CHECK (price_unit_status IN ('RAW_UNADJUSTED', 'UNIT_UNVERIFIED')),
    amount_unit_status text NOT NULL CHECK (amount_unit_status IN ('RAW_UNADJUSTED', 'UNIT_UNVERIFIED')),
    volume_unit_status text NOT NULL CHECK (volume_unit_status IN ('RAW_UNADJUSTED', 'UNIT_UNVERIFIED')),
    calendar_id text NOT NULL CHECK (length(calendar_id) > 0),
    calendar_revision text NOT NULL CHECK (length(calendar_revision) > 0),
    calendar_sha256 char(64) NOT NULL CHECK (calendar_sha256 ~ '^[0-9a-f]{64}$'),
    scope_id text NOT NULL CHECK (scope_id = 'SOURCE_INGRESS'),
    scope_contract_version text NOT NULL CHECK (length(scope_contract_version) > 0),
    scope_contract_digest char(64) NOT NULL CHECK (scope_contract_digest ~ '^[0-9a-f]{64}$'),
    scope_catalog_digest char(64) NOT NULL CHECK (scope_catalog_digest ~ '^[0-9a-f]{64}$'),
    source_semantic_revision text NOT NULL CHECK (length(source_semantic_revision) > 0),
    business_input_digest char(64) NOT NULL CHECK (business_input_digest ~ '^[0-9a-f]{64}$'),
    plan_sha256 char(64) NOT NULL CHECK (plan_sha256 ~ '^[0-9a-f]{64}$'),
    contract_sha256 char(64) NOT NULL CHECK (contract_sha256 ~ '^[0-9a-f]{64}$'),
    head_generation bigint NOT NULL CHECK (head_generation >= 0),
    durability_status text NOT NULL CHECK (durability_status IN ('PENDING', 'VERIFIED_POST_COMMIT_READBACK')),
    commit_ack_observed_at timestamptz,
    durable_at timestamptz,
    durability_source_capture_id uuid,
    durability_source_digest char(64) CHECK (durability_source_digest IS NULL OR durability_source_digest ~ '^[0-9a-f]{64}$'),
    seal_digest_sha256 char(64) CHECK (seal_digest_sha256 IS NULL OR seal_digest_sha256 ~ '^[0-9a-f]{64}$'),
    seal_status text NOT NULL CHECK (seal_status IN ('PENDING', 'SEALED')),
    seal_writer_transaction_id text CHECK (seal_writer_transaction_id IS NULL OR seal_writer_transaction_id ~ '^[1-9][0-9]*$'),
    sealed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (namespace_id, ingress_attempt_id),
    UNIQUE (namespace_id, target_session, package_sha256, revision_id),
    UNIQUE (namespace_id, source_set_id),
    FOREIGN KEY (namespace_id, target_session, package_sha256)
        REFERENCES stock.source_set_candidates(namespace_id, target_session, package_sha256),
    FOREIGN KEY (namespace_id, source_content_id)
        REFERENCES stock.source_contents(namespace_id, content_id),
    FOREIGN KEY (namespace_id, source_capture_id)
        REFERENCES stock.source_captures(namespace_id, capture_id),
    CHECK (raw_row_count = capture_expected_row_count),
    CHECK (raw_row_count = target_present_count + target_missing_count + read_failed_count),
    CHECK (source_available_at IS NULL OR source_time_quality = 'VERIFIED_SOURCE_TIME'),
    CHECK (source_time_quality <> 'VERIFIED_SOURCE_TIME' OR source_available_at IS NOT NULL),
    CHECK (
        (seal_status = 'PENDING' AND source_set_id IS NULL AND durability_status = 'PENDING'
         AND commit_ack_observed_at IS NULL AND durable_at IS NULL
         AND durability_source_capture_id IS NULL AND durability_source_digest IS NULL
         AND seal_digest_sha256 IS NULL AND seal_writer_transaction_id IS NULL AND sealed_at IS NULL)
        OR
        (seal_status = 'SEALED' AND source_set_id IS NOT NULL
         AND durability_status = 'VERIFIED_POST_COMMIT_READBACK'
         AND commit_ack_observed_at IS NOT NULL AND durable_at IS NOT NULL
         AND durability_source_capture_id = source_capture_id
         AND durability_source_digest = source_digest
         AND seal_digest_sha256 IS NOT NULL AND seal_writer_transaction_id IS NOT NULL
         AND sealed_at IS NOT NULL AND sealed_at >= durable_at
         AND commit_ack_observed_at <= durable_at)
    )
);

CREATE INDEX source_sets_accepted_session_idx
    ON stock.source_sets (namespace_id, target_session DESC, revision_id DESC)
    WHERE seal_status = 'SEALED';

COMMENT ON TABLE stock.source_sets IS 'P03正式来源集合。以稳定尝试键承载PENDING，独立提交观察后生成正式source_set_id并单次封口；仅代表明确TDX来源范围，不代表稳定证券身份或严格PIT。';
COMMENT ON COLUMN stock.source_sets.namespace_id IS 'DEV来源命名空间UUID，必须绑定当前Stock A部署。';
COMMENT ON COLUMN stock.source_sets.ingress_attempt_id IS '正式写入尝试的幂等UUID；在durable_at确定前用于定位PENDING，不是最终source_set_id。';
COMMENT ON COLUMN stock.source_sets.target_session IS '目标交易会话日期；必须由随记录绑定的官方版本化沪深日历确认。';
COMMENT ON COLUMN stock.source_sets.package_sha256 IS 'TDX VIPDATA原始包字节SHA-256，小写64位十六进制，作为精确来源版本。';
COMMENT ON COLUMN stock.source_sets.source_set_id IS '封口后正式source_set UUID，由封口摘要确定；摘要包含durable_at，PENDING阶段为空。';
COMMENT ON COLUMN stock.source_sets.revision_id IS '同一日期与来源版本内的正整数封口修订号；历史修订须显式控制。';
COMMENT ON COLUMN stock.source_sets.source_content_id IS '引用stock.source_contents中的原始来源内容摘要，不保存来源包字节。';
COMMENT ON COLUMN stock.source_sets.source_capture_id IS '引用stock.source_captures中的本次真实捕获UUID。';
COMMENT ON COLUMN stock.source_sets.source_digest IS '完整来源集合候选语义摘要；覆盖来源Raw、质量分母、单位和候选合同。';
COMMENT ON COLUMN stock.source_sets.source_raw_digest IS '对目标日完整Raw行集按合同排序计算的SHA-256。';
COMMENT ON COLUMN stock.source_sets.source_dataset_id IS '来源数据集身份；当前为TDX_VIPDATA_PACKAGE。';
COMMENT ON COLUMN stock.source_sets.source_byte_count IS '原始来源包字节数，仅记录大小，不存包内容。';
COMMENT ON COLUMN stock.source_sets.parser_contract IS '实际用于来源扫描和规范化的解析合同标识。';
COMMENT ON COLUMN stock.source_sets.capture_expected_row_count IS '捕获声明的完整来源代码观察分母，单位为行。';
COMMENT ON COLUMN stock.source_sets.raw_row_count IS 'PG Raw实际读取行数，须等于捕获分母及状态分区合计。';
COMMENT ON COLUMN stock.source_sets.target_present_count IS '存在目标日Bar的来源代码行数；不等同正式上市证券总体。';
COMMENT ON COLUMN stock.source_sets.target_missing_count IS '缺少目标日Bar的来源代码行数；不推断停牌、退市或未上市。';
COMMENT ON COLUMN stock.source_sets.read_failed_count IS '来源读取失败行数；正式P03当前要求为0。';
COMMENT ON COLUMN stock.source_sets.capture_read_started_at IS '本机开始读取来源的带时区观测时刻，不代表available_at或首次捕获。';
COMMENT ON COLUMN stock.source_sets.capture_validated_at IS '本机完成本次来源校验的带时区观测时刻，不代表供应方发布时间。';
COMMENT ON COLUMN stock.source_sets.source_available_at IS '供应方真实可得时间；未获证明时必须为NULL，不能用读取或封口时钟代替。';
COMMENT ON COLUMN stock.source_sets.source_time_quality IS '来源时钟资格：UNKNOWN未知、NON_PIT_OBSERVED后验观察、VERIFIED_SOURCE_TIME已验证来源时钟。';
COMMENT ON COLUMN stock.source_sets.source_clock_role IS '捕获记录声明的时钟角色，须与真实capture记录一致。';
COMMENT ON COLUMN stock.source_sets.price_unit_status IS '原始价格单位状态；RAW_UNADJUSTED表示未复权来源坐标，UNIT_UNVERIFIED表示单位未验证。';
COMMENT ON COLUMN stock.source_sets.amount_unit_status IS '成交额单位状态；UNIT_UNVERIFIED时不得作为规范化成交额消费。';
COMMENT ON COLUMN stock.source_sets.volume_unit_status IS '成交量单位状态；UNIT_UNVERIFIED时不得作为规范化成交量消费。';
COMMENT ON COLUMN stock.source_sets.calendar_id IS '用于确认目标交易日的交易所日历身份。';
COMMENT ON COLUMN stock.source_sets.calendar_revision IS '正式绑定的版本化交易所日历修订标识。';
COMMENT ON COLUMN stock.source_sets.calendar_sha256 IS '版本化官方交易日历原始字节SHA-256。';
COMMENT ON COLUMN stock.source_sets.scope_id IS 'P00正式计划范围；本表仅接受SOURCE_INGRESS窄范围。';
COMMENT ON COLUMN stock.source_sets.scope_contract_version IS 'SOURCE_INGRESS范围合同版本。';
COMMENT ON COLUMN stock.source_sets.scope_contract_digest IS 'SOURCE_INGRESS范围修订闭包绑定摘要。';
COMMENT ON COLUMN stock.source_sets.scope_catalog_digest IS '现役范围修订目录摘要。';
COMMENT ON COLUMN stock.source_sets.source_semantic_revision IS '来源字段实际使用的语义解析修订标识。';
COMMENT ON COLUMN stock.source_sets.business_input_digest IS '本次计划消费的有界业务输入摘要，不含运行时任意JSON载荷。';
COMMENT ON COLUMN stock.source_sets.plan_sha256 IS '本次目标日期SOURCE_INGRESS P00计划的稳定SHA-256。';
COMMENT ON COLUMN stock.source_sets.contract_sha256 IS '本次计划使用的已装配day-chain机器合同SHA-256。';
COMMENT ON COLUMN stock.source_sets.head_generation IS '来源接受前态代数；按已封口正式来源序列递增，不切换业务Head。';
COMMENT ON COLUMN stock.source_sets.durability_status IS '持久性状态：PENDING尚未确认、VERIFIED_POST_COMMIT_READBACK表示PENDING提交已由另一事务精确读回。';
COMMENT ON COLUMN stock.source_sets.commit_ack_observed_at IS '写入尝试事务COMMIT成功返回后由调用端观察的带时区时刻，不是来源可得时间。';
COMMENT ON COLUMN stock.source_sets.durable_at IS '独立PG事务读到精确PENDING尝试后记录的持久观察时刻；source_set_id摘要显式依赖此值，不是PG commit timestamp或source_available_at。';
COMMENT ON COLUMN stock.source_sets.durability_source_capture_id IS '持久观察回执所绑定的真实来源捕获UUID。';
COMMENT ON COLUMN stock.source_sets.durability_source_digest IS '持久观察回执所绑定的来源集合摘要。';
COMMENT ON COLUMN stock.source_sets.seal_digest_sha256 IS '最终封口证明摘要，绑定source_set_id、durable_at及封口事务身份。';
COMMENT ON COLUMN stock.source_sets.seal_status IS '正式封口状态：PENDING待恢复或SEALED已按精确键封口。';
COMMENT ON COLUMN stock.source_sets.seal_writer_transaction_id IS '执行唯一PENDING→SEALED更新的PG事务ID十进制值。';
COMMENT ON COLUMN stock.source_sets.sealed_at IS 'SEALED状态更新前在封口事务内观察的PG带时区时刻；最终持久性另由提交后独立读回证明。';
COMMENT ON COLUMN stock.source_sets.created_at IS 'PENDING主行插入时PG事务内观察的时刻；仅审计，不用于durable_at或source_set_id。';

ALTER TABLE stock.source_sets OWNER TO stock_a;

CREATE TABLE stock.source_set_capabilities (
    namespace_id uuid NOT NULL,
    ingress_attempt_id uuid NOT NULL,
    capability_id text NOT NULL CHECK (capability_id IN ('BAOSTOCK_SUPPLEMENT', 'P03_COMMIT_ACK')),
    status text NOT NULL,
    plan_sha256 char(64) NOT NULL CHECK (plan_sha256 ~ '^[0-9a-f]{64}$'),
    policy_sha256 char(64) NOT NULL CHECK (policy_sha256 ~ '^[0-9a-f]{64}$'),
    binding_sha256 char(64) NOT NULL CHECK (binding_sha256 ~ '^[0-9a-f]{64}$'),
    policy_contract text,
    max_logins integer,
    max_queries integer,
    query_timeout_seconds integer,
    reason_code text,
    data_quality text,
    actual_logins integer NOT NULL CHECK (actual_logins >= 0),
    actual_queries integer NOT NULL CHECK (actual_queries >= 0),
    network_accessed boolean NOT NULL,
    writer_transaction_id text,
    observer_transaction_id text,
    pending_row_xmin text,
    pending_creation_transaction_id text,
    commit_ack_observed_at timestamptz,
    durable_at timestamptz,
    source_capture_id uuid,
    source_digest char(64) CHECK (source_digest IS NULL OR source_digest ~ '^[0-9a-f]{64}$'),
    capability_digest char(64) NOT NULL CHECK (capability_digest ~ '^[0-9a-f]{64}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (namespace_id, ingress_attempt_id, capability_id),
    FOREIGN KEY (namespace_id, ingress_attempt_id)
        REFERENCES stock.source_sets(namespace_id, ingress_attempt_id),
    CHECK (
        (capability_id = 'BAOSTOCK_SUPPLEMENT' AND status = 'NOT_REQUESTED'
         AND policy_contract IS NOT NULL AND max_logins IS NOT NULL
         AND max_queries IS NOT NULL AND query_timeout_seconds IS NOT NULL
         AND reason_code IS NOT NULL AND data_quality IS NOT NULL
         AND policy_contract = 'STOCK-A-OPTIONAL-BAOSTOCK-SUPPLEMENT/R1'
         AND max_logins = 1 AND max_queries = 10 AND query_timeout_seconds = 30
         AND reason_code = 'OPTIONAL_SOURCE_NOT_REQUESTED'
         AND data_quality = 'NOT_MEASURED_NO_RESPONSE'
         AND actual_logins = 0 AND actual_queries = 0 AND network_accessed IS FALSE
         AND writer_transaction_id IS NULL AND observer_transaction_id IS NULL
         AND pending_row_xmin IS NULL AND pending_creation_transaction_id IS NULL
         AND commit_ack_observed_at IS NULL AND durable_at IS NULL
         AND source_capture_id IS NULL AND source_digest IS NULL)
        OR
        (capability_id = 'P03_COMMIT_ACK' AND status = 'VERIFIED_POST_COMMIT_READBACK'
         AND policy_contract IS NULL AND max_logins IS NULL AND max_queries IS NULL
         AND query_timeout_seconds IS NULL AND reason_code = 'PENDING_ROW_INDEPENDENTLY_READ'
         AND data_quality IS NULL AND actual_logins = 0 AND actual_queries = 0
         AND network_accessed IS FALSE
         AND writer_transaction_id IS NOT NULL
         AND writer_transaction_id ~ '^[1-9][0-9]*$'
         AND observer_transaction_id IS NOT NULL
         AND observer_transaction_id ~ '^[1-9][0-9]*$'
         AND pending_row_xmin IS NOT NULL
         AND pending_row_xmin ~ '^[1-9][0-9]*$'
         AND pending_creation_transaction_id IS NOT NULL
         AND pending_creation_transaction_id ~ '^[1-9][0-9]*$'
         AND commit_ack_observed_at IS NOT NULL AND durable_at IS NOT NULL
         AND source_capture_id IS NOT NULL AND source_digest IS NOT NULL
         AND durable_at >= commit_ack_observed_at)
    )
);

CREATE INDEX source_set_capabilities_attempt_idx
    ON stock.source_set_capabilities (namespace_id, ingress_attempt_id);

COMMENT ON TABLE stock.source_set_capabilities IS '每个P03尝试最多两条typed能力记录：精确P00/P02 Bao零请求绑定和PENDING提交后独立读回ACK；不保存通用payload。';
COMMENT ON COLUMN stock.source_set_capabilities.namespace_id IS 'DEV来源命名空间UUID，与source_sets尝试主行相同。';
COMMENT ON COLUMN stock.source_set_capabilities.ingress_attempt_id IS '引用source_sets的稳定PENDING幂等尝试UUID，最终source_set_id由主行单独生成。';
COMMENT ON COLUMN stock.source_set_capabilities.capability_id IS '能力记录角色：BAOSTOCK_SUPPLEMENT可选补充源，P03_COMMIT_ACK为PENDING持久观察回执。';
COMMENT ON COLUMN stock.source_set_capabilities.status IS '能力状态；Bao未请求为NOT_REQUESTED，确认回执为VERIFIED_POST_COMMIT_READBACK。';
COMMENT ON COLUMN stock.source_set_capabilities.plan_sha256 IS '能力与P00 SOURCE_INGRESS计划摘要的精确绑定。';
COMMENT ON COLUMN stock.source_set_capabilities.policy_sha256 IS '能力与当前登记策略或本次PENDING来源策略的精确绑定。';
COMMENT ON COLUMN stock.source_set_capabilities.binding_sha256 IS 'P02摘要；以目标日、包SHA、计划/策略、预算、零请求和质量声明重算。';
COMMENT ON COLUMN stock.source_set_capabilities.policy_contract IS '可选来源策略合同标识；仅Bao能力行填写。';
COMMENT ON COLUMN stock.source_set_capabilities.max_logins IS 'Bao允许的最大登录次数，本任务预算为1。';
COMMENT ON COLUMN stock.source_set_capabilities.max_queries IS 'Bao每目标交易会话允许的最大查询次数，本任务预算为10。';
COMMENT ON COLUMN stock.source_set_capabilities.query_timeout_seconds IS 'Bao单次查询超时秒数，本任务预算为30秒。';
COMMENT ON COLUMN stock.source_set_capabilities.reason_code IS '非请求或ACK的稳定原因码。';
COMMENT ON COLUMN stock.source_set_capabilities.data_quality IS 'Bao响应质量状态；未请求时为NOT_MEASURED_NO_RESPONSE。';
COMMENT ON COLUMN stock.source_set_capabilities.actual_logins IS '本次实际Bao登录次数；未请求时为0。';
COMMENT ON COLUMN stock.source_set_capabilities.actual_queries IS '本次实际Bao查询次数；未请求时为0。';
COMMENT ON COLUMN stock.source_set_capabilities.network_accessed IS '本次是否访问网络；本批P02 NOT_REQUESTED必须为false。';
COMMENT ON COLUMN stock.source_set_capabilities.writer_transaction_id IS '写入尝试或幂等恢复事务的PG XID十进制值。';
COMMENT ON COLUMN stock.source_set_capabilities.observer_transaction_id IS '独立读取PENDING行的PG XID十进制值，必须区别于写入事务。';
COMMENT ON COLUMN stock.source_set_capabilities.pending_row_xmin IS '被独立读回的PENDING主行xmin十进制值，用于核实实际创建事务。';
COMMENT ON COLUMN stock.source_set_capabilities.pending_creation_transaction_id IS '按独立读取事务XID与xmin age推导的PENDING行创建full transaction ID十进制值；取模2^32等于pending_row_xmin。';
COMMENT ON COLUMN stock.source_set_capabilities.commit_ack_observed_at IS '提交事务成功返回后由调用端观察的带时区时刻。';
COMMENT ON COLUMN stock.source_set_capabilities.durable_at IS '独立事务精确读回PENDING尝试后观察到的带时区时刻；不是来源available_at或PG server commit timestamp。';
COMMENT ON COLUMN stock.source_set_capabilities.source_capture_id IS 'ACK绑定的真实来源捕获UUID。';
COMMENT ON COLUMN stock.source_set_capabilities.source_digest IS 'ACK绑定的来源候选语义摘要。';
COMMENT ON COLUMN stock.source_set_capabilities.capability_digest IS '本能力行全部typed业务字段的SHA-256，不包含数据库生成时间。';
COMMENT ON COLUMN stock.source_set_capabilities.created_at IS '能力行插入时PG事务内观察时刻；不代替回执所记提交或独立观察时钟。';

ALTER TABLE stock.source_set_capabilities OWNER TO stock_a;
