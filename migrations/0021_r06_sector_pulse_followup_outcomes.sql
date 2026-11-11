CREATE TABLE stock.source_sector_pulse_followup_facts (
    namespace_id uuid NOT NULL,
    t0_session date NOT NULL,
    t0_package_sha256 character(64) NOT NULL,
    t0_source_capture_id uuid NOT NULL,
    t0_source_content_id character(64) NOT NULL,
    t0_source_raw_digest character(64) NOT NULL,
    t0_profile_revision character(64) NOT NULL,
    snapshot_id uuid NOT NULL,
    board_kind text NOT NULL,
    board_native_id text NOT NULL,
    board_name text NOT NULL,
    pulse_algorithm_contract text NOT NULL,
    pulse_state text NOT NULL,
    pulse_reason_codes text[] NOT NULL,
    followup_contract text NOT NULL,
    horizon_sessions smallint NOT NULL,
    target_session date NOT NULL,
    target_package_sha256 character(64) NOT NULL,
    target_source_capture_id uuid NOT NULL,
    target_source_content_id character(64) NOT NULL,
    target_source_raw_digest character(64) NOT NULL,
    calendar_id text NOT NULL,
    calendar_revision text NOT NULL,
    calendar_sha256 character(64) NOT NULL,
    fixed_member_denominator integer NOT NULL,
    observed_member_count integer NOT NULL,
    unknown_member_count integer NOT NULL,
    equal_weight_return_numerator numeric,
    equal_weight_return_denominator numeric,
    outcome_status text NOT NULL,
    reason_codes text[] NOT NULL,
    research_mode text NOT NULL,
    membership_mode text NOT NULL,
    price_coordinate text NOT NULL,
    t0_source_time_quality text NOT NULL,
    t0_source_clock_role text NOT NULL,
    t0_source_available_at timestamp with time zone,
    target_source_time_quality text NOT NULL,
    target_source_clock_role text NOT NULL,
    target_source_available_at timestamp with time zone,
    recorded_at timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT source_sector_pulse_followup_facts_pkey PRIMARY KEY (
        namespace_id,
        t0_session,
        t0_package_sha256,
        t0_source_capture_id,
        t0_source_content_id,
        t0_source_raw_digest,
        t0_profile_revision,
        snapshot_id,
        board_kind,
        board_native_id,
        pulse_algorithm_contract,
        followup_contract,
        horizon_sessions,
        target_session,
        calendar_id,
        calendar_revision,
        calendar_sha256
    ),
    CONSTRAINT source_sector_pulse_followup_identity_ck CHECK (
        t0_package_sha256 ~ '^[0-9a-f]{64}$'
        AND t0_source_content_id ~ '^[0-9a-f]{64}$'
        AND t0_source_raw_digest ~ '^[0-9a-f]{64}$'
        AND t0_profile_revision ~ '^[0-9a-f]{64}$'
        AND target_package_sha256 ~ '^[0-9a-f]{64}$'
        AND target_source_content_id ~ '^[0-9a-f]{64}$'
        AND target_source_raw_digest ~ '^[0-9a-f]{64}$'
        AND calendar_sha256 ~ '^[0-9a-f]{64}$'
        AND t0_session < target_session
        AND board_kind IN ('INDUSTRY', 'CONCEPT')
        AND board_native_id <> ''
        AND pulse_algorithm_contract = 'SOURCE_CROSS_PERIOD_PULSE_R1'
        AND followup_contract = 'SOURCE_SECTOR_PULSE_FOLLOWUP_CLOSE_R1'
        AND pulse_state IN ('TRUE', 'FALSE', 'UNKNOWN')
        AND horizon_sessions IN (1, 3, 5)
        AND calendar_id <> ''
        AND calendar_revision <> ''
        AND outcome_status IN ('OBSERVED', 'UNKNOWN')
        AND research_mode = 'NON_PIT_RETROSPECTIVE'
        AND membership_mode = 'CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT'
        AND price_coordinate = 'RAW_UNADJUSTED'
        AND t0_source_time_quality <> ''
        AND t0_source_clock_role <> ''
        AND target_source_time_quality <> ''
        AND target_source_clock_role <> ''
        AND array_position(pulse_reason_codes, NULL) IS NULL
        AND array_position(reason_codes, NULL) IS NULL
    ),
    CONSTRAINT source_sector_pulse_followup_coverage_ck CHECK (
        fixed_member_denominator >= 0
        AND observed_member_count >= 0
        AND unknown_member_count >= 0
        AND observed_member_count + unknown_member_count = fixed_member_denominator
        AND (
            outcome_status = 'OBSERVED'
            AND fixed_member_denominator > 0
            AND observed_member_count = fixed_member_denominator
            AND unknown_member_count = 0
            AND equal_weight_return_numerator IS NOT NULL
            AND equal_weight_return_denominator > 0
            OR outcome_status = 'UNKNOWN'
            AND (
                fixed_member_denominator = 0
                OR observed_member_count < fixed_member_denominator
            )
            AND equal_weight_return_numerator IS NULL
            AND equal_weight_return_denominator IS NULL
        )
    ),
    CONSTRAINT source_sector_pulse_followup_pulse_fk FOREIGN KEY (
        namespace_id,
        t0_session,
        t0_package_sha256,
        t0_source_capture_id,
        t0_source_content_id,
        t0_source_raw_digest,
        t0_profile_revision,
        snapshot_id,
        board_kind,
        board_native_id,
        pulse_algorithm_contract
    ) REFERENCES stock.source_sector_cross_period_pulse_facts (
        namespace_id,
        target_session,
        package_sha256,
        source_capture_id,
        source_content_id,
        source_raw_digest,
        profile_revision,
        snapshot_id,
        board_kind,
        board_native_id,
        algorithm_contract
    ),
    CONSTRAINT source_sector_pulse_followup_snapshot_fk FOREIGN KEY (
        snapshot_id
    ) REFERENCES stock.tdx_sector_source_snapshots (snapshot_id),
    CONSTRAINT source_sector_pulse_followup_target_capture_fk FOREIGN KEY (
        namespace_id,
        target_source_capture_id
    ) REFERENCES stock.source_captures (namespace_id, capture_id)
);

CREATE INDEX source_sector_pulse_followup_lookup_idx
    ON stock.source_sector_pulse_followup_facts (
        namespace_id,
        t0_session,
        t0_package_sha256,
        snapshot_id,
        board_kind,
        board_native_id,
        horizon_sessions
    );

COMMENT ON TABLE stock.source_sector_pulse_followup_facts IS '保存已存板块PULSE对应的T+1、T+3、T+5后继Raw收盘等权篮子结果及固定成员覆盖质量；仅作NON_PIT探索回顾。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.namespace_id IS '绑定唯一Stock A来源命名空间UUID。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.t0_session IS '已保存PULSE的T0官方交易会话日期。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.t0_package_sha256 IS 'T0来源Raw包SHA-256；结果只绑定此来源版本。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.t0_source_capture_id IS 'T0 Raw实际捕获UUID。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.t0_source_content_id IS 'T0规范来源内容摘要SHA-256。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.t0_source_raw_digest IS 'T0完整Raw捕获行摘要SHA-256。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.t0_profile_revision IS 'T0已存PULSE绑定的画像修订SHA-256。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.snapshot_id IS 'T0 PULSE使用的当前TDX行业或GN概念成员快照UUID；历史回看为NON_PIT。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.board_kind IS '板块类型枚举：INDUSTRY行业、CONCEPT概念。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.board_native_id IS 'TDX来源目录中的原生板块编号。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.board_name IS 'T0板块快照中的显示名称。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.pulse_algorithm_contract IS 'T0冻结PULSE算法合同标识；本迁移不修改算法。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.pulse_state IS 'T0已存PULSE状态枚举：TRUE、FALSE或UNKNOWN；与后继数值结果分列。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.pulse_reason_codes IS 'T0 PULSE三态对应的具名原因码数组。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.followup_contract IS '固定成员Raw收盘等权后继结果的算法修订标识。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.horizon_sessions IS '后继期限，按官方交易会话计数，允许1、3、5。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.target_session IS '该期限对应的官方交易会话终点日期，不按自然日推算。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.target_package_sha256 IS '终点Raw来源包SHA-256；须与T0同一来源包坐标。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.target_source_capture_id IS '终点Raw实际捕获UUID。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.target_source_content_id IS '终点规范来源内容摘要SHA-256。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.target_source_raw_digest IS '终点完整Raw捕获行摘要SHA-256。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.calendar_id IS '官方交易会话日历业务标识。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.calendar_revision IS '官方交易会话日历修订版本。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.calendar_sha256 IS '官方日历年度资源SHA-256。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.fixed_member_denominator IS 'T0后验成员快照中的固定完整板块成员数；缺价成员仍留在分母。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.observed_member_count IS 'T0与终点Raw收盘、来源身份及未复权坐标均可验证的成员数。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.unknown_member_count IS '无法评价的固定篮子成员数；不将缺值记为零收益或删除成员。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.equal_weight_return_numerator IS '完整固定等权篮子平均简单收益的精确分子；有UNKNOWN成员时为空。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.equal_weight_return_denominator IS '完整固定等权篮子平均简单收益的精确正分母；有UNKNOWN成员时为空。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.outcome_status IS '后继结果质量枚举：OBSERVED为全成员可评价，UNKNOWN为部分或全部成员证据不足。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.reason_codes IS '成员篮子覆盖、Raw质量或来源坐标无法评价时的具名原因码数组。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.research_mode IS '时态语义固定为NON_PIT_RETROSPECTIVE，不表示严格PIT、预测或可交易收益。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.membership_mode IS '成员模式固定为CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT，表示当前快照后验回看。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.price_coordinate IS '价格坐标固定为RAW_UNADJUSTED，使用原始未复权收盘价。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.t0_source_time_quality IS 'T0来源捕获的实际时钟质量标识。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.t0_source_clock_role IS 'T0来源捕获时钟角色；不将后验时钟伪装为PIT可用时间。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.t0_source_available_at IS 'T0来源记录的可用时间戳；可为空，按原来源时钟语义解释。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.target_source_time_quality IS '后继终点来源捕获的实际时钟质量标识。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.target_source_clock_role IS '后继终点来源捕获时钟角色。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.target_source_available_at IS '后继终点来源记录的可用时间戳；可为空，按原来源时钟语义解释。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.recorded_at IS '该typed结果事务写入PG的数据库时间；不替代来源可用时间。';

COMMENT ON INDEX stock.source_sector_pulse_followup_lookup_idx IS '按已存T0来源版本和板块键批量读取官方会话后继结果。';
