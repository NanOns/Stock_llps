CREATE TABLE stock.source_sector_pulse_adjacent_transitions (
    namespace_id uuid NOT NULL,
    previous_session date NOT NULL,
    target_session date NOT NULL,
    previous_package_sha256 character(64) NOT NULL,
    previous_source_capture_id uuid NOT NULL,
    previous_source_content_id character(64) NOT NULL,
    previous_source_raw_digest character(64) NOT NULL,
    previous_profile_revision character(64) NOT NULL,
    target_package_sha256 character(64) NOT NULL,
    target_source_capture_id uuid NOT NULL,
    target_source_content_id character(64) NOT NULL,
    target_source_raw_digest character(64) NOT NULL,
    target_profile_revision character(64) NOT NULL,
    calendar_id text NOT NULL,
    calendar_revision text NOT NULL,
    calendar_sha256 character(64) NOT NULL,
    snapshot_id uuid NOT NULL,
    board_kind text NOT NULL,
    board_native_id text NOT NULL,
    pulse_algorithm_contract text NOT NULL,
    transition_contract text NOT NULL,
    previous_pulse_state text NOT NULL,
    target_pulse_state text NOT NULL,
    transition_state text NOT NULL,
    previous_reason_codes text[] NOT NULL,
    target_reason_codes text[] NOT NULL,
    calculation_status text NOT NULL,
    research_mode text NOT NULL,
    price_coordinate text NOT NULL,
    formal_rotation_state text NOT NULL,
    recorded_at timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT source_sector_pulse_adjacent_transitions_pkey PRIMARY KEY (
        namespace_id,
        previous_session,
        target_session,
        previous_package_sha256,
        previous_source_capture_id,
        previous_source_content_id,
        previous_source_raw_digest,
        previous_profile_revision,
        target_package_sha256,
        target_source_capture_id,
        target_source_content_id,
        target_source_raw_digest,
        target_profile_revision,
        snapshot_id,
        board_kind,
        board_native_id,
        pulse_algorithm_contract,
        transition_contract,
        calendar_revision,
        calendar_sha256,
        calendar_id
    ),
    CONSTRAINT source_sector_pulse_adjacent_transition_kind_ck
        CHECK (board_kind IN ('INDUSTRY', 'CONCEPT')),
    CONSTRAINT source_sector_pulse_adjacent_transition_state_ck
        CHECK (
            previous_pulse_state IN ('TRUE', 'FALSE', 'UNKNOWN')
            AND target_pulse_state IN ('TRUE', 'FALSE', 'UNKNOWN')
            AND transition_state IN (
                'PULSE_APPEARED',
                'PULSE_PERSISTED',
                'PULSE_CEASED',
                'PULSE_NOT_PRESENT_BOTH',
                'PULSE_TRANSITION_UNKNOWN'
            )
            AND (
                (previous_pulse_state = 'UNKNOWN' OR target_pulse_state = 'UNKNOWN')
                AND transition_state = 'PULSE_TRANSITION_UNKNOWN'
                OR previous_pulse_state = 'FALSE' AND target_pulse_state = 'TRUE'
                    AND transition_state = 'PULSE_APPEARED'
                OR previous_pulse_state = 'TRUE' AND target_pulse_state = 'TRUE'
                    AND transition_state = 'PULSE_PERSISTED'
                OR previous_pulse_state = 'TRUE' AND target_pulse_state = 'FALSE'
                    AND transition_state = 'PULSE_CEASED'
                OR previous_pulse_state = 'FALSE' AND target_pulse_state = 'FALSE'
                    AND transition_state = 'PULSE_NOT_PRESENT_BOTH'
            )
        ),
    CONSTRAINT source_sector_pulse_adjacent_transition_key_ck CHECK (
        previous_session < target_session
        AND previous_package_sha256 ~ '^[0-9a-f]{64}$'
        AND previous_source_content_id ~ '^[0-9a-f]{64}$'
        AND previous_source_raw_digest ~ '^[0-9a-f]{64}$'
        AND previous_profile_revision ~ '^[0-9a-f]{64}$'
        AND target_package_sha256 ~ '^[0-9a-f]{64}$'
        AND target_source_content_id ~ '^[0-9a-f]{64}$'
        AND target_source_raw_digest ~ '^[0-9a-f]{64}$'
        AND target_profile_revision ~ '^[0-9a-f]{64}$'
        AND calendar_id <> ''
        AND calendar_revision <> ''
        AND calendar_sha256 ~ '^[0-9a-f]{64}$'
        AND board_native_id <> ''
        AND pulse_algorithm_contract = 'SOURCE_CROSS_PERIOD_PULSE_R1'
        AND transition_contract = 'SOURCE_CROSS_PERIOD_PULSE_TRANSITION_R1'
        AND calculation_status = 'COMPLETE'
        AND research_mode = 'NON_PIT_RETROSPECTIVE'
        AND price_coordinate = 'RAW_UNADJUSTED'
        AND formal_rotation_state = 'NOT_EVALUATED'
        AND array_position(previous_reason_codes, NULL) IS NULL
        AND array_position(target_reason_codes, NULL) IS NULL
    ),
    CONSTRAINT source_sector_pulse_adjacent_transition_previous_fk FOREIGN KEY (
        namespace_id,
        previous_session,
        previous_package_sha256,
        previous_source_capture_id,
        previous_source_content_id,
        previous_source_raw_digest,
        previous_profile_revision,
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
    CONSTRAINT source_sector_pulse_adjacent_transition_target_fk FOREIGN KEY (
        namespace_id,
        target_session,
        target_package_sha256,
        target_source_capture_id,
        target_source_content_id,
        target_source_raw_digest,
        target_profile_revision,
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
    )
);

CREATE INDEX source_sector_pulse_adjacent_lookup_idx
    ON stock.source_sector_pulse_adjacent_transitions (
        namespace_id,
        target_session,
        target_package_sha256,
        target_source_capture_id,
        target_profile_revision,
        snapshot_id,
        board_kind,
        board_native_id
    );

COMMENT ON TABLE stock.source_sector_pulse_adjacent_transitions IS '保存同一板块快照在严格相邻官方会话间的来源级PULSE状态变化；保留两端来源修订与UNKNOWN原因，不代表正式Rotation生命周期或PIT结果。';
COMMENT ON COLUMN stock.source_sector_pulse_adjacent_transitions.namespace_id IS '绑定唯一Stock A DEV来源命名空间UUID。';
COMMENT ON COLUMN stock.source_sector_pulse_adjacent_transitions.previous_session IS '变化前一端的官方交易会话日期，必须与目标会话严格相邻。';
COMMENT ON COLUMN stock.source_sector_pulse_adjacent_transitions.target_session IS '变化后一端的官方交易会话日期；HTTP读取按该日期定位。';
COMMENT ON COLUMN stock.source_sector_pulse_adjacent_transitions.previous_package_sha256 IS '变化前一端Raw来源包SHA-256。';
COMMENT ON COLUMN stock.source_sector_pulse_adjacent_transitions.previous_source_capture_id IS '变化前一端Raw实际捕获UUID。';
COMMENT ON COLUMN stock.source_sector_pulse_adjacent_transitions.previous_source_content_id IS '变化前一端规范来源内容摘要SHA-256。';
COMMENT ON COLUMN stock.source_sector_pulse_adjacent_transitions.previous_source_raw_digest IS '变化前一端Raw捕获行摘要SHA-256。';
COMMENT ON COLUMN stock.source_sector_pulse_adjacent_transitions.previous_profile_revision IS '变化前一端PULSE所绑定的现行价格画像修订SHA-256。';
COMMENT ON COLUMN stock.source_sector_pulse_adjacent_transitions.target_package_sha256 IS '变化后一端Raw来源包SHA-256。';
COMMENT ON COLUMN stock.source_sector_pulse_adjacent_transitions.target_source_capture_id IS '变化后一端Raw实际捕获UUID。';
COMMENT ON COLUMN stock.source_sector_pulse_adjacent_transitions.target_source_content_id IS '变化后一端规范来源内容摘要SHA-256。';
COMMENT ON COLUMN stock.source_sector_pulse_adjacent_transitions.target_source_raw_digest IS '变化后一端Raw捕获行摘要SHA-256。';
COMMENT ON COLUMN stock.source_sector_pulse_adjacent_transitions.target_profile_revision IS '变化后一端PULSE所绑定的现行价格画像修订SHA-256。';
COMMENT ON COLUMN stock.source_sector_pulse_adjacent_transitions.calendar_id IS '验证相邻关系所用的官方交易日历标识。';
COMMENT ON COLUMN stock.source_sector_pulse_adjacent_transitions.calendar_revision IS '验证相邻关系所用的官方交易日历修订。';
COMMENT ON COLUMN stock.source_sector_pulse_adjacent_transitions.calendar_sha256 IS '官方交易日历原始资源的SHA-256摘要。';
COMMENT ON COLUMN stock.source_sector_pulse_adjacent_transitions.snapshot_id IS '两端共同引用的同一行业或GN概念成员快照UUID；成员含义为NON_PIT回看。';
COMMENT ON COLUMN stock.source_sector_pulse_adjacent_transitions.board_kind IS '板块类型枚举：INDUSTRY行业或CONCEPT对应GN概念。';
COMMENT ON COLUMN stock.source_sector_pulse_adjacent_transitions.board_native_id IS '原生板块编号；与共同快照及类型共同定位同一板块。';
COMMENT ON COLUMN stock.source_sector_pulse_adjacent_transitions.pulse_algorithm_contract IS '两端共同使用的冻结来源级PULSE算法合同标识。';
COMMENT ON COLUMN stock.source_sector_pulse_adjacent_transitions.transition_contract IS '描述性相邻变化归类合同版本，不代表正式Rotation阶段。';
COMMENT ON COLUMN stock.source_sector_pulse_adjacent_transitions.previous_pulse_state IS '变化前一端的PULSE状态：TRUE、FALSE或UNKNOWN。';
COMMENT ON COLUMN stock.source_sector_pulse_adjacent_transitions.target_pulse_state IS '变化后一端的PULSE状态：TRUE、FALSE或UNKNOWN。';
COMMENT ON COLUMN stock.source_sector_pulse_adjacent_transitions.transition_state IS '相邻状态变化：出现、连续成立、不再成立、连续未成立或含UNKNOWN无法比较。';
COMMENT ON COLUMN stock.source_sector_pulse_adjacent_transitions.previous_reason_codes IS '变化前一端PULSE的具名原因码；UNKNOWN时保留原始质量原因。';
COMMENT ON COLUMN stock.source_sector_pulse_adjacent_transitions.target_reason_codes IS '变化后一端PULSE的具名原因码；UNKNOWN时保留原始质量原因。';
COMMENT ON COLUMN stock.source_sector_pulse_adjacent_transitions.calculation_status IS '本对相邻会话完整同类板块分区状态；持久化行仅接受COMPLETE。';
COMMENT ON COLUMN stock.source_sector_pulse_adjacent_transitions.research_mode IS '历史研究时间语义；当前值NON_PIT_RETROSPECTIVE，不表示当时可用。';
COMMENT ON COLUMN stock.source_sector_pulse_adjacent_transitions.price_coordinate IS '两端共同的价格坐标；当前PULSE合同使用RAW_UNADJUSTED原始未复权。';
COMMENT ON COLUMN stock.source_sector_pulse_adjacent_transitions.formal_rotation_state IS '正式Rotation生命周期状态；本描述性变化固定为NOT_EVALUATED。';
COMMENT ON COLUMN stock.source_sector_pulse_adjacent_transitions.recorded_at IS '变化事实写入PG的带时区记录时刻，不是行情可用时钟。';
COMMENT ON INDEX stock.source_sector_pulse_adjacent_lookup_idx IS '按目标日期、精确目标来源、快照和板块编号批量读取相邻变化。';

ALTER TABLE stock.source_sector_pulse_adjacent_transitions OWNER TO stock_a;
ALTER INDEX stock.source_sector_pulse_adjacent_lookup_idx OWNER TO stock_a;
