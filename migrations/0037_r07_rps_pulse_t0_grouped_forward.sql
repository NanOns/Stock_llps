CREATE TABLE stock.source_rps_pulse_t0_change_facts (
    source_t0_fact_id bigint PRIMARY KEY
        REFERENCES stock.source_t0_early_observation_facts(id) ON DELETE CASCADE,
    target_session date NOT NULL,
    previous_session date NOT NULL,
    source_package_id bigint NOT NULL REFERENCES stock.source_packages(source_package_id),
    source_capture_id bigint NOT NULL,
    previous_source_capture_id bigint,
    enrollment_id bigint NOT NULL REFERENCES stock.source_research_enrollments(id),
    source_symbol text NOT NULL,
    board_kind text NOT NULL,
    board_native_id text NOT NULL,
    membership_snapshot_id bigint NOT NULL
        REFERENCES stock.tdx_sector_source_snapshots(snapshot_id),
    catalog_id bigint NOT NULL REFERENCES stock.tdx_sector_catalog(id),
    rule_fact_id bigint REFERENCES stock.source_rule_evaluations(id),
    rule_id text NOT NULL,
    rule_version text NOT NULL,
    rule_state text NOT NULL,
    calculation_version text NOT NULL,
    source_pool_status text NOT NULL,
    source_pool_reason_codes text[] NOT NULL DEFAULT '{}',
    previous_seed_fact_id bigint REFERENCES stock.source_pool_seed_day_facts(id),
    rps5_change_state text NOT NULL,
    rps5_reason_codes text[] NOT NULL DEFAULT '{}',
    rps5_previous_rank_first integer,
    rps5_previous_rank_last integer,
    rps5_previous_valid_member_count integer,
    rps20_change_state text NOT NULL,
    rps20_reason_codes text[] NOT NULL DEFAULT '{}',
    rps20_previous_rank_first integer,
    rps20_previous_rank_last integer,
    rps20_previous_valid_member_count integer,
    membership_mode text NOT NULL,
    research_mode text NOT NULL,
    price_coordinate text NOT NULL,
    recorded_at timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT source_rps_pulse_symbol_ck
        CHECK (source_symbol ~ '^(SH|SZ)\.[0-9]{6}$'),
    CONSTRAINT source_rps_pulse_board_ck
        CHECK (board_kind IN ('INDUSTRY', 'CONCEPT') AND board_native_id <> ''),
    CONSTRAINT source_rps_pulse_rule_state_ck
        CHECK (rule_state IN ('TRUE', 'FALSE', 'UNKNOWN', 'NOT_APPLICABLE')),
    CONSTRAINT source_rps_pulse_version_ck
        CHECK (calculation_version = 'RPS_PULSE_T0_GROUPED_FORWARD_R1'),
    CONSTRAINT source_rps_pulse_source_status_ck
        CHECK (source_pool_status IN ('AVAILABLE', 'UNKNOWN', 'NOT_COMPARABLE', 'NOT_COMPUTED')),
    CONSTRAINT source_rps_pulse_source_reason_count_ck
        CHECK (cardinality(source_pool_reason_codes) <= 12),
    CONSTRAINT source_rps_pulse_rps5_state_ck
        CHECK (rps5_change_state IN (
            'IMPROVED', 'WORSENED', 'UNCHANGED', 'UNKNOWN',
            'NOT_COMPARABLE', 'NOT_COMPUTED'
        )),
    CONSTRAINT source_rps_pulse_rps20_state_ck
        CHECK (rps20_change_state IN (
            'IMPROVED', 'WORSENED', 'UNCHANGED', 'UNKNOWN',
            'NOT_COMPARABLE', 'NOT_COMPUTED'
        )),
    CONSTRAINT source_rps_pulse_metric_reason_count_ck
        CHECK (cardinality(rps5_reason_codes) <= 12 AND cardinality(rps20_reason_codes) <= 12),
    CONSTRAINT source_rps_pulse_rank_count_ck
        CHECK (
            (rps5_previous_rank_first IS NULL AND rps5_previous_rank_last IS NULL
             AND rps5_previous_valid_member_count IS NULL)
            OR
            (rps5_previous_rank_first > 0
             AND rps5_previous_rank_last >= rps5_previous_rank_first
             AND rps5_previous_valid_member_count >= rps5_previous_rank_last)
        ),
    CONSTRAINT source_rps_pulse_rank20_count_ck
        CHECK (
            (rps20_previous_rank_first IS NULL AND rps20_previous_rank_last IS NULL
             AND rps20_previous_valid_member_count IS NULL)
            OR
            (rps20_previous_rank_first > 0
             AND rps20_previous_rank_last >= rps20_previous_rank_first
             AND rps20_previous_valid_member_count >= rps20_previous_rank_last)
        ),
    CONSTRAINT source_rps_pulse_dates_ck CHECK (previous_session < target_session),
    CONSTRAINT source_rps_pulse_mode_ck CHECK (
        membership_mode = 'CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT'
        AND research_mode = 'NON_PIT_EXPLORATORY'
        AND price_coordinate = 'RAW_UNADJUSTED'
    )
);

CREATE INDEX source_rps_pulse_t0_group_read_idx
    ON stock.source_rps_pulse_t0_change_facts (
        target_session, source_package_id, source_capture_id,
        rule_id, rule_version, rule_state, board_kind
    );

COMMENT ON TABLE stock.source_rps_pulse_t0_change_facts IS
    '按最新T0登记×股票×板块关系保存前一官方会话至T0的RPS5/RPS20并列名次变化；关联精确T0证据，供原Focus/Cohort读取后继Forward；历史成员回看为NON_PIT，价格为RAW_UNADJUSTED。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.source_t0_fact_id IS
    '唯一对应的最新T0板内观察事实ID；其登记、T0、来源捕获、证券、板块、快照、目录和证据修订共同构成关系键。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.target_session IS
    'RPS变化标签所对应的T0官方交易会话日期。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.previous_session IS
    '与T0直接相邻的前一官方交易会话日期。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.source_package_id IS
    'T0来源Raw包的数字ID。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.source_capture_id IS
    'T0精确来源捕获ID；不与同日其它捕获合并。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.previous_source_capture_id IS
    '前一官方会话使用的精确来源捕获ID；不可唯一确定时为空并保留失败原因。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.enrollment_id IS
    '来源研究T0登记ID；不同登记分别计为关系，不等于独立证券。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.source_symbol IS
    '来源证券代码；仅定位行情，不作为稳定证券身份。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.board_kind IS
    '行业或概念板块类型。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.board_native_id IS
    '成员快照中的板块原生编号。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.membership_snapshot_id IS
    'T0使用的固定板块成员快照数字ID；历史回看不代表当时真实成员。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.catalog_id IS
    'T0板块目录数字ID。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.rule_fact_id IS
    '本登记关联的原来源规则事实ID；无来源规则时为空。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.rule_id IS
    '精确来源规则标识；无规则登记时使用UNSCOPED。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.rule_version IS
    '来源规则计算版本；无规则登记时使用UNSCOPED。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.rule_state IS
    'T0来源规则状态TRUE、FALSE、UNKNOWN或NOT_APPLICABLE；不等同正式资格。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.calculation_version IS
    'RPS位次变化关系事实的计算版本。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.source_pool_status IS
    '前态与T0来源、画像及来源池闭合状态；失败时仍保留关系分母。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.source_pool_reason_codes IS
    '来源捕获、完整来源池或相邻Seed范围不闭合的有限原因码。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.previous_seed_fact_id IS
    '前一官方会话该证券的精确Seed事实ID；缺失时为空。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.rps5_change_state IS
    'T0相对前一官方会话的RPS5并列名次变化：前移、后退、不变、未知、不可比或未计算。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.rps5_reason_codes IS
    'RPS5变化未知或不可比的有限原因码。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.rps5_previous_rank_first IS
    '前一会话RPS5并列名次区间起点，从1开始。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.rps5_previous_rank_last IS
    '前一会话RPS5并列名次区间终点。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.rps5_previous_valid_member_count IS
    '前一会话该板、来源和Seed口径下RPS5有效成员分母。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.rps20_change_state IS
    'T0相对前一官方会话的RPS20并列名次变化状态。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.rps20_reason_codes IS
    'RPS20变化未知或不可比的有限原因码。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.rps20_previous_rank_first IS
    '前一会话RPS20并列名次区间起点，从1开始。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.rps20_previous_rank_last IS
    '前一会话RPS20并列名次区间终点。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.rps20_previous_valid_member_count IS
    '前一会话该板、来源和Seed口径下RPS20有效成员分母。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.membership_mode IS
    '成员时间语义固定为CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.research_mode IS
    '研究时态固定为NON_PIT_EXPLORATORY，不用于严格PIT或LIVE。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.price_coordinate IS
    '用于结果描述的价格坐标固定为RAW_UNADJUSTED原始未复权。';
COMMENT ON COLUMN stock.source_rps_pulse_t0_change_facts.recorded_at IS
    '该衍生关系事实提交到PG的记录时刻，不是行情可用时钟。';

ALTER TABLE stock.source_rps_pulse_t0_change_facts OWNER TO stock_a;
