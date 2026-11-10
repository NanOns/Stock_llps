-- R06：持久化现役D0相邻三态与D1五类来源结构研究日事实。
CREATE TABLE stock.source_d0_launch_day_facts (
    namespace_id uuid NOT NULL,
    before_session date NOT NULL,
    target_session date NOT NULL,
    source_symbol text NOT NULL,
    market text NOT NULL CHECK (market IN ('SH', 'SZ')),
    package_sha256 char(64) NOT NULL CHECK (package_sha256 ~ '^[0-9a-f]{64}$'),
    source_content_id char(64) NOT NULL CHECK (source_content_id ~ '^[0-9a-f]{64}$'),
    before_capture_id uuid NOT NULL,
    target_capture_id uuid NOT NULL,
    producer_version text NOT NULL,
    algorithm_contract text NOT NULL,
    session_contract text NOT NULL,
    before_window_start date NOT NULL,
    target_window_start date NOT NULL,
    before_source_count integer NOT NULL CHECK (before_source_count >= 0),
    target_source_count integer NOT NULL CHECK (target_source_count > 0),
    union_source_count integer NOT NULL CHECK (union_source_count >= target_source_count),
    before_state text CHECK (before_state IN ('TRUE', 'FALSE', 'UNKNOWN')),
    before_reason text,
    after_state text CHECK (after_state IN ('TRUE', 'FALSE', 'UNKNOWN')),
    after_reason text,
    transition text NOT NULL,
    source_time_quality text NOT NULL,
    source_clock_role text NOT NULL,
    source_available_at timestamptz,
    calendar_id text NOT NULL,
    calendar_revision text NOT NULL,
    calendar_sha256 char(64) NOT NULL CHECK (calendar_sha256 ~ '^[0-9a-f]{64}$'),
    research_mode text NOT NULL CHECK (research_mode = 'NON_PIT_RETROSPECTIVE'),
    price_coordinate text NOT NULL CHECK (price_coordinate = 'RAW_UNADJUSTED'),
    PRIMARY KEY (
        namespace_id, target_session, source_symbol, package_sha256,
        before_capture_id, target_capture_id, producer_version, algorithm_contract,
        session_contract, calendar_revision, calendar_sha256
    ),
    FOREIGN KEY (namespace_id, before_capture_id)
        REFERENCES stock.source_captures(namespace_id, capture_id),
    FOREIGN KEY (namespace_id, target_capture_id)
        REFERENCES stock.source_captures(namespace_id, capture_id),
    FOREIGN KEY (namespace_id, source_content_id)
        REFERENCES stock.source_contents(namespace_id, content_id),
    CHECK (before_session < target_session),
    CHECK (before_window_start <= before_session),
    CHECK (target_window_start <= target_session),
    CHECK (before_state IS NOT NULL OR before_reason IS NULL),
    CHECK (after_state IS NOT NULL OR after_reason IS NULL)
);
CREATE INDEX source_d0_launch_day_lookup_idx
    ON stock.source_d0_launch_day_facts
    (namespace_id, target_session, package_sha256, target_capture_id, source_symbol);

CREATE TABLE stock.source_d1_structure_day_facts (
    namespace_id uuid NOT NULL,
    before_session date NOT NULL,
    target_session date NOT NULL,
    source_symbol text NOT NULL,
    market text NOT NULL CHECK (market IN ('SH', 'SZ')),
    board text NOT NULL CHECK (board IN ('MAIN_BOARD', 'GEM')),
    structure_type text NOT NULL CHECK (structure_type IN (
        'PIVOT_LOW', 'PIVOT_HIGH', 'RANGE_FLOOR', 'RANGE_CEILING', 'GAP_ZONE'
    )),
    package_sha256 char(64) NOT NULL CHECK (package_sha256 ~ '^[0-9a-f]{64}$'),
    source_content_id char(64) NOT NULL CHECK (source_content_id ~ '^[0-9a-f]{64}$'),
    target_capture_id uuid NOT NULL,
    d1_history_session_count smallint NOT NULL CHECK (d1_history_session_count > 0),
    history_window_start date NOT NULL,
    history_window_end date NOT NULL,
    calendar_id text NOT NULL,
    calendar_revision text NOT NULL,
    calendar_sha256 char(64) NOT NULL CHECK (calendar_sha256 ~ '^[0-9a-f]{64}$'),
    producer_contract text NOT NULL,
    producer_version text NOT NULL,
    material_change_contract text NOT NULL,
    source_time_quality text NOT NULL,
    source_clock_role text NOT NULL,
    source_available_at timestamptz,
    research_mode text NOT NULL CHECK (research_mode = 'NON_PIT_RETROSPECTIVE'),
    price_coordinate text NOT NULL CHECK (price_coordinate = 'RAW_UNADJUSTED'),
    history_row_count integer NOT NULL CHECK (history_row_count > 0),
    target_bar_status text NOT NULL,
    target_bar_reason text,
    target_price_quality text NOT NULL,
    target_price_reason text,
    generation_state text NOT NULL CHECK (generation_state IN ('GENERATED', 'NOT_GENERATED', 'UNKNOWN')),
    generation_reason text,
    anchor_id text,
    anchor_created_session date,
    anchor_available_from_session date,
    anchor_test_from_session date,
    coordinate_id text,
    relation text CHECK (relation IN ('SUPPORT', 'RESISTANCE')),
    lower_numerator numeric(100, 0),
    lower_denominator numeric(100, 0),
    upper_numerator numeric(100, 0),
    upper_denominator numeric(100, 0),
    source_event_id text,
    source_ref text,
    historical_anchor_count integer NOT NULL CHECK (historical_anchor_count >= 0),
    material_change_anchor_count integer NOT NULL CHECK (material_change_anchor_count >= 0),
    count_only_anchor_count integer NOT NULL CHECK (count_only_anchor_count >= 0),
    unchanged_anchor_count integer NOT NULL CHECK (unchanged_anchor_count >= 0),
    unknown_anchor_count integer NOT NULL CHECK (unknown_anchor_count >= 0),
    not_comparable_anchor_count integer NOT NULL CHECK (not_comparable_anchor_count >= 0),
    not_evaluable_anchor_count integer NOT NULL CHECK (not_evaluable_anchor_count >= 0),
    not_applicable_anchor_count integer NOT NULL CHECK (not_applicable_anchor_count >= 0),
    persisted_detail_count integer NOT NULL CHECK (persisted_detail_count >= 0),
    PRIMARY KEY (
        namespace_id, target_session, source_symbol, structure_type, package_sha256,
        target_capture_id, d1_history_session_count, calendar_revision, calendar_sha256,
        producer_contract, producer_version, material_change_contract
    ),
    FOREIGN KEY (namespace_id, target_capture_id)
        REFERENCES stock.source_captures(namespace_id, capture_id),
    FOREIGN KEY (namespace_id, source_content_id)
        REFERENCES stock.source_contents(namespace_id, content_id),
    CHECK (before_session < target_session),
    CHECK (history_window_start <= before_session AND history_window_end = target_session),
    CHECK (
        material_change_anchor_count + count_only_anchor_count + unchanged_anchor_count
        + unknown_anchor_count + not_comparable_anchor_count + not_evaluable_anchor_count
        + not_applicable_anchor_count = historical_anchor_count
    ),
    CHECK (
        (generation_state = 'GENERATED' AND anchor_id IS NOT NULL
         AND anchor_created_session IS NOT NULL AND relation IS NOT NULL)
        OR (generation_state <> 'GENERATED' AND anchor_id IS NULL
            AND anchor_created_session IS NULL AND relation IS NULL
            AND lower_numerator IS NULL AND lower_denominator IS NULL
            AND upper_numerator IS NULL AND upper_denominator IS NULL)
    ),
    CHECK ((lower_numerator IS NULL) = (lower_denominator IS NULL)),
    CHECK ((upper_numerator IS NULL) = (upper_denominator IS NULL)),
    CHECK (lower_denominator IS NULL OR lower_denominator > 0),
    CHECK (upper_denominator IS NULL OR upper_denominator > 0)
);
CREATE INDEX source_d1_structure_day_lookup_idx
    ON stock.source_d1_structure_day_facts
    (namespace_id, target_session, package_sha256, target_capture_id, structure_type, source_symbol);

CREATE TABLE stock.source_d1_material_anchor_day_facts (
    namespace_id uuid NOT NULL,
    before_session date NOT NULL,
    target_session date NOT NULL,
    source_symbol text NOT NULL,
    structure_type text NOT NULL CHECK (structure_type IN (
        'PIVOT_LOW', 'PIVOT_HIGH', 'RANGE_FLOOR', 'RANGE_CEILING', 'GAP_ZONE'
    )),
    package_sha256 char(64) NOT NULL CHECK (package_sha256 ~ '^[0-9a-f]{64}$'),
    target_capture_id uuid NOT NULL,
    d1_history_session_count smallint NOT NULL CHECK (d1_history_session_count > 0),
    calendar_revision text NOT NULL,
    calendar_sha256 char(64) NOT NULL CHECK (calendar_sha256 ~ '^[0-9a-f]{64}$'),
    producer_contract text NOT NULL,
    producer_version text NOT NULL,
    material_change_contract text NOT NULL,
    anchor_id text NOT NULL,
    created_session date NOT NULL,
    previous_session date NOT NULL,
    coordinate_id text,
    relation text NOT NULL CHECK (relation IN ('SUPPORT', 'RESISTANCE')),
    lower_numerator numeric(100, 0),
    lower_denominator numeric(100, 0),
    upper_numerator numeric(100, 0),
    upper_denominator numeric(100, 0),
    birth_status text NOT NULL CHECK (birth_status IN ('AVAILABLE', 'UNKNOWN')),
    birth_capture_id uuid,
    birth_entry_sha256 char(64),
    previous_capture_id uuid,
    previous_entry_sha256 char(64),
    target_entry_sha256 char(64),
    official_sessions_adjacent boolean NOT NULL CHECK (official_sessions_adjacent),
    anchor_birth_before_previous_session boolean NOT NULL,
    change_classification text NOT NULL,
    material_change_classification text NOT NULL,
    support_change_classification text NOT NULL,
    gap_fill_classification text NOT NULL,
    change_reasons text[] NOT NULL,
    material_change_reasons text[] NOT NULL,
    quality_reasons text[] NOT NULL,
    before_state text,
    before_quality text,
    before_acceptance text,
    before_freshness text,
    before_breach_streak integer CHECK (before_breach_streak IS NULL OR before_breach_streak >= 0),
    before_acceptance_streak integer CHECK (before_acceptance_streak IS NULL OR before_acceptance_streak >= 0),
    before_test_count integer CHECK (before_test_count IS NULL OR before_test_count >= 0),
    before_separated_sessions integer CHECK (before_separated_sessions IS NULL OR before_separated_sessions >= 0),
    before_retest_qualified boolean,
    after_state text,
    after_quality text,
    after_acceptance text,
    after_freshness text,
    after_breach_streak integer CHECK (after_breach_streak IS NULL OR after_breach_streak >= 0),
    after_acceptance_streak integer CHECK (after_acceptance_streak IS NULL OR after_acceptance_streak >= 0),
    after_test_count integer CHECK (after_test_count IS NULL OR after_test_count >= 0),
    after_separated_sessions integer CHECK (after_separated_sessions IS NULL OR after_separated_sessions >= 0),
    after_retest_qualified boolean,
    gap_before_state text,
    gap_before_quality text,
    gap_before_reason text,
    gap_after_state text,
    gap_after_quality text,
    gap_after_reason text,
    PRIMARY KEY (
        namespace_id, target_session, source_symbol, structure_type, package_sha256,
        target_capture_id, d1_history_session_count, calendar_revision, calendar_sha256,
        producer_contract, producer_version, material_change_contract,
        anchor_id, created_session
    ),
    FOREIGN KEY (
        namespace_id, target_session, source_symbol, structure_type, package_sha256,
        target_capture_id, d1_history_session_count, calendar_revision, calendar_sha256,
        producer_contract, producer_version, material_change_contract
    ) REFERENCES stock.source_d1_structure_day_facts (
        namespace_id, target_session, source_symbol, structure_type, package_sha256,
        target_capture_id, d1_history_session_count, calendar_revision, calendar_sha256,
        producer_contract, producer_version, material_change_contract
    ),
    FOREIGN KEY (namespace_id, target_capture_id)
        REFERENCES stock.source_captures(namespace_id, capture_id),
    FOREIGN KEY (namespace_id, birth_capture_id)
        REFERENCES stock.source_captures(namespace_id, capture_id),
    FOREIGN KEY (namespace_id, previous_capture_id)
        REFERENCES stock.source_captures(namespace_id, capture_id),
    CHECK (before_session < target_session AND previous_session = before_session),
    CHECK ((lower_numerator IS NULL) = (lower_denominator IS NULL)),
    CHECK ((upper_numerator IS NULL) = (upper_denominator IS NULL)),
    CHECK (lower_denominator IS NULL OR lower_denominator > 0),
    CHECK (upper_denominator IS NULL OR upper_denominator > 0),
    CHECK (birth_entry_sha256 IS NULL OR birth_entry_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (previous_entry_sha256 IS NULL OR previous_entry_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (target_entry_sha256 IS NULL OR target_entry_sha256 ~ '^[0-9a-f]{64}$')
);
CREATE INDEX source_d1_material_anchor_day_lookup_idx
    ON stock.source_d1_material_anchor_day_facts
    (namespace_id, target_session, package_sha256, target_capture_id, structure_type, source_symbol, created_session);

COMMENT ON TABLE stock.source_d0_launch_day_facts IS '按来源股票保存现役D0启动场景相邻官方会话三态、转移与原因；NON_PIT后验研究事实，不代表证券身份或交易资格。';
COMMENT ON COLUMN stock.source_d0_launch_day_facts.namespace_id IS '来源数据命名空间UUID。';
COMMENT ON COLUMN stock.source_d0_launch_day_facts.before_session IS 'D0前态官方交易会话。';
COMMENT ON COLUMN stock.source_d0_launch_day_facts.target_session IS 'D0目标官方交易会话。';
COMMENT ON COLUMN stock.source_d0_launch_day_facts.source_symbol IS 'TDX来源股票代码；不是稳定security_id。';
COMMENT ON COLUMN stock.source_d0_launch_day_facts.market IS '来源市场SH或SZ，须与代码前缀一致。';
COMMENT ON COLUMN stock.source_d0_launch_day_facts.package_sha256 IS '本次D0计算绑定的来源包SHA-256。';
COMMENT ON COLUMN stock.source_d0_launch_day_facts.source_content_id IS '规范来源内容摘要，引用stock.source_contents。';
COMMENT ON COLUMN stock.source_d0_launch_day_facts.before_capture_id IS '前态Raw来源捕获UUID。';
COMMENT ON COLUMN stock.source_d0_launch_day_facts.target_capture_id IS '目标Raw来源捕获UUID。';
COMMENT ON COLUMN stock.source_d0_launch_day_facts.producer_version IS '现役D0来源计算器版本。';
COMMENT ON COLUMN stock.source_d0_launch_day_facts.algorithm_contract IS '现役D0算法合同标识；本表不改变其公式。';
COMMENT ON COLUMN stock.source_d0_launch_day_facts.session_contract IS '现役D0会话与窗口合同标识。';
COMMENT ON COLUMN stock.source_d0_launch_day_facts.before_window_start IS '前态D0读取Raw的首个官方会话。';
COMMENT ON COLUMN stock.source_d0_launch_day_facts.target_window_start IS '目标D0读取Raw的首个官方会话。';
COMMENT ON COLUMN stock.source_d0_launch_day_facts.before_source_count IS '前态来源总体分母，单位为来源股票只数。';
COMMENT ON COLUMN stock.source_d0_launch_day_facts.target_source_count IS '目标来源总体分母，单位为来源股票只数。';
COMMENT ON COLUMN stock.source_d0_launch_day_facts.union_source_count IS '前后来源键并集分母，单位为来源股票只数。';
COMMENT ON COLUMN stock.source_d0_launch_day_facts.before_state IS 'D0前态TRUE、FALSE或UNKNOWN；来源键不存在时为空。';
COMMENT ON COLUMN stock.source_d0_launch_day_facts.before_reason IS 'D0前态主原因码；前态缺失时为空。';
COMMENT ON COLUMN stock.source_d0_launch_day_facts.after_state IS 'D0目标态TRUE、FALSE或UNKNOWN；来源键不存在时为空。';
COMMENT ON COLUMN stock.source_d0_launch_day_facts.after_reason IS 'D0目标态主原因码；目标态缺失时为空。';
COMMENT ON COLUMN stock.source_d0_launch_day_facts.transition IS '现役D0前后状态转移枚举，包括未知状态变化。';
COMMENT ON COLUMN stock.source_d0_launch_day_facts.source_time_quality IS '来源可用时钟质量合同值。';
COMMENT ON COLUMN stock.source_d0_launch_day_facts.source_clock_role IS '来源时钟角色，如实际观察或后验研究时钟。';
COMMENT ON COLUMN stock.source_d0_launch_day_facts.source_available_at IS '来源数值可用时间，含时区；未知时为空。';
COMMENT ON COLUMN stock.source_d0_launch_day_facts.calendar_id IS '生成前后会话的正式交易日历标识。';
COMMENT ON COLUMN stock.source_d0_launch_day_facts.calendar_revision IS '正式交易日历修订键。';
COMMENT ON COLUMN stock.source_d0_launch_day_facts.calendar_sha256 IS '正式交易日历内容SHA-256。';
COMMENT ON COLUMN stock.source_d0_launch_day_facts.research_mode IS '固定NON_PIT_RETROSPECTIVE后验研究边界。';
COMMENT ON COLUMN stock.source_d0_launch_day_facts.price_coordinate IS 'D0输入价格坐标，固定RAW_UNADJUSTED原始未复权。';

COMMENT ON TABLE stock.source_d1_structure_day_facts IS '每个目标来源股票和五类D1结构的生成状态、目标质量、历史锚分类分母及typed坐标；后验NON_PIT研究事实。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.namespace_id IS '来源数据命名空间UUID。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.before_session IS 'D1相邻观察前态官方会话。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.target_session IS 'D1目标官方交易会话。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.source_symbol IS 'TDX来源股票代码，不代表稳定证券身份。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.market IS '来源市场SH或SZ。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.board IS '现役算法使用的板别MAIN_BOARD或GEM。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.structure_type IS '五类现役结构之一：PIVOT_LOW、PIVOT_HIGH、RANGE_FLOOR、RANGE_CEILING、GAP_ZONE。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.package_sha256 IS '本次D1计算绑定的来源包SHA-256。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.source_content_id IS '目标来源内容摘要，引用stock.source_contents。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.target_capture_id IS '目标Raw来源捕获UUID。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.d1_history_session_count IS 'D1历史窗口官方会话数，由现役算法合同确定。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.history_window_start IS 'D1历史窗口首个官方会话。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.history_window_end IS 'D1历史窗口末个官方会话，等于目标日。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.calendar_id IS 'D1使用的正式交易日历标识。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.calendar_revision IS 'D1使用的正式交易日历修订。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.calendar_sha256 IS '正式交易日历内容SHA-256。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.producer_contract IS '现役D1结构计算合同标识。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.producer_version IS '现役D1生产者版本标识。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.material_change_contract IS '现役D1历史锚材料变化分类合同。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.source_time_quality IS '目标来源可用时钟质量合同值。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.source_clock_role IS '目标来源时钟角色。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.source_available_at IS '来源数值可用时间，含时区；未知时为空。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.research_mode IS '固定NON_PIT_RETROSPECTIVE后验研究边界。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.price_coordinate IS 'D1输入价格坐标，固定RAW_UNADJUSTED原始未复权。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.history_row_count IS '该股票本次历史窗口可用Raw行数，单位为日行。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.target_bar_status IS '目标日原始Bar读取状态枚举。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.target_bar_reason IS '目标日Bar缺失或降级原因码；正常时可为空。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.target_price_quality IS '目标日OHLC、量额等输入价格质量状态。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.target_price_reason IS '目标日价格质量原因码；正常时可为空。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.generation_state IS '单类结构生成状态GENERATED、NOT_GENERATED或UNKNOWN。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.generation_reason IS '结构未生成或未知的现役原因码；成功时可为空。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.anchor_id IS '生成结构锚的现役来源标识；未生成时为空。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.anchor_created_session IS '结构锚生成官方会话。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.anchor_available_from_session IS '结构锚最早可用于观察的官方会话。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.anchor_test_from_session IS '结构锚开始测试的官方会话；不适用时为空。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.coordinate_id IS '结构价格坐标标识；未知时为空。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.relation IS '结构关系SUPPORT支撑或RESISTANCE阻力。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.lower_numerator IS '结构精确下界整数分子；未生成或无下界时为空。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.lower_denominator IS '结构精确下界正整数分母；未生成或无下界时为空。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.upper_numerator IS '结构精确上界整数分子；未生成或无上界时为空。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.upper_denominator IS '结构精确上界正整数分母；未生成或无上界时为空。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.source_event_id IS '现役D1来源事件标识；无来源事件时为空。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.source_ref IS '现役D1证据引用键；无证据引用时为空。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.historical_anchor_count IS '该股该类历史锚总分母，单位为锚个数。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.material_change_anchor_count IS '材料变化锚数。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.count_only_anchor_count IS '仅诊断计数变化锚数。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.unchanged_anchor_count IS '已知未变化锚数。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.unknown_anchor_count IS '状态未知锚数。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.not_comparable_anchor_count IS '输入不具可比性的锚数。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.not_evaluable_anchor_count IS '输入不具可评价性的锚数。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.not_applicable_anchor_count IS '合同规定不适用的锚数。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.persisted_detail_count IS '与该股该结构类型关联的材料变化或质量锚明细行数。';

COMMENT ON TABLE stock.source_d1_material_anchor_day_facts IS '按现役D1锚保存材料变化和未知/不可比/不可评价质量明细，包含前后支撑与缺口填补typed字段；不是正式资格或交易事件。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.namespace_id IS '来源数据命名空间UUID。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.before_session IS '目标日前一个相邻官方观察会话。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.target_session IS 'D1目标官方交易会话。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.source_symbol IS 'TDX来源股票代码，不代表稳定证券身份。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.structure_type IS '五类现役D1来源结构类型。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.package_sha256 IS '本次D1计算绑定的来源包SHA-256。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.target_capture_id IS '目标Raw来源捕获UUID。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.d1_history_session_count IS 'D1历史窗口官方会话数，由现役算法合同确定。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.calendar_revision IS '正式交易日历修订键。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.calendar_sha256 IS '正式交易日历内容SHA-256。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.producer_contract IS '现役D1结构计算合同标识。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.producer_version IS '现役D1生产者版本标识。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.material_change_contract IS '现役D1历史锚材料变化分类合同。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.anchor_id IS '现役算法生成的来源锚标识。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.created_session IS '锚首次生成的官方会话。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.previous_session IS 'D1比较前态官方会话，须等于前一会话字段。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.coordinate_id IS '锚价格坐标标识；未知时为空。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.relation IS '锚关系SUPPORT支撑或RESISTANCE阻力。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.lower_numerator IS '锚精确下界整数分子；不存在时为空。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.lower_denominator IS '锚精确下界正整数分母；不存在时为空。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.upper_numerator IS '锚精确上界整数分子；不存在时为空。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.upper_denominator IS '锚精确上界正整数分母；不存在时为空。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.birth_status IS '锚出生事实AVAILABLE可用或UNKNOWN未知。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.birth_capture_id IS '锚出生来源Raw捕获UUID；未能证明时为空。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.birth_entry_sha256 IS '锚出生股票条目摘要；未能证明时为空。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.previous_capture_id IS '比较前态来源Raw捕获UUID；不可用时为空。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.previous_entry_sha256 IS '比较前态股票条目摘要；不可用时为空。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.target_entry_sha256 IS '目标态股票条目摘要；不可用时为空。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.official_sessions_adjacent IS '现役日历确认前后态为相邻官方会话，固定为真。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.anchor_birth_before_previous_session IS '锚出生是否早于比较前态会话。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.change_classification IS '现役D1逐锚前后字段变化分类。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.material_change_classification IS '现役材料变化分类，包括MATERIAL_CHANGE、COUNT_ONLY、UNCHANGED及质量状态。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.support_change_classification IS '现役支撑状态变化分类。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.gap_fill_classification IS '现役缺口填补变化分类。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.change_reasons IS '现役前后变化原因码数组，可为空数组。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.material_change_reasons IS '现役材料变化原因码数组，可为空数组。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.quality_reasons IS '未知、不可比或不可评价质量原因码数组，可为空数组。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.before_state IS '前态来源支撑状态；未观察时为空。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.before_quality IS '前态支撑状态质量；不可判定时为空。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.before_acceptance IS '前态锚接受枚举；不适用或未知时为空。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.before_freshness IS '前态观察新鲜度枚举；不可用时为空。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.before_breach_streak IS '前态连续破位计数，单位为会话数。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.before_acceptance_streak IS '前态连续接受计数，单位为会话数。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.before_test_count IS '前态有效测试累计数，单位为次。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.before_separated_sessions IS '前态有效测试间隔，单位为官方会话数。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.before_retest_qualified IS '前态重测合格布尔值；未知或不适用时为空。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.after_state IS '目标态来源支撑状态；未观察时为空。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.after_quality IS '目标态支撑状态质量；不可判定时为空。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.after_acceptance IS '目标态锚接受枚举；不适用或未知时为空。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.after_freshness IS '目标态观察新鲜度枚举；不可用时为空。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.after_breach_streak IS '目标态连续破位计数，单位为会话数。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.after_acceptance_streak IS '目标态连续接受计数，单位为会话数。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.after_test_count IS '目标态有效测试累计数，单位为次。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.after_separated_sessions IS '目标态有效测试间隔，单位为官方会话数。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.after_retest_qualified IS '目标态重测合格布尔值；未知或不适用时为空。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.gap_before_state IS '前态缺口填补阶段；不可用时为空。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.gap_before_quality IS '前态缺口填补质量；不可用时为空。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.gap_before_reason IS '前态缺口填补原因码；不可用时为空。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.gap_after_state IS '目标态缺口填补阶段；不可用时为空。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.gap_after_quality IS '目标态缺口填补质量；不可用时为空。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.gap_after_reason IS '目标态缺口填补原因码；不可用时为空。';
