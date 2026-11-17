-- 已存来源级研究登记的逐官方会话状态；只表达NON_PIT来源观察段。
CREATE TABLE stock.source_research_observation_segment_days (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_enrollment_id bigint NOT NULL
        REFERENCES stock.source_research_enrollments(id),
    source_package_id bigint NOT NULL
        REFERENCES stock.source_packages(source_package_id),
    source_symbol text NOT NULL,
    rule_id text NOT NULL,
    rule_version text NOT NULL,
    t0_session date NOT NULL,
    anchor_rule_fact_id bigint NOT NULL
        REFERENCES stock.source_rule_evaluations(id),
    anchor_capture_id bigint NOT NULL
        REFERENCES stock.source_captures(capture_id),
    enrollment_kind text NOT NULL CHECK (enrollment_kind IN (
        'ENTRY', 'FIRST_OBSERVED_LEFT_CENSORED', 'DAILY_LANDMARK', 'REENTRY'
    )),
    parent_enrollment_id bigint
        REFERENCES stock.source_research_enrollments(id),
    observation_session date NOT NULL,
    previous_session date,
    target_capture_id bigint REFERENCES stock.source_captures(capture_id),
    evidence_rule_fact_id bigint REFERENCES stock.source_rule_evaluations(id),
    linked_enrollment_id bigint
        REFERENCES stock.source_research_enrollments(id),
    observation_state text CHECK (
        observation_state IS NULL
        OR observation_state IN ('TRUE', 'FALSE', 'UNKNOWN')
    ),
    lifecycle text NOT NULL CHECK (
        lifecycle IN ('ACTIVE', 'PAUSED', 'EXITED', 'EXITED_UNKNOWN')
    ),
    event_code text NOT NULL CHECK (event_code IN (
        'ENTRY', 'FIRST_OBSERVED_LEFT_CENSORED', 'DAILY_LANDMARK', 'REENTRY',
        'CONTINUES', 'EXIT_PROVED', 'PAUSED_UNKNOWN', 'LEFT_CENSORED',
        'NO_MATCH', 'UNKNOWN', 'REENTRY_NOT_LINKED'
    )),
    quality_status text NOT NULL CHECK (
        quality_status IN ('OBSERVED', 'UNKNOWN', 'INCOMPLETE', 'NOT_COMPUTED')
    ),
    comparability_status text NOT NULL CHECK (
        comparability_status IN ('INITIAL', 'COMPARABLE', 'INCOMPARABLE', 'INCOMPLETE')
    ),
    reason_code text NOT NULL,
    profile_revision text,
    history_session_count smallint,
    d1_history_session_count smallint,
    event_window_start date,
    calendar_revision text,
    source_time_quality text,
    source_clock_role text,
    scope text NOT NULL CHECK (scope = 'NON_PIT_EXPLORATORY'),
    price_coordinate text NOT NULL CHECK (price_coordinate = 'RAW_UNADJUSTED'),
    identity_status text NOT NULL CHECK (identity_status = 'SOURCE_SYMBOL_ONLY'),
    producer_contract text NOT NULL CHECK (
        producer_contract = 'SOURCE_RESEARCH_OBSERVATION_SEGMENTS_R1'
    ),
    producer_version text NOT NULL CHECK (producer_version = 'R1'),
    UNIQUE (source_enrollment_id, observation_session),
    CHECK (t0_session <= observation_session),
    CHECK (previous_session IS NULL OR previous_session < observation_session),
    CHECK (parent_enrollment_id IS DISTINCT FROM source_enrollment_id),
    CHECK ((observation_state IS NULL) = (evidence_rule_fact_id IS NULL)),
    CHECK (quality_status <> 'OBSERVED' OR observation_state IS NOT NULL),
    CHECK (
        (enrollment_kind = 'REENTRY' AND parent_enrollment_id IS NOT NULL)
        OR (enrollment_kind <> 'REENTRY' AND parent_enrollment_id IS NULL)
    )
);

CREATE INDEX source_research_observation_segment_days_page_idx
    ON stock.source_research_observation_segment_days
       (source_package_id, rule_id, observation_session, source_enrollment_id);

COMMENT ON TABLE stock.source_research_observation_segment_days IS
    '已存来源级研究登记在官方会话上的状态推进事实；不代表正式Focus Episode、PIT或交易状态。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.id IS
    '逐会话观察事实数字主键。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.source_enrollment_id IS
    '现存来源级登记ID；每个登记与观察会话唯一对应一行。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.source_package_id IS
    '来源数字包ID，限定来源原件版本。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.source_symbol IS
    '来源代码；身份语义为SOURCE_SYMBOL_ONLY，不等同稳定证券实体。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.rule_id IS
    '现存冻结来源规则标识。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.rule_version IS
    '产生观察事实的来源规则版本。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.t0_session IS
    '登记锚点官方交易会话日期。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.anchor_rule_fact_id IS
    'T0已保存规则事实ID，证明该观察段的原始登记依据。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.anchor_capture_id IS
    'T0来源捕获ID；不与后继会话捕获混用。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.enrollment_kind IS
    '原登记类别：已证入组、左截断、每日观察点或带直接父段的已证重入。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.parent_enrollment_id IS
    'REENTRY原登记引用的直接父观察段ID；其它类别为空。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.observation_session IS
    '本行观察状态所属官方交易会话日期，不能晚于离线研究截止。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.previous_session IS
    '状态机要求的紧邻前一官方会话日期；首个T0行为空。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.target_capture_id IS
    '本观察会话的来源捕获ID；输入范围不闭合时为空。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.evidence_rule_fact_id IS
    '本会话实际读取的规则事实ID；无已存事实时为空，不把缺失改为FALSE。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.linked_enrollment_id IS
    '状态推进命中且父段相符的REENTRY子登记ID；未证明合法链接时为空。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.observation_state IS
    '本会话规则三态TRUE/FALSE/UNKNOWN；无已保存事实时为空。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.lifecycle IS
    '复用状态机推进后的来源观察段生命周期：ACTIVE、PAUSED、EXITED或EXITED_UNKNOWN。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.event_code IS
    '本会话发生的状态事件；包含持续、暂停、已证退出、左截断和重入。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.quality_status IS
    '事实质量状态：OBSERVED、UNKNOWN、INCOMPLETE或NOT_COMPUTED。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.comparability_status IS
    '与前会话的可比状态；INITIAL表示登记T0，INCOMPARABLE不伪造状态转移。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.reason_code IS
    '状态或质量的明确原因码；不以无事实推断退出。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.profile_revision IS
    '本观察会话使用的精确画像修订；窗口数量变化仍单独记录。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.history_session_count IS
    '画像输入的官方会话窗口数，单位为会话；未知时为空。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.d1_history_session_count IS
    'D1输入的官方会话窗口数，单位为会话；未知时为空。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.event_window_start IS
    '本观察规则事件窗口的起始官方会话日期。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.calendar_revision IS
    '本观察事实绑定的官方交易日历修订。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.source_time_quality IS
    '该观察会话来源时钟质量；不代表历史PIT。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.source_clock_role IS
    '该观察会话来源捕获时钟角色。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.scope IS
    '研究范围固定为NON_PIT_EXPLORATORY。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.price_coordinate IS
    '价格坐标固定为RAW_UNADJUSTED，表示未复权原始价格语义。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.identity_status IS
    '身份能力固定为SOURCE_SYMBOL_ONLY，不声称证券稳定实体已证明。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.producer_contract IS
    '离线来源观察段生产合同标识。';
COMMENT ON COLUMN stock.source_research_observation_segment_days.producer_version IS
    '离线来源观察段生产器版本。';
