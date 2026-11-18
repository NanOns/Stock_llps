-- R08/业务07：保存已存LAUNCH_CONFIRM原始事实的正式事件准入与T0冻结线。
CREATE TABLE stock.source_d0_formal_admission_day_facts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    raw_fact_id bigint NOT NULL REFERENCES stock.source_d0_scenario_day_facts(id),
    seed_fact_id bigint REFERENCES stock.source_pool_seed_day_facts(id),
    source_package_id bigint NOT NULL REFERENCES stock.source_packages(source_package_id),
    source_capture_id bigint NOT NULL REFERENCES stock.source_captures(capture_id),
    target_session date NOT NULL,
    source_symbol text NOT NULL,
    scenario text NOT NULL CHECK (scenario = 'LAUNCH_CONFIRM'),
    raw_role text NOT NULL CHECK (raw_role IN ('FORMAL', 'DIAGNOSTIC')),
    raw_state text NOT NULL CHECK (raw_state IN ('TRUE', 'FALSE', 'UNKNOWN')),
    raw_reason text NOT NULL,
    raw_producer_version text NOT NULL,
    algorithm_contract text NOT NULL,
    session_contract text NOT NULL,
    history_window_start date NOT NULL,
    history_session_count smallint NOT NULL CHECK (history_session_count BETWEEN 1 AND 75),
    source_count integer NOT NULL CHECK (source_count > 0),
    calendar_id text NOT NULL,
    calendar_revision text NOT NULL,
    calendar_sha256 character(64) NOT NULL CHECK (calendar_sha256 ~ '^[0-9a-f]{64}$'),
    source_time_quality text NOT NULL,
    source_clock_role text NOT NULL,
    source_available_at timestamp with time zone,
    capture_started_at timestamp with time zone NOT NULL,
    capture_validated_at timestamp with time zone NOT NULL,
    profile_revision text,
    seed_producer_version text,
    identity_scope text NOT NULL CHECK (identity_scope = 'SOURCE_SYMBOL_ONLY'),
    research_mode text NOT NULL CHECK (research_mode = 'NON_PIT_EXPLORATORY'),
    price_coordinate text NOT NULL CHECK (price_coordinate = 'RAW_UNADJUSTED'),
    admission_contract text NOT NULL CHECK (admission_contract = 'STOCK_A_D0_EVENT_ADMISSION_R1'),
    admission_status text NOT NULL CHECK (admission_status IN ('READY', 'BLOCKED', 'NOT_READY', 'NOT_REQUIRED')),
    admission_reason text NOT NULL,
    research_universe_state text NOT NULL CHECK (research_universe_state IN ('TRUE', 'FALSE', 'UNKNOWN', 'NOT_EVALUATED')),
    research_universe_reason text NOT NULL,
    actual_quote_state text NOT NULL CHECK (actual_quote_state IN ('TRUE', 'FALSE', 'UNKNOWN', 'NOT_EVALUATED')),
    actual_quote_reason text NOT NULL,
    coordinate_window_state text NOT NULL CHECK (coordinate_window_state IN ('TRUE', 'FALSE', 'UNKNOWN', 'NOT_EVALUATED')),
    coordinate_window_reason text NOT NULL,
    d0_liquidity_state text NOT NULL CHECK (d0_liquidity_state IN ('TRUE', 'FALSE', 'UNKNOWN', 'NOT_EVALUATED')),
    d0_liquidity_reason text NOT NULL,
    non_core_damage_state text NOT NULL CHECK (non_core_damage_state IN ('TRUE', 'FALSE', 'UNKNOWN', 'NOT_EVALUATED')),
    non_core_damage_reason text NOT NULL,
    non_extreme_extension_state text NOT NULL CHECK (non_extreme_extension_state IN ('TRUE', 'FALSE', 'UNKNOWN', 'NOT_EVALUATED')),
    non_extreme_extension_reason text NOT NULL,
    non_first_day_damage_state text NOT NULL CHECK (non_first_day_damage_state IN ('TRUE', 'FALSE', 'UNKNOWN', 'NOT_EVALUATED')),
    non_first_day_damage_reason text NOT NULL,
    non_severe_drop_state text NOT NULL CHECK (non_severe_drop_state IN ('TRUE', 'FALSE', 'UNKNOWN', 'NOT_EVALUATED')),
    non_severe_drop_reason text NOT NULL,
    reason_codes text[] NOT NULL,
    event_id text,
    failure_level_numerator numeric,
    failure_level_denominator numeric,
    failure_coordinate text,
    failure_source text,
    failure_window_start date,
    failure_window_session_count smallint,
    failure_source_event_ref text,
    effective_from_session date,
    CONSTRAINT source_d0_formal_admission_symbol_ck
        CHECK (source_symbol ~ '^(SH|SZ)\.[0-9]{6}$'),
    CONSTRAINT source_d0_formal_admission_capture_ck
        CHECK (capture_validated_at >= capture_started_at),
    CONSTRAINT source_d0_formal_admission_raw_status_ck
        CHECK (
            (raw_state = 'FALSE' AND admission_status = 'NOT_REQUIRED')
            OR (raw_state <> 'FALSE' AND admission_status <> 'NOT_REQUIRED')
        ),
    CONSTRAINT source_d0_formal_admission_line_ck
        CHECK (
            (
                admission_status = 'READY'
                AND raw_role = 'FORMAL'
                AND raw_state = 'TRUE'
                AND failure_level_numerator IS NOT NULL
                AND failure_level_denominator IS NOT NULL
                AND failure_level_denominator > 0
                AND failure_coordinate = 'RAW_UNADJUSTED'
                AND failure_source = 'T0_PHC20'
                AND failure_window_start IS NOT NULL
                AND failure_window_session_count = 20
                AND failure_source_event_ref IS NOT NULL
                AND event_id IS NOT NULL
                AND effective_from_session > target_session
                AND research_universe_state = 'TRUE'
                AND actual_quote_state = 'TRUE'
                AND coordinate_window_state = 'TRUE'
                AND d0_liquidity_state = 'TRUE'
                AND non_core_damage_state = 'TRUE'
                AND non_extreme_extension_state = 'TRUE'
                AND non_first_day_damage_state = 'TRUE'
                AND non_severe_drop_state = 'TRUE'
            )
            OR
            (
                admission_status <> 'READY'
                AND event_id IS NULL
                AND failure_level_numerator IS NULL
                AND failure_level_denominator IS NULL
                AND failure_coordinate IS NULL
                AND failure_source IS NULL
                AND failure_window_start IS NULL
                AND failure_window_session_count IS NULL
                AND failure_source_event_ref IS NULL
                AND effective_from_session IS NULL
            )
        ),
    CONSTRAINT source_d0_formal_admission_unique_input UNIQUE (
        target_session,
        source_package_id,
        source_capture_id,
        source_symbol,
        scenario,
        raw_producer_version,
        algorithm_contract,
        session_contract,
        calendar_revision
    )
);

CREATE INDEX source_d0_formal_admission_day_status_idx
    ON stock.source_d0_formal_admission_day_facts
    (target_session, source_package_id, source_capture_id, admission_status, source_symbol);
CREATE INDEX source_d0_formal_admission_symbol_day_idx
    ON stock.source_d0_formal_admission_day_facts
    (source_symbol, target_session DESC, source_package_id, admission_status);

COMMENT ON TABLE stock.source_d0_formal_admission_day_facts IS '按已存LAUNCH_CONFIRM D0原始事实保存正式事件准入三态、公共门原因及仅READY事件的出生PHC20冻结失效线；不代表官方证券身份、严格PIT或交易资格。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.id IS '正式准入事实行的数据库代理编号。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.raw_fact_id IS '外键引用对应的已存D0场景原始事实行，不回写原始三态。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.seed_fact_id IS '外键引用本次准入读取的唯一Core/Profile/Seed事实；来源缺失或歧义时为空。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.source_package_id IS '读取的数字TDX来源包业务编号。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.source_capture_id IS '目标交易会话对应的来源捕获业务编号。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.target_session IS 'D0正式事件准入评估的目标官方交易会话。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.source_symbol IS '来源证券代码；仅表示来源符号，不是稳定security_id。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.scenario IS '本表正式准入场景，当前固定为LAUNCH_CONFIRM。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.raw_role IS '本次准入消费的原始场景角色：FORMAL正式评估或DIAGNOSTIC诊断；不改变原始事实。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.raw_state IS '已存LAUNCH_CONFIRM原始谓词归约三态：TRUE、FALSE或UNKNOWN。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.raw_reason IS '原始场景主原因码；保留源事实语义，不由准入门覆盖。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.raw_producer_version IS '生成原始场景事实的生产者版本。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.algorithm_contract IS '原始场景使用的算法合同标识。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.session_contract IS '原始场景使用的官方会话窗口合同标识。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.history_window_start IS '原始场景严格历史窗口的起始官方交易会话。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.history_session_count IS '原始场景窗口实际请求的官方交易会话数，单位为会话。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.source_count IS '目标日闭合来源池的唯一来源股票数，也是准入事实分区的预期总行数。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.calendar_id IS '用于验证原始场景窗口的官方交易日历标识。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.calendar_revision IS '官方交易日历修订键。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.calendar_sha256 IS '对应官方交易日历资源的SHA-256摘要。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.source_time_quality IS '来源时间质量枚举；NON_PIT_OBSERVED表示后验捕获，不表示历史可用时钟。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.source_clock_role IS '来源捕获时钟的实际语义角色。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.source_available_at IS '来源实际可用时刻，含时区；来源未提供时保持空值。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.capture_started_at IS '本机开始读取目标来源会话的实际时刻，含时区。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.capture_validated_at IS '本机完成目标来源读取与验证的实际时刻，含时区。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.profile_revision IS '本行所引用Core/Profile画像的精确修订键；未能唯一匹配时为空。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.seed_producer_version IS '生成所引用Core/Profile/Seed事实的生产者版本。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.identity_scope IS '证券身份范围；当前明确为SOURCE_SYMBOL_ONLY来源代码级。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.research_mode IS '研究时间语义；当前固定为NON_PIT_EXPLORATORY后验探索。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.price_coordinate IS '价格坐标；当前固定为RAW_UNADJUSTED原生未复权坐标。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.admission_contract IS '正式事件准入纯核合同版本。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.admission_status IS '准入结果：READY、BLOCKED、NOT_READY或NOT_REQUIRED。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.admission_reason IS '准入纯核返回的主状态原因码。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.research_universe_state IS '官方ResearchUniverse公共门三态；缺权威范围证据时为UNKNOWN。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.research_universe_reason IS 'ResearchUniverse门的可复核原因码。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.actual_quote_state IS '目标会话实际报价存在性公共门三态。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.actual_quote_reason IS '实际报价存在性门的可复核原因码。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.coordinate_window_state IS '价格坐标、来源捕获和严格前20官方会话窗口一致性公共门三态。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.coordinate_window_reason IS '坐标、捕获或历史窗口门的可复核原因码。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.d0_liquidity_state IS 'D0前20会话金额中位数阈值公共门三态；金额单位未核实则为UNKNOWN。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.d0_liquidity_reason IS 'D0流动性门的可复核原因码。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.non_core_damage_state IS '非core_damage公共门三态；TRUE表示已证没有Core价格损伤。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.non_core_damage_reason IS 'Core损伤公共门的可复核原因码。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.non_extreme_extension_state IS '非EXTREME extension公共门三态。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.non_extreme_extension_reason IS 'Profile extension公共门的可复核原因码。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.non_first_day_damage_state IS '非首日价格损伤公共门三态；损伤定义为C/MA20<0.97。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.non_first_day_damage_reason IS '首日价格损伤门的可复核原因码。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.non_severe_drop_state IS '非severe_drop公共门三态；LAUNCH原始ret1为正时该严重下跌门已证为FALSE。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.non_severe_drop_reason IS '严重下跌公共门的可复核原因码。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.reason_codes IS '本次准入全部阻断或未就绪原因码数组，不包含未经验证的解释文本。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.event_id IS '仅READY正式事件的稳定可读业务键；其它状态为空。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.failure_level_numerator IS '仅READY事件冻结失效线的精确有理数分子，价格单位为RAW_UNADJUSTED。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.failure_level_denominator IS '仅READY事件冻结失效线的正整数精确有理数分母。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.failure_coordinate IS '仅READY事件冻结线的价格坐标，当前为RAW_UNADJUSTED。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.failure_source IS '仅READY事件冻结线来源，本场景固定为T0_PHC20。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.failure_window_start IS '仅READY事件冻结线所用严格前20会话窗口起始日。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.failure_window_session_count IS '仅READY事件冻结线使用的严格历史会话数，固定为20。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.failure_source_event_ref IS '仅READY冻结线引用的原始场景事实业务键。';
COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.effective_from_session IS '仅READY冻结线开始可用于后继失效检验的下一官方交易会话；T0当日不生效。';

ALTER TABLE stock.source_d0_formal_admission_day_facts OWNER TO stock_a;
