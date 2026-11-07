-- 来源级研究事实，不代表正式证券身份、PIT、交易或正式事件。
CREATE TABLE stock.source_rule_evaluations (
    namespace_id uuid NOT NULL,
    target_session date NOT NULL,
    source_symbol text NOT NULL,
    package_sha256 char(64) NOT NULL,
    source_capture_id uuid NOT NULL,
    rule_id text NOT NULL,
    rule_version text NOT NULL,
    profile_revision text NOT NULL,
    history_session_count smallint NOT NULL CHECK (history_session_count BETWEEN 1 AND 75),
    d1_history_session_count smallint NOT NULL CHECK (d1_history_session_count BETWEEN 1 AND 75),
    event_window_start date NOT NULL,
    calendar_revision text NOT NULL,
    state text NOT NULL CHECK (state IN ('TRUE', 'FALSE', 'UNKNOWN')),
    reasons text[] NOT NULL,
    event_count integer NOT NULL CHECK (event_count >= 0),
    frozen_line_numerator numeric(100,0),
    frozen_line_denominator numeric(100,0),
    frozen_line_source text,
    scope text NOT NULL CHECK (scope = 'NON_PIT_EXPLORATORY'),
    PRIMARY KEY (namespace_id, target_session, source_symbol, package_sha256, source_capture_id,
                 rule_id, rule_version, profile_revision, history_session_count,
                 d1_history_session_count, event_window_start, calendar_revision),
    FOREIGN KEY (namespace_id, source_capture_id) REFERENCES stock.source_captures(namespace_id, capture_id),
    CHECK ((frozen_line_numerator IS NULL) = (frozen_line_denominator IS NULL)),
    CHECK (frozen_line_denominator IS NULL OR frozen_line_denominator > 0)
);
CREATE INDEX source_rule_evaluations_group_idx ON stock.source_rule_evaluations
    (namespace_id, target_session, package_sha256, rule_id, state);

CREATE TABLE stock.source_forward_path_facts (
    namespace_id uuid NOT NULL,
    target_session date NOT NULL,
    source_symbol text NOT NULL,
    package_sha256 char(64) NOT NULL,
    source_capture_id uuid NOT NULL,
    horizon smallint NOT NULL CHECK (horizon IN (1, 3, 5, 10, 20)),
    path_rule_version text NOT NULL,
    calendar_revision text NOT NULL,
    endpoint_session date NOT NULL,
    maturity text NOT NULL,
    endpoint_quality text NOT NULL,
    endpoint_reason text,
    endpoint_numerator numeric(100,0),
    endpoint_denominator numeric(100,0),
    path_quality text NOT NULL,
    path_reasons text[] NOT NULL,
    mfe_numerator numeric(100,0), mfe_denominator numeric(100,0),
    mae_numerator numeric(100,0), mae_denominator numeric(100,0),
    mdd_numerator numeric(100,0), mdd_denominator numeric(100,0),
    price_coordinate text NOT NULL CHECK (price_coordinate = 'RAW_UNADJUSTED'),
    scope text NOT NULL CHECK (scope = 'NON_PIT_EXPLORATORY'),
    PRIMARY KEY (namespace_id, target_session, source_symbol, package_sha256, source_capture_id,
                 horizon, path_rule_version, calendar_revision),
    FOREIGN KEY (namespace_id, source_capture_id) REFERENCES stock.source_captures(namespace_id, capture_id),
    CHECK ((endpoint_numerator IS NULL) = (endpoint_denominator IS NULL)),
    CHECK (endpoint_denominator IS NULL OR endpoint_denominator > 0),
    CHECK ((mfe_numerator IS NULL) = (mfe_denominator IS NULL)),
    CHECK ((mae_numerator IS NULL) = (mae_denominator IS NULL)),
    CHECK ((mdd_numerator IS NULL) = (mdd_denominator IS NULL)),
    CHECK (mfe_denominator IS NULL OR mfe_denominator > 0),
    CHECK (mae_denominator IS NULL OR mae_denominator > 0),
    CHECK (mdd_denominator IS NULL OR mdd_denominator > 0)
);
CREATE INDEX source_forward_path_facts_group_idx ON stock.source_forward_path_facts
    (namespace_id, target_session, package_sha256, horizon);

COMMENT ON TABLE stock.source_rule_evaluations IS '按来源股票和规则冻结的非PIT研究三态事实，不等于正式入选或事件';
COMMENT ON TABLE stock.source_forward_path_facts IS '来源股票共享的已到期非复权后验路径研究事实，不代表交易收益';
COMMENT ON COLUMN stock.source_rule_evaluations.namespace_id IS '来源命名空间';
COMMENT ON COLUMN stock.source_rule_evaluations.target_session IS '规则冻结的T0官方交易日';
COMMENT ON COLUMN stock.source_rule_evaluations.source_symbol IS '来源股票代码，非稳定证券身份';
COMMENT ON COLUMN stock.source_rule_evaluations.package_sha256 IS '来源包内容摘要';
COMMENT ON COLUMN stock.source_rule_evaluations.source_capture_id IS 'T0来源捕获标识';
COMMENT ON COLUMN stock.source_rule_evaluations.rule_id IS '来源规则或非正式形态诊断标识';
COMMENT ON COLUMN stock.source_rule_evaluations.rule_version IS '规则计算版本';
COMMENT ON COLUMN stock.source_rule_evaluations.profile_revision IS '即时画像计算修订';
COMMENT ON COLUMN stock.source_rule_evaluations.history_session_count IS '画像历史窗口的官方会话数';
COMMENT ON COLUMN stock.source_rule_evaluations.d1_history_session_count IS 'D1历史输入窗口的官方会话数';
COMMENT ON COLUMN stock.source_rule_evaluations.event_window_start IS 'D1锚事件窗口的起始官方会话';
COMMENT ON COLUMN stock.source_rule_evaluations.calendar_revision IS '官方日历修订';
COMMENT ON COLUMN stock.source_rule_evaluations.state IS '规则三态TRUE、FALSE或UNKNOWN';
COMMENT ON COLUMN stock.source_rule_evaluations.reasons IS '生产者提供的状态原因码，空数组表示未提供';
COMMENT ON COLUMN stock.source_rule_evaluations.event_count IS '已证锚或诊断父数量，不保存每个事件身份';
COMMENT ON COLUMN stock.source_rule_evaluations.frozen_line_numerator IS 'T0来源候选线的精确分子；未产生候选线时为空，非正式失效线';
COMMENT ON COLUMN stock.source_rule_evaluations.frozen_line_denominator IS 'T0来源候选线的精确正分母；未产生候选线时为空';
COMMENT ON COLUMN stock.source_rule_evaluations.frozen_line_source IS 'T0候选线的来源字段，未产生时为空';
COMMENT ON COLUMN stock.source_rule_evaluations.scope IS 'NON_PIT_EXPLORATORY，后验来源研究';
COMMENT ON COLUMN stock.source_forward_path_facts.namespace_id IS '来源命名空间';
COMMENT ON COLUMN stock.source_forward_path_facts.target_session IS '冻结T0官方交易日';
COMMENT ON COLUMN stock.source_forward_path_facts.source_symbol IS '来源股票代码，非稳定证券身份';
COMMENT ON COLUMN stock.source_forward_path_facts.package_sha256 IS '来源包内容摘要';
COMMENT ON COLUMN stock.source_forward_path_facts.source_capture_id IS 'T0来源捕获标识';
COMMENT ON COLUMN stock.source_forward_path_facts.horizon IS 'T0后的官方会话数';
COMMENT ON COLUMN stock.source_forward_path_facts.path_rule_version IS '路径公式版本';
COMMENT ON COLUMN stock.source_forward_path_facts.calendar_revision IS '官方日历修订';
COMMENT ON COLUMN stock.source_forward_path_facts.endpoint_session IS '已到期的官方端点交易日';
COMMENT ON COLUMN stock.source_forward_path_facts.maturity IS '端点成熟及来源质量状态';
COMMENT ON COLUMN stock.source_forward_path_facts.endpoint_quality IS '端点收益质量';
COMMENT ON COLUMN stock.source_forward_path_facts.endpoint_reason IS '端点不可用原因，可为空';
COMMENT ON COLUMN stock.source_forward_path_facts.endpoint_numerator IS '端点相对T0收盘收益精确分子，可为空';
COMMENT ON COLUMN stock.source_forward_path_facts.endpoint_denominator IS '端点收益精确正分母，可为空';
COMMENT ON COLUMN stock.source_forward_path_facts.path_quality IS '完整路径质量，与端点质量分开';
COMMENT ON COLUMN stock.source_forward_path_facts.path_reasons IS '路径缺失或质量原因码';
COMMENT ON COLUMN stock.source_forward_path_facts.mfe_numerator IS '最大有利价格变动的精确分子，可为空';
COMMENT ON COLUMN stock.source_forward_path_facts.mfe_denominator IS '最大有利价格变动的精确正分母，可为空';
COMMENT ON COLUMN stock.source_forward_path_facts.mae_numerator IS '最大不利价格变动的精确分子，可为空';
COMMENT ON COLUMN stock.source_forward_path_facts.mae_denominator IS '最大不利价格变动的精确正分母，可为空';
COMMENT ON COLUMN stock.source_forward_path_facts.mdd_numerator IS '收盘先峰后谷回撤的精确分子，可为空';
COMMENT ON COLUMN stock.source_forward_path_facts.mdd_denominator IS '收盘回撤的精确正分母，可为空';
COMMENT ON COLUMN stock.source_forward_path_facts.price_coordinate IS '原始未复权价格坐标';
COMMENT ON COLUMN stock.source_forward_path_facts.scope IS 'NON_PIT_EXPLORATORY，后验来源研究';
