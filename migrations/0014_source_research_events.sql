-- 来源级D1多锚研究事件，不生成正式证券、结构或交易身份。
CREATE TABLE stock.source_research_events (
    namespace_id uuid NOT NULL,
    target_session date NOT NULL,
    source_symbol text NOT NULL,
    package_sha256 char(64) NOT NULL,
    target_capture_id uuid NOT NULL,
    event_type text NOT NULL CHECK (event_type = 'SOURCE_D1_BREAKOUT'),
    anchor_session date NOT NULL CHECK (anchor_session < target_session),
    event_version text NOT NULL,
    d1_history_session_count smallint NOT NULL CHECK (d1_history_session_count BETWEEN 1 AND 75),
    calendar_revision text NOT NULL,
    anchor_capture_id uuid NOT NULL,
    anchor_entry_sha256 char(64) NOT NULL,
    source_content_id char(64) NOT NULL,
    price_coordinate text NOT NULL CHECK (price_coordinate = 'RAW_UNADJUSTED'),
    generation_status text NOT NULL,
    lower_numerator numeric(100,0), lower_denominator numeric(100,0),
    upper_numerator numeric(100,0), upper_denominator numeric(100,0),
    previous_session date NOT NULL,
    previous_state text, previous_quality text,
    target_state text, target_quality text,
    change_classification text NOT NULL,
    reason_code text,
    scope text NOT NULL CHECK (scope = 'NON_PIT_EXPLORATORY'),
    PRIMARY KEY (namespace_id, target_session, source_symbol, package_sha256, target_capture_id,
                 event_type, anchor_session, event_version, d1_history_session_count, calendar_revision),
    FOREIGN KEY (namespace_id, target_capture_id) REFERENCES stock.source_captures(namespace_id, capture_id),
    FOREIGN KEY (namespace_id, anchor_capture_id) REFERENCES stock.source_captures(namespace_id, capture_id),
    CHECK ((lower_numerator IS NULL) = (lower_denominator IS NULL)),
    CHECK ((upper_numerator IS NULL) = (upper_denominator IS NULL)),
    CHECK (lower_denominator IS NULL OR lower_denominator > 0),
    CHECK (upper_denominator IS NULL OR upper_denominator > 0)
);
CREATE INDEX source_research_events_lookup_idx ON stock.source_research_events
    (namespace_id, target_session, source_symbol, event_type);
COMMENT ON TABLE stock.source_research_events IS 'T0来源股票的独立D1历史锚事件及冻结平台；后验研究，不代表正式episode';
COMMENT ON COLUMN stock.source_research_events.namespace_id IS '来源命名空间';
COMMENT ON COLUMN stock.source_research_events.target_session IS '事件被纳入研究的T0官方会话';
COMMENT ON COLUMN stock.source_research_events.source_symbol IS '来源股票代码，不代表稳定证券身份';
COMMENT ON COLUMN stock.source_research_events.package_sha256 IS '来源包摘要';
COMMENT ON COLUMN stock.source_research_events.target_capture_id IS 'T0来源捕获标识';
COMMENT ON COLUMN stock.source_research_events.event_type IS '来源D1突破锚事件类型，非正式事件';
COMMENT ON COLUMN stock.source_research_events.anchor_session IS '已证D1原始谓词为真的锚会话';
COMMENT ON COLUMN stock.source_research_events.event_version IS '来源事件计算版本';
COMMENT ON COLUMN stock.source_research_events.d1_history_session_count IS 'D1历史计算窗口的官方会话数';
COMMENT ON COLUMN stock.source_research_events.calendar_revision IS '官方交易日历修订';
COMMENT ON COLUMN stock.source_research_events.anchor_capture_id IS '锚会话来源捕获标识';
COMMENT ON COLUMN stock.source_research_events.anchor_entry_sha256 IS '锚会话来源代码条目摘要';
COMMENT ON COLUMN stock.source_research_events.source_content_id IS '关联来源内容标识';
COMMENT ON COLUMN stock.source_research_events.price_coordinate IS '原始未复权价格坐标';
COMMENT ON COLUMN stock.source_research_events.generation_status IS '来源平台生成状态，未生成并非正式资格否定';
COMMENT ON COLUMN stock.source_research_events.lower_numerator IS '冻结平台下界精确分子，未生成时为空';
COMMENT ON COLUMN stock.source_research_events.lower_denominator IS '冻结平台下界精确正分母，未生成时为空';
COMMENT ON COLUMN stock.source_research_events.upper_numerator IS '冻结平台上界精确分子，未生成时为空';
COMMENT ON COLUMN stock.source_research_events.upper_denominator IS '冻结平台上界精确正分母，未生成时为空';
COMMENT ON COLUMN stock.source_research_events.previous_session IS 'T0前一个官方观察会话';
COMMENT ON COLUMN stock.source_research_events.previous_state IS '前一会话来源支撑状态，未观察时为空';
COMMENT ON COLUMN stock.source_research_events.previous_quality IS '前一会话支撑状态质量，未观察时为空';
COMMENT ON COLUMN stock.source_research_events.target_state IS 'T0来源支撑状态，未观察时为空';
COMMENT ON COLUMN stock.source_research_events.target_quality IS 'T0支撑状态质量，未观察时为空';
COMMENT ON COLUMN stock.source_research_events.change_classification IS '同锚前后来源观察变化分类';
COMMENT ON COLUMN stock.source_research_events.reason_code IS '生成或比较不可用的原因，可为空';
COMMENT ON COLUMN stock.source_research_events.scope IS 'NON_PIT_EXPLORATORY，后验来源研究';
