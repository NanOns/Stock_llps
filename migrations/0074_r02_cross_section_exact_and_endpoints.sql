-- 0074_r02_cross_section_exact_and_endpoints.sql
-- 目的：闭合 R02 审计复审 REM-08（全市场横截面10项因子无损精确有理数表达）与
--       REM-10（6个RPS端点 + 4个相对市场起始日端点的共享日级持久化依据）。
-- 遵守 A10 固定单库单 schema（stock_a.stock）、DB-08 零下游 SHA 列与全表/列中文注释约束。

ALTER TABLE stock.source_cross_section_day_facts
    ADD COLUMN IF NOT EXISTS rps5_exact text CHECK (rps5_exact IS NULL OR rps5_exact ~ '^-?[0-9]+(/[1-9][0-9]*)?$'),
    ADD COLUMN IF NOT EXISTS rps20_exact text CHECK (rps20_exact IS NULL OR rps20_exact ~ '^-?[0-9]+(/[1-9][0-9]*)?$'),
    ADD COLUMN IF NOT EXISTS rps5_delta1_pp_exact text CHECK (rps5_delta1_pp_exact IS NULL OR rps5_delta1_pp_exact ~ '^-?[0-9]+(/[1-9][0-9]*)?$'),
    ADD COLUMN IF NOT EXISTS rps5_delta3_pp_exact text CHECK (rps5_delta3_pp_exact IS NULL OR rps5_delta3_pp_exact ~ '^-?[0-9]+(/[1-9][0-9]*)?$'),
    ADD COLUMN IF NOT EXISTS rps20_delta1_pp_exact text CHECK (rps20_delta1_pp_exact IS NULL OR rps20_delta1_pp_exact ~ '^-?[0-9]+(/[1-9][0-9]*)?$'),
    ADD COLUMN IF NOT EXISTS rps20_delta3_pp_exact text CHECK (rps20_delta3_pp_exact IS NULL OR rps20_delta3_pp_exact ~ '^-?[0-9]+(/[1-9][0-9]*)?$'),
    ADD COLUMN IF NOT EXISTS rel_market_1_exact text CHECK (rel_market_1_exact IS NULL OR rel_market_1_exact ~ '^-?[0-9]+(/[1-9][0-9]*)?$'),
    ADD COLUMN IF NOT EXISTS rel_market_3_exact text CHECK (rel_market_3_exact IS NULL OR rel_market_3_exact ~ '^-?[0-9]+(/[1-9][0-9]*)?$'),
    ADD COLUMN IF NOT EXISTS rel_market_5_exact text CHECK (rel_market_5_exact IS NULL OR rel_market_5_exact ~ '^-?[0-9]+(/[1-9][0-9]*)?$'),
    ADD COLUMN IF NOT EXISTS rel_market_20_exact text CHECK (rel_market_20_exact IS NULL OR rel_market_20_exact ~ '^-?[0-9]+(/[1-9][0-9]*)?$');

COMMENT ON COLUMN stock.source_cross_section_day_facts.rps5_exact IS '5日全市场RPS精确有理分数文本（格式 p 或 p/q，供下游阈值与等号判定无损还原Fraction，未知为NULL）';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rps20_exact IS '20日全市场RPS精确有理分数文本（供下游阈值与等号判定无损还原Fraction，未知为NULL）';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rps5_delta1_pp_exact IS '5日RPS较T-1变化（百分点）精确有理分数文本（无损还原Fraction，未知为NULL）';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rps5_delta3_pp_exact IS '5日RPS较T-3变化（百分点）精确有理分数文本（无损还原Fraction，未知为NULL）';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rps20_delta1_pp_exact IS '20日RPS较T-1变化（百分点）精确有理分数文本（无损还原Fraction，未知为NULL）';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rps20_delta3_pp_exact IS '20日RPS较T-3变化（百分点）精确有理分数文本（无损还原Fraction，未知为NULL）';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rel_market_1_exact IS '1日相对市场等权收益差精确有理分数文本（无损还原Fraction，保D1严格>0与任意阈值判定）';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rel_market_3_exact IS '3日相对市场等权收益差精确有理分数文本（无损还原Fraction，未知为NULL）';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rel_market_5_exact IS '5日相对市场等权收益差精确有理分数文本（无损还原Fraction，未知为NULL）';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rel_market_20_exact IS '20日相对市场等权收益差精确有理分数文本（无损还原Fraction，未知为NULL）';

CREATE TABLE IF NOT EXISTS stock.source_cross_section_endpoints (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    target_session date NOT NULL,
    price_capture_id bigint NOT NULL REFERENCES stock.source_captures(capture_id),
    universe_capture_id bigint REFERENCES stock.source_captures(capture_id),
    universe_version text NOT NULL,
    calendar_id text NOT NULL,
    calendar_revision text NOT NULL,
    contract_version text NOT NULL,
    endpoint_key text NOT NULL CHECK (endpoint_key IN (
        'rps5_current', 'rps5_minus1', 'rps5_minus3',
        'rps20_current', 'rps20_minus1', 'rps20_minus3',
        'rel_market_1', 'rel_market_3', 'rel_market_5', 'rel_market_20'
    )),
    endpoint_kind text NOT NULL CHECK (endpoint_kind IN ('RANK_RPS', 'RELATIVE_MARKET')),
    return_period integer NOT NULL CHECK (return_period IN (1, 3, 5, 20)),
    offset_sessions integer NOT NULL CHECK (offset_sessions IN (0, 1, 3)),
    endpoint_status text NOT NULL CHECK (endpoint_status IN ('AVAILABLE', 'UNAVAILABLE')),
    endpoint_date date,
    return_start_date date,
    universe_id text,
    universe_revision text,
    denominator_count integer NOT NULL CHECK (denominator_count >= 0),
    known_count integer NOT NULL CHECK (known_count >= 0 AND known_count <= denominator_count),
    unknown_count integer NOT NULL CHECK (unknown_count >= 0 AND unknown_count = denominator_count - known_count),
    computed_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT source_cross_section_endpoints_business_uq
        UNIQUE (target_session, price_capture_id, universe_version, calendar_revision, contract_version, endpoint_key),
    CONSTRAINT source_cross_section_endpoints_avail_chk
        CHECK (
            (endpoint_status = 'UNAVAILABLE' AND endpoint_date IS NULL AND return_start_date IS NULL)
            OR (endpoint_status = 'AVAILABLE' AND endpoint_date IS NOT NULL AND return_start_date IS NOT NULL AND universe_id IS NOT NULL AND universe_revision IS NOT NULL)
        )
);

CREATE INDEX IF NOT EXISTS source_cross_section_endpoints_session_idx
    ON stock.source_cross_section_endpoints (target_session, price_capture_id, calendar_revision);

COMMENT ON TABLE stock.source_cross_section_endpoints IS '全市场横截面日级共享端点事实表：按目标日保存6个RPS端点（T/T-1/T-3）与4个相对市场起始日端点的真实日期、总体身份与分母/已知/未知计数，供Profile交接与审计核验共享引用。';
COMMENT ON COLUMN stock.source_cross_section_endpoints.id IS '自增代理主键';
COMMENT ON COLUMN stock.source_cross_section_endpoints.target_session IS '横截面目标交易日（官方交易日历会话）';
COMMENT ON COLUMN stock.source_cross_section_endpoints.price_capture_id IS '前复权价格来源捕获ID（外键引用 stock.source_captures）';
COMMENT ON COLUMN stock.source_cross_section_endpoints.universe_capture_id IS '日期有效证券总体来源捕获ID（可空，独立于价格捕获）';
COMMENT ON COLUMN stock.source_cross_section_endpoints.universe_version IS '目标日横截面业务总体版本标识';
COMMENT ON COLUMN stock.source_cross_section_endpoints.calendar_id IS '官方交易日历标识';
COMMENT ON COLUMN stock.source_cross_section_endpoints.calendar_revision IS '官方交易日历修订版本';
COMMENT ON COLUMN stock.source_cross_section_endpoints.contract_version IS '横截面计算合同版本（如 STOCK_A_CROSS_SECTION_10_R1）';
COMMENT ON COLUMN stock.source_cross_section_endpoints.endpoint_key IS '端点逻辑键（rps5/20 的 current/minus1/minus3 及 rel_market_1/3/5/20）';
COMMENT ON COLUMN stock.source_cross_section_endpoints.endpoint_kind IS '端点类别（RANK_RPS 排名端点 / RELATIVE_MARKET 相对市场端点）';
COMMENT ON COLUMN stock.source_cross_section_endpoints.return_period IS '端点收益窗口长度（1、3、5 或 20 个官方交易日）';
COMMENT ON COLUMN stock.source_cross_section_endpoints.offset_sessions IS '端点相对目标日的官方交易日偏移（0、1 或 3）';
COMMENT ON COLUMN stock.source_cross_section_endpoints.endpoint_status IS '端点可用状态（AVAILABLE 具备真实端点数据 / UNAVAILABLE 历史不足或未提供）';
COMMENT ON COLUMN stock.source_cross_section_endpoints.endpoint_date IS '端点真实截止交易日（AVAILABLE 时非空）';
COMMENT ON COLUMN stock.source_cross_section_endpoints.return_start_date IS '端点真实收益窗口起始交易日（AVAILABLE 时非空）';
COMMENT ON COLUMN stock.source_cross_section_endpoints.universe_id IS '该端点真实使用的总体标识（AVAILABLE 时非空）';
COMMENT ON COLUMN stock.source_cross_section_endpoints.universe_revision IS '该端点真实使用的总体修订（AVAILABLE 时非空）';
COMMENT ON COLUMN stock.source_cross_section_endpoints.denominator_count IS '该端点总体分母证券总数';
COMMENT ON COLUMN stock.source_cross_section_endpoints.known_count IS '该端点可观测有效收益证券数';
COMMENT ON COLUMN stock.source_cross_section_endpoints.unknown_count IS '该端点未知或缺失收益证券数（等于分母减已知数）';
COMMENT ON COLUMN stock.source_cross_section_endpoints.computed_at IS '端点事实落库时间戳（带时区）';
