-- R02 BaoStock 扩展设计（R3终审定稿）：新增日期有效证券主资料、逐日名录、通用参考指数日线、
-- 通用指数日因子、市场复合环境展示状态、全市场10项横截面因子及极简日配额台账共7张typed表。
-- 全部表与列均带中文注释，0 SHA列，无冗余last_job_id，无独立除权公司行为表。

-- 1. BaoStock 证券基本资料表（支持 ipo_date 可空隔离）
CREATE TABLE IF NOT EXISTS stock.source_baostock_stock_basic (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_capture_id bigint NOT NULL REFERENCES stock.source_captures(capture_id),
    source_symbol text NOT NULL,
    market text NOT NULL CHECK (market = ANY (ARRAY['SH'::text, 'SZ'::text, 'BJ'::text, 'OTHER'::text])),
    board text NOT NULL CHECK (board = ANY (ARRAY['MAIN_BOARD'::text, 'GEM'::text, 'STAR_BOARD'::text, 'BEIJING_BOARD'::text, 'B_SHARE'::text, 'INDEX'::text, 'OTHER'::text])),
    code_name text NOT NULL,
    security_type text NOT NULL CHECK (security_type = ANY (ARRAY['STOCK'::text, 'INDEX'::text, 'OTHER'::text])),
    listing_status text NOT NULL CHECK (listing_status = ANY (ARRAY['LISTED'::text, 'DELISTED'::text, 'UNKNOWN'::text])),
    ipo_date date,
    out_date date,
    field_quality text NOT NULL CHECK (field_quality = ANY (ARRAY['OBSERVED'::text, 'PARTIAL_UNKNOWN'::text, 'CONFLICT_QUARANTINED'::text])),
    quality_reason text,
    source_clock_role text NOT NULL,
    pit_status text NOT NULL CHECK (pit_status = ANY (ARRAY['NON_PIT_RETROSPECTIVE'::text, 'OBSERVED_AT_CAPTURE'::text])),
    captured_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT source_baostock_stock_basic_uq UNIQUE (source_capture_id, source_symbol),
    CONSTRAINT source_baostock_stock_basic_date_order_check CHECK (
        ipo_date IS NULL OR out_date IS NULL OR out_date >= ipo_date
    )
);

CREATE INDEX IF NOT EXISTS idx_baostock_stock_basic_symbol
    ON stock.source_baostock_stock_basic (source_symbol, source_capture_id);

COMMENT ON TABLE stock.source_baostock_stock_basic IS 'BaoStock 证券基本资料快照表：保存每次捕获的证券代码、名称、类型、上市状态及上市/退市日期（ipo_date 可空并做冲突隔离，不因缺字段删行）。';
COMMENT ON COLUMN stock.source_baostock_stock_basic.id IS '自增代理主键。';
COMMENT ON COLUMN stock.source_baostock_stock_basic.source_capture_id IS '真实捕获编号，外键指向 stock.source_captures(capture_id)。';
COMMENT ON COLUMN stock.source_baostock_stock_basic.source_symbol IS '规范化来源证券代码（如 SH.600000、SZ.399317）。';
COMMENT ON COLUMN stock.source_baostock_stock_basic.market IS '所属市场枚举（SH/SZ/BJ/OTHER）。';
COMMENT ON COLUMN stock.source_baostock_stock_basic.board IS '所属板块或指数类别枚举。';
COMMENT ON COLUMN stock.source_baostock_stock_basic.code_name IS '捕获时刻 BaoStock 返回的证券简称（不冒充历史更名轨迹）。';
COMMENT ON COLUMN stock.source_baostock_stock_basic.security_type IS '证券类型（STOCK=股票，INDEX=指数，OTHER=其他）。';
COMMENT ON COLUMN stock.source_baostock_stock_basic.listing_status IS '捕获时刻上市状态（LISTED=在市，DELISTED=退市，UNKNOWN=未知）。';
COMMENT ON COLUMN stock.source_baostock_stock_basic.ipo_date IS '上市日期（缺失时为 NULL 并降级 field_quality，不伪造）。';
COMMENT ON COLUMN stock.source_baostock_stock_basic.out_date IS '退市日期（在市股票为 NULL；有效交易区间采用半开区间 [ipo_date, out_date)）。';
COMMENT ON COLUMN stock.source_baostock_stock_basic.field_quality IS '本条基本资料字段完整性质量（OBSERVED/PARTIAL_UNKNOWN/CONFLICT_QUARANTINED）。';
COMMENT ON COLUMN stock.source_baostock_stock_basic.quality_reason IS '质量降级或冲突隔离原因说明。';
COMMENT ON COLUMN stock.source_baostock_stock_basic.source_clock_role IS '来源捕获时钟角色说明。';
COMMENT ON COLUMN stock.source_baostock_stock_basic.pit_status IS '时点语义标注（NON_PIT_RETROSPECTIVE 或 OBSERVED_AT_CAPTURE）。';
COMMENT ON COLUMN stock.source_baostock_stock_basic.captured_at IS '本次 BaoStock 接口实际捕获时间戳（带时区）。';
COMMENT ON COLUMN stock.source_baostock_stock_basic.created_at IS '记录入库时间戳（带时区）。';

-- 2. BaoStock 逐日全证券在市名录表（query_all_stock(day=d)）
CREATE TABLE IF NOT EXISTS stock.source_baostock_session_directory (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_capture_id bigint NOT NULL REFERENCES stock.source_captures(capture_id),
    session_date date NOT NULL,
    source_symbol text NOT NULL,
    market text NOT NULL CHECK (market = ANY (ARRAY['SH'::text, 'SZ'::text, 'BJ'::text, 'OTHER'::text])),
    board text NOT NULL CHECK (board = ANY (ARRAY['MAIN_BOARD'::text, 'GEM'::text, 'STAR_BOARD'::text, 'BEIJING_BOARD'::text, 'B_SHARE'::text, 'INDEX'::text, 'OTHER'::text])),
    session_code_name text NOT NULL,
    trade_status text NOT NULL CHECK (trade_status = ANY (ARRAY['TRADING'::text, 'SUSPENDED'::text, 'UNKNOWN'::text])),
    captured_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT source_baostock_session_directory_uq UNIQUE (source_capture_id, session_date, source_symbol)
);

CREATE INDEX IF NOT EXISTS idx_baostock_session_dir_date_board
    ON stock.source_baostock_session_directory (session_date, source_capture_id, board);

COMMENT ON TABLE stock.source_baostock_session_directory IS 'BaoStock 逐日全证券在市名录表（query_all_stock）：作为来源级 NON_PIT 日期有效 Universe 的正式输入，记录每日在市证券代码、当日简称与交易状态。';
COMMENT ON COLUMN stock.source_baostock_session_directory.id IS '自增代理主键。';
COMMENT ON COLUMN stock.source_baostock_session_directory.source_capture_id IS '真实捕获编号，外键指向 stock.source_captures(capture_id)。';
COMMENT ON COLUMN stock.source_baostock_session_directory.session_date IS '官方交易日日期。';
COMMENT ON COLUMN stock.source_baostock_session_directory.source_symbol IS '规范化来源证券代码。';
COMMENT ON COLUMN stock.source_baostock_session_directory.market IS '所属市场枚举（SH/SZ/BJ/OTHER）。';
COMMENT ON COLUMN stock.source_baostock_session_directory.board IS '所属板块或指数类别枚举。';
COMMENT ON COLUMN stock.source_baostock_session_directory.session_code_name IS 'BaoStock 该交易日返回的证券简称（未独立证实时不作严格历史 PIT）。';
COMMENT ON COLUMN stock.source_baostock_session_directory.trade_status IS '当日交易状态（TRADING=正常交易，SUSPENDED=停牌，UNKNOWN=未知）。';
COMMENT ON COLUMN stock.source_baostock_session_directory.captured_at IS '本次 BaoStock 接口实际捕获时间戳（带时区）。';
COMMENT ON COLUMN stock.source_baostock_session_directory.created_at IS '记录入库时间戳（带时区）。';

-- 3. 通用参考指数日线表（价格单位为点 INDEX_POINT，与股票人民币隔离）
CREATE TABLE IF NOT EXISTS stock.source_market_index_daily (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_capture_id bigint NOT NULL REFERENCES stock.source_captures(capture_id),
    source_symbol text NOT NULL,
    official_code text NOT NULL,
    session_date date NOT NULL,
    price_coordinate text NOT NULL CHECK (price_coordinate = 'RAW_UNADJUSTED'::text),
    price_unit text NOT NULL CHECK (price_unit = 'INDEX_POINT'::text),
    bar_status text NOT NULL CHECK (bar_status = ANY (ARRAY['TARGET_PRESENT'::text, 'TARGET_MISSING'::text, 'READ_FAILED'::text])),
    open_points numeric(20, 6),
    high_points numeric(20, 6),
    low_points numeric(20, 6),
    close_points numeric(20, 6),
    preclose_points numeric(20, 6),
    volume_raw numeric(28, 4),
    amount_raw numeric(28, 4),
    pct_chg_reported numeric(20, 8),
    price_quality text NOT NULL CHECK (price_quality = ANY (ARRAY['OBSERVED'::text, 'UNKNOWN_DATA'::text, 'INVALID'::text])),
    price_reason text,
    pit_status text NOT NULL CHECK (pit_status = ANY (ARRAY['NON_PIT_RETROSPECTIVE'::text, 'OBSERVED_AT_CAPTURE'::text])),
    captured_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT source_market_index_daily_uq UNIQUE (source_capture_id, source_symbol, session_date)
);

CREATE INDEX IF NOT EXISTS idx_market_index_daily_symbol_date
    ON stock.source_market_index_daily (source_symbol, session_date, source_capture_id);

COMMENT ON TABLE stock.source_market_index_daily IS '通用市场参考指数日线事实表：所有配置指数共用此表，价格单位为指数点（INDEX_POINT），不与股票价格表混放，也不保存易变的配置角色。';
COMMENT ON COLUMN stock.source_market_index_daily.id IS '自增代理主键。';
COMMENT ON COLUMN stock.source_market_index_daily.source_capture_id IS '指数日线真实捕获编号，外键指向 stock.source_captures(capture_id)。';
COMMENT ON COLUMN stock.source_market_index_daily.source_symbol IS '规范化供应方指数代码（如 SZ.399317、SH.000001）。';
COMMENT ON COLUMN stock.source_market_index_daily.official_code IS '官方6位指数代码（如 399317、000001）。';
COMMENT ON COLUMN stock.source_market_index_daily.session_date IS '官方交易日日期。';
COMMENT ON COLUMN stock.source_market_index_daily.price_coordinate IS '价格坐标，指数固定为 RAW_UNADJUSTED。';
COMMENT ON COLUMN stock.source_market_index_daily.price_unit IS '价格单位，固定为 INDEX_POINT（指数点，非人民币元）。';
COMMENT ON COLUMN stock.source_market_index_daily.bar_status IS '日线捕获状态（TARGET_PRESENT/TARGET_MISSING/READ_FAILED）。';
COMMENT ON COLUMN stock.source_market_index_daily.open_points IS '指数开盘点位（单位：点）。';
COMMENT ON COLUMN stock.source_market_index_daily.high_points IS '指数最高点位（单位：点）。';
COMMENT ON COLUMN stock.source_market_index_daily.low_points IS '指数最低点位（单位：点）。';
COMMENT ON COLUMN stock.source_market_index_daily.close_points IS '指数收盘点位（单位：点）。';
COMMENT ON COLUMN stock.source_market_index_daily.preclose_points IS '指数前收盘点位（单位：点）。';
COMMENT ON COLUMN stock.source_market_index_daily.volume_raw IS '供应方返回的原始成交量（不与股票或其他指数直接加总）。';
COMMENT ON COLUMN stock.source_market_index_daily.amount_raw IS '供应方返回的原始成交额（不与其他重叠指数直接加总）。';
COMMENT ON COLUMN stock.source_market_index_daily.pct_chg_reported IS '供应方返回的涨跌幅诊断值（单位：百分数）。';
COMMENT ON COLUMN stock.source_market_index_daily.price_quality IS '点位质量（OBSERVED/UNKNOWN_DATA/INVALID）。';
COMMENT ON COLUMN stock.source_market_index_daily.price_reason IS '质量异常或缺失原因代码。';
COMMENT ON COLUMN stock.source_market_index_daily.pit_status IS '时点语义标注（NON_PIT_RETROSPECTIVE 或 OBSERVED_AT_CAPTURE）。';
COMMENT ON COLUMN stock.source_market_index_daily.captured_at IS '本次接口实际捕获时间戳（带时区）。';
COMMENT ON COLUMN stock.source_market_index_daily.created_at IS '记录入库时间戳（带时区）。';

-- 4. 通用参考指数日因子与趋势事实表
CREATE TABLE IF NOT EXISTS stock.source_market_index_day_facts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    target_session date NOT NULL,
    source_symbol text NOT NULL,
    official_code text NOT NULL,
    index_capture_id bigint NOT NULL REFERENCES stock.source_captures(capture_id),
    config_version text NOT NULL,
    index_role_at_run text NOT NULL CHECK (index_role_at_run = ANY (ARRAY['PRIMARY_MARKET'::text, 'OBSERVATION_BENCHMARK'::text])),
    calendar_id text NOT NULL,
    calendar_revision text NOT NULL,
    contract_version text NOT NULL,
    close_points numeric(20, 6),
    ma20_points numeric(24, 10),
    ma20_5d_ago_points numeric(24, 10),
    ret1_fraction numeric(24, 12),
    ret1_quality text NOT NULL CHECK (ret1_quality = ANY (ARRAY['OBSERVED'::text, 'UNKNOWN_DATA'::text])),
    ret1_reason text,
    ret5_fraction numeric(24, 12),
    ret5_quality text NOT NULL CHECK (ret5_quality = ANY (ARRAY['OBSERVED'::text, 'UNKNOWN_DATA'::text])),
    ret5_reason text,
    ret20_fraction numeric(24, 12),
    ret20_quality text NOT NULL CHECK (ret20_quality = ANY (ARRAY['OBSERVED'::text, 'UNKNOWN_DATA'::text])),
    ret20_reason text,
    close_vs_ma20_fraction numeric(24, 12),
    close_vs_ma20_quality text NOT NULL CHECK (close_vs_ma20_quality = ANY (ARRAY['OBSERVED'::text, 'UNKNOWN_DATA'::text])),
    close_vs_ma20_reason text,
    trend_state text NOT NULL CHECK (trend_state = ANY (ARRAY['STRONG'::text, 'NEUTRAL'::text, 'WEAK'::text, 'UNKNOWN'::text])),
    trend_reason text,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT source_market_index_day_facts_uq UNIQUE (target_session, source_symbol, index_capture_id, calendar_revision, contract_version)
);

COMMENT ON TABLE stock.source_market_index_day_facts IS '通用参考指数日级因子与趋势事实表：各启用指数独立计算 ret1/ret5/ret20（fraction）、close_vs_ma20（fraction）与 ALG-26 趋势状态，供市场主趋势与风格剪刀差只读消费。';
COMMENT ON COLUMN stock.source_market_index_day_facts.id IS '自增代理主键。';
COMMENT ON COLUMN stock.source_market_index_day_facts.target_session IS '目标交易日日期。';
COMMENT ON COLUMN stock.source_market_index_day_facts.source_symbol IS '规范化供应方指数代码。';
COMMENT ON COLUMN stock.source_market_index_day_facts.official_code IS '官方6位指数代码。';
COMMENT ON COLUMN stock.source_market_index_day_facts.index_capture_id IS '所用指数日线的真实捕获编号。';
COMMENT ON COLUMN stock.source_market_index_day_facts.config_version IS '生成时参考指数目录配置版本。';
COMMENT ON COLUMN stock.source_market_index_day_facts.index_role_at_run IS '生成时该指数的配置角色（PRIMARY_MARKET 或 OBSERVATION_BENCHMARK）。';
COMMENT ON COLUMN stock.source_market_index_day_facts.calendar_id IS '官方交易日历标识。';
COMMENT ON COLUMN stock.source_market_index_day_facts.calendar_revision IS '官方交易日历版本号。';
COMMENT ON COLUMN stock.source_market_index_day_facts.contract_version IS '指数因子算法合同版本。';
COMMENT ON COLUMN stock.source_market_index_day_facts.close_points IS '目标日收盘点位（单位：点）。';
COMMENT ON COLUMN stock.source_market_index_day_facts.ma20_points IS '目标日20会话收盘均线点位（单位：点）。';
COMMENT ON COLUMN stock.source_market_index_day_facts.ma20_5d_ago_points IS '5个官方市场会话前20日均线点位（单位：点）。';
COMMENT ON COLUMN stock.source_market_index_day_facts.ret1_fraction IS '1日收益率（单位：fraction，close/preclose-1）。';
COMMENT ON COLUMN stock.source_market_index_day_facts.ret1_quality IS '1日收益率质量（OBSERVED/UNKNOWN_DATA）。';
COMMENT ON COLUMN stock.source_market_index_day_facts.ret1_reason IS '1日收益率未知原因。';
COMMENT ON COLUMN stock.source_market_index_day_facts.ret5_fraction IS '5个官方会话收益率（单位：fraction）。';
COMMENT ON COLUMN stock.source_market_index_day_facts.ret5_quality IS '5日收益率质量（OBSERVED/UNKNOWN_DATA）。';
COMMENT ON COLUMN stock.source_market_index_day_facts.ret5_reason IS '5日收益率未知原因。';
COMMENT ON COLUMN stock.source_market_index_day_facts.ret20_fraction IS '20个官方会话收益率（单位：fraction）。';
COMMENT ON COLUMN stock.source_market_index_day_facts.ret20_quality IS '20日收益率质量（OBSERVED/UNKNOWN_DATA）。';
COMMENT ON COLUMN stock.source_market_index_day_facts.ret20_reason IS '20日收益率未知原因。';
COMMENT ON COLUMN stock.source_market_index_day_facts.close_vs_ma20_fraction IS '收盘相对MA20偏离比例（单位：fraction，close/MA20-1；显示百分数时乘100）。';
COMMENT ON COLUMN stock.source_market_index_day_facts.close_vs_ma20_quality IS 'MA20偏离质量（OBSERVED/UNKNOWN_DATA）。';
COMMENT ON COLUMN stock.source_market_index_day_facts.close_vs_ma20_reason IS 'MA20偏离未知原因。';
COMMENT ON COLUMN stock.source_market_index_day_facts.trend_state IS 'ALG-26指数趋势状态（STRONG/NEUTRAL/WEAK/UNKNOWN）。';
COMMENT ON COLUMN stock.source_market_index_day_facts.trend_reason IS '趋势状态判定原因代码。';
COMMENT ON COLUMN stock.source_market_index_day_facts.created_at IS '记录入库时间戳（带时区）。';

-- 5. 市场复合环境日状态与滞后展示事实表（绑定独立股票捕获与指数捕获，不混用）
CREATE TABLE IF NOT EXISTS stock.source_market_environment_day_facts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    target_session date NOT NULL,
    stock_capture_id bigint NOT NULL REFERENCES stock.source_captures(capture_id),
    index_capture_id bigint REFERENCES stock.source_captures(capture_id),
    primary_index_symbol text NOT NULL,
    index_config_version text NOT NULL,
    calendar_id text NOT NULL,
    calendar_revision text NOT NULL,
    rule_version text NOT NULL,
    trend_axis text NOT NULL CHECK (trend_axis = ANY (ARRAY['STRONG'::text, 'NEUTRAL'::text, 'WEAK'::text, 'UNKNOWN'::text])),
    breadth_axis text NOT NULL CHECK (breadth_axis = ANY (ARRAY['STRONG'::text, 'NEUTRAL'::text, 'WEAK'::text, 'UNKNOWN'::text])),
    participation_axis text NOT NULL CHECK (participation_axis = ANY (ARRAY['EXPANDING'::text, 'NORMAL'::text, 'THIN'::text, 'UNKNOWN'::text])),
    pressure_axis text NOT NULL CHECK (pressure_axis = ANY (ARRAY['HIGH'::text, 'ELEVATED'::text, 'LOW'::text, 'UNKNOWN'::text])),
    raw_status text NOT NULL CHECK (raw_status = ANY (ARRAY['KNOWN'::text, 'UNKNOWN'::text])),
    raw_label text CHECK (raw_label IS NULL OR raw_label = ANY (ARRAY['DEFENSIVE'::text, 'EXPANSIVE'::text, 'IMPROVING'::text, 'MIXED'::text])),
    displayed_label text CHECK (displayed_label IS NULL OR displayed_label = ANY (ARRAY['DEFENSIVE'::text, 'EXPANSIVE'::text, 'IMPROVING'::text, 'MIXED'::text])),
    display_status text NOT NULL CHECK (display_status = ANY (ARRAY['FIRST_OBSERVED'::text, 'HELD'::text, 'HYSTERESIS_PENDING'::text, 'TRANSITIONED_CONFIRMED'::text, 'TRANSITIONED_IMMEDIATE_DEFENSIVE'::text, 'STALE'::text, 'UNKNOWN'::text, 'RULE_VERSION_CHANGED'::text])),
    pending_label text CHECK (pending_label IS NULL OR pending_label = ANY (ARRAY['DEFENSIVE'::text, 'EXPANSIVE'::text, 'IMPROVING'::text, 'MIXED'::text])),
    pending_count integer NOT NULL DEFAULT 0 CHECK (pending_count >= 0),
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT source_market_environment_day_facts_uq UNIQUE (target_session, stock_capture_id, primary_index_symbol, calendar_revision, rule_version)
);

COMMENT ON TABLE stock.source_market_environment_day_facts IS '§11.4 市场复合研究环境日状态表：独立记录四轴、raw复合标签及严格官方相邻会话归约后的滞后展示状态，GET 纯只读不重算历史。';
COMMENT ON COLUMN stock.source_market_environment_day_facts.id IS '自增代理主键。';
COMMENT ON COLUMN stock.source_market_environment_day_facts.target_session IS '目标交易日日期。';
COMMENT ON COLUMN stock.source_market_environment_day_facts.stock_capture_id IS '股票宽度/参与/压力事实对应的真实捕获编号。';
COMMENT ON COLUMN stock.source_market_environment_day_facts.index_capture_id IS '主趋势参考指数对应的真实捕获编号（指数未采集时可为 NULL）。';
COMMENT ON COLUMN stock.source_market_environment_day_facts.primary_index_symbol IS '生成时实际生效的唯一 PRIMARY_MARKET 指数代码。';
COMMENT ON COLUMN stock.source_market_environment_day_facts.index_config_version IS '生成时参考指数目录配置版本。';
COMMENT ON COLUMN stock.source_market_environment_day_facts.calendar_id IS '官方交易日历标识。';
COMMENT ON COLUMN stock.source_market_environment_day_facts.calendar_revision IS '官方交易日历版本号。';
COMMENT ON COLUMN stock.source_market_environment_day_facts.rule_version IS '市场复合展示规则版本（如 MARKET_PRESENTATION_R1）。';
COMMENT ON COLUMN stock.source_market_environment_day_facts.trend_axis IS '趋势轴状态（STRONG/NEUTRAL/WEAK/UNKNOWN）。';
COMMENT ON COLUMN stock.source_market_environment_day_facts.breadth_axis IS '宽度轴状态（STRONG/NEUTRAL/WEAK/UNKNOWN）。';
COMMENT ON COLUMN stock.source_market_environment_day_facts.participation_axis IS '成交参与轴状态（EXPANDING/NORMAL/THIN/UNKNOWN）。';
COMMENT ON COLUMN stock.source_market_environment_day_facts.pressure_axis IS '涨跌停压力轴状态（HIGH/ELEVATED/LOW/UNKNOWN）。';
COMMENT ON COLUMN stock.source_market_environment_day_facts.raw_status IS '当日原始复合标签是否唯一确定（KNOWN/UNKNOWN）。';
COMMENT ON COLUMN stock.source_market_environment_day_facts.raw_label IS '当日原始复合研究标签（DEFENSIVE/EXPANSIVE/IMPROVING/MIXED 或 NULL）。';
COMMENT ON COLUMN stock.source_market_environment_day_facts.displayed_label IS '经相邻官方会话滞后规则归约后的当前展示标签。';
COMMENT ON COLUMN stock.source_market_environment_day_facts.display_status IS '展示状态机状态（FIRST_OBSERVED/HELD/HYSTERESIS_PENDING/TRANSITIONED_CONFIRMED/TRANSITIONED_IMMEDIATE_DEFENSIVE/STALE/UNKNOWN/RULE_VERSION_CHANGED）。';
COMMENT ON COLUMN stock.source_market_environment_day_facts.pending_label IS '待2日确认的目标切换标签。';
COMMENT ON COLUMN stock.source_market_environment_day_facts.pending_count IS '待确认连续相邻会话计数（0或1）。';
COMMENT ON COLUMN stock.source_market_environment_day_facts.created_at IS '记录入库时间戳（带时区）。';

-- 6. 全市场横截面 10 项因子日级事实表（ALG-09/10/11 完整10项）
CREATE TABLE IF NOT EXISTS stock.source_cross_section_day_facts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    target_session date NOT NULL,
    source_symbol text NOT NULL,
    price_capture_id bigint NOT NULL REFERENCES stock.source_captures(capture_id),
    universe_capture_id bigint REFERENCES stock.source_captures(capture_id),
    universe_version text NOT NULL,
    price_coordinate text NOT NULL CHECK (price_coordinate = 'FORWARD_ADJUSTED'::text),
    calendar_id text NOT NULL,
    calendar_revision text NOT NULL,
    contract_version text NOT NULL,
    pit_status text NOT NULL CHECK (pit_status = ANY (ARRAY['NON_PIT_RETROSPECTIVE'::text, 'OBSERVED_AT_CAPTURE'::text])),
    -- 10 项横截面因子数值、质量与原因
    rps5 numeric(18, 10),
    rps5_quality text NOT NULL CHECK (rps5_quality = ANY (ARRAY['OBSERVED'::text, 'UNKNOWN_DATA'::text])),
    rps5_reason text,
    rps20 numeric(18, 10),
    rps20_quality text NOT NULL CHECK (rps20_quality = ANY (ARRAY['OBSERVED'::text, 'UNKNOWN_DATA'::text])),
    rps20_reason text,
    rps5_delta1_pp numeric(18, 10),
    rps5_delta1_pp_quality text NOT NULL CHECK (rps5_delta1_pp_quality = ANY (ARRAY['OBSERVED'::text, 'UNKNOWN_DATA'::text])),
    rps5_delta1_pp_reason text,
    rps5_delta3_pp numeric(18, 10),
    rps5_delta3_pp_quality text NOT NULL CHECK (rps5_delta3_pp_quality = ANY (ARRAY['OBSERVED'::text, 'UNKNOWN_DATA'::text])),
    rps5_delta3_pp_reason text,
    rps20_delta1_pp numeric(18, 10),
    rps20_delta1_pp_quality text NOT NULL CHECK (rps20_delta1_pp_quality = ANY (ARRAY['OBSERVED'::text, 'UNKNOWN_DATA'::text])),
    rps20_delta1_pp_reason text,
    rps20_delta3_pp numeric(18, 10),
    rps20_delta3_pp_quality text NOT NULL CHECK (rps20_delta3_pp_quality = ANY (ARRAY['OBSERVED'::text, 'UNKNOWN_DATA'::text])),
    rps20_delta3_pp_reason text,
    rel_market_1 numeric(24, 12),
    rel_market_1_quality text NOT NULL CHECK (rel_market_1_quality = ANY (ARRAY['OBSERVED'::text, 'UNKNOWN_DATA'::text])),
    rel_market_1_reason text,
    rel_market_3 numeric(24, 12),
    rel_market_3_quality text NOT NULL CHECK (rel_market_3_quality = ANY (ARRAY['OBSERVED'::text, 'UNKNOWN_DATA'::text])),
    rel_market_3_reason text,
    rel_market_5 numeric(24, 12),
    rel_market_5_quality text NOT NULL CHECK (rel_market_5_quality = ANY (ARRAY['OBSERVED'::text, 'UNKNOWN_DATA'::text])),
    rel_market_5_reason text,
    rel_market_20 numeric(24, 12),
    rel_market_20_quality text NOT NULL CHECK (rel_market_20_quality = ANY (ARRAY['OBSERVED'::text, 'UNKNOWN_DATA'::text])),
    rel_market_20_reason text,
    -- 端点与分母元数据（不复制庞大成员数组，无 SHA）
    target_universe_count integer NOT NULL CHECK (target_universe_count >= 0),
    rps5_known_count integer NOT NULL DEFAULT 0 CHECK (rps5_known_count >= 0),
    rps20_known_count integer NOT NULL DEFAULT 0 CHECK (rps20_known_count >= 0),
    rel_market_1_start_session date,
    rel_market_1_denominator integer NOT NULL DEFAULT 0 CHECK (rel_market_1_denominator >= 0),
    rel_market_1_known_count integer NOT NULL DEFAULT 0 CHECK (rel_market_1_known_count >= 0),
    rel_market_3_start_session date,
    rel_market_3_denominator integer NOT NULL DEFAULT 0 CHECK (rel_market_3_denominator >= 0),
    rel_market_3_known_count integer NOT NULL DEFAULT 0 CHECK (rel_market_3_known_count >= 0),
    rel_market_5_start_session date,
    rel_market_5_denominator integer NOT NULL DEFAULT 0 CHECK (rel_market_5_denominator >= 0),
    rel_market_5_known_count integer NOT NULL DEFAULT 0 CHECK (rel_market_5_known_count >= 0),
    rel_market_20_start_session date,
    rel_market_20_denominator integer NOT NULL DEFAULT 0 CHECK (rel_market_20_denominator >= 0),
    rel_market_20_known_count integer NOT NULL DEFAULT 0 CHECK (rel_market_20_known_count >= 0),
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT source_cross_section_day_facts_uq UNIQUE (target_session, source_symbol, price_capture_id, universe_version, calendar_revision, contract_version)
);

CREATE INDEX IF NOT EXISTS idx_cross_section_day_session_symbol
    ON stock.source_cross_section_day_facts (target_session, price_capture_id, source_symbol);

COMMENT ON TABLE stock.source_cross_section_day_facts IS 'ALG-09/10/11 全市场横截面10项因子日级事实表：保存 rps5/rps20、rps5/20_delta1/3_pp 及 rel_market_1/3/5/20 精确数值、独立质量与起点/端点分母计数（0 SHA）。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.id IS '自增代理主键。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.target_session IS '目标交易日日期。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.source_symbol IS '规范化来源证券代码。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.price_capture_id IS '所用 QFQ 价格日线的真实捕获编号。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.universe_capture_id IS '所用日期有效 Universe 来源（stock_basic/session_directory）的真实捕获编号。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.universe_version IS '日期有效 Universe 口径与版本标识（如 BAOSTOCK_LISTING_INTERVAL_UNIVERSE_NON_PIT_R1）。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.price_coordinate IS '价格坐标，固定为 FORWARD_ADJUSTED。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.calendar_id IS '官方交易日历标识。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.calendar_revision IS '官方交易日历版本号。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.contract_version IS '横截面算法合同版本。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.pit_status IS '时点语义标注（NON_PIT_RETROSPECTIVE 或 OBSERVED_AT_CAPTURE）。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rps5 IS '5日收益率全市场中秩百分位（单位：0-100）。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rps5_quality IS 'rps5质量（OBSERVED/UNKNOWN_DATA）。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rps5_reason IS 'rps5未知原因。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rps20 IS '20日收益率全市场中秩百分位（单位：0-100）。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rps20_quality IS 'rps20质量（OBSERVED/UNKNOWN_DATA）。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rps20_reason IS 'rps20未知原因。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rps5_delta1_pp IS 'rps5相对前1官方会话的百分点变化（单位：pp）。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rps5_delta1_pp_quality IS 'rps5_delta1_pp质量（OBSERVED/UNKNOWN_DATA）。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rps5_delta1_pp_reason IS 'rps5_delta1_pp未知原因。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rps5_delta3_pp IS 'rps5相对前3官方会话的百分点变化（单位：pp）。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rps5_delta3_pp_quality IS 'rps5_delta3_pp质量（OBSERVED/UNKNOWN_DATA）。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rps5_delta3_pp_reason IS 'rps5_delta3_pp未知原因。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rps20_delta1_pp IS 'rps20相对前1官方会话的百分点变化（单位：pp）。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rps20_delta1_pp_quality IS 'rps20_delta1_pp质量（OBSERVED/UNKNOWN_DATA）。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rps20_delta1_pp_reason IS 'rps20_delta1_pp未知原因。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rps20_delta3_pp IS 'rps20相对前3官方会话的百分点变化（单位：pp）。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rps20_delta3_pp_quality IS 'rps20_delta3_pp质量（OBSERVED/UNKNOWN_DATA）。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rps20_delta3_pp_reason IS 'rps20_delta3_pp未知原因。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rel_market_1 IS '1日相对起始日Universe等权市场超额收益（单位：fraction）。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rel_market_1_quality IS 'rel_market_1质量（OBSERVED/UNKNOWN_DATA）。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rel_market_1_reason IS 'rel_market_1未知原因。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rel_market_3 IS '3日相对起始日Universe等权市场超额收益（单位：fraction）。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rel_market_3_quality IS 'rel_market_3质量（OBSERVED/UNKNOWN_DATA）。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rel_market_3_reason IS 'rel_market_3未知原因。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rel_market_5 IS '5日相对起始日Universe等权市场超额收益（单位：fraction）。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rel_market_5_quality IS 'rel_market_5质量（OBSERVED/UNKNOWN_DATA）。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rel_market_5_reason IS 'rel_market_5未知原因。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rel_market_20 IS '20日相对起始日Universe等权市场超额收益（单位：fraction）。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rel_market_20_quality IS 'rel_market_20质量（OBSERVED/UNKNOWN_DATA）。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rel_market_20_reason IS 'rel_market_20未知原因。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.target_universe_count IS '目标交易日正式研究总体成员总数。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rps5_known_count IS '目标日rps5有效可评价成员数。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rps20_known_count IS '目标日rps20有效可评价成员数。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rel_market_1_start_session IS 'rel_market_1对应的起始官方会话日期。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rel_market_1_denominator IS 'rel_market_1起始日Universe总成员数。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rel_market_1_known_count IS 'rel_market_1起始日Universe可评价成员数。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rel_market_3_start_session IS 'rel_market_3对应的起始官方会话日期。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rel_market_3_denominator IS 'rel_market_3起始日Universe总成员数。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rel_market_3_known_count IS 'rel_market_3起始日Universe可评价成员数。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rel_market_5_start_session IS 'rel_market_5对应的起始官方会话日期。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rel_market_5_denominator IS 'rel_market_5起始日Universe总成员数。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rel_market_5_known_count IS 'rel_market_5起始日Universe可评价成员数。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rel_market_20_start_session IS 'rel_market_20对应的起始官方会话日期。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rel_market_20_denominator IS 'rel_market_20起始日Universe总成员数。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.rel_market_20_known_count IS 'rel_market_20起始日Universe可评价成员数。';
COMMENT ON COLUMN stock.source_cross_section_day_facts.created_at IS '记录入库时间戳（带时区）。';

-- 7. BaoStock 极简自然日全局 API 配额台账表（5列，无冗余 last_job_id）
CREATE TABLE IF NOT EXISTS stock.baostock_daily_api_quota_ledger (
    quota_date date NOT NULL,
    api_name text NOT NULL,
    call_count bigint NOT NULL DEFAULT 0 CHECK (call_count >= 0),
    failed_count bigint NOT NULL DEFAULT 0 CHECK (failed_count >= 0),
    updated_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT baostock_daily_api_quota_ledger_pk PRIMARY KEY (quota_date, api_name)
);

COMMENT ON TABLE stock.baostock_daily_api_quota_ledger IS 'BaoStock 极简自然日（北京时间）全局 API 请求计数台账：每次真实请求前原子扣减并独立提交，作为内部工程防呆预算保护，不冒充官方 SLA 承诺。';
COMMENT ON COLUMN stock.baostock_daily_api_quota_ledger.quota_date IS '北京时间自然日日期。';
COMMENT ON COLUMN stock.baostock_daily_api_quota_ledger.api_name IS 'BaoStock 接口方法名（如 query_history_k_data_plus、query_stock_basic 等）。';
COMMENT ON COLUMN stock.baostock_daily_api_quota_ledger.call_count IS '当日累计已发出的请求总次数（含失败请求）。';
COMMENT ON COLUMN stock.baostock_daily_api_quota_ledger.failed_count IS '当日累计失败请求次数。';
COMMENT ON COLUMN stock.baostock_daily_api_quota_ledger.updated_at IS '最近一次更新时间戳（带时区）。';
