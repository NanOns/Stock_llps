-- R02 能力推进（MKT-04 / §11.4）：新增日线连板计数事实表 stock.source_limit_up_streak_day_facts，
-- 严格按相邻官方市场会话递推每只证券的精确连板数、未知下界、首板状态与停牌 RESET 语义，
-- 供全市场日线连板梯队只读查询消费（不输出日线无法证明的封板顺序或炸板次数）。

CREATE TABLE IF NOT EXISTS stock.source_limit_up_streak_day_facts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    target_session date NOT NULL,
    source_symbol text NOT NULL,
    market text NOT NULL CHECK (market = ANY (ARRAY['SH'::text, 'SZ'::text, 'BJ'::text])),
    board text NOT NULL CHECK (board = ANY (ARRAY['MAIN_BOARD'::text, 'GEM'::text, 'STAR_BOARD'::text, 'BEIJING_BOARD'::text, 'B_SHARE'::text, 'OTHER'::text])),
    research_eligible boolean NOT NULL,
    source_capture_id bigint NOT NULL REFERENCES stock.source_captures(capture_id),
    price_coordinate text NOT NULL CHECK (price_coordinate = 'RAW_UNADJUSTED'::text),
    contract_version text NOT NULL,
    calendar_id text NOT NULL,
    calendar_revision text NOT NULL,
    streak_status text NOT NULL CHECK (streak_status = ANY (ARRAY['EXACT'::text, 'LOWER_BOUND_ONLY'::text, 'NA_RESET'::text, 'UNKNOWN'::text])),
    exact_streak integer CHECK (exact_streak IS NULL OR exact_streak >= 0),
    lower_bound_streak integer CHECK (lower_bound_streak IS NULL OR lower_bound_streak >= 0),
    is_first_board text NOT NULL CHECK (is_first_board = ANY (ARRAY['TRUE'::text, 'FALSE'::text, 'NA'::text, 'UNKNOWN'::text])),
    prior_session_status text NOT NULL CHECK (prior_session_status = ANY (ARRAY['ZERO'::text, 'EXACT_STREAK'::text, 'LOWER_BOUND_ONLY'::text, 'NA_RESET'::text, 'LEFT_CENSORED'::text, 'UNKNOWN'::text])),
    close_limit_up text NOT NULL CHECK (close_limit_up = ANY (ARRAY['TRUE'::text, 'FALSE'::text, 'NA'::text, 'UNKNOWN'::text])),
    touched_limit_up text NOT NULL CHECK (touched_limit_up = ANY (ARRAY['TRUE'::text, 'FALSE'::text, 'NA'::text, 'UNKNOWN'::text])),
    one_word_limit_up text NOT NULL CHECK (one_word_limit_up = ANY (ARRAY['TRUE'::text, 'FALSE'::text, 'NA'::text, 'UNKNOWN'::text])),
    reason text NOT NULL,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT source_limit_up_streak_day_facts_uq UNIQUE (target_session, source_symbol, source_capture_id, contract_version, calendar_revision),
    CONSTRAINT source_limit_up_streak_status_value_check CHECK (
        (streak_status = 'EXACT' AND exact_streak IS NOT NULL AND lower_bound_streak = exact_streak)
        OR (streak_status = 'LOWER_BOUND_ONLY' AND exact_streak IS NULL AND lower_bound_streak IS NOT NULL AND lower_bound_streak >= 1)
        OR (streak_status IN ('NA_RESET', 'UNKNOWN') AND exact_streak IS NULL AND lower_bound_streak IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_source_limit_up_streak_session_eligible
    ON stock.source_limit_up_streak_day_facts (target_session, source_capture_id, research_eligible, streak_status);

COMMENT ON TABLE stock.source_limit_up_streak_day_facts IS 'MKT-04/§11.4 严格日线连板计数事实表：基于相邻官方市场会话的 RAW 涨跌停限价事实递推每只证券的精确连板数、左截尾/前日未知下界、首板状态与停牌 RESET 语义。';
COMMENT ON COLUMN stock.source_limit_up_streak_day_facts.id IS '自增代理主键。';
COMMENT ON COLUMN stock.source_limit_up_streak_day_facts.target_session IS '目标交易日（官方市场会话日期）。';
COMMENT ON COLUMN stock.source_limit_up_streak_day_facts.source_symbol IS '来源证券代码（如 SH.600000、SZ.000001）。';
COMMENT ON COLUMN stock.source_limit_up_streak_day_facts.market IS '证券所属市场枚举（SH/SZ/BJ）。';
COMMENT ON COLUMN stock.source_limit_up_streak_day_facts.board IS '证券所属板块枚举（MAIN_BOARD/GEM/STAR_BOARD/BEIJING_BOARD/B_SHARE/OTHER）。';
COMMENT ON COLUMN stock.source_limit_up_streak_day_facts.research_eligible IS '是否纳入正式全市场研究池（主板与创业板为 true，科创板等为 false）。';
COMMENT ON COLUMN stock.source_limit_up_streak_day_facts.source_capture_id IS '关联的 BaoStock 来源捕获编号，外键指向 stock.source_captures(capture_id)。';
COMMENT ON COLUMN stock.source_limit_up_streak_day_facts.price_coordinate IS '价格坐标，固定为 RAW_UNADJUSTED（真实未复权日线）。';
COMMENT ON COLUMN stock.source_limit_up_streak_day_facts.contract_version IS '连板算法合同版本（如 STOCK_A_LIMIT_UP_LADDER_R1）。';
COMMENT ON COLUMN stock.source_limit_up_streak_day_facts.calendar_id IS '官方交易日历标识（如 CN_SSE_SZSE_EQUITY）。';
COMMENT ON COLUMN stock.source_limit_up_streak_day_facts.calendar_revision IS '官方交易日历版本号。';
COMMENT ON COLUMN stock.source_limit_up_streak_day_facts.streak_status IS '连板计数确定性状态（EXACT=精确计数，LOWER_BOUND_ONLY=左截尾或前日未知仅下界，NA_RESET=停牌或无涨跌停重置，UNKNOWN=当日限价未知）。';
COMMENT ON COLUMN stock.source_limit_up_streak_day_facts.exact_streak IS '精确连板天数（单位：交易日；非涨停为 0，精确连板为 >=1，非 EXACT 状态为 NULL）。';
COMMENT ON COLUMN stock.source_limit_up_streak_day_facts.lower_bound_streak IS '已知连续涨停下界天数（单位：交易日；EXACT 时等于 exact_streak，LOWER_BOUND_ONLY 时 >=1，NA_RESET/UNKNOWN 时为 NULL）。';
COMMENT ON COLUMN stock.source_limit_up_streak_day_facts.is_first_board IS '首板判定三态枚举（TRUE=当日涨停且前会话明确为0或NA_RESET，FALSE=非首板，NA=停牌无限制，UNKNOWN=前会话未知或左截尾）。';
COMMENT ON COLUMN stock.source_limit_up_streak_day_facts.prior_session_status IS '前一官方会话连板前置状态（ZERO/EXACT_STREAK/LOWER_BOUND_ONLY/NA_RESET/LEFT_CENSORED/UNKNOWN）。';
COMMENT ON COLUMN stock.source_limit_up_streak_day_facts.close_limit_up IS '当日收盘涨停标志（TRUE/FALSE/NA/UNKNOWN）。';
COMMENT ON COLUMN stock.source_limit_up_streak_day_facts.touched_limit_up IS '当日最高价触及涨停标志（TRUE/FALSE/NA/UNKNOWN）。';
COMMENT ON COLUMN stock.source_limit_up_streak_day_facts.one_word_limit_up IS '当日一字涨停标志（TRUE/FALSE/NA/UNKNOWN）。';
COMMENT ON COLUMN stock.source_limit_up_streak_day_facts.reason IS '连板状态判定原因代码。';
COMMENT ON COLUMN stock.source_limit_up_streak_day_facts.created_at IS '记录写入时间戳（带时区，非业务交易时间）。';
