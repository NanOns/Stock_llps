-- R01-CLOSE-01：修正正式研究板别范围（仅主板与创业板）、放宽动态N历史窗口上限至250，并创建日线涨跌停限价事实表。
DO $$
BEGIN
    IF current_database() <> 'stock_a'
       OR current_schema() <> 'stock'
       OR session_user <> 'stock_a'
       OR current_user <> 'stock_a' THEN
        RAISE EXCEPTION 'R01_CLOSE_01_0064_TARGET_IDENTITY_REJECTED';
    END IF;
    IF (SELECT count(*) FROM stock.migration_ledger
        WHERE lineage = 'STOCK_A_INITIAL_R1'
          AND migration_id BETWEEN 1 AND 63) <> 63
       OR (SELECT count(DISTINCT migration_id) FROM stock.migration_ledger
           WHERE migration_id BETWEEN 1 AND 63) <> 63 THEN
        RAISE EXCEPTION 'R01_CLOSE_01_0064_MIGRATION_PREFIX_REJECTED';
    END IF;
END;
$$;

-- 清理正式研究事实表中遗留的科创板（STAR_BOARD）行；底层来源表与基础画像表保留科创板。
DELETE FROM stock.source_d0_formal_admission_day_facts
WHERE source_symbol ~ '^SH\.68[89][0-9]{3}$';

DELETE FROM stock.source_t0_early_observation_facts
WHERE source_symbol ~ '^SH\.68[89][0-9]{3}$';

DELETE FROM stock.source_research_enrollments
WHERE rule_fact_id IN (
    SELECT id FROM stock.source_rule_evaluations WHERE source_symbol ~ '^SH\.68[89][0-9]{3}$'
)
   OR previous_rule_fact_id IN (
    SELECT id FROM stock.source_rule_evaluations WHERE source_symbol ~ '^SH\.68[89][0-9]{3}$'
)
   OR d0_fact_id IN (
    SELECT id FROM stock.source_d0_launch_day_facts WHERE source_symbol ~ '^SH\.68[89][0-9]{3}$'
)
   OR adjacent_d0_fact_id IN (
    SELECT id FROM stock.source_d0_launch_day_facts WHERE source_symbol ~ '^SH\.68[89][0-9]{3}$'
);

DELETE FROM stock.source_rule_evaluations
WHERE source_symbol ~ '^SH\.68[89][0-9]{3}$';

DELETE FROM stock.source_research_events
WHERE source_symbol ~ '^SH\.68[89][0-9]{3}$';

DELETE FROM stock.source_forward_path_facts
WHERE source_symbol ~ '^SH\.68[89][0-9]{3}$';

DELETE FROM stock.source_d0_launch_day_facts
WHERE source_symbol ~ '^SH\.68[89][0-9]{3}$';

DELETE FROM stock.source_d1_material_anchor_day_facts
WHERE d1_structure_id IN (
    SELECT id FROM stock.source_d1_structure_day_facts
    WHERE board = 'STAR_BOARD' OR source_symbol ~ '^SH\.68[89][0-9]{3}$'
);

DELETE FROM stock.source_d1_structure_day_facts
WHERE board = 'STAR_BOARD' OR source_symbol ~ '^SH\.68[89][0-9]{3}$';

DELETE FROM stock.source_pool_seed_day_facts
WHERE board = 'STAR_BOARD' OR source_symbol ~ '^SH\.68[89][0-9]{3}$';

-- 修正D1与Seed板别范围：仅允许沪深主板（MAIN_BOARD）和创业板（GEM），不含科创板。
ALTER TABLE stock.source_d1_structure_day_facts
    DROP CONSTRAINT source_d1_structure_day_facts_baostock_board_check,
    ADD CONSTRAINT source_d1_structure_day_facts_baostock_board_check
        CHECK (board IN ('MAIN_BOARD', 'GEM'));

COMMENT ON CONSTRAINT source_d1_structure_day_facts_baostock_board_check
    ON stock.source_d1_structure_day_facts IS
    '限制正式D1研究范围为沪深主板（MAIN_BOARD）与创业板（GEM），不含科创板和北交所。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.board IS
    '正式研究证券板别，仅允许MAIN_BOARD主板与GEM创业板。';

-- 修正Seed板别范围并放宽动态历史窗口上限至250会话。
ALTER TABLE stock.source_pool_seed_day_facts
    DROP CONSTRAINT source_pool_seed_day_facts_baostock_identity_ck,
    ADD CONSTRAINT source_pool_seed_day_facts_baostock_identity_ck CHECK (
        source_symbol ~ '^(SH|SZ)\.[0-9]{6}$'
        AND market = left(source_symbol, 2)
        AND board IN ('MAIN_BOARD', 'GEM')
        AND source_capture_id > 0
        AND profile_revision ~ '^PROFILE_R[0-9]{2}([|][1-9][0-9]{0,2}[|]([A-Za-z0-9_.~-]|%[0-9A-F]{2})+[|]([A-Za-z0-9_.~-]|%[0-9A-F]{2})+)?$'
        AND history_window_end = target_session
        AND history_window_start <= target_session
        AND history_session_count BETWEEN 1 AND 250
        AND calendar_id <> ''
        AND calendar_revision <> ''
        AND source_time_quality <> ''
        AND source_clock_role <> ''
        AND research_mode = 'NON_PIT_RETROSPECTIVE'
        AND price_coordinate = 'FORWARD_ADJUSTED'
    );

COMMENT ON CONSTRAINT source_pool_seed_day_facts_baostock_identity_ck
    ON stock.source_pool_seed_day_facts IS
    '校验主板与创业板Seed研究键、1至250会话窗口、质量和前复权坐标。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.board IS
    '正式研究证券板别，仅允许MAIN_BOARD主板与GEM创业板。';

-- 放宽剩余研究表中的75会话历史窗口上限至250会话，与动态N配置一致。
ALTER TABLE stock.source_research_events
    DROP CONSTRAINT source_research_events_d1_history_session_count_check,
    ADD CONSTRAINT source_research_events_d1_history_session_count_check
        CHECK (d1_history_session_count BETWEEN 1 AND 250);

ALTER TABLE stock.source_rule_evaluations
    DROP CONSTRAINT source_rule_evaluations_d1_history_session_count_check,
    ADD CONSTRAINT source_rule_evaluations_d1_history_session_count_check
        CHECK (d1_history_session_count BETWEEN 1 AND 250);

ALTER TABLE stock.source_s2_t1_cohort_stats
    DROP CONSTRAINT source_s2_t1_cohort_source_scope_ck,
    ADD CONSTRAINT source_s2_t1_cohort_source_scope_ck CHECK (
        source_pool_count > 0
        AND member_count BETWEEN 0 AND source_pool_count
        AND t0_history_session_count BETWEEN 1 AND 250
        AND endpoint_history_session_count BETWEEN 1 AND 250
    );

-- 创建MATH:ALG-04日线涨跌停限价事实表（基于RAW_UNADJUSTED未复权价格与已证实制度子域）。
CREATE TABLE stock.source_price_limit_day_facts (
    id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    target_session date NOT NULL,
    source_symbol text NOT NULL,
    market text NOT NULL CHECK (market IN ('SH', 'SZ', 'BJ')),
    board text NOT NULL CHECK (board IN ('MAIN_BOARD', 'GEM', 'STAR_BOARD', 'BEIJING_BOARD', 'B_SHARE', 'OTHER')),
    research_eligible boolean NOT NULL,
    source_capture_id bigint NOT NULL REFERENCES stock.source_captures(capture_id),
    price_coordinate text NOT NULL CHECK (price_coordinate = 'RAW_UNADJUSTED'),
    calendar_id text NOT NULL,
    calendar_revision text NOT NULL,
    contract_version text NOT NULL,
    rule_id text NOT NULL,
    limit_status text NOT NULL CHECK (limit_status IN ('ASSESSABLE', 'NA_SUSPENDED', 'UNKNOWN')),
    limit_pct numeric(8, 4),
    reference_price numeric(18, 6),
    upper_limit numeric(18, 2),
    lower_limit numeric(18, 2),
    close_limit_up text NOT NULL CHECK (close_limit_up IN ('TRUE', 'FALSE', 'NA', 'UNKNOWN')),
    close_limit_down text NOT NULL CHECK (close_limit_down IN ('TRUE', 'FALSE', 'NA', 'UNKNOWN')),
    touched_limit_up text NOT NULL CHECK (touched_limit_up IN ('TRUE', 'FALSE', 'NA', 'UNKNOWN')),
    touched_limit_down text NOT NULL CHECK (touched_limit_down IN ('TRUE', 'FALSE', 'NA', 'UNKNOWN')),
    one_word_limit_up text NOT NULL CHECK (one_word_limit_up IN ('TRUE', 'FALSE', 'NA', 'UNKNOWN')),
    one_word_limit_down text NOT NULL CHECK (one_word_limit_down IN ('TRUE', 'FALSE', 'NA', 'UNKNOWN')),
    quality text NOT NULL,
    reason text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT source_price_limit_day_facts_uq UNIQUE (
        target_session, source_symbol, source_capture_id, contract_version, calendar_revision
    )
);

CREATE INDEX source_price_limit_day_facts_market_idx
    ON stock.source_price_limit_day_facts (
        target_session, source_capture_id, contract_version, research_eligible
    );

COMMENT ON TABLE stock.source_price_limit_day_facts IS
    '日线涨跌停限价与触及/收盘/一字可判定事实表，仅使用RAW_UNADJUSTED未复权价格与已证实制度子域。';
COMMENT ON COLUMN stock.source_price_limit_day_facts.id IS '自增代理主键。';
COMMENT ON COLUMN stock.source_price_limit_day_facts.target_session IS '目标官方交易会话日期。';
COMMENT ON COLUMN stock.source_price_limit_day_facts.source_symbol IS '来源证券代码（如SH.600000）。';
COMMENT ON COLUMN stock.source_price_limit_day_facts.market IS '交易所市场代码（SH/SZ/BJ）。';
COMMENT ON COLUMN stock.source_price_limit_day_facts.board IS '证券板别（MAIN_BOARD/GEM/STAR_BOARD等）。';
COMMENT ON COLUMN stock.source_price_limit_day_facts.research_eligible IS '是否属于正式研究范围（沪深主板或创业板）。';
COMMENT ON COLUMN stock.source_price_limit_day_facts.source_capture_id IS '绑定的BaoStock来源捕获ID。';
COMMENT ON COLUMN stock.source_price_limit_day_facts.price_coordinate IS '价格坐标，限价判定固定为RAW_UNADJUSTED未复权。';
COMMENT ON COLUMN stock.source_price_limit_day_facts.calendar_id IS '官方交易日历ID。';
COMMENT ON COLUMN stock.source_price_limit_day_facts.calendar_revision IS '官方交易日历修订号。';
COMMENT ON COLUMN stock.source_price_limit_day_facts.contract_version IS '限价引擎合同版本号。';
COMMENT ON COLUMN stock.source_price_limit_day_facts.rule_id IS '适用的涨跌停制度规则ID或未知/停牌原因标识。';
COMMENT ON COLUMN stock.source_price_limit_day_facts.limit_status IS '限价可判定状态：ASSESSABLE可判定、NA_SUSPENDED停牌无交易、UNKNOWN规则或参考价未知。';
COMMENT ON COLUMN stock.source_price_limit_day_facts.limit_pct IS '涨跌幅比例（如0.1000表示10%、0.2000表示20%），未知或停牌时为空。';
COMMENT ON COLUMN stock.source_price_limit_day_facts.reference_price IS '已核验的未复权前收参考价（元），未知或停牌时为空。';
COMMENT ON COLUMN stock.source_price_limit_day_facts.upper_limit IS '按0.01元tick HALF_UP舍入后的理论涨停价（元）。';
COMMENT ON COLUMN stock.source_price_limit_day_facts.lower_limit IS '按0.01元tick HALF_UP舍入后的理论跌停价（元）。';
COMMENT ON COLUMN stock.source_price_limit_day_facts.close_limit_up IS '收盘是否涨停（三态+NA）。';
COMMENT ON COLUMN stock.source_price_limit_day_facts.close_limit_down IS '收盘是否跌停（三态+NA）。';
COMMENT ON COLUMN stock.source_price_limit_day_facts.touched_limit_up IS '日内最高价是否触及涨停（三态+NA）。';
COMMENT ON COLUMN stock.source_price_limit_day_facts.touched_limit_down IS '日内最低价是否触及跌停（三态+NA）。';
COMMENT ON COLUMN stock.source_price_limit_day_facts.one_word_limit_up IS '是否日线一字涨停（O=H=L=C=涨停价，三态+NA）。';
COMMENT ON COLUMN stock.source_price_limit_day_facts.one_word_limit_down IS '是否日线一字跌停（O=H=L=C=跌停价，三态+NA）。';
COMMENT ON COLUMN stock.source_price_limit_day_facts.quality IS '限价事实质量标记（VERIFIED_RAW_NON_PIT/NA_SUSPENDED/UNKNOWN）。';
COMMENT ON COLUMN stock.source_price_limit_day_facts.reason IS '可判定性、停牌或未知原因说明代码。';
COMMENT ON COLUMN stock.source_price_limit_day_facts.created_at IS '记录写入时间戳。';
