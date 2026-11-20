-- SOURCE-SWITCH-01：将现役D1和Seed研究范围扩展至科创板。
DO $$
BEGIN
    IF current_database() <> 'stock_a'
       OR current_schema() <> 'stock'
       OR session_user <> 'stock_a'
       OR current_user <> 'stock_a' THEN
        RAISE EXCEPTION 'SOURCE_SWITCH_0057_TARGET_IDENTITY_REJECTED';
    END IF;
    IF (SELECT count(*) FROM stock.migration_ledger
        WHERE lineage = 'STOCK_A_INITIAL_R1'
          AND migration_id BETWEEN 1 AND 56) <> 56
       OR (SELECT count(DISTINCT migration_id) FROM stock.migration_ledger
           WHERE migration_id BETWEEN 1 AND 56) <> 56 THEN
        RAISE EXCEPTION 'SOURCE_SWITCH_0057_MIGRATION_PREFIX_REJECTED';
    END IF;
END;
$$;

ALTER TABLE stock.source_d1_structure_day_facts
    DROP CONSTRAINT source_d1_structure_day_facts_board_check,
    ADD CONSTRAINT source_d1_structure_day_facts_baostock_board_check
        CHECK (board IN ('MAIN_BOARD', 'GEM', 'STAR_BOARD'));

COMMENT ON CONSTRAINT source_d1_structure_day_facts_baostock_board_check
    ON stock.source_d1_structure_day_facts IS
    '限制BaoStock研究证券范围为沪深主板、创业板与科创板，不含北交所。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.board IS
    'BaoStock来源证券板别，当前研究范围为MAIN_BOARD主板、GEM创业板和STAR_BOARD科创板。';

ALTER TABLE stock.source_pool_seed_day_facts
    ADD CONSTRAINT source_pool_seed_day_facts_baostock_identity_ck CHECK (
        source_symbol ~ '^(SH|SZ)\.[0-9]{6}$'
        AND market = left(source_symbol, 2)
        AND board IN ('MAIN_BOARD', 'GEM', 'STAR_BOARD')
        AND source_capture_id > 0
        AND profile_revision ~ '^PROFILE_R[0-9]{2}([|][1-9][0-9]?[|]([A-Za-z0-9_.~-]|%[0-9A-F]{2})+[|]([A-Za-z0-9_.~-]|%[0-9A-F]{2})+)?$'
        AND history_window_end = target_session
        AND history_window_start <= target_session
        AND history_session_count BETWEEN 1 AND 75
        AND calendar_id <> ''
        AND calendar_revision <> ''
        AND source_time_quality <> ''
        AND source_clock_role <> ''
        AND research_mode = 'NON_PIT_RETROSPECTIVE'
        AND price_coordinate = 'FORWARD_ADJUSTED'
    );

COMMENT ON CONSTRAINT source_pool_seed_day_facts_baostock_identity_ck
    ON stock.source_pool_seed_day_facts IS
    '校验BaoStock主板、创业板、科创板Seed研究键、窗口、质量和前复权坐标。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.board IS
    'BaoStock来源证券板别，当前研究范围为MAIN_BOARD主板、GEM创业板和STAR_BOARD科创板。';
