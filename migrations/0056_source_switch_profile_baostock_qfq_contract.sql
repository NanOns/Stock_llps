-- SOURCE-SWITCH-01：把来源代码画像表的旧TDX/RAW约束切换为BaoStock/QFQ。
DO $$
BEGIN
    IF current_database() <> 'stock_a'
       OR current_schema() <> 'stock'
       OR session_user <> 'stock_a'
       OR current_user <> 'stock_a' THEN
        RAISE EXCEPTION 'SOURCE_SWITCH_0056_TARGET_IDENTITY_REJECTED';
    END IF;
    IF (SELECT count(*) FROM stock.migration_ledger
        WHERE lineage = 'STOCK_A_INITIAL_R1'
          AND migration_id BETWEEN 1 AND 55) <> 55
       OR (SELECT count(DISTINCT migration_id) FROM stock.migration_ledger
           WHERE migration_id BETWEEN 1 AND 55) <> 55 THEN
        RAISE EXCEPTION 'SOURCE_SWITCH_0056_MIGRATION_PREFIX_REJECTED';
    END IF;
    IF EXISTS (SELECT 1 FROM stock.source_symbol_price_profiles) THEN
        RAISE EXCEPTION 'SOURCE_SWITCH_0056_PROFILE_FACTS_MUST_BE_RESET';
    END IF;
END;
$$;

ALTER TABLE stock.source_symbol_price_profiles
    DROP CONSTRAINT source_symbol_price_profiles_board_check,
    ADD CONSTRAINT source_symbol_price_profiles_board_baostock_check
        CHECK (board IN ('MAIN_BOARD', 'GEM', 'STAR_BOARD')),
    DROP CONSTRAINT source_symbol_price_profiles_price_coordinate_check,
    ADD CONSTRAINT source_symbol_price_profiles_price_coordinate_baostock_check
        CHECK (price_coordinate = 'FORWARD_ADJUSTED'),
    DROP CONSTRAINT source_symbol_price_profiles_target_bar_status_check,
    ADD CONSTRAINT source_symbol_price_profiles_target_bar_status_baostock_check
        CHECK (
            target_bar_status IN (
                'BAOSTOCK_TARGET_BAR_PRESENT',
                'BAOSTOCK_TARGET_BAR_MISSING',
                'BAOSTOCK_READ_FAILED',
                'HISTORY_ROW_NOT_STORED'
            )
        ),
    DROP CONSTRAINT source_symbol_price_profiles_amount_unit_status_check,
    ADD CONSTRAINT source_symbol_price_profiles_amount_unit_baostock_check
        CHECK (amount_unit_status = 'CNY_VERIFIED'),
    DROP CONSTRAINT source_symbol_price_profiles_volume_unit_status_check,
    ADD CONSTRAINT source_symbol_price_profiles_volume_unit_baostock_check
        CHECK (volume_unit_status = 'SHARES_VERIFIED');

COMMENT ON TABLE stock.source_symbol_price_profiles IS
    '保存BaoStock前复权价格计算的来源代码级Core43/Profile11画像；按目标会话、捕获和修订绑定，均为NON_PIT后验研究，不签发稳定证券身份。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.board IS
    'BaoStock来源证券板别；当前画像范围仅包含沪深主板、创业板和科创板，不包含北交所。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.price_coordinate IS
    '画像所用价格坐标；现役生产值固定为BaoStock FORWARD_ADJUSTED前复权。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.target_bar_status IS
    '目标会话BaoStock日线状态：BAOSTOCK_TARGET_BAR_PRESENT为有目标Bar，BAOSTOCK_TARGET_BAR_MISSING为请求完成但无Bar，BAOSTOCK_READ_FAILED为来源读取失败，HISTORY_ROW_NOT_STORED为PG缺少历史行。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.amount_unit_status IS
    '画像目标日成交额单位核验；现役BaoStock单位合同固定为CNY_VERIFIED（人民币元）。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.volume_unit_status IS
    '画像目标日成交量单位核验；现役BaoStock单位合同固定为SHARES_VERIFIED（股）。';
