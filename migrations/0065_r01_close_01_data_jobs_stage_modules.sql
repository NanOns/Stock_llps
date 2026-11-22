-- R01-CLOSE-01：将涨跌停限价与四类后继研究模块加入data_jobs_selected_modules_check。
DO $$
BEGIN
    IF current_database() <> 'stock_a'
       OR current_schema() <> 'stock'
       OR session_user <> 'stock_a'
       OR current_user <> 'stock_a' THEN
        RAISE EXCEPTION 'R01_CLOSE_01_0065_TARGET_IDENTITY_REJECTED';
    END IF;
    IF (SELECT count(*) FROM stock.migration_ledger
        WHERE lineage = 'STOCK_A_INITIAL_R1'
          AND migration_id BETWEEN 1 AND 64) <> 64
       OR (SELECT count(DISTINCT migration_id) FROM stock.migration_ledger
           WHERE migration_id BETWEEN 1 AND 64) <> 64 THEN
        RAISE EXCEPTION 'R01_CLOSE_01_0065_MIGRATION_PREFIX_REJECTED';
    END IF;
END;
$$;

ALTER TABLE stock.data_jobs
    DROP CONSTRAINT data_jobs_selected_modules_check,
    ADD CONSTRAINT data_jobs_selected_modules_check CHECK (
        selected_modules <@ ARRAY[
            'BAOSTOCK_PRICES'::text,
            'TDX_SECTOR_MEMBERS'::text,
            'CORE_PROFILE'::text,
            'PRICE_LIMITS'::text,
            'MARKET_BREADTH'::text,
            'MARKET_PARTICIPATION'::text,
            'D0'::text,
            'D1'::text,
            'FOCUS_FORMAL'::text,
            'SECTOR_PULSE'::text,
            'SECTOR_TRANSITION'::text,
            'SECTOR_FOLLOWUP'::text,
            'FORWARD_PATH'::text,
            'COHORT_STATS'::text,
            'FEP_BASELINE'::text,
            'OBSERVATION_SEGMENTS'::text
        ]
    );

COMMENT ON CONSTRAINT data_jobs_selected_modules_check ON stock.data_jobs IS
    '校验数据初始化与日更任务选择的现役来源、基础画像/限价及后继研究模块枚举。';
