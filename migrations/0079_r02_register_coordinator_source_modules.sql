ALTER TABLE stock.data_jobs
    DROP CONSTRAINT data_jobs_selected_modules_check,
    ADD CONSTRAINT data_jobs_selected_modules_check CHECK (
        selected_modules <@ ARRAY[
            'BAOSTOCK_PRICES'::text,
            'BAOSTOCK_UNIVERSE'::text,
            'BAOSTOCK_MARKET_INDICES'::text,
            'TDX_SECTOR_MEMBERS'::text,
            'CORE_PROFILE'::text,
            'CROSS_SECTION'::text,
            'PRICE_LIMITS'::text,
            'LIMIT_UP_LADDER'::text,
            'MARKET_BREADTH'::text,
            'MARKET_PARTICIPATION'::text,
            'MARKET_INDICES_ENV'::text,
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
    '仅允许现役DataJobCoordinator来源、基础和研究模块枚举；所选模块须能由唯一协调器正常登记并恢复。';
