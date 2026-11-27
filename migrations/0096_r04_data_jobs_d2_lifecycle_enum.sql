-- R04 第二轮前向迁移（0096）：在 stock.data_jobs.data_jobs_selected_modules_check 中允许 D2_LIFECYCLE 模块。

ALTER TABLE stock.data_jobs
    DROP CONSTRAINT IF EXISTS data_jobs_selected_modules_check;

ALTER TABLE stock.data_jobs
    ADD CONSTRAINT data_jobs_selected_modules_check
        CHECK (
            selected_modules <@ ARRAY[
                'BAOSTOCK_PRICES',
                'BAOSTOCK_UNIVERSE',
                'BAOSTOCK_MARKET_INDICES',
                'TDX_SECTOR_MEMBERS',
                'CORE_PROFILE',
                'CROSS_SECTION',
                'PRICE_LIMITS',
                'LIMIT_UP_LADDER',
                'MARKET_BREADTH',
                'MARKET_PARTICIPATION',
                'MARKET_INDICES_ENV',
                'D0',
                'D1',
                'FOCUS_FORMAL',
                'SECTOR_PULSE',
                'SECTOR_TRANSITION',
                'SECTOR_FOLLOWUP',
                'R03_SECTOR_NATIVE',
                'R03_SECTOR_ROTATION',
                'R03_SECTOR_OVERLAP',
                'R03_SECTOR_LOO',
                'D2_LIFECYCLE',
                'FORWARD_PATH',
                'COHORT_STATS',
                'FEP_BASELINE',
                'OBSERVATION_SEGMENTS'
            ]::text[]
        );
