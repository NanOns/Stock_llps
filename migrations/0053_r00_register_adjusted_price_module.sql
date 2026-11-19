-- R00：允许现有初始化/日更任务合同选择已接通的仿射调整价格模块。
ALTER TABLE stock.data_jobs
    DROP CONSTRAINT data_jobs_selected_modules_check,
    ADD CONSTRAINT data_jobs_selected_modules_check CHECK (
        selected_modules <@ ARRAY[
            'TDX_RAW',
            'TDX_SECTOR_MEMBERS',
            'BAOSTOCK',
            'CORE_PROFILE',
            'MARKET_BREADTH',
            'MARKET_PARTICIPATION',
            'D0',
            'D1',
            'FOCUS_FORMAL',
            'SECTOR_PULSE',
            'SECTOR_TRANSITION',
            'SECTOR_FOLLOWUP',
            'ADJUSTED_PRICE'
        ]::text[]
    );

COMMENT ON COLUMN stock.data_jobs.selected_modules IS
    '本次用户明确选择的现役来源、基础或研究模块代码数组；只允许登记在当前运营菜单合同中的模块，包括ADJUSTED_PRICE仿射复权价格。';
