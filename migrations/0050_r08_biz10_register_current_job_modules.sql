-- R08/业务10：将当前运营菜单中接通的行业/GN成员与相邻变化模块登记到任务合同。
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
            'SECTOR_FOLLOWUP'
        ]::text[]
    );

COMMENT ON COLUMN stock.data_jobs.source_version_fingerprint IS
    '本任务输入绑定的来源包/捕获、窗口、日历与现役算法合同身份SHA-256；同日期异来源、异窗口或异算法版本不得误用旧完成状态。';
