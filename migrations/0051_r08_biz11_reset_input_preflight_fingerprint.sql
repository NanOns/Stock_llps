-- R08/业务11：将重置前已核验的Raw包与板块来源身份绑定到同一任务。
ALTER TABLE stock.data_jobs
    ADD COLUMN reset_preflight_fingerprint text
        CHECK (
            reset_preflight_fingerprint IS NULL
            OR reset_preflight_fingerprint ~ '^[0-9a-f]{64}$'
        );

COMMENT ON COLUMN stock.data_jobs.reset_preflight_fingerprint IS
    '全业务事实重置前逐会话验证的Raw包摘要、官方会话范围及只读行业/GN来源摘要；中断重试必须复用同一身份。';
