-- 为 data_job_progress 补充 R03 正式市场双端 Universe 完成键及 R03 板块实际端点/前态输入引用（仅最少必要 typed 字段）。

ALTER TABLE stock.data_job_progress
    ADD COLUMN IF NOT EXISTS universe_version_t text NULL,
    ADD COLUMN IF NOT EXISTS universe_version_t1 text NULL,
    ADD COLUMN IF NOT EXISTS universe_version_t3 text NULL,
    ADD COLUMN IF NOT EXISTS source_capture_id_t1 bigint NULL,
    ADD COLUMN IF NOT EXISTS source_capture_id_t3 bigint NULL,
    ADD COLUMN IF NOT EXISTS profile_revision_t1 text NULL,
    ADD COLUMN IF NOT EXISTS profile_revision_t3 text NULL,
    ADD COLUMN IF NOT EXISTS prev_scope_key text NULL,
    ADD COLUMN IF NOT EXISTS prev_member_version text NULL;

CREATE INDEX IF NOT EXISTS data_job_progress_r03_market_lookup_ix
    ON stock.data_job_progress (
        target_session,
        business_table,
        producer_version,
        source_capture_id,
        profile_revision,
        universe_version_t,
        universe_version_t3
    )
    WHERE business_table = 'stock.r03_formal_market_breadth_facts';

COMMENT ON COLUMN stock.data_job_progress.universe_version_t IS
    't 日正式端点实际消费的 Research Universe 版本号；正式市场宽度五元完成键及 R03 板块 t 端依赖引用。';
COMMENT ON COLUMN stock.data_job_progress.universe_version_t1 IS
    't-1 端点实际消费的 Research Universe 版本号；用于 R03 板块精确依赖失效。';
COMMENT ON COLUMN stock.data_job_progress.universe_version_t3 IS
    't-3 端点实际消费的 Research Universe 版本号；正式市场宽度五元完成键及 R03 板块 t-3 端依赖引用。';
COMMENT ON COLUMN stock.data_job_progress.source_capture_id_t1 IS
    't-1 端点实际消费的来源捕获ID；用于 R03 板块跨版端点精确依赖失效。';
COMMENT ON COLUMN stock.data_job_progress.source_capture_id_t3 IS
    't-3 端点实际消费的来源捕获ID；用于 R03 板块跨版端点精确依赖失效。';
COMMENT ON COLUMN stock.data_job_progress.profile_revision_t1 IS
    't-1 端点实际消费的 QFQ Profile 修订号；用于 R03 板块跨版端点精确依赖失效。';
COMMENT ON COLUMN stock.data_job_progress.profile_revision_t3 IS
    't-3 端点实际消费的 QFQ Profile 修订号；用于 R03 板块与正式市场跨版端点精确依赖失效。';
COMMENT ON COLUMN stock.data_job_progress.prev_scope_key IS
    '当前日级 R03 分区实际消费的上一交易日或前态 scope_key；用于跨版前态依赖链追踪。';
COMMENT ON COLUMN stock.data_job_progress.prev_member_version IS
    '当前日级 R03 分区实际消费的上一交易日或前态 member_version；用于跨版前态依赖链追踪。';
