-- R08/业务10：记录初始化/日更预设与实际读回来源、算法和窗口身份。
ALTER TABLE stock.data_jobs
    ADD COLUMN initialization_mode text NOT NULL DEFAULT 'REUSE_RAW'
        CHECK (initialization_mode IN ('REUSE_RAW', 'RESET_ALL_BUSINESS_FACTS')),
    ADD COLUMN daily_mode text NOT NULL DEFAULT 'CUSTOM'
        CHECK (daily_mode IN ('FULL_THREE_LAYERS', 'SOURCE_ONLY', 'CUSTOM')),
    ADD COLUMN profile_history_session_count integer
        CHECK (profile_history_session_count IS NULL OR profile_history_session_count BETWEEN 1 AND 32767),
    ADD COLUMN reset_preview_fingerprint text
        CHECK (reset_preview_fingerprint IS NULL OR reset_preview_fingerprint ~ '^[0-9a-f]{64}$'),
    ADD CONSTRAINT data_jobs_layer_mode_check CHECK (
        (mode = 'INITIALIZE' AND daily_mode = 'CUSTOM')
        OR (mode = 'MANUAL_DAILY' AND initialization_mode = 'REUSE_RAW')
    );

COMMENT ON COLUMN stock.data_jobs.initialization_mode IS
    '初始化方式：REUSE_RAW复用基础来源补算派生，RESET_ALL_BUSINESS_FACTS经用户双重确认后清理本项目业务事实并重建。';
COMMENT ON COLUMN stock.data_jobs.daily_mode IS
    '手动日更方式：FULL_THREE_LAYERS完整三层、SOURCE_ONLY仅来源增量、CUSTOM为用户自选模块；初始化任务固定CUSTOM。';
COMMENT ON COLUMN stock.data_jobs.profile_history_session_count IS
    'Core/Profile及其下游计算窗口的官方历史会话数；与Raw基础历史和各模块输出会话数独立。';
COMMENT ON COLUMN stock.data_jobs.reset_preview_fingerprint IS
    '用户确认的完整业务事实影响预览SHA-256指纹；只用于在同一任务恢复时核对原始删除范围。';

ALTER TABLE stock.data_job_progress
    ADD COLUMN source_package_id bigint
        REFERENCES stock.source_packages(source_package_id) ON DELETE RESTRICT,
    ADD COLUMN source_capture_id bigint
        REFERENCES stock.source_captures(capture_id) ON DELETE RESTRICT,
    ADD COLUMN baostock_capture_id uuid,
    ADD COLUMN profile_revision text,
    ADD COLUMN producer_version text,
    ADD COLUMN profile_history_session_count integer
        CHECK (profile_history_session_count IS NULL OR profile_history_session_count >= 1),
    ADD COLUMN d1_history_session_count integer
        CHECK (d1_history_session_count IS NULL OR d1_history_session_count >= 1),
    ADD COLUMN calendar_revision text,
    ADD COLUMN industry_snapshot_id bigint
        REFERENCES stock.tdx_sector_source_snapshots(snapshot_id) ON DELETE RESTRICT,
    ADD COLUMN concept_snapshot_id bigint
        REFERENCES stock.tdx_sector_source_snapshots(snapshot_id) ON DELETE RESTRICT;

COMMENT ON COLUMN stock.data_job_progress.source_package_id IS
    '本阶段结果绑定的TDX来源包数字ID；BaoStock独立捕获另用UUID。';
COMMENT ON COLUMN stock.data_job_progress.source_capture_id IS
    '本阶段结果绑定的精确TDX来源捕获数字ID；多个捕获不得合并计为完成。';
COMMENT ON COLUMN stock.data_job_progress.baostock_capture_id IS
    '本阶段BaoStock来源行绑定的实际查询捕获UUID；非BaoStock阶段为空。';
COMMENT ON COLUMN stock.data_job_progress.profile_revision IS
    '本阶段依赖的精确Core/Profile语义修订；不等同于数据来源包版本。';
COMMENT ON COLUMN stock.data_job_progress.producer_version IS
    '本阶段实际PG业务事实记录的生产者或算法合同版本。';
COMMENT ON COLUMN stock.data_job_progress.profile_history_session_count IS
    '本阶段实际使用的Profile官方历史输入窗口会话数；不是跨输出日预热并集长度。';
COMMENT ON COLUMN stock.data_job_progress.d1_history_session_count IS
    '本阶段D1实际使用的官方历史输入窗口会话数。';
COMMENT ON COLUMN stock.data_job_progress.calendar_revision IS
    '本阶段实际采用的官方交易日历修订。';
COMMENT ON COLUMN stock.data_job_progress.industry_snapshot_id IS
    '本阶段行业PULSE依赖的精确TDX行业成员快照数字ID；研究含NON_PIT时保留该身份。';
COMMENT ON COLUMN stock.data_job_progress.concept_snapshot_id IS
    '本阶段GN概念PULSE依赖的精确TDX概念成员快照数字ID；研究含NON_PIT时保留该身份。';
