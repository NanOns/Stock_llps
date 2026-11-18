-- R08/业务09：为现有单Runner任务记录实际模块、范围操作和输入版本，并显式记录派生清理。
ALTER TABLE stock.data_jobs
    ADD COLUMN operation text NOT NULL DEFAULT 'FILL_MISSING'
        CHECK (operation IN ('FILL_MISSING', 'RECOMPUTE', 'CLEAR_DERIVED', 'CLEAR_AND_REGENERATE')),
    ADD COLUMN selected_modules text[] NOT NULL DEFAULT ARRAY[]::text[]
        CHECK (selected_modules <@ ARRAY[
            'TDX_RAW', 'BAOSTOCK', 'CORE_PROFILE', 'MARKET_BREADTH',
            'MARKET_PARTICIPATION', 'D0', 'D1', 'FOCUS_FORMAL',
            'SECTOR_PULSE', 'SECTOR_FOLLOWUP'
        ]::text[]),
    ADD COLUMN module_output_session_counts smallint[] NOT NULL DEFAULT ARRAY[]::smallint[]
        CHECK (
            cardinality(selected_modules) = cardinality(module_output_session_counts)
            AND (cardinality(module_output_session_counts) = 0
                 OR 0 < ALL (module_output_session_counts))
        ),
    ADD COLUMN source_version_fingerprint text NOT NULL DEFAULT repeat('0', 64)
        CHECK (source_version_fingerprint ~ '^[0-9a-f]{64}$');

COMMENT ON COLUMN stock.data_jobs.operation
    IS '本任务对已选模块的执行方式：补缺、按范围重算、清理派生结果或清理后重算。';
COMMENT ON COLUMN stock.data_jobs.selected_modules
    IS '用户明确选择的现役模块代码；数组仅登记实际选择，不代表未执行模块已完成。';
COMMENT ON COLUMN stock.data_jobs.module_output_session_counts
    IS '与selected_modules同序的每模块输出官方会话数；仅表示输出范围，不含算法预热历史。';
COMMENT ON COLUMN stock.data_jobs.source_version_fingerprint
    IS '本任务输入Raw会话的source_package_id与source_capture_id有序清单SHA-256；捕获身份变化生成不同幂等键。';

ALTER TABLE stock.data_job_progress
    ADD COLUMN deleted_rows bigint
        CHECK (deleted_rows IS NULL OR deleted_rows >= 0);

COMMENT ON COLUMN stock.data_job_progress.deleted_rows
    IS '仅派生清理任务使用：事务提交后实际删除并独立确认的行数；普通生成任务为空。';

ALTER TABLE stock.data_job_progress
    DROP CONSTRAINT data_job_progress_status_check,
    ADD CONSTRAINT data_job_progress_status_check CHECK (
        status IN (
            'NOT_STARTED', 'RUNNING', 'COMPLETED', 'ALREADY_COMPLETE', 'NO_CHANGE',
            'NOT_COMPUTED', 'INCOMPLETE', 'BLOCKED', 'FAILED', 'INTERRUPTED',
            'SKIPPED_BY_USER', 'SKIPPED_NOT_IMPLEMENTED', 'NOT_DUE', 'CLEARED'
        )
    );

COMMENT ON COLUMN stock.data_job_progress.status
    IS '阶段/表单元状态；包含截止前不具备后继期限的NOT_DUE及已清理CLEARED，并区分零变化、未计算、未知分母、失败和跳过。';
