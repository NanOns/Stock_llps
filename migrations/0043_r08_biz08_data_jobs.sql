-- R08/业务08：保存用户显式启动的数据任务及已提交读回的阶段/业务表进度。
-- 同一迁移同时解除Core/Profile历史窗口旧75会话存储上限；实际范围仍由官方日历与来源覆盖决定。
ALTER TABLE stock.source_symbol_price_profiles
    DROP CONSTRAINT source_symbol_price_profiles_history_requested_session_co_check,
    DROP CONSTRAINT source_symbol_price_profiles_history_stored_session_count_check,
    DROP CONSTRAINT source_symbol_price_profiles_history_actual_price_bar_cou_check;

ALTER TABLE stock.source_symbol_price_profiles
    ADD CONSTRAINT source_symbol_profiles_history_requested_range_ck
        CHECK (history_requested_session_count BETWEEN 1 AND 32767),
    ADD CONSTRAINT source_symbol_profiles_history_stored_range_ck
        CHECK (history_stored_session_count BETWEEN 0 AND 32767),
    ADD CONSTRAINT source_symbol_profiles_history_actual_bars_range_ck
        CHECK (history_actual_price_bar_count BETWEEN 0 AND 32767);

COMMENT ON COLUMN stock.source_symbol_price_profiles.history_requested_session_count
    IS '按显式官方日历请求的历史会话数，正整数且不超过smallint存储范围；不表示实际有效Bar数。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.history_stored_session_count
    IS 'PG中实际读到的窗口Raw会话行数；小于请求数表示存在未持久会话，不表示有效Bar数量。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.history_actual_price_bar_count
    IS '该来源代码在请求历史窗口内实际存在的合法价格Bar数；可小于请求会话数。';

CREATE TABLE stock.data_jobs (
    job_id uuid PRIMARY KEY,
    request_key text NOT NULL UNIQUE CHECK (length(request_key) BETWEEN 1 AND 1024),
    mode text NOT NULL CHECK (mode IN ('INITIALIZE', 'MANUAL_DAILY')),
    target_session date NOT NULL,
    research_cutoff date NOT NULL,
    history_session_count integer NOT NULL CHECK (history_session_count BETWEEN 1 AND 32767),
    d0_output_session_count integer NOT NULL CHECK (d0_output_session_count BETWEEN 0 AND 32767),
    d1_output_session_count integer NOT NULL CHECK (d1_output_session_count BETWEEN 0 AND 32767),
    calendar_id text NOT NULL CHECK (btrim(calendar_id) <> ''),
    calendar_revision text NOT NULL CHECK (btrim(calendar_revision) <> ''),
    status text NOT NULL CHECK (status IN ('RUNNING', 'COMPLETED', 'NO_CHANGE', 'INCOMPLETE', 'BLOCKED', 'FAILED', 'INTERRUPTED')),
    planned_units integer NOT NULL CHECK (planned_units >= 0),
    completed_units integer NOT NULL DEFAULT 0 CHECK (completed_units >= 0),
    source_package_id bigint,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    started_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    last_error_code text,
    last_error_message text,
    CONSTRAINT data_jobs_completed_units_ck CHECK (completed_units <= planned_units),
    CONSTRAINT data_jobs_source_package_fk FOREIGN KEY (source_package_id)
        REFERENCES stock.source_packages(source_package_id) ON DELETE RESTRICT
);

CREATE UNIQUE INDEX data_jobs_one_running_ix
    ON stock.data_jobs ((status))
    WHERE status = 'RUNNING';
CREATE INDEX data_jobs_latest_ix
    ON stock.data_jobs (created_at DESC, job_id);

COMMENT ON TABLE stock.data_jobs IS '用户明确触发的初始化或手动日更任务主状态；不保存行情副本或通用payload。';
COMMENT ON COLUMN stock.data_jobs.job_id IS '任务唯一标识；与任务请求及其阶段/表进度行关联。';
COMMENT ON COLUMN stock.data_jobs.request_key IS '同一业务输入重试复用的可读键，由模式、目标会话、窗口及日历修订组成。';
COMMENT ON COLUMN stock.data_jobs.mode IS '任务模式：INITIALIZE表示历史初始化，MANUAL_DAILY表示用户手动日更。';
COMMENT ON COLUMN stock.data_jobs.target_session IS '显式目标官方交易会话；不由机器当前日期推断。';
COMMENT ON COLUMN stock.data_jobs.research_cutoff IS '启动时经授权的研究截止会话；任务不得读取其后的行情。';
COMMENT ON COLUMN stock.data_jobs.history_session_count IS 'Raw基础历史及Core/Profile预热请求的官方会话数。';
COMMENT ON COLUMN stock.data_jobs.d0_output_session_count IS '本任务计划生成并保存的最近D0官方会话数；零表示用户跳过。';
COMMENT ON COLUMN stock.data_jobs.d1_output_session_count IS '本任务计划生成并保存的最近D1官方会话数；零表示用户跳过。';
COMMENT ON COLUMN stock.data_jobs.calendar_id IS '计划使用的官方交易日历身份。';
COMMENT ON COLUMN stock.data_jobs.calendar_revision IS '计划使用的官方交易日历修订。';
COMMENT ON COLUMN stock.data_jobs.status IS '任务状态：运行中、完成、无变化、不完整、受限、失败或中断。';
COMMENT ON COLUMN stock.data_jobs.planned_units IS '已知计划的阶段与业务表单元总数；未知行数不作为分母。';
COMMENT ON COLUMN stock.data_jobs.completed_units IS '事务提交且独立读回核验完成的阶段与业务表单元数。';
COMMENT ON COLUMN stock.data_jobs.source_package_id IS '实际Raw生产所用数字来源包ID；尚未读取来源时为空。';
COMMENT ON COLUMN stock.data_jobs.created_at IS '任务记录创建时刻，数据库时区为timestamptz。';
COMMENT ON COLUMN stock.data_jobs.started_at IS '后台执行开始时刻；计划预览不会填写。';
COMMENT ON COLUMN stock.data_jobs.updated_at IS '任务状态最近一次实际持久化更新时刻。';
COMMENT ON COLUMN stock.data_jobs.completed_at IS '任务达到终态时刻；运行、受限或中断时为空。';
COMMENT ON COLUMN stock.data_jobs.last_error_code IS '最近一次失败或阻断的稳定原因码；无异常时为空。';
COMMENT ON COLUMN stock.data_jobs.last_error_message IS '最近一次失败或阻断的中文说明；无异常时为空。';

CREATE TABLE stock.data_job_progress (
    job_id uuid NOT NULL REFERENCES stock.data_jobs(job_id) ON DELETE CASCADE,
    stage_order smallint NOT NULL CHECK (stage_order > 0),
    stage_code text NOT NULL CHECK (btrim(stage_code) <> ''),
    target_session date NOT NULL,
    business_table text NOT NULL CHECK (business_table LIKE 'stock.%'),
    planned_units integer NOT NULL CHECK (planned_units > 0),
    completed_units integer NOT NULL DEFAULT 0 CHECK (completed_units >= 0),
    expected_rows bigint,
    verified_rows bigint,
    inserted_rows bigint,
    updated_rows bigint,
    status text NOT NULL CHECK (status IN ('NOT_STARTED', 'RUNNING', 'COMPLETED', 'ALREADY_COMPLETE', 'NO_CHANGE', 'NOT_COMPUTED', 'INCOMPLETE', 'BLOCKED', 'FAILED', 'INTERRUPTED', 'SKIPPED_BY_USER', 'SKIPPED_NOT_IMPLEMENTED')),
    quality_status text,
    error_code text,
    error_message text,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    started_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    verified_at timestamptz,
    PRIMARY KEY (job_id, stage_code, target_session, business_table),
    CONSTRAINT data_job_progress_completed_units_ck CHECK (completed_units <= planned_units),
    CONSTRAINT data_job_progress_row_counts_ck CHECK (
        (expected_rows IS NULL OR expected_rows >= 0)
        AND (verified_rows IS NULL OR verified_rows >= 0)
        AND (inserted_rows IS NULL OR inserted_rows >= 0)
        AND (updated_rows IS NULL OR updated_rows >= 0)
    )
);

CREATE INDEX data_job_progress_job_order_ix
    ON stock.data_job_progress (job_id, stage_order, target_session, business_table);

COMMENT ON TABLE stock.data_job_progress IS '按真实生产阶段、目标会话与实际PG业务表记录任务进度；完成状态仅在提交后读回核验后写入。';
COMMENT ON COLUMN stock.data_job_progress.job_id IS '所属数据任务ID。';
COMMENT ON COLUMN stock.data_job_progress.stage_order IS '生产链中阶段的显示顺序。';
COMMENT ON COLUMN stock.data_job_progress.stage_code IS '现役计算或存储阶段代码，不承载自由格式业务对象。';
COMMENT ON COLUMN stock.data_job_progress.target_session IS '该阶段与业务表单元对应的官方目标会话。';
COMMENT ON COLUMN stock.data_job_progress.business_table IS '本阶段真实写入并核验的stock业务表名。';
COMMENT ON COLUMN stock.data_job_progress.planned_units IS '该业务表单元已知的计划处理单位数。';
COMMENT ON COLUMN stock.data_job_progress.completed_units IS '提交并独立读回核验后的处理单位数。';
COMMENT ON COLUMN stock.data_job_progress.expected_rows IS '可由闭合生产分母确定的预期行数；未知时为空。';
COMMENT ON COLUMN stock.data_job_progress.verified_rows IS '提交后从PostgreSQL独立读回并核对的实际行数；未核验时为空。';
COMMENT ON COLUMN stock.data_job_progress.inserted_rows IS '本次成功提交并读回确认的新插入行数。';
COMMENT ON COLUMN stock.data_job_progress.updated_rows IS '本次成功提交并读回确认的变更行数；同输入幂等时为零。';
COMMENT ON COLUMN stock.data_job_progress.status IS '阶段/表单元状态；零变化、未计算、未知分母、失败和跳过分别表达。';
COMMENT ON COLUMN stock.data_job_progress.quality_status IS '生产者返回且读回确认的质量状态；未知时为空。';
COMMENT ON COLUMN stock.data_job_progress.error_code IS '该阶段/表最近一次异常或阻断的稳定原因码。';
COMMENT ON COLUMN stock.data_job_progress.error_message IS '该阶段/表最近一次异常或阻断的中文说明。';
COMMENT ON COLUMN stock.data_job_progress.attempt_count IS '该阶段/表被真实后台执行尝试的次数。';
COMMENT ON COLUMN stock.data_job_progress.started_at IS '该阶段/表第一次进入后台执行的时刻。';
COMMENT ON COLUMN stock.data_job_progress.updated_at IS '该阶段/表状态最近一次持久化更新时刻。';
COMMENT ON COLUMN stock.data_job_progress.completed_at IS '该阶段/表单元达到完成或明确跳过状态的时刻。';
COMMENT ON COLUMN stock.data_job_progress.verified_at IS '实际业务行完成事务提交后的独立读回核验时刻。';
