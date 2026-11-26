-- 为BaoStock单行双坐标事实分别保存来源捕获身份与观察时钟。
-- 业务键仍为(source_symbol, session_date)，本迁移不引入多版本事实。

ALTER TABLE stock.source_raw_daily
    ADD COLUMN raw_source_capture_id bigint,
    ADD COLUMN qfq_source_capture_id bigint,
    ADD COLUMN raw_observed_started_at timestamptz,
    ADD COLUMN raw_observed_completed_at timestamptz,
    ADD COLUMN qfq_observed_started_at timestamptz,
    ADD COLUMN qfq_observed_completed_at timestamptz;

ALTER TABLE stock.data_job_progress
    ADD COLUMN rule_ids text[],
    ADD COLUMN followup_target_sessions date[];

COMMENT ON COLUMN stock.data_job_progress.rule_ids IS
    '该进度实际声明并完成的规则ID子集；NULL表示该进度未声明规则子集，查询不得以其它规则行代替。';
COMMENT ON COLUMN stock.data_job_progress.followup_target_sessions IS
    '该进度实际声明的后继目标官方会话集合；NULL表示全范围，数组表示本次真实局部目标会话集合。';

UPDATE stock.source_raw_daily
   SET raw_source_capture_id = CASE
           WHEN source_code IS NOT NULL AND raw_response_source_code IS NOT NULL
           THEN source_capture_id END,
       qfq_source_capture_id = CASE
           WHEN source_code IS NOT NULL AND qfq_response_source_code IS NOT NULL
           THEN source_capture_id END,
       raw_observed_started_at = CASE
           WHEN source_code IS NOT NULL AND raw_response_source_code IS NOT NULL
           THEN observed_started_at END,
       raw_observed_completed_at = CASE
           WHEN source_code IS NOT NULL AND raw_response_source_code IS NOT NULL
           THEN observed_completed_at END,
       qfq_observed_started_at = CASE
           WHEN source_code IS NOT NULL AND qfq_response_source_code IS NOT NULL
           THEN observed_started_at END,
       qfq_observed_completed_at = CASE
           WHEN source_code IS NOT NULL AND qfq_response_source_code IS NOT NULL
           THEN observed_completed_at END
 WHERE source_code IS NOT NULL;

ALTER TABLE stock.source_raw_daily
    ADD CONSTRAINT source_raw_daily_raw_capture_fk
        FOREIGN KEY (raw_source_capture_id)
        REFERENCES stock.source_captures(capture_id) ON DELETE RESTRICT NOT VALID,
    ADD CONSTRAINT source_raw_daily_qfq_capture_fk
        FOREIGN KEY (qfq_source_capture_id)
        REFERENCES stock.source_captures(capture_id) ON DELETE RESTRICT NOT VALID,
    ADD CONSTRAINT source_raw_daily_raw_clock_order_ck CHECK (
        raw_observed_started_at IS NULL OR raw_observed_completed_at IS NULL
        OR raw_observed_completed_at >= raw_observed_started_at
    ) NOT VALID,
    ADD CONSTRAINT source_raw_daily_qfq_clock_order_ck CHECK (
        qfq_observed_started_at IS NULL OR qfq_observed_completed_at IS NULL
        OR qfq_observed_completed_at >= qfq_observed_started_at
    ) NOT VALID;

COMMENT ON COLUMN stock.source_raw_daily.raw_source_capture_id IS
    'RAW未复权坐标本行当前有效字段对应的BaoStock查询捕获身份；仅溯源元数据，不是事实键。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_source_capture_id IS
    'QFQ前复权坐标本行当前有效字段对应的BaoStock查询捕获身份；仅溯源元数据，不是事实键。';
COMMENT ON COLUMN stock.source_raw_daily.raw_observed_started_at IS
    'RAW未复权坐标查询观察窗口开始时刻（含时区）；历史回填沿用原行已记录窗口，不拆分伪造精度。';
COMMENT ON COLUMN stock.source_raw_daily.raw_observed_completed_at IS
    'RAW未复权坐标查询观察窗口完成时刻（含时区）；历史回填沿用原行已记录窗口，不拆分伪造精度。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_observed_started_at IS
    'QFQ前复权坐标查询观察窗口开始时刻（含时区）；历史回填沿用原行已记录窗口，不拆分伪造精度。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_observed_completed_at IS
    'QFQ前复权坐标查询观察窗口完成时刻（含时区）；历史回填沿用原行已记录窗口，不拆分伪造精度。';
