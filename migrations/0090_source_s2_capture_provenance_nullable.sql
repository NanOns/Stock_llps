ALTER TABLE stock.source_s2_t1_cohort_stats
    ALTER COLUMN source_capture_id DROP NOT NULL,
    ALTER COLUMN endpoint_source_capture_id DROP NOT NULL;

COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.source_capture_id IS
    'T0 QFQ来源捕获溯源元数据；全体证券唯一捕获时填写，否则为空；不作业务键或读取筛选。';
COMMENT ON COLUMN stock.source_s2_t1_cohort_stats.endpoint_source_capture_id IS
    'T+1 QFQ来源捕获溯源元数据；全体证券唯一捕获时填写，否则为空；不作业务键或读取筛选。';
