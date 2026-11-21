ALTER TABLE stock.source_fep_baseline_facts
    ALTER COLUMN source_capture_id DROP NOT NULL;

COMMENT ON COLUMN stock.source_fep_baseline_facts.source_capture_id IS
    '单T0统计绑定BaoStock捕获ID；EQUAL_DATE跨独立T0日期汇总没有唯一捕获时为空。';
