-- 在唯一FEP条件统计typed表中启用独立REENTRY lane。
ALTER TABLE stock.source_fep_baseline_facts
    DROP CONSTRAINT source_fep_baseline_facts_lane_check;

ALTER TABLE stock.source_fep_baseline_facts
    ADD CONSTRAINT source_fep_baseline_facts_lane_check
    CHECK (lane IN ('ENTRY', 'DAILY_LANDMARK', 'REENTRY'));

COMMENT ON COLUMN stock.source_fep_baseline_facts.lane IS
    'FEP独立观察lane：ENTRY为首次相邻FALSE到TRUE，DAILY_LANDMARK为持续TRUE观察点，REENTRY为已证退出后独立重入；不含左截断。';
