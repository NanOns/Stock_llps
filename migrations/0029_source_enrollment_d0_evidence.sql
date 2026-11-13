-- 同一LAUNCH_CONFIRM谓词的相邻D0证据引用。
ALTER TABLE stock.source_research_enrollments
    ADD COLUMN adjacent_d0_fact_id bigint REFERENCES stock.source_d0_launch_day_facts(id),
    ADD CONSTRAINT source_enrollment_d0_evidence_ck CHECK (
        adjacent_d0_fact_id IS NULL OR rule_fact_id IS NOT NULL
    );
COMMENT ON COLUMN stock.source_research_enrollments.adjacent_d0_fact_id IS '与来源规则同谓词、同来源及相邻会话的D0证据主键；不适用或无法唯一绑定时为空。';
