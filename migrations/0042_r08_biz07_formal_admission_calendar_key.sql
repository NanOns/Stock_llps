-- R08/业务07：正式准入事实仅保留日历业务标识，不复制日历内容摘要。
ALTER TABLE stock.source_d0_formal_admission_day_facts
    DROP COLUMN calendar_sha256;
