-- R08/业务08补充：放宽D0正式Focus事实的请求会话窗口持久化范围。
ALTER TABLE stock.source_d0_formal_admission_day_facts
    DROP CONSTRAINT source_d0_formal_admission_day_fact_history_session_count_check,
    ADD CONSTRAINT source_d0_formal_admission_history_session_range_ck
        CHECK (history_session_count BETWEEN 1 AND 32767);

COMMENT ON COLUMN stock.source_d0_formal_admission_day_facts.history_session_count
    IS '原始D0场景请求的官方历史会话数，单位为会话；范围为1至32767，不表示实际有效Bar数。';
