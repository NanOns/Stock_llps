ALTER TABLE stock.source_rule_evaluations
    DROP CONSTRAINT source_rule_evaluations_history_session_count_check;

ALTER TABLE stock.source_rule_evaluations
    ADD CONSTRAINT source_rule_evaluations_history_session_count_check
    CHECK (history_session_count BETWEEN 1 AND 250);

COMMENT ON COLUMN stock.source_rule_evaluations.history_session_count IS
    '冻结规则画像历史窗口的官方会话数；由BaoStock运行预算限制，当前合同上限为250。';
