-- 邻接查询先按会话、来源和规则缩小候选，长版本字段在候选行核对。
DROP INDEX stock.source_rule_evaluations_adjacent_lookup_idx;
CREATE INDEX source_rule_evaluations_adjacent_lookup_idx
    ON stock.source_rule_evaluations
    (target_session, source_package_id, source_symbol, rule_id);
