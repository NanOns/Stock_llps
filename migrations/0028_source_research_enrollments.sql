-- 已证来源级研究匹配的登记；不代表正式 Focus Episode。
CREATE TABLE stock.source_research_enrollments (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    d0_fact_id bigint REFERENCES stock.source_d0_launch_day_facts(id),
    rule_fact_id bigint REFERENCES stock.source_rule_evaluations(id),
    previous_rule_fact_id bigint REFERENCES stock.source_rule_evaluations(id),
    enrollment_kind text NOT NULL CHECK (enrollment_kind IN (
        'ENTRY', 'FIRST_OBSERVED_LEFT_CENSORED', 'DAILY_LANDMARK'
    )),
    reason_code text NOT NULL,
    CHECK ((d0_fact_id IS NOT NULL)::integer + (rule_fact_id IS NOT NULL)::integer = 1),
    CHECK (previous_rule_fact_id IS NULL OR rule_fact_id IS NOT NULL),
    UNIQUE (d0_fact_id),
    UNIQUE (rule_fact_id)
);

CREATE INDEX source_rule_evaluations_adjacent_lookup_idx
    ON stock.source_rule_evaluations
    (target_session, source_package_id, source_symbol, rule_id, rule_version,
     profile_revision, history_session_count, d1_history_session_count, calendar_revision);

COMMENT ON TABLE stock.source_research_enrollments IS '已证来源级研究匹配的登记，引用原始typed事实；不证明PIT、正式Focus资格或Episode。';
COMMENT ON COLUMN stock.source_research_enrollments.id IS '研究登记数字主键，仅用于引用，不是证券身份。';
COMMENT ON COLUMN stock.source_research_enrollments.d0_fact_id IS 'D0相邻三态事实主键；与规则事实引用二选一，日期、来源、时间质量和原价坐标由原事实提供。';
COMMENT ON COLUMN stock.source_research_enrollments.rule_fact_id IS '已证TRUE的来源规则事实主键；与D0事实引用二选一，规则、画像修订和来源版本由原事实提供。';
COMMENT ON COLUMN stock.source_research_enrollments.previous_rule_fact_id IS '证明相邻前态的同规则事实主键；无可比较前态时为空。';
COMMENT ON COLUMN stock.source_research_enrollments.enrollment_kind IS 'ENTRY为已证相邻FALSE到TRUE；FIRST_OBSERVED_LEFT_CENSORED为左截断首次观察；DAILY_LANDMARK为相邻TRUE观察，不声称历史首次。';
COMMENT ON COLUMN stock.source_research_enrollments.reason_code IS '登记判断原因码，记录相邻转入或前态不足。';
