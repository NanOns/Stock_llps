ALTER TABLE stock.source_research_enrollments
    ADD COLUMN parent_enrollment_id bigint;

ALTER TABLE stock.source_research_enrollments
    DROP CONSTRAINT source_research_enrollments_enrollment_kind_check;

ALTER TABLE stock.source_research_enrollments
    ADD CONSTRAINT source_research_enrollments_enrollment_kind_check
    CHECK (enrollment_kind IN (
        'ENTRY', 'FIRST_OBSERVED_LEFT_CENSORED', 'DAILY_LANDMARK', 'REENTRY'
    ));

ALTER TABLE stock.source_research_enrollments
    ADD CONSTRAINT source_research_enrollments_parent_enrollment_id_fkey
    FOREIGN KEY (parent_enrollment_id)
    REFERENCES stock.source_research_enrollments(id);

ALTER TABLE stock.source_research_enrollments
    ADD CONSTRAINT source_research_enrollments_parent_kind_ck
    CHECK (
        (enrollment_kind = 'REENTRY'
         AND rule_fact_id IS NOT NULL
         AND d0_fact_id IS NULL
         AND parent_enrollment_id IS NOT NULL)
        OR
        (enrollment_kind <> 'REENTRY' AND parent_enrollment_id IS NULL)
    );

ALTER TABLE stock.source_research_enrollments
    ADD CONSTRAINT source_research_enrollments_parent_not_self_ck
    CHECK (parent_enrollment_id IS NULL OR parent_enrollment_id <> id);

CREATE INDEX source_research_enrollments_parent_idx
    ON stock.source_research_enrollments (parent_enrollment_id)
    WHERE parent_enrollment_id IS NOT NULL;

COMMENT ON COLUMN stock.source_research_enrollments.parent_enrollment_id IS
    '来源级同规则观察段的直接父登记ID；仅REENTRY子段设置，不代表正式Focus Episode。';

COMMENT ON COLUMN stock.source_research_enrollments.enrollment_kind IS
    'ENTRY为已证FALSE到TRUE；FIRST_OBSERVED_LEFT_CENSORED为无合法前态的首次观察；DAILY_LANDMARK为活动段每日观察点，不是新入组；REENTRY为已证退出后带父段ID的新来源级观察段。';

COMMENT ON COLUMN stock.source_research_enrollments.reason_code IS
    '登记判断原因码；REENTRY使用MATCH_AFTER_PROVED_EXIT，DAILY_LANDMARK记录相邻持续，未知前态保留左截断原因。';
