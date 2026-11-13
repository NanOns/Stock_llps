-- 冻结规则各期限的来源级离线统计，不保存规则明细或虚构收益。
CREATE TABLE stock.source_cohort_group_stats (
 target_session date NOT NULL, research_cutoff date NOT NULL,
 source_package_id bigint NOT NULL, source_capture_id bigint NOT NULL,
 rule_id text NOT NULL, rule_version text NOT NULL, profile_revision text NOT NULL,
 history_session_count smallint NOT NULL, d1_history_session_count smallint NOT NULL,
 event_window_start date NOT NULL, calendar_revision text NOT NULL,
 path_rule_version text NOT NULL, horizon smallint NOT NULL CHECK (horizon IN (1,3,5,10,20)),
 state text NOT NULL CHECK (state IN ('TRUE','FALSE','UNKNOWN')),
 weighting text NOT NULL CHECK (weighting IN ('stock','event')),
 source_pool_count bigint NOT NULL CHECK (source_pool_count > 0),
 member_count bigint NOT NULL CHECK (member_count >= 0),
 evaluable_count bigint NOT NULL CHECK (evaluable_count >= 0),
 positive_count bigint NOT NULL CHECK (positive_count >= 0),
 median_numerator numeric, median_denominator numeric,
 maturity_codes text[] NOT NULL, maturity_counts bigint[] NOT NULL,
 path_quality_codes text[] NOT NULL, path_quality_counts bigint[] NOT NULL,
 reason_codes text[] NOT NULL, reason_counts bigint[] NOT NULL,
 PRIMARY KEY (target_session,research_cutoff,source_package_id,source_capture_id,
  rule_id,rule_version,profile_revision,history_session_count,d1_history_session_count,
  event_window_start,calendar_revision,path_rule_version,horizon,state,weighting),
 FOREIGN KEY (source_package_id) REFERENCES stock.source_packages(source_package_id),
 FOREIGN KEY (source_capture_id) REFERENCES stock.source_captures(capture_id),
 CHECK (evaluable_count <= member_count AND positive_count <= evaluable_count),
 CHECK ((median_numerator IS NULL) = (median_denominator IS NULL)),
 CHECK (median_denominator IS NULL OR median_denominator > 0),
 CHECK (cardinality(maturity_codes)=cardinality(maturity_counts)),
 CHECK (cardinality(path_quality_codes)=cardinality(path_quality_counts)),
 CHECK (cardinality(reason_codes)=cardinality(reason_counts))
);
COMMENT ON TABLE stock.source_cohort_group_stats IS '冻结来源规则按三态和代码或事件权重的离线统计；仅为非PIT研究，不表示正式入选或交易收益。';
COMMENT ON COLUMN stock.source_cohort_group_stats.target_session IS '规则冻结的T0官方会话。';
COMMENT ON COLUMN stock.source_cohort_group_stats.research_cutoff IS '可使用结果事实的研究截止会话。';
COMMENT ON COLUMN stock.source_cohort_group_stats.source_package_id IS '来源包数字身份。';
COMMENT ON COLUMN stock.source_cohort_group_stats.source_capture_id IS 'T0来源捕获数字身份。';
COMMENT ON COLUMN stock.source_cohort_group_stats.rule_id IS '冻结规则标识。';
COMMENT ON COLUMN stock.source_cohort_group_stats.rule_version IS '冻结规则算法版本。';
COMMENT ON COLUMN stock.source_cohort_group_stats.profile_revision IS '画像修订。';
COMMENT ON COLUMN stock.source_cohort_group_stats.history_session_count IS '画像历史窗口的官方会话数。';
COMMENT ON COLUMN stock.source_cohort_group_stats.d1_history_session_count IS 'D1历史窗口的官方会话数。';
COMMENT ON COLUMN stock.source_cohort_group_stats.event_window_start IS '事件窗口起始官方会话。';
COMMENT ON COLUMN stock.source_cohort_group_stats.calendar_revision IS '官方日历修订。';
COMMENT ON COLUMN stock.source_cohort_group_stats.path_rule_version IS '后继路径算法版本。';
COMMENT ON COLUMN stock.source_cohort_group_stats.horizon IS 'T0后的官方会话数。';
COMMENT ON COLUMN stock.source_cohort_group_stats.state IS '规则三态TRUE、FALSE、UNKNOWN。';
COMMENT ON COLUMN stock.source_cohort_group_stats.weighting IS 'stock按来源代码，event按已证事件权重。';
COMMENT ON COLUMN stock.source_cohort_group_stats.source_pool_count IS '完整规则来源代码总分母。';
COMMENT ON COLUMN stock.source_cohort_group_stats.member_count IS '本组按指定权重的分母。';
COMMENT ON COLUMN stock.source_cohort_group_stats.evaluable_count IS '端点收益可评价的加权数。';
COMMENT ON COLUMN stock.source_cohort_group_stats.positive_count IS '端点收益大于零的加权数。';
COMMENT ON COLUMN stock.source_cohort_group_stats.median_numerator IS '精确中位端点收益分子，无可评价值时为空。';
COMMENT ON COLUMN stock.source_cohort_group_stats.median_denominator IS '精确中位端点收益正分母，无可评价值时为空。';
COMMENT ON COLUMN stock.source_cohort_group_stats.maturity_codes IS '成熟状态码，按对应计数数组位置配对。';
COMMENT ON COLUMN stock.source_cohort_group_stats.maturity_counts IS '各成熟状态的加权计数。';
COMMENT ON COLUMN stock.source_cohort_group_stats.path_quality_codes IS '路径质量码，按对应计数数组位置配对。';
COMMENT ON COLUMN stock.source_cohort_group_stats.path_quality_counts IS '各路径质量的加权计数。';
COMMENT ON COLUMN stock.source_cohort_group_stats.reason_codes IS '不可评价原因码，按对应计数数组位置配对。';
COMMENT ON COLUMN stock.source_cohort_group_stats.reason_counts IS '各不可评价原因的加权计数。';
