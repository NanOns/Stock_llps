-- 已存来源Cohort/Forward事实的FEP条件统计；仅描述性，不表示预测能力。
CREATE TABLE stock.source_fep_baseline_facts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    research_cutoff date NOT NULL,
    source_package_id bigint NOT NULL REFERENCES stock.source_packages(source_package_id),
    source_capture_id bigint REFERENCES stock.source_captures(capture_id),
    target_session date,
    period_start_session date,
    period_end_session date,
    rule_id text NOT NULL,
    rule_version text NOT NULL,
    profile_family text NOT NULL,
    profile_revision text,
    history_session_count smallint,
    d1_history_session_count smallint,
    event_window_start date,
    calendar_revision text NOT NULL,
    path_rule_version text NOT NULL,
    source_time_quality text NOT NULL,
    parser_contract text NOT NULL,
    lane text NOT NULL CHECK (lane IN ('ENTRY', 'DAILY_LANDMARK')),
    horizon smallint NOT NULL CHECK (horizon IN (1, 3, 5)),
    summary_scope text NOT NULL CHECK (summary_scope IN ('T0', 'EQUAL_DATE')),
    group_code text NOT NULL CHECK (
        group_code IN ('MATCH', 'NO_MATCH', 'UNKNOWN', 'MATCH_VS_NO_MATCH')
    ),
    metric_code text NOT NULL CHECK (
        metric_code IN ('RAW_CLOSE_RETURN', 'RAW_MFE', 'RAW_MAE', 'RAW_MDD_CLOSE')
    ),
    condition_code text NOT NULL CHECK (
        condition_code IN ('ALL', 'B_FIXED_POSITIVE', 'B_FIXED_NON_POSITIVE', 'B_FIXED_UNKNOWN')
    ),
    market_condition_code text CHECK (
        market_condition_code IS NULL OR market_condition_code IN (
            'B_FIXED_POSITIVE', 'B_FIXED_NON_POSITIVE', 'B_FIXED_UNKNOWN'
        )
    ),
    status text NOT NULL CHECK (status IN (
        'OBSERVED', 'UNKNOWN', 'NOT_COMPUTED', 'NOT_DUE', 'INCOMPLETE', 'NOT_READY',
        'NOT_EVALUABLE', 'INSUFFICIENT_INDEPENDENT_DATES', 'DESCRIPTIVE_ONLY'
    )),
    capability_code text NOT NULL CHECK (capability_code = 'DESCRIPTIVE_ONLY'),
    entity_code text NOT NULL CHECK (entity_code = 'SOURCE_STOCK'),
    target_code text NOT NULL CHECK (target_code = 'RAW_CLOSE_RETURN'),
    scope text NOT NULL CHECK (scope = 'NON_PIT_EXPLORATORY'),
    price_coordinate text NOT NULL CHECK (price_coordinate = 'RAW_UNADJUSTED'),
    source_pool_count bigint,
    t0_match_count bigint,
    t0_no_match_count bigint,
    t0_unknown_count bigint,
    entry_count bigint,
    daily_landmark_count bigint,
    reentry_count bigint,
    left_censored_count bigint,
    member_count bigint,
    due_count bigint,
    not_due_count bigint,
    observed_count bigint,
    unknown_count bigint,
    not_computed_count bigint,
    positive_count bigint,
    non_positive_count bigint,
    p25_numerator numeric(100, 0),
    p25_denominator numeric(100, 0),
    p50_numerator numeric(100, 0),
    p50_denominator numeric(100, 0),
    p75_numerator numeric(100, 0),
    p75_denominator numeric(100, 0),
    median_difference_numerator numeric(100, 0),
    median_difference_denominator numeric(100, 0),
    positive_fraction_difference_numerator numeric(100, 0),
    positive_fraction_difference_denominator numeric(100, 0),
    requested_date_count bigint,
    condition_date_count bigint,
    valid_date_count bigint,
    not_due_date_count bigint,
    unknown_date_count bigint,
    not_computed_date_count bigint,
    equal_date_mean_median_difference_numerator numeric(100, 0),
    equal_date_mean_median_difference_denominator numeric(100, 0),
    equal_date_mean_positive_fraction_difference_numerator numeric(100, 0),
    equal_date_mean_positive_fraction_difference_denominator numeric(100, 0),
    positive_direction_date_count bigint,
    negative_direction_date_count bigint,
    zero_direction_date_count bigint,
    market_metric_numerator numeric(100, 0),
    market_metric_denominator numeric(100, 0),
    market_source_count bigint,
    market_eligible_count bigint,
    market_time_quality text,
    reason_codes text[] NOT NULL DEFAULT '{}',
    reason_counts bigint[] NOT NULL DEFAULT '{}',
    CHECK (
        (summary_scope = 'T0'
         AND target_session IS NOT NULL AND source_capture_id IS NOT NULL
         AND profile_revision IS NOT NULL AND history_session_count IS NOT NULL
         AND d1_history_session_count IS NOT NULL AND event_window_start IS NOT NULL
         AND period_start_session IS NULL AND period_end_session IS NULL)
        OR
        (summary_scope = 'EQUAL_DATE'
         AND target_session IS NULL AND source_capture_id IS NULL
         AND profile_revision IS NULL AND history_session_count IS NULL
         AND d1_history_session_count IS NULL AND event_window_start IS NULL
         AND period_start_session IS NOT NULL AND period_end_session IS NOT NULL
         AND period_start_session <= period_end_session)
    ),
    CHECK (source_pool_count IS NULL OR source_pool_count >= 0),
    CHECK (t0_match_count IS NULL OR t0_match_count >= 0),
    CHECK (t0_no_match_count IS NULL OR t0_no_match_count >= 0),
    CHECK (t0_unknown_count IS NULL OR t0_unknown_count >= 0),
    CHECK (member_count IS NULL OR member_count >= 0),
    CHECK (due_count IS NULL OR due_count >= 0),
    CHECK (not_due_count IS NULL OR not_due_count >= 0),
    CHECK (observed_count IS NULL OR observed_count >= 0),
    CHECK (unknown_count IS NULL OR unknown_count >= 0),
    CHECK (not_computed_count IS NULL OR not_computed_count >= 0),
    CHECK (positive_count IS NULL OR positive_count >= 0),
    CHECK (non_positive_count IS NULL OR non_positive_count >= 0),
    CHECK (cardinality(reason_codes) = cardinality(reason_counts)),
    CHECK ((p25_numerator IS NULL) = (p25_denominator IS NULL)),
    CHECK (p25_denominator IS NULL OR p25_denominator > 0),
    CHECK ((p50_numerator IS NULL) = (p50_denominator IS NULL)),
    CHECK (p50_denominator IS NULL OR p50_denominator > 0),
    CHECK ((p75_numerator IS NULL) = (p75_denominator IS NULL)),
    CHECK (p75_denominator IS NULL OR p75_denominator > 0),
    CHECK ((median_difference_numerator IS NULL) = (median_difference_denominator IS NULL)),
    CHECK (median_difference_denominator IS NULL OR median_difference_denominator > 0),
    CHECK ((positive_fraction_difference_numerator IS NULL) =
           (positive_fraction_difference_denominator IS NULL)),
    CHECK (positive_fraction_difference_denominator IS NULL OR
           positive_fraction_difference_denominator > 0),
    CHECK ((equal_date_mean_median_difference_numerator IS NULL) =
           (equal_date_mean_median_difference_denominator IS NULL)),
    CHECK (equal_date_mean_median_difference_denominator IS NULL OR
           equal_date_mean_median_difference_denominator > 0),
    CHECK ((equal_date_mean_positive_fraction_difference_numerator IS NULL) =
           (equal_date_mean_positive_fraction_difference_denominator IS NULL)),
    CHECK (equal_date_mean_positive_fraction_difference_denominator IS NULL OR
           equal_date_mean_positive_fraction_difference_denominator > 0),
    CHECK ((market_metric_numerator IS NULL) = (market_metric_denominator IS NULL)),
    CHECK (market_metric_denominator IS NULL OR market_metric_denominator > 0)
);

CREATE UNIQUE INDEX source_fep_baseline_t0_business_key
    ON stock.source_fep_baseline_facts (
        research_cutoff, source_package_id, target_session, source_capture_id,
        rule_id, rule_version, profile_family, profile_revision,
        history_session_count, d1_history_session_count, event_window_start,
        calendar_revision, path_rule_version, source_time_quality, parser_contract,
        lane, horizon, group_code, metric_code, condition_code
    ) WHERE summary_scope = 'T0';

CREATE UNIQUE INDEX source_fep_baseline_equal_date_business_key
    ON stock.source_fep_baseline_facts (
        research_cutoff, source_package_id, period_start_session, period_end_session,
        rule_id, rule_version, profile_family, calendar_revision, path_rule_version,
        source_time_quality, parser_contract, lane, horizon, group_code, metric_code,
        condition_code
    ) WHERE summary_scope = 'EQUAL_DATE';

CREATE INDEX source_fep_baseline_read_idx
    ON stock.source_fep_baseline_facts (
        research_cutoff, source_package_id, rule_id, horizon, lane, summary_scope
    );

COMMENT ON TABLE stock.source_fep_baseline_facts IS '从已存完整来源T0与Forward事实离线生成的FEP条件统计；仅为NON_PIT描述研究，不表示概率、预测或模型资格。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.id IS 'FEP条件统计 typed 行的数字主键。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.research_cutoff IS '本次静态研究可读取事实的最晚官方会话。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.source_package_id IS '完整来源Raw与规则事实的数字来源包身份。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.source_capture_id IS '单T0行绑定的来源捕获身份；跨日期汇总为空。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.target_session IS '单T0统计所属的官方交易会话；跨日期汇总为空。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.period_start_session IS '跨独立T0日期汇总的首个会话；单T0行为空。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.period_end_session IS '跨独立T0日期汇总的末个会话；单T0行为空。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.rule_id IS '冻结T0规则标识。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.rule_version IS '冻结T0规则算法版本。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.profile_family IS '可跨日期比较的画像算法族；不替代逐日精确画像修订。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.profile_revision IS '单T0精确画像修订；跨日期汇总保留NULL并按画像算法族分组。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.history_session_count IS '单T0画像输入官方会话数；跨日期汇总为空。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.d1_history_session_count IS '单T0 D1输入官方会话数；跨日期汇总为空。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.event_window_start IS '单T0事件窗起始官方会话；跨日期汇总为空。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.calendar_revision IS 'T0与后继使用的官方交易日历修订。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.path_rule_version IS '已存Forward后继路径公式版本。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.source_time_quality IS '来源时钟质量；UNKNOWN、NON_PIT_OBSERVED和已验证来源时钟严格区分。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.parser_contract IS '原始来源捕获使用的解析合同版本。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.lane IS 'FEP观察lane；只允许 ENTRY 或 DAILY_LANDMARK。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.horizon IS '从T0起算的官方后继会话数，仅支持本批T+1、T+3、T+5。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.summary_scope IS 'T0表示单日完整来源统计，EQUAL_DATE表示按独立日期等权的已存汇总。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.group_code IS 'T0规则MATCH、NO_MATCH、UNKNOWN或MATCH与NO_MATCH逐日对照。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.metric_code IS 'RAW_CLOSE_RETURN或已存同坐标Forward的MFE、MAE、MDD_CLOSE。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.condition_code IS '预先定义的全部日期或T0固定来源宽度正、非正、未知分组。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.market_condition_code IS '单T0来源市场宽度分层；质量或覆盖不闭合时为B_FIXED_UNKNOWN。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.status IS '数值状态：已观察、未知、未计算、未到期、不完整、未就绪、日期不足或仅描述。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.capability_code IS '当前唯一能力标签DESCRIPTIVE_ONLY；不表示预测概率。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.entity_code IS '实体范围固定为来源代码级股票SOURCE_STOCK。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.target_code IS '目标固定为未复权收盘端点收益RAW_CLOSE_RETURN。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.scope IS '研究范围固定为后验NON_PIT_EXPLORATORY。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.price_coordinate IS '价格坐标固定为RAW_UNADJUSTED。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.source_pool_count IS '该T0冻结规则的完整来源代码分母。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.t0_match_count IS '该T0规则TRUE三态总数，含其他独立lane。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.t0_no_match_count IS '该T0规则FALSE三态完整来源数。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.t0_unknown_count IS '该T0规则UNKNOWN三态完整来源数。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.entry_count IS '该T0中精确ENTRY登记数，不含REENTRY及左截断。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.daily_landmark_count IS '该T0中相邻持续TRUE的DAILY_LANDMARK登记数。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.reentry_count IS '该T0中独立REENTRY登记数，不混入ENTRY。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.left_censored_count IS '该T0中FIRST_OBSERVED_LEFT_CENSORED登记数，不混入ENTRY。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.member_count IS '本lane与规则状态组的完整股票样本数。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.due_count IS '已到期后继会话数；未到期只由官方日历判定。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.not_due_count IS '超过研究截止的后继样本数，不读取对应行情。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.observed_count IS '该指标具有合法精确Forward值的样本数。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.unknown_count IS '该指标输入质量未知或数值不可评价的样本数。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.not_computed_count IS '已到期但没有可用持久化计算结果的样本数。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.positive_count IS '端点收益严格大于零的可评价样本数。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.non_positive_count IS '端点收益小于或等于零的可评价样本数。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.p25_numerator IS '精确最近秩p25分子；无可评价样本时为空。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.p25_denominator IS '精确最近秩p25正分母；不使用显示舍入。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.p50_numerator IS '精确最近秩p50分子；偶数样本不取两项平均。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.p50_denominator IS '精确最近秩p50正分母；不使用Cohort均值中位数。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.p75_numerator IS '精确最近秩p75分子；无可评价样本时为空。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.p75_denominator IS '精确最近秩p75正分母；不使用显示舍入。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.median_difference_numerator IS '同T0 lane MATCH最近秩p50减NO_MATCH最近秩p50的分子。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.median_difference_denominator IS '同T0中位数差的精确正分母。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.positive_fraction_difference_numerator IS '同T0 MATCH减NO_MATCH正收益率差的精确分子。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.positive_fraction_difference_denominator IS '同T0正收益率差的精确正分母。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.requested_date_count IS '按日期等权汇总请求的T0会话数。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.condition_date_count IS '落入当前来源市场条件桶的独立T0日期数。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.valid_date_count IS '具有同日可评价MATCH与NO_MATCH统计的独立T0日期数。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.not_due_date_count IS '所选日期中后继期限尚未到期的T0日期数。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.unknown_date_count IS '所选日期中输入或质量未知的T0日期数。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.not_computed_date_count IS '所选日期中已有到期目标但未形成结果的T0日期数。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.equal_date_mean_median_difference_numerator IS '逐T0最近秩中位差等权均值的精确分子。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.equal_date_mean_median_difference_denominator IS '逐T0最近秩中位差等权均值的精确正分母。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.equal_date_mean_positive_fraction_difference_numerator IS '逐T0正收益率差等权均值的精确分子。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.equal_date_mean_positive_fraction_difference_denominator IS '逐T0正收益率差等权均值的精确正分母。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.positive_direction_date_count IS '同日中位差大于零的独立T0日期数。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.negative_direction_date_count IS '同日中位差小于零的独立T0日期数。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.zero_direction_date_count IS '同日中位差等于零的独立T0日期数。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.market_metric_numerator IS '已存固定来源宽度的精确分子；覆盖不全时只供解释并标未知条件。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.market_metric_denominator IS '已存固定来源宽度的精确正分母。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.market_source_count IS '同源市场宽度完整来源代码分母。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.market_eligible_count IS '同日市场宽度两端均可比较的来源代码数。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.market_time_quality IS '市场宽度的来源时间质量；后验NON_PIT不提升为严格PIT。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.reason_codes IS '本统计对应的不可评价原因码数组。';
COMMENT ON COLUMN stock.source_fep_baseline_facts.reason_counts IS '与原因码按位置配对的样本数数组，可重叠且不可当独立分母。';
