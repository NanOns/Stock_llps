-- R00：保存有真实公司行为来源的仿射调整日线，Raw父事实保持不变。
CREATE TABLE stock.source_corporate_action_events (
    event_ref text NOT NULL,
    semantic_revision char(64) NOT NULL CHECK (semantic_revision ~ '^[0-9a-f]{64}$'),
    source_market text NOT NULL CHECK (source_market IN ('SH', 'SZ')),
    source_symbol text NOT NULL CHECK (source_symbol ~ '^(SH|SZ)\.[0-9]{6}$'),
    source_notice_id text NOT NULL,
    source_ref text NOT NULL CHECK (source_ref LIKE 'https://%'),
    effective_session date NOT NULL,
    published_date date NOT NULL,
    available_at timestamptz NOT NULL,
    available_at_quality text NOT NULL CHECK (
        available_at_quality IN ('EXACT_TIMESTAMP', 'DATE_ONLY_NEXT_DAY_BOUNDARY')
    ),
    captured_at timestamptz NOT NULL,
    action_kind text NOT NULL CHECK (
        action_kind IN ('CAPITAL_RESERVE_TO_STOCK', 'UNSUPPORTED', 'UNRESOLVED')
    ),
    cash_per_10 numeric NOT NULL CHECK (cash_per_10 >= 0),
    rights_price numeric NOT NULL CHECK (rights_price >= 0),
    bonus_per_10 numeric NOT NULL CHECK (bonus_per_10 >= 0),
    rights_per_10 numeric NOT NULL CHECK (rights_per_10 >= 0),
    declared_bonus_per_10 numeric CHECK (
        declared_bonus_per_10 IS NULL OR declared_bonus_per_10 >= 0
    ),
    share_change_ratio numeric CHECK (share_change_ratio IS NULL OR share_change_ratio >= 0),
    source_quality text NOT NULL CHECK (
        source_quality IN ('VERIFIED_OFFICIAL_NOTICE', 'UNSUPPORTED', 'UNRESOLVED')
    ),
    PRIMARY KEY (event_ref, semantic_revision),
    CHECK ((source_market = 'SH' AND source_symbol LIKE 'SH.%')
        OR (source_market = 'SZ' AND source_symbol LIKE 'SZ.%')),
    CHECK (published_date <= effective_session),
    CHECK (available_at::date >= published_date)
);

COMMENT ON TABLE stock.source_corporate_action_events IS '保存从公开交易所实施公告解析的来源代码级公司行为；捕获时钟与公告可得时钟分列，不把公告日期冒充精确发布时间。';
COMMENT ON COLUMN stock.source_corporate_action_events.event_ref IS '公司行为稳定引用，由来源公告编号、来源代码和生效会话组成。';
COMMENT ON COLUMN stock.source_corporate_action_events.semantic_revision IS '解析后事件字段及来源引用的SHA-256修订摘要；不是原始公告文件或业务身份。';
COMMENT ON COLUMN stock.source_corporate_action_events.source_market IS '来源市场枚举SH或SZ，用于与TDX来源代码核对。';
COMMENT ON COLUMN stock.source_corporate_action_events.source_symbol IS '来源证券代码，格式SH.六位代码或SZ.六位代码；不宣称稳定security_id。';
COMMENT ON COLUMN stock.source_corporate_action_events.source_notice_id IS '交易所公告编号，用于回到原始公开公告。';
COMMENT ON COLUMN stock.source_corporate_action_events.source_ref IS '交易所公开公告的HTTPS定位链接；不保存PDF二进制。';
COMMENT ON COLUMN stock.source_corporate_action_events.effective_session IS '公告确认的除权除息生效交易会话。';
COMMENT ON COLUMN stock.source_corporate_action_events.published_date IS '公告明确的发布日期；仅日精度时不推断盘中可得时刻。';
COMMENT ON COLUMN stock.source_corporate_action_events.available_at IS '可得时刻或保守日期边界；日期精度公告使用发布日期次日零时，不等同原始发布时间。';
COMMENT ON COLUMN stock.source_corporate_action_events.available_at_quality IS '可得时钟精度：EXACT_TIMESTAMP精确时刻，DATE_ONLY_NEXT_DAY_BOUNDARY仅有日期时使用次日零时边界。';
COMMENT ON COLUMN stock.source_corporate_action_events.captured_at IS '本机读取公开公告的实际捕获时刻，含时区；不得用作历史发布时间。';
COMMENT ON COLUMN stock.source_corporate_action_events.action_kind IS '公司行为类型：资本公积转增、已知但未支持、或作用范围未确认。';
COMMENT ON COLUMN stock.source_corporate_action_events.cash_per_10 IS '每十股现金分红金额，单位按来源公告；非现金事件为零。';
COMMENT ON COLUMN stock.source_corporate_action_events.rights_price IS '配股认购价格，单位为公告价格单位；无配股时为零。';
COMMENT ON COLUMN stock.source_corporate_action_events.bonus_per_10 IS '仿射公式使用的每十股有效股份增加数；有回购股等例外时按公告股份变动比例换算。';
COMMENT ON COLUMN stock.source_corporate_action_events.rights_per_10 IS '每十股配股数量；没有配股时为零。';
COMMENT ON COLUMN stock.source_corporate_action_events.declared_bonus_per_10 IS '公告中声明的每十股送转数量；可与有效股份变动比例不同。';
COMMENT ON COLUMN stock.source_corporate_action_events.share_change_ratio IS '公告用于除权参考价公式的总股本变动比例；无独立比例时为空。';
COMMENT ON COLUMN stock.source_corporate_action_events.source_quality IS '来源质量枚举：VERIFIED_OFFICIAL_NOTICE为官方公告可复核，UNSUPPORTED或UNRESOLVED不进入有效调整计算。';

CREATE INDEX source_corporate_action_symbol_effective_idx
    ON stock.source_corporate_action_events (source_symbol, effective_session, available_at);
COMMENT ON INDEX stock.source_corporate_action_symbol_effective_idx IS '按来源代码、生效会话和知识截止读取有限公司行为事件。';

CREATE TABLE stock.source_adjusted_daily_prices (
    adjusted_price_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_package_id bigint NOT NULL REFERENCES stock.source_packages (source_package_id),
    source_capture_id bigint NOT NULL,
    target_capture_id bigint NOT NULL,
    source_symbol text NOT NULL CHECK (source_symbol ~ '^(SH|SZ)\.[0-9]{6}$'),
    source_session date NOT NULL,
    target_session date NOT NULL,
    source_coordinate_id text NOT NULL CHECK (source_coordinate_id LIKE 'RAW:%'),
    target_coordinate_id text NOT NULL CHECK (target_coordinate_id LIKE 'ADJ:%'),
    event_set_revision char(64) NOT NULL CHECK (event_set_revision ~ '^[0-9a-f]{64}$'),
    event_refs text[] NOT NULL,
    applied_event_refs text[] NOT NULL,
    alpha_numerator numeric NOT NULL,
    alpha_denominator numeric NOT NULL CHECK (alpha_denominator > 0),
    beta_numerator numeric NOT NULL,
    beta_denominator numeric NOT NULL CHECK (beta_denominator > 0),
    price_tick numeric NOT NULL CHECK (price_tick > 0),
    adjusted_open numeric,
    adjusted_high numeric,
    adjusted_low numeric,
    adjusted_close numeric,
    price_quality text NOT NULL CHECK (price_quality IN ('OBSERVED', 'UNKNOWN')),
    adjustment_status text NOT NULL CHECK (
        adjustment_status IN ('APPLIED', 'IDENTITY_VERIFIED', 'UNKNOWN', 'NOT_COMPUTED')
    ),
    quality_reason text,
    action_coverage_status text NOT NULL CHECK (
        action_coverage_status IN ('SINGLE_NOTICE_WINDOW', 'COMPLETE', 'INCOMPLETE')
    ),
    research_mode text NOT NULL CHECK (research_mode = 'RETROSPECTIVE_NON_PIT'),
    calendar_id text NOT NULL,
    calendar_revision text NOT NULL,
    parameter_version text NOT NULL,
    algorithm_version text NOT NULL,
    window_start_session date NOT NULL,
    window_session_count integer NOT NULL CHECK (window_session_count > 0),
    input_digest char(64) NOT NULL CHECK (input_digest ~ '^[0-9a-f]{64}$'),
    created_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (source_capture_id, source_symbol, source_session)
        REFERENCES stock.source_raw_daily (source_capture_id, source_symbol, session_date)
        ON DELETE CASCADE,
    FOREIGN KEY (target_capture_id, source_symbol, target_session)
        REFERENCES stock.source_raw_daily (source_capture_id, source_symbol, session_date)
        ON DELETE CASCADE,
    UNIQUE (
        source_capture_id, source_symbol, source_session,
        target_capture_id, target_session, target_coordinate_id,
        event_set_revision, parameter_version, algorithm_version, calendar_revision
    ),
    CHECK (source_session <= target_session),
    CHECK (window_start_session <= source_session),
    CHECK (
        (price_quality = 'OBSERVED' AND adjustment_status IN ('APPLIED', 'IDENTITY_VERIFIED')
         AND adjusted_open > 0 AND adjusted_high > 0 AND adjusted_low > 0 AND adjusted_close > 0
         AND quality_reason IS NULL)
        OR
        (price_quality = 'UNKNOWN' AND adjustment_status IN ('UNKNOWN', 'NOT_COMPUTED')
         AND adjusted_open IS NULL AND adjusted_high IS NULL AND adjusted_low IS NULL AND adjusted_close IS NULL
         AND quality_reason IS NOT NULL)
    )
);

COMMENT ON TABLE stock.source_adjusted_daily_prices IS '保存由单股实际研究消费者需要的已提交仿射调整日线；通过精确TDX Raw捕获、目标会话、公司行为修订、坐标、算法和官方日历绑定，不改写Raw父事实。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.adjusted_price_id IS '调整日线行的数据库内部定位键；业务身份由下列来源、会话、坐标和修订列共同确定。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.source_package_id IS '来源TDX官方VIPDATA包的数字业务键。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.source_capture_id IS '该source_session Raw行的精确捕获ID。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.target_capture_id IS '该target_session Raw行的精确捕获ID，用于绑定目标价格坐标。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.source_symbol IS 'TDX来源代码；本事实限定于来源代码，不推导稳定证券实体。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.source_session IS '被仿射变换的原始价格交易会话。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.target_session IS '调整结果所采用的目标坐标锚定交易会话。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.source_coordinate_id IS '原始价格坐标身份，必须以RAW:开头。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.target_coordinate_id IS '调整目标坐标身份，必须以ADJ:开头并绑定算法及事件修订。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.event_set_revision IS '本次目标价格坐标中公司行为输入集合的语义修订摘要。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.event_refs IS '目标坐标所依据的事件引用数组；不存放事件整对象或原始公告内容。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.applied_event_refs IS 'source_session至target_session之间实际参与仿射组合的事件引用。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.alpha_numerator IS '仿射尺度alpha精确分子；与分母列共同表达有理数。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.alpha_denominator IS '仿射尺度alpha精确正分母。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.beta_numerator IS '仿射平移beta精确分子；与分母列共同表达有理数。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.beta_denominator IS '仿射平移beta精确正分母。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.price_tick IS '最终OHLC一次HALF_UP量化采用的价格最小刻度。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.adjusted_open IS '目标坐标下的开盘价；价格质量非OBSERVED时为空。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.adjusted_high IS '目标坐标下的最高价；价格质量非OBSERVED时为空。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.adjusted_low IS '目标坐标下的最低价；价格质量非OBSERVED时为空。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.adjusted_close IS '目标坐标下的收盘价；价格质量非OBSERVED时为空。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.price_quality IS '调整后OHLC质量：OBSERVED已验证、UNKNOWN缺来源或输入质量。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.adjustment_status IS '调整状态：APPLIED实际应用事件、IDENTITY_VERIFIED已证明恒等、UNKNOWN或NOT_COMPUTED。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.quality_reason IS '未知或未计算原因；有效价格行为时为空。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.action_coverage_status IS '事件来源覆盖：SINGLE_NOTICE_WINDOW仅本公告事件窗，COMPLETE完整覆盖，INCOMPLETE不完整；不把未知事件当作零调整。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.research_mode IS '历史复权研究时间身份；当前真实样本固定标为RETROSPECTIVE_NON_PIT。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.calendar_id IS '目标会话使用的官方交易日历身份。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.calendar_revision IS '目标会话使用的官方交易日历修订。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.parameter_version IS '公司行为单位和仿射公式参数合同版本。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.algorithm_version IS '仿射调整生产算法版本。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.window_start_session IS '本次单股调整输入窗口起始官方交易会话。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.window_session_count IS '本次实际输入的官方交易会话数量，不是自然日数。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.input_digest IS '有界Raw输入与事件集合语义的SHA-256摘要；不承载输入payload。';
COMMENT ON COLUMN stock.source_adjusted_daily_prices.created_at IS '调整事实事务提交时的数据库生成时刻；不代表行情或事件发布时间。';

CREATE INDEX source_adjusted_daily_symbol_target_idx
    ON stock.source_adjusted_daily_prices (source_symbol, target_session, source_session);
COMMENT ON INDEX stock.source_adjusted_daily_symbol_target_idx IS '支持个股调整日线按目标会话和来源日期有界读取。';

ALTER TABLE stock.source_corporate_action_events OWNER TO stock_a;
ALTER TABLE stock.source_adjusted_daily_prices OWNER TO stock_a;
