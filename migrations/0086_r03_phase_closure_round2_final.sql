-- R03 全阶段第二轮（最终）集中收尾：补充正式市场环境业务唯一键与前态引用、Native 最后已知展示/有效成熟度双列（0086）。

-- 1. stock.source_market_environment_day_facts（A包：正式市场环境同一版本绑定与前态引用）
ALTER TABLE stock.source_market_environment_day_facts
    ADD COLUMN IF NOT EXISTS formal_scope_key text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS profile_revision text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS universe_version_t text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS universe_version_t3 text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS breadth_source_kind text NOT NULL DEFAULT 'UNKNOWN',
    ADD COLUMN IF NOT EXISTS prev_trade_date date NULL,
    ADD COLUMN IF NOT EXISTS prev_formal_scope_key text NULL;

ALTER TABLE stock.source_market_environment_day_facts
    DROP CONSTRAINT IF EXISTS source_market_environment_day_facts_breadth_source_kind_check,
    ADD CONSTRAINT source_market_environment_day_facts_breadth_source_kind_check
        CHECK (breadth_source_kind IN ('FORMAL_UNIVERSE', 'UNKNOWN', 'SOURCE_LEGACY'));

ALTER TABLE stock.source_market_environment_day_facts
    DROP CONSTRAINT IF EXISTS source_market_environment_day_facts_uq,
    ADD CONSTRAINT source_market_environment_day_facts_uq UNIQUE (
        target_session,
        stock_capture_id,
        primary_index_symbol,
        calendar_revision,
        rule_version,
        formal_scope_key,
        profile_revision,
        universe_version_t,
        universe_version_t3
    );

COMMENT ON COLUMN stock.source_market_environment_day_facts.formal_scope_key IS '绑定的正式市场宽度 scope_key；未绑定正式宽度时为空字符串';
COMMENT ON COLUMN stock.source_market_environment_day_facts.profile_revision IS '绑定的个股特征/参与度 profile_revision；未指定时为空字符串';
COMMENT ON COLUMN stock.source_market_environment_day_facts.universe_version_t IS '绑定的当日研究总体版本 universe_version_t；未绑定时为空字符串';
COMMENT ON COLUMN stock.source_market_environment_day_facts.universe_version_t3 IS '绑定的 t-3 日研究总体版本 universe_version_t3；未绑定时为空字符串';
COMMENT ON COLUMN stock.source_market_environment_day_facts.breadth_source_kind IS '宽度轴来源类型：FORMAL_UNIVERSE=正式日期总体宽度，UNKNOWN=缺失/未知，SOURCE_LEGACY=旧来源级诊断兼容行';
COMMENT ON COLUMN stock.source_market_environment_day_facts.prev_trade_date IS '实际消费的上一有效市场环境交易日（用于跨日滞后前态追踪与级联失效）';
COMMENT ON COLUMN stock.source_market_environment_day_facts.prev_formal_scope_key IS '上一有效市场环境记录的 formal_scope_key';

-- 2. stock.r03_sector_native_day_facts（C包：分离持久化 last_known_display 与 last_known_effective）
ALTER TABLE stock.r03_sector_native_day_facts
    ADD COLUMN IF NOT EXISTS last_known_display text NULL,
    ADD COLUMN IF NOT EXISTS last_known_effective text NULL;

ALTER TABLE stock.r03_sector_native_day_facts
    DROP CONSTRAINT IF EXISTS r03_sector_native_day_facts_last_known_display_check,
    ADD CONSTRAINT r03_sector_native_day_facts_last_known_display_check
        CHECK (last_known_display IS NULL OR last_known_display IN ('NONE', 'PREWATCH', 'WARM', 'CONFIRMED')),
    DROP CONSTRAINT IF EXISTS r03_sector_native_day_facts_last_known_effective_check,
    ADD CONSTRAINT r03_sector_native_day_facts_last_known_effective_check
        CHECK (last_known_effective IS NULL OR last_known_effective IN ('NONE', 'PREWATCH', 'WARM', 'CONFIRMED'));

COMMENT ON COLUMN stock.r03_sector_native_day_facts.last_known_display IS '最近一次已知展示成熟度（NONE/PREWATCH/WARM/CONFIRMED），用于 UNKNOWN 日保护展示滞后不被提前降级';
COMMENT ON COLUMN stock.r03_sector_native_day_facts.last_known_effective IS '最近一次已知有效成熟度（NONE/PREWATCH/WARM/CONFIRMED），用于 UNKNOWN 恢复后连续降级与滞后判断';
