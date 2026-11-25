-- R02 前向迁移 0075：扩展 stock.source_price_limit_day_facts.limit_status 约束，
-- 允许写入注册制 IPO 前5个交易日无涨跌幅限制状态 NA_NO_PRICE_LIMIT，并与连板引擎 NA_RESET 语义对齐。

ALTER TABLE stock.source_price_limit_day_facts
    DROP CONSTRAINT IF EXISTS source_price_limit_day_facts_limit_status_check;

ALTER TABLE stock.source_price_limit_day_facts
    ADD CONSTRAINT source_price_limit_day_facts_limit_status_check
    CHECK (
        limit_status = ANY (
            ARRAY[
                'ASSESSABLE'::text,
                'NA_SUSPENDED'::text,
                'NA_NO_PRICE_LIMIT'::text,
                'UNKNOWN'::text
            ]
        )
    );

COMMENT ON COLUMN stock.source_price_limit_day_facts.limit_status IS '限价可判定状态：ASSESSABLE可判定、NA_SUSPENDED停牌无交易、NA_NO_PRICE_LIMIT无涨跌幅限制（如注册制上市前5个交易日）、UNKNOWN规则或参考价未知。';
COMMENT ON COLUMN stock.source_price_limit_day_facts.quality IS '限价事实质量标记（VERIFIED_RAW_NON_PIT/NA_SUSPENDED/NA_NO_PRICE_LIMIT/UNKNOWN）。';
