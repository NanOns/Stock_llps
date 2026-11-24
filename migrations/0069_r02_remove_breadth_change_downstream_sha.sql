-- R02-P1：移除相邻市场宽度变化表中的下游集合SHA-256持久化列，保留逐代码集合相等性与分母核验（DB-08）。

ALTER TABLE stock.source_market_breadth_change_days
    DROP COLUMN source_set_sha256_before,
    DROP COLUMN source_set_sha256_target,
    DROP COLUMN eligible_set_sha256_before,
    DROP COLUMN eligible_set_sha256_target;

COMMENT ON TABLE stock.source_market_breadth_change_days IS '保存相邻官方会话已存BaoStock QFQ市场宽度的typed变化结果；只有同一捕获、日历、算法、来源集合和可评价集合逐代码完全一致时才给出差值，不持久化下游SHA摘要。';
