-- R02 缺陷修复（BUG-R02-01）：为个股价格画像事实表补齐 11 项基础画像枚举值数组列 profile_values，
-- 与 profile_field_ids / profile_statuses / profile_qualities / profile_reasons 严格一一对应，
-- 缺失或未观测项以空字符串 '' 占位，禁止 NULL 元素。

ALTER TABLE stock.source_symbol_price_profiles
    ADD COLUMN IF NOT EXISTS profile_values text[]
    NOT NULL
    DEFAULT ARRAY['', '', '', '', '', '', '', '', '', '', '']::text[];

ALTER TABLE stock.source_symbol_price_profiles
    DROP CONSTRAINT IF EXISTS source_symbol_price_profiles_profile_values_check;

ALTER TABLE stock.source_symbol_price_profiles
    ADD CONSTRAINT source_symbol_price_profiles_profile_values_check
    CHECK (
        COALESCE(array_ndims(profile_values), 0) = 1
        AND COALESCE(array_length(profile_values, 1), 0) = 11
        AND array_lower(profile_values, 1) = 1
        AND array_position(profile_values, NULL) IS NULL
    );

COMMENT ON COLUMN stock.source_symbol_price_profiles.profile_values IS '11项基础价格画像（ALG-17/§9.7）对应的实际分类枚举值数组，按 profile_field_ids 字典序一一对应；OBSERVED 时为精确分类值（如 UPTREND_STRONG/HIGH_ZONE/BULL_ALIGNED/UP 等），UNKNOWN/NOT_APPLICABLE/CONTRACT_UNBOUND 时为空字符串 ''''。';
