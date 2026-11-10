-- 现役全池Seed消费者需要的Core收益和流动性字段；历史行保留空值。

ALTER TABLE stock.source_symbol_price_profiles
    ADD COLUMN ret1 NUMERIC,
    ADD COLUMN ret1_exact_numerator NUMERIC(100, 0),
    ADD COLUMN ret1_exact_denominator NUMERIC(100, 0),
    ADD COLUMN ret1_quality TEXT,
    ADD COLUMN ret1_reason TEXT,
    ADD CONSTRAINT source_profiles_ret1_check CHECK (coalesce((
        (ret1_quality = 'OBSERVED' AND ret1 IS NOT NULL
         AND ret1::text NOT IN ('NaN', 'Infinity', '-Infinity')
         AND ret1_exact_numerator IS NOT NULL AND ret1_exact_denominator > 0
         AND ret1_reason IS NULL)
        OR (ret1_quality IN ('UNKNOWN', 'NOT_APPLICABLE') AND ret1 IS NULL
         AND ret1_exact_numerator IS NULL AND ret1_exact_denominator IS NULL
         AND ret1_reason IS NOT NULL AND btrim(ret1_reason) <> '')
        OR (ret1 IS NULL AND ret1_exact_numerator IS NULL
         AND ret1_exact_denominator IS NULL AND ret1_quality IS NULL AND ret1_reason IS NULL)
    ), false));
COMMENT ON COLUMN stock.source_symbol_price_profiles.ret1 IS '单会话收益率，fraction；旧画像行可为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ret1_exact_numerator IS '单会话收益率，fraction精确值分子；未知时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ret1_exact_denominator IS '单会话收益率，fraction精确值正分母；未知时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ret1_quality IS '单会话收益率，fraction质量：OBSERVED、UNKNOWN或NOT_APPLICABLE；旧画像为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ret1_reason IS '单会话收益率，fraction不可用的具名原因；已观测时为空。';

ALTER TABLE stock.source_symbol_price_profiles
    ADD COLUMN ret5 NUMERIC,
    ADD COLUMN ret5_exact_numerator NUMERIC(100, 0),
    ADD COLUMN ret5_exact_denominator NUMERIC(100, 0),
    ADD COLUMN ret5_quality TEXT,
    ADD COLUMN ret5_reason TEXT,
    ADD CONSTRAINT source_profiles_ret5_check CHECK (coalesce((
        (ret5_quality = 'OBSERVED' AND ret5 IS NOT NULL
         AND ret5::text NOT IN ('NaN', 'Infinity', '-Infinity')
         AND ret5_exact_numerator IS NOT NULL AND ret5_exact_denominator > 0
         AND ret5_reason IS NULL)
        OR (ret5_quality IN ('UNKNOWN', 'NOT_APPLICABLE') AND ret5 IS NULL
         AND ret5_exact_numerator IS NULL AND ret5_exact_denominator IS NULL
         AND ret5_reason IS NOT NULL AND btrim(ret5_reason) <> '')
        OR (ret5 IS NULL AND ret5_exact_numerator IS NULL
         AND ret5_exact_denominator IS NULL AND ret5_quality IS NULL AND ret5_reason IS NULL)
    ), false));
COMMENT ON COLUMN stock.source_symbol_price_profiles.ret5 IS '五会话收益率，fraction；旧画像行可为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ret5_exact_numerator IS '五会话收益率，fraction精确值分子；未知时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ret5_exact_denominator IS '五会话收益率，fraction精确值正分母；未知时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ret5_quality IS '五会话收益率，fraction质量：OBSERVED、UNKNOWN或NOT_APPLICABLE；旧画像为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ret5_reason IS '五会话收益率，fraction不可用的具名原因；已观测时为空。';

ALTER TABLE stock.source_symbol_price_profiles
    ADD COLUMN liquidity_mean20 NUMERIC,
    ADD COLUMN liquidity_mean20_exact_numerator NUMERIC(100, 0),
    ADD COLUMN liquidity_mean20_exact_denominator NUMERIC(100, 0),
    ADD COLUMN liquidity_mean20_quality TEXT,
    ADD COLUMN liquidity_mean20_reason TEXT,
    ADD CONSTRAINT source_profiles_liquidity_mean20_check CHECK (coalesce((
        (liquidity_mean20_quality = 'OBSERVED' AND liquidity_mean20 IS NOT NULL
         AND liquidity_mean20::text NOT IN ('NaN', 'Infinity', '-Infinity')
         AND liquidity_mean20_exact_numerator IS NOT NULL AND liquidity_mean20_exact_denominator > 0
         AND liquidity_mean20_reason IS NULL)
        OR (liquidity_mean20_quality IN ('UNKNOWN', 'NOT_APPLICABLE') AND liquidity_mean20 IS NULL
         AND liquidity_mean20_exact_numerator IS NULL AND liquidity_mean20_exact_denominator IS NULL
         AND liquidity_mean20_reason IS NOT NULL AND btrim(liquidity_mean20_reason) <> '')
        OR (liquidity_mean20 IS NULL AND liquidity_mean20_exact_numerator IS NULL
         AND liquidity_mean20_exact_denominator IS NULL AND liquidity_mean20_quality IS NULL AND liquidity_mean20_reason IS NULL)
    ), false));
COMMENT ON COLUMN stock.source_symbol_price_profiles.liquidity_mean20 IS '严格前20个有效会话成交额均值，CNY；来源单位未证实时为UNKNOWN；旧画像行可为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.liquidity_mean20_exact_numerator IS '严格前20个有效会话成交额均值，CNY；来源单位未证实时为UNKNOWN精确值分子；未知时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.liquidity_mean20_exact_denominator IS '严格前20个有效会话成交额均值，CNY；来源单位未证实时为UNKNOWN精确值正分母；未知时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.liquidity_mean20_quality IS '严格前20个有效会话成交额均值，CNY；来源单位未证实时为UNKNOWN质量：OBSERVED、UNKNOWN或NOT_APPLICABLE；旧画像为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.liquidity_mean20_reason IS '严格前20个有效会话成交额均值，CNY；来源单位未证实时为UNKNOWN不可用的具名原因；已观测时为空。';
