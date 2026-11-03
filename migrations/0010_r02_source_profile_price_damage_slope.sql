ALTER TABLE stock.source_symbol_price_profiles
    ADD COLUMN ma_slope20_atr NUMERIC,
    ADD COLUMN ma_slope20_atr_exact_numerator NUMERIC(100, 0),
    ADD COLUMN ma_slope20_atr_exact_denominator NUMERIC(100, 0),
    ADD COLUMN ma_slope20_atr_quality TEXT,
    ADD COLUMN ma_slope20_atr_reason TEXT,
    ADD COLUMN price_damage BOOLEAN,
    ADD COLUMN price_damage_quality TEXT,
    ADD COLUMN price_damage_reason TEXT;

ALTER TABLE stock.source_symbol_price_profiles
    ADD CONSTRAINT source_symbol_profiles_slope_quality_check
        CHECK (
            (
                ma_slope20_atr IS NULL
                AND ma_slope20_atr_exact_numerator IS NULL
                AND ma_slope20_atr_exact_denominator IS NULL
                AND ma_slope20_atr_quality IS NULL
                AND ma_slope20_atr_reason IS NULL
            )
            OR (
                ma_slope20_atr_quality = 'OBSERVED'
                AND ma_slope20_atr IS NOT NULL
                AND ma_slope20_atr::text NOT IN ('NaN', 'Infinity', '-Infinity')
                AND ma_slope20_atr_exact_numerator IS NOT NULL
                AND ma_slope20_atr_exact_numerator::text NOT IN ('NaN', 'Infinity', '-Infinity')
                AND ma_slope20_atr_exact_denominator IS NOT NULL
                AND ma_slope20_atr_exact_denominator > 0
                AND ma_slope20_atr_exact_denominator::text NOT IN ('NaN', 'Infinity', '-Infinity')
                AND ma_slope20_atr_reason IS NULL
            )
            OR (
                ma_slope20_atr_quality = 'UNKNOWN'
                AND ma_slope20_atr IS NULL
                AND ma_slope20_atr_exact_numerator IS NULL
                AND ma_slope20_atr_exact_denominator IS NULL
                AND ma_slope20_atr_reason IS NOT NULL
                AND btrim(ma_slope20_atr_reason) <> ''
            )
        ),
    ADD CONSTRAINT source_symbol_profiles_damage_quality_check
        CHECK (
            (
                price_damage IS NULL
                AND price_damage_quality IS NULL
                AND price_damage_reason IS NULL
            )
            OR (
                price_damage_quality = 'OBSERVED'
                AND price_damage IS NOT NULL
                AND price_damage_reason IS NULL
            )
            OR (
                price_damage_quality = 'UNKNOWN'
                AND price_damage IS NULL
                AND price_damage_reason IS NOT NULL
                AND btrim(price_damage_reason) <> ''
            )
        ),
    ADD CONSTRAINT source_symbol_profiles_damage_slope_pair_check
        CHECK (
            (
                ma_slope20_atr IS NULL
                AND ma_slope20_atr_exact_numerator IS NULL
                AND ma_slope20_atr_exact_denominator IS NULL
                AND ma_slope20_atr_quality IS NULL
                AND ma_slope20_atr_reason IS NULL
                AND price_damage IS NULL
                AND price_damage_quality IS NULL
                AND price_damage_reason IS NULL
            )
            OR (
                ma_slope20_atr_quality IS NOT NULL
                AND price_damage_quality IS NOT NULL
            )
        );

COMMENT ON COLUMN stock.source_symbol_price_profiles.ma_slope20_atr IS 'Core MA20与前5个合法actual端点MA20之差除以当前ATR20；无量纲，沿RAW_UNADJUSTED来源价格坐标；旧画像修订为空表示该修订未计算。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ma_slope20_atr_exact_numerator IS 'Core MA20趋势斜率精确Fraction的有符号分子；与分母共同表示斜率精确值，旧画像修订为空表示未计算。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ma_slope20_atr_exact_denominator IS 'Core MA20趋势斜率精确Fraction的正分母；单位price/ATR相消为无量纲，旧画像修订为空表示未计算。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ma_slope20_atr_quality IS 'Core MA20趋势斜率质量：OBSERVED为精确数值可用，UNKNOWN为依赖不足或非法并须有原因；旧画像修订为空表示未计算。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.ma_slope20_atr_reason IS 'Core MA20趋势斜率未知原因代码；OBSERVED及未计算的旧画像修订为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.price_damage IS 'Core既有价格损伤规则原生布尔值：收盘严格低于前20实际低点减0.5倍ATR20且ret1小于0；FALSE是已观察事实，不代表总体风险或买入资格。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.price_damage_quality IS 'Core价格损伤质量：OBSERVED时布尔值为真或假，UNKNOWN时无布尔值且有原因；旧画像修订为空表示未计算。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.price_damage_reason IS 'Core价格损伤未知原因代码；OBSERVED及未计算的旧画像修订为空。';
