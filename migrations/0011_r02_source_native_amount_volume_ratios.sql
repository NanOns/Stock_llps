ALTER TABLE stock.source_symbol_price_profiles
    ADD COLUMN amount_ratio5 NUMERIC,
    ADD COLUMN amount_ratio5_exact_numerator NUMERIC(100, 0),
    ADD COLUMN amount_ratio5_exact_denominator NUMERIC(100, 0),
    ADD COLUMN amount_ratio5_quality TEXT,
    ADD COLUMN amount_ratio5_reason TEXT,
    ADD COLUMN amount_ratio20 NUMERIC,
    ADD COLUMN amount_ratio20_exact_numerator NUMERIC(100, 0),
    ADD COLUMN amount_ratio20_exact_denominator NUMERIC(100, 0),
    ADD COLUMN amount_ratio20_quality TEXT,
    ADD COLUMN amount_ratio20_reason TEXT,
    ADD COLUMN volume_ratio5 NUMERIC,
    ADD COLUMN volume_ratio5_exact_numerator NUMERIC(100, 0),
    ADD COLUMN volume_ratio5_exact_denominator NUMERIC(100, 0),
    ADD COLUMN volume_ratio5_quality TEXT,
    ADD COLUMN volume_ratio5_reason TEXT,
    ADD COLUMN volume_ratio20 NUMERIC,
    ADD COLUMN volume_ratio20_exact_numerator NUMERIC(100, 0),
    ADD COLUMN volume_ratio20_exact_denominator NUMERIC(100, 0),
    ADD COLUMN volume_ratio20_quality TEXT,
    ADD COLUMN volume_ratio20_reason TEXT;

ALTER TABLE stock.source_symbol_price_profiles
    ADD CONSTRAINT source_profiles_amount_ratio5_check
        CHECK (
            (
                amount_ratio5_quality = 'OBSERVED'
                AND amount_ratio5 IS NOT NULL
                AND amount_ratio5::text NOT IN ('NaN', 'Infinity', '-Infinity')
                AND amount_ratio5 >= 0
                AND amount_ratio5_exact_numerator IS NOT NULL
                AND amount_ratio5_exact_numerator >= 0
                AND amount_ratio5_exact_denominator > 0
                AND amount_ratio5_reason IS NULL
            )
            OR
            (
                amount_ratio5_quality = 'UNKNOWN'
                AND amount_ratio5 IS NULL
                AND amount_ratio5_exact_numerator IS NULL
                AND amount_ratio5_exact_denominator IS NULL
                AND amount_ratio5_reason IS NOT NULL
                AND btrim(amount_ratio5_reason) <> ''
            )
            OR (
                amount_ratio5 IS NULL
                AND amount_ratio5_exact_numerator IS NULL
                AND amount_ratio5_exact_denominator IS NULL
                AND amount_ratio5_quality IS NULL
                AND amount_ratio5_reason IS NULL
            )
        ),
    ADD CONSTRAINT source_profiles_amount_ratio20_check
        CHECK (
            (
                amount_ratio20_quality = 'OBSERVED'
                AND amount_ratio20 IS NOT NULL
                AND amount_ratio20::text NOT IN ('NaN', 'Infinity', '-Infinity')
                AND amount_ratio20 >= 0
                AND amount_ratio20_exact_numerator IS NOT NULL
                AND amount_ratio20_exact_numerator >= 0
                AND amount_ratio20_exact_denominator > 0
                AND amount_ratio20_reason IS NULL
            )
            OR
            (
                amount_ratio20_quality = 'UNKNOWN'
                AND amount_ratio20 IS NULL
                AND amount_ratio20_exact_numerator IS NULL
                AND amount_ratio20_exact_denominator IS NULL
                AND amount_ratio20_reason IS NOT NULL
                AND btrim(amount_ratio20_reason) <> ''
            )
            OR (
                amount_ratio20 IS NULL
                AND amount_ratio20_exact_numerator IS NULL
                AND amount_ratio20_exact_denominator IS NULL
                AND amount_ratio20_quality IS NULL
                AND amount_ratio20_reason IS NULL
            )
        ),
    ADD CONSTRAINT source_profiles_volume_ratio5_check
        CHECK (
            (
                volume_ratio5_quality = 'OBSERVED'
                AND volume_ratio5 IS NOT NULL
                AND volume_ratio5::text NOT IN ('NaN', 'Infinity', '-Infinity')
                AND volume_ratio5 >= 0
                AND volume_ratio5_exact_numerator IS NOT NULL
                AND volume_ratio5_exact_numerator >= 0
                AND volume_ratio5_exact_denominator > 0
                AND volume_ratio5_reason IS NULL
            )
            OR
            (
                volume_ratio5_quality = 'UNKNOWN'
                AND volume_ratio5 IS NULL
                AND volume_ratio5_exact_numerator IS NULL
                AND volume_ratio5_exact_denominator IS NULL
                AND volume_ratio5_reason IS NOT NULL
                AND btrim(volume_ratio5_reason) <> ''
            )
            OR (
                volume_ratio5 IS NULL
                AND volume_ratio5_exact_numerator IS NULL
                AND volume_ratio5_exact_denominator IS NULL
                AND volume_ratio5_quality IS NULL
                AND volume_ratio5_reason IS NULL
            )
        ),
    ADD CONSTRAINT source_profiles_volume_ratio20_check
        CHECK (
            (
                volume_ratio20_quality = 'OBSERVED'
                AND volume_ratio20 IS NOT NULL
                AND volume_ratio20::text NOT IN ('NaN', 'Infinity', '-Infinity')
                AND volume_ratio20 >= 0
                AND volume_ratio20_exact_numerator IS NOT NULL
                AND volume_ratio20_exact_numerator >= 0
                AND volume_ratio20_exact_denominator > 0
                AND volume_ratio20_reason IS NULL
            )
            OR
            (
                volume_ratio20_quality = 'UNKNOWN'
                AND volume_ratio20 IS NULL
                AND volume_ratio20_exact_numerator IS NULL
                AND volume_ratio20_exact_denominator IS NULL
                AND volume_ratio20_reason IS NOT NULL
                AND btrim(volume_ratio20_reason) <> ''
            )
            OR (
                volume_ratio20 IS NULL
                AND volume_ratio20_exact_numerator IS NULL
                AND volume_ratio20_exact_denominator IS NULL
                AND volume_ratio20_quality IS NULL
                AND volume_ratio20_reason IS NULL
            )
        );

COMMENT ON COLUMN stock.source_symbol_price_profiles.amount_ratio5 IS 'Core原生成交金额比值：当前TDX同包原生金额除以严格前5个合法actual原生金额均值；无量纲，不代表人民币。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.amount_ratio5_exact_numerator IS '原生金额比值的精确非负整数分子；与正分母共同表示有理数。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.amount_ratio5_exact_denominator IS '原生金额比值的精确正整数分母；与分子共同表示有理数。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.amount_ratio5_quality IS '原生金额比值质量：OBSERVED为精确值可用，UNKNOWN为当前或严格前5个actual依赖不可用。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.amount_ratio5_reason IS '原生金额比值质量为UNKNOWN时的明确原因码；OBSERVED时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.amount_ratio20 IS 'Core原生成交金额比值：当前TDX同包原生金额除以严格前20个合法actual原生金额均值；无量纲，不代表人民币。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.amount_ratio20_exact_numerator IS '原生金额比值的精确非负整数分子；与正分母共同表示有理数。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.amount_ratio20_exact_denominator IS '原生金额比值的精确正整数分母；与分子共同表示有理数。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.amount_ratio20_quality IS '原生金额比值质量：OBSERVED为精确值可用，UNKNOWN为当前或严格前20个actual依赖不可用。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.amount_ratio20_reason IS '原生金额比值质量为UNKNOWN时的明确原因码；OBSERVED时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.volume_ratio5 IS 'Core原生成交量比值：当前TDX同包原生数量除以严格前5个合法actual原生数量均值；无量纲，不代表股数。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.volume_ratio5_exact_numerator IS '原生数量比值的精确非负整数分子；与正分母共同表示有理数。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.volume_ratio5_exact_denominator IS '原生数量比值的精确正整数分母；与分子共同表示有理数。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.volume_ratio5_quality IS '原生数量比值质量：OBSERVED为精确值可用，UNKNOWN为当前或严格前5个actual依赖不可用。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.volume_ratio5_reason IS '原生数量比值质量为UNKNOWN时的明确原因码；OBSERVED时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.volume_ratio20 IS 'Core原生成交量比值：当前TDX同包原生数量除以严格前20个合法actual原生数量均值；无量纲，不代表股数。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.volume_ratio20_exact_numerator IS '原生数量比值的精确非负整数分子；与正分母共同表示有理数。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.volume_ratio20_exact_denominator IS '原生数量比值的精确正整数分母；与分子共同表示有理数。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.volume_ratio20_quality IS '原生数量比值质量：OBSERVED为精确值可用，UNKNOWN为当前或严格前20个actual依赖不可用。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.volume_ratio20_reason IS '原生数量比值质量为UNKNOWN时的明确原因码；OBSERVED时为空。';
