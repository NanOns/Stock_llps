COMMENT ON COLUMN stock.source_pool_seed_day_facts.target_close_numerator IS
    '目标日BaoStock QFQ（adjustflag=2）收盘价的精确分子，单位为人民币元/股；仅当target_close_quality=OBSERVED时保存，属于NON_PIT后验坐标，不替代RAW实际成交或限价输入。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.target_close_denominator IS
    '目标日BaoStock QFQ（adjustflag=2）收盘价精确分数的正分母；仅当target_close_quality=OBSERVED时保存。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.target_close_quality IS
    '目标日QFQ收盘字段自身的质量状态；OBSERVED表示目标收盘值可用，UNKNOWN表示缺失或质量未证，不代表交易状态或完整OHLC质量。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.target_close_reason IS
    '目标日QFQ收盘质量为UNKNOWN时的具名原因码；OBSERVED时为空，不承载RAW成交或交易状态原因。';
