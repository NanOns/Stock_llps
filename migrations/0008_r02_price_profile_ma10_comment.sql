-- 为0007漏写的Core MA10数据库列注释补齐，不改表结构或业务行。
COMMENT ON COLUMN stock.source_symbol_price_profiles.ma10 IS 'Core MA10原始价格坐标末10个合法ACTUAL Bar收盘均值；仅OBSERVED时有值，单位沿用来源价格坐标。';
