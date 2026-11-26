-- 市场宽度按日期保存单一有效事实；capture仅作可空溯源，逐证券QFQ捕获留在逐股事实。
ALTER TABLE stock.source_market_breadth_days
    ALTER COLUMN source_capture_id DROP NOT NULL,
    ALTER COLUMN capture_read_started_at DROP NOT NULL,
    ALTER COLUMN capture_validated_at DROP NOT NULL;

ALTER TABLE stock.source_market_breadth_symbols
    ALTER COLUMN source_capture_id DROP NOT NULL;

ALTER TABLE stock.source_market_breadth_change_days
    ALTER COLUMN before_source_capture_id DROP NOT NULL,
    ALTER COLUMN target_source_capture_id DROP NOT NULL;

COMMENT ON COLUMN stock.source_market_breadth_days.source_capture_id IS
    '目标日来源捕获溯源编号；多证券或坐标对应多个捕获、不能唯一归属时为空，不参与业务键或Reader筛选。';
COMMENT ON COLUMN stock.source_market_breadth_days.capture_read_started_at IS
    '唯一目标来源捕获的读取开始时刻；无唯一捕获身份时为空，时间含时区。';
COMMENT ON COLUMN stock.source_market_breadth_days.capture_validated_at IS
    '唯一目标来源捕获的验证完成时刻；无唯一捕获身份时为空，时间含时区。';
COMMENT ON COLUMN stock.source_market_breadth_symbols.source_capture_id IS
    '该证券目标日QFQ坐标来源捕获溯源编号；缺失时为空，不参与业务键。';
COMMENT ON COLUMN stock.source_market_breadth_change_days.before_source_capture_id IS
    '前态宽度来源捕获溯源编号；来源身份不唯一时为空，不参与变化业务键。';
COMMENT ON COLUMN stock.source_market_breadth_change_days.target_source_capture_id IS
    '目标宽度来源捕获溯源编号；来源身份不唯一时为空，不参与变化业务键。';
