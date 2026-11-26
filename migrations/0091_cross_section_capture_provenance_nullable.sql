-- 横截面按目标日/证券与端点业务键保存；价格捕获仅作可为空溯源元数据。
-- 多证券历史窗口可由逐证券的不同坐标捕获组成，不能伪造单一整表捕获身份。
ALTER TABLE stock.source_cross_section_day_facts
    ALTER COLUMN price_capture_id DROP NOT NULL;

ALTER TABLE stock.source_cross_section_endpoints
    ALTER COLUMN price_capture_id DROP NOT NULL;

COMMENT ON COLUMN stock.source_cross_section_day_facts.price_capture_id IS
    'QFQ价格来源捕获溯源编号；目标日横截面输入含多个捕获或无法唯一归属时为NULL，不参与业务键、范围筛选或坐标连续性判定。';

COMMENT ON COLUMN stock.source_cross_section_endpoints.price_capture_id IS
    'QFQ价格来源捕获溯源编号；端点输入含多个捕获或无法唯一归属时为NULL，不参与业务键、范围筛选或坐标连续性判定。';
