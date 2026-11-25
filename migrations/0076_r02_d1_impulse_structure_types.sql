-- R02 前向迁移 0076：扩展 stock.source_d1_structure_day_facts.structure_type 约束，
-- 允许写入中阳线支撑锚点 IMPULSE_BODY 与 IMPULSE_LOW（ALG-29 / §10.4），兼容既有5类结构事实。

ALTER TABLE stock.source_d1_structure_day_facts
    DROP CONSTRAINT IF EXISTS source_d1_structure_day_facts_structure_type_check;

ALTER TABLE stock.source_d1_structure_day_facts
    ADD CONSTRAINT source_d1_structure_day_facts_structure_type_check
    CHECK (
        structure_type = ANY (
            ARRAY[
                'PIVOT_LOW'::text,
                'PIVOT_HIGH'::text,
                'RANGE_FLOOR'::text,
                'RANGE_CEILING'::text,
                'GAP_ZONE'::text,
                'IMPULSE_BODY'::text,
                'IMPULSE_LOW'::text
            ]
        )
    );

COMMENT ON COLUMN stock.source_d1_structure_day_facts.structure_type IS 'D1结构锚点类型：PIVOT_LOW/PIVOT_HIGH/RANGE_FLOOR/RANGE_CEILING/GAP_ZONE及中阳线支撑锚IMPULSE_BODY/IMPULSE_LOW。';
