-- R08/业务09：为BaoStock成交额与成交量单位合同补充明确输入单位。
-- 旧版本没有保存该语义时使用UNKNOWN标记；新写入器拒绝该标记。
ALTER TABLE stock.source_baostock_unit_contracts
    ADD COLUMN amount_input_unit text,
    ADD COLUMN volume_input_unit text;

UPDATE stock.source_baostock_unit_contracts
SET amount_input_unit = 'LEGACY_UNIT_NOT_RECORDED',
    volume_input_unit = 'LEGACY_UNIT_NOT_RECORDED';

ALTER TABLE stock.source_baostock_unit_contracts
    ALTER COLUMN amount_input_unit SET NOT NULL,
    ALTER COLUMN volume_input_unit SET NOT NULL,
    ADD CONSTRAINT source_baostock_amount_input_unit_valid_ck
        CHECK (
            length(amount_input_unit) BETWEEN 1 AND 64
            AND btrim(amount_input_unit) <> ''
        ),
    ADD CONSTRAINT source_baostock_volume_input_unit_valid_ck
        CHECK (
            length(volume_input_unit) BETWEEN 1 AND 64
            AND btrim(volume_input_unit) <> ''
        );

COMMENT ON COLUMN stock.source_baostock_unit_contracts.amount_input_unit IS 'BaoStock成交金额原始响应数值的输入单位；LEGACY_UNIT_NOT_RECORDED表示历史单位语义未保存，禁止新写入使用。';
COMMENT ON COLUMN stock.source_baostock_unit_contracts.volume_input_unit IS 'BaoStock成交量原始响应数值的输入单位；LEGACY_UNIT_NOT_RECORDED表示历史单位语义未保存，禁止新写入使用。';
