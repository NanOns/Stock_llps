-- R01 缺陷修复：放宽个股价格画像停牌状态约束并接入 BaoStock raw_tradestatus_value，
-- 同时修正 source_raw_daily 中 TARGET_MISSING 缺失行误标 VERIFIED 的单位状态。

ALTER TABLE stock.source_symbol_price_profiles
    DROP CONSTRAINT IF EXISTS source_symbol_price_profiles_suspension_status_check;

ALTER TABLE stock.source_symbol_price_profiles
    ADD CONSTRAINT source_symbol_price_profiles_suspension_status_check
    CHECK (
        suspension_status = ANY (
            ARRAY[
                'SUSPENSION_STATUS_UNKNOWN'::text,
                'SUSPENSION_STATUS_TRADING'::text,
                'SUSPENSION_STATUS_SUSPENDED'::text
            ]
        )
    );

COMMENT ON COLUMN stock.source_symbol_price_profiles.suspension_status IS '目标交易日个股停牌状态，基于同捕获同日 BaoStock raw_tradestatus_value（1=正常交易 SUSPENSION_STATUS_TRADING，0=停牌 SUSPENSION_STATUS_SUSPENDED，缺失或非 OBSERVED 质量为 SUSPENSION_STATUS_UNKNOWN）。';

UPDATE stock.source_symbol_price_profiles AS p
SET suspension_status = CASE
    WHEN r.raw_tradestatus_quality = 'OBSERVED' AND r.raw_tradestatus_value = '1'
        THEN 'SUSPENSION_STATUS_TRADING'
    WHEN r.raw_tradestatus_quality = 'OBSERVED' AND r.raw_tradestatus_value = '0'
        THEN 'SUSPENSION_STATUS_SUSPENDED'
    ELSE 'SUSPENSION_STATUS_UNKNOWN'
END
FROM stock.source_raw_daily AS r
WHERE p.source_raw_id = r.id;

UPDATE stock.source_raw_daily
SET amount_unit_status = 'UNIT_UNVERIFIED',
    volume_unit_status = 'UNIT_UNVERIFIED',
    qfq_amount_unit_status = CASE
        WHEN qfq_bar_status = 'TARGET_PRESENT' THEN qfq_amount_unit_status
        ELSE NULL
    END,
    qfq_volume_unit_status = CASE
        WHEN qfq_bar_status = 'TARGET_PRESENT' THEN qfq_volume_unit_status
        ELSE NULL
    END
WHERE bar_status <> 'TARGET_PRESENT';
