-- R05 工作包 A 前向迁移（0098）：
-- 为 stock.source_forward_path_facts 增加 MFE / MAE / MDD_CLOSE 分项质量与原因字段，
-- 支持在部分字段缺失或中间会话坐标不可比时独立保存端点收益与各路径指标的三态质量。

ALTER TABLE stock.source_forward_path_facts
    ADD COLUMN mfe_quality text NOT NULL DEFAULT 'UNKNOWN'
        CHECK (mfe_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_DUE')),
    ADD COLUMN mfe_reason text,
    ADD COLUMN mae_quality text NOT NULL DEFAULT 'UNKNOWN'
        CHECK (mae_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_DUE')),
    ADD COLUMN mae_reason text,
    ADD COLUMN mdd_quality text NOT NULL DEFAULT 'UNKNOWN'
        CHECK (mdd_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_DUE')),
    ADD COLUMN mdd_reason text;

UPDATE stock.source_forward_path_facts
SET
    mfe_quality = CASE WHEN mfe_numerator IS NOT NULL THEN 'OBSERVED' ELSE 'UNKNOWN' END,
    mfe_reason = CASE WHEN mfe_numerator IS NOT NULL THEN NULL ELSE COALESCE(path_reasons[1], 'LEGACY_PATH_UNKNOWN') END,
    mae_quality = CASE WHEN mae_numerator IS NOT NULL THEN 'OBSERVED' ELSE 'UNKNOWN' END,
    mae_reason = CASE WHEN mae_numerator IS NOT NULL THEN NULL ELSE COALESCE(path_reasons[1], 'LEGACY_PATH_UNKNOWN') END,
    mdd_quality = CASE WHEN mdd_numerator IS NOT NULL THEN 'OBSERVED' ELSE 'UNKNOWN' END,
    mdd_reason = CASE WHEN mdd_numerator IS NOT NULL THEN NULL ELSE COALESCE(path_reasons[1], 'LEGACY_PATH_UNKNOWN') END;

COMMENT ON COLUMN stock.source_forward_path_facts.mfe_quality IS 'MFE（T+1..T+N日内最高价相对T0收盘最大收益）分项质量（OBSERVED/UNKNOWN/NOT_DUE）。';
COMMENT ON COLUMN stock.source_forward_path_facts.mfe_reason IS 'MFE不可观测或未到期时的原因码；OBSERVED时为NULL。';
COMMENT ON COLUMN stock.source_forward_path_facts.mae_quality IS 'MAE（T+1..T+N日内最低价相对T0收盘最小收益）分项质量（OBSERVED/UNKNOWN/NOT_DUE）。';
COMMENT ON COLUMN stock.source_forward_path_facts.mae_reason IS 'MAE不可观测或未到期时的原因码；OBSERVED时为NULL。';
COMMENT ON COLUMN stock.source_forward_path_facts.mdd_quality IS 'MDD_CLOSE（含T0逐日收盘先峰后谷最大回撤）分项质量（OBSERVED/UNKNOWN/NOT_DUE）。';
COMMENT ON COLUMN stock.source_forward_path_facts.mdd_reason IS 'MDD_CLOSE不可观测或未到期时的原因码；OBSERVED时为NULL。';
