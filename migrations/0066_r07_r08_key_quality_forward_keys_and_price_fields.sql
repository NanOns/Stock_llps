-- 代码预备迁移：跨会话自然键及BaoStock OHLC字段级质量。
-- 应先在独立READ ONLY事务单独运行首个DO块；正式迁移也会先重复该只读检查。
-- 异常时保留原行并中止；后续仅补字段质量元数据，不改价格值。

DO $$
DECLARE
    invalid_raw_ohlc bigint;
    invalid_qfq_ohlc bigint;
    invalid_raw_status bigint;
    invalid_raw_is_st bigint;
    invalid_qfq_status bigint;
    invalid_qfq_is_st bigint;
BEGIN
    SELECT count(*) INTO invalid_raw_ohlc
    FROM stock.source_raw_daily
    WHERE (
        (
            price_quality IS NOT NULL
            AND price_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE')
            AND (
                (
                    price_quality = 'OBSERVED'
                    AND bar_status = 'TARGET_PRESENT'
                    AND raw_open IS NOT NULL AND raw_open > 0
                    AND raw_high IS NOT NULL AND raw_high > 0
                    AND raw_low IS NOT NULL AND raw_low > 0
                    AND raw_close IS NOT NULL AND raw_close > 0
                    AND raw_high >= GREATEST(raw_open, raw_low, raw_close)
                    AND raw_low <= LEAST(raw_open, raw_high, raw_close)
                    AND price_reason IS NULL
                )
                OR
                (
                    price_quality IN ('UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE')
                    AND raw_open IS NULL AND raw_high IS NULL
                    AND raw_low IS NULL AND raw_close IS NULL
                    AND price_reason IS NOT NULL AND btrim(price_reason) <> ''
                )
            )
        )
    ) IS NOT TRUE;

    SELECT count(*) INTO invalid_qfq_ohlc
    FROM stock.source_raw_daily
    WHERE (
        (
            (qfq_price_quality IS NULL AND qfq_open IS NULL AND qfq_high IS NULL
             AND qfq_low IS NULL AND qfq_close IS NULL AND qfq_price_reason IS NULL)
            OR
            (
                qfq_price_quality IS NOT NULL
                AND qfq_price_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE')
                AND (
                    (
                        qfq_price_quality = 'OBSERVED'
                        AND qfq_bar_status = 'TARGET_PRESENT'
                        AND qfq_open IS NOT NULL AND qfq_open > 0
                        AND qfq_high IS NOT NULL AND qfq_high > 0
                        AND qfq_low IS NOT NULL AND qfq_low > 0
                        AND qfq_close IS NOT NULL AND qfq_close > 0
                        AND qfq_high >= GREATEST(qfq_open, qfq_low, qfq_close)
                        AND qfq_low <= LEAST(qfq_open, qfq_high, qfq_close)
                        AND qfq_price_reason IS NULL
                    )
                    OR
                    (
                        qfq_price_quality IN ('UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE')
                        AND qfq_open IS NULL AND qfq_high IS NULL
                        AND qfq_low IS NULL AND qfq_close IS NULL
                        AND qfq_price_reason IS NOT NULL
                        AND btrim(qfq_price_reason) <> ''
                    )
                )
            )
        )
    ) IS NOT TRUE;

    SELECT count(*) INTO invalid_raw_status
    FROM stock.source_raw_daily
    WHERE (
        (raw_tradestatus_quality IS NULL AND raw_tradestatus_value IS NULL
         AND raw_tradestatus_reason IS NULL)
        OR
        (raw_tradestatus_quality IS NOT NULL AND raw_tradestatus_quality = 'OBSERVED'
         AND raw_tradestatus_value IS NOT NULL
         AND raw_tradestatus_value IN ('0', '1') AND raw_tradestatus_reason IS NULL)
        OR
        (raw_tradestatus_quality IS NOT NULL
         AND raw_tradestatus_quality IN ('UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE')
         AND raw_tradestatus_value IS NULL AND raw_tradestatus_reason IS NOT NULL
         AND btrim(raw_tradestatus_reason) <> '')
    ) IS NOT TRUE;

    SELECT count(*) INTO invalid_raw_is_st
    FROM stock.source_raw_daily
    WHERE (
        (raw_is_st_quality IS NULL AND raw_is_st_value IS NULL
         AND raw_is_st_reason IS NULL)
        OR
        (raw_is_st_quality IS NOT NULL AND raw_is_st_quality = 'OBSERVED'
         AND raw_is_st_value IS NOT NULL
         AND raw_is_st_value IN ('0', '1') AND raw_is_st_reason IS NULL)
        OR
        (raw_is_st_quality IS NOT NULL
         AND raw_is_st_quality IN ('UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE')
         AND raw_is_st_value IS NULL AND raw_is_st_reason IS NOT NULL
         AND btrim(raw_is_st_reason) <> '')
    ) IS NOT TRUE;

    SELECT count(*) INTO invalid_qfq_status
    FROM stock.source_raw_daily
    WHERE (
        (qfq_tradestatus_quality IS NULL AND qfq_tradestatus_value IS NULL
         AND qfq_tradestatus_reason IS NULL)
        OR
        (qfq_tradestatus_quality IS NOT NULL AND qfq_tradestatus_quality = 'OBSERVED'
         AND qfq_tradestatus_value IS NOT NULL
         AND qfq_tradestatus_value IN ('0', '1') AND qfq_tradestatus_reason IS NULL)
        OR
        (qfq_tradestatus_quality IS NOT NULL
         AND qfq_tradestatus_quality IN ('UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE')
         AND qfq_tradestatus_value IS NULL AND qfq_tradestatus_reason IS NOT NULL
         AND btrim(qfq_tradestatus_reason) <> '')
    ) IS NOT TRUE;

    SELECT count(*) INTO invalid_qfq_is_st
    FROM stock.source_raw_daily
    WHERE (
        (qfq_is_st_quality IS NULL AND qfq_is_st_value IS NULL
         AND qfq_is_st_reason IS NULL)
        OR
        (qfq_is_st_quality IS NOT NULL AND qfq_is_st_quality = 'OBSERVED'
         AND qfq_is_st_value IS NOT NULL
         AND qfq_is_st_value IN ('0', '1') AND qfq_is_st_reason IS NULL)
        OR
        (qfq_is_st_quality IS NOT NULL
         AND qfq_is_st_quality IN ('UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE')
         AND qfq_is_st_value IS NULL AND qfq_is_st_reason IS NOT NULL
         AND btrim(qfq_is_st_reason) <> '')
    ) IS NOT TRUE;

    IF invalid_raw_ohlc + invalid_qfq_ohlc + invalid_raw_status
       + invalid_raw_is_st + invalid_qfq_status + invalid_qfq_is_st > 0 THEN
        RAISE EXCEPTION 'R07_R08_KEY_QUALITY_EXISTING_ROWS_REJECTED'
            USING DETAIL = format(
                'RAW_OHLC=%s, QFQ_OHLC=%s, RAW_TRADESTATUS=%s, RAW_IS_ST=%s, QFQ_TRADESTATUS=%s, QFQ_IS_ST=%s；保留原行，按可追溯来源证据有界恢复后再申请迁移。',
                invalid_raw_ohlc, invalid_qfq_ohlc, invalid_raw_status,
                invalid_raw_is_st, invalid_qfq_status, invalid_qfq_is_st
            );
    END IF;
END $$;

ALTER TABLE stock.source_sector_cross_period_pulse_facts
    DROP CONSTRAINT source_sector_cross_period_pulse_business_uq,
    ADD CONSTRAINT source_sector_cross_period_pulse_business_uq
        UNIQUE (target_session, source_capture_id, catalog_id, profile_revision, algorithm_contract);

ALTER TABLE stock.source_rule_evaluations
    DROP CONSTRAINT source_rule_evaluations_business_uq,
    ADD CONSTRAINT source_rule_evaluations_business_uq
        UNIQUE NULLS NOT DISTINCT (
            target_session, source_capture_id, source_symbol, rule_id, rule_version,
            profile_revision, history_session_count, d1_history_session_count,
            event_window_start, calendar_revision
        );

ALTER TABLE stock.source_forward_path_facts
    DROP CONSTRAINT source_forward_business_uq,
    ADD CONSTRAINT source_forward_business_uq
        UNIQUE (target_session, source_capture_id, source_symbol, horizon,
                path_rule_version, calendar_revision);

ALTER TABLE stock.source_raw_daily
    ADD COLUMN raw_open_quality text,
    ADD COLUMN raw_open_reason text,
    ADD COLUMN raw_high_quality text,
    ADD COLUMN raw_high_reason text,
    ADD COLUMN raw_low_quality text,
    ADD COLUMN raw_low_reason text,
    ADD COLUMN raw_close_quality text,
    ADD COLUMN raw_close_reason text,
    ADD COLUMN qfq_open_quality text,
    ADD COLUMN qfq_open_reason text,
    ADD COLUMN qfq_high_quality text,
    ADD COLUMN qfq_high_reason text,
    ADD COLUMN qfq_low_quality text,
    ADD COLUMN qfq_low_reason text,
    ADD COLUMN qfq_close_quality text,
    ADD COLUMN qfq_close_reason text;

-- 旧版仅在完整OHLC时保存字段值。完整组可证明四项均OBSERVED；
-- 旧版非完整组无法定位曾经可观测的字段，故逐字段保守标UNKNOWN。
UPDATE stock.source_raw_daily
SET raw_open_quality = CASE WHEN price_quality = 'OBSERVED' THEN 'OBSERVED'
                            WHEN price_quality = 'NOT_APPLICABLE' THEN 'NOT_APPLICABLE'
                            ELSE 'UNKNOWN' END,
    raw_open_reason = CASE WHEN price_quality = 'OBSERVED' THEN NULL
                           WHEN price_quality = 'NOT_APPLICABLE' THEN 'LEGACY_GROUP_NOT_APPLICABLE'
                           ELSE 'LEGACY_FIELD_QUALITY_NOT_RECORDED' END,
    raw_high_quality = CASE WHEN price_quality = 'OBSERVED' THEN 'OBSERVED'
                            WHEN price_quality = 'NOT_APPLICABLE' THEN 'NOT_APPLICABLE'
                            ELSE 'UNKNOWN' END,
    raw_high_reason = CASE WHEN price_quality = 'OBSERVED' THEN NULL
                           WHEN price_quality = 'NOT_APPLICABLE' THEN 'LEGACY_GROUP_NOT_APPLICABLE'
                           ELSE 'LEGACY_FIELD_QUALITY_NOT_RECORDED' END,
    raw_low_quality = CASE WHEN price_quality = 'OBSERVED' THEN 'OBSERVED'
                           WHEN price_quality = 'NOT_APPLICABLE' THEN 'NOT_APPLICABLE'
                           ELSE 'UNKNOWN' END,
    raw_low_reason = CASE WHEN price_quality = 'OBSERVED' THEN NULL
                          WHEN price_quality = 'NOT_APPLICABLE' THEN 'LEGACY_GROUP_NOT_APPLICABLE'
                          ELSE 'LEGACY_FIELD_QUALITY_NOT_RECORDED' END,
    raw_close_quality = CASE WHEN price_quality = 'OBSERVED' THEN 'OBSERVED'
                             WHEN price_quality = 'NOT_APPLICABLE' THEN 'NOT_APPLICABLE'
                             ELSE 'UNKNOWN' END,
    raw_close_reason = CASE WHEN price_quality = 'OBSERVED' THEN NULL
                            WHEN price_quality = 'NOT_APPLICABLE' THEN 'LEGACY_GROUP_NOT_APPLICABLE'
                            ELSE 'LEGACY_FIELD_QUALITY_NOT_RECORDED' END,
    qfq_price_quality = COALESCE(qfq_price_quality, 'UNKNOWN'),
    qfq_price_reason = CASE WHEN qfq_price_quality IS NULL
                            THEN 'LEGACY_QFQ_GROUP_QUALITY_NOT_RECORDED'
                            ELSE qfq_price_reason END,
    qfq_open_quality = CASE WHEN qfq_price_quality = 'OBSERVED' THEN 'OBSERVED'
                            WHEN qfq_price_quality = 'NOT_APPLICABLE' THEN 'NOT_APPLICABLE'
                            ELSE 'UNKNOWN' END,
    qfq_open_reason = CASE WHEN qfq_price_quality = 'OBSERVED' THEN NULL
                           WHEN qfq_price_quality = 'NOT_APPLICABLE' THEN 'LEGACY_GROUP_NOT_APPLICABLE'
                           ELSE 'LEGACY_FIELD_QUALITY_NOT_RECORDED' END,
    qfq_high_quality = CASE WHEN qfq_price_quality = 'OBSERVED' THEN 'OBSERVED'
                            WHEN qfq_price_quality = 'NOT_APPLICABLE' THEN 'NOT_APPLICABLE'
                            ELSE 'UNKNOWN' END,
    qfq_high_reason = CASE WHEN qfq_price_quality = 'OBSERVED' THEN NULL
                           WHEN qfq_price_quality = 'NOT_APPLICABLE' THEN 'LEGACY_GROUP_NOT_APPLICABLE'
                           ELSE 'LEGACY_FIELD_QUALITY_NOT_RECORDED' END,
    qfq_low_quality = CASE WHEN qfq_price_quality = 'OBSERVED' THEN 'OBSERVED'
                           WHEN qfq_price_quality = 'NOT_APPLICABLE' THEN 'NOT_APPLICABLE'
                           ELSE 'UNKNOWN' END,
    qfq_low_reason = CASE WHEN qfq_price_quality = 'OBSERVED' THEN NULL
                          WHEN qfq_price_quality = 'NOT_APPLICABLE' THEN 'LEGACY_GROUP_NOT_APPLICABLE'
                          ELSE 'LEGACY_FIELD_QUALITY_NOT_RECORDED' END,
    qfq_close_quality = CASE WHEN qfq_price_quality = 'OBSERVED' THEN 'OBSERVED'
                             WHEN qfq_price_quality = 'NOT_APPLICABLE' THEN 'NOT_APPLICABLE'
                             ELSE 'UNKNOWN' END,
    qfq_close_reason = CASE WHEN qfq_price_quality = 'OBSERVED' THEN NULL
                            WHEN qfq_price_quality = 'NOT_APPLICABLE' THEN 'LEGACY_GROUP_NOT_APPLICABLE'
                            ELSE 'LEGACY_FIELD_QUALITY_NOT_RECORDED' END;

ALTER TABLE stock.source_raw_daily
    ALTER COLUMN qfq_price_quality SET NOT NULL,
    ALTER COLUMN raw_open_quality SET NOT NULL,
    ALTER COLUMN raw_high_quality SET NOT NULL,
    ALTER COLUMN raw_low_quality SET NOT NULL,
    ALTER COLUMN raw_close_quality SET NOT NULL,
    ALTER COLUMN qfq_open_quality SET NOT NULL,
    ALTER COLUMN qfq_high_quality SET NOT NULL,
    ALTER COLUMN qfq_low_quality SET NOT NULL,
    ALTER COLUMN qfq_close_quality SET NOT NULL;

DO $$
DECLARE
    item record;
    dropped_count integer := 0;
BEGIN
    FOR item IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'stock.source_raw_daily'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) ILIKE '%raw_open%'
          AND pg_get_constraintdef(oid) ILIKE '%raw_high%'
          AND pg_get_constraintdef(oid) ILIKE '%raw_close%'
          AND pg_get_constraintdef(oid) NOT ILIKE '%qfq_open%'
    LOOP
        EXECUTE format(
            'ALTER TABLE stock.source_raw_daily DROP CONSTRAINT %I', item.conname
        );
        dropped_count := dropped_count + 1;
    END LOOP;
    IF dropped_count <> 1 THEN
        RAISE EXCEPTION 'R07_R08_KEY_QUALITY_RAW_GROUP_CONSTRAINT_COUNT_UNEXPECTED: %',
            dropped_count;
    END IF;
END $$;

ALTER TABLE stock.source_raw_daily
    DROP CONSTRAINT source_raw_daily_qfq_quality_ck,
    DROP CONSTRAINT source_raw_daily_raw_tradestatus_quality_ck,
    DROP CONSTRAINT source_raw_daily_raw_is_st_quality_ck,
    ADD CONSTRAINT source_raw_daily_raw_ohlc_field_quality_ck CHECK (
        (
            (
                raw_open_quality IS NULL AND raw_open_reason IS NULL
                AND raw_high_quality IS NULL AND raw_high_reason IS NULL
                AND raw_low_quality IS NULL AND raw_low_reason IS NULL
                AND raw_close_quality IS NULL AND raw_close_reason IS NULL
            )
            OR
            (
                raw_open_quality IS NOT NULL AND raw_open_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE')
                AND ((raw_open_quality = 'OBSERVED' AND raw_open IS NOT NULL AND raw_open > 0 AND raw_open_reason IS NULL)
                     OR (raw_open_quality IN ('UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE') AND raw_open IS NULL AND raw_open_reason IS NOT NULL AND btrim(raw_open_reason) <> ''))
                AND raw_high_quality IS NOT NULL AND raw_high_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE')
                AND ((raw_high_quality = 'OBSERVED' AND raw_high IS NOT NULL AND raw_high > 0 AND raw_high_reason IS NULL)
                     OR (raw_high_quality IN ('UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE') AND raw_high IS NULL AND raw_high_reason IS NOT NULL AND btrim(raw_high_reason) <> ''))
                AND raw_low_quality IS NOT NULL AND raw_low_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE')
                AND ((raw_low_quality = 'OBSERVED' AND raw_low IS NOT NULL AND raw_low > 0 AND raw_low_reason IS NULL)
                     OR (raw_low_quality IN ('UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE') AND raw_low IS NULL AND raw_low_reason IS NOT NULL AND btrim(raw_low_reason) <> ''))
                AND raw_close_quality IS NOT NULL AND raw_close_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE')
                AND ((raw_close_quality = 'OBSERVED' AND raw_close IS NOT NULL AND raw_close > 0 AND raw_close_reason IS NULL)
                     OR (raw_close_quality IN ('UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE') AND raw_close IS NULL AND raw_close_reason IS NOT NULL AND btrim(raw_close_reason) <> ''))
            )
        ) IS TRUE
    ),
    ADD CONSTRAINT source_raw_daily_qfq_ohlc_field_quality_ck CHECK (
        (
            (
                qfq_open_quality IS NULL AND qfq_open_reason IS NULL
                AND qfq_high_quality IS NULL AND qfq_high_reason IS NULL
                AND qfq_low_quality IS NULL AND qfq_low_reason IS NULL
                AND qfq_close_quality IS NULL AND qfq_close_reason IS NULL
            )
            OR
            (
                qfq_open_quality IS NOT NULL AND qfq_open_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE')
                AND ((qfq_open_quality = 'OBSERVED' AND qfq_open IS NOT NULL AND qfq_open > 0 AND qfq_open_reason IS NULL)
                     OR (qfq_open_quality IN ('UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE') AND qfq_open IS NULL AND qfq_open_reason IS NOT NULL AND btrim(qfq_open_reason) <> ''))
                AND qfq_high_quality IS NOT NULL AND qfq_high_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE')
                AND ((qfq_high_quality = 'OBSERVED' AND qfq_high IS NOT NULL AND qfq_high > 0 AND qfq_high_reason IS NULL)
                     OR (qfq_high_quality IN ('UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE') AND qfq_high IS NULL AND qfq_high_reason IS NOT NULL AND btrim(qfq_high_reason) <> ''))
                AND qfq_low_quality IS NOT NULL AND qfq_low_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE')
                AND ((qfq_low_quality = 'OBSERVED' AND qfq_low IS NOT NULL AND qfq_low > 0 AND qfq_low_reason IS NULL)
                     OR (qfq_low_quality IN ('UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE') AND qfq_low IS NULL AND qfq_low_reason IS NOT NULL AND btrim(qfq_low_reason) <> ''))
                AND qfq_close_quality IS NOT NULL AND qfq_close_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE')
                AND ((qfq_close_quality = 'OBSERVED' AND qfq_close IS NOT NULL AND qfq_close > 0 AND qfq_close_reason IS NULL)
                     OR (qfq_close_quality IN ('UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE') AND qfq_close IS NULL AND qfq_close_reason IS NOT NULL AND btrim(qfq_close_reason) <> ''))
            )
        ) IS TRUE
    ),
    ADD CONSTRAINT source_raw_daily_raw_price_quality_consistency_ck CHECK (
        (
            price_quality IS NOT NULL
            AND (
                (
                    raw_open_quality IS NULL AND raw_high_quality IS NULL
                    AND raw_low_quality IS NULL AND raw_close_quality IS NULL
                    AND price_quality = 'OBSERVED'
                    AND bar_status = 'TARGET_PRESENT'
                    AND raw_open IS NOT NULL AND raw_high IS NOT NULL
                    AND raw_low IS NOT NULL AND raw_close IS NOT NULL
                    AND raw_high >= GREATEST(raw_open, raw_low, raw_close)
                    AND raw_low <= LEAST(raw_open, raw_high, raw_close)
                    AND price_reason IS NULL
                )
                OR
                (
                    raw_open_quality IS NULL AND raw_high_quality IS NULL
                    AND raw_low_quality IS NULL AND raw_close_quality IS NULL
                    AND price_quality IN ('UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE')
                    AND raw_open IS NULL AND raw_high IS NULL
                    AND raw_low IS NULL AND raw_close IS NULL
                    AND price_reason IS NOT NULL AND btrim(price_reason) <> ''
                )
                OR
                (
                    raw_open_quality IS NOT NULL AND raw_high_quality IS NOT NULL
                    AND raw_low_quality IS NOT NULL AND raw_close_quality IS NOT NULL
                    AND price_quality = 'OBSERVED'
                    AND raw_open_quality = 'OBSERVED'
                    AND raw_high_quality = 'OBSERVED'
                    AND raw_low_quality = 'OBSERVED'
                    AND raw_close_quality = 'OBSERVED'
                    AND bar_status = 'TARGET_PRESENT'
                    AND raw_open IS NOT NULL AND raw_high IS NOT NULL
                    AND raw_low IS NOT NULL AND raw_close IS NOT NULL
                    AND raw_high >= GREATEST(raw_open, raw_low, raw_close)
                    AND raw_low <= LEAST(raw_open, raw_high, raw_close)
                    AND price_reason IS NULL
                )
                OR
                (
                    raw_open_quality IS NOT NULL AND raw_high_quality IS NOT NULL
                    AND raw_low_quality IS NOT NULL AND raw_close_quality IS NOT NULL
                    AND price_quality = 'BAD_VALUE'
                    AND (
                        raw_open_quality = 'BAD_VALUE' OR raw_high_quality = 'BAD_VALUE'
                        OR raw_low_quality = 'BAD_VALUE' OR raw_close_quality = 'BAD_VALUE'
                        OR (
                            raw_open_quality = 'UNKNOWN' AND raw_high_quality = 'UNKNOWN'
                            AND raw_low_quality = 'UNKNOWN' AND raw_close_quality = 'UNKNOWN'
                            AND raw_open_reason = 'LEGACY_FIELD_QUALITY_NOT_RECORDED'
                            AND raw_high_reason = 'LEGACY_FIELD_QUALITY_NOT_RECORDED'
                            AND raw_low_reason = 'LEGACY_FIELD_QUALITY_NOT_RECORDED'
                            AND raw_close_reason = 'LEGACY_FIELD_QUALITY_NOT_RECORDED'
                        )
                    )
                    AND price_reason IS NOT NULL AND btrim(price_reason) <> ''
                )
                OR
                (
                    raw_open_quality IS NOT NULL AND raw_high_quality IS NOT NULL
                    AND raw_low_quality IS NOT NULL AND raw_close_quality IS NOT NULL
                    AND price_quality = 'NOT_APPLICABLE'
                    AND raw_open_quality = 'NOT_APPLICABLE'
                    AND raw_high_quality = 'NOT_APPLICABLE'
                    AND raw_low_quality = 'NOT_APPLICABLE'
                    AND raw_close_quality = 'NOT_APPLICABLE'
                    AND price_reason IS NOT NULL AND btrim(price_reason) <> ''
                )
                OR
                (
                    raw_open_quality IS NOT NULL AND raw_high_quality IS NOT NULL
                    AND raw_low_quality IS NOT NULL AND raw_close_quality IS NOT NULL
                    AND price_quality = 'UNKNOWN'
                    AND raw_open_quality <> 'BAD_VALUE'
                    AND raw_high_quality <> 'BAD_VALUE'
                    AND raw_low_quality <> 'BAD_VALUE'
                    AND raw_close_quality <> 'BAD_VALUE'
                    AND NOT (
                        raw_open_quality = 'NOT_APPLICABLE'
                        AND raw_high_quality = 'NOT_APPLICABLE'
                        AND raw_low_quality = 'NOT_APPLICABLE'
                        AND raw_close_quality = 'NOT_APPLICABLE'
                    )
                    AND NOT (
                        raw_open_quality = 'OBSERVED' AND raw_high_quality = 'OBSERVED'
                        AND raw_low_quality = 'OBSERVED' AND raw_close_quality = 'OBSERVED'
                    )
                    AND price_reason IS NOT NULL AND btrim(price_reason) <> ''
                )
            )
            AND (raw_high IS NULL OR raw_open IS NULL OR raw_high >= raw_open)
            AND (raw_high IS NULL OR raw_low IS NULL OR raw_high >= raw_low)
            AND (raw_high IS NULL OR raw_close IS NULL OR raw_high >= raw_close)
            AND (raw_low IS NULL OR raw_open IS NULL OR raw_low <= raw_open)
            AND (raw_low IS NULL OR raw_close IS NULL OR raw_low <= raw_close)
        ) IS TRUE
    ),
    ADD CONSTRAINT source_raw_daily_qfq_price_quality_consistency_ck CHECK (
        (
            (qfq_price_quality IS NULL AND qfq_open IS NULL AND qfq_high IS NULL
             AND qfq_low IS NULL AND qfq_close IS NULL AND qfq_price_reason IS NULL
             AND qfq_open_quality IS NULL AND qfq_high_quality IS NULL
             AND qfq_low_quality IS NULL AND qfq_close_quality IS NULL)
            OR
            (
                qfq_price_quality IS NOT NULL
                AND (
                    (
                        qfq_open_quality IS NULL AND qfq_high_quality IS NULL
                        AND qfq_low_quality IS NULL AND qfq_close_quality IS NULL
                        AND qfq_price_quality = 'OBSERVED'
                        AND qfq_bar_status = 'TARGET_PRESENT'
                        AND qfq_open IS NOT NULL AND qfq_high IS NOT NULL
                        AND qfq_low IS NOT NULL AND qfq_close IS NOT NULL
                        AND qfq_high >= GREATEST(qfq_open, qfq_low, qfq_close)
                        AND qfq_low <= LEAST(qfq_open, qfq_high, qfq_close)
                        AND qfq_price_reason IS NULL
                    )
                    OR
                    (
                        qfq_open_quality IS NULL AND qfq_high_quality IS NULL
                        AND qfq_low_quality IS NULL AND qfq_close_quality IS NULL
                        AND qfq_price_quality IN ('UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE')
                        AND qfq_open IS NULL AND qfq_high IS NULL
                        AND qfq_low IS NULL AND qfq_close IS NULL
                        AND qfq_price_reason IS NOT NULL
                        AND btrim(qfq_price_reason) <> ''
                    )
                    OR
                    (
                        qfq_open_quality IS NOT NULL AND qfq_high_quality IS NOT NULL
                        AND qfq_low_quality IS NOT NULL AND qfq_close_quality IS NOT NULL
                        AND qfq_price_quality = 'OBSERVED'
                        AND qfq_open_quality = 'OBSERVED'
                        AND qfq_high_quality = 'OBSERVED'
                        AND qfq_low_quality = 'OBSERVED'
                        AND qfq_close_quality = 'OBSERVED'
                        AND qfq_bar_status = 'TARGET_PRESENT'
                        AND qfq_open IS NOT NULL AND qfq_high IS NOT NULL
                        AND qfq_low IS NOT NULL AND qfq_close IS NOT NULL
                        AND qfq_high >= GREATEST(qfq_open, qfq_low, qfq_close)
                        AND qfq_low <= LEAST(qfq_open, qfq_high, qfq_close)
                        AND qfq_price_reason IS NULL
                    )
                    OR
                    (
                        qfq_open_quality IS NOT NULL AND qfq_high_quality IS NOT NULL
                        AND qfq_low_quality IS NOT NULL AND qfq_close_quality IS NOT NULL
                        AND qfq_price_quality = 'BAD_VALUE'
                        AND (
                            qfq_open_quality = 'BAD_VALUE' OR qfq_high_quality = 'BAD_VALUE'
                            OR qfq_low_quality = 'BAD_VALUE' OR qfq_close_quality = 'BAD_VALUE'
                            OR (
                                qfq_open_quality = 'UNKNOWN' AND qfq_high_quality = 'UNKNOWN'
                                AND qfq_low_quality = 'UNKNOWN' AND qfq_close_quality = 'UNKNOWN'
                                AND qfq_open_reason = 'LEGACY_FIELD_QUALITY_NOT_RECORDED'
                                AND qfq_high_reason = 'LEGACY_FIELD_QUALITY_NOT_RECORDED'
                                AND qfq_low_reason = 'LEGACY_FIELD_QUALITY_NOT_RECORDED'
                                AND qfq_close_reason = 'LEGACY_FIELD_QUALITY_NOT_RECORDED'
                            )
                        )
                        AND qfq_price_reason IS NOT NULL
                        AND btrim(qfq_price_reason) <> ''
                    )
                    OR
                    (
                        qfq_open_quality IS NOT NULL AND qfq_high_quality IS NOT NULL
                        AND qfq_low_quality IS NOT NULL AND qfq_close_quality IS NOT NULL
                        AND qfq_price_quality = 'NOT_APPLICABLE'
                        AND qfq_open_quality = 'NOT_APPLICABLE'
                        AND qfq_high_quality = 'NOT_APPLICABLE'
                        AND qfq_low_quality = 'NOT_APPLICABLE'
                        AND qfq_close_quality = 'NOT_APPLICABLE'
                        AND qfq_price_reason IS NOT NULL
                        AND btrim(qfq_price_reason) <> ''
                    )
                    OR
                    (
                        qfq_open_quality IS NOT NULL AND qfq_high_quality IS NOT NULL
                        AND qfq_low_quality IS NOT NULL AND qfq_close_quality IS NOT NULL
                        AND qfq_price_quality = 'UNKNOWN'
                        AND qfq_open_quality <> 'BAD_VALUE'
                        AND qfq_high_quality <> 'BAD_VALUE'
                        AND qfq_low_quality <> 'BAD_VALUE'
                        AND qfq_close_quality <> 'BAD_VALUE'
                        AND NOT (
                            qfq_open_quality = 'NOT_APPLICABLE'
                            AND qfq_high_quality = 'NOT_APPLICABLE'
                            AND qfq_low_quality = 'NOT_APPLICABLE'
                            AND qfq_close_quality = 'NOT_APPLICABLE'
                        )
                        AND NOT (
                            qfq_open_quality = 'OBSERVED' AND qfq_high_quality = 'OBSERVED'
                            AND qfq_low_quality = 'OBSERVED' AND qfq_close_quality = 'OBSERVED'
                        )
                        AND qfq_price_reason IS NOT NULL
                        AND btrim(qfq_price_reason) <> ''
                    )
                )
            )
            AND (qfq_high IS NULL OR qfq_open IS NULL OR qfq_high >= qfq_open)
            AND (qfq_high IS NULL OR qfq_low IS NULL OR qfq_high >= qfq_low)
            AND (qfq_high IS NULL OR qfq_close IS NULL OR qfq_high >= qfq_close)
            AND (qfq_low IS NULL OR qfq_open IS NULL OR qfq_low <= qfq_open)
            AND (qfq_low IS NULL OR qfq_close IS NULL OR qfq_low <= qfq_close)
        ) IS TRUE
    ),
    ADD CONSTRAINT source_raw_daily_raw_tradestatus_quality_ck CHECK (
        (
            (raw_tradestatus_quality IS NULL AND raw_tradestatus_value IS NULL AND raw_tradestatus_reason IS NULL)
            OR (raw_tradestatus_quality IS NOT NULL AND raw_tradestatus_quality = 'OBSERVED'
                AND raw_tradestatus_value IS NOT NULL AND raw_tradestatus_value IN ('0', '1')
                AND raw_tradestatus_reason IS NULL)
            OR (raw_tradestatus_quality IS NOT NULL
                AND raw_tradestatus_quality IN ('UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE')
                AND raw_tradestatus_value IS NULL AND raw_tradestatus_reason IS NOT NULL
                AND btrim(raw_tradestatus_reason) <> '')
        ) IS TRUE
    ),
    ADD CONSTRAINT source_raw_daily_raw_is_st_quality_ck CHECK (
        (
            (raw_is_st_quality IS NULL AND raw_is_st_value IS NULL AND raw_is_st_reason IS NULL)
            OR (raw_is_st_quality IS NOT NULL AND raw_is_st_quality = 'OBSERVED'
                AND raw_is_st_value IS NOT NULL AND raw_is_st_value IN ('0', '1')
                AND raw_is_st_reason IS NULL)
            OR (raw_is_st_quality IS NOT NULL
                AND raw_is_st_quality IN ('UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE')
                AND raw_is_st_value IS NULL AND raw_is_st_reason IS NOT NULL
                AND btrim(raw_is_st_reason) <> '')
        ) IS TRUE
    ),
    ADD CONSTRAINT source_raw_daily_qfq_tradestatus_quality_ck CHECK (
        (
            (qfq_tradestatus_quality IS NULL AND qfq_tradestatus_value IS NULL AND qfq_tradestatus_reason IS NULL)
            OR (qfq_tradestatus_quality IS NOT NULL AND qfq_tradestatus_quality = 'OBSERVED'
                AND qfq_tradestatus_value IS NOT NULL AND qfq_tradestatus_value IN ('0', '1')
                AND qfq_tradestatus_reason IS NULL)
            OR (qfq_tradestatus_quality IS NOT NULL
                AND qfq_tradestatus_quality IN ('UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE')
                AND qfq_tradestatus_value IS NULL AND qfq_tradestatus_reason IS NOT NULL
                AND btrim(qfq_tradestatus_reason) <> '')
        ) IS TRUE
    ),
    ADD CONSTRAINT source_raw_daily_qfq_is_st_quality_ck CHECK (
        (
            (qfq_is_st_quality IS NULL AND qfq_is_st_value IS NULL AND qfq_is_st_reason IS NULL)
            OR (qfq_is_st_quality IS NOT NULL AND qfq_is_st_quality = 'OBSERVED'
                AND qfq_is_st_value IS NOT NULL AND qfq_is_st_value IN ('0', '1')
                AND qfq_is_st_reason IS NULL)
            OR (qfq_is_st_quality IS NOT NULL
                AND qfq_is_st_quality IN ('UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE')
                AND qfq_is_st_value IS NULL AND qfq_is_st_reason IS NOT NULL
                AND btrim(qfq_is_st_reason) <> '')
        ) IS TRUE
    );

COMMENT ON COLUMN stock.source_raw_daily.raw_open IS
    '未复权开盘价，单位元/股；现役BaoStock来源使用adjustflag=3，按raw_open_quality逐字段判断，组质量仍由price_quality表达完整OHLC可用性。';
COMMENT ON COLUMN stock.source_raw_daily.raw_high IS
    '未复权最高价，单位元/股；现役BaoStock来源使用adjustflag=3，按raw_high_quality逐字段判断，组质量仍由price_quality表达完整OHLC可用性。';
COMMENT ON COLUMN stock.source_raw_daily.raw_low IS
    '未复权最低价，单位元/股；现役BaoStock来源使用adjustflag=3，按raw_low_quality逐字段判断，组质量仍由price_quality表达完整OHLC可用性。';
COMMENT ON COLUMN stock.source_raw_daily.raw_close IS
    '未复权收盘价，单位元/股；现役BaoStock来源使用adjustflag=3，按raw_close_quality逐字段判断，组质量仍由price_quality表达完整OHLC可用性。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_open IS
    'BaoStock adjustflag=2前复权开盘价，单位元/股；按qfq_open_quality逐字段判断，组质量仍由qfq_price_quality表达完整OHLC可用性。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_high IS
    'BaoStock adjustflag=2前复权最高价，单位元/股；按qfq_high_quality逐字段判断，组质量仍由qfq_price_quality表达完整OHLC可用性。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_low IS
    'BaoStock adjustflag=2前复权最低价，单位元/股；按qfq_low_quality逐字段判断，组质量仍由qfq_price_quality表达完整OHLC可用性。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_close IS
    'BaoStock adjustflag=2前复权收盘价，单位元/股；按qfq_close_quality逐字段判断，组质量仍由qfq_price_quality表达完整OHLC可用性。';

COMMENT ON COLUMN stock.source_raw_daily.raw_open_quality IS
    'RAW开盘价字段质量：OBSERVED、UNKNOWN、NOT_APPLICABLE或BAD_VALUE；旧组无法定位单项状态时为UNKNOWN。';
COMMENT ON COLUMN stock.source_raw_daily.raw_open_reason IS
    'RAW开盘价非OBSERVED原因；LEGACY_FIELD_QUALITY_NOT_RECORDED表示旧版无法定位字段状态，OBSERVED时为空。';
COMMENT ON COLUMN stock.source_raw_daily.raw_high_quality IS
    'RAW最高价字段质量：OBSERVED、UNKNOWN、NOT_APPLICABLE或BAD_VALUE；旧组无法定位单项状态时为UNKNOWN。';
COMMENT ON COLUMN stock.source_raw_daily.raw_high_reason IS
    'RAW最高价非OBSERVED原因；LEGACY_FIELD_QUALITY_NOT_RECORDED表示旧版无法定位字段状态，OBSERVED时为空。';
COMMENT ON COLUMN stock.source_raw_daily.raw_low_quality IS
    'RAW最低价字段质量：OBSERVED、UNKNOWN、NOT_APPLICABLE或BAD_VALUE；旧组无法定位单项状态时为UNKNOWN。';
COMMENT ON COLUMN stock.source_raw_daily.raw_low_reason IS
    'RAW最低价非OBSERVED原因；LEGACY_FIELD_QUALITY_NOT_RECORDED表示旧版无法定位字段状态，OBSERVED时为空。';
COMMENT ON COLUMN stock.source_raw_daily.raw_close_quality IS
    'RAW收盘价字段质量：OBSERVED、UNKNOWN、NOT_APPLICABLE或BAD_VALUE；旧组无法定位单项状态时为UNKNOWN。';
COMMENT ON COLUMN stock.source_raw_daily.raw_close_reason IS
    'RAW收盘价非OBSERVED原因；LEGACY_FIELD_QUALITY_NOT_RECORDED表示旧版无法定位字段状态，OBSERVED时为空。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_open_quality IS
    'QFQ开盘价字段质量：OBSERVED、UNKNOWN、NOT_APPLICABLE或BAD_VALUE；旧组无法定位单项状态时为UNKNOWN。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_open_reason IS
    'QFQ开盘价非OBSERVED原因；LEGACY_FIELD_QUALITY_NOT_RECORDED表示旧版无法定位字段状态，OBSERVED时为空。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_high_quality IS
    'QFQ最高价字段质量：OBSERVED、UNKNOWN、NOT_APPLICABLE或BAD_VALUE；旧组无法定位单项状态时为UNKNOWN。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_high_reason IS
    'QFQ最高价非OBSERVED原因；LEGACY_FIELD_QUALITY_NOT_RECORDED表示旧版无法定位字段状态，OBSERVED时为空。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_low_quality IS
    'QFQ最低价字段质量：OBSERVED、UNKNOWN、NOT_APPLICABLE或BAD_VALUE；旧组无法定位单项状态时为UNKNOWN。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_low_reason IS
    'QFQ最低价非OBSERVED原因；LEGACY_FIELD_QUALITY_NOT_RECORDED表示旧版无法定位字段状态，OBSERVED时为空。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_close_quality IS
    'QFQ收盘价字段质量：OBSERVED、UNKNOWN、NOT_APPLICABLE或BAD_VALUE；旧组无法定位单项状态时为UNKNOWN。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_close_reason IS
    'QFQ收盘价非OBSERVED原因；LEGACY_FIELD_QUALITY_NOT_RECORDED表示旧版无法定位字段状态，OBSERVED时为空。';
COMMENT ON COLUMN stock.source_raw_daily.price_quality IS
    'RAW完整OHLC组质量；只有四个逐字段均OBSERVED且区间关系有效时才为OBSERVED，否则保留UNKNOWN、NOT_APPLICABLE或BAD_VALUE。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_price_quality IS
    'QFQ完整OHLC组质量；只有四个逐字段均OBSERVED且区间关系有效时才为OBSERVED，否则保留UNKNOWN、NOT_APPLICABLE或BAD_VALUE。';
