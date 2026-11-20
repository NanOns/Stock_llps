-- SOURCE-SWITCH-01：保存BaoStock RAW与QFQ逐响应行的全部13个字段。
DO $$
BEGIN
    IF current_database() <> 'stock_a'
       OR current_schema() <> 'stock'
       OR session_user <> 'stock_a'
       OR current_user <> 'stock_a' THEN
        RAISE EXCEPTION 'SOURCE_SWITCH_0058_TARGET_IDENTITY_REJECTED';
    END IF;
    IF (SELECT count(*) FROM stock.migration_ledger
        WHERE lineage = 'STOCK_A_INITIAL_R1'
          AND migration_id BETWEEN 1 AND 57) <> 57
       OR (SELECT count(DISTINCT migration_id) FROM stock.migration_ledger
           WHERE migration_id BETWEEN 1 AND 57) <> 57 THEN
        RAISE EXCEPTION 'SOURCE_SWITCH_0058_MIGRATION_PREFIX_REJECTED';
    END IF;
END;
$$;

ALTER TABLE stock.source_raw_daily
    ADD COLUMN raw_response_session_date date,
    ADD COLUMN raw_response_source_code text,
    ADD COLUMN raw_adjustflag smallint,
    ADD COLUMN raw_response_fields text[],
    ADD COLUMN qfq_response_session_date date,
    ADD COLUMN qfq_response_source_code text,
    ADD COLUMN qfq_adjustflag smallint,
    ADD COLUMN qfq_response_fields text[],
    ADD COLUMN qfq_bar_status text,
    ADD COLUMN qfq_bar_reason text,
    ADD COLUMN qfq_preclose stock.finite_numeric,
    ADD COLUMN qfq_preclose_quality text,
    ADD COLUMN qfq_preclose_reason text,
    ADD COLUMN qfq_volume stock.finite_numeric,
    ADD COLUMN qfq_volume_quality text,
    ADD COLUMN qfq_volume_reason text,
    ADD COLUMN qfq_volume_unit_status text,
    ADD COLUMN qfq_amount stock.finite_numeric,
    ADD COLUMN qfq_amount_quality text,
    ADD COLUMN qfq_amount_reason text,
    ADD COLUMN qfq_amount_unit_status text,
    ADD COLUMN qfq_turn_value stock.finite_numeric,
    ADD COLUMN qfq_turn_quality text,
    ADD COLUMN qfq_turn_reason text,
    ADD COLUMN qfq_tradestatus_value text,
    ADD COLUMN qfq_tradestatus_quality text,
    ADD COLUMN qfq_tradestatus_reason text,
    ADD COLUMN qfq_is_st_value text,
    ADD COLUMN qfq_is_st_quality text,
    ADD COLUMN qfq_is_st_reason text,
    ADD CONSTRAINT source_raw_daily_response_identity_ck CHECK (
        (raw_response_session_date IS NULL OR raw_response_session_date = session_date)
        AND (raw_response_source_code IS NULL OR raw_response_source_code = source_code)
        AND (raw_adjustflag IS NULL OR raw_adjustflag = 3)
        AND (qfq_response_session_date IS NULL OR qfq_response_session_date = session_date)
        AND (qfq_response_source_code IS NULL OR qfq_response_source_code = source_code)
        AND (qfq_adjustflag IS NULL OR qfq_adjustflag = 2)
    ),
    ADD CONSTRAINT source_raw_daily_qfq_field_quality_ck CHECK (
        (qfq_bar_status IS NULL OR qfq_bar_status IN ('TARGET_PRESENT', 'TARGET_MISSING', 'READ_FAILED'))
        AND (qfq_preclose_quality IS NULL OR qfq_preclose_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE'))
        AND (qfq_volume_quality IS NULL OR qfq_volume_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE'))
        AND (qfq_amount_quality IS NULL OR qfq_amount_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE'))
        AND (qfq_turn_quality IS NULL OR qfq_turn_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE'))
        AND (qfq_tradestatus_quality IS NULL OR qfq_tradestatus_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE'))
        AND (qfq_is_st_quality IS NULL OR qfq_is_st_quality IN ('OBSERVED', 'UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE'))
        AND (qfq_volume_unit_status IS NULL OR qfq_volume_unit_status = 'SHARES_VERIFIED')
        AND (qfq_amount_unit_status IS NULL OR qfq_amount_unit_status = 'CNY_VERIFIED')
    );

COMMENT ON COLUMN stock.source_raw_daily.raw_response_session_date IS
    'BaoStock adjustflag=3响应行返回的date字段；与共享session_date一致时记录实际未复权响应行。';
COMMENT ON COLUMN stock.source_raw_daily.raw_response_source_code IS
    'BaoStock adjustflag=3响应行返回的code字段原值；应与共享source_code一致。';
COMMENT ON COLUMN stock.source_raw_daily.raw_adjustflag IS
    'BaoStock未复权响应行返回的adjustflag，固定为3；无未复权Bar时为空。';
COMMENT ON COLUMN stock.source_raw_daily.raw_response_fields IS
    'BaoStock adjustflag=3该响应实际返回的字段名集合；具体值分别保存在RAW typed列。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_response_session_date IS
    'BaoStock adjustflag=2响应行返回的date字段；与共享session_date一致时记录实际前复权响应行。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_response_source_code IS
    'BaoStock adjustflag=2响应行返回的code字段原值；应与共享source_code一致。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_adjustflag IS
    'BaoStock前复权响应行返回的adjustflag，固定为2；无前复权Bar时为空。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_response_fields IS
    'BaoStock adjustflag=2该响应实际返回的字段名集合；具体值分别保存在QFQ typed列。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_bar_status IS
    'BaoStock前复权坐标目标Bar状态：TARGET_PRESENT为收到该行，TARGET_MISSING为请求完成但无该日行，READ_FAILED为请求未完成。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_bar_reason IS
    'BaoStock前复权坐标目标Bar状态原因；无原因时为空。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_preclose IS
    'BaoStock前复权响应行的preclose字段，按价格输入单位倍率精确存为人民币数值。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_preclose_quality IS
    'QFQ前收字段质量：OBSERVED、UNKNOWN、NOT_APPLICABLE或BAD_VALUE。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_preclose_reason IS
    'QFQ前收字段非OBSERVED时的来源或质量原因。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_volume IS
    'BaoStock前复权响应行的volume字段，按单位合同精确规范为股数。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_volume_quality IS
    'QFQ成交量字段质量：OBSERVED、UNKNOWN、NOT_APPLICABLE或BAD_VALUE。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_volume_reason IS
    'QFQ成交量字段非OBSERVED时的来源或质量原因。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_volume_unit_status IS
    'QFQ成交量单位状态；SHARES_VERIFIED表示单位合同核验为股。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_amount IS
    'BaoStock前复权响应行的amount字段，按单位合同精确规范为人民币元。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_amount_quality IS
    'QFQ成交额字段质量：OBSERVED、UNKNOWN、NOT_APPLICABLE或BAD_VALUE。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_amount_reason IS
    'QFQ成交额字段非OBSERVED时的来源或质量原因。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_amount_unit_status IS
    'QFQ成交额单位状态；CNY_VERIFIED表示单位合同核验为人民币元。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_turn_value IS
    'BaoStock前复权响应行的turn字段，按PERCENT单位合同精确规范为比例值。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_turn_quality IS
    'QFQ换手率字段质量：OBSERVED、UNKNOWN、NOT_APPLICABLE或BAD_VALUE。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_turn_reason IS
    'QFQ换手率字段非OBSERVED时的来源或质量原因。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_tradestatus_value IS
    'BaoStock前复权响应行的tradestatus字段原值，枚举文本0或1；不推断缺失状态。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_tradestatus_quality IS
    'QFQ交易状态字段质量：OBSERVED、UNKNOWN、NOT_APPLICABLE或BAD_VALUE。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_tradestatus_reason IS
    'QFQ交易状态字段非OBSERVED时的来源或质量原因。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_is_st_value IS
    'BaoStock前复权响应行的isST字段原值，枚举文本0或1；名称状态仍按捕获时点理解。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_is_st_quality IS
    'QFQ isST字段质量：OBSERVED、UNKNOWN、NOT_APPLICABLE或BAD_VALUE。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_is_st_reason IS
    'QFQ isST字段非OBSERVED时的来源或质量原因。';
