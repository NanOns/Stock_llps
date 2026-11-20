-- SOURCE-SWITCH-01：增加BaoStock双坐标价格、通用来源捕获与可重试请求事实。
-- 本迁移可在现有业务事实尚未清理时前向应用；旧TDX来源包和仿射事实暂留，后续迁移再退役。

ALTER TABLE stock.source_captures
    ADD COLUMN provider text,
    ADD COLUMN capture_uuid uuid,
    ADD COLUMN range_start_session date,
    ADD COLUMN range_end_session date,
    ADD COLUMN price_coordinates text[],
    ADD COLUMN requested_symbol_count integer,
    ADD COLUMN unit_contract_version text;

ALTER TABLE stock.source_captures
    ALTER COLUMN source_package_id DROP NOT NULL;

ALTER TABLE stock.source_captures
    ADD CONSTRAINT source_captures_provider_identity_ck CHECK (
        (
            provider IS NULL
            AND capture_uuid IS NULL
            AND range_start_session IS NULL
            AND range_end_session IS NULL
            AND price_coordinates IS NULL
            AND requested_symbol_count IS NULL
            AND unit_contract_version IS NULL
        )
        OR (
            provider IS NOT NULL
            AND provider IN ('BAOSTOCK', 'TDX')
            AND capture_uuid IS NOT NULL
            AND range_start_session IS NOT NULL
            AND range_end_session IS NOT NULL
            AND range_start_session <= range_end_session
            AND price_coordinates IS NOT NULL
            AND requested_symbol_count IS NOT NULL
            AND requested_symbol_count >= 0
            AND (
                (
                    provider = 'BAOSTOCK'
                    AND source_package_id IS NULL
                    AND (
                        cardinality(price_coordinates) = 0
                        OR (
                            cardinality(price_coordinates) = 2
                            AND price_coordinates @> ARRAY[
                                'RAW_UNADJUSTED', 'FORWARD_ADJUSTED'
                            ]::text[]
                            AND price_coordinates <@ ARRAY[
                                'RAW_UNADJUSTED', 'FORWARD_ADJUSTED'
                            ]::text[]
                        )
                    )
                    AND (
                        cardinality(price_coordinates) = 0
                        AND unit_contract_version IS NULL
                        OR cardinality(price_coordinates) = 2
                        AND requested_symbol_count > 0
                        AND unit_contract_version IS NOT NULL
                    )
                )
                OR (
                    provider = 'TDX'
                    AND cardinality(price_coordinates) = 0
                    AND unit_contract_version IS NULL
                )
            )
        )
    ) NOT VALID;

CREATE UNIQUE INDEX source_captures_capture_uuid_uq
    ON stock.source_captures (capture_uuid)
    WHERE capture_uuid IS NOT NULL;
COMMENT ON INDEX stock.source_captures_capture_uuid_uq IS
    '保证每个新通用来源捕获UUID唯一；迁移前TDX捕获保留空UUID。';

CREATE INDEX source_captures_provider_range_idx
    ON stock.source_captures (provider, range_start_session, range_end_session)
    WHERE provider IS NOT NULL;
COMMENT ON INDEX stock.source_captures_provider_range_idx IS
    '按来源提供方和显式交易会话范围定位新捕获记录。';

COMMENT ON TABLE stock.source_captures IS
    '记录来源读取的数值捕获身份、观察时钟和质量。新捕获使用提供方、UUID、会话范围及坐标集合；旧TDX捕获暂保留原来源包关联。';
COMMENT ON COLUMN stock.source_captures.capture_id IS
    '来源捕获的稳定数据库数字键，继续供现有业务事实和进度表外键引用。';
COMMENT ON COLUMN stock.source_captures.source_package_id IS
    '旧TDX来源包数字键，仅供迁移前的TDX捕获兼容；新BaoStock捕获必须为空。';
COMMENT ON COLUMN stock.source_captures.provider IS
    '来源提供方固定枚举：BAOSTOCK为股票价格，TDX为行业/GN目录及成员关系；旧捕获为空。';
COMMENT ON COLUMN stock.source_captures.capture_uuid IS
    '本次真实来源捕获运行的UUID，由采集应用生成并唯一标识；旧捕获为空，不从包摘要伪造。';
COMMENT ON COLUMN stock.source_captures.range_start_session IS
    '本次捕获覆盖的起始官方交易会话；单日TDX成员快照起止会话相同。';
COMMENT ON COLUMN stock.source_captures.range_end_session IS
    '本次捕获覆盖的结束官方交易会话；不代表截止该日之后的行情可用性。';
COMMENT ON COLUMN stock.source_captures.price_coordinates IS
    '捕获包含的价格坐标集合；BaoStock配对行情捕获同时包含RAW_UNADJUSTED与FORWARD_ADJUSTED，adjustflag分别为3和2；证券基本资料及TDX成员捕获为空数组。';
COMMENT ON COLUMN stock.source_captures.requested_symbol_count IS
    '本次捕获按供应方基本资料及有效上市窗口登记的来源代码数量；不是正式选股或真实上市分母。';
COMMENT ON COLUMN stock.source_captures.unit_contract_version IS
    'BaoStock价格、金额、股数和换手率的单位合同版本；价格捕获必填，基本资料和TDX成员捕获为空。';

ALTER TABLE stock.source_captures
    ADD CONSTRAINT source_captures_unit_contract_fk
        FOREIGN KEY (unit_contract_version)
        REFERENCES stock.source_baostock_unit_contracts (unit_contract_version)
        ON DELETE RESTRICT NOT VALID;

DO $$
DECLARE
    item record;
BEGIN
    FOR item IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'stock.source_captures'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) ILIKE '%normalized_count%'
    LOOP
        EXECUTE format(
            'ALTER TABLE stock.source_captures DROP CONSTRAINT %I', item.conname
        );
    END LOOP;
END $$;

ALTER TABLE stock.source_captures
    ADD CONSTRAINT source_captures_normalized_count_nonnegative_ck
        CHECK (normalized_count >= 0) NOT VALID;

COMMENT ON COLUMN stock.source_captures.normalized_count IS
    '本次捕获已规范化并持久化的记录数；开始时可为0，随批次写入更新，最终数量由typed行独立读回。';

ALTER TABLE stock.source_raw_daily
    ADD COLUMN source_code text,
    ADD COLUMN returned_fields text[],
    ADD COLUMN unit_contract_version text,
    ADD COLUMN raw_preclose stock.finite_numeric,
    ADD COLUMN raw_preclose_quality text,
    ADD COLUMN raw_preclose_reason text,
    ADD COLUMN qfq_open stock.finite_numeric,
    ADD COLUMN qfq_high stock.finite_numeric,
    ADD COLUMN qfq_low stock.finite_numeric,
    ADD COLUMN qfq_close stock.finite_numeric,
    ADD COLUMN qfq_price_quality text,
    ADD COLUMN qfq_price_reason text,
    ADD COLUMN raw_turn_value stock.finite_numeric,
    ADD COLUMN raw_turn_quality text,
    ADD COLUMN raw_turn_reason text,
    ADD COLUMN raw_tradestatus_value text,
    ADD COLUMN raw_tradestatus_quality text,
    ADD COLUMN raw_tradestatus_reason text,
    ADD COLUMN raw_is_st_value text,
    ADD COLUMN raw_is_st_quality text,
    ADD COLUMN raw_is_st_reason text;

ALTER TABLE stock.source_raw_daily
    DROP CONSTRAINT source_raw_daily_source_symbol_check,
    DROP CONSTRAINT source_raw_daily_market_check,
    DROP CONSTRAINT source_raw_daily_board_check,
    ADD CONSTRAINT source_raw_daily_source_symbol_switch_ck CHECK (
        source_symbol ~ '^(SH|SZ|BJ)\.[0-9]{6}$'
    ) NOT VALID,
    ADD CONSTRAINT source_raw_daily_market_switch_ck CHECK (
        market IN ('SH', 'SZ', 'BJ')
    ) NOT VALID,
    ADD CONSTRAINT source_raw_daily_board_switch_ck CHECK (
        board IN ('MAIN_BOARD', 'GEM', 'STAR_BOARD', 'BEIJING_BOARD')
    ) NOT VALID;

ALTER TABLE stock.source_raw_daily
    DROP CONSTRAINT source_raw_daily_amount_unit_status_check,
    DROP CONSTRAINT source_raw_daily_volume_unit_status_check,
    DROP CONSTRAINT source_raw_daily_source_clock_role_check,
    ADD CONSTRAINT source_raw_daily_amount_unit_status_switch_ck CHECK (
        amount_unit_status IN ('UNIT_UNVERIFIED', 'CNY_VERIFIED')
    ) NOT VALID,
    ADD CONSTRAINT source_raw_daily_volume_unit_status_switch_ck CHECK (
        volume_unit_status IN ('UNIT_UNVERIFIED', 'SHARES_VERIFIED')
    ) NOT VALID,
    ADD CONSTRAINT source_raw_daily_source_clock_role_switch_ck CHECK (
        source_clock_role IN (
            'LOCAL_VERIFIED_VIPDATA_TARGET_SCAN_NON_PIT',
            'BAOSTOCK_QUERY_CAPTURED_NON_PIT'
        )
    ) NOT VALID;

COMMENT ON COLUMN stock.source_raw_daily.source_symbol IS
    'BaoStock来源证券代码，规范为SH/SZ/BJ加六位代码；不代表稳定security_id或正式选股资格。';
COMMENT ON COLUMN stock.source_raw_daily.market IS
    '来源市场枚举：SH上海、SZ深圳、BJ北京；板别独立由board字段表达。';
COMMENT ON COLUMN stock.source_raw_daily.board IS
    '价格源代码段对应的市场板别：MAIN_BOARD、GEM、STAR_BOARD或BEIJING_BOARD；正式选股范围另行筛选。';

ALTER TABLE stock.source_raw_daily
    ALTER COLUMN source_package_id DROP NOT NULL,
    ALTER COLUMN source_entry_bytes DROP NOT NULL,
    ALTER COLUMN source_record_count DROP NOT NULL,
    ALTER COLUMN future_source_rows_excluded DROP NOT NULL;

COMMENT ON COLUMN stock.source_raw_daily.source_package_id IS
    '旧TDX原始包关联键；BaoStock行情行必须留空并使用source_capture_id。';
COMMENT ON COLUMN stock.source_raw_daily.source_entry_bytes IS
    '仅旧TDX ZIP单证券条目字节数；BaoStock在线接口行留空。';
COMMENT ON COLUMN stock.source_raw_daily.source_record_count IS
    '仅旧TDX ZIP单证券条目扫描记录数；BaoStock在线接口行留空，响应行数见source_capture_requests。';
COMMENT ON COLUMN stock.source_raw_daily.future_source_rows_excluded IS
    '仅旧TDX全历史文件扫描时排除的未来行数；BaoStock按显式日期范围请求的在线事实留空。';

ALTER TABLE stock.source_raw_daily
    ADD CONSTRAINT source_raw_daily_source_code_ck CHECK (
        source_code IS NULL OR source_code ~* '^(SH|SZ|BJ)\.[0-9]{6}$'
    ) NOT VALID,
    ADD CONSTRAINT source_raw_daily_returned_fields_ck CHECK (
        source_code IS NULL
        OR (returned_fields IS NOT NULL AND cardinality(returned_fields) > 0)
    ) NOT VALID,
    ADD CONSTRAINT source_raw_daily_source_unit_contract_ck CHECK (
        source_code IS NULL OR unit_contract_version IS NOT NULL
    ) NOT VALID,
    ADD CONSTRAINT source_raw_daily_raw_preclose_quality_ck CHECK (
        (
            raw_preclose_quality IS NULL
            AND raw_preclose IS NULL
            AND raw_preclose_reason IS NULL
        )
        OR (
            raw_preclose_quality = 'OBSERVED'
            AND raw_preclose IS NOT NULL
            AND raw_preclose > 0
            AND raw_preclose_reason IS NULL
        )
        OR (
            raw_preclose_quality IN ('UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE')
            AND raw_preclose IS NULL
            AND raw_preclose_reason IS NOT NULL
            AND btrim(raw_preclose_reason) <> ''
        )
    ) NOT VALID,
    ADD CONSTRAINT source_raw_daily_qfq_quality_ck CHECK (
        (
            qfq_price_quality IS NULL
            AND qfq_open IS NULL
            AND qfq_high IS NULL
            AND qfq_low IS NULL
            AND qfq_close IS NULL
            AND qfq_price_reason IS NULL
        )
        OR (
            qfq_price_quality = 'OBSERVED'
            AND qfq_open > 0
            AND qfq_high > 0
            AND qfq_low > 0
            AND qfq_close > 0
            AND qfq_price_reason IS NULL
        )
        OR (
            qfq_price_quality IN ('UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE')
            AND qfq_open IS NULL
            AND qfq_high IS NULL
            AND qfq_low IS NULL
            AND qfq_close IS NULL
            AND qfq_price_reason IS NOT NULL
            AND btrim(qfq_price_reason) <> ''
        )
    ) NOT VALID,
    ADD CONSTRAINT source_raw_daily_raw_turn_quality_ck CHECK (
        (
            raw_turn_quality IS NULL
            AND raw_turn_value IS NULL
            AND raw_turn_reason IS NULL
        )
        OR (
            raw_turn_quality = 'OBSERVED'
            AND raw_turn_value IS NOT NULL
            AND raw_turn_value >= 0
            AND raw_turn_reason IS NULL
        )
        OR (
            raw_turn_quality IN ('UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE')
            AND raw_turn_value IS NULL
            AND raw_turn_reason IS NOT NULL
            AND btrim(raw_turn_reason) <> ''
        )
    ) NOT VALID,
    ADD CONSTRAINT source_raw_daily_raw_tradestatus_quality_ck CHECK (
        (
            raw_tradestatus_quality IS NULL
            AND raw_tradestatus_value IS NULL
            AND raw_tradestatus_reason IS NULL
        )
        OR (
            raw_tradestatus_quality = 'OBSERVED'
            AND raw_tradestatus_value IN ('0', '1')
            AND raw_tradestatus_reason IS NULL
        )
        OR (
            raw_tradestatus_quality IN ('UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE')
            AND raw_tradestatus_value IS NULL
            AND raw_tradestatus_reason IS NOT NULL
            AND btrim(raw_tradestatus_reason) <> ''
        )
    ) NOT VALID,
    ADD CONSTRAINT source_raw_daily_raw_is_st_quality_ck CHECK (
        (
            raw_is_st_quality IS NULL
            AND raw_is_st_value IS NULL
            AND raw_is_st_reason IS NULL
        )
        OR (
            raw_is_st_quality = 'OBSERVED'
            AND raw_is_st_value IN ('0', '1')
            AND raw_is_st_reason IS NULL
        )
        OR (
            raw_is_st_quality IN ('UNKNOWN', 'NOT_APPLICABLE', 'BAD_VALUE')
            AND raw_is_st_value IS NULL
            AND raw_is_st_reason IS NOT NULL
            AND btrim(raw_is_st_reason) <> ''
        )
    ) NOT VALID;

ALTER TABLE stock.source_raw_daily
    ADD CONSTRAINT source_raw_daily_unit_contract_fk
        FOREIGN KEY (unit_contract_version)
        REFERENCES stock.source_baostock_unit_contracts (unit_contract_version)
        ON DELETE RESTRICT NOT VALID;

CREATE INDEX source_raw_daily_qfq_symbol_session_idx
    ON stock.source_raw_daily (source_symbol, session_date DESC, source_capture_id)
    WHERE qfq_price_quality = 'OBSERVED';
COMMENT ON INDEX stock.source_raw_daily_qfq_symbol_session_idx IS
    '支持按证券、会话和捕获读取已观察到的BaoStock前复权价格；旧TDX行不进入该索引。';

COMMENT ON TABLE stock.source_raw_daily IS
    '保存每个来源捕获、来源证券代码和交易会话一行的行情事实。新BaoStock行在同一行分别保存未复权与前复权OHLC，量额、换手、交易状态和ST字段只保存未复权口径；旧TDX行暂留至最终退役迁移。';
COMMENT ON COLUMN stock.source_raw_daily.price_quality IS
    '未复权OHLC质量：OBSERVED已观察、UNKNOWN未知、NOT_APPLICABLE不适用或BAD_VALUE非法；不描述QFQ列组。';
COMMENT ON COLUMN stock.source_raw_daily.raw_open IS
    '未复权开盘价，单位为人民币元/股；QFQ开盘价另存于qfq_open。';
COMMENT ON COLUMN stock.source_raw_daily.raw_high IS
    '未复权最高价，单位为人民币元/股；QFQ最高价另存于qfq_high。';
COMMENT ON COLUMN stock.source_raw_daily.raw_low IS
    '未复权最低价，单位为人民币元/股；QFQ最低价另存于qfq_low。';
COMMENT ON COLUMN stock.source_raw_daily.raw_close IS
    '未复权收盘价，单位为人民币元/股；真实当日价格和限价参考使用此坐标。';
COMMENT ON COLUMN stock.source_raw_daily.raw_amount IS
    '未复权成交金额，按单位合同转换为人民币元；前复权不改变成交金额。';
COMMENT ON COLUMN stock.source_raw_daily.raw_volume IS
    '未复权成交量，按单位合同转换为整数股；前复权不改变成交量。';
COMMENT ON COLUMN stock.source_raw_daily.source_clock_role IS
    '来源观察时钟角色；新BaoStock历史查询固定标记后验捕获NON_PIT，不倒签历史可得时刻。';
COMMENT ON COLUMN stock.source_raw_daily.source_code IS
    'BaoStock query_history_k_data_plus响应中的原始来源代码；旧TDX行为空。';
COMMENT ON COLUMN stock.source_raw_daily.returned_fields IS
    '本次BaoStock响应实际返回的字段名集合；旧TDX行为空，不保存来源响应对象。';
COMMENT ON COLUMN stock.source_raw_daily.unit_contract_version IS
    '外键绑定本行BaoStock价格、成交金额、成交量和换手率的单位及转换倍率；旧TDX行为空。';
COMMENT ON COLUMN stock.source_raw_daily.raw_preclose IS
    'BaoStock adjustflag=3返回的未复权前收盘价，单位为人民币元/股。';
COMMENT ON COLUMN stock.source_raw_daily.raw_preclose_quality IS
    '未复权前收盘价质量：OBSERVED、UNKNOWN、NOT_APPLICABLE或BAD_VALUE；旧TDX行为空。';
COMMENT ON COLUMN stock.source_raw_daily.raw_preclose_reason IS
    '未复权前收盘价未知、不适用或非法原因；OBSERVED时为空。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_open IS
    'BaoStock adjustflag=2前复权开盘价；使用无固定小数位的高精度数值类型。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_high IS
    'BaoStock adjustflag=2前复权最高价；使用无固定小数位的高精度数值类型。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_low IS
    'BaoStock adjustflag=2前复权最低价；使用无固定小数位的高精度数值类型。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_close IS
    'BaoStock adjustflag=2前复权收盘价；后验重建坐标为NON_PIT，不冒充当时已保存值。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_price_quality IS
    'QFQ OHLC组质量：OBSERVED、UNKNOWN、NOT_APPLICABLE或BAD_VALUE；与未复权price_quality分开。';
COMMENT ON COLUMN stock.source_raw_daily.qfq_price_reason IS
    'QFQ OHLC组未知、不适用或非法原因；质量为OBSERVED时为空。';
COMMENT ON COLUMN stock.source_raw_daily.raw_turn_value IS
    'BaoStock未复权换手率，按单位合同规范化为比例值；不为QFQ重复保存。';
COMMENT ON COLUMN stock.source_raw_daily.raw_turn_quality IS
    '未复权换手率质量：OBSERVED、UNKNOWN、NOT_APPLICABLE或BAD_VALUE。';
COMMENT ON COLUMN stock.source_raw_daily.raw_turn_reason IS
    '未复权换手率未知、不适用或非法原因；OBSERVED时为空。';
COMMENT ON COLUMN stock.source_raw_daily.raw_tradestatus_value IS
    'BaoStock未复权交易状态原值：1表示交易，0表示停牌；不从无Bar推断停牌。';
COMMENT ON COLUMN stock.source_raw_daily.raw_tradestatus_quality IS
    '未复权交易状态字段质量：OBSERVED、UNKNOWN、NOT_APPLICABLE或BAD_VALUE。';
COMMENT ON COLUMN stock.source_raw_daily.raw_tradestatus_reason IS
    '未复权交易状态未知、不适用或非法原因；OBSERVED时为空。';
COMMENT ON COLUMN stock.source_raw_daily.raw_is_st_value IS
    'BaoStock未复权isST原值：1表示来源标记ST，0表示来源未标记ST；不替代官方身份。';
COMMENT ON COLUMN stock.source_raw_daily.raw_is_st_quality IS
    '未复权isST字段质量：OBSERVED、UNKNOWN、NOT_APPLICABLE或BAD_VALUE。';
COMMENT ON COLUMN stock.source_raw_daily.raw_is_st_reason IS
    '未复权isST未知、不适用或非法原因；OBSERVED时为空。';

CREATE TABLE stock.source_capture_requests (
    capture_id bigint NOT NULL,
    source_code text NOT NULL,
    adjustment_flag text NOT NULL,
    range_start_session date NOT NULL,
    range_end_session date NOT NULL,
    eligible_start_session date NOT NULL,
    eligible_end_session date NOT NULL,
    request_status text NOT NULL,
    attempt_count integer NOT NULL DEFAULT 0,
    completed_through_session date,
    response_row_count bigint NOT NULL DEFAULT 0,
    returned_fields text[],
    last_attempt_started_at timestamptz,
    last_attempt_completed_at timestamptz,
    retry_after timestamptz,
    last_error_code text,
    last_error_message text,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (capture_id, source_code, adjustment_flag),
    FOREIGN KEY (capture_id)
        REFERENCES stock.source_captures (capture_id) ON DELETE RESTRICT,
    CHECK (source_code ~ '^(sh|sz|bj)\.[0-9]{6}$'),
    CHECK (adjustment_flag IN ('2', '3')),
    CHECK (range_start_session <= range_end_session),
    CHECK (
        eligible_start_session BETWEEN range_start_session AND range_end_session
        AND eligible_end_session BETWEEN eligible_start_session AND range_end_session
    ),
    CHECK (
        completed_through_session IS NULL
        OR completed_through_session BETWEEN range_start_session AND range_end_session
    ),
    CHECK (request_status IN (
        'PENDING', 'RUNNING', 'COMPLETED', 'NO_DATA',
        'RETRYABLE_FAILURE', 'PERMANENT_FAILURE'
    )),
    CHECK (attempt_count >= 0),
    CHECK (response_row_count >= 0),
    CHECK (
        request_status NOT IN ('RETRYABLE_FAILURE', 'PERMANENT_FAILURE')
        OR NULLIF(btrim(last_error_code), '') IS NOT NULL
    ),
    CHECK (
        last_attempt_completed_at IS NULL
        OR last_attempt_started_at IS NULL
        OR last_attempt_completed_at >= last_attempt_started_at
    )
);

CREATE INDEX source_capture_requests_retry_idx
    ON stock.source_capture_requests (capture_id, request_status, retry_after, source_code)
    WHERE request_status IN ('PENDING', 'RETRYABLE_FAILURE');
COMMENT ON INDEX stock.source_capture_requests_retry_idx IS
    '按捕获、请求状态和重试时刻恢复尚未完成的BaoStock证券及复权坐标请求。';

COMMENT ON TABLE stock.source_capture_requests IS
    '记录一次BaoStock捕获中每个来源代码与adjustflag组合的有界请求状态、断点、重试和响应行数；不保存响应payload。';
COMMENT ON COLUMN stock.source_capture_requests.capture_id IS
    '外键引用唯一stock.source_captures捕获数字键；同一捕获内的证券/坐标请求归属一致。';
COMMENT ON COLUMN stock.source_capture_requests.source_code IS
    'BaoStock query_history_k_data_plus使用的原始沪深北代码，小写sh、sz或bj前缀加六位代码。';
COMMENT ON COLUMN stock.source_capture_requests.adjustment_flag IS
    'BaoStock坐标参数：3为未复权RAW，2为前复权QFQ；每个代码分别请求。';
COMMENT ON COLUMN stock.source_capture_requests.range_start_session IS
    '本次请求显式限定的起始官方交易会话；不得由机器当前日期回退或扩展。';
COMMENT ON COLUMN stock.source_capture_requests.range_end_session IS
    '本次请求显式限定的结束官方交易会话；断点和重试不得越过该会话。';
COMMENT ON COLUMN stock.source_capture_requests.eligible_start_session IS
    '按BaoStock基本资料上市/退市日期裁定的该证券本次有效研究范围起始会话；仍标记NON_PIT。';
COMMENT ON COLUMN stock.source_capture_requests.eligible_end_session IS
    '按BaoStock基本资料上市/退市日期裁定的该证券本次有效研究范围结束会话；仍标记NON_PIT。';
COMMENT ON COLUMN stock.source_capture_requests.request_status IS
    '请求状态：PENDING待请求、RUNNING执行中、COMPLETED完成、NO_DATA无行、RETRYABLE_FAILURE可重试、PERMANENT_FAILURE不可重试。';
COMMENT ON COLUMN stock.source_capture_requests.attempt_count IS
    '该代码及复权坐标在本捕获内已实际尝试的次数；重试必须遵守来源预算。';
COMMENT ON COLUMN stock.source_capture_requests.completed_through_session IS
    '已完整接收、校验并持久化到该官方会话的断点；失败或部分响应不得越过未提交会话。';
COMMENT ON COLUMN stock.source_capture_requests.response_row_count IS
    '当前捕获请求累计通过字段和日期校验的响应行数；不表示目标窗口无缺口。';
COMMENT ON COLUMN stock.source_capture_requests.returned_fields IS
    '该代码和坐标最近一次真实响应中按原顺序返回的字段名；失败前未取得响应字段时为空，不保存任何行情响应行。';
COMMENT ON COLUMN stock.source_capture_requests.last_attempt_started_at IS
    '最近一次来源请求实际开始时刻，含时区；不是历史available_at。';
COMMENT ON COLUMN stock.source_capture_requests.last_attempt_completed_at IS
    '最近一次来源请求完成或失败时刻，含时区；不是历史available_at。';
COMMENT ON COLUMN stock.source_capture_requests.retry_after IS
    '本地退避后允许再次请求的时刻；为空表示无需等待额外退避。';
COMMENT ON COLUMN stock.source_capture_requests.last_error_code IS
    '最近一次可重试或不可重试失败的稳定原因码；正常状态为空。';
COMMENT ON COLUMN stock.source_capture_requests.last_error_message IS
    '最近一次失败的简明中文说明；不得包含凭据或整份来源响应。';
COMMENT ON COLUMN stock.source_capture_requests.updated_at IS
    '该请求进度最近一次持久化时刻，由数据库生成。';

ALTER TABLE stock.data_jobs
    ADD COLUMN source_capture_id bigint;

COMMENT ON COLUMN stock.data_jobs.source_capture_id IS
    '本任务实际完成来源采集所绑定的通用捕获数字键；任务开始或来源尚未完成时为空。';

ALTER TABLE stock.tdx_sector_source_snapshots
    ADD COLUMN source_capture_id bigint;

COMMENT ON COLUMN stock.tdx_sector_source_snapshots.source_capture_id IS
    '新TDX远程行业/GN成员快照关联的通用来源捕获数字键；迁移前旧快照为空。';

CREATE INDEX tdx_sector_source_snapshots_capture_idx
    ON stock.tdx_sector_source_snapshots (source_capture_id)
    WHERE source_capture_id IS NOT NULL;
COMMENT ON INDEX stock.tdx_sector_source_snapshots_capture_idx IS
    '按真实来源捕获回查对应的TDX行业或GN成员快照。';

ALTER TABLE stock.source_symbol_display_names
    ADD COLUMN source_capture_id bigint;

ALTER TABLE stock.source_symbol_display_names
    ALTER COLUMN source_file DROP NOT NULL;

ALTER TABLE stock.source_symbol_display_names
    DROP CONSTRAINT source_symbol_display_names_name_ck,
    DROP CONSTRAINT source_symbol_display_names_file_ck,
    DROP CONSTRAINT source_symbol_display_names_contract_ck,
    DROP CONSTRAINT source_symbol_display_names_authority_ck,
    DROP CONSTRAINT source_symbol_display_names_time_quality_ck,
    DROP CONSTRAINT source_symbol_display_names_clock_role_ck,
    DROP CONSTRAINT source_symbol_display_names_identity_ck,
    ADD CONSTRAINT source_symbol_display_names_name_ck CHECK (
        display_name = btrim(display_name)
        AND char_length(display_name) BETWEEN 1 AND 128
    ) NOT VALID,
    ADD CONSTRAINT source_symbol_display_names_file_ck CHECK (
        source_file IS NULL OR source_file IN (
            'shs.tnf', 'szs.tnf', 'bjs.tnf', 'BAOSTOCK_QUERY_STOCK_BASIC'
        )
    ) NOT VALID,
    ADD CONSTRAINT source_symbol_display_names_contract_ck CHECK (
        parser_contract IN (
            'TDX_TNF_DISPLAY_NAME_R1', 'BAOSTOCK_QUERY_STOCK_BASIC/R1'
        )
    ) NOT VALID,
    ADD CONSTRAINT source_symbol_display_names_authority_ck CHECK (
        source_authority IN (
            'UNVERIFIED_LOCAL_TDX_INSTALLATION', 'BAOSTOCK_PUBLIC_API'
        )
    ) NOT VALID,
    ADD CONSTRAINT source_symbol_display_names_time_quality_ck CHECK (
        source_time_quality IN ('NON_PIT_OBSERVED', 'CAPTURED_NON_PIT')
    ) NOT VALID,
    ADD CONSTRAINT source_symbol_display_names_clock_role_ck CHECK (
        source_clock_role IN (
            'OBSERVED_CAPTURED', 'BAOSTOCK_QUERY_STOCK_BASIC_CAPTURED_NON_PIT'
        )
    ) NOT VALID,
    ADD CONSTRAINT source_symbol_display_names_identity_ck CHECK (
        identity_status IN (
            'SOURCE_SYMBOL_ONLY_NOT_STABLE_ENTITY',
            'SOURCE_CODE_NAME_NOT_STABLE_ENTITY'
        )
    ) NOT VALID;

ALTER TABLE stock.source_symbol_display_names
    ADD CONSTRAINT source_symbol_display_names_capture_required_ck CHECK (
        parser_contract <> 'BAOSTOCK_QUERY_STOCK_BASIC/R1'
        OR source_capture_id IS NOT NULL
    ) NOT VALID,
    ADD CONSTRAINT source_symbol_display_names_baostock_contract_ck CHECK (
        parser_contract <> 'BAOSTOCK_QUERY_STOCK_BASIC/R1'
        OR (
            source_file = 'BAOSTOCK_QUERY_STOCK_BASIC'
            AND source_authority = 'BAOSTOCK_PUBLIC_API'
            AND source_time_quality = 'CAPTURED_NON_PIT'
            AND source_clock_role = 'BAOSTOCK_QUERY_STOCK_BASIC_CAPTURED_NON_PIT'
            AND identity_status = 'SOURCE_CODE_NAME_NOT_STABLE_ENTITY'
            AND pit_status = 'NON_PIT'
            AND source_available_at IS NULL
            AND source_capture_id IS NOT NULL
        )
    ) NOT VALID,
    ADD CONSTRAINT source_symbol_display_names_capture_fk
        FOREIGN KEY (source_capture_id)
        REFERENCES stock.source_captures (capture_id)
        ON DELETE RESTRICT NOT VALID;

COMMENT ON TABLE stock.source_symbol_display_names IS
    '保存当前可见证券代码名称快照。新名称来自BaoStock query_stock_basic的code_name并绑定实际捕获；快照和时钟均为NON_PIT，不构成稳定实体身份。旧TDX TNF名称行仅为清理前兼容。';
COMMENT ON COLUMN stock.source_symbol_display_names.source_symbol IS
    '来源证券代码，作为当前名称快照键；不代表稳定security_id或官方同实体证明。';
COMMENT ON COLUMN stock.source_symbol_display_names.display_name IS
    '新快照保存BaoStock query_stock_basic返回的code_name；仅用于显示和代码搜索，不证明证券实体身份。';
COMMENT ON COLUMN stock.source_symbol_display_names.source_file IS
    '旧TDX行填写TNF文件名；BaoStock基本资料快照固定写BAOSTOCK_QUERY_STOCK_BASIC。';
COMMENT ON COLUMN stock.source_symbol_display_names.parser_contract IS
    '名称来源合同：旧TDX_TNF_DISPLAY_NAME_R1或BaoStock_QUERY_STOCK_BASIC/R1。';
COMMENT ON COLUMN stock.source_symbol_display_names.source_authority IS
    '名称来源权威：旧本机TDX未独立核验，或BAOSTOCK_PUBLIC_API来源观察；不等同监管身份认证。';
COMMENT ON COLUMN stock.source_symbol_display_names.source_time_quality IS
    '名称来源时钟质量：NON_PIT_OBSERVED或CAPTURED_NON_PIT，不回填历史名称有效期。';
COMMENT ON COLUMN stock.source_symbol_display_names.source_clock_role IS
    '名称快照时钟角色；BaoStock使用BAOSTOCK_QUERY_STOCK_BASIC_CAPTURED_NON_PIT。';
COMMENT ON COLUMN stock.source_symbol_display_names.source_available_at IS
    '名称来源真实可得时刻；无法证明历史时刻时为空，不以本机捕获时间代填。';
COMMENT ON COLUMN stock.source_symbol_display_names.captured_at IS
    '读取BaoStock query_stock_basic或旧TNF名称快照的实际带时区时刻；不是历史available_at。';
COMMENT ON COLUMN stock.source_symbol_display_names.identity_status IS
    '名称身份状态；SOURCE_CODE_NAME_NOT_STABLE_ENTITY表示来源代码名称不构成稳定证券实体证明。';
COMMENT ON COLUMN stock.source_symbol_display_names.pit_status IS
    'PIT状态固定NON_PIT，仅供当前展示和搜索，不回填历史名称。';
COMMENT ON COLUMN stock.source_symbol_display_names.source_capture_id IS
    'BaoStock query_stock_basic本次真实读取对应的通用来源捕获数字键；旧TNF名称行为空。';

DO $$
DECLARE
    item record;
BEGIN
    FOR item IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'stock.source_raw_daily'::regclass
          AND contype = 'c'
          AND (
              pg_get_constraintdef(oid) ILIKE '%amount_unit_status%'
              OR pg_get_constraintdef(oid) ILIKE '%volume_unit_status%'
              OR pg_get_constraintdef(oid) ILIKE '%source_clock_role%'
          )
    LOOP
        EXECUTE format(
            'ALTER TABLE stock.source_raw_daily DROP CONSTRAINT %I', item.conname
        );
    END LOOP;
END $$;

ALTER TABLE stock.source_raw_daily
    ADD CONSTRAINT source_raw_daily_amount_unit_status_ck CHECK (
        amount_unit_status IN ('UNIT_UNVERIFIED', 'CNY_VERIFIED')
    ) NOT VALID,
    ADD CONSTRAINT source_raw_daily_volume_unit_status_ck CHECK (
        volume_unit_status IN ('UNIT_UNVERIFIED', 'SHARES_VERIFIED')
    ) NOT VALID,
    ADD CONSTRAINT source_raw_daily_source_clock_role_ck CHECK (
        source_clock_role IN (
            'LOCAL_VERIFIED_VIPDATA_TARGET_SCAN_NON_PIT',
            'BAOSTOCK_QUERY_CAPTURED_NON_PIT'
        )
    ) NOT VALID;

COMMENT ON COLUMN stock.source_raw_daily.amount_unit_status IS
    '金额单位状态：旧TDX行保留UNIT_UNVERIFIED；BaoStock按单位合同规范化后使用CNY_VERIFIED。';
COMMENT ON COLUMN stock.source_raw_daily.volume_unit_status IS
    '数量单位状态：旧TDX行保留UNIT_UNVERIFIED；BaoStock按单位合同规范化后使用SHARES_VERIFIED。';

DO $$
DECLARE
    item record;
    capture_column text;
BEGIN
    FOR item IN
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'stock'
          AND column_name IN (
              'source_package_id', 'previous_source_package_id',
              'target_source_package_id', 't0_source_package_id',
              'package_sha256', 'previous_package_sha256',
              'target_package_sha256', 't0_package_sha256'
          )
          AND table_name NOT IN ('source_packages', 'source_captures')
        ORDER BY table_name, ordinal_position
    LOOP
        capture_column := CASE item.column_name
            WHEN 'source_package_id' THEN 'source_capture_id'
            WHEN 'previous_source_package_id' THEN 'previous_source_capture_id'
            WHEN 'target_source_package_id' THEN 'target_source_capture_id'
            WHEN 't0_source_package_id' THEN 't0_source_capture_id'
            WHEN 'package_sha256' THEN 'source_capture_id'
            WHEN 'previous_package_sha256' THEN 'previous_source_capture_id'
            WHEN 'target_package_sha256' THEN 'target_source_capture_id'
            WHEN 't0_package_sha256' THEN 't0_source_capture_id'
            ELSE NULL
        END;
        EXECUTE format(
            'COMMENT ON COLUMN stock.%I.%I IS %L',
            item.table_name,
            item.column_name,
            '旧TDX来源包或摘要身份，仅为清理前兼容；新的BaoStock事实将该字段置空并使用对应capture列。'
        );
        IF capture_column IS NOT NULL AND EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'stock'
              AND table_name = item.table_name
              AND column_name = capture_column
        ) THEN
            EXECUTE format(
                'COMMENT ON COLUMN stock.%I.%I IS %L',
                item.table_name,
                capture_column,
                CASE capture_column
                    WHEN 'source_capture_id' THEN
                        '该业务事实来源的真实捕获数字键，引用stock.source_captures.capture_id；新来源写入必须绑定。'
                    WHEN 'previous_source_capture_id' THEN
                        '前一侧来源观察的真实捕获数字键，引用stock.source_captures.capture_id。'
                    WHEN 'target_source_capture_id' THEN
                        '目标侧来源观察的真实捕获数字键，引用stock.source_captures.capture_id。'
                    WHEN 't0_source_capture_id' THEN
                        'T0来源观察的真实捕获数字键，引用stock.source_captures.capture_id。'
                    WHEN 'endpoint_source_capture_id' THEN
                        '终点来源观察的真实捕获数字键，引用stock.source_captures.capture_id。'
                    ELSE
                        '该业务事实关联的来源捕获数字键，引用stock.source_captures.capture_id。'
                END
            );
        END IF;
    END LOOP;
END $$;

DO $$
DECLARE
    item record;
    capture_column text;
BEGIN
    FOR item IN
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'stock'
          AND column_name IN (
              'source_package_id', 'previous_source_package_id',
              'target_source_package_id', 't0_source_package_id',
              'package_sha256', 'previous_package_sha256',
              'target_package_sha256', 't0_package_sha256'
          )
          AND table_name NOT IN ('source_packages', 'source_captures')
        ORDER BY table_name, ordinal_position
    LOOP
        capture_column := CASE item.column_name
            WHEN 'source_package_id' THEN 'source_capture_id'
            WHEN 'previous_source_package_id' THEN 'previous_source_capture_id'
            WHEN 'target_source_package_id' THEN 'target_source_capture_id'
            WHEN 't0_source_package_id' THEN 't0_source_capture_id'
            WHEN 'package_sha256' THEN 'source_capture_id'
            WHEN 'previous_package_sha256' THEN 'previous_source_capture_id'
            WHEN 'target_package_sha256' THEN 'target_source_capture_id'
            WHEN 't0_package_sha256' THEN 't0_source_capture_id'
            ELSE NULL
        END;
        IF capture_column IS NULL THEN
            RAISE EXCEPTION '没有为.stock.%的package身份列%登记capture映射',
                item.table_name, item.column_name;
        END IF;
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'stock'
              AND table_name = item.table_name
              AND column_name = capture_column
        ) THEN
            EXECUTE format(
                'ALTER TABLE stock.%I ADD COLUMN %I bigint',
                item.table_name, capture_column
            );
            EXECUTE format(
                'COMMENT ON COLUMN stock.%I.%I IS %L',
                item.table_name,
                capture_column,
                '来源捕获数字键；新写入绑定stock.source_captures中的实际供应方捕获，旧TDX行暂为空。'
            );
        END IF;
    END LOOP;
END $$;

DO $$
DECLARE
    item record;
    table_oid oid;
    column_number smallint;
    is_primary_key_column boolean;
BEGIN
    FOR item IN
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'stock'
          AND column_name IN (
              'source_package_id', 'previous_source_package_id',
              'target_source_package_id', 't0_source_package_id',
              'package_sha256', 'previous_package_sha256',
              'target_package_sha256', 't0_package_sha256'
          )
          AND table_name NOT IN ('source_packages', 'source_captures')
          AND is_nullable = 'NO'
        ORDER BY table_name, column_name
    LOOP
        table_oid := format('stock.%I', item.table_name)::regclass;
        SELECT attnum INTO column_number
        FROM pg_attribute
        WHERE attrelid = table_oid AND attname = item.column_name
          AND attnum > 0 AND NOT attisdropped;
        SELECT EXISTS (
            SELECT 1
            FROM pg_constraint primary_key
            WHERE primary_key.contype = 'p'
              AND primary_key.conrelid = table_oid
              AND column_number = ANY(primary_key.conkey)
        ) INTO is_primary_key_column;
        IF NOT is_primary_key_column THEN
            EXECUTE format(
                'ALTER TABLE stock.%I ALTER COLUMN %I DROP NOT NULL',
                item.table_name, item.column_name
            );
        END IF;
    END LOOP;
END $$;

DO $$
DECLARE
    item record;
    capture_relation oid := 'stock.source_captures'::regclass;
    capture_attnum smallint;
    constraint_name text;
BEGIN
    SELECT attnum INTO capture_attnum
    FROM pg_attribute
    WHERE attrelid = capture_relation AND attname = 'capture_id'
      AND attnum > 0 AND NOT attisdropped;
    IF capture_attnum IS NULL THEN
        RAISE EXCEPTION 'source_captures.capture_id缺失';
    END IF;

    FOR item IN
        SELECT table_class.oid AS relation_oid,
               table_class.relname AS table_name,
               attribute.attname AS column_name,
               attribute.attnum AS attribute_number
        FROM pg_class table_class
        JOIN pg_namespace table_namespace ON table_namespace.oid = table_class.relnamespace
        JOIN pg_attribute attribute ON attribute.attrelid = table_class.oid
        WHERE table_namespace.nspname = 'stock'
          AND table_class.relkind IN ('r', 'p')
          AND table_class.relname <> 'source_captures'
          AND attribute.attnum > 0
          AND NOT attribute.attisdropped
          AND attribute.atttypid = 'bigint'::regtype
          AND attribute.attname IN (
              'source_capture_id', 'target_capture_id', 'anchor_capture_id',
              'previous_source_capture_id', 'target_source_capture_id',
              't0_source_capture_id', 'endpoint_source_capture_id'
          )
        ORDER BY table_class.relname, attribute.attname
    LOOP
        IF NOT EXISTS (
            SELECT 1
            FROM pg_constraint foreign_key
            WHERE foreign_key.contype = 'f'
              AND foreign_key.conrelid = item.relation_oid
              AND foreign_key.confrelid = capture_relation
              AND foreign_key.conkey = ARRAY[item.attribute_number]::smallint[]
              AND foreign_key.confkey = ARRAY[capture_attnum]::smallint[]
        ) THEN
            constraint_name := left(item.table_name, 35)
                || '_capture_fk_' || substr(md5(item.column_name), 1, 8);
            EXECUTE format(
                'ALTER TABLE stock.%I ADD CONSTRAINT %I FOREIGN KEY (%I) REFERENCES stock.source_captures (capture_id) ON DELETE RESTRICT NOT VALID',
                item.table_name, constraint_name, item.column_name
            );
        END IF;
    END LOOP;
END $$;
