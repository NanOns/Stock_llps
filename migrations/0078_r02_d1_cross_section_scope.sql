ALTER TABLE stock.source_pool_seed_day_facts
    DROP CONSTRAINT source_pool_seed_scope_ck,
    ADD CONSTRAINT source_pool_seed_scope_ck CHECK (
        (
            cross_section_universe_capture_id IS NULL
            AND cross_section_universe_version IS NULL
            AND cross_section_contract_version IS NULL
            AND universe_match_status IS NULL
            AND ranking_universe_count IS NULL
            AND evaluation_universe_count IS NULL
        )
        OR (
            universe_match_status IS NOT NULL
            AND evaluation_universe_count IS NOT NULL
            AND evaluation_universe_count > 0
            AND (
                (
                    universe_match_status='FALLBACK_CROSS_SECTION_INCOMPLETE'
                    AND ranking_universe_count IS NULL
                    AND cross_section_universe_version IS NOT NULL
                    AND btrim(cross_section_universe_version)<>''
                    AND cross_section_contract_version IS NOT NULL
                    AND btrim(cross_section_contract_version)<>''
                )
                OR (
                    universe_match_status<>'FALLBACK_CROSS_SECTION_INCOMPLETE'
                    AND ranking_universe_count IS NOT NULL
                    AND ranking_universe_count>=0
                )
            )
            AND universe_match_status IN (
                'EXACT_MATCH',
                'EVALUATION_SUBSET_OF_RANKING',
                'FALLBACK_UNIVERSE_MISMATCH',
                'FALLBACK_CROSS_SECTION_UNAVAILABLE',
                'FALLBACK_CROSS_SECTION_INCOMPLETE'
            )
            AND (
                universe_match_status<>'EXACT_MATCH'
                OR ranking_universe_count=evaluation_universe_count
            )
            AND (
                universe_match_status<>'EVALUATION_SUBSET_OF_RANKING'
                OR ranking_universe_count>evaluation_universe_count
            )
            AND (
                (
                    universe_match_status='FALLBACK_CROSS_SECTION_UNAVAILABLE'
                    AND cross_section_universe_capture_id IS NULL
                    AND cross_section_universe_version IS NULL
                    AND cross_section_contract_version IS NULL
                    AND ranking_universe_count=evaluation_universe_count
                )
                OR (
                    universe_match_status<>'FALLBACK_CROSS_SECTION_UNAVAILABLE'
                    AND cross_section_universe_version IS NOT NULL
                    AND btrim(cross_section_universe_version)<>''
                    AND cross_section_contract_version IS NOT NULL
                    AND btrim(cross_section_contract_version)<>''
                )
            )
        )
    );

ALTER TABLE stock.source_d1_structure_day_facts
    ADD COLUMN cross_section_universe_capture_id bigint
        REFERENCES stock.source_captures(capture_id),
    ADD COLUMN cross_section_universe_version text,
    ADD COLUMN cross_section_contract_version text,
    ADD CONSTRAINT source_d1_cross_section_scope_ck CHECK (
        (
            cross_section_universe_capture_id IS NULL
            AND cross_section_universe_version IS NULL
            AND cross_section_contract_version IS NULL
        )
        OR (
            producer_version='STOCK_A_D1_BAOSTOCK_QFQ_R4'
            AND cross_section_universe_version IS NOT NULL
            AND btrim(cross_section_universe_version)<>''
            AND cross_section_contract_version IS NOT NULL
            AND btrim(cross_section_contract_version)<>''
        )
    );

ALTER TABLE stock.source_d1_structure_day_facts
    DROP CONSTRAINT source_d1_structure_business_uq,
    ADD CONSTRAINT source_d1_structure_business_uq UNIQUE NULLS NOT DISTINCT (
        source_capture_id,
        target_session,
        source_symbol,
        structure_type,
        target_capture_id,
        d1_history_session_count,
        calendar_revision,
        producer_contract,
        producer_version,
        material_change_contract,
        cross_section_universe_capture_id,
        cross_section_universe_version,
        cross_section_contract_version
    );

COMMENT ON COLUMN stock.source_d1_structure_day_facts.cross_section_universe_capture_id IS
    'R4 D1目标日读取的横截面Universe来源捕获BIGINT；旧历史或未绑定横截面时为空。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.cross_section_universe_version IS
    'R4 D1目标日读取的横截面Universe版本；旧历史或未绑定横截面时为空。';
COMMENT ON COLUMN stock.source_d1_structure_day_facts.cross_section_contract_version IS
    'R4 D1目标日横截面的算法合同版本；旧历史或未绑定横截面时为空。';
COMMENT ON CONSTRAINT source_d1_cross_section_scope_ck ON stock.source_d1_structure_day_facts IS
    'R3或旧R4历史允许横截面scope为空；新R4绑定时必须保存Universe版本与合同。';
COMMENT ON CONSTRAINT source_d1_structure_business_uq ON stock.source_d1_structure_day_facts IS
    'D1唯一键包含精确横截面Universe身份；NULL旧历史按同scope冲突处理。';
COMMENT ON CONSTRAINT source_pool_seed_scope_ck ON stock.source_pool_seed_day_facts IS
    '未闭合横截面排名分母保存NULL；仅已知完整范围保存非负计数，旧历史scope全空。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.ranking_universe_count IS
    'RPS排名总体成员计数；横截面不完整时为空，旧历史为空。';
