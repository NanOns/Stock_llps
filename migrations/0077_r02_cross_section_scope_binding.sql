ALTER TABLE stock.source_pool_seed_day_facts
    ADD COLUMN cross_section_universe_capture_id bigint
        REFERENCES stock.source_captures(capture_id),
    ADD COLUMN cross_section_universe_version text,
    ADD COLUMN cross_section_contract_version text,
    ADD COLUMN universe_match_status text,
    ADD COLUMN ranking_universe_count integer,
    ADD COLUMN evaluation_universe_count integer,
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
            AND ranking_universe_count IS NOT NULL
            AND evaluation_universe_count IS NOT NULL
            AND universe_match_status IN (
                'EXACT_MATCH',
                'EVALUATION_SUBSET_OF_RANKING',
                'FALLBACK_UNIVERSE_MISMATCH',
                'FALLBACK_CROSS_SECTION_UNAVAILABLE',
                'FALLBACK_CROSS_SECTION_INCOMPLETE'
            )
            AND ranking_universe_count >= 0
            AND evaluation_universe_count > 0
            AND (
                universe_match_status <> 'EXACT_MATCH'
                OR ranking_universe_count = evaluation_universe_count
            )
            AND (
                universe_match_status <> 'EVALUATION_SUBSET_OF_RANKING'
                OR ranking_universe_count > evaluation_universe_count
            )
            AND (
                (
                    universe_match_status = 'FALLBACK_CROSS_SECTION_UNAVAILABLE'
                    AND cross_section_universe_capture_id IS NULL
                    AND cross_section_universe_version IS NULL
                    AND cross_section_contract_version IS NULL
                )
                OR (
                    universe_match_status <> 'FALLBACK_CROSS_SECTION_UNAVAILABLE'
                    AND cross_section_universe_version IS NOT NULL
                    AND btrim(cross_section_universe_version) <> ''
                    AND cross_section_contract_version IS NOT NULL
                    AND btrim(cross_section_contract_version) <> ''
                )
            )
        )
    );

ALTER TABLE stock.source_symbol_price_profiles
    ADD COLUMN cross_section_universe_capture_id bigint
        REFERENCES stock.source_captures(capture_id),
    ADD COLUMN cross_section_universe_version text,
    ADD COLUMN cross_section_contract_version text,
    ADD CONSTRAINT source_profile_cross_section_scope_ck CHECK (
        (
            cross_section_universe_capture_id IS NULL
            AND cross_section_universe_version IS NULL
            AND cross_section_contract_version IS NULL
        )
        OR (
            cross_section_universe_version IS NOT NULL
            AND btrim(cross_section_universe_version) <> ''
            AND cross_section_contract_version IS NOT NULL
            AND btrim(cross_section_contract_version) <> ''
        )
    );

ALTER TABLE stock.source_pool_seed_day_facts
    DROP CONSTRAINT source_pool_seed_business_uq,
    ADD CONSTRAINT source_pool_seed_business_uq UNIQUE NULLS NOT DISTINCT (
        target_session,
        source_capture_id,
        source_symbol,
        profile_revision,
        producer_version,
        cross_section_universe_capture_id,
        cross_section_universe_version,
        cross_section_contract_version,
        universe_match_status
    );

COMMENT ON COLUMN stock.source_pool_seed_day_facts.cross_section_universe_capture_id IS
    'Seed所用横截面Universe来源捕获BIGINT；缺失横截面或旧历史时为空。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.cross_section_universe_version IS
    'Seed排名总体的Universe版本；来源级回退不可用和旧历史时为空。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.cross_section_contract_version IS
    'Seed读取的横截面算法合同版本；来源级回退不可用和旧历史时为空。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.universe_match_status IS
    '排名总体与评价总体关系：同总体、排名超集、显式回退或横截面不完整；旧历史为空表示未记录。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.ranking_universe_count IS
    'RPS排名总体成员计数；横截面不完整时为0，旧历史为空。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.evaluation_universe_count IS
    'Seed谓词的评价总体成员计数；单位为证券行，旧历史为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.cross_section_universe_capture_id IS
    'Profile.relative所用横截面Universe来源捕获BIGINT；未使用横截面或旧历史时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.cross_section_universe_version IS
    'Profile.relative所用横截面Universe版本；未使用横截面或旧历史时为空。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.cross_section_contract_version IS
    'Profile.relative所用横截面算法合同版本；未使用横截面或旧历史时为空。';

COMMENT ON CONSTRAINT source_pool_seed_scope_ck ON stock.source_pool_seed_day_facts IS
    '允许旧事实六项scope字段全空；新事实必须保存完整总体状态和有效计数。';
COMMENT ON CONSTRAINT source_profile_cross_section_scope_ck ON stock.source_symbol_price_profiles IS
    'Profile.relative横截面版本和合同必须同时出现；旧事实或未绑定事实均为空。';
COMMENT ON CONSTRAINT source_pool_seed_business_uq ON stock.source_pool_seed_day_facts IS
    '同一日、捕获、证券、画像、生产者及精确横截面范围只允许一行Seed事实。';
