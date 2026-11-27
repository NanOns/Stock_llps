-- R04 前向微调迁移 0094：在 source_d0_formal_admission_day_facts 中允许 RECOVERY_TURN 的 R20_MA20 与 R5_MA5 冻结来源枚举。

ALTER TABLE stock.source_d0_formal_admission_day_facts
    DROP CONSTRAINT IF EXISTS source_d0_formal_admission_line_ck;

ALTER TABLE stock.source_d0_formal_admission_day_facts
    ADD CONSTRAINT source_d0_formal_admission_line_ck
        CHECK (
            (
                admission_status = 'READY'
                AND raw_role = 'FORMAL'
                AND raw_state = 'TRUE'
                AND failure_level_numerator IS NOT NULL
                AND failure_level_denominator IS NOT NULL
                AND failure_level_denominator > 0
                AND failure_coordinate IN ('RAW_UNADJUSTED', 'FORWARD_ADJUSTED')
                AND failure_source IN (
                    'T0_PHC20',
                    'T0_MA20',
                    'T0_MA5',
                    'R20_MA20',
                    'R5_MA5',
                    'PRIOR_SUPPORT_LOWER',
                    'T0_MA20_FALLBACK'
                )
                AND failure_window_start IS NOT NULL
                AND failure_window_session_count >= 1
                AND failure_source_event_ref IS NOT NULL
                AND event_id IS NOT NULL
                AND effective_from_session > target_session
                AND research_universe_state = 'TRUE'
                AND actual_quote_state = 'TRUE'
                AND coordinate_window_state = 'TRUE'
                AND d0_liquidity_state = 'TRUE'
                AND non_core_damage_state = 'TRUE'
                AND non_extreme_extension_state = 'TRUE'
                AND non_first_day_damage_state = 'TRUE'
                AND non_severe_drop_state = 'TRUE'
            )
            OR
            (
                admission_status <> 'READY'
                AND event_id IS NULL
                AND failure_level_numerator IS NULL
                AND failure_level_denominator IS NULL
                AND failure_coordinate IS NULL
                AND failure_source IS NULL
                AND failure_window_start IS NULL
                AND failure_window_session_count IS NULL
                AND failure_source_event_ref IS NULL
                AND effective_from_session IS NULL
            )
        );
