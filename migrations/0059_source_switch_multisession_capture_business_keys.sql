ALTER TABLE stock.source_d0_launch_day_facts
    DROP CONSTRAINT source_d0_launch_business_uq,
    ADD CONSTRAINT source_d0_launch_business_uq
        UNIQUE NULLS NOT DISTINCT (
            source_capture_id,
            target_session,
            source_symbol,
            before_capture_id,
            target_capture_id,
            producer_version,
            algorithm_contract,
            session_contract,
            calendar_revision
        );
COMMENT ON CONSTRAINT source_d0_launch_business_uq
    ON stock.source_d0_launch_day_facts IS
    'D0启动日事实以paired捕获、目标会话、来源代码、前后捕获及算法合同组成幂等业务键；同一捕获可覆盖多个交易会话。';

ALTER TABLE stock.source_d0_scenario_day_facts
    DROP CONSTRAINT source_d0_scenario_business_uq,
    ADD CONSTRAINT source_d0_scenario_business_uq
        UNIQUE (
            target_session,
            source_symbol,
            source_capture_id,
            scenario,
            producer_version,
            calendar_revision
        );
COMMENT ON CONSTRAINT source_d0_scenario_business_uq
    ON stock.source_d0_scenario_day_facts IS
    'D0逐日场景以目标会话、来源代码、paired捕获、场景、生产者和日历修订组成幂等业务键。';

ALTER TABLE stock.source_pool_seed_day_facts
    DROP CONSTRAINT source_pool_seed_business_uq,
    ADD CONSTRAINT source_pool_seed_business_uq
        UNIQUE (
            target_session,
            source_symbol,
            source_capture_id,
            profile_revision,
            producer_version
        );
COMMENT ON CONSTRAINT source_pool_seed_business_uq
    ON stock.source_pool_seed_day_facts IS
    'Seed画像事实以目标会话、来源代码、paired捕获、画像修订和生产者组成幂等业务键。';

ALTER TABLE stock.source_d1_structure_day_facts
    DROP CONSTRAINT source_d1_structure_business_uq,
    ADD CONSTRAINT source_d1_structure_business_uq
        UNIQUE (
            source_capture_id,
            target_session,
            source_symbol,
            structure_type,
            target_capture_id,
            d1_history_session_count,
            calendar_revision,
            producer_contract,
            producer_version,
            material_change_contract
        );
COMMENT ON CONSTRAINT source_d1_structure_business_uq
    ON stock.source_d1_structure_day_facts IS
    'D1结构事实以paired捕获、目标会话、来源代码、结构类型、窗口、日历和生产合同组成幂等业务键。';
