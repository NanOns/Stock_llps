-- 删除PULSE结果表中不再使用的下游来源内容摘要。
ALTER TABLE stock.source_sector_pulse_adjacent_transitions
    DROP COLUMN previous_source_content_id,
    DROP COLUMN target_source_content_id;

ALTER TABLE stock.source_sector_pulse_followup_facts
    DROP COLUMN t0_source_content_id,
    DROP COLUMN target_source_content_id,
    DROP COLUMN board_kind,
    DROP COLUMN board_native_id,
    DROP COLUMN board_name,
    DROP COLUMN pulse_state,
    DROP COLUMN pulse_reason_codes;

-- 终点捕获、包与会话必须指向同一个实际来源父记录。
ALTER TABLE stock.source_captures
    ADD CONSTRAINT source_captures_identity_package_session_uq
    UNIQUE (capture_id, source_package_id, target_session);
ALTER TABLE stock.source_sector_pulse_followup_facts
    ADD CONSTRAINT pulse_followup_target_capture_package_session_fk
    FOREIGN KEY (target_source_capture_id, target_source_package_id, target_session)
    REFERENCES stock.source_captures (capture_id, source_package_id, target_session);

-- 板块成员中保留的板块代码供现役查询使用，必须与目录父记录一致。
ALTER TABLE stock.tdx_sector_catalog
    ADD CONSTRAINT sector_catalog_identity_board_uq
    UNIQUE (id, board_kind, board_native_id);
ALTER TABLE stock.tdx_sector_members
    ADD CONSTRAINT sector_members_catalog_board_fk
    FOREIGN KEY (catalog_id, board_kind, board_native_id)
    REFERENCES stock.tdx_sector_catalog (id, board_kind, board_native_id);
