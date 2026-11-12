-- 0022已将画像修订列转为text；保持业务键，仅拒绝不完整的百分号编码。
ALTER TABLE stock.source_pool_seed_day_facts DROP CONSTRAINT r06_profile_revision_ck_1;
ALTER TABLE stock.source_pool_seed_day_facts ADD CONSTRAINT r06_profile_revision_ck_1
    CHECK (profile_revision ~ '^PROFILE_R[0-9]{2}([|][1-9][0-9]?[|]([A-Za-z0-9_.~-]|%[0-9A-F]{2})+[|]([A-Za-z0-9_.~-]|%[0-9A-F]{2})+)?$');
ALTER TABLE stock.source_rule_evaluations DROP CONSTRAINT r06_profile_revision_ck_2;
ALTER TABLE stock.source_rule_evaluations ADD CONSTRAINT r06_profile_revision_ck_2
    CHECK (profile_revision ~ '^PROFILE_R[0-9]{2}([|][1-9][0-9]?[|]([A-Za-z0-9_.~-]|%[0-9A-F]{2})+[|]([A-Za-z0-9_.~-]|%[0-9A-F]{2})+)?$');
ALTER TABLE stock.source_sector_cross_period_pulse_facts DROP CONSTRAINT r06_profile_revision_ck_3;
ALTER TABLE stock.source_sector_cross_period_pulse_facts ADD CONSTRAINT r06_profile_revision_ck_3
    CHECK (profile_revision ~ '^PROFILE_R[0-9]{2}([|][1-9][0-9]?[|]([A-Za-z0-9_.~-]|%[0-9A-F]{2})+[|]([A-Za-z0-9_.~-]|%[0-9A-F]{2})+)?$');
ALTER TABLE stock.source_symbol_price_profiles DROP CONSTRAINT r06_profile_revision_ck_7;
ALTER TABLE stock.source_symbol_price_profiles ADD CONSTRAINT r06_profile_revision_ck_7
    CHECK (profile_revision ~ '^PROFILE_R[0-9]{2}([|][1-9][0-9]?[|]([A-Za-z0-9_.~-]|%[0-9A-F]{2})+[|]([A-Za-z0-9_.~-]|%[0-9A-F]{2})+)?$');

COMMENT ON COLUMN stock.source_symbol_price_profiles.profile_revision IS '画像算法修订及可逆窗口、日历身份和修订链；完整文本参与业务键。';
COMMENT ON COLUMN stock.source_pool_seed_day_facts.profile_revision IS 'Seed绑定的完整画像修订键，与来源画像精确匹配。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.profile_revision IS 'PULSE绑定的完整画像修订键，与Seed及来源画像精确匹配。';
COMMENT ON COLUMN stock.source_rule_evaluations.profile_revision IS '规则计算绑定的完整画像修订键，保留窗口和日历修订差异。';
