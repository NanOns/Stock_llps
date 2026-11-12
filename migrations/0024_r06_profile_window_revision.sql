-- 保留算法版本，并允许以可逆日历身份及窗口表达不同计算输入。
ALTER TABLE stock.source_pool_seed_day_facts DROP CONSTRAINT r06_profile_revision_ck_1;
ALTER TABLE stock.source_pool_seed_day_facts ADD CONSTRAINT r06_profile_revision_ck_1
    CHECK (profile_revision ~ '^PROFILE_R[0-9]{2}([|][1-9][0-9]?[|][A-Za-z0-9_.~%-]+[|][A-Za-z0-9_.~%-]+)?$');
ALTER TABLE stock.source_rule_evaluations DROP CONSTRAINT r06_profile_revision_ck_2;
ALTER TABLE stock.source_rule_evaluations ADD CONSTRAINT r06_profile_revision_ck_2
    CHECK (profile_revision ~ '^PROFILE_R[0-9]{2}([|][1-9][0-9]?[|][A-Za-z0-9_.~%-]+[|][A-Za-z0-9_.~%-]+)?$');
ALTER TABLE stock.source_sector_cross_period_pulse_facts DROP CONSTRAINT r06_profile_revision_ck_3;
ALTER TABLE stock.source_sector_cross_period_pulse_facts ADD CONSTRAINT r06_profile_revision_ck_3
    CHECK (profile_revision ~ '^PROFILE_R[0-9]{2}([|][1-9][0-9]?[|][A-Za-z0-9_.~%-]+[|][A-Za-z0-9_.~%-]+)?$');
ALTER TABLE stock.source_symbol_price_profiles DROP CONSTRAINT r06_profile_revision_ck_7;
ALTER TABLE stock.source_symbol_price_profiles ADD CONSTRAINT r06_profile_revision_ck_7
    CHECK (profile_revision ~ '^PROFILE_R[0-9]{2}([|][1-9][0-9]?[|][A-Za-z0-9_.~%-]+[|][A-Za-z0-9_.~%-]+)?$');
