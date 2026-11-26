-- 为 Rotation Episode 唯一业务键补齐 member_version，隔离同日期同 scope 下不同成员版本的独立 Episode。

ALTER TABLE stock.r03_sector_rotation_episodes
    DROP CONSTRAINT r03_sector_rotation_episodes_uq,
    ADD CONSTRAINT r03_sector_rotation_episodes_uq UNIQUE (
        episode_key,
        scope_key,
        member_version,
        contract_version
    );

COMMENT ON CONSTRAINT r03_sector_rotation_episodes_uq ON stock.r03_sector_rotation_episodes IS
    '同一确定性 Episode、数据研究口径、出生时成员版本号与算法合同下仅允许一条出生记录。';
