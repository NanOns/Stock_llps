-- 用途：固定项目数据库内的安装身份、集中迁移账本和角色命名空间绑定。
-- 前置条件：由迁移入口只读确认数据库为stock_a且schema stock已存在；此文件不创建数据库或schema。
CREATE TABLE stock.installation_identity (
    singleton boolean PRIMARY KEY CHECK (singleton IS TRUE),
    application_id text NOT NULL CHECK (application_id = 'STOCK_A'),
    project_uuid uuid NOT NULL,
    instance_uuid uuid NOT NULL UNIQUE,
    cluster_system_identifier text NOT NULL CHECK (cluster_system_identifier ~ '^[0-9]+$'),
    migration_lineage text NOT NULL CHECK (migration_lineage = 'STOCK_A_INITIAL_R1')
);
COMMENT ON TABLE stock.installation_identity IS 'Stock A在固定PostgreSQL实例中的单例安装身份，绑定项目、部署实例、物理簇和迁移主线。';
COMMENT ON COLUMN stock.installation_identity.singleton IS '单例标记，唯一合法值为TRUE，用于确保本表仅有一条安装身份。';
COMMENT ON COLUMN stock.installation_identity.application_id IS '应用固定标识，必须为STOCK_A。';
COMMENT ON COLUMN stock.installation_identity.project_uuid IS 'Stock A项目稳定UUID，对应正式运行配置。';
COMMENT ON COLUMN stock.installation_identity.instance_uuid IS '本部署实例UUID，由显式绑定流程生成并与配置一致。';
COMMENT ON COLUMN stock.installation_identity.cluster_system_identifier IS 'PostgreSQL物理簇system_identifier十进制文本，必须与实测簇一致。';
COMMENT ON COLUMN stock.installation_identity.migration_lineage IS '唯一迁移主线标识，当前固定为STOCK_A_INITIAL_R1。';

CREATE TABLE stock.migration_ledger (
    migration_id integer PRIMARY KEY CHECK (migration_id > 0),
    source_sha char(64) NOT NULL CHECK (source_sha ~ '^[0-9a-f]{64}$'),
    applied_at timestamptz NOT NULL,
    lineage text NOT NULL CHECK (lineage = 'STOCK_A_INITIAL_R1')
);
COMMENT ON TABLE stock.migration_ledger IS '记录Stock A集中迁移的有序版本、迁移源SHA、实际应用时间和唯一迁移主线。';
COMMENT ON COLUMN stock.migration_ledger.migration_id IS '正整数迁移序号，在本项目唯一业务schema内严格有序。';
COMMENT ON COLUMN stock.migration_ledger.source_sha IS '迁移SQL原始UTF-8字节的SHA-256小写十六进制摘要。';
COMMENT ON COLUMN stock.migration_ledger.applied_at IS '迁移事务写入的数据库时间，含时区；不表示业务数据可用时间。';
COMMENT ON COLUMN stock.migration_ledger.lineage IS '迁移主线标识，当前固定为STOCK_A_INITIAL_R1。';

CREATE TABLE stock.namespaces (
    namespace_id uuid PRIMARY KEY,
    environment text NOT NULL CHECK (environment IN ('DEV', 'PROD', 'TEST')),
    instance_uuid uuid NOT NULL REFERENCES stock.installation_identity(instance_uuid),
    UNIQUE (environment, instance_uuid)
);
COMMENT ON TABLE stock.namespaces IS '在唯一stock schema内登记DEV、PROD、TEST运行命名空间，作为后续业务复合键的隔离边界。';
COMMENT ON COLUMN stock.namespaces.namespace_id IS '命名空间稳定UUID，由项目、环境和部署实例确定。';
COMMENT ON COLUMN stock.namespaces.environment IS '运行环境固定枚举：DEV开发、PROD生产、TEST合成测试。';
COMMENT ON COLUMN stock.namespaces.instance_uuid IS '所属部署实例UUID，外键绑定本库唯一安装身份。';

CREATE TABLE stock.role_bindings (
    role_name name NOT NULL,
    namespace_id uuid NOT NULL REFERENCES stock.namespaces(namespace_id),
    capability text NOT NULL CHECK (capability IN ('READ', 'WRITE_CANDIDATE', 'ACCEPT', 'TEST')),
    PRIMARY KEY (role_name, namespace_id, capability)
);
COMMENT ON TABLE stock.role_bindings IS '将受控PostgreSQL业务角色绑定到明确环境命名空间和能力，未登记角色默认拒绝。';
COMMENT ON COLUMN stock.role_bindings.role_name IS 'PostgreSQL角色名，须与本项目受控角色组及实际登录角色关系核验。';
COMMENT ON COLUMN stock.role_bindings.namespace_id IS '能力授权适用的命名空间UUID，外键绑定stock.namespaces。';
COMMENT ON COLUMN stock.role_bindings.capability IS '固定能力枚举：READ读取、WRITE_CANDIDATE写开发候选、ACCEPT受控接受、TEST合成测试。';

ALTER TABLE stock.installation_identity OWNER TO stock_a_owner;
ALTER TABLE stock.migration_ledger OWNER TO stock_a_owner;
ALTER TABLE stock.namespaces OWNER TO stock_a_owner;
ALTER TABLE stock.role_bindings OWNER TO stock_a_owner;
