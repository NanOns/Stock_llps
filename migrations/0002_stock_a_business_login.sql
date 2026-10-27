-- 前向迁移：统一由正式业务账号拥有本项目身份表。
-- 不改写已应用的0001，不删除其中的历史角色/命名空间记录，也不触及其它数据库。
ALTER TABLE stock.installation_identity OWNER TO stock_a;
ALTER TABLE stock.migration_ledger OWNER TO stock_a;
ALTER TABLE stock.namespaces OWNER TO stock_a;
ALTER TABLE stock.role_bindings OWNER TO stock_a;
COMMENT ON TABLE stock.namespaces IS '保留的应用运行上下文标识表；环境字段不授权PostgreSQL登录角色或数据库权限。';
COMMENT ON TABLE stock.role_bindings IS '0001历史迁移遗留记录表；当前运行与写入准入不读取此表，也不据此授权。';
COMMENT ON COLUMN stock.role_bindings.role_name IS '历史角色名记录；保留既有迁移数据，不作为当前身份或权限判据。';
COMMENT ON COLUMN stock.role_bindings.namespace_id IS '历史关联命名空间；保留既有外键与数据，不参与当前写入准入。';
COMMENT ON COLUMN stock.role_bindings.capability IS '历史能力枚举记录；不代表当前PostgreSQL角色或应用权限。';
