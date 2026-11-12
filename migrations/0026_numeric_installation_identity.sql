-- 安装身份保留单例和簇绑定，项目及部署实例改用数据库生成的数字标识。
ALTER TABLE stock.installation_identity
    ADD COLUMN project_id bigint GENERATED ALWAYS AS IDENTITY,
    ADD COLUMN instance_id bigint GENERATED ALWAYS AS IDENTITY;
ALTER TABLE stock.installation_identity
    ADD CONSTRAINT installation_identity_instance_id_key UNIQUE (instance_id);
ALTER TABLE stock.installation_identity
    DROP COLUMN project_uuid,
    DROP COLUMN instance_uuid;
COMMENT ON COLUMN stock.installation_identity.project_id IS '项目安装身份的数据库生成数字标识。';
COMMENT ON COLUMN stock.installation_identity.instance_id IS '部署实例的数据库生成数字标识，与配置及数据库簇共同核验。';
