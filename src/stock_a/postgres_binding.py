"""由明确CLI任务执行stock身份与最低权限角色绑定。"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import tempfile
import uuid
from typing import Any

from . import postgres_identity as identity


_MIGRATION_ID = 1
_MIGRATION_LINEAGE = "STOCK_A_INITIAL_R1"
_MIGRATION_PATH_PARTS = ("migrations", "0001_stock_identity.sql")
_ROLE_FLAGS = (False, False, False, False, False, False, False)
_ROLE_BINDINGS = (
    ("stock_a_reader", "PROD", "READ"),
    ("stock_a_dev_writer", "DEV", "WRITE_CANDIDATE"),
    ("stock_a_prod_writer", "PROD", "ACCEPT"),
    ("stock_a_test_writer", "TEST", "TEST"),
)
_ROLE_NAMES = ("stock_a_owner", "stock_a_migrator", "stock_a_reader",
               "stock_a_dev_writer", "stock_a_prod_writer", "stock_a_test_writer")
_EXPECTED_TABLES = {"installation_identity", "migration_ledger", "namespaces", "role_bindings"}


class BindingRefused(Exception):
    """固定身份或对象状态不允许安全绑定。"""


def _migration_source() -> tuple[Path, bytes, str]:
    root = Path(__file__).resolve().parents[2]
    path = root.joinpath(*_MIGRATION_PATH_PARTS)
    if path.is_symlink():
        raise BindingRefused("集中迁移文件不能通过符号链接替换")
    resolved = path.resolve(strict=True)
    if not resolved.is_relative_to(root) or not resolved.is_file():
        raise BindingRefused("集中迁移文件路径不在批准源码根内")
    raw = resolved.read_bytes()
    if not raw or len(raw) > 128 * 1024:
        raise BindingRefused("集中迁移文件为空或超过固定大小预算")
    return resolved, raw, hashlib.sha256(raw).hexdigest()


def _configured_connection() -> dict[str, Any]:
    import psycopg
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    dsn = os.environ.get("STOCK_A_PG_DSN")
    if dsn:
        return identity._fixed_connection_parameters(dsn, os.environ, conninfo_to_dict)
    credentials = identity._load_private_credentials()
    return identity._private_connection_parameters(credentials, make_conninfo, conninfo_to_dict)


def _schema_tables(cursor: Any) -> set[str]:
    cursor.execute("""SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
       WHERE n.nspname='stock' AND c.relkind IN ('r','p','v','m','S','f') ORDER BY c.relname""")
    return {row[0] for row in cursor.fetchall()}


def _role_state(cursor: Any, role: str) -> tuple[bool, ...] | None:
    cursor.execute("""SELECT rolcanlogin, rolsuper, rolcreatedb, rolcreaterole,
       rolreplication, rolbypassrls, rolinherit FROM pg_roles WHERE rolname=%s""", (role,))
    row = cursor.fetchone()
    return None if row is None else tuple(bool(value) for value in row)


def _validate_or_create_roles(cursor: Any, sql: Any) -> None:
    states = {name: _role_state(cursor, name) for name in _ROLE_NAMES}
    for name, state in states.items():
        if state is not None and state != _ROLE_FLAGS:
            raise BindingRefused(f"角色{name}已存在但属性不符合受控无登录角色合同，拒绝修改")
    for name, state in states.items():
        if state is None:
            cursor.execute(sql.SQL("CREATE ROLE {} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT").format(sql.Identifier(name)))
    cursor.execute("""SELECT rolname, rolsuper, rolcreatedb, rolcreaterole, rolreplication,
       rolbypassrls FROM pg_roles WHERE rolname = ANY(%s) ORDER BY rolname""", (list(_ROLE_NAMES),))
    rows = cursor.fetchall()
    if len(rows) != len(_ROLE_NAMES) or any(any(bool(flag) for flag in row[1:]) for row in rows):
        raise BindingRefused("受控项目角色创建后属性核验失败")


def _database_seed(cursor: Any, *, project_uuid: uuid.UUID, instance_uuid: uuid.UUID,
                   cluster_id: str, migration_sha: str) -> None:
    cursor.execute("""INSERT INTO stock.installation_identity
       (singleton, application_id, project_uuid, instance_uuid, cluster_system_identifier, migration_lineage)
       VALUES (TRUE, 'STOCK_A', %s, %s, %s, %s)""",
       (project_uuid, instance_uuid, cluster_id, _MIGRATION_LINEAGE))
    namespaces: dict[str, uuid.UUID] = {}
    for environment in ("DEV", "PROD", "TEST"):
        namespaces[environment] = uuid.uuid5(project_uuid, f"STOCK_A:{environment}:{instance_uuid}")
        cursor.execute("INSERT INTO stock.namespaces(namespace_id, environment, instance_uuid) VALUES (%s, %s, %s)",
                       (namespaces[environment], environment, instance_uuid))
    for role_name, environment, capability in _ROLE_BINDINGS:
        cursor.execute("INSERT INTO stock.role_bindings(role_name, namespace_id, capability) VALUES (%s, %s, %s)",
                       (role_name, namespaces[environment], capability))
    cursor.execute("INSERT INTO stock.migration_ledger(migration_id, source_sha, applied_at, lineage) VALUES (%s, %s, clock_timestamp(), %s)",
                   (_MIGRATION_ID, migration_sha, _MIGRATION_LINEAGE))


def _verify_seed(cursor: Any, *, project_uuid: uuid.UUID, instance_uuid: uuid.UUID,
                 cluster_id: str, migration_sha: str) -> None:
    cursor.execute("""SELECT application_id, project_uuid, instance_uuid,
       cluster_system_identifier, migration_lineage FROM stock.installation_identity WHERE singleton IS TRUE""")
    if cursor.fetchone() != ("STOCK_A", project_uuid, instance_uuid, cluster_id, _MIGRATION_LINEAGE):
        raise BindingRefused("现有安装身份与项目、实例、物理簇或迁移主线冲突")
    cursor.execute("SELECT migration_id, source_sha, lineage FROM stock.migration_ledger ORDER BY migration_id")
    if cursor.fetchall() != [(_MIGRATION_ID, migration_sha, _MIGRATION_LINEAGE)]:
        raise BindingRefused("现有迁移账本缺失、额外或摘要冲突")
    expected_namespaces = {
        (environment, instance_uuid, uuid.uuid5(project_uuid, f"STOCK_A:{environment}:{instance_uuid}"))
        for environment in ("DEV", "PROD", "TEST")
    }
    cursor.execute("SELECT environment, instance_uuid, namespace_id FROM stock.namespaces ORDER BY environment")
    if set(cursor.fetchall()) != expected_namespaces:
        raise BindingRefused("现有环境命名空间集合与配置不一致")
    expected_bindings = {
        (role, uuid.uuid5(project_uuid, f"STOCK_A:{environment}:{instance_uuid}"), capability)
        for role, environment, capability in _ROLE_BINDINGS
    }
    cursor.execute("SELECT role_name::text, namespace_id, capability FROM stock.role_bindings")
    if set(cursor.fetchall()) != expected_bindings:
        raise BindingRefused("现有角色能力绑定与项目合同不一致")
    cursor.execute("""SELECT count(DISTINCT c.oid) FILTER (WHERE c.relkind IN ('r','p')),
       count(a.attnum) FILTER (WHERE c.relkind IN ('r','p') AND a.attnum > 0 AND NOT a.attisdropped),
       count(DISTINCT c.oid) FILTER (WHERE c.relkind IN ('r','p') AND obj_description(c.oid, 'pg_class') IS NULL),
       count(a.attnum) FILTER (WHERE c.relkind IN ('r','p') AND a.attnum > 0 AND NOT a.attisdropped
         AND col_description(c.oid, a.attnum) IS NULL)
       FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
       LEFT JOIN pg_attribute a ON a.attrelid=c.oid WHERE n.nspname='stock'""")
    comments = tuple(int(value or 0) for value in cursor.fetchone())
    if comments != (4, 16, 0, 0):
        raise BindingRefused("表/字段中文注释覆盖与当前集中迁移不一致")


def _verify_effective_roles(cursor: Any) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    for role in _ROLE_NAMES:
        cursor.execute("SELECT set_config('role', %s, true)", (role,))
        cursor.execute("""SELECT current_user,
           has_schema_privilege(current_user, 'stock', 'USAGE'),
           has_schema_privilege(current_user, 'stock', 'CREATE'),
           has_table_privilege(current_user, 'stock.installation_identity', 'SELECT'),
           has_table_privilege(current_user, 'stock.installation_identity', 'UPDATE'),
           EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
             WHERE n.nspname='stock' AND c.relowner=(SELECT oid FROM pg_roles WHERE rolname=current_user))""")
        row = cursor.fetchone()
        if row is None or row[0] != role:
            raise BindingRefused("角色有效权限检查未能切换到目标角色")
        expected = (True, role == "stock_a_owner", role == "stock_a_owner",
                    role == "stock_a_owner", role == "stock_a_owner")
        actual = (bool(row[1]), bool(row[2]), bool(row[3]), bool(row[4]), bool(row[5]))
        if actual != expected:
            raise BindingRefused(f"角色{role}的schema/table/owner权限不符合最低权限预期")
        checks.append({"role": role, "schema_create": bool(row[2]), "identity_table_select": bool(row[3]),
                       "identity_table_update": bool(row[4]), "owns_business_objects": bool(row[5]),
                       "login": False, "superuser": False, "createdb": False,
                       "createrole": False, "bypassrls": False, "inherit": False,
                       "tested_as": "SET_LOCAL_ROLE_IN_ADMIN_SESSION"})
        cursor.execute("RESET ROLE")
    return checks


def _update_runtime_config(config_path: Path, config: dict[str, Any], *, instance_uuid: uuid.UUID,
                           cluster_id: str, migration_digest: str) -> None:
    config = json.loads(json.dumps(config))
    app = config["application_identity"]
    app["deployment_instance_uuid"] = str(instance_uuid)
    app["cluster_system_identifier"] = cluster_id
    app["migration_digest"] = migration_digest
    app["deployment_applied"] = True
    encoded = (json.dumps(config, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode("utf-8")
    temp_root = Path("G:/codex_tmp")
    temp_root.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix="stock-a-runtime-bind-", suffix=".tmp", dir=temp_root)
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_path, config_path)
    finally:
        if temp_path.exists():
            temp_path.unlink()


def bind_stock_namespace(config: dict[str, Any], config_path: Path, *, timeout_seconds: float = 20.0) -> dict[str, Any]:
    """只绑定已存在的stock schema；所有数据库变更在一个可回滚事务内完成。"""
    import psycopg
    from psycopg import sql

    path, migration, migration_sha = _migration_source()
    expected_migration_digest = hashlib.sha256(
        f"{_MIGRATION_ID}:{migration_sha}:{_MIGRATION_LINEAGE}".encode("utf-8")
    ).hexdigest()
    try:
        project_uuid = uuid.UUID(config["application_identity"]["project_uuid"])
    except (KeyError, ValueError, TypeError) as error:
        raise BindingRefused("项目UUID配置无效") from error
    parameters = _configured_connection()
    parameters.update(connect_timeout=max(1, int(timeout_seconds)),
                      options=f"-c search_path=stock,pg_catalog -c statement_timeout={max(1, int(timeout_seconds * 1000))} -c lock_timeout=1000")
    try:
        with psycopg.connect(**parameters) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(20261027, 150102)")
                physical = identity._fetch_physical(cursor, deadline=identity.time.monotonic() + timeout_seconds)
                checks = identity._path_evidence(physical, config)
                if (physical["database"] != "stock_a" or physical["schema"] != "stock"
                        or physical["business_schema_exists"] is not True
                        or any(item.status != "PASS" for item in checks)
                        or physical["role_attributes"] is None
                        or physical["role_attributes"].get("rolsuper") is not True):
                    raise BindingRefused("固定数据库、已存在schema、G盘物理身份或显式管理角色未通过实测核验")
                cluster_id = physical["cluster_system_identifier"]
                if not isinstance(cluster_id, str) or not cluster_id.isdecimal() or int(cluster_id) <= 0:
                    raise BindingRefused("PostgreSQL物理簇标识无效")
                app = config["application_identity"]
                config_bound = app.get("deployment_applied") is True
                config_instance = app.get("deployment_instance_uuid")
                config_cluster = app.get("cluster_system_identifier")
                tables = _schema_tables(cursor)
                identity_present = physical["identity"] is not None
                migration_action = "IDEMPOTENT_REUSE" if identity_present else "APPLIED"
                ledger_present = physical["migration_table_exists"] and physical["migration_digest"] is not None
                if not identity_present and (ledger_present or tables):
                    raise BindingRefused("stock schema存在未登记对象或部分身份状态，拒绝接管或覆盖")
                if identity_present:
                    record = physical["identity"]
                    instance_uuid = uuid.UUID(record["instance_uuid"])
                    if (record["application_id"] != "STOCK_A" or record["project_uuid"] != str(project_uuid)
                            or record["cluster_system_identifier"] != cluster_id
                            or record["migration_lineage"] != _MIGRATION_LINEAGE
                            or physical["migration_digest"] != expected_migration_digest
                            or len(tables) != len(_EXPECTED_TABLES) or not _EXPECTED_TABLES.issubset(tables)):
                        raise BindingRefused("既有Stock身份、迁移或正式对象集合与本项目不一致")
                    if config_bound and (config_instance != str(instance_uuid) or config_cluster != cluster_id):
                        raise BindingRefused("数据库实例或簇身份与已部署配置冲突")
                else:
                    if config_bound:
                        raise BindingRefused("配置声明已部署但数据库身份表缺失，拒绝猜测补齐")
                    if tables:
                        raise BindingRefused("stock schema并非空schema，拒绝覆盖既有对象")
                    instance_uuid = uuid.uuid4()
                _validate_or_create_roles(cursor, sql)
                if not identity_present:
                    # migration内只有固定stock限定的CREATE TABLE/COMMENT/OWNER语句；从不创建数据库或schema。
                    cursor.execute(migration.decode("utf-8"))
                    cursor.execute("REVOKE CREATE ON SCHEMA stock FROM PUBLIC")
                    cursor.execute("GRANT USAGE, CREATE ON SCHEMA stock TO stock_a_owner")
                    cursor.execute("GRANT USAGE ON SCHEMA stock TO stock_a_migrator, stock_a_reader, stock_a_dev_writer, stock_a_prod_writer, stock_a_test_writer")
                    cursor.execute("REVOKE CREATE ON SCHEMA stock FROM stock_a_migrator, stock_a_reader, stock_a_dev_writer, stock_a_prod_writer, stock_a_test_writer")
                    cursor.execute("REVOKE ALL ON TABLE stock.installation_identity, stock.migration_ledger, stock.namespaces, stock.role_bindings FROM PUBLIC, stock_a_migrator, stock_a_reader, stock_a_dev_writer, stock_a_prod_writer, stock_a_test_writer")
                    cursor.execute("ALTER DEFAULT PRIVILEGES FOR ROLE stock_a_owner IN SCHEMA stock REVOKE ALL ON TABLES FROM PUBLIC")
                    _database_seed(cursor, project_uuid=project_uuid, instance_uuid=instance_uuid,
                                   cluster_id=cluster_id, migration_sha=migration_sha)
                _verify_seed(cursor, project_uuid=project_uuid, instance_uuid=instance_uuid,
                             cluster_id=cluster_id, migration_sha=migration_sha)
                role_checks = _verify_effective_roles(cursor)
            # Context manager commits here; failed SQL rolls the whole bootstrap back.
        _update_runtime_config(config_path, config, instance_uuid=instance_uuid,
                               cluster_id=cluster_id, migration_digest=expected_migration_digest)
        return {"status": "BOUND", "database": "stock_a", "schema": "stock",
                "project_uuid": str(project_uuid), "instance_uuid": str(instance_uuid),
                "cluster_system_identifier": cluster_id, "migration_lineage": _MIGRATION_LINEAGE,
                "migration_id": _MIGRATION_ID, "migration_source_sha256": migration_sha,
                "migration_action": migration_action,
                "database_comments": {"tables": 4, "columns": 16, "missing": 0},
                "migration_path": str(path), "roles": role_checks,
                "role_isolation_scope": "ROLE_ACL_CHECKS_VIA_SET_ROLE; independent login and RLS isolation not claimed",
                "database_transaction": "COMMITTED", "runtime_config": "UPDATED",
                "business_write_admission": "DENY_UNTIL_READBACK"}
    except BindingRefused:
        raise
    except Exception as error:
        # Driver messages can disclose DSNs, credentials or server details.
        raise BindingRefused(f"数据库绑定失败，底层错误类别为{type(error).__name__}；敏感异常文本已省略") from error
