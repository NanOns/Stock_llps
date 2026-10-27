"""PostgreSQL启动身份与只读存储就绪核验。"""
from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import re
import ntpath
import time
from typing import Any
import uuid


@dataclass(frozen=True)
class IdentityCheck:
    """一个不含秘密的PG身份或部署条件检查结果。"""

    check: str
    status: str
    expected: str | None = None
    actual: str | None = None
    reason: str | None = None


def _valid_uuid(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        return str(uuid.UUID(value)) == value.lower()
    except (ValueError, AttributeError):
        return False


def _normalize_pg_path(value: str, *, base: str | None = None) -> Path:
    path = Path(value)
    if not path.is_absolute() and base:
        path = Path(base) / path
    return path.resolve(strict=True)


def _on_g_drive(path: Path) -> bool:
    return path.drive.casefold() == "g:"


def _safe_compare(actual: Any, expected: Any, check: str) -> IdentityCheck:
    if actual == expected:
        return IdentityCheck(check, "PASS", str(expected), str(actual))
    return IdentityCheck(check, "REJECT", str(expected), str(actual), "实际值与固定项目身份不一致")


def evaluate_identity(
    config: dict[str, Any],
    actual: dict[str, Any],
) -> list[IdentityCheck]:
    """按固定项目合同评估已采集字段；不执行IO，也不允许缺失值通过。"""
    expected_app = config["application_identity"]
    checks: list[IdentityCheck] = []
    checks.append(_safe_compare(actual.get("database"), "stock_a", "current_database"))
    checks.append(_safe_compare(actual.get("schema"), "stock_a", "current_schema"))
    checks.append(_safe_compare(actual.get("application_id"), "STOCK_A", "application_id"))
    checks.append(_safe_compare(actual.get("project_uuid"), expected_app.get("project_uuid"), "project_uuid"))
    expected_instance = expected_app.get("deployment_instance_uuid")
    if not expected_app.get("deployment_applied") or not _valid_uuid(expected_instance):
        checks.append(IdentityCheck("deployment_instance_uuid", "BLOCKED", reason="配置仍为未绑定占位，须经显式启用任务绑定"))
    else:
        checks.append(_safe_compare(actual.get("instance_uuid"), expected_instance, "deployment_instance_uuid"))
    expected_cluster = expected_app.get("cluster_system_identifier")
    if not expected_app.get("deployment_applied") or not isinstance(expected_cluster, str) or not expected_cluster.isdecimal():
        checks.append(IdentityCheck("cluster_system_identifier", "BLOCKED", reason="配置仍为未绑定占位，须经显式启用任务绑定"))
    else:
        checks.append(_safe_compare(actual.get("cluster_system_identifier"), expected_cluster, "cluster_system_identifier"))
    expected_lineage = expected_app.get("migration_lineage")
    checks.append(_safe_compare(actual.get("migration_lineage"), expected_lineage, "migration_lineage"))
    migration_digest = actual.get("migration_digest")
    checks.append(IdentityCheck(
        "migration_digest", "BLOCKED",
        actual="已读取" if isinstance(migration_digest, str) and re.fullmatch(r"[0-9a-f]{64}", migration_digest) else None,
        reason="runtime.defaults尚未登记经批准的迁移摘要，不能把存在账本等同精确版本匹配",
    ))
    role = actual.get("session_user")
    allowed_roles = expected_app.get("roles", [])
    checks.append(IdentityCheck(
        "session_user", "PASS" if isinstance(role, str) and role in allowed_roles else "REJECT",
        expected="配置登记角色之一", actual=role,
        reason=None if isinstance(role, str) and role in allowed_roles else "session_user不在配置登记角色中",
    ))
    role_attrs = actual.get("role_attributes")
    attrs_safe = isinstance(role_attrs, dict) and not any(role_attrs.get(k) is True for k in ("rolsuper", "rolbypassrls", "owns_business_objects"))
    checks.append(IdentityCheck("role_attributes", "PASS" if attrs_safe else "REJECT", actual="安全属性已核验" if attrs_safe else None,
                                 reason=None if attrs_safe else "角色属性缺失或包含超级用户、BYPASSRLS或业务对象所有权"))
    namespace = actual.get("namespace_binding")
    checks.append(IdentityCheck("role_namespace_binding", "PASS" if namespace == "MATCH" else "BLOCKED",
                                 actual=namespace if isinstance(namespace, str) else None,
                                 reason=None if namespace == "MATCH" else "现行runtime.defaults未声明namespace；未执行角色绑定表DDL"))
    return checks


def _query_one(cursor: Any, sql: str, params: tuple[Any, ...] = ()) -> Any:
    cursor.execute(sql, params)
    row = cursor.fetchone()
    return None if row is None else row[0]


def _fetch_physical(cursor: Any) -> dict[str, Any]:
    cursor.execute("""SELECT current_database(), current_schema(), session_user,
       current_setting('data_directory'), current_setting('config_file'),
       current_setting('log_directory'), current_setting('temp_tablespaces'),
       current_setting('port'), current_setting('server_version_num'),
       current_setting('logging_collector'), inet_server_addr()::text""")
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError("PG未返回基础运行身份")
    database, schema, session_user, data_dir, config_file, log_dir, temp_tablespaces, port, version, logging_collector, server_address = row
    current_logfile = None
    try:
        cursor.execute("SAVEPOINT stock_a_identity_log_probe")
        cursor.execute("SELECT pg_current_logfile()")
        got = cursor.fetchone()
        current_logfile = got[0] if got else None
    except Exception:
        cursor.execute("ROLLBACK TO SAVEPOINT stock_a_identity_log_probe")
    finally:
        cursor.execute("RELEASE SAVEPOINT stock_a_identity_log_probe")
    cursor.execute("SELECT oid::bigint, spcname, pg_tablespace_location(oid) FROM pg_tablespace ORDER BY oid")
    tablespaces = [{"oid": int(oid), "name": name, "location": location} for oid, name, location in cursor.fetchall()]
    system_identifier = None
    try:
        cursor.execute("SAVEPOINT stock_a_identity_cluster_probe")
        cursor.execute("SELECT system_identifier::text FROM pg_control_system()")
        got = cursor.fetchone()
        system_identifier = got[0] if got else None
    except Exception:
        cursor.execute("ROLLBACK TO SAVEPOINT stock_a_identity_cluster_probe")
        system_identifier = None
    finally:
        cursor.execute("RELEASE SAVEPOINT stock_a_identity_cluster_probe")
    cursor.execute("SELECT to_regclass('stock_a.installation_identity')::text, to_regclass('stock_a.migration_ledger')::text")
    identity_table, migration_table = cursor.fetchone()
    identity = None
    if identity_table:
        cursor.execute("SELECT application_id, project_uuid::text, instance_uuid::text, cluster_system_identifier, migration_lineage FROM stock_a.installation_identity WHERE singleton IS TRUE")
        vals = cursor.fetchone()
        if vals:
            identity = dict(zip(("application_id", "project_uuid", "instance_uuid", "cluster_system_identifier", "migration_lineage"), vals))
    migration_digest = None
    if migration_table:
        cursor.execute("SELECT migration_id, source_sha, lineage FROM stock_a.migration_ledger ORDER BY migration_id")
        rows = cursor.fetchall()
        if rows:
            import hashlib
            migration_digest = hashlib.sha256("\n".join(f"{r[0]}:{r[1]}:{r[2]}" for r in rows).encode("utf-8")).hexdigest()
            if identity:
                lineages = {r[2] for r in rows}
                if len(lineages) != 1 or next(iter(lineages)) != identity["migration_lineage"]:
                    migration_digest = None
    cursor.execute("SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = session_user")
    role_row = cursor.fetchone()
    role_attributes = None if not role_row else {"rolsuper": bool(role_row[0]), "rolbypassrls": bool(role_row[1]), "owns_business_objects": None}
    # Ownership is verified by an explicit catalog query over persistent objects in the business schema.
    cursor.execute("""SELECT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
       WHERE n.nspname='stock_a' AND c.relowner=(SELECT oid FROM pg_roles WHERE rolname=session_user))""")
    if role_attributes is not None:
        role_attributes["owns_business_objects"] = bool(cursor.fetchone()[0])
    # Role→namespace needs an explicitly bound runtime namespace, absent from the current contract.
    return {
        "database": database, "schema": schema, "session_user": session_user,
        "data_directory": data_dir, "config_file": config_file, "log_directory": log_dir,
        "temp_tablespaces": temp_tablespaces, "port": int(port), "server_version_num": int(version),
        "server_address": server_address,
        "logging_collector": logging_collector == "on", "current_logfile": current_logfile,
        "tablespaces": tablespaces, "cluster_system_identifier": system_identifier,
        "identity_table_exists": bool(identity_table), "migration_table_exists": bool(migration_table),
        "identity": identity, "migration_digest": migration_digest, "role_attributes": role_attributes,
        "namespace_binding": "UNBOUND",
    }


def _path_evidence(physical: dict[str, Any], config: dict[str, Any]) -> list[IdentityCheck]:
    pg = config["postgresql"]
    checks: list[IdentityCheck] = []
    data_value = physical["data_directory"]
    try:
        data = _normalize_pg_path(data_value)
        expected_data = Path(os.path.abspath(pg["data_directory"]))
        match_data = ntpath.normcase(str(data)) == ntpath.normcase(str(expected_data))
        checks.append(IdentityCheck("data_directory", "PASS" if match_data and _on_g_drive(data) else "REJECT",
                                    str(expected_data), str(data), None if match_data and _on_g_drive(data) else "PG数据目录解析后的最终路径未精确匹配G:/Postgres/data"))
        wal = data / "pg_wal"
        resolved_wal = _normalize_pg_path(str(wal))
        expected_wal = Path(os.path.abspath(pg["wal_directory"]))
        match_wal = ntpath.normcase(str(resolved_wal)) == ntpath.normcase(str(expected_wal))
        checks.append(IdentityCheck("wal_directory", "PASS" if match_wal and _on_g_drive(resolved_wal) else "REJECT", str(expected_wal), str(resolved_wal),
                                    None if match_wal and _on_g_drive(resolved_wal) else "WAL目录解析后的最终路径未精确匹配G盘默认"))
    except (OSError, ValueError) as error:
        checks.append(IdentityCheck("data_directory", "BLOCKED", reason=f"无法解析PG实际目录（{type(error).__name__}）"))
    try:
        log = _normalize_pg_path(physical["log_directory"], base=physical["data_directory"])
        expected_log = Path(os.path.abspath(pg["log_directory"]))
        match_log = ntpath.normcase(str(log)) == ntpath.normcase(str(expected_log))
        checks.append(IdentityCheck("log_directory", "PASS" if match_log and _on_g_drive(log) else "REJECT", str(expected_log), str(log),
                                    None if match_log and _on_g_drive(log) else "日志目录解析后的最终路径未精确匹配G盘默认"))
    except (OSError, ValueError) as error:
        checks.append(IdentityCheck("log_directory", "BLOCKED", reason=f"无法解析PG实际日志目录（{type(error).__name__}）"))
    for item in physical["tablespaces"]:
        if item["name"] == "pg_default":
            location = str(data_value)
        elif item["name"] == "pg_global":
            location = str(Path(data_value) / "global")
        else:
            location = item["location"]
        try:
            resolved = _normalize_pg_path(location)
            status = "PASS" if _on_g_drive(resolved) else "REJECT"
            checks.append(IdentityCheck(f"tablespace:{item['name']}", status, "G盘", str(resolved), None if status == "PASS" else "表空间最终路径不在G盘"))
        except (OSError, ValueError) as error:
            checks.append(IdentityCheck(f"tablespace:{item['name']}", "BLOCKED", reason=f"无法解析表空间最终路径（{type(error).__name__}）"))
    temp = physical["temp_tablespaces"].strip()
    if temp != pg["temp_tablespaces"]:
        checks.append(IdentityCheck("temp_tablespaces", "REJECT", expected=pg["temp_tablespaces"] or "默认表空间", actual=temp, reason="实际临时表空间设置与部署配置不一致"))
    else:
        checks.append(IdentityCheck("temp_tablespaces", "PASS", expected="数据库默认表空间", actual="未设置temp_tablespaces"))
    if not physical["logging_collector"]:
        checks.append(IdentityCheck("current_logfile", "BLOCKED", reason="logging_collector未启用，无法证明实际活动日志文件路径"))
    elif not physical["current_logfile"]:
        checks.append(IdentityCheck("current_logfile", "BLOCKED", reason="未返回当前活动日志文件路径"))
    else:
        try:
            active_log = _normalize_pg_path(physical["current_logfile"], base=physical["data_directory"])
            status = "PASS" if _on_g_drive(active_log) else "REJECT"
            checks.append(IdentityCheck("current_logfile", status, "G盘", str(active_log), None if status == "PASS" else "实际活动日志文件不在G盘"))
        except (OSError, ValueError) as error:
            checks.append(IdentityCheck("current_logfile", "BLOCKED", reason=f"无法解析活动日志文件最终路径（{type(error).__name__}）"))
    return checks


def inspect_postgres(config: dict[str, Any], *, timeout_seconds: float = 5.0) -> dict[str, Any]:
    """使用仅环境提供的DSN建立受限只读会话并返回无秘密的核验摘要。"""
    if type(timeout_seconds) not in (int, float) or timeout_seconds <= 0 or timeout_seconds > 30:
        raise ValueError("timeout_seconds必须在(0,30]内")
    dsn = os.environ.get("STOCK_A_PG_DSN")
    if not dsn:
        return {"status": "BLOCKED", "connection_attempted": False,
                "reason": "未提供安全环境变量STOCK_A_PG_DSN；未尝试连接", "checks": [], "physical": None}
    try:
        import psycopg
    except ImportError:
        return {"status": "UNAVAILABLE", "connection_attempted": False,
                "reason": "缺少PostgreSQL驱动psycopg；未尝试连接", "checks": [], "physical": None}
    start = time.monotonic()
    try:
        with psycopg.connect(dsn, connect_timeout=max(1, int(timeout_seconds)), options=f"-c default_transaction_read_only=on -c statement_timeout={int(timeout_seconds * 1000)} -c lock_timeout=1000", autocommit=True) as conn:
            info = conn.info
            if info.host != "127.0.0.1" or info.port != 5432 or info.dbname != "stock_a":
                return {"status": "REJECTED", "connection_attempted": True,
                        "reason": "连接目标不等于合同固定的127.0.0.1:5432/stock_a", "checks": [],
                        "connection_target": {"host": info.host, "port": info.port, "database": info.dbname}, "physical": None}
            with conn.cursor() as cursor:
                cursor.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                physical = _fetch_physical(cursor)
            checks = _path_evidence(physical, config)
            checks.extend([
                _safe_compare(physical["port"], 5432, "server_port"),
                _safe_compare(physical["server_address"], "127.0.0.1", "server_address"),
            ])
            if not config["application_identity"].get("deployment_applied"):
                checks.extend([
                    IdentityCheck("deployment_applied", "BLOCKED", expected="true", actual="false", reason="配置仍标记未部署；业务读写与发布在显式部署绑定前拒绝"),
                    IdentityCheck("deployment_instance_uuid", "BLOCKED", reason="实例UUID仍为显式启用时绑定占位"),
                    IdentityCheck("cluster_system_identifier", "BLOCKED", reason="簇标识仍为显式启用时绑定占位"),
                ])
            if physical["identity"] is None:
                checks.extend(evaluate_identity(config, {
                    "database": physical["database"], "schema": physical["schema"],
                    "cluster_system_identifier": physical["cluster_system_identifier"],
                    "session_user": physical["session_user"], "role_attributes": physical["role_attributes"],
                    "migration_digest": physical["migration_digest"], "namespace_binding": physical["namespace_binding"],
                }))
                checks.extend([
                    IdentityCheck("installation_identity", "BLOCKED", reason="stock_a.installation_identity缺失或无可读单例；禁止本任务创建"),
                    IdentityCheck("migration_lineage", "BLOCKED", reason="stock_a.migration_ledger缺失或为空；迁移须由后续集中迁移实施"),
                ])
            else:
                actual = {**physical["identity"], "database": physical["database"], "schema": physical["schema"],
                          "cluster_system_identifier": physical["cluster_system_identifier"],
                          "session_user": physical["session_user"], "role_attributes": physical["role_attributes"],
                          "migration_digest": physical["migration_digest"], "namespace_binding": physical["namespace_binding"]}
                checks.extend(evaluate_identity(config, actual))
            with conn.cursor() as cursor:
                cursor.execute("COMMIT")
            physical_summary = {k: v for k, v in physical.items() if k not in ("identity",)}
            physical_summary["identity"] = "PRESENT" if physical["identity"] else "MISSING"
            statuses = {item.status for item in checks}
            overall = "READY" if statuses == {"PASS"} else ("REJECTED" if "REJECT" in statuses else "BLOCKED")
            return {"status": overall, "connection_attempted": True,
                    "elapsed_seconds": round(time.monotonic() - start, 6),
                    "transaction": "REPEATABLE_READ_READ_ONLY", "checks": [item.__dict__ for item in checks],
                    "physical": physical_summary}
    except Exception as error:
        # Do not include exception text: drivers can embed DSNs or server-provided private details.
        name = type(error).__name__
        code = getattr(error, "sqlstate", None)
        lower = name.casefold()
        if code == "28P01" or code == "28000":
            status = "AUTHENTICATION_FAILED"
        elif code and str(code).startswith("28") or (code and str(code).startswith("42501")) or "permission" in lower:
            status = "PERMISSION_DENIED"
        elif code == "57014" or "timeout" in lower:
            status = "TIMEOUT"
        elif code and str(code).startswith("08") or "connect" in lower or "operational" in lower:
            status = "UNAVAILABLE"
        else:
            status = "ENGINEERING_ERROR"
        return {"status": status, "connection_attempted": True, "reason": "只读核验未完成；底层异常文本已省略",
                "cause_type": name, "sqlstate": code, "elapsed_seconds": round(time.monotonic() - start, 6),
                "checks": [], "physical": None}


def write_admission(inspection: dict[str, Any]) -> tuple[bool, str]:
    """所有后续PG写入/发布调用方共用的失败关闭准入判定。"""
    if inspection.get("status") != "READY":
        return False, "PostgreSQL身份或物理存储尚未全部核验通过；拒绝新项目写入与发布"
    return True, "PostgreSQL身份与物理存储核验通过"
