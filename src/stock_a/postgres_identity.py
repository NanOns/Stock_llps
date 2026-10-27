"""PostgreSQL启动身份与只读存储就绪核验。"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
import ntpath
import os
from pathlib import Path
import re
import time
from typing import Any
import uuid


_ROLE_ATTRIBUTE_KEYS = ("rolsuper", "rolbypassrls", "owns_business_objects")
_MAX_TABLESPACES = 64
_MAX_MIGRATIONS = 4096
_MAX_CHECKS = 128
_MAX_INSPECTION_SECONDS = 30.0


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
    if actual is not None and expected is not None and actual == expected:
        return IdentityCheck(check, "PASS", str(expected), str(actual))
    return IdentityCheck(check, "REJECT", str(expected), str(actual), "实际值与固定项目身份不一致")


def _configuration_identity(config: dict[str, Any]) -> str:
    config_bytes = json.dumps(config, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
    return hashlib.sha256(config_bytes).hexdigest()


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
    deployment_applied = expected_app.get("deployment_applied") is True
    if not deployment_applied or not _valid_uuid(expected_instance):
        checks.append(IdentityCheck("deployment_instance_uuid", "BLOCKED", reason="配置仍为未绑定占位，须经显式启用任务绑定"))
    else:
        checks.append(_safe_compare(actual.get("instance_uuid"), expected_instance, "deployment_instance_uuid"))
    expected_cluster = expected_app.get("cluster_system_identifier")
    cluster_bound = deployment_applied and isinstance(expected_cluster, str) and expected_cluster.isdecimal()
    if not cluster_bound:
        checks.append(IdentityCheck("cluster_system_identifier", "BLOCKED", reason="配置仍为未绑定占位，须经显式启用任务绑定"))
        checks.append(IdentityCheck("physical_cluster_system_identifier", "BLOCKED", reason="缺少已绑定簇标识配置，物理簇暂不能与项目配置比较"))
    else:
        checks.append(_safe_compare(actual.get("cluster_system_identifier"), expected_cluster, "cluster_system_identifier"))
        checks.append(_safe_compare(actual.get("actual_cluster_system_identifier"), expected_cluster, "physical_cluster_system_identifier"))
    checks.append(_safe_compare(actual.get("cluster_system_identifier"), actual.get("actual_cluster_system_identifier"), "marked_vs_physical_cluster_system_identifier"))
    expected_lineage = expected_app.get("migration_lineage")
    checks.append(_safe_compare(actual.get("migration_lineage"), expected_lineage, "migration_lineage"))
    migration_digest = actual.get("migration_digest")
    checks.append(IdentityCheck(
        "migration_digest", "BLOCKED",
        actual=migration_digest if isinstance(migration_digest, str) and re.fullmatch(r"[0-9a-f]{64}", migration_digest) else None,
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
    attrs_safe = (isinstance(role_attrs, dict)
                  and all(k in role_attrs and type(role_attrs[k]) is bool and role_attrs[k] is False
                          for k in _ROLE_ATTRIBUTE_KEYS))
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


def _fetch_physical(cursor: Any, *, deadline: float) -> dict[str, Any]:
    _check_deadline(deadline)
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
    except Exception as error:
        _rollback_probe(cursor, "stock_a_identity_log_probe", error)
        raise
    else:
        cursor.execute("RELEASE SAVEPOINT stock_a_identity_log_probe")
    _check_deadline(deadline)
    cursor.execute("SELECT oid::bigint, spcname, pg_tablespace_location(oid) FROM pg_tablespace ORDER BY oid LIMIT %s", (_MAX_TABLESPACES + 1,))
    tablespaces = _bounded_rows(cursor, _MAX_TABLESPACES, "表空间数量超过核验预算")
    tablespaces = [{"oid": int(oid), "name": name, "location": location} for oid, name, location in tablespaces]
    system_identifier = None
    try:
        cursor.execute("SAVEPOINT stock_a_identity_cluster_probe")
        cursor.execute("SELECT system_identifier::text FROM pg_control_system()")
        got = cursor.fetchone()
        system_identifier = got[0] if got else None
    except Exception as error:
        _rollback_probe(cursor, "stock_a_identity_cluster_probe", error)
        raise
    else:
        cursor.execute("RELEASE SAVEPOINT stock_a_identity_cluster_probe")
    _check_deadline(deadline)
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
        _check_deadline(deadline)
        cursor.execute("SELECT migration_id, source_sha, lineage FROM stock_a.migration_ledger ORDER BY migration_id LIMIT %s", (_MAX_MIGRATIONS + 1,))
        rows = _bounded_rows(cursor, _MAX_MIGRATIONS, "迁移账本行数超过核验预算")
        if rows:
            migration_digest, lineages = _migration_ledger_digest(rows)
            if identity:
                if len(lineages) != 1 or next(iter(lineages)) != identity["migration_lineage"]:
                    migration_digest = None
    _check_deadline(deadline)
    cursor.execute("SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = session_user")
    role_row = cursor.fetchone()
    role_attributes = None if not role_row else {"rolsuper": bool(role_row[0]), "rolbypassrls": bool(role_row[1]), "owns_business_objects": None}
    # Ownership is verified by an explicit catalog query over persistent objects in the business schema.
    _check_deadline(deadline)
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


def _check_deadline(deadline: float) -> None:
    if time.monotonic() >= deadline:
        raise TimeoutError("PG只读核验超过整次耗时预算")


class _DeadlineCursor:
    """在每条固定查询前以剩余整次预算重设语句超时。"""

    def __init__(self, cursor: Any, deadline: float):
        self._cursor = cursor
        self._deadline = deadline

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> Any:
        _check_deadline(self._deadline)
        remaining_ms = max(1, math.ceil((self._deadline - time.monotonic()) * 1000))
        self._cursor.execute("SELECT set_config('statement_timeout', %s, true)", (str(remaining_ms),))
        _check_deadline(self._deadline)
        return self._cursor.execute(sql, params)

    def fetchone(self) -> Any:
        _check_deadline(self._deadline)
        return self._cursor.fetchone()

    def fetchmany(self, size: int) -> Any:
        _check_deadline(self._deadline)
        return self._cursor.fetchmany(size)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cursor, name)


def _bounded_rows(cursor: Any, maximum: int, message: str) -> list[Any]:
    rows = cursor.fetchmany(maximum + 1)
    if len(rows) > maximum:
        raise OverflowError(message)
    return rows


def _migration_ledger_digest(rows: list[tuple[Any, Any, Any]]) -> tuple[str, set[Any]]:
    """按migration_id/source_sha/lineage流式摘要，保留既有LF分隔语义。"""
    digest = hashlib.sha256()
    lineages: set[Any] = set()
    for index, (migration_id, source_sha, lineage) in enumerate(rows):
        if index:
            digest.update(b"\n")
        digest.update(f"{migration_id}:{source_sha}:{lineage}".encode("utf-8"))
        lineages.add(lineage)
    return digest.hexdigest(), lineages


def _rollback_probe(cursor: Any, savepoint: str, original: Exception) -> None:
    """恢复探针事务；清理失败时保留原异常作为主因。"""
    try:
        raw_cursor = getattr(cursor, "_cursor", cursor)
        raw_cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
    except Exception as cleanup_error:
        original.add_note(f"savepoint清理失败：{type(cleanup_error).__name__}")


def classify_inspection_error(error: Exception) -> str:
    """将参数、权限、不可用和工程错误稳定映射为CLI类别。"""
    code = getattr(error, "sqlstate", None)
    lower = type(error).__name__.casefold()
    if isinstance(error, TimeoutError) or code == "57014" or "timeout" in lower:
        return "TIMEOUT"
    if code in ("28P01", "28000"):
        return "AUTHENTICATION_FAILED"
    if isinstance(error, OverflowError) or (code and str(code).startswith("08")) or "connect" in lower or "operational" in lower:
        return "UNAVAILABLE"
    if isinstance(error, PermissionError) or code and (str(code).startswith("28") or str(code).startswith("42501")) or "permission" in lower:
        return "PERMISSION_DENIED"
    if isinstance(error, ValueError):
        return "INVALID_ARGUMENT"
    return "ENGINEERING_ERROR"


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
    except PermissionError:
        raise
    except FileNotFoundError:
        checks.append(IdentityCheck("data_directory", "BLOCKED", reason="PG实际数据目录不存在或不可解析"))
    except ValueError as error:
        checks.append(IdentityCheck("data_directory", "BLOCKED", reason=f"无法解析PG实际目录（{type(error).__name__}）"))
    except OSError:
        raise
    try:
        log = _normalize_pg_path(physical["log_directory"], base=physical["data_directory"])
        expected_log = Path(os.path.abspath(pg["log_directory"]))
        match_log = ntpath.normcase(str(log)) == ntpath.normcase(str(expected_log))
        checks.append(IdentityCheck("log_directory", "PASS" if match_log and _on_g_drive(log) else "REJECT", str(expected_log), str(log),
                                    None if match_log and _on_g_drive(log) else "日志目录解析后的最终路径未精确匹配G盘默认"))
    except PermissionError:
        raise
    except FileNotFoundError:
        checks.append(IdentityCheck("log_directory", "BLOCKED", reason="PG实际日志目录不存在或不可解析"))
    except ValueError as error:
        checks.append(IdentityCheck("log_directory", "BLOCKED", reason=f"无法解析PG实际日志目录（{type(error).__name__}）"))
    except OSError:
        raise
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
        except PermissionError:
            raise
        except FileNotFoundError:
            checks.append(IdentityCheck(f"tablespace:{item['name']}", "BLOCKED", reason="表空间路径不存在或不可解析"))
        except ValueError as error:
            checks.append(IdentityCheck(f"tablespace:{item['name']}", "BLOCKED", reason=f"无法解析表空间最终路径（{type(error).__name__}）"))
        except OSError:
            raise
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
        except PermissionError:
            raise
        except FileNotFoundError:
            checks.append(IdentityCheck("current_logfile", "BLOCKED", reason="实际活动日志文件不存在或不可解析"))
        except ValueError as error:
            checks.append(IdentityCheck("current_logfile", "BLOCKED", reason=f"无法解析活动日志文件最终路径（{type(error).__name__}）"))
        except OSError:
            raise
    return checks


def _fixed_connection_parameters(dsn: str, environ: Any, conninfo_to_dict: Any) -> dict[str, Any]:
    """解析并在连接前约束libpq目标；绝不把原始DSN交给驱动。"""
    values = conninfo_to_dict(dsn)
    if environ.get("PGSERVICE") or environ.get("PGSERVICEFILE"):
        raise ValueError("不支持libpq service配置")
    allowed = {"host", "hostaddr", "port", "dbname", "user", "password", "passfile", "connect_timeout", "application_name", "sslmode", "sslrootcert", "sslcert", "sslkey", "options"}
    if set(values) - allowed:
        raise ValueError("DSN含不支持的连接参数")
    for key in ("host", "hostaddr", "port", "dbname"):
        env_key = "PG" + {"dbname": "DATABASE"}.get(key, key.upper())
        if key.upper() in ("HOST", "HOSTADDR", "PORT", "DATABASE") and environ.get("PG" + key.upper()) is not None:
            env_key = "PG" + key.upper()
        env_value = environ.get(env_key)
        if env_value is not None and key in values and env_value != values[key]:
            raise ValueError("DSN与连接环境目标冲突")
        value = values.get(key, env_value)
        expected = {"host": "127.0.0.1", "hostaddr": "127.0.0.1", "port": "5432", "dbname": "stock_a"}[key]
        if value is not None and value != expected:
            raise ValueError("PG连接目标不等于合同固定的127.0.0.1:5432/stock_a")
    host = values.get("host", environ.get("PGHOST", "127.0.0.1"))
    hostaddr = values.get("hostaddr", environ.get("PGHOSTADDR", "127.0.0.1"))
    port = values.get("port", environ.get("PGPORT", "5432"))
    dbname = values.get("dbname", environ.get("PGDATABASE", "stock_a"))
    if host != "127.0.0.1" or hostaddr != "127.0.0.1" or port != "5432" or dbname != "stock_a":
        raise ValueError("PG连接目标不等于合同固定的127.0.0.1:5432/stock_a")
    result = {k: v for k, v in values.items() if k in allowed}
    result.update(host="127.0.0.1", hostaddr="127.0.0.1", port=5432, dbname="stock_a")
    if "user" not in result and environ.get("PGUSER"):
        result["user"] = environ["PGUSER"]
    if "password" not in result and environ.get("PGPASSWORD"):
        result["password"] = environ["PGPASSWORD"]
    if "passfile" not in result and environ.get("PGPASSFILE"):
        result["passfile"] = environ["PGPASSFILE"]
    return result


def inspect_postgres(config: dict[str, Any], *, timeout_seconds: float = 5.0) -> dict[str, Any]:
    """使用仅环境提供的DSN建立受限只读会话并返回无秘密的核验摘要。"""
    if type(timeout_seconds) not in (int, float) or not math.isfinite(timeout_seconds) or timeout_seconds <= 0 or timeout_seconds > _MAX_INSPECTION_SECONDS:
        raise ValueError("timeout_seconds必须在(0,30]内")
    dsn = os.environ.get("STOCK_A_PG_DSN")
    if not dsn:
        return {"status": "BLOCKED", "connection_attempted": False,
                "reason": "未提供安全环境变量STOCK_A_PG_DSN；未尝试连接", "checks": [], "physical": None}
    try:
        import psycopg
        from psycopg.conninfo import conninfo_to_dict
    except ImportError:
        return {"status": "UNAVAILABLE", "connection_attempted": False,
                "reason": "缺少PostgreSQL驱动psycopg；未尝试连接", "checks": [], "physical": None}
    try:
        connection_parameters = _fixed_connection_parameters(dsn, os.environ, conninfo_to_dict)
    except psycopg.ProgrammingError as error:
        # psycopg 将 conninfo 语法错误包装为 ProgrammingError；仅在此解析边界
        # 转成参数错误，查询阶段的同类异常仍由主核验分类器处理。
        return {"status": "INVALID_ARGUMENT", "connection_attempted": False,
                "reason": "DSN语法无效；未尝试连接", "cause_type": type(error).__name__,
                "sqlstate": getattr(error, "sqlstate", None), "checks": [], "physical": None}
    except (ValueError, TypeError):
        return {"status": "REJECTED", "connection_attempted": False,
                "reason": "连接目标或DSN参数不符合固定合同；未尝试连接", "checks": [], "physical": None}
    start = time.monotonic()
    deadline = start + min(float(timeout_seconds), _MAX_INSPECTION_SECONDS)
    try:
        connection_parameters.update(connect_timeout=max(1, math.ceil(timeout_seconds)), options=f"-c default_transaction_read_only=on -c statement_timeout={max(1, math.ceil(timeout_seconds * 1000))} -c lock_timeout=1000")
        with psycopg.connect(**connection_parameters, autocommit=True) as conn:
            info = conn.info
            if info.host != "127.0.0.1" or getattr(info, "hostaddr", "127.0.0.1") != "127.0.0.1" or info.port != 5432 or info.dbname != "stock_a":
                return {"status": "REJECTED", "connection_attempted": True,
                        "reason": "连接目标不等于合同固定的127.0.0.1:5432/stock_a", "checks": [],
                        "connection_target": {"host": info.host, "port": info.port, "database": info.dbname}, "physical": None}
            with conn.cursor() as cursor:
                cursor.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                physical = _fetch_physical(_DeadlineCursor(cursor, deadline), deadline=deadline)
            _check_deadline(deadline)
            checks = _path_evidence(physical, config)
            checks.extend([
                _safe_compare(physical["port"], 5432, "server_port"),
                _safe_compare(physical["server_address"], "127.0.0.1", "server_address"),
            ])
            deployment_applied = config["application_identity"].get("deployment_applied") is True
            checks.append(IdentityCheck(
                "deployment_applied", "PASS" if deployment_applied else "BLOCKED",
                expected="true", actual="true" if deployment_applied else "false",
                reason=None if deployment_applied else "配置仍标记未部署；业务读写与发布在显式部署绑定前拒绝"))
            if physical["identity"] is None:
                identity_checks = evaluate_identity(config, {
                    "database": physical["database"], "schema": physical["schema"],
                    "cluster_system_identifier": None,
                    "actual_cluster_system_identifier": physical["cluster_system_identifier"],
                    "session_user": physical["session_user"], "role_attributes": physical["role_attributes"],
                    "migration_digest": physical["migration_digest"], "namespace_binding": physical["namespace_binding"],
                })
                checks.extend(
                    IdentityCheck("migration_lineage", "BLOCKED", reason="stock_a.migration_ledger缺失或为空；迁移须由后续集中迁移实施")
                    if item.check == "migration_lineage" else
                    IdentityCheck(item.check, "BLOCKED", expected=item.expected, actual=item.actual,
                                  reason="installation_identity缺失；数据库标记身份尚不可比较")
                    if item.check in {"application_id", "project_uuid", "deployment_instance_uuid",
                                      "cluster_system_identifier", "marked_vs_physical_cluster_system_identifier"}
                    else item
                    for item in identity_checks
                )
                checks.append(IdentityCheck("installation_identity", "BLOCKED", reason="stock_a.installation_identity缺失或无可读单例；禁止本任务创建"))
            else:
                actual = {**physical["identity"], "database": physical["database"], "schema": physical["schema"],
                          "actual_cluster_system_identifier": physical["cluster_system_identifier"],
                          "session_user": physical["session_user"], "role_attributes": physical["role_attributes"],
                          "migration_digest": physical["migration_digest"], "namespace_binding": physical["namespace_binding"]}
                checks.extend(evaluate_identity(config, actual))
            with conn.cursor() as cursor:
                _check_deadline(deadline)
                cursor.execute("COMMIT")
            physical_summary = {k: v for k, v in physical.items() if k not in ("identity",)}
            physical_summary["identity"] = "PRESENT" if physical["identity"] else "MISSING"
            physical_summary["identity_record"] = physical["identity"]
            physical_summary["tablespace_count"] = len(physical["tablespaces"])
            if len(checks) > _MAX_CHECKS:
                raise OverflowError("PG核验检查项超过输出预算")
            statuses = {item.status for item in checks}
            overall = "READY" if statuses == {"PASS"} else ("REJECTED" if "REJECT" in statuses else "BLOCKED")
            result = {"status": overall, "connection_attempted": True,
                      "elapsed_seconds": round(time.monotonic() - start, 6),
                      "transaction": "REPEATABLE_READ_READ_ONLY", "checks": [item.__dict__ for item in checks],
                      "physical": physical_summary, "deployment_applied": config["application_identity"].get("deployment_applied") is True}
            if overall == "READY":
                result.update({"inspection_context": str(uuid.uuid4()),
                               "configuration_identity": _configuration_identity(config),
                               "admission_actions": ["WRITE", "PUBLISH"]})
            return result
    except Exception as error:
        # Do not include exception text: drivers can embed DSNs or server-provided private details.
        name = type(error).__name__
        code = getattr(error, "sqlstate", None)
        status = classify_inspection_error(error)
        return {"status": status, "connection_attempted": True, "reason": "只读核验未完成；底层异常文本已省略",
                "cause_type": name, "sqlstate": code, "elapsed_seconds": round(time.monotonic() - start, 6),
                "checks": [], "physical": None}


def write_admission(
    inspection: dict[str, Any], *, requested_context: str | None = None,
    requested_configuration: dict[str, Any] | None = None, requested_action: str | None = None,
) -> tuple[bool, str]:
    """所有后续PG写入/发布调用方共用的失败关闭准入判定。"""
    if not isinstance(inspection, dict):
        return False, "PostgreSQL核验结果格式无效；拒绝新项目写入与发布"
    required = {"current_database", "current_schema", "application_id", "project_uuid", "deployment_instance_uuid",
                "cluster_system_identifier", "physical_cluster_system_identifier", "marked_vs_physical_cluster_system_identifier",
                "migration_lineage", "migration_digest", "session_user",
                "role_attributes", "role_namespace_binding", "data_directory", "wal_directory", "log_directory",
                "temp_tablespaces", "server_port", "server_address", "deployment_applied", "current_logfile"}
    checks = inspection.get("checks")
    if not isinstance(checks, list) or len(checks) > _MAX_CHECKS:
        return False, "PG核验检查项缺失或超过输出预算；拒绝新项目写入与发布"
    by_name = {item.get("check"): item for item in checks if isinstance(item, dict)}
    physical = inspection.get("physical")
    physical_required = {
        "database", "schema", "session_user", "data_directory", "config_file", "log_directory",
        "temp_tablespaces", "port", "server_version_num", "server_address", "logging_collector",
        "current_logfile", "tablespaces", "tablespace_count", "cluster_system_identifier", "identity", "identity_record", "migration_digest",
        "role_attributes", "namespace_binding",
    }
    tablespaces = physical.get("tablespaces") if isinstance(physical, dict) else None
    tablespace_names: list[str] = []
    if isinstance(tablespaces, list) and 0 < len(tablespaces) <= _MAX_TABLESPACES:
        for tablespace in tablespaces:
            if (not isinstance(tablespace, dict)
                    or not isinstance(tablespace.get("name"), str) or not tablespace["name"]
                    or type(tablespace.get("oid")) is not int
                    or not isinstance(tablespace.get("location"), str)):
                tablespace_names = []
                break
            tablespace_names.append(tablespace["name"])
    if len(set(tablespace_names)) != len(tablespace_names):
        tablespace_names = []
    tablespace_summary_complete = (
        isinstance(tablespaces, list) and 0 < len(tablespace_names) == len(tablespaces) <= _MAX_TABLESPACES
        and isinstance(physical, dict) and type(physical.get("tablespace_count")) is int
        and physical.get("tablespace_count") == len(tablespaces)
    )
    identity_record = physical.get("identity_record") if isinstance(physical, dict) else None
    config_app = requested_configuration.get("application_identity") if isinstance(requested_configuration, dict) else None
    config_pg = requested_configuration.get("postgresql") if isinstance(requested_configuration, dict) else None

    def canonical_path(value: Any, base: Any = None) -> str | None:
        if not isinstance(value, str) or not value:
            return None
        candidate = value if ntpath.isabs(value) else ntpath.join(str(base), value) if isinstance(base, str) else None
        return ntpath.normcase(ntpath.normpath(candidate)) if candidate and ntpath.isabs(candidate) else None

    physical_values_complete = (
        isinstance(physical, dict)
        and all(isinstance(physical.get(key), str) and bool(physical[key]) for key in (
            "database", "schema", "session_user", "data_directory", "config_file", "log_directory", "server_address",
        ))
        and type(physical.get("port")) is int and physical["port"] > 0
        and type(physical.get("server_version_num")) is int and physical["server_version_num"] > 0
        and physical.get("identity") == "PRESENT"
        and isinstance(identity_record, dict)
        and all(isinstance(identity_record.get(key), str) and bool(identity_record[key]) for key in (
            "application_id", "project_uuid", "instance_uuid", "cluster_system_identifier", "migration_lineage",
        ))
        and physical.get("namespace_binding") == "MATCH"
        and isinstance(physical.get("role_attributes"), dict)
        and all(key in physical["role_attributes"] and type(physical["role_attributes"][key]) is bool
                for key in _ROLE_ATTRIBUTE_KEYS)
        and all(physical["role_attributes"][key] is False for key in _ROLE_ATTRIBUTE_KEYS)
        and physical.get("database") == "stock_a" and physical.get("schema") == "stock_a"
        and physical.get("server_address") == "127.0.0.1" and physical.get("port") == 5432
        and isinstance(physical.get("cluster_system_identifier"), str)
        and identity_record.get("cluster_system_identifier") == physical.get("cluster_system_identifier")
        and isinstance(physical.get("migration_digest"), str)
        and re.fullmatch(r"[0-9a-f]{64}", physical.get("migration_digest", "")) is not None
        and isinstance(config_app, dict) and config_app.get("deployment_applied") is True
        and isinstance(config_app.get("roles"), list)
        and config_app.get("application_id") == "STOCK_A"
        and _valid_uuid(config_app.get("project_uuid"))
        and _valid_uuid(config_app.get("deployment_instance_uuid"))
        and isinstance(config_app.get("cluster_system_identifier"), str)
        and config_app.get("cluster_system_identifier", "").isdecimal()
        and isinstance(config_app.get("migration_lineage"), str) and bool(config_app.get("migration_lineage"))
        and isinstance(config_pg, dict)
        and all(canonical_path(config_pg.get(key)) is not None for key in ("data_directory", "wal_directory", "log_directory"))
        and all(ntpath.splitdrive(canonical_path(config_pg.get(key)) or "")[0].casefold() == "g:" for key in ("data_directory", "wal_directory", "log_directory"))
        and canonical_path(config_pg.get("data_directory")) == canonical_path(physical.get("data_directory") if isinstance(physical, dict) else None)
        and canonical_path(config_pg.get("wal_directory")) == canonical_path(ntpath.join(physical.get("data_directory", ""), "pg_wal") if isinstance(physical, dict) else None)
        and canonical_path(config_pg.get("log_directory")) == canonical_path(physical.get("log_directory") if isinstance(physical, dict) else None, physical.get("data_directory") if isinstance(physical, dict) else None)
        and config_pg.get("temp_tablespaces") == physical.get("temp_tablespaces")
        and identity_record.get("application_id") == "STOCK_A"
        and identity_record.get("project_uuid") == config_app.get("project_uuid")
        and identity_record.get("instance_uuid") == config_app.get("deployment_instance_uuid")
        and identity_record.get("cluster_system_identifier") == config_app.get("cluster_system_identifier")
        and identity_record.get("migration_lineage") == config_app.get("migration_lineage")
        and physical.get("session_user") in config_app.get("roles", [])
    )

    def check_actual_is(name: str, expected: Any) -> bool:
        item = by_name.get(name)
        return isinstance(item, dict) and item.get("actual") == (None if expected is None else str(expected))

    # PASS项必须逐值反映本次摘要。否则一份手工拼出的READY检查不能覆盖冲突摘要。
    scalar_actuals = {
        "current_database": physical.get("database") if isinstance(physical, dict) else None,
        "current_schema": physical.get("schema") if isinstance(physical, dict) else None,
        "application_id": identity_record.get("application_id") if isinstance(identity_record, dict) else None,
        "project_uuid": identity_record.get("project_uuid") if isinstance(identity_record, dict) else None,
        "deployment_instance_uuid": identity_record.get("instance_uuid") if isinstance(identity_record, dict) else None,
        "cluster_system_identifier": identity_record.get("cluster_system_identifier") if isinstance(identity_record, dict) else None,
        "physical_cluster_system_identifier": physical.get("cluster_system_identifier") if isinstance(physical, dict) else None,
        "marked_vs_physical_cluster_system_identifier": identity_record.get("cluster_system_identifier") if isinstance(identity_record, dict) else None,
        "migration_lineage": identity_record.get("migration_lineage") if isinstance(identity_record, dict) else None,
        "migration_digest": physical.get("migration_digest") if isinstance(physical, dict) else None,
        "session_user": physical.get("session_user") if isinstance(physical, dict) else None,
        "role_attributes": "安全属性已核验",
        "role_namespace_binding": physical.get("namespace_binding") if isinstance(physical, dict) else None,
        "temp_tablespaces": "未设置temp_tablespaces" if isinstance(physical, dict) and physical.get("temp_tablespaces") == "" else physical.get("temp_tablespaces") if isinstance(physical, dict) else None,
        "server_port": physical.get("port") if isinstance(physical, dict) else None,
        "server_address": physical.get("server_address") if isinstance(physical, dict) else None,
        "deployment_applied": "true",
        "current_logfile": None,
    }
    values_consistent = all(check_actual_is(name, value) for name, value in scalar_actuals.items() if name != "current_logfile")
    # 路径检查由既有最终路径解析生成；比对Windows规范形式以兼容大小写和分隔符。
    if isinstance(physical, dict):
        # 路径派生会调用 ntpath.join；先拒绝不完整摘要和错误类型，避免把输入问题变成异常。
        if not physical_values_complete or not tablespace_summary_complete:
            return False, "PostgreSQL物理路径或表空间摘要无效；拒绝新项目写入与发布"
        path_actuals = {
            "data_directory": (physical.get("data_directory"), None),
            "wal_directory": (ntpath.join(physical.get("data_directory", ""), "pg_wal"), None),
            "log_directory": (physical.get("log_directory"), physical.get("data_directory")),
            "current_logfile": (physical.get("current_logfile"), physical.get("data_directory")),
        }
        values_consistent = values_consistent and all(
            isinstance(by_name.get(name), dict)
            and canonical_path(by_name[name].get("actual")) == canonical_path(value, base)
            for name, (value, base) in path_actuals.items()
        )
        configured_paths = {
            "data_directory": config_pg.get("data_directory") if isinstance(config_pg, dict) else None,
            "wal_directory": config_pg.get("wal_directory") if isinstance(config_pg, dict) else None,
            "log_directory": config_pg.get("log_directory") if isinstance(config_pg, dict) else None,
        }
        for name, configured in configured_paths.items():
            item = by_name.get(name)
            values_consistent = values_consistent and isinstance(item, dict) and canonical_path(item.get("expected")) == canonical_path(configured)
        temp_item = by_name.get("temp_tablespaces")
        temp_expected = "数据库默认表空间" if physical.get("temp_tablespaces") == "" else physical.get("temp_tablespaces")
        values_consistent = values_consistent and isinstance(temp_item, dict) and temp_item.get("expected") == temp_expected
        for name in ("current_logfile",):
            item = by_name.get(name)
            actual_path = canonical_path(item.get("actual")) if isinstance(item, dict) else None
            values_consistent = values_consistent and isinstance(item, dict) and item.get("expected") == "G盘" and actual_path is not None and ntpath.splitdrive(actual_path)[0].casefold() == "g:"
        for tablespace in tablespaces if isinstance(tablespaces, list) else []:
            name = tablespace.get("name") if isinstance(tablespace, dict) else None
            location = (physical.get("data_directory") if name == "pg_default" else
                        ntpath.join(physical.get("data_directory", ""), "global") if name == "pg_global" else
                        tablespace.get("location") if isinstance(tablespace, dict) else None)
            item = by_name.get(f"tablespace:{name}") if isinstance(name, str) else None
            actual_path = canonical_path(item.get("actual")) if isinstance(item, dict) else None
            values_consistent = values_consistent and isinstance(item, dict) and item.get("expected") == "G盘" and canonical_path(item.get("actual")) == canonical_path(location) and actual_path is not None and ntpath.splitdrive(actual_path)[0].casefold() == "g:"
    dynamic_required = {f"tablespace:{name}" for name in tablespace_names}
    required.update(dynamic_required)
    actions = inspection.get("admission_actions")
    try:
        requested_identity = _configuration_identity(requested_configuration) if isinstance(requested_configuration, dict) else None
    except (TypeError, ValueError):
        requested_identity = None
    if (inspection.get("status") != "READY" or inspection.get("connection_attempted") is not True
            or inspection.get("deployment_applied") is not True
            or not isinstance(inspection.get("inspection_context"), str) or not inspection["inspection_context"]
            or requested_context != inspection.get("inspection_context")
            or requested_action not in ("WRITE", "PUBLISH")
            or not isinstance(actions, list) or requested_action not in actions
            or not isinstance(physical, dict) or not physical_required.issubset(physical)
            or not isinstance(physical.get("current_logfile"), str) or not physical["current_logfile"]
            or physical.get("logging_collector") is not True or not tablespace_summary_complete
            or not physical_values_complete or not dynamic_required or not values_consistent
            or not isinstance(inspection.get("configuration_identity"), str)
            or requested_identity != inspection.get("configuration_identity")
            or set(by_name) != required
            or len(by_name) != len(checks)
            or any(item.get("status") != "PASS" or item.get("actual") is None for item in by_name.values())):
        return False, "PostgreSQL身份或物理存储尚未全部核验通过；拒绝新项目写入与发布"
    return True, "PostgreSQL身份与物理存储核验通过"
