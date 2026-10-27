"""PostgreSQL业务登录及迁移身份核验。"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import stat
import time
from typing import Any
import uuid


_MAX_MIGRATIONS = 4096
_MAX_CHECKS = 128
_MAX_INSPECTION_SECONDS = 30.0
_PRIVATE_CREDENTIALS_PATH = Path("G:/codex work/Stock A/config/private/postgresql.credentials.json")
_PRIVATE_CONFIG_ROOT = Path("G:/codex work/Stock A/config/private")
_MAX_CREDENTIAL_FILE_BYTES = 8192
_CREDENTIAL_SCHEMA = "STOCK-A-PG-CREDENTIALS/R1"
_BUSINESS_SCHEMA = "stock"


class _CredentialContractError(ValueError):
    """凭据合同拒绝的安全原因码；不携带路径或凭据字段值。"""

    def __init__(self, reason_code: str, *, missing_fields: tuple[str, ...] = (),
                 invalid_fields: tuple[str, ...] = (), extra_field_count: int = 0) -> None:
        self.reason_code = reason_code
        self.missing_fields = missing_fields
        self.invalid_fields = invalid_fields
        self.extra_field_count = extra_field_count
        super().__init__(reason_code)


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


def _safe_compare(actual: Any, expected: Any, check: str) -> IdentityCheck:
    if check == "server_address" and expected == "127.0.0.1":
        if _is_fixed_server_address(actual):
            return IdentityCheck(check, "PASS", str(expected), str(actual))
        return IdentityCheck(check, "REJECT", str(expected), str(actual), "实际地址不是固定的127.0.0.1/32主机地址")
    if actual is not None and expected is not None and actual == expected:
        return IdentityCheck(check, "PASS", str(expected), str(actual))
    return IdentityCheck(check, "REJECT", str(expected), str(actual), "实际值与固定项目身份不一致")


def _is_fixed_server_address(value: Any) -> bool:
    """接受固定IPv4主机地址及其/32表示，拒绝网络掩码和其它地址。"""
    if not isinstance(value, str) or not value or value != value.strip():
        return False
    try:
        interface = ipaddress.ip_interface(value)
    except ValueError:
        return False
    return (interface.version == 4 and interface.network.prefixlen == 32
            and interface.ip == ipaddress.IPv4Address("127.0.0.1"))


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
    checks.append(_safe_compare(actual.get("schema"), _BUSINESS_SCHEMA, "current_schema"))
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
        checks.append(IdentityCheck("installation_cluster_system_identifier", "BLOCKED", reason="配置仍为未绑定占位，须经显式部署核验绑定"))
    else:
        checks.append(_safe_compare(actual.get("cluster_system_identifier"), expected_cluster, "installation_cluster_system_identifier"))
    expected_lineage = expected_app.get("migration_lineage")
    checks.append(_safe_compare(actual.get("migration_lineage"), expected_lineage, "migration_lineage"))
    migration_digest = actual.get("migration_digest")
    expected_migration_digest = expected_app.get("migration_digest")
    migration_digest_matches = isinstance(migration_digest, str) and migration_digest == expected_migration_digest
    migration_digest_expected = isinstance(expected_migration_digest, str) and re.fullmatch(r"[0-9a-f]{64}", expected_migration_digest) is not None
    migration_digest_status = ("BLOCKED" if not deployment_applied else
                               "PASS" if migration_digest_expected and migration_digest_matches else
                               "REJECT" if migration_digest_expected else "BLOCKED")
    checks.append(IdentityCheck(
        "migration_digest", migration_digest_status,
        expected=expected_migration_digest if migration_digest_expected else None,
        actual=migration_digest if isinstance(migration_digest, str) and re.fullmatch(r"[0-9a-f]{64}", migration_digest) else None,
        reason=None if migration_digest_status == "PASS" else "部署身份未绑定，或runtime.defaults迁移账本摘要与实际迁移不匹配",
    ))
    role = actual.get("session_user")
    checks.append(IdentityCheck(
        "session_user", "PASS" if role == "stock_a" else "REJECT",
        expected="stock_a", actual=role,
        reason=None if role == "stock_a" else "业务连接必须使用stock_a登录账号",
    ))
    role_attrs = actual.get("role_attributes")
    attrs_safe = (isinstance(role_attrs, dict)
                  and type(role_attrs.get("rolsuper")) is bool and role_attrs["rolsuper"] is False
                  and type(role_attrs.get("rolcreatedb")) is bool and role_attrs["rolcreatedb"] is False
                  and type(role_attrs.get("rolcreaterole")) is bool and role_attrs["rolcreaterole"] is False
                  and type(role_attrs.get("rolbypassrls")) is bool and role_attrs["rolbypassrls"] is False
                  and type(role_attrs.get("owns_business_objects")) is bool
                  and role_attrs["owns_business_objects"] is True)
    checks.append(IdentityCheck("role_attributes", "PASS" if attrs_safe else "REJECT", actual="非超级用户、无CREATEDB/CREATEROLE/BYPASSRLS且拥有项目对象" if attrs_safe else None,
                                 reason=None if attrs_safe else "stock_a不得具备集群级超级用户、建库、建角色或BYPASSRLS权限，并须拥有项目对象"))
    return checks


def _query_one(cursor: Any, sql: str, params: tuple[Any, ...] = ()) -> Any:
    cursor.execute(sql, params)
    row = cursor.fetchone()
    return None if row is None else row[0]


def _fetch_business_identity(cursor: Any, *, deadline: float) -> dict[str, Any]:
    """仅读取业务账号可见的连接身份、项目身份和迁移账本，不探测管理员目录。"""
    _check_deadline(deadline)
    cursor.execute("""SELECT current_database(), current_schema(), session_user, current_user,
       current_setting('port'), inet_server_addr()::text""")
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError("PG未返回连接身份")
    database, schema, session_user, current_user, port, server_address = row
    database_owner = _query_one(cursor, "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = current_database()")
    cursor.execute("""SELECT n.nspname, pg_get_userbyid(n.nspowner),
       has_schema_privilege(session_user, n.oid, 'USAGE'),
       has_schema_privilege(session_user, n.oid, 'CREATE')
       FROM pg_namespace n WHERE n.nspname = %s""", (_BUSINESS_SCHEMA,))
    schema_row = cursor.fetchone()
    schema_info = None if schema_row is None else {
        "name": schema_row[0], "owner": schema_row[1],
        "session_user_usage": bool(schema_row[2]), "session_user_create": bool(schema_row[3]),
    }
    cursor.execute("""SELECT r.rolsuper, r.rolcreatedb, r.rolcreaterole, r.rolbypassrls,
       NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relnamespace = n.oid AND c.relkind IN ('r','p','v','m','S')
         AND c.relowner <> r.oid)
       FROM pg_roles r CROSS JOIN pg_namespace n
       WHERE r.rolname = session_user AND n.nspname = %s""", (_BUSINESS_SCHEMA,))
    role_row = cursor.fetchone()
    role_attributes = None if role_row is None else {
        "rolsuper": bool(role_row[0]), "rolcreatedb": bool(role_row[1]),
        "rolcreaterole": bool(role_row[2]), "rolbypassrls": bool(role_row[3]),
        "owns_business_objects": bool(role_row[4]),
    }
    cursor.execute("SELECT to_regclass('stock.installation_identity')::text, to_regclass('stock.migration_ledger')::text")
    identity_table, migration_table = cursor.fetchone()
    identity = None
    if identity_table:
        cursor.execute("""SELECT application_id, project_uuid::text, instance_uuid::text,
           cluster_system_identifier, migration_lineage
           FROM stock.installation_identity WHERE singleton IS TRUE""")
        vals = cursor.fetchone()
        if vals:
            identity = dict(zip(("application_id", "project_uuid", "instance_uuid",
                                 "cluster_system_identifier", "migration_lineage"), vals))
    migration_digest = None
    if migration_table:
        _check_deadline(deadline)
        cursor.execute("SELECT migration_id, source_sha, lineage FROM stock.migration_ledger ORDER BY migration_id LIMIT %s", (_MAX_MIGRATIONS + 1,))
        rows = _bounded_rows(cursor, _MAX_MIGRATIONS, "迁移账本行数超过核验预算")
        if rows:
            migration_digest, lineages = _migration_ledger_digest(rows)
            if identity and (len(lineages) != 1 or next(iter(lineages)) != identity["migration_lineage"]):
                migration_digest = None
    return {
        "database": database, "database_owner": database_owner,
        "schema": schema, "session_user": session_user, "current_user": current_user,
        "port": int(port), "server_address": server_address,
        "business_schema_exists": schema_info is not None, "business_schema": schema_info,
        "identity_table_exists": bool(identity_table), "migration_table_exists": bool(migration_table),
        "identity": identity, "migration_digest": migration_digest, "role_attributes": role_attributes,
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


def classify_inspection_error(error: Exception) -> str:
    """将参数、权限、不可用和工程错误稳定映射为CLI类别。"""
    code = getattr(error, "sqlstate", None)
    lower = type(error).__name__.casefold()
    # 某些PostgreSQL/libpq启动错误没有SQLSTATE；仅在内存中有限匹配，不写入结果或日志。
    try:
        detail = str(error)[:2048].casefold()
    except Exception:
        detail = ""
    if isinstance(error, TimeoutError) or code == "57014" or "timeout" in lower:
        return "TIMEOUT"
    if (code in ("28P01", "28000") or "password authentication failed" in detail
            or "authentication failed" in detail):
        return "AUTHENTICATION_FAILED"
    if code == "3D000" or "database" in detail and "does not exist" in detail:
        return "DATABASE_NOT_FOUND"
    if code == "42501" or isinstance(error, PermissionError) or "permission" in lower or "permission denied" in detail:
        return "PERMISSION_DENIED"
    if "timeout expired" in detail or "timed out" in detail or "deadline exceeded" in detail:
        return "TIMEOUT"
    if isinstance(error, OverflowError) or (code and str(code).startswith("08")):
        return "UNAVAILABLE"
    if code and str(code).startswith("28"):
        return "PERMISSION_DENIED"
    if "operational" in lower or "connect" in lower or "network" in lower:
        return "UNAVAILABLE"
    if isinstance(error, ValueError):
        return "INVALID_ARGUMENT"
    return "ENGINEERING_ERROR"


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


def _load_private_credentials(path: Path | None = None) -> dict[str, Any]:
    """只读取唯一登记的普通凭据文件；错误消息不包含文件内容或OS路径细节。"""
    approved_root = _PRIVATE_CONFIG_ROOT
    path = _PRIVATE_CREDENTIALS_PATH if path is None else path
    try:
        # 路径检查只作早期拒绝；最终安全判断针对实际打开的同一文件句柄。
        for candidate in (approved_root.parent, approved_root, path):
            if candidate.is_symlink() or (hasattr(candidate, "is_junction") and candidate.is_junction()):
                raise _CredentialContractError("PATH_REPARSE_POINT")
        root = approved_root.resolve(strict=True)
        resolved = path.resolve(strict=True)
        if root != approved_root or resolved.parent != root or resolved.name != "postgresql.credentials.json":
            raise _CredentialContractError("PATH_OUTSIDE_REGISTERED_ROOT")
        # 只打开一次，在读取前从该句柄核验最终路径和文件类型。
        # 若检查与打开之间发生替换，实际句柄路径不匹配时会在读取前拒绝。
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(resolved, flags)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_size <= 0 or info.st_size > _MAX_CREDENTIAL_FILE_BYTES:
                raise _CredentialContractError("FILE_TYPE_OR_SIZE")
            opened_path = _opened_file_path(fd)
            if _canonical_path(opened_path) != _canonical_path(resolved):
                raise _CredentialContractError("OPEN_HANDLE_PATH_MISMATCH")
            with os.fdopen(fd, "rb", closefd=False) as stream:
                raw = stream.read(_MAX_CREDENTIAL_FILE_BYTES + 1)
        finally:
            os.close(fd)
        if len(raw) > _MAX_CREDENTIAL_FILE_BYTES:
            raise _CredentialContractError("FILE_EXCEEDS_READ_LIMIT")
        def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
            result: dict[str, Any] = {}
            for key, value in pairs:
                if key in result:
                    raise _CredentialContractError("JSON_DUPLICATE_FIELD")
                result[key] = value
            return result

        def reject_non_finite(_value: str) -> None:
            raise _CredentialContractError("JSON_NON_FINITE_NUMBER")

        data = json.loads(raw.decode("utf-8"), object_pairs_hook=reject_duplicate_keys,
                          parse_constant=reject_non_finite)
    except FileNotFoundError as error:
        raise FileNotFoundError("登记的PG凭据文件不存在") from error
    except PermissionError as error:
        raise PermissionError("操作系统拒绝读取登记的PG凭据文件") from error
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise RuntimeError("登记的PG凭据文件读取失败") from error
    allowed_fields = {"schema", "host", "port", "database", "username", "password"}
    if not isinstance(data, dict):
        raise _CredentialContractError("JSON_ROOT_NOT_OBJECT")
    missing_fields = tuple(sorted(allowed_fields - set(data)))
    extra_field_count = len(set(data) - allowed_fields)
    if missing_fields or extra_field_count:
        raise _CredentialContractError("FIELD_SET_MISMATCH", missing_fields=missing_fields,
                                       extra_field_count=extra_field_count)
    invalid_fields: list[str] = []
    if data.get("schema") != _CREDENTIAL_SCHEMA:
        invalid_fields.append("schema")
    if data.get("host") != "127.0.0.1":
        invalid_fields.append("host")
    if type(data.get("port")) is not int or data["port"] != 5432:
        invalid_fields.append("port")
    if data.get("database") != "stock_a":
        invalid_fields.append("database")
    if not isinstance(data.get("username"), str) or not data["username"]:
        invalid_fields.append("username")
    if not isinstance(data.get("password"), str) or not data["password"]:
        invalid_fields.append("password")
    if invalid_fields:
        raise _CredentialContractError("SCHEMA_TARGET_OR_VALUE_INVALID", invalid_fields=tuple(sorted(invalid_fields)))
    return data


def _canonical_path(path: str | Path) -> str:
    """统一路径分隔符与Windows大小写，供句柄最终路径比对。"""
    value = str(path).removeprefix("\\\\?\\")
    return os.path.normcase(os.path.normpath(value))


def _opened_file_path(fd: int) -> str:
    """返回已打开文件句柄的系统最终路径；不支持的平台失败关闭。"""
    if os.name == "nt":
        import ctypes
        import msvcrt
        handle = msvcrt.get_osfhandle(fd)
        get_path = ctypes.windll.kernel32.GetFinalPathNameByHandleW
        get_path.argtypes = (ctypes.c_void_p, ctypes.c_wchar_p, ctypes.c_uint32, ctypes.c_uint32)
        get_path.restype = ctypes.c_uint32
        buffer = ctypes.create_unicode_buffer(32768)
        length = get_path(handle, buffer, len(buffer), 0)
        if length == 0 or length >= len(buffer):
            raise OSError("无法验证凭据文件句柄最终路径")
        return buffer.value
    # POSIX以已打开文件的/proc句柄路径为准，缺少该能力时拒绝私密文件读取。
    proc_path = Path(f"/proc/self/fd/{fd}")
    if not proc_path.exists():
        raise OSError("当前平台不支持验证凭据文件句柄路径")
    return os.readlink(proc_path)


def _private_connection_parameters(credentials: dict[str, Any], make_conninfo: Any,
                                   conninfo_to_dict: Any) -> dict[str, Any]:
    """通过psycopg/libpq conninfo构造器转义秘密，再复用固定目标校验。"""
    conninfo = make_conninfo(
        host=credentials["host"], hostaddr="127.0.0.1", port=credentials["port"],
        dbname=credentials["database"], user=credentials["username"], password=credentials["password"],
    )
    return _fixed_connection_parameters(conninfo, {}, conninfo_to_dict)


def inspect_postgres(config: dict[str, Any], *, timeout_seconds: float = 5.0) -> dict[str, Any]:
    """使用固定环境DSN或唯一登记私密配置核验业务数据库连接与迁移身份。"""
    if type(timeout_seconds) not in (int, float) or not math.isfinite(timeout_seconds) or timeout_seconds <= 0 or timeout_seconds > _MAX_INSPECTION_SECONDS:
        raise ValueError("timeout_seconds必须在(0,30]内")
    try:
        import psycopg
        from psycopg.conninfo import conninfo_to_dict, make_conninfo
    except ImportError:
        return {"status": "DRIVER_MISSING", "connection_attempted": False,
                "reason": "缺少PostgreSQL驱动psycopg；未尝试连接", "cause_type": "ImportError",
                "checks": [], "database_connection": None}
    dsn = os.environ.get("STOCK_A_PG_DSN")
    if dsn:
        try:
            connection_parameters = _fixed_connection_parameters(dsn, os.environ, conninfo_to_dict)
        except psycopg.ProgrammingError as error:
            return {"status": "INVALID_ARGUMENT", "connection_attempted": False,
                    "reason": "STOCK_A_PG_DSN语法无效；未尝试连接", "preparation_failure": "ENVIRONMENT_DSN_SYNTAX",
                    "cause_type": type(error).__name__, "sqlstate": getattr(error, "sqlstate", None),
                    "checks": [], "database_connection": None}
        except (ValueError, TypeError):
            return {"status": "REJECTED", "connection_attempted": False,
                    "reason": "STOCK_A_PG_DSN目标或参数不符合固定合同；未尝试连接",
                    "preparation_failure": "ENVIRONMENT_DSN_CONTRACT", "checks": [], "database_connection": None}
        except Exception as error:
            return {"status": "ENGINEERING_ERROR", "connection_attempted": False,
                    "reason": "准备STOCK_A_PG_DSN连接参数时发生工程错误；未尝试连接",
                    "preparation_failure": "ENVIRONMENT_DSN_PREPARATION", "cause_type": type(error).__name__,
                    "checks": [], "database_connection": None}
    else:
        try:
            credentials = _load_private_credentials()
        except FileNotFoundError:
            return {"status": "BLOCKED", "connection_attempted": False,
                    "reason": "登记的PG凭据文件不存在；未尝试连接",
                    "preparation_failure": "PRIVATE_CREDENTIALS_MISSING", "checks": [], "database_connection": None}
        except PermissionError:
            return {"status": "PERMISSION_DENIED", "connection_attempted": False,
                    "reason": "操作系统拒绝读取登记的PG凭据文件；未尝试连接",
                    "preparation_failure": "PRIVATE_CREDENTIALS_ACCESS_DENIED", "checks": [], "database_connection": None}
        except _CredentialContractError as error:
            return {"status": "REJECTED", "connection_attempted": False,
                    "reason": "登记的PG凭据文件路径、结构或目标合同无效；未尝试连接",
                    "preparation_failure": "PRIVATE_CREDENTIALS_CONTRACT",
                    "credential_failure": error.reason_code,
                    **({"missing_registered_fields": list(error.missing_fields),
                       "unregistered_field_count": error.extra_field_count}
                       if error.reason_code == "FIELD_SET_MISMATCH" else {}),
                    **({"invalid_registered_fields": list(error.invalid_fields)}
                       if error.reason_code == "SCHEMA_TARGET_OR_VALUE_INVALID" else {}),
                    "checks": [], "database_connection": None}
        except ValueError:
            return {"status": "REJECTED", "connection_attempted": False,
                    "reason": "登记的PG凭据文件路径、结构或目标合同无效；未尝试连接",
                    "preparation_failure": "PRIVATE_CREDENTIALS_CONTRACT",
                    "credential_failure": "UNCLASSIFIED_CONTRACT_REJECTION", "checks": [], "database_connection": None}
        except RuntimeError:
            return {"status": "ENGINEERING_ERROR", "connection_attempted": False,
                    "reason": "读取登记的PG凭据文件时发生工程错误；未尝试连接",
                    "preparation_failure": "PRIVATE_CREDENTIALS_IO", "checks": [], "database_connection": None}
        except Exception as error:
            return {"status": "ENGINEERING_ERROR", "connection_attempted": False,
                    "reason": "准备登记的PG凭据文件时发生工程错误；未尝试连接",
                    "preparation_failure": "PRIVATE_CREDENTIALS_PREPARATION", "cause_type": type(error).__name__,
                    "checks": [], "database_connection": None}
        try:
            connection_parameters = _private_connection_parameters(credentials, make_conninfo, conninfo_to_dict)
        except psycopg.ProgrammingError as error:
            return {"status": "INVALID_ARGUMENT", "connection_attempted": False,
                    "reason": "登记的PG凭据无法构造固定连接参数；未尝试连接",
                    "preparation_failure": "PRIVATE_CREDENTIALS_CONNINFO", "cause_type": type(error).__name__,
                    "sqlstate": getattr(error, "sqlstate", None), "checks": [], "database_connection": None}
        except (ValueError, TypeError):
            return {"status": "REJECTED", "connection_attempted": False,
                    "reason": "登记的PG凭据无法形成固定目标连接参数；未尝试连接",
                    "preparation_failure": "PRIVATE_CREDENTIALS_TARGET", "checks": [], "database_connection": None}
        except Exception as error:
            return {"status": "ENGINEERING_ERROR", "connection_attempted": False,
                    "reason": "准备登记的PG固定连接参数时发生工程错误；未尝试连接",
                    "preparation_failure": "PRIVATE_CREDENTIALS_CONNINFO", "cause_type": type(error).__name__,
                    "checks": [], "database_connection": None}
    start = time.monotonic()
    deadline = start + min(float(timeout_seconds), _MAX_INSPECTION_SECONDS)
    try:
        connection_parameters.update(connect_timeout=max(1, math.ceil(timeout_seconds)), options=f"-c default_transaction_read_only=on -c search_path=stock,pg_catalog -c statement_timeout={max(1, math.ceil(timeout_seconds * 1000))} -c lock_timeout=1000")
        with psycopg.connect(**connection_parameters, autocommit=True) as conn:
            info = conn.info
            if info.host != "127.0.0.1" or getattr(info, "hostaddr", "127.0.0.1") != "127.0.0.1" or info.port != 5432 or info.dbname != "stock_a":
                return {"status": "REJECTED", "connection_attempted": True,
                        "reason": "连接目标不等于合同固定的127.0.0.1:5432/stock_a", "checks": [],
                        "connection_target": {"host": info.host, "port": info.port, "database": info.dbname}, "database_connection": None}
            with conn.cursor() as cursor:
                cursor.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                physical = _fetch_business_identity(_DeadlineCursor(cursor, deadline), deadline=deadline)
            _check_deadline(deadline)
            checks: list[IdentityCheck] = []
            checks.extend([
                _safe_compare(physical["port"], 5432, "server_port"),
                _safe_compare(physical["server_address"], "127.0.0.1", "server_address"),
                _safe_compare(physical["current_user"], "stock_a", "current_user"),
                IdentityCheck("business_schema_exists", "PASS" if physical["business_schema_exists"] else "REJECT",
                              expected="stock已存在", actual="stock已存在" if physical["business_schema_exists"] else "stock不存在",
                              reason=None if physical["business_schema_exists"] else "固定schema stock不存在；禁止自动创建schema"),
                _safe_compare(physical.get("database_owner"), "stock_a", "database_owner"),
                _safe_compare((physical.get("business_schema") or {}).get("owner"), "stock_a", "business_schema_owner"),
            ])
            schema_info = physical.get("business_schema") or {}
            for key, label in (("session_user_usage", "schema_usage"), ("session_user_create", "schema_create")):
                granted = schema_info.get(key) is True
                checks.append(IdentityCheck(label, "PASS" if granted else "REJECT", expected="true",
                                            actual="true" if granted else "false",
                                            reason=None if granted else "stock_a必须能使用并管理固定stock schema"))
            deployment_applied = config["application_identity"].get("deployment_applied") is True
            checks.append(IdentityCheck(
                "deployment_applied", "PASS" if deployment_applied else "BLOCKED",
                expected="true", actual="true" if deployment_applied else "false",
                reason=None if deployment_applied else "配置仍标记未部署；须先单独执行部署身份核验"))
            if physical["identity"] is None:
                identity_checks = evaluate_identity(config, {
                    "database": physical["database"], "schema": physical["schema"],
                    "cluster_system_identifier": None,
                    "session_user": physical["session_user"], "role_attributes": physical["role_attributes"],
                    "migration_digest": physical["migration_digest"],
                })
                checks.extend(
                    IdentityCheck("migration_lineage", "BLOCKED", reason="stock.migration_ledger缺失或为空；须由集中迁移实施")
                    if item.check == "migration_lineage" else
                    IdentityCheck(item.check, "BLOCKED", expected=item.expected, actual=item.actual,
                                  reason="installation_identity缺失；数据库标记身份尚不可比较")
                    if item.check in {"application_id", "project_uuid", "deployment_instance_uuid",
                                      "installation_cluster_system_identifier"}
                    else item
                    for item in identity_checks
                )
                checks.append(IdentityCheck("installation_identity", "BLOCKED", reason="stock.installation_identity缺失或无可读单例；须由明确集中迁移创建"))
            else:
                actual = {**physical["identity"], "database": physical["database"], "schema": physical["schema"],
                          "session_user": physical["session_user"], "role_attributes": physical["role_attributes"],
                          "migration_digest": physical["migration_digest"]}
                checks.extend(evaluate_identity(config, actual))
            with conn.cursor() as cursor:
                _check_deadline(deadline)
                cursor.execute("COMMIT")
            physical_summary = dict(physical)
            physical_summary["identity"] = "PRESENT" if physical["identity"] else "MISSING"
            physical_summary["identity_record"] = physical["identity"]
            physical_summary["deployment_storage_checks"] = "NOT_RUN_BY_BUSINESS_LOGIN; REQUIRE_SEPARATE_ADMIN_DEPLOYMENT_CHECK"
            if len(checks) > _MAX_CHECKS:
                raise OverflowError("PG核验检查项超过输出预算")
            statuses = {item.status for item in checks}
            overall = "READY" if statuses == {"PASS"} else ("REJECTED" if "REJECT" in statuses else "BLOCKED")
            account_checks = {"server_port", "server_address", "business_schema_exists", "database_owner",
                              "business_schema_owner", "schema_usage", "schema_create", "current_database",
                              "current_schema", "session_user", "current_user", "role_attributes"}
            account_ready = all(item.status == "PASS" for item in checks if item.check in account_checks)
            result = {"status": "DB_MIGRATION_READY" if overall == "READY" else overall,
                      "database_account_status": "DB_ACCOUNT_READY" if account_ready else "NOT_READY",
                      "business_pipeline_status": "BUSINESS_PIPELINE_PENDING",
                      "connection_attempted": True,
                      "elapsed_seconds": round(time.monotonic() - start, 6),
                      "transaction": "REPEATABLE_READ_READ_ONLY", "checks": [item.__dict__ for item in checks],
                      "database_connection": physical_summary, "deployment_applied": config["application_identity"].get("deployment_applied") is True}
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
                "checks": [], "database_connection": None}


def write_admission(
    inspection: dict[str, Any], *, requested_context: str | None = None,
    requested_configuration: dict[str, Any] | None = None, requested_action: str | None = None,
) -> tuple[bool, str]:
    """按真实stock_a登录、项目身份与已绑定部署配置决定是否可写。"""
    if not isinstance(inspection, dict) or inspection.get("status") != "DB_MIGRATION_READY":
        return False, "PostgreSQL身份或部署核验未就绪；拒绝新项目写入与发布"
    physical = inspection.get("database_connection")
    checks = inspection.get("checks")
    if not isinstance(physical, dict) or not isinstance(checks, list):
        return False, "PostgreSQL核验摘要不完整；拒绝新项目写入与发布"
    if (inspection.get("connection_attempted") is not True
            or inspection.get("deployment_applied") is not True
            or physical.get("session_user") != "stock_a"
            or physical.get("current_user") != "stock_a"
            or physical.get("database") != "stock_a"
            or physical.get("schema") != "stock"
            or physical.get("business_schema_exists") is not True
            or physical.get("database_owner") != "stock_a"
            or (physical.get("business_schema") or {}).get("owner") != "stock_a"
            or physical.get("role_attributes", {}).get("rolsuper") is not False
            or physical.get("role_attributes", {}).get("rolcreatedb") is not False
            or physical.get("role_attributes", {}).get("rolcreaterole") is not False
            or physical.get("role_attributes", {}).get("rolbypassrls") is not False
            or physical.get("role_attributes", {}).get("owns_business_objects") is not True
            or not checks or any(not isinstance(item, dict) or item.get("status") != "PASS" for item in checks)):
        return False, "真实stock_a登录、数据库身份或部署核验不匹配；拒绝新项目写入与发布"
    context = inspection.get("inspection_context")
    if (not isinstance(context, str) or not context or requested_context != context
            or requested_action not in ("WRITE", "PUBLISH")
            or requested_action not in inspection.get("admission_actions", [])):
        return False, "写入请求未绑定本次核验上下文、配置或允许动作"
    try:
        requested_digest = _configuration_identity(requested_configuration) if isinstance(requested_configuration, dict) else None
    except (TypeError, ValueError):
        requested_digest = None
    if requested_digest != inspection.get("configuration_identity"):
        return False, "写入请求配置与实际核验配置不一致"
    return True, "PostgreSQL stock_a业务账号及项目部署身份核验通过"
