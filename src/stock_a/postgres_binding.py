"""将已部署Stock A身份表前向迁移至唯一业务登录账号。"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Any

from . import postgres_identity as identity

_MIGRATIONS = (
    (1, "STOCK_A_INITIAL_R1", ("migrations", "0001_stock_identity.sql")),
    (2, "STOCK_A_INITIAL_R1", ("migrations", "0002_stock_a_business_login.sql")),
)


class BindingRefused(Exception):
    """身份、迁移或登录账号不符合固定Stock A合同。"""


def _migration_sources() -> list[tuple[int, str, bytes, str]]:
    root = Path(__file__).resolve().parents[2]
    result = []
    for migration_id, lineage, parts in _MIGRATIONS:
        path = root.joinpath(*parts)
        if path.is_symlink():
            raise BindingRefused("集中迁移文件不能通过链接替换")
        resolved = path.resolve(strict=True)
        if not resolved.is_relative_to(root) or not resolved.is_file():
            raise BindingRefused("集中迁移文件不在批准源码根内")
        raw = resolved.read_bytes()
        if not raw or len(raw) > 128 * 1024:
            raise BindingRefused("集中迁移文件为空或超过固定大小预算")
        result.append((migration_id, lineage, raw, hashlib.sha256(raw).hexdigest()))
    return result


def _connection_parameters() -> dict[str, Any]:
    import psycopg
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    dsn = os.environ.get("STOCK_A_PG_DSN")
    if dsn:
        return identity._fixed_connection_parameters(dsn, os.environ, conninfo_to_dict)
    credentials = identity._load_private_credentials()
    if credentials.get("username") != "stock_a":
        raise BindingRefused("迁移和业务连接必须使用stock_a登录账号")
    return identity._private_connection_parameters(credentials, make_conninfo, conninfo_to_dict)


def _update_runtime_config(config_path: Path, config: dict[str, Any], digest: str) -> None:
    updated = json.loads(json.dumps(config))
    updated["application_identity"]["migration_digest"] = digest
    encoded = (json.dumps(updated, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode("utf-8")
    temp_root = Path("G:/codex_tmp")
    temp_root.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix="stock-a-runtime-migration-", suffix=".tmp", dir=temp_root)
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
    """在既有stock schema中由stock_a登录执行待用前向迁移；绝不建库或schema。"""
    import psycopg

    if type(timeout_seconds) not in (int, float) or not 0 < timeout_seconds <= 30:
        raise ValueError("迁移耗时必须在(0,30]秒内")
    migrations = _migration_sources()
    expected_rows = [(mid, digest, lineage) for mid, lineage, _raw, digest in migrations]
    new_digest = hashlib.sha256("\n".join(f"{mid}:{sha}:{lineage}" for mid, sha, lineage in expected_rows).encode("utf-8")).hexdigest()
    parameters = _connection_parameters()
    parameters.update(connect_timeout=max(1, int(timeout_seconds)),
                      options=f"-c search_path=stock,pg_catalog -c statement_timeout={max(1, int(timeout_seconds * 1000))} -c lock_timeout=1000")
    try:
        with psycopg.connect(**parameters) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT current_database(), current_schema(), session_user, current_user")
                database, schema, session_user, current_user = cursor.fetchone()
                if (database, schema, session_user, current_user) != ("stock_a", "stock", "stock_a", "stock_a"):
                    raise BindingRefused("迁移要求stock_a数据库、stock schema及真实stock_a登录会话")
                cursor.execute("SELECT to_regclass('stock.installation_identity') IS NOT NULL, to_regclass('stock.migration_ledger') IS NOT NULL")
                if cursor.fetchone() != (True, True):
                    raise BindingRefused("现有安装身份或迁移账本缺失；本命令不会初始化安装或创建schema")
                cursor.execute("SELECT migration_id, source_sha, lineage FROM stock.migration_ledger ORDER BY migration_id FOR UPDATE")
                existing = cursor.fetchall()
                if existing != expected_rows[:len(existing)] or len(existing) not in (1, 2):
                    raise BindingRefused("现有迁移账本与不可变历史迁移摘要不匹配")
                applied = []
                for mid, lineage, raw, digest in migrations[len(existing):]:
                    cursor.execute(raw.decode("utf-8"))
                    cursor.execute("INSERT INTO stock.migration_ledger(migration_id, source_sha, applied_at, lineage) VALUES (%s,%s,clock_timestamp(),%s)", (mid, digest, lineage))
                    applied.append(mid)
                cursor.execute("SELECT count(*) FROM stock.migration_ledger")
                count = int(cursor.fetchone()[0])
                if count != len(expected_rows):
                    raise BindingRefused("前向迁移提交前账本读回数量不匹配")
                cursor.execute("SELECT session_user, current_user, count(*) FROM stock.installation_identity WHERE application_id='STOCK_A'")
                if cursor.fetchone() != ("stock_a", "stock_a", 1):
                    raise BindingRefused("身份单例或实际业务登录读回不匹配")
        if config["application_identity"].get("migration_digest") != new_digest:
            _update_runtime_config(config_path, config, new_digest)
        return {"status": "BOUND", "database": database, "schema": schema,
                "session_user": session_user, "applied_migrations": applied,
                "migration_count": len(expected_rows), "migration_digest": new_digest,
                "business_write_admission": "REQUIRES_IDENTITY_READBACK"}
    except BindingRefused:
        raise
    except Exception as error:
        raise BindingRefused(f"前向迁移失败，底层错误类别为{type(error).__name__}；异常文本已省略") from error
