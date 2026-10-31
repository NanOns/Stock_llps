"""将已部署Stock A身份表前向迁移至唯一业务登录账号。"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Any

from . import identity

_MIGRATIONS = (
    (
        1,
        "STOCK_A_INITIAL_R1",
        ("migrations", "0001_stock_identity.sql"),
        "010b4fcd0497b847c1dc5e1c90725b9c692012cc2d7dbede0ce702c40a1a7428",
    ),
    (
        2,
        "STOCK_A_INITIAL_R1",
        ("migrations", "0002_stock_a_business_login.sql"),
        "b991f72174ffed1747b616b4d75264ddafca48776cccab9c251bcc9198da2abb",
    ),
    (
        3,
        "STOCK_A_INITIAL_R1",
        ("migrations", "0003_r01_source_identity_canonical.sql"),
        "293c6d24a04b05200cc3f0df6f4b7673626055a15da99a7eeac69f0ffe441d5a",
    ),
    (
        4,
        "STOCK_A_INITIAL_R1",
        ("migrations", "0004_r01_source_raw_daily.sql"),
        "6b14004deb034e2441fb0ec2288aa46c980f808dc27d02ce2dcb34fe1ca8eed4",
    ),
    (
        5,
        "STOCK_A_INITIAL_R1",
        ("migrations", "0005_r01_p03_source_set_candidate.sql"),
        "cb069824c9c41144d10e4e78cc3b2d18c615c18f8a8c0132d74a902410ba29ba",
    ),
    (
        6,
        "STOCK_A_INITIAL_R1",
        ("migrations", "0006_r01_p03_formal_source_set.sql"),
        "0ccf175991947e18de88aca809653dc49e9b4669cfcd5a0a62a0f6f6879921b8",
    ),
    (
        7,
        "STOCK_A_INITIAL_R1",
        ("migrations", "0007_r02_tdx_source_symbol_price_profile.sql"),
        "0ef16c44b9c07d0aa6f494f6793690fae84651a3e5bcc3fac3a5dc83dbafbc48",
    ),
)
_TASK_SCOPE_MIGRATIONS = {
    "R01_R02_PG_TASK_AUTHORITY_AND_SOURCE_PERSIST_R1": frozenset({3}),
    "R01_R02_SOURCE_RAW_PG_DAILY_READ_R1": frozenset({4}),
    "R01_R02_P03_SOURCE_SET_BROWSER_RULE_R1": frozenset({5}),
    "R01_R02_P03_FORMAL_TWO_DAY_WRITE_R1": frozenset({6}),
    "R02_TDX_SOURCE_SYMBOL_PRICE_PROFILE_0007_R1": frozenset({7}),
}
_BASE_TABLES = {
    "installation_identity",
    "migration_ledger",
    "namespaces",
    "role_bindings",
}
_R01_TABLES = _BASE_TABLES | {
    "source_contents",
    "source_captures",
    "entities",
    "identity_observations",
    "calendar_sessions",
    "canonical_daily",
}
_RELATIONS_BY_MIGRATION_COUNT = {
    2: _BASE_TABLES,
    3: _R01_TABLES,
    4: _R01_TABLES | {"source_raw_daily"},
    5: _R01_TABLES
    | {
        "source_raw_daily",
        "source_set_candidates",
        "source_set_candidate_inputs",
    },
    6: _R01_TABLES
    | {
        "source_raw_daily",
        "source_set_candidates",
        "source_set_candidate_inputs",
        "source_sets",
        "source_set_capabilities",
    },
    7: _R01_TABLES
    | {
        "source_raw_daily",
        "source_set_candidates",
        "source_set_candidate_inputs",
        "source_sets",
        "source_set_capabilities",
        "source_symbol_price_profiles",
    },
}


class BindingRefused(Exception):
    """身份、迁移或登录账号不符合固定Stock A合同。"""


class BindingCommittedConfigError(BindingRefused):
    """PG事务已提交，但运行配置摘要同步失败。"""


def _validate_task_scope(task_scope: str | None, migration_id: int) -> None:
    """只放行任务登记表内的范围与迁移配对，不接受通用SQL执行权限。"""
    if not isinstance(
        task_scope, str
    ) or migration_id not in _TASK_SCOPE_MIGRATIONS.get(task_scope, ()):
        raise BindingRefused("PG绑定必须绑定已批准任务范围及其唯一迁移ID")


def _validate_migration_source(migration_id: int, source_sha: str) -> None:
    """固定迁移登记表内各SQL字节摘要，避免替换已批准源码扩大写入。"""
    expected = next((item[3] for item in _MIGRATIONS if item[0] == migration_id), None)
    if expected != source_sha:
        raise BindingRefused("本批批准的迁移源码SHA不匹配；拒绝执行DDL")


def registered_task_scopes() -> tuple[str, ...]:
    """返回集中迁移CLI允许选择的显式任务范围。"""
    return tuple(sorted(_TASK_SCOPE_MIGRATIONS))


def registered_migration_ids() -> tuple[int, ...]:
    """返回集中迁移CLI登记的迁移ID；实际配对仍由任务scope再次核验。"""
    return tuple(
        sorted(
            {
                migration_id
                for values in _TASK_SCOPE_MIGRATIONS.values()
                for migration_id in values
            }
        )
    )


def _validate_migration_history(
    existing: list[tuple[Any, Any, Any]],
    expected_rows: list[tuple[int, str, str]],
) -> int:
    """验证不可变迁移前缀，允许精确已应用迁移的幂等读回。"""
    if (
        len(existing) < 2
        or len(existing) > len(expected_rows)
        or existing != expected_rows[: len(existing)]
    ):
        raise BindingRefused("现有迁移账本与不可变历史迁移摘要不匹配")
    return len(existing)


def _ledger_digest(rows: list[tuple[int, str, str]]) -> str:
    """按既有稳定语义摘要一个已应用迁移前缀。"""
    return hashlib.sha256(
        "\n".join(
            f"{mid}:{source_sha}:{lineage}" for mid, source_sha, lineage in rows
        ).encode("utf-8")
    ).hexdigest()


def _relation_names(cursor: Any) -> set[str]:
    cursor.execute("""SELECT c.relname FROM pg_class c
       JOIN pg_namespace n ON n.oid=c.relnamespace
       WHERE n.nspname='stock' AND c.relkind IN ('r','p','v','m','f')
       ORDER BY c.relname""")
    return {row[0] for row in cursor.fetchall()}


def _validate_business_role(business: dict[str, Any]) -> None:
    """保留目标账号、对象所有权和集群级降权属性的实际连接检查。"""
    attributes = business.get("role_attributes")
    schema = business.get("business_schema") or {}
    if (
        business.get("database") != "stock_a"
        or business.get("schema") != "stock"
        or business.get("session_user") != "stock_a"
        or business.get("current_user") != "stock_a"
        or business.get("database_owner") != "stock_a"
        or schema.get("owner") != "stock_a"
        or schema.get("session_user_usage") is not True
        or schema.get("session_user_create") is not True
        or not isinstance(attributes, dict)
        or any(
            attributes.get(key) is not False
            for key in ("rolsuper", "rolcreatedb", "rolcreaterole", "rolbypassrls")
        )
        or attributes.get("owns_business_objects") is not True
    ):
        raise BindingRefused(
            "stock_a目标连接、schema权限或非超级用户角色属性不符合固定合同"
        )


def _validate_application_identity(
    actual: dict[str, Any] | None, expected: dict[str, Any]
) -> None:
    if not isinstance(actual, dict):
        raise BindingRefused("迁移前缺少可核对的安装身份单例")
    for field, expected_value in (
        ("application_id", "STOCK_A"),
        ("project_uuid", expected.get("project_uuid")),
        ("instance_uuid", expected.get("deployment_instance_uuid")),
        ("cluster_system_identifier", expected.get("cluster_system_identifier")),
        ("migration_lineage", expected.get("migration_lineage")),
    ):
        if actual.get(field) != expected_value:
            raise BindingRefused(f"迁移前安装身份不匹配：{field}")


def _migration_sources() -> list[tuple[int, str, bytes, str]]:
    root = Path("G:/codex work/Stock A").resolve(strict=True)
    result = []
    for migration_id, lineage, parts, expected_sha in _MIGRATIONS:
        path = root.joinpath(*parts)
        if path.is_symlink():
            raise BindingRefused("集中迁移文件不能通过链接替换")
        resolved = path.resolve(strict=True)
        if not resolved.is_relative_to(root) or not resolved.is_file():
            raise BindingRefused("集中迁移文件不在批准源码根内")
        raw = resolved.read_bytes()
        if not raw or len(raw) > 128 * 1024:
            raise BindingRefused("集中迁移文件为空或超过固定大小预算")
        actual_sha = hashlib.sha256(raw).hexdigest()
        if actual_sha != expected_sha:
            raise BindingRefused(f"集中迁移{migration_id:04d}源码SHA与登记摘要不匹配")
        result.append((migration_id, lineage, raw, actual_sha))
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
    return identity._private_connection_parameters(
        credentials, make_conninfo, conninfo_to_dict
    )


def _update_runtime_config(
    config_path: Path, config: dict[str, Any], digest: str
) -> None:
    updated = json.loads(json.dumps(config))
    updated["application_identity"]["migration_digest"] = digest
    encoded = (
        json.dumps(updated, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
    ).encode("utf-8")
    temp_root = Path("G:/codex_tmp")
    temp_root.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix="stock-a-runtime-migration-", suffix=".tmp", dir=temp_root
    )
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


def bind_stock_namespace(
    config: dict[str, Any],
    config_path: Path,
    *,
    task_scope: str,
    migration_id: int,
    timeout_seconds: float = 20.0,
) -> dict[str, Any]:
    """在固定stock_a.stock中执行当前已授权且唯一的集中迁移。"""
    import psycopg

    if type(timeout_seconds) not in (int, float) or not 0 < timeout_seconds <= 30:
        raise ValueError("迁移耗时必须在(0,30]秒内")
    _validate_task_scope(task_scope, migration_id)
    expected_identity = (
        config.get("application_identity") if isinstance(config, dict) else None
    )
    if (
        not isinstance(expected_identity, dict)
        or expected_identity.get("deployment_applied") is not True
        or not identity._valid_uuid(expected_identity.get("project_uuid"))
        or not identity._valid_uuid(expected_identity.get("deployment_instance_uuid"))
        or not isinstance(expected_identity.get("cluster_system_identifier"), str)
        or not expected_identity["cluster_system_identifier"].isdecimal()
        or not isinstance(expected_identity.get("migration_lineage"), str)
        or not expected_identity["migration_lineage"].strip()
    ):
        raise BindingRefused("迁移配置中的项目/部署身份未完整绑定；拒绝连接和迁移")
    migrations = _migration_sources()
    expected_rows = [
        (mid, digest, lineage) for mid, lineage, _raw, digest in migrations
    ]
    selected = [item for item in migrations if item[0] == migration_id]
    if len(selected) != 1:
        raise BindingRefused("任务登记的迁移ID未对应唯一集中迁移源码")
    _validate_migration_source(selected[0][0], selected[0][3])
    try:
        parameters = _connection_parameters()
    except ValueError as error:
        raise BindingRefused("PG连接目标或凭据合同无效；拒绝连接和迁移") from error
    approved_config = Path("G:/codex work/Stock A/config/runtime.defaults.json")
    if config_path.is_symlink() or config_path.resolve(
        strict=True
    ) != approved_config.resolve(strict=True):
        raise BindingRefused(
            "迁移配置必须是唯一批准的runtime.defaults.json，且不得经链接替换"
        )
    parameters.update(
        connect_timeout=max(1, int(timeout_seconds)),
        options=(
            "-c search_path=stock,pg_catalog "
            "-c default_transaction_isolation=serializable "
            f"-c statement_timeout={max(1, int(timeout_seconds * 1000))} "
            "-c lock_timeout=1000"
        ),
    )
    transaction_committed = False
    try:
        with psycopg.connect(**parameters) as connection:
            with connection.cursor() as cursor:
                info = connection.info
                if (
                    info.host != "127.0.0.1"
                    or info.port != 5432
                    or info.dbname != "stock_a"
                    or getattr(info, "hostaddr", "127.0.0.1") != "127.0.0.1"
                ):
                    raise BindingRefused("迁移连接目标不等于固定127.0.0.1:5432/stock_a")
                cursor.execute(
                    "SELECT current_database(), current_schema(), session_user, current_user"
                )
                database, schema, session_user, current_user = cursor.fetchone()
                if (database, schema, session_user, current_user) != (
                    "stock_a",
                    "stock",
                    "stock_a",
                    "stock_a",
                ):
                    raise BindingRefused(
                        "迁移要求stock_a数据库、stock schema及真实stock_a登录会话"
                    )
                deadline = identity.time.monotonic() + float(timeout_seconds)
                business = identity._fetch_business_identity(
                    identity._DeadlineCursor(cursor, deadline), deadline=deadline
                )
                _validate_business_role(business)
                actual_identity = business.get("identity")
                _validate_application_identity(actual_identity, expected_identity)
                cursor.execute(
                    "SELECT to_regclass('stock.installation_identity') IS NOT NULL, to_regclass('stock.migration_ledger') IS NOT NULL"
                )
                if cursor.fetchone() != (True, True):
                    raise BindingRefused(
                        "现有安装身份或迁移账本缺失；本命令不会初始化安装或创建schema"
                    )
                cursor.execute(
                    "SELECT migration_id, source_sha, lineage FROM stock.migration_ledger ORDER BY migration_id"
                )
                existing = cursor.fetchall()
                applied_count = _validate_migration_history(existing, expected_rows)
                current_digest = _ledger_digest(expected_rows[:applied_count])
                target_digest = _ledger_digest(expected_rows[:migration_id])
                prior_digest = _ledger_digest(expected_rows[: migration_id - 1])
                if business.get("migration_digest") != current_digest:
                    raise BindingRefused(
                        "数据库业务身份读取到的迁移摘要与精确账本前缀不匹配"
                    )
                if config["application_identity"].get("migration_digest") not in {
                    current_digest,
                    prior_digest,
                    target_digest,
                }:
                    raise BindingRefused(
                        "runtime.defaults迁移摘要不对应本次精确迁移前缀"
                    )
                if not actual_identity.get(
                    "migration_lineage"
                ) == expected_identity.get("migration_lineage"):
                    raise BindingRefused("安装身份迁移lineage与运行配置不匹配")
                expected_relations = _RELATIONS_BY_MIGRATION_COUNT.get(applied_count)
                if expected_relations is None:
                    raise BindingRefused("迁移账本没有对应的受控对象清单")
                if _relation_names(cursor) != expected_relations:
                    raise BindingRefused(
                        "现有stock关系清单与迁移账本版本不一致；拒绝执行DDL"
                    )
                cursor.execute(
                    "LOCK TABLE stock.installation_identity, stock.migration_ledger IN SHARE ROW EXCLUSIVE MODE"
                )
                cursor.execute("""SELECT application_id, project_uuid::text, instance_uuid::text,
                   cluster_system_identifier, migration_lineage
                   FROM stock.installation_identity WHERE singleton IS TRUE FOR UPDATE""")
                locked_identity = cursor.fetchone()
                if locked_identity != (
                    "STOCK_A",
                    expected_identity["project_uuid"],
                    expected_identity["deployment_instance_uuid"],
                    expected_identity["cluster_system_identifier"],
                    expected_identity["migration_lineage"],
                ):
                    raise BindingRefused("部署核验后安装身份发生变化；拒绝提交迁移")
                cursor.execute(
                    "SELECT migration_id, source_sha, lineage FROM stock.migration_ledger ORDER BY migration_id FOR UPDATE"
                )
                locked_rows = cursor.fetchall()
                if locked_rows != existing:
                    raise BindingRefused("部署核验后迁移账本发生变化；拒绝提交迁移")
                applied = []
                if applied_count == migration_id - 1:
                    mid, lineage, raw, digest = selected[0]
                    cursor.execute(raw.decode("utf-8"))
                    cursor.execute(
                        "INSERT INTO stock.migration_ledger(migration_id, source_sha, applied_at, lineage) VALUES (%s,%s,clock_timestamp(),%s)",
                        (mid, digest, lineage),
                    )
                    applied.append(mid)
                elif applied_count != migration_id:
                    raise BindingRefused(
                        "只允许逐步应用当前登记的下一条迁移；不自动执行或跳过其它迁移"
                    )
                cursor.execute(
                    "SELECT migration_id, source_sha, lineage FROM stock.migration_ledger ORDER BY migration_id"
                )
                committed_rows = cursor.fetchall()
                if committed_rows != expected_rows:
                    raise BindingRefused("集中迁移提交前账本精确读回不匹配")
                final_relations = _RELATIONS_BY_MIGRATION_COUNT.get(migration_id)
                if (
                    final_relations is None
                    or _relation_names(cursor) != final_relations
                ):
                    raise BindingRefused("集中迁移提交前stock关系清单不匹配")
                cursor.execute(
                    "SELECT session_user, current_user, count(*) FROM stock.installation_identity WHERE application_id='STOCK_A'"
                )
                if cursor.fetchone() != ("stock_a", "stock_a", 1):
                    raise BindingRefused("身份单例或实际业务登录读回不匹配")
        transaction_committed = True
        new_digest = _ledger_digest(expected_rows[:migration_id])
        if config["application_identity"].get("migration_digest") != new_digest:
            try:
                _update_runtime_config(config_path, config, new_digest)
            except Exception as error:
                raise BindingCommittedConfigError(
                    "PG迁移已提交，但runtime.defaults.json摘要未同步；修复配置写入后重试幂等读回"
                ) from error
        return {
            "status": "BOUND",
            "database": database,
            "schema": schema,
            "session_user": session_user,
            "applied_migrations": applied,
            "migration_count": len(expected_rows),
            "migration_digest": new_digest,
            "task_scope": task_scope,
            "migration_id": migration_id,
            "storage_location_status": "USER_CONFIRMED_NOT_INDEPENDENTLY_VERIFIED",
            "business_write_admission": "MIGRATION_SCOPE_ONLY",
        }
    except BindingRefused:
        raise
    except Exception as error:
        if transaction_committed:
            raise BindingCommittedConfigError(
                "PG迁移已提交，但迁移后本地配置处理失败；重试前先只读核验ledger和配置摘要"
            ) from error
        raise BindingRefused(
            f"前向迁移失败，底层错误类别为{type(error).__name__}；异常文本已省略"
        ) from error
