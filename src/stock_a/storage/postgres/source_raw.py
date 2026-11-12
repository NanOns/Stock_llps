"""在固定stock_a.stock中持久化并读取TDX来源代码级Raw日线。"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
import os
from pathlib import Path
import re
import time
from typing import Any, Iterator, Mapping, Sequence

from . import binding as postgres_binding
from . import identity as postgres_identity
from .typed import _finite_decimal


RAW_DAILY_APPEND_SCOPE = "STOCK_A_SOURCE_RAW_DAILY_APPEND_R1"
LEGACY_RAW_DAILY_APPEND_SCOPE = "R01_R02_SOURCE_RAW_PG_DAILY_READ_R1"
RAW_DAILY_APPEND_TASK_SCOPES = (
    RAW_DAILY_APPEND_SCOPE,
    LEGACY_RAW_DAILY_APPEND_SCOPE,
)
_RAW_DAILY_APPEND_TASK_SCOPES = frozenset(RAW_DAILY_APPEND_TASK_SCOPES)
_DATASET_ID = "TDX_VIPDATA_PACKAGE"
_PARSER_CONTRACT = "stock_a.sources.tdx.vipdata.scan_vipdata_target_quality|TDX_HSJDAY_ZIP_ENTRY_32_BYTE_LAYOUT_R1|SOURCE_RAW_DAILY_R1"
_CLOCK_ROLE = "LOCAL_VERIFIED_VIPDATA_TARGET_SCAN_NON_PIT"
_PACKAGE_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL = re.compile(r"^(SH|SZ)\.[0-9]{6}$")
_PRICE_FACT_SYMBOL = re.compile(r"^(SH|SZ|BJ)\.[0-9]{6}$")
_BAR_STATUSES = {"TARGET_PRESENT", "TARGET_MISSING", "READ_FAILED"}
_QUALITY = {"OBSERVED", "UNKNOWN", "NOT_APPLICABLE", "BAD_VALUE"}
_RAW_QUALITY = {"RAW_SOURCE_VALUE_OBSERVED", "UNKNOWN", "BAD_VALUE", "NOT_APPLICABLE"}

_RAW_COLUMNS = (
    "source_symbol",
    "market",
    "board",
    "session_date",
    "source_package_id",
    "source_capture_id",
    "source_entry_bytes",
    "source_record_count",
    "bar_status",
    "bar_reason",
    "raw_open",
    "raw_high",
    "raw_low",
    "raw_close",
    "price_quality",
    "price_reason",
    "raw_amount",
    "raw_amount_quality",
    "raw_amount_reason",
    "amount_quality",
    "amount_reason",
    "amount_unit_status",
    "raw_volume",
    "raw_volume_quality",
    "raw_volume_reason",
    "volume_quality",
    "volume_reason",
    "volume_unit_status",
    "first_source_date",
    "last_source_date",
    "future_source_rows_excluded",
    "observed_started_at",
    "observed_completed_at",
    "source_clock_role",
    "source_available_at",
)
_RAW_SELECT = ", ".join(_RAW_COLUMNS)
_RAW_INSERT = f"""INSERT INTO source_raw_daily ({_RAW_SELECT})
VALUES ({", ".join(["%s"] * len(_RAW_COLUMNS))})
ON CONFLICT (source_capture_id, source_symbol, session_date) DO NOTHING"""


class RawSourcePgError(Exception):
    """固定来源Raw PG操作被拒绝或未完成。"""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        committed_rows: int | None = None,
        metadata_committed: bool = False,
    ):
        self.code = code
        self.committed_rows = committed_rows
        self.metadata_committed = metadata_committed
        super().__init__(message)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise RawSourcePgError("SOURCE_CLOCK_INVALID", "来源观察时钟必须包含时区")
    return value


def _connection_parameters() -> dict[str, Any]:
    try:
        import psycopg
        from psycopg.conninfo import conninfo_to_dict, make_conninfo
    except ImportError as error:
        raise RawSourcePgError(
            "PG_DRIVER_MISSING", "缺少PostgreSQL驱动psycopg"
        ) from error
    dsn = os.environ.get("STOCK_A_PG_DSN")
    if dsn:
        try:
            return postgres_identity._fixed_connection_parameters(
                dsn, os.environ, conninfo_to_dict
            )
        except (ValueError, TypeError, psycopg.ProgrammingError) as error:
            raise RawSourcePgError(
                "PG_TARGET_REJECTED", "PG连接目标或连接参数不符合固定合同"
            ) from error
    try:
        credentials = postgres_identity._load_private_credentials()
        if credentials.get("username") != "stock_a":
            raise RawSourcePgError(
                "PG_LOGIN_REJECTED", "来源Raw PG读写必须使用stock_a业务登录账号"
            )
        return postgres_identity._private_connection_parameters(
            credentials, make_conninfo, conninfo_to_dict
        )
    except RawSourcePgError:
        raise
    except Exception as error:
        raise RawSourcePgError(
            "PG_CREDENTIALS_UNAVAILABLE", "无法从唯一登记的私密配置准备PG连接"
        ) from error


@contextmanager
def _transaction(
    config: Mapping[str, Any], *, write: bool, statement_timeout_ms: int = 10000
) -> Iterator[tuple[Any, Any, None]]:
    import psycopg

    if (
        type(statement_timeout_ms) is not int
        or not 1000 <= statement_timeout_ms <= 30000
    ):
        raise ValueError("PG语句超时必须在1至30秒之间")
    parameters = _connection_parameters()
    parameters.update(
        connect_timeout=5,
        options=(
            f"-c search_path=stock,pg_catalog -c statement_timeout={statement_timeout_ms} -c lock_timeout=1000"
        ),
    )
    try:
        with psycopg.connect(**parameters) as connection:
            with connection.cursor() as cursor:
                if write:
                    cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                else:
                    cursor.execute(
                        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
                    )
                cursor.execute("SET LOCAL search_path TO stock, pg_catalog")
                _validate_connection(cursor, connection, config)
                # 身份核验临时按其总预算缩短超时；业务语句须恢复调用者的有界预算。
                cursor.execute(
                    "SELECT set_config('statement_timeout', %s, true)",
                    (str(statement_timeout_ms),),
                )
                yield connection, cursor, None
    except RawSourcePgError:
        raise
    except Exception as error:
        if getattr(error, "preserve_transaction_error", False) is True:
            raise
        sqlstate = getattr(error, "sqlstate", None)
        failure_code = (
            "PG_TIMEOUT"
            if sqlstate == "57014"
            else "PG_READ_PERMISSION_DENIED"
            if not write and sqlstate == "42501"
            else "PG_WRITE_FAILED"
            if write
            else "PG_READ_FAILED"
        )
        raise RawSourcePgError(
            failure_code,
            f"固定stock_a.stock来源Raw事务失败（{type(error).__name__}，SQLSTATE={sqlstate or 'UNKNOWN'}）",
        ) from error


def _validate_connection(
    cursor: Any, connection: Any, config: Mapping[str, Any]
) -> None:
    info = connection.info
    if (
        info.host != "127.0.0.1"
        or info.port != 5432
        or info.dbname != "stock_a"
        or getattr(info, "hostaddr", "127.0.0.1") != "127.0.0.1"
    ):
        raise RawSourcePgError(
            "PG_TARGET_REJECTED", "来源Raw操作仅允许127.0.0.1:5432/stock_a"
        )
    cursor.execute(
        "SELECT current_database(), current_schema(), session_user, current_user, inet_server_addr()::text, inet_server_port()"
    )
    actual = cursor.fetchone()
    if actual != ("stock_a", "stock", "stock_a", "stock_a", "127.0.0.1/32", 5432):
        raise RawSourcePgError(
            "PG_IDENTITY_REJECTED",
            "来源Raw操作要求stock_a数据库、stock schema及stock_a真实登录",
        )

    expected = config.get("application_identity")
    if (
        not isinstance(expected, Mapping)
        or expected.get("deployment_applied") is not True
    ):
        raise RawSourcePgError(
            "APPLICATION_IDENTITY_MISSING", "运行配置没有已绑定的Stock A项目与部署身份"
        )
    deadline = postgres_identity.time.monotonic() + 8.0
    business = postgres_identity._fetch_business_identity(
        postgres_identity._DeadlineCursor(cursor, deadline),
        deadline=deadline,
    )
    postgres_binding._validate_business_role(business)
    try:
        postgres_binding._validate_application_identity(
            business.get("identity"), expected
        )
    except postgres_binding.BindingRefused as error:
        raise RawSourcePgError(
            "APPLICATION_IDENTITY_REJECTED",
            "PG中的Stock A项目、部署实例、簇或迁移lineage不匹配",
        ) from error
    migration_version = business.get("migration_version")
    if type(migration_version) is not int or migration_version != expected.get(
        "migration_version"
    ):
        raise RawSourcePgError(
            "MIGRATION_IDENTITY_REJECTED", "PG迁移版本与唯一运行配置不一致"
        )
    _validate_raw_migration_prefix(cursor, config, migration_version)


def _validate_raw_migration_prefix(
    cursor: Any,
    config: Mapping[str, Any],
    business_version: Any,
) -> int:
    """验证连续迁移编号及lineage，并拒绝未应用所需迁移的连接。"""
    try:
        sources = postgres_binding._migration_sources()
        registered_ids = [migration_id for migration_id, _lineage, _raw in sources]
        if registered_ids != list(range(1, len(registered_ids) + 1)):
            raise postgres_binding.BindingRefused("迁移登记ID不是连续前缀")
        expected_rows = [
            (migration_id, lineage) for migration_id, lineage, _raw in sources
        ]
        cursor.execute(
            "SELECT migration_id, lineage FROM migration_ledger ORDER BY migration_id LIMIT %s",
            (len(expected_rows) + 1,),
        )
        existing = cursor.fetchall()
        applied_count = postgres_binding._validate_migration_history(
            existing, expected_rows
        )
        if applied_count < 4:
            raise postgres_binding.BindingRefused("Raw Daily 需要0001-0004全部迁移")
        identity = config.get("application_identity")
        configured_version = (
            identity.get("migration_version") if isinstance(identity, Mapping) else None
        )
        if (
            type(business_version) is not int
            or business_version != applied_count
            or configured_version != applied_count
        ):
            raise postgres_binding.BindingRefused("数据库、配置和迁移编号前缀不一致")
        return applied_count
    except (postgres_binding.BindingRefused, OSError, ValueError) as error:
        raise RawSourcePgError(
            "MIGRATION_HISTORY_REJECTED",
            "PG Raw 需要连续迁移编号及一致lineage，0004不可缺失",
        ) from error


def _validate_source_input(
    package: Any, target_session: date, *, task_scope: str
) -> None:
    if task_scope not in _RAW_DAILY_APPEND_TASK_SCOPES:
        raise RawSourcePgError(
            "TASK_SCOPE_REJECTED", "Raw APPEND只接受已登记的每日业务操作标识"
        )
    if type(target_session) is not date:
        raise RawSourcePgError(
            "TARGET_SESSION_INVALID", "必须显式提供date类型目标交易会话"
        )
    from stock_a.sources.tdx.vipdata import VerifiedVipdataPackage

    if (
        not isinstance(package, VerifiedVipdataPackage)
        or getattr(package, "zip_crc", None) != "PASS"
        or getattr(package, "file_size", 0) <= 0
    ):
        raise RawSourcePgError(
            "PACKAGE_NOT_VERIFIED",
            "入库仅接受原始包入口已完成字节完整性及ZIP CRC校验的VIPDATA包",
        )


def validate_source_candidate_limit(candidate_total: int) -> None:
    """本批完整保留的候选数上限；超出时拒绝，不丢尾或抽样。"""
    if type(candidate_total) is not int or not 1 <= candidate_total <= 5000:
        raise RawSourcePgError(
            "CANDIDATE_LIMIT_EXCEEDED", "本批目标日候选须为1至5000条；拒绝截断或遗漏"
        )


def _register_source_package(config: Mapping[str, Any], package: Any) -> int:
    """在原件接收入口校验一次包摘要，再只向下游返回数据库数字包ID。"""
    from stock_a.sources.tdx.vipdata import vipdata_package_entry_sha256

    package_sha256 = vipdata_package_entry_sha256(package)
    byte_count = getattr(package, "file_size", None)
    if (
        not isinstance(package_sha256, str)
        or _PACKAGE_SHA256.fullmatch(package_sha256) is None
        or type(byte_count) is not int
        or byte_count <= 0
    ):
        raise RawSourcePgError("PACKAGE_NOT_VERIFIED", "原始包接收记录缺少有效字节校验")
    with _transaction(config, write=True) as (_connection, cursor, _scope):
        # 包登记频率低；受控表锁将入口字节校验与数字ID分配串行化。
        cursor.execute("LOCK TABLE source_packages IN SHARE ROW EXCLUSIVE MODE")
        cursor.execute(
            """SELECT source_package_id, byte_count, package_sha256::text
FROM source_packages WHERE dataset_id=%s ORDER BY source_package_id""",
            (_DATASET_ID,),
        )
        for existing_id, existing_bytes, existing_sha in cursor.fetchall():
            if str(existing_sha).strip() != package_sha256:
                continue
            if existing_bytes is not None and int(existing_bytes) != byte_count:
                raise RawSourcePgError(
                    "SOURCE_PACKAGE_CONFLICT",
                    "相同TDX包字节摘要对应的登记行大小不一致",
                )
            if existing_bytes is None:
                cursor.execute(
                    "UPDATE source_packages SET byte_count=%s WHERE source_package_id=%s AND byte_count IS NULL",
                    (byte_count, existing_id),
                )
                if cursor.rowcount != 1:
                    raise RawSourcePgError(
                        "SOURCE_PACKAGE_CONFLICT",
                        "历史来源包大小补录未能通过原件入口校验写入",
                    )
            return int(existing_id)
        cursor.execute(
            """INSERT INTO source_packages(dataset_id, package_sha256, byte_count)
VALUES (%s, %s, %s)
RETURNING source_package_id, byte_count""",
            (_DATASET_ID, package_sha256, byte_count),
        )
        row = cursor.fetchone()
        if row is None or int(row[1]) != byte_count:
            raise RawSourcePgError(
                "SOURCE_PACKAGE_CONFLICT", "同一已校验原始包的登记行与文件字节数不一致"
            )
        return int(row[0])


def _register_source_capture(
    config: Mapping[str, Any],
    *,
    source_package_id: int,
    target_session: date,
    read_started_at: datetime,
    validated_at: datetime,
    normalized_count: int,
) -> dict[str, Any]:
    with _transaction(config, write=True) as (_connection, cursor, _scope):
        cursor.execute(
            """INSERT INTO source_captures(
  source_package_id, target_session, parser_contract, read_started_at, validated_at,
  source_available_at, source_time_quality, source_clock_role, normalized_count
) VALUES (%s, %s, %s, %s, %s, NULL, 'NON_PIT_OBSERVED', %s, %s)
ON CONFLICT (source_package_id, parser_contract, target_session) DO NOTHING""",
            (
                source_package_id,
                target_session,
                _PARSER_CONTRACT,
                read_started_at,
                validated_at,
                _CLOCK_ROLE,
                normalized_count,
            ),
        )
        capture = _read_capture(cursor, source_package_id, target_session)
        if (
            capture is None
            or capture["parser_contract"] != _PARSER_CONTRACT
            or capture["normalized_count"] != normalized_count
            or capture["source_time_quality"] != "NON_PIT_OBSERVED"
            or capture["source_clock_role"] != _CLOCK_ROLE
        ):
            raise RawSourcePgError(
                "SOURCE_CAPTURE_CONFLICT", "同一来源包/目标日捕获合同或完整分母不一致"
            )
        return capture


def _field_value(field: Any) -> Any | None:
    if field is None or getattr(field, "quality", None) != "OBSERVED":
        return None
    return _finite_decimal(field.value)


def _field_quality(field: Any, *, reason_fallback: str) -> tuple[str, str | None]:
    quality = getattr(field, "quality", None)
    reason = getattr(field, "reason", None)
    if quality == "OBSERVED":
        return "RAW_SOURCE_VALUE_OBSERVED", None
    if isinstance(reason, str) and reason.startswith("DATA_INVALID_"):
        return "BAD_VALUE", reason
    return "UNKNOWN", reason if isinstance(reason, str) and reason else reason_fallback


def _normalized_quality(
    quality: str | None, reason: str | None, *, fallback: str
) -> tuple[str, str | None]:
    if quality in _QUALITY:
        if quality == "OBSERVED":
            return quality, None
        return quality, reason or fallback
    return "UNKNOWN", reason or fallback


def _raw_candidate(
    row: Any,
    capture: Any | None,
    *,
    candidate: Mapping[str, Any],
    source_package_id: int,
    capture_id: int,
    target_session: date,
) -> dict[str, Any]:
    status = row.status
    if status not in _BAR_STATUSES:
        raise RawSourcePgError(
            "SCAN_STATUS_UNSUPPORTED", "目标日扫描出现不可持久化状态"
        )
    if (
        row.symbol != candidate["symbol"]
        or row.market != candidate["market"]
        or row.board != candidate["board"]
    ):
        raise RawSourcePgError(
            "CANDIDATE_BINDING_MISMATCH", "目标日质量行与动态候选目录不匹配"
        )
    if row.target_session != target_session:
        raise RawSourcePgError(
            "SOURCE_REVISION_MISMATCH", "目标日质量行与本次会话不匹配"
        )

    target = capture.target_bar if capture is not None else None
    if status == "TARGET_PRESENT" and target is None:
        raise RawSourcePgError("TARGET_BAR_MISSING", "扫描报告有目标Bar但捕获对象缺失")
    if status != "TARGET_PRESENT" and target is not None:
        raise RawSourcePgError("TARGET_BAR_CONFLICT", "非存在状态不能携带目标Bar")
    reason = row.reason
    if status == "TARGET_PRESENT" and reason is not None:
        raise RawSourcePgError("TARGET_REASON_CONFLICT", "存在目标Bar不能携带缺失原因")
    if status != "TARGET_PRESENT" and not (isinstance(reason, str) and reason.strip()):
        raise RawSourcePgError(
            "TARGET_REASON_MISSING", "缺Bar或读取失败必须保留明确原因"
        )

    if target is None:
        raw_prices = (None, None, None, None)
        price_quality, price_reason = "UNKNOWN", reason
        raw_amount = raw_volume = None
        raw_amount_quality = raw_volume_quality = "UNKNOWN"
        raw_amount_reason = raw_volume_reason = reason
        amount_quality, amount_reason = "UNKNOWN", reason
        volume_quality, volume_reason = "UNKNOWN", reason
    else:
        price_quality, price_reason = _normalized_quality(
            row.price_quality,
            row.price_reason,
            fallback="PRICE_QUALITY_UNKNOWN",
        )
        raw_prices = tuple(
            _field_value(getattr(target, name)) if price_quality == "OBSERVED" else None
            for name in ("raw_open", "raw_high", "raw_low", "raw_close")
        )
        amount_quality, amount_reason = _normalized_quality(
            row.amount_quality,
            row.amount_reason,
            fallback="AMOUNT_QUALITY_UNKNOWN",
        )
        volume_quality, volume_reason = _normalized_quality(
            row.volume_quality,
            row.volume_reason,
            fallback="VOLUME_QUALITY_UNKNOWN",
        )
        raw_amount_field = getattr(target, "raw_amount", None)
        raw_volume_field = getattr(target, "raw_volume", None)
        raw_amount = _field_value(raw_amount_field)
        raw_volume = (
            getattr(raw_volume_field, "value", None)
            if getattr(raw_volume_field, "quality", None) == "OBSERVED"
            else None
        )
        raw_amount_quality, raw_amount_reason = _field_quality(
            raw_amount_field,
            reason_fallback="RAW_AMOUNT_NOT_OBSERVED",
        )
        raw_volume_quality, raw_volume_reason = _field_quality(
            raw_volume_field,
            reason_fallback="RAW_VOLUME_NOT_OBSERVED",
        )
        if raw_volume is not None and type(raw_volume) is not int:
            raise RawSourcePgError("RAW_VOLUME_INVALID", "TDX来源原始数量必须为整数")
        if raw_volume is not None and raw_volume < 0:
            raise RawSourcePgError("RAW_VOLUME_INVALID", "TDX来源原始数量不能为负数")

    source_ref = capture.capture.source_package_entry if capture is not None else None
    if source_ref is not None and source_ref.source_symbol != row.symbol:
        raise RawSourcePgError("ENTRY_SYMBOL_MISMATCH", "条目捕获没有绑定本次来源代码")
    if (
        capture is None
        and status == "TARGET_MISSING"
        and not candidate.get("empty_placeholder")
    ):
        raise RawSourcePgError(
            "MISSING_CAPTURE", "非空缺Bar候选必须保留完整条目捕获和质量"
        )

    start = min((row.observed_started_at,), default=None)
    # 运行级捕获时钟在最终扫描闭合后绑定，暂存时不写库。
    values = {
        "source_symbol": row.symbol,
        "market": row.market,
        "board": row.board,
        "session_date": target_session,
        "source_package_id": source_package_id,
        "source_capture_id": capture_id,
        "source_entry_bytes": int(candidate["zip_entry_bytes"]),
        "source_record_count": int(row.record_count),
        "bar_status": status,
        "bar_reason": reason,
        "raw_open": raw_prices[0],
        "raw_high": raw_prices[1],
        "raw_low": raw_prices[2],
        "raw_close": raw_prices[3],
        "price_quality": price_quality,
        "price_reason": price_reason,
        "raw_amount": raw_amount,
        "raw_amount_quality": raw_amount_quality,
        "raw_amount_reason": raw_amount_reason,
        "amount_quality": amount_quality,
        "amount_reason": amount_reason,
        "amount_unit_status": "UNIT_UNVERIFIED",
        "raw_volume": raw_volume,
        "raw_volume_quality": raw_volume_quality,
        "raw_volume_reason": raw_volume_reason,
        "volume_quality": volume_quality,
        "volume_reason": volume_reason,
        "volume_unit_status": "UNIT_UNVERIFIED",
        "first_source_date": row.first_date,
        "last_source_date": row.last_date,
        "future_source_rows_excluded": int(row.future_rows_excluded),
        "observed_started_at": start,
        "observed_completed_at": None,
        "source_clock_role": _CLOCK_ROLE,
        "source_available_at": None,
    }
    return values


def _validate_raw_candidate(row: Mapping[str, Any]) -> None:
    if set(row) != set(_RAW_COLUMNS):
        raise RawSourcePgError(
            "RAW_ROW_SHAPE_INVALID", "Raw候选列集合与typed表合同不一致"
        )
    if not _SYMBOL.fullmatch(row["source_symbol"]):
        raise RawSourcePgError(
            "RAW_IDENTITY_INVALID",
            "Raw来源必须保留合法TDX来源代码，不从首次观测推断稳定证券ID",
        )
    if row["market"] != row["source_symbol"][:2] or row["board"] not in {
        "MAIN_BOARD",
        "GEM",
    }:
        raise RawSourcePgError(
            "RAW_MARKET_INVALID", "Raw来源市场或板别超出当前沪深主板/GEM范围"
        )
    if (
        type(row["source_package_id"]) is not int
        or row["source_package_id"] <= 0
        or type(row["source_capture_id"]) is not int
        or row["source_capture_id"] <= 0
    ):
        raise RawSourcePgError(
            "RAW_REVISION_INVALID", "Raw行必须绑定正数来源包ID与捕获ID"
        )
    if row["bar_status"] not in _BAR_STATUSES:
        raise RawSourcePgError("RAW_BAR_STATUS_INVALID", "目标Bar状态不在固定枚举范围")
    if (
        row["price_quality"] not in _QUALITY
        or row["amount_quality"] not in _QUALITY
        or row["volume_quality"] not in _QUALITY
    ):
        raise RawSourcePgError(
            "RAW_FIELD_QUALITY_INVALID", "Raw逐字段质量不在固定枚举范围"
        )
    if (
        row["raw_amount_quality"] not in _RAW_QUALITY
        or row["raw_volume_quality"] not in _RAW_QUALITY
    ):
        raise RawSourcePgError(
            "RAW_SOURCE_VALUE_QUALITY_INVALID", "Raw来源数值质量不在固定枚举范围"
        )


def _db_scalar(value: Any) -> Any:
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise RawSourcePgError("RAW_DB_NUMERIC_INVALID", "PG返回了非有限数值")
        normalized = value.normalize()
        return "0" if normalized == 0 else format(normalized, "f")
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat(timespec="microseconds")
    if isinstance(value, date):
        return value.isoformat()
    return value


def _same_raw_rows(
    expected: Sequence[Mapping[str, Any]], actual: Sequence[Mapping[str, Any]]
) -> bool:
    if len(expected) != len(actual):
        return False
    expected_by_symbol = {row["source_symbol"]: row for row in expected}
    actual_by_symbol = {row["source_symbol"]: row for row in actual}
    return expected_by_symbol.keys() == actual_by_symbol.keys() and all(
        all(
            _db_scalar(expected_by_symbol[symbol][column])
            == _db_scalar(actual_by_symbol[symbol][column])
            for column in _RAW_COLUMNS
        )
        for symbol in expected_by_symbol
    )


def _row_values(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(row[column] for column in _RAW_COLUMNS)


def _read_raw_rows(
    cursor: Any, target_session: date, source_package_id: int
) -> list[dict[str, Any]]:
    cursor.execute(
        f"SELECT {_RAW_SELECT} FROM source_raw_daily "
        "WHERE session_date=%s AND source_package_id=%s ORDER BY source_symbol",
        (target_session, source_package_id),
    )
    return [
        dict(zip(_RAW_COLUMNS, values, strict=True)) for values in cursor.fetchall()
    ]


def _read_capture(
    cursor: Any, source_package_id: int, target_session: date
) -> dict[str, Any] | None:
    cursor.execute(
        """SELECT capture_id, parser_contract, read_started_at, validated_at,
source_available_at, source_time_quality, source_clock_role, normalized_count
FROM source_captures
WHERE source_package_id=%s AND target_session=%s
ORDER BY capture_id""",
        (source_package_id, target_session),
    )
    rows = cursor.fetchall()
    if not rows:
        return None
    if len(rows) != 1:
        raise RawSourcePgError(
            "CAPTURE_IDENTITY_AMBIGUOUS", "来源包与目标交易日对应多个有效捕获记录"
        )
    capture_id, parser, started, validated, available, quality, role, count = rows[0]
    return {
        "capture_id": int(capture_id),
        "parser_contract": parser,
        "read_started_at": started,
        "validated_at": validated,
        "source_available_at": available,
        "source_time_quality": quality,
        "source_clock_role": role,
        "normalized_count": int(count),
    }


def _complete_version_state(
    config: Mapping[str, Any],
    *,
    source_package_id: int,
    target_session: date,
    pool: Any,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    with _transaction(config, write=False) as (_connection, cursor, _scope):
        capture = _read_capture(cursor, source_package_id, target_session)
        rows = _read_raw_rows(cursor, target_session, source_package_id)
        if capture is None:
            if rows:
                raise RawSourcePgError(
                    "RAW_CAPTURE_MISSING", "现有来源Raw行没有对应的精确捕获记录"
                )
            return None, None
        if capture["parser_contract"] != _PARSER_CONTRACT or capture[
            "normalized_count"
        ] != len(pool.candidates):
            raise RawSourcePgError(
                "CAPTURE_REVISION_CONFLICT",
                "同一来源包/目标日的捕获合同或完整候选分母与当前输入冲突",
            )
        if len(rows) > capture["normalized_count"]:
            raise RawSourcePgError(
                "RAW_ROW_COUNT_EXCEEDED", "同一来源版本的持久化候选数超过捕获分母"
            )
        return {"rows": rows, "row_count": len(rows)}, capture


def _insert_raw_batch(
    config: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    *,
    chunk_size: int,
) -> int:
    if not 1 <= len(rows) <= chunk_size <= 100:
        raise RawSourcePgError(
            "RAW_CHUNK_INVALID", "Raw批次必须为1至100条且不超过显式chunk_size"
        )
    first = rows[0]
    symbols = [row["source_symbol"] for row in rows]
    if len(symbols) != len(set(symbols)) or any(
        row["session_date"] != first["session_date"]
        or row["source_package_id"] != first["source_package_id"]
        or row["source_capture_id"] != first["source_capture_id"]
        for row in rows
    ):
        raise RawSourcePgError(
            "RAW_CHUNK_SCOPE_INVALID", "Raw批次跨越来源版本或重复来源代码"
        )
    for row in rows:
        _validate_raw_candidate(row)
    with _transaction(config, write=True) as (_connection, cursor, _scope):
        inserted_count = 0
        for row in rows:
            cursor.execute(_RAW_INSERT, _row_values(row))
            inserted_count += max(0, int(cursor.rowcount))
        cursor.execute(
            f"SELECT {_RAW_SELECT} FROM source_raw_daily "
            "WHERE session_date=%s AND source_package_id=%s "
            "AND source_symbol = ANY(%s) ORDER BY source_symbol",
            (
                first["session_date"],
                first["source_package_id"],
                symbols,
            ),
        )
        actual_rows = [
            dict(zip(_RAW_COLUMNS, values, strict=True)) for values in cursor.fetchall()
        ]
        if not _same_raw_rows(rows, actual_rows):
            raise RawSourcePgError(
                "RAW_REVISION_CONFLICT",
                "同一来源包/代码/交易日已存在不同Raw值；事务已回滚",
            )
    return inserted_count


def _existing_count(
    config: Mapping[str, Any],
    target_session: date,
    source_package_id: int,
) -> int:
    with _transaction(config, write=False) as (_connection, cursor, _scope):
        cursor.execute(
            "SELECT count(*) FROM source_raw_daily WHERE session_date=%s AND source_package_id=%s",
            (target_session, source_package_id),
        )
        return int(cursor.fetchone()[0])


def import_verified_vipdata_target(
    config: Mapping[str, Any],
    package: Any,
    target_session: date,
    *,
    task_scope: str,
    scan_budget: Any,
    chunk_size: int = 100,
) -> dict[str, Any]:
    """重用VIPDATA单遍评价器，把目标日候选写入版本化Raw typed行。"""
    from stock_a.sources.tdx.day import UnitContract
    from stock_a.sources.tdx.vipdata import (
        VipdataTargetScanBudget,
        scan_vipdata_target_quality,
        vipdata_research_candidate_pool,
    )

    _validate_source_input(package, target_session, task_scope=task_scope)
    if (
        not isinstance(scan_budget, VipdataTargetScanBudget)
        or not 1 <= chunk_size <= 100
    ):
        raise RawSourcePgError(
            "SCAN_BUDGET_INVALID", "来源扫描预算或PG写入chunk_size不符合有界合同"
        )
    if config.get("paths", {}).get("temporary_root") != "G:/codex_tmp":
        raise RawSourcePgError("TEMP_ROOT_REJECTED", "项目配置临时根不是G:/codex_tmp")
    pool = vipdata_research_candidate_pool(package)
    total = len(pool.candidates)
    validate_source_candidate_limit(total)

    source_package_id = _register_source_package(config, package)
    prior_state, prior_capture = _complete_version_state(
        config,
        source_package_id=source_package_id,
        target_session=target_session,
        pool=pool,
    )
    if prior_state is not None and prior_state["row_count"] == total:
        return {
            "status": "ALREADY_COMPLETE",
            "target_session": target_session.isoformat(),
            "source_package_id": source_package_id,
            "candidate_total": total,
            "row_count": total,
            "source_scan_reused": False,
            "source_zip_reopened_for_parse": False,
            "database_writes": 0,
            "storage_location_status": "USER_CONFIRMED_NOT_INDEPENDENTLY_VERIFIED",
        }

    expected_entry = {item["symbol"]: item for item in pool.candidates}
    staged: list[dict[str, Any]] = []
    scan_started = datetime.now(timezone.utc)
    observed_starts: list[datetime] = []
    observed_finishes: list[datetime] = []

    def on_candidate(quality_row: Any, entry_capture: Any | None) -> None:
        candidate = expected_entry.get(quality_row.symbol)
        if candidate is None:
            raise RawSourcePgError(
                "CANDIDATE_NOT_FOUND", "扫描返回了不在本次ZIP动态池中的来源代码"
            )
        if quality_row.observed_started_at is not None:
            observed_starts.append(quality_row.observed_started_at)
        if quality_row.observed_completed_at is not None:
            observed_finishes.append(quality_row.observed_completed_at)
        staged.append(
            _raw_candidate(
                quality_row,
                entry_capture,
                candidate=candidate,
                source_package_id=source_package_id,
                capture_id=(prior_capture["capture_id"] if prior_capture else None),
                target_session=target_session,
            )
        )
        if len(staged) > 5000:
            raise RawSourcePgError(
                "CANDIDATE_LIMIT_EXCEEDED", "扫描回调观察行超过5000条上限"
            )

    report = scan_vipdata_target_quality(
        package,
        pool,
        target_session=target_session,
        units=UnitContract("TDX_RAW_SOURCE_VALUES_UNIT_UNVERIFIED_R1", None, None),
        budget=scan_budget,
        on_candidate=on_candidate,
    )
    if (
        report.candidate_total != total
        or len(staged) != total
        or report.not_evaluated != 0
        or report.evaluated != total
        or report.target_present + report.target_missing + report.read_failed != total
    ):
        raise RawSourcePgError(
            "TARGET_SCAN_INCOMPLETE", "全来源目标日扫描分母未闭合；未写入捕获或Raw行"
        )
    scan_finished = datetime.now(timezone.utc)
    if observed_starts:
        scan_started = min(observed_starts)
    if observed_finishes:
        scan_finished = max(observed_finishes)
    scan_started, scan_finished = _aware(scan_started), _aware(scan_finished)

    for row in staged:
        row["observed_started_at"] = scan_started
        row["observed_completed_at"] = scan_finished
    staged.sort(key=lambda row: row["source_symbol"])
    if len({row["source_symbol"] for row in staged}) != total:
        raise RawSourcePgError(
            "CANDIDATE_DUPLICATE", "动态候选池出现重复来源代码；拒绝进入PG"
        )
    if prior_capture is not None:
        for row in staged:
            row["observed_started_at"] = prior_capture["read_started_at"]
            row["observed_completed_at"] = prior_capture["validated_at"]
    capture = _register_source_capture(
        config,
        source_package_id=source_package_id,
        target_session=target_session,
        read_started_at=(
            prior_capture["read_started_at"] if prior_capture else scan_started
        ),
        validated_at=(
            prior_capture["validated_at"] if prior_capture else scan_finished
        ),
        normalized_count=total,
    )
    capture_id = capture["capture_id"]
    for row in staged:
        row["source_capture_id"] = capture_id
        _validate_raw_candidate(row)

    before = prior_state["row_count"] if prior_state is not None else 0
    committed = 0
    try:
        for start in range(0, len(staged), chunk_size):
            committed += _insert_raw_batch(
                config,
                staged[start : start + chunk_size],
                chunk_size=chunk_size,
            )
    except RawSourcePgError as error:
        count = _existing_count(config, target_session, source_package_id)
        raise RawSourcePgError(
            error.code,
            str(error),
            committed_rows=count,
            metadata_committed=prior_capture is not None or count > 0,
        ) from error
    except Exception as error:
        count = _existing_count(config, target_session, source_package_id)
        raise RawSourcePgError(
            "PG_WRITE_FAILED",
            f"Raw分块写入中断（{type(error).__name__}）",
            committed_rows=count,
            metadata_committed=prior_capture is not None or count > 0,
        ) from error

    after = _existing_count(config, target_session, source_package_id)
    if after != total:
        raise RawSourcePgError(
            "RAW_ROW_COUNT_INCOMPLETE",
            "Raw入库结束行数与候选分母不一致",
            committed_rows=after,
            metadata_committed=True,
        )
    persisted, _persisted_capture = _complete_version_state(
        config,
        source_package_id=source_package_id,
        target_session=target_session,
        pool=pool,
    )
    if (
        persisted is None
        or persisted["row_count"] != total
        or not _same_raw_rows(staged, persisted["rows"])
    ):
        raise RawSourcePgError(
            "RAW_FINAL_READBACK_MISMATCH",
            "Raw全量PG独立读回与本次逐字段期望不一致",
            committed_rows=after,
            metadata_committed=True,
        )
    return {
        "status": "COMPLETE"
        if report.read_failed == 0
        else "COMPLETE_WITH_READ_FAILURES",
        "target_session": target_session.isoformat(),
        "source_package_id": source_package_id,
        "candidate_total": total,
        "target_present": report.target_present,
        "target_missing": report.target_missing,
        "read_failed": report.read_failed,
        "not_evaluated": report.not_evaluated,
        "source_bytes_read": report.bytes_read,
        "source_records_read": report.records_read,
        "scan_elapsed_seconds": report.elapsed_seconds,
        "row_count_before": before,
        "row_count_after": after,
        "new_rows_committed": max(0, after - before),
        "source_capture_id": capture_id,
        "source_clock_role": _CLOCK_ROLE,
        "source_available_at": None,
        "unit_status": "UNIT_UNVERIFIED",
        "stable_security_identity_issued": False,
        "canonical_or_foundation_written": False,
        "storage_location_status": "USER_CONFIRMED_NOT_INDEPENDENTLY_VERIFIED",
    }


def import_verified_vipdata_history(
    config: Mapping[str, Any],
    package: Any,
    sessions: Sequence[date],
    *,
    task_scope: str,
    scan_budget: Any,
    chunk_size: int = 100,
    max_raw_rows: int = 750_000,
) -> dict[str, Any]:
    """单遍扫描一组官方会话，并按会话追加到既有Raw typed表。"""
    from bisect import bisect_right
    from dataclasses import replace

    from stock_a.sources.tdx.day import UnitContract
    from stock_a.sources.tdx.vipdata import (
        VipdataTargetScanBudget,
        _target_quality_row,
        build_vipdata_entry_capture_for_target,
        scan_vipdata_target_quality,
        vipdata_research_candidate_pool,
    )

    if (
        not isinstance(sessions, (tuple, list))
        or not sessions
        or len(sessions) > 75
        or any(type(day) is not date for day in sessions)
        or tuple(sorted(set(sessions))) != tuple(sessions)
    ):
        raise RawSourcePgError(
            "HISTORY_SESSION_WINDOW_INVALID",
            "真实历史会话须为1至75个严格递增且不重复的date",
        )
    _validate_source_input(package, sessions[-1], task_scope=task_scope)
    if (
        not isinstance(scan_budget, VipdataTargetScanBudget)
        or not 1 <= chunk_size <= 100
    ):
        raise RawSourcePgError(
            "SCAN_BUDGET_INVALID", "历史来源扫描预算或PG分块大小无效"
        )
    if config.get("paths", {}).get("temporary_root") != "G:/codex_tmp":
        raise RawSourcePgError(
            "TEMP_ROOT_REJECTED", "项目临时目录必须固定为G:/codex_tmp"
        )

    pool = vipdata_research_candidate_pool(package)
    total = len(pool.candidates)
    validate_source_candidate_limit(total)
    planned_rows = total * len(sessions)
    if (
        type(max_raw_rows) is not int
        or planned_rows > max_raw_rows
        or planned_rows > 750_000
    ):
        raise RawSourcePgError(
            "HISTORY_ROW_BUDGET_EXCEEDED",
            f"历史Raw行预算拒绝完整候选集：{total}个来源候选×{len(sessions)}个会话={planned_rows}行",
        )

    source_package_id = _register_source_package(config, package)
    prior_by_session: dict[
        date, tuple[dict[str, Any] | None, dict[str, Any] | None]
    ] = {}
    for session in sessions:
        prior_state, prior_capture = _complete_version_state(
            config,
            source_package_id=source_package_id,
            target_session=session,
            pool=pool,
        )
        prior_by_session[session] = (prior_state, prior_capture)

    if all(
        state is not None and state["row_count"] == total
        for state, _capture in prior_by_session.values()
    ):
        existing = []
        for session in sessions:
            state, capture = prior_by_session[session]
            assert state is not None and capture is not None
            existing.append(
                {
                    "target_session": session.isoformat(),
                    "candidate_total": total,
                    "row_count_before": total,
                    "row_count_after": total,
                    "new_rows_committed": 0,
                    "source_capture_id": capture["capture_id"],
                    "status": "ALREADY_COMPLETE",
                }
            )
        return {
            "status": "ALREADY_COMPLETE",
            "target_session": sessions[-1].isoformat(),
            "session_start": sessions[0].isoformat(),
            "session_count": len(sessions),
            "source_package_id": source_package_id,
            "candidate_total": total,
            "planned_raw_rows": planned_rows,
            "source_scan_reused": False,
            "source_package_parse_passes": 0,
            "per_session_full_package_rescans": 0,
            "source_bytes_read": 0,
            "source_records_read": 0,
            "session_results": existing,
            "database_writes": 0,
            "storage_location_status": "USER_CONFIRMED_NOT_INDEPENDENTLY_VERIFIED",
        }

    candidate_by_symbol = {item["symbol"]: item for item in pool.candidates}
    session_set = set(sessions)
    staged_entries: dict[str, dict[str, Any]] = {}
    observed_starts: list[datetime] = []
    observed_finishes: list[datetime] = []

    def collect_candidate(quality_row: Any, entry_capture: Any | None) -> None:
        symbol = quality_row.symbol
        if symbol not in candidate_by_symbol or symbol in staged_entries:
            raise RawSourcePgError(
                "CANDIDATE_BINDING_MISMATCH", "单遍来源结果不属于唯一动态候选目录"
            )
        if quality_row.observed_started_at is not None:
            observed_starts.append(quality_row.observed_started_at)
        if quality_row.observed_completed_at is not None:
            observed_finishes.append(quality_row.observed_completed_at)
        if entry_capture is None:
            staged_entries[symbol] = {
                "quality": quality_row,
                "scan_summary": None,
                "bars": {},
                "future_counts": {},
            }
            return
        if not getattr(entry_capture.scan_summary, "complete", False):
            raise RawSourcePgError("SOURCE_SCAN_INCOMPLETE", "来源条目没有完成单遍解码")
        record_dates = [decoded.session_date for decoded in entry_capture.rows]
        if record_dates != sorted(record_dates) or len(record_dates) != len(
            set(record_dates)
        ):
            raise RawSourcePgError(
                "SOURCE_HISTORY_ORDER_INVALID", "VIPDATA条目日期必须有序且唯一"
            )
        future_counts: dict[date, int] = {}
        for session in sessions:
            future_counts[session] = len(record_dates) - bisect_right(
                record_dates, session
            )
        bars = {
            decoded.session_date: decoded
            for decoded in entry_capture.rows
            if decoded.session_date in session_set
        }
        staged_entries[symbol] = {
            "quality": quality_row,
            "scan_summary": entry_capture.scan_summary,
            "bars": bars,
            "future_counts": future_counts,
        }

    scan_started_wall = time.monotonic()
    scan_report = scan_vipdata_target_quality(
        package,
        pool,
        target_session=sessions[-1],
        units=UnitContract("TDX_RAW_SOURCE_VALUES_UNIT_UNVERIFIED_R1", None, None),
        budget=scan_budget,
        on_candidate=collect_candidate,
    )
    scan_elapsed = time.monotonic() - scan_started_wall
    if (
        scan_report.candidate_total != total
        or len(staged_entries) != total
        or scan_report.evaluated != total
        or scan_report.not_evaluated != 0
        or scan_report.target_present
        + scan_report.target_missing
        + scan_report.read_failed
        != total
    ):
        raise RawSourcePgError(
            "TARGET_SCAN_INCOMPLETE", "历史单遍扫描候选分母没有闭合，拒绝持久化"
        )

    scan_started = (
        min(observed_starts) if observed_starts else datetime.now(timezone.utc)
    )
    scan_finished = (
        max(observed_finishes) if observed_finishes else datetime.now(timezone.utc)
    )
    scan_started, scan_finished = _aware(scan_started), _aware(scan_finished)
    session_results: list[dict[str, Any]] = []
    committed_total = 0

    for session in sessions:
        prior_state, prior_capture = prior_by_session[session]
        staged: list[dict[str, Any]] = []
        status_counts = {
            "TARGET_PRESENT": 0,
            "TARGET_MISSING": 0,
            "READ_FAILED": 0,
            "NOT_EVALUATED": 0,
        }
        for source_index, candidate in enumerate(pool.candidates):
            entry = staged_entries[candidate["symbol"]]
            base_quality = entry["quality"]
            scan_summary = entry["scan_summary"]
            if scan_summary is None:
                quality = replace(base_quality, target_session=session)
                capture = None
            else:
                target_bar = entry["bars"].get(session)
                capture = build_vipdata_entry_capture_for_target(
                    package,
                    candidate,
                    scan_summary=scan_summary,
                    target_session=session,
                    target_bar=target_bar,
                    future_source_rows_excluded=entry["future_counts"][session],
                )
                quality = _target_quality_row(
                    candidate,
                    target_session=session,
                    source_index=source_index,
                    page_offset=source_index,
                    capture=capture,
                    status="TARGET_PRESENT"
                    if target_bar is not None
                    else "TARGET_MISSING",
                    reason=None
                    if target_bar is not None
                    else "TARGET_SESSION_BAR_MISSING",
                    bytes_read=scan_summary.byte_count,
                    record_count=scan_summary.record_count,
                )
            status_counts[quality.status] = status_counts.get(quality.status, 0) + 1
            staged.append(
                _raw_candidate(
                    quality,
                    capture,
                    candidate=candidate,
                    source_package_id=source_package_id,
                    capture_id=(prior_capture["capture_id"] if prior_capture else None),
                    target_session=session,
                )
            )

        if (
            len(staged) != total
            or status_counts["NOT_EVALUATED"]
            or sum(status_counts.values()) != total
        ):
            raise RawSourcePgError(
                "TARGET_SCAN_INCOMPLETE", f"{session.isoformat()}来源候选状态分母未闭合"
            )
        staged.sort(key=lambda row: row["source_symbol"])
        if len({row["source_symbol"] for row in staged}) != total:
            raise RawSourcePgError(
                "CANDIDATE_DUPLICATE", "历史候选出现重复来源代码，拒绝写入PG"
            )

        effective_started = (
            prior_capture["read_started_at"]
            if prior_capture is not None
            else scan_started
        )
        effective_finished = (
            prior_capture["validated_at"]
            if prior_capture is not None
            else scan_finished
        )
        for row in staged:
            row["observed_started_at"] = effective_started
            row["observed_completed_at"] = effective_finished
        capture_row = _register_source_capture(
            config,
            source_package_id=source_package_id,
            target_session=session,
            read_started_at=effective_started,
            validated_at=effective_finished,
            normalized_count=total,
        )
        capture_id = capture_row["capture_id"]
        for row in staged:
            row["source_capture_id"] = capture_id
            _validate_raw_candidate(row)

        before = prior_state["row_count"] if prior_state is not None else 0
        if before < total:
            try:
                for start in range(0, len(staged), chunk_size):
                    committed_total += _insert_raw_batch(
                        config,
                        staged[start : start + chunk_size],
                        chunk_size=chunk_size,
                    )
            except RawSourcePgError as error:
                committed_rows = _existing_count(config, session, source_package_id)
                committed_sessions = [
                    item["target_session"]
                    for item in session_results
                    if item.get("row_count_after") == total
                ]
                raise RawSourcePgError(
                    error.code,
                    f"历史会话{session.isoformat()}写入失败；此前已完成会话={len(committed_sessions)}；"
                    f"本会话PG现存行={committed_rows}/{total}；可按同包同窗幂等重试",
                    committed_rows=committed_rows,
                    metadata_committed=prior_capture is not None or committed_rows > 0,
                ) from error
        after = _existing_count(config, session, source_package_id)
        if after != total:
            raise RawSourcePgError(
                "RAW_ROW_COUNT_INCOMPLETE",
                f"{session.isoformat()}历史行数不完整：{after}/{total}",
                committed_rows=after,
                metadata_committed=True,
            )
        persisted, _persisted_capture = _complete_version_state(
            config,
            source_package_id=source_package_id,
            target_session=session,
            pool=pool,
        )
        if (
            persisted is None
            or persisted["row_count"] != total
            or not _same_raw_rows(staged, persisted["rows"])
        ):
            raise RawSourcePgError(
                "RAW_FINAL_READBACK_MISMATCH",
                f"{session.isoformat()}历史Raw完整逐字段读回不匹配",
                committed_rows=after,
                metadata_committed=True,
            )
        session_results.append(
            {
                "target_session": session.isoformat(),
                "candidate_total": total,
                "target_present": status_counts["TARGET_PRESENT"],
                "target_missing": status_counts["TARGET_MISSING"],
                "read_failed": status_counts["READ_FAILED"],
                "not_evaluated": status_counts["NOT_EVALUATED"],
                "row_count_before": before,
                "row_count_after": after,
                "new_rows_committed": max(0, after - before),
                "source_capture_id": capture_id,
                "status": "COMPLETE_WITH_READ_FAILURES"
                if status_counts["READ_FAILED"]
                else "COMPLETE",
            }
        )

    return {
        "status": "COMPLETE_WITH_READ_FAILURES"
        if scan_report.read_failed
        else "COMPLETE",
        "target_session": sessions[-1].isoformat(),
        "session_start": sessions[0].isoformat(),
        "session_count": len(sessions),
        "source_package_id": source_package_id,
        "candidate_total": total,
        "planned_raw_rows": planned_rows,
        "source_scan_reused": False,
        "source_package_parse_passes": 1,
        "per_session_full_package_rescans": 0,
        "source_bytes_read": scan_report.bytes_read,
        "source_records_read": scan_report.records_read,
        "source_scan_elapsed_seconds": round(scan_report.elapsed_seconds, 6),
        "source_scan_wall_seconds": round(scan_elapsed, 6),
        "database_committed_rows": committed_total,
        "session_results": session_results,
        "source_clock_role": _CLOCK_ROLE,
        "source_available_at": None,
        "unit_status": "UNIT_UNVERIFIED",
        "stable_security_identity_issued": False,
        "canonical_or_foundation_written": False,
        "storage_location_status": "USER_CONFIRMED_NOT_INDEPENDENTLY_VERIFIED",
    }


def read_raw_daily_stock(
    config: Mapping[str, Any],
    *,
    source_symbol: str,
    target_session: date,
    source_package_id: int,
) -> dict[str, Any] | None:
    if not isinstance(source_symbol, str) or _SYMBOL.fullmatch(source_symbol) is None:
        raise RawSourcePgError(
            "SOURCE_SYMBOL_INVALID", "PG来源Raw查询必须使用SH./SZ.来源代码"
        )
    if (
        type(target_session) is not date
        or type(source_package_id) is not int
        or source_package_id <= 0
    ):
        raise RawSourcePgError(
            "PG_QUERY_KEY_INVALID", "PG查询必须提供精确目标会话与正数来源包ID"
        )
    with _transaction(config, write=False) as (_connection, cursor, _scope):
        cursor.execute(
            """SELECT r.*, c.read_started_at AS capture_read_started_at,
       c.validated_at AS capture_validated_at, c.source_time_quality AS capture_time_quality,
       c.source_clock_role AS capture_clock_role, c.normalized_count AS capture_normalized_count,
       p.dataset_id, p.byte_count
FROM source_raw_daily r
 JOIN source_captures c ON c.capture_id=r.source_capture_id
  AND c.source_package_id=r.source_package_id AND c.target_session=r.session_date
 JOIN source_packages p ON p.source_package_id=r.source_package_id
 WHERE r.source_symbol=%s AND r.session_date=%s AND r.source_package_id=%s""",
            (source_symbol, target_session, source_package_id),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        names = [item.name for item in cursor.description]
        result = dict(zip(names, row, strict=True))
        if result["dataset_id"] != _DATASET_ID:
            raise RawSourcePgError(
                "SOURCE_REFERENCE_MISMATCH",
                "Raw事实连接到错误的来源包数据集",
            )
        result["source_package_id"] = int(result["source_package_id"])
        result["source_capture_id"] = int(result["source_capture_id"])
        result["identity_status"] = "SOURCE_SYMBOL_ONLY"
        result["source_object_status"] = "TDX_SOURCE_SYMBOL_OBSERVED"
        result["target_bar_status"] = {
            "TARGET_PRESENT": "TDX_TARGET_BAR_PRESENT",
            "TARGET_MISSING": "TDX_TARGET_BAR_MISSING",
            "READ_FAILED": "TDX_READ_FAILED",
        }[result["bar_status"]]
        result["stable_security_id_status"] = "STABLE_SECURITY_ID_UNVERIFIED"
        result["listing_status"] = "LISTING_STATUS_UNKNOWN"
        result["suspension_status"] = "SUSPENSION_STATUS_UNKNOWN"
        result["official_listing_universe_status"] = "UNKNOWN"
        result["pit_status"] = "NON_PIT_RETROSPECTIVE"
        result["price_coordinate"] = "RAW_UNADJUSTED"
        result["amount_unit_status"] = "UNIT_UNVERIFIED"
        result["volume_unit_status"] = "UNIT_UNVERIFIED"
        return result


def _validate_history_query(
    *,
    source_symbol: str | None,
    target_session: date,
    source_package_id: int,
    sessions: Sequence[date],
    starts_at_target: bool = False,
) -> tuple[date, ...]:
    if source_symbol is not None and (
        not isinstance(source_symbol, str) or _SYMBOL.fullmatch(source_symbol) is None
    ):
        raise RawSourcePgError(
            "SOURCE_SYMBOL_INVALID", "PG历史查询证券须为SH./SZ.来源代码"
        )
    if (
        type(target_session) is not date
        or type(source_package_id) is not int
        or source_package_id <= 0
    ):
        raise RawSourcePgError(
            "PG_QUERY_KEY_INVALID", "PG历史查询必须显式绑定目标会话和正数来源包ID"
        )
    if (
        not isinstance(sessions, (tuple, list))
        or not sessions
        or (starts_at_target and len(sessions) > 11)
        or any(type(day) is not date for day in sessions)
        or tuple(sorted(set(sessions))) != tuple(sessions)
        or (sessions[0] if starts_at_target else sessions[-1]) != target_session
    ):
        raise RawSourcePgError(
            "HISTORY_SESSION_WINDOW_INVALID",
            "PG会话窗口须递增；历史以目标日结束，后续最多11个并以目标日开始",
        )
    return tuple(sessions)


def _read_history_rows(
    cursor: Any,
    *,
    source_symbols: Sequence[str],
    sessions: Sequence[date],
    source_package_id: int,
    include_parser_contract: bool = False,
) -> list[dict[str, Any]]:
    if not source_symbols:
        return []
    # 每次读取的代码数随历史长度缩小，避免冷盘上单条查询跨越过多随机页。
    # 分块仍在同一只读快照内，并按代码排序，不能改变调用者的分母或输出次序。
    symbols = sorted(set(source_symbols))
    chunk_size = max(1, min(100, 1500 // max(1, len(sessions))))
    result = []
    for start in range(0, len(symbols), chunk_size):
        cursor.execute(
            f"""SELECT r.*, c.read_started_at AS capture_read_started_at,
       c.validated_at AS capture_validated_at, c.source_time_quality AS capture_time_quality,
       c.source_clock_role AS capture_clock_role,
       c.normalized_count AS capture_normalized_count{", c.parser_contract" if include_parser_contract else ""},
       p.dataset_id, p.byte_count
FROM source_raw_daily r
 JOIN source_captures c ON c.capture_id=r.source_capture_id
  AND c.source_package_id=r.source_package_id AND c.target_session=r.session_date
 JOIN source_packages p ON p.source_package_id=r.source_package_id
 WHERE r.source_symbol=ANY(%s) AND r.session_date=ANY(%s::date[])
  AND r.source_package_id=%s
ORDER BY r.source_symbol, r.session_date""",
            (
                symbols[start : start + chunk_size],
                list(sessions),
                source_package_id,
            ),
        )
        names = [item.name for item in cursor.description]
        result.extend(
            dict(zip(names, values, strict=True)) for values in cursor.fetchall()
        )
    for row in result:
        if (
            row["dataset_id"] != _DATASET_ID
            or int(row["source_package_id"]) != source_package_id
            or row["source_symbol"] not in source_symbols
            or row["session_date"] not in sessions
            or row["capture_normalized_count"] <= 0
        ):
            raise RawSourcePgError(
                "SOURCE_REFERENCE_MISMATCH",
                "PG历史Raw行与数字来源包及来源捕获不匹配",
            )
        if row["bar_status"] not in _BAR_STATUSES:
            raise RawSourcePgError(
                "SOURCE_STATUS_INVALID", "PG历史Raw包含未登记的来源状态"
            )
        row["source_package_id"] = int(row["source_package_id"])
        row["source_capture_id"] = int(row["source_capture_id"])
        row["identity_status"] = "SOURCE_SYMBOL_ONLY"
        row["source_object_status"] = "TDX_SOURCE_SYMBOL_OBSERVED"
        row["target_bar_status"] = {
            "TARGET_PRESENT": "TDX_TARGET_BAR_PRESENT",
            "TARGET_MISSING": "TDX_TARGET_BAR_MISSING",
            "READ_FAILED": "TDX_READ_FAILED",
        }[row["bar_status"]]
        row["stable_security_id_status"] = "STABLE_SECURITY_ID_UNVERIFIED"
        row["listing_status"] = "LISTING_STATUS_UNKNOWN"
        row["suspension_status"] = "SUSPENSION_STATUS_UNKNOWN"
        row["official_listing_universe_status"] = "UNKNOWN"
        row["pit_status"] = "NON_PIT_RETROSPECTIVE"
        row["price_coordinate"] = "RAW_UNADJUSTED"
        row["amount_unit_status"] = "UNIT_UNVERIFIED"
        row["volume_unit_status"] = "UNIT_UNVERIFIED"
    return result


def read_raw_daily_forward_pool(
    config: Mapping[str, Any],
    *,
    source_symbols: Sequence[str],
    sessions: Sequence[date],
    research_cutoff: date,
    source_package_id: int,
) -> dict[str, Any]:
    """一次只读事务批量读取已冻结来源池的T0及后继Raw行。"""
    days = tuple(sessions)
    symbols = tuple(source_symbols)
    if (
        not days
        or len(days) > 21
        or tuple(sorted(set(days))) != days
        or any(type(day) is not date for day in days)
        or type(research_cutoff) is not date
        or days[-1] > research_cutoff
        or not symbols
        or len(set(symbols)) != len(symbols)
        or any(
            not isinstance(symbol, str) or _SYMBOL.fullmatch(symbol) is None
            for symbol in symbols
        )
        or type(source_package_id) is not int
        or source_package_id <= 0
    ):
        raise RawSourcePgError("FORWARD_POOL_INPUT_INVALID", "后继来源池或会话绑定无效")
    with _transaction(config, write=False, statement_timeout_ms=30000) as (
        _connection,
        cursor,
        _scope,
    ):
        rows = _read_history_rows(
            cursor,
            source_symbols=symbols,
            sessions=days,
            source_package_id=source_package_id,
        )
        session_versions = {}
        for day in days:
            status, actual, expected = _page_version_status(
                cursor, day, source_package_id
            )
            session_versions[day.isoformat()] = {
                "status": status,
                "row_count": actual,
                "expected_count": expected,
            }
        cursor.execute(
            "SELECT max(session_date) FROM source_raw_daily "
            "WHERE source_package_id=%s AND session_date<=%s",
            (source_package_id, research_cutoff),
        )
        saved_through = cursor.fetchone()[0]
    grouped: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in symbols}
    for row in rows:
        grouped[row["source_symbol"]].append(row)
    return {
        "rows_by_symbol": grouped,
        "saved_through": saved_through,
        "session_versions": session_versions,
    }


def read_raw_daily_history_stock(
    config: Mapping[str, Any],
    *,
    source_symbol: str,
    target_session: date,
    source_package_id: int,
    sessions: Sequence[date],
    starts_at_target: bool = False,
    research_cutoff: date | None = None,
    include_parser_contract: bool = False,
) -> dict[str, Any]:
    """从固定stock业务schema读取单证券的精确包版本与官方会话窗口。"""
    selected_sessions = _validate_history_query(
        source_symbol=source_symbol,
        target_session=target_session,
        source_package_id=source_package_id,
        sessions=sessions,
        starts_at_target=starts_at_target,
    )
    if starts_at_target and (
        type(research_cutoff) is not date or selected_sessions[-1] > research_cutoff
    ):
        raise RawSourcePgError(
            "RESEARCH_CUTOFF_INVALID", "后续窗口必须绑定不早于窗口结束的研究截止"
        )
    with _transaction(config, write=False) as (_connection, cursor, _scope):
        cursor.execute(
            "SELECT count(*) FROM source_raw_daily "
            "WHERE source_symbol=%s AND session_date=%s AND source_package_id=%s",
            (source_symbol, target_session, source_package_id),
        )
        target_count = int(cursor.fetchone()[0])
        if target_count == 0:
            raise RawSourcePgError(
                "PG_SOURCE_NO_MATCH", "PG中没有该目标日与显式VIPDATA来源包ID的Raw候选"
            )
        if target_count != 1:
            raise RawSourcePgError(
                "PG_SOURCE_DUPLICATE", "PG目标日来源代码存在重复Raw行"
            )
        rows = _read_history_rows(
            cursor,
            source_symbols=(source_symbol,),
            sessions=selected_sessions,
            source_package_id=source_package_id,
            include_parser_contract=include_parser_contract,
        )
        saved_range = None
        if starts_at_target:
            cursor.execute(
                "SELECT min(session_date), max(session_date) FROM source_raw_daily "
                "WHERE source_symbol=%s AND source_package_id=%s AND session_date<=%s",
                (source_symbol, source_package_id, research_cutoff),
            )
            saved_range = cursor.fetchone()
    actual_sessions = tuple(row["session_date"] for row in rows)
    if actual_sessions != tuple(sorted(set(actual_sessions))) or len(
        actual_sessions
    ) != len(set(actual_sessions)):
        raise RawSourcePgError(
            "PG_HISTORY_SESSION_ORDER_INVALID", "PG历史Raw行重复或未按会话递增"
        )
    if target_session not in actual_sessions:
        raise RawSourcePgError(
            "PG_HISTORY_SESSION_MISSING",
            "PG历史窗口未返回显式目标日Raw行；拒绝用其它日期回退。",
        )
    missing = tuple(day for day in selected_sessions if day not in set(actual_sessions))
    return {
        "status": "AVAILABLE" if not missing else "PARTIAL_UNKNOWN",
        "source_symbol": source_symbol,
        "target_session": target_session.isoformat(),
        "source_package_id": source_package_id,
        "session_start": selected_sessions[0].isoformat(),
        "session_end": selected_sessions[-1].isoformat(),
        "session_count": len(selected_sessions),
        "history_rows": rows,
        "saved_session_range": (
            [day.isoformat() if day else None for day in saved_range]
            if saved_range is not None
            else None
        ),
        "history_complete": not missing,
        "history_status": "COMPLETE" if not missing else "UNKNOWN_HISTORY",
        "missing_history_sessions": [day.isoformat() for day in missing],
        "source_zip_opened": False,
        "source_recalculated": False,
        "ddl_executed": False,
        "database_write": False,
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "pit_status": "NON_PIT_RETROSPECTIVE",
    }


def read_raw_daily_history_batch(
    config: Mapping[str, Any],
    *,
    source_symbols: Sequence[str],
    target_session: date,
    research_cutoff: date,
    source_package_id: int,
    sessions: Sequence[date],
    expected_source_package_id: int,
    expected_target_capture_id: int,
    starts_at_target: bool = False,
) -> dict[str, Any]:
    """在单个只读事务中读取已展示来源代码的有界RAW会话窗口。"""
    selected_symbols = tuple(source_symbols)
    selected_sessions = _validate_history_query(
        source_symbol=None,
        target_session=target_session,
        source_package_id=source_package_id,
        sessions=sessions,
        starts_at_target=starts_at_target,
    )
    if (
        type(research_cutoff) is not date
        or target_session > research_cutoff
        or type(starts_at_target) is not bool
        or (starts_at_target and selected_sessions[-1] > research_cutoff)
        or not selected_symbols
        or len(selected_symbols) > 100
        or tuple(sorted(set(selected_symbols))) != selected_symbols
        or any(
            _PRICE_FACT_SYMBOL.fullmatch(symbol) is None for symbol in selected_symbols
        )
        or type(source_package_id) is not int
        or source_package_id <= 0
        or type(expected_source_package_id) is not int
        or expected_source_package_id != source_package_id
        or type(expected_target_capture_id) is not int
        or expected_target_capture_id <= 0
    ):
        raise RawSourcePgError(
            "PRICE_SAFETY_BATCH_SCOPE_INVALID",
            "价格事实RAW批量读取必须绑定截止日、数字来源包ID和最多100个去重来源代码",
        )

    with _transaction(config, write=False) as (_connection, cursor, _scope):
        cursor.execute(
            """SELECT r.source_symbol, r.session_date,
       r.source_package_id, r.source_capture_id,
       r.source_record_count, r.bar_status, r.bar_reason,
       r.raw_open, r.raw_high, r.raw_low, r.raw_close, r.price_quality,
       r.price_reason, c.normalized_count AS capture_normalized_count, p.dataset_id
FROM source_raw_daily r
JOIN source_captures c
   ON c.capture_id=r.source_capture_id AND c.source_package_id=r.source_package_id
  AND c.target_session=r.session_date
 JOIN source_packages p ON p.source_package_id=r.source_package_id
 WHERE r.source_symbol=ANY(%s)
  AND r.session_date=ANY(%s::date[]) AND r.source_package_id=%s
ORDER BY r.source_symbol, r.session_date""",
            (
                list(selected_symbols),
                list(selected_sessions),
                source_package_id,
            ),
        )
        names = [item.name for item in cursor.description]
        rows = [dict(zip(names, values, strict=True)) for values in cursor.fetchall()]
        for row in rows:
            if (
                row["dataset_id"] != _DATASET_ID
                or row["source_symbol"] not in selected_symbols
                or row["session_date"] not in selected_sessions
                or row["capture_normalized_count"] <= 0
                or row["bar_status"] not in _BAR_STATUSES
                or row["price_quality"] not in _QUALITY
            ):
                raise RawSourcePgError(
                    "SOURCE_REFERENCE_MISMATCH",
                    "价格事实历史行与包、来源捕获或内容元数据不匹配",
                )
            row["source_package_id"] = int(row["source_package_id"])
            row["source_capture_id"] = int(row["source_capture_id"])
            row["price_coordinate"] = "RAW_UNADJUSTED"

    rows_by_symbol = {symbol: [] for symbol in selected_symbols}
    for row in rows:
        if row["source_package_id"] != expected_source_package_id:
            raise RawSourcePgError(
                "SOURCE_PACKAGE_MISMATCH",
                "价格事实RAW历史行与明确来源包数字ID不一致",
            )
        if row["session_date"] == target_session and (
            row["source_capture_id"] != expected_target_capture_id
        ):
            raise RawSourcePgError(
                "TARGET_CAPTURE_MISMATCH",
                "价格事实RAW目标行与来源捕获身份不一致",
            )
        rows_by_symbol[row["source_symbol"]].append(row)

    missing_by_symbol = {
        symbol: [
            session.isoformat()
            for session in selected_sessions
            if session not in {row["session_date"] for row in symbol_rows}
        ]
        for symbol, symbol_rows in rows_by_symbol.items()
    }
    return {
        "source_symbols": list(selected_symbols),
        "session_dates": [session.isoformat() for session in selected_sessions],
        "target_session": target_session.isoformat(),
        "research_cutoff": research_cutoff.isoformat(),
        "source_package_id": source_package_id,
        "window_direction": "STARTS_AT_TARGET"
        if starts_at_target
        else "ENDS_AT_TARGET",
        "history_rows_by_symbol": rows_by_symbol,
        "missing_sessions_by_symbol": missing_by_symbol,
        "raw_history_rows_read": len(rows),
        "raw_history_batch_reads": 1,
        "transaction": "REPEATABLE READ READ ONLY",
        "database_write_attempted": False,
        "ddl_executed": False,
        "dml_executed": False,
        "source_zip_opened": False,
    }


def read_source_market_breadth_history(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    source_package_id: int,
    session_window: Sequence[date],
) -> dict[str, Any]:
    """从一个只读快照按Raw捕获读取来源篮与所需历史价格行。"""
    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
        or type(source_package_id) is not int
        or source_package_id <= 0
    ):
        raise RawSourcePgError(
            "MARKET_BREADTH_QUERY_INVALID",
            "市场宽度查询须绑定截止日内的明确日期和正数来源包ID",
        )
    if (
        not isinstance(session_window, (tuple, list))
        or len(session_window) != 5
        or any(type(session) is not date for session in session_window)
        or tuple(sorted(set(session_window))) != tuple(session_window)
        or session_window[-1] != target_session
        or any(session > research_cutoff for session in session_window)
    ):
        raise RawSourcePgError(
            "MARKET_BREADTH_SESSION_WINDOW_INVALID",
            "必须提供截止日内t−4至t的五个相邻官方会话",
        )

    price_sessions = (
        session_window[0],
        session_window[1],
        session_window[3],
        session_window[4],
    )
    with _transaction(config, write=False) as (_connection, cursor, _scope):
        target_rows = _read_raw_rows(cursor, target_session, source_package_id)
        if not target_rows:
            raise RawSourcePgError(
                "MARKET_BREADTH_SOURCE_VERSION_MISSING",
                "PG中没有该日期和数字来源包的Raw来源行",
            )
        source_symbols = tuple(row["source_symbol"] for row in target_rows)
        capture_ids = {int(row["source_capture_id"]) for row in target_rows}
        if (
            len(source_symbols) != len(set(source_symbols))
            or tuple(sorted(source_symbols)) != source_symbols
            or len(target_rows) > 5000
            or len(capture_ids) != 1
        ):
            raise RawSourcePgError(
                "MARKET_BREADTH_SOURCE_VERSION_AMBIGUOUS",
                "目标日Raw来源行未形成唯一、有界的捕获版本",
            )
        capture_id = next(iter(capture_ids))
        capture = _read_capture(cursor, source_package_id, target_session)
        if capture is None:
            raise RawSourcePgError(
                "MARKET_BREADTH_CAPTURE_MISSING", "目标日Raw行缺少来源捕获记录"
            )
        cursor.execute(
            """SELECT dataset_id, byte_count FROM source_packages
WHERE source_package_id=%s""",
            (source_package_id,),
        )
        content = cursor.fetchone()
        if content is None:
            raise RawSourcePgError(
                "MARKET_BREADTH_CONTENT_MISSING", "目标日Raw行缺少来源内容记录"
            )
        dataset_id, byte_count = content
        if (
            capture["parser_contract"] != _PARSER_CONTRACT
            or dataset_id != _DATASET_ID
            or capture["normalized_count"] != len(target_rows)
        ):
            raise RawSourcePgError(
                "MARKET_BREADTH_SOURCE_VERSION_INVALID",
                "Raw行、捕获记录与来源包父记录不一致",
            )
        target_status_counts = {
            status: sum(row["bar_status"] == status for row in target_rows)
            for status in ("TARGET_PRESENT", "TARGET_MISSING", "READ_FAILED")
        }
        if sum(target_status_counts.values()) != len(target_rows):
            raise RawSourcePgError(
                "MARKET_BREADTH_BAR_STATUS_INVALID",
                "Raw来源池包含未登记的Bar状态",
            )
        cursor.execute(
            f"SELECT {_RAW_SELECT} FROM source_raw_daily "
            "WHERE source_symbol=ANY(%s::text[]) "
            "AND session_date=ANY(%s::date[]) AND source_package_id=%s "
            "ORDER BY source_symbol, session_date",
            (list(source_symbols), list(price_sessions), source_package_id),
        )
        history_rows = [
            dict(zip(_RAW_COLUMNS, row, strict=True)) for row in cursor.fetchall()
        ]

    expected_symbols = set(source_symbols)
    expected_sessions = set(price_sessions)
    history_keys = [(row["source_symbol"], row["session_date"]) for row in history_rows]
    if (
        len(history_rows) > len(source_symbols) * len(price_sessions)
        or len(history_keys) != len(set(history_keys))
        or any(
            row["source_symbol"] not in expected_symbols
            or row["session_date"] not in expected_sessions
            or int(row["source_package_id"]) != source_package_id
            or row["bar_status"] not in _BAR_STATUSES
            for row in history_rows
        )
    ):
        raise RawSourcePgError(
            "MARKET_BREADTH_HISTORY_REFERENCE_INVALID",
            "同一数字包历史Raw行包含重复键或越界来源身份",
        )

    source_version = {
        "target_session": target_session.isoformat(),
        "source_package_id": source_package_id,
        "source_capture_id": int(capture_id),
        "source_dataset_id": dataset_id,
        "source_byte_count": int(byte_count),
        "parser_contract": capture["parser_contract"],
        "source_time_quality": capture["source_time_quality"],
        "source_available_at": _db_scalar(capture["source_available_at"]),
        "capture_read_started_at": _db_scalar(capture["read_started_at"]),
        "capture_validated_at": _db_scalar(capture["validated_at"]),
        "source_clock_role": capture["source_clock_role"],
        "raw_row_count": len(target_rows),
        "capture_expected_row_count": capture["normalized_count"],
        "target_status_counts": target_status_counts,
    }
    observations = [
        {
            "source_symbol": row["source_symbol"],
            "session_date": row["session_date"],
            "source_package_id": int(row["source_package_id"]),
            "raw_close": row["raw_close"],
            "price_quality": row["price_quality"],
            "price_reason": row["price_reason"],
            "bar_status": row["bar_status"],
            "price_coordinate": "RAW_UNADJUSTED",
        }
        for row in history_rows
    ]
    return {
        "source_version": source_version,
        "source_symbols": source_symbols,
        "target_raw_row_count": len(target_rows),
        "target_status_counts": target_status_counts,
        "price_sessions": tuple(price_sessions),
        "history_rows": observations,
        "history_row_count": len(observations),
        "transaction": "REPEATABLE READ READ ONLY",
        "database_write_attempted": False,
        "ddl_executed": False,
        "dml_executed": False,
        "source_zip_opened": False,
    }


def _page_version_status(
    cursor: Any, target_session: date, source_package_id: int
) -> tuple[str, int, int | None]:
    """同一只读快照中核验未过滤的目标日分母和来源引用。"""
    cursor.execute(
        """SELECT count(*)::bigint, count(DISTINCT r.source_capture_id)::bigint,
       min(c.normalized_count), max(c.normalized_count),
       count(*) FILTER (WHERE c.capture_id IS NULL OR p.source_package_id IS NULL
            OR c.source_package_id IS DISTINCT FROM r.source_package_id
            OR c.target_session IS DISTINCT FROM r.session_date
            OR p.dataset_id IS DISTINCT FROM %s)::bigint
FROM source_raw_daily r
 LEFT JOIN source_captures c ON c.capture_id=r.source_capture_id
 LEFT JOIN source_packages p ON p.source_package_id=r.source_package_id
 WHERE r.session_date=%s AND r.source_package_id=%s""",
        (_DATASET_ID, target_session, source_package_id),
    )
    actual, captures, expected, expected_max, broken = cursor.fetchone()
    if actual == 0:
        return "EMPTY", 0, None
    if (
        captures != 1
        or broken
        or expected is None
        or expected <= 0
        or expected != expected_max
        or actual > expected
    ):
        raise RawSourcePgError(
            "SOURCE_REFERENCE_MISMATCH", "Raw分页的来源引用或捕获分母不一致"
        )
    return ("CLOSED" if actual == expected else "PARTIAL", int(actual), int(expected))


def read_raw_daily_history_page(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_package_id: int,
    sessions: Sequence[date],
    offset: int,
    limit: int,
    market: str,
    board: str,
    source_symbol: str | None = None,
    include_parser_contract: bool = False,
) -> dict[str, Any]:
    """SQL过滤并分页选候选，再以单个只读PG事务批量读取本页历史。"""
    selected_sessions = _validate_history_query(
        source_symbol=source_symbol,
        target_session=target_session,
        source_package_id=source_package_id,
        sessions=sessions,
    )
    if (
        type(offset) is not int
        or not 0 <= offset <= 2**31 - 1
        or type(limit) is not int
        or not 1 <= limit <= 100
    ):
        raise RawSourcePgError(
            "PG_PAGE_INVALID", "PG历史分页offset须为非负整数且limit为1至100"
        )
    if len(selected_sessions) * limit > 750_000:
        raise RawSourcePgError("PG_HISTORY_BUDGET_EXCEEDED", "单页历史行数预算超限")
    if market not in {"ALL", "SH", "SZ"} or board not in {"ALL", "MAIN_BOARD", "GEM"}:
        raise RawSourcePgError("PG_FILTER_INVALID", "PG历史分页市场或板别过滤无效")
    with _transaction(config, write=False, statement_timeout_ms=30000) as (
        _connection,
        cursor,
        _scope,
    ):
        version_status, version_rows, expected_rows = _page_version_status(
            cursor, target_session, source_package_id
        )
        cursor.execute(
            """SELECT bar_status, count(*) FROM source_raw_daily
 WHERE session_date=%s AND source_package_id=%s
  AND (%s='ALL' OR market=%s) AND (%s='ALL' OR board=%s)
  AND (%s::text IS NULL OR source_symbol=%s::text)
GROUP BY bar_status ORDER BY bar_status""",
            (
                target_session,
                source_package_id,
                market,
                market,
                board,
                board,
                source_symbol,
                source_symbol,
            ),
        )
        by_status = {status: int(count) for status, count in cursor.fetchall()}
        total = sum(by_status.values())
        cursor.execute(
            """SELECT source_symbol, market, board FROM source_raw_daily
 WHERE session_date=%s AND source_package_id=%s
  AND (%s='ALL' OR market=%s) AND (%s='ALL' OR board=%s)
  AND (%s::text IS NULL OR source_symbol=%s::text)
ORDER BY source_symbol LIMIT %s OFFSET %s""",
            (
                target_session,
                source_package_id,
                market,
                market,
                board,
                board,
                source_symbol,
                source_symbol,
                limit,
                offset,
            ),
        )
        page = cursor.fetchall()
        symbols = [row[0] for row in page]
        history_rows = _read_history_rows(
            cursor,
            source_symbols=symbols,
            sessions=selected_sessions,
            source_package_id=source_package_id,
            include_parser_contract=include_parser_contract,
        )
    grouped: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in symbols}
    for row in history_rows:
        grouped[row["source_symbol"]].append(row)
    candidates = []
    for symbol, candidate_market, candidate_board in page:
        rows = grouped[symbol]
        actual_sessions = tuple(row["session_date"] for row in rows)
        missing = sorted(set(selected_sessions) - set(actual_sessions))
        candidates.append(
            {
                "source_symbol": symbol,
                "market": candidate_market,
                "board": candidate_board,
                "history_rows": rows,
                "history_complete": not missing,
                "missing_sessions": [day.isoformat() for day in missing],
            }
        )
    next_offset = offset + len(page)
    return {
        "status": "AVAILABLE" if page else "EMPTY_PAGE",
        "target_session": target_session.isoformat(),
        "source_package_id": source_package_id,
        "session_start": selected_sessions[0].isoformat(),
        "session_end": selected_sessions[-1].isoformat(),
        "session_count": len(selected_sessions),
        "offset": offset,
        "limit": limit,
        "page_count": len(page),
        "total_count": total,
        "next_offset": next_offset if next_offset < total else None,
        "bar_status_counts": by_status,
        "partition_closed": version_status == "CLOSED"
        and total == sum(by_status.values()),
        "version_status": version_status,
        "version_row_count": version_rows,
        "version_expected_row_count": expected_rows,
        "market_filter": market,
        "board_filter": board,
        "source_symbol_filter": source_symbol,
        "candidates": candidates,
        "source_zip_opened": False,
        "source_recalculated": False,
        "ddl_executed": False,
        "database_write": False,
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "pit_status": "NON_PIT_RETROSPECTIVE",
    }


def read_raw_daily_page(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_package_id: int,
    offset: int,
    limit: int,
    market: str,
    board: str,
) -> dict[str, Any]:
    if (
        type(target_session) is not date
        or type(source_package_id) is not int
        or source_package_id <= 0
    ):
        raise RawSourcePgError(
            "PG_QUERY_KEY_INVALID", "PG分页查询必须提供精确目标会话与正数来源包ID"
        )
    if (
        type(offset) is not int
        or offset < 0
        or offset > 2**31 - 1
        or type(limit) is not int
        or not 1 <= limit <= 100
    ):
        raise RawSourcePgError(
            "PG_PAGE_INVALID", "PG分页offset须为非负整数，limit须在1至100内"
        )
    if market not in {"ALL", "SH", "SZ"} or board not in {"ALL", "MAIN_BOARD", "GEM"}:
        raise RawSourcePgError("PG_FILTER_INVALID", "PG市场或板别过滤值无效")
    with _transaction(config, write=False) as (_connection, cursor, _scope):
        version_status, version_rows, expected_rows = _page_version_status(
            cursor, target_session, source_package_id
        )
        cursor.execute(
            """SELECT bar_status, count(*) FROM source_raw_daily
 WHERE session_date=%s AND source_package_id=%s
  AND (%s='ALL' OR market=%s) AND (%s='ALL' OR board=%s)
GROUP BY bar_status ORDER BY bar_status""",
            (
                target_session,
                source_package_id,
                market,
                market,
                board,
                board,
            ),
        )
        by_status = {status: count for status, count in cursor.fetchall()}
        total = sum(by_status.values())
        cursor.execute(
            f"""SELECT {_RAW_SELECT} FROM source_raw_daily
 WHERE session_date=%s AND source_package_id=%s
  AND (%s='ALL' OR market=%s) AND (%s='ALL' OR board=%s)
ORDER BY source_symbol LIMIT %s OFFSET %s""",
            (
                target_session,
                source_package_id,
                market,
                market,
                board,
                board,
                limit,
                offset,
            ),
        )
        rows = [
            dict(zip(_RAW_COLUMNS, values, strict=True)) for values in cursor.fetchall()
        ]
    page_rows = [{key: _db_scalar(value) for key, value in row.items()} for row in rows]
    for row in page_rows:
        row.update(
            {
                "source_object_status": "TDX_SOURCE_SYMBOL_OBSERVED",
                "target_bar_status": {
                    "TARGET_PRESENT": "TDX_TARGET_BAR_PRESENT",
                    "TARGET_MISSING": "TDX_TARGET_BAR_MISSING",
                    "READ_FAILED": "TDX_READ_FAILED",
                }[row["bar_status"]],
                "stable_security_id_status": "STABLE_SECURITY_ID_UNVERIFIED",
                "listing_status": "LISTING_STATUS_UNKNOWN",
                "suspension_status": "SUSPENSION_STATUS_UNKNOWN",
            }
        )
    next_offset = offset + len(rows)
    return {
        "status": "AVAILABLE" if rows else "EMPTY_PAGE",
        "target_session": target_session.isoformat(),
        "source_package_id": source_package_id,
        "offset": offset,
        "limit": limit,
        "page_count": len(rows),
        "total_count": total,
        "next_offset": next_offset if next_offset < total else None,
        "bar_status_counts": by_status,
        "partition_closed": version_status == "CLOSED"
        and total == sum(by_status.values()),
        "version_status": version_status,
        "version_row_count": version_rows,
        "version_expected_row_count": expected_rows,
        "market_filter": market,
        "board_filter": board,
        "rows": page_rows,
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "source_object_status": "TDX_SOURCE_SYMBOL_OBSERVED",
        "stable_security_id_status": "STABLE_SECURITY_ID_UNVERIFIED",
        "listing_status": "LISTING_STATUS_UNKNOWN",
        "suspension_status": "SUSPENSION_STATUS_UNKNOWN",
        "official_listing_universe_status": "UNKNOWN",
        "price_coordinate": "RAW_UNADJUSTED",
        "source_zip_opened": False,
        "ddl_executed": False,
        "source_recalculated": False,
    }


def read_raw_daily_catalog(
    config: Mapping[str, Any],
    *,
    max_target_session: date,
    year: int | None = None,
    offset: int = 0,
    limit: int = 100,
) -> dict[str, Any]:
    """只读取PG已保存的Raw日期版本目录，不打开来源ZIP或执行写入。"""
    if type(max_target_session) is not date:
        raise RawSourcePgError(
            "PG_CATALOG_CUTOFF_REQUIRED", "Raw目录必须绑定显式研究截止日期"
        )
    if year is not None and (type(year) is not int or not 1900 <= year <= 9998):
        raise RawSourcePgError("PG_CATALOG_YEAR_INVALID", "Raw目录年份无效")
    if (
        type(offset) is not int
        or not 0 <= offset <= 2**31 - 1
        or type(limit) is not int
        or not 1 <= limit <= 200
    ):
        raise RawSourcePgError(
            "PG_CATALOG_PAGE_INVALID", "Raw目录offset须为非负整数，limit须在1到200"
        )

    def _where() -> tuple[str, tuple[Any, ...]]:
        clauses = ["r.session_date<=%s"]
        params: list[Any] = [max_target_session]
        if year is not None:
            clauses.extend(("r.session_date >= %s", "r.session_date < %s"))
            params.extend((date(year, 1, 1), date(year + 1, 1, 1)))
        return " AND ".join(clauses), tuple(params)

    with _transaction(config, write=False) as (_connection, cursor, _scope):
        where_sql, params = _where()
        cursor.execute(
            f"""SELECT count(*) FROM (
    SELECT r.session_date, r.source_package_id
    FROM source_raw_daily r
    WHERE {where_sql}
    GROUP BY r.session_date, r.source_package_id
) AS versions""",
            params,
        )
        total_versions = int(cursor.fetchone()[0])
        cursor.execute(
            f"""SELECT r.session_date, r.source_package_id, count(*)::bigint AS row_count,
       count(*) FILTER (WHERE r.bar_status='TARGET_PRESENT')::bigint AS target_present,
       count(*) FILTER (WHERE r.bar_status='TARGET_MISSING')::bigint AS target_missing,
       count(*) FILTER (WHERE r.bar_status='READ_FAILED')::bigint AS read_failed,
       count(*) FILTER (WHERE r.bar_status NOT IN ('TARGET_PRESENT','TARGET_MISSING','READ_FAILED'))::bigint AS unknown_bar_status,
       count(DISTINCT r.source_capture_id)::bigint AS capture_count,
       min(c.normalized_count) AS expected_row_count,
       max(c.normalized_count) AS expected_row_count_max,
       min(c.read_started_at) AS capture_read_started_at,
       max(c.read_started_at) AS capture_read_started_at_max,
       min(c.validated_at) AS capture_validated_at,
       max(c.validated_at) AS capture_validated_at_max,
       array_agg(DISTINCT c.source_time_quality ORDER BY c.source_time_quality) AS source_time_qualities,
       array_agg(DISTINCT c.source_clock_role ORDER BY c.source_clock_role) AS source_clock_roles,
       count(*) FILTER (WHERE c.source_available_at IS NOT NULL)::bigint AS source_available_at_count,
        count(*) FILTER (WHERE c.capture_id IS NULL OR p.source_package_id IS NULL OR
             c.source_package_id IS DISTINCT FROM r.source_package_id OR
             c.target_session IS DISTINCT FROM r.session_date)::bigint AS broken_reference_count,
        array_agg(DISTINCT p.dataset_id ORDER BY p.dataset_id) AS dataset_ids,
       array_agg(DISTINCT r.price_quality ORDER BY r.price_quality) AS price_qualities,
       array_agg(DISTINCT r.amount_quality ORDER BY r.amount_quality) AS amount_qualities,
       array_agg(DISTINCT r.amount_unit_status ORDER BY r.amount_unit_status) AS amount_unit_statuses,
       array_agg(DISTINCT r.volume_quality ORDER BY r.volume_quality) AS volume_qualities,
       array_agg(DISTINCT r.volume_unit_status ORDER BY r.volume_unit_status) AS volume_unit_statuses
FROM source_raw_daily r
LEFT JOIN source_captures c
   ON c.capture_id=r.source_capture_id
 LEFT JOIN source_packages p
   ON p.source_package_id=r.source_package_id
WHERE {where_sql}
GROUP BY r.session_date, r.source_package_id
ORDER BY r.session_date DESC, r.source_package_id
LIMIT %s OFFSET %s""",
            (*params, limit, offset),
        )
        names = [item.name for item in cursor.description]
        grouped = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]

    versions: list[dict[str, Any]] = []
    for item in grouped:
        row_count = int(item["row_count"])
        status_counts = {
            "TARGET_PRESENT": int(item["target_present"]),
            "TARGET_MISSING": int(item["target_missing"]),
            "READ_FAILED": int(item["read_failed"]),
        }
        dataset_ids = item["dataset_ids"] or []
        clock_roles = item["source_clock_roles"] or []
        time_qualities = item["source_time_qualities"] or []
        if (
            sum(status_counts.values()) != row_count
            or item["unknown_bar_status"] != 0
            or item["capture_count"] != 1
            or item["broken_reference_count"] != 0
            or item["expected_row_count"] is None
            or item["expected_row_count"] <= 0
            or item["expected_row_count"] != item["expected_row_count_max"]
            or row_count > item["expected_row_count"]
            or dataset_ids != [_DATASET_ID]
            or len(clock_roles) != 1
            or len(time_qualities) != 1
            or item["capture_read_started_at"] != item["capture_read_started_at_max"]
            or item["capture_validated_at"] != item["capture_validated_at_max"]
        ):
            raise RawSourcePgError(
                "SOURCE_REFERENCE_MISMATCH", "Raw目录行、来源引用、时钟或状态分区未闭合"
            )
        versions.append(
            {
                "session_date": item["session_date"].isoformat(),
                "source_package_id": item["source_package_id"],
                "row_count": row_count,
                "expected_row_count": int(item["expected_row_count"]),
                "bar_status_counts": status_counts,
                "source_capture_count": item["capture_count"],
                "capture_read_started_at": _db_scalar(item["capture_read_started_at"]),
                "capture_validated_at": _db_scalar(item["capture_validated_at"]),
                "source_time_quality": time_qualities[0],
                "source_clock_role": clock_roles[0],
                "source_available_at_count": int(item["source_available_at_count"]),
                "dataset_id": dataset_ids[0],
                "price_qualities": item["price_qualities"] or [],
                "amount_qualities": item["amount_qualities"] or [],
                "amount_unit_statuses": item["amount_unit_statuses"] or [],
                "volume_qualities": item["volume_qualities"] or [],
                "volume_unit_statuses": item["volume_unit_statuses"] or [],
                "identity_status": "SOURCE_SYMBOL_ONLY",
                "pit_status": "NON_PIT_RETROSPECTIVE",
                "version_status": "CLOSED"
                if row_count == item["expected_row_count"]
                else "PARTIAL",
            }
        )
    next_offset = offset + len(versions)
    return {
        "status": "AVAILABLE" if versions else "EMPTY",
        "year_filter": year,
        "offset": offset,
        "limit": limit,
        "page_count": len(versions),
        "total_version_count": total_versions,
        "next_offset": next_offset if next_offset < total_versions else None,
        "versions": versions,
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "database_write_attempted": False,
        "dml_executed": False,
        "ddl_executed": False,
        "source_zip_opened": False,
        "source_recalculated": False,
    }


def read_raw_daily_readiness(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_package_id: int,
) -> dict[str, Any]:
    """按明确日期和数字包ID汇总已保存Raw行，不读取原件或摘要字段。"""
    if (
        type(target_session) is not date
        or type(source_package_id) is not int
        or source_package_id <= 0
    ):
        raise RawSourcePgError(
            "PG_READINESS_KEY_INVALID", "准备状态查询须提供日期和正数来源包ID"
        )

    with _transaction(config, write=False) as (_connection, cursor, _scope):
        cursor.execute(
            """SELECT count(*)::bigint AS stored_row_count,
       count(*) FILTER (WHERE r.bar_status='TARGET_PRESENT')::bigint AS target_present,
       count(*) FILTER (WHERE r.bar_status='TARGET_MISSING')::bigint AS target_missing,
       count(*) FILTER (WHERE r.bar_status='READ_FAILED')::bigint AS read_failed,
       count(*) FILTER (WHERE r.bar_status NOT IN ('TARGET_PRESENT','TARGET_MISSING','READ_FAILED'))::bigint AS unknown_bar_status,
       count(DISTINCT r.source_capture_id)::bigint AS capture_count,
       min(c.normalized_count) AS expected_row_count,
       max(c.normalized_count) AS expected_row_count_max,
       min(c.read_started_at) AS capture_read_started_at,
       max(c.read_started_at) AS capture_read_started_at_max,
       min(c.validated_at) AS capture_validated_at,
       max(c.validated_at) AS capture_validated_at_max,
       array_agg(DISTINCT c.source_time_quality ORDER BY c.source_time_quality) AS source_time_qualities,
       array_agg(DISTINCT c.source_clock_role ORDER BY c.source_clock_role) AS source_clock_roles,
       count(*) FILTER (WHERE c.source_available_at IS NOT NULL)::bigint AS source_available_at_count,
       count(*) FILTER (WHERE c.capture_id IS NULL OR p.source_package_id IS NULL
           OR c.source_package_id IS DISTINCT FROM r.source_package_id
           OR c.target_session IS DISTINCT FROM r.session_date)::bigint AS broken_reference_count,
       array_agg(DISTINCT p.dataset_id ORDER BY p.dataset_id) AS dataset_ids,
       array_agg(DISTINCT r.price_quality ORDER BY r.price_quality) AS price_qualities,
       array_agg(DISTINCT r.amount_quality ORDER BY r.amount_quality) AS amount_qualities,
       array_agg(DISTINCT r.amount_unit_status ORDER BY r.amount_unit_status) AS amount_unit_statuses,
       array_agg(DISTINCT r.volume_quality ORDER BY r.volume_quality) AS volume_qualities,
       array_agg(DISTINCT r.volume_unit_status ORDER BY r.volume_unit_status) AS volume_unit_statuses
FROM source_raw_daily r
LEFT JOIN source_captures c
  ON c.capture_id=r.source_capture_id
LEFT JOIN source_packages p
  ON p.source_package_id=r.source_package_id
WHERE r.session_date=%s AND r.source_package_id=%s""",
            (target_session, source_package_id),
        )
        row = cursor.fetchone()
        names = [item.name for item in cursor.description]
        item = dict(zip(names, row, strict=True))

    stored_count = int(item["stored_row_count"])
    if stored_count == 0:
        return {
            "status": "EMPTY",
            "source_date": target_session.isoformat(),
            "source_package_id": source_package_id,
        }

    counts = {
        "TARGET_PRESENT": int(item["target_present"]),
        "TARGET_MISSING": int(item["target_missing"]),
        "READ_FAILED": int(item["read_failed"]),
    }
    source_time_qualities = item["source_time_qualities"] or []
    source_clock_roles = item["source_clock_roles"] or []
    dataset_ids = item["dataset_ids"] or []
    expected_count = item["expected_row_count"]
    if (
        sum(counts.values()) != stored_count
        or item["unknown_bar_status"] != 0
        or item["capture_count"] != 1
        or item["broken_reference_count"] != 0
        or expected_count is None
        or expected_count <= 0
        or expected_count != item["expected_row_count_max"]
        or stored_count > expected_count
        or dataset_ids != [_DATASET_ID]
        or len(source_time_qualities) != 1
        or len(source_clock_roles) != 1
        or item["capture_read_started_at"] != item["capture_read_started_at_max"]
        or item["capture_validated_at"] != item["capture_validated_at_max"]
    ):
        raise RawSourcePgError(
            "SOURCE_REFERENCE_MISMATCH",
            "精确日期/来源包ID的Raw行、来源引用、时钟或状态分区未闭合",
        )

    return {
        "status": "AVAILABLE",
        "source_date": target_session.isoformat(),
        "source_package_id": source_package_id,
        "stored_row_count": stored_count,
        "capture_expected_row_count": int(expected_count),
        "unpersisted_candidate_count": int(expected_count) - stored_count,
        "bar_status_counts": counts,
        "source_capture_count": int(item["capture_count"]),
        "capture_read_started_at": _db_scalar(item["capture_read_started_at"]),
        "capture_validated_at": _db_scalar(item["capture_validated_at"]),
        "source_available_at_count": int(item["source_available_at_count"]),
        "source_time_quality": source_time_qualities[0],
        "source_clock_role": source_clock_roles[0],
        "dataset_id": dataset_ids[0],
        "price_qualities": item["price_qualities"] or [],
        "amount_qualities": item["amount_qualities"] or [],
        "amount_unit_statuses": item["amount_unit_statuses"] or [],
        "volume_qualities": item["volume_qualities"] or [],
        "volume_unit_statuses": item["volume_unit_statuses"] or [],
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "price_coordinate": "RAW_UNADJUSTED",
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "version_status": "CLOSED" if stored_count == expected_count else "PARTIAL",
        "evidence_type": "REAL_PG",
        "producer": "source_raw_daily + source_captures + source_packages",
        "consumer": "现有Raw目录、来源分页、Raw单股及本状态GET",
        "database_write": False,
        "ddl_executed": False,
        "source_zip_opened": False,
        "source_recalculated": False,
    }


def read_source_version_context(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_package_id: int,
) -> dict[str, Any]:
    """按现存Raw、捕获和数字包父记录读取一个明确来源版本。"""
    summary = read_raw_daily_readiness(
        config,
        target_session=target_session,
        source_package_id=source_package_id,
    )
    if summary.get("status") != "AVAILABLE":
        return summary
    if summary.get("version_status") != "CLOSED":
        raise RawSourcePgError(
            "SOURCE_VERSION_INCOMPLETE",
            "明确日期与数字来源包的Raw行尚未覆盖捕获时记录的来源代码数",
        )
    with _transaction(config, write=False) as (_connection, cursor, _scope):
        cursor.execute(
            """SELECT DISTINCT r.source_package_id, r.source_capture_id,
       c.read_started_at, c.validated_at, c.source_available_at,
       c.source_time_quality, c.source_clock_role, c.parser_contract,
       p.dataset_id, p.byte_count
FROM source_raw_daily r
JOIN source_captures c ON c.capture_id=r.source_capture_id
JOIN source_packages p ON p.source_package_id=r.source_package_id
WHERE r.session_date=%s AND r.source_package_id=%s
ORDER BY r.source_capture_id, r.source_package_id
LIMIT 2""",
            (target_session, source_package_id),
        )
        versions = cursor.fetchall()
    if len(versions) != 1:
        raise RawSourcePgError(
            "SOURCE_VERSION_AMBIGUOUS",
            "指定日期与来源包ID没有唯一捕获和Raw事实引用",
        )
    (
        source_package_id,
        source_capture_id,
        capture_started_at,
        capture_validated_at,
        source_available_at,
        source_time_quality,
        source_clock_role,
        parser_contract,
        dataset_id,
        byte_count,
    ) = versions[0]
    if (
        dataset_id != _DATASET_ID
        or source_available_at is not None
        and source_time_quality != "VERIFIED_SOURCE_TIME"
    ):
        raise RawSourcePgError(
            "SOURCE_VERSION_REFERENCE_INVALID",
            "来源包或捕获元数据与数字包键/可得时钟不一致",
        )
    return {
        "status": "AVAILABLE",
        "target_session": target_session.isoformat(),
        "source_package_id": int(source_package_id),
        "source_capture_id": int(source_capture_id),
        "source_dataset_id": dataset_id,
        "source_byte_count": int(byte_count),
        "parser_contract": parser_contract,
        "capture_started_at": _db_scalar(capture_started_at),
        "capture_validated_at": _db_scalar(capture_validated_at),
        "source_available_at": _db_scalar(source_available_at),
        "source_time_quality": source_time_quality,
        "source_clock_role": source_clock_role,
        "raw_row_count": summary["stored_row_count"],
        "capture_expected_row_count": summary["capture_expected_row_count"],
        "bar_status_counts": summary["bar_status_counts"],
        "price_qualities": summary["price_qualities"],
        "amount_qualities": summary["amount_qualities"],
        "amount_unit_statuses": summary["amount_unit_statuses"],
        "volume_qualities": summary["volume_qualities"],
        "volume_unit_statuses": summary["volume_unit_statuses"],
        "version_status": "COMPLETE",
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "official_listing_universe_status": "UNKNOWN",
        "stable_security_id_status": "STABLE_SECURITY_ID_UNVERIFIED",
        "listing_status": "LISTING_STATUS_UNKNOWN",
        "suspension_status": "SUSPENSION_STATUS_UNKNOWN",
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "price_coordinate": "RAW_UNADJUSTED",
        "transaction": "REPEATABLE READ READ ONLY",
        "database_write": False,
        "ddl_executed": False,
    }


def raw_source_capacity(config: Mapping[str, Any]) -> dict[str, Any]:
    """读取本批Raw表行数、占用页和关系总字节，不请求外部统计扩展。"""
    with _transaction(config, write=False) as (_connection, cursor, _context):
        cursor.execute("""SELECT count(*)::bigint, pg_relation_size('source_raw_daily')::bigint,
pg_total_relation_size('source_raw_daily')::bigint,
ceil(pg_relation_size('source_raw_daily')::numeric/8192)::bigint
FROM source_raw_daily""")
        row_count, heap_bytes, total_bytes, heap_pages = cursor.fetchone()
        cursor.execute("""SELECT indexrelid::regclass::text, pg_relation_size(indexrelid)::bigint
FROM pg_index WHERE indrelid='source_raw_daily'::regclass ORDER BY indexrelid::regclass::text""")
        indexes = [{"name": name, "bytes": size} for name, size in cursor.fetchall()]
        cursor.execute("""SELECT n_live_tup::bigint, n_dead_tup::bigint
FROM pg_stat_user_tables WHERE schemaname='stock' AND relname='source_raw_daily'""")
        stats = cursor.fetchone() or (0, 0)
    return {
        "row_count": row_count,
        "heap_bytes": heap_bytes,
        "total_relation_bytes": total_bytes,
        "heap_pages_8k": heap_pages,
        "index_bytes": sum(item["bytes"] for item in indexes),
        "indexes": indexes,
        "stats_live_estimate": stats[0],
        "stats_dead_estimate": stats[1],
    }
