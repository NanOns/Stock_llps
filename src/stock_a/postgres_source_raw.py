"""在固定stock_a.stock中持久化并读取TDX来源代码级Raw日线。"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
import time
from typing import Any, Iterator, Mapping, Sequence
import uuid

from . import postgres_binding, postgres_identity
from .postgres_typed import (
    SourceCaptureCandidate,
    SourceContentCandidate,
    TypedBridgeError,
    _digest,
    _finite_decimal,
    source_capture_insert_plan,
    source_capture_readback_plan,
    source_content_insert_plan,
    source_content_readback_plan,
    validate_source_capture_readback,
    validate_source_content_readback,
)


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
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL = re.compile(r"^(SH|SZ)\.[0-9]{6}$")
_BAR_STATUSES = {"TARGET_PRESENT", "TARGET_MISSING", "READ_FAILED"}
_QUALITY = {"OBSERVED", "UNKNOWN", "NOT_APPLICABLE", "BAD_VALUE"}
_RAW_QUALITY = {"RAW_SOURCE_VALUE_OBSERVED", "UNKNOWN", "BAD_VALUE", "NOT_APPLICABLE"}

_RAW_COLUMNS = (
    "namespace_id", "source_symbol", "market", "board", "session_date", "package_sha256",
    "source_content_id", "source_capture_id", "source_entry_sha256", "source_entry_bytes",
    "source_record_count", "bar_status", "bar_reason", "raw_open", "raw_high", "raw_low",
    "raw_close", "price_quality", "price_reason", "raw_amount", "raw_amount_quality",
    "raw_amount_reason", "amount_quality", "amount_reason", "amount_unit_status", "raw_volume",
    "raw_volume_quality", "raw_volume_reason", "volume_quality", "volume_reason",
    "volume_unit_status", "first_source_date", "last_source_date", "future_source_rows_excluded",
    "observed_started_at", "observed_completed_at", "source_clock_role", "source_available_at",
)
_RAW_SELECT = ", ".join(_RAW_COLUMNS)
_RAW_INSERT = f"""INSERT INTO stock.source_raw_daily ({_RAW_SELECT})
VALUES ({", ".join(["%s"] * len(_RAW_COLUMNS))})
ON CONFLICT (namespace_id, source_symbol, session_date, package_sha256) DO NOTHING"""


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
        raise RawSourcePgError("PG_DRIVER_MISSING", "缺少PostgreSQL驱动psycopg") from error
    dsn = os.environ.get("STOCK_A_PG_DSN")
    if dsn:
        try:
            return postgres_identity._fixed_connection_parameters(dsn, os.environ, conninfo_to_dict)
        except (ValueError, TypeError, psycopg.ProgrammingError) as error:
            raise RawSourcePgError("PG_TARGET_REJECTED", "PG连接目标或连接参数不符合固定合同") from error
    try:
        credentials = postgres_identity._load_private_credentials()
        if credentials.get("username") != "stock_a":
            raise RawSourcePgError("PG_LOGIN_REJECTED", "来源Raw PG读写必须使用stock_a业务登录账号")
        return postgres_identity._private_connection_parameters(credentials, make_conninfo, conninfo_to_dict)
    except RawSourcePgError:
        raise
    except Exception as error:
        raise RawSourcePgError("PG_CREDENTIALS_UNAVAILABLE", "无法从唯一登记的私密配置准备PG连接") from error


@contextmanager
def _transaction(config: Mapping[str, Any], *, write: bool) -> Iterator[tuple[Any, Any, str]]:
    import psycopg

    parameters = _connection_parameters()
    parameters.update(
        connect_timeout=5,
        options=("-c search_path=stock,pg_catalog -c statement_timeout=10000 -c lock_timeout=1000"),
    )
    try:
        with psycopg.connect(**parameters) as connection:
            with connection.cursor() as cursor:
                if write:
                    cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                else:
                    cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                namespace_id = _validate_connection(cursor, connection, config)
                yield connection, cursor, namespace_id
    except RawSourcePgError:
        raise
    except Exception as error:
        sqlstate = getattr(error, "sqlstate", None)
        raise RawSourcePgError(
            "PG_WRITE_FAILED" if write else "PG_READ_FAILED",
            f"固定stock_a.stock来源Raw事务失败（{type(error).__name__}，SQLSTATE={sqlstate or 'UNKNOWN'}）",
        ) from error


def _validate_connection(cursor: Any, connection: Any, config: Mapping[str, Any]) -> str:
    info = connection.info
    if (info.host != "127.0.0.1" or info.port != 5432 or info.dbname != "stock_a"
            or getattr(info, "hostaddr", "127.0.0.1") != "127.0.0.1"):
        raise RawSourcePgError("PG_TARGET_REJECTED", "来源Raw操作仅允许127.0.0.1:5432/stock_a")
    cursor.execute("SELECT current_database(), current_schema(), session_user, current_user, inet_server_addr()::text, inet_server_port()")
    actual = cursor.fetchone()
    if actual != ("stock_a", "stock", "stock_a", "stock_a", "127.0.0.1/32", 5432):
        raise RawSourcePgError("PG_IDENTITY_REJECTED", "来源Raw操作要求stock_a数据库、stock schema及stock_a真实登录")

    expected = config.get("application_identity")
    if not isinstance(expected, Mapping) or expected.get("deployment_applied") is not True:
        raise RawSourcePgError("APPLICATION_IDENTITY_MISSING", "运行配置没有已绑定的Stock A项目与部署身份")
    deadline = postgres_identity.time.monotonic() + 8.0
    business = postgres_identity._fetch_business_identity(
        postgres_identity._DeadlineCursor(cursor, deadline), deadline=deadline,
    )
    postgres_binding._validate_business_role(business)
    try:
        postgres_binding._validate_application_identity(business.get("identity"), expected)
    except postgres_binding.BindingRefused as error:
        raise RawSourcePgError("APPLICATION_IDENTITY_REJECTED", "PG中的Stock A项目、部署实例、簇或迁移lineage不匹配") from error
    ids = business.get("migration_digest")
    if not isinstance(ids, str) or ids != expected.get("migration_digest"):
        raise RawSourcePgError("MIGRATION_IDENTITY_REJECTED", "PG迁移ledger摘要与唯一运行配置不一致")
    _validate_raw_migration_prefix(cursor, config, business.get("migration_digest"))
    cursor.execute(
        "SELECT namespace_id::text FROM stock.namespaces WHERE environment='DEV' AND instance_uuid=%s::uuid ORDER BY namespace_id",
        (expected.get("deployment_instance_uuid"),),
    )
    namespaces = [row[0] for row in cursor.fetchall()]
    if len(namespaces) != 1:
        raise RawSourcePgError("DEV_NAMESPACE_AMBIGUOUS", "当前部署必须恰有一个DEV来源Raw命名空间")
    return namespaces[0]


def _validate_raw_migration_prefix(
    cursor: Any, config: Mapping[str, Any], business_digest: Any,
) -> int:
    """验证集中登记的不可变迁移前缀，并拒绝未应用所需迁移的连接。"""
    try:
        sources = postgres_binding._migration_sources()
        registered_ids = [migration_id for migration_id, _lineage, _raw, _sha in sources]
        if registered_ids != list(range(1, len(registered_ids) + 1)):
            raise postgres_binding.BindingRefused("迁移登记ID不是连续前缀")
        expected_rows = [(migration_id, sha, lineage)
                         for migration_id, lineage, _raw, sha in sources]
        cursor.execute(
            "SELECT migration_id, source_sha, lineage FROM stock.migration_ledger ORDER BY migration_id"
        )
        existing = cursor.fetchall()
        applied_count = postgres_binding._validate_migration_history(existing, expected_rows)
        if applied_count < 4:
            raise postgres_binding.BindingRefused("Raw Daily 需要0001-0004全部迁移")
        expected_digest = postgres_binding._ledger_digest(expected_rows[:applied_count])
        identity = config.get("application_identity")
        configured_digest = identity.get("migration_digest") if isinstance(identity, Mapping) else None
        if (not isinstance(business_digest, str) or business_digest != expected_digest
                or configured_digest != expected_digest):
            raise postgres_binding.BindingRefused("数据库、配置和迁移前缀摘要不一致")
        return applied_count
    except (postgres_binding.BindingRefused, OSError, ValueError) as error:
        raise RawSourcePgError(
            "MIGRATION_HISTORY_REJECTED",
            "PG Raw 需要完整登记迁移源 SHA 对应的不可变前缀，0004 不可修改或缺失",
        ) from error


def _validate_source_input(package: Any, target_session: date, *, task_scope: str) -> None:
    if task_scope not in _RAW_DAILY_APPEND_TASK_SCOPES:
        raise RawSourcePgError("TASK_SCOPE_REJECTED", "Raw APPEND只接受已登记的每日业务操作标识")
    if type(target_session) is not date:
        raise RawSourcePgError("TARGET_SESSION_INVALID", "必须显式提供date类型目标交易会话")
    if (not isinstance(getattr(package, "sha256", None), str)
            or _SHA256.fullmatch(package.sha256) is None
            or getattr(package, "zip_crc", None) != "PASS"
            or getattr(package, "file_size", 0) <= 0):
        raise RawSourcePgError("PACKAGE_NOT_VERIFIED", "入库仅接受本轮SHA/CRC完整复验的VIPDATA包")


def validate_source_candidate_limit(candidate_total: int) -> None:
    """本批完整保留的候选数上限；超出时拒绝，不丢尾或抽样。"""
    if type(candidate_total) is not int or not 1 <= candidate_total <= 5000:
        raise RawSourcePgError("CANDIDATE_LIMIT_EXCEEDED", "本批目标日候选须为1至5000条；拒绝截断或遗漏")


def _candidate_digest(pool: Any) -> str:
    catalog = [
        {
            "symbol": item["symbol"], "market": item["market"], "board": item["board"],
            "entry_path": item["zip_entry_path"], "entry_bytes": item["zip_entry_bytes"],
            "empty_placeholder": bool(item.get("empty_placeholder")),
        }
        for item in pool.candidates
    ]
    return _digest({"contract": "TDX_VIPDATA_TARGET_CANDIDATE_CATALOG_R1", "candidates": catalog})


def _source_identity(namespace_id: str, package: Any, target_session: date, pool: Any) -> tuple[SourceContentCandidate, str, str]:
    content_id = _digest({"dataset_id": _DATASET_ID, "byte_sha256": package.sha256})
    content = SourceContentCandidate(
        namespace_id=namespace_id, content_id=content_id, dataset_id=_DATASET_ID,
        byte_sha256=package.sha256, byte_count=package.file_size,
    )
    name = f"{_PARSER_CONTRACT}|{package.sha256}|{target_session.isoformat()}"
    capture_id = str(uuid.uuid5(uuid.UUID(namespace_id), name))
    return content, capture_id, _candidate_digest(pool)


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


def _normalized_quality(quality: str | None, reason: str | None, *, fallback: str) -> tuple[str, str | None]:
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
    namespace_id: str,
    package_sha256: str,
    content_id: str,
    capture_id: str,
    target_session: date,
) -> dict[str, Any]:
    status = row.status
    if status not in _BAR_STATUSES:
        raise RawSourcePgError("SCAN_STATUS_UNSUPPORTED", "目标日扫描出现不可持久化状态")
    if row.symbol != candidate["symbol"] or row.market != candidate["market"] or row.board != candidate["board"]:
        raise RawSourcePgError("CANDIDATE_BINDING_MISMATCH", "目标日质量行与动态候选目录不匹配")
    if row.target_session != target_session or row.package_sha256 != package_sha256:
        raise RawSourcePgError("SOURCE_REVISION_MISMATCH", "目标日质量行与本次包版本/会话不匹配")

    target = capture.target_bar if capture is not None else None
    if status == "TARGET_PRESENT" and target is None:
        raise RawSourcePgError("TARGET_BAR_MISSING", "扫描报告有目标Bar但捕获对象缺失")
    if status != "TARGET_PRESENT" and target is not None:
        raise RawSourcePgError("TARGET_BAR_CONFLICT", "非存在状态不能携带目标Bar")
    reason = row.reason
    if status == "TARGET_PRESENT" and reason is not None:
        raise RawSourcePgError("TARGET_REASON_CONFLICT", "存在目标Bar不能携带缺失原因")
    if status != "TARGET_PRESENT" and not (isinstance(reason, str) and reason.strip()):
        raise RawSourcePgError("TARGET_REASON_MISSING", "缺Bar或读取失败必须保留明确原因")

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
            row.price_quality, row.price_reason, fallback="PRICE_QUALITY_UNKNOWN",
        )
        raw_prices = tuple(
            _field_value(getattr(target, name)) if price_quality == "OBSERVED" else None
            for name in ("raw_open", "raw_high", "raw_low", "raw_close")
        )
        amount_quality, amount_reason = _normalized_quality(
            row.amount_quality, row.amount_reason, fallback="AMOUNT_QUALITY_UNKNOWN",
        )
        volume_quality, volume_reason = _normalized_quality(
            row.volume_quality, row.volume_reason, fallback="VOLUME_QUALITY_UNKNOWN",
        )
        raw_amount_field = getattr(target, "raw_amount", None)
        raw_volume_field = getattr(target, "raw_volume", None)
        raw_amount = _field_value(raw_amount_field)
        raw_volume = getattr(raw_volume_field, "value", None) if getattr(raw_volume_field, "quality", None) == "OBSERVED" else None
        raw_amount_quality, raw_amount_reason = _field_quality(
            raw_amount_field, reason_fallback="RAW_AMOUNT_NOT_OBSERVED",
        )
        raw_volume_quality, raw_volume_reason = _field_quality(
            raw_volume_field, reason_fallback="RAW_VOLUME_NOT_OBSERVED",
        )
        if raw_volume is not None and type(raw_volume) is not int:
            raise RawSourcePgError("RAW_VOLUME_INVALID", "TDX来源原始数量必须为整数")
        if raw_volume is not None and raw_volume < 0:
            raise RawSourcePgError("RAW_VOLUME_INVALID", "TDX来源原始数量不能为负数")

    source_ref = capture.capture.source_package_entry if capture is not None else None
    if source_ref is not None and source_ref.package_sha256 != package_sha256:
        raise RawSourcePgError("ENTRY_PACKAGE_MISMATCH", "条目捕获没有绑定本次完整包SHA")
    if source_ref is not None and source_ref.source_symbol != row.symbol:
        raise RawSourcePgError("ENTRY_SYMBOL_MISMATCH", "条目捕获没有绑定本次来源代码")
    entry_sha = row.entry_sha256
    if entry_sha is not None and _SHA256.fullmatch(entry_sha) is None:
        raise RawSourcePgError("ENTRY_SHA_INVALID", "来源条目SHA格式非法")
    if status == "TARGET_PRESENT" and entry_sha is None:
        raise RawSourcePgError("ENTRY_SHA_MISSING", "存在目标Bar但缺少逐条目SHA")
    if capture is None and status == "TARGET_MISSING" and not candidate.get("empty_placeholder"):
        raise RawSourcePgError("MISSING_CAPTURE", "非空缺Bar候选必须保留完整条目捕获和质量")

    start = min((row.observed_started_at,), default=None)
    # 运行级捕获时钟在最终扫描闭合后绑定，暂存时不写库。
    values = {
        "namespace_id": namespace_id,
        "source_symbol": row.symbol,
        "market": row.market,
        "board": row.board,
        "session_date": target_session,
        "package_sha256": package_sha256,
        "source_content_id": content_id,
        "source_capture_id": capture_id,
        "source_entry_sha256": entry_sha,
        "source_entry_bytes": int(candidate["zip_entry_bytes"]),
        "source_record_count": int(row.record_count),
        "bar_status": status,
        "bar_reason": reason,
        "raw_open": raw_prices[0], "raw_high": raw_prices[1],
        "raw_low": raw_prices[2], "raw_close": raw_prices[3],
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
    _validate_raw_candidate(values)
    return values


def _validate_raw_candidate(row: Mapping[str, Any]) -> None:
    if set(row) != set(_RAW_COLUMNS):
        raise RawSourcePgError("RAW_ROW_SHAPE_INVALID", "Raw候选列集合与typed表合同不一致")
    if row["namespace_id"] is None or not _SYMBOL.fullmatch(row["source_symbol"]):
        raise RawSourcePgError("RAW_IDENTITY_INVALID", "Raw来源身份必须是命名空间加来源代码，不接受稳定证券ID推断")
    if row["market"] != row["source_symbol"][:2] or row["board"] not in {"MAIN_BOARD", "GEM"}:
        raise RawSourcePgError("RAW_MARKET_INVALID", "Raw来源市场或板别超出当前沪深主板/GEM范围")
    if not _SHA256.fullmatch(str(row["package_sha256"])) or not _SHA256.fullmatch(str(row["source_content_id"])):
        raise RawSourcePgError("RAW_REVISION_INVALID", "Raw行必须绑定完整包SHA与内容摘要")
    if row["bar_status"] not in _BAR_STATUSES:
        raise RawSourcePgError("RAW_BAR_STATUS_INVALID", "目标Bar状态不在固定枚举范围")
    if row["price_quality"] not in _QUALITY or row["amount_quality"] not in _QUALITY or row["volume_quality"] not in _QUALITY:
        raise RawSourcePgError("RAW_FIELD_QUALITY_INVALID", "Raw逐字段质量不在固定枚举范围")
    if row["raw_amount_quality"] not in _RAW_QUALITY or row["raw_volume_quality"] not in _RAW_QUALITY:
        raise RawSourcePgError("RAW_SOURCE_VALUE_QUALITY_INVALID", "Raw来源数值质量不在固定枚举范围")


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
    if isinstance(value, uuid.UUID):
        return str(value)
    return value


def _row_digest(rows: Sequence[Mapping[str, Any]]) -> str:
    digest = hashlib.sha256()
    for row in sorted(rows, key=lambda item: item["source_symbol"]):
        encoded = json.dumps(
            {key: _db_scalar(row[key]) for key in _RAW_COLUMNS},
            ensure_ascii=False, sort_keys=True, allow_nan=False, separators=(",", ":"),
        ).encode("utf-8")
        digest.update(encoded)
        digest.update(b"\n")
    return digest.hexdigest()


def _row_values(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(row[column] for column in _RAW_COLUMNS)


def _read_raw_rows(cursor: Any, namespace_id: str, target_session: date, package_sha256: str) -> list[dict[str, Any]]:
    cursor.execute(
        f"SELECT {_RAW_SELECT} FROM stock.source_raw_daily "
        "WHERE namespace_id=%s::uuid AND session_date=%s AND package_sha256=%s ORDER BY source_symbol",
        (namespace_id, target_session, package_sha256),
    )
    return [dict(zip(_RAW_COLUMNS, values, strict=True)) for values in cursor.fetchall()]


def _read_capture(cursor: Any, namespace_id: str, capture_id: str) -> SourceCaptureCandidate | None:
    cursor.execute(
        """SELECT namespace_id::text,capture_id::text,content_id,parser_contract,read_started_at,
validated_at,source_available_at,source_time_quality,source_clock_role,canonical_digest,normalized_count
FROM stock.source_captures WHERE namespace_id=%s::uuid AND capture_id=%s::uuid""",
        (namespace_id, capture_id),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return SourceCaptureCandidate(*row)


def _complete_version_state(
    config: Mapping[str, Any], *, package: Any, target_session: date, pool: Any,
) -> tuple[str, dict[str, Any] | None, SourceCaptureCandidate | None]:
    with _transaction(config, write=False) as (_connection, cursor, namespace_id):
        content, capture_id, _catalog_digest = _source_identity(namespace_id, package, target_session, pool)
        capture = _read_capture(cursor, namespace_id, capture_id)
        rows = _read_raw_rows(cursor, namespace_id, target_session, package.sha256)
        if capture is None:
            if rows:
                raise RawSourcePgError("RAW_CAPTURE_MISSING", "现有来源Raw行没有对应的精确捕获记录")
            return namespace_id, None, None
        if (capture.content_id != content.content_id or capture.parser_contract != _PARSER_CONTRACT
                or capture.normalized_count != len(pool.candidates)):
            raise RawSourcePgError("CAPTURE_REVISION_CONFLICT", "同一来源包/目标日的捕获摘要与当前明确输入冲突")
        if len(rows) > capture.normalized_count:
            raise RawSourcePgError("RAW_ROW_COUNT_EXCEEDED", "同一来源版本的持久化候选数超过捕获分母")
        if len(rows) == capture.normalized_count:
            if _row_digest(rows) != capture.canonical_digest:
                raise RawSourcePgError("RAW_CAPTURE_DIGEST_MISMATCH", "完整Raw版本行集与source_captures独立摘要不一致")
            return namespace_id, {"rows": rows, "row_count": len(rows), "canonical_digest": capture.canonical_digest}, capture
        return namespace_id, {"rows": rows, "row_count": len(rows), "canonical_digest": capture.canonical_digest}, capture


def _ensure_source_metadata(
    cursor: Any,
    namespace_id: str,
    content: SourceContentCandidate,
    capture: SourceCaptureCandidate,
) -> None:
    if namespace_id != content.namespace_id or namespace_id != capture.namespace_id:
        raise RawSourcePgError("NAMESPACE_SCOPE_REJECTED", "来源引用必须属于当前唯一DEV命名空间")
    content_sql, content_params = source_content_insert_plan(content)
    cursor.execute(content_sql, content_params)
    content_readback_sql, content_readback_params = source_content_readback_plan(content)
    cursor.execute(content_readback_sql, content_readback_params)
    if not validate_source_content_readback(content, cursor.fetchall()):
        raise RawSourcePgError("SOURCE_CONTENT_CONFLICT", "同一包内容摘要读回与本次真实ZIP不一致")
    capture_sql, capture_params = source_capture_insert_plan(capture)
    cursor.execute(capture_sql, capture_params)
    capture_readback_sql, capture_readback_params = source_capture_readback_plan(capture)
    cursor.execute(capture_readback_sql, capture_readback_params)
    if not validate_source_capture_readback(capture, cursor.fetchall()):
        raise RawSourcePgError("SOURCE_CAPTURE_CONFLICT", "同一包/目标日捕获摘要读回与本次明示来源不一致")


def _insert_raw_batch(
    config: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    *,
    chunk_size: int,
    content: SourceContentCandidate | None = None,
    capture: SourceCaptureCandidate | None = None,
) -> int:
    if not 1 <= len(rows) <= chunk_size <= 100:
        raise RawSourcePgError("RAW_CHUNK_INVALID", "Raw批次必须为1至100条且不超过显式chunk_size")
    first = rows[0]
    symbols = [row["source_symbol"] for row in rows]
    if len(symbols) != len(set(symbols)) or any(
        row["namespace_id"] != first["namespace_id"]
        or row["session_date"] != first["session_date"]
        or row["package_sha256"] != first["package_sha256"]
        or row["source_content_id"] != first["source_content_id"]
        or row["source_capture_id"] != first["source_capture_id"]
        for row in rows
    ):
        raise RawSourcePgError("RAW_CHUNK_SCOPE_INVALID", "Raw批次跨越来源版本或重复来源代码")
    for row in rows:
        _validate_raw_candidate(row)
    with _transaction(config, write=True) as (_connection, cursor, namespace_id):
        if namespace_id != first["namespace_id"]:
            raise RawSourcePgError("NAMESPACE_SCOPE_REJECTED", "Raw DML只能进入当前唯一DEV命名空间")
        if (content is None) != (capture is None):
            raise RawSourcePgError("SOURCE_METADATA_PAIR_INVALID", "来源内容与捕获元数据必须同时提供")
        if content is not None and capture is not None:
            _ensure_source_metadata(cursor, namespace_id, content, capture)
        inserted_count = 0
        for row in rows:
            cursor.execute(_RAW_INSERT, _row_values(row))
            inserted_count += max(0, int(cursor.rowcount))
        cursor.execute(
            f"SELECT {_RAW_SELECT} FROM stock.source_raw_daily "
            "WHERE namespace_id=%s::uuid AND session_date=%s AND package_sha256=%s "
            "AND source_symbol = ANY(%s) ORDER BY source_symbol",
            (first["namespace_id"], first["session_date"], first["package_sha256"], symbols),
        )
        actual_rows = [dict(zip(_RAW_COLUMNS, values, strict=True)) for values in cursor.fetchall()]
        expected = {row["source_symbol"]: row for row in rows}
        actual = {row["source_symbol"]: row for row in actual_rows}
        if set(actual) != set(expected) or any(
            any(_db_scalar(actual[symbol][column]) != _db_scalar(expected[symbol][column]) for column in _RAW_COLUMNS)
            for symbol in expected
        ):
            raise RawSourcePgError("RAW_REVISION_CONFLICT", "同一source_symbol/日期/包SHA已有不同Raw值；事务已回滚")
    return inserted_count


def _existing_count(config: Mapping[str, Any], namespace_id: str, target_session: date, package_sha256: str) -> int:
    with _transaction(config, write=False) as (_connection, cursor, active_namespace):
        if active_namespace != namespace_id:
            raise RawSourcePgError("NAMESPACE_SCOPE_REJECTED", "Raw读回的命名空间与入库范围不一致")
        cursor.execute(
            "SELECT count(*) FROM stock.source_raw_daily WHERE namespace_id=%s::uuid AND session_date=%s AND package_sha256=%s",
            (namespace_id, target_session, package_sha256),
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
    if not isinstance(scan_budget, VipdataTargetScanBudget) or not 1 <= chunk_size <= 100:
        raise RawSourcePgError("SCAN_BUDGET_INVALID", "来源扫描预算或PG写入chunk_size不符合有界合同")
    if config.get("paths", {}).get("temporary_root") != "G:/codex_tmp":
        raise RawSourcePgError("TEMP_ROOT_REJECTED", "项目配置临时根不是G:/codex_tmp")
    pool = vipdata_research_candidate_pool(package)
    total = len(pool.candidates)
    validate_source_candidate_limit(total)

    namespace_id, prior_state, prior_capture = _complete_version_state(
        config, package=package, target_session=target_session, pool=pool,
    )
    if prior_state is not None and prior_state["row_count"] == total:
        return {
            "status": "ALREADY_COMPLETE", "namespace_id": namespace_id,
            "target_session": target_session.isoformat(), "package_sha256": package.sha256,
            "candidate_total": total, "row_count": total,
            "canonical_digest": prior_state["canonical_digest"],
            "source_scan_reused": False, "source_zip_reopened_for_parse": False,
            "database_writes": 0, "storage_location_status": "USER_CONFIRMED_NOT_INDEPENDENTLY_VERIFIED",
        }

    content, capture_id, catalog_digest = _source_identity(namespace_id, package, target_session, pool)
    expected_entry = {item["symbol"]: item for item in pool.candidates}
    staged: list[dict[str, Any]] = []
    scan_started = datetime.now(timezone.utc)
    observed_starts: list[datetime] = []
    observed_finishes: list[datetime] = []

    def on_candidate(quality_row: Any, entry_capture: Any | None) -> None:
        candidate = expected_entry.get(quality_row.symbol)
        if candidate is None:
            raise RawSourcePgError("CANDIDATE_NOT_FOUND", "扫描返回了不在本次ZIP动态池中的来源代码")
        if quality_row.observed_started_at is not None:
            observed_starts.append(quality_row.observed_started_at)
        if quality_row.observed_completed_at is not None:
            observed_finishes.append(quality_row.observed_completed_at)
        staged.append(_raw_candidate(
            quality_row, entry_capture, candidate=candidate, namespace_id=namespace_id,
            package_sha256=package.sha256, content_id=content.content_id,
            capture_id=capture_id, target_session=target_session,
        ))
        if len(staged) > 5000:
            raise RawSourcePgError("CANDIDATE_LIMIT_EXCEEDED", "扫描回调观察行超过5000条上限")

    report = scan_vipdata_target_quality(
        package,
        pool,
        target_session=target_session,
        units=UnitContract("TDX_RAW_SOURCE_VALUES_UNIT_UNVERIFIED_R1", None, None),
        budget=scan_budget,
        on_candidate=on_candidate,
    )
    if (report.candidate_total != total or len(staged) != total or report.not_evaluated != 0
            or report.evaluated != total or report.target_present + report.target_missing + report.read_failed != total):
        raise RawSourcePgError("TARGET_SCAN_INCOMPLETE", "全来源目标日扫描分母未闭合；未写入捕获或Raw行")
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
        raise RawSourcePgError("CANDIDATE_DUPLICATE", "动态候选池出现重复来源代码；拒绝进入PG")
    row_digest = _row_digest(staged)
    capture = SourceCaptureCandidate(
        namespace_id=namespace_id,
        capture_id=capture_id,
        content_id=content.content_id,
        parser_contract=_PARSER_CONTRACT,
        read_started_at=prior_capture.read_started_at if prior_capture is not None else scan_started,
        validated_at=prior_capture.validated_at if prior_capture is not None else scan_finished,
        source_available_at=None,
        source_time_quality="NON_PIT_OBSERVED",
        source_clock_role=_CLOCK_ROLE,
        canonical_digest=row_digest,
        normalized_count=total,
    )
    # 既有不完整批次按首次capture时钟重建目标行摘要，禁止倒签或静默改版。
    if prior_capture is not None:
        for row in staged:
            row["observed_started_at"] = prior_capture.read_started_at
            row["observed_completed_at"] = prior_capture.validated_at
        row_digest = _row_digest(staged)
        capture = SourceCaptureCandidate(
            namespace_id=namespace_id, capture_id=capture_id, content_id=content.content_id,
            parser_contract=_PARSER_CONTRACT, read_started_at=prior_capture.read_started_at,
            validated_at=prior_capture.validated_at, source_available_at=None,
            source_time_quality="NON_PIT_OBSERVED", source_clock_role=_CLOCK_ROLE,
            canonical_digest=row_digest, normalized_count=total,
        )
    if prior_capture is not None and row_digest != prior_capture.canonical_digest:
        raise RawSourcePgError("RAW_CAPTURE_DIGEST_CONFLICT", "重扫同一包/目标日产生不同Raw typed摘要")

    before = prior_state["row_count"] if prior_state is not None else 0
    committed = 0
    try:
        for start in range(0, len(staged), chunk_size):
            committed += _insert_raw_batch(
                config,
                staged[start:start + chunk_size],
                chunk_size=chunk_size,
                content=content if start == 0 else None,
                capture=capture if start == 0 else None,
            )
    except RawSourcePgError as error:
        count = _existing_count(config, namespace_id, target_session, package.sha256)
        raise RawSourcePgError(
            error.code,
            str(error),
            committed_rows=count,
            metadata_committed=prior_capture is not None or count > 0,
        ) from error
    except Exception as error:
        count = _existing_count(config, namespace_id, target_session, package.sha256)
        raise RawSourcePgError(
            "PG_WRITE_FAILED",
            f"Raw分块写入中断（{type(error).__name__}）",
            committed_rows=count,
            metadata_committed=prior_capture is not None or count > 0,
        ) from error

    after = _existing_count(config, namespace_id, target_session, package.sha256)
    if after != total:
        raise RawSourcePgError(
            "RAW_ROW_COUNT_INCOMPLETE",
            "Raw入库结束行数与候选分母不一致",
            committed_rows=after,
            metadata_committed=True,
        )
    persisted = _complete_version_state(config, package=package, target_session=target_session, pool=pool)[1]
    if persisted is None or persisted["row_count"] != total or persisted["canonical_digest"] != row_digest:
        raise RawSourcePgError(
            "RAW_FINAL_READBACK_MISMATCH",
            "Raw全量PG读回行数或精确摘要不匹配",
            committed_rows=after,
            metadata_committed=True,
        )
    return {
        "status": "COMPLETE" if report.read_failed == 0 else "COMPLETE_WITH_READ_FAILURES",
        "namespace_id": namespace_id,
        "target_session": target_session.isoformat(), "package_sha256": package.sha256,
        "candidate_total": total, "target_present": report.target_present,
        "target_missing": report.target_missing, "read_failed": report.read_failed,
        "not_evaluated": report.not_evaluated, "source_bytes_read": report.bytes_read,
        "source_records_read": report.records_read, "scan_elapsed_seconds": report.elapsed_seconds,
        "canonical_digest": row_digest, "row_count_before": before,
        "row_count_after": after, "new_rows_committed": max(0, after - before),
        "source_capture_id": capture_id, "source_content_id": content.content_id,
        "source_clock_role": _CLOCK_ROLE, "source_available_at": None,
        "unit_status": "UNIT_UNVERIFIED", "stable_security_identity_issued": False,
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

    if (not isinstance(sessions, (tuple, list)) or not sessions
            or len(sessions) > 75 or any(type(day) is not date for day in sessions)
            or tuple(sorted(set(sessions))) != tuple(sessions)):
        raise RawSourcePgError("HISTORY_SESSION_WINDOW_INVALID", "真实历史会话须为1至75个严格递增且不重复的date")
    _validate_source_input(package, sessions[-1], task_scope=task_scope)
    if not isinstance(scan_budget, VipdataTargetScanBudget) or not 1 <= chunk_size <= 100:
        raise RawSourcePgError("SCAN_BUDGET_INVALID", "历史来源扫描预算或PG分块大小无效")
    if config.get("paths", {}).get("temporary_root") != "G:/codex_tmp":
        raise RawSourcePgError("TEMP_ROOT_REJECTED", "项目临时目录必须固定为G:/codex_tmp")

    pool = vipdata_research_candidate_pool(package)
    total = len(pool.candidates)
    validate_source_candidate_limit(total)
    planned_rows = total * len(sessions)
    if type(max_raw_rows) is not int or planned_rows > max_raw_rows or planned_rows > 750_000:
        raise RawSourcePgError(
            "HISTORY_ROW_BUDGET_EXCEEDED",
            f"历史Raw行预算拒绝完整候选集：{total}个来源候选×{len(sessions)}个会话={planned_rows}行",
        )

    prior_by_session: dict[date, tuple[dict[str, Any] | None, SourceCaptureCandidate | None]] = {}
    namespace_id: str | None = None
    for session in sessions:
        active_namespace, prior_state, prior_capture = _complete_version_state(
            config, package=package, target_session=session, pool=pool,
        )
        if namespace_id is not None and active_namespace != namespace_id:
            raise RawSourcePgError("NAMESPACE_SCOPE_REJECTED", "历史扫描期间DEV来源命名空间发生变化")
        namespace_id = active_namespace
        prior_by_session[session] = (prior_state, prior_capture)
    assert namespace_id is not None

    if all(state is not None and state["row_count"] == total
           for state, _capture in prior_by_session.values()):
        existing = []
        for session in sessions:
            state, capture = prior_by_session[session]
            assert state is not None and capture is not None
            existing.append({
                "target_session": session.isoformat(),
                "candidate_total": total,
                "row_count_before": total,
                "row_count_after": total,
                "new_rows_committed": 0,
                "canonical_digest": state["canonical_digest"],
                "source_capture_id": capture.capture_id,
                "status": "ALREADY_COMPLETE",
            })
        return {
            "status": "ALREADY_COMPLETE",
            "namespace_id": namespace_id,
            "target_session": sessions[-1].isoformat(),
            "session_start": sessions[0].isoformat(),
            "session_count": len(sessions),
            "package_sha256": package.sha256,
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
            raise RawSourcePgError("CANDIDATE_BINDING_MISMATCH", "单遍来源结果不属于唯一动态候选目录")
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
        if record_dates != sorted(record_dates) or len(record_dates) != len(set(record_dates)):
            raise RawSourcePgError("SOURCE_HISTORY_ORDER_INVALID", "VIPDATA条目日期必须有序且唯一")
        future_counts: dict[date, int] = {}
        for session in sessions:
            future_counts[session] = len(record_dates) - bisect_right(record_dates, session)
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
    if (scan_report.candidate_total != total or len(staged_entries) != total
            or scan_report.evaluated != total or scan_report.not_evaluated != 0
            or scan_report.target_present + scan_report.target_missing + scan_report.read_failed != total):
        raise RawSourcePgError("TARGET_SCAN_INCOMPLETE", "历史单遍扫描候选分母没有闭合，拒绝持久化")

    scan_started = min(observed_starts) if observed_starts else datetime.now(timezone.utc)
    scan_finished = max(observed_finishes) if observed_finishes else datetime.now(timezone.utc)
    scan_started, scan_finished = _aware(scan_started), _aware(scan_finished)
    content, _last_capture_id, _catalog_digest = _source_identity(
        namespace_id, package, sessions[-1], pool,
    )
    session_results: list[dict[str, Any]] = []
    committed_total = 0

    for session in sessions:
        prior_state, prior_capture = prior_by_session[session]
        content, capture_id, _catalog_digest = _source_identity(namespace_id, package, session, pool)
        staged: list[dict[str, Any]] = []
        status_counts = {"TARGET_PRESENT": 0, "TARGET_MISSING": 0, "READ_FAILED": 0, "NOT_EVALUATED": 0}
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
                    package_sha256=package.sha256,
                    target_session=session,
                    source_index=source_index,
                    page_offset=source_index,
                    capture=capture,
                    status="TARGET_PRESENT" if target_bar is not None else "TARGET_MISSING",
                    reason=None if target_bar is not None else "TARGET_SESSION_BAR_MISSING",
                    bytes_read=scan_summary.byte_count,
                    record_count=scan_summary.record_count,
                )
            status_counts[quality.status] = status_counts.get(quality.status, 0) + 1
            staged.append(_raw_candidate(
                quality,
                capture,
                candidate=candidate,
                namespace_id=namespace_id,
                package_sha256=package.sha256,
                content_id=content.content_id,
                capture_id=capture_id,
                target_session=session,
            ))

        if (len(staged) != total or status_counts["NOT_EVALUATED"]
                or sum(status_counts.values()) != total):
            raise RawSourcePgError("TARGET_SCAN_INCOMPLETE", f"{session.isoformat()}来源候选状态分母未闭合")
        staged.sort(key=lambda row: row["source_symbol"])
        if len({row["source_symbol"] for row in staged}) != total:
            raise RawSourcePgError("CANDIDATE_DUPLICATE", "历史候选出现重复来源代码，拒绝写入PG")

        effective_started = prior_capture.read_started_at if prior_capture is not None else scan_started
        effective_finished = prior_capture.validated_at if prior_capture is not None else scan_finished
        for row in staged:
            row["observed_started_at"] = effective_started
            row["observed_completed_at"] = effective_finished
        row_digest = _row_digest(staged)
        capture_candidate = SourceCaptureCandidate(
            namespace_id=namespace_id,
            capture_id=capture_id,
            content_id=content.content_id,
            parser_contract=_PARSER_CONTRACT,
            read_started_at=effective_started,
            validated_at=effective_finished,
            source_available_at=None,
            source_time_quality="NON_PIT_OBSERVED",
            source_clock_role=_CLOCK_ROLE,
            canonical_digest=row_digest,
            normalized_count=total,
        )
        if prior_capture is not None and row_digest != prior_capture.canonical_digest:
            raise RawSourcePgError(
                "RAW_CAPTURE_DIGEST_CONFLICT",
                f"{session.isoformat()}历史重扫与初次不可变捕获摘要不同，拒绝覆盖",
            )

        before = prior_state["row_count"] if prior_state is not None else 0
        if before < total:
            try:
                for start in range(0, len(staged), chunk_size):
                    committed_total += _insert_raw_batch(
                        config,
                        staged[start:start + chunk_size],
                        chunk_size=chunk_size,
                        content=content if start == 0 else None,
                        capture=capture_candidate if start == 0 else None,
                    )
            except RawSourcePgError as error:
                committed_rows = _existing_count(config, namespace_id, session, package.sha256)
                committed_sessions = [
                    item["target_session"] for item in session_results
                    if item.get("row_count_after") == total
                ]
                raise RawSourcePgError(
                    error.code,
                    f"历史会话{session.isoformat()}写入失败；此前已完成会话={len(committed_sessions)}；"
                    f"本会话PG现存行={committed_rows}/{total}；可按同包同窗幂等重试",
                    committed_rows=committed_rows,
                    metadata_committed=prior_capture is not None or committed_rows > 0,
                ) from error
        after = _existing_count(config, namespace_id, session, package.sha256)
        if after != total:
            raise RawSourcePgError(
                "RAW_ROW_COUNT_INCOMPLETE",
                f"{session.isoformat()}历史行数不完整：{after}/{total}",
                committed_rows=after,
                metadata_committed=True,
            )
        persisted = _complete_version_state(
            config, package=package, target_session=session, pool=pool,
        )[1]
        if persisted is None or persisted["row_count"] != total or persisted["canonical_digest"] != row_digest:
            raise RawSourcePgError(
                "RAW_FINAL_READBACK_MISMATCH",
                f"{session.isoformat()}历史Raw完整读回摘要不匹配",
                committed_rows=after,
                metadata_committed=True,
            )
        session_results.append({
            "target_session": session.isoformat(),
            "candidate_total": total,
            "target_present": status_counts["TARGET_PRESENT"],
            "target_missing": status_counts["TARGET_MISSING"],
            "read_failed": status_counts["READ_FAILED"],
            "not_evaluated": status_counts["NOT_EVALUATED"],
            "row_count_before": before,
            "row_count_after": after,
            "new_rows_committed": max(0, after - before),
            "canonical_digest": row_digest,
            "source_capture_id": capture_id,
            "source_content_id": content.content_id,
            "status": "COMPLETE_WITH_READ_FAILURES" if status_counts["READ_FAILED"] else "COMPLETE",
        })

    return {
        "status": "COMPLETE_WITH_READ_FAILURES" if scan_report.read_failed else "COMPLETE",
        "namespace_id": namespace_id,
        "target_session": sessions[-1].isoformat(),
        "session_start": sessions[0].isoformat(),
        "session_count": len(sessions),
        "package_sha256": package.sha256,
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
    config: Mapping[str, Any], *, source_symbol: str, target_session: date, package_sha256: str,
) -> dict[str, Any] | None:
    if not isinstance(source_symbol, str) or _SYMBOL.fullmatch(source_symbol) is None:
        raise RawSourcePgError("SOURCE_SYMBOL_INVALID", "PG来源Raw查询必须使用SH./SZ.来源代码")
    if type(target_session) is not date or not isinstance(package_sha256, str) or _SHA256.fullmatch(package_sha256) is None:
        raise RawSourcePgError("PG_QUERY_KEY_INVALID", "PG查询必须提供精确目标会话与64位来源包SHA")
    with _transaction(config, write=False) as (_connection, cursor, namespace_id):
        cursor.execute(
            """SELECT r.*, c.read_started_at AS capture_read_started_at,
       c.validated_at AS capture_validated_at, c.source_time_quality AS capture_time_quality,
       c.source_clock_role AS capture_clock_role, c.canonical_digest AS capture_digest,
       s.dataset_id, s.byte_sha, s.byte_count, c.content_id AS capture_content_id
FROM stock.source_raw_daily r
JOIN stock.source_captures c ON c.namespace_id=r.namespace_id AND c.capture_id=r.source_capture_id
JOIN stock.source_contents s ON s.namespace_id=r.namespace_id AND s.content_id=r.source_content_id
WHERE r.namespace_id=%s::uuid AND r.source_symbol=%s AND r.session_date=%s AND r.package_sha256=%s""",
            (namespace_id, source_symbol, target_session, package_sha256),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        names = [item.name for item in cursor.description]
        result = dict(zip(names, row, strict=True))
        if (result["byte_sha"] != package_sha256 or result["dataset_id"] != _DATASET_ID
                or result["capture_content_id"] != result["source_content_id"]):
            raise RawSourcePgError("SOURCE_REFERENCE_MISMATCH", "Raw读回的package SHA或source_contents数据集不匹配")
        result["namespace_id"] = str(result["namespace_id"])
        result["source_capture_id"] = str(result["source_capture_id"])
        result["identity_status"] = "SOURCE_SYMBOL_ONLY"
        result["pit_status"] = "NON_PIT_RETROSPECTIVE"
        result["amount_unit_status"] = "UNIT_UNVERIFIED"
        result["volume_unit_status"] = "UNIT_UNVERIFIED"
        return result


def _validate_history_query(
    *, source_symbol: str | None, target_session: date, package_sha256: str, sessions: Sequence[date],
) -> tuple[date, ...]:
    if source_symbol is not None and (not isinstance(source_symbol, str) or _SYMBOL.fullmatch(source_symbol) is None):
        raise RawSourcePgError("SOURCE_SYMBOL_INVALID", "PG历史查询证券须为SH./SZ.来源代码")
    if (type(target_session) is not date or not isinstance(package_sha256, str)
            or _SHA256.fullmatch(package_sha256) is None):
        raise RawSourcePgError("PG_QUERY_KEY_INVALID", "PG历史查询必须显式绑定目标会话和64位包SHA")
    if (not isinstance(sessions, (tuple, list)) or not 1 <= len(sessions) <= 75
            or any(type(day) is not date for day in sessions)
            or tuple(sorted(set(sessions))) != tuple(sessions)
            or sessions[-1] != target_session):
        raise RawSourcePgError("HISTORY_SESSION_WINDOW_INVALID", "PG历史查询会话须为1至75个递增会话并以目标日结束")
    return tuple(sessions)


def _read_history_rows(
    cursor: Any,
    *,
    namespace_id: str,
    source_symbols: Sequence[str],
    sessions: Sequence[date],
    package_sha256: str,
) -> list[dict[str, Any]]:
    if not source_symbols:
        return []
    cursor.execute(
        f"""SELECT r.*, c.read_started_at AS capture_read_started_at,
       c.validated_at AS capture_validated_at, c.source_time_quality AS capture_time_quality,
       c.source_clock_role AS capture_clock_role, c.canonical_digest AS capture_digest,
       c.normalized_count AS capture_normalized_count, s.dataset_id, s.byte_sha, s.byte_count,
       c.content_id AS capture_content_id
FROM stock.source_raw_daily r
JOIN stock.source_captures c ON c.namespace_id=r.namespace_id AND c.capture_id=r.source_capture_id
JOIN stock.source_contents s ON s.namespace_id=r.namespace_id AND s.content_id=r.source_content_id
WHERE r.namespace_id=%s::uuid AND r.source_symbol=ANY(%s) AND r.session_date=ANY(%s::date[])
  AND r.package_sha256=%s
ORDER BY r.source_symbol, r.session_date""",
        (namespace_id, list(source_symbols), list(sessions), package_sha256),
    )
    names = [item.name for item in cursor.description]
    result = [dict(zip(names, values, strict=True)) for values in cursor.fetchall()]
    for row in result:
        if (row["byte_sha"] != package_sha256 or row["dataset_id"] != _DATASET_ID
                or row["source_symbol"] not in source_symbols
                or row["session_date"] not in sessions
                or row["source_entry_sha256"] is not None and _SHA256.fullmatch(row["source_entry_sha256"]) is None
                or row["capture_normalized_count"] <= 0
                or row["capture_content_id"] != row["source_content_id"]):
            raise RawSourcePgError("SOURCE_REFERENCE_MISMATCH", "PG历史Raw行与显式包、来源捕获或内容元数据不匹配")
        if row["bar_status"] not in _BAR_STATUSES:
            raise RawSourcePgError("SOURCE_STATUS_INVALID", "PG历史Raw包含未登记的来源状态")
        row["namespace_id"] = str(row["namespace_id"])
        row["source_capture_id"] = str(row["source_capture_id"])
        row["identity_status"] = "SOURCE_SYMBOL_ONLY"
        row["pit_status"] = "NON_PIT_RETROSPECTIVE"
        row["price_coordinate"] = "RAW_UNADJUSTED"
        row["amount_unit_status"] = "UNIT_UNVERIFIED"
        row["volume_unit_status"] = "UNIT_UNVERIFIED"
    return result


def read_raw_daily_history_stock(
    config: Mapping[str, Any],
    *,
    source_symbol: str,
    target_session: date,
    package_sha256: str,
    sessions: Sequence[date],
) -> dict[str, Any]:
    """从固定PG namespace读取单证券的精确包版本与官方会话窗口。"""
    selected_sessions = _validate_history_query(
        source_symbol=source_symbol, target_session=target_session,
        package_sha256=package_sha256, sessions=sessions,
    )
    with _transaction(config, write=False) as (_connection, cursor, namespace_id):
        cursor.execute(
            "SELECT count(*) FROM stock.source_raw_daily WHERE namespace_id=%s::uuid "
            "AND source_symbol=%s AND session_date=%s AND package_sha256=%s",
            (namespace_id, source_symbol, target_session, package_sha256),
        )
        target_count = int(cursor.fetchone()[0])
        if target_count == 0:
            raise RawSourcePgError("PG_SOURCE_NO_MATCH", "PG中没有该目标日与显式VIPDATA包SHA的Raw候选")
        if target_count != 1:
            raise RawSourcePgError("PG_SOURCE_DUPLICATE", "PG目标日来源代码存在重复Raw行")
        rows = _read_history_rows(
            cursor, namespace_id=namespace_id, source_symbols=(source_symbol,),
            sessions=selected_sessions, package_sha256=package_sha256,
        )
    actual_sessions = tuple(row["session_date"] for row in rows)
    if actual_sessions != selected_sessions:
        missing = sorted(set(selected_sessions) - set(actual_sessions))
        raise RawSourcePgError(
            "PG_HISTORY_SESSION_MISSING",
            "PG历史窗口未闭合；缺少明确交易会话：" + ",".join(day.isoformat() for day in missing),
        )
    return {
        "status": "AVAILABLE",
        "namespace_id": namespace_id,
        "source_symbol": source_symbol,
        "target_session": target_session.isoformat(),
        "package_sha256": package_sha256,
        "session_start": selected_sessions[0].isoformat(),
        "session_end": selected_sessions[-1].isoformat(),
        "session_count": len(selected_sessions),
        "history_rows": rows,
        "source_zip_opened": False,
        "source_recalculated": False,
        "ddl_executed": False,
        "database_write": False,
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "pit_status": "NON_PIT_RETROSPECTIVE",
    }


def _page_version_status(cursor: Any, namespace_id: str, target_session: date,
                         package_sha256: str) -> tuple[str, int, int | None]:
    """同一只读快照中核验未过滤的目标日分母和来源引用。"""
    cursor.execute("""SELECT count(*)::bigint, count(DISTINCT r.source_capture_id)::bigint,
       min(c.normalized_count), max(c.normalized_count),
       count(*) FILTER (WHERE c.capture_id IS NULL OR s.content_id IS NULL
           OR c.content_id IS DISTINCT FROM r.source_content_id
           OR s.byte_sha IS DISTINCT FROM r.package_sha256
           OR s.dataset_id IS DISTINCT FROM %s)::bigint
FROM stock.source_raw_daily r
LEFT JOIN stock.source_captures c ON c.namespace_id=r.namespace_id AND c.capture_id=r.source_capture_id
LEFT JOIN stock.source_contents s ON s.namespace_id=r.namespace_id AND s.content_id=r.source_content_id
WHERE r.namespace_id=%s::uuid AND r.session_date=%s AND r.package_sha256=%s""",
                   (_DATASET_ID, namespace_id, target_session, package_sha256))
    actual, captures, expected, expected_max, broken = cursor.fetchone()
    if actual == 0:
        return "EMPTY", 0, None
    if (captures != 1 or broken or expected is None or expected <= 0
            or expected != expected_max or actual > expected):
        raise RawSourcePgError("SOURCE_REFERENCE_MISMATCH", "Raw分页的来源引用或捕获分母不一致")
    return ("CLOSED" if actual == expected else "PARTIAL", int(actual), int(expected))


def read_raw_daily_history_page(
    config: Mapping[str, Any],
    *,
    target_session: date,
    package_sha256: str,
    sessions: Sequence[date],
    offset: int,
    limit: int,
    market: str,
    board: str,
) -> dict[str, Any]:
    """SQL过滤并分页选候选，再以单个只读PG事务批量读取本页历史。"""
    selected_sessions = _validate_history_query(
        source_symbol=None, target_session=target_session,
        package_sha256=package_sha256, sessions=sessions,
    )
    if (type(offset) is not int or not 0 <= offset <= 2**31 - 1
            or type(limit) is not int or not 1 <= limit <= 100):
        raise RawSourcePgError("PG_PAGE_INVALID", "PG历史分页offset须为非负整数且limit为1至100")
    if market not in {"ALL", "SH", "SZ"} or board not in {"ALL", "MAIN_BOARD", "GEM"}:
        raise RawSourcePgError("PG_FILTER_INVALID", "PG历史分页市场或板别过滤无效")
    with _transaction(config, write=False) as (_connection, cursor, namespace_id):
        version_status, version_rows, expected_rows = _page_version_status(
            cursor, namespace_id, target_session, package_sha256,
        )
        cursor.execute(
            """SELECT bar_status, count(*) FROM stock.source_raw_daily
WHERE namespace_id=%s::uuid AND session_date=%s AND package_sha256=%s
  AND (%s='ALL' OR market=%s) AND (%s='ALL' OR board=%s)
GROUP BY bar_status ORDER BY bar_status""",
            (namespace_id, target_session, package_sha256, market, market, board, board),
        )
        by_status = {status: int(count) for status, count in cursor.fetchall()}
        total = sum(by_status.values())
        cursor.execute(
            """SELECT source_symbol, market, board FROM stock.source_raw_daily
WHERE namespace_id=%s::uuid AND session_date=%s AND package_sha256=%s
  AND (%s='ALL' OR market=%s) AND (%s='ALL' OR board=%s)
ORDER BY source_symbol LIMIT %s OFFSET %s""",
            (namespace_id, target_session, package_sha256, market, market, board, board, limit, offset),
        )
        page = cursor.fetchall()
        symbols = [row[0] for row in page]
        history_rows = _read_history_rows(
            cursor, namespace_id=namespace_id, source_symbols=symbols,
            sessions=selected_sessions, package_sha256=package_sha256,
        )
    grouped: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in symbols}
    for row in history_rows:
        grouped[row["source_symbol"]].append(row)
    candidates = []
    for symbol, candidate_market, candidate_board in page:
        rows = grouped[symbol]
        actual_sessions = tuple(row["session_date"] for row in rows)
        missing = sorted(set(selected_sessions) - set(actual_sessions))
        candidates.append({
            "source_symbol": symbol,
            "market": candidate_market,
            "board": candidate_board,
            "history_rows": rows,
            "history_complete": not missing,
            "missing_sessions": [day.isoformat() for day in missing],
        })
    next_offset = offset + len(page)
    return {
        "status": "AVAILABLE" if page else "EMPTY_PAGE",
        "namespace_id": namespace_id,
        "target_session": target_session.isoformat(),
        "package_sha256": package_sha256,
        "session_start": selected_sessions[0].isoformat(),
        "session_end": selected_sessions[-1].isoformat(),
        "session_count": len(selected_sessions),
        "offset": offset,
        "limit": limit,
        "page_count": len(page),
        "total_count": total,
        "next_offset": next_offset if next_offset < total else None,
        "bar_status_counts": by_status,
        "partition_closed": version_status == "CLOSED" and total == sum(by_status.values()),
        "version_status": version_status,
        "version_row_count": version_rows,
        "version_expected_row_count": expected_rows,
        "market_filter": market,
        "board_filter": board,
        "candidates": candidates,
        "source_zip_opened": False,
        "source_recalculated": False,
        "ddl_executed": False,
        "database_write": False,
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "pit_status": "NON_PIT_RETROSPECTIVE",
    }


def read_raw_daily_page(
    config: Mapping[str, Any], *, target_session: date, package_sha256: str,
    offset: int, limit: int, market: str, board: str,
) -> dict[str, Any]:
    if (type(target_session) is not date or not isinstance(package_sha256, str)
            or _SHA256.fullmatch(package_sha256) is None):
        raise RawSourcePgError("PG_QUERY_KEY_INVALID", "PG分页查询必须提供精确目标会话与64位来源包SHA")
    if type(offset) is not int or offset < 0 or offset > 2**31 - 1 or type(limit) is not int or not 1 <= limit <= 100:
        raise RawSourcePgError("PG_PAGE_INVALID", "PG分页offset须为非负整数，limit须在1至100内")
    if market not in {"ALL", "SH", "SZ"} or board not in {"ALL", "MAIN_BOARD", "GEM"}:
        raise RawSourcePgError("PG_FILTER_INVALID", "PG市场或板别过滤值无效")
    with _transaction(config, write=False) as (_connection, cursor, namespace_id):
        version_status, version_rows, expected_rows = _page_version_status(
            cursor, namespace_id, target_session, package_sha256,
        )
        cursor.execute(
            """SELECT bar_status, count(*) FROM stock.source_raw_daily
WHERE namespace_id=%s::uuid AND session_date=%s AND package_sha256=%s
  AND (%s='ALL' OR market=%s) AND (%s='ALL' OR board=%s)
GROUP BY bar_status ORDER BY bar_status""",
            (namespace_id, target_session, package_sha256, market, market, board, board),
        )
        by_status = {status: count for status, count in cursor.fetchall()}
        total = sum(by_status.values())
        cursor.execute(
            f"""SELECT {_RAW_SELECT} FROM stock.source_raw_daily
WHERE namespace_id=%s::uuid AND session_date=%s AND package_sha256=%s
  AND (%s='ALL' OR market=%s) AND (%s='ALL' OR board=%s)
ORDER BY source_symbol LIMIT %s OFFSET %s""",
            (namespace_id, target_session, package_sha256, market, market, board, board, limit, offset),
        )
        rows = [dict(zip(_RAW_COLUMNS, values, strict=True)) for values in cursor.fetchall()]
    page_rows = [{key: _db_scalar(value) for key, value in row.items()} for row in rows]
    next_offset = offset + len(rows)
    return {
        "status": "AVAILABLE" if rows else "EMPTY_PAGE",
        "namespace_id": namespace_id, "target_session": target_session.isoformat(),
        "package_sha256": package_sha256, "offset": offset, "limit": limit,
        "page_count": len(rows), "total_count": total,
        "next_offset": next_offset if next_offset < total else None,
        "bar_status_counts": by_status,
        "partition_closed": version_status == "CLOSED" and total == sum(by_status.values()),
        "version_status": version_status,
        "version_row_count": version_rows,
        "version_expected_row_count": expected_rows,
        "market_filter": market, "board_filter": board,
        "rows": page_rows, "pit_status": "NON_PIT_RETROSPECTIVE",
        "identity_status": "SOURCE_SYMBOL_ONLY", "source_zip_opened": False,
        "ddl_executed": False, "source_recalculated": False,
    }


def read_raw_daily_catalog(
    config: Mapping[str, Any], *, year: int | None = None, offset: int = 0, limit: int = 100,
) -> dict[str, Any]:
    """只读取PG已保存的Raw日期版本目录，不打开来源ZIP或执行写入。"""
    if year is not None and (type(year) is not int or not 1900 <= year <= 9998):
        raise RawSourcePgError("PG_CATALOG_YEAR_INVALID", "Raw目录年份无效")
    if (type(offset) is not int or not 0 <= offset <= 2**31 - 1
            or type(limit) is not int or not 1 <= limit <= 200):
        raise RawSourcePgError("PG_CATALOG_PAGE_INVALID", "Raw目录offset须为非负整数，limit须在1到200")

    def _where(namespace_id: str) -> tuple[str, tuple[Any, ...]]:
        clauses = ["r.namespace_id=%s::uuid"]
        params: list[Any] = [namespace_id]
        if year is not None:
            clauses.extend(("r.session_date >= %s", "r.session_date < %s"))
            params.extend((date(year, 1, 1), date(year + 1, 1, 1)))
        return " AND ".join(clauses), tuple(params)

    with _transaction(config, write=False) as (_connection, cursor, namespace_id):
        where_sql, params = _where(namespace_id)
        cursor.execute(
            f"""SELECT count(*) FROM (
    SELECT r.session_date, r.package_sha256
    FROM stock.source_raw_daily r
    WHERE {where_sql}
    GROUP BY r.session_date, r.package_sha256
) AS versions""",
            params,
        )
        total_versions = int(cursor.fetchone()[0])
        cursor.execute(
            f"""SELECT r.session_date, r.package_sha256, count(*)::bigint AS row_count,
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
       count(*) FILTER (WHERE c.capture_id IS NULL OR s.content_id IS NULL OR c.content_id IS DISTINCT FROM r.source_content_id)::bigint AS broken_reference_count,
       array_agg(DISTINCT s.dataset_id ORDER BY s.dataset_id) AS dataset_ids,
       array_agg(DISTINCT s.byte_sha ORDER BY s.byte_sha) AS content_shas,
       array_agg(DISTINCT r.price_quality ORDER BY r.price_quality) AS price_qualities,
       array_agg(DISTINCT r.amount_quality ORDER BY r.amount_quality) AS amount_qualities,
       array_agg(DISTINCT r.amount_unit_status ORDER BY r.amount_unit_status) AS amount_unit_statuses,
       array_agg(DISTINCT r.volume_quality ORDER BY r.volume_quality) AS volume_qualities,
       array_agg(DISTINCT r.volume_unit_status ORDER BY r.volume_unit_status) AS volume_unit_statuses
FROM stock.source_raw_daily r
LEFT JOIN stock.source_captures c
  ON c.namespace_id=r.namespace_id AND c.capture_id=r.source_capture_id
LEFT JOIN stock.source_contents s
  ON s.namespace_id=r.namespace_id AND s.content_id=r.source_content_id
WHERE {where_sql}
GROUP BY r.session_date, r.package_sha256
ORDER BY r.session_date DESC, r.package_sha256
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
        content_shas = item["content_shas"] or []
        dataset_ids = item["dataset_ids"] or []
        clock_roles = item["source_clock_roles"] or []
        time_qualities = item["source_time_qualities"] or []
        if (sum(status_counts.values()) != row_count or item["unknown_bar_status"] != 0
                or item["capture_count"] != 1 or item["broken_reference_count"] != 0
                or item["expected_row_count"] is None or item["expected_row_count"] <= 0
                or item["expected_row_count"] != item["expected_row_count_max"]
                or row_count > item["expected_row_count"]
                or content_shas != [item["package_sha256"]] or dataset_ids != [_DATASET_ID]
                or len(clock_roles) != 1 or len(time_qualities) != 1
                or item["capture_read_started_at"] != item["capture_read_started_at_max"]
                or item["capture_validated_at"] != item["capture_validated_at_max"]):
            raise RawSourcePgError("SOURCE_REFERENCE_MISMATCH", "Raw目录行、来源引用、时钟或状态分区未闭合")
        versions.append({
            "session_date": item["session_date"].isoformat(),
            "package_sha256": item["package_sha256"],
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
            "version_status": "CLOSED" if row_count == item["expected_row_count"] else "PARTIAL",
        })
    next_offset = offset + len(versions)
    return {
        "status": "AVAILABLE" if versions else "EMPTY",
        "namespace_id": namespace_id,
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
    config: Mapping[str, Any], *, target_session: date, package_sha256: str,
) -> dict[str, Any]:
    """对一个明确日期和包SHA做有界的PG只读汇总，不扫描目录或来源文件。"""
    if type(target_session) is not date:
        raise RawSourcePgError("PG_READINESS_DATE_INVALID", "准备状态查询必须提供明确交易日期")
    if not isinstance(package_sha256, str) or _SHA256.fullmatch(package_sha256) is None:
        raise RawSourcePgError("PG_READINESS_SHA_INVALID", "准备状态查询必须提供完整来源包SHA")

    with _transaction(config, write=False) as (_connection, cursor, namespace_id):
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
       count(*) FILTER (WHERE c.capture_id IS NULL OR s.content_id IS NULL
           OR c.content_id IS DISTINCT FROM r.source_content_id)::bigint AS broken_reference_count,
       array_agg(DISTINCT s.dataset_id ORDER BY s.dataset_id) AS dataset_ids,
       array_agg(DISTINCT s.byte_sha ORDER BY s.byte_sha) AS content_shas,
       array_agg(DISTINCT r.price_quality ORDER BY r.price_quality) AS price_qualities,
       array_agg(DISTINCT r.amount_quality ORDER BY r.amount_quality) AS amount_qualities,
       array_agg(DISTINCT r.amount_unit_status ORDER BY r.amount_unit_status) AS amount_unit_statuses,
       array_agg(DISTINCT r.volume_quality ORDER BY r.volume_quality) AS volume_qualities,
       array_agg(DISTINCT r.volume_unit_status ORDER BY r.volume_unit_status) AS volume_unit_statuses
FROM stock.source_raw_daily r
LEFT JOIN stock.source_captures c
  ON c.namespace_id=r.namespace_id AND c.capture_id=r.source_capture_id
LEFT JOIN stock.source_contents s
  ON s.namespace_id=r.namespace_id AND s.content_id=r.source_content_id
WHERE r.namespace_id=%s::uuid AND r.session_date=%s AND r.package_sha256=%s""",
            (namespace_id, target_session, package_sha256),
        )
        row = cursor.fetchone()
        names = [item.name for item in cursor.description]
        item = dict(zip(names, row, strict=True))

    stored_count = int(item["stored_row_count"])
    if stored_count == 0:
        return {
            "status": "EMPTY",
            "namespace_id": namespace_id,
            "source_date": target_session.isoformat(),
            "package_sha256": package_sha256,
        }

    counts = {
        "TARGET_PRESENT": int(item["target_present"]),
        "TARGET_MISSING": int(item["target_missing"]),
        "READ_FAILED": int(item["read_failed"]),
    }
    source_time_qualities = item["source_time_qualities"] or []
    source_clock_roles = item["source_clock_roles"] or []
    dataset_ids = item["dataset_ids"] or []
    content_shas = item["content_shas"] or []
    expected_count = item["expected_row_count"]
    if (
        sum(counts.values()) != stored_count or item["unknown_bar_status"] != 0
        or item["capture_count"] != 1 or item["broken_reference_count"] != 0
        or expected_count is None or expected_count <= 0
        or expected_count != item["expected_row_count_max"]
        or stored_count > expected_count
        or content_shas != [package_sha256] or dataset_ids != [_DATASET_ID]
        or len(source_time_qualities) != 1 or len(source_clock_roles) != 1
        or item["capture_read_started_at"] != item["capture_read_started_at_max"]
        or item["capture_validated_at"] != item["capture_validated_at_max"]
    ):
        raise RawSourcePgError(
            "SOURCE_REFERENCE_MISMATCH", "精确日期/SHA的Raw行、来源引用、时钟或状态分区未闭合",
        )

    return {
        "status": "AVAILABLE",
        "namespace_id": namespace_id,
        "source_date": target_session.isoformat(),
        "package_sha256": package_sha256,
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
        "producer": "stock.source_raw_daily + stock.source_captures + stock.source_contents",
        "consumer": "现有Raw目录、来源分页、Raw单股及本状态GET",
        "database_write": False,
        "ddl_executed": False,
        "source_zip_opened": False,
        "source_recalculated": False,
    }


def raw_source_capacity(config: Mapping[str, Any]) -> dict[str, Any]:
    """读取本批Raw表行数、占用页和关系总字节，不请求外部统计扩展。"""
    with _transaction(config, write=False) as (_connection, cursor, _namespace_id):
        cursor.execute("""SELECT count(*)::bigint, pg_relation_size('stock.source_raw_daily')::bigint,
pg_total_relation_size('stock.source_raw_daily')::bigint,
ceil(pg_relation_size('stock.source_raw_daily')::numeric/8192)::bigint
FROM stock.source_raw_daily""")
        row_count, heap_bytes, total_bytes, heap_pages = cursor.fetchone()
        cursor.execute("""SELECT indexrelid::regclass::text, pg_relation_size(indexrelid)::bigint
FROM pg_index WHERE indrelid='stock.source_raw_daily'::regclass ORDER BY indexrelid::regclass::text""")
        indexes = [{"name": name, "bytes": size} for name, size in cursor.fetchall()]
        cursor.execute("""SELECT n_live_tup::bigint, n_dead_tup::bigint
FROM pg_stat_user_tables WHERE schemaname='stock' AND relname='source_raw_daily'""")
        stats = cursor.fetchone() or (0, 0)
    return {
        "row_count": row_count, "heap_bytes": heap_bytes, "total_relation_bytes": total_bytes,
        "heap_pages_8k": heap_pages, "index_bytes": sum(item["bytes"] for item in indexes),
        "indexes": indexes, "stats_live_estimate": stats[0], "stats_dead_estimate": stats[1],
    }
