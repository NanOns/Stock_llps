"""按精确P03来源版本有界保存和读取价格画像。"""

from __future__ import annotations

from datetime import date
from hashlib import sha256
import json
from typing import Any, Mapping, Sequence
from uuid import UUID

from .source_raw import RawSourcePgError, _transaction


MAX_PROFILE_ROWS_PER_INSERT = 4940
INSERT_CHUNK_SIZE = 50
_CORE_COLUMN_NAMES = (
    "ma5",
    "ma10",
    "ma20",
    "ma60",
    "atr20",
    "clv",
    "prior_high20",
    "ret20",
    "pos60",
    "bias20_atr",
)
_PROFILE_COLUMNS = (
    "namespace_id",
    "source_set_id",
    "source_set_revision",
    "source_capture_id",
    "source_symbol",
    "market",
    "board",
    "target_session",
    "package_sha256",
    "profile_revision",
    "record_content_sha256",
    "calculation_scope",
    "producer_contract",
    "producer_version",
    "producer_contract_sha256",
    "price_coordinate",
    "identity_status",
    "stable_security_id_status",
    "listing_status",
    "suspension_status",
    "pit_status",
    "source_time_quality",
    "source_available_at",
    "history_calendar_id",
    "history_calendar_revision",
    "history_window_start",
    "history_window_end",
    "history_requested_session_count",
    "history_stored_session_count",
    "history_records_complete",
    "history_records_status",
    "history_actual_price_bar_count",
    "target_bar_status",
    "target_price_quality",
    "ma20_window_valid_bar_count",
    "ma20_window_expected_bar_count",
    "atr20_window_valid_bar_count",
    "atr20_window_expected_bar_count",
    "price_window_usable",
    "price_window_status",
    "price_window_reason",
    "history_window_sha256",
    "amount_unit_status",
    "volume_unit_status",
    *(
        name
        for field in _CORE_COLUMN_NAMES
        for name in (
            field,
            f"{field}_exact_numerator",
            f"{field}_exact_denominator",
            f"{field}_quality",
            f"{field}_reason",
        )
    ),
    "profile_field_ids",
    "profile_statuses",
    "profile_qualities",
    "profile_reasons",
)
_CORE_QUALITY_FIELDS = frozenset(_CORE_COLUMN_NAMES)
_CORE_FIELD_QUERY_COLUMNS = {
    f"core.{field}": f"{field}_quality" for field in _CORE_COLUMN_NAMES
}


class PriceProfilePgError(Exception):
    """精确来源画像PG读取或写入失败。"""

    preserve_transaction_error = True

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def _normalize_record(record: Mapping[str, Any]) -> dict[str, Any]:
    if frozenset(record) not in {
        frozenset(set(_PROFILE_COLUMNS) - {"record_content_sha256"}),
        frozenset(_PROFILE_COLUMNS),
    }:
        raise PriceProfilePgError(
            "PROFILE_RECORD_FIELDS_INVALID", "价格画像写入字段与typed表合同不一致"
        )
    normalized = dict(record)
    if not isinstance(normalized.get("namespace_id"), str):
        raise PriceProfilePgError("PROFILE_NAMESPACE_INVALID", "画像命名空间无效")
    try:
        normalized["namespace_id"] = str(UUID(normalized["namespace_id"]))
        normalized["source_set_id"] = str(UUID(str(normalized["source_set_id"])))
        normalized["source_capture_id"] = str(
            UUID(str(normalized["source_capture_id"]))
        )
    except (ValueError, TypeError, AttributeError) as error:
        raise PriceProfilePgError(
            "PROFILE_REFERENCE_INVALID", "画像来源引用无效"
        ) from error
    if type(normalized.get("target_session")) is not date:
        raise PriceProfilePgError("PROFILE_TARGET_DATE_INVALID", "画像目标日期无效")
    if (
        type(normalized.get("history_window_start")) is not date
        or type(normalized.get("history_window_end")) is not date
    ):
        raise PriceProfilePgError("PROFILE_WINDOW_INVALID", "画像历史窗口日期无效")
    for name in (
        "source_set_revision",
        "history_requested_session_count",
        "history_stored_session_count",
        "history_actual_price_bar_count",
        "ma20_window_valid_bar_count",
        "ma20_window_expected_bar_count",
        "atr20_window_valid_bar_count",
        "atr20_window_expected_bar_count",
    ):
        if type(normalized.get(name)) is not int:
            raise PriceProfilePgError(
                "PROFILE_WINDOW_COUNT_INVALID", "画像窗口计数必须为整数"
            )
    for name in (
        "history_records_complete",
        "price_window_usable",
    ):
        if type(normalized.get(name)) is not bool:
            raise PriceProfilePgError(
                "PROFILE_STATUS_INVALID", "画像质量状态必须为布尔值"
            )
    for name in (
        "profile_field_ids",
        "profile_statuses",
        "profile_qualities",
        "profile_reasons",
    ):
        values = normalized.get(name)
        if (
            not isinstance(values, list)
            or len(values) != 11
            or any(not isinstance(item, str) for item in values)
        ):
            raise PriceProfilePgError(
                "PROFILE_COMPONENTS_INVALID", "11项Profile状态未按合同闭合"
            )
    if len(set(normalized["profile_field_ids"])) != 11:
        raise PriceProfilePgError(
            "PROFILE_COMPONENTS_INVALID", "Profile字段标识存在重复"
        )
    for name in _CORE_COLUMN_NAMES:
        value = normalized.get(name)
        quality = normalized.get(f"{name}_quality")
        reason = normalized.get(f"{name}_reason")
        numerator = normalized.get(f"{name}_exact_numerator")
        denominator = normalized.get(f"{name}_exact_denominator")
        if quality not in {"OBSERVED", "UNKNOWN", "NOT_APPLICABLE"}:
            raise PriceProfilePgError(
                "PROFILE_CORE_QUALITY_INVALID", "Core价格字段质量无效"
            )
        if quality == "OBSERVED":
            if (
                value is None
                or reason is not None
                or type(numerator) is not int
                or type(denominator) is not int
                or denominator <= 0
            ):
                raise PriceProfilePgError(
                    "PROFILE_CORE_VALUE_INVALID", "已观测Core值与原因不一致"
                )
        elif (
            value is not None
            or numerator is not None
            or denominator is not None
            or not isinstance(reason, str)
            or not reason
        ):
            raise PriceProfilePgError(
                "PROFILE_CORE_VALUE_INVALID", "未知Core值必须保留原因且不存数值"
            )
    normalized["record_content_sha256"] = _content_sha(normalized)
    return normalized


def _canonical_value(value: Any) -> Any:
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, list):
        return [_canonical_value(item) for item in value]
    if hasattr(value, "as_tuple") and value.__class__.__name__ == "Decimal":
        return format(value, "f")
    return value


def _content_sha(record: Mapping[str, Any]) -> str:
    content = {
        name: _canonical_value(value)
        for name, value in sorted(record.items())
        if name != "record_content_sha256"
    }
    return sha256(
        json.dumps(
            content,
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def insert_profile_records_in_transaction(
    cursor: Any, records: Sequence[Mapping[str, Any]]
) -> list[tuple[str, str]]:
    """用受控批量INSERT返回数据库实际写入的来源代码与内容摘要。"""
    if not isinstance(records, (tuple, list)) or not records:
        raise PriceProfilePgError("PROFILE_BATCH_EMPTY", "价格画像批次不能为空")
    if len(records) > MAX_PROFILE_ROWS_PER_INSERT:
        raise PriceProfilePgError(
            "PROFILE_BATCH_LIMIT_EXCEEDED", "画像写入超过单日授权上限"
        )
    prepared = [_normalize_record(item) for item in records]
    symbols = [item["source_symbol"] for item in prepared]
    if len(set(symbols)) != len(symbols):
        raise PriceProfilePgError(
            "PROFILE_BATCH_DUPLICATE_SYMBOL", "画像批次来源代码重复"
        )
    if len({item["target_session"] for item in prepared}) != 1:
        raise PriceProfilePgError(
            "PROFILE_BATCH_DATE_MIXED", "单次画像写入不得混合目标日期"
        )
    inserted: list[tuple[str, str]] = []
    columns = ", ".join(_PROFILE_COLUMNS)
    row_template = f"({', '.join(['%s'] * len(_PROFILE_COLUMNS))})"
    for offset in range(0, len(prepared), INSERT_CHUNK_SIZE):
        chunk = prepared[offset : offset + INSERT_CHUNK_SIZE]
        values_sql = ", ".join(row_template for _ in chunk)
        parameters = [item[name] for item in chunk for name in _PROFILE_COLUMNS]
        cursor.execute(
            f"INSERT INTO stock.source_symbol_price_profiles ({columns}) "
            f"VALUES {values_sql} RETURNING source_symbol, record_content_sha256",
            parameters,
        )
        returned = cursor.fetchall()
        if len(returned) != len(chunk):
            raise PriceProfilePgError(
                "PROFILE_INSERT_RETURNING_MISMATCH", "画像INSERT返回行数不匹配"
            )
        inserted.extend(
            (str(symbol), str(content_sha).strip()) for symbol, content_sha in returned
        )
    return inserted


def persist_profile_records(
    config: Mapping[str, Any], records: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    """同一SERIALIZABLE事务逐键核对内容后只INSERT缺失记录。"""
    if not isinstance(records, (tuple, list)) or not records:
        raise PriceProfilePgError("PROFILE_BATCH_EMPTY", "价格画像批次不能为空")
    prepared = [_normalize_record(item) for item in records]
    if len(prepared) > MAX_PROFILE_ROWS_PER_INSERT:
        raise PriceProfilePgError(
            "PROFILE_BATCH_LIMIT_EXCEEDED", "画像写入超过单日授权上限"
        )
    first = prepared[0]
    batch_key = (
        first["namespace_id"],
        first["source_set_id"],
        first["target_session"],
        first["package_sha256"],
        first["profile_revision"],
    )
    for item in prepared:
        if (
            item["namespace_id"],
            item["source_set_id"],
            item["target_session"],
            item["package_sha256"],
            item["profile_revision"],
        ) != batch_key:
            raise PriceProfilePgError(
                "PROFILE_BATCH_KEY_MIXED", "单次画像写入混入不同来源版本"
            )
    requested = {item["source_symbol"]: item for item in prepared}
    with _transaction(config, write=True) as (_connection, cursor, namespace_id):
        if namespace_id != first["namespace_id"]:
            raise PriceProfilePgError(
                "PROFILE_NAMESPACE_MISMATCH", "画像命名空间与PG连接不一致"
            )
        cursor.execute(
            """SELECT source_set_id::text, revision_id, seal_status,
       durability_status, package_sha256, raw_row_count, source_capture_id::text
FROM stock.source_sets
WHERE namespace_id=%s::uuid AND source_set_id=%s::uuid AND target_session=%s
  AND package_sha256=%s""",
            (
                namespace_id,
                first["source_set_id"],
                first["target_session"],
                first["package_sha256"],
            ),
        )
        source_set = cursor.fetchone()
        if source_set is None or source_set[2:6] != (
            "SEALED",
            "VERIFIED_POST_COMMIT_READBACK",
            first["package_sha256"],
            len(prepared),
        ):
            raise PriceProfilePgError(
                "PROFILE_SOURCE_SET_INVALID", "P03来源集合封口或分母不匹配"
            )
        if int(source_set[1]) != first["source_set_revision"]:
            raise PriceProfilePgError(
                "PROFILE_SOURCE_SET_REVISION_MISMATCH", "P03来源集合修订不匹配"
            )
        if str(source_set[6]) != first["source_capture_id"]:
            raise PriceProfilePgError(
                "PROFILE_SOURCE_CAPTURE_MISMATCH", "画像捕获引用与P03不匹配"
            )
        cursor.execute(
            """SELECT source_symbol, market, board, source_capture_id::text
FROM stock.source_raw_daily
WHERE namespace_id=%s::uuid AND session_date=%s AND package_sha256=%s
ORDER BY source_symbol""",
            (namespace_id, first["target_session"], first["package_sha256"]),
        )
        source_rows = cursor.fetchall()
        if len(source_rows) != len(prepared):
            raise PriceProfilePgError(
                "PROFILE_SOURCE_ROW_COUNT_MISMATCH", "画像来源代码分母不匹配"
            )
        expected_by_symbol = {
            item["source_symbol"]: (
                item["market"],
                item["board"],
                item["source_capture_id"],
            )
            for item in prepared
        }
        actual_by_symbol = {
            symbol: (market, board, capture_id)
            for symbol, market, board, capture_id in source_rows
        }
        if actual_by_symbol != expected_by_symbol or set(expected_by_symbol) != set(
            requested
        ):
            raise PriceProfilePgError(
                "PROFILE_SOURCE_SYMBOL_SET_MISMATCH", "画像来源代码集合与PG Raw不一致"
            )
        cursor.execute(
            """SELECT source_symbol, record_content_sha256
FROM stock.source_symbol_price_profiles
WHERE namespace_id=%s::uuid AND source_set_id=%s::uuid AND target_session=%s
  AND package_sha256=%s AND profile_revision=%s
ORDER BY source_symbol""",
            batch_key,
        )
        existing = {
            symbol: str(content_sha).strip()
            for symbol, content_sha in cursor.fetchall()
        }
        expected_sha = {
            symbol: item["record_content_sha256"] for symbol, item in requested.items()
        }
        for symbol, content_sha in existing.items():
            if expected_sha.get(symbol) != content_sha:
                raise PriceProfilePgError(
                    "PROFILE_CONTENT_CONFLICT",
                    "同一画像业务键已保存不同内容；拒绝覆盖并回滚事务",
                )
        missing_records = [
            item for symbol, item in sorted(requested.items()) if symbol not in existing
        ]
        if missing_records:
            inserted = insert_profile_records_in_transaction(cursor, missing_records)
            if {symbol: digest for symbol, digest in inserted} != {
                item["source_symbol"]: item["record_content_sha256"]
                for item in missing_records
            }:
                raise PriceProfilePgError(
                    "PROFILE_INSERT_READBACK_MISMATCH", "画像INSERT内容回执不匹配"
                )
        else:
            inserted = []
        cursor.execute(
            """SELECT count(*)::bigint FROM stock.source_symbol_price_profiles
WHERE namespace_id=%s::uuid AND source_set_id=%s::uuid AND target_session=%s
  AND package_sha256=%s AND profile_revision=%s""",
            batch_key,
        )
        row_count_after = int(cursor.fetchone()[0])
        if row_count_after != len(prepared):
            raise PriceProfilePgError(
                "PROFILE_DAY_COUNT_MISMATCH", "画像日分母未精确闭合"
            )
    return {
        "status": "PERSISTED" if inserted else "IDEMPOTENT_READBACK",
        "target_session": first["target_session"].isoformat(),
        "source_set_id": first["source_set_id"],
        "package_sha256": first["package_sha256"],
        "profile_revision": first["profile_revision"],
        "requested_count": len(prepared),
        "inserted_count": len(inserted),
        "existing_same_content_count": len(existing),
        "row_count_after": row_count_after,
        "transaction": "SERIALIZABLE_COMMITTED",
    }


def list_profile_snapshots(config: Mapping[str, Any]) -> dict[str, Any]:
    with _transaction(config, write=False) as (_connection, cursor, namespace_id):
        cursor.execute(
            """SELECT p.target_session, p.source_set_id::text, p.package_sha256,
       p.profile_revision, p.history_window_start, p.history_window_end,
       p.history_requested_session_count, count(*)::bigint,
       min(s.raw_row_count), count(*)=min(s.raw_row_count)
FROM stock.source_symbol_price_profiles p
JOIN stock.source_sets s ON s.namespace_id=p.namespace_id
  AND s.source_set_id=p.source_set_id AND s.target_session=p.target_session
  AND s.package_sha256=p.package_sha256 AND s.seal_status='SEALED'
WHERE p.namespace_id=%s::uuid
GROUP BY p.target_session, p.source_set_id, p.package_sha256, p.profile_revision,
         p.history_window_start, p.history_window_end, p.history_requested_session_count
ORDER BY p.target_session DESC, p.source_set_id""",
            (namespace_id,),
        )
        snapshots = [
            {
                "target_session": row[0].isoformat(),
                "source_set_id": row[1],
                "package_sha256": str(row[2]).strip(),
                "profile_revision": str(row[3]).strip(),
                "window_start": row[4].isoformat(),
                "window_end": row[5].isoformat(),
                "history_session_count": int(row[6]),
                "source_symbol_count": int(row[7]),
                "expected_source_symbol_count": int(row[8]),
                "complete": bool(row[9]),
            }
            for row in cursor.fetchall()
        ]
    return {
        "status": "AVAILABLE" if snapshots else "EMPTY",
        "namespace_id": namespace_id,
        "snapshots": snapshots,
    }


def read_profile_page(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_set_id: str,
    package_sha256: str,
    profile_revision: str,
    offset: int,
    limit: int,
    market: str = "ALL",
    board: str = "ALL",
    target_bar_status: str = "ALL",
    price_window_status: str = "ALL",
    source_symbol: str | None = None,
    core_field: str | None = None,
    core_quality: str | None = None,
) -> dict[str, Any]:
    if type(target_session) is not date or type(offset) is not int or offset < 0:
        raise PriceProfilePgError("PROFILE_PAGE_INVALID", "画像查询日期或分页偏移无效")
    if type(limit) is not int or not 1 <= limit <= 100:
        raise PriceProfilePgError("PROFILE_PAGE_INVALID", "画像页大小必须为1至100")
    try:
        source_set_id = str(UUID(source_set_id))
    except (ValueError, TypeError, AttributeError) as error:
        raise PriceProfilePgError(
            "PROFILE_SOURCE_SET_ID_INVALID", "P03来源集合ID格式无效"
        ) from error
    if (
        not isinstance(profile_revision, str)
        or len(profile_revision) != 64
        or any(item not in "0123456789abcdef" for item in profile_revision)
    ):
        raise PriceProfilePgError(
            "PROFILE_REVISION_INVALID", "画像修订SHA必须是64位小写十六进制"
        )
    if market not in {"ALL", "SH", "SZ"} or board not in {"ALL", "MAIN_BOARD", "GEM"}:
        raise PriceProfilePgError("PROFILE_FILTER_INVALID", "市场或板别筛选值无效")
    if target_bar_status not in {
        "ALL",
        "TDX_TARGET_BAR_PRESENT",
        "TDX_TARGET_BAR_MISSING",
        "TDX_READ_FAILED",
    }:
        raise PriceProfilePgError("PROFILE_FILTER_INVALID", "目标Bar状态筛选值无效")
    if price_window_status not in {
        "ALL",
        "USABLE",
        "TARGET_MISSING",
        "READ_FAILED",
        "HISTORY_ROW_NOT_STORED",
        "INSUFFICIENT_VALID_BARS",
        "PRICE_FIELDS_UNKNOWN",
    }:
        raise PriceProfilePgError("PROFILE_FILTER_INVALID", "价格窗口状态筛选值无效")
    if (core_field is None) != (core_quality is None):
        raise PriceProfilePgError(
            "PROFILE_FILTER_INVALID", "Core质量筛选必须同时指定字段和质量"
        )
    core_quality_column = None
    if core_field is not None:
        core_quality_column = _CORE_FIELD_QUERY_COLUMNS.get(core_field)
        if core_quality_column is None or core_quality not in {
            "OBSERVED",
            "UNKNOWN",
            "NOT_APPLICABLE",
        }:
            raise PriceProfilePgError(
                "PROFILE_FILTER_INVALID", "Core字段或质量筛选无效"
            )
    if source_symbol is not None and (
        not isinstance(source_symbol, str)
        or len(source_symbol) != 9
        or source_symbol[:2] not in {"SH", "SZ"}
        or source_symbol[2] != "."
        or not source_symbol[3:].isdecimal()
    ):
        raise PriceProfilePgError(
            "PROFILE_SYMBOL_INVALID", "来源代码必须符合SH./SZ.六位数字格式"
        )

    conditions = [
        "p.namespace_id=%s::uuid",
        "p.target_session=%s",
        "p.source_set_id=%s::uuid",
        "p.package_sha256=%s",
        "p.profile_revision=%s",
    ]
    parameters: list[Any] = [
        None,
        target_session,
        source_set_id,
        package_sha256,
        profile_revision,
    ]
    if market != "ALL":
        conditions.append("p.market=%s")
        parameters.append(market)
    if board != "ALL":
        conditions.append("p.board=%s")
        parameters.append(board)
    if target_bar_status != "ALL":
        conditions.append("p.target_bar_status=%s")
        parameters.append(target_bar_status)
    if price_window_status != "ALL":
        conditions.append("p.price_window_status=%s")
        parameters.append(price_window_status)
    if source_symbol is not None:
        conditions.append("p.source_symbol=%s")
        parameters.append(source_symbol)
    if core_quality_column is not None:
        conditions.append(f"p.{core_quality_column}=%s")
        parameters.append(core_quality)
    where_sql = " AND ".join(conditions)
    with _transaction(config, write=False) as (_connection, cursor, namespace_id):
        parameters[0] = namespace_id
        cursor.execute(
            """SELECT source_set_id::text, revision_id, seal_status,
       durability_status, package_sha256, raw_row_count
FROM stock.source_sets WHERE namespace_id=%s::uuid AND source_set_id=%s::uuid
  AND target_session=%s AND package_sha256=%s""",
            (namespace_id, source_set_id, target_session, package_sha256),
        )
        source_set = cursor.fetchone()
        if source_set is None or source_set[2:5] != (
            "SEALED",
            "VERIFIED_POST_COMMIT_READBACK",
            package_sha256,
        ):
            raise PriceProfilePgError(
                "PROFILE_SOURCE_SET_NOT_FOUND", "没有匹配的完整SEALED来源版本"
            )
        expected_total = int(source_set[5])
        cursor.execute(
            """SELECT count(*)::bigint FROM stock.source_symbol_price_profiles
WHERE namespace_id=%s::uuid AND source_set_id=%s::uuid AND target_session=%s
  AND package_sha256=%s AND profile_revision=%s""",
            (
                namespace_id,
                source_set_id,
                target_session,
                package_sha256,
                profile_revision,
            ),
        )
        snapshot_total = int(cursor.fetchone()[0])
        if snapshot_total != expected_total:
            raise PriceProfilePgError(
                "PROFILE_SNAPSHOT_INCOMPLETE", "该精确画像修订未覆盖完整来源代码分母"
            )
        cursor.execute(
            f"SELECT count(*)::bigint FROM stock.source_symbol_price_profiles p WHERE {where_sql}",
            parameters,
        )
        total = int(cursor.fetchone()[0])
        cursor.execute(
            f"""SELECT p.*, r.raw_open AS target_raw_open,
       r.raw_high AS target_raw_high, r.raw_low AS target_raw_low,
       r.raw_close AS target_raw_close, r.price_quality AS target_raw_price_quality,
       r.price_reason AS target_raw_price_reason
FROM stock.source_symbol_price_profiles p
JOIN stock.source_raw_daily r ON r.namespace_id=p.namespace_id
  AND r.source_symbol=p.source_symbol AND r.session_date=p.target_session
  AND r.package_sha256=p.package_sha256
WHERE {where_sql}
ORDER BY p.source_symbol LIMIT %s OFFSET %s""",
            [*parameters, limit, offset],
        )
        names = [item.name for item in cursor.description]
        rows = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
        for row in rows:
            for column in ("namespace_id", "source_set_id", "source_capture_id"):
                if isinstance(row.get(column), UUID):
                    row[column] = str(row[column])
    return {
        "status": "AVAILABLE" if rows else "NO_MATCH",
        "namespace_id": namespace_id,
        "target_session": target_session.isoformat(),
        "source_set_id": source_set_id,
        "package_sha256": package_sha256,
        "profile_revision": profile_revision,
        "source_set_revision": int(source_set[1]),
        "history_window": {
            "start": rows[0]["history_window_start"].isoformat() if rows else None,
            "end": rows[0]["history_window_end"].isoformat() if rows else None,
            "requested_session_count": int(rows[0]["history_requested_session_count"])
            if rows
            else None,
        },
        "page": {
            "offset": offset,
            "limit": limit,
            "count": len(rows),
            "total": total,
            "next_offset": offset + len(rows) if offset + len(rows) < total else None,
        },
        "rows": rows,
        "execution": {
            "sql_pagination": True,
            "all_candidates_recomputed": False,
            "source_zip_opened": False,
            "database_write": False,
        },
    }
