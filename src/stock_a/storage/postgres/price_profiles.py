"""按精确P03来源版本有界保存和读取价格画像。"""

from __future__ import annotations

from datetime import date
from hashlib import sha256
import json
import re
from typing import Any, Mapping, Sequence
from uuid import UUID

from .source_raw import RawSourcePgError, _transaction


MAX_PROFILE_ROWS_PER_BATCH = 5000
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


def validate_profile_batch_size(row_count: int) -> None:
    """验证受控写入批次容量；业务分母仍由精确PG来源版本决定。"""
    if type(row_count) is not int or not 1 <= row_count <= MAX_PROFILE_ROWS_PER_BATCH:
        raise PriceProfilePgError(
            "PROFILE_BATCH_LIMIT_EXCEEDED",
            f"画像写入批次须为1至{MAX_PROFILE_ROWS_PER_BATCH}条",
        )


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
    validate_profile_batch_size(len(records))
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
    validate_profile_batch_size(len(records))
    prepared = [_normalize_record(item) for item in records]
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


def list_profile_snapshots(
    config: Mapping[str, Any], *, max_target_session: date
) -> dict[str, Any]:
    if type(max_target_session) is not date:
        raise PriceProfilePgError(
            "PROFILE_SNAPSHOT_CUTOFF_REQUIRED", "画像目录必须绑定显式研究截止日期"
        )
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
WHERE p.namespace_id=%s::uuid AND p.target_session<=%s
GROUP BY p.target_session, p.source_set_id, p.package_sha256, p.profile_revision,
         p.history_window_start, p.history_window_end, p.history_requested_session_count
ORDER BY p.target_session DESC, p.source_set_id""",
            (namespace_id, max_target_session),
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


def _snapshot_for_session(
    cursor: Any,
    *,
    namespace_id: str,
    target_session: date,
    max_target_session: date,
) -> dict[str, Any]:
    cursor.execute(
        """SELECT p.target_session, p.source_set_id::text, p.package_sha256,
       p.profile_revision, p.history_window_start, p.history_window_end,
       p.history_requested_session_count, count(*)::bigint,
       min(s.raw_row_count), count(*)=min(s.raw_row_count)
FROM stock.source_symbol_price_profiles p
JOIN stock.source_sets s ON s.namespace_id=p.namespace_id
  AND s.source_set_id=p.source_set_id AND s.target_session=p.target_session
  AND s.package_sha256=p.package_sha256 AND s.seal_status='SEALED'
WHERE p.namespace_id=%s::uuid AND p.target_session=%s AND p.target_session<=%s
GROUP BY p.target_session, p.source_set_id, p.package_sha256, p.profile_revision,
         p.history_window_start, p.history_window_end, p.history_requested_session_count
ORDER BY p.source_set_id, p.profile_revision""",
        (namespace_id, target_session, max_target_session),
    )
    rows = cursor.fetchall()
    if not rows:
        raise PriceProfilePgError(
            "PROFILE_CHANGE_SNAPSHOT_MISSING",
            f"PG中没有截止{max_target_session.isoformat()}范围内{target_session.isoformat()}的完整画像快照",
        )
    if len(rows) != 1:
        raise PriceProfilePgError(
            "PROFILE_CHANGE_SNAPSHOT_AMBIGUOUS",
            f"{target_session.isoformat()}有多个来源版本；必须先明确唯一已保存快照",
        )
    row = rows[0]
    snapshot = {
        "target_session": row[0],
        "source_set_id": row[1],
        "package_sha256": str(row[2]).strip(),
        "profile_revision": str(row[3]).strip(),
        "window_start": row[4],
        "window_end": row[5],
        "history_session_count": int(row[6]),
        "source_symbol_count": int(row[7]),
        "expected_source_symbol_count": int(row[8]),
        "complete": bool(row[9]),
    }
    if not snapshot["complete"]:
        raise PriceProfilePgError(
            "PROFILE_CHANGE_SNAPSHOT_INCOMPLETE",
            f"{target_session.isoformat()}画像分母未闭合，不能比较",
        )
    return snapshot


_PROFILE_CHANGE_CTES = """
WITH old_rows AS (
    SELECT p.*, r.raw_close AS joined_raw_close,
           r.price_quality AS joined_raw_price_quality,
           r.price_reason AS joined_raw_price_reason
    FROM stock.source_symbol_price_profiles p
    LEFT JOIN stock.source_raw_daily r
      ON r.namespace_id=p.namespace_id AND r.source_symbol=p.source_symbol
     AND r.session_date=p.target_session AND r.package_sha256=p.package_sha256
    WHERE p.namespace_id=%s::uuid AND p.target_session=%s
      AND p.source_set_id=%s::uuid AND p.package_sha256=%s
      AND p.profile_revision=%s
), new_rows AS (
    SELECT p.*, r.raw_close AS joined_raw_close,
           r.price_quality AS joined_raw_price_quality,
           r.price_reason AS joined_raw_price_reason
    FROM stock.source_symbol_price_profiles p
    LEFT JOIN stock.source_raw_daily r
      ON r.namespace_id=p.namespace_id AND r.source_symbol=p.source_symbol
     AND r.session_date=p.target_session AND r.package_sha256=p.package_sha256
    WHERE p.namespace_id=%s::uuid AND p.target_session=%s
      AND p.source_set_id=%s::uuid AND p.package_sha256=%s
      AND p.profile_revision=%s
), paired AS (
    SELECT COALESCE(o.source_symbol, n.source_symbol) AS source_symbol,
           o.source_symbol IS NOT NULL AS before_present,
           n.source_symbol IS NOT NULL AS after_present,
           o.target_bar_status AS before_bar_status,
           n.target_bar_status AS after_bar_status,
           o.price_window_status AS before_window_status,
           n.price_window_status AS after_window_status,
           o.price_window_reason AS before_window_reason,
           n.price_window_reason AS after_window_reason,
           o.joined_raw_close AS before_raw_close,
           n.joined_raw_close AS after_raw_close,
           o.joined_raw_price_quality AS before_raw_price_quality,
           n.joined_raw_price_quality AS after_raw_price_quality,
           o.joined_raw_price_reason AS before_raw_price_reason,
           n.joined_raw_price_reason AS after_raw_price_reason,
           o.ma20 AS before_ma20,
           n.ma20 AS after_ma20,
           o.ma20_quality AS before_ma20_quality,
           n.ma20_quality AS after_ma20_quality,
           o.price_coordinate AS before_price_coordinate,
           n.price_coordinate AS after_price_coordinate,
           o.source_available_at AS before_source_available_at,
           n.source_available_at AS after_source_available_at,
           o.source_time_quality AS before_source_time_quality,
           n.source_time_quality AS after_source_time_quality,
           o.history_window_start AS before_window_start,
           n.history_window_start AS after_window_start,
           o.history_window_end AS before_window_end,
           n.history_window_end AS after_window_end,
           o.history_requested_session_count AS before_window_count,
           n.history_requested_session_count AS after_window_count,
           o.profile_revision::text AS before_profile_revision,
           n.profile_revision::text AS after_profile_revision,
           o.profile_field_ids AS before_profile_field_ids,
           n.profile_field_ids AS after_profile_field_ids,
           o.profile_statuses AS before_profile_statuses,
           n.profile_statuses AS after_profile_statuses,
           o.profile_qualities AS before_profile_qualities,
           n.profile_qualities AS after_profile_qualities,
           o.profile_reasons AS before_profile_reasons,
           n.profile_reasons AS after_profile_reasons,
           o.producer_contract AS before_producer_contract,
           n.producer_contract AS after_producer_contract,
           o.producer_contract_sha256 AS before_semantics_sha256,
           n.producer_contract_sha256 AS after_semantics_sha256,
           COALESCE(
               o.source_symbol IS NOT NULL AND n.source_symbol IS NOT NULL
               AND o.producer_contract=n.producer_contract
               AND o.producer_contract_sha256=n.producer_contract_sha256
               AND o.producer_version=n.producer_version
               AND o.calculation_scope=n.calculation_scope
               AND o.price_coordinate=n.price_coordinate,
               FALSE
           ) AS core_compatible,
           COALESCE(
               o.source_symbol IS NOT NULL AND n.source_symbol IS NOT NULL
               AND o.producer_contract=n.producer_contract
               AND o.producer_contract_sha256=n.producer_contract_sha256
               AND o.producer_version=n.producer_version
               AND o.calculation_scope=n.calculation_scope
               AND o.price_coordinate=n.price_coordinate
               AND o.profile_field_ids=n.profile_field_ids,
               FALSE
           ) AS profile_compatible,
           o.ma5_quality AS before_ma5_quality, n.ma5_quality AS after_ma5_quality,
           o.ma10_quality AS before_ma10_quality, n.ma10_quality AS after_ma10_quality,
           o.ma60_quality AS before_ma60_quality, n.ma60_quality AS after_ma60_quality,
           o.atr20_quality AS before_atr20_quality, n.atr20_quality AS after_atr20_quality,
           o.clv_quality AS before_clv_quality, n.clv_quality AS after_clv_quality,
           o.prior_high20_quality AS before_prior_high20_quality, n.prior_high20_quality AS after_prior_high20_quality,
           o.ret20_quality AS before_ret20_quality, n.ret20_quality AS after_ret20_quality,
           o.pos60_quality AS before_pos60_quality, n.pos60_quality AS after_pos60_quality,
           o.bias20_atr_quality AS before_bias20_atr_quality, n.bias20_atr_quality AS after_bias20_atr_quality,
           o.ma5_reason AS before_ma5_reason, n.ma5_reason AS after_ma5_reason,
           o.ma10_reason AS before_ma10_reason, n.ma10_reason AS after_ma10_reason,
           o.ma20_reason AS before_ma20_reason, n.ma20_reason AS after_ma20_reason,
           o.ma60_reason AS before_ma60_reason, n.ma60_reason AS after_ma60_reason,
           o.atr20_reason AS before_atr20_reason, n.atr20_reason AS after_atr20_reason,
           o.clv_reason AS before_clv_reason, n.clv_reason AS after_clv_reason,
           o.prior_high20_reason AS before_prior_high20_reason, n.prior_high20_reason AS after_prior_high20_reason,
           o.ret20_reason AS before_ret20_reason, n.ret20_reason AS after_ret20_reason,
           o.pos60_reason AS before_pos60_reason, n.pos60_reason AS after_pos60_reason,
           o.bias20_atr_reason AS before_bias20_atr_reason, n.bias20_atr_reason AS after_bias20_atr_reason
    FROM old_rows o FULL OUTER JOIN new_rows n USING (source_symbol)
), relations AS (
    SELECT *,
           CASE WHEN core_compatible
                  AND before_raw_price_quality='OBSERVED'
                  AND before_ma20_quality='OBSERVED'
                THEN CASE WHEN before_raw_close>before_ma20 THEN 'ABOVE'
                          WHEN before_raw_close<before_ma20 THEN 'BELOW'
                          ELSE 'EQUAL' END END AS before_close_ma20_relation,
           CASE WHEN core_compatible
                  AND after_raw_price_quality='OBSERVED'
                  AND after_ma20_quality='OBSERVED'
                THEN CASE WHEN after_raw_close>after_ma20 THEN 'ABOVE'
                          WHEN after_raw_close<after_ma20 THEN 'BELOW'
                          ELSE 'EQUAL' END END AS after_close_ma20_relation
    FROM paired
), classified AS (
    SELECT *,
           array_remove(ARRAY[
               CASE WHEN before_present AND NOT after_present THEN 'BEFORE_ONLY'::text END,
               CASE WHEN NOT before_present AND after_present THEN 'AFTER_ONLY'::text END,
               CASE WHEN before_present AND after_present
                         AND before_bar_status IS DISTINCT FROM after_bar_status
                    THEN 'TARGET_BAR_STATUS_CHANGED'::text END,
               CASE WHEN before_present AND after_present
                         AND (before_window_status IS DISTINCT FROM after_window_status
                              OR before_window_reason IS DISTINCT FROM after_window_reason)
                    THEN 'PRICE_WINDOW_STATUS_CHANGED'::text END,
               CASE WHEN before_close_ma20_relation IS NOT NULL
                         AND after_close_ma20_relation IS NOT NULL
                         AND before_close_ma20_relation IS DISTINCT FROM after_close_ma20_relation
                    THEN 'RAW_CLOSE_VS_MA20_CHANGED'::text END,
               CASE WHEN profile_compatible AND (
                         before_profile_statuses IS DISTINCT FROM after_profile_statuses
                         OR before_profile_qualities IS DISTINCT FROM after_profile_qualities)
                    THEN 'PROFILE_STATE_CHANGED'::text END,
               CASE WHEN core_compatible AND (
                         before_ma5_quality IS DISTINCT FROM after_ma5_quality
                         OR before_ma5_reason IS DISTINCT FROM after_ma5_reason
                         OR before_ma10_quality IS DISTINCT FROM after_ma10_quality
                         OR before_ma10_reason IS DISTINCT FROM after_ma10_reason
                         OR before_ma20_quality IS DISTINCT FROM after_ma20_quality
                         OR before_ma20_reason IS DISTINCT FROM after_ma20_reason
                         OR before_ma60_quality IS DISTINCT FROM after_ma60_quality
                         OR before_ma60_reason IS DISTINCT FROM after_ma60_reason
                         OR before_atr20_quality IS DISTINCT FROM after_atr20_quality
                         OR before_atr20_reason IS DISTINCT FROM after_atr20_reason
                         OR before_clv_quality IS DISTINCT FROM after_clv_quality
                         OR before_clv_reason IS DISTINCT FROM after_clv_reason
                         OR before_prior_high20_quality IS DISTINCT FROM after_prior_high20_quality
                         OR before_prior_high20_reason IS DISTINCT FROM after_prior_high20_reason
                         OR before_ret20_quality IS DISTINCT FROM after_ret20_quality
                         OR before_ret20_reason IS DISTINCT FROM after_ret20_reason
                         OR before_pos60_quality IS DISTINCT FROM after_pos60_quality
                         OR before_pos60_reason IS DISTINCT FROM after_pos60_reason
                         OR before_bias20_atr_quality IS DISTINCT FROM after_bias20_atr_quality
                         OR before_bias20_atr_reason IS DISTINCT FROM after_bias20_atr_reason)
                    THEN 'CORE_QUALITY_CHANGED'::text END
           ], NULL) AS change_types,
           array_remove(ARRAY[
               CASE WHEN core_compatible AND (before_ma5_quality IS DISTINCT FROM after_ma5_quality OR before_ma5_reason IS DISTINCT FROM after_ma5_reason) THEN 'core.ma5'::text END,
               CASE WHEN core_compatible AND (before_ma10_quality IS DISTINCT FROM after_ma10_quality OR before_ma10_reason IS DISTINCT FROM after_ma10_reason) THEN 'core.ma10'::text END,
               CASE WHEN core_compatible AND (before_ma20_quality IS DISTINCT FROM after_ma20_quality OR before_ma20_reason IS DISTINCT FROM after_ma20_reason) THEN 'core.ma20'::text END,
               CASE WHEN core_compatible AND (before_ma60_quality IS DISTINCT FROM after_ma60_quality OR before_ma60_reason IS DISTINCT FROM after_ma60_reason) THEN 'core.ma60'::text END,
               CASE WHEN core_compatible AND (before_atr20_quality IS DISTINCT FROM after_atr20_quality OR before_atr20_reason IS DISTINCT FROM after_atr20_reason) THEN 'core.atr20'::text END,
               CASE WHEN core_compatible AND (before_clv_quality IS DISTINCT FROM after_clv_quality OR before_clv_reason IS DISTINCT FROM after_clv_reason) THEN 'core.clv'::text END,
               CASE WHEN core_compatible AND (before_prior_high20_quality IS DISTINCT FROM after_prior_high20_quality OR before_prior_high20_reason IS DISTINCT FROM after_prior_high20_reason) THEN 'core.prior_high20'::text END,
               CASE WHEN core_compatible AND (before_ret20_quality IS DISTINCT FROM after_ret20_quality OR before_ret20_reason IS DISTINCT FROM after_ret20_reason) THEN 'core.ret20'::text END,
               CASE WHEN core_compatible AND (before_pos60_quality IS DISTINCT FROM after_pos60_quality OR before_pos60_reason IS DISTINCT FROM after_pos60_reason) THEN 'core.pos60'::text END,
               CASE WHEN core_compatible AND (before_bias20_atr_quality IS DISTINCT FROM after_bias20_atr_quality OR before_bias20_atr_reason IS DISTINCT FROM after_bias20_atr_reason) THEN 'core.bias20_atr'::text END
           ], NULL) AS core_quality_changed_fields
    FROM relations
)
"""


def _profile_change_snapshot_parameters(
    namespace_id: str, snapshot: Mapping[str, Any]
) -> tuple[Any, ...]:
    return (
        namespace_id,
        snapshot["target_session"],
        snapshot["source_set_id"],
        snapshot["package_sha256"],
        snapshot["profile_revision"],
    )


def read_profile_change_page(
    config: Mapping[str, Any],
    *,
    before_session: date,
    after_session: date,
    max_target_session: date,
    offset: int,
    limit: int,
    source_symbol: str | None = None,
    change_type: str = "ALL",
) -> dict[str, Any]:
    """只比较两组精确PG画像；使用稳定SQL分页，不重算Core。"""
    change_types = {
        "ALL",
        "BEFORE_ONLY",
        "AFTER_ONLY",
        "TARGET_BAR_STATUS_CHANGED",
        "PRICE_WINDOW_STATUS_CHANGED",
        "RAW_CLOSE_VS_MA20_CHANGED",
        "PROFILE_STATE_CHANGED",
        "CORE_QUALITY_CHANGED",
    }
    if (
        type(before_session) is not date
        or type(after_session) is not date
        or type(max_target_session) is not date
        or before_session >= after_session
        or after_session > max_target_session
    ):
        raise PriceProfilePgError(
            "PROFILE_CHANGE_DATE_INVALID",
            "变化比较日期必须递增，且不晚于显式研究截止日",
        )
    if (
        type(offset) is not int
        or not 0 <= offset <= 2**31 - 1
        or type(limit) is not int
        or not 1 <= limit <= 30
    ):
        raise PriceProfilePgError(
            "PROFILE_CHANGE_PAGE_INVALID", "变化列表offset须非负且limit须在1到30"
        )
    if change_type not in change_types:
        raise PriceProfilePgError("PROFILE_CHANGE_FILTER_INVALID", "变化类型筛选值无效")
    if source_symbol is not None and (
        not isinstance(source_symbol, str)
        or source_symbol != source_symbol.strip()
        or len(source_symbol) > 9
        or re.fullmatch(r"[A-Za-z0-9.]+", source_symbol) is None
    ):
        raise PriceProfilePgError(
            "PROFILE_CHANGE_SYMBOL_INVALID", "来源代码搜索只接受字母、数字和点号"
        )
    symbol_search = source_symbol.upper() if source_symbol else None
    with _transaction(config, write=False) as (_connection, cursor, namespace_id):
        before = _snapshot_for_session(
            cursor,
            namespace_id=namespace_id,
            target_session=before_session,
            max_target_session=max_target_session,
        )
        after = _snapshot_for_session(
            cursor,
            namespace_id=namespace_id,
            target_session=after_session,
            max_target_session=max_target_session,
        )
        base_parameters = (
            *_profile_change_snapshot_parameters(namespace_id, before),
            *_profile_change_snapshot_parameters(namespace_id, after),
        )
        cursor.execute(
            _PROFILE_CHANGE_CTES
            + """SELECT count(*)::bigint AS source_symbol_union_count,
       count(*) FILTER (WHERE before_present AND after_present)::bigint AS intersection_count,
       count(*) FILTER (WHERE before_present AND NOT after_present)::bigint AS before_only_count,
       count(*) FILTER (WHERE NOT before_present AND after_present)::bigint AS after_only_count,
       count(*) FILTER (WHERE before_present AND after_present AND NOT core_compatible)::bigint AS core_semantics_incomparable_count,
       count(*) FILTER (WHERE before_present AND after_present AND NOT profile_compatible)::bigint AS profile_semantics_incomparable_count,
       count(*) FILTER (WHERE before_present AND after_present
           AND (before_close_ma20_relation IS NULL OR after_close_ma20_relation IS NULL))::bigint AS close_ma20_unknown_count,
       count(*) FILTER (WHERE before_present AND after_present AND (
           array_position(before_profile_qualities, 'OBSERVED') IS NULL
           OR array_position(after_profile_qualities, 'OBSERVED') IS NULL))::bigint AS profile_unobserved_pair_count,
       count(*) FILTER (WHERE cardinality(change_types)>0)::bigint AS changed_source_symbol_count,
       count(*) FILTER (WHERE cardinality(change_types)=0)::bigint AS unchanged_source_symbol_count
FROM classified""",
            base_parameters,
        )
        summary_row = cursor.fetchone()
        summary_names = [item.name for item in cursor.description]
        summary = dict(zip(summary_names, summary_row, strict=True))
        cursor.execute(
            _PROFILE_CHANGE_CTES
            + """SELECT source_symbol, before_present, after_present,
       before_bar_status, after_bar_status,
       before_window_status, after_window_status, before_window_reason, after_window_reason,
       before_raw_close, after_raw_close, before_raw_price_quality, after_raw_price_quality,
       before_raw_price_reason, after_raw_price_reason,
       before_ma20, after_ma20, before_ma20_quality, after_ma20_quality,
       before_close_ma20_relation, after_close_ma20_relation,
       core_compatible, profile_compatible, change_types, core_quality_changed_fields,
       before_profile_field_ids, after_profile_field_ids,
       before_profile_statuses, after_profile_statuses,
       before_profile_qualities, after_profile_qualities,
       before_profile_reasons, after_profile_reasons,
       before_source_available_at, after_source_available_at,
       before_source_time_quality, after_source_time_quality,
       before_price_coordinate, after_price_coordinate,
       before_window_start, after_window_start, before_window_end, after_window_end,
       before_window_count, after_window_count,
       before_profile_revision, after_profile_revision,
       before_semantics_sha256, after_semantics_sha256,
       before_ma5_quality, after_ma5_quality, before_ma5_reason, after_ma5_reason,
       before_ma10_quality, after_ma10_quality, before_ma10_reason, after_ma10_reason,
       before_ma20_quality, after_ma20_quality, before_ma20_reason, after_ma20_reason,
       before_ma60_quality, after_ma60_quality, before_ma60_reason, after_ma60_reason,
       before_atr20_quality, after_atr20_quality, before_atr20_reason, after_atr20_reason,
       before_clv_quality, after_clv_quality, before_clv_reason, after_clv_reason,
       before_prior_high20_quality, after_prior_high20_quality, before_prior_high20_reason, after_prior_high20_reason,
       before_ret20_quality, after_ret20_quality, before_ret20_reason, after_ret20_reason,
       before_pos60_quality, after_pos60_quality, before_pos60_reason, after_pos60_reason,
       before_bias20_atr_quality, after_bias20_atr_quality, before_bias20_atr_reason, after_bias20_atr_reason,
       count(*) OVER ()::bigint AS filtered_total
FROM classified
WHERE cardinality(change_types)>0
  AND (%s='ALL' OR %s=ANY(change_types))
  AND (%s::text IS NULL OR strpos(source_symbol,%s::text)>0)
ORDER BY source_symbol
LIMIT %s OFFSET %s""",
            (
                *base_parameters,
                change_type,
                change_type,
                symbol_search,
                symbol_search,
                limit,
                offset,
            ),
        )
        names = [item.name for item in cursor.description]
        rows = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]

    for row in rows:
        row["change_types"] = list(row["change_types"] or [])
        row["core_quality_changed_fields"] = list(
            row["core_quality_changed_fields"] or []
        )
        row["profile_field_changes"] = []
        if row["profile_compatible"]:
            ids = row["before_profile_field_ids"] or []
            before_statuses = row["before_profile_statuses"] or []
            after_statuses = row["after_profile_statuses"] or []
            before_qualities = row["before_profile_qualities"] or []
            after_qualities = row["after_profile_qualities"] or []
            before_reasons = row["before_profile_reasons"] or []
            after_reasons = row["after_profile_reasons"] or []
            if (
                len(ids)
                == len(before_statuses)
                == len(after_statuses)
                == len(before_qualities)
                == len(after_qualities)
                == len(before_reasons)
                == len(after_reasons)
            ):
                row["profile_field_changes"] = [
                    {
                        "field_id": field_id,
                        "before_status": before_status,
                        "after_status": after_status,
                        "before_quality": before_quality,
                        "after_quality": after_quality,
                        "before_reason": before_reason or None,
                        "after_reason": after_reason or None,
                    }
                    for field_id, before_status, after_status, before_quality, after_quality, before_reason, after_reason in zip(
                        ids,
                        before_statuses,
                        after_statuses,
                        before_qualities,
                        after_qualities,
                        before_reasons,
                        after_reasons,
                        strict=True,
                    )
                    if before_status != after_status or before_quality != after_quality
                ]
        row["core_quality_changes"] = [
            {
                "field_id": field_id,
                "before_quality": row.get(
                    "before_" + field_id.removeprefix("core.") + "_quality"
                ),
                "after_quality": row.get(
                    "after_" + field_id.removeprefix("core.") + "_quality"
                ),
                "before_reason": row.get(
                    "before_" + field_id.removeprefix("core.") + "_reason"
                ),
                "after_reason": row.get(
                    "after_" + field_id.removeprefix("core.") + "_reason"
                ),
            }
            for field_id in row["core_quality_changed_fields"]
        ]
        for key, value in tuple(row.items()):
            if isinstance(value, date):
                row[key] = value.isoformat()
    filtered_total = int(rows[0]["filtered_total"]) if rows else 0
    for row in rows:
        row.pop("filtered_total", None)
    return {
        "status": "AVAILABLE",
        "namespace_id": namespace_id,
        "before_snapshot": before,
        "after_snapshot": after,
        "summary": {key: int(value or 0) for key, value in summary.items()},
        "page": {
            "offset": offset,
            "limit": limit,
            "count": len(rows),
            "total": filtered_total,
            "next_offset": offset + len(rows)
            if offset + len(rows) < filtered_total
            else None,
        },
        "rows": rows,
        "database_write": False,
        "core_recalculated": False,
    }


def read_profile_page(
    config: Mapping[str, Any],
    *,
    target_session: date,
    max_target_session: date,
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
    if type(max_target_session) is not date:
        raise PriceProfilePgError(
            "PROFILE_CUTOFF_REQUIRED", "画像查询必须绑定显式研究截止日期"
        )
    if type(target_session) is not date or target_session > max_target_session:
        raise PriceProfilePgError(
            "PROFILE_DATE_AFTER_CUTOFF", "画像目标日期晚于显式研究截止日期"
        )
    if type(offset) is not int or offset < 0:
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
