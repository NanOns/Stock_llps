"""按精确P03来源版本有界保存和读取价格画像。"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
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
_PROFILE_QUERY_FIELD_IDS = frozenset(
    {
        "profile.closed_month_trend",
        "profile.closed_week_trend",
        "profile.compression",
        "profile.drawdown60",
        "profile.extension",
        "profile.ma_structure",
        "profile.near_high20",
        "profile.participation",
        "profile.position",
        "profile.relative",
        "profile.trend",
    }
)
_CORE_FIELD_QUERY_COLUMNS = {
    f"core.{field}": f"{field}_quality" for field in _CORE_COLUMN_NAMES
}
_PRICE_POSITION_QUERY_FIELDS = {
    "c_vs_ma20": {
        "left_field": "raw_close",
        "left_value_sql": "r.raw_close",
        "left_quality_sql": "COALESCE(r.price_quality, 'UNKNOWN')",
        "left_reason_sql": "CASE WHEN r.source_symbol IS NULL THEN 'SOURCE_RAW_ROW_MISSING' WHEN r.price_quality IS DISTINCT FROM 'OBSERVED' THEN COALESCE(r.price_reason, 'RAW_PRICE_REASON_MISSING') ELSE r.price_reason END",
        "right_field": "ma20",
        "right_value_sql": "p.ma20",
        "right_quality_sql": "p.ma20_quality",
        "right_reason_sql": "p.ma20_reason",
    },
    "c_vs_prior_high20": {
        "left_field": "raw_close",
        "left_value_sql": "r.raw_close",
        "left_quality_sql": "COALESCE(r.price_quality, 'UNKNOWN')",
        "left_reason_sql": "CASE WHEN r.source_symbol IS NULL THEN 'SOURCE_RAW_ROW_MISSING' WHEN r.price_quality IS DISTINCT FROM 'OBSERVED' THEN COALESCE(r.price_reason, 'RAW_PRICE_REASON_MISSING') ELSE r.price_reason END",
        "right_field": "prior_high20",
        "right_value_sql": "p.prior_high20",
        "right_quality_sql": "p.prior_high20_quality",
        "right_reason_sql": "p.prior_high20_reason",
    },
    "ma5_vs_ma20": {
        "left_field": "ma5",
        "left_value_sql": "p.ma5",
        "left_quality_sql": "p.ma5_quality",
        "left_reason_sql": "p.ma5_reason",
        "right_field": "ma20",
        "right_value_sql": "p.ma20",
        "right_quality_sql": "p.ma20_quality",
        "right_reason_sql": "p.ma20_reason",
    },
}
_PRICE_POSITION_STATUSES = frozenset({"ABOVE", "EQUAL", "BELOW", "UNKNOWN"})
_PRICE_RELATION_TRANSITION_TYPES = frozenset(
    {
        "ALL",
        "STRICT_CROSS_UP",
        "STRICT_CROSS_DOWN",
        "EQUAL_BOUNDARY",
        "SAME_RELATION",
        "NOT_COMPARABLE",
        "ONLY_BEFORE",
        "ONLY_AFTER",
    }
)
_PROFILE_MA20_DIRECTIONS = frozenset(
    {"ALL", "INCREASED", "UNCHANGED", "DECREASED", "UNKNOWN"}
)

_BIAS20_ATR_BAND_DEFINITIONS = {
    "ALL": {
        "label": "全部距离状态",
        "predicate": None,
        "definition": "不按bias20_atr距离或质量过滤。",
    },
    "NEAR_BOTH_HALF": {
        "label": "距MA20不超过±0.5 ATR",
        "predicate": """(
            p.bias20_atr_exact_numerator::numeric * 2
        ) BETWEEN -p.bias20_atr_exact_denominator::numeric
          AND p.bias20_atr_exact_denominator::numeric""",
        "definition": "精确 -1/2 ≤ (RAW收盘−MA20)/ATR20 ≤ +1/2。",
    },
    "NEAR_BELOW_ONE": {
        "label": "MA20下方0至1 ATR",
        "predicate": """p.bias20_atr_exact_numerator::numeric
            >= -p.bias20_atr_exact_denominator::numeric
            AND p.bias20_atr_exact_numerator::numeric < 0""",
        "definition": "精确 -1 ≤ (RAW收盘−MA20)/ATR20 ＜ 0。",
    },
    "NEAR_ABOVE_ONE": {
        "label": "MA20上方0至1 ATR",
        "predicate": """p.bias20_atr_exact_numerator::numeric > 0
            AND p.bias20_atr_exact_numerator::numeric
              <= p.bias20_atr_exact_denominator::numeric""",
        "definition": "精确 0 ＜ (RAW收盘−MA20)/ATR20 ≤ +1。",
    },
    "FAR_BELOW_ONE": {
        "label": "MA20下方超过1 ATR",
        "predicate": """p.bias20_atr_exact_numerator::numeric
            < -p.bias20_atr_exact_denominator::numeric""",
        "definition": "精确 (RAW收盘−MA20)/ATR20 ＜ -1。",
    },
    "FAR_ABOVE_ONE": {
        "label": "MA20上方超过1 ATR",
        "predicate": """p.bias20_atr_exact_numerator::numeric
            > p.bias20_atr_exact_denominator::numeric""",
        "definition": "精确 (RAW收盘−MA20)/ATR20 ＞ +1。",
    },
    "UNKNOWN": {
        "label": "距离未知",
        "predicate": "BIAS20_ATR_UNKNOWN",
        "definition": "Core质量、精确分数、RAW价格坐标或相关价格质量不可用。",
    },
}
_BIAS20_ATR_STATE_VALUES = frozenset(
    {
        "ALL",
        "FAR_BELOW_ONE",
        "NEAR_BELOW_ONE",
        "AT_MA20",
        "NEAR_ABOVE_ONE",
        "FAR_ABOVE_ONE",
        "UNKNOWN",
        "ABSENT",
    }
)
_BIAS20_ATR_UNKNOWN_SQL = """(
    p.bias20_atr_quality IS DISTINCT FROM 'OBSERVED'
    OR p.bias20_atr_exact_numerator IS NULL
    OR p.bias20_atr_exact_denominator IS NULL
    OR p.bias20_atr_exact_denominator <= 0
    OR p.price_coordinate IS DISTINCT FROM 'RAW_UNADJUSTED'
    OR p.ma20_quality IS DISTINCT FROM 'OBSERVED'
    OR p.ma20 IS NULL
    OR p.atr20_quality IS DISTINCT FROM 'OBSERVED'
    OR p.atr20 IS NULL
    OR p.atr20 <= 0
    OR r.price_quality IS DISTINCT FROM 'OBSERVED'
    OR r.raw_close IS NULL
)"""
_BIAS20_ATR_UNKNOWN_REASON_SQL = """CASE
    WHEN p.bias20_atr_quality IS DISTINCT FROM 'OBSERVED'
      THEN COALESCE(p.bias20_atr_reason,
                    'BIAS20_ATR_QUALITY=' ||
                    COALESCE(p.bias20_atr_quality, 'UNKNOWN'))
    WHEN p.bias20_atr_exact_numerator IS NULL
      OR p.bias20_atr_exact_denominator IS NULL
      OR p.bias20_atr_exact_denominator <= 0
      THEN 'BIAS20_ATR_EXACT_VALUE_UNAVAILABLE'
    WHEN p.price_coordinate IS DISTINCT FROM 'RAW_UNADJUSTED'
      THEN 'PRICE_COORDINATE_NOT_RAW_UNADJUSTED'
    WHEN p.ma20_quality IS DISTINCT FROM 'OBSERVED' OR p.ma20 IS NULL
      THEN COALESCE(p.ma20_reason, 'MA20_QUALITY_OR_VALUE_UNAVAILABLE')
    WHEN p.atr20_quality IS DISTINCT FROM 'OBSERVED'
      OR p.atr20 IS NULL
      OR p.atr20 <= 0
      THEN COALESCE(p.atr20_reason, 'ATR20_NOT_POSITIVE_OR_UNAVAILABLE')
    WHEN r.price_quality IS DISTINCT FROM 'OBSERVED'
      THEN COALESCE(r.price_reason, 'RAW_PRICE_QUALITY=' ||
                    COALESCE(r.price_quality, 'UNKNOWN'))
    WHEN r.raw_close IS NULL THEN 'RAW_CLOSE_UNAVAILABLE'
    ELSE NULL
END"""


def _profile_ma20_direction(row: Mapping[str, Any]) -> tuple[str, str | None]:
    """证据有效时直接比较双日已存MA20数值，不隐式归零。"""
    if not row["before_present"]:
        return "UNKNOWN", "BEFORE_PROFILE_MISSING"
    if not row["after_present"]:
        return "UNKNOWN", "AFTER_PROFILE_MISSING"
    before_coordinate = row["before_price_coordinate"]
    after_coordinate = row["after_price_coordinate"]
    if before_coordinate != after_coordinate:
        return "UNKNOWN", "PRICE_COORDINATE_MISMATCH"
    if before_coordinate != "RAW_UNADJUSTED":
        return (
            "UNKNOWN",
            f"PRICE_COORDINATE_UNSUPPORTED:{before_coordinate or 'UNKNOWN'}",
        )
    if not row["core_compatible"]:
        return "UNKNOWN", "CORE_SEMANTICS_INCOMPARABLE"
    for side in ("before", "after"):
        if row[f"{side}_ma20_quality"] != "OBSERVED":
            reason = row.get(f"{side}_ma20_reason") or "NO_REASON"
            return "UNKNOWN", f"{side.upper()}_MA20_QUALITY_UNKNOWN:{reason}"
        value = row[f"{side}_ma20"]
        if value is None:
            return "UNKNOWN", f"{side.upper()}_MA20_VALUE_MISSING"
        if not isinstance(value, Decimal):
            raise PriceProfilePgError(
                "PROFILE_MA20_NUMERIC_TYPE_INVALID",
                "双日MA20方向只接受PG NUMERIC/Decimal值",
            )
        if not value.is_finite() or value <= 0:
            return "UNKNOWN", f"{side.upper()}_MA20_VALUE_INVALID"
    before_ma20 = row["before_ma20"]
    after_ma20 = row["after_ma20"]
    if after_ma20 > before_ma20:
        return "INCREASED", None
    if after_ma20 < before_ma20:
        return "DECREASED", None
    return "UNCHANGED", None


_PROFILE_MA20_DIRECTION_REASON_SQL = """CASE
    WHEN NOT before_present THEN 'BEFORE_PROFILE_MISSING'
    WHEN NOT after_present THEN 'AFTER_PROFILE_MISSING'
    WHEN before_price_coordinate IS DISTINCT FROM after_price_coordinate
      THEN 'PRICE_COORDINATE_MISMATCH'
    WHEN before_price_coordinate IS DISTINCT FROM 'RAW_UNADJUSTED'
      THEN 'PRICE_COORDINATE_UNSUPPORTED:' ||
           COALESCE(before_price_coordinate, 'UNKNOWN')
    WHEN after_price_coordinate IS DISTINCT FROM 'RAW_UNADJUSTED'
      THEN 'PRICE_COORDINATE_UNSUPPORTED:' ||
           COALESCE(after_price_coordinate, 'UNKNOWN')
    WHEN NOT core_compatible THEN 'CORE_SEMANTICS_INCOMPARABLE'
    WHEN before_ma20_quality IS DISTINCT FROM 'OBSERVED'
      THEN 'BEFORE_MA20_QUALITY_UNKNOWN:' ||
           COALESCE(before_ma20_reason, 'NO_REASON')
    WHEN after_ma20_quality IS DISTINCT FROM 'OBSERVED'
      THEN 'AFTER_MA20_QUALITY_UNKNOWN:' ||
           COALESCE(after_ma20_reason, 'NO_REASON')
    WHEN before_ma20 IS NULL THEN 'BEFORE_MA20_VALUE_MISSING'
    WHEN after_ma20 IS NULL THEN 'AFTER_MA20_VALUE_MISSING'
    WHEN before_ma20::text IN ('NaN', 'Infinity', '-Infinity')
      OR before_ma20 <= 0 THEN 'BEFORE_MA20_VALUE_INVALID'
    WHEN after_ma20::text IN ('NaN', 'Infinity', '-Infinity')
      OR after_ma20 <= 0 THEN 'AFTER_MA20_VALUE_INVALID'
    ELSE NULL
END"""
_PROFILE_MA20_DIRECTION_SQL = """CASE
    WHEN ma20_direction_reason IS NOT NULL THEN 'UNKNOWN'
    WHEN after_ma20 > before_ma20 THEN 'INCREASED'
    WHEN after_ma20 < before_ma20 THEN 'DECREASED'
    ELSE 'UNCHANGED'
END"""


def _price_position_status_expression(
    fields: Mapping[str, str], *, coordinate_sql: str
) -> str:
    left = fields["left_value_sql"]
    right = fields["right_value_sql"]
    return f"""CASE
WHEN {coordinate_sql} IS DISTINCT FROM 'RAW_UNADJUSTED'
  OR {fields["left_quality_sql"]} IS DISTINCT FROM 'OBSERVED'
  OR {left} IS NULL
  OR {fields["right_quality_sql"]} IS DISTINCT FROM 'OBSERVED'
  OR {right} IS NULL THEN 'UNKNOWN'
WHEN {left} > {right} THEN 'ABOVE'
WHEN {left} < {right} THEN 'BELOW'
ELSE 'EQUAL' END"""


def _price_position_reason_expression(
    fields: Mapping[str, str], *, coordinate_sql: str
) -> str:
    return f"""CASE
WHEN {coordinate_sql} IS DISTINCT FROM 'RAW_UNADJUSTED'
  THEN 'PRICE_COORDINATE_NOT_RAW_UNADJUSTED'
WHEN {fields["left_quality_sql"]} IS DISTINCT FROM 'OBSERVED'
  THEN '{fields["left_field"]}_quality=' || COALESCE({fields["left_quality_sql"]}, 'UNKNOWN')
    || '; reason=' || COALESCE({fields["left_reason_sql"]}, 'REASON_NOT_PROVIDED')
WHEN {fields["left_value_sql"]} IS NULL THEN 'OBSERVED_LEFT_VALUE_MISSING'
WHEN {fields["right_quality_sql"]} IS DISTINCT FROM 'OBSERVED'
  THEN '{fields["right_field"]}_quality=' || COALESCE({fields["right_quality_sql"]}, 'UNKNOWN')
    || '; reason=' || COALESCE({fields["right_reason_sql"]}, 'REASON_NOT_PROVIDED')
WHEN {fields["right_value_sql"]} IS NULL THEN 'OBSERVED_RIGHT_VALUE_MISSING'
ELSE NULL END"""


def _profile_pair_price_position_fields(position: str, *, side: str) -> dict[str, str]:
    definition = _PRICE_POSITION_QUERY_FIELDS[position]

    def field_expressions(field: str) -> tuple[str, str, str]:
        if field == "raw_close":
            return (
                f"{side}_raw_close",
                f"COALESCE({side}_raw_price_quality, 'UNKNOWN')",
                f"""CASE
WHEN NOT {side}_raw_record_present THEN 'SOURCE_RAW_ROW_MISSING'
WHEN {side}_raw_price_quality IS DISTINCT FROM 'OBSERVED'
  THEN COALESCE({side}_raw_price_reason, 'RAW_PRICE_REASON_MISSING')
ELSE {side}_raw_price_reason END""",
            )
        return (
            f"{side}_{field}",
            f"{side}_{field}_quality",
            f"{side}_{field}_reason",
        )

    left_value, left_quality, left_reason = field_expressions(definition["left_field"])
    right_value, right_quality, right_reason = field_expressions(
        definition["right_field"]
    )
    return {
        "left_field": definition["left_field"],
        "left_value_sql": left_value,
        "left_quality_sql": left_quality,
        "left_reason_sql": left_reason,
        "right_field": definition["right_field"],
        "right_value_sql": right_value,
        "right_quality_sql": right_quality,
        "right_reason_sql": right_reason,
    }


def _price_position_status_sql(position: str) -> str:
    return _price_position_status_expression(
        _PRICE_POSITION_QUERY_FIELDS[position], coordinate_sql="p.price_coordinate"
    )


def _price_position_reason_sql(position: str) -> str:
    return _price_position_reason_expression(
        _PRICE_POSITION_QUERY_FIELDS[position], coordinate_sql="p.price_coordinate"
    )


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
    source_set_id: str | None = None,
    package_sha256: str | None = None,
    profile_revision: str | None = None,
) -> dict[str, Any]:
    exact_identity = (source_set_id, package_sha256, profile_revision)
    if any(value is not None for value in exact_identity):
        if not all(isinstance(value, str) and value for value in exact_identity):
            raise PriceProfilePgError(
                "PROFILE_CHANGE_SNAPSHOT_IDENTITY_INVALID",
                "精确快照必须同时提供source_set_id、package_sha256和profile_revision",
            )
        try:
            source_set_id = str(UUID(source_set_id))
        except (ValueError, TypeError, AttributeError) as error:
            raise PriceProfilePgError(
                "PROFILE_CHANGE_SNAPSHOT_IDENTITY_INVALID", "source_set_id不是UUID"
            ) from error
        if len(package_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in package_sha256
        ):
            raise PriceProfilePgError(
                "PROFILE_CHANGE_SNAPSHOT_IDENTITY_INVALID",
                "package_sha256不是小写SHA-256",
            )
        if len(profile_revision) != 64 or any(
            character not in "0123456789abcdef" for character in profile_revision
        ):
            raise PriceProfilePgError(
                "PROFILE_CHANGE_SNAPSHOT_IDENTITY_INVALID",
                "profile_revision不是小写SHA-256",
            )

    predicates = [
        "p.namespace_id=%s::uuid",
        "p.target_session=%s",
        "p.target_session<=%s",
    ]
    parameters: list[Any] = [namespace_id, target_session, max_target_session]
    if source_set_id is not None:
        predicates.extend(
            (
                "p.source_set_id=%s::uuid",
                "p.package_sha256=%s",
                "p.profile_revision=%s",
            )
        )
        parameters.extend((source_set_id, package_sha256, profile_revision))
    cursor.execute(
        """SELECT p.target_session, p.source_set_id::text, p.package_sha256,
       p.profile_revision, p.history_calendar_id, p.history_calendar_revision,
       p.history_window_start, p.history_window_end,
       p.history_requested_session_count, count(*)::bigint,
       min(s.raw_row_count), count(*)=min(s.raw_row_count)
FROM stock.source_symbol_price_profiles p
JOIN stock.source_sets s ON s.namespace_id=p.namespace_id
  AND s.source_set_id=p.source_set_id AND s.target_session=p.target_session
  AND s.package_sha256=p.package_sha256 AND s.seal_status='SEALED'
WHERE """
        + " AND ".join(predicates)
        + """
GROUP BY p.target_session, p.source_set_id, p.package_sha256, p.profile_revision,
         p.history_calendar_id, p.history_calendar_revision,
         p.history_window_start, p.history_window_end, p.history_requested_session_count
ORDER BY p.source_set_id, p.profile_revision""",
        parameters,
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
        "history_calendar_id": row[4],
        "history_calendar_revision": row[5],
        "window_start": row[6],
        "window_end": row[7],
        "history_session_count": int(row[8]),
        "source_symbol_count": int(row[9]),
        "expected_source_symbol_count": int(row[10]),
        "complete": bool(row[11]),
    }
    if not snapshot["complete"]:
        raise PriceProfilePgError(
            "PROFILE_CHANGE_SNAPSHOT_INCOMPLETE",
            f"{target_session.isoformat()}画像分母未闭合，不能比较",
        )
    return snapshot


def _bias20_atr_state_expression(side: str) -> str:
    if side not in {"before", "after"}:
        raise ValueError("side must be before or after")
    return f"""CASE
        WHEN NOT {side}_present THEN 'ABSENT'::text
        WHEN {side}_bias20_atr_quality IS DISTINCT FROM 'OBSERVED'
          OR {side}_bias20_atr_exact_numerator IS NULL
          OR {side}_bias20_atr_exact_denominator IS NULL
          OR {side}_bias20_atr_exact_denominator <= 0
          OR {side}_price_coordinate IS DISTINCT FROM 'RAW_UNADJUSTED'
          OR {side}_ma20_quality IS DISTINCT FROM 'OBSERVED'
          OR {side}_ma20 IS NULL
          OR {side}_atr20_quality IS DISTINCT FROM 'OBSERVED'
          OR {side}_atr20 IS NULL
          OR {side}_atr20 <= 0
          OR NOT {side}_raw_record_present
          OR {side}_raw_price_quality IS DISTINCT FROM 'OBSERVED'
          OR {side}_raw_close IS NULL
          THEN 'UNKNOWN'::text
        WHEN {side}_bias20_atr_exact_numerator::numeric
             < -{side}_bias20_atr_exact_denominator::numeric
          THEN 'FAR_BELOW_ONE'::text
        WHEN {side}_bias20_atr_exact_numerator::numeric = 0
          THEN 'AT_MA20'::text
        WHEN {side}_bias20_atr_exact_numerator::numeric < 0
          THEN 'NEAR_BELOW_ONE'::text
        WHEN {side}_bias20_atr_exact_numerator::numeric
             <= {side}_bias20_atr_exact_denominator::numeric
          THEN 'NEAR_ABOVE_ONE'::text
        ELSE 'FAR_ABOVE_ONE'::text
    END"""


def _bias20_atr_state_reason_expression(side: str) -> str:
    if side not in {"before", "after"}:
        raise ValueError("side must be before or after")
    return f"""CASE
        WHEN NOT {side}_present THEN 'NO_PERSISTED_PROFILE'
        WHEN {side}_bias20_atr_quality IS DISTINCT FROM 'OBSERVED'
          THEN COALESCE({side}_bias20_atr_reason,
                        'BIAS20_ATR_QUALITY=' ||
                        COALESCE({side}_bias20_atr_quality, 'UNKNOWN'))
        WHEN {side}_bias20_atr_exact_numerator IS NULL
          OR {side}_bias20_atr_exact_denominator IS NULL
          OR {side}_bias20_atr_exact_denominator <= 0
          THEN 'BIAS20_ATR_EXACT_VALUE_UNAVAILABLE'
        WHEN {side}_price_coordinate IS DISTINCT FROM 'RAW_UNADJUSTED'
          THEN 'PRICE_COORDINATE_NOT_RAW_UNADJUSTED'
        WHEN {side}_ma20_quality IS DISTINCT FROM 'OBSERVED'
          OR {side}_ma20 IS NULL
          THEN COALESCE({side}_ma20_reason,
                        'MA20_QUALITY_OR_VALUE_UNAVAILABLE')
        WHEN {side}_atr20_quality IS DISTINCT FROM 'OBSERVED'
          OR {side}_atr20 IS NULL
          OR {side}_atr20 <= 0
          THEN COALESCE({side}_atr20_reason,
                        'ATR20_NOT_POSITIVE_OR_UNAVAILABLE')
        WHEN NOT {side}_raw_record_present
          THEN 'RAW_ROW_MISSING'
        WHEN {side}_raw_price_quality IS DISTINCT FROM 'OBSERVED'
          OR {side}_raw_close IS NULL
          THEN 'RAW_PRICE_QUALITY_UNKNOWN:' ||
               COALESCE({side}_raw_price_reason,
                        {side}_window_reason,
                        'NO_REASON')
        ELSE NULL::text
    END"""


_BIAS20_ATR_PAIR_COMPATIBILITY_SQL = """CASE
    WHEN NOT before_present OR NOT after_present THEN 'SINGLE_SIDE'::text
    WHEN before_price_coordinate IS DISTINCT FROM 'RAW_UNADJUSTED'
      OR after_price_coordinate IS DISTINCT FROM 'RAW_UNADJUSTED'
      OR NOT core_compatible THEN 'PAIR_INCOMPARABLE'::text
    WHEN before_bias20_atr_state='UNKNOWN'
      OR after_bias20_atr_state='UNKNOWN' THEN 'PAIR_UNKNOWN'::text
    ELSE 'COMPARABLE'::text
END"""


_BIAS20_ATR_PAIR_REASON_SQL = """CASE
    WHEN NOT before_present THEN 'BEFORE_PROFILE_ABSENT'
    WHEN NOT after_present THEN 'AFTER_PROFILE_ABSENT'
    WHEN before_price_coordinate IS DISTINCT FROM after_price_coordinate
      THEN 'PRICE_COORDINATE_MISMATCH:' ||
           COALESCE(before_price_coordinate, 'UNKNOWN') || '->' ||
           COALESCE(after_price_coordinate, 'UNKNOWN')
    WHEN before_price_coordinate IS DISTINCT FROM 'RAW_UNADJUSTED'
      OR after_price_coordinate IS DISTINCT FROM 'RAW_UNADJUSTED'
      THEN 'PRICE_COORDINATE_NOT_RAW_UNADJUSTED:' ||
           COALESCE(before_price_coordinate, 'UNKNOWN') || '->' ||
           COALESCE(after_price_coordinate, 'UNKNOWN')
    WHEN NOT core_compatible AND before_semantics_sha256
         IS DISTINCT FROM after_semantics_sha256
      THEN 'CORE_SEMANTICS_SHA_MISMATCH:' ||
           COALESCE(before_semantics_sha256, 'UNKNOWN') || '->' ||
           COALESCE(after_semantics_sha256, 'UNKNOWN')
    WHEN NOT core_compatible AND before_producer_contract
         IS DISTINCT FROM after_producer_contract
      THEN 'CORE_PRODUCER_CONTRACT_MISMATCH:' ||
           COALESCE(before_producer_contract, 'UNKNOWN') || '->' ||
           COALESCE(after_producer_contract, 'UNKNOWN')
    WHEN NOT core_compatible AND before_producer_version
         IS DISTINCT FROM after_producer_version
      THEN 'CORE_PRODUCER_VERSION_MISMATCH:' ||
           COALESCE(before_producer_version, 'UNKNOWN') || '->' ||
           COALESCE(after_producer_version, 'UNKNOWN')
    WHEN NOT core_compatible AND before_calculation_scope
         IS DISTINCT FROM after_calculation_scope
      THEN 'CORE_CALCULATION_SCOPE_MISMATCH:' ||
           COALESCE(before_calculation_scope, 'UNKNOWN') || '->' ||
           COALESCE(after_calculation_scope, 'UNKNOWN')
    WHEN NOT core_compatible THEN 'CORE_SEMANTICS_INCOMPATIBLE'
    WHEN before_bias20_atr_state='UNKNOWN'
      AND after_bias20_atr_state='UNKNOWN'
      THEN 'BOTH_ENDPOINTS_UNKNOWN:' ||
           COALESCE(before_bias20_atr_state_reason, 'NO_REASON') || ' / ' ||
           COALESCE(after_bias20_atr_state_reason, 'NO_REASON')
    WHEN before_bias20_atr_state='UNKNOWN'
      THEN 'BEFORE_ENDPOINT_UNKNOWN:' ||
           COALESCE(before_bias20_atr_state_reason, 'NO_REASON')
    WHEN after_bias20_atr_state='UNKNOWN'
      THEN 'AFTER_ENDPOINT_UNKNOWN:' ||
           COALESCE(after_bias20_atr_state_reason, 'NO_REASON')
    WHEN before_bias20_atr_state IS DISTINCT FROM after_bias20_atr_state
      THEN 'COMPARABLE_STATE_CHANGED'
    ELSE 'COMPARABLE_STATE_UNCHANGED'
END"""


_PROFILE_CHANGE_CTES = f"""
WITH old_rows AS (
    SELECT p.*,
           r.source_symbol IS NOT NULL AS joined_raw_present,
           r.raw_open AS joined_raw_open,
           r.raw_high AS joined_raw_high,
           r.raw_low AS joined_raw_low,
           r.raw_close AS joined_raw_close,
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
    SELECT p.*,
           r.source_symbol IS NOT NULL AS joined_raw_present,
           r.raw_open AS joined_raw_open,
           r.raw_high AS joined_raw_high,
           r.raw_low AS joined_raw_low,
           r.raw_close AS joined_raw_close,
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
           o.market AS before_market,
           n.market AS after_market,
           o.board AS before_board,
           n.board AS after_board,
           o.source_set_id::text AS before_source_set_id,
           n.source_set_id::text AS after_source_set_id,
           o.source_set_revision AS before_source_set_revision,
           n.source_set_revision AS after_source_set_revision,
           o.source_capture_id::text AS before_source_capture_id,
           n.source_capture_id::text AS after_source_capture_id,
           o.package_sha256::text AS before_package_sha256,
           n.package_sha256::text AS after_package_sha256,
           o.calculation_scope AS before_calculation_scope,
           n.calculation_scope AS after_calculation_scope,
           o.producer_contract AS before_producer_contract,
           n.producer_contract AS after_producer_contract,
           o.producer_version AS before_producer_version,
           n.producer_version AS after_producer_version,
           o.producer_contract_sha256::text AS before_semantics_sha256,
           n.producer_contract_sha256::text AS after_semantics_sha256,
           o.identity_status AS before_identity_status,
           n.identity_status AS after_identity_status,
           o.stable_security_id_status AS before_stable_security_id_status,
           n.stable_security_id_status AS after_stable_security_id_status,
           o.listing_status AS before_listing_status,
           n.listing_status AS after_listing_status,
           o.suspension_status AS before_suspension_status,
           n.suspension_status AS after_suspension_status,
           o.pit_status AS before_pit_status,
           n.pit_status AS after_pit_status,
           o.target_bar_status AS before_bar_status,
           n.target_bar_status AS after_bar_status,
           o.target_price_quality AS before_target_price_quality,
           n.target_price_quality AS after_target_price_quality,
           COALESCE(o.joined_raw_present, FALSE) AS before_raw_record_present,
           COALESCE(n.joined_raw_present, FALSE) AS after_raw_record_present,
           o.price_window_status AS before_window_status,
           n.price_window_status AS after_window_status,
           o.price_window_reason AS before_window_reason,
           n.price_window_reason AS after_window_reason,
           o.joined_raw_close AS before_raw_close,
           n.joined_raw_close AS after_raw_close,
           o.joined_raw_open AS before_raw_open,
           n.joined_raw_open AS after_raw_open,
           o.joined_raw_high AS before_raw_high,
           n.joined_raw_high AS after_raw_high,
           o.joined_raw_low AS before_raw_low,
           n.joined_raw_low AS after_raw_low,
           o.joined_raw_price_quality AS before_raw_price_quality,
           n.joined_raw_price_quality AS after_raw_price_quality,
           o.joined_raw_price_reason AS before_raw_price_reason,
           n.joined_raw_price_reason AS after_raw_price_reason,
           o.ma20 AS before_ma20,
           n.ma20 AS after_ma20,
           o.ma5 AS before_ma5,
           n.ma5 AS after_ma5,
           o.ma10 AS before_ma10,
           n.ma10 AS after_ma10,
           o.ma60 AS before_ma60,
           n.ma60 AS after_ma60,
           o.atr20 AS before_atr20,
           n.atr20 AS after_atr20,
           o.clv AS before_clv,
           n.clv AS after_clv,
           o.prior_high20 AS before_prior_high20,
           n.prior_high20 AS after_prior_high20,
           o.ret20 AS before_ret20,
           n.ret20 AS after_ret20,
           o.pos60 AS before_pos60,
           n.pos60 AS after_pos60,
           o.bias20_atr AS before_bias20_atr,
           n.bias20_atr AS after_bias20_atr,
           o.bias20_atr_exact_numerator AS before_bias20_atr_exact_numerator,
           n.bias20_atr_exact_numerator AS after_bias20_atr_exact_numerator,
           o.bias20_atr_exact_denominator AS before_bias20_atr_exact_denominator,
           n.bias20_atr_exact_denominator AS after_bias20_atr_exact_denominator,
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
           o.amount_unit_status AS before_amount_unit_status,
           n.amount_unit_status AS after_amount_unit_status,
           o.volume_unit_status AS before_volume_unit_status,
           n.volume_unit_status AS after_volume_unit_status,
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
               AND cardinality(o.profile_field_ids)=11
               AND cardinality(n.profile_field_ids)=11
               AND o.profile_field_ids @> n.profile_field_ids
               AND o.profile_field_ids <@ n.profile_field_ids,
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
), endpoint_relations AS (
    SELECT *,
           CASE WHEN before_present
                  AND before_raw_record_present
                  AND before_raw_price_quality='OBSERVED'
                  AND before_ma20_quality='OBSERVED'
                  AND before_raw_close IS NOT NULL
                  AND before_ma20 IS NOT NULL
                  AND before_calculation_scope IS NOT NULL
                  AND before_producer_contract IS NOT NULL
                  AND before_producer_version IS NOT NULL
                  AND before_semantics_sha256 IS NOT NULL
                THEN CASE WHEN before_raw_close>before_ma20 THEN 'ABOVE'
                          WHEN before_raw_close<before_ma20 THEN 'BELOW'
                          ELSE 'EQUAL' END END AS before_close_ma20_relation_local,
           CASE WHEN after_present
                  AND after_raw_record_present
                  AND after_raw_price_quality='OBSERVED'
                  AND after_ma20_quality='OBSERVED'
                  AND after_raw_close IS NOT NULL
                  AND after_ma20 IS NOT NULL
                  AND after_calculation_scope IS NOT NULL
                  AND after_producer_contract IS NOT NULL
                  AND after_producer_version IS NOT NULL
                  AND after_semantics_sha256 IS NOT NULL
                THEN CASE WHEN after_raw_close>after_ma20 THEN 'ABOVE'
                          WHEN after_raw_close<after_ma20 THEN 'BELOW'
                          ELSE 'EQUAL' END END AS after_close_ma20_relation_local
    FROM paired
), source_pool_relations AS (
    SELECT *,
           CASE WHEN before_price_coordinate='RAW_UNADJUSTED'
                THEN before_close_ma20_relation_local END
                AS before_source_pool_close_ma20_relation,
           CASE WHEN after_price_coordinate='RAW_UNADJUSTED'
                THEN after_close_ma20_relation_local END
                AS after_source_pool_close_ma20_relation
    FROM endpoint_relations
), relations AS (
    SELECT *,
           CASE WHEN core_compatible
                THEN before_close_ma20_relation_local END
                AS before_close_ma20_relation,
           CASE WHEN core_compatible
                THEN after_close_ma20_relation_local END
                AS after_close_ma20_relation,
           CASE WHEN before_present
                     AND before_source_pool_close_ma20_relation IS NULL
                THEN CASE
                    WHEN NOT before_raw_record_present THEN 'RAW_ROW_MISSING'
                    WHEN before_bar_status='TDX_TARGET_BAR_MISSING'
                      THEN 'TARGET_BAR_MISSING:' ||
                           COALESCE(before_raw_price_reason, before_window_reason, 'NO_REASON')
                    WHEN before_raw_price_quality IS DISTINCT FROM 'OBSERVED'
                      THEN 'RAW_PRICE_QUALITY_UNKNOWN:' ||
                           COALESCE(before_raw_price_reason, 'NO_REASON')
                    WHEN before_price_coordinate IS DISTINCT FROM 'RAW_UNADJUSTED'
                      THEN 'PRICE_COORDINATE_UNSUPPORTED:' ||
                           COALESCE(before_price_coordinate, 'UNKNOWN')
                    WHEN before_calculation_scope IS NULL
                      OR before_producer_contract IS NULL
                      OR before_producer_version IS NULL
                      OR before_semantics_sha256 IS NULL
                      THEN 'CORE_SEMANTICS_UNKNOWN'
                    WHEN before_ma20_quality IS DISTINCT FROM 'OBSERVED'
                      THEN 'MA20_QUALITY_UNKNOWN:' ||
                           COALESCE(before_ma20_reason, 'NO_REASON')
                    WHEN before_raw_close IS NULL OR before_ma20 IS NULL
                      THEN 'PRICE_VALUE_MISSING:' ||
                           COALESCE(before_ma20_reason, 'NO_REASON')
                    ELSE 'RELATION_UNKNOWN'
                END END AS before_source_pool_unknown_reason,
           CASE WHEN after_present
                     AND after_source_pool_close_ma20_relation IS NULL
                THEN CASE
                    WHEN NOT after_raw_record_present THEN 'RAW_ROW_MISSING'
                    WHEN after_bar_status='TDX_TARGET_BAR_MISSING'
                      THEN 'TARGET_BAR_MISSING:' ||
                           COALESCE(after_raw_price_reason, after_window_reason, 'NO_REASON')
                    WHEN after_raw_price_quality IS DISTINCT FROM 'OBSERVED'
                      THEN 'RAW_PRICE_QUALITY_UNKNOWN:' ||
                           COALESCE(after_raw_price_reason, 'NO_REASON')
                    WHEN after_price_coordinate IS DISTINCT FROM 'RAW_UNADJUSTED'
                      THEN 'PRICE_COORDINATE_UNSUPPORTED:' ||
                           COALESCE(after_price_coordinate, 'UNKNOWN')
                    WHEN after_calculation_scope IS NULL
                      OR after_producer_contract IS NULL
                      OR after_producer_version IS NULL
                      OR after_semantics_sha256 IS NULL
                      THEN 'CORE_SEMANTICS_UNKNOWN'
                    WHEN after_ma20_quality IS DISTINCT FROM 'OBSERVED'
                      THEN 'MA20_QUALITY_UNKNOWN:' ||
                           COALESCE(after_ma20_reason, 'NO_REASON')
                    WHEN after_raw_close IS NULL OR after_ma20 IS NULL
                      THEN 'PRICE_VALUE_MISSING:' ||
                           COALESCE(after_ma20_reason, 'NO_REASON')
                    ELSE 'RELATION_UNKNOWN'
                END END AS after_source_pool_unknown_reason,
           COALESCE((before_present AND after_present
            AND before_source_pool_close_ma20_relation IS NOT NULL
            AND after_source_pool_close_ma20_relation IS NOT NULL
            AND before_price_coordinate='RAW_UNADJUSTED'
            AND after_price_coordinate='RAW_UNADJUSTED'
            AND core_compatible
            AND before_market IS NOT DISTINCT FROM after_market
            AND before_board IS NOT DISTINCT FROM after_board), FALSE)
                AS source_pool_pair_compatible
    FROM source_pool_relations
), classified_base AS (
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
               CASE WHEN profile_compatible AND EXISTS (
                         SELECT 1
                         FROM unnest(before_profile_field_ids) AS field(field_id)
                         WHERE before_profile_statuses[
                                   array_position(before_profile_field_ids, field.field_id)]
                                   IS DISTINCT FROM after_profile_statuses[
                                   array_position(after_profile_field_ids, field.field_id)]
                            OR before_profile_qualities[
                                   array_position(before_profile_field_ids, field.field_id)]
                                   IS DISTINCT FROM after_profile_qualities[
                                   array_position(after_profile_field_ids, field.field_id)])
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
), bias20_atr_states AS (
    SELECT *,
           {_bias20_atr_state_expression("before")}
               AS before_bias20_atr_state,
           {_bias20_atr_state_expression("after")}
               AS after_bias20_atr_state,
           {_bias20_atr_state_reason_expression("before")}
               AS before_bias20_atr_state_reason,
           {_bias20_atr_state_reason_expression("after")}
               AS after_bias20_atr_state_reason
    FROM classified_base
), bias20_atr_pairs AS (
    SELECT *,
           {_BIAS20_ATR_PAIR_COMPATIBILITY_SQL}
               AS bias20_atr_pair_compatibility
    FROM bias20_atr_states
), classified AS (
    SELECT *,
           {_BIAS20_ATR_PAIR_REASON_SQL} AS bias20_atr_pair_reason,
           CASE
             WHEN before_bias20_atr_state='ABSENT'
               THEN 'BEFORE_ABSENT'::text
             WHEN after_bias20_atr_state='ABSENT'
               THEN 'AFTER_ABSENT'::text
             WHEN bias20_atr_pair_compatibility='PAIR_INCOMPARABLE'
               THEN 'PAIR_INCOMPARABLE'::text
             WHEN bias20_atr_pair_compatibility='PAIR_UNKNOWN'
               THEN 'PAIR_UNKNOWN'::text
             WHEN before_bias20_atr_state IS DISTINCT FROM after_bias20_atr_state
               THEN 'COMPARABLE_STATE_CHANGED'::text
             ELSE 'COMPARABLE_STATE_UNCHANGED'::text
           END AS bias20_atr_transition_status
    FROM bias20_atr_pairs
)
"""


def _profile_relation_endpoint_projection() -> str:
    expressions: list[str] = []
    for position in _PRICE_POSITION_QUERY_FIELDS:
        for side in ("before", "after"):
            fields = _profile_pair_price_position_fields(position, side=side)
            expressions.extend(
                (
                    f"{_price_position_status_expression(fields, coordinate_sql=f'{side}_price_coordinate')} "
                    f"AS {side}_{position}_relation",
                    f"CASE WHEN NOT {side}_present THEN 'NO_{side.upper()}_PROFILE' "
                    f"ELSE {_price_position_reason_expression(fields, coordinate_sql=f'{side}_price_coordinate')} END "
                    f"AS {side}_{position}_relation_reason",
                )
            )
    return ",\n           ".join(expressions)


def _profile_relation_transition_projection() -> str:
    expressions: list[str] = []
    for position in _PRICE_POSITION_QUERY_FIELDS:
        before = f"before_{position}_relation"
        after = f"after_{position}_relation"
        reason = f"""CASE
WHEN NOT before_present THEN 'NO_BEFORE_PROFILE'
WHEN NOT after_present THEN 'NO_AFTER_PROFILE'
WHEN before_price_coordinate IS DISTINCT FROM after_price_coordinate
  THEN 'PRICE_COORDINATE_MISMATCH'
WHEN before_price_coordinate IS DISTINCT FROM 'RAW_UNADJUSTED'
  OR after_price_coordinate IS DISTINCT FROM 'RAW_UNADJUSTED'
  THEN 'PRICE_COORDINATE_NOT_RAW_UNADJUSTED'
WHEN NOT core_compatible THEN 'CORE_SEMANTICS_INCOMPATIBLE'
WHEN {before}='UNKNOWN' OR {after}='UNKNOWN' THEN concat_ws('; ',
  CASE WHEN {before}='UNKNOWN' THEN 'BEFORE:' || COALESCE(before_{position}_relation_reason, 'UNKNOWN_REASON_NOT_PROVIDED') END,
  CASE WHEN {after}='UNKNOWN' THEN 'AFTER:' || COALESCE(after_{position}_relation_reason, 'UNKNOWN_REASON_NOT_PROVIDED') END)
ELSE NULL END"""
        transition = f"""CASE
WHEN NOT before_present AND after_present THEN 'ONLY_AFTER'
WHEN before_present AND NOT after_present THEN 'ONLY_BEFORE'
WHEN before_price_coordinate IS DISTINCT FROM after_price_coordinate
  OR before_price_coordinate IS DISTINCT FROM 'RAW_UNADJUSTED'
  OR after_price_coordinate IS DISTINCT FROM 'RAW_UNADJUSTED'
  OR NOT core_compatible
  OR {before}='UNKNOWN' OR {after}='UNKNOWN' THEN 'NOT_COMPARABLE'
WHEN {before}={after} THEN 'SAME_RELATION'
WHEN {before}='EQUAL' OR {after}='EQUAL' THEN 'EQUAL_BOUNDARY'
WHEN {before}='BELOW' AND {after}='ABOVE' THEN 'STRICT_CROSS_UP'
WHEN {before}='ABOVE' AND {after}='BELOW' THEN 'STRICT_CROSS_DOWN'
ELSE 'NOT_COMPARABLE' END"""
        expressions.extend(
            (
                f"{transition} AS {position}_transition_type",
                f"{reason} AS {position}_transition_reason",
            )
        )
    return ",\n           ".join(expressions)


_PROFILE_RELATION_TRANSITION_CTES = (
    _PROFILE_CHANGE_CTES
    + f""", relation_endpoints AS (
    SELECT *,
           {_profile_relation_endpoint_projection()}
    FROM classified
), relation_transitions AS (
    SELECT *,
           {_profile_relation_transition_projection()}
    FROM relation_endpoints
)
"""
)


_PROFILE_CHANGE_HIGHLIGHT_CTES = (
    _PROFILE_CHANGE_CTES
    + """
, highlight_classified AS (
    SELECT *,
           CASE
             WHEN before_close_ma20_relation='BELOW'
                  AND after_close_ma20_relation='ABOVE'
               THEN 'CLOSE_MA20_BELOW_TO_ABOVE'::text
             WHEN before_close_ma20_relation='ABOVE'
                  AND after_close_ma20_relation='BELOW'
               THEN 'CLOSE_MA20_ABOVE_TO_BELOW'::text
             WHEN before_close_ma20_relation IS NOT NULL
                  AND after_close_ma20_relation IS NOT NULL
                  AND before_close_ma20_relation IS DISTINCT FROM after_close_ma20_relation
                  AND (before_close_ma20_relation='EQUAL'
                       OR after_close_ma20_relation='EQUAL')
               THEN 'CLOSE_MA20_EQUAL_TRANSITION'::text
             WHEN 'TARGET_BAR_STATUS_CHANGED'=ANY(change_types)
               THEN 'TARGET_BAR_STATUS'::text
             WHEN 'PRICE_WINDOW_STATUS_CHANGED'=ANY(change_types)
               THEN 'PRICE_WINDOW_STATUS'::text
             WHEN 'PROFILE_STATE_CHANGED'=ANY(change_types)
               THEN 'PROFILE_STATE'::text
             WHEN 'CORE_QUALITY_CHANGED'=ANY(change_types)
               THEN 'CORE_QUALITY'::text
             WHEN 'BEFORE_ONLY'=ANY(change_types) OR 'AFTER_ONLY'=ANY(change_types)
               THEN 'SOURCE_SET_MEMBERSHIP'::text
             ELSE 'RECORDED_CHANGE'::text
           END AS highlight_category
    FROM classified
    WHERE cardinality(change_types)>0
), highlight_candidates AS (
    SELECT *,
           CASE highlight_category
             WHEN 'CLOSE_MA20_BELOW_TO_ABOVE' THEN 1
             WHEN 'CLOSE_MA20_ABOVE_TO_BELOW' THEN 1
             WHEN 'CLOSE_MA20_EQUAL_TRANSITION' THEN 1
             WHEN 'TARGET_BAR_STATUS' THEN 2
             WHEN 'PRICE_WINDOW_STATUS' THEN 3
             WHEN 'PROFILE_STATE' THEN 4
             WHEN 'CORE_QUALITY' THEN 5
             WHEN 'SOURCE_SET_MEMBERSHIP' THEN 6
             ELSE 7
           END::integer AS highlight_priority
    FROM highlight_classified
)
"""
)


MAX_PROFILE_CHANGE_HIGHLIGHTS = 30


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
    profile_field_id: str | None = None,
    profile_change_kind: str | None = None,
    before_bias20_atr_state: str = "ALL",
    after_bias20_atr_state: str = "ALL",
    before_source_set_id: str | None = None,
    before_package_sha256: str | None = None,
    before_profile_revision: str | None = None,
    after_source_set_id: str | None = None,
    after_package_sha256: str | None = None,
    after_profile_revision: str | None = None,
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
    if (
        not isinstance(before_bias20_atr_state, str)
        or before_bias20_atr_state not in _BIAS20_ATR_STATE_VALUES
        or not isinstance(after_bias20_atr_state, str)
        or after_bias20_atr_state not in _BIAS20_ATR_STATE_VALUES
    ):
        raise PriceProfilePgError(
            "PROFILE_CHANGE_BIAS20_ATR_STATE_INVALID",
            "双日MA20-ATR距离状态只接受ALL、五个精确数值状态、UNKNOWN或ABSENT",
        )
    profile_change_kinds = {
        "QUALITY_CHANGED",
        "STATUS_CHANGED",
        "ANY_STATE_CHANGED",
    }
    if (profile_field_id is None) != (profile_change_kind is None):
        raise PriceProfilePgError(
            "PROFILE_CHANGE_FIELD_FILTER_INVALID",
            "Profile11字段与质量/状态变化类别必须成对提供",
        )
    if (
        profile_field_id is not None
        and profile_field_id not in _PROFILE_QUERY_FIELD_IDS
    ):
        raise PriceProfilePgError(
            "PROFILE_CHANGE_FIELD_INVALID",
            "Profile11字段不在已采用的11项合同白名单中",
        )
    if (
        profile_change_kind is not None
        and profile_change_kind not in profile_change_kinds
    ):
        raise PriceProfilePgError(
            "PROFILE_CHANGE_FIELD_KIND_INVALID",
            "Profile11字段变化类别只接受QUALITY_CHANGED/STATUS_CHANGED/ANY_STATE_CHANGED",
        )
    if source_symbol is not None and (
        not isinstance(source_symbol, str)
        or source_symbol != source_symbol.strip()
        or len(source_symbol) > 9
        or re.fullmatch(r"[A-Za-z0-9.]+", source_symbol) is None
    ):
        raise PriceProfilePgError(
            "PROFILE_CHANGE_SYMBOL_INVALID", "来源代码搜索只接受字母、数字和点号"
        )
    before_identity = (
        before_source_set_id,
        before_package_sha256,
        before_profile_revision,
    )
    after_identity = (
        after_source_set_id,
        after_package_sha256,
        after_profile_revision,
    )
    if any(value is not None for value in before_identity) and not all(
        value is not None for value in before_identity
    ):
        raise PriceProfilePgError(
            "PROFILE_CHANGE_SNAPSHOT_IDENTITY_INVALID",
            "起始快照必须同时提供source_set_id、package_sha256和profile_revision",
        )
    if any(value is not None for value in after_identity) and not all(
        value is not None for value in after_identity
    ):
        raise PriceProfilePgError(
            "PROFILE_CHANGE_SNAPSHOT_IDENTITY_INVALID",
            "结束快照必须同时提供source_set_id、package_sha256和profile_revision",
        )
    symbol_search = source_symbol.upper() if source_symbol else None
    with _transaction(config, write=False) as (_connection, cursor, namespace_id):
        before = _snapshot_for_session(
            cursor,
            namespace_id=namespace_id,
            target_session=before_session,
            max_target_session=max_target_session,
            source_set_id=before_source_set_id,
            package_sha256=before_package_sha256,
            profile_revision=before_profile_revision,
        )
        after = _snapshot_for_session(
            cursor,
            namespace_id=namespace_id,
            target_session=after_session,
            max_target_session=max_target_session,
            source_set_id=after_source_set_id,
            package_sha256=after_package_sha256,
            profile_revision=after_profile_revision,
        )
        base_parameters = (
            *_profile_change_snapshot_parameters(namespace_id, before),
            *_profile_change_snapshot_parameters(namespace_id, after),
        )
        cursor.execute(
            _PROFILE_CHANGE_HIGHLIGHT_CTES
            + """SELECT count(*)::bigint AS source_symbol_union_count,
       count(*) FILTER (WHERE before_present AND after_present)::bigint AS intersection_count,
       count(*) FILTER (WHERE before_present AND NOT after_present)::bigint AS before_only_count,
       count(*) FILTER (WHERE NOT before_present AND after_present)::bigint AS after_only_count,
       count(*) FILTER (WHERE before_present AND after_present
           AND before_source_set_id IS DISTINCT FROM after_source_set_id)::bigint AS cross_source_set_pair_count,
       count(*) FILTER (WHERE before_present AND after_present
           AND before_price_coordinate IS DISTINCT FROM after_price_coordinate)::bigint AS cross_price_coordinate_pair_count,
       count(*) FILTER (WHERE before_present AND after_present AND NOT core_compatible)::bigint AS core_semantics_incomparable_count,
       count(*) FILTER (WHERE before_present AND after_present AND NOT profile_compatible)::bigint AS profile_semantics_incomparable_count,
       count(*) FILTER (WHERE before_present AND after_present
           AND (before_close_ma20_relation IS NULL OR after_close_ma20_relation IS NULL))::bigint AS close_ma20_unknown_count,
       count(*) FILTER (WHERE before_present AND after_present AND (
           array_position(before_profile_qualities, 'OBSERVED') IS NULL
           OR array_position(after_profile_qualities, 'OBSERVED') IS NULL))::bigint AS profile_unobserved_pair_count,
       count(*) FILTER (WHERE cardinality(change_types)>0)::bigint AS changed_source_symbol_count,
       count(*) FILTER (WHERE cardinality(change_types)=0)::bigint AS unchanged_source_symbol_count,
       (SELECT count(*)::bigint FROM highlight_candidates) AS highlight_eligible_count
FROM classified""",
            base_parameters,
        )
        summary_row = cursor.fetchone()
        summary_names = [item.name for item in cursor.description]
        summary = dict(zip(summary_names, summary_row, strict=True))
        cursor.execute(
            _PROFILE_CHANGE_HIGHLIGHT_CTES
            + """SELECT source_symbol, before_present, after_present,
       before_source_set_id, after_source_set_id,
       before_package_sha256, after_package_sha256,
       before_identity_status, after_identity_status,
       before_stable_security_id_status, after_stable_security_id_status,
       before_listing_status, after_listing_status,
       before_suspension_status, after_suspension_status,
       before_pit_status, after_pit_status,
       before_bar_status, after_bar_status,
       before_window_status, after_window_status, before_window_reason, after_window_reason,
       before_raw_close, after_raw_close, before_raw_price_quality, after_raw_price_quality,
       before_raw_price_reason, after_raw_price_reason,
       before_ma20, after_ma20, before_ma20_quality, after_ma20_quality,
       before_ma20_reason, after_ma20_reason,
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
       before_ma20_quality, after_ma20_quality, before_ma60_quality, after_ma60_quality,
       before_ma60_reason, after_ma60_reason,
       before_atr20_quality, after_atr20_quality, before_atr20_reason, after_atr20_reason,
       before_clv_quality, after_clv_quality, before_clv_reason, after_clv_reason,
       before_prior_high20_quality, after_prior_high20_quality,
       before_prior_high20_reason, after_prior_high20_reason,
       before_ret20_quality, after_ret20_quality, before_ret20_reason, after_ret20_reason,
       before_pos60_quality, after_pos60_quality, before_pos60_reason, after_pos60_reason,
       before_bias20_atr_quality, after_bias20_atr_quality,
       before_bias20_atr_reason, after_bias20_atr_reason,
       highlight_category, highlight_priority
FROM highlight_candidates
ORDER BY highlight_priority, source_symbol COLLATE "C"
LIMIT %s""",
            (*base_parameters, MAX_PROFILE_CHANGE_HIGHLIGHTS),
        )
        highlight_names = [item.name for item in cursor.description]
        highlight_rows = [
            dict(zip(highlight_names, row, strict=True)) for row in cursor.fetchall()
        ]
        has_bias20_atr_state_filter = (
            before_bias20_atr_state != "ALL" or after_bias20_atr_state != "ALL"
        )
        page_conditions = []
        page_filter_parameters: list[Any] = []
        if not has_bias20_atr_state_filter:
            page_conditions.append("cardinality(change_types)>0")
            page_conditions.append("(%s='ALL' OR %s=ANY(change_types))")
            page_filter_parameters.extend((change_type, change_type))
        elif change_type != "ALL":
            page_conditions.append("%s=ANY(change_types)")
            page_filter_parameters.append(change_type)
        page_conditions.append("(%s::text IS NULL OR strpos(source_symbol,%s::text)>0)")
        page_filter_parameters.extend((symbol_search, symbol_search))
        if before_bias20_atr_state != "ALL":
            page_conditions.append("before_bias20_atr_state=%s")
            page_filter_parameters.append(before_bias20_atr_state)
        if after_bias20_atr_state != "ALL":
            page_conditions.append("after_bias20_atr_state=%s")
            page_filter_parameters.append(after_bias20_atr_state)
        if profile_field_id is not None and profile_change_kind is not None:
            page_conditions.append(
                """profile_compatible
  AND array_position(before_profile_field_ids,%s::text) IS NOT NULL
  AND array_position(after_profile_field_ids,%s::text) IS NOT NULL
  AND CASE %s
        WHEN 'QUALITY_CHANGED' THEN
          before_profile_qualities[array_position(before_profile_field_ids,%s::text)]
            IS DISTINCT FROM
          after_profile_qualities[array_position(after_profile_field_ids,%s::text)]
        WHEN 'STATUS_CHANGED' THEN
          before_profile_statuses[array_position(before_profile_field_ids,%s::text)]
            IS DISTINCT FROM
          after_profile_statuses[array_position(after_profile_field_ids,%s::text)]
        WHEN 'ANY_STATE_CHANGED' THEN
          before_profile_qualities[array_position(before_profile_field_ids,%s::text)]
            IS DISTINCT FROM
          after_profile_qualities[array_position(after_profile_field_ids,%s::text)]
          OR before_profile_statuses[array_position(before_profile_field_ids,%s::text)]
            IS DISTINCT FROM
          after_profile_statuses[array_position(after_profile_field_ids,%s::text)]
        ELSE FALSE
      END"""
            )
            page_filter_parameters.extend(
                (
                    profile_field_id,
                    profile_field_id,
                    profile_change_kind,
                    profile_field_id,
                    profile_field_id,
                    profile_field_id,
                    profile_field_id,
                    profile_field_id,
                    profile_field_id,
                    profile_field_id,
                    profile_field_id,
                )
            )
        page_where = "WHERE " + "\n  AND ".join(page_conditions)
        page_parameters = (*base_parameters, *page_filter_parameters)
        cursor.execute(
            _PROFILE_CHANGE_CTES
            + """SELECT source_symbol, before_present, after_present,
       before_source_set_id, after_source_set_id,
       before_package_sha256, after_package_sha256,
       before_identity_status, after_identity_status,
       before_stable_security_id_status, after_stable_security_id_status,
       before_listing_status, after_listing_status,
       before_suspension_status, after_suspension_status,
       before_pit_status, after_pit_status,
       before_bar_status, after_bar_status,
       before_window_status, after_window_status, before_window_reason, after_window_reason,
       before_raw_close, after_raw_close, before_raw_price_quality, after_raw_price_quality,
       before_raw_price_reason, after_raw_price_reason,
       before_ma20, after_ma20, before_ma20_quality, after_ma20_quality,
       before_ma20_reason, after_ma20_reason,
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
       before_ma20_quality, after_ma20_quality,
       before_ma60_quality, after_ma60_quality, before_ma60_reason, after_ma60_reason,
       before_atr20_quality, after_atr20_quality, before_atr20_reason, after_atr20_reason,
       before_clv_quality, after_clv_quality, before_clv_reason, after_clv_reason,
       before_prior_high20_quality, after_prior_high20_quality, before_prior_high20_reason, after_prior_high20_reason,
       before_ret20_quality, after_ret20_quality, before_ret20_reason, after_ret20_reason,
       before_pos60_quality, after_pos60_quality, before_pos60_reason, after_pos60_reason,
       before_bias20_atr_quality, after_bias20_atr_quality,
       before_bias20_atr_reason, after_bias20_atr_reason,
       before_raw_record_present, after_raw_record_present,
       before_target_price_quality, after_target_price_quality,
       before_atr20, after_atr20,
       before_bias20_atr, after_bias20_atr,
       before_bias20_atr_exact_numerator, after_bias20_atr_exact_numerator,
       before_bias20_atr_exact_denominator, after_bias20_atr_exact_denominator,
       before_bias20_atr_state, after_bias20_atr_state,
       before_bias20_atr_state_reason, after_bias20_atr_state_reason,
       bias20_atr_pair_compatibility, bias20_atr_pair_reason,
       bias20_atr_transition_status,
       count(*) OVER ()::bigint AS filtered_total
FROM classified
"""
            + page_where
            + """
ORDER BY source_symbol COLLATE "C"
LIMIT %s OFFSET %s""",
            (*page_parameters, limit, offset),
        )
        names = [item.name for item in cursor.description]
        rows = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
        if rows:
            filtered_total = int(rows[0]["filtered_total"])
        else:
            cursor.execute(
                _PROFILE_CHANGE_CTES
                + f"SELECT count(*)::bigint AS filtered_total FROM classified\n{page_where}",
                page_parameters,
            )
            filtered_total = int(cursor.fetchone()[0])

    for row in (*rows, *highlight_rows):
        row["change_types"] = list(row["change_types"] or [])
        row["core_quality_changed_fields"] = list(
            row["core_quality_changed_fields"] or []
        )
        row["profile_field_changes"] = []
        if row["profile_compatible"]:
            before_ids = row["before_profile_field_ids"] or []
            after_ids = row["after_profile_field_ids"] or []
            before_statuses = row["before_profile_statuses"] or []
            after_statuses = row["after_profile_statuses"] or []
            before_qualities = row["before_profile_qualities"] or []
            after_qualities = row["after_profile_qualities"] or []
            before_reasons = row["before_profile_reasons"] or []
            after_reasons = row["after_profile_reasons"] or []
            if (
                len(before_ids)
                == len(before_statuses)
                == len(after_statuses)
                == len(before_qualities)
                == len(after_qualities)
                == len(before_reasons)
                == len(after_reasons)
                == len(after_ids)
                and len(set(before_ids)) == len(before_ids)
                and len(set(after_ids)) == len(after_ids)
                and set(before_ids) == set(after_ids)
            ):
                before_fields = {
                    field_id: (status, quality, reason)
                    for field_id, status, quality, reason in zip(
                        before_ids,
                        before_statuses,
                        before_qualities,
                        before_reasons,
                        strict=True,
                    )
                }
                after_fields = {
                    field_id: (status, quality, reason)
                    for field_id, status, quality, reason in zip(
                        after_ids,
                        after_statuses,
                        after_qualities,
                        after_reasons,
                        strict=True,
                    )
                }
                row["profile_field_changes"] = [
                    {
                        "field_id": field_id,
                        "before_status": before_value[0],
                        "after_status": after_value[0],
                        "before_quality": before_value[1],
                        "after_quality": after_value[1],
                        "before_reason": before_value[2] or None,
                        "after_reason": after_value[2] or None,
                    }
                    for field_id, before_value in before_fields.items()
                    if before_value[:2] != (after_value := after_fields[field_id])[:2]
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
    for row in rows:
        row.pop("filtered_total", None)
    return {
        "status": "AVAILABLE",
        "namespace_id": namespace_id,
        "before_snapshot": before,
        "after_snapshot": after,
        "summary": {key: int(value or 0) for key, value in summary.items()},
        "highlights": {
            "status": "AVAILABLE",
            "limit": MAX_PROFILE_CHANGE_HIGHLIGHTS,
            "eligible_count": int(summary["highlight_eligible_count"] or 0),
            "display_count": len(highlight_rows),
            "undisplayed_count": int(summary["highlight_eligible_count"] or 0)
            - len(highlight_rows),
            "ordering": "CATEGORY_PRIORITY_THEN_SOURCE_SYMBOL_ASC",
            "rows": highlight_rows,
        },
        "page": {
            "offset": offset,
            "limit": limit,
            "count": len(rows),
            "total": filtered_total,
            "next_offset": offset + len(rows)
            if offset + len(rows) < filtered_total
            else None,
        },
        "profile_field_filter": (
            {"field_id": profile_field_id, "kind": profile_change_kind}
            if profile_field_id is not None
            else None
        ),
        "bias20_atr_state_filter": {
            "before": before_bias20_atr_state,
            "after": after_bias20_atr_state,
            "uses_full_source_union": has_bias20_atr_state_filter,
        },
        "rows": rows,
        "database_write": False,
        "core_recalculated": False,
    }


def read_profile_relation_screen(
    config: Mapping[str, Any],
    *,
    before_session: date,
    after_session: date,
    max_target_session: date,
    offset: int,
    limit: int,
    relation_field: str,
    transition_type: str = "ALL",
    ma20_direction: str = "ALL",
    end_position_1_field: str | None = None,
    end_position_1_status: str | None = None,
    end_position_2_field: str | None = None,
    end_position_2_status: str | None = None,
    source_symbol: str | None = None,
    before_source_set_id: str,
    before_package_sha256: str,
    before_profile_revision: str,
    after_source_set_id: str,
    after_package_sha256: str,
    after_profile_revision: str,
) -> dict[str, Any]:
    """在完整两端来源并集上读取关系转移及结束日位置联合筛选。"""
    if (
        type(before_session) is not date
        or type(after_session) is not date
        or type(max_target_session) is not date
        or before_session >= after_session
        or after_session > max_target_session
    ):
        raise PriceProfilePgError(
            "PROFILE_RELATION_DATE_INVALID",
            "RAW关系变化日期必须递增且不晚于显式研究截止日",
        )
    if (
        type(offset) is not int
        or not 0 <= offset <= 2**31 - 1
        or type(limit) is not int
        or not 1 <= limit <= 100
    ):
        raise PriceProfilePgError(
            "PROFILE_RELATION_PAGE_INVALID", "关系筛选offset须非负且limit须在1到100"
        )
    if relation_field not in _PRICE_POSITION_QUERY_FIELDS:
        raise PriceProfilePgError(
            "PROFILE_RELATION_FIELD_INVALID", "RAW关系字段不在允许的三类价格关系内"
        )
    if transition_type not in _PRICE_RELATION_TRANSITION_TYPES:
        raise PriceProfilePgError(
            "PROFILE_RELATION_TYPE_INVALID", "RAW关系转移分类无效"
        )
    if ma20_direction not in _PROFILE_MA20_DIRECTIONS:
        raise PriceProfilePgError(
            "PROFILE_MA20_DIRECTION_INVALID",
            "MA20方向只接受ALL/INCREASED/UNCHANGED/DECREASED/UNKNOWN",
        )
    if source_symbol is not None and (
        not isinstance(source_symbol, str)
        or source_symbol != source_symbol.strip()
        or len(source_symbol) > 9
        or re.fullmatch(r"[A-Za-z0-9.]+", source_symbol) is None
    ):
        raise PriceProfilePgError(
            "PROFILE_RELATION_SYMBOL_INVALID", "来源代码筛选只接受字母、数字和点号"
        )
    end_filters: list[tuple[str, str]] = []
    for field, status in (
        (end_position_1_field, end_position_1_status),
        (end_position_2_field, end_position_2_status),
    ):
        if field is None and status is None:
            continue
        if (
            field not in _PRICE_POSITION_QUERY_FIELDS
            or status not in _PRICE_POSITION_STATUSES
        ):
            raise PriceProfilePgError(
                "PROFILE_RELATION_END_FILTER_INVALID",
                "结束日位置条件必须同时指定允许字段和ABOVE/EQUAL/BELOW/UNKNOWN状态",
            )
        end_filters.append((field, status))
    if len({field for field, _status in end_filters}) != len(end_filters):
        raise PriceProfilePgError(
            "PROFILE_RELATION_END_FILTER_INVALID", "两个结束日位置条件不能重复字段"
        )
    if any(
        not isinstance(value, str) or not value
        for value in (
            before_source_set_id,
            before_package_sha256,
            before_profile_revision,
            after_source_set_id,
            after_package_sha256,
            after_profile_revision,
        )
    ):
        raise PriceProfilePgError(
            "PROFILE_RELATION_SNAPSHOT_IDENTITY_INVALID",
            "前后快照必须完整提供source_set、package SHA和profile revision",
        )

    with _transaction(config, write=False) as (_connection, cursor, namespace_id):
        before = _snapshot_for_session(
            cursor,
            namespace_id=namespace_id,
            target_session=before_session,
            max_target_session=max_target_session,
            source_set_id=before_source_set_id,
            package_sha256=before_package_sha256,
            profile_revision=before_profile_revision,
        )
        after = _snapshot_for_session(
            cursor,
            namespace_id=namespace_id,
            target_session=after_session,
            max_target_session=max_target_session,
            source_set_id=after_source_set_id,
            package_sha256=after_package_sha256,
            profile_revision=after_profile_revision,
        )
        base_parameters = (
            *_profile_change_snapshot_parameters(namespace_id, before),
            *_profile_change_snapshot_parameters(namespace_id, after),
        )
        count_columns = [
            "count(*)::bigint AS source_symbol_union_count",
            "count(*) FILTER (WHERE before_present AND after_present)::bigint AS intersection_count",
            "count(*) FILTER (WHERE before_present AND NOT after_present)::bigint AS before_only_count",
            "count(*) FILTER (WHERE NOT before_present AND after_present)::bigint AS after_only_count",
        ]
        count_columns.extend(
            (
                f"count(*) FILTER (WHERE {relation_field}_transition_type='STRICT_CROSS_UP')::bigint AS strict_cross_up_count",
                f"count(*) FILTER (WHERE {relation_field}_transition_type='STRICT_CROSS_DOWN')::bigint AS strict_cross_down_count",
                f"count(*) FILTER (WHERE {relation_field}_transition_type='EQUAL_BOUNDARY')::bigint AS equal_boundary_count",
                f"count(*) FILTER (WHERE {relation_field}_transition_type='SAME_RELATION')::bigint AS same_relation_count",
                f"count(*) FILTER (WHERE {relation_field}_transition_type='NOT_COMPARABLE')::bigint AS not_comparable_count",
                f"count(*) FILTER (WHERE {relation_field}_transition_type='ONLY_BEFORE')::bigint AS only_before_count",
                f"count(*) FILTER (WHERE {relation_field}_transition_type='ONLY_AFTER')::bigint AS only_after_count",
                f"count(*) FILTER (WHERE {relation_field}_transition_type IN ('STRICT_CROSS_UP','STRICT_CROSS_DOWN','EQUAL_BOUNDARY','SAME_RELATION'))::bigint AS comparable_count",
                f"count(*) FILTER (WHERE before_present AND before_{relation_field}_relation='UNKNOWN')::bigint AS before_unknown_count",
                f"count(*) FILTER (WHERE after_present AND after_{relation_field}_relation='UNKNOWN')::bigint AS after_unknown_count",
            )
        )
        count_columns.extend(
            f"count(*) FILTER (WHERE ma20_direction='{direction}')::bigint AS ma20_{direction.lower()}_count"
            for direction in ("INCREASED", "UNCHANGED", "DECREASED", "UNKNOWN")
        )
        filters = []
        filter_parameters: list[Any] = []
        if transition_type != "ALL":
            filters.append(f"{relation_field}_transition_type=%s")
            filter_parameters.append(transition_type)
        if ma20_direction != "ALL":
            filters.append("ma20_direction=%s")
            filter_parameters.append(ma20_direction)
        for field, status in end_filters:
            filters.append(f"after_{field}_relation=%s")
            filter_parameters.append(status)
        symbol_search = source_symbol.upper() if source_symbol else None
        if symbol_search is not None:
            filters.append("source_symbol=%s")
            filter_parameters.append(symbol_search)
        filter_sql = " AND ".join(filters) if filters else "TRUE"
        unknown_reason_sql = f"""SELECT COALESCE(
    jsonb_agg(
        jsonb_build_object('side', side, 'reason', reason, 'count', row_count)
        ORDER BY side, reason NULLS FIRST
    ),
    '[]'::jsonb
) AS unknown_reason_counts
FROM (
    SELECT side, reason, count(*)::bigint AS row_count
    FROM (
    SELECT 'BEFORE'::text AS side,
           before_{relation_field}_relation_reason AS reason
    FROM relation_transitions
    WHERE before_present AND before_{relation_field}_relation='UNKNOWN'
    UNION ALL
    SELECT 'AFTER'::text AS side,
           after_{relation_field}_relation_reason AS reason
    FROM relation_transitions
    WHERE after_present AND after_{relation_field}_relation='UNKNOWN'
) AS unknowns
    GROUP BY side, reason
) AS grouped_unknowns"""
        ma20_unknown_reason_sql = """SELECT COALESCE(
    jsonb_agg(
        jsonb_build_object('reason', reason, 'count', row_count)
        ORDER BY row_count DESC, reason
    ),
    '[]'::jsonb
) AS ma20_direction_unknown_reason_counts
FROM (
    SELECT ma20_direction_reason AS reason, count(*)::bigint AS row_count
    FROM ma20_directional
    WHERE ma20_direction='UNKNOWN'
    GROUP BY ma20_direction_reason
) AS grouped_ma20_unknowns"""
        count_names = [
            "source_symbol_union_count",
            "intersection_count",
            "before_only_count",
            "after_only_count",
            "strict_cross_up_count",
            "strict_cross_down_count",
            "equal_boundary_count",
            "same_relation_count",
            "not_comparable_count",
            "only_before_count",
            "only_after_count",
            "comparable_count",
            "before_unknown_count",
            "after_unknown_count",
            "ma20_increased_count",
            "ma20_unchanged_count",
            "ma20_decreased_count",
            "ma20_unknown_count",
        ]
        cursor.execute(
            _PROFILE_RELATION_TRANSITION_CTES
            + f""", ma20_evidence AS (
    SELECT *, {_PROFILE_MA20_DIRECTION_REASON_SQL} AS ma20_direction_reason
    FROM relation_transitions
), ma20_directional AS (
    SELECT *, {_PROFILE_MA20_DIRECTION_SQL} AS ma20_direction
    FROM ma20_evidence
), summary AS (SELECT """
            + ",\n       ".join(count_columns)
            + f"""\nFROM ma20_directional
), unknown_reason_counts AS (
    {unknown_reason_sql}
), ma20_direction_unknown_reason_counts AS (
    {ma20_unknown_reason_sql}
), source_code_presence AS (
    SELECT count(*) FILTER (WHERE before_present)::bigint AS before_count,
           count(*) FILTER (WHERE after_present)::bigint AS after_count
    FROM relation_transitions
    WHERE %s::text IS NOT NULL AND source_symbol=%s
), filtered AS (
    SELECT * FROM ma20_directional WHERE {filter_sql}
), filtered_total AS (
    SELECT count(*)::bigint AS filtered_count FROM filtered
), page_rows AS (
    SELECT * FROM filtered
    ORDER BY source_symbol COLLATE "C"
    LIMIT %s OFFSET %s
)
SELECT summary.*,
       unknown_reason_counts.unknown_reason_counts,
       ma20_direction_unknown_reason_counts.ma20_direction_unknown_reason_counts,
       source_code_presence.before_count AS source_code_before_count,
       source_code_presence.after_count AS source_code_after_count,
       filtered_total.filtered_count,
       page_rows.*
FROM summary
CROSS JOIN unknown_reason_counts
CROSS JOIN ma20_direction_unknown_reason_counts
CROSS JOIN source_code_presence
CROSS JOIN filtered_total
LEFT JOIN page_rows ON TRUE
ORDER BY page_rows.source_symbol COLLATE "C" """,
            (
                *base_parameters,
                symbol_search,
                symbol_search,
                *filter_parameters,
                limit,
                offset,
            ),
        )
        names = [item.name for item in cursor.description]
        result_rows = cursor.fetchall()
        first = dict(zip(names, result_rows[0], strict=True))
        summary = {name: first[name] for name in count_names}
        unknown_reason_counts = first["unknown_reason_counts"]
        ma20_direction_unknown_reason_counts = first[
            "ma20_direction_unknown_reason_counts"
        ]
        filtered_total = int(first["filtered_count"])
        source_code_before_count = int(first["source_code_before_count"])
        source_code_after_count = int(first["source_code_after_count"])
        metadata_names = set(count_names) | {
            "unknown_reason_counts",
            "ma20_direction_unknown_reason_counts",
            "filtered_count",
            "source_code_before_count",
            "source_code_after_count",
        }
        rows = []
        for result_row in result_rows:
            row = dict(zip(names, result_row, strict=True))
            if row.get("source_symbol") is None:
                continue
            for name in metadata_names:
                row.pop(name, None)
            classified_direction, classified_reason = _profile_ma20_direction(row)
            if (
                row.get("ma20_direction") != classified_direction
                or row.get("ma20_direction_reason") != classified_reason
            ):
                raise PriceProfilePgError(
                    "PROFILE_MA20_DIRECTION_PROJECTION_MISMATCH",
                    "PG关系方向投影与Decimal复核不一致",
                )
            rows.append(row)

    field_definition = _PRICE_POSITION_QUERY_FIELDS
    for row in rows:
        transitions: dict[str, Any] = {}
        for field, definition in field_definition.items():
            transition = {
                "before_relation": row.pop(f"before_{field}_relation"),
                "before_reason": row.pop(f"before_{field}_relation_reason"),
                "after_relation": row.pop(f"after_{field}_relation"),
                "after_reason": row.pop(f"after_{field}_relation_reason"),
                "transition_type": row.pop(f"{field}_transition_type"),
                "transition_reason": row.pop(f"{field}_transition_reason"),
                "left_field": definition["left_field"],
                "right_field": definition["right_field"],
            }
            transitions[field] = transition
        row["price_relation_transitions"] = transitions
        row["change_types"] = list(row["change_types"] or [])
        for key, value in tuple(row.items()):
            if isinstance(value, (date, UUID)):
                row[key] = value.isoformat() if isinstance(value, date) else str(value)

    transition_counts = {
        key.removesuffix("_count"): int(summary[key] or 0)
        for key in (
            "strict_cross_up_count",
            "strict_cross_down_count",
            "equal_boundary_count",
            "same_relation_count",
            "not_comparable_count",
            "only_before_count",
            "only_after_count",
            "comparable_count",
        )
    }
    ma20_direction_counts = {
        direction: int(summary[f"ma20_{direction.lower()}_count"] or 0)
        for direction in ("INCREASED", "UNCHANGED", "DECREASED", "UNKNOWN")
    }
    if sum(ma20_direction_counts.values()) != int(summary["source_symbol_union_count"]):
        raise PriceProfilePgError(
            "PROFILE_MA20_DIRECTION_DENOMINATOR_MISMATCH",
            "双日MA20四态计数未覆盖完整来源并集",
        )
    if (
        sum(int(item["count"] or 0) for item in ma20_direction_unknown_reason_counts)
        != ma20_direction_counts["UNKNOWN"]
    ):
        raise PriceProfilePgError(
            "PROFILE_MA20_UNKNOWN_REASON_COVERAGE_MISMATCH",
            "UNKNOWN主原因计数未覆盖全部方向UNKNOWN来源",
        )
    source_code_presence = None
    if source_symbol is not None:
        if source_code_before_count and source_code_after_count:
            presence_status = "BOTH_PRESENT"
        elif source_code_before_count:
            presence_status = "BEFORE_ONLY"
        elif source_code_after_count:
            presence_status = "AFTER_ONLY"
        else:
            presence_status = "NO_PERSISTED_SOURCE_ROW"
        source_code_presence = {
            "source_symbol": source_symbol.upper(),
            "status": presence_status,
            "before_present": bool(source_code_before_count),
            "after_present": bool(source_code_after_count),
        }
    return {
        "status": "AVAILABLE" if filtered_total else "NO_MATCH",
        "namespace_id": namespace_id,
        "before_snapshot": before,
        "after_snapshot": after,
        "relation_field": relation_field,
        "transition_type": transition_type,
        "ma20_direction": ma20_direction,
        "source_code_presence": source_code_presence,
        "end_position_filters": [
            {"field": field, "status": status} for field, status in end_filters
        ],
        "summary": {
            "source_symbol_union_count": int(summary["source_symbol_union_count"]),
            "intersection_count": int(summary["intersection_count"]),
            "before_only_count": int(summary["before_only_count"]),
            "after_only_count": int(summary["after_only_count"]),
            "comparable_count": transition_counts["comparable"],
            "not_comparable_count": transition_counts["not_comparable"],
            "before_unknown_count": int(summary["before_unknown_count"]),
            "after_unknown_count": int(summary["after_unknown_count"]),
            "transition_counts": transition_counts,
            "unknown_reason_counts": unknown_reason_counts,
            "ma20_direction_counts": ma20_direction_counts,
            "ma20_direction_unknown_reason_counts": ma20_direction_unknown_reason_counts,
            "filtered_count": filtered_total,
        },
        "page": {
            "offset": offset,
            "limit": limit,
            "count": len(rows),
            "total": filtered_total,
            "next_offset": offset + len(rows)
            if offset + len(rows) < filtered_total
            else None,
            "last_offset": ((filtered_total - 1) // limit) * limit
            if filtered_total
            else None,
        },
        "rows": rows,
        "database_write": False,
        "core_recalculated": False,
    }


_PROFILE_BREADTH_SUMMARY_SQL = """
SELECT count(*)::bigint AS union_count,
       count(DISTINCT source_symbol)::bigint AS distinct_union_count,
       count(*) FILTER (WHERE before_present)::bigint AS before_count,
       count(*) FILTER (WHERE after_present)::bigint AS after_count,
       count(*) FILTER (WHERE before_present AND after_present)::bigint AS intersection_count,
       count(*) FILTER (WHERE before_present AND NOT after_present)::bigint AS before_only_count,
       count(*) FILTER (WHERE NOT before_present AND after_present)::bigint AS after_only_count,
       count(*) FILTER (WHERE before_present AND before_source_pool_close_ma20_relation='ABOVE')::bigint AS before_above,
       count(*) FILTER (WHERE before_present AND before_source_pool_close_ma20_relation='BELOW')::bigint AS before_below,
       count(*) FILTER (WHERE before_present AND before_source_pool_close_ma20_relation='EQUAL')::bigint AS before_equal,
       count(*) FILTER (WHERE before_present AND before_source_pool_close_ma20_relation IS NULL)::bigint AS before_unknown,
       count(*) FILTER (WHERE after_present AND after_source_pool_close_ma20_relation='ABOVE')::bigint AS after_above,
       count(*) FILTER (WHERE after_present AND after_source_pool_close_ma20_relation='BELOW')::bigint AS after_below,
       count(*) FILTER (WHERE after_present AND after_source_pool_close_ma20_relation='EQUAL')::bigint AS after_equal,
       count(*) FILTER (WHERE after_present AND after_source_pool_close_ma20_relation IS NULL)::bigint AS after_unknown,
       count(*) FILTER (WHERE source_pool_pair_compatible)::bigint AS compatible_count,
       count(*) FILTER (WHERE before_present AND after_present AND NOT source_pool_pair_compatible)::bigint AS incomparable_count,
       count(*) FILTER (WHERE source_pool_pair_compatible AND before_source_pool_close_ma20_relation='BELOW' AND after_source_pool_close_ma20_relation='BELOW')::bigint AS below_to_below,
       count(*) FILTER (WHERE source_pool_pair_compatible AND before_source_pool_close_ma20_relation='BELOW' AND after_source_pool_close_ma20_relation='EQUAL')::bigint AS below_to_equal,
       count(*) FILTER (WHERE source_pool_pair_compatible AND before_source_pool_close_ma20_relation='BELOW' AND after_source_pool_close_ma20_relation='ABOVE')::bigint AS below_to_above,
       count(*) FILTER (WHERE source_pool_pair_compatible AND before_source_pool_close_ma20_relation='EQUAL' AND after_source_pool_close_ma20_relation='BELOW')::bigint AS equal_to_below,
       count(*) FILTER (WHERE source_pool_pair_compatible AND before_source_pool_close_ma20_relation='EQUAL' AND after_source_pool_close_ma20_relation='EQUAL')::bigint AS equal_to_equal,
       count(*) FILTER (WHERE source_pool_pair_compatible AND before_source_pool_close_ma20_relation='EQUAL' AND after_source_pool_close_ma20_relation='ABOVE')::bigint AS equal_to_above,
       count(*) FILTER (WHERE source_pool_pair_compatible AND before_source_pool_close_ma20_relation='ABOVE' AND after_source_pool_close_ma20_relation='BELOW')::bigint AS above_to_below,
       count(*) FILTER (WHERE source_pool_pair_compatible AND before_source_pool_close_ma20_relation='ABOVE' AND after_source_pool_close_ma20_relation='EQUAL')::bigint AS above_to_equal,
       count(*) FILTER (WHERE source_pool_pair_compatible AND before_source_pool_close_ma20_relation='ABOVE' AND after_source_pool_close_ma20_relation='ABOVE')::bigint AS above_to_above,
       count(*) FILTER (WHERE source_pool_pair_compatible AND before_source_pool_close_ma20_relation='ABOVE')::bigint AS paired_above_before,
       count(*) FILTER (WHERE source_pool_pair_compatible AND after_source_pool_close_ma20_relation='ABOVE')::bigint AS paired_above_after
FROM classified
"""

_PROFILE_BREADTH_BOARD_SQL = """
SELECT endpoint.side, endpoint.market, endpoint.board, endpoint.relation,
       count(*)::bigint AS row_count
FROM classified
CROSS JOIN LATERAL (
    VALUES
      ('before'::text, before_present, before_market, before_board,
       COALESCE(before_source_pool_close_ma20_relation, 'UNKNOWN')::text),
      ('after'::text, after_present, after_market, after_board,
       COALESCE(after_source_pool_close_ma20_relation, 'UNKNOWN')::text)
) AS endpoint(side, present, market, board, relation)
WHERE endpoint.present
GROUP BY endpoint.side, endpoint.market, endpoint.board, endpoint.relation
ORDER BY endpoint.side, endpoint.market NULLS FIRST,
         endpoint.board NULLS FIRST, endpoint.relation
"""

_PROFILE_BREADTH_UNKNOWN_SQL = """
SELECT endpoint.side, endpoint.reason, count(*)::bigint AS row_count
FROM classified
CROSS JOIN LATERAL (
    VALUES
      ('before'::text, before_present, before_source_pool_unknown_reason),
      ('after'::text, after_present, after_source_pool_unknown_reason)
) AS endpoint(side, present, reason)
WHERE endpoint.present AND endpoint.reason IS NOT NULL
GROUP BY endpoint.side, endpoint.reason
ORDER BY endpoint.side, endpoint.reason
"""

_PROFILE_BREADTH_INCOMPARABLE_SQL = """
SELECT CASE
    WHEN NOT before_raw_record_present OR NOT after_raw_record_present
      THEN 'RAW_ROW_MISSING'
    WHEN before_bar_status='TDX_TARGET_BAR_MISSING'
      OR after_bar_status='TDX_TARGET_BAR_MISSING'
      THEN 'TARGET_BAR_MISSING'
    WHEN before_raw_price_quality IS DISTINCT FROM 'OBSERVED'
      OR after_raw_price_quality IS DISTINCT FROM 'OBSERVED'
      THEN 'RAW_CLOSE_QUALITY_UNKNOWN'
    WHEN before_price_coordinate IS DISTINCT FROM 'RAW_UNADJUSTED'
      OR after_price_coordinate IS DISTINCT FROM 'RAW_UNADJUSTED'
      OR before_price_coordinate IS DISTINCT FROM after_price_coordinate
      THEN 'PRICE_COORDINATE_INCOMPATIBLE'
    WHEN NOT core_compatible THEN 'CORE_SEMANTICS_INCOMPATIBLE'
    WHEN before_market IS DISTINCT FROM after_market
      OR before_board IS DISTINCT FROM after_board
      THEN 'SOURCE_LABEL_CHANGED'
    WHEN before_ma20_quality IS DISTINCT FROM 'OBSERVED'
      OR after_ma20_quality IS DISTINCT FROM 'OBSERVED'
      THEN 'MA20_QUALITY_UNKNOWN'
    ELSE 'RELATION_UNKNOWN'
END AS reason,
count(*)::bigint AS row_count
FROM classified
WHERE before_present AND after_present
  AND NOT source_pool_pair_compatible
GROUP BY reason
ORDER BY reason
"""


def read_profile_breadth(
    config: Mapping[str, Any],
    *,
    before_session: date,
    after_session: date,
    max_target_session: date,
    before_source_set_id: str,
    before_package_sha256: str,
    before_profile_revision: str,
    after_source_set_id: str,
    after_package_sha256: str,
    after_profile_revision: str,
) -> dict[str, Any]:
    """按两份精确封口画像只读聚合来源池MA20关系。"""
    if (
        type(before_session) is not date
        or type(after_session) is not date
        or type(max_target_session) is not date
        or before_session >= after_session
        or after_session > max_target_session
    ):
        raise PriceProfilePgError(
            "PROFILE_BREADTH_DATE_INVALID",
            "广度对照日期必须递增，且不晚于显式研究截止日",
        )
    identities = (
        before_source_set_id,
        before_package_sha256,
        before_profile_revision,
        after_source_set_id,
        after_package_sha256,
        after_profile_revision,
    )
    if any(not value for value in identities):
        raise PriceProfilePgError(
            "PROFILE_BREADTH_SNAPSHOT_IDENTITY_INVALID",
            "广度对照必须显式提供前后source_set、package SHA和profile revision",
        )

    with _transaction(config, write=False) as (_connection, cursor, namespace_id):
        before = _snapshot_for_session(
            cursor,
            namespace_id=namespace_id,
            target_session=before_session,
            max_target_session=max_target_session,
            source_set_id=before_source_set_id,
            package_sha256=before_package_sha256,
            profile_revision=before_profile_revision,
        )
        after = _snapshot_for_session(
            cursor,
            namespace_id=namespace_id,
            target_session=after_session,
            max_target_session=max_target_session,
            source_set_id=after_source_set_id,
            package_sha256=after_package_sha256,
            profile_revision=after_profile_revision,
        )
        parameters = (
            *_profile_change_snapshot_parameters(namespace_id, before),
            *_profile_change_snapshot_parameters(namespace_id, after),
        )
        cursor.execute(_PROFILE_CHANGE_CTES + _PROFILE_BREADTH_SUMMARY_SQL, parameters)
        names = [column.name for column in cursor.description]
        row = cursor.fetchone()
        if row is None:
            raise PriceProfilePgError(
                "PROFILE_BREADTH_EMPTY_RESULT", "广度查询没有返回汇总行"
            )
        summary = {name: int(value) for name, value in zip(names, row, strict=True)}
        cursor.execute(_PROFILE_CHANGE_CTES + _PROFILE_BREADTH_BOARD_SQL, parameters)
        board_rows = cursor.fetchall()
        cursor.execute(_PROFILE_CHANGE_CTES + _PROFILE_BREADTH_UNKNOWN_SQL, parameters)
        unknown_rows = cursor.fetchall()
        cursor.execute(
            _PROFILE_CHANGE_CTES + _PROFILE_BREADTH_INCOMPARABLE_SQL,
            parameters,
        )
        incomparable_rows = cursor.fetchall()

    before_count = summary["before_count"]
    after_count = summary["after_count"]
    intersection_count = summary["intersection_count"]
    union_count = summary["union_count"]
    if (
        summary["distinct_union_count"] != union_count
        or before_count != before["source_symbol_count"]
        or after_count != after["source_symbol_count"]
        or union_count
        != intersection_count
        + summary["before_only_count"]
        + summary["after_only_count"]
        or intersection_count
        != summary["compatible_count"] + summary["incomparable_count"]
    ):
        raise PriceProfilePgError(
            "PROFILE_BREADTH_DENOMINATOR_INVARIANT_FAILED",
            "广度并集、交集、快照分母或共同集合恒等式不成立",
        )

    relation_names = ("ABOVE", "BELOW", "EQUAL", "UNKNOWN")
    side_summaries: dict[str, dict[str, Any]] = {}
    for side in ("before", "after"):
        positions = {
            relation: summary[f"{side}_{relation.lower()}"]
            for relation in relation_names
        }
        source_count = summary[f"{side}_count"]
        eligible_count = sum(positions[relation] for relation in relation_names[:3])
        if sum(positions.values()) != source_count:
            raise PriceProfilePgError(
                "PROFILE_BREADTH_POSITION_INVARIANT_FAILED",
                f"{side}端MA20关系分类未覆盖完整来源分母",
            )
        unknown_reasons = {
            str(reason): int(count)
            for result_side, reason, count in unknown_rows
            if result_side == side
        }
        if sum(unknown_reasons.values()) != positions["UNKNOWN"]:
            raise PriceProfilePgError(
                "PROFILE_BREADTH_UNKNOWN_REASON_INVARIANT_FAILED",
                f"{side}端UNKNOWN主原因没有互斥覆盖全部未知项",
            )
        groups: dict[tuple[str, str], dict[str, int]] = {}
        for result_side, market, board, relation, count in board_rows:
            if result_side != side:
                continue
            key = (str(market or "UNKNOWN"), str(board or "UNKNOWN"))
            groups.setdefault(key, dict.fromkeys(relation_names, 0))[str(relation)] += (
                int(count)
            )
        board_distribution = [
            {
                "market": market,
                "board": board,
                "positions": groups[(market, board)],
                "source_count": sum(groups[(market, board)].values()),
                "research_board_label": market in {"SH", "SZ"}
                and board in {"MAIN_BOARD", "GEM"},
            }
            for market, board in sorted(groups)
        ]
        if sum(item["source_count"] for item in board_distribution) != source_count:
            raise PriceProfilePgError(
                "PROFILE_BREADTH_BOARD_INVARIANT_FAILED",
                f"{side}端来源market/board分组未覆盖完整分母",
            )
        side_summaries[side] = {
            "source_symbol_count": source_count,
            "positions": positions,
            "eligible_count": eligible_count,
            "eligible_coverage_fraction": {
                "numerator": eligible_count,
                "denominator": source_count,
            },
            "above_eligible_fraction": {
                "numerator": positions["ABOVE"],
                "denominator": eligible_count,
            },
            "unknown_reasons": [
                {"reason": reason, "count": count}
                for reason, count in sorted(unknown_reasons.items())
            ],
            "board_distribution": board_distribution,
        }

    transition_matrix = {
        "BELOW_TO_BELOW": summary["below_to_below"],
        "BELOW_TO_EQUAL": summary["below_to_equal"],
        "BELOW_TO_ABOVE": summary["below_to_above"],
        "EQUAL_TO_BELOW": summary["equal_to_below"],
        "EQUAL_TO_EQUAL": summary["equal_to_equal"],
        "EQUAL_TO_ABOVE": summary["equal_to_above"],
        "ABOVE_TO_BELOW": summary["above_to_below"],
        "ABOVE_TO_EQUAL": summary["above_to_equal"],
        "ABOVE_TO_ABOVE": summary["above_to_above"],
    }
    compatible_count = summary["compatible_count"]
    if sum(transition_matrix.values()) != compatible_count:
        raise PriceProfilePgError(
            "PROFILE_BREADTH_TRANSITION_INVARIANT_FAILED",
            "九格关系转移矩阵之和不等于共同可比集合",
        )
    incomparable_reasons = [
        {"reason": str(reason), "count": int(count)}
        for reason, count in incomparable_rows
    ]
    if (
        sum(item["count"] for item in incomparable_reasons)
        != summary["incomparable_count"]
    ):
        raise PriceProfilePgError(
            "PROFILE_BREADTH_INCOMPARABLE_REASON_INVARIANT_FAILED",
            "共同集合不可比主原因没有互斥覆盖全部不可比项",
        )
    paired_above_before = summary["paired_above_before"]
    paired_above_after = summary["paired_above_after"]
    return {
        "status": "AVAILABLE",
        "namespace_id": namespace_id,
        "before_snapshot": before,
        "after_snapshot": after,
        "membership": {
            "source_symbol_union_count": union_count,
            "intersection_count": intersection_count,
            "before_only_count": summary["before_only_count"],
            "after_only_count": summary["after_only_count"],
        },
        "before": side_summaries["before"],
        "after": side_summaries["after"],
        "paired": {
            "compatible_count": compatible_count,
            "incomparable_count": summary["incomparable_count"],
            "incomparable_reasons": incomparable_reasons,
            "transition_matrix": transition_matrix,
            "strict_cross_up_count": transition_matrix["BELOW_TO_ABOVE"],
            "strict_cross_down_count": transition_matrix["ABOVE_TO_BELOW"],
            "equal_transition_count": (
                transition_matrix["BELOW_TO_EQUAL"]
                + transition_matrix["EQUAL_TO_BELOW"]
                + transition_matrix["EQUAL_TO_EQUAL"]
                + transition_matrix["EQUAL_TO_ABOVE"]
                + transition_matrix["ABOVE_TO_EQUAL"]
            ),
            "paired_above_before_count": paired_above_before,
            "paired_above_after_count": paired_above_after,
            "delta_above_percentage_points": (
                {
                    "numerator": 100 * (paired_above_after - paired_above_before),
                    "denominator": compatible_count,
                }
                if compatible_count
                else None
            ),
        },
        "research_scope": "SOURCE_SYMBOL_ONLY",
        "price_coordinate": "RAW_UNADJUSTED",
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "formal_listing_universe": "UNKNOWN_NOT_CONNECTED",
        "database_write": False,
        "ddl_executed": False,
        "core_recalculated": False,
    }


def read_profile_breadth_members(
    config: Mapping[str, Any],
    *,
    before_session: date,
    after_session: date,
    max_target_session: date,
    before_source_set_id: str,
    before_package_sha256: str,
    before_profile_revision: str,
    after_source_set_id: str,
    after_package_sha256: str,
    after_profile_revision: str,
    mode: str,
    offset: int,
    limit: int,
    side: str | None = None,
    relation: str | None = None,
    from_relation: str | None = None,
    to_relation: str | None = None,
) -> dict[str, Any]:
    """按现役广度分类投影精确快照成员，并在单条只读查询中分页。"""
    if (
        type(before_session) is not date
        or type(after_session) is not date
        or type(max_target_session) is not date
        or before_session >= after_session
        or after_session > max_target_session
    ):
        raise PriceProfilePgError(
            "PROFILE_BREADTH_DATE_INVALID",
            "广度明细日期必须递增，且不晚于显式研究截止日",
        )
    if (
        type(offset) is not int
        or not 0 <= offset <= 2**31 - 1
        or type(limit) is not int
        or not 1 <= limit <= 100
    ):
        raise PriceProfilePgError(
            "PROFILE_BREADTH_MEMBER_PAGE_INVALID",
            "广度明细offset须非负且limit须在1到100",
        )

    relation_names = {"ABOVE", "BELOW", "EQUAL", "UNKNOWN"}
    predicates: str
    filter_parameters: tuple[Any, ...]
    if mode == "ENDPOINT":
        if (
            side not in {"BEFORE", "AFTER"}
            or relation not in relation_names
            or from_relation is not None
            or to_relation is not None
        ):
            raise PriceProfilePgError(
                "PROFILE_BREADTH_MEMBER_FILTER_INVALID",
                "单端分类必须指定BEFORE/AFTER及有效关系",
            )
        prefix = "before" if side == "BEFORE" else "after"
        if relation == "UNKNOWN":
            predicates = (
                f"{prefix}_present AND {prefix}_source_pool_close_ma20_relation IS NULL"
            )
            filter_parameters = ()
        else:
            predicates = (
                f"{prefix}_present AND {prefix}_source_pool_close_ma20_relation=%s"
            )
            filter_parameters = (relation,)
    elif mode == "TRANSITION":
        if (
            side is not None
            or relation is not None
            or from_relation not in {"BELOW", "EQUAL", "ABOVE"}
            or to_relation not in {"BELOW", "EQUAL", "ABOVE"}
        ):
            raise PriceProfilePgError(
                "PROFILE_BREADTH_MEMBER_FILTER_INVALID",
                "九格转移必须指定有效的起始和结束关系",
            )
        predicates = (
            "source_pool_pair_compatible "
            "AND before_source_pool_close_ma20_relation=%s "
            "AND after_source_pool_close_ma20_relation=%s"
        )
        filter_parameters = (from_relation, to_relation)
    elif mode == "INCOMPARABLE":
        if any(
            value is not None for value in (side, relation, from_relation, to_relation)
        ):
            raise PriceProfilePgError(
                "PROFILE_BREADTH_MEMBER_FILTER_INVALID",
                "不可比查询不接受关系筛选参数",
            )
        predicates = (
            "before_present AND after_present AND NOT source_pool_pair_compatible"
        )
        filter_parameters = ()
    else:
        raise PriceProfilePgError(
            "PROFILE_BREADTH_MEMBER_MODE_INVALID", "广度明细模式无效"
        )

    identities = (
        before_source_set_id,
        before_package_sha256,
        before_profile_revision,
        after_source_set_id,
        after_package_sha256,
        after_profile_revision,
    )
    if any(not isinstance(value, str) or not value for value in identities):
        raise PriceProfilePgError(
            "PROFILE_BREADTH_SNAPSHOT_IDENTITY_INVALID",
            "广度明细必须显式提供前后source_set、package SHA和profile revision",
        )

    with _transaction(config, write=False) as (_connection, cursor, namespace_id):
        before = _snapshot_for_session(
            cursor,
            namespace_id=namespace_id,
            target_session=before_session,
            max_target_session=max_target_session,
            source_set_id=before_source_set_id,
            package_sha256=before_package_sha256,
            profile_revision=before_profile_revision,
        )
        after = _snapshot_for_session(
            cursor,
            namespace_id=namespace_id,
            target_session=after_session,
            max_target_session=max_target_session,
            source_set_id=after_source_set_id,
            package_sha256=after_package_sha256,
            profile_revision=after_profile_revision,
        )
        snapshot_parameters = (
            *_profile_change_snapshot_parameters(namespace_id, before),
            *_profile_change_snapshot_parameters(namespace_id, after),
        )
        cursor.execute(
            _PROFILE_CHANGE_CTES
            + f"""
, member_rows AS (
    SELECT source_symbol, before_present, after_present,
           before_market, after_market, before_board, after_board,
           before_source_set_id, after_source_set_id,
           before_source_set_revision, after_source_set_revision,
           before_source_capture_id, after_source_capture_id,
           before_package_sha256, after_package_sha256,
           before_profile_revision, after_profile_revision,
           before_identity_status, after_identity_status,
           before_stable_security_id_status, after_stable_security_id_status,
           before_listing_status, after_listing_status,
           before_suspension_status, after_suspension_status,
           before_pit_status, after_pit_status,
           before_bar_status, after_bar_status,
           before_window_status, after_window_status,
           before_window_reason, after_window_reason,
           before_raw_open, after_raw_open,
           before_raw_high, after_raw_high,
           before_raw_low, after_raw_low,
           before_raw_close, after_raw_close,
           before_raw_price_quality, after_raw_price_quality,
           before_raw_price_reason, after_raw_price_reason,
           before_ma20, after_ma20,
           before_ma20_quality, after_ma20_quality,
           before_ma20_reason, after_ma20_reason,
           before_price_coordinate, after_price_coordinate,
           before_source_pool_close_ma20_relation,
           after_source_pool_close_ma20_relation,
           before_source_pool_unknown_reason,
           after_source_pool_unknown_reason,
           core_compatible, profile_compatible,
           source_pool_pair_compatible
    FROM classified
    WHERE {predicates}
)
SELECT totals.total,
       page.source_symbol, page.before_present, page.after_present,
       page.before_market, page.after_market, page.before_board, page.after_board,
       page.before_source_set_id, page.after_source_set_id,
       page.before_source_set_revision, page.after_source_set_revision,
       page.before_source_capture_id, page.after_source_capture_id,
       page.before_package_sha256, page.after_package_sha256,
       page.before_profile_revision, page.after_profile_revision,
       page.before_identity_status, page.after_identity_status,
       page.before_stable_security_id_status, page.after_stable_security_id_status,
       page.before_listing_status, page.after_listing_status,
       page.before_suspension_status, page.after_suspension_status,
       page.before_pit_status, page.after_pit_status,
       page.before_bar_status, page.after_bar_status,
       page.before_window_status, page.after_window_status,
       page.before_window_reason, page.after_window_reason,
       page.before_raw_open, page.after_raw_open,
       page.before_raw_high, page.after_raw_high,
       page.before_raw_low, page.after_raw_low,
       page.before_raw_close, page.after_raw_close,
       page.before_raw_price_quality, page.after_raw_price_quality,
       page.before_raw_price_reason, page.after_raw_price_reason,
       page.before_ma20, page.after_ma20,
       page.before_ma20_quality, page.after_ma20_quality,
       page.before_ma20_reason, page.after_ma20_reason,
       page.before_price_coordinate, page.after_price_coordinate,
       page.before_source_pool_close_ma20_relation,
       page.after_source_pool_close_ma20_relation,
       page.before_source_pool_unknown_reason,
       page.after_source_pool_unknown_reason,
       page.core_compatible, page.profile_compatible,
       page.source_pool_pair_compatible
FROM (SELECT count(*)::bigint AS total FROM member_rows) totals
LEFT JOIN LATERAL (
    SELECT * FROM member_rows
    ORDER BY source_symbol COLLATE "C"
    OFFSET %s LIMIT %s
) page ON TRUE
ORDER BY page.source_symbol COLLATE "C"
""",
            (*snapshot_parameters, *filter_parameters, offset, limit),
        )
        names = [column.name for column in cursor.description]
        fetched = cursor.fetchall()
        total = int(fetched[0][0]) if fetched else 0
        rows = [
            dict(zip(names[1:], row[1:], strict=True))
            for row in fetched
            if row[1] is not None
        ]

    next_offset = offset + len(rows) if offset + len(rows) < total else None
    return {
        "status": "AVAILABLE",
        "namespace_id": namespace_id,
        "before_snapshot": before,
        "after_snapshot": after,
        "query": {
            "mode": mode,
            "side": side,
            "relation": relation,
            "from_relation": from_relation,
            "to_relation": to_relation,
        },
        "page": {
            "offset": offset,
            "limit": limit,
            "count": len(rows),
            "total": total,
            "next_offset": next_offset,
            "last_offset": ((total - 1) // limit * limit) if total else None,
        },
        "rows": rows,
        "research_scope": "SOURCE_SYMBOL_ONLY",
        "price_coordinate": "RAW_UNADJUSTED",
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "formal_listing_universe": "UNKNOWN_NOT_CONNECTED",
        "database_write": False,
        "ddl_executed": False,
        "core_recalculated": False,
    }


def classify_source_profile_pair_status(
    *,
    before_present: bool,
    after_present: bool,
    core_compatible: bool,
    profile_compatible: bool,
    change_types: Sequence[str],
) -> dict[str, str]:
    """把成员存在状态与变化分类状态分开，避免把不可比当作无变化。"""
    if not before_present and not after_present:
        return {
            "pair_status": "NO_SAVED_SOURCE_CODE_MATCH",
            "state_change_status": "NOT_APPLICABLE",
        }
    if before_present and not after_present:
        return {"pair_status": "BEFORE_ONLY", "state_change_status": "NOT_APPLICABLE"}
    if after_present and not before_present:
        return {"pair_status": "AFTER_ONLY", "state_change_status": "NOT_APPLICABLE"}
    if not core_compatible or not profile_compatible:
        state_change_status = "NOT_COMPARABLE"
    elif change_types:
        state_change_status = "RECORDED_CHANGE"
    else:
        state_change_status = "NO_RECORDED_STATE_CHANGE"
    return {"pair_status": "BOTH_PRESENT", "state_change_status": state_change_status}


def classify_source_profile_highlight_status(
    *,
    source_present: bool,
    pair_status: str | None,
    state_change_status: str | None,
    qualities_complete: bool,
    position: int | None,
    highlight_category: str | None,
    highlight_priority: int | None,
    eligible_count: int,
    display_limit: int = MAX_PROFILE_CHANGE_HIGHLIGHTS,
) -> dict[str, Any]:
    """把同一重点候选生产者的精确查找结果转成可审计状态。"""
    if (
        type(eligible_count) is not int
        or eligible_count < 0
        or type(display_limit) is not int
        or display_limit < 1
    ):
        raise ValueError("重点变化计数或展示上限无效")

    display_count = min(eligible_count, display_limit)
    undisplayed_count = eligible_count - display_count
    candidate = position is not None
    if candidate and (
        type(position) is not int or position < 1 or position > eligible_count
    ):
        candidate = False
        position = None

    if candidate:
        status = "DISPLAYED" if position <= display_limit else "ELIGIBLE_NOT_DISPLAYED"
        reason = "SAME_PRODUCER_CANDIDATE"
    elif not source_present:
        status = "NO_SAVED_SOURCE_ROWS"
        reason = "NO_PERSISTED_PAIR_MEMBER"
    elif pair_status == "BOTH_PRESENT" and state_change_status == "RECORDED_CHANGE":
        status = "OTHER_CHANGE_NOT_ELIGIBLE"
        reason = "SAME_PRODUCER_DID_NOT_SELECT_RECORDED_CHANGE"
    elif (
        pair_status == "BOTH_PRESENT"
        and state_change_status == "NO_RECORDED_STATE_CHANGE"
        and qualities_complete
    ):
        status = "NOT_TRIGGERED"
        reason = "COMPARABLE_PAIR_WITHOUT_CLASSIFIED_CHANGE"
    else:
        status = "UNKNOWN_NOT_VERIFIED"
        reason = "PAIR_OR_REQUIRED_QUALITY_INCOMPLETE"

    return {
        "status": status,
        "reason": reason,
        "candidate": candidate,
        "position": position,
        "category": highlight_category if candidate else None,
        "priority": highlight_priority if candidate else None,
        "eligible_count": eligible_count,
        "display_count": display_count,
        "undisplayed_count": undisplayed_count,
        "display_limit": display_limit,
    }


def _field_pair_status(
    *,
    before_value: Any,
    after_value: Any,
    before_quality: str | None,
    after_quality: str | None,
    compatible: bool,
) -> str:
    if not compatible:
        return "NOT_COMPARABLE"
    if (
        before_quality != "OBSERVED"
        or after_quality != "OBSERVED"
        or before_value is None
        or after_value is None
    ):
        return "UNKNOWN"
    return (
        "SAME_VALUE"
        if Decimal(before_value) == Decimal(after_value)
        else "DIFFERENT_VALUES"
    )


def read_profile_pair(
    config: Mapping[str, Any],
    *,
    before_session: date,
    after_session: date,
    max_target_session: date,
    source_symbol: str,
    before_source_set_id: str,
    before_package_sha256: str,
    before_profile_revision: str,
    after_source_set_id: str,
    after_package_sha256: str,
    after_profile_revision: str,
) -> dict[str, Any]:
    """按两端精确封口快照，从现役配对SQL读取单个来源代码。"""
    if (
        type(before_session) is not date
        or type(after_session) is not date
        or type(max_target_session) is not date
        or before_session >= after_session
        or after_session > max_target_session
    ):
        raise PriceProfilePgError(
            "PROFILE_CHANGE_DATE_INVALID",
            "对照日期必须递增，且不晚于显式研究截止日",
        )
    if (
        not isinstance(source_symbol, str)
        or len(source_symbol) != 9
        or source_symbol[:2] not in {"SH", "SZ"}
        or source_symbol[2] != "."
        or not source_symbol[3:].isdecimal()
    ):
        raise PriceProfilePgError(
            "PROFILE_SYMBOL_INVALID", "来源代码必须符合SH./SZ.六位数字格式"
        )
    with _transaction(config, write=False) as (_connection, cursor, namespace_id):
        before = _snapshot_for_session(
            cursor,
            namespace_id=namespace_id,
            target_session=before_session,
            max_target_session=max_target_session,
            source_set_id=before_source_set_id,
            package_sha256=before_package_sha256,
            profile_revision=before_profile_revision,
        )
        after = _snapshot_for_session(
            cursor,
            namespace_id=namespace_id,
            target_session=after_session,
            max_target_session=max_target_session,
            source_set_id=after_source_set_id,
            package_sha256=after_package_sha256,
            profile_revision=after_profile_revision,
        )
        parameters = (
            *_profile_change_snapshot_parameters(namespace_id, before),
            *_profile_change_snapshot_parameters(namespace_id, after),
        )
        cursor.execute(
            _PROFILE_CHANGE_CTES + "SELECT * FROM classified WHERE source_symbol=%s",
            (*parameters, source_symbol),
        )
        names = [column.name for column in cursor.description]
        result = cursor.fetchone()
        row = dict(zip(names, result, strict=True)) if result else None
        cursor.execute(
            _PROFILE_CHANGE_HIGHLIGHT_CTES
            + """, ranked_candidates AS (
    SELECT source_symbol, highlight_category, highlight_priority,
           row_number() OVER (
               ORDER BY highlight_priority, source_symbol COLLATE "C"
           )::bigint AS highlight_position
    FROM highlight_candidates
)
SELECT (SELECT count(*)::bigint FROM highlight_candidates) AS eligible_count,
       candidate.source_symbol,
       candidate.highlight_category,
       candidate.highlight_priority,
       candidate.highlight_position
FROM (VALUES (TRUE)) AS requested(singleton)
LEFT JOIN ranked_candidates candidate ON candidate.source_symbol=%s""",
            (*parameters, source_symbol),
        )
        highlight_columns = [column.name for column in cursor.description]
        highlight_result = cursor.fetchone()
        highlight_record = dict(zip(highlight_columns, highlight_result, strict=True))

    if row is None:
        highlight_lookup = classify_source_profile_highlight_status(
            source_present=False,
            pair_status=None,
            state_change_status=None,
            qualities_complete=False,
            position=None,
            highlight_category=None,
            highlight_priority=None,
            eligible_count=int(highlight_record["eligible_count"]),
        )
        return {
            "status": "NO_SAVED_SOURCE_CODE_MATCH",
            "namespace_id": namespace_id,
            "source_symbol": source_symbol,
            "before_snapshot": before,
            "after_snapshot": after,
            "item": None,
            "highlight_lookup": highlight_lookup,
            "database_write": False,
            "core_recalculated": False,
        }

    row["change_types"] = list(row["change_types"] or [])
    row["core_quality_changed_fields"] = list(row["core_quality_changed_fields"] or [])
    row.update(
        classify_source_profile_pair_status(
            before_present=bool(row["before_present"]),
            after_present=bool(row["after_present"]),
            core_compatible=bool(row["core_compatible"]),
            profile_compatible=bool(row["profile_compatible"]),
            change_types=row["change_types"],
        )
    )
    both_present = row["pair_status"] == "BOTH_PRESENT"
    same_coordinate = row["before_price_coordinate"] == row["after_price_coordinate"]
    price_compatible = both_present and same_coordinate
    row["raw_ohlc_comparison_status"] = (
        "NOT_APPLICABLE"
        if not both_present
        else "NOT_COMPARABLE"
        if not same_coordinate
        else "COMPARABLE"
        if row["before_raw_price_quality"]
        == row["after_raw_price_quality"]
        == "OBSERVED"
        and row["before_raw_record_present"]
        and row["after_raw_record_present"]
        else "UNKNOWN"
    )
    row["raw_close_comparison_status"] = (
        "NOT_APPLICABLE"
        if not both_present
        else _field_pair_status(
            before_value=row["before_raw_close"],
            after_value=row["after_raw_close"],
            before_quality=row["before_raw_price_quality"],
            after_quality=row["after_raw_price_quality"],
            compatible=price_compatible,
        )
    )
    row["close_ma20_comparison_status"] = (
        "NOT_APPLICABLE"
        if not both_present
        else "NOT_COMPARABLE"
        if not row["core_compatible"]
        else "UNKNOWN"
        if row["before_raw_price_quality"] != "OBSERVED"
        or row["after_raw_price_quality"] != "OBSERVED"
        or row["before_ma20_quality"] != "OBSERVED"
        or row["after_ma20_quality"] != "OBSERVED"
        else "COMPARABLE"
    )
    core_statuses = [
        _field_pair_status(
            before_value=row.get(f"before_{field}"),
            after_value=row.get(f"after_{field}"),
            before_quality=row.get(f"before_{field}_quality"),
            after_quality=row.get(f"after_{field}_quality"),
            compatible=bool(row["core_compatible"]),
        )
        for field in _CORE_COLUMN_NAMES
    ]
    row["core_comparison_status"] = (
        "NOT_APPLICABLE"
        if not both_present
        else "NOT_COMPARABLE"
        if not row["core_compatible"]
        else "COMPARABLE"
        if all(status != "UNKNOWN" for status in core_statuses)
        else "PARTIALLY_COMPARABLE"
        if any(status != "UNKNOWN" for status in core_statuses)
        else "UNKNOWN"
    )
    before_profile_qualities = row["before_profile_qualities"] or []
    after_profile_qualities = row["after_profile_qualities"] or []
    profile_lists_compatible = len(before_profile_qualities) == len(
        after_profile_qualities
    )
    profile_observed = (
        sum(
            before == "OBSERVED" and after == "OBSERVED"
            for before, after in zip(
                before_profile_qualities, after_profile_qualities, strict=True
            )
        )
        if both_present and profile_lists_compatible
        else 0
    )
    row["profile_comparison_status"] = (
        "NOT_APPLICABLE"
        if not both_present
        else "NOT_COMPARABLE"
        if not row["profile_compatible"] or not profile_lists_compatible
        else "COMPARABLE"
        if profile_observed == len(before_profile_qualities)
        else "PARTIALLY_COMPARABLE"
        if profile_observed
        else "UNKNOWN"
    )
    highlight_lookup = classify_source_profile_highlight_status(
        source_present=True,
        pair_status=row["pair_status"],
        state_change_status=row["state_change_status"],
        qualities_complete=(
            row["raw_ohlc_comparison_status"] == "COMPARABLE"
            and row["close_ma20_comparison_status"] == "COMPARABLE"
            and row["core_comparison_status"] == "COMPARABLE"
            and row["profile_comparison_status"] == "COMPARABLE"
        ),
        position=highlight_record["highlight_position"],
        highlight_category=highlight_record["highlight_category"],
        highlight_priority=highlight_record["highlight_priority"],
        eligible_count=int(highlight_record["eligible_count"]),
    )
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
    return {
        "status": "AVAILABLE",
        "namespace_id": namespace_id,
        "source_symbol": source_symbol,
        "before_snapshot": before,
        "after_snapshot": after,
        "item": row,
        "highlight_lookup": highlight_lookup,
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
    profile_field_id: str | None = None,
    profile_quality: str | None = None,
    profile_status: str | None = None,
    position_1_field: str | None = None,
    position_1_status: str | None = None,
    position_2_field: str | None = None,
    position_2_status: str | None = None,
    bias20_atr_band: str = "ALL",
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
    if bias20_atr_band not in _BIAS20_ATR_BAND_DEFINITIONS:
        raise PriceProfilePgError(
            "PROFILE_FILTER_INVALID", "MA20-ATR距离档位筛选值无效"
        )
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
    if profile_field_id is None:
        if profile_quality is not None or profile_status is not None:
            raise PriceProfilePgError(
                "PROFILE_FILTER_INVALID", "Profile11质量或状态筛选必须指定字段"
            )
    elif profile_field_id not in _PROFILE_QUERY_FIELD_IDS:
        raise PriceProfilePgError(
            "PROFILE_FILTER_INVALID", "Profile11字段不在已采用的11项合同白名单中"
        )
    if profile_quality is not None and profile_status is not None:
        raise PriceProfilePgError(
            "PROFILE_FILTER_INVALID", "Profile11质量与状态筛选一次只能选择一项"
        )
    if profile_quality is not None and not isinstance(profile_quality, str):
        raise PriceProfilePgError(
            "PROFILE_FILTER_INVALID", "Profile11质量筛选值格式无效"
        )
    if profile_status is not None and not isinstance(profile_status, str):
        raise PriceProfilePgError(
            "PROFILE_FILTER_INVALID", "Profile11状态筛选值格式无效"
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
    position_filters: list[tuple[str, str]] = []
    for field, status in (
        (position_1_field, position_1_status),
        (position_2_field, position_2_status),
    ):
        if field is None and status is None:
            continue
        if (
            field not in _PRICE_POSITION_QUERY_FIELDS
            or status not in _PRICE_POSITION_STATUSES
        ):
            raise PriceProfilePgError(
                "PROFILE_FILTER_INVALID", "价格位置条件字段或关系状态无效"
            )
        position_filters.append((field, status))
    if len({field for field, _status in position_filters}) != len(position_filters):
        raise PriceProfilePgError(
            "PROFILE_FILTER_INVALID", "两个价格位置条件不能重复使用同一比较字段"
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
    if profile_quality is not None:
        conditions.append(
            "p.profile_qualities[array_position(p.profile_field_ids,%s)]=%s"
        )
        parameters.extend((profile_field_id, profile_quality))
    if profile_status is not None:
        conditions.append(
            "p.profile_statuses[array_position(p.profile_field_ids,%s)]=%s"
        )
        parameters.extend((profile_field_id, profile_status))
    for field, status in position_filters:
        conditions.append(f"({_price_position_status_sql(field)})=%s")
        parameters.append(status)
    if bias20_atr_band == "UNKNOWN":
        conditions.append(_BIAS20_ATR_UNKNOWN_SQL)
    elif bias20_atr_band != "ALL":
        conditions.append(f"NOT {_BIAS20_ATR_UNKNOWN_SQL}")
        conditions.append(
            str(_BIAS20_ATR_BAND_DEFINITIONS[bias20_atr_band]["predicate"])
        )
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
        profile_field_summary = None
        if profile_field_id is not None:
            profile_ids = sorted(_PROFILE_QUERY_FIELD_IDS)
            cursor.execute(
                """SELECT count(*)::bigint
FROM stock.source_symbol_price_profiles p
WHERE p.namespace_id=%s::uuid AND p.source_set_id=%s::uuid
  AND p.target_session=%s AND p.package_sha256=%s AND p.profile_revision=%s
  AND (
    COALESCE(array_ndims(p.profile_field_ids), 0) <> 1
    OR COALESCE(array_length(p.profile_field_ids, 1), 0) <> 11
    OR COALESCE(array_ndims(p.profile_statuses), 0) <> 1
    OR COALESCE(array_length(p.profile_statuses, 1), 0) <> 11
    OR COALESCE(array_ndims(p.profile_qualities), 0) <> 1
    OR COALESCE(array_length(p.profile_qualities, 1), 0) <> 11
    OR COALESCE(array_ndims(p.profile_reasons), 0) <> 1
    OR COALESCE(array_length(p.profile_reasons, 1), 0) <> 11
    OR array_lower(p.profile_field_ids, 1) IS DISTINCT FROM 1
    OR array_lower(p.profile_statuses, 1) IS DISTINCT FROM array_lower(p.profile_field_ids, 1)
    OR array_lower(p.profile_qualities, 1) IS DISTINCT FROM array_lower(p.profile_field_ids, 1)
    OR array_lower(p.profile_reasons, 1) IS DISTINCT FROM array_lower(p.profile_field_ids, 1)
    OR (SELECT count(DISTINCT field_id)::bigint
        FROM unnest(p.profile_field_ids) AS fields(field_id)) <> 11
    OR NOT (p.profile_field_ids @> %s::text[])
    OR NOT (p.profile_field_ids <@ %s::text[])
    OR array_position(p.profile_statuses, NULL) IS NOT NULL
    OR array_position(p.profile_qualities, NULL) IS NOT NULL
    OR array_position(p.profile_reasons, NULL) IS NOT NULL
  )""",
                (
                    namespace_id,
                    source_set_id,
                    target_session,
                    package_sha256,
                    profile_revision,
                    profile_ids,
                    profile_ids,
                ),
            )
            invalid_array_rows = int(cursor.fetchone()[0])
            if invalid_array_rows:
                raise PriceProfilePgError(
                    "PROFILE_COMPONENTS_INVALID",
                    "精确来源快照存在Profile11平行数组长度、字段集合或索引未对齐记录",
                )
            cursor.execute(
                """SELECT p.profile_statuses[field_index.subscript],
       p.profile_qualities[field_index.subscript],
       p.profile_reasons[field_index.subscript], count(*)::bigint
FROM stock.source_symbol_price_profiles p
CROSS JOIN LATERAL generate_subscripts(p.profile_field_ids, 1)
  AS field_index(subscript)
WHERE p.namespace_id=%s::uuid AND p.source_set_id=%s::uuid
  AND p.target_session=%s AND p.package_sha256=%s AND p.profile_revision=%s
  AND p.profile_field_ids[field_index.subscript]=%s
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3""",
                (
                    namespace_id,
                    source_set_id,
                    target_session,
                    package_sha256,
                    profile_revision,
                    profile_field_id,
                ),
            )
            profile_groups = cursor.fetchall()
            quality_groups: dict[str, dict[str, Any]] = {}
            status_groups: dict[str, dict[str, Any]] = {}
            reason_groups: list[dict[str, Any]] = []
            profile_denominator = 0
            for status, quality, reason, raw_count in profile_groups:
                count = int(raw_count)
                profile_denominator += count
                quality_item = quality_groups.setdefault(
                    quality, {"quality": quality, "count": 0, "reason_counts": []}
                )
                quality_item["count"] += count
                status_item = status_groups.setdefault(
                    status, {"status": status, "count": 0}
                )
                status_item["count"] += count
                reason_item = {
                    "status": status,
                    "quality": quality,
                    "reason": reason,
                    "count": count,
                }
                reason_groups.append(reason_item)
                quality_item["reason_counts"].append(
                    {"status": status, "reason": reason, "count": count}
                )
            if profile_denominator != expected_total:
                raise PriceProfilePgError(
                    "PROFILE_COMPONENTS_INCOMPLETE",
                    "所选Profile11字段没有覆盖精确来源快照的完整分母",
                )
            profile_field_summary = {
                "field_id": profile_field_id,
                "source_denominator": profile_denominator,
                "quality_counts": list(quality_groups.values()),
                "status_counts": list(status_groups.values()),
                "reason_counts": reason_groups,
            }
            if profile_quality is not None and profile_quality not in quality_groups:
                raise PriceProfilePgError(
                    "PROFILE_FILTER_INVALID",
                    "Profile11质量值不在该精确快照的已存状态中",
                )
            if profile_status is not None and profile_status not in status_groups:
                raise PriceProfilePgError(
                    "PROFILE_FILTER_INVALID",
                    "Profile11状态值不在该精确快照的已存状态中",
                )
        cursor.execute(
            f"""SELECT count(*)::bigint FROM stock.source_symbol_price_profiles p
LEFT JOIN stock.source_raw_daily r ON r.namespace_id=p.namespace_id
  AND r.source_symbol=p.source_symbol AND r.session_date=p.target_session
  AND r.package_sha256=p.package_sha256
WHERE {where_sql}""",
            parameters,
        )
        total = int(cursor.fetchone()[0])
        position_projection = ",\n       ".join(
            f"{_price_position_status_sql(field)} AS {field}_status, "
            f"{_price_position_reason_sql(field)} AS {field}_reason"
            for field in _PRICE_POSITION_QUERY_FIELDS
        )
        if profile_field_id is None:
            selected_profile_projection = """NULL::text AS selected_profile_field_id,
       NULL::text AS selected_profile_status,
       NULL::text AS selected_profile_quality,
       NULL::text AS selected_profile_reason"""
            selected_profile_parameters: list[Any] = []
        else:
            selected_profile_projection = """p.profile_field_ids[array_position(p.profile_field_ids,%s)] AS selected_profile_field_id,
       p.profile_statuses[array_position(p.profile_field_ids,%s)] AS selected_profile_status,
       p.profile_qualities[array_position(p.profile_field_ids,%s)] AS selected_profile_quality,
       p.profile_reasons[array_position(p.profile_field_ids,%s)] AS selected_profile_reason"""
            selected_profile_parameters = [profile_field_id] * 4
        cursor.execute(
            f"""SELECT p.*, r.raw_open AS target_raw_open,
       r.raw_high AS target_raw_high, r.raw_low AS target_raw_low,
       r.raw_close AS target_raw_close,
       COALESCE(r.price_quality, 'UNKNOWN') AS target_raw_price_quality,
       CASE WHEN r.source_symbol IS NULL THEN 'SOURCE_RAW_ROW_MISSING'
            WHEN r.price_quality IS DISTINCT FROM 'OBSERVED'
              THEN COALESCE(r.price_reason, 'RAW_PRICE_REASON_MISSING')
            ELSE r.price_reason END AS target_raw_price_reason,
       {_BIAS20_ATR_UNKNOWN_REASON_SQL} AS bias20_atr_unknown_reason,
       {position_projection},
       {selected_profile_projection}
FROM stock.source_symbol_price_profiles p
LEFT JOIN stock.source_raw_daily r ON r.namespace_id=p.namespace_id
  AND r.source_symbol=p.source_symbol AND r.session_date=p.target_session
  AND r.package_sha256=p.package_sha256
WHERE {where_sql}
ORDER BY p.source_symbol COLLATE "C" LIMIT %s OFFSET %s""",
            [*selected_profile_parameters, *parameters, limit, offset],
        )
        names = [item.name for item in cursor.description]
        rows = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
        if not rows and total > 0 and offset < total:
            raise PriceProfilePgError(
                "PROFILE_PAGE_COUNT_MISMATCH",
                "同一只读快照中的筛选总数与分页行数不一致",
            )
        for row in rows:
            for column in ("namespace_id", "source_set_id", "source_capture_id"):
                if isinstance(row.get(column), UUID):
                    row[column] = str(row[column])
            positions: dict[str, Any] = {}
            for field, definition in _PRICE_POSITION_QUERY_FIELDS.items():
                left_field = definition["left_field"]
                right_field = definition["right_field"]
                left_value_key = (
                    "target_raw_close" if left_field == "raw_close" else left_field
                )
                left_quality_key = (
                    "target_raw_price_quality"
                    if left_field == "raw_close"
                    else f"{left_field}_quality"
                )
                left_reason_key = (
                    "target_raw_price_reason"
                    if left_field == "raw_close"
                    else f"{left_field}_reason"
                )
                positions[field] = {
                    "status": row.pop(f"{field}_status"),
                    "reason": row.pop(f"{field}_reason"),
                    "price_coordinate": row["price_coordinate"],
                    "left": {
                        "field": left_field,
                        "value": row.get(left_value_key),
                        "quality": row.get(left_quality_key),
                        "reason": row.get(left_reason_key),
                    },
                    "right": {
                        "field": right_field,
                        "value": row.get(right_field),
                        "quality": row.get(f"{right_field}_quality"),
                        "reason": row.get(f"{right_field}_reason"),
                    },
                }
            row["price_positions"] = positions
            if profile_field_id is None:
                row["selected_profile_field"] = None
            else:
                row["selected_profile_field"] = {
                    "field_id": row.pop("selected_profile_field_id"),
                    "status": row.pop("selected_profile_status"),
                    "quality": row.pop("selected_profile_quality"),
                    "reason": row.pop("selected_profile_reason"),
                }
    return {
        "status": "AVAILABLE"
        if rows
        else "EMPTY_PAGE_OUT_OF_RANGE"
        if total > 0
        else "NO_MATCH",
        "namespace_id": namespace_id,
        "target_session": target_session.isoformat(),
        "source_set_id": source_set_id,
        "package_sha256": package_sha256,
        "profile_revision": profile_revision,
        "source_set_revision": int(source_set[1]),
        "source_pool_total": snapshot_total,
        "bias20_atr_band": bias20_atr_band,
        "bias20_atr_band_definition": _BIAS20_ATR_BAND_DEFINITIONS[bias20_atr_band],
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
        "price_position_filters": [
            {"field": field, "status": status} for field, status in position_filters
        ],
        "profile_field_options": sorted(_PROFILE_QUERY_FIELD_IDS),
        "profile_field_filter": {
            "field_id": profile_field_id,
            "quality": profile_quality,
            "status": profile_status,
        },
        "profile_field_summary": profile_field_summary,
        "rows": rows,
        "execution": {
            "sql_pagination": True,
            "price_position_filter": "POSTGRES_NUMERIC",
            "bias20_atr_filter": "POSTGRES_EXACT_NUMERIC_CROSS_MULTIPLICATION",
            "bias20_atr_filter_scope": "COMPLETE_SEALED_SOURCE_PROFILE_SET",
            "profile_field_filter": "POSTGRES_ARRAY_FIELD_ID_INDEX",
            "profile_field_summary": "POSTGRES_FULL_SOURCE_SET",
            "all_candidates_recomputed": False,
            "source_zip_opened": False,
            "database_write": False,
        },
    }
