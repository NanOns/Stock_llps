"""按来源日期、包版本与价格画像修订保存和读取价格画像。"""

from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json
import re
from fractions import Fraction
from typing import Any, Mapping, Sequence
from uuid import UUID

from .source_raw import RawSourcePgError, _transaction


MAX_PROFILE_ROWS_PER_BATCH = 5000
MAX_PROFILE_OBSERVATION_REVISIONS = 16
MAX_PROFILE_OBSERVATION_QUALITY_ROWS = 800
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
    "ma_slope20_atr",
    "amount_ratio5",
    "amount_ratio20",
    "volume_ratio5",
    "volume_ratio20",
)
_PROFILE_COLUMNS = (
    "namespace_id",
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
    "price_damage",
    "price_damage_quality",
    "price_damage_reason",
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
_CORE_FIELD_QUERY_COLUMNS["core.price_damage"] = "price_damage_quality"
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
    for name in ("package_sha256", "profile_revision"):
        value = normalized.get(name)
        if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
            raise PriceProfilePgError(
                "PROFILE_BUSINESS_KEY_INVALID",
                "画像包SHA和修订SHA必须是规范小写SHA-256",
            )
    if (
        not isinstance(normalized.get("source_symbol"), str)
        or re.fullmatch(r"(SH|SZ)\.[0-9]{6}", normalized["source_symbol"]) is None
    ):
        raise PriceProfilePgError(
            "PROFILE_BUSINESS_KEY_INVALID", "画像来源代码不符合直接业务键格式"
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
            if name in {
                "amount_ratio5",
                "amount_ratio20",
                "volume_ratio5",
                "volume_ratio20",
            } and (value < 0 or numerator < 0):
                raise PriceProfilePgError(
                    "PROFILE_CORE_VALUE_INVALID", "原生量额比值必须为非负精确数值"
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
        if (
            quality == "OBSERVED"
            and max(len(str(abs(numerator))), len(str(denominator))) > 100
        ):
            raise PriceProfilePgError(
                "PROFILE_CORE_EXACT_VALUE_TOO_LARGE",
                "Core精确分子或分母超过NUMERIC(100,0)",
            )
    damage_quality = normalized.get("price_damage_quality")
    damage_value = normalized.get("price_damage")
    damage_reason = normalized.get("price_damage_reason")
    if damage_quality == "OBSERVED":
        if type(damage_value) is not bool or damage_reason is not None:
            raise PriceProfilePgError(
                "PROFILE_PRICE_DAMAGE_INVALID",
                "已观察价格损伤必须保存原生布尔值且无未知原因",
            )
    elif damage_quality == "UNKNOWN":
        if (
            damage_value is not None
            or not isinstance(damage_reason, str)
            or not damage_reason.strip()
            or len(damage_reason.encode("utf-8")) > 128
        ):
            raise PriceProfilePgError(
                "PROFILE_PRICE_DAMAGE_INVALID", "未知价格损伤必须为空并保留有效原因"
            )
    else:
        raise PriceProfilePgError(
            "PROFILE_PRICE_DAMAGE_QUALITY_INVALID",
            "价格损伤质量必须为OBSERVED或UNKNOWN",
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
        first["target_session"],
        first["package_sha256"],
        first["profile_revision"],
    )
    for item in prepared:
        if (
            item["namespace_id"],
            item["target_session"],
            item["package_sha256"],
            item["profile_revision"],
        ) != batch_key:
            raise PriceProfilePgError(
                "PROFILE_BATCH_KEY_MIXED", "单次画像写入混入不同来源版本"
            )
    capture_ids = {item["source_capture_id"] for item in prepared}
    if len(capture_ids) != 1:
        raise PriceProfilePgError(
            "PROFILE_BATCH_CAPTURE_MIXED", "单次画像写入必须绑定唯一Raw捕获"
        )
    source_capture_id = next(iter(capture_ids))
    requested = {item["source_symbol"]: item for item in prepared}
    with _transaction(config, write=True) as (_connection, cursor, namespace_id):
        if namespace_id != first["namespace_id"]:
            raise PriceProfilePgError(
                "PROFILE_NAMESPACE_MISMATCH", "画像命名空间与PG连接不一致"
            )
        cursor.execute(
            """SELECT source_symbol, market, board, source_capture_id::text
FROM stock.source_raw_daily
WHERE namespace_id=%s::uuid AND session_date=%s AND package_sha256=%s
  AND source_capture_id=%s::uuid
ORDER BY source_symbol""",
            (
                namespace_id,
                first["target_session"],
                first["package_sha256"],
                source_capture_id,
            ),
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
WHERE namespace_id=%s::uuid AND target_session=%s
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
WHERE namespace_id=%s::uuid AND target_session=%s
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
            """SELECT p.target_session, p.package_sha256,
       p.profile_revision, p.history_window_start, p.history_window_end,
       p.history_requested_session_count, count(*)::bigint,
        min(r.raw_row_count),
        count(*)=min(r.raw_row_count)
         AND min(r.raw_symbol_count)=min(r.raw_row_count)
         AND min(r.capture_count)=1
         AND count(DISTINCT p.source_capture_id)=1
         AND min(p.source_capture_id::text)=min(r.raw_capture_id),
       p.history_calendar_id, p.history_calendar_revision, p.price_coordinate
FROM stock.source_symbol_price_profiles p
LEFT JOIN (
    SELECT namespace_id, session_date, package_sha256,
           count(*)::bigint AS raw_row_count,
           count(DISTINCT source_symbol)::bigint AS raw_symbol_count,
           count(DISTINCT source_capture_id)::bigint AS capture_count,
           min(source_capture_id::text) AS raw_capture_id
    FROM stock.source_raw_daily
    GROUP BY namespace_id, session_date, package_sha256
) r ON r.namespace_id=p.namespace_id AND r.session_date=p.target_session
  AND r.package_sha256=p.package_sha256
WHERE p.namespace_id=%s::uuid AND p.target_session<=%s
GROUP BY p.target_session, p.package_sha256, p.profile_revision,
         p.history_window_start, p.history_window_end, p.history_requested_session_count,
         p.history_calendar_id, p.history_calendar_revision, p.price_coordinate
ORDER BY p.target_session DESC, p.package_sha256, p.profile_revision""",
            (namespace_id, max_target_session),
        )
        snapshots = [
            {
                "target_session": row[0].isoformat(),
                "package_sha256": str(row[1]).strip(),
                "profile_revision": str(row[2]).strip(),
                "window_start": row[3].isoformat(),
                "window_end": row[4].isoformat(),
                "history_session_count": int(row[5]),
                "source_symbol_count": int(row[6]),
                "expected_source_symbol_count": int(row[7]),
                "complete": bool(row[8]),
                "history_calendar_id": row[9],
                "history_calendar_revision": row[10],
                "price_coordinate": row[11],
            }
            for row in cursor.fetchall()
        ]
    return {
        "status": "AVAILABLE" if snapshots else "EMPTY",
        "namespace_id": namespace_id,
        "snapshots": snapshots,
    }


def read_source_profile_observability(
    config: Mapping[str, Any],
    *,
    target_session: date,
    package_sha256: str,
    source_capture_id: str,
    current_semantics_sha256: str,
) -> dict[str, Any]:
    """按画像行键与Raw捕获读取画像版本、符号集合和字段质量摘要。"""
    if (
        type(target_session) is not date
        or not isinstance(package_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", package_sha256) is None
        or not isinstance(current_semantics_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", current_semantics_sha256) is None
    ):
        raise PriceProfilePgError(
            "PROFILE_OBSERVATION_KEY_INVALID", "画像观察必须绑定精确日期、SHA与修订"
        )
    try:
        canonical_capture_id = str(UUID(source_capture_id))
    except (ValueError, TypeError, AttributeError) as error:
        raise PriceProfilePgError(
            "PROFILE_OBSERVATION_KEY_INVALID", "画像观察行键或捕获UUID无效"
        ) from error
    if canonical_capture_id != source_capture_id:
        raise PriceProfilePgError(
            "PROFILE_OBSERVATION_KEY_INVALID", "画像行键与捕获UUID必须使用规范格式"
        )

    core_values = ", ".join(
        f"('core.{name}',p.{name}_quality,p.{name}_reason)"
        for name in _CORE_COLUMN_NAMES
    )
    core_values += (
        ", ('core.price_damage',p.price_damage_quality,p.price_damage_reason)"
    )
    with _transaction(config, write=False) as (_connection, cursor, namespace_id):
        cursor.execute(
            """SELECT count(*)::bigint AS raw_rows,
       count(DISTINCT r.source_symbol)::bigint AS raw_symbols,
       count(DISTINCT r.source_capture_id)::bigint AS capture_count,
       min(c.normalized_count) AS expected_count,
       max(c.normalized_count) AS expected_count_max
FROM stock.source_raw_daily r
LEFT JOIN stock.source_captures c ON c.namespace_id=r.namespace_id
  AND c.capture_id=r.source_capture_id
WHERE r.namespace_id=%s::uuid AND r.source_capture_id=%s::uuid
  AND r.session_date=%s::date AND r.package_sha256=%s""",
            (namespace_id, canonical_capture_id, target_session, package_sha256),
        )
        raw_row, raw_symbol_row, capture_count, expected_count, expected_count_max = (
            cursor.fetchone()
        )
        raw_rows = int(raw_row)
        raw_symbols = int(raw_symbol_row)
        if (
            not 1 <= raw_rows <= MAX_PROFILE_ROWS_PER_BATCH
            or raw_rows > MAX_PROFILE_ROWS_PER_BATCH
            or raw_symbols > MAX_PROFILE_ROWS_PER_BATCH
            or raw_symbols != raw_rows
            or capture_count != 1
            or expected_count != raw_rows
            or expected_count != expected_count_max
        ):
            raise PriceProfilePgError(
                "PROFILE_OBSERVATION_ROW_BUDGET_EXCEEDED",
                "画像观察的Raw代码集合与唯一捕获分母不一致",
            )

        cursor.execute(
            """SELECT profile_revision::text AS profile_revision,
       producer_contract,producer_version,producer_contract_sha256,
       count(*)::bigint AS stored_rows,
       count(DISTINCT source_symbol)::bigint AS distinct_source_symbols,
       count(*) FILTER (WHERE history_records_complete)::bigint
           AS history_records_complete_rows,
       count(*) FILTER (WHERE history_stored_session_count>=24
                         AND history_requested_session_count>=24)::bigint
           AS history_24_session_eligible_rows,
       count(*) FILTER (WHERE history_requested_session_count>=24)::bigint
           AS history_requested_24_plus_rows,
       min(history_requested_session_count) AS min_history_requested_sessions,
       max(history_requested_session_count) AS max_history_requested_sessions,
       min(history_stored_session_count) AS min_history_stored_sessions,
       max(history_stored_session_count) AS max_history_stored_sessions,
       count(*) FILTER (WHERE price_window_usable)::bigint
           AS price_window_usable_rows,
       array_agg(DISTINCT target_bar_status ORDER BY target_bar_status)
           AS target_bar_statuses,
       array_agg(DISTINCT target_price_quality ORDER BY target_price_quality)
           AS target_price_qualities,
       array_agg(DISTINCT price_window_status ORDER BY price_window_status)
           AS price_window_statuses
FROM stock.source_symbol_price_profiles
WHERE namespace_id=%s::uuid AND target_session=%s::date AND package_sha256=%s
  AND source_capture_id=%s::uuid
GROUP BY profile_revision,producer_contract,producer_version,producer_contract_sha256
ORDER BY profile_revision
LIMIT %s""",
            (
                namespace_id,
                target_session,
                package_sha256,
                canonical_capture_id,
                MAX_PROFILE_OBSERVATION_REVISIONS + 1,
            ),
        )
        revision_columns = [column.name for column in cursor.description]
        revisions = [
            dict(zip(revision_columns, row, strict=True)) for row in cursor.fetchall()
        ]
        if len(revisions) > MAX_PROFILE_OBSERVATION_REVISIONS:
            raise PriceProfilePgError(
                "PROFILE_OBSERVATION_REVISION_BUDGET_EXCEEDED",
                "精确来源集合画像修订数超过固定上限",
            )
        for revision in revisions:
            revision["profile_revision"] = str(revision["profile_revision"]).strip()
            revision["producer_contract_sha256"] = str(
                revision["producer_contract_sha256"]
            ).strip()
            for name in (
                "history_records_complete_rows",
                "history_24_session_eligible_rows",
                "history_requested_24_plus_rows",
                "price_window_usable_rows",
            ):
                revision[name] = int(revision[name])
            revision["stored_rows"] = int(revision["stored_rows"])
            revision["distinct_source_symbols"] = int(
                revision["distinct_source_symbols"]
            )
            for name in (
                "target_bar_statuses",
                "target_price_qualities",
                "price_window_statuses",
            ):
                revision[name] = revision[name] or []

        matching_current = [
            item
            for item in revisions
            if item["producer_contract_sha256"] == current_semantics_sha256
        ]
        selected = None
        if len(matching_current) == 1:
            selected = matching_current[0]
            selection_rule = "UNIQUE_CURRENT_SEMANTICS_SHA256"
        elif len(revisions) == 1:
            selected = revisions[0]
            selection_rule = "UNIQUE_EXACT_CAPTURE_REVISION"
        elif revisions:
            selection_rule = "AMBIGUOUS_NO_UNIQUE_SEMANTICS_MATCH"
        else:
            selection_rule = "NO_STORED_PROFILE_REVISION"

        set_comparison = {
            "raw_row_count": raw_rows,
            "raw_symbol_count": raw_symbols,
            "profile_row_count": 0,
            "profile_symbol_count": 0,
            "matched_symbol_count": 0,
            "raw_only_symbol_count": raw_symbols,
            "profile_only_symbol_count": 0,
            "symbol_sets_match": False,
        }
        core_quality_rows: list[dict[str, Any]] = []
        profile_quality_rows: list[dict[str, Any]] = []
        if selected is not None:
            profile_revision = selected["profile_revision"]
            cursor.execute(
                """WITH raw_symbols AS (
    SELECT DISTINCT source_symbol
    FROM stock.source_raw_daily
    WHERE namespace_id=%s::uuid AND source_capture_id=%s::uuid
      AND session_date=%s::date AND package_sha256=%s
), profile_symbols AS (
    SELECT DISTINCT source_symbol
    FROM stock.source_symbol_price_profiles
    WHERE namespace_id=%s::uuid AND target_session=%s::date
      AND package_sha256=%s AND source_capture_id=%s::uuid
      AND profile_revision=%s
)
SELECT count(r.source_symbol)::bigint AS raw_symbol_count,
       count(p.source_symbol)::bigint AS profile_symbol_count,
       count(*) FILTER (WHERE r.source_symbol IS NOT NULL
                         AND p.source_symbol IS NOT NULL)::bigint
           AS matched_symbol_count,
       count(*) FILTER (WHERE r.source_symbol IS NOT NULL
                         AND p.source_symbol IS NULL)::bigint
           AS raw_only_symbol_count,
       count(*) FILTER (WHERE r.source_symbol IS NULL
                         AND p.source_symbol IS NOT NULL)::bigint
           AS profile_only_symbol_count,
       array_agg(r.source_symbol ORDER BY r.source_symbol)
           FILTER (WHERE r.source_symbol IS NOT NULL) AS raw_symbol_set,
       array_agg(p.source_symbol ORDER BY p.source_symbol)
           FILTER (WHERE p.source_symbol IS NOT NULL) AS profile_symbol_set
FROM raw_symbols r FULL OUTER JOIN profile_symbols p USING (source_symbol)""",
                (
                    namespace_id,
                    canonical_capture_id,
                    target_session,
                    package_sha256,
                    namespace_id,
                    target_session,
                    package_sha256,
                    canonical_capture_id,
                    profile_revision,
                ),
            )
            comparison = cursor.fetchone()
            raw_symbol_set = comparison[5] or []
            profile_symbol_set = comparison[6] or []
            raw_symbol_set_sha256 = sha256(
                "\n".join(raw_symbol_set).encode("utf-8")
            ).hexdigest()
            profile_symbol_set_sha256 = sha256(
                "\n".join(profile_symbol_set).encode("utf-8")
            ).hexdigest()
            set_comparison.update(
                {
                    "raw_symbol_count": int(comparison[0]),
                    "profile_row_count": int(selected["stored_rows"]),
                    "profile_symbol_count": int(comparison[1]),
                    "matched_symbol_count": int(comparison[2]),
                    "raw_only_symbol_count": int(comparison[3]),
                    "profile_only_symbol_count": int(comparison[4]),
                    "raw_symbol_set_sha256": raw_symbol_set_sha256,
                    "profile_symbol_set_sha256": profile_symbol_set_sha256,
                    "symbol_sets_sha256_match": (
                        raw_symbol_set_sha256 == profile_symbol_set_sha256
                    ),
                    "symbol_sets_match": (
                        comparison[3] == 0
                        and comparison[4] == 0
                        and int(selected["stored_rows"]) == int(comparison[0])
                        and raw_symbol_set_sha256 == profile_symbol_set_sha256
                    ),
                }
            )
            cursor.execute(
                f"""SELECT f.field_id,f.quality,f.reason_code,count(*)::bigint AS row_count
FROM stock.source_symbol_price_profiles p
CROSS JOIN LATERAL (VALUES {core_values})
     AS f(field_id,quality,reason_code)
WHERE p.namespace_id=%s::uuid AND p.target_session=%s::date
  AND p.package_sha256=%s AND p.source_capture_id=%s::uuid
  AND p.profile_revision=%s
GROUP BY f.field_id,f.quality,f.reason_code
ORDER BY f.field_id,f.quality,f.reason_code
LIMIT %s""",
                (
                    namespace_id,
                    target_session,
                    package_sha256,
                    canonical_capture_id,
                    profile_revision,
                    MAX_PROFILE_OBSERVATION_QUALITY_ROWS + 1,
                ),
            )
            quality_columns = [column.name for column in cursor.description]
            core_quality_rows = [
                dict(zip(quality_columns, row, strict=True))
                for row in cursor.fetchall()
            ]
            if len(core_quality_rows) > MAX_PROFILE_OBSERVATION_QUALITY_ROWS:
                raise PriceProfilePgError(
                    "PROFILE_OBSERVATION_QUALITY_BUDGET_EXCEEDED",
                    "Core画像字段质量分组超过固定上限",
                )
            cursor.execute(
                """SELECT f.field_id,f.status,f.quality,f.reason_code,
       count(*)::bigint AS row_count
FROM stock.source_symbol_price_profiles p
CROSS JOIN LATERAL unnest(
    p.profile_field_ids,p.profile_statuses,p.profile_qualities,p.profile_reasons
) AS f(field_id,status,quality,reason_code)
WHERE p.namespace_id=%s::uuid AND p.target_session=%s::date
  AND p.package_sha256=%s AND p.source_capture_id=%s::uuid
  AND p.profile_revision=%s
GROUP BY f.field_id,f.status,f.quality,f.reason_code
ORDER BY f.field_id,f.status,f.quality,f.reason_code
LIMIT %s""",
                (
                    namespace_id,
                    target_session,
                    package_sha256,
                    canonical_capture_id,
                    profile_revision,
                    MAX_PROFILE_OBSERVATION_QUALITY_ROWS + 1,
                ),
            )
            quality_columns = [column.name for column in cursor.description]
            profile_quality_rows = [
                dict(zip(quality_columns, row, strict=True))
                for row in cursor.fetchall()
            ]
            if len(profile_quality_rows) > MAX_PROFILE_OBSERVATION_QUALITY_ROWS:
                raise PriceProfilePgError(
                    "PROFILE_OBSERVATION_QUALITY_BUDGET_EXCEEDED",
                    "Profile字段质量分组超过固定上限",
                )

    if selected is None:
        status = "AMBIGUOUS_STORED_REVISIONS" if revisions else "NOT_COMPUTED"
    else:
        status = (
            "PG_SOURCE_PROFILE_STORED"
            if set_comparison["symbol_sets_match"]
            else "SOURCE_PROFILE_SYMBOL_SET_MISMATCH"
        )
    return {
        "status": status,
        "namespace_id": namespace_id,
        "target_session": target_session.isoformat(),
        "source_capture_id": canonical_capture_id,
        "package_sha256": package_sha256,
        "current_semantics_sha256": current_semantics_sha256,
        "profile_revision_selection": {
            "rule": selection_rule,
            "selected_profile_revision": (
                selected["profile_revision"] if selected is not None else None
            ),
            "candidate_revision_count": len(revisions),
            "current_semantics_match_count": len(matching_current),
        },
        "revision_candidates": revisions,
        "selected_revision": selected,
        "set_comparison": set_comparison,
        "core_quality_rows": core_quality_rows,
        "profile_quality_rows": profile_quality_rows,
        "database_write": False,
        "ddl_executed": False,
    }


def _snapshot_for_session(
    cursor: Any,
    *,
    namespace_id: str,
    target_session: date,
    max_target_session: date,
    package_sha256: str | None = None,
    profile_revision: str | None = None,
) -> dict[str, Any]:
    exact_identity = (package_sha256, profile_revision)
    if any(value is not None for value in exact_identity):
        if not all(
            isinstance(value, str) and value
            for value in (package_sha256, profile_revision)
        ):
            raise PriceProfilePgError(
                "PROFILE_CHANGE_SNAPSHOT_IDENTITY_INVALID",
                "精确快照必须同时提供package_sha256和profile_revision",
            )
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
    if package_sha256 is not None:
        predicates.extend(("p.package_sha256=%s", "p.profile_revision=%s"))
        parameters.extend((package_sha256, profile_revision))
    cursor.execute(
        """SELECT p.target_session, p.package_sha256,
       p.profile_revision, p.history_calendar_id, p.history_calendar_revision,
       p.history_window_start, p.history_window_end,
       p.history_requested_session_count, count(*)::bigint,
       min(r.raw_row_count),
       count(*)=min(r.raw_row_count)
         AND min(r.raw_symbol_count)=min(r.raw_row_count)
         AND min(r.capture_count)=1
         AND count(DISTINCT p.source_capture_id)=1
         AND min(p.source_capture_id::text)=min(r.raw_capture_id)
FROM stock.source_symbol_price_profiles p
LEFT JOIN (
    SELECT namespace_id, session_date, package_sha256,
           count(*)::bigint AS raw_row_count,
           count(DISTINCT source_symbol)::bigint AS raw_symbol_count,
           count(DISTINCT source_capture_id)::bigint AS capture_count,
           min(source_capture_id::text) AS raw_capture_id
    FROM stock.source_raw_daily
    GROUP BY namespace_id, session_date, package_sha256
) r ON r.namespace_id=p.namespace_id AND r.session_date=p.target_session
  AND r.package_sha256=p.package_sha256
WHERE """
        + " AND ".join(predicates)
        + """
GROUP BY p.target_session, p.package_sha256, p.profile_revision,
         p.history_calendar_id, p.history_calendar_revision,
         p.history_window_start, p.history_window_end, p.history_requested_session_count
ORDER BY p.package_sha256, p.profile_revision""",
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
            f"{target_session.isoformat()}仍有多个画像版本；请精确提供该端package_sha256和profile_revision",
        )
    row = rows[0]
    snapshot = {
        "target_session": row[0],
        "package_sha256": str(row[1]).strip(),
        "profile_revision": str(row[2]).strip(),
        "history_calendar_id": row[3],
        "history_calendar_revision": row[4],
        "window_start": row[5],
        "window_end": row[6],
        "history_session_count": int(row[7]),
        "source_symbol_count": int(row[8]),
        "expected_source_symbol_count": int(row[9]),
        "complete": bool(row[10]),
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
    WHEN before_history_calendar_id IS DISTINCT FROM after_history_calendar_id
      OR before_history_calendar_revision IS DISTINCT FROM after_history_calendar_revision
      THEN 'HISTORY_CALENDAR_MISMATCH'
    WHEN before_window_count IS DISTINCT FROM after_window_count
      THEN 'HISTORY_WINDOW_LENGTH_MISMATCH'
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
     AND r.source_capture_id=p.source_capture_id
    WHERE p.namespace_id=%s::uuid AND p.target_session=%s
      AND p.package_sha256=%s
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
     AND r.source_capture_id=p.source_capture_id
    WHERE p.namespace_id=%s::uuid AND p.target_session=%s
      AND p.package_sha256=%s
      AND p.profile_revision=%s
), paired AS (
    SELECT COALESCE(o.source_symbol, n.source_symbol) AS source_symbol,
           o.source_symbol IS NOT NULL AS before_present,
           n.source_symbol IS NOT NULL AS after_present,
           o.market AS before_market,
           n.market AS after_market,
           o.board AS before_board,
           n.board AS after_board,
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
           n.amount_ratio20 AS after_amount_ratio20,
           n.amount_ratio20_quality AS after_amount_ratio20_quality,
           n.amount_ratio20_reason AS after_amount_ratio20_reason,
           n.amount_ratio20_exact_numerator AS after_amount_ratio20_exact_numerator,
           n.amount_ratio20_exact_denominator AS after_amount_ratio20_exact_denominator,
           n.volume_ratio20 AS after_volume_ratio20,
           n.volume_ratio20_quality AS after_volume_ratio20_quality,
           n.volume_ratio20_reason AS after_volume_ratio20_reason,
           n.volume_ratio20_exact_numerator AS after_volume_ratio20_exact_numerator,
           n.volume_ratio20_exact_denominator AS after_volume_ratio20_exact_denominator,
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
           o.ma_slope20_atr AS before_ma_slope20_atr,
           n.ma_slope20_atr AS after_ma_slope20_atr,
           o.ma_slope20_atr_exact_numerator AS before_ma_slope20_atr_exact_numerator,
           n.ma_slope20_atr_exact_numerator AS after_ma_slope20_atr_exact_numerator,
           o.ma_slope20_atr_exact_denominator AS before_ma_slope20_atr_exact_denominator,
           n.ma_slope20_atr_exact_denominator AS after_ma_slope20_atr_exact_denominator,
           o.ma_slope20_atr_quality AS before_ma_slope20_atr_quality,
           n.ma_slope20_atr_quality AS after_ma_slope20_atr_quality,
           o.ma_slope20_atr_reason AS before_ma_slope20_atr_reason,
           n.ma_slope20_atr_reason AS after_ma_slope20_atr_reason,
           o.price_damage AS before_price_damage,
           n.price_damage AS after_price_damage,
           o.price_damage_quality AS before_price_damage_quality,
           n.price_damage_quality AS after_price_damage_quality,
           o.price_damage_reason AS before_price_damage_reason,
           n.price_damage_reason AS after_price_damage_reason,
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
           o.history_calendar_id AS before_history_calendar_id,
           n.history_calendar_id AS after_history_calendar_id,
           o.history_calendar_revision AS before_history_calendar_revision,
           n.history_calendar_revision AS after_history_calendar_revision,
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
               AND o.price_coordinate=n.price_coordinate
               AND o.history_calendar_id=n.history_calendar_id
               AND o.history_calendar_revision=n.history_calendar_revision
               AND o.history_requested_session_count=n.history_requested_session_count,
               FALSE
           ) AS core_compatible,
           COALESCE(
               o.source_symbol IS NOT NULL AND n.source_symbol IS NOT NULL
               AND o.producer_contract=n.producer_contract
               AND o.producer_contract_sha256=n.producer_contract_sha256
               AND o.producer_version=n.producer_version
               AND o.calculation_scope=n.calculation_scope
               AND o.price_coordinate=n.price_coordinate
               AND o.history_calendar_id=n.history_calendar_id
               AND o.history_calendar_revision=n.history_calendar_revision
               AND o.history_requested_session_count=n.history_requested_session_count
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
               CASE WHEN before_present AND after_present AND core_compatible
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
), bias20_atr_classified AS (
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
), core_transition_states AS (
    SELECT *,
           CASE
             WHEN NOT before_present THEN 'ABSENT'::text
             WHEN before_price_damage_quality IS NULL
                  AND before_price_damage IS NULL THEN 'NOT_COMPUTED'::text
             WHEN before_price_damage_quality='OBSERVED'
                  AND before_price_damage IS NOT NULL
               THEN CASE WHEN before_price_damage THEN 'TRUE'::text ELSE 'FALSE'::text END
             ELSE 'UNKNOWN'::text
           END AS before_price_damage_state,
           CASE
             WHEN NOT after_present THEN 'ABSENT'::text
             WHEN after_price_damage_quality IS NULL
                  AND after_price_damage IS NULL THEN 'NOT_COMPUTED'::text
             WHEN after_price_damage_quality='OBSERVED'
                  AND after_price_damage IS NOT NULL
               THEN CASE WHEN after_price_damage THEN 'TRUE'::text ELSE 'FALSE'::text END
             ELSE 'UNKNOWN'::text
           END AS after_price_damage_state,
           CASE
             WHEN NOT before_present THEN 'NO_BEFORE_PROFILE'::text
             WHEN before_price_damage_quality IS NULL AND before_price_damage IS NULL
               THEN 'NOT_COMPUTED_FOR_REVISION'::text
             WHEN before_price_damage_quality IS DISTINCT FROM 'OBSERVED'
               THEN COALESCE(before_price_damage_reason, 'QUALITY_NOT_OBSERVED')
             WHEN before_price_damage IS NULL THEN 'OBSERVED_VALUE_MISSING'::text
             ELSE NULL::text
           END AS before_price_damage_state_reason,
           CASE
             WHEN NOT after_present THEN 'NO_AFTER_PROFILE'::text
             WHEN after_price_damage_quality IS NULL AND after_price_damage IS NULL
               THEN 'NOT_COMPUTED_FOR_REVISION'::text
             WHEN after_price_damage_quality IS DISTINCT FROM 'OBSERVED'
               THEN COALESCE(after_price_damage_reason, 'QUALITY_NOT_OBSERVED')
             WHEN after_price_damage IS NULL THEN 'OBSERVED_VALUE_MISSING'::text
             ELSE NULL::text
           END AS after_price_damage_state_reason,
           CASE
             WHEN NOT before_present OR NOT after_present THEN 'PAIR_INCOMPARABLE'::text
             WHEN before_price_coordinate IS DISTINCT FROM after_price_coordinate
               OR before_price_coordinate IS DISTINCT FROM 'RAW_UNADJUSTED'
               OR after_price_coordinate IS DISTINCT FROM 'RAW_UNADJUSTED'
               OR NOT core_compatible THEN 'PAIR_INCOMPARABLE'::text
             WHEN NOT before_raw_record_present OR NOT after_raw_record_present
               OR before_raw_price_quality IS DISTINCT FROM 'OBSERVED'
               OR after_raw_price_quality IS DISTINCT FROM 'OBSERVED'
               OR before_raw_close IS NULL OR after_raw_close IS NULL
               OR before_price_damage_quality IS DISTINCT FROM 'OBSERVED'
               OR after_price_damage_quality IS DISTINCT FROM 'OBSERVED'
               OR before_price_damage IS NULL OR after_price_damage IS NULL
               THEN 'PAIR_UNKNOWN'::text
             ELSE 'PAIR_COMPARABLE'::text
           END AS price_damage_pair_compatibility,
           CASE
             WHEN NOT before_present THEN 'NO_BEFORE_PROFILE'::text
             WHEN NOT after_present THEN 'NO_AFTER_PROFILE'::text
             WHEN before_price_coordinate IS DISTINCT FROM after_price_coordinate
               THEN 'PRICE_COORDINATE_MISMATCH'::text
             WHEN before_price_coordinate IS DISTINCT FROM 'RAW_UNADJUSTED'
               OR after_price_coordinate IS DISTINCT FROM 'RAW_UNADJUSTED'
               THEN 'PRICE_COORDINATE_NOT_RAW_UNADJUSTED'::text
             WHEN before_history_calendar_id IS DISTINCT FROM after_history_calendar_id
               OR before_history_calendar_revision IS DISTINCT FROM after_history_calendar_revision
               THEN 'HISTORY_CALENDAR_MISMATCH'::text
             WHEN before_window_count IS DISTINCT FROM after_window_count
               THEN 'HISTORY_WINDOW_LENGTH_MISMATCH'::text
             WHEN NOT core_compatible THEN 'CORE_SEMANTICS_INCOMPATIBLE'::text
             WHEN NOT before_raw_record_present THEN 'BEFORE_RAW_ROW_MISSING'::text
             WHEN NOT after_raw_record_present THEN 'AFTER_RAW_ROW_MISSING'::text
             WHEN before_raw_price_quality IS DISTINCT FROM 'OBSERVED'
               THEN 'BEFORE_RAW_PRICE_UNKNOWN:' || COALESCE(before_raw_price_reason, 'NO_REASON')
             WHEN after_raw_price_quality IS DISTINCT FROM 'OBSERVED'
               THEN 'AFTER_RAW_PRICE_UNKNOWN:' || COALESCE(after_raw_price_reason, 'NO_REASON')
             WHEN before_price_damage_quality IS NULL AND before_price_damage IS NULL
               THEN 'BEFORE_NOT_COMPUTED_FOR_REVISION'::text
             WHEN after_price_damage_quality IS NULL AND after_price_damage IS NULL
               THEN 'AFTER_NOT_COMPUTED_FOR_REVISION'::text
             WHEN before_price_damage_quality IS DISTINCT FROM 'OBSERVED'
               THEN 'BEFORE_PRICE_DAMAGE_UNKNOWN:' || COALESCE(before_price_damage_reason, 'QUALITY_NOT_OBSERVED')
             WHEN after_price_damage_quality IS DISTINCT FROM 'OBSERVED'
               THEN 'AFTER_PRICE_DAMAGE_UNKNOWN:' || COALESCE(after_price_damage_reason, 'QUALITY_NOT_OBSERVED')
             WHEN before_price_damage IS NULL THEN 'BEFORE_PRICE_DAMAGE_VALUE_MISSING'::text
             WHEN after_price_damage IS NULL THEN 'AFTER_PRICE_DAMAGE_VALUE_MISSING'::text
             ELSE NULL::text
           END AS price_damage_pair_reason,
           CASE
             WHEN NOT before_present THEN 'ABSENT'::text
             WHEN before_ma_slope20_atr_quality IS NULL
                  AND before_ma_slope20_atr_exact_numerator IS NULL
                  AND before_ma_slope20_atr_exact_denominator IS NULL
               THEN 'NOT_COMPUTED'::text
             WHEN before_ma_slope20_atr_quality='OBSERVED'
                  AND before_ma_slope20_atr IS NOT NULL
                  AND before_ma_slope20_atr_exact_numerator IS NOT NULL
                  AND before_ma_slope20_atr_exact_denominator > 0
                  AND before_ma_slope20_atr::text NOT IN ('NaN', 'Infinity', '-Infinity')
                  AND before_ma_slope20_atr_exact_numerator::text NOT IN ('NaN', 'Infinity', '-Infinity')
                  AND before_ma_slope20_atr_exact_denominator::text NOT IN ('NaN', 'Infinity', '-Infinity')
               THEN CASE
                 WHEN before_ma_slope20_atr_exact_numerator < 0 THEN 'NEGATIVE'::text
                 WHEN before_ma_slope20_atr_exact_numerator = 0 THEN 'ZERO'::text
                 ELSE 'POSITIVE'::text
               END
             ELSE 'UNKNOWN'::text
           END AS before_ma_slope20_atr_sign,
           CASE
             WHEN NOT after_present THEN 'ABSENT'::text
             WHEN after_ma_slope20_atr_quality IS NULL
                  AND after_ma_slope20_atr_exact_numerator IS NULL
                  AND after_ma_slope20_atr_exact_denominator IS NULL
               THEN 'NOT_COMPUTED'::text
             WHEN after_ma_slope20_atr_quality='OBSERVED'
                  AND after_ma_slope20_atr IS NOT NULL
                  AND after_ma_slope20_atr_exact_numerator IS NOT NULL
                  AND after_ma_slope20_atr_exact_denominator > 0
                  AND after_ma_slope20_atr::text NOT IN ('NaN', 'Infinity', '-Infinity')
                  AND after_ma_slope20_atr_exact_numerator::text NOT IN ('NaN', 'Infinity', '-Infinity')
                  AND after_ma_slope20_atr_exact_denominator::text NOT IN ('NaN', 'Infinity', '-Infinity')
               THEN CASE
                 WHEN after_ma_slope20_atr_exact_numerator < 0 THEN 'NEGATIVE'::text
                 WHEN after_ma_slope20_atr_exact_numerator = 0 THEN 'ZERO'::text
                 ELSE 'POSITIVE'::text
               END
             ELSE 'UNKNOWN'::text
           END AS after_ma_slope20_atr_sign,
           CASE
             WHEN NOT before_present THEN 'NO_BEFORE_PROFILE'::text
             WHEN before_ma_slope20_atr_quality IS NULL
                  AND before_ma_slope20_atr_exact_numerator IS NULL
                  AND before_ma_slope20_atr_exact_denominator IS NULL
               THEN 'NOT_COMPUTED_FOR_REVISION'::text
             WHEN before_ma_slope20_atr_quality IS DISTINCT FROM 'OBSERVED'
               THEN COALESCE(before_ma_slope20_atr_reason, 'QUALITY_NOT_OBSERVED')
             WHEN before_ma_slope20_atr_exact_denominator IS NULL
               OR before_ma_slope20_atr_exact_denominator <= 0
               THEN 'EXACT_DENOMINATOR_INVALID'::text
             WHEN before_ma_slope20_atr_exact_numerator IS NULL
               THEN 'EXACT_NUMERATOR_MISSING'::text
             WHEN before_ma_slope20_atr IS NULL
               OR before_ma_slope20_atr::text IN ('NaN', 'Infinity', '-Infinity')
               OR before_ma_slope20_atr_exact_numerator::text IN ('NaN', 'Infinity', '-Infinity')
               OR before_ma_slope20_atr_exact_denominator::text IN ('NaN', 'Infinity', '-Infinity')
               THEN 'EXACT_VALUE_INVALID'::text
             ELSE NULL::text
           END AS before_ma_slope20_atr_state_reason,
           CASE
             WHEN NOT after_present THEN 'NO_AFTER_PROFILE'::text
             WHEN after_ma_slope20_atr_quality IS NULL
                  AND after_ma_slope20_atr_exact_numerator IS NULL
                  AND after_ma_slope20_atr_exact_denominator IS NULL
               THEN 'NOT_COMPUTED_FOR_REVISION'::text
             WHEN after_ma_slope20_atr_quality IS DISTINCT FROM 'OBSERVED'
               THEN COALESCE(after_ma_slope20_atr_reason, 'QUALITY_NOT_OBSERVED')
             WHEN after_ma_slope20_atr_exact_denominator IS NULL
               OR after_ma_slope20_atr_exact_denominator <= 0
               THEN 'EXACT_DENOMINATOR_INVALID'::text
             WHEN after_ma_slope20_atr_exact_numerator IS NULL
               THEN 'EXACT_NUMERATOR_MISSING'::text
             WHEN after_ma_slope20_atr IS NULL
               OR after_ma_slope20_atr::text IN ('NaN', 'Infinity', '-Infinity')
               OR after_ma_slope20_atr_exact_numerator::text IN ('NaN', 'Infinity', '-Infinity')
               OR after_ma_slope20_atr_exact_denominator::text IN ('NaN', 'Infinity', '-Infinity')
               THEN 'EXACT_VALUE_INVALID'::text
             ELSE NULL::text
           END AS after_ma_slope20_atr_state_reason,
           CASE
             WHEN NOT before_present OR NOT after_present THEN 'PAIR_INCOMPARABLE'::text
             WHEN before_price_coordinate IS DISTINCT FROM after_price_coordinate
               OR before_price_coordinate IS DISTINCT FROM 'RAW_UNADJUSTED'
               OR after_price_coordinate IS DISTINCT FROM 'RAW_UNADJUSTED'
               OR NOT core_compatible THEN 'PAIR_INCOMPARABLE'::text
             WHEN NOT before_raw_record_present OR NOT after_raw_record_present
               OR before_raw_price_quality IS DISTINCT FROM 'OBSERVED'
               OR after_raw_price_quality IS DISTINCT FROM 'OBSERVED'
               OR before_raw_close IS NULL OR after_raw_close IS NULL
               OR before_ma_slope20_atr_quality IS DISTINCT FROM 'OBSERVED'
               OR after_ma_slope20_atr_quality IS DISTINCT FROM 'OBSERVED'
               OR before_ma_slope20_atr IS NULL OR after_ma_slope20_atr IS NULL
               OR before_ma_slope20_atr_exact_numerator IS NULL
               OR after_ma_slope20_atr_exact_numerator IS NULL
               OR before_ma_slope20_atr_exact_denominator IS NULL
               OR after_ma_slope20_atr_exact_denominator IS NULL
               OR before_ma_slope20_atr_exact_denominator <= 0
               OR after_ma_slope20_atr_exact_denominator <= 0
               OR before_ma_slope20_atr::text IN ('NaN', 'Infinity', '-Infinity')
               OR after_ma_slope20_atr::text IN ('NaN', 'Infinity', '-Infinity')
               OR before_ma_slope20_atr_exact_numerator::text IN ('NaN', 'Infinity', '-Infinity')
               OR after_ma_slope20_atr_exact_numerator::text IN ('NaN', 'Infinity', '-Infinity')
               OR before_ma_slope20_atr_exact_denominator::text IN ('NaN', 'Infinity', '-Infinity')
               OR after_ma_slope20_atr_exact_denominator::text IN ('NaN', 'Infinity', '-Infinity')
               THEN 'PAIR_UNKNOWN'::text
             ELSE 'PAIR_COMPARABLE'::text
           END AS ma_slope20_pair_compatibility,
           CASE
             WHEN NOT before_present THEN 'NO_BEFORE_PROFILE'::text
             WHEN NOT after_present THEN 'NO_AFTER_PROFILE'::text
             WHEN before_price_coordinate IS DISTINCT FROM after_price_coordinate
               THEN 'PRICE_COORDINATE_MISMATCH'::text
             WHEN before_price_coordinate IS DISTINCT FROM 'RAW_UNADJUSTED'
               OR after_price_coordinate IS DISTINCT FROM 'RAW_UNADJUSTED'
               THEN 'PRICE_COORDINATE_NOT_RAW_UNADJUSTED'::text
             WHEN before_history_calendar_id IS DISTINCT FROM after_history_calendar_id
               OR before_history_calendar_revision IS DISTINCT FROM after_history_calendar_revision
               THEN 'HISTORY_CALENDAR_MISMATCH'::text
             WHEN before_window_count IS DISTINCT FROM after_window_count
               THEN 'HISTORY_WINDOW_LENGTH_MISMATCH'::text
             WHEN NOT core_compatible THEN 'CORE_SEMANTICS_INCOMPATIBLE'::text
             WHEN NOT before_raw_record_present THEN 'BEFORE_RAW_ROW_MISSING'::text
             WHEN NOT after_raw_record_present THEN 'AFTER_RAW_ROW_MISSING'::text
             WHEN before_raw_price_quality IS DISTINCT FROM 'OBSERVED'
               THEN 'BEFORE_RAW_PRICE_UNKNOWN:' || COALESCE(before_raw_price_reason, 'NO_REASON')
             WHEN after_raw_price_quality IS DISTINCT FROM 'OBSERVED'
               THEN 'AFTER_RAW_PRICE_UNKNOWN:' || COALESCE(after_raw_price_reason, 'NO_REASON')
             WHEN before_ma_slope20_atr_quality IS NULL
                  AND before_ma_slope20_atr_exact_numerator IS NULL
                  AND before_ma_slope20_atr_exact_denominator IS NULL
               THEN 'BEFORE_NOT_COMPUTED_FOR_REVISION'::text
             WHEN after_ma_slope20_atr_quality IS NULL
                  AND after_ma_slope20_atr_exact_numerator IS NULL
                  AND after_ma_slope20_atr_exact_denominator IS NULL
               THEN 'AFTER_NOT_COMPUTED_FOR_REVISION'::text
             WHEN before_ma_slope20_atr_quality IS DISTINCT FROM 'OBSERVED'
               THEN 'BEFORE_MA_SLOPE20_UNKNOWN:' || COALESCE(before_ma_slope20_atr_reason, 'QUALITY_NOT_OBSERVED')
             WHEN after_ma_slope20_atr_quality IS DISTINCT FROM 'OBSERVED'
               THEN 'AFTER_MA_SLOPE20_UNKNOWN:' || COALESCE(after_ma_slope20_atr_reason, 'QUALITY_NOT_OBSERVED')
             WHEN before_ma_slope20_atr_exact_denominator IS NULL
               OR before_ma_slope20_atr_exact_denominator <= 0
               THEN 'BEFORE_MA_SLOPE20_DENOMINATOR_INVALID'::text
             WHEN after_ma_slope20_atr_exact_denominator IS NULL
               OR after_ma_slope20_atr_exact_denominator <= 0
               THEN 'AFTER_MA_SLOPE20_DENOMINATOR_INVALID'::text
             WHEN before_ma_slope20_atr_exact_numerator IS NULL
               THEN 'BEFORE_MA_SLOPE20_NUMERATOR_MISSING'::text
             WHEN after_ma_slope20_atr_exact_numerator IS NULL
               THEN 'AFTER_MA_SLOPE20_NUMERATOR_MISSING'::text
             WHEN before_ma_slope20_atr IS NULL
               OR before_ma_slope20_atr::text IN ('NaN', 'Infinity', '-Infinity')
               OR after_ma_slope20_atr IS NULL
               OR after_ma_slope20_atr::text IN ('NaN', 'Infinity', '-Infinity')
               OR before_ma_slope20_atr_exact_numerator::text IN ('NaN', 'Infinity', '-Infinity')
               OR after_ma_slope20_atr_exact_numerator::text IN ('NaN', 'Infinity', '-Infinity')
               OR before_ma_slope20_atr_exact_denominator::text IN ('NaN', 'Infinity', '-Infinity')
               OR after_ma_slope20_atr_exact_denominator::text IN ('NaN', 'Infinity', '-Infinity')
               THEN 'MA_SLOPE20_EXACT_VALUE_INVALID'::text
             ELSE NULL::text
           END AS ma_slope20_pair_reason
    FROM bias20_atr_classified
), classified AS (
    SELECT *,
           CASE
             WHEN price_damage_pair_compatibility='PAIR_UNKNOWN' THEN 'UNKNOWN'::text
             WHEN price_damage_pair_compatibility='PAIR_INCOMPARABLE' THEN 'NOT_COMPARABLE'::text
             WHEN before_price_damage AND after_price_damage THEN 'UNCHANGED_TRUE'::text
             WHEN NOT before_price_damage AND after_price_damage THEN 'FALSE_TO_TRUE'::text
             WHEN before_price_damage AND NOT after_price_damage THEN 'TRUE_TO_FALSE'::text
             ELSE 'UNCHANGED_FALSE'::text
           END AS price_damage_transition,
           CASE
             WHEN ma_slope20_pair_compatibility='PAIR_UNKNOWN' THEN 'UNKNOWN'::text
             WHEN ma_slope20_pair_compatibility='PAIR_INCOMPARABLE' THEN 'NOT_COMPARABLE'::text
             ELSE before_ma_slope20_atr_sign || '_TO_' || after_ma_slope20_atr_sign
           END AS ma_slope20_transition_status
    FROM core_transition_states
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
WHEN before_history_calendar_id IS DISTINCT FROM after_history_calendar_id
  OR before_history_calendar_revision IS DISTINCT FROM after_history_calendar_revision
  THEN 'HISTORY_CALENDAR_MISMATCH'
WHEN before_window_count IS DISTINCT FROM after_window_count
  THEN 'HISTORY_WINDOW_LENGTH_MISMATCH'
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
    price_damage_transition: str = "ALL",
    ma_slope20_transition: str = "ALL",
    profile_field_id: str | None = None,
    profile_change_kind: str | None = None,
    before_bias20_atr_state: str = "ALL",
    after_bias20_atr_state: str = "ALL",
    before_package_sha256: str | None = None,
    before_profile_revision: str | None = None,
    after_package_sha256: str | None = None,
    after_profile_revision: str | None = None,
    expected_namespace_id: str | None = None,
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
    if expected_namespace_id is not None:
        try:
            canonical_namespace_id = str(UUID(expected_namespace_id))
        except (ValueError, TypeError, AttributeError) as error:
            raise PriceProfilePgError(
                "PROFILE_NAMESPACE_ID_INVALID",
                "expected_namespace_id必须是UUID",
            ) from error
        if canonical_namespace_id != expected_namespace_id:
            raise PriceProfilePgError(
                "PROFILE_NAMESPACE_ID_INVALID",
                "expected_namespace_id必须为规范小写UUID",
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
    if price_damage_transition not in {"ALL", "FALSE_TO_TRUE", "TRUE_TO_FALSE"}:
        raise PriceProfilePgError(
            "PROFILE_CHANGE_PRICE_DAMAGE_TRANSITION_INVALID",
            "价格损伤变化只接受ALL、FALSE_TO_TRUE或TRUE_TO_FALSE",
        )
    if ma_slope20_transition not in {
        "ALL",
        "NEGATIVE_TO_POSITIVE",
        "POSITIVE_TO_NEGATIVE",
        "ANY_SIGN_CHANGED",
    }:
        raise PriceProfilePgError(
            "PROFILE_CHANGE_MA_SLOPE20_TRANSITION_INVALID",
            "MA20/ATR斜率变化只接受ALL、NEGATIVE_TO_POSITIVE、POSITIVE_TO_NEGATIVE或ANY_SIGN_CHANGED",
        )
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
    if (before_package_sha256 is None) != (before_profile_revision is None):
        raise PriceProfilePgError(
            "PROFILE_CHANGE_SNAPSHOT_IDENTITY_INVALID",
            "起始快照须成对提供package_sha256与profile_revision；存量行键可省略",
        )
    if (after_package_sha256 is None) != (after_profile_revision is None):
        raise PriceProfilePgError(
            "PROFILE_CHANGE_SNAPSHOT_IDENTITY_INVALID",
            "结束快照须成对提供package_sha256与profile_revision；存量行键可省略",
        )
    symbol_search = source_symbol.upper() if source_symbol else None
    with _transaction(config, write=False) as (_connection, cursor, namespace_id):
        if expected_namespace_id is not None and namespace_id != expected_namespace_id:
            raise PriceProfilePgError(
                "PROFILE_NAMESPACE_MISMATCH",
                "Profile变化读取namespace与请求绑定不一致；未读取画像表",
            )
        before = _snapshot_for_session(
            cursor,
            namespace_id=namespace_id,
            target_session=before_session,
            max_target_session=max_target_session,
            package_sha256=before_package_sha256,
            profile_revision=before_profile_revision,
        )
        after = _snapshot_for_session(
            cursor,
            namespace_id=namespace_id,
            target_session=after_session,
            max_target_session=max_target_session,
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
           AND before_package_sha256 IS DISTINCT FROM after_package_sha256)::bigint AS cross_package_pair_count,
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
       count(*)::bigint AS core_source_symbol_union_count,
       count(*) FILTER (WHERE before_present AND after_present
           AND price_damage_pair_compatibility='PAIR_COMPARABLE')::bigint AS core_damage_comparable_count,
       count(*) FILTER (WHERE before_present AND after_present
           AND price_damage_pair_compatibility<>'PAIR_COMPARABLE')::bigint AS core_damage_not_comparable_count,
       count(*) FILTER (WHERE before_present AND after_present
           AND price_damage_pair_compatibility='PAIR_UNKNOWN')::bigint AS core_damage_unknown_count,
       count(*) FILTER (WHERE before_present AND after_present
           AND (before_price_damage_state_reason='NOT_COMPUTED_FOR_REVISION'
                OR after_price_damage_state_reason='NOT_COMPUTED_FOR_REVISION'))::bigint AS core_damage_not_computed_count,
       count(*) FILTER (WHERE price_damage_transition='FALSE_TO_TRUE')::bigint AS core_damage_false_to_true_count,
       count(*) FILTER (WHERE price_damage_transition='TRUE_TO_FALSE')::bigint AS core_damage_true_to_false_count,
       count(*) FILTER (WHERE price_damage_transition='UNCHANGED_TRUE')::bigint AS core_damage_unchanged_true_count,
       count(*) FILTER (WHERE price_damage_transition='UNCHANGED_FALSE')::bigint AS core_damage_unchanged_false_count,
       count(*) FILTER (WHERE before_present AND after_present
           AND ma_slope20_pair_compatibility='PAIR_COMPARABLE')::bigint AS core_slope_comparable_count,
       count(*) FILTER (WHERE before_present AND after_present
           AND ma_slope20_pair_compatibility<>'PAIR_COMPARABLE')::bigint AS core_slope_not_comparable_count,
       count(*) FILTER (WHERE before_present AND after_present
           AND ma_slope20_pair_compatibility='PAIR_UNKNOWN')::bigint AS core_slope_unknown_count,
       count(*) FILTER (WHERE before_present AND after_present
           AND (before_ma_slope20_atr_state_reason='NOT_COMPUTED_FOR_REVISION'
                OR after_ma_slope20_atr_state_reason='NOT_COMPUTED_FOR_REVISION'))::bigint AS core_slope_not_computed_count,
       count(*) FILTER (WHERE ma_slope20_transition_status='NEGATIVE_TO_NEGATIVE')::bigint AS core_slope_negative_to_negative_count,
       count(*) FILTER (WHERE ma_slope20_transition_status='NEGATIVE_TO_ZERO')::bigint AS core_slope_negative_to_zero_count,
       count(*) FILTER (WHERE ma_slope20_transition_status='NEGATIVE_TO_POSITIVE')::bigint AS core_slope_negative_to_positive_count,
       count(*) FILTER (WHERE ma_slope20_transition_status='ZERO_TO_NEGATIVE')::bigint AS core_slope_zero_to_negative_count,
       count(*) FILTER (WHERE ma_slope20_transition_status='ZERO_TO_ZERO')::bigint AS core_slope_zero_to_zero_count,
       count(*) FILTER (WHERE ma_slope20_transition_status='ZERO_TO_POSITIVE')::bigint AS core_slope_zero_to_positive_count,
       count(*) FILTER (WHERE ma_slope20_transition_status='POSITIVE_TO_NEGATIVE')::bigint AS core_slope_positive_to_negative_count,
       count(*) FILTER (WHERE ma_slope20_transition_status='POSITIVE_TO_ZERO')::bigint AS core_slope_positive_to_zero_count,
       count(*) FILTER (WHERE ma_slope20_transition_status='POSITIVE_TO_POSITIVE')::bigint AS core_slope_positive_to_positive_count,
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
        has_core_transition_filter = (
            price_damage_transition != "ALL" or ma_slope20_transition != "ALL"
        )
        has_full_source_union_filter = (
            has_bias20_atr_state_filter or has_core_transition_filter
        )
        page_conditions = []
        page_filter_parameters: list[Any] = []
        if not has_full_source_union_filter:
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
        if price_damage_transition != "ALL":
            page_conditions.append("price_damage_transition=%s")
            page_filter_parameters.append(price_damage_transition)
        if ma_slope20_transition == "NEGATIVE_TO_POSITIVE":
            page_conditions.append(
                "ma_slope20_transition_status='NEGATIVE_TO_POSITIVE'"
            )
        elif ma_slope20_transition == "POSITIVE_TO_NEGATIVE":
            page_conditions.append(
                "ma_slope20_transition_status='POSITIVE_TO_NEGATIVE'"
            )
        elif ma_slope20_transition == "ANY_SIGN_CHANGED":
            page_conditions.append(
                "ma_slope20_pair_compatibility='PAIR_COMPARABLE' "
                "AND before_ma_slope20_atr_sign IS DISTINCT FROM after_ma_slope20_atr_sign"
            )
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
       before_price_damage, after_price_damage,
       before_price_damage_quality, after_price_damage_quality,
       before_price_damage_reason, after_price_damage_reason,
       before_price_damage_state, after_price_damage_state,
       before_price_damage_state_reason, after_price_damage_state_reason,
       price_damage_pair_compatibility, price_damage_pair_reason,
       price_damage_transition,
       before_ma_slope20_atr, after_ma_slope20_atr,
       before_ma_slope20_atr_exact_numerator, after_ma_slope20_atr_exact_numerator,
       before_ma_slope20_atr_exact_denominator, after_ma_slope20_atr_exact_denominator,
       before_ma_slope20_atr_quality, after_ma_slope20_atr_quality,
       before_ma_slope20_atr_reason, after_ma_slope20_atr_reason,
       before_ma_slope20_atr_sign, after_ma_slope20_atr_sign,
       before_ma_slope20_atr_state_reason, after_ma_slope20_atr_state_reason,
       ma_slope20_pair_compatibility, ma_slope20_pair_reason,
       ma_slope20_transition_status,
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
    summary_values = {key: int(value or 0) for key, value in summary.items()}
    core_summary_keys = {
        "core_source_symbol_union_count",
        "core_damage_comparable_count",
        "core_damage_not_comparable_count",
        "core_damage_unknown_count",
        "core_damage_not_computed_count",
        "core_damage_false_to_true_count",
        "core_damage_true_to_false_count",
        "core_damage_unchanged_true_count",
        "core_damage_unchanged_false_count",
        "core_slope_comparable_count",
        "core_slope_not_comparable_count",
        "core_slope_unknown_count",
        "core_slope_not_computed_count",
        "core_slope_negative_to_negative_count",
        "core_slope_negative_to_zero_count",
        "core_slope_negative_to_positive_count",
        "core_slope_zero_to_negative_count",
        "core_slope_zero_to_zero_count",
        "core_slope_zero_to_positive_count",
        "core_slope_positive_to_negative_count",
        "core_slope_positive_to_zero_count",
        "core_slope_positive_to_positive_count",
    }
    core_counts = {key: summary_values.pop(key) for key in core_summary_keys}
    core_transitions = {
        "source_symbol_union_count": core_counts["core_source_symbol_union_count"],
        "intersection_count": summary_values["intersection_count"],
        "before_only_count": summary_values["before_only_count"],
        "after_only_count": summary_values["after_only_count"],
        "price_damage": {
            "comparable_count": core_counts["core_damage_comparable_count"],
            "not_comparable_count": core_counts["core_damage_not_comparable_count"],
            "unknown_count": core_counts["core_damage_unknown_count"],
            "not_computed_for_revision_count": core_counts[
                "core_damage_not_computed_count"
            ],
            "unknown_is_subset_of_not_comparable": True,
            "false_to_true_count": core_counts["core_damage_false_to_true_count"],
            "true_to_false_count": core_counts["core_damage_true_to_false_count"],
            "unchanged_true_count": core_counts["core_damage_unchanged_true_count"],
            "unchanged_false_count": core_counts["core_damage_unchanged_false_count"],
        },
        "ma_slope20_atr": {
            "comparable_count": core_counts["core_slope_comparable_count"],
            "not_comparable_count": core_counts["core_slope_not_comparable_count"],
            "unknown_count": core_counts["core_slope_unknown_count"],
            "not_computed_for_revision_count": core_counts[
                "core_slope_not_computed_count"
            ],
            "unknown_is_subset_of_not_comparable": True,
            "sign_transition_matrix": {
                before_sign: {
                    after_sign: core_counts[
                        "core_slope_"
                        + before_sign.lower()
                        + "_to_"
                        + after_sign.lower()
                        + "_count"
                    ]
                    for after_sign in ("NEGATIVE", "ZERO", "POSITIVE")
                }
                for before_sign in ("NEGATIVE", "ZERO", "POSITIVE")
            },
        },
    }
    return {
        "status": "AVAILABLE",
        "namespace_id": namespace_id,
        "before_snapshot": before,
        "after_snapshot": after,
        "summary": summary_values,
        "core_transitions": core_transitions,
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
        "core_transition_filter": {
            "price_damage": price_damage_transition,
            "ma_slope20_atr": ma_slope20_transition,
            "change_type": change_type,
            "uses_full_source_union": has_core_transition_filter,
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
    after_amount_ratio20_min: Decimal | str | None = None,
    after_volume_ratio20_min: Decimal | str | None = None,
    before_package_sha256: str,
    before_profile_revision: str,
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
    ratio_bounds = {}
    ratio_bound_text = {}
    for field, value in (
        ("amount_ratio20", after_amount_ratio20_min),
        ("volume_ratio20", after_volume_ratio20_min),
    ):
        if value is None:
            continue
        if (
            isinstance(value, bool)
            or not isinstance(value, (Decimal, str))
            or len(str(value)) > 128
            or (
                isinstance(value, str)
                and re.fullmatch(
                    r"[+]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?", value
                )
                is None
            )
        ):
            raise PriceProfilePgError(
                "PROFILE_FILTER_INVALID", f"{field}必须是非负有限Decimal"
            )
        try:
            decimal_value = Decimal(value)
        except (InvalidOperation, ValueError, TypeError) as error:
            raise PriceProfilePgError(
                "PROFILE_FILTER_INVALID", f"{field}必须是非负有限Decimal"
            ) from error
        if (
            not decimal_value.is_finite()
            or decimal_value < 0
            or abs(decimal_value.as_tuple().exponent) > 128
            or decimal_value.adjusted() > 128
        ):
            raise PriceProfilePgError(
                "PROFILE_FILTER_INVALID", f"{field}必须是范围内非负有限Decimal"
            )
        ratio_bounds[field] = Fraction(decimal_value)
        ratio_bound_text[field] = str(decimal_value)
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
            before_package_sha256,
            before_profile_revision,
            after_package_sha256,
            after_profile_revision,
        )
    ):
        raise PriceProfilePgError(
            "PROFILE_RELATION_SNAPSHOT_IDENTITY_INVALID",
            "前后快照必须精确提供package SHA和profile revision",
        )

    with _transaction(config, write=False) as (_connection, cursor, namespace_id):
        before = _snapshot_for_session(
            cursor,
            namespace_id=namespace_id,
            target_session=before_session,
            max_target_session=max_target_session,
            package_sha256=before_package_sha256,
            profile_revision=before_profile_revision,
        )
        after = _snapshot_for_session(
            cursor,
            namespace_id=namespace_id,
            target_session=after_session,
            max_target_session=max_target_session,
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
        for field, bound in ratio_bounds.items():
            prefix = f"after_{field}"
            filters.extend(
                (
                    "core_compatible",
                    "after_price_coordinate='RAW_UNADJUSTED'",
                    f"{prefix}_quality='OBSERVED'",
                    f"{prefix} IS NOT NULL",
                    f"{prefix}_exact_numerator IS NOT NULL",
                    f"{prefix}_exact_denominator > 0",
                    f"{prefix}::text NOT IN ('NaN', 'Infinity', '-Infinity')",
                    f"{prefix}_exact_numerator::text NOT IN ('NaN', 'Infinity', '-Infinity')",
                    f"{prefix}_exact_denominator::text NOT IN ('NaN', 'Infinity', '-Infinity')",
                    f"{prefix}_exact_numerator * %s::numeric >= %s::numeric * {prefix}_exact_denominator",
                )
            )
            filter_parameters.extend((bound.denominator, bound.numerator))
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
        "after_ratio20_min_filters": ratio_bound_text,
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
    before_package_sha256: str,
    before_profile_revision: str,
    after_package_sha256: str,
    after_profile_revision: str,
) -> dict[str, Any]:
    """按两份精确画像只读聚合来源池MA20关系。"""
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
        (before_package_sha256, before_profile_revision),
        (after_package_sha256, after_profile_revision),
    )
    if any(not package_sha or not revision for package_sha, revision in identities):
        raise PriceProfilePgError(
            "PROFILE_BREADTH_SNAPSHOT_IDENTITY_INVALID",
            "广度对照必须显式提供前后package SHA和profile revision",
        )

    with _transaction(config, write=False) as (_connection, cursor, namespace_id):
        before = _snapshot_for_session(
            cursor,
            namespace_id=namespace_id,
            target_session=before_session,
            max_target_session=max_target_session,
            package_sha256=before_package_sha256,
            profile_revision=before_profile_revision,
        )
        after = _snapshot_for_session(
            cursor,
            namespace_id=namespace_id,
            target_session=after_session,
            max_target_session=max_target_session,
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
    before_package_sha256: str,
    before_profile_revision: str,
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
        before_package_sha256,
        before_profile_revision,
        after_package_sha256,
        after_profile_revision,
    )
    if any(not isinstance(value, str) or not value for value in identities):
        raise PriceProfilePgError(
            "PROFILE_BREADTH_SNAPSHOT_IDENTITY_INVALID",
            "广度明细必须显式提供前后package SHA和profile revision",
        )

    with _transaction(config, write=False) as (_connection, cursor, namespace_id):
        before = _snapshot_for_session(
            cursor,
            namespace_id=namespace_id,
            target_session=before_session,
            max_target_session=max_target_session,
            package_sha256=before_package_sha256,
            profile_revision=before_profile_revision,
        )
        after = _snapshot_for_session(
            cursor,
            namespace_id=namespace_id,
            target_session=after_session,
            max_target_session=max_target_session,
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
    before_package_sha256: str,
    before_profile_revision: str,
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
            package_sha256=before_package_sha256,
            profile_revision=before_profile_revision,
        )
        after = _snapshot_for_session(
            cursor,
            namespace_id=namespace_id,
            target_session=after_session,
            max_target_session=max_target_session,
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
    price_damage_state: str = "ALL",
    ma_slope20_sign: str = "ALL",
    amount_ratio20_min: Decimal | str | None = None,
    amount_ratio20_max: Decimal | str | None = None,
    volume_ratio20_min: Decimal | str | None = None,
    volume_ratio20_max: Decimal | str | None = None,
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
    if price_damage_state not in {"ALL", "TRUE", "FALSE", "UNKNOWN"}:
        raise PriceProfilePgError(
            "PROFILE_FILTER_INVALID", "价格损伤筛选值必须为ALL、TRUE、FALSE或UNKNOWN"
        )
    if ma_slope20_sign not in {
        "ALL",
        "POSITIVE",
        "ZERO",
        "NEGATIVE",
        "UNKNOWN",
    }:
        raise PriceProfilePgError(
            "PROFILE_FILTER_INVALID",
            "MA20斜率筛选值必须为ALL、POSITIVE、ZERO、NEGATIVE或UNKNOWN",
        )

    def ratio_bound(value: Decimal | str | None, field: str) -> Fraction | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (Decimal, str)):
            raise PriceProfilePgError(
                "PROFILE_FILTER_INVALID", f"{field}必须是非负有限Decimal"
            )
        try:
            decimal_value = value if isinstance(value, Decimal) else Decimal(value)
        except (InvalidOperation, TypeError, ValueError) as error:
            raise PriceProfilePgError(
                "PROFILE_FILTER_INVALID", f"{field}必须是非负有限Decimal"
            ) from error
        if not decimal_value.is_finite() or decimal_value < 0:
            raise PriceProfilePgError(
                "PROFILE_FILTER_INVALID", f"{field}必须是非负有限Decimal"
            )
        return Fraction(decimal_value)

    ratio_bounds = {
        "amount_ratio20": (
            ratio_bound(amount_ratio20_min, "amount_ratio20_min"),
            ratio_bound(amount_ratio20_max, "amount_ratio20_max"),
        ),
        "volume_ratio20": (
            ratio_bound(volume_ratio20_min, "volume_ratio20_min"),
            ratio_bound(volume_ratio20_max, "volume_ratio20_max"),
        ),
    }
    for field, (minimum, maximum) in ratio_bounds.items():
        if minimum is not None and maximum is not None and minimum > maximum:
            raise PriceProfilePgError(
                "PROFILE_FILTER_INVALID", f"{field}下界不得大于上界"
            )
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
        "p.package_sha256=%s",
        "p.profile_revision=%s",
    ]
    parameters: list[Any] = [
        None,
        target_session,
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
    if price_damage_state in {"TRUE", "FALSE"}:
        conditions.extend(["p.price_damage_quality='OBSERVED'", "p.price_damage=%s"])
        parameters.append(price_damage_state == "TRUE")
    elif price_damage_state == "UNKNOWN":
        conditions.append("p.price_damage_quality='UNKNOWN'")
    if ma_slope20_sign == "UNKNOWN":
        conditions.append("p.ma_slope20_atr_quality='UNKNOWN'")
    elif ma_slope20_sign != "ALL":
        conditions.extend(
            [
                "p.ma_slope20_atr_quality='OBSERVED'",
                "p.ma_slope20_atr_exact_numerator IS NOT NULL",
            ]
        )
        sign_operator = {"POSITIVE": "> 0", "ZERO": "= 0", "NEGATIVE": "< 0"}[
            ma_slope20_sign
        ]
        conditions.append(f"p.ma_slope20_atr_exact_numerator {sign_operator}")
    for field, (minimum, maximum) in ratio_bounds.items():
        for bound, operator in ((minimum, ">="), (maximum, "<=")):
            if bound is None:
                continue
            conditions.extend(
                [
                    f"p.{field}_quality='OBSERVED'",
                    f"p.{field}_exact_numerator IS NOT NULL",
                    f"p.{field}_exact_denominator > 0",
                    f"p.{field}_exact_numerator * %s::numeric {operator} "
                    "%s::numeric * p."
                    f"{field}_exact_denominator",
                ]
            )
            parameters.extend((bound.denominator, bound.numerator))
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
        snapshot = _snapshot_for_session(
            cursor,
            namespace_id=namespace_id,
            target_session=target_session,
            max_target_session=max_target_session,
            package_sha256=package_sha256,
            profile_revision=profile_revision,
        )
        parameters[0] = namespace_id
        cursor.execute(
            """SELECT count(*)::bigint,
       count(DISTINCT r.source_symbol)::bigint,
       count(DISTINCT r.source_capture_id)::bigint,
       min(c.normalized_count), max(c.normalized_count)
FROM stock.source_raw_daily r
LEFT JOIN stock.source_captures c ON c.namespace_id=r.namespace_id
  AND c.capture_id=r.source_capture_id
WHERE r.namespace_id=%s::uuid AND r.session_date=%s
  AND r.package_sha256=%s""",
            (namespace_id, target_session, package_sha256),
        )
        (
            raw_total,
            raw_symbols,
            capture_count,
            capture_expected,
            capture_expected_max,
        ) = cursor.fetchone()
        expected_total = int(raw_total)
        if (
            expected_total < 1
            or int(raw_symbols) != expected_total
            or int(capture_count) != 1
            or capture_expected != expected_total
            or capture_expected_max != expected_total
        ):
            raise PriceProfilePgError(
                "PROFILE_RAW_SOURCE_INCOMPLETE",
                "目标日期与包SHA没有唯一且完整的Raw捕获来源代码集合",
            )
        cursor.execute(
            """SELECT count(*)::bigint FROM stock.source_symbol_price_profiles
WHERE namespace_id=%s::uuid AND target_session=%s
  AND package_sha256=%s AND profile_revision=%s""",
            (
                namespace_id,
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
WHERE p.namespace_id=%s::uuid
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
WHERE p.namespace_id=%s::uuid
  AND p.target_session=%s AND p.package_sha256=%s AND p.profile_revision=%s
  AND p.profile_field_ids[field_index.subscript]=%s
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3""",
                (
                    namespace_id,
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
  AND r.source_capture_id=p.source_capture_id
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
  AND r.source_capture_id=p.source_capture_id
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
            for column in ("namespace_id", "source_capture_id"):
                if isinstance(row.get(column), UUID):
                    row[column] = str(row[column])
            slope_columns = (
                "ma_slope20_atr",
                "ma_slope20_atr_exact_numerator",
                "ma_slope20_atr_exact_denominator",
                "ma_slope20_atr_quality",
                "ma_slope20_atr_reason",
            )
            damage_columns = (
                "price_damage",
                "price_damage_quality",
                "price_damage_reason",
            )
            slope_uncomputed = all(row.get(column) is None for column in slope_columns)
            damage_uncomputed = all(
                row.get(column) is None for column in damage_columns
            )
            if slope_uncomputed != damage_uncomputed:
                raise PriceProfilePgError(
                    "PROFILE_NEW_CORE_FIELDS_INCOMPLETE",
                    "画像修订只包含部分价格损伤/斜率字段",
                )
            row["ma_slope20_atr_computation_status"] = (
                "NOT_COMPUTED_FOR_REVISION" if slope_uncomputed else "COMPUTED"
            )
            row["price_damage_computation_status"] = (
                "NOT_COMPUTED_FOR_REVISION" if damage_uncomputed else "COMPUTED"
            )
            for field in (
                "amount_ratio5",
                "amount_ratio20",
                "volume_ratio5",
                "volume_ratio20",
            ):
                ratio_uncomputed = all(
                    row.get(column) is None
                    for column in (
                        field,
                        f"{field}_exact_numerator",
                        f"{field}_exact_denominator",
                        f"{field}_quality",
                        f"{field}_reason",
                    )
                )
                row[f"{field}_computation_status"] = (
                    "NOT_COMPUTED_FOR_REVISION" if ratio_uncomputed else "COMPUTED"
                )
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
        "package_sha256": package_sha256,
        "profile_revision": profile_revision,
        "source_pool_total": snapshot_total,
        "ratio_metadata": {
            "basis": "TDX_NATIVE_SAME_PACKAGE_SCALE_INVARIANT",
            "absolute_amount_unit_status": "UNIT_UNVERIFIED",
            "absolute_volume_unit_status": "UNIT_UNVERIFIED",
        },
        "ratio_filters": {
            field: {
                "minimum": str(minimum) if minimum is not None else None,
                "maximum": str(maximum) if maximum is not None else None,
            }
            for field, (minimum, maximum) in ratio_bounds.items()
        },
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
            "ratio_filter": "POSTGRES_EXACT_FRACTION_CROSS_MULTIPLICATION",
            "price_position_filter": "POSTGRES_NUMERIC",
            "bias20_atr_filter": "POSTGRES_EXACT_NUMERIC_CROSS_MULTIPLICATION",
            "bias20_atr_filter_scope": "COMPLETE_SOURCE_PROFILE_SET",
            "profile_field_filter": "POSTGRES_ARRAY_FIELD_ID_INDEX",
            "profile_field_summary": "POSTGRES_FULL_SOURCE_SET",
            "all_candidates_recomputed": False,
            "source_zip_opened": False,
            "database_write": False,
        },
    }
