"""保存并读取按BaoStock捕获版本隔离的typed来源观察。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from fractions import Fraction
from typing import Any
from uuid import UUID

from stock_a.domain import FieldValue
from stock_a.sources.baostock.daily import BaoDailyObservation, BaoUnitContract
from stock_a.sources.baostock.identity import normalize_baostock_symbol

from . import source_raw
from .typed import _finite_decimal

_CAPTURE_CLOCK_ROLE = "CAPTURED_AT_THIS_QUERY_NOT_HISTORICAL_FIRST_AVAILABLE"
_QUALITY = frozenset({"OBSERVED", "UNKNOWN_DATA", "NOT_APPLICABLE"})
_FIELD_NAMES = (
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "turn",
    "tradestatus",
    "is_st",
)
_DAILY_COLUMNS = (
    "capture_id",
    "source_code",
    "source_symbol",
    "session_date",
    "captured_at",
    "capture_clock_role",
    "adjustment_flag",
    "price_coordinate",
    "unit_contract_version",
    "returned_fields",
    *(
        f"{field_name}_{suffix}"
        for field_name in _FIELD_NAMES
        for suffix in ("value", "quality", "reason")
    ),
)


class BaoStockDailyPgError(Exception):
    """BaoStock typed来源写入、读回或业务键冲突错误。"""

    preserve_transaction_error = True

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


@dataclass(frozen=True)
class BaoStockDailyRecord:
    """一条规范观察及其来源代码、捕获时钟，不包含原始响应payload。"""

    source_symbol: str
    observation: BaoDailyObservation
    captured_at: datetime
    adjustment_flag: str
    price_coordinate: str


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise BaoStockDailyPgError(
            "CAPTURE_CLOCK_INVALID", "BaoStock捕获时钟必须含时区"
        )
    return value


def _contract_values(units: BaoUnitContract) -> tuple[Any, ...]:
    if not isinstance(units, BaoUnitContract):
        raise BaoStockDailyPgError("UNIT_CONTRACT_INVALID", "缺少BaoStock显式单位合同")
    try:
        close_to_cny = _finite_decimal(units.close_to_cny)
        amount_to_cny = _finite_decimal(units.amount_to_cny)
        volume_to_shares = _finite_decimal(units.volume_to_shares)
    except ValueError as error:
        raise BaoStockDailyPgError(
            "UNIT_CONTRACT_NOT_DECIMAL",
            "BaoStock单位倍率必须能由PostgreSQL numeric精确表示",
        ) from error
    if (
        not isinstance(units.version, str)
        or not 1 <= len(units.version) <= 128
        or units.version != units.version.strip()
        or not isinstance(units.close_input_unit, str)
        or not 1 <= len(units.close_input_unit) <= 64
        or units.close_input_unit != units.close_input_unit.strip()
        or units.close_input_unit == "LEGACY_UNIT_NOT_RECORDED"
        or not isinstance(units.amount_input_unit, str)
        or not 1 <= len(units.amount_input_unit) <= 64
        or units.amount_input_unit != units.amount_input_unit.strip()
        or units.amount_input_unit == "LEGACY_UNIT_NOT_RECORDED"
        or not isinstance(units.volume_input_unit, str)
        or not 1 <= len(units.volume_input_unit) <= 64
        or units.volume_input_unit != units.volume_input_unit.strip()
        or units.volume_input_unit == "LEGACY_UNIT_NOT_RECORDED"
        or units.turn_input_unit != "PERCENT"
    ):
        raise BaoStockDailyPgError(
            "UNIT_CONTRACT_INVALID", "BaoStock单位合同字段不符合PG合同"
        )
    return (
        units.version,
        units.close_input_unit,
        close_to_cny,
        units.amount_input_unit,
        amount_to_cny,
        units.volume_input_unit,
        volume_to_shares,
        units.turn_input_unit,
    )


def _field_parts(field: FieldValue, *, numeric: bool) -> tuple[Any, str, str | None]:
    if not isinstance(field, FieldValue) or field.quality not in _QUALITY:
        raise BaoStockDailyPgError(
            "FIELD_QUALITY_INVALID", "BaoStock字段质量不符合规范枚举"
        )
    if field.quality == "OBSERVED":
        if field.reason is not None or field.value is None:
            raise BaoStockDailyPgError(
                "FIELD_VALUE_INVALID", "OBSERVED字段必须带值且无缺失原因"
            )
        if numeric:
            try:
                value: Any = _finite_decimal(field.value)
            except ValueError as error:
                raise BaoStockDailyPgError(
                    "FIELD_VALUE_NOT_DECIMAL", "字段值不能无损表示为PostgreSQL numeric"
                ) from error
            if isinstance(field.value, Fraction) and field.value.denominator != 1:
                # PostgreSQL numeric can store finite decimal fractions; _finite_decimal
                # has already rejected any non-terminating rational.
                pass
        else:
            if isinstance(field.value, bool) or not isinstance(field.value, (str, int)):
                raise BaoStockDailyPgError(
                    "STATUS_VALUE_INVALID", "BaoStock状态值必须为文本或整数"
                )
            value = str(field.value)
        return value, field.quality, None
    if (
        field.value is not None
        or not isinstance(field.reason, str)
        or not field.reason.strip()
    ):
        raise BaoStockDailyPgError(
            "FIELD_UNKNOWN_INVALID", "未知字段必须为空值并保留中文原因"
        )
    return None, field.quality, field.reason


def _record_values(
    capture_id: UUID,
    record: BaoStockDailyRecord,
    units: BaoUnitContract,
    *,
    cutoff: date,
) -> tuple[Any, ...]:
    if not isinstance(capture_id, UUID):
        raise BaoStockDailyPgError("CAPTURE_ID_INVALID", "捕获标识必须是UUID")
    if not isinstance(record, BaoStockDailyRecord):
        raise BaoStockDailyPgError("RECORD_INVALID", "BaoStock记录必须使用typed记录")
    observation = record.observation
    if not isinstance(observation, BaoDailyObservation):
        raise BaoStockDailyPgError("OBSERVATION_INVALID", "缺少规范化BaoStock日线观察")
    if type(cutoff) is not date or type(observation.session_date) is not date:
        raise BaoStockDailyPgError(
            "SESSION_DATE_INVALID", "目标会话与截止日必须显式为date"
        )
    if observation.session_date > cutoff:
        raise BaoStockDailyPgError(
            "RESEARCH_CUTOFF_EXCEEDED", "BaoStock行情日期超出本次显式研究截止日"
        )
    captured_at = _aware(record.captured_at)
    if record.adjustment_flag != "3" or record.price_coordinate != "RAW_UNADJUSTED":
        raise BaoStockDailyPgError(
            "PRICE_COORDINATE_INVALID",
            "BaoStock来源写入只接受已核验的未复权adjustflag=3响应",
        )
    symbol = normalize_baostock_symbol(record.source_symbol)
    source_symbol = normalize_baostock_symbol(observation.source_code)
    if symbol is None or source_symbol is None or symbol != source_symbol:
        raise BaoStockDailyPgError(
            "SOURCE_CODE_MISMATCH", "BaoStock来源代码与规范证券代码不一致"
        )
    if observation.unit_contract_version != units.version:
        raise BaoStockDailyPgError(
            "UNIT_CONTRACT_MISMATCH", "日线观察与单位合同版本不一致"
        )
    if observation.returned_fields != tuple(
        sorted(set(observation.returned_fields))
    ) or any(
        not isinstance(name, str) or not name.strip()
        for name in observation.returned_fields
    ):
        raise BaoStockDailyPgError(
            "RETURNED_FIELDS_INVALID", "实际返回字段必须已排序且不重复"
        )

    values: list[Any] = [
        capture_id,
        observation.source_code,
        symbol,
        observation.session_date,
        captured_at,
        _CAPTURE_CLOCK_ROLE,
        record.adjustment_flag,
        record.price_coordinate,
        units.version,
        list(observation.returned_fields),
    ]
    for name in _FIELD_NAMES:
        field = getattr(observation, name)
        value, quality, reason = _field_parts(
            field, numeric=name not in {"tradestatus", "is_st"}
        )
        if (
            name == "volume"
            and quality == "OBSERVED"
            and value != value.to_integral_value()
        ):
            raise BaoStockDailyPgError(
                "VOLUME_NOT_INTEGER", "BaoStock成交量转换后必须为整数股"
            )
        if (
            name in {"tradestatus", "is_st"}
            and quality == "OBSERVED"
            and value not in {"0", "1"}
        ):
            # The normalizer should already soften invalid status enums to UNKNOWN_DATA.
            raise BaoStockDailyPgError(
                "STATUS_VALUE_INVALID", "BaoStock状态字段只接受0或1"
            )
        values.extend((value, quality, reason))
    return tuple(values)


_DAILY_INSERT = (
    f"INSERT INTO source_baostock_daily ({', '.join(_DAILY_COLUMNS)}) "
    f"VALUES ({', '.join(['%s'] * len(_DAILY_COLUMNS))}) "
    "ON CONFLICT (capture_id, source_code, session_date) DO NOTHING"
)


def persist_baostock_daily_capture(
    config: Mapping[str, Any],
    *,
    capture_id: UUID,
    records: Sequence[BaoStockDailyRecord],
    units: BaoUnitContract,
    cutoff: date,
) -> dict[str, Any]:
    """单事务保存规范化BaoStock行；同捕获重试校验摘要，禁止越过显式截止日。"""
    if not isinstance(config, Mapping):
        raise BaoStockDailyPgError("RUNTIME_CONFIG_INVALID", "缺少唯一PG运行配置")
    if (
        not isinstance(records, Sequence)
        or isinstance(records, (str, bytes))
        or not records
    ):
        raise BaoStockDailyPgError(
            "RECORDS_INVALID", "一次BaoStock捕获必须包含至少一条规范行"
        )
    contract_values = _contract_values(units)
    normalized: list[tuple[Any, ...]] = []
    keys: set[tuple[str, date]] = set()
    for record in records:
        values = _record_values(capture_id, record, units, cutoff=cutoff)
        key = (values[1], values[3])
        if key in keys:
            raise BaoStockDailyPgError(
                "DUPLICATE_SOURCE_ROW", "同一捕获重复返回来源代码与日期"
            )
        keys.add(key)
        normalized.append(values)

    inserted = 0
    unchanged = 0
    inserted_by_session: dict[str, int] = {}
    unchanged_by_session: dict[str, int] = {}
    try:
        with source_raw._transaction(config, write=True) as (
            _connection,
            cursor,
            _scope,
        ):
            cursor.execute(
                """INSERT INTO source_baostock_unit_contracts (
    unit_contract_version, close_input_unit, close_to_cny,
    amount_input_unit, amount_to_cny, volume_input_unit,
    volume_to_shares, turn_input_unit
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (unit_contract_version) DO NOTHING""",
                contract_values,
            )
            cursor.execute(
                """SELECT unit_contract_version, close_input_unit, close_to_cny,
       amount_input_unit, amount_to_cny, volume_input_unit,
       volume_to_shares, turn_input_unit
FROM source_baostock_unit_contracts WHERE unit_contract_version=%s""",
                (units.version,),
            )
            stored_contract = cursor.fetchone()
            if stored_contract != contract_values:
                raise BaoStockDailyPgError(
                    "UNIT_CONTRACT_VERSION_CONFLICT",
                    "同一单位合同版本绑定了不同输入单位或转换倍率",
                )
            for values in normalized:
                cursor.execute(_DAILY_INSERT, values)
                if cursor.rowcount == 1:
                    inserted += 1
                    session_key = values[3].isoformat()
                    inserted_by_session[session_key] = (
                        inserted_by_session.get(session_key, 0) + 1
                    )
                    continue
                cursor.execute(
                    f"""SELECT {", ".join(_DAILY_COLUMNS)}
FROM source_baostock_daily
WHERE capture_id=%s AND source_code=%s AND session_date=%s""",
                    (capture_id, values[1], values[3]),
                )
                existing = cursor.fetchone()
                if existing is None or tuple(existing) != values:
                    raise BaoStockDailyPgError(
                        "CAPTURE_CONTENT_CONFLICT",
                        "同一捕获业务键已存在不同typed字段、质量、单位或时钟",
                    )
                unchanged += 1
                session_key = values[3].isoformat()
                unchanged_by_session[session_key] = (
                    unchanged_by_session.get(session_key, 0) + 1
                )
    except BaoStockDailyPgError:
        raise
    except Exception as error:
        if getattr(error, "preserve_transaction_error", False) is True:
            raise
        raise BaoStockDailyPgError(
            "PG_WRITE_FAILED", f"BaoStock来源捕获写入失败（{type(error).__name__}）"
        ) from error
    return {
        "inserted_rows": inserted,
        "unchanged_rows": unchanged,
        "inserted_rows_by_session": inserted_by_session,
        "unchanged_rows_by_session": unchanged_by_session,
    }


def _read_field(value: Any, quality: str, reason: str) -> FieldValue:
    if quality == "OBSERVED":
        return FieldValue(value)
    return FieldValue(None, quality, reason)


def resolve_baostock_daily_capture(
    config: Mapping[str, Any],
    *,
    source_symbol: str,
    target_session: date,
    cutoff: date,
) -> dict[str, Any]:
    """为单股单日详情定位最新明确捕获；并列捕获时不猜选。"""
    if not isinstance(config, Mapping):
        raise BaoStockDailyPgError("RUNTIME_CONFIG_INVALID", "缺少唯一PG运行配置")
    symbol = normalize_baostock_symbol(source_symbol)
    if symbol is None:
        raise BaoStockDailyPgError(
            "SOURCE_SYMBOL_INVALID", "BaoStock读取证券代码必须为沪深规范来源代码"
        )
    if (
        type(target_session) is not date
        or type(cutoff) is not date
        or target_session > cutoff
    ):
        raise BaoStockDailyPgError(
            "READ_RANGE_INVALID", "目标行情日期必须是date且不超过显式截止日"
        )
    try:
        with source_raw._transaction(config, write=False) as (
            _connection,
            cursor,
            _scope,
        ):
            cursor.execute(
                """SELECT capture_id,captured_at,unit_contract_version
                   FROM source_baostock_daily
                   WHERE source_symbol=%s AND session_date=%s
                     AND session_date<=%s
                   ORDER BY captured_at DESC,capture_id::text DESC
                   LIMIT 2""",
                (symbol, target_session, cutoff),
            )
            candidates = cursor.fetchall()
    except Exception as error:
        if getattr(error, "preserve_transaction_error", False) is True:
            raise
        raise BaoStockDailyPgError(
            "PG_READ_FAILED", f"BaoStock捕获身份读取失败（{type(error).__name__}）"
        ) from error
    if not candidates:
        return {
            "status": "NOT_AVAILABLE",
            "reason_code": "NO_CAPTURE_FOR_EXACT_SYMBOL_SESSION",
            "source_symbol": symbol,
            "target_session": target_session.isoformat(),
            "cutoff": cutoff.isoformat(),
            "candidates": [],
        }
    if len(candidates) > 1 and candidates[0][1] == candidates[1][1]:
        tied = [row for row in candidates if row[1] == candidates[0][1]]
        return {
            "status": "AMBIGUOUS",
            "reason_code": "LATEST_CAPTURE_TIME_TIED",
            "source_symbol": symbol,
            "target_session": target_session.isoformat(),
            "cutoff": cutoff.isoformat(),
            "candidates": [
                {
                    "capture_id": str(row[0]),
                    "captured_at": row[1].isoformat(),
                    "unit_contract_version": row[2],
                }
                for row in tied
            ],
        }
    capture_id, captured_at, contract_version = candidates[0]
    if captured_at.tzinfo is None or captured_at.utcoffset() is None:
        raise BaoStockDailyPgError(
            "CAPTURE_CLOCK_INVALID", "PG中BaoStock捕获时钟缺少时区"
        )
    return {
        "status": "AVAILABLE",
        "reason_code": None,
        "source_symbol": symbol,
        "target_session": target_session.isoformat(),
        "cutoff": cutoff.isoformat(),
        "capture_id": capture_id,
        "captured_at": captured_at,
        "unit_contract_version": contract_version,
        "capture_clock_role": _CAPTURE_CLOCK_ROLE,
        "price_coordinate": "RAW_UNADJUSTED",
        "adjustment_flag": "3",
        "selection_rule": "MAX_CAPTURED_AT_FOR_EXACT_SYMBOL_AND_SESSION",
    }


def read_baostock_daily_capture(
    config: Mapping[str, Any],
    *,
    capture_id: UUID,
    source_symbols: Sequence[str],
    start_session: date,
    end_session: date,
    cutoff: date,
    limit: int = 500,
    offset: int = 0,
) -> dict[str, Any]:
    """按精确捕获、证券和日期分页读取；不查捕获截止日后的行情。"""
    if not isinstance(config, Mapping) or not isinstance(capture_id, UUID):
        raise BaoStockDailyPgError(
            "READ_IDENTITY_INVALID", "读取必须绑定唯一配置与捕获UUID"
        )
    if (
        type(start_session) is not date
        or type(end_session) is not date
        or type(cutoff) is not date
        or start_session > end_session
        or end_session > cutoff
    ):
        raise BaoStockDailyPgError(
            "READ_RANGE_INVALID", "读取日期范围必须正序且不超过显式截止日"
        )
    if (
        type(limit) is not int
        or not 1 <= limit <= 5000
        or type(offset) is not int
        or offset < 0
    ):
        raise BaoStockDailyPgError("PAGE_INVALID", "读取分页须为1至5000行且偏移非负")
    symbols = tuple(normalize_baostock_symbol(value) for value in source_symbols)
    if (
        not symbols
        or any(value is None for value in symbols)
        or len(set(symbols)) != len(symbols)
    ):
        raise BaoStockDailyPgError(
            "SOURCE_SYMBOLS_INVALID", "读取须显式提供不重复沪深来源代码"
        )

    columns = [
        "capture_id",
        "source_code",
        "source_symbol",
        "session_date",
        "captured_at",
        "capture_clock_role",
        "adjustment_flag",
        "price_coordinate",
        "unit_contract_version",
        "returned_fields",
        "close_input_unit",
        "close_to_cny",
        "amount_input_unit",
        "amount_to_cny",
        "volume_input_unit",
        "volume_to_shares",
        "turn_input_unit",
    ]
    for name in _FIELD_NAMES:
        columns.extend((f"{name}_value", f"{name}_quality", f"{name}_reason"))
    select_sql = ", ".join(columns)
    try:
        with source_raw._transaction(config, write=False) as (
            _connection,
            cursor,
            _scope,
        ):
            cursor.execute(
                """SELECT count(*) FROM source_baostock_daily
WHERE capture_id=%s AND source_symbol=ANY(%s)
  AND session_date BETWEEN %s AND %s AND session_date<=%s""",
                (capture_id, list(symbols), start_session, end_session, cutoff),
            )
            total = int(cursor.fetchone()[0])
            cursor.execute(
                f"""SELECT {select_sql} FROM source_baostock_daily d
JOIN source_baostock_unit_contracts u USING (unit_contract_version)
WHERE capture_id=%s AND source_symbol=ANY(%s)
  AND session_date BETWEEN %s AND %s AND session_date<=%s
ORDER BY session_date, source_symbol COLLATE "C" LIMIT %s OFFSET %s""",
                (
                    capture_id,
                    list(symbols),
                    start_session,
                    end_session,
                    cutoff,
                    limit,
                    offset,
                ),
            )
            rows = cursor.fetchall()
    except Exception as error:
        if getattr(error, "preserve_transaction_error", False) is True:
            raise
        raise BaoStockDailyPgError(
            "PG_READ_FAILED", f"BaoStock来源事实读取失败（{type(error).__name__}）"
        ) from error

    output: list[dict[str, Any]] = []
    for row in rows:
        item = dict(zip(columns, row, strict=True))
        fields = {
            name: _read_field(
                item.pop(f"{name}_value"),
                item.pop(f"{name}_quality"),
                item.pop(f"{name}_reason"),
            )
            for name in _FIELD_NAMES
        }
        item["session_date"] = item["session_date"].isoformat()
        item["capture_id"] = str(item["capture_id"])
        item["returned_fields"] = list(item["returned_fields"])
        item["fields"] = fields
        output.append(item)
    return {
        "status": "AVAILABLE" if output else "NOT_COMPUTED",
        "capture_id": str(capture_id),
        "cutoff": cutoff.isoformat(),
        "page": {
            "offset": offset,
            "limit": limit,
            "count": len(output),
            "total": total,
            "next_offset": offset + len(output)
            if offset + len(output) < total
            else None,
        },
        "rows": output,
        "execution": {"source_network_called": False, "database_write": False},
    }


def read_baostock_symbol_session_coverage(
    config: Mapping[str, Any],
    *,
    target_sessions: Sequence[date],
    cutoff: date,
) -> dict[str, Any]:
    """按显式会话读回BaoStock已存证券、捕获UUID及逐捕获行数。"""
    if not isinstance(config, Mapping):
        raise BaoStockDailyPgError("RUNTIME_CONFIG_INVALID", "缺少唯一PG运行配置")
    if (
        not isinstance(target_sessions, Sequence)
        or isinstance(target_sessions, (str, bytes))
        or not target_sessions
        or type(cutoff) is not date
    ):
        raise BaoStockDailyPgError(
            "SESSION_PLAN_INVALID", "必须显式提供非空目标会话和截止日"
        )
    sessions = tuple(target_sessions)
    if (
        any(type(session) is not date or session > cutoff for session in sessions)
        or tuple(sorted(set(sessions))) != sessions
    ):
        raise BaoStockDailyPgError(
            "SESSION_PLAN_INVALID", "目标会话必须唯一、递增且不晚于显式截止日"
        )

    try:
        with source_raw._transaction(config, write=False) as (
            _connection,
            cursor,
            _scope,
        ):
            cursor.execute(
                """SELECT session_date, capture_id, source_symbol, count(*)
FROM stock.source_baostock_daily
WHERE session_date = ANY(%s) AND session_date <= %s
GROUP BY session_date, capture_id, source_symbol
ORDER BY session_date, capture_id::text, source_symbol COLLATE "C""",
                (list(sessions), cutoff),
            )
            rows = cursor.fetchall()
    except Exception as error:
        if getattr(error, "preserve_transaction_error", False) is True:
            raise
        raise BaoStockDailyPgError(
            "PG_READ_FAILED", f"BaoStock会话覆盖读回失败（{type(error).__name__}）"
        ) from error

    by_session: dict[date, dict[str, Any]] = {
        session: {"row_count": 0, "source_symbols": set(), "captures": {}}
        for session in sessions
    }
    for session, capture_id, source_symbol, row_count in rows:
        if (
            type(session) is not date
            or session not in by_session
            or session > cutoff
            or not isinstance(capture_id, UUID)
            or normalize_baostock_symbol(source_symbol) != source_symbol
            or type(row_count) is not int
            or row_count < 1
        ):
            raise BaoStockDailyPgError(
                "PG_READ_INVALID", "BaoStock会话覆盖读回包含非法日期、身份或行数"
            )
        item = by_session[session]
        item["row_count"] += row_count
        item["source_symbols"].add(source_symbol)
        capture = item["captures"].setdefault(
            capture_id, {"row_count": 0, "source_symbols": set()}
        )
        capture["row_count"] += row_count
        capture["source_symbols"].add(source_symbol)

    output_sessions: list[dict[str, Any]] = []
    for session in sessions:
        item = by_session[session]
        captures = [
            {
                "capture_id": str(capture_id),
                "row_count": capture["row_count"],
                "source_symbols": sorted(capture["source_symbols"]),
            }
            for capture_id, capture in sorted(
                item["captures"].items(), key=lambda entry: str(entry[0])
            )
        ]
        output_sessions.append(
            {
                "session_date": session.isoformat(),
                "status": "AVAILABLE" if item["row_count"] else "NOT_COMPUTED",
                "row_count": item["row_count"],
                "source_symbols": sorted(item["source_symbols"]),
                "source_capture_ids": [capture["capture_id"] for capture in captures],
                "captures": captures,
            }
        )
    return {
        "status": (
            "AVAILABLE"
            if any(item["row_count"] for item in by_session.values())
            else "NOT_COMPUTED"
        ),
        "cutoff": cutoff.isoformat(),
        "target_sessions": [session.isoformat() for session in sessions],
        "sessions": output_sessions,
        "execution": {"source_network_called": False, "database_write": False},
    }
