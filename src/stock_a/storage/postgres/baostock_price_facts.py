"""BaoStock双坐标日线、捕获请求及代码名称的唯一PostgreSQL读写边界。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from stock_a.domain import FieldValue
from stock_a.sources.baostock.daily import BaoUnitContract
from stock_a.sources.baostock.identity import (
    classify_baostock_board,
    normalize_baostock_symbol,
)

from . import source_raw
from .typed import _finite_decimal


RAW_COORDINATE = "RAW_UNADJUSTED"
QFQ_COORDINATE = "FORWARD_ADJUSTED"
_COORDINATE_FLAGS = {RAW_COORDINATE: "3", QFQ_COORDINATE: "2"}
_NON_PIT_CLOCK_ROLE = "BAOSTOCK_QUERY_CAPTURED_NON_PIT"
_NAME_PARSER_CONTRACT = "BAOSTOCK_QUERY_STOCK_BASIC/R1"
_NAME_SOURCE_FILE = "BAOSTOCK_QUERY_STOCK_BASIC"
_PRICE_FIELD_QUALITY_MIGRATION_ID = 66
_PRICE_COLUMNS = (
    "source_symbol",
    "market",
    "board",
    "session_date",
    "source_capture_id",
    "bar_status",
    "bar_reason",
    "raw_open",
    "raw_high",
    "raw_low",
    "raw_close",
    "raw_open_quality",
    "raw_open_reason",
    "raw_high_quality",
    "raw_high_reason",
    "raw_low_quality",
    "raw_low_reason",
    "raw_close_quality",
    "raw_close_reason",
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
    "observed_started_at",
    "observed_completed_at",
    "source_clock_role",
    "source_available_at",
    "source_code",
    "returned_fields",
    "unit_contract_version",
    "raw_preclose",
    "raw_preclose_quality",
    "raw_preclose_reason",
    "raw_response_session_date",
    "raw_response_source_code",
    "raw_adjustflag",
    "raw_response_fields",
    "qfq_open",
    "qfq_high",
    "qfq_low",
    "qfq_close",
    "qfq_open_quality",
    "qfq_open_reason",
    "qfq_high_quality",
    "qfq_high_reason",
    "qfq_low_quality",
    "qfq_low_reason",
    "qfq_close_quality",
    "qfq_close_reason",
    "qfq_price_quality",
    "qfq_price_reason",
    "qfq_response_session_date",
    "qfq_response_source_code",
    "qfq_adjustflag",
    "qfq_response_fields",
    "qfq_bar_status",
    "qfq_bar_reason",
    "qfq_preclose",
    "qfq_preclose_quality",
    "qfq_preclose_reason",
    "qfq_volume",
    "qfq_volume_quality",
    "qfq_volume_reason",
    "qfq_volume_unit_status",
    "qfq_amount",
    "qfq_amount_quality",
    "qfq_amount_reason",
    "qfq_amount_unit_status",
    "qfq_turn_value",
    "qfq_turn_quality",
    "qfq_turn_reason",
    "qfq_tradestatus_value",
    "qfq_tradestatus_quality",
    "qfq_tradestatus_reason",
    "qfq_is_st_value",
    "qfq_is_st_quality",
    "qfq_is_st_reason",
    "raw_turn_value",
    "raw_turn_quality",
    "raw_turn_reason",
    "raw_tradestatus_value",
    "raw_tradestatus_quality",
    "raw_tradestatus_reason",
    "raw_is_st_value",
    "raw_is_st_quality",
    "raw_is_st_reason",
)
_RAW_PRICE_COLUMNS = (
    "raw_open",
    "raw_high",
    "raw_low",
    "raw_close",
    "raw_open_quality",
    "raw_open_reason",
    "raw_high_quality",
    "raw_high_reason",
    "raw_low_quality",
    "raw_low_reason",
    "raw_close_quality",
    "raw_close_reason",
    "price_quality",
    "price_reason",
    "raw_preclose",
    "raw_preclose_quality",
    "raw_preclose_reason",
)
_RAW_RESPONSE_COLUMNS = (
    "raw_response_session_date",
    "raw_response_source_code",
    "raw_adjustflag",
    "raw_response_fields",
)
_QFQ_PRICE_COLUMNS = (
    "qfq_open",
    "qfq_high",
    "qfq_low",
    "qfq_close",
    "qfq_open_quality",
    "qfq_open_reason",
    "qfq_high_quality",
    "qfq_high_reason",
    "qfq_low_quality",
    "qfq_low_reason",
    "qfq_close_quality",
    "qfq_close_reason",
    "qfq_price_quality",
    "qfq_price_reason",
)
_QFQ_RESPONSE_COLUMNS = (
    "qfq_response_session_date",
    "qfq_response_source_code",
    "qfq_adjustflag",
    "qfq_response_fields",
    "qfq_bar_status",
    "qfq_bar_reason",
    "qfq_preclose",
    "qfq_preclose_quality",
    "qfq_preclose_reason",
    "qfq_volume",
    "qfq_volume_quality",
    "qfq_volume_reason",
    "qfq_volume_unit_status",
    "qfq_amount",
    "qfq_amount_quality",
    "qfq_amount_reason",
    "qfq_amount_unit_status",
    "qfq_turn_value",
    "qfq_turn_quality",
    "qfq_turn_reason",
    "qfq_tradestatus_value",
    "qfq_tradestatus_quality",
    "qfq_tradestatus_reason",
    "qfq_is_st_value",
    "qfq_is_st_quality",
    "qfq_is_st_reason",
)
_RAW_ACTIVITY_COLUMNS = (
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
    "raw_turn_value",
    "raw_turn_quality",
    "raw_turn_reason",
    "raw_tradestatus_value",
    "raw_tradestatus_quality",
    "raw_tradestatus_reason",
    "raw_is_st_value",
    "raw_is_st_quality",
    "raw_is_st_reason",
)
_QUALITY_MAP = {
    "OBSERVED": "OBSERVED",
    "UNKNOWN": "UNKNOWN",
    "UNKNOWN_DATA": "UNKNOWN",
    "NOT_APPLICABLE": "NOT_APPLICABLE",
    "BAD_VALUE": "BAD_VALUE",
}


class BaoStockPriceFactsError(Exception):
    """BaoStock双坐标事实写入或读取合同错误。"""

    preserve_transaction_error = True

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def _require_price_field_quality_migration(cursor: Any) -> None:
    cursor.execute(
        "SELECT EXISTS (SELECT 1 FROM migration_ledger "
        "WHERE migration_id=%s AND lineage='STOCK_A_INITIAL_R1')",
        (_PRICE_FIELD_QUALITY_MIGRATION_ID,),
    )
    if cursor.fetchone() != (True,):
        raise BaoStockPriceFactsError(
            "PRICE_FIELD_QUALITY_MIGRATION_REQUIRED",
            "0066字段级价格质量迁移未应用，拒绝读写新价格投影",
        )


def _aware(value: Any, label: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise BaoStockPriceFactsError(
            "CAPTURE_CLOCK_INVALID", f"{label}必须为带时区时刻"
        )
    return value


def _date_list(values: Sequence[date], *, label: str) -> tuple[date, ...]:
    if (
        isinstance(values, (str, bytes))
        or not isinstance(values, Sequence)
        or not values
        or any(type(value) is not date for value in values)
        or tuple(sorted(set(values))) != tuple(values)
    ):
        raise BaoStockPriceFactsError(
            "PRICE_READ_SESSION_SCOPE_INVALID",
            f"{label}必须为非空、递增且无重复的官方会话列表",
        )
    return tuple(values)


def _get(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _unit_values(unit_contract: BaoUnitContract) -> tuple[Any, ...]:
    if not isinstance(unit_contract, BaoUnitContract):
        raise BaoStockPriceFactsError(
            "UNIT_CONTRACT_INVALID", "缺少BaoStock显式单位合同"
        )
    if (
        not unit_contract.version
        or unit_contract.version != unit_contract.version.strip()
        or len(unit_contract.version) > 128
        or unit_contract.turn_input_unit != "PERCENT"
    ):
        raise BaoStockPriceFactsError(
            "UNIT_CONTRACT_INVALID", "BaoStock单位合同字段无效"
        )
    try:
        close_to_cny = _finite_decimal(unit_contract.close_to_cny)
        amount_to_cny = _finite_decimal(unit_contract.amount_to_cny)
        volume_to_shares = _finite_decimal(unit_contract.volume_to_shares)
    except ValueError as error:
        raise BaoStockPriceFactsError(
            "UNIT_CONTRACT_NOT_DECIMAL",
            "BaoStock单位倍率必须可无损表示为PostgreSQL numeric",
        ) from error
    if min(close_to_cny, amount_to_cny, volume_to_shares) <= 0:
        raise BaoStockPriceFactsError(
            "UNIT_CONTRACT_INVALID", "BaoStock单位倍率必须为正数"
        )
    text_units = (
        unit_contract.close_input_unit,
        unit_contract.amount_input_unit,
        unit_contract.volume_input_unit,
    )
    if any(
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or value == "LEGACY_UNIT_NOT_RECORDED"
        or len(value) > 64
        for value in text_units
    ):
        raise BaoStockPriceFactsError(
            "UNIT_CONTRACT_INVALID", "BaoStock输入单位必须显式登记"
        )
    return (
        unit_contract.version,
        unit_contract.close_input_unit,
        close_to_cny,
        unit_contract.amount_input_unit,
        amount_to_cny,
        unit_contract.volume_input_unit,
        volume_to_shares,
        unit_contract.turn_input_unit,
    )


def _ensure_unit_contract(cursor: Any, unit_contract: BaoUnitContract) -> None:
    values = _unit_values(unit_contract)
    cursor.execute(
        """INSERT INTO stock.source_baostock_unit_contracts
             (unit_contract_version,close_input_unit,close_to_cny,
              amount_input_unit,amount_to_cny,volume_input_unit,
              volume_to_shares,turn_input_unit)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT (unit_contract_version) DO NOTHING""",
        values,
    )
    cursor.execute(
        """SELECT unit_contract_version,close_input_unit,close_to_cny,
                  amount_input_unit,amount_to_cny,volume_input_unit,
                  volume_to_shares,turn_input_unit
           FROM stock.source_baostock_unit_contracts
           WHERE unit_contract_version=%s FOR SHARE""",
        (unit_contract.version,),
    )
    if cursor.fetchone() != values:
        raise BaoStockPriceFactsError(
            "UNIT_CONTRACT_CONFLICT", "同一BaoStock单位合同版本对应不同单位或倍率"
        )


def begin_baostock_price_capture(
    config: Mapping[str, Any],
    *,
    capture_uuid: UUID,
    range_start_session: date,
    range_end_session: date,
    price_coordinates: Sequence[str],
    requested_symbol_count: int,
    started_at: datetime,
    parser_contract: str,
    unit_contract: BaoUnitContract,
) -> int:
    """登记一个可由RAW与QFQ请求共同续跑的真实BaoStock捕获。"""
    if (
        not isinstance(capture_uuid, UUID)
        or type(range_start_session) is not date
        or type(range_end_session) is not date
        or range_start_session > range_end_session
        or type(requested_symbol_count) is not int
        or requested_symbol_count < 1
        or not isinstance(parser_contract, str)
        or not parser_contract.strip()
        or parser_contract != parser_contract.strip()
        or len(parser_contract) > 512
    ):
        raise BaoStockPriceFactsError(
            "CAPTURE_INPUT_INVALID", "BaoStock捕获身份字段无效"
        )
    started = _aware(started_at, "采集开始时间")
    if isinstance(price_coordinates, (str, bytes)) or not isinstance(
        price_coordinates, Sequence
    ):
        raise BaoStockPriceFactsError(
            "PRICE_COORDINATES_INVALID", "价格坐标必须是明确列表"
        )
    coordinates = tuple(price_coordinates)
    if set(coordinates) != {RAW_COORDINATE, QFQ_COORDINATE} or len(coordinates) != 2:
        raise BaoStockPriceFactsError(
            "PRICE_COORDINATES_INVALID",
            "价格捕获必须配对RAW_UNADJUSTED与FORWARD_ADJUSTED",
        )
    coordinates = (RAW_COORDINATE, QFQ_COORDINATE)

    with source_raw._transaction(config, write=True, statement_timeout_ms=30000) as (
        _connection,
        cursor,
        _scope,
    ):
        _ensure_unit_contract(cursor, unit_contract)
        cursor.execute(
            """SELECT capture_id,provider,capture_uuid,range_start_session,
                      range_end_session,price_coordinates,parser_contract,
                      requested_symbol_count,unit_contract_version
               FROM stock.source_captures WHERE capture_uuid=%s FOR UPDATE""",
            (capture_uuid,),
        )
        existing = cursor.fetchone()
        identity = (
            "BAOSTOCK",
            capture_uuid,
            range_start_session,
            range_end_session,
            list(coordinates),
            parser_contract,
            requested_symbol_count,
            unit_contract.version,
        )
        if existing is not None:
            existing_identity = (
                existing[1],
                existing[2],
                existing[3],
                existing[4],
                list(existing[5] or ()),
                existing[6],
                existing[7],
                existing[8],
            )
            if existing_identity != identity:
                raise BaoStockPriceFactsError(
                    "CAPTURE_UUID_CONFLICT",
                    "同一捕获UUID对应不同的提供方、会话范围、坐标、单位合同或证券总体",
                )
            return int(existing[0])

        cursor.execute(
            """INSERT INTO stock.source_captures
                 (provider,capture_uuid,range_start_session,range_end_session,
                  price_coordinates,parser_contract,read_started_at,validated_at,
                  source_available_at,source_time_quality,source_clock_role,
                  normalized_count,requested_symbol_count,unit_contract_version)
               VALUES ('BAOSTOCK',%s,%s,%s,%s,%s,%s,%s,NULL,
                       'NON_PIT_OBSERVED',%s,0,%s,%s)
               ON CONFLICT (capture_uuid) WHERE capture_uuid IS NOT NULL DO NOTHING
               RETURNING capture_id""",
            (
                capture_uuid,
                range_start_session,
                range_end_session,
                list(coordinates),
                parser_contract,
                started,
                started,
                _NON_PIT_CLOCK_ROLE,
                requested_symbol_count,
                unit_contract.version,
            ),
        )
        inserted = cursor.fetchone()
        if inserted is not None:
            return int(inserted[0])
        cursor.execute(
            """SELECT capture_id,provider,capture_uuid,range_start_session,
                      range_end_session,price_coordinates,parser_contract,
                      requested_symbol_count,unit_contract_version
               FROM stock.source_captures WHERE capture_uuid=%s FOR UPDATE""",
            (capture_uuid,),
        )
        existing = cursor.fetchone()
        if existing is None:
            raise BaoStockPriceFactsError(
                "CAPTURE_INSERT_READBACK_MISSING", "来源捕获写入冲突后无法读回捕获UUID"
            )
        existing_identity = (
            existing[1],
            existing[2],
            existing[3],
            existing[4],
            list(existing[5] or ()),
            existing[6],
            existing[7],
            existing[8],
        )
        if existing_identity != identity:
            raise BaoStockPriceFactsError(
                "CAPTURE_UUID_CONFLICT", "捕获UUID已绑定到不同的BaoStock输入范围或合同"
            )
        return int(existing[0])


def read_baostock_capture_roster(
    config: Mapping[str, Any],
    *,
    source_capture_id: int,
    cutoff: date,
) -> dict[str, Any]:
    """读取可续跑的paired capture证券总体、双坐标请求和字段schema。"""
    if (
        type(source_capture_id) is not int
        or source_capture_id <= 0
        or type(cutoff) is not date
    ):
        raise BaoStockPriceFactsError(
            "CAPTURE_ROSTER_SCOPE_INVALID", "捕获续跑目录必须绑定capture与截止日"
        )
    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _connection,
        cursor,
        _scope,
    ):
        cursor.execute(
            """SELECT capture_id,capture_uuid,provider,range_start_session,
                      range_end_session,price_coordinates,parser_contract,
                      requested_symbol_count,unit_contract_version,normalized_count,
                      read_started_at,validated_at,source_time_quality,source_clock_role
               FROM stock.source_captures
               WHERE capture_id=%s AND provider='BAOSTOCK'
                 AND range_end_session<=%s""",
            (source_capture_id, cutoff),
        )
        capture = cursor.fetchone()
        if capture is None:
            return {
                "status": "NOT_FOUND",
                "source_capture_id": source_capture_id,
                "cutoff": cutoff.isoformat(),
                "symbols": [],
                "database_write_attempted": False,
                "source_io_attempted": False,
            }
        (
            capture_id,
            capture_uuid,
            provider,
            range_start,
            range_end,
            coordinates,
            parser_contract,
            requested_symbol_count,
            unit_contract_version,
            normalized_count,
            read_started_at,
            validated_at,
            source_time_quality,
            source_clock_role,
        ) = capture
        if (
            type(capture_uuid) is not UUID
            or type(range_start) is not date
            or type(range_end) is not date
            or range_start > range_end
            or len(tuple(coordinates or ())) != 2
            or set(coordinates or ()) != {RAW_COORDINATE, QFQ_COORDINATE}
            or type(requested_symbol_count) is not int
            or requested_symbol_count <= 0
            or not isinstance(unit_contract_version, str)
            or source_time_quality != "NON_PIT_OBSERVED"
            or source_clock_role != _NON_PIT_CLOCK_ROLE
        ):
            raise BaoStockPriceFactsError(
                "CAPTURE_ROSTER_IDENTITY_INVALID",
                "paired BaoStock capture身份或NON_PIT时钟无效",
            )
        cursor.execute(
            """SELECT source_code,adjustment_flag,eligible_start_session,
                      eligible_end_session,request_status,attempt_count,
                      completed_through_session,response_row_count,
                      last_attempt_started_at,last_attempt_completed_at,retry_after,
                      last_error_code,last_error_message,returned_fields
               FROM stock.source_capture_requests
               WHERE capture_id=%s AND range_end_session<=%s
               ORDER BY source_code COLLATE "C",adjustment_flag""",
            (source_capture_id, cutoff),
        )
        names = [column.name for column in cursor.description]
        request_rows = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
    by_symbol: dict[str, dict[str, dict[str, Any]]] = {}
    for row in request_rows:
        symbol = str(row["source_code"])
        flag = str(row["adjustment_flag"])
        coordinate = (
            RAW_COORDINATE if flag == "3" else QFQ_COORDINATE if flag == "2" else None
        )
        if coordinate is None or coordinate in by_symbol.setdefault(symbol, {}):
            raise BaoStockPriceFactsError(
                "CAPTURE_ROSTER_DUPLICATE_COORDINATE",
                f"捕获{symbol}的复权请求行重复或无效",
            )
        request = {
            "price_coordinate": coordinate,
            "adjustment_flag": flag,
            "eligible_start_session": row["eligible_start_session"].isoformat(),
            "eligible_end_session": row["eligible_end_session"].isoformat(),
            "request_status": row["request_status"],
            "attempt_count": int(row["attempt_count"]),
            "completed_through_session": (
                row["completed_through_session"].isoformat()
                if row["completed_through_session"]
                else None
            ),
            "response_row_count": int(row["response_row_count"]),
            "last_attempt_started_at": (
                row["last_attempt_started_at"].isoformat()
                if row["last_attempt_started_at"]
                else None
            ),
            "last_attempt_completed_at": (
                row["last_attempt_completed_at"].isoformat()
                if row["last_attempt_completed_at"]
                else None
            ),
            "retry_after": row["retry_after"].isoformat()
            if row["retry_after"]
            else None,
            "last_error_code": row["last_error_code"],
            "last_error_message": row["last_error_message"],
            "returned_fields": list(row["returned_fields"] or ()),
        }
        by_symbol[symbol][coordinate] = request
    symbols = []
    for source_code in sorted(by_symbol):
        coordinates_for_code = by_symbol[source_code]
        raw = coordinates_for_code.get(RAW_COORDINATE)
        qfq = coordinates_for_code.get(QFQ_COORDINATE)
        windows_match = bool(
            raw
            and qfq
            and raw["eligible_start_session"] == qfq["eligible_start_session"]
            and raw["eligible_end_session"] == qfq["eligible_end_session"]
        )
        symbols.append(
            {
                "source_code": source_code,
                "source_symbol": normalize_baostock_symbol(source_code),
                "eligible_start_session": raw["eligible_start_session"]
                if raw
                else None,
                "eligible_end_session": raw["eligible_end_session"] if raw else None,
                "eligible_windows_match": windows_match,
                "request_complete": windows_match
                and all(
                    _attempts_complete(request["request_status"])
                    and (
                        request["request_status"] == "NO_DATA"
                        or (
                            request["completed_through_session"] is not None
                            and request["completed_through_session"]
                            >= request["eligible_end_session"]
                        )
                    )
                    for request in (raw, qfq)
                ),
                "coordinates": coordinates_for_code,
            }
        )
    if len(symbols) > requested_symbol_count:
        raise BaoStockPriceFactsError(
            "CAPTURE_ROSTER_COUNT_INVALID", "已存BaoStock请求证券数超过捕获总体"
        )
    registration_complete = len(symbols) == requested_symbol_count
    return {
        "status": "AVAILABLE" if registration_complete else "INCOMPLETE",
        "source_capture_id": int(capture_id),
        "capture_uuid": str(capture_uuid),
        "provider": provider,
        "range_start_session": range_start.isoformat(),
        "range_end_session": range_end.isoformat(),
        "price_coordinates": [RAW_COORDINATE, QFQ_COORDINATE],
        "parser_contract": parser_contract,
        "requested_symbol_count": requested_symbol_count,
        "registration_complete": registration_complete,
        "source_capture_unit_contract_version": unit_contract_version,
        "normalized_row_count": int(normalized_count),
        "captured_at_start": read_started_at.isoformat(),
        "captured_at_validated": validated_at.isoformat(),
        "source_time_quality": source_time_quality,
        "source_clock_role": source_clock_role,
        "symbols": symbols,
        "database_write_attempted": False,
        "source_io_attempted": False,
    }


def _normalized_code(value: Any) -> tuple[str, str]:
    symbol = normalize_baostock_symbol(value)
    if symbol is None:
        raise BaoStockPriceFactsError(
            "SOURCE_CODE_INVALID", "BaoStock来源代码无法规范化"
        )
    return symbol.lower(), symbol


def read_baostock_raw_repair_symbols(
    config: Mapping[str, Any],
    *,
    source_capture_id: int,
    cutoff: date,
) -> tuple[str, ...]:
    """列出成功RAW请求已返回数据、但此前价格行缺少对应RAW Bar的代码。"""
    if (
        type(source_capture_id) is not int
        or source_capture_id <= 0
        or type(cutoff) is not date
    ):
        raise BaoStockPriceFactsError(
            "RAW_REPAIR_SCOPE_INVALID", "RAW档案修复必须绑定正capture与显式截止日"
        )
    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _connection,
        cursor,
        _scope,
    ):
        cursor.execute(
            """SELECT DISTINCT price.source_symbol
               FROM stock.source_raw_daily price
               JOIN stock.source_capture_requests request
                 ON request.capture_id=price.source_capture_id
                AND request.source_code=price.source_code
                AND request.adjustment_flag='3'
               WHERE price.source_capture_id=%s
                 AND price.session_date<=%s
                 AND request.request_status IN ('COMPLETED','NO_DATA')
                 AND request.response_row_count>0
                 AND price.bar_status='TARGET_MISSING'
                 AND price.bar_reason='RAW_BAR_NOT_RETURNED'
               ORDER BY price.source_symbol""",
            (source_capture_id, cutoff),
        )
        return tuple(str(row[0]) for row in cursor.fetchall())


def _eligible_windows(
    eligible_sessions_by_symbol: Mapping[str, Any],
    *,
    capture_start: date,
    capture_end: date,
) -> dict[str, tuple[str, tuple[date, ...]]]:
    if not isinstance(eligible_sessions_by_symbol, Mapping):
        raise BaoStockPriceFactsError(
            "ELIGIBLE_SESSIONS_INVALID", "缺少按证券登记的有效官方会话窗口"
        )
    result: dict[str, tuple[str, tuple[date, ...]]] = {}
    for symbol_key, value in eligible_sessions_by_symbol.items():
        source_code, source_symbol = _normalized_code(symbol_key)
        if isinstance(value, Mapping):
            value = value.get("eligible_sessions", value.get("sessions", ()))
        if value is None:
            value = ()
        if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
            raise BaoStockPriceFactsError(
                "ELIGIBLE_SESSIONS_INVALID", f"证券{source_symbol}的有效会话不是列表"
            )
        sessions = tuple(value)
        if (
            any(type(item) is not date for item in sessions)
            or tuple(sorted(set(sessions))) != sessions
            or any(item < capture_start or item > capture_end for item in sessions)
        ):
            raise BaoStockPriceFactsError(
                "ELIGIBLE_SESSIONS_INVALID",
                f"证券{source_symbol}的有效会话越出捕获范围",
            )
        if not sessions:
            continue
        if source_code in result:
            raise BaoStockPriceFactsError(
                "ELIGIBLE_SESSIONS_DUPLICATE",
                f"证券{source_symbol}重复登记有效会话窗口",
            )
        result[source_code] = (source_symbol, sessions)
    return result


def _coordinate_key(value: Any) -> str | None:
    if value in ("3", 3, RAW_COORDINATE):
        return RAW_COORDINATE
    if value in ("2", 2, QFQ_COORDINATE):
        return QFQ_COORDINATE
    return None


def _normalize_request_attempts(
    request_attempts: Any,
    *,
    eligible_codes: Mapping[str, tuple[str, tuple[date, ...]]],
) -> dict[tuple[str, str], dict[str, Any]]:
    """接受代码×adjustflag请求行或坐标×代码尝试映射。"""
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    pending: list[tuple[Any, Any]] = []
    if isinstance(request_attempts, Mapping):
        for outer_key, outer_value in request_attempts.items():
            outer_coordinate = _coordinate_key(outer_key)
            if outer_coordinate is not None and isinstance(outer_value, Mapping):
                for source_code, attempt in outer_value.items():
                    pending.append(
                        (
                            {
                                "source_code": source_code,
                                "adjustment_flag": _COORDINATE_FLAGS[outer_coordinate],
                            },
                            attempt,
                        )
                    )
            elif isinstance(outer_key, tuple) and len(outer_key) == 2:
                pending.append(
                    (
                        {"source_code": outer_key[0], "adjustment_flag": outer_key[1]},
                        outer_value,
                    )
                )
            elif (
                isinstance(outer_value, Mapping)
                and _get(outer_value, "source_code") is not None
            ):
                pending.append((outer_value, outer_value))
    elif isinstance(request_attempts, Sequence) and not isinstance(
        request_attempts, (str, bytes)
    ):
        pending.extend((row, row) for row in request_attempts)
    else:
        raise BaoStockPriceFactsError(
            "REQUEST_ATTEMPTS_INVALID", "来源请求尝试必须为代码与坐标行映射"
        )

    for identity, attempt in pending:
        raw_code = _get(
            identity,
            "source_code",
            _get(identity, "requested_code", _get(identity, "source_symbol")),
        )
        if raw_code is None:
            raw_code = _get(
                attempt,
                "source_code",
                _get(attempt, "requested_code", _get(attempt, "source_symbol")),
            )
        adjustment_flag = _get(identity, "adjustment_flag")
        if adjustment_flag is None:
            adjustment_flag = _get(attempt, "adjustment_flag")
        coordinate = _coordinate_key(adjustment_flag)
        if raw_code is None or coordinate is None:
            raise BaoStockPriceFactsError(
                "REQUEST_ATTEMPTS_INVALID", "请求尝试缺少来源代码或adjustflag 3/2"
            )
        source_code, source_symbol = _normalized_code(raw_code)
        if source_code not in eligible_codes:
            raise BaoStockPriceFactsError(
                "REQUEST_SYMBOL_NOT_ELIGIBLE", f"请求证券{source_symbol}未登记有效窗口"
            )
        key = (source_code, _COORDINATE_FLAGS[coordinate])
        if key in rows:
            raise BaoStockPriceFactsError(
                "REQUEST_ATTEMPTS_DUPLICATE", f"证券{source_symbol}的复权请求重复"
            )
        status = _get(
            attempt, "request_status", _get(attempt, "status", "NOT_ATTEMPTED")
        )
        attempt_count = _get(attempt, "attempt_count", 0)
        response_count = _get(
            attempt, "response_row_count", _get(attempt, "rows_received", 0)
        )
        if type(attempt_count) is not int or attempt_count < 0:
            raise BaoStockPriceFactsError(
                "REQUEST_ATTEMPTS_INVALID", "请求尝试次数无效"
            )
        if type(response_count) is not int or response_count < 0:
            raise BaoStockPriceFactsError(
                "REQUEST_ATTEMPTS_INVALID", "请求响应行数无效"
            )
        status_text = str(status)
        complete = _get(attempt, "complete", True)
        if status_text in {"COMPLETED", "NO_DATA"}:
            db_status = status_text
        elif status_text == "RESPONSE_RECEIVED" and complete is True:
            db_status = "NO_DATA" if response_count == 0 else "COMPLETED"
        elif (
            status_text in {"NOT_ATTEMPTED", "NOT_ATTEMPTED_BUDGET"}
            or attempt_count == 0
        ):
            db_status = "PENDING"
        elif status_text in {"RESPONSE_REJECTED", "INVALID_RESPONSE"}:
            db_status = "PERMANENT_FAILURE"
        else:
            db_status = "RETRYABLE_FAILURE"
        error_code = _get(attempt, "last_error_code", _get(attempt, "local_error"))
        if db_status in {"RETRYABLE_FAILURE", "PERMANENT_FAILURE"} and not error_code:
            error_code = status_text or db_status
        rows[key] = {
            "source_code": source_code,
            "source_symbol": source_symbol,
            "adjustment_flag": key[1],
            "request_status": db_status,
            "attempt_count": attempt_count,
            "response_row_count": response_count,
            "last_attempt_started_at": _get(
                attempt, "last_attempt_started_at", _get(attempt, "request_started_at")
            ),
            "last_attempt_completed_at": _get(
                attempt,
                "last_attempt_completed_at",
                _get(attempt, "request_completed_at"),
            ),
            "retry_after": _get(attempt, "retry_after"),
            "last_error_code": str(error_code)[:256] if error_code else None,
            "last_error_message": str(
                _get(attempt, "last_error_message", _get(attempt, "local_error", ""))
            )[:1000]
            or None,
            "returned_fields": _coordinate_schema(identity, attempt, coordinate),
        }

    for source_code, (source_symbol, _sessions) in eligible_codes.items():
        for adjustment_flag in ("3", "2"):
            rows.setdefault(
                (source_code, adjustment_flag),
                {
                    "source_code": source_code,
                    "source_symbol": source_symbol,
                    "adjustment_flag": adjustment_flag,
                    "request_status": "PENDING",
                    "attempt_count": 0,
                    "response_row_count": 0,
                    "last_attempt_started_at": None,
                    "last_attempt_completed_at": None,
                    "retry_after": None,
                    "last_error_code": None,
                    "last_error_message": None,
                    "returned_fields": None,
                },
            )
    return rows


def _parse_optional_clock(value: Any, *, label: str) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as error:
            raise BaoStockPriceFactsError(
                "REQUEST_CLOCK_INVALID", f"{label}格式无效"
            ) from error
    return _aware(value, label)


def _field_value(
    value: Any,
    *,
    label: str,
    numeric: bool,
    default_reason: str,
    integer: bool = False,
) -> tuple[Any, str, str | None]:
    if not isinstance(value, FieldValue):
        raise BaoStockPriceFactsError("FIELD_VALUE_INVALID", f"{label}必须为FieldValue")
    quality = getattr(value, "quality", None)
    if quality not in _QUALITY_MAP:
        raise BaoStockPriceFactsError(
            "FIELD_QUALITY_INVALID", "BaoStock字段质量不在合同枚举"
        )
    quality = _QUALITY_MAP[quality]
    if quality == "OBSERVED":
        if value.value is None or value.reason is not None:
            raise BaoStockPriceFactsError(
                "FIELD_VALUE_INVALID", f"{label}观察值缺失或含错误原因"
            )
        if numeric:
            try:
                value_out = _finite_decimal(value.value)
            except ValueError as error:
                raise BaoStockPriceFactsError(
                    "FIELD_VALUE_NOT_FINITE", f"{label}不能无损保存为PostgreSQL numeric"
                ) from error
            if integer and value_out != value_out.to_integral_value():
                raise BaoStockPriceFactsError(
                    "FIELD_VALUE_NOT_INTEGER", f"{label}必须为整数股数"
                )
            if integer:
                if value_out < 0 or value_out > 9223372036854775807:
                    raise BaoStockPriceFactsError(
                        "FIELD_VALUE_OUT_OF_RANGE", f"{label}超出PostgreSQL bigint范围"
                    )
                value_out = int(value_out)
        else:
            if isinstance(value.value, bool) or not isinstance(value.value, (str, int)):
                raise BaoStockPriceFactsError(
                    "FIELD_STATUS_INVALID", f"{label}状态值类型无效"
                )
            value_out = str(value.value)
        return value_out, quality, None
    if value.value is not None:
        raise BaoStockPriceFactsError(
            "FIELD_VALUE_INVALID", f"{label}未知或不适用时必须为空"
        )
    reason = (
        value.reason
        if isinstance(value.reason, str) and value.reason.strip()
        else default_reason
    )
    return None, quality, reason[:1000]


def _field_reason(fields: Sequence[tuple[str, str | None]]) -> str:
    values = [f"{name}:{reason}" for name, reason in fields if reason]
    return ";".join(values)[:1000] if values else "字段质量不完整"


def _price_group(
    observation: Any,
    *,
    prefix: str,
    unavailable_reason: str,
) -> dict[str, Any]:
    names = ("open", "high", "low", "close")
    values: dict[str, Any] = {}
    field_columns: dict[str, Any] = {}
    qualities: list[str] = []
    reasons: list[tuple[str, str | None]] = []
    for name in names:
        field = _get(observation, name) if observation is not None else None
        if field is None:
            values[name] = None
            qualities.append("UNKNOWN")
            reasons.append((name, unavailable_reason))
            field_columns[f"{prefix}_{name}"] = None
            field_columns[f"{prefix}_{name}_quality"] = "UNKNOWN"
            field_columns[f"{prefix}_{name}_reason"] = unavailable_reason
            continue
        value, quality, reason = _field_value(
            field,
            label=f"{prefix}_{name}",
            numeric=True,
            default_reason=unavailable_reason,
        )
        if quality == "OBSERVED" and value <= 0:
            raise BaoStockPriceFactsError(
                "PRICE_VALUE_INVALID", f"{prefix}_{name}必须为正数"
            )
        values[name] = value
        qualities.append(quality)
        reasons.append((name, reason))
        field_columns[f"{prefix}_{name}"] = value
        field_columns[f"{prefix}_{name}_quality"] = quality
        field_columns[f"{prefix}_{name}_reason"] = reason
    quality_column = "qfq_price_quality" if prefix == "qfq" else "price_quality"
    reason_column = "qfq_price_reason" if prefix == "qfq" else "price_reason"
    known_values = {
        name: values[name]
        for name, quality in zip(names, qualities, strict=True)
        if quality == "OBSERVED"
    }
    high_value = known_values.get("high")
    low_value = known_values.get("low")
    if (
        high_value is not None
        and any(value > high_value for value in known_values.values())
    ) or (
        low_value is not None
        and any(value < low_value for value in known_values.values())
    ):
        raise BaoStockPriceFactsError("PRICE_OHLC_INVALID", f"{prefix}OHLC区间关系非法")
    if all(quality == "OBSERVED" for quality in qualities):
        return {**field_columns, quality_column: "OBSERVED", reason_column: None}
    group_quality = (
        "BAD_VALUE"
        if "BAD_VALUE" in qualities
        else "NOT_APPLICABLE"
        if all(item == "NOT_APPLICABLE" for item in qualities)
        else "UNKNOWN"
    )
    return {
        **field_columns,
        quality_column: group_quality,
        reason_column: _field_reason(reasons),
    }


def _unknown_price_group(prefix: str, reason: str) -> dict[str, Any]:
    quality_column = "qfq_price_quality" if prefix == "qfq" else "price_quality"
    reason_column = "qfq_price_reason" if prefix == "qfq" else "price_reason"
    fields = {
        column: value
        for name in ("open", "high", "low", "close")
        for column, value in (
            (f"{prefix}_{name}", None),
            (f"{prefix}_{name}_quality", "UNKNOWN"),
            (f"{prefix}_{name}_reason", reason),
        )
    }
    return {**fields, quality_column: "UNKNOWN", reason_column: reason}


def _coordinate_schema(
    record: Any,
    attempt: Mapping[str, Any],
    coordinate: str,
) -> tuple[str, ...] | None:
    schemas = _get(record, "response_fields_by_coordinate")
    fields = schemas.get(coordinate) if isinstance(schemas, Mapping) else None
    if fields is None:
        short = "raw" if coordinate == RAW_COORDINATE else "qfq"
        for attribute in (
            f"{short}_returned_fields",
            f"{short}_response_fields",
            f"{coordinate.lower()}_response_fields",
        ):
            fields = _get(record, attribute)
            if fields is not None:
                break
    if fields is None:
        fields = attempt.get("returned_fields")
    if fields is None and coordinate == RAW_COORDINATE:
        observation = _get(record, "raw_observation")
        fields = (
            _get(observation, "returned_fields") if observation is not None else None
        )
    if fields is None and coordinate == QFQ_COORDINATE:
        observation = _get(record, "qfq_observation")
        fields = (
            _get(observation, "returned_fields") if observation is not None else None
        )
    if fields is None:
        return None
    if isinstance(fields, (str, bytes)) or not isinstance(fields, Sequence):
        if not _request_complete(attempt):
            return None
        raise BaoStockPriceFactsError(
            "RESPONSE_FIELDS_INVALID", "来源响应字段schema必须为字符串列表"
        )
    normalized = tuple(str(item) for item in fields)
    if (
        not normalized
        or len(normalized) != len(set(normalized))
        or any(not item or item != item.strip() for item in normalized)
    ):
        if not _request_complete(attempt):
            return None
        raise BaoStockPriceFactsError(
            "RESPONSE_FIELDS_INVALID", "来源响应字段schema为空、重复或含首尾空格"
        )
    return tuple(sorted(normalized))


def _market_board(source_symbol: str) -> tuple[str, str]:
    market, code = source_symbol.split(".", 1)
    if market == "BJ":
        return market, "BEIJING_BOARD"
    if market == "SH" and code.startswith(("688", "689")):
        return market, "STAR_BOARD"
    if market == "SZ" and code.startswith(("300", "301")):
        return market, "GEM"
    return market, "MAIN_BOARD"


def _request_complete(row: Mapping[str, Any]) -> bool:
    return row.get("request_status") in {"COMPLETED", "NO_DATA"}


def _unknown_field(reason: str) -> FieldValue:
    return FieldValue(value=None, quality="UNKNOWN_DATA", reason=reason)


def _activity_field(
    observation: Any,
    name: str,
    *,
    request_complete: bool,
    integer: bool = False,
    coordinate_prefix: str = "raw",
) -> tuple[Any, str, str | None]:
    if observation is None:
        reason = (
            f"{coordinate_prefix.upper()}_BAR_NOT_RETURNED"
            if request_complete
            else f"{coordinate_prefix.upper()}_REQUEST_NOT_COMPLETED"
        )
        field = _unknown_field(reason)
    else:
        field = _get(observation, name)
        if field is None:
            field = _unknown_field(f"{coordinate_prefix.upper()}_FIELD_NOT_RETURNED")
    is_numeric = name in {"amount", "turn", "volume"}
    result, quality, reason = _field_value(
        field,
        label=f"{coordinate_prefix}_{name}",
        numeric=is_numeric,
        integer=integer,
        default_reason=f"{coordinate_prefix.upper()}_FIELD_UNKNOWN",
    )
    if name in {"tradestatus", "is_st"} and quality == "OBSERVED":
        if result not in {"0", "1"}:
            raise BaoStockPriceFactsError(
                "STATUS_VALUE_INVALID", f"{coordinate_prefix}_{name}必须为0或1"
            )
    if name in {"amount", "volume", "turn"} and quality == "OBSERVED" and result < 0:
        raise BaoStockPriceFactsError(
            "ACTIVITY_VALUE_INVALID", f"{coordinate_prefix}_{name}不能为负数"
        )
    return result, quality, reason


def _raw_payload(
    record: Any,
    *,
    request_complete: bool,
    response_fields: tuple[str, ...] | None,
) -> dict[str, Any]:
    observation = _get(record, "raw_observation")
    if observation is not None:
        if _get(observation, "adjustment_flag") != "3":
            raise BaoStockPriceFactsError(
                "RAW_ADJUSTMENT_FLAG_INVALID", "RAW观察必须来自adjustflag=3"
            )
        if _get(observation, "price_coordinate") != RAW_COORDINATE:
            raise BaoStockPriceFactsError(
                "RAW_COORDINATE_INVALID", "RAW观察坐标标识不匹配"
            )
        group = _price_group(
            observation,
            prefix="raw",
            unavailable_reason="RAW_OHLC_UNKNOWN",
        )
        preclose_field = _get(observation, "preclose")
        if preclose_field is None:
            preclose_field = _unknown_field("RAW_PRECLOSE_NOT_RETURNED")
        preclose, preclose_quality, preclose_reason = _field_value(
            preclose_field,
            label="raw_preclose",
            numeric=True,
            default_reason="RAW_PRECLOSE_UNKNOWN",
        )
        if preclose_quality == "OBSERVED" and preclose <= 0:
            raise BaoStockPriceFactsError("PRECLOSE_INVALID", "RAW前收盘价必须为正数")
    else:
        reason = (
            "RAW_BAR_NOT_RETURNED" if request_complete else "RAW_REQUEST_NOT_COMPLETED"
        )
        group = {
            **_unknown_price_group("raw", reason),
        }
        preclose = None
        preclose_quality = "UNKNOWN"
        preclose_reason = reason

    activity: dict[str, Any] = {}
    for field_name, storage_name, numeric, integer in (
        ("amount", "raw_amount", True, False),
        ("volume", "raw_volume", True, True),
        ("turn", "raw_turn_value", True, False),
        ("tradestatus", "raw_tradestatus_value", False, False),
        ("is_st", "raw_is_st_value", False, False),
    ):
        value, quality, reason = _activity_field(
            observation,
            field_name,
            request_complete=request_complete,
            integer=integer,
        )
        if field_name in {"amount", "volume", "turn"} and not numeric:
            raise BaoStockPriceFactsError(
                "FIELD_CONTRACT_INVALID", "活动值数值合同错误"
            )
        if field_name == "amount":
            activity.update(
                raw_amount=value,
                raw_amount_quality=(
                    "RAW_SOURCE_VALUE_OBSERVED" if quality == "OBSERVED" else quality
                ),
                raw_amount_reason=reason,
                amount_quality=quality,
                amount_reason=reason,
                amount_unit_status="CNY_VERIFIED",
            )
        elif field_name == "volume":
            activity.update(
                raw_volume=value,
                raw_volume_quality=(
                    "RAW_SOURCE_VALUE_OBSERVED" if quality == "OBSERVED" else quality
                ),
                raw_volume_reason=reason,
                volume_quality=quality,
                volume_reason=reason,
                volume_unit_status="SHARES_VERIFIED",
            )
        else:
            prefix = "raw_turn" if field_name == "turn" else f"raw_{field_name}"
            activity[f"{prefix}_value"] = value
            activity[f"{prefix}_quality"] = quality
            activity[f"{prefix}_reason"] = reason
    if observation is None:
        bar_status = "TARGET_MISSING" if request_complete else "READ_FAILED"
        bar_reason = (
            "RAW_BAR_NOT_RETURNED" if request_complete else "RAW_REQUEST_NOT_COMPLETED"
        )
    else:
        bar_status = "TARGET_PRESENT"
        bar_reason = None
    return {
        **group,
        "raw_preclose": preclose,
        "raw_preclose_quality": preclose_quality,
        "raw_preclose_reason": preclose_reason,
        "raw_response_session_date": (
            _get(observation, "session_date") if observation is not None else None
        ),
        "raw_response_source_code": (
            _get(observation, "source_code") if observation is not None else None
        ),
        "raw_adjustflag": 3 if observation is not None else None,
        "raw_response_fields": list(response_fields) if response_fields else None,
        **activity,
        "bar_status": bar_status,
        "bar_reason": bar_reason,
    }


def _qfq_payload(
    record: Any,
    *,
    request_complete: bool,
    response_fields: tuple[str, ...] | None,
) -> dict[str, Any]:
    observation = _get(record, "qfq_observation")
    legacy_ohlc = _get(record, "qfq_ohlc")
    if observation is None and legacy_ohlc is not None:
        raise BaoStockPriceFactsError(
            "QFQ_RESPONSE_IDENTITY_REQUIRED",
            "QFQ价格行必须绑定adjustflag=2响应身份；不能仅凭旧式OHLC对象创建观察值",
        )
    qfq = observation
    if qfq is None:
        reason = (
            "QFQ_BAR_NOT_RETURNED" if request_complete else "QFQ_REQUEST_NOT_COMPLETED"
        )
        price = {
            **_unknown_price_group("qfq", reason),
        }
        preclose = (None, "UNKNOWN", reason)
    else:
        if observation is not None and (
            _get(observation, "adjustment_flag") != "2"
            or _get(observation, "price_coordinate") != QFQ_COORDINATE
        ):
            raise BaoStockPriceFactsError(
                "QFQ_ADJUSTMENT_FLAG_INVALID", "QFQ响应必须来自adjustflag=2"
            )
        price = _price_group(qfq, prefix="qfq", unavailable_reason="QFQ_OHLC_UNKNOWN")
        preclose_field = _get(qfq, "preclose")
        if preclose_field is None:
            preclose_field = _unknown_field("QFQ_PRECLOSE_NOT_RETURNED")
        preclose_value, preclose_quality, preclose_reason = _field_value(
            preclose_field,
            label="qfq_preclose",
            numeric=True,
            default_reason="QFQ_PRECLOSE_UNKNOWN",
        )
        if preclose_quality == "OBSERVED" and preclose_value <= 0:
            raise BaoStockPriceFactsError("PRECLOSE_INVALID", "QFQ前收盘价必须为正数")
        preclose = (preclose_value, preclose_quality, preclose_reason)

    activity: dict[str, Any] = {}
    for field_name, value_column, quality_column, reason_column, integer in (
        ("amount", "qfq_amount", "qfq_amount_quality", "qfq_amount_reason", False),
        ("volume", "qfq_volume", "qfq_volume_quality", "qfq_volume_reason", True),
        ("turn", "qfq_turn_value", "qfq_turn_quality", "qfq_turn_reason", False),
        (
            "tradestatus",
            "qfq_tradestatus_value",
            "qfq_tradestatus_quality",
            "qfq_tradestatus_reason",
            False,
        ),
        ("is_st", "qfq_is_st_value", "qfq_is_st_quality", "qfq_is_st_reason", False),
    ):
        value, quality, reason = _activity_field(
            observation,
            field_name,
            request_complete=request_complete,
            integer=integer,
            coordinate_prefix="qfq",
        )
        activity[value_column] = value
        activity[quality_column] = quality
        activity[reason_column] = reason
        if field_name == "amount":
            activity["qfq_amount_unit_status"] = "CNY_VERIFIED"
        elif field_name == "volume":
            activity["qfq_volume_unit_status"] = "SHARES_VERIFIED"

    if observation is not None:
        bar_status = "TARGET_PRESENT"
        bar_reason = None
    else:
        bar_status = "TARGET_MISSING" if request_complete else "READ_FAILED"
        bar_reason = (
            "QFQ_BAR_NOT_RETURNED" if request_complete else "QFQ_REQUEST_NOT_COMPLETED"
        )
    return {
        **price,
        "qfq_response_session_date": (
            _get(observation, "session_date") if observation is not None else None
        ),
        "qfq_response_source_code": (
            _get(observation, "source_code") if observation is not None else None
        ),
        "qfq_adjustflag": 2 if observation is not None else None,
        "qfq_response_fields": list(response_fields) if response_fields else None,
        "qfq_bar_status": bar_status,
        "qfq_bar_reason": bar_reason,
        "qfq_preclose": preclose[0],
        "qfq_preclose_quality": preclose[1],
        "qfq_preclose_reason": preclose[2],
        **activity,
    }


def _board_for_symbol(source_symbol: str) -> tuple[str, str]:
    classification = classify_baostock_board(source_symbol)
    if classification is None or classification[1] not in {
        "MAIN_BOARD",
        "GEM",
        "STAR_BOARD",
        "BEIJING_BOARD",
    }:
        raise BaoStockPriceFactsError(
            "SOURCE_BOARD_UNSUPPORTED", "BaoStock来源代码不属于可持久化的A股板别"
        )
    return classification


def _source_code(record: Any, source_symbol: str) -> str:
    code = _get(record, "source_code", _get(record, "requested_code"))
    if code is None:
        observation = _get(record, "raw_observation")
        code = _get(observation, "source_code") if observation is not None else None
    if not isinstance(code, str) or normalize_baostock_symbol(code) != source_symbol:
        raise BaoStockPriceFactsError(
            "SOURCE_CODE_MISMATCH",
            "价格行必须保存请求响应中的真实lowercase BaoStock source_code",
        )
    return code


def _status_reason(row: Mapping[str, Any], *, fallback: str) -> str:
    value = row.get("last_error_code") or row.get("last_error_message") or fallback
    return str(value)[:1000]


def _raw_activity_payload(
    observation: Any,
    *,
    request_complete: bool,
    request_row: Mapping[str, Any],
) -> dict[str, Any]:
    reason = (
        "RAW_BAR_NOT_RETURNED"
        if request_complete
        else _status_reason(request_row, fallback="RAW_REQUEST_NOT_COMPLETED")
    )
    output: dict[str, Any] = {}
    for field_name, prefix, numeric, integer in (
        ("amount", "raw_amount", True, False),
        ("volume", "raw_volume", True, True),
        ("turn", "raw_turn", True, False),
        ("tradestatus", "raw_tradestatus", False, False),
        ("is_st", "raw_is_st", False, False),
    ):
        field = _get(observation, field_name) if observation is not None else None
        if field is None:
            field = _unknown_field(reason)
        value, quality, field_reason = _field_value(
            field,
            label=prefix,
            numeric=numeric,
            integer=integer,
            default_reason=reason,
        )
        if field_name in {"tradestatus", "is_st"} and quality == "OBSERVED":
            if value not in {"0", "1"}:
                raise BaoStockPriceFactsError(
                    "STATUS_VALUE_INVALID", f"{prefix}必须为0或1"
                )
        if (
            field_name in {"amount", "volume", "turn"}
            and quality == "OBSERVED"
            and value < 0
        ):
            raise BaoStockPriceFactsError(
                "ACTIVITY_VALUE_INVALID", f"{prefix}不能为负数"
            )
        if field_name == "amount":
            output.update(
                raw_amount=value,
                raw_amount_quality=(
                    "RAW_SOURCE_VALUE_OBSERVED" if quality == "OBSERVED" else quality
                ),
                raw_amount_reason=field_reason,
                amount_quality=quality,
                amount_reason=field_reason,
                amount_unit_status="CNY_VERIFIED",
            )
        elif field_name == "volume":
            output.update(
                raw_volume=value,
                raw_volume_quality=(
                    "RAW_SOURCE_VALUE_OBSERVED" if quality == "OBSERVED" else quality
                ),
                raw_volume_reason=field_reason,
                volume_quality=quality,
                volume_reason=field_reason,
                volume_unit_status="SHARES_VERIFIED",
            )
        else:
            output[f"{prefix}_value"] = value
            output[f"{prefix}_quality"] = quality
            output[f"{prefix}_reason"] = field_reason
    return output


def _raw_preclose_payload(
    observation: Any,
    *,
    request_complete: bool,
    request_row: Mapping[str, Any],
) -> dict[str, Any]:
    if observation is None:
        reason = (
            "RAW_BAR_NOT_RETURNED"
            if request_complete
            else _status_reason(request_row, fallback="RAW_REQUEST_NOT_COMPLETED")
        )
        return {
            "raw_preclose": None,
            "raw_preclose_quality": "UNKNOWN",
            "raw_preclose_reason": reason,
        }
    field = _get(observation, "preclose")
    if field is None:
        field = _unknown_field("RAW_PRECLOSE_NOT_RETURNED")
    value, quality, reason = _field_value(
        field,
        label="raw_preclose",
        numeric=True,
        default_reason="RAW_PRECLOSE_UNKNOWN",
    )
    if quality == "OBSERVED" and value <= 0:
        raise BaoStockPriceFactsError("PRECLOSE_INVALID", "RAW前收盘价必须为正数")
    return {
        "raw_preclose": value,
        "raw_preclose_quality": quality,
        "raw_preclose_reason": reason,
    }


def _attempts_complete(status: str) -> bool:
    return status in {"COMPLETED", "NO_DATA"}


def _attempt_schema(
    record: Any,
    request_row: Mapping[str, Any],
    coordinate: str,
) -> tuple[str, ...] | None:
    schema = _coordinate_schema(record, request_row, coordinate)
    if schema is None and request_row.get("request_status") in {"COMPLETED", "NO_DATA"}:
        raise BaoStockPriceFactsError(
            "RESPONSE_FIELDS_MISSING", f"{coordinate}成功响应缺少真实returned_fields"
        )
    return schema


def _record_keys(
    records: Sequence[Any],
    eligible_codes: Mapping[str, tuple[str, tuple[date, ...]]],
    *,
    raw_requests: Mapping[str, Mapping[str, Any]],
    qfq_requests: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[tuple[str, date], Any], dict[tuple[str, str], tuple[str, ...]]]:
    by_key: dict[tuple[str, date], Any] = {}
    schemas: dict[tuple[str, str], tuple[str, ...]] = {}
    if isinstance(records, (str, bytes)) or not isinstance(records, Sequence):
        raise BaoStockPriceFactsError("PRICE_RECORDS_INVALID", "价格批次记录必须为序列")
    for record in records:
        raw_symbol = _get(record, "source_symbol")
        session = _get(record, "session_date")
        if type(session) is not date:
            raise BaoStockPriceFactsError(
                "PRICE_RECORDS_INVALID", "价格记录会话日期无效"
            )
        _, symbol = _normalized_code(raw_symbol)
        source_code = _source_code(record, symbol)
        eligible = eligible_codes.get(source_code)
        if eligible is None or session not in eligible[1]:
            raise BaoStockPriceFactsError(
                "PRICE_RECORD_OUTSIDE_ELIGIBLE_WINDOW",
                f"价格记录{symbol}/{session.isoformat()}不在query_stock_basic有效窗口",
            )
        key = (symbol, session)
        if key in by_key:
            raise BaoStockPriceFactsError(
                "PRICE_RECORD_DUPLICATE", "捕获批次重复证券会话行"
            )
        by_key[key] = record
        for coordinate, request_map in (
            (RAW_COORDINATE, raw_requests),
            (QFQ_COORDINATE, qfq_requests),
        ):
            request = request_map[source_code]
            fields = _attempt_schema(record, request, coordinate)
            if fields is not None:
                prior = schemas.get((source_code, coordinate))
                if prior is not None and prior != fields:
                    raise BaoStockPriceFactsError(
                        "RESPONSE_FIELDS_INCONSISTENT",
                        f"同一捕获中{source_code}的{coordinate}returned_fields发生变化",
                    )
                schemas[(source_code, coordinate)] = fields
    for source_code, (_symbol, sessions) in eligible_codes.items():
        raw_complete = _attempts_complete(raw_requests[source_code]["request_status"])
        qfq_complete = _attempts_complete(qfq_requests[source_code]["request_status"])
        if raw_complete or qfq_complete:
            missing = [
                session
                for session in sessions
                if (eligible_codes[source_code][0], session) not in by_key
            ]
            if missing:
                raise BaoStockPriceFactsError(
                    "PRICE_WINDOW_RECORDS_INCOMPLETE",
                    f"{source_code}有效窗口缺少{len(missing)}条NoBar/质量记录",
                )
        if raw_complete and qfq_complete:
            raw_fields = schemas.get((source_code, RAW_COORDINATE))
            qfq_fields = schemas.get((source_code, QFQ_COORDINATE))
            if raw_fields is None or qfq_fields is None or raw_fields != qfq_fields:
                raise BaoStockPriceFactsError(
                    "RESPONSE_FIELDS_COORDINATE_MISMATCH",
                    f"{source_code}的RAW/QFQ实际响应字段schema不同",
                )
    return by_key, schemas


def _persist_capture_requests(
    cursor: Any,
    *,
    capture_id: int,
    range_start: date,
    range_end: date,
    eligible_codes: Mapping[str, tuple[str, tuple[date, ...]]],
    request_rows: Mapping[tuple[str, str], Mapping[str, Any]],
    batch_started_at: datetime,
    batch_completed_at: datetime,
) -> tuple[
    dict[tuple[str, str], dict[str, Any]],
    dict[tuple[str, str], str | None],
    int,
]:
    codes = sorted(eligible_codes)
    cursor.execute(
        """SELECT source_code,adjustment_flag,range_start_session,range_end_session,
                  eligible_start_session,eligible_end_session,request_status,
                  attempt_count,completed_through_session,response_row_count,
                  last_attempt_started_at,last_attempt_completed_at,retry_after,
                  last_error_code,last_error_message,returned_fields
           FROM stock.source_capture_requests
           WHERE capture_id=%s AND source_code=ANY(%s::text[])
           FOR UPDATE""",
        (capture_id, codes),
    )
    existing = {(str(row[0]), str(row[1])): row for row in cursor.fetchall()}
    written: dict[tuple[str, str], dict[str, Any]] = {}
    previous_statuses = {key: str(row[6]) for key, row in existing.items()}
    insert_sql = """INSERT INTO stock.source_capture_requests
        (capture_id,source_code,adjustment_flag,range_start_session,range_end_session,
         eligible_start_session,eligible_end_session,request_status,attempt_count,
         completed_through_session,response_row_count,last_attempt_started_at,
         last_attempt_completed_at,retry_after,last_error_code,last_error_message,
         returned_fields,updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,clock_timestamp())
        ON CONFLICT (capture_id,source_code,adjustment_flag) DO UPDATE SET
          request_status=EXCLUDED.request_status,
          attempt_count=EXCLUDED.attempt_count,
          completed_through_session=EXCLUDED.completed_through_session,
          response_row_count=EXCLUDED.response_row_count,
          last_attempt_started_at=EXCLUDED.last_attempt_started_at,
          last_attempt_completed_at=EXCLUDED.last_attempt_completed_at,
          retry_after=EXCLUDED.retry_after,
          last_error_code=EXCLUDED.last_error_code,
          last_error_message=EXCLUDED.last_error_message,
          returned_fields=EXCLUDED.returned_fields,
          updated_at=clock_timestamp()"""
    rows = []
    for key, incoming in request_rows.items():
        source_code, adjustment_flag = key
        _symbol, eligible_sessions = eligible_codes[source_code]
        if not eligible_sessions:
            continue
        eligible_start = eligible_sessions[0]
        eligible_end = eligible_sessions[-1]
        attempt_count = int(incoming["attempt_count"])
        response_row_count = int(incoming["response_row_count"])
        status = str(incoming["request_status"])
        old = existing.get(key)
        last_started = _parse_optional_clock(
            incoming.get("last_attempt_started_at"), label="来源请求开始时间"
        )
        last_completed = _parse_optional_clock(
            incoming.get("last_attempt_completed_at"), label="来源请求完成时间"
        )
        retry_after = _parse_optional_clock(
            incoming.get("retry_after"), label="请求重试时刻"
        )
        error_code = incoming.get("last_error_code")
        error_message = incoming.get("last_error_message")
        returned_fields = incoming.get("returned_fields")
        if attempt_count > 0:
            last_started = last_started or batch_started_at
            last_completed = last_completed or batch_completed_at
        if status in {"COMPLETED", "NO_DATA"}:
            completed_through = range_end
            error_code = None
            error_message = None
        else:
            completed_through = None
            if status in {"RETRYABLE_FAILURE", "PERMANENT_FAILURE"}:
                error_code = error_code or status
                error_message = error_message or str(error_code)
        if old is not None:
            if (
                old[2] != range_start
                or old[3] != range_end
                or old[4] != eligible_start
                or old[5] != eligible_end
            ):
                raise BaoStockPriceFactsError(
                    "CAPTURE_REQUEST_WINDOW_CONFLICT",
                    f"同一捕获证券{source_code}的请求会话/有效窗口发生变化",
                )
            old_status = str(old[6])
            old_attempts = int(old[7])
            old_response_count = int(old[9])
            old_returned_fields = tuple(old[15] or ()) or None
            old_resolved = old_status in {"COMPLETED", "NO_DATA"}
            new_resolved = status in {"COMPLETED", "NO_DATA"}
            if (
                old_resolved
                and new_resolved
                and (
                    old_status != status
                    or old_response_count != response_row_count
                    or (
                        old_returned_fields is not None
                        and returned_fields is not None
                        and old_returned_fields != tuple(returned_fields)
                    )
                )
            ):
                raise BaoStockPriceFactsError(
                    "CAPTURE_REQUEST_CONTENT_CONFLICT",
                    f"同一捕获证券{source_code}/{adjustment_flag}响应行数或完成状态发生变化",
                )
            attempt_count = max(old_attempts, attempt_count)
            if old_resolved and not new_resolved:
                status = old_status
                response_row_count = old_response_count
                completed_through = old[8]
                retry_after = old[12]
                error_code = None
                error_message = None
                returned_fields = old_returned_fields
            elif (
                not new_resolved
                and old_status
                in {
                    "RETRYABLE_FAILURE",
                    "PERMANENT_FAILURE",
                }
                and status == "PENDING"
            ):
                status = old_status
                response_row_count = old_response_count
                retry_after = old[12]
                error_code = old[13]
                error_message = old[14]
                returned_fields = old_returned_fields
            last_started = last_started or old[10]
            last_completed = last_completed or old[11]
            if returned_fields is None:
                returned_fields = old_returned_fields
        if (
            last_started is not None
            and last_completed is not None
            and last_completed < last_started
        ):
            raise BaoStockPriceFactsError(
                "REQUEST_CLOCK_ORDER_INVALID", "来源请求完成时刻早于开始时刻"
            )
        row = {
            "capture_id": capture_id,
            "source_code": source_code,
            "adjustment_flag": adjustment_flag,
            "range_start_session": range_start,
            "range_end_session": range_end,
            "eligible_start_session": eligible_start,
            "eligible_end_session": eligible_end,
            "request_status": status,
            "attempt_count": attempt_count,
            "completed_through_session": completed_through,
            "response_row_count": response_row_count,
            "last_attempt_started_at": last_started,
            "last_attempt_completed_at": last_completed,
            "retry_after": retry_after,
            "last_error_code": error_code,
            "last_error_message": error_message,
            "returned_fields": list(returned_fields)
            if returned_fields is not None
            else None,
        }
        written[key] = row
        rows.append(
            tuple(
                row[name]
                for name in (
                    "capture_id",
                    "source_code",
                    "adjustment_flag",
                    "range_start_session",
                    "range_end_session",
                    "eligible_start_session",
                    "eligible_end_session",
                    "request_status",
                    "attempt_count",
                    "completed_through_session",
                    "response_row_count",
                    "last_attempt_started_at",
                    "last_attempt_completed_at",
                    "retry_after",
                    "last_error_code",
                    "last_error_message",
                    "returned_fields",
                )
            )
        )
    for row in rows:
        cursor.execute(insert_sql, row)
    return written, previous_statuses, len(rows)


def _build_price_row(
    record: Any,
    *,
    source_code: str,
    source_symbol: str,
    session: date,
    capture_id: int,
    unit_contract_version: str,
    returned_fields: tuple[str, ...],
    raw_request: Mapping[str, Any],
    qfq_request: Mapping[str, Any],
    observed_started_at: datetime,
    observed_completed_at: datetime,
) -> dict[str, Any]:
    raw_complete = _attempts_complete(str(raw_request["request_status"]))
    qfq_complete = _attempts_complete(str(qfq_request["request_status"]))
    raw_observation = _get(record, "raw_observation")
    qfq_ohlc = _get(record, "qfq_ohlc", _get(record, "qfq_observation"))
    qfq_observation = _get(record, "qfq_observation")
    if raw_observation is not None:
        if not raw_complete:
            raise BaoStockPriceFactsError(
                "RAW_OBSERVATION_WITH_FAILED_REQUEST",
                "RAW请求未完整成功却收到价格行，拒绝持久化未绑定来源事实",
            )
        if (
            _get(raw_observation, "session_date") != session
            or _get(raw_observation, "adjustment_flag") != "3"
            or _get(raw_observation, "price_coordinate") != RAW_COORDINATE
            or normalize_baostock_symbol(_get(raw_observation, "source_code"))
            != source_symbol
            or _get(raw_observation, "unit_contract_version") != unit_contract_version
        ):
            raise BaoStockPriceFactsError(
                "RAW_OBSERVATION_IDENTITY_MISMATCH",
                "RAW观察与capture/证券/会话/单位合同不一致",
            )
    if qfq_observation is not None:
        if not qfq_complete:
            raise BaoStockPriceFactsError(
                "QFQ_OBSERVATION_WITH_FAILED_REQUEST",
                "QFQ请求未完整成功却收到响应行，拒绝持久化未绑定来源事实",
            )
        if (
            _get(qfq_observation, "session_date") != session
            or _get(qfq_observation, "adjustment_flag") != "2"
            or _get(qfq_observation, "price_coordinate") != QFQ_COORDINATE
            or normalize_baostock_symbol(_get(qfq_observation, "source_code"))
            != source_symbol
            or _get(qfq_observation, "unit_contract_version") != unit_contract_version
        ):
            raise BaoStockPriceFactsError(
                "QFQ_OBSERVATION_IDENTITY_MISMATCH",
                "QFQ响应与capture/证券/会话/单位合同不一致",
            )
    if qfq_ohlc is not None and not qfq_complete:
        raise BaoStockPriceFactsError(
            "QFQ_OBSERVATION_WITH_FAILED_REQUEST",
            "QFQ请求未完整成功却收到价格行，拒绝持久化未绑定来源事实",
        )

    raw_fields = _coordinate_schema(record, raw_request, RAW_COORDINATE)
    qfq_fields = _coordinate_schema(record, qfq_request, QFQ_COORDINATE)
    raw_payload = _raw_payload(
        record, request_complete=raw_complete, response_fields=raw_fields
    )
    qfq_payload = _qfq_payload(
        record, request_complete=qfq_complete, response_fields=qfq_fields
    )
    market, board = _board_for_symbol(source_symbol)
    source_code, normalized_symbol = _normalized_code(source_code)
    if normalized_symbol != source_symbol:
        raise BaoStockPriceFactsError(
            "SOURCE_CODE_SYMBOL_MISMATCH", "source_code与source_symbol不匹配"
        )
    if raw_observation is None:
        bar_status = "TARGET_MISSING" if raw_complete else "READ_FAILED"
        bar_reason = (
            "RAW_BAR_NOT_RETURNED"
            if raw_complete
            else _status_reason(raw_request, fallback="RAW_REQUEST_NOT_COMPLETED")
        )
    else:
        bar_status = "TARGET_PRESENT"
        bar_reason = None
    return {
        "source_symbol": source_symbol,
        "market": market,
        "board": board,
        "session_date": session,
        "source_capture_id": capture_id,
        "bar_status": bar_status,
        "bar_reason": bar_reason,
        **raw_payload,
        **qfq_payload,
        "observed_started_at": observed_started_at,
        "observed_completed_at": observed_completed_at,
        "source_clock_role": _NON_PIT_CLOCK_ROLE,
        "source_available_at": None,
        "source_code": source_code,
        "returned_fields": list(returned_fields),
        "unit_contract_version": unit_contract_version,
    }


def _same_row_value(left: Any, right: Any) -> bool:
    if isinstance(left, list) and isinstance(right, tuple):
        return tuple(left) == right
    if isinstance(left, tuple) and isinstance(right, list):
        return left == tuple(right)
    return left == right


def _merge_price_row(
    existing: Mapping[str, Any] | None,
    incoming: Mapping[str, Any],
    *,
    raw_attempt_status: str,
    qfq_attempt_status: str,
    prior_statuses: Mapping[tuple[str, str], str | None],
    source_code: str,
) -> tuple[dict[str, Any], bool]:
    """只补齐失败坐标；同一已成功捕获坐标内容变化即拒绝。"""
    if existing is None:
        return dict(incoming), True
    merged = dict(existing)
    if existing.get("source_code") not in (None, source_code):
        raise BaoStockPriceFactsError(
            "PRICE_CAPTURE_CONTENT_CONFLICT", "同一捕获证券的source_code发生变化"
        )
    old_fields = existing.get("returned_fields")
    new_fields = incoming.get("returned_fields")
    if (
        old_fields is not None
        and new_fields is not None
        and set(old_fields) != set(new_fields)
    ):
        raise BaoStockPriceFactsError(
            "PRICE_CAPTURE_SCHEMA_CONFLICT",
            "同一捕获的RAW/QFQ实际响应字段schema发生变化",
        )
    if old_fields is None and new_fields is not None:
        merged["returned_fields"] = list(new_fields)
    if merged.get("source_code") is None:
        merged["source_code"] = source_code
    for column in (
        "unit_contract_version",
        "market",
        "board",
    ):
        if existing.get(column) not in (None, incoming.get(column)):
            raise BaoStockPriceFactsError(
                "PRICE_CAPTURE_CONTENT_CONFLICT", f"同一捕获证券的{column}发生变化"
            )
        if merged.get(column) is None:
            merged[column] = incoming.get(column)

    raw_key = (source_code, "3")
    qfq_key = (source_code, "2")
    raw_incoming_complete = _attempts_complete(raw_attempt_status)
    qfq_incoming_complete = _attempts_complete(qfq_attempt_status)
    raw_was_complete = prior_statuses.get(raw_key) in {"COMPLETED", "NO_DATA"}
    qfq_was_complete = prior_statuses.get(qfq_key) in {"COMPLETED", "NO_DATA"}
    raw_bar_repaired_from_journal = (
        raw_was_complete
        and existing.get("bar_status") == "TARGET_MISSING"
        and existing.get("bar_reason") == "RAW_BAR_NOT_RETURNED"
        and incoming.get("bar_status") == "TARGET_PRESENT"
    )
    if raw_bar_repaired_from_journal:
        raw_was_complete = False
    groups = (
        (_RAW_PRICE_COLUMNS, raw_incoming_complete, raw_was_complete, True),
        (_RAW_ACTIVITY_COLUMNS, raw_incoming_complete, raw_was_complete, True),
        (_RAW_RESPONSE_COLUMNS, raw_incoming_complete, raw_was_complete, False),
        (_QFQ_PRICE_COLUMNS, qfq_incoming_complete, qfq_was_complete, True),
        (_QFQ_RESPONSE_COLUMNS, qfq_incoming_complete, qfq_was_complete, False),
    )
    for columns, incoming_complete, prior_complete, strict_existing in groups:
        if not incoming_complete:
            continue
        if prior_complete and strict_existing:
            if any(
                not _same_row_value(existing.get(name), incoming.get(name))
                for name in columns
            ):
                raise BaoStockPriceFactsError(
                    "PRICE_CAPTURE_CONTENT_CONFLICT",
                    "同一capture_id已成功保存的坐标事实内容变化",
                )
            continue
        for name in columns:
            previous_value = existing.get(name)
            incoming_value = incoming.get(name)
            if (
                prior_complete
                and previous_value is not None
                and not _same_row_value(previous_value, incoming_value)
            ):
                raise BaoStockPriceFactsError(
                    "PRICE_CAPTURE_CONTENT_CONFLICT",
                    f"同一capture_id已保存的响应字段{name}发生变化",
                )
            if not prior_complete or previous_value is None:
                merged[name] = incoming_value

    if raw_incoming_complete:
        if raw_was_complete and (
            existing.get("bar_status") != incoming.get("bar_status")
            or existing.get("bar_reason") != incoming.get("bar_reason")
        ):
            raise BaoStockPriceFactsError(
                "PRICE_CAPTURE_CONTENT_CONFLICT", "同一捕获RAW Bar状态发生变化"
            )
        if not raw_was_complete:
            merged["bar_status"] = incoming.get("bar_status")
            merged["bar_reason"] = incoming.get("bar_reason")
    if incoming.get("observed_started_at") is not None:
        prior_start = merged.get("observed_started_at")
        merged["observed_started_at"] = (
            incoming["observed_started_at"]
            if prior_start is None
            else min(prior_start, incoming["observed_started_at"])
        )
    if incoming.get("observed_completed_at") is not None:
        prior_end = merged.get("observed_completed_at")
        merged["observed_completed_at"] = (
            incoming["observed_completed_at"]
            if prior_end is None
            else max(prior_end, incoming["observed_completed_at"])
        )
    merged["source_capture_id"] = incoming["source_capture_id"]
    merged["source_symbol"] = incoming["source_symbol"]
    merged["source_clock_role"] = incoming["source_clock_role"]
    merged["source_available_at"] = None
    return merged, False


def persist_baostock_price_batch(
    config: Mapping[str, Any],
    *,
    source_capture_id: int,
    records: Sequence[Any],
    request_attempts: Any,
    eligible_sessions_by_symbol: Mapping[str, Any],
    unit_contract: BaoUnitContract,
    batch_started_at: datetime,
    batch_completed_at: datetime,
) -> dict[str, Any]:
    """在一笔Stock A事务内写请求状态、双坐标Raw行并逐字段读回。"""
    if type(source_capture_id) is not int or source_capture_id <= 0:
        raise BaoStockPriceFactsError(
            "CAPTURE_ID_INVALID", "source_capture_id必须为正整数"
        )
    started = _aware(batch_started_at, "本批开始时间")
    completed = _aware(batch_completed_at, "本批完成时间")
    if completed < started:
        raise BaoStockPriceFactsError(
            "BATCH_CLOCK_ORDER_INVALID", "本批完成时刻早于开始时刻"
        )
    unit_values = _unit_values(unit_contract)
    if isinstance(records, (str, bytes)) or not isinstance(records, Sequence):
        raise BaoStockPriceFactsError("PRICE_RECORDS_INVALID", "价格批次必须为记录序列")

    with source_raw._transaction(config, write=True, statement_timeout_ms=30000) as (
        _connection,
        cursor,
        _scope,
    ):
        _require_price_field_quality_migration(cursor)
        cursor.execute(
            """SELECT provider,capture_uuid,range_start_session,range_end_session,
                      price_coordinates,parser_contract,requested_symbol_count,
                      unit_contract_version
               FROM stock.source_captures WHERE capture_id=%s FOR UPDATE""",
            (source_capture_id,),
        )
        capture = cursor.fetchone()
        if capture is None:
            raise BaoStockPriceFactsError("CAPTURE_NOT_FOUND", "BaoStock价格捕获不存在")
        (
            provider,
            capture_uuid,
            range_start,
            range_end,
            capture_coordinates,
            _parser_contract,
            requested_symbol_count,
            capture_unit_version,
        ) = capture
        if (
            provider != "BAOSTOCK"
            or not isinstance(capture_uuid, UUID)
            or type(range_start) is not date
            or type(range_end) is not date
            or range_start > range_end
            or tuple(capture_coordinates or ()) != (RAW_COORDINATE, QFQ_COORDINATE)
            or capture_unit_version != unit_contract.version
            or type(requested_symbol_count) is not int
            or requested_symbol_count < 1
        ):
            raise BaoStockPriceFactsError(
                "CAPTURE_IDENTITY_INVALID",
                "capture提供方、范围、双坐标或单位合同身份不一致",
            )
        _ensure_unit_contract(cursor, unit_contract)
        eligible_codes = _eligible_windows(
            eligible_sessions_by_symbol,
            capture_start=range_start,
            capture_end=range_end,
        )
        if len(eligible_codes) > requested_symbol_count:
            raise BaoStockPriceFactsError(
                "CAPTURE_SYMBOL_COUNT_EXCEEDED", "批次有效证券数超过捕获登记的请求总体"
            )
        attempts = _normalize_request_attempts(
            request_attempts, eligible_codes=eligible_codes
        )
        raw_attempts = {
            source_code: attempts[(source_code, "3")] for source_code in eligible_codes
        }
        qfq_attempts = {
            source_code: attempts[(source_code, "2")] for source_code in eligible_codes
        }
        record_by_key, schemas = _record_keys(
            records,
            eligible_codes,
            raw_requests=raw_attempts,
            qfq_requests=qfq_attempts,
        )
        written_requests, previous_request_statuses, request_count = (
            _persist_capture_requests(
                cursor,
                capture_id=source_capture_id,
                range_start=range_start,
                range_end=range_end,
                eligible_codes=eligible_codes,
                request_rows=attempts,
                batch_started_at=started,
                batch_completed_at=completed,
            )
        )

        symbols = sorted({symbol for symbol, _session in record_by_key})
        sessions = sorted({session for _symbol, session in record_by_key})
        existing_rows: dict[tuple[str, date], dict[str, Any]] = {}
        if symbols and sessions:
            cursor.execute(
                f"""SELECT {",".join(_PRICE_COLUMNS)} FROM stock.source_raw_daily
                    WHERE source_capture_id=%s
                      AND source_symbol=ANY(%s::text[])
                      AND session_date=ANY(%s::date[])
                    FOR UPDATE""",
                (source_capture_id, symbols, sessions),
            )
            for row in cursor.fetchall():
                existing = dict(zip(_PRICE_COLUMNS, row, strict=True))
                existing_rows[
                    (str(existing["source_symbol"]), existing["session_date"])
                ] = existing

        desired_rows: dict[tuple[str, date], dict[str, Any]] = {}
        raw_rows_inserted = 0
        raw_rows_updated = 0
        raw_rows_unchanged = 0
        for key, record in record_by_key.items():
            source_symbol, session = key
            source_code, normalized_symbol = _normalized_code(
                _get(record, "source_code")
            )
            if normalized_symbol != source_symbol:
                raise BaoStockPriceFactsError(
                    "SOURCE_CODE_SYMBOL_MISMATCH", "请求source_code与来源证券代码不一致"
                )
            raw_request = raw_attempts[source_code]
            qfq_request = qfq_attempts[source_code]
            response_schemas = _get(record, "response_fields_by_coordinate")
            raw_in_batch = (
                RAW_COORDINATE in response_schemas
                if isinstance(response_schemas, Mapping)
                else True
            )
            qfq_in_batch = (
                QFQ_COORDINATE in response_schemas
                if isinstance(response_schemas, Mapping)
                else True
            )
            raw_row_request = (
                raw_request
                if raw_in_batch
                else {**raw_request, "request_status": "PENDING"}
            )
            qfq_row_request = (
                qfq_request
                if qfq_in_batch
                else {**qfq_request, "request_status": "PENDING"}
            )
            raw_complete = _attempts_complete(str(raw_row_request["request_status"]))
            qfq_complete = _attempts_complete(str(qfq_row_request["request_status"]))
            if not raw_complete and not qfq_complete:
                continue
            if not raw_complete and _get(record, "raw_observation") is not None:
                raise BaoStockPriceFactsError(
                    "RAW_OBSERVATION_WITH_FAILED_REQUEST", "RAW成功Bar缺少完整请求状态"
                )
            if (
                not qfq_complete
                and _get(record, "qfq_ohlc", _get(record, "qfq_observation"))
                is not None
            ):
                raise BaoStockPriceFactsError(
                    "QFQ_OBSERVATION_WITH_FAILED_REQUEST", "QFQ成功Bar缺少完整请求状态"
                )
            returned_fields = schemas.get((source_code, RAW_COORDINATE)) or schemas.get(
                (source_code, QFQ_COORDINATE)
            )
            if returned_fields is None:
                raise BaoStockPriceFactsError(
                    "RESPONSE_FIELDS_MISSING", f"{source_code}缺少真实响应字段schema"
                )
            observation = _get(record, "raw_observation")
            if (
                observation is not None
                and _get(observation, "unit_contract_version") != unit_values[0]
            ):
                raise BaoStockPriceFactsError(
                    "RAW_UNIT_CONTRACT_MISMATCH", "RAW观察单位合同版本与本次捕获不一致"
                )
            incoming = _build_price_row(
                record,
                source_code=source_code,
                source_symbol=source_symbol,
                session=session,
                capture_id=source_capture_id,
                unit_contract_version=unit_values[0],
                returned_fields=returned_fields,
                raw_request=raw_row_request,
                qfq_request=qfq_row_request,
                observed_started_at=started,
                observed_completed_at=completed,
            )
            existing = existing_rows.get(key)
            merged, _is_new = _merge_price_row(
                existing,
                incoming,
                raw_attempt_status=str(raw_row_request["request_status"]),
                qfq_attempt_status=str(qfq_row_request["request_status"]),
                prior_statuses=previous_request_statuses,
                source_code=source_code,
            )
            desired_rows[key] = merged
            if existing is None:
                raw_rows_inserted += 1
            elif any(
                not _same_row_value(existing.get(name), merged.get(name))
                for name in _PRICE_COLUMNS
            ):
                raw_rows_updated += 1
            else:
                raw_rows_unchanged += 1

        if desired_rows:
            insert_sql = (
                f"INSERT INTO stock.source_raw_daily ({','.join(_PRICE_COLUMNS)}) "
                f"VALUES ({','.join(['%s'] * len(_PRICE_COLUMNS))}) "
                "ON CONFLICT (source_capture_id,source_symbol,session_date) DO NOTHING"
            )
            update_columns = tuple(
                name
                for name in _PRICE_COLUMNS
                if name not in {"source_capture_id", "source_symbol", "session_date"}
            )
            update_sql = (
                "UPDATE stock.source_raw_daily SET "
                + ",".join(f"{name}=%s" for name in update_columns)
                + " WHERE source_capture_id=%s AND source_symbol=%s AND session_date=%s"
            )
            for key, desired in desired_rows.items():
                existing = existing_rows.get(key)
                if existing is None:
                    cursor.execute(
                        insert_sql, tuple(desired[name] for name in _PRICE_COLUMNS)
                    )
                elif any(
                    not _same_row_value(existing.get(name), desired.get(name))
                    for name in update_columns
                ):
                    cursor.execute(
                        update_sql,
                        tuple(desired[name] for name in update_columns)
                        + (source_capture_id, key[0], key[1]),
                    )
                    if cursor.rowcount != 1:
                        raise BaoStockPriceFactsError(
                            "PRICE_FACT_UPDATE_CONFLICT", "双坐标价格事实并发更新冲突"
                        )

            cursor.execute(
                f"""SELECT {",".join(_PRICE_COLUMNS)} FROM stock.source_raw_daily
                    WHERE source_capture_id=%s
                      AND source_symbol=ANY(%s::text[])
                      AND session_date=ANY(%s::date[])""",
                (
                    source_capture_id,
                    sorted({key[0] for key in desired_rows}),
                    sorted({key[1] for key in desired_rows}),
                ),
            )
            readback_rows = {}
            for row in cursor.fetchall():
                actual = dict(zip(_PRICE_COLUMNS, row, strict=True))
                readback_rows[
                    (str(actual["source_symbol"]), actual["session_date"])
                ] = actual
            for key, expected in desired_rows.items():
                actual = readback_rows.get(key)
                if actual is None or any(
                    not _same_row_value(actual.get(name), expected.get(name))
                    for name in _PRICE_COLUMNS
                ):
                    raise BaoStockPriceFactsError(
                        "PRICE_FACT_READBACK_MISMATCH",
                        f"BaoStock双坐标事实写入后独立字段读回不一致：{key[0]}/{key[1].isoformat()}",
                    )

        cursor.execute(
            "SELECT count(*) FROM stock.source_raw_daily WHERE source_capture_id=%s",
            (source_capture_id,),
        )
        normalized_count = int(cursor.fetchone()[0])
        facts_changed = _price_facts_changed(raw_rows_inserted, raw_rows_updated)
        cursor.execute(
            """UPDATE stock.source_captures
               SET normalized_count=%s,
                   validated_at=CASE
                       WHEN normalized_count IS DISTINCT FROM %s OR %s
                       THEN GREATEST(read_started_at,clock_timestamp())
                       ELSE validated_at
                   END
               WHERE capture_id=%s AND provider='BAOSTOCK'""",
            (normalized_count, normalized_count, facts_changed, source_capture_id),
        )
        if cursor.rowcount != 1:
            raise BaoStockPriceFactsError(
                "CAPTURE_UPDATE_CONFLICT", "来源捕获读回或规范化行数更新失败"
            )
        cursor.execute(
            "SELECT normalized_count FROM stock.source_captures WHERE capture_id=%s",
            (source_capture_id,),
        )
        if cursor.fetchone() != (normalized_count,):
            raise BaoStockPriceFactsError(
                "CAPTURE_READBACK_MISMATCH", "捕获规范化行数没有独立读回"
            )
        cursor.execute(
            """SELECT source_code,adjustment_flag,range_start_session,range_end_session,
                      eligible_start_session,eligible_end_session,request_status,
                      attempt_count,completed_through_session,response_row_count,
                      last_attempt_started_at,last_attempt_completed_at,retry_after,
                      last_error_code,last_error_message,returned_fields
               FROM stock.source_capture_requests
               WHERE capture_id=%s AND source_code=ANY(%s::text[])
               ORDER BY source_code,adjustment_flag""",
            (source_capture_id, sorted(eligible_codes)),
        )
        request_columns = [column.name for column in cursor.description]
        request_readback = {
            (str(row[0]), str(row[1])): dict(zip(request_columns, row, strict=True))
            for row in cursor.fetchall()
        }
        request_readback_count = len(request_readback)
        request_expected = len(written_requests)
        if request_readback_count != request_expected or any(
            not _same_row_value(
                tuple(
                    request_readback[key].get(name)
                    for name in (
                        "source_code",
                        "adjustment_flag",
                        "range_start_session",
                        "range_end_session",
                        "eligible_start_session",
                        "eligible_end_session",
                        "request_status",
                        "attempt_count",
                        "completed_through_session",
                        "response_row_count",
                        "last_attempt_started_at",
                        "last_attempt_completed_at",
                        "retry_after",
                        "last_error_code",
                        "last_error_message",
                        "returned_fields",
                    )
                ),
                tuple(
                    written_requests[key].get(name)
                    for name in (
                        "source_code",
                        "adjustment_flag",
                        "range_start_session",
                        "range_end_session",
                        "eligible_start_session",
                        "eligible_end_session",
                        "request_status",
                        "attempt_count",
                        "completed_through_session",
                        "response_row_count",
                        "last_attempt_started_at",
                        "last_attempt_completed_at",
                        "retry_after",
                        "last_error_code",
                        "last_error_message",
                        "returned_fields",
                    )
                ),
            )
            for key in written_requests
        ):
            raise BaoStockPriceFactsError(
                "CAPTURE_REQUEST_READBACK_MISMATCH",
                "BaoStock请求状态或字段schema未完整读回",
            )
        all_requests_complete = bool(written_requests) and all(
            row["request_status"] in {"COMPLETED", "NO_DATA"}
            for row in written_requests.values()
        )
        return {
            "status": "COMPLETED" if all_requests_complete else "INCOMPLETE",
            "source_capture_id": source_capture_id,
            "request_rows": request_readback_count,
            "price_rows_inserted": raw_rows_inserted,
            "price_rows_updated": raw_rows_updated,
            "price_rows_unchanged": raw_rows_unchanged,
            "capture_normalized_row_count": normalized_count,
            "readback_rows": len(desired_rows),
            "database_write_attempted": True,
            "source_io_attempted": False,
        }


_PRICE_READ_COLUMNS = (
    "source_capture_id",
    "capture_uuid",
    "provider",
    "range_start_session",
    "range_end_session",
    "price_coordinates",
    "capture_read_started_at",
    "capture_validated_at",
    "source_time_quality",
    "capture_clock_role",
    "capture_available_at",
    "source_symbol",
    "source_code",
    "market",
    "board",
    "session_date",
    "bar_status",
    "bar_reason",
    "raw_open",
    "raw_high",
    "raw_low",
    "raw_close",
    "raw_open_quality",
    "raw_open_reason",
    "raw_high_quality",
    "raw_high_reason",
    "raw_low_quality",
    "raw_low_reason",
    "raw_close_quality",
    "raw_close_reason",
    "price_quality",
    "price_reason",
    "raw_preclose",
    "raw_preclose_quality",
    "raw_preclose_reason",
    "raw_response_session_date",
    "raw_response_source_code",
    "raw_adjustflag",
    "raw_response_fields",
    "qfq_open",
    "qfq_high",
    "qfq_low",
    "qfq_close",
    "qfq_open_quality",
    "qfq_open_reason",
    "qfq_high_quality",
    "qfq_high_reason",
    "qfq_low_quality",
    "qfq_low_reason",
    "qfq_close_quality",
    "qfq_close_reason",
    "qfq_price_quality",
    "qfq_price_reason",
    "qfq_response_session_date",
    "qfq_response_source_code",
    "qfq_adjustflag",
    "qfq_response_fields",
    "qfq_bar_status",
    "qfq_bar_reason",
    "qfq_preclose",
    "qfq_preclose_quality",
    "qfq_preclose_reason",
    "qfq_volume",
    "qfq_volume_quality",
    "qfq_volume_reason",
    "qfq_volume_unit_status",
    "qfq_amount",
    "qfq_amount_quality",
    "qfq_amount_reason",
    "qfq_amount_unit_status",
    "qfq_turn_value",
    "qfq_turn_quality",
    "qfq_turn_reason",
    "qfq_tradestatus_value",
    "qfq_tradestatus_quality",
    "qfq_tradestatus_reason",
    "qfq_is_st_value",
    "qfq_is_st_quality",
    "qfq_is_st_reason",
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
    "raw_turn_value",
    "raw_turn_quality",
    "raw_turn_reason",
    "raw_tradestatus_value",
    "raw_tradestatus_quality",
    "raw_tradestatus_reason",
    "raw_is_st_value",
    "raw_is_st_quality",
    "raw_is_st_reason",
    "returned_fields",
    "unit_contract_version",
    "close_input_unit",
    "close_to_cny",
    "amount_input_unit",
    "amount_to_cny",
    "volume_input_unit",
    "volume_to_shares",
    "turn_input_unit",
    "raw_request_status",
    "raw_request_row_count",
    "raw_request_started_at",
    "raw_request_completed_at",
    "qfq_request_status",
    "qfq_request_row_count",
    "qfq_request_started_at",
    "qfq_request_completed_at",
    "observed_started_at",
    "observed_completed_at",
)
_PRICE_READ_SELECT = """SELECT
    d.id AS id,d.source_capture_id,c.capture_uuid,c.provider,c.range_start_session,
    c.range_end_session,c.price_coordinates,
    c.read_started_at AS capture_read_started_at,
    c.validated_at AS capture_validated_at,
    c.source_time_quality,c.source_clock_role AS capture_clock_role,
    c.source_available_at AS capture_available_at,
    d.source_symbol,d.source_code,d.market,d.board,d.session_date,
    d.bar_status,d.bar_reason,d.raw_open,d.raw_high,d.raw_low,d.raw_close,
    d.raw_open_quality,d.raw_open_reason,d.raw_high_quality,d.raw_high_reason,
    d.raw_low_quality,d.raw_low_reason,d.raw_close_quality,d.raw_close_reason,
    d.price_quality,d.price_reason,d.raw_preclose,d.raw_preclose_quality,
    d.raw_preclose_reason,d.raw_response_session_date,d.raw_response_source_code,
    d.raw_adjustflag,d.raw_response_fields,d.qfq_open,d.qfq_high,d.qfq_low,d.qfq_close,
    d.qfq_open_quality,d.qfq_open_reason,d.qfq_high_quality,d.qfq_high_reason,
    d.qfq_low_quality,d.qfq_low_reason,d.qfq_close_quality,d.qfq_close_reason,
    d.qfq_price_quality,d.qfq_price_reason,d.qfq_response_session_date,
    d.qfq_response_source_code,d.qfq_adjustflag,d.qfq_response_fields,
    d.qfq_bar_status,d.qfq_bar_reason,d.qfq_preclose,d.qfq_preclose_quality,
    d.qfq_preclose_reason,d.qfq_volume,d.qfq_volume_quality,d.qfq_volume_reason,
    d.qfq_volume_unit_status,d.qfq_amount,d.qfq_amount_quality,d.qfq_amount_reason,
    d.qfq_amount_unit_status,d.qfq_turn_value,d.qfq_turn_quality,d.qfq_turn_reason,
    d.qfq_tradestatus_value,d.qfq_tradestatus_quality,d.qfq_tradestatus_reason,
    d.qfq_is_st_value,d.qfq_is_st_quality,d.qfq_is_st_reason,
    d.raw_amount,d.raw_amount_quality,
    d.raw_amount_reason,d.amount_quality,d.amount_reason,d.amount_unit_status,
    d.raw_volume,d.raw_volume_quality,d.raw_volume_reason,d.volume_quality,
    d.volume_reason,d.volume_unit_status,d.raw_turn_value,d.raw_turn_quality,
    d.raw_turn_reason,d.raw_tradestatus_value,d.raw_tradestatus_quality,
    d.raw_tradestatus_reason,d.raw_is_st_value,d.raw_is_st_quality,
    d.raw_is_st_reason,d.returned_fields,u.unit_contract_version,
    u.close_input_unit,u.close_to_cny,u.amount_input_unit,u.amount_to_cny,
    u.volume_input_unit,u.volume_to_shares,u.turn_input_unit,
    raw_request.request_status AS raw_request_status,
    raw_request.response_row_count AS raw_request_row_count,
    raw_request.last_attempt_started_at AS raw_request_started_at,
    raw_request.last_attempt_completed_at AS raw_request_completed_at,
    qfq_request.request_status AS qfq_request_status,
    qfq_request.response_row_count AS qfq_request_row_count,
    qfq_request.last_attempt_started_at AS qfq_request_started_at,
    qfq_request.last_attempt_completed_at AS qfq_request_completed_at,
    d.observed_started_at,d.observed_completed_at
FROM stock.source_raw_daily d
JOIN stock.source_captures c ON c.capture_id=d.source_capture_id
JOIN stock.source_baostock_unit_contracts u
  ON u.unit_contract_version=d.unit_contract_version
JOIN stock.source_capture_requests raw_request
  ON raw_request.capture_id=d.source_capture_id
 AND raw_request.source_code=d.source_code AND raw_request.adjustment_flag='3'
JOIN stock.source_capture_requests qfq_request
  ON qfq_request.capture_id=d.source_capture_id
 AND qfq_request.source_code=d.source_code AND qfq_request.adjustment_flag='2'
WHERE c.provider='BAOSTOCK'
  AND c.range_end_session<=%s
  AND c.price_coordinates @> ARRAY['RAW_UNADJUSTED','FORWARD_ADJUSTED']::text[]
  AND d.session_date<=%s
"""


def _price_row_payload(row: Mapping[str, Any], price_coordinate: str) -> dict[str, Any]:
    if price_coordinate not in (RAW_COORDINATE, QFQ_COORDINATE):
        raise BaoStockPriceFactsError(
            "PRICE_COORDINATE_INVALID", "价格坐标必须为RAW或FORWARD_ADJUSTED"
        )
    raw_selected = price_coordinate == RAW_COORDINATE
    prefix = "raw" if raw_selected else "qfq"
    result = dict(row)
    result.update(
        {
            "price_coordinate": price_coordinate,
            "qfq_price_coordinate": QFQ_COORDINATE
            if price_coordinate == QFQ_COORDINATE
            else None,
            "adjustment_flag": _COORDINATE_FLAGS[price_coordinate],
            "selected_open": row[f"{prefix}_open"],
            "selected_high": row[f"{prefix}_high"],
            "selected_low": row[f"{prefix}_low"],
            "selected_close": row[f"{prefix}_close"],
            "selected_open_quality": result[f"{prefix}_open_quality"],
            "selected_open_reason": result[f"{prefix}_open_reason"],
            "selected_high_quality": result[f"{prefix}_high_quality"],
            "selected_high_reason": result[f"{prefix}_high_reason"],
            "selected_low_quality": result[f"{prefix}_low_quality"],
            "selected_low_reason": result[f"{prefix}_low_reason"],
            "selected_close_quality": result[f"{prefix}_close_quality"],
            "selected_close_reason": result[f"{prefix}_close_reason"],
            "selected_bar_status": (
                row["bar_status"] if raw_selected else row["qfq_bar_status"]
            ),
            "selected_bar_reason": (
                row["bar_reason"] if raw_selected else row["qfq_bar_reason"]
            ),
            "selected_price_quality": (
                row["price_quality"] if raw_selected else row["qfq_price_quality"]
            ),
            "selected_price_reason": (
                row["price_reason"] if raw_selected else row["qfq_price_reason"]
            ),
            "selected_preclose": (
                row["raw_preclose"] if raw_selected else row["qfq_preclose"]
            ),
            "selected_preclose_quality": (
                row["raw_preclose_quality"]
                if raw_selected
                else row["qfq_preclose_quality"]
            ),
            "selected_preclose_reason": (
                row["raw_preclose_reason"]
                if raw_selected
                else row["qfq_preclose_reason"]
            ),
            "time_role": "NON_PIT_RETROSPECTIVE",
            "pit_status": "NON_PIT",
            "source_available_at": row["capture_available_at"],
            "capture_clock_role": row["capture_clock_role"],
        }
    )
    return result


def _price_facts_changed(inserted_rows: int, updated_rows: int) -> bool:
    return inserted_rows > 0 or updated_rows > 0


def _read_price_rows(
    config: Mapping[str, Any],
    *,
    source_symbol: str,
    source_capture_id: int,
    sessions: Sequence[date],
    cutoff: date,
    price_coordinate: str,
) -> list[dict[str, Any]]:
    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _connection,
        cursor,
        _scope,
    ):
        return read_baostock_price_history_rows_cursor(
            cursor,
            source_symbols=(source_symbol,),
            source_capture_id=source_capture_id,
            sessions=sessions,
            cutoff=cutoff,
            price_coordinate=price_coordinate,
        )


def read_baostock_price_history_rows_cursor(
    cursor: Any,
    *,
    source_symbols: Sequence[str],
    source_capture_id: int,
    sessions: Sequence[date],
    cutoff: date,
    price_coordinate: str,
) -> list[dict[str, Any]]:
    """在调用方同一只读快照内读取指定捕获的RAW或QFQ typed日线。"""
    _require_price_field_quality_migration(cursor)
    if (
        type(source_capture_id) is not int
        or source_capture_id <= 0
        or type(cutoff) is not date
        or price_coordinate not in (RAW_COORDINATE, QFQ_COORDINATE)
        or not isinstance(source_symbols, Sequence)
        or isinstance(source_symbols, (str, bytes))
        or not source_symbols
        or not isinstance(sessions, Sequence)
        or isinstance(sessions, (str, bytes))
        or not sessions
        or any(type(session) is not date or session > cutoff for session in sessions)
        or tuple(sorted(set(sessions))) != tuple(sessions)
    ):
        raise BaoStockPriceFactsError(
            "PRICE_READ_SCOPE_INVALID", "同事务历史价格读取捕获、会话或坐标无效"
        )
    normalized_symbols = tuple(
        sorted({_normalized_code(value)[1] for value in source_symbols})
    )
    if not normalized_symbols or len(normalized_symbols) != len(source_symbols):
        raise BaoStockPriceFactsError(
            "PRICE_SYMBOL_SCOPE_INVALID", "同事务历史价格读取证券为空或重复"
        )
    chunk_size = max(1, min(100, 1500 // len(sessions)))
    result = []
    for start in range(0, len(normalized_symbols), chunk_size):
        cursor.execute(
            _PRICE_READ_SELECT
            + " AND d.source_capture_id=%s"
            + " AND d.source_symbol=ANY(%s::text[])"
            + " AND d.session_date=ANY(%s::date[])"
            + " ORDER BY d.session_date,d.source_symbol",
            (
                cutoff,
                cutoff,
                source_capture_id,
                list(normalized_symbols[start : start + chunk_size]),
                list(sessions),
            ),
        )
        names = [column.name for column in cursor.description]
        result.extend(
            _price_row_payload(dict(zip(names, row, strict=True)), price_coordinate)
            for row in cursor.fetchall()
        )
    return result


def read_baostock_price_history(
    config: Mapping[str, Any],
    *,
    source_symbol: str,
    target_session: date,
    sessions: Sequence[date],
    source_capture_id: int,
    price_coordinate: str,
    cutoff: date,
    starts_at_target: bool = False,
) -> dict[str, Any]:
    """按显式捕获、坐标、官方会话和截止日分页前的历史价格事实。"""
    _source_code_value, normalized_symbol = _normalized_code(source_symbol)
    if (
        type(target_session) is not date
        or type(cutoff) is not date
        or target_session > cutoff
        or type(source_capture_id) is not int
        or source_capture_id <= 0
    ):
        raise BaoStockPriceFactsError(
            "PRICE_READ_SCOPE_INVALID", "历史读取键或截止日无效"
        )
    target_sessions = _date_list(sessions, label="历史读取会话")
    if type(starts_at_target) is not bool or any(
        item > cutoff for item in target_sessions
    ):
        raise BaoStockPriceFactsError(
            "PRICE_READ_CUTOFF_EXCEEDED", "历史读取会话超过目标日或研究截止日"
        )
    if target_session not in target_sessions:
        raise BaoStockPriceFactsError(
            "PRICE_READ_TARGET_MISSING", "显式历史会话列表必须包含目标会话"
        )
    if starts_at_target:
        if target_sessions[0] != target_session or any(
            item < target_session for item in target_sessions
        ):
            raise BaoStockPriceFactsError(
                "PRICE_READ_WINDOW_INVALID", "后续价格读取须从目标会话开始并保持递增"
            )
    elif target_sessions[-1] != target_session or any(
        item > target_session for item in target_sessions
    ):
        raise BaoStockPriceFactsError(
            "PRICE_READ_WINDOW_INVALID", "历史价格读取须以目标会话结束并保持递增"
        )
    rows = _read_price_rows(
        config,
        source_symbol=normalized_symbol,
        source_capture_id=source_capture_id,
        sessions=target_sessions,
        cutoff=cutoff,
        price_coordinate=price_coordinate,
    )
    rows_by_session = {row["session_date"].isoformat(): row for row in rows}
    missing_sessions = [
        session.isoformat()
        for session in target_sessions
        if session.isoformat() not in rows_by_session
    ]
    return {
        "status": "NOT_COMPUTED"
        if not rows
        else "INCOMPLETE"
        if missing_sessions
        else "AVAILABLE",
        "source_symbol": normalized_symbol,
        "source_capture_id": source_capture_id,
        "price_coordinate": price_coordinate,
        "target_session": target_session.isoformat(),
        "cutoff": cutoff.isoformat(),
        "rows": rows,
        "row_count": len(rows),
        "expected_session_count": len(target_sessions),
        "window_direction": "STARTS_AT_TARGET"
        if starts_at_target
        else "ENDS_AT_TARGET",
        "missing_sessions": missing_sessions,
        "database_write_attempted": False,
        "source_io_attempted": False,
    }


def read_baostock_price_history_batch(
    config: Mapping[str, Any],
    *,
    source_symbols: Sequence[str],
    sessions: Sequence[date],
    source_capture_id: int,
    price_coordinate: str,
    cutoff: date,
    offset: int = 0,
    limit: int = 1000,
) -> dict[str, Any]:
    """有界分页读取指定BaoStock捕获的多证券双坐标历史。"""
    if (
        type(cutoff) is not date
        or type(source_capture_id) is not int
        or source_capture_id <= 0
    ):
        raise BaoStockPriceFactsError(
            "PRICE_READ_SCOPE_INVALID", "批量历史缺少捕获或研究截止"
        )
    if price_coordinate not in (RAW_COORDINATE, QFQ_COORDINATE):
        raise BaoStockPriceFactsError(
            "PRICE_COORDINATE_INVALID", "批量历史价格坐标无效"
        )
    if type(offset) is not int or not 0 <= offset <= 2**31 - 1:
        raise BaoStockPriceFactsError("PRICE_PAGE_INVALID", "批量历史分页offset无效")
    if type(limit) is not int or not 1 <= limit <= 5000:
        raise BaoStockPriceFactsError(
            "PRICE_PAGE_INVALID", "批量历史分页limit须为1至5000"
        )
    if isinstance(source_symbols, (str, bytes)) or not isinstance(
        source_symbols, Sequence
    ):
        raise BaoStockPriceFactsError(
            "PRICE_SYMBOL_SCOPE_INVALID", "证券范围必须为显式列表"
        )
    symbols = tuple(sorted({_normalized_code(value)[1] for value in source_symbols}))
    if not symbols or len(symbols) != len(source_symbols):
        raise BaoStockPriceFactsError(
            "PRICE_SYMBOL_SCOPE_INVALID", "证券范围为空或包含重复代码"
        )
    source_codes = tuple(symbol.lower() for symbol in symbols)
    target_sessions = _date_list(sessions, label="批量历史官方会话")
    if any(session > cutoff for session in target_sessions):
        raise BaoStockPriceFactsError(
            "PRICE_READ_CUTOFF_EXCEEDED", "批量历史会话超过截止日"
        )
    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _connection,
        cursor,
        _scope,
    ):
        _require_price_field_quality_migration(cursor)
        cursor.execute(
            """SELECT requested_symbol_count
               FROM stock.source_captures
               WHERE capture_id=%s AND provider='BAOSTOCK'
                 AND range_end_session<=%s""",
            (source_capture_id, cutoff),
        )
        capture = cursor.fetchone()
        if capture is None:
            coverage = [
                {
                    "session_date": session.isoformat(),
                    "source_capture_id": source_capture_id,
                    "price_coordinate": price_coordinate,
                    "expected_symbol_count": None,
                    "completed_request_count": 0,
                    "unregistered_symbol_count": len(symbols),
                    "row_count": 0,
                    "symbol_count": 0,
                    "quality_row_count": 0,
                    "observed_value_row_count": 0,
                    "coverage_status": "NOT_COMPUTED",
                }
                for session in target_sessions
            ]
            return {
                "status": "NOT_COMPUTED",
                "source_capture_id": source_capture_id,
                "price_coordinate": price_coordinate,
                "target_sessions": [item.isoformat() for item in target_sessions],
                "cutoff": cutoff.isoformat(),
                "rows": [],
                "coverage": coverage,
                "page": {
                    "offset": offset,
                    "limit": limit,
                    "count": 0,
                    "total": 0,
                    "next_offset": None,
                },
                "database_write_attempted": False,
                "source_io_attempted": False,
            }
        cursor.execute(
            """SELECT count(*)::bigint
               FROM stock.source_raw_daily d
               JOIN stock.source_captures c ON c.capture_id=d.source_capture_id
               JOIN stock.source_baostock_unit_contracts u
                 ON u.unit_contract_version=d.unit_contract_version
               JOIN stock.source_capture_requests rr
                 ON rr.capture_id=d.source_capture_id AND rr.source_code=d.source_code
                AND rr.adjustment_flag='3'
               JOIN stock.source_capture_requests qr
                 ON qr.capture_id=d.source_capture_id AND qr.source_code=d.source_code
                AND qr.adjustment_flag='2'
               WHERE d.source_capture_id=%s AND d.source_symbol=ANY(%s::text[])
                 AND d.session_date=ANY(%s::date[]) AND d.session_date<=%s
                 AND c.provider='BAOSTOCK' AND c.range_end_session<=%s
                 AND c.price_coordinates @> ARRAY[
                     'RAW_UNADJUSTED','FORWARD_ADJUSTED'
                 ]::text[]""",
            (
                source_capture_id,
                list(symbols),
                list(target_sessions),
                cutoff,
                cutoff,
            ),
        )
        total = int(cursor.fetchone()[0])
        cursor.execute(
            _PRICE_READ_SELECT
            + " AND d.source_capture_id=%s"
            + " AND d.source_symbol=ANY(%s::text[])"
            + " AND d.session_date=ANY(%s::date[])"
            + " ORDER BY d.session_date,d.source_symbol OFFSET %s LIMIT %s",
            (
                cutoff,
                cutoff,
                source_capture_id,
                list(symbols),
                list(target_sessions),
                offset,
                limit,
            ),
        )
        names = [column.name for column in cursor.description]
        rows = [
            _price_row_payload(dict(zip(names, values, strict=True)), price_coordinate)
            for values in cursor.fetchall()
        ]
        cursor.execute(
            """WITH request_codes AS (
                   SELECT requested.source_code,
                          count(request.capture_id)::bigint AS request_count,
                          count(DISTINCT request.adjustment_flag)::bigint
                              AS coordinate_count,
                          count(DISTINCT request.eligible_start_session)::bigint
                              AS start_count,
                          count(DISTINCT request.eligible_end_session)::bigint
                              AS end_count,
                          bool_and(
                              request.request_status IN ('COMPLETED','NO_DATA')
                              AND (request.request_status='NO_DATA'
                                   OR request.completed_through_session>=request.range_end_session)
                          ) AS complete,
                          min(request.eligible_start_session) AS eligible_start,
                          max(request.eligible_end_session) AS eligible_end
                   FROM unnest(%s::text[]) AS requested(source_code)
                   LEFT JOIN stock.source_capture_requests request
                     ON request.capture_id=%s
                    AND request.source_code=requested.source_code
                    AND request.range_end_session<=%s
                   GROUP BY requested.source_code
               ), eligible AS (
                   SELECT d.session_date,
                          count(*) FILTER (
                              WHERE r.request_count=2 AND r.coordinate_count=2
                                AND r.start_count=1 AND r.end_count=1
                                AND d.session_date BETWEEN r.eligible_start AND r.eligible_end
                          )::bigint AS expected_symbols,
                          count(*) FILTER (
                              WHERE r.request_count=2 AND r.coordinate_count=2
                                AND r.start_count=1 AND r.end_count=1
                                AND d.session_date BETWEEN r.eligible_start AND r.eligible_end
                                AND r.complete
                          )::bigint AS completed_requests,
                          count(*) FILTER (
                              WHERE r.request_count<>2 OR r.coordinate_count<>2
                                OR r.start_count<>1 OR r.end_count<>1
                          )::bigint AS unregistered_symbols
                   FROM (SELECT unnest(%s::date[]) AS session_date) d
                   CROSS JOIN request_codes r
                   GROUP BY d.session_date
               ), stored AS (
                   SELECT session_date,count(*)::bigint AS row_count,
                          count(DISTINCT source_symbol)::bigint AS symbol_count,
                          count(*) FILTER (WHERE price_quality IS NOT NULL)::bigint AS raw_quality_rows,
                          count(*) FILTER (WHERE price_quality='OBSERVED')::bigint AS raw_observed_rows,
                          count(*) FILTER (WHERE qfq_price_quality IS NOT NULL)::bigint AS qfq_quality_rows,
                          count(*) FILTER (WHERE qfq_price_quality='OBSERVED')::bigint AS qfq_observed_rows
                   FROM stock.source_raw_daily
                   WHERE source_capture_id=%s AND source_symbol=ANY(%s::text[])
                     AND session_date=ANY(%s::date[]) AND session_date<=%s
                   GROUP BY session_date
               )
                SELECT e.session_date,e.expected_symbols,e.completed_requests,
                       e.unregistered_symbols,
                      coalesce(s.row_count,0),coalesce(s.symbol_count,0),
                      coalesce(s.raw_quality_rows,0),coalesce(s.raw_observed_rows,0),
                      coalesce(s.qfq_quality_rows,0),coalesce(s.qfq_observed_rows,0)
               FROM eligible e LEFT JOIN stored s USING (session_date)
               ORDER BY e.session_date""",
            (
                list(source_codes),
                source_capture_id,
                cutoff,
                list(target_sessions),
                source_capture_id,
                list(symbols),
                list(target_sessions),
                cutoff,
            ),
        )
        coverage = []
        for row in cursor.fetchall():
            (
                session,
                expected,
                completed,
                unregistered,
                row_count,
                symbol_count,
                raw_quality,
                raw_observed,
                qfq_quality,
                qfq_observed,
            ) = row
            quality_count = int(
                raw_quality if price_coordinate == RAW_COORDINATE else qfq_quality
            )
            complete_rows = (
                int(unregistered) == 0
                and int(expected) == int(completed)
                and int(row_count) == int(expected)
                and int(symbol_count) == int(row_count)
                and quality_count == int(row_count)
            )
            coverage.append(
                {
                    "session_date": session.isoformat(),
                    "source_capture_id": source_capture_id,
                    "price_coordinate": price_coordinate,
                    "expected_symbol_count": int(expected),
                    "completed_request_count": int(completed),
                    "unregistered_symbol_count": int(unregistered),
                    "row_count": int(row_count),
                    "symbol_count": int(symbol_count),
                    "quality_row_count": quality_count,
                    "observed_value_row_count": int(
                        raw_observed
                        if price_coordinate == RAW_COORDINATE
                        else qfq_observed
                    ),
                    "coverage_status": "COMPLETE" if complete_rows else "INCOMPLETE",
                }
            )
    next_offset = offset + len(rows) if offset + len(rows) < total else None
    return {
        "status": "AVAILABLE"
        if all(item["coverage_status"] == "COMPLETE" for item in coverage)
        else "INCOMPLETE",
        "source_capture_id": source_capture_id,
        "price_coordinate": price_coordinate,
        "target_sessions": [item.isoformat() for item in target_sessions],
        "cutoff": cutoff.isoformat(),
        "rows": rows,
        "coverage": coverage,
        "page": {
            "offset": offset,
            "limit": limit,
            "count": len(rows),
            "total": total,
            "next_offset": next_offset,
        },
        "database_write_attempted": False,
        "source_io_attempted": False,
    }


def read_baostock_price_page(
    config: Mapping[str, Any],
    *,
    target_session: date,
    cutoff: date,
    source_capture_id: int | None,
    price_coordinate: str,
    offset: int,
    limit: int,
    market: str,
    board: str,
    allow_full_pool_page: bool = False,
) -> dict[str, Any]:
    """按真实BaoStock捕获和所选坐标读取一页价格候选。"""
    maximum_page_size = 5000 if allow_full_pool_page is True else 100
    if (
        type(target_session) is not date
        or type(cutoff) is not date
        or target_session > cutoff
        or type(offset) is not int
        or offset < 0
        or type(limit) is not int
        or not 1 <= limit <= maximum_page_size
        or type(allow_full_pool_page) is not bool
        or allow_full_pool_page
        and (
            type(source_capture_id) is not int
            or source_capture_id <= 0
            or market != "ALL"
            or board != "ALL"
        )
        or market not in {"ALL", "SH", "SZ", "BJ"}
        or board not in {"ALL", "MAIN_BOARD", "GEM", "STAR_BOARD", "BEIJING_BOARD"}
        or price_coordinate not in (RAW_COORDINATE, QFQ_COORDINATE)
    ):
        raise BaoStockPriceFactsError(
            "PRICE_PAGE_SCOPE_INVALID",
            "价格候选分页参数无效；普通页最多100行，全池页须绑定捕获并读取全部市场与板别",
        )
    catalog = read_baostock_price_catalog(config, cutoff=cutoff)
    versions = [
        item
        for item in catalog["price_coordinate_versions"]
        if item["session_date"] == target_session.isoformat()
        and item["price_coordinate"] == price_coordinate
        and (
            source_capture_id is None or item["source_capture_id"] == source_capture_id
        )
    ]
    if not versions:
        return {
            "status": "EMPTY",
            "target_session": target_session.isoformat(),
            "source_capture_id": source_capture_id,
            "price_coordinate": price_coordinate,
            "version_status": "EMPTY",
            "version_row_count": 0,
            "version_expected_row_count": 0,
            "offset": offset,
            "limit": limit,
            "page_count": 0,
            "total_count": 0,
            "next_offset": None,
            "bar_status_counts": {},
            "partition_closed": False,
            "rows": [],
        }
    capture_ids = {item["source_capture_id"] for item in versions}
    if len(capture_ids) != 1:
        return {
            "status": "AMBIGUOUS_CAPTURE",
            "target_session": target_session.isoformat(),
            "candidate_source_capture_ids": sorted(capture_ids),
            "price_coordinate": price_coordinate,
            "version_status": "AMBIGUOUS",
            "rows": [],
        }
    capture_id = next(iter(capture_ids))
    version = versions[0]
    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _connection,
        cursor,
        _scope,
    ):
        _require_price_field_quality_migration(cursor)
        where_market = "(%s='ALL' OR d.market=%s)"
        where_board = "(%s='ALL' OR d.board=%s)"
        status_column = (
            "bar_status" if price_coordinate == RAW_COORDINATE else "qfq_bar_status"
        )
        cursor.execute(
            f"""SELECT d.{status_column},count(*)::bigint
               FROM stock.source_raw_daily d
               WHERE d.source_capture_id=%s AND d.session_date=%s
                 AND (%s='ALL' OR d.market=%s)
                 AND (%s='ALL' OR d.board=%s)
                GROUP BY d.{status_column} ORDER BY d.{status_column}""",
            (capture_id, target_session, market, market, board, board),
        )
        status_counts = {status: int(count) for status, count in cursor.fetchall()}
        total = sum(status_counts.values())
        cursor.execute(
            _PRICE_READ_SELECT
            + f" AND d.session_date=%s AND d.source_capture_id=%s AND {where_market} AND {where_board}"
            + " ORDER BY d.source_symbol LIMIT %s OFFSET %s",
            (
                cutoff,
                cutoff,
                target_session,
                capture_id,
                market,
                market,
                board,
                board,
                limit,
                offset,
            ),
        )
        names = [column.name for column in cursor.description]
        rows = [dict(zip(names, values, strict=True)) for values in cursor.fetchall()]
    selected_rows = [_price_row_payload(row, price_coordinate) for row in rows]
    return {
        "status": "AVAILABLE" if selected_rows else "EMPTY_PAGE",
        "target_session": target_session.isoformat(),
        "source_capture_id": capture_id,
        "price_coordinate": price_coordinate,
        "offset": offset,
        "limit": limit,
        "page_count": len(selected_rows),
        "total_count": total,
        "next_offset": offset + len(selected_rows)
        if offset + len(selected_rows) < total
        else None,
        "bar_status_counts": status_counts,
        "partition_closed": version["coverage_status"] == "COMPLETE",
        "version_status": "CLOSED"
        if version["coverage_status"] == "COMPLETE"
        else "INCOMPLETE",
        "version_row_count": version["row_count"],
        "version_expected_row_count": version["expected_row_count"],
        "market_filter": market,
        "board_filter": board,
        "rows": selected_rows,
        "database_write_attempted": False,
        "source_io_attempted": False,
    }


def resolve_baostock_stock_code(
    config: Mapping[str, Any],
    *,
    stock_code: str,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int | None = None,
) -> dict[str, Any]:
    """在精确paired BaoStock捕获中按六位代码唯一解析来源代码。"""
    if (
        not isinstance(stock_code, str)
        or not stock_code.isdigit()
        or len(stock_code) != 6
        or type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
        or (
            source_capture_id is not None
            and (type(source_capture_id) is not int or source_capture_id <= 0)
        )
    ):
        raise BaoStockPriceFactsError(
            "STOCK_CODE_INVALID", "股票代码或精确捕获查询键无效"
        )
    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _connection,
        cursor,
        _scope,
    ):
        cursor.execute(
            """SELECT DISTINCT d.source_symbol,d.source_capture_id
               FROM stock.source_raw_daily d
               JOIN stock.source_captures c ON c.capture_id=d.source_capture_id
               WHERE d.session_date=%s AND d.session_date<=%s
                 AND c.range_end_session<=%s AND c.provider='BAOSTOCK'
                 AND c.price_coordinates @> ARRAY['RAW_UNADJUSTED','FORWARD_ADJUSTED']::text[]
                 AND right(d.source_symbol,6)=%s
                 AND (%s::bigint IS NULL OR d.source_capture_id=%s)
               ORDER BY d.source_symbol,d.source_capture_id LIMIT 3""",
            (
                target_session,
                research_cutoff,
                research_cutoff,
                stock_code,
                source_capture_id,
                source_capture_id,
            ),
        )
        matches = cursor.fetchall()
    if not matches:
        raise BaoStockPriceFactsError(
            "STOCK_CODE_NOT_FOUND", "该BaoStock捕获中没有此六位代码"
        )
    symbols = {row[0] for row in matches}
    capture_ids = {int(row[1]) for row in matches}
    if len(symbols) != 1 or len(capture_ids) != 1:
        raise BaoStockPriceFactsError(
            "STOCK_CODE_AMBIGUOUS", "六位代码或价格捕获对应多个真实来源键"
        )
    return {
        "stock_code": stock_code,
        "source_symbol": next(iter(symbols)),
        "source_capture_id": next(iter(capture_ids)),
        "target_session": target_session.isoformat(),
        "resolution_scope": "EXACT_BAOSTOCK_PAIRED_CAPTURE",
        "price_coordinates": [RAW_COORDINATE, QFQ_COORDINATE],
        "database_write_attempted": False,
        "source_io_attempted": False,
    }


def read_baostock_price_detail(
    config: Mapping[str, Any],
    *,
    source_symbol: str,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int | None = None,
) -> dict[str, Any]:
    """读取同一真实capture内RAW/QFQ配对的单股日线详情。"""
    _source_code_value, normalized_symbol = _normalized_code(source_symbol)
    if type(target_session) is not date or type(research_cutoff) is not date:
        raise BaoStockPriceFactsError(
            "PRICE_READ_SCOPE_INVALID", "详情读取必须绑定目标日和截止日"
        )
    if target_session > research_cutoff:
        raise BaoStockPriceFactsError(
            "PRICE_READ_CUTOFF_EXCEEDED", "详情目标日超过研究截止日"
        )
    if source_capture_id is not None and (
        type(source_capture_id) is not int or source_capture_id <= 0
    ):
        raise BaoStockPriceFactsError("CAPTURE_ID_INVALID", "来源捕获数字键无效")
    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _connection,
        cursor,
        _scope,
    ):
        _require_price_field_quality_migration(cursor)
        cursor.execute(
            """SELECT DISTINCT d.source_capture_id
               FROM stock.source_raw_daily d
               JOIN stock.source_captures c ON c.capture_id=d.source_capture_id
               JOIN stock.source_capture_requests rr
                 ON rr.capture_id=d.source_capture_id AND rr.source_code=d.source_code
                AND rr.adjustment_flag='3'
               JOIN stock.source_capture_requests qr
                 ON qr.capture_id=d.source_capture_id AND qr.source_code=d.source_code
                AND qr.adjustment_flag='2'
               WHERE d.source_symbol=%s AND d.session_date=%s
                 AND d.session_date<=%s AND c.range_end_session<=%s
                 AND c.provider='BAOSTOCK'
                 AND c.price_coordinates @> ARRAY['RAW_UNADJUSTED','FORWARD_ADJUSTED']::text[]
                 AND (%s::bigint IS NULL OR d.source_capture_id=%s)
               ORDER BY d.source_capture_id""",
            (
                normalized_symbol,
                target_session,
                research_cutoff,
                research_cutoff,
                source_capture_id,
                source_capture_id,
            ),
        )
        capture_ids = [int(row[0]) for row in cursor.fetchall()]
    if not capture_ids:
        return {
            "status": "NOT_COMPUTED",
            "source_symbol": normalized_symbol,
            "target_session": target_session.isoformat(),
            "research_cutoff": research_cutoff.isoformat(),
            "source_capture_id": source_capture_id,
            "row": None,
            "database_write_attempted": False,
            "source_io_attempted": False,
        }
    if len(capture_ids) != 1:
        return {
            "status": "AMBIGUOUS_CAPTURE",
            "source_symbol": normalized_symbol,
            "target_session": target_session.isoformat(),
            "research_cutoff": research_cutoff.isoformat(),
            "candidate_source_capture_ids": capture_ids,
            "row": None,
            "database_write_attempted": False,
            "source_io_attempted": False,
        }
    capture_id = capture_ids[0]
    rows = _read_price_rows(
        config,
        source_symbol=normalized_symbol,
        source_capture_id=capture_id,
        sessions=(target_session,),
        cutoff=research_cutoff,
        price_coordinate=RAW_COORDINATE,
    )
    return {
        "status": "AVAILABLE" if rows else "NOT_COMPUTED",
        "source_symbol": normalized_symbol,
        "target_session": target_session.isoformat(),
        "research_cutoff": research_cutoff.isoformat(),
        "source_capture_id": capture_id,
        "row": rows[0] if rows else None,
        "database_write_attempted": False,
        "source_io_attempted": False,
    }


def _basic_name_content_matches(
    existing_row: Sequence[Any], expected_item: Mapping[str, Any]
) -> bool:
    return len(existing_row) > 2 and existing_row[2] == expected_item.get(
        "display_name"
    )


def persist_baostock_basic_names(
    config: Mapping[str, Any],
    *,
    source_capture_id: int,
    names: Sequence[Mapping[str, Any]],
    captured_at: datetime,
) -> dict[str, Any]:
    """以同次BaoStock基本资料捕获替换当前code_name显示快照。"""
    if type(source_capture_id) is not int or source_capture_id <= 0:
        raise BaoStockPriceFactsError("CAPTURE_ID_INVALID", "名称捕获数字键无效")
    captured = _aware(captured_at, "基本资料捕获时间")
    if isinstance(names, (str, bytes)) or not isinstance(names, Sequence) or not names:
        raise BaoStockPriceFactsError(
            "BASIC_NAMES_INVALID", "query_stock_basic名称必须为非空行列表"
        )
    normalized_rows: dict[str, dict[str, Any]] = {}
    for row in names:
        if not isinstance(row, Mapping):
            raise BaoStockPriceFactsError(
                "BASIC_NAMES_INVALID", "基本资料名称行必须为字段映射"
            )
        code = row.get("code", row.get("source_code"))
        symbol_value = normalize_baostock_symbol(code)
        code_name = row.get("code_name")
        if symbol_value is None or not isinstance(code_name, str):
            raise BaoStockPriceFactsError(
                "BASIC_NAMES_INVALID", "基本资料缺少有效code或code_name"
            )
        display_name = code_name.strip()
        if not display_name or len(display_name) > 128:
            raise BaoStockPriceFactsError(
                "BASIC_NAMES_INVALID", "code_name为空或超出保存长度"
            )
        if symbol_value in normalized_rows:
            raise BaoStockPriceFactsError(
                "BASIC_NAMES_DUPLICATE", f"基本资料代码重复：{symbol_value}"
            )
        market, _board = _board_for_symbol(symbol_value)
        normalized_rows[symbol_value] = {
            "source_symbol": symbol_value,
            "market": market,
            "display_name": display_name,
            "source_file": _NAME_SOURCE_FILE,
            "parser_contract": _NAME_PARSER_CONTRACT,
            "source_authority": "BAOSTOCK_PUBLIC_API",
            "source_time_quality": "CAPTURED_NON_PIT",
            "source_clock_role": "BAOSTOCK_QUERY_STOCK_BASIC_CAPTURED_NON_PIT",
            "source_available_at": None,
            "captured_at": captured,
            "identity_status": "SOURCE_CODE_NAME_NOT_STABLE_ENTITY",
            "pit_status": "NON_PIT",
            "source_capture_id": source_capture_id,
        }

    with source_raw._transaction(config, write=True, statement_timeout_ms=30000) as (
        _connection,
        cursor,
        _scope,
    ):
        cursor.execute(
            """SELECT provider,capture_uuid FROM stock.source_captures
               WHERE capture_id=%s FOR SHARE""",
            (source_capture_id,),
        )
        capture = cursor.fetchone()
        if (
            capture is None
            or capture[0] != "BAOSTOCK"
            or not isinstance(capture[1], UUID)
        ):
            raise BaoStockPriceFactsError(
                "BASIC_NAMES_CAPTURE_INVALID", "code_name必须绑定真实BaoStock来源捕获"
            )
        cursor.execute(
            """SELECT source_symbol,market,display_name,source_file,parser_contract,
                      source_authority,source_time_quality,source_clock_role,
                      source_available_at,captured_at,identity_status,pit_status,
                      source_capture_id
               FROM stock.source_symbol_display_names
               WHERE source_capture_id=%s ORDER BY source_symbol""",
            (source_capture_id,),
        )
        existing_rows = cursor.fetchall()
        expected_items = sorted(
            normalized_rows.values(), key=lambda item: item["source_symbol"]
        )
        if existing_rows:
            existing_by_symbol = {str(row[0]): row for row in existing_rows}
            expected_by_symbol = {
                item["source_symbol"]: item for item in expected_items
            }
            same_scope = (
                len(existing_by_symbol) == len(expected_by_symbol)
                and set(existing_by_symbol) == set(expected_by_symbol)
                and all(
                    _basic_name_content_matches(
                        existing_by_symbol[symbol], expected_by_symbol[symbol]
                    )
                    and existing_by_symbol[symbol][1]
                    == expected_by_symbol[symbol]["market"]
                    and existing_by_symbol[symbol][3]
                    == expected_by_symbol[symbol]["source_file"]
                    and existing_by_symbol[symbol][4]
                    == expected_by_symbol[symbol]["parser_contract"]
                    and existing_by_symbol[symbol][5]
                    == expected_by_symbol[symbol]["source_authority"]
                    and existing_by_symbol[symbol][6]
                    == expected_by_symbol[symbol]["source_time_quality"]
                    and existing_by_symbol[symbol][7]
                    == expected_by_symbol[symbol]["source_clock_role"]
                    and existing_by_symbol[symbol][8]
                    == expected_by_symbol[symbol]["source_available_at"]
                    and existing_by_symbol[symbol][10]
                    == expected_by_symbol[symbol]["identity_status"]
                    and existing_by_symbol[symbol][11]
                    == expected_by_symbol[symbol]["pit_status"]
                    and existing_by_symbol[symbol][12]
                    == expected_by_symbol[symbol]["source_capture_id"]
                    for symbol in expected_by_symbol
                )
            )
            if same_scope:
                return {
                    "status": "ALREADY_COMPLETE",
                    "source_capture_id": source_capture_id,
                    "stored_name_count": len(existing_rows),
                    "inserted_rows": 0,
                    "content_reused": True,
                    "name_revision_status": "FIRST_CAPTURE_PRESERVED",
                    "database_write_attempted": False,
                    "source_io_attempted": False,
                }
            raise BaoStockPriceFactsError(
                "BASIC_NAMES_CAPTURE_CONFLICT",
                "同一BaoStock基本资料捕获已保存不同code_name内容",
            )
        cursor.execute("DELETE FROM stock.source_symbol_display_names")
        insert_sql = """INSERT INTO stock.source_symbol_display_names
            (source_symbol,market,display_name,source_file,parser_contract,
             source_authority,source_time_quality,source_clock_role,source_available_at,
             captured_at,identity_status,pit_status,source_capture_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        insert_values = [
            tuple(
                item[name]
                for name in (
                    "source_symbol",
                    "market",
                    "display_name",
                    "source_file",
                    "parser_contract",
                    "source_authority",
                    "source_time_quality",
                    "source_clock_role",
                    "source_available_at",
                    "captured_at",
                    "identity_status",
                    "pit_status",
                    "source_capture_id",
                )
            )
            for item in expected_items
        ]
        cursor.executemany(insert_sql, insert_values)
        cursor.execute(
            """SELECT source_symbol,market,display_name,source_file,parser_contract,
                      source_authority,source_time_quality,source_clock_role,
                      source_available_at,captured_at,identity_status,pit_status,
                      source_capture_id
               FROM stock.source_symbol_display_names
               WHERE source_capture_id=%s ORDER BY source_symbol""",
            (source_capture_id,),
        )
        readback = cursor.fetchall()
        if len(readback) != len(expected_items) or any(
            readback[index]
            != tuple(
                item[name]
                for name in (
                    "source_symbol",
                    "market",
                    "display_name",
                    "source_file",
                    "parser_contract",
                    "source_authority",
                    "source_time_quality",
                    "source_clock_role",
                    "source_available_at",
                    "captured_at",
                    "identity_status",
                    "pit_status",
                    "source_capture_id",
                )
            )
            for index, item in enumerate(expected_items)
        ):
            raise BaoStockPriceFactsError(
                "BASIC_NAMES_READBACK_MISMATCH", "BaoStock code_name快照未完整读回"
            )
    return {
        "status": "COMPLETED",
        "source_capture_id": source_capture_id,
        "stored_name_count": len(expected_items),
        "inserted_rows": len(expected_items),
        "database_write_attempted": True,
        "source_io_attempted": False,
    }


def read_baostock_basic_names_for_capture(
    config: Mapping[str, Any], *, source_capture_id: int
) -> dict[str, Any]:
    """读取已持久化且绑定BaoStock捕获的基本资料显示名快照。"""
    if type(source_capture_id) is not int or source_capture_id <= 0:
        raise BaoStockPriceFactsError(
            "BASIC_NAMES_CAPTURE_ID_INVALID", "基本资料读取必须绑定正捕获ID"
        )
    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _connection,
        cursor,
        _scope,
    ):
        cursor.execute(
            """SELECT requested_symbol_count FROM stock.source_captures
               WHERE capture_id=%s AND provider='BAOSTOCK'""",
            (source_capture_id,),
        )
        capture = cursor.fetchone()
        if capture is None or type(capture[0]) is not int or capture[0] <= 0:
            raise BaoStockPriceFactsError(
                "BASIC_NAMES_CAPTURE_NOT_FOUND", "BaoStock名称捕获不存在或证券数无效"
            )
        cursor.execute(
            """SELECT source_symbol,market,display_name,source_file,parser_contract,
                      source_authority,source_time_quality,source_clock_role,
                      source_available_at,captured_at,identity_status,pit_status,
                      source_capture_id
               FROM stock.source_symbol_display_names
               WHERE source_capture_id=%s ORDER BY source_symbol""",
            (source_capture_id,),
        )
        rows = cursor.fetchall()
    if len(rows) != capture[0]:
        raise BaoStockPriceFactsError(
            "BASIC_NAMES_CAPTURE_INCOMPLETE",
            "已存BaoStock基本资料名称数与捕获证券数不一致",
        )
    expected_metadata = (
        _NAME_SOURCE_FILE,
        _NAME_PARSER_CONTRACT,
        "BAOSTOCK_PUBLIC_API",
        "CAPTURED_NON_PIT",
        "BAOSTOCK_QUERY_STOCK_BASIC_CAPTURED_NON_PIT",
        None,
        "SOURCE_CODE_NAME_NOT_STABLE_ENTITY",
        "NON_PIT",
    )
    names: list[dict[str, str]] = []
    symbols: list[str] = []
    captured_times = set()
    for row in rows:
        (
            source_symbol,
            market,
            display_name,
            source_file,
            parser_contract,
            source_authority,
            source_time_quality,
            source_clock_role,
            source_available_at,
            captured_at,
            identity_status,
            pit_status,
            stored_capture_id,
        ) = row
        parsed_symbol = normalize_baostock_symbol(source_symbol)
        board_identity = (
            _board_for_symbol(parsed_symbol) if parsed_symbol is not None else None
        )
        if (
            parsed_symbol is None
            or type(stored_capture_id) is not int
            or stored_capture_id != source_capture_id
            or board_identity is None
            or market != board_identity[0]
            or not isinstance(display_name, str)
            or not display_name.strip()
            or (
                source_file,
                parser_contract,
                source_authority,
                source_time_quality,
                source_clock_role,
                source_available_at,
                identity_status,
                pit_status,
            )
            != expected_metadata
            or not isinstance(captured_at, datetime)
            or captured_at.tzinfo is None
            or captured_at.utcoffset() is None
        ):
            raise BaoStockPriceFactsError(
                "BASIC_NAMES_CAPTURE_ROW_INVALID",
                "已存BaoStock基本资料名称行身份、NON_PIT语义或捕获时钟无效",
            )
        symbols.append(parsed_symbol)
        names.append({"code": parsed_symbol.lower(), "code_name": display_name})
        captured_times.add(captured_at)
    if len(symbols) != len(set(symbols)) or len(captured_times) != 1:
        raise BaoStockPriceFactsError(
            "BASIC_NAMES_CAPTURE_PARTITION_INVALID",
            "已存BaoStock名称快照包含重复代码或多个捕获时钟",
        )
    return {
        "status": "AVAILABLE",
        "source_capture_id": source_capture_id,
        "source_symbols": tuple(symbols),
        "names": tuple(names),
        "captured_at": next(iter(captured_times)),
        "stored_name_count": len(names),
        "source_network_called": False,
        "database_write_attempted": False,
    }


def read_baostock_price_catalog(
    config: Mapping[str, Any],
    *,
    cutoff: date,
    source_capture_id: int | None = None,
) -> dict[str, Any]:
    """按捕获×坐标×会话返回BaoStock价格事实闭合分母与行数。"""
    if type(cutoff) is not date:
        raise BaoStockPriceFactsError(
            "CUTOFF_REQUIRED", "价格目录读取必须绑定研究截止日"
        )
    if source_capture_id is not None and (
        type(source_capture_id) is not int or source_capture_id <= 0
    ):
        raise BaoStockPriceFactsError("CAPTURE_ID_INVALID", "来源捕获数字键无效")
    capture_filter = "" if source_capture_id is None else " AND d.source_capture_id=%s"
    parameters: tuple[Any, ...] = (cutoff, cutoff, cutoff)
    if source_capture_id is not None:
        parameters += (source_capture_id,)
    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _connection,
        cursor,
        _scope,
    ):
        cursor.execute(
            """WITH request_codes AS (
                   SELECT capture_id,source_code,
                          count(*)=2
                            AND count(DISTINCT adjustment_flag)=2
                            AND count(DISTINCT eligible_start_session)=1
                            AND count(DISTINCT eligible_end_session)=1
                            AND bool_and(
                                request_status IN ('COMPLETED','NO_DATA')
                                AND (request_status='NO_DATA'
                                     OR completed_through_session>=range_end_session)
                            ) AS code_requests_complete,
                          min(eligible_start_session) AS eligible_start_session,
                          max(eligible_end_session) AS eligible_end_session,
                          coalesce(sum(response_row_count),0)::bigint
                              AS response_row_count
                   FROM stock.source_capture_requests
                   WHERE range_end_session<=%s
                   GROUP BY capture_id,source_code
               ), price_sessions AS (
                SELECT DISTINCT d.session_date,d.source_capture_id,
                       c.requested_symbol_count
                   FROM stock.source_raw_daily d
                   JOIN stock.source_captures c ON c.capture_id=d.source_capture_id
                   WHERE d.session_date<=%s AND c.range_end_session<=%s
                     AND c.provider='BAOSTOCK'
                     AND c.price_coordinates @> ARRAY[
                         'RAW_UNADJUSTED','FORWARD_ADJUSTED'
                     ]::text[]
                     """
            + capture_filter
            + """
               ), request_coverage AS (
                   SELECT s.session_date,s.source_capture_id,
                           max(s.requested_symbol_count)::bigint
                               AS capture_requested_symbol_count,
                           count(DISTINCT r.source_code)::bigint
                               AS registered_symbol_count,
                           count(*) FILTER (
                              WHERE s.session_date BETWEEN r.eligible_start_session
                                                       AND r.eligible_end_session
                          )::bigint AS expected_symbol_count,
                          coalesce(bool_and(r.code_requests_complete) FILTER (
                              WHERE s.session_date BETWEEN r.eligible_start_session
                                                       AND r.eligible_end_session
                          ),FALSE) AS requests_complete,
                          coalesce(sum(r.response_row_count) FILTER (
                              WHERE s.session_date BETWEEN r.eligible_start_session
                                                       AND r.eligible_end_session
                          ),0)::bigint AS capture_request_response_row_count
                   FROM price_sessions s
                   JOIN request_codes r ON r.capture_id=s.source_capture_id
                   GROUP BY s.session_date,s.source_capture_id
               ), raw_rows AS (
                   SELECT d.session_date,d.source_capture_id,
                          count(*)::bigint AS row_count,
                          count(DISTINCT d.source_symbol)::bigint AS symbol_count,
                          count(*) FILTER (WHERE d.price_quality IS NOT NULL)::bigint
                              AS raw_quality_row_count,
                          count(*) FILTER (WHERE d.price_quality='OBSERVED')::bigint
                              AS raw_observed_row_count,
                          count(*) FILTER (WHERE d.qfq_price_quality IS NOT NULL)::bigint
                              AS qfq_quality_row_count,
                          count(*) FILTER (WHERE d.qfq_price_quality='OBSERVED')::bigint
                              AS qfq_observed_row_count
                   FROM stock.source_raw_daily d
                   JOIN price_sessions s
                     ON s.session_date=d.session_date
                    AND s.source_capture_id=d.source_capture_id
                   GROUP BY d.session_date,d.source_capture_id
               )
                SELECT d.session_date,d.source_capture_id,r.requests_complete,
                       r.capture_requested_symbol_count,r.registered_symbol_count,
                       r.expected_symbol_count,r.capture_request_response_row_count,
                      d.row_count,d.symbol_count,d.raw_quality_row_count,
                      d.raw_observed_row_count,d.qfq_quality_row_count,
                      d.qfq_observed_row_count
               FROM raw_rows d
               JOIN request_coverage r
                 ON r.session_date=d.session_date
                AND r.source_capture_id=d.source_capture_id
               ORDER BY d.session_date,d.source_capture_id""",
            parameters,
        )
        rows = cursor.fetchall()
    coordinate_versions: list[dict[str, Any]] = []
    capture_ids_by_session: dict[str, set[int]] = {}
    for row in rows:
        (
            session,
            capture_id,
            requests_complete,
            capture_requested,
            registered_symbols,
            expected_symbols,
            request_response_rows,
            row_count,
            symbol_count,
            raw_quality_count,
            raw_observed_count,
            qfq_quality_count,
            qfq_observed_count,
        ) = row
        session_text = session.isoformat()
        capture_id = int(capture_id)
        expected_symbols = int(expected_symbols)
        row_count = int(row_count)
        capture_requested = int(capture_requested)
        registered_symbols = int(registered_symbols)
        capture_ids_by_session.setdefault(session_text, set()).add(capture_id)
        rows_closed = (
            bool(requests_complete)
            and expected_symbols > 0
            and registered_symbols == capture_requested
            and capture_requested > 0
            and row_count == expected_symbols
            and int(symbol_count) == row_count
        )
        for coordinate, quality_count, observed_count in (
            (RAW_COORDINATE, int(raw_quality_count), int(raw_observed_count)),
            (QFQ_COORDINATE, int(qfq_quality_count), int(qfq_observed_count)),
        ):
            coordinate_versions.append(
                {
                    "session_date": session_text,
                    "source_capture_id": capture_id,
                    "price_coordinate": coordinate,
                    "coverage_status": (
                        "COMPLETE"
                        if rows_closed and quality_count == row_count
                        else "INCOMPLETE"
                    ),
                    "requests_complete": bool(requests_complete),
                    "capture_requested_symbol_count": capture_requested,
                    "registered_symbol_count": registered_symbols,
                    "row_count": row_count,
                    "expected_row_count": expected_symbols,
                    "symbol_count": int(symbol_count),
                    "quality_row_count": quality_count,
                    "observed_value_row_count": observed_count,
                    "capture_request_response_row_count": int(request_response_rows),
                }
            )
    closed_sessions = sorted(
        session
        for session, capture_ids in capture_ids_by_session.items()
        if len(capture_ids) == 1
        and all(
            item["coverage_status"] == "COMPLETE"
            for item in coordinate_versions
            if item["session_date"] == session
        )
    )
    ambiguous_sessions = sorted(
        session
        for session, capture_ids in capture_ids_by_session.items()
        if len(capture_ids) != 1 or session not in closed_sessions
    )
    return {
        "status": "AVAILABLE",
        "cutoff": cutoff.isoformat(),
        "source_capture_id": source_capture_id,
        "stored_session_count": len(closed_sessions),
        "first_stored_session": closed_sessions[0] if closed_sessions else None,
        "last_stored_session": closed_sessions[-1] if closed_sessions else None,
        "closed_sessions": closed_sessions,
        "ambiguous_sessions": ambiguous_sessions,
        "price_coordinate_versions": coordinate_versions,
        "database_write_attempted": False,
        "source_io_attempted": False,
    }
