"""D0来源股票原始场景的有界PostgreSQL只读读取。"""

from __future__ import annotations

from datetime import date
import re
import time
from typing import Any, Mapping, Sequence
from uuid import UUID

from . import source_raw


MAX_SOURCE_SYMBOLS = 10000
_SYMBOL = re.compile(r"^(SH|SZ|BJ)\.[0-9]{6}$")
_MAX_HISTORY_CELLS = MAX_SOURCE_SYMBOLS * 25


class D0RawPgError(ValueError):
    """封存来源或D0原始会话读取失败。"""

    preserve_transaction_error = True

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def _read_source_version(
    cursor: Any,
    *,
    target_session: date,
    source_capture_id: int,
    research_cutoff: date,
) -> dict[str, Any]:
    cursor.execute(
        """SELECT capture_id,provider,capture_uuid,range_start_session,
                  range_end_session,price_coordinates,parser_contract,
                  read_started_at,validated_at,
                  source_available_at,source_time_quality,source_clock_role,
                  normalized_count,requested_symbol_count,unit_contract_version
           FROM stock.source_captures WHERE capture_id=%s""",
        (source_capture_id,),
    )
    capture = cursor.fetchone()
    if capture is None:
        raise D0RawPgError(
            "D0_SOURCE_CAPTURE_NOT_FOUND", "指定的BaoStock来源捕获不存在。"
        )
    (
        capture_id,
        provider,
        capture_uuid,
        range_start,
        range_end,
        coordinates,
        parser_contract,
        capture_read_started_at,
        capture_validated_at,
        source_available_at,
        source_time_quality,
        source_clock_role,
        normalized_count,
        requested_symbol_count,
        unit_contract_version,
    ) = capture
    if (
        type(capture_id) is not int
        or capture_id != source_capture_id
        or provider != "BAOSTOCK"
        or not isinstance(capture_uuid, UUID)
        or type(range_start) is not date
        or type(range_end) is not date
        or not range_start <= target_session <= range_end <= research_cutoff
        or set(coordinates or ()) != {"RAW_UNADJUSTED", "FORWARD_ADJUSTED"}
        or not isinstance(parser_contract, str)
        or not parser_contract.strip()
        or type(normalized_count) is not int
        or normalized_count < 0
        or type(requested_symbol_count) is not int
        or requested_symbol_count < 1
        or source_time_quality != "NON_PIT_OBSERVED"
        or source_clock_role != "BAOSTOCK_QUERY_CAPTURED_NON_PIT"
        or source_available_at is not None
        or not isinstance(unit_contract_version, str)
        or not unit_contract_version
    ):
        raise D0RawPgError(
            "D0_SOURCE_CAPTURE_INVALID",
            "Raw来源捕获的数值身份、范围、坐标、单位或NON_PIT时钟不合法。",
        )
    cursor.execute(
        """WITH request_codes AS (
               SELECT source_code,
                      count(*)=2 AND count(DISTINCT adjustment_flag)=2
                        AND bool_and(request_status IN ('COMPLETED','NO_DATA')
                          AND (request_status='NO_DATA'
                               OR completed_through_session>=range_end_session)) AS complete,
                      min(eligible_start_session) AS eligible_start,
                      max(eligible_end_session) AS eligible_end,
                      count(DISTINCT eligible_start_session)=1
                        AND count(DISTINCT eligible_end_session)=1 AS same_window
               FROM stock.source_capture_requests
               WHERE capture_id=%s AND range_end_session<=%s
               GROUP BY source_code
           )
           SELECT count(*) FILTER (
                      WHERE %s BETWEEN eligible_start AND eligible_end
                  )::bigint,
                  count(*) FILTER (
                      WHERE %s BETWEEN eligible_start AND eligible_end AND complete
                        AND same_window
                  )::bigint
           FROM request_codes""",
        (source_capture_id, research_cutoff, target_session, target_session),
    )
    eligible_count, complete_count = (int(value) for value in cursor.fetchone())
    if eligible_count <= 0 or eligible_count != complete_count:
        raise D0RawPgError(
            "D0_SOURCE_CAPTURE_REQUESTS_INCOMPLETE",
            "目标会话的RAW/QFQ来源请求或证券有效窗口没有闭合。",
        )
    cursor.execute(
        """SELECT count(*)::bigint,count(DISTINCT source_symbol)::bigint,
                  count(*) FILTER (WHERE board IN ('MAIN_BOARD', 'GEM'))::bigint
           FROM stock.source_raw_daily
           WHERE source_capture_id=%s AND session_date=%s
             AND price_quality IS NOT NULL AND qfq_price_quality IS NOT NULL""",
        (source_capture_id, target_session),
    )
    stored_rows, stored_symbols, research_expected_rows = (
        int(value) for value in cursor.fetchone()
    )
    if stored_rows != eligible_count or stored_symbols != eligible_count:
        raise D0RawPgError(
            "D0_SOURCE_CAPTURE_ROWS_INCOMPLETE",
            "双坐标Raw目标会话行数未闭合到当前有效证券总体。",
        )
    return {
        "target_session": target_session,
        "source_capture_id": source_capture_id,
        "capture_uuid": str(capture_uuid),
        "provider": provider,
        "range_start_session": range_start,
        "range_end_session": range_end,
        "price_coordinates": tuple(coordinates),
        "parser_contract": parser_contract,
        "unit_contract_version": unit_contract_version,
        "capture_expected_row_count": eligible_count,
        "research_expected_row_count": research_expected_rows,
        "capture_normalized_row_count": normalized_count,
        "requested_symbol_count": requested_symbol_count,
        "capture_read_started_at": capture_read_started_at,
        "capture_validated_at": capture_validated_at,
        "source_available_at": source_available_at,
        "source_time_quality": source_time_quality,
        "source_clock_role": source_clock_role,
    }


def _validate_source_version(
    source_version: Mapping[str, Any],
    *,
    target_session: date,
    source_capture_id: int,
    calendar: Mapping[str, str],
) -> None:
    if (
        source_version.get("target_session") != target_session
        or source_version.get("source_capture_id") != source_capture_id
        or source_version.get("provider") != "BAOSTOCK"
        or tuple(source_version.get("price_coordinates", ()))
        != ("RAW_UNADJUSTED", "FORWARD_ADJUSTED")
        or source_version.get("source_time_quality") != "NON_PIT_OBSERVED"
        or not 1
        <= source_version.get("capture_expected_row_count", 0)
        <= MAX_SOURCE_SYMBOLS
    ):
        raise D0RawPgError(
            "D0_SOURCE_VERSION_INVALID",
            "Raw捕获、价格坐标、NON_PIT时钟或目标日证券分母不合法。",
        )
    if set(calendar) != {"calendar_id", "calendar_revision"} or any(
        not isinstance(calendar[key], str) or not calendar[key] for key in calendar
    ):
        raise D0RawPgError("D0_SOURCE_CALENDAR_INVALID", "官方日历身份或修订不完整。")


def _validate_target_rows(
    target_rows: Sequence[Mapping[str, Any]],
    source_version: Mapping[str, Any],
    *,
    target_session: date,
    source_capture_id: int,
) -> tuple[str, ...]:
    symbols = tuple(row["source_symbol"] for row in target_rows)
    counts = {
        status: sum(row["bar_status"] == status for row in target_rows)
        for status in ("TARGET_PRESENT", "TARGET_MISSING", "READ_FAILED")
    }
    if (
        len(target_rows) != source_version["research_expected_row_count"]
        or len(symbols) != len(set(symbols))
        or tuple(sorted(symbols)) != symbols
        or any(_SYMBOL.fullmatch(symbol) is None for symbol in symbols)
        or sum(counts.values()) != len(target_rows)
    ):
        raise D0RawPgError(
            "D0_TARGET_RAW_PARTITION_MISMATCH",
            "目标日Raw来源池与有效证券分母或Bar质量分区不一致。",
        )
    for row in target_rows:
        if (
            row["session_date"] != target_session
            or row.get("source_capture_id") != source_capture_id
            or row.get("price_quality") is None
            or row.get("qfq_price_quality") is None
        ):
            raise D0RawPgError(
                "D0_TARGET_RAW_REFERENCE_MISMATCH",
                "目标日双坐标Raw行与捕获身份或质量事实不一致。",
            )
    return symbols


def _read_target_rows(
    cursor: Any, *, target_session: date, source_capture_id: int
) -> list[dict[str, Any]]:
    cursor.execute(
        """SELECT source_symbol,market,board,session_date,source_capture_id,
                  bar_status,bar_reason,qfq_bar_status,qfq_bar_reason,
                  raw_open,raw_high,raw_low,raw_close,
                  price_quality,price_reason,raw_preclose,raw_preclose_quality,
                  raw_preclose_reason,qfq_open,qfq_high,qfq_low,qfq_close,
                  qfq_price_quality,qfq_price_reason,raw_amount,raw_amount_quality,
                  raw_amount_reason,amount_quality,amount_reason,amount_unit_status,
                  raw_volume,raw_volume_quality,raw_volume_reason,volume_quality,
                  volume_reason,volume_unit_status,qfq_tradestatus_value,
                  qfq_tradestatus_quality,qfq_tradestatus_reason,
                  raw_turn_value,raw_turn_quality,
                  raw_turn_reason,raw_tradestatus_value,raw_tradestatus_quality,
                  raw_tradestatus_reason,raw_is_st_value,raw_is_st_quality,raw_is_st_reason,
                  source_code,returned_fields,unit_contract_version,observed_started_at,
                  observed_completed_at,source_clock_role,source_available_at
           FROM stock.source_raw_daily
           WHERE session_date=%s AND source_capture_id=%s
             AND board IN ('MAIN_BOARD', 'GEM')
             AND price_quality IS NOT NULL AND qfq_price_quality IS NOT NULL
           ORDER BY source_symbol""",
        (target_session, source_capture_id),
    )
    names = [column.name for column in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def _read_history_grid(
    cursor: Any,
    *,
    source_symbols: Sequence[str],
    sessions: Sequence[date],
    source_capture_id: int,
    source_version: Mapping[str, Any],
) -> tuple[dict[str, dict[date, dict[str, Any] | None]], int, int]:
    cells = len(source_symbols) * len(sessions)
    if not 1 <= cells <= _MAX_HISTORY_CELLS:
        raise D0RawPgError(
            "D0_HISTORY_SCOPE_EXCEEDED", "双坐标RAW/QFQ历史读取超过有界上限。"
        )
    fetched = []
    chunk_size = max(1, min(100, 1500 // len(sessions)))
    for start in range(0, len(source_symbols), chunk_size):
        cursor.execute(
            """SELECT p.source_symbol,d.session_date,r.source_capture_id,
                      r.bar_status,r.bar_reason,r.qfq_bar_status,r.qfq_bar_reason,
                      r.raw_open,r.raw_high,r.raw_low,r.raw_close,
                      r.price_quality,r.price_reason,r.raw_preclose,r.raw_preclose_quality,
                      r.raw_preclose_reason,r.qfq_open,r.qfq_high,r.qfq_low,r.qfq_close,
                      r.qfq_price_quality,r.qfq_price_reason,r.raw_amount,
                      r.raw_amount_quality,r.raw_amount_reason,r.amount_quality,
                      r.amount_reason,r.amount_unit_status,r.raw_volume,
                      r.raw_volume_quality,r.raw_volume_reason,r.volume_quality,
                      r.volume_reason,r.volume_unit_status,r.qfq_tradestatus_value,
                      r.qfq_tradestatus_quality,r.qfq_tradestatus_reason,
                      r.raw_turn_value,
                      r.raw_turn_quality,r.raw_turn_reason,r.raw_tradestatus_value,
                      r.raw_tradestatus_quality,r.raw_tradestatus_reason,r.raw_is_st_value,
                      r.raw_is_st_quality,r.raw_is_st_reason,r.source_code,r.returned_fields,
                      r.unit_contract_version,r.observed_started_at,r.observed_completed_at,
                      r.source_clock_role,r.source_available_at,c.capture_uuid,c.provider,
                      c.range_start_session,c.range_end_session,c.price_coordinates,
                      c.source_time_quality,c.source_clock_role AS registered_clock_role,
                      c.source_available_at AS capture_available_at,c.normalized_count
               FROM unnest(%s::text[]) AS p(source_symbol)
               CROSS JOIN unnest(%s::date[]) AS d(session_date)
               LEFT JOIN stock.source_raw_daily r
                 ON r.source_symbol=p.source_symbol AND r.session_date=d.session_date
                AND r.source_capture_id=%s
               LEFT JOIN stock.source_captures c ON c.capture_id=r.source_capture_id
               ORDER BY p.source_symbol COLLATE "C",d.session_date""",
            (
                list(source_symbols[start : start + chunk_size]),
                list(sessions),
                source_capture_id,
            ),
        )
        column_names = [column.name for column in cursor.description]
        fetched.extend(cursor.fetchall())
    if len(fetched) != cells:
        raise D0RawPgError(
            "D0_HISTORY_GRID_INCOMPLETE",
            "RAW/QFQ历史查询没有返回完整来源代码×会话网格。",
        )
    result = {symbol: {} for symbol in source_symbols}
    present_rows = 0
    for values in fetched:
        row = dict(zip(column_names, values, strict=True))
        symbol = row["source_symbol"]
        session = row["session_date"]
        if row["source_capture_id"] is None:
            result[symbol][session] = None
            continue
        present_rows += 1
        row["reference_quality"] = (
            row["source_capture_id"] == source_capture_id
            and row["capture_uuid"] is not None
            and row["provider"] == "BAOSTOCK"
            and row["range_end_session"] is not None
            and row["range_end_session"] <= source_version["range_end_session"]
            and tuple(row["price_coordinates"] or ())
            == tuple(source_version["price_coordinates"])
            and row["unit_contract_version"] == source_version["unit_contract_version"]
            and row["registered_clock_role"] == "BAOSTOCK_QUERY_CAPTURED_NON_PIT"
            and row["source_available_at"] is None
            and row["bar_status"] in source_raw._BAR_STATUSES
            and row["price_quality"] in source_raw._QUALITY
            and row["qfq_price_quality"]
            in {"OBSERVED", "UNKNOWN", "NOT_APPLICABLE", "BAD_VALUE"}
        )
        row["raw_price_coordinate"] = "RAW_UNADJUSTED"
        row["qfq_price_coordinate"] = "FORWARD_ADJUSTED"
        row["price_coordinate"] = "RAW_UNADJUSTED"
        row["time_role"] = "NON_PIT_RETROSPECTIVE"
        row["source_symbol"] = symbol
        row["session_date"] = session
        row["source_capture_id"] = int(row["source_capture_id"])
        result[symbol][session] = row
    return result, present_rows, cells - present_rows


def read_source_d0_raw_window(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int,
    sessions: Sequence[date],
    expected_calendar: Mapping[str, str],
    history_source_symbol: str | None = None,
) -> dict[str, Any]:
    """一次一致只读事务核验Raw捕获分母并读取来源池历史网格。"""
    selected_sessions = tuple(sessions)
    if (
        history_source_symbol is not None
        and _SYMBOL.fullmatch(history_source_symbol) is None
    ):
        raise D0RawPgError(
            "D0_SOURCE_SYMBOL_INVALID", "指定的单股历史来源代码格式无效。"
        )
    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or not 1 <= len(selected_sessions) <= 25
        or any(type(session) is not date for session in selected_sessions)
        or tuple(sorted(set(selected_sessions))) != selected_sessions
        or selected_sessions[-1] != target_session
        or any(
            session > research_cutoff or session > target_session
            for session in selected_sessions
        )
    ):
        raise D0RawPgError(
            "D0_QUERY_SCOPE_INVALID",
            "D0读取必须绑定截止日内的显式目标、官方会话和正数来源捕获ID。",
        )
    started = time.perf_counter()
    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _connection,
        cursor,
        _unused,
    ):
        source_version_started = time.perf_counter()
        source_version = _read_source_version(
            cursor,
            target_session=target_session,
            source_capture_id=source_capture_id,
            research_cutoff=research_cutoff,
        )
        _validate_source_version(
            source_version,
            target_session=target_session,
            source_capture_id=source_capture_id,
            calendar=expected_calendar,
        )
        source_version_seconds = time.perf_counter() - source_version_started

        target_scan_started = time.perf_counter()
        target_rows = _read_target_rows(
            cursor,
            target_session=target_session,
            source_capture_id=source_capture_id,
        )
        symbols = _validate_target_rows(
            target_rows,
            source_version,
            target_session=target_session,
            source_capture_id=source_capture_id,
        )
        target_status_counts = {
            status: sum(row["bar_status"] == status for row in target_rows)
            for status in ("TARGET_PRESENT", "TARGET_MISSING", "READ_FAILED")
        }
        source_version["bar_status_counts"] = target_status_counts
        source_version["target_present_count"] = target_status_counts["TARGET_PRESENT"]
        source_version["target_missing_count"] = target_status_counts["TARGET_MISSING"]
        source_version["read_failed_count"] = target_status_counts["READ_FAILED"]
        target_scan_seconds = time.perf_counter() - target_scan_started

        history_scan_started = time.perf_counter()
        history_symbols = (
            symbols
            if history_source_symbol is None
            else (history_source_symbol,)
            if history_source_symbol in symbols
            else ()
        )
        rows_by_symbol, present_rows, missing_cells = _read_history_grid(
            cursor,
            source_symbols=history_symbols,
            sessions=selected_sessions,
            source_capture_id=source_capture_id,
            source_version=source_version,
        )
        history_scan_seconds = time.perf_counter() - history_scan_started

    return {
        "source_version": source_version,
        "source_symbols": symbols,
        "target_rows": {row["source_symbol"]: row for row in target_rows},
        "history_rows_by_symbol": rows_by_symbol,
        "source_version_identity": {
            "source_capture_id": source_capture_id,
            "target_session": target_session.isoformat(),
        },
        "source_count": len(symbols),
        "history_sessions": [session.isoformat() for session in selected_sessions],
        "raw_history_rows_read": present_rows,
        "history_grid_cells": len(rows_by_symbol) * len(selected_sessions),
        "history_symbol_count": len(rows_by_symbol),
        "missing_history_cells": missing_cells,
        "raw_history_batch_reads": 1,
        "pg_source_version_read_seconds": source_version_seconds,
        "pg_target_scan_seconds": target_scan_seconds,
        "pg_history_scan_seconds": history_scan_seconds,
        "pg_read_total_seconds": time.perf_counter() - started,
        "transaction": "REPEATABLE READ READ ONLY",
        "database_write_attempted": False,
        "ddl_executed": False,
        "dml_executed": False,
        "source_zip_opened": False,
        "source_scan_performed": False,
    }


__all__ = ["D0RawPgError", "MAX_SOURCE_SYMBOLS", "read_source_d0_raw_window"]
