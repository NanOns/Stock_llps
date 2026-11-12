"""D0来源股票原始场景的有界PostgreSQL只读读取。"""

from __future__ import annotations

from datetime import date
import time
from typing import Any, Mapping, Sequence

from . import source_raw


MAX_SOURCE_SYMBOLS = 5000
_SYMBOL = source_raw._SYMBOL
_MAX_HISTORY_CELLS = MAX_SOURCE_SYMBOLS * 25


class D0RawPgError(ValueError):
    """封存来源或D0原始会话读取失败。"""

    preserve_transaction_error = True

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def _read_source_version(
    cursor: Any, *, target_session: date, source_package_id: int
) -> dict[str, Any]:
    cursor.execute(
        """SELECT DISTINCT r.source_package_id, r.source_capture_id,
       c.read_started_at, c.validated_at, c.source_available_at,
       c.source_time_quality, c.source_clock_role, c.parser_contract,
       c.normalized_count, p.dataset_id, p.byte_count
FROM source_raw_daily r
JOIN source_captures c ON c.capture_id=r.source_capture_id
  AND c.source_package_id=r.source_package_id AND c.target_session=r.session_date
JOIN source_packages p ON p.source_package_id=r.source_package_id
WHERE r.session_date=%s AND r.source_package_id=%s
ORDER BY r.source_capture_id, r.source_package_id LIMIT 2""",
        (target_session, source_package_id),
    )
    rows = cursor.fetchall()
    if len(rows) != 1:
        raise D0RawPgError(
            "D0_SOURCE_VERSION_NOT_UNIQUE",
            "指定交易日与来源包数字ID没有唯一Raw捕获。",
        )
    (
        package_id,
        capture_id,
        capture_read_started_at,
        capture_validated_at,
        source_available_at,
        source_time_quality,
        source_clock_role,
        parser_contract,
        expected_count,
        dataset_id,
        byte_count,
    ) = rows[0]
    if (
        int(package_id) != source_package_id
        or type(capture_id) is not int
        or type(expected_count) is not int
        or expected_count <= 0
        or dataset_id != source_raw._DATASET_ID
        or (
            source_available_at is not None
            and source_time_quality != "VERIFIED_SOURCE_TIME"
        )
    ):
        raise D0RawPgError(
            "D0_SOURCE_VERSION_INVALID",
            "Raw来源版本的数字引用、捕获数量或时钟元数据不合法。",
        )
    return {
        "target_session": target_session,
        "source_package_id": source_package_id,
        "source_capture_id": capture_id,
        "source_dataset_id": dataset_id,
        "source_byte_count": int(byte_count),
        "parser_contract": parser_contract,
        "capture_expected_row_count": expected_count,
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
    source_package_id: int,
    calendar: Mapping[str, str],
) -> None:
    if (
        source_version["target_session"] != target_session
        or source_version["source_package_id"] != source_package_id
    ):
        raise D0RawPgError(
            "D0_SOURCE_VERSION_IDENTITY_MISMATCH", "Raw来源版本身份不匹配。"
        )
    if (
        source_version["source_dataset_id"] != source_raw._DATASET_ID
        or source_version["source_time_quality"]
        not in {"UNKNOWN", "NON_PIT_OBSERVED", "VERIFIED_SOURCE_TIME"}
        or not 1 <= source_version["capture_expected_row_count"] <= MAX_SOURCE_SYMBOLS
    ):
        raise D0RawPgError(
            "D0_SOURCE_VERSION_INVALID",
            "Raw来源捕获、代码分母或来源质量状态不合法。",
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
    source_package_id: int,
) -> tuple[str, ...]:
    symbols = tuple(row["source_symbol"] for row in target_rows)
    counts = {
        status: sum(row["bar_status"] == status for row in target_rows)
        for status in ("TARGET_PRESENT", "TARGET_MISSING", "READ_FAILED")
    }
    if (
        len(target_rows) != source_version["capture_expected_row_count"]
        or len(symbols) != len(set(symbols))
        or tuple(sorted(symbols)) != symbols
        or any(_SYMBOL.fullmatch(symbol) is None for symbol in symbols)
        or sum(counts.values()) != len(target_rows)
    ):
        raise D0RawPgError(
            "D0_TARGET_RAW_PARTITION_MISMATCH",
            "目标日Raw来源池与捕获代码数或Bar质量分区不一致。",
        )
    for row in target_rows:
        if (
            row["session_date"] != target_session
            or int(row["source_package_id"]) != source_package_id
            or int(row["source_capture_id"]) != source_version["source_capture_id"]
        ):
            raise D0RawPgError(
                "D0_TARGET_RAW_REFERENCE_MISMATCH",
                "目标日Raw行与来源包或捕获数字ID不一致。",
            )
    return symbols


def _read_history_grid(
    cursor: Any,
    *,
    source_symbols: Sequence[str],
    sessions: Sequence[date],
    source_package_id: int,
    source_version: Mapping[str, Any],
) -> tuple[dict[str, dict[date, dict[str, Any] | None]], int, int]:
    cells = len(source_symbols) * len(sessions)
    if not 1 <= cells <= _MAX_HISTORY_CELLS:
        raise D0RawPgError("D0_HISTORY_SCOPE_EXCEEDED", "严格RAW历史读取超过有界上限。")
    fetched = []
    chunk_size = max(1, min(100, 1500 // len(sessions)))
    for start in range(0, len(source_symbols), chunk_size):
        cursor.execute(
            """SELECT p.source_symbol, d.session_date,
       r.source_package_id, r.source_capture_id,
       r.bar_status, r.bar_reason, r.raw_open, r.raw_high, r.raw_low, r.raw_close,
       r.price_quality, r.price_reason, r.raw_amount, r.raw_amount_quality,
       r.raw_amount_reason, r.amount_unit_status,
       c.capture_id AS joined_capture_id, c.source_package_id AS capture_package_id,
       c.target_session AS capture_target_session,
       c.normalized_count AS capture_normalized_count, c.parser_contract,
       s.dataset_id
FROM unnest(%s::text[]) AS p(source_symbol)
CROSS JOIN unnest(%s::date[]) AS d(session_date)
LEFT JOIN source_raw_daily r
  ON r.source_symbol=p.source_symbol AND r.session_date=d.session_date
 AND r.source_package_id=%s
LEFT JOIN source_captures c
  ON c.capture_id=r.source_capture_id AND c.source_package_id=r.source_package_id
 AND c.target_session=r.session_date
LEFT JOIN source_packages s ON s.source_package_id=r.source_package_id
ORDER BY p.source_symbol COLLATE "C", d.session_date""",
            (
                list(source_symbols[start : start + chunk_size]),
                list(sessions),
                source_package_id,
            ),
        )
        column_names = [column.name for column in cursor.description]
        fetched.extend(cursor.fetchall())
    if len(fetched) != cells:
        raise D0RawPgError(
            "D0_HISTORY_GRID_INCOMPLETE", "RAW历史查询没有返回完整的来源代码×会话网格。"
        )
    result = {symbol: {} for symbol in source_symbols}
    present_rows = 0
    for values in fetched:
        row = dict(zip(column_names, values, strict=True))
        symbol = row["source_symbol"]
        session = row["session_date"]
        if row["source_package_id"] is None:
            result[symbol][session] = None
            continue
        present_rows += 1
        row["reference_quality"] = (
            int(row["source_package_id"]) == source_package_id
            and type(row["source_capture_id"]) is int
            and row["joined_capture_id"] == row["source_capture_id"]
            and row["capture_package_id"] == row["source_package_id"]
            and row["capture_target_session"] == session
            and row["capture_normalized_count"]
            == source_version["capture_expected_row_count"]
            and row["parser_contract"] == source_version["parser_contract"]
            and row["dataset_id"] == source_raw._DATASET_ID
            and row["bar_status"] in source_raw._BAR_STATUSES
            and row["price_quality"] in source_raw._QUALITY
        )
        row["price_coordinate"] = "RAW_UNADJUSTED"
        row["source_symbol"] = symbol
        row["session_date"] = session
        row["source_package_id"] = int(row["source_package_id"])
        row["source_capture_id"] = int(row["source_capture_id"])
        result[symbol][session] = row
    return result, present_rows, cells - present_rows


def read_source_d0_raw_window(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    source_package_id: int,
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
        or type(source_package_id) is not int
        or source_package_id <= 0
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
            "D0读取必须绑定截止日内的显式目标、官方会话和正数来源包ID。",
        )
    started = time.perf_counter()
    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _connection,
        cursor,
        _unused,
    ):
        source_version_started = time.perf_counter()
        source_version = _read_source_version(
            cursor, target_session=target_session, source_package_id=source_package_id
        )
        _validate_source_version(
            source_version,
            target_session=target_session,
            source_package_id=source_package_id,
            calendar=expected_calendar,
        )
        source_version_seconds = time.perf_counter() - source_version_started

        target_scan_started = time.perf_counter()
        target_rows = source_raw._read_raw_rows(
            cursor, target_session, source_package_id
        )
        symbols = _validate_target_rows(
            target_rows,
            source_version,
            target_session=target_session,
            source_package_id=source_package_id,
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
            source_package_id=source_package_id,
            source_version=source_version,
        )
        history_scan_seconds = time.perf_counter() - history_scan_started

    return {
        "source_version": source_version,
        "source_symbols": symbols,
        "target_rows": {row["source_symbol"]: row for row in target_rows},
        "history_rows_by_symbol": rows_by_symbol,
        "source_version_identity": {
            "source_package_id": source_package_id,
            "source_capture_id": source_version["source_capture_id"],
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
