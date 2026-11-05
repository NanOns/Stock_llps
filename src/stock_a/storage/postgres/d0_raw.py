"""D0来源股票原始场景的有界PostgreSQL只读读取。"""

from __future__ import annotations

from datetime import date
import re
import time
from typing import Any, Mapping, Sequence
import uuid

from . import source_raw


_SYMBOL = re.compile(r"^(SH|SZ)\.[0-9]{6}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
MAX_SOURCE_SYMBOLS = 5000
_MAX_SOURCE_SYMBOLS = MAX_SOURCE_SYMBOLS
_MAX_HISTORY_CELLS = _MAX_SOURCE_SYMBOLS * 21


class D0RawPgError(ValueError):
    """封存来源或D0原始会话读取失败。"""

    preserve_transaction_error = True

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def _read_source_version(
    cursor: Any,
    *,
    namespace_id: str,
    target_session: date,
    package_sha256: str,
) -> dict[str, Any]:
    cursor.execute(
        """SELECT DISTINCT r.source_content_id, r.source_capture_id::text,
       c.read_started_at, c.validated_at, c.source_available_at,
       c.source_time_quality, c.source_clock_role, c.parser_contract,
       c.canonical_digest, c.normalized_count, s.dataset_id, s.byte_sha, s.byte_count
FROM stock.source_raw_daily r
JOIN stock.source_captures c ON c.namespace_id=r.namespace_id
  AND c.capture_id=r.source_capture_id
JOIN stock.source_contents s ON s.namespace_id=r.namespace_id
  AND s.content_id=r.source_content_id
WHERE r.namespace_id=%s::uuid AND r.session_date=%s AND r.package_sha256=%s
ORDER BY r.source_capture_id, r.source_content_id
LIMIT 2""",
        (namespace_id, target_session, package_sha256),
    )
    rows = cursor.fetchall()
    if len(rows) != 1:
        raise D0RawPgError(
            "D0_SOURCE_VERSION_NOT_UNIQUE",
            "指定日期与完整包SHA没有唯一Raw捕获和内容引用。",
        )
    (
        source_content_id,
        source_capture_id,
        capture_read_started_at,
        capture_validated_at,
        source_available_at,
        source_time_quality,
        source_clock_role,
        parser_contract,
        source_digest,
        capture_expected_row_count,
        source_dataset_id,
        content_sha,
        source_byte_count,
    ) = rows[0]
    if (
        source_dataset_id != source_raw._DATASET_ID
        or str(content_sha).strip() != package_sha256
        or type(capture_expected_row_count) is not int
        or capture_expected_row_count <= 0
        or (
            source_available_at is not None
            and source_time_quality != "VERIFIED_SOURCE_TIME"
        )
    ):
        raise D0RawPgError(
            "D0_SOURCE_VERSION_INVALID",
            "Raw来源版本的包身份、捕获数量或时钟元数据不合法。",
        )
    return {
        "namespace_id": namespace_id,
        "target_session": target_session,
        "package_sha256": package_sha256,
        "source_content_id": str(source_content_id).strip(),
        "source_capture_id": source_capture_id,
        "source_dataset_id": source_dataset_id,
        "source_byte_count": int(source_byte_count),
        "parser_contract": parser_contract,
        "capture_expected_row_count": capture_expected_row_count,
        "capture_read_started_at": capture_read_started_at,
        "capture_validated_at": capture_validated_at,
        "source_available_at": source_available_at,
        "source_time_quality": source_time_quality,
        "source_clock_role": source_clock_role,
        "source_digest": str(source_digest).strip(),
    }


def _validate_source_version(
    source_version: Mapping[str, Any],
    *,
    namespace_id: str,
    target_session: date,
    package_sha256: str,
    calendar: Mapping[str, str],
) -> None:
    if (
        source_version["namespace_id"] != namespace_id
        or source_version["target_session"] != target_session
        or source_version["package_sha256"] != package_sha256
    ):
        raise D0RawPgError(
            "D0_SOURCE_VERSION_IDENTITY_MISMATCH", "Raw来源版本身份不匹配。"
        )
    if (
        source_version["source_dataset_id"] != source_raw._DATASET_ID
        or source_version["source_time_quality"]
        not in {"UNKNOWN", "NON_PIT_OBSERVED", "VERIFIED_SOURCE_TIME"}
        or not 1 <= source_version["capture_expected_row_count"] <= _MAX_SOURCE_SYMBOLS
    ):
        raise D0RawPgError(
            "D0_SOURCE_VERSION_INVALID",
            "Raw来源捕获、代码分母或来源质量状态不合法。",
        )
    if any(calendar.get(key) is None for key in calendar):
        raise D0RawPgError(
            "D0_SOURCE_CALENDAR_INVALID",
            "官方日历身份、修订或摘要不完整。",
        )


def _validate_target_rows(
    target_rows: Sequence[Mapping[str, Any]],
    source_version: Mapping[str, Any],
    *,
    namespace_id: str,
    target_session: date,
    package_sha256: str,
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
            str(row["namespace_id"]) != namespace_id
            or row["session_date"] != target_session
            or str(row["package_sha256"]).strip() != package_sha256
            or str(row["source_capture_id"]) != source_version["source_capture_id"]
            or row["source_content_id"] != source_version["source_content_id"]
        ):
            raise D0RawPgError(
                "D0_TARGET_RAW_REFERENCE_MISMATCH",
                "目标日Raw行与捕获、内容或包SHA不一致。",
            )
    if (
        source_raw._row_digest(target_rows)
        != str(source_version["source_digest"]).strip()
    ):
        raise D0RawPgError(
            "D0_TARGET_RAW_DIGEST_MISMATCH", "目标日Raw行摘要与捕获记录不一致。"
        )
    return symbols


def _read_history_grid(
    cursor: Any,
    *,
    namespace_id: str,
    source_symbols: Sequence[str],
    sessions: Sequence[date],
    package_sha256: str,
    source_version: Mapping[str, Any],
) -> tuple[dict[str, dict[date, dict[str, Any] | None]], int, int]:
    cells = len(source_symbols) * len(sessions)
    if not 1 <= cells <= _MAX_HISTORY_CELLS:
        raise D0RawPgError("D0_HISTORY_SCOPE_EXCEEDED", "严格RAW历史读取超过有界上限。")
    cursor.execute(
        """SELECT p.source_symbol, d.session_date,
       r.namespace_id::text AS namespace_id, r.package_sha256, r.source_content_id,
       r.source_capture_id::text AS source_capture_id, r.source_entry_sha256,
       r.bar_status, r.bar_reason, r.raw_open, r.raw_high, r.raw_low, r.raw_close,
       r.price_quality, r.price_reason, r.raw_amount, r.raw_amount_quality,
       r.raw_amount_reason, r.amount_unit_status,
       c.capture_id::text AS joined_capture_id, c.content_id AS capture_content_id,
       c.normalized_count AS capture_normalized_count, c.parser_contract,
       s.dataset_id, s.byte_sha
FROM unnest(%s::text[]) AS p(source_symbol)
CROSS JOIN unnest(%s::date[]) AS d(session_date)
LEFT JOIN stock.source_raw_daily r
  ON r.namespace_id=%s::uuid AND r.source_symbol=p.source_symbol
 AND r.session_date=d.session_date AND r.package_sha256=%s
LEFT JOIN stock.source_captures c
  ON c.namespace_id=r.namespace_id AND c.capture_id=r.source_capture_id
LEFT JOIN stock.source_contents s
  ON s.namespace_id=r.namespace_id AND s.content_id=r.source_content_id
ORDER BY p.source_symbol COLLATE "C", d.session_date""",
        (list(source_symbols), list(sessions), namespace_id, package_sha256),
    )
    column_names = [column.name for column in cursor.description]
    fetched = cursor.fetchall()
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
        if row["namespace_id"] is None:
            result[symbol][session] = None
            continue
        present_rows += 1
        reference_quality = (
            str(row["package_sha256"]).strip() == package_sha256
            and row["source_content_id"] == source_version["source_content_id"]
            and row["capture_normalized_count"] is not None
            and row["capture_normalized_count"] > 0
            and row["joined_capture_id"] == row["source_capture_id"]
            and row["capture_content_id"] == row["source_content_id"]
            and row["dataset_id"] == source_raw._DATASET_ID
            and str(row["byte_sha"]).strip() == package_sha256
            and row["parser_contract"] == source_version["parser_contract"]
            and row["bar_status"] in source_raw._BAR_STATUSES
            and row["price_quality"] in source_raw._QUALITY
            and (
                row["source_entry_sha256"] is None
                or _SHA256.fullmatch(row["source_entry_sha256"]) is not None
            )
        )
        row["price_coordinate"] = "RAW_UNADJUSTED"
        row["reference_quality"] = reference_quality
        row["source_symbol"] = symbol
        row["session_date"] = session
        row["namespace_id"] = namespace_id
        row["package_sha256"] = str(row["package_sha256"]).strip()
        result[symbol][session] = row
    return result, present_rows, cells - present_rows


def read_source_d0_raw_window(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    package_sha256: str,
    sessions: Sequence[date],
    expected_calendar: Mapping[str, str],
    expected_namespace_id: str | None = None,
) -> dict[str, Any]:
    """一次一致只读事务核验Raw捕获分母并读取来源池历史网格。"""
    selected_sessions = tuple(sessions)
    if expected_namespace_id is not None:
        try:
            canonical_namespace_id = str(uuid.UUID(expected_namespace_id))
        except (ValueError, TypeError, AttributeError) as error:
            raise D0RawPgError(
                "D0_NAMESPACE_ID_INVALID",
                "expected_namespace_id必须是规范小写UUID。",
            ) from error
        if canonical_namespace_id != expected_namespace_id:
            raise D0RawPgError(
                "D0_NAMESPACE_ID_INVALID",
                "expected_namespace_id必须是规范小写UUID。",
            )
    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
        or not isinstance(package_sha256, str)
        or _SHA256.fullmatch(package_sha256) is None
        or not 1 <= len(selected_sessions) <= 21
        or any(type(session) is not date for session in selected_sessions)
        or tuple(sorted(set(selected_sessions))) != selected_sessions
        or selected_sessions[-1] != target_session
        or any(session > research_cutoff for session in selected_sessions)
        or any(session > target_session for session in selected_sessions)
        or set(expected_calendar)
        != {"calendar_id", "calendar_revision", "calendar_sha256"}
    ):
        raise D0RawPgError(
            "D0_QUERY_SCOPE_INVALID",
            "D0读取必须绑定截止日内的显式目标、官方会话和完整包SHA。",
        )

    started = time.perf_counter()
    with source_raw._transaction(config, write=False) as (
        _connection,
        cursor,
        namespace_id,
    ):
        if expected_namespace_id is not None and namespace_id != expected_namespace_id:
            raise D0RawPgError(
                "D0_NAMESPACE_MISMATCH",
                "当前部署DEV命名空间与请求绑定不一致，未读取来源行情表。",
            )
        source_version_started = time.perf_counter()
        source_version = _read_source_version(
            cursor,
            namespace_id=namespace_id,
            target_session=target_session,
            package_sha256=package_sha256,
        )
        _validate_source_version(
            source_version,
            namespace_id=namespace_id,
            target_session=target_session,
            package_sha256=package_sha256,
            calendar=expected_calendar,
        )
        source_version_seconds = time.perf_counter() - source_version_started

        target_scan_started = time.perf_counter()
        target_rows = source_raw._read_raw_rows(
            cursor, namespace_id, target_session, package_sha256
        )
        symbols = _validate_target_rows(
            target_rows,
            source_version,
            namespace_id=namespace_id,
            target_session=target_session,
            package_sha256=package_sha256,
        )
        target_status_counts = {
            status: sum(row["bar_status"] == status for row in target_rows)
            for status in ("TARGET_PRESENT", "TARGET_MISSING", "READ_FAILED")
        }
        source_version.update(
            {
                "target_present_count": target_status_counts["TARGET_PRESENT"],
                "target_missing_count": target_status_counts["TARGET_MISSING"],
                "read_failed_count": target_status_counts["READ_FAILED"],
                "bar_status_counts": target_status_counts,
            }
        )
        target_scan_seconds = time.perf_counter() - target_scan_started

        history_scan_started = time.perf_counter()
        rows_by_symbol, present_rows, missing_cells = _read_history_grid(
            cursor,
            namespace_id=namespace_id,
            source_symbols=symbols,
            sessions=selected_sessions,
            package_sha256=package_sha256,
            source_version=source_version,
        )
        history_scan_seconds = time.perf_counter() - history_scan_started

    return {
        "namespace_id": namespace_id,
        "source_version": source_version,
        "source_symbols": symbols,
        "target_rows": {row["source_symbol"]: row for row in target_rows},
        "history_rows_by_symbol": rows_by_symbol,
        "target_raw_digest": source_raw._row_digest(target_rows),
        "source_count": len(symbols),
        "history_sessions": [session.isoformat() for session in selected_sessions],
        "raw_history_rows_read": present_rows,
        "history_grid_cells": len(symbols) * len(selected_sessions),
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
