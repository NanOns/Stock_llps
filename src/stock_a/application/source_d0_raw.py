"""来源股票D0 LAUNCH_CONFIRM诊断级原始场景只读应用用例。"""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime
from fractions import Fraction
import re
import time
from typing import Any, Mapping

from stock_a.application.source_set_candidate import (
    SourceSetCandidateApplicationError,
    _official_calendar,
)
from stock_a.research.d0_launch_raw import (
    calculate_d0_launch_confirm_raw,
    fraction_payload,
)
from stock_a.storage.postgres.d0_raw import D0RawPgError, read_source_d0_raw_window


_SYMBOL = re.compile(r"^(SH|SZ)\.[0-9]{6}$")
_STATUSES = {"TRUE", "FALSE", "UNKNOWN", "ALL"}
_MAX_LIMIT = 5000


class SourceD0RawError(ValueError):
    """D0原始场景读取的参数、合同或PG错误。"""

    def __init__(self, code: str, message: str, *, category: str = "UNAVAILABLE"):
        self.code = code
        self.category = category
        super().__init__(message)


def _fraction(value: Any) -> dict[str, str] | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    try:
        return fraction_payload(Fraction(value))
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def _iso(value: Any) -> str | None:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return None if value is None else str(value)


def _target_row_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "target_bar_status": row.get("bar_status"),
        "target_bar_reason": row.get("bar_reason"),
        "raw_ohlc": {
            key: _fraction(row.get(column))
            for key, column in (
                ("open", "raw_open"),
                ("high", "raw_high"),
                ("low", "raw_low"),
                ("close", "raw_close"),
            )
        },
        "price_quality": row.get("price_quality"),
        "price_reason": row.get("price_reason"),
        "raw_amount": _fraction(row.get("raw_amount")),
        "raw_amount_quality": row.get("raw_amount_quality"),
        "raw_amount_reason": row.get("raw_amount_reason"),
        "amount_unit_status": row.get("amount_unit_status"),
        "raw_volume_quality": row.get("raw_volume_quality"),
        "volume_unit_status": row.get("volume_unit_status"),
        "source_capture_id": str(row.get("source_capture_id")),
        "source_content_id": row.get("source_content_id"),
        "source_entry_sha256": row.get("source_entry_sha256"),
        "source_clock_role": row.get("source_clock_role"),
        "source_available_at": _iso(row.get("source_available_at")),
        "observed_started_at": _iso(row.get("observed_started_at")),
        "observed_completed_at": _iso(row.get("observed_completed_at")),
        "first_source_date": _iso(row.get("first_source_date")),
        "last_source_date": _iso(row.get("last_source_date")),
        "future_source_rows_excluded": row.get("future_source_rows_excluded"),
        "price_coordinate": "RAW_UNADJUSTED",
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "stable_security_id_status": "UNVERIFIED",
        "pit_status": "NON_PIT_RETROSPECTIVE",
    }


def query_source_d0_raw_launch(
    config: Mapping[str, Any],
    *,
    scenario: str,
    target_session: date,
    research_cutoff: date,
    source_set_id: str,
    package_sha256: str,
    state: str = "ALL",
    symbol: str | None = None,
    offset: int = 0,
    limit: int = 30,
) -> dict[str, Any]:
    """计算完整SEALED来源代码池后返回状态过滤、稳定排序的有界页。"""
    if scenario != "LAUNCH_CONFIRM":
        raise SourceD0RawError(
            "D0_SCENARIO_NOT_IMPLEMENTED",
            "本读者只实现LAUNCH_CONFIRM诊断raw谓词；其他场景标记NOT_IMPLEMENTED。",
            category="ARGUMENT",
        )
    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
    ):
        raise SourceD0RawError(
            "D0_DATE_AFTER_CUTOFF",
            "目标官方会话必须不晚于显式research_cutoff。",
            category="ARGUMENT",
        )
    if state not in _STATUSES:
        raise SourceD0RawError(
            "D0_STATE_INVALID",
            "状态只允许TRUE/FALSE/UNKNOWN/ALL。",
            category="ARGUMENT",
        )
    if symbol is not None and _SYMBOL.fullmatch(symbol) is None:
        raise SourceD0RawError(
            "D0_SOURCE_SYMBOL_INVALID",
            "精确来源代码须为SH./SZ.加六位数字。",
            category="ARGUMENT",
        )
    if type(offset) is not int or offset < 0:
        raise SourceD0RawError(
            "D0_OFFSET_INVALID", "分页offset必须为非负整数。", category="ARGUMENT"
        )
    if type(limit) is not int or not 1 <= limit <= _MAX_LIMIT:
        raise SourceD0RawError(
            "D0_LIMIT_INVALID",
            f"分页limit必须为1至{_MAX_LIMIT}之间的整数。",
            category="ARGUMENT",
        )

    try:
        calendar = _official_calendar(target_session)
        target_index = calendar["sessions"].index(target_session)
    except (SourceSetCandidateApplicationError, ValueError) as error:
        raise SourceD0RawError("D0_OFFICIAL_CALENDAR_INVALID", str(error)) from error
    sessions = calendar["sessions"][max(0, target_index - 20) : target_index + 1]
    calendar_identity = {
        "calendar_id": calendar["calendar_id"],
        "calendar_revision": calendar["calendar_revision"],
        "calendar_sha256": calendar["calendar_sha256"],
    }

    process_started = time.perf_counter()
    cpu_started = time.process_time()
    try:
        pg_result = read_source_d0_raw_window(
            config,
            target_session=target_session,
            research_cutoff=research_cutoff,
            source_set_id=source_set_id,
            package_sha256=package_sha256,
            sessions=sessions,
            expected_calendar=calendar_identity,
        )
    except D0RawPgError as error:
        raise SourceD0RawError(error.code, str(error)) from error
    read_cpu_seconds = time.process_time() - cpu_started

    source_set = pg_result["source_set"]
    calculate_started = time.perf_counter()
    calculate_cpu_started = time.process_time()
    results = []
    unknown_reasons: Counter[str] = Counter()
    false_reasons: Counter[str] = Counter()
    state_counts: Counter[str] = Counter()
    for source_symbol in pg_result["source_symbols"]:
        item = calculate_d0_launch_confirm_raw(
            target_session=target_session,
            official_sessions=sessions,
            history_rows_by_session=pg_result["history_rows_by_symbol"][source_symbol],
            source_symbol=source_symbol,
            package_sha256=package_sha256,
            namespace_id=pg_result["namespace_id"],
            source_content_id=source_set["source_content_id"],
            target_capture_id=source_set["source_capture_id"],
            parser_contract=source_set["parser_contract"],
            include_history_window=symbol == source_symbol,
        )
        target_row = pg_result["target_rows"][source_symbol]
        item.update(
            {
                "market": target_row.get("market"),
                "board": target_row.get("board"),
                "target": _target_row_payload(target_row),
            }
        )
        state_counts[item["scenario_raw"]] += 1
        if item["scenario_raw"] == "UNKNOWN":
            unknown_reasons[item["main_reason"]] += 1
        elif item["scenario_raw"] == "FALSE":
            false_reasons[item["main_reason"]] += 1
        results.append(item)
    calculate_seconds = time.perf_counter() - calculate_started
    calculate_cpu_seconds = time.process_time() - calculate_cpu_started

    results.sort(key=lambda item: item["source_symbol"])
    selected = [
        item
        for item in results
        if (state == "ALL" or item["scenario_raw"] == state)
        and (symbol is None or item["source_symbol"] == symbol)
    ]
    selected_count = len(selected)
    page = selected[offset : offset + limit]
    source_count = pg_result["source_count"]
    if sum(state_counts.values()) != source_count:
        raise SourceD0RawError(
            "D0_STATE_PARTITION_OPEN", "TRUE/FALSE/UNKNOWN没有闭合到完整来源池。"
        )

    return {
        "ok": True,
        "contract": "STOCK_A_D0_LAUNCH_CONFIRM_SOURCE_SYMBOL_RAW_R1",
        "producer_version": "STOCK_A_D0_LAUNCH_RAW_READER_R1",
        "algorithm_contract": "ALG-28 + §11.6 D0_SCENARIO_FACTS_R3",
        "scenario": scenario,
        "scenario_role": "DIAGNOSTIC_ONLY",
        "scenario_raw": "PER_SOURCE_SYMBOL_TRUE_FALSE_UNKNOWN",
        "formal_admission_status": "NOT_AUTHORIZED_SOURCE_SYMBOL_DIAGNOSTIC",
        "research_universe_status": "UNKNOWN",
        "absolute_liquidity_cny_status": "UNKNOWN_UNIT_UNVERIFIED",
        "turn_status": "UNKNOWN_NOT_PRODUCED",
        "stable_security_id_status": "STABLE_SECURITY_ID_UNVERIFIED",
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "episode_status": "NOT_ISSUED",
        "other_scenarios": {
            "RECOVERY_TURN": "NOT_IMPLEMENTED",
            "STRONG_PULLBACK": "NOT_IMPLEMENTED",
            "TREND_CONTINUE": "NOT_IMPLEMENTED",
        },
        "session_contract": "D0_SESSION_R2",
        "target_session": target_session.isoformat(),
        "research_cutoff": research_cutoff.isoformat(),
        "strict_official_sessions": [session.isoformat() for session in sessions],
        "official_calendar": calendar_identity,
        "namespace_id": pg_result["namespace_id"],
        "source_set": {
            "source_set_id": source_set["source_set_id"],
            "revision_id": source_set["revision_id"],
            "package_sha256": str(source_set["package_sha256"]).strip(),
            "source_content_id": source_set["source_content_id"],
            "source_capture_id": source_set["source_capture_id"],
            "parser_contract": source_set["parser_contract"],
            "source_raw_digest": pg_result["target_raw_digest"],
            "source_digest": source_set["source_digest"],
            "seal_status": source_set["seal_status"],
            "sealed_at": _iso(source_set["sealed_at"]),
            "source_time_quality": source_set["source_time_quality"],
            "source_available_at": _iso(source_set["source_available_at"]),
            "source_clock_role": source_set["source_clock_role"],
            "price_unit_status": source_set["price_unit_status"],
            "amount_unit_status": source_set["amount_unit_status"],
            "calendar_id": source_set["calendar_id"],
            "calendar_revision": source_set["calendar_revision"],
            "calendar_sha256": source_set["calendar_sha256"],
        },
        "source_pool": {
            "source_count": source_count,
            "target_present_count": source_set["target_present_count"],
            "target_missing_count": source_set["target_missing_count"],
            "read_failed_count": source_set["read_failed_count"],
            "scenario_state_counts": {
                key: state_counts.get(key, 0) for key in ("TRUE", "FALSE", "UNKNOWN")
            },
            "scenario_state_partition_closed": sum(state_counts.values())
            == source_count,
            "unknown_main_reason_counts": dict(sorted(unknown_reasons.items())),
            "unknown_main_reason_partition_closed": sum(unknown_reasons.values())
            == state_counts.get("UNKNOWN", 0),
            "false_main_reason_counts": dict(sorted(false_reasons.items())),
            "evaluated_before_filter_and_pagination": True,
        },
        "selection": {
            "state": state,
            "symbol": symbol,
            "matched_count": selected_count,
            "offset": offset,
            "limit": limit,
            "page_count": len(page),
            "next_offset": offset + len(page)
            if offset + len(page) < selected_count
            else None,
            "ordering": "source_symbol COLLATE C ASC",
        },
        "results": page,
        "execution": {
            "transaction": pg_result["transaction"],
            "raw_history_batch_reads": pg_result["raw_history_batch_reads"],
            "raw_history_rows_read": pg_result["raw_history_rows_read"],
            "history_grid_cells": pg_result["history_grid_cells"],
            "missing_history_cells": pg_result["missing_history_cells"],
            "pg_source_set_read_seconds": pg_result["pg_source_set_read_seconds"],
            "pg_target_scan_seconds": pg_result["pg_target_scan_seconds"],
            "pg_history_scan_seconds": pg_result["pg_history_scan_seconds"],
            "pg_read_total_seconds": pg_result["pg_read_total_seconds"],
            "pg_and_validation_cpu_seconds": read_cpu_seconds,
            "local_calculation_cpu_seconds": calculate_cpu_seconds,
            "local_calculation_seconds": calculate_seconds,
            "total_wall_seconds": time.perf_counter() - process_started,
            "database_write_attempted": pg_result["database_write_attempted"],
            "ddl_executed": pg_result["ddl_executed"],
            "dml_executed": pg_result["dml_executed"],
            "source_zip_opened": pg_result["source_zip_opened"],
            "source_scan_performed": pg_result["source_scan_performed"],
        },
    }


__all__ = ["SourceD0RawError", "query_source_d0_raw_launch"]
