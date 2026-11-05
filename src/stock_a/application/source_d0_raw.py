"""来源股票D0 LAUNCH_CONFIRM诊断级原始场景只读应用用例。"""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime
from fractions import Fraction
import re
import time
from typing import Any, Mapping
from uuid import UUID

from stock_a.application.source_set_candidate import (
    SourceSetCandidateApplicationError,
    _official_calendar,
)
from stock_a.research.d0_launch_raw import (
    calculate_d0_launch_confirm_raw,
    fraction_payload,
)
from stock_a.storage.postgres.d0_raw import (
    MAX_SOURCE_SYMBOLS,
    D0RawPgError,
    read_source_d0_raw_window,
)


_SYMBOL = re.compile(r"^(SH|SZ)\.[0-9]{6}$")
_STATUSES = {"TRUE", "FALSE", "UNKNOWN", "ALL"}
_TRANSITIONS = (
    "FALSE_TO_TRUE",
    "FALSE_TO_FALSE",
    "FALSE_TO_UNKNOWN",
    "TRUE_TO_TRUE",
    "TRUE_TO_FALSE",
    "TRUE_TO_UNKNOWN",
    "UNKNOWN_TO_TRUE",
    "UNKNOWN_TO_FALSE",
    "UNKNOWN_TO_UNKNOWN",
)
_CHANGE_SELECTIONS = {*_TRANSITIONS, "BEFORE_ONLY", "AFTER_ONLY", "ALL"}
_MAX_LIMIT = MAX_SOURCE_SYMBOLS
_MAX_CHANGE_LIMIT = 2 * MAX_SOURCE_SYMBOLS


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


def _classify_transition(before_state: str | None, after_state: str | None) -> str:
    if before_state is None and after_state is not None:
        return "AFTER_ONLY"
    if after_state is None and before_state is not None:
        return "BEFORE_ONLY"
    if before_state is None and after_state is None:
        raise SourceD0RawError("D0_CHANGE_EMPTY_PAIR", "变化联结键不能同时缺少前后端。")
    if before_state not in {"TRUE", "FALSE", "UNKNOWN"} or after_state not in {
        "TRUE",
        "FALSE",
        "UNKNOWN",
    }:
        raise SourceD0RawError(
            "D0_CHANGE_STATE_INVALID", "变化联结端点必须是TRUE/FALSE/UNKNOWN。"
        )
    return f"{before_state}_TO_{after_state}"


def _canonical_expected_namespace(value: str | None) -> str | None:
    if value is None:
        return None
    try:
        canonical = str(UUID(value))
    except (ValueError, TypeError, AttributeError) as error:
        raise SourceD0RawError(
            "D0_NAMESPACE_ID_INVALID",
            "expected_namespace_id必须是规范小写UUID。",
            category="ARGUMENT",
        ) from error
    if canonical != value:
        raise SourceD0RawError(
            "D0_NAMESPACE_ID_INVALID",
            "expected_namespace_id必须是规范小写UUID。",
            category="ARGUMENT",
        )
    return canonical


def query_source_d0_raw_launch(
    config: Mapping[str, Any],
    *,
    scenario: str,
    target_session: date,
    research_cutoff: date,
    source_set_id: str,
    package_sha256: str,
    state: str | None = None,
    symbol: str | None = None,
    offset: int = 0,
    limit: int = 30,
    action: str = "current",
    before_session: date | None = None,
    before_source_set_id: str | None = None,
    before_package_sha256: str | None = None,
    transition: str | None = None,
    expected_namespace_id: str | None = None,
    _history_symbol: str | None = None,
) -> dict[str, Any]:
    """读取LAUNCH原始状态，或联结相邻官方会话的三态变化。"""
    expected_namespace_id = _canonical_expected_namespace(expected_namespace_id)
    if _history_symbol is not None and (
        action != "current"
        or not isinstance(_history_symbol, str)
        or _SYMBOL.fullmatch(_history_symbol) is None
    ):
        raise SourceD0RawError(
            "D0_SOURCE_SYMBOL_INVALID", "内部历史窗口代码无效。", category="ARGUMENT"
        )
    if action == "changes":
        return _query_source_d0_raw_launch_changes(
            config,
            scenario=scenario,
            before_session=before_session,
            before_source_set_id=before_source_set_id,
            before_package_sha256=before_package_sha256,
            target_session=target_session,
            research_cutoff=research_cutoff,
            source_set_id=source_set_id,
            package_sha256=package_sha256,
            state=state,
            symbol=symbol,
            offset=offset,
            limit=limit,
            transition=transition,
            expected_namespace_id=expected_namespace_id,
        )
    if action != "current":
        raise SourceD0RawError(
            "D0_ACTION_INVALID",
            "动作只允许current或changes。",
            category="ARGUMENT",
        )
    if any(
        value is not None
        for value in (
            before_session,
            before_source_set_id,
            before_package_sha256,
            transition,
        )
    ):
        raise SourceD0RawError(
            "D0_CHANGES_ARGUMENTS_REQUIRE_CHANGES_ACTION",
            "相邻变化参数只能与--action changes一起使用。",
            category="ARGUMENT",
        )
    state = "ALL" if state is None else state
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
            expected_namespace_id=expected_namespace_id,
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
            include_history_window=source_symbol in (symbol, _history_symbol),
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


def _query_source_d0_raw_launch_changes(
    config: Mapping[str, Any],
    *,
    scenario: str,
    before_session: date | None,
    before_source_set_id: str | None,
    before_package_sha256: str | None,
    target_session: date,
    research_cutoff: date,
    source_set_id: str,
    package_sha256: str,
    state: str | None,
    symbol: str | None,
    offset: int,
    limit: int,
    transition: str | None,
    expected_namespace_id: str | None,
) -> dict[str, Any]:
    if scenario != "LAUNCH_CONFIRM":
        raise SourceD0RawError(
            "D0_SCENARIO_NOT_IMPLEMENTED",
            "本读者只实现LAUNCH_CONFIRM诊断raw谓词；其他场景标记NOT_IMPLEMENTED。",
            category="ARGUMENT",
        )
    if state is not None:
        raise SourceD0RawError(
            "D0_STATE_NOT_ALLOWED_FOR_CHANGES",
            "changes动作不接受单日--state；请使用--transition。",
            category="ARGUMENT",
        )
    if (
        type(before_session) is not date
        or type(target_session) is not date
        or type(research_cutoff) is not date
        or before_session >= target_session
        or target_session > research_cutoff
        or before_session > research_cutoff
    ):
        raise SourceD0RawError(
            "D0_CHANGE_DATE_INVALID",
            "changes需要截止日以内且前后有序的两个官方会话。",
            category="ARGUMENT",
        )
    if (
        not isinstance(before_source_set_id, str)
        or not before_source_set_id
        or not isinstance(before_package_sha256, str)
        or not isinstance(package_sha256, str)
        or before_package_sha256 != package_sha256
    ):
        raise SourceD0RawError(
            "D0_CHANGE_PACKAGE_IDENTITY_MISMATCH",
            "相邻比较两端必须显式绑定同一个完整来源包SHA。",
            category="ARGUMENT",
        )
    if transition is None:
        transition = "ALL"
    if transition not in _CHANGE_SELECTIONS:
        raise SourceD0RawError(
            "D0_TRANSITION_INVALID",
            "变化筛选须为九种三态转移、BEFORE_ONLY、AFTER_ONLY或ALL。",
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
    if type(limit) is not int or not 1 <= limit <= _MAX_CHANGE_LIMIT:
        raise SourceD0RawError(
            "D0_LIMIT_INVALID",
            f"相邻变化分页limit必须为1至{_MAX_CHANGE_LIMIT}之间的整数。",
            category="ARGUMENT",
        )

    try:
        after_calendar = _official_calendar(target_session)
        before_index = after_calendar["sessions"].index(before_session)
        after_index = after_calendar["sessions"].index(target_session)
    except (SourceSetCandidateApplicationError, ValueError) as error:
        raise SourceD0RawError("D0_OFFICIAL_CALENDAR_INVALID", str(error)) from error
    if after_index != before_index + 1:
        raise SourceD0RawError(
            "D0_SESSIONS_NOT_ADJACENT",
            "前后日期必须是同一版本官方交易日历中的相邻会话。",
            category="ARGUMENT",
        )
    try:
        before_calendar = _official_calendar(before_session)
    except SourceSetCandidateApplicationError as error:
        raise SourceD0RawError("D0_OFFICIAL_CALENDAR_INVALID", str(error)) from error
    before_identity = {
        key: before_calendar[key]
        for key in ("calendar_id", "calendar_revision", "calendar_sha256")
    }
    after_identity = {
        key: after_calendar[key]
        for key in ("calendar_id", "calendar_revision", "calendar_sha256")
    }
    if before_identity != after_identity or after_index != before_index + 1:
        raise SourceD0RawError(
            "D0_SESSIONS_NOT_ADJACENT",
            "前后日期必须是同一版本官方交易日历中的相邻会话。",
            category="ARGUMENT",
        )
    before_sessions = tuple(
        before_calendar["sessions"][max(0, before_index - 20) : before_index + 1]
    )
    after_sessions = tuple(
        after_calendar["sessions"][max(0, after_index - 20) : after_index + 1]
    )
    if len(before_sessions) != 21 or len(after_sessions) != 21:
        raise SourceD0RawError(
            "D0_CHANGE_WINDOW_LEFT_TRUNCATED",
            "相邻变化的两端都必须具备目标会话及前20个官方会话。",
        )

    started = time.perf_counter()
    try:
        before_result = query_source_d0_raw_launch(
            config,
            scenario=scenario,
            target_session=before_session,
            research_cutoff=research_cutoff,
            source_set_id=before_source_set_id,
            package_sha256=before_package_sha256,
            state="ALL",
            offset=0,
            limit=_MAX_LIMIT,
            expected_namespace_id=expected_namespace_id,
            _history_symbol=symbol,
        )
        after_result = query_source_d0_raw_launch(
            config,
            scenario=scenario,
            target_session=target_session,
            research_cutoff=research_cutoff,
            source_set_id=source_set_id,
            package_sha256=package_sha256,
            state="ALL",
            offset=0,
            limit=_MAX_LIMIT,
            expected_namespace_id=expected_namespace_id,
            _history_symbol=symbol,
        )
    except SourceD0RawError:
        raise

    before_source = before_result["source_set"]
    after_source = after_result["source_set"]
    compatible_identity = (
        before_result["namespace_id"] == after_result["namespace_id"]
        and before_source["package_sha256"] == after_source["package_sha256"]
        and before_source["source_content_id"] == after_source["source_content_id"]
        and before_result["official_calendar"] == after_result["official_calendar"]
        and before_result["session_contract"] == after_result["session_contract"]
        and before_result["algorithm_contract"] == after_result["algorithm_contract"]
        and before_result["producer_version"] == after_result["producer_version"]
    )
    if not compatible_identity:
        raise SourceD0RawError(
            "D0_PAIR_NOT_COMPARABLE",
            "两端命名空间、完整包、来源内容、日历或算法身份不兼容，拒绝计算转移。",
        )

    def indexed_rows(
        result: Mapping[str, Any],
    ) -> dict[tuple[str, str], dict[str, Any]]:
        pool = result["source_pool"]
        rows = result["results"]
        if (
            len(rows) != pool["source_count"]
            or result["selection"]["matched_count"] != pool["source_count"]
            or result["selection"]["offset"] != 0
            or result["selection"]["limit"] < pool["source_count"]
        ):
            raise SourceD0RawError(
                "D0_CHANGE_ENDPOINT_INCOMPLETE",
                "相邻变化端点没有先计算并返回完整来源股票池。",
            )
        indexed: dict[tuple[str, str], dict[str, Any]] = {}
        for item in rows:
            source_symbol = item.get("source_symbol")
            market = item.get("market")
            if (
                not isinstance(source_symbol, str)
                or _SYMBOL.fullmatch(source_symbol) is None
                or market not in {"SH", "SZ"}
                or source_symbol[:2] != market
            ):
                raise SourceD0RawError(
                    "D0_CHANGE_SOURCE_KEY_INVALID",
                    "来源股票变化键必须包含相符的交易所与来源代码。",
                )
            key = (market, source_symbol)
            if key in indexed:
                raise SourceD0RawError(
                    "D0_CHANGE_SOURCE_KEY_DUPLICATED", "端点来源股票联结键重复。"
                )
            indexed[key] = item
        return indexed

    before_rows = indexed_rows(before_result)
    after_rows = indexed_rows(after_result)
    before_keys = set(before_rows)
    after_keys = set(after_rows)
    union_keys = before_keys | after_keys
    counts: Counter[str] = Counter()
    unknown_causes: Counter[str] = Counter()
    changes: list[dict[str, Any]] = []
    for key in sorted(union_keys, key=lambda item: (item[1], item[0])):
        before_item = before_rows.get(key)
        after_item = after_rows.get(key)
        transition_name = _classify_transition(
            None if before_item is None else before_item["scenario_raw"],
            None if after_item is None else after_item["scenario_raw"],
        )
        if before_item is not None and after_item is not None:
            if before_item["scenario_raw"] == "UNKNOWN":
                unknown_causes[
                    str(before_item.get("main_reason") or "UNKNOWN_REASON_UNAVAILABLE")
                ] += 1
            if after_item["scenario_raw"] == "UNKNOWN":
                unknown_causes[
                    str(after_item.get("main_reason") or "UNKNOWN_REASON_UNAVAILABLE")
                ] += 1
        counts[transition_name] += 1
        item = {
            "market": key[0],
            "source_symbol": key[1],
            "transition": transition_name,
            "prior_unknown": before_item is not None
            and before_item["scenario_raw"] == "UNKNOWN",
            "before": before_item,
            "after": after_item,
            "identity_status": "SOURCE_SYMBOL_ONLY",
            "price_coordinate": "RAW_UNADJUSTED",
            "pit_status": "NON_PIT_RETROSPECTIVE",
            "scenario_role": "DIAGNOSTIC_ONLY",
            "formal_admission_status": "NOT_AUTHORIZED_SOURCE_SYMBOL_DIAGNOSTIC",
        }
        changes.append(item)

    selected = [
        item
        for item in changes
        if (transition == "ALL" or item["transition"] == transition)
        and (symbol is None or item["source_symbol"] == symbol)
    ]
    matched_count = len(selected)
    page = selected[offset : offset + limit]
    if sum(counts.values()) != len(union_keys):
        raise SourceD0RawError(
            "D0_CHANGE_PARTITION_OPEN", "变化分区没有闭合到来源键并集。"
        )
    comparable_count = len(before_keys & after_keys)
    transition_counts = {name: counts.get(name, 0) for name in _TRANSITIONS}
    if sum(transition_counts.values()) != comparable_count:
        raise SourceD0RawError(
            "D0_CHANGE_COMPARABLE_PARTITION_OPEN",
            "九种三态转移没有闭合到双端可比交集。",
        )

    def endpoint_summary(result: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "target_session": result["target_session"],
            "research_cutoff": result["research_cutoff"],
            "strict_official_sessions": result["strict_official_sessions"],
            "official_calendar": result["official_calendar"],
            "namespace_id": result["namespace_id"],
            "source_set": result["source_set"],
            "source_pool": result["source_pool"],
            "session_contract": result["session_contract"],
            "algorithm_contract": result["algorithm_contract"],
            "scenario_role": result["scenario_role"],
            "formal_admission_status": result["formal_admission_status"],
            "research_universe_status": result["research_universe_status"],
            "absolute_liquidity_cny_status": result["absolute_liquidity_cny_status"],
            "stable_security_id_status": result["stable_security_id_status"],
            "pit_status": result["pit_status"],
            "price_coordinate": "RAW_UNADJUSTED",
            "execution": result["execution"],
        }

    return {
        "ok": True,
        "contract": "STOCK_A_D0_LAUNCH_ADJACENT_TRANSITION_RAW_R1",
        "producer_version": "STOCK_A_D0_LAUNCH_RAW_READER_R1",
        "action": "changes",
        "scenario": scenario,
        "pair_status": "COMPARABLE",
        "transition_semantics": "THREE_VALUED_KLEENE_CROSS_SESSION",
        "scenario_role": "DIAGNOSTIC_ONLY",
        "scenario_raw": "PER_SOURCE_SYMBOL_TRUE_FALSE_UNKNOWN",
        "formal_admission_status": "NOT_AUTHORIZED_SOURCE_SYMBOL_DIAGNOSTIC",
        "research_universe_status": "UNKNOWN",
        "absolute_liquidity_cny_status": "UNKNOWN_UNIT_UNVERIFIED",
        "turn_status": "UNKNOWN_NOT_PRODUCED",
        "stable_security_id_status": "STABLE_SECURITY_ID_UNVERIFIED",
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "episode_status": "NOT_ISSUED",
        "session_contract": "D0_SESSION_R2",
        "research_cutoff": research_cutoff.isoformat(),
        "price_coordinate": "RAW_UNADJUSTED",
        "entity_identity": "SOURCE_SYMBOL_ONLY",
        "before": endpoint_summary(before_result),
        "after": endpoint_summary(after_result),
        "source_key_set": {
            "key": "market+source_symbol",
            "before_count": len(before_keys),
            "after_count": len(after_keys),
            "comparable_intersection_count": comparable_count,
            "source_union_count": len(union_keys),
            "before_only_count": counts.get("BEFORE_ONLY", 0),
            "after_only_count": counts.get("AFTER_ONLY", 0),
            "pair_not_comparable_count": 0,
            "union_partition_closed": len(before_keys & after_keys)
            + counts.get("BEFORE_ONLY", 0)
            + counts.get("AFTER_ONLY", 0)
            == len(union_keys),
        },
        "transition_counts": transition_counts,
        "missing_endpoint_counts": {
            "BEFORE_ONLY": counts.get("BEFORE_ONLY", 0),
            "AFTER_ONLY": counts.get("AFTER_ONLY", 0),
        },
        "unknown_endpoint_main_reason_counts": dict(sorted(unknown_causes.items())),
        "selection": {
            "transition": transition,
            "symbol": symbol,
            "matched_count": matched_count,
            "offset": offset,
            "limit": limit,
            "page_count": len(page),
            "next_offset": offset + len(page)
            if offset + len(page) < matched_count
            else None,
            "ordering": "source_symbol COLLATE C ASC, market COLLATE C ASC",
            "evaluated_before_filter_and_pagination": True,
        },
        "results": page,
        "execution": {
            "transaction": "2 x REPEATABLE READ READ ONLY",
            "database_write_attempted": False,
            "ddl_executed": False,
            "dml_executed": False,
            "source_zip_opened": False,
            "source_scan_performed": False,
            "total_wall_seconds": time.perf_counter() - started,
        },
    }


__all__ = ["SourceD0RawError", "query_source_d0_raw_launch"]
