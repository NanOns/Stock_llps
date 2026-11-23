"""用户手动触发的有界BaoStock日线读取、规范化与PG保存。"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from math import isfinite
from time import perf_counter, sleep
from typing import Any
from uuid import UUID

from stock_a.domain import ContractError, FieldValue
from stock_a.sources.baostock.client import query_daily_range_rows
from stock_a.sources.baostock.daily import (
    BaoDailyObservation,
    BaoUnitContract,
    normalize_daily_row,
)
from stock_a.sources.baostock.identity import normalize_baostock_symbol
from stock_a.storage.postgres import baostock as postgres_baostock


_FIELD_MAP = {
    "session_date": "date",
    "source_code": "code",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "preclose": "preclose",
    "volume": "volume",
    "amount": "amount",
    "turn": "turn",
    "tradestatus": "tradestatus",
    "isST": "isST",
}
_MAX_CALENDAR_DAYS_PER_RANGE = 14
_MAX_ROWS_PER_SYMBOL = 20
_MAX_QUERIES_PER_TARGET_SESSION = 10
_MAX_LOGIN_BATCHES_PER_JOB = 10000
_MAX_RAW_SOURCE_ROWS_PER_SYMBOL = 250
_CAPTURE_CLOCK_ROLE = "CAPTURED_AT_THIS_QUERY_NOT_HISTORICAL_FIRST_AVAILABLE"
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


class BaoStockDailyJobError(ValueError):
    """BaoStock手动来源作业的请求或响应合同错误。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


@dataclass(frozen=True)
class BaoStockQfqOhlc:
    """BaoStock前复权价格坐标，保持原始精度并与未复权事实隔离。"""

    open: FieldValue
    high: FieldValue
    low: FieldValue
    close: FieldValue
    preclose: FieldValue


@dataclass(frozen=True)
class BaoStockPriceWindowRecord:
    """一证券一官方会话的双坐标内存配对；缺Bar保持未知，不推断停牌。"""

    source_symbol: str
    requested_source_code: str
    source_code: str
    session_date: date
    raw_observation: BaoDailyObservation | None
    qfq_ohlc: BaoStockQfqOhlc | None
    qfq_observation: BaoDailyObservation | None
    response_fields_by_coordinate: Mapping[str, tuple[str, ...]]
    raw_captured_at: datetime | None
    qfq_captured_at: datetime | None
    coverage_status: str
    time_role: str = "NON_PIT_RETROSPECTIVE"


def _readback_record_matches(
    row: Mapping[str, Any],
    record: postgres_baostock.BaoStockDailyRecord,
    *,
    capture_id: UUID,
    units: BaoUnitContract,
) -> bool:
    """逐字段核对BaoStock PG读回，不用逐行摘要替代typed事实。"""
    observation = record.observation
    expected_identity = {
        "capture_id": str(capture_id),
        "source_code": observation.source_code,
        "source_symbol": record.source_symbol,
        "session_date": observation.session_date.isoformat(),
        "captured_at": record.captured_at,
        "capture_clock_role": _CAPTURE_CLOCK_ROLE,
        "adjustment_flag": record.adjustment_flag,
        "price_coordinate": record.price_coordinate,
        "unit_contract_version": units.version,
        "returned_fields": list(observation.returned_fields),
        "close_input_unit": units.close_input_unit,
        "close_to_cny": units.close_to_cny,
        "amount_input_unit": units.amount_input_unit,
        "amount_to_cny": units.amount_to_cny,
        "volume_input_unit": units.volume_input_unit,
        "volume_to_shares": units.volume_to_shares,
        "turn_input_unit": units.turn_input_unit,
    }
    if any(row.get(name) != value for name, value in expected_identity.items()):
        return False

    fields = row.get("fields")
    if not isinstance(fields, Mapping):
        return False
    for name in _FIELD_NAMES:
        expected = getattr(observation, name)
        actual = fields.get(name)
        if not isinstance(expected, FieldValue) or not isinstance(actual, FieldValue):
            return False
        expected_value = expected.value
        if name in {"tradestatus", "is_st"} and expected.quality == "OBSERVED":
            expected_value = str(expected_value)
        if (
            actual.value != expected_value
            or actual.quality != expected.quality
            or actual.reason != expected.reason
        ):
            return False
    return True


def _valid_sessions(
    target_sessions: Sequence[date],
    official_sessions: Sequence[date],
    *,
    cutoff: date,
) -> tuple[date, ...]:
    if (
        not isinstance(target_sessions, Sequence)
        or isinstance(target_sessions, (str, bytes))
        or not isinstance(official_sessions, Sequence)
        or isinstance(official_sessions, (str, bytes))
        or not target_sessions
        or type(cutoff) is not date
    ):
        raise BaoStockDailyJobError(
            "SESSION_PLAN_INVALID", "必须显式提供官方会话、目标会话和截止日"
        )
    targets = tuple(target_sessions)
    calendar = tuple(official_sessions)
    if (
        any(type(value) is not date for value in targets)
        or any(type(value) is not date for value in calendar)
        or tuple(sorted(set(calendar))) != calendar
        or tuple(sorted(set(targets))) != targets
        or any(value > cutoff for value in targets)
    ):
        raise BaoStockDailyJobError(
            "SESSION_PLAN_INVALID", "会话日期必须唯一、递增并不超过显式截止日"
        )
    if len(targets) > _MAX_ROWS_PER_SYMBOL:
        raise BaoStockDailyJobError(
            "SESSION_BUDGET_EXCEEDED", "目标会话超过BaoStock单证券响应上限"
        )
    positions = [calendar.index(value) for value in targets if value in calendar]
    if len(positions) != len(targets) or positions != list(
        range(positions[0], positions[0] + len(positions))
    ):
        raise BaoStockDailyJobError(
            "OFFICIAL_SESSION_REQUIRED", "目标日期必须是官方日历中的连续会话范围"
        )
    if (targets[-1] - targets[0]).days > _MAX_CALENDAR_DAYS_PER_RANGE:
        raise BaoStockDailyJobError(
            "SESSION_RANGE_TOO_WIDE", "BaoStock单次范围最多跨14个日历日"
        )
    return targets


def _request_budget(config: Mapping[str, Any]) -> tuple[int, int, float]:
    source_policy = config.get("source_policy")
    bao = (
        source_policy.get("baostock_supplement")
        if isinstance(source_policy, Mapping)
        else None
    )
    if not isinstance(bao, Mapping):
        raise BaoStockDailyJobError(
            "BAOSTOCK_POLICY_INVALID", "缺少BaoStock来源与请求预算配置"
        )
    budget = bao.get("request_budget")
    if not isinstance(budget, Mapping):
        raise BaoStockDailyJobError("REQUEST_BUDGET_INVALID", "缺少BaoStock请求预算")
    max_logins = budget.get("max_logins")
    max_queries = budget.get("max_queries_per_target_session")
    timeout = budget.get("query_timeout_seconds")
    if (
        type(max_logins) is not int
        or not 1 <= max_logins <= _MAX_LOGIN_BATCHES_PER_JOB
        or type(max_queries) is not int
        or not 1 <= max_queries <= _MAX_QUERIES_PER_TARGET_SESSION
        or isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or not 0.2 <= float(timeout) <= 30.0
    ):
        raise BaoStockDailyJobError(
            "REQUEST_BUDGET_INVALID", "BaoStock请求预算超出有界合同"
        )
    return max_logins, max_queries, float(timeout)


def _request_budget_plan(
    *,
    requested_symbols: int,
    target_session_count: int,
    calendar_day_span: int,
    max_login_batches: int,
    max_queries_per_target_session: int,
) -> dict[str, int]:
    """按每个登录批次可用的单会话查询预算核算来源范围。"""
    request_units_per_symbol = target_session_count
    max_symbols_per_batch = max_queries_per_target_session
    requests_required = requested_symbols * request_units_per_symbol
    required_login_batches = (
        requested_symbols + max_symbols_per_batch - 1
    ) // max_symbols_per_batch
    total_request_budget = (
        max_queries_per_target_session * target_session_count * max_login_batches
    )
    over_batch_bound = (
        requested_symbols if required_login_batches > max_login_batches else 0
    )
    return {
        "requested_symbols": requested_symbols,
        "target_session_count": target_session_count,
        "calendar_day_span": calendar_day_span,
        "request_units_per_symbol": request_units_per_symbol,
        "max_login_batches": max_login_batches,
        "required_login_batches": required_login_batches,
        "max_queries_per_target_session": max_queries_per_target_session,
        "total_request_budget": total_request_budget,
        "requests_required": requests_required,
        "max_symbols_per_batch": max_symbols_per_batch,
        "symbols_over_batch_bound": over_batch_bound,
        "not_attempted_symbols": (requested_symbols if over_batch_bound else 0),
    }


def plan_baostock_capture_batches(
    target_sessions: Sequence[date],
    symbols_by_session: Mapping[str, Sequence[str]],
    *,
    max_login_batches: int,
    max_queries_per_target_session: int,
) -> dict[str, Any]:
    """按准确Raw成员的连续会话段切分有预算的BaoStock登录批次。"""
    sessions = tuple(target_sessions)
    if (
        not sessions
        or any(type(value) is not date for value in sessions)
        or tuple(sorted(set(sessions))) != sessions
        or not isinstance(symbols_by_session, Mapping)
        or type(max_login_batches) is not int
        or not 1 <= max_login_batches <= _MAX_LOGIN_BATCHES_PER_JOB
        or type(max_queries_per_target_session) is not int
        or not 1 <= max_queries_per_target_session <= _MAX_QUERIES_PER_TARGET_SESSION
    ):
        raise BaoStockDailyJobError(
            "BATCH_PLAN_INVALID", "BaoStock批次计划输入或登录预算无效"
        )

    ranges: list[tuple[date, ...]] = []
    for session in sessions:
        if not ranges or (session - ranges[-1][0]).days > _MAX_CALENDAR_DAYS_PER_RANGE:
            ranges.append((session,))
        else:
            ranges[-1] = (*ranges[-1], session)

    batches: list[dict[str, Any]] = []
    symbol_session_units = 0
    for date_range in ranges:
        presence: dict[str, set[date]] = {}
        for session in date_range:
            raw_symbols = symbols_by_session.get(session.isoformat(), ())
            if not isinstance(raw_symbols, Sequence) or isinstance(
                raw_symbols, (str, bytes)
            ):
                raise BaoStockDailyJobError(
                    "BATCH_SYMBOLS_INVALID", "BaoStock每会话来源代码池必须是序列"
                )
            if any(
                not isinstance(symbol, str)
                or normalize_baostock_symbol(symbol) != symbol
                for symbol in raw_symbols
            ):
                raise BaoStockDailyJobError(
                    "BATCH_SYMBOLS_INVALID", "BaoStock来源代码必须规范且不重复"
                )
            unique = set(raw_symbols)
            if len(unique) != len(raw_symbols):
                raise BaoStockDailyJobError(
                    "BATCH_SYMBOLS_INVALID", "BaoStock来源代码必须规范且不重复"
                )
            symbol_session_units += len(unique)
            for symbol in unique:
                presence.setdefault(symbol, set()).add(session)

        symbols_by_run: dict[tuple[date, ...], list[str]] = {}
        position_by_session = {
            session: index for index, session in enumerate(date_range)
        }
        for symbol, present_sessions in presence.items():
            ordered = sorted(present_sessions, key=position_by_session.__getitem__)
            run: list[date] = []
            previous_position: int | None = None
            for session in ordered:
                position = position_by_session[session]
                if previous_position is not None and position != previous_position + 1:
                    symbols_by_run.setdefault(tuple(run), []).append(symbol)
                    run = []
                run.append(session)
                previous_position = position
            if run:
                symbols_by_run.setdefault(tuple(run), []).append(symbol)

        for run_sessions, run_symbols in sorted(
            symbols_by_run.items(),
            key=lambda item: (item[0][0], item[0][-1]),
        ):
            ordered_symbols = tuple(sorted(run_symbols))
            for offset in range(
                0, len(ordered_symbols), max_queries_per_target_session
            ):
                batches.append(
                    {
                        "range_start": date_range[0],
                        "range_end": date_range[-1],
                        "target_sessions": run_sessions,
                        "source_symbols": ordered_symbols[
                            offset : offset + max_queries_per_target_session
                        ],
                    }
                )

    required_batches = len(batches)
    return {
        "status": (
            "READY" if required_batches <= max_login_batches else "BLOCKED_BUDGET"
        ),
        "target_session_count": len(sessions),
        "symbol_session_units": symbol_session_units,
        "required_login_batches": required_batches,
        "allowed_login_batches": max_login_batches,
        "max_queries_per_target_session": max_queries_per_target_session,
        "batch_count": required_batches,
        "batches": tuple(batches),
    }


def _response_time(value: object) -> datetime:
    if not isinstance(value, str):
        raise BaoStockDailyJobError("CAPTURE_CLOCK_INVALID", "来源响应缺少实际捕获时钟")
    try:
        captured_at = datetime.fromisoformat(value)
    except ValueError as error:
        raise BaoStockDailyJobError(
            "CAPTURE_CLOCK_INVALID", "来源捕获时钟不是有效ISO时间"
        ) from error
    if captured_at.tzinfo is None or captured_at.utcoffset() is None:
        raise BaoStockDailyJobError("CAPTURE_CLOCK_INVALID", "来源捕获时钟必须包含时区")
    return captured_at


def fetch_baostock_price_window(
    symbols: Sequence[str],
    official_sessions: Sequence[date],
    *,
    cutoff: date,
    units: BaoUnitContract,
    max_api_calls_per_batch: int,
    max_login_batches: int,
    query_timeout_seconds: float,
    max_history_session_count: int,
    max_retry_attempts: int,
    retry_backoff_seconds: float,
    coordinates: Sequence[str] | None = None,
    transport: object | None = None,
    request_attempt_offsets: Mapping[str, Mapping[str, int]] | None = None,
) -> dict[str, Any]:
    """按显式证券和官方会话读取并配对BaoStock未复权/前复权价格。"""
    if (
        not isinstance(symbols, Sequence)
        or isinstance(symbols, (str, bytes))
        or not symbols
    ):
        raise BaoStockDailyJobError(
            "SOURCE_SYMBOLS_REQUIRED", "必须显式提供BaoStock证券代码"
        )
    normalized_symbols = tuple(normalize_baostock_symbol(value) for value in symbols)
    if any(value is None for value in normalized_symbols) or len(
        set(normalized_symbols)
    ) != len(normalized_symbols):
        raise BaoStockDailyJobError(
            "SOURCE_SYMBOLS_INVALID", "BaoStock证券代码须有效且不重复"
        )
    if (
        not isinstance(official_sessions, Sequence)
        or isinstance(official_sessions, (str, bytes))
        or not official_sessions
        or any(type(value) is not date for value in official_sessions)
        or tuple(sorted(set(official_sessions))) != tuple(official_sessions)
        or type(max_history_session_count) is not int
        or not 1 <= max_history_session_count <= 250
        or len(official_sessions) > max_history_session_count
        or type(cutoff) is not date
        or any(value > cutoff for value in official_sessions)
    ):
        raise BaoStockDailyJobError(
            "OFFICIAL_SESSION_REQUIRED",
            "必须提供唯一、递增且不超过cutoff的1至250个官方会话",
        )
    if not isinstance(units, BaoUnitContract):
        raise BaoStockDailyJobError("UNIT_CONTRACT_REQUIRED", "缺少BaoStock单位合同")
    if (
        type(max_api_calls_per_batch) is not int
        or not 1 <= max_api_calls_per_batch <= 100
        or type(max_login_batches) is not int
        or not 1 <= max_login_batches <= _MAX_LOGIN_BATCHES_PER_JOB
        or type(max_retry_attempts) is not int
        or not 0 <= max_retry_attempts <= 3
        or isinstance(query_timeout_seconds, bool)
        or not isinstance(query_timeout_seconds, (int, float))
        or not isfinite(float(query_timeout_seconds))
        or not 0.2 <= float(query_timeout_seconds) <= 30.0
        or isinstance(retry_backoff_seconds, bool)
        or not isinstance(retry_backoff_seconds, (int, float))
        or not isfinite(float(retry_backoff_seconds))
        or not 0 <= float(retry_backoff_seconds) <= 60.0
    ):
        raise BaoStockDailyJobError(
            "REQUEST_BUDGET_INVALID",
            "API/登录预算、重试次数、超时或退避时间超出有界合同",
        )

    target_sessions = tuple(official_sessions)
    source_symbols = tuple(normalized_symbols)
    all_coordinate_specs = (
        ("3", "RAW_UNADJUSTED"),
        ("2", "FORWARD_ADJUSTED"),
    )
    selected_coordinates = (
        tuple(coordinates)
        if coordinates is not None
        else tuple(coordinate for _, coordinate in all_coordinate_specs)
    )
    if (
        not selected_coordinates
        or len(set(selected_coordinates)) != len(selected_coordinates)
        or any(
            coordinate not in {item[1] for item in all_coordinate_specs}
            for coordinate in selected_coordinates
        )
    ):
        raise BaoStockDailyJobError(
            "PRICE_COORDINATES_INVALID",
            "价格坐标请求必须是未重复的RAW/QFQ坐标子集。",
        )
    coordinate_specs = tuple(
        item for item in all_coordinate_specs if item[1] in selected_coordinates
    )
    request_attempt_offsets = request_attempt_offsets or {}
    if not isinstance(request_attempt_offsets, Mapping) or any(
        not isinstance(attempts, Mapping)
        or any(
            symbol not in source_symbols
            or type(attempt_count) is not int
            or attempt_count < 0
            for symbol, attempt_count in attempts.items()
        )
        for attempts in request_attempt_offsets.values()
    ):
        raise BaoStockDailyJobError(
            "REQUEST_ATTEMPT_OFFSETS_INVALID",
            "来源请求台账中的历史尝试次数无效。",
        )
    request_attempts: dict[str, dict[str, dict[str, Any]]] = {
        coordinate: {
            symbol: {
                "request_started_at": None,
                "request_completed_at": None,
                "status": "NOT_ATTEMPTED",
                "response_row_count": 0,
                "attempt_count": int(
                    request_attempt_offsets.get(coordinate, {}).get(symbol, 0)
                ),
                "local_error": None,
                "returned_fields": None,
            }
            for symbol in normalized_symbols
        }
        for _, coordinate in coordinate_specs
    }
    initial_batches_per_coordinate = (
        len(source_symbols) + max_api_calls_per_batch - 1
    ) // max_api_calls_per_batch
    initial_login_batches = initial_batches_per_coordinate * len(coordinate_specs)
    total_api_budget = max_login_batches * max_api_calls_per_batch
    planned_api_calls = len(source_symbols) * len(coordinate_specs)
    started = perf_counter()

    if (
        initial_login_batches > max_login_batches
        or planned_api_calls > total_api_budget
    ):
        for by_symbol in request_attempts.values():
            for attempt in by_symbol.values():
                attempt["status"] = "NOT_ATTEMPTED_BUDGET"
                attempt["local_error"] = "REQUEST_BUDGET_EXCEEDED"
        return {
            "status": "NOT_ATTEMPTED_BUDGET",
            "completion_status": "INCOMPLETE",
            "source": "BAOSTOCK",
            "time_role": "NON_PIT_RETROSPECTIVE",
            "capture_clock_role": _CAPTURE_CLOCK_ROLE,
            "cutoff": cutoff.isoformat(),
            "target_sessions": [session.isoformat() for session in target_sessions],
            "requested_symbols": len(source_symbols),
            "planned_api_calls": planned_api_calls,
            "total_api_budget": total_api_budget,
            "max_api_calls_per_batch": max_api_calls_per_batch,
            "max_login_batches": max_login_batches,
            "required_initial_login_batches": initial_login_batches,
            "login_batches": 0,
            "login_budget_batches": 0,
            "login_batch_count_unknown": 0,
            "api_call_count_unknown_batches": 0,
            "api_calls": 0,
            "retry_count": 0,
            "retry_passes_used": 0,
            "records": (),
            "record_count": 0,
            "errors": (),
            "request_attempts": request_attempts,
            "unattempted_symbols": {
                coordinate: list(source_symbols) for _, coordinate in coordinate_specs
            },
            "coordinate_stats": {
                coordinate: {
                    "adjustflag": flag,
                    "api_calls": 0,
                    "login_batches": 0,
                    "elapsed_seconds": 0.0,
                    "successful_symbols": [],
                    "failed_symbols": [],
                    "unattempted_symbols": list(source_symbols),
                    "first_captured_at": None,
                    "last_captured_at": None,
                }
                for flag, coordinate in coordinate_specs
            },
            "reason": "初始双坐标请求超过显式登录/API预算，未登录、未发送查询",
            "source_network_called": False,
            "database_write": False,
            "database_write_attempted": False,
            "elapsed_seconds": perf_counter() - started,
        }

    coordinate_rows: dict[str, dict[str, dict[date, BaoDailyObservation]]] = {
        coordinate: {} for _, coordinate in coordinate_specs
    }
    coordinate_capture_times: dict[str, dict[str, datetime]] = {
        coordinate: {} for _, coordinate in coordinate_specs
    }
    attempted_symbols: dict[str, set[str]] = {
        coordinate: set() for _, coordinate in coordinate_specs
    }
    coordinate_stats: dict[str, dict[str, Any]] = {
        coordinate: {
            "adjustflag": flag,
            "api_calls": 0,
            "login_batches": 0,
            "elapsed_seconds": 0.0,
            "successful_symbols": [],
            "failed_symbols": [],
            "unattempted_symbols": [],
            "first_captured_at": None,
            "last_captured_at": None,
        }
        for flag, coordinate in coordinate_specs
    }
    errors: list[dict[str, Any]] = []
    login_batches = 0
    login_budget_batches = 0
    login_batch_count_unknown = 0
    api_call_count_unknown_batches = 0
    api_calls = 0
    retry_api_calls = 0
    retry_passes_used = 0
    required_fields = {
        "date",
        "code",
        "open",
        "high",
        "low",
        "close",
        "preclose",
        "volume",
        "amount",
        "adjustflag",
        "turn",
        "tradestatus",
        "isST",
    }

    for attempt in range(max_retry_attempts + 1):
        if attempt > 0:
            if all(
                len(coordinate_rows[coordinate]) == len(source_symbols)
                for _, coordinate in coordinate_specs
            ):
                break
            if login_budget_batches >= max_login_batches:
                break
            retry_passes_used = attempt
            delay = min(float(retry_backoff_seconds) * (2 ** (attempt - 1)), 60.0)
            if delay > 0:
                sleep(delay)

        for adjustflag, coordinate in coordinate_specs:
            pending_symbols = [
                symbol
                for symbol in source_symbols
                if symbol not in coordinate_rows[coordinate]
            ]
            coordinate_started = perf_counter()
            for offset in range(0, len(pending_symbols), max_api_calls_per_batch):
                batch_symbols = tuple(
                    pending_symbols[offset : offset + max_api_calls_per_batch]
                )
                symbols_to_query = list(batch_symbols)

                network_response: Mapping[str, Any] = {}
                network_responses: Mapping[str, Any] = {}
                remote_attempted: set[str] = set()
                if symbols_to_query:
                    if login_budget_batches >= max_login_batches:
                        break
                    login_budget_batches += 1
                    try:
                        network_response = query_daily_range_rows(
                            symbols=tuple(symbols_to_query),
                            start_date=target_sessions[0].isoformat(),
                            end_date=target_sessions[-1].isoformat(),
                            timeout_seconds=float(query_timeout_seconds),
                            max_rows_per_symbol=_MAX_RAW_SOURCE_ROWS_PER_SYMBOL,
                            adjustflag=adjustflag,
                            max_api_calls=max_api_calls_per_batch,
                            target_session_count=len(target_sessions),
                            transport=transport,
                        )
                    except Exception as error:
                        login_batch_count_unknown += 1
                        api_call_count_unknown_batches += 1
                        for symbol in symbols_to_query:
                            request_attempt = request_attempts[coordinate][symbol]
                            request_attempt["attempt_count"] += 1
                            request_attempt["status"] = "CLIENT_EXCEPTION"
                            request_attempt["local_error"] = type(error).__name__
                        errors.append(
                            {
                                "adjustflag": adjustflag,
                                "coordinate": coordinate,
                                "attempt": attempt,
                                "symbols": list(symbols_to_query),
                                "status": "CLIENT_EXCEPTION",
                                "error_type": type(error).__name__,
                            }
                        )
                        continue
                    if not isinstance(network_response, Mapping):
                        network_response = {}
                    session_cleanup_failed = (
                        network_response.get("status") == "LOGOUT_FAILED"
                    )
                    login_attempted = network_response.get("login_attempted") is True
                    query_count = network_response.get("query_calls", 0)
                    if type(query_count) is not int or not 0 <= query_count <= len(
                        symbols_to_query
                    ):
                        errors.append(
                            {
                                "adjustflag": adjustflag,
                                "coordinate": coordinate,
                                "attempt": attempt,
                                "symbols": list(symbols_to_query),
                                "status": "API_CALL_COUNT_INVALID",
                            }
                        )
                        query_count = (
                            min(len(symbols_to_query), max(0, query_count))
                            if type(query_count) is int
                            else 0
                        )
                    if query_count > 0 and not login_attempted:
                        errors.append(
                            {
                                "adjustflag": adjustflag,
                                "coordinate": coordinate,
                                "attempt": attempt,
                                "symbols": list(symbols_to_query),
                                "status": "API_CALLS_WITHOUT_LOGIN",
                            }
                        )
                        query_count = 0
                    if login_attempted:
                        login_batches += 1
                        coordinate_stats[coordinate]["login_batches"] += 1
                    else:
                        login_budget_batches -= 1
                    api_calls += query_count
                    coordinate_stats[coordinate]["api_calls"] += query_count
                    if attempt > 0:
                        retry_api_calls += query_count
                    remote_attempted = set(symbols_to_query[:query_count])
                    attempted_symbols[coordinate].update(remote_attempted)
                    remote_responses = network_response.get("responses")
                    if isinstance(remote_responses, Mapping):
                        network_responses = remote_responses
                response_map = network_responses
                batch_errors: list[dict[str, Any]] = []
                for symbol in batch_symbols:
                    if symbol not in remote_attempted:
                        if symbol in network_responses:
                            batch_errors.append(
                                {
                                    "adjustflag": adjustflag,
                                    "coordinate": coordinate,
                                    "attempt": attempt,
                                    "symbol": symbol,
                                    "status": "RESPONSE_FOR_UNATTEMPTED_SYMBOL",
                                }
                            )
                        continue
                    symbol_response = response_map.get(symbol)
                    request_attempt = request_attempts[coordinate][symbol]
                    request_attempt["attempt_count"] += 1
                    if isinstance(symbol_response, Mapping):
                        request_started_at = symbol_response.get("request_started_at")
                        request_completed_at = symbol_response.get(
                            "request_completed_at"
                        )
                        if request_attempt["request_started_at"] is None and isinstance(
                            request_started_at, str
                        ):
                            request_attempt["request_started_at"] = request_started_at
                        if isinstance(request_completed_at, str):
                            request_attempt["request_completed_at"] = (
                                request_completed_at
                            )
                    if isinstance(symbol_response, Mapping):
                        response_rows = symbol_response.get("rows")
                        request_attempt["response_row_count"] = (
                            len(response_rows) if isinstance(response_rows, list) else 0
                        )
                        request_attempt["status"] = (
                            "LOGOUT_FAILED"
                            if session_cleanup_failed
                            else symbol_response.get("status", "QUERY_FAILED")
                        )
                        request_attempt["local_error"] = (
                            network_response.get("logout_error_code") or "LOGOUT_FAILED"
                            if session_cleanup_failed
                            else symbol_response.get("local_error")
                            or symbol_response.get("query_error_code")
                        )
                        if isinstance(symbol_response.get("response_fields"), list):
                            request_attempt["returned_fields"] = tuple(
                                symbol_response["response_fields"]
                            )
                    else:
                        request_attempt["status"] = "NO_SYMBOL_RESPONSE"
                        request_attempt["response_row_count"] = 0
                        request_attempt["local_error"] = network_response.get("status")
                    if (
                        session_cleanup_failed
                        or not isinstance(symbol_response, Mapping)
                        or symbol_response.get("status") != "RESPONSE_RECEIVED"
                        or symbol_response.get("complete") is not True
                    ):
                        batch_errors.append(
                            {
                                "adjustflag": adjustflag,
                                "coordinate": coordinate,
                                "attempt": attempt,
                                "symbol": symbol,
                                "status": (
                                    "LOGOUT_FAILED"
                                    if session_cleanup_failed
                                    else symbol_response.get(
                                        "status", "NO_SYMBOL_RESPONSE"
                                    )
                                    if isinstance(symbol_response, Mapping)
                                    else "NO_SYMBOL_RESPONSE"
                                ),
                                "error_code": (
                                    network_response.get("logout_error_code")
                                    or "LOGOUT_FAILED"
                                    if session_cleanup_failed
                                    else symbol_response.get("local_error")
                                    if isinstance(symbol_response, Mapping)
                                    else network_response.get("status")
                                ),
                                "query_error_code": (
                                    symbol_response.get("query_error_code")
                                    if isinstance(symbol_response, Mapping)
                                    else None
                                ),
                            }
                        )
                        continue

                    response_fields = symbol_response.get("response_fields")
                    rows = symbol_response.get("rows")
                    if (
                        not isinstance(response_fields, list)
                        or len(response_fields) != len(set(response_fields))
                        or not required_fields.issubset(response_fields)
                        or not isinstance(rows, list)
                        or symbol_response.get("adjustflag") != adjustflag
                        or symbol_response.get("coordinate") != coordinate
                    ):
                        request_attempt["status"] = "RESPONSE_REJECTED"
                        request_attempt["local_error"] = (
                            "RESPONSE_FIELDS_OR_COORDINATE_INVALID"
                        )
                        batch_errors.append(
                            {
                                "adjustflag": adjustflag,
                                "coordinate": coordinate,
                                "attempt": attempt,
                                "symbol": symbol,
                                "status": "RESPONSE_FIELDS_OR_COORDINATE_INVALID",
                            }
                        )
                        continue

                    request_attempt["returned_fields"] = tuple(response_fields)

                    try:
                        captured_at = _response_time(
                            symbol_response.get("request_completed_at")
                        )
                    except BaoStockDailyJobError as error:
                        request_attempt["status"] = "RESPONSE_REJECTED"
                        request_attempt["local_error"] = error.code
                        batch_errors.append(
                            {
                                "adjustflag": adjustflag,
                                "coordinate": coordinate,
                                "attempt": attempt,
                                "symbol": symbol,
                                "status": error.code,
                            }
                        )
                        continue

                    symbol_rows: dict[date, BaoDailyObservation] = {}
                    invalid_reason: str | None = None
                    for row in rows:
                        if not isinstance(row, Mapping):
                            invalid_reason = "ROW_NOT_MAPPING"
                            break
                        raw_date = row.get("date")
                        try:
                            session_date = date.fromisoformat(raw_date)
                        except (TypeError, ValueError):
                            invalid_reason = "ROW_DATE_INVALID"
                            break
                        if (
                            session_date.isoformat() != raw_date
                            or session_date not in target_sessions
                            or session_date > cutoff
                            or normalize_baostock_symbol(row.get("code")) != symbol
                            or row.get("adjustflag") != adjustflag
                        ):
                            invalid_reason = "ROW_OUTSIDE_OFFICIAL_WINDOW_OR_COORDINATE"
                            break
                        if session_date in symbol_rows:
                            invalid_reason = "DUPLICATE_SYMBOL_SESSION"
                            break
                        try:
                            observation = normalize_daily_row(
                                row,
                                response_fields,
                                _FIELD_MAP,
                                units,
                                adjustflag=adjustflag,
                            )
                        except ContractError as error:
                            invalid_reason = getattr(
                                error, "code", "ROW_CONTRACT_INVALID"
                            )
                            break
                        if observation.session_date != session_date:
                            invalid_reason = "NORMALIZED_SESSION_MISMATCH"
                            break
                        symbol_rows[session_date] = observation

                    if invalid_reason is not None:
                        request_attempt["status"] = "RESPONSE_REJECTED"
                        request_attempt["local_error"] = invalid_reason
                        batch_errors.append(
                            {
                                "adjustflag": adjustflag,
                                "coordinate": coordinate,
                                "attempt": attempt,
                                "symbol": symbol,
                                "status": invalid_reason,
                            }
                        )
                        continue

                    coordinate_rows[coordinate][symbol] = symbol_rows
                    coordinate_capture_times[coordinate][symbol] = captured_at
                    stats = coordinate_stats[coordinate]
                    first = stats["first_captured_at"]
                    last = stats["last_captured_at"]
                    stats["first_captured_at"] = (
                        captured_at if first is None else min(first, captured_at)
                    )
                    stats["last_captured_at"] = (
                        captured_at if last is None else max(last, captured_at)
                    )
                errors.extend(batch_errors)

            coordinate_stats[coordinate]["elapsed_seconds"] += max(
                0.0, perf_counter() - coordinate_started
            )
            if login_budget_batches >= max_login_batches:
                break

        if login_budget_batches >= max_login_batches:
            break

    records: list[BaoStockPriceWindowRecord] = []
    for symbol in source_symbols:
        raw_rows = coordinate_rows.get("RAW_UNADJUSTED", {}).get(symbol, {})
        qfq_rows = coordinate_rows.get("FORWARD_ADJUSTED", {}).get(symbol, {})
        raw_captured_at = coordinate_capture_times.get("RAW_UNADJUSTED", {}).get(symbol)
        qfq_captured_at = coordinate_capture_times.get("FORWARD_ADJUSTED", {}).get(
            symbol
        )
        for session in target_sessions:
            raw_observation = raw_rows.get(session)
            qfq_observation = qfq_rows.get(session)
            response_fields_by_coordinate = {
                coordinate: tuple(
                    request_attempts[coordinate][symbol]["returned_fields"]
                )
                for _, coordinate in coordinate_specs
                if request_attempts[coordinate][symbol].get("returned_fields")
                is not None
            }
            qfq_ohlc = (
                BaoStockQfqOhlc(
                    open=qfq_observation.open,
                    high=qfq_observation.high,
                    low=qfq_observation.low,
                    close=qfq_observation.close,
                    preclose=qfq_observation.preclose,
                )
                if qfq_observation is not None
                else None
            )
            if raw_observation is not None and qfq_ohlc is not None:
                coverage_status = "PAIRED_RAW_AND_QFQ"
            elif raw_observation is not None:
                coverage_status = "QFQ_BAR_MISSING_UNKNOWN"
            elif qfq_ohlc is not None:
                coverage_status = "RAW_BAR_MISSING_UNKNOWN"
            else:
                coverage_status = "NO_BAR_IN_EITHER_COORDINATE_UNKNOWN"
            records.append(
                BaoStockPriceWindowRecord(
                    source_symbol=symbol,
                    requested_source_code=symbol.lower(),
                    source_code=(
                        raw_observation.source_code
                        if raw_observation is not None
                        else qfq_observation.source_code
                        if qfq_observation is not None
                        else symbol.lower()
                    ),
                    session_date=session,
                    raw_observation=raw_observation,
                    qfq_ohlc=qfq_ohlc,
                    qfq_observation=qfq_observation,
                    response_fields_by_coordinate=response_fields_by_coordinate,
                    raw_captured_at=raw_captured_at,
                    qfq_captured_at=qfq_captured_at,
                    coverage_status=coverage_status,
                )
            )

    unattempted_by_coordinate: dict[str, list[str]] = {}
    failed_by_coordinate: dict[str, list[str]] = {}
    all_queries_completed = True
    for _, coordinate in coordinate_specs:
        successful = set(coordinate_rows[coordinate])
        attempted = attempted_symbols[coordinate]
        unattempted = [symbol for symbol in source_symbols if symbol not in attempted]
        failed = [
            symbol
            for symbol in source_symbols
            if symbol in attempted and symbol not in successful
        ]
        unattempted_by_coordinate[coordinate] = unattempted
        failed_by_coordinate[coordinate] = failed
        stats = coordinate_stats[coordinate]
        stats["successful_symbols"] = [
            symbol for symbol in source_symbols if symbol in successful
        ]
        stats["failed_symbols"] = failed
        stats["unattempted_symbols"] = unattempted
        if len(successful) != len(source_symbols):
            all_queries_completed = False

    pair_count = sum(
        record.coverage_status == "PAIRED_RAW_AND_QFQ" for record in records
    )
    missing_bar_count = len(records) - pair_count
    return {
        "status": "COMPLETED" if all_queries_completed else "INCOMPLETE",
        "completion_status": "COMPLETED" if all_queries_completed else "INCOMPLETE",
        "source": "BAOSTOCK",
        "time_role": "NON_PIT_RETROSPECTIVE",
        "capture_clock_role": _CAPTURE_CLOCK_ROLE,
        "cutoff": cutoff.isoformat(),
        "target_sessions": [session.isoformat() for session in target_sessions],
        "requested_symbols": len(source_symbols),
        "planned_api_calls": planned_api_calls,
        "total_api_budget": total_api_budget,
        "max_api_calls_per_batch": max_api_calls_per_batch,
        "max_login_batches": max_login_batches,
        "required_initial_login_batches": initial_login_batches,
        "login_batches": login_batches,
        "login_budget_batches": login_budget_batches,
        "login_batch_count_unknown": login_batch_count_unknown,
        "api_call_count_unknown_batches": api_call_count_unknown_batches,
        "api_calls": api_calls,
        "retry_count": retry_api_calls,
        "retry_passes_used": retry_passes_used,
        "records": tuple(records),
        "record_count": len(records),
        "paired_record_count": pair_count,
        "missing_bar_pair_count": missing_bar_count,
        "coverage_status": (
            "COMPLETE_COORDINATE_ROWS"
            if missing_bar_count == 0
            else "INCOMPLETE_NO_BAR_UNKNOWN"
        ),
        "errors": tuple(errors),
        "request_attempts": request_attempts,
        "failed_symbols": failed_by_coordinate,
        "unattempted_symbols": unattempted_by_coordinate,
        "coordinate_stats": coordinate_stats,
        "source_network_called": transport is None
        and (login_batches > 0 or login_batch_count_unknown > 0),
        "database_write": False,
        "database_write_attempted": False,
        "elapsed_seconds": perf_counter() - started,
    }


def run_manual_baostock_daily_capture(
    config: Mapping[str, Any],
    *,
    manual_start: bool,
    capture_id: UUID,
    source_symbols: Sequence[str],
    target_sessions: Sequence[date],
    official_sessions: Sequence[date],
    cutoff: date,
    units: BaoUnitContract,
    transport: object | None = None,
    persist: Callable[..., Mapping[str, int]] | None = None,
    readback: Callable[..., Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """读取一个有界官方会话区间；无Bar及字段未知保留未知语义。"""
    if not isinstance(config, Mapping):
        raise BaoStockDailyJobError("RUNTIME_CONFIG_INVALID", "缺少唯一运行配置")
    if manual_start is not True:
        raise BaoStockDailyJobError(
            "MANUAL_START_REQUIRED", "BaoStock采集必须由用户手动启动"
        )
    if not isinstance(capture_id, UUID):
        raise BaoStockDailyJobError("CAPTURE_ID_INVALID", "BaoStock捕获标识必须是UUID")
    if not isinstance(units, BaoUnitContract):
        raise BaoStockDailyJobError(
            "UNIT_CONTRACT_REQUIRED", "BaoStock来源需要显式单位合同"
        )
    sessions = _valid_sessions(target_sessions, official_sessions, cutoff=cutoff)
    if (
        not isinstance(source_symbols, Sequence)
        or isinstance(source_symbols, (str, bytes))
        or not source_symbols
    ):
        raise BaoStockDailyJobError(
            "SOURCE_SYMBOLS_REQUIRED", "必须显式提供来源证券代码"
        )
    symbols = tuple(normalize_baostock_symbol(value) for value in source_symbols)
    if any(value is None for value in symbols) or len(set(symbols)) != len(symbols):
        raise BaoStockDailyJobError("SOURCE_SYMBOLS_INVALID", "证券代码须有效且不重复")
    max_logins, max_queries, timeout = _request_budget(config)
    budget_plan = _request_budget_plan(
        requested_symbols=len(symbols),
        target_session_count=len(sessions),
        calendar_day_span=(sessions[-1] - sessions[0]).days + 1,
        max_login_batches=max_logins,
        max_queries_per_target_session=max_queries,
    )
    planned_symbol_sessions = len(symbols) * len(sessions)
    if budget_plan["required_login_batches"] > max_logins:
        session_symbol_counts = {
            value.isoformat(): {
                "requested": len(symbols),
                "attempted": 0,
                "not_attempted": len(symbols),
            }
            for value in sessions
        }
        return {
            "status": "NOT_ATTEMPTED_BUDGET",
            "completion_status": "INCOMPLETE",
            "source": "BAOSTOCK",
            "capture_id": str(capture_id),
            "cutoff": cutoff.isoformat(),
            "target_sessions": [value.isoformat() for value in sessions],
            "requested_symbols": len(symbols),
            "planned_symbol_sessions": planned_symbol_sessions,
            "attempted_symbol_queries": 0,
            "attempted_symbol_sessions": 0,
            "successful_bar_rows": 0,
            "unknown_symbol_sessions": 0,
            "no_bar_unknown_sessions": 0,
            "failed_symbol_queries": 0,
            "not_attempted_symbols": len(symbols),
            "not_attempted_symbol_sessions": planned_symbol_sessions,
            "session_symbol_counts": session_symbol_counts,
            "persisted_rows_by_session": {value.isoformat(): 0 for value in sessions},
            "rejected_rows": 0,
            "non_observed_field_values": 0,
            "succeeded_symbol_sessions": 0,
            "unknown_count": planned_symbol_sessions,
            "query_status": "REQUEST_BUDGET_EXCEEDED",
            "query_calls": 0,
            "retry_count": 0,
            "budget_plan": budget_plan,
            "reason": (
                "BaoStock目标证券数超出当前逐会话查询预算；未抽样、未登录且未发送网络请求"
            ),
            "adjustment_flag": "3",
            "price_coordinate": "RAW_UNADJUSTED",
            "source_network_called": False,
            "database_write": False,
            "database_write_attempted": False,
        }
    if len(symbols) > max_queries:
        raise BaoStockDailyJobError(
            "SYMBOL_BATCH_REQUIRED",
            "本批来源代码超过单批查询预算；数据任务必须先按预算拆分证券批次。",
        )

    response = query_daily_range_rows(
        symbols=symbols,
        start_date=sessions[0].isoformat(),
        end_date=sessions[-1].isoformat(),
        timeout_seconds=timeout,
        max_rows_per_symbol=min(_MAX_ROWS_PER_SYMBOL, len(sessions)),
        max_api_calls=max_queries,
        target_session_count=len(sessions),
        transport=transport,
    )
    response_status = response.get("status")
    if response_status == "REQUEST_BUDGET_EXCEEDED":
        session_symbol_counts = {
            value.isoformat(): {
                "requested": len(symbols),
                "attempted": 0,
                "not_attempted": len(symbols),
            }
            for value in sessions
        }
        return {
            "status": "NOT_ATTEMPTED_BUDGET",
            "completion_status": "INCOMPLETE",
            "source": "BAOSTOCK",
            "capture_id": str(capture_id),
            "cutoff": cutoff.isoformat(),
            "target_sessions": [value.isoformat() for value in sessions],
            "requested_symbols": len(symbols),
            "planned_symbol_sessions": planned_symbol_sessions,
            "attempted_symbol_queries": 0,
            "attempted_symbol_sessions": 0,
            "successful_bar_rows": 0,
            "unknown_symbol_sessions": 0,
            "no_bar_unknown_sessions": 0,
            "failed_symbol_queries": 0,
            "not_attempted_symbols": len(symbols),
            "not_attempted_symbol_sessions": planned_symbol_sessions,
            "session_symbol_counts": session_symbol_counts,
            "persisted_rows_by_session": {value.isoformat(): 0 for value in sessions},
            "rejected_rows": 0,
            "non_observed_field_values": 0,
            "succeeded_symbol_sessions": 0,
            "unknown_count": planned_symbol_sessions,
            "query_status": response_status,
            "query_calls": 0,
            "retry_count": 0,
            "budget_plan": response.get("budget_plan", budget_plan),
            "reason": response.get("reason", "超出BaoStock逐会话查询预算；未发起请求"),
            "adjustment_flag": "3",
            "price_coordinate": "RAW_UNADJUSTED",
            "source_network_called": False,
            "database_write": False,
            "database_write_attempted": False,
        }
    symbol_responses = response.get("responses")
    if not isinstance(symbol_responses, Mapping):
        symbol_responses = {}
    attempted_queries = response.get("query_calls", 0)
    if type(attempted_queries) is not int or attempted_queries < 0:
        attempted_queries = 0

    session_set = set(sessions)
    records: list[postgres_baostock.BaoStockDailyRecord] = []
    returned_by_symbol: dict[str, set[date]] = {}
    query_failed_symbols = 0
    no_bar_sessions = 0
    rejected_rows = 0
    for symbol in symbols:
        result = symbol_responses.get(symbol)
        if not isinstance(result, Mapping):
            continue
        if (
            result.get("status") != "RESPONSE_RECEIVED"
            or result.get("complete") is not True
        ):
            query_failed_symbols += 1
            continue
        rows = result.get("rows")
        fields = result.get("response_fields")
        if not isinstance(rows, list) or not isinstance(fields, list):
            query_failed_symbols += 1
            continue
        if (
            result.get("adjustflag") != "3"
            or result.get("coordinate") != "RAW_UNADJUSTED"
        ):
            query_failed_symbols += 1
            continue
        captured_at = _response_time(result.get("request_completed_at"))
        observed_dates: set[date] = set()
        returned_dates: set[date] = set()
        duplicates: set[date] = set()
        symbol_records: list[postgres_baostock.BaoStockDailyRecord] = []
        for row in rows:
            if isinstance(row, Mapping):
                raw_session = row.get("date")
                if isinstance(raw_session, str):
                    try:
                        parsed_raw_session = date.fromisoformat(raw_session)
                    except ValueError:
                        parsed_raw_session = None
                    if parsed_raw_session in session_set:
                        returned_dates.add(parsed_raw_session)
            try:
                if not isinstance(row, Mapping) or row.get("adjustflag") != "3":
                    raise ContractError("响应行缺少未复权adjustflag=3合同")
                observation = normalize_daily_row(
                    row,
                    fields,
                    _FIELD_MAP,
                    units,
                )
                if (
                    observation.session_date not in session_set
                    or observation.session_date > cutoff
                    or normalize_baostock_symbol(observation.source_code) != symbol
                ):
                    raise ContractError("响应交易日或来源证券代码超出显式请求范围")
                if observation.session_date in observed_dates:
                    duplicates.add(observation.session_date)
                    rejected_rows += 1
                    continue
                observed_dates.add(observation.session_date)
                symbol_records.append(
                    postgres_baostock.BaoStockDailyRecord(
                        source_symbol=symbol,
                        observation=observation,
                        captured_at=captured_at,
                        adjustment_flag=str(result.get("adjustflag", "3")),
                        price_coordinate=str(
                            result.get("coordinate", "RAW_UNADJUSTED")
                        ),
                    )
                )
            except ContractError:
                rejected_rows += 1
        if duplicates:
            rejected_rows += sum(
                1
                for item in symbol_records
                if item.observation.session_date in duplicates
            )
            symbol_records = [
                item
                for item in symbol_records
                if item.observation.session_date not in duplicates
            ]
        returned_by_symbol[symbol] = returned_dates
        records.extend(symbol_records)

    attempted_symbols = min(attempted_queries, len(symbols))
    query_failed_symbols += sum(
        1 for symbol in symbols[:attempted_symbols] if symbol not in symbol_responses
    )
    attempted_symbol_sessions = attempted_symbols * len(sessions)
    successful_bar_rows = len(records)
    unknown_symbol_sessions = max(0, attempted_symbol_sessions - successful_bar_rows)
    for symbol in symbols[:attempted_symbols]:
        if symbol not in returned_by_symbol:
            continue
        no_bar_sessions += len(session_set - returned_by_symbol[symbol])

    field_unknown_count = sum(
        1
        for record in records
        for field_name in (
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
        if getattr(record.observation, field_name).quality != "OBSERVED"
    )
    outcome: dict[str, Any] = {
        "status": "NOT_AVAILABLE" if not records else "SOURCE_OBSERVED_NOT_PERSISTED",
        "source": "BAOSTOCK",
        "capture_id": str(capture_id),
        "cutoff": cutoff.isoformat(),
        "target_sessions": [value.isoformat() for value in sessions],
        "requested_symbols": len(symbols),
        "planned_symbol_sessions": planned_symbol_sessions,
        "attempted_symbol_queries": attempted_queries,
        "attempted_symbol_sessions": attempted_symbol_sessions,
        "successful_bar_rows": successful_bar_rows,
        "unknown_symbol_sessions": unknown_symbol_sessions,
        "no_bar_unknown_sessions": no_bar_sessions,
        "failed_symbol_queries": query_failed_symbols,
        "not_attempted_symbol_sessions": max(
            0, planned_symbol_sessions - attempted_symbol_sessions
        ),
        "not_attempted_symbols": max(0, len(symbols) - attempted_symbols),
        "session_symbol_counts": {
            value.isoformat(): {
                "requested": len(symbols),
                "attempted": attempted_symbols,
                "not_attempted": max(0, len(symbols) - attempted_symbols),
            }
            for value in sessions
        },
        "persisted_rows_by_session": {value.isoformat(): 0 for value in sessions},
        "rejected_rows": rejected_rows,
        "non_observed_field_values": field_unknown_count,
        "succeeded_symbol_sessions": successful_bar_rows,
        "unknown_count": unknown_symbol_sessions,
        "query_status": response_status,
        "query_calls": response.get("query_calls", 0),
        "retry_count": response.get("retry_count", 0),
        "budget_plan": budget_plan,
        "adjustment_flag": "3",
        "price_coordinate": "RAW_UNADJUSTED",
        "no_bar_means_suspension": False,
        "source_network_called": transport is None,
        "synthetic_transport_injected": transport is not None,
        "database_write": False,
        "database_write_attempted": False,
    }
    if not records:
        outcome["status"] = "NOT_AVAILABLE"
        outcome["reason"] = "未取得可验证目标日Bar；缺Bar不推断停牌或退市"
        return outcome

    writer = persist or postgres_baostock.persist_baostock_daily_capture
    try:
        persisted = writer(
            config,
            capture_id=capture_id,
            records=records,
            units=units,
            cutoff=cutoff,
        )
    except Exception as error:
        outcome.update(
            {
                "status": "SOURCE_OBSERVED_PERSIST_FAILED",
                "persistence_error_type": type(error).__name__,
                "persistence_error_code": getattr(error, "code", "PG_WRITE_FAILED"),
                "database_write": False,
                "database_write_attempted": persist is None,
                "persistence_sink_injected": persist is not None,
            }
        )
        return outcome
    if not isinstance(persisted, Mapping):
        raise BaoStockDailyJobError(
            "PERSIST_RESULT_INVALID", "BaoStock来源写入器未返回typed计数"
        )
    inserted = persisted.get("inserted_rows", 0)
    unchanged = persisted.get("unchanged_rows", 0)
    if (
        type(inserted) is not int
        or inserted < 0
        or type(unchanged) is not int
        or unchanged < 0
    ):
        raise BaoStockDailyJobError(
            "PERSIST_RESULT_INVALID", "来源写入行数必须为非负整数"
        )
    if inserted + unchanged != successful_bar_rows:
        outcome["status"] = "PG_READBACK_MISMATCH"
    else:
        outcome["status"] = (
            "COMPLETED"
            if unknown_symbol_sessions == 0 and field_unknown_count == 0
            else "INCOMPLETE"
        )
    outcome.update(
        {
            "inserted_rows": inserted,
            "unchanged_rows": unchanged,
            "persisted_rows": inserted + unchanged,
            "database_write": persist is None,
            "database_write_attempted": persist is None,
            "persistence_sink_injected": persist is not None,
            "pg_readback_verified": False,
        }
    )
    inserted_by_session = persisted.get("inserted_rows_by_session")
    if (
        not isinstance(inserted_by_session, Mapping)
        or any(
            not isinstance(key, str) or type(value) is not int or value < 0
            for key, value in inserted_by_session.items()
        )
        or sum(inserted_by_session.values()) != inserted
    ):
        outcome.update(
            {
                "status": "PERSIST_RESULT_INVALID",
                "persistence_error_code": "PERSIST_SESSION_COUNTS_INVALID",
                "reason": "BaoStock写入回执缺少按官方会话分解的插入行数；结果不可闭合。",
            }
        )
        return outcome
    reader = readback or postgres_baostock.read_baostock_daily_capture
    expected_records = {
        (
            record.source_symbol,
            record.observation.session_date.isoformat(),
            units.version,
        ): record
        for record in records
    }
    read_rows: list[Mapping[str, Any]] = []
    readback_total: int | None = None
    readback_available = True
    pagination_complete = True
    page_offset = 0
    while True:
        try:
            verified = reader(
                config,
                capture_id=capture_id,
                source_symbols=symbols,
                start_session=sessions[0],
                end_session=sessions[-1],
                cutoff=cutoff,
                limit=500,
                offset=page_offset,
            )
        except Exception as error:
            outcome.update(
                {
                    "status": "PERSISTED_READBACK_FAILED",
                    "readback_error_type": type(error).__name__,
                    "readback_error_code": getattr(error, "code", "PG_READ_FAILED"),
                    "pg_readback_verified": False,
                    "pg_readback_rows": len(read_rows),
                    "pg_readback_total": readback_total,
                }
            )
            return outcome
        if not isinstance(verified, Mapping):
            raise BaoStockDailyJobError(
                "READBACK_RESULT_INVALID", "BaoStock独立读回未返回typed结果"
            )
        page_rows = verified.get("rows")
        page = verified.get("page")
        if not isinstance(page_rows, list) or not isinstance(page, Mapping):
            raise BaoStockDailyJobError(
                "READBACK_RESULT_INVALID", "BaoStock独立读回缺少分页行和计数"
            )
        page_count = page.get("count")
        page_total = page.get("total")
        next_offset = page.get("next_offset")
        if (
            type(page_count) is not int
            or page_count < 0
            or page_count > 500
            or page_count != len(page_rows)
            or type(page_total) is not int
            or page_total < 0
            or type(page.get("offset")) is not int
            or page.get("offset") != page_offset
            or (next_offset is not None and type(next_offset) is not int)
            or page_total != len(expected_records)
        ):
            pagination_complete = False
            if readback_total is None and type(page_total) is int and page_total >= 0:
                readback_total = page_total
            break
        if readback_total is None:
            readback_total = page_total
        elif page_total != readback_total:
            pagination_complete = False
            break
        readback_available = (
            readback_available and verified.get("status") == "AVAILABLE"
        )
        if any(not isinstance(row, Mapping) for row in page_rows):
            pagination_complete = False
            break
        read_rows.extend(page_rows)
        expected_next_offset = (
            page_offset + page_count if page_offset + page_count < page_total else None
        )
        if next_offset != expected_next_offset:
            pagination_complete = False
            break
        if next_offset is None:
            break
        if page_count == 0:
            pagination_complete = False
            break
        page_offset = next_offset

    actual_rows: dict[tuple[object, ...], Mapping[str, Any]] = {}
    duplicate_readback_key = False
    for row in read_rows:
        key = (
            row.get("source_symbol"),
            row.get("session_date"),
            row.get("unit_contract_version"),
        )
        if key in actual_rows:
            duplicate_readback_key = True
        actual_rows[key] = row
    readback_matches = (
        pagination_complete
        and readback_available
        and readback_total == len(expected_records)
        and len(read_rows) == readback_total
        and len(read_rows) == len(expected_records)
        and not duplicate_readback_key
        and set(actual_rows) == set(expected_records)
        and all(
            _readback_record_matches(
                actual_rows[key],
                expected_record,
                capture_id=capture_id,
                units=units,
            )
            for key, expected_record in expected_records.items()
        )
    )
    outcome.update(
        {
            "pg_readback_verified": readback_matches,
            "pg_readback_rows": len(read_rows),
            "pg_readback_total": readback_total,
        }
    )
    if not readback_matches:
        outcome["status"] = "PG_READBACK_MISMATCH"
        return outcome
    persisted_rows_by_session = {value.isoformat(): 0 for value in sessions}
    for record in records:
        session_key = record.observation.session_date.isoformat()
        persisted_rows_by_session[session_key] += 1
    outcome["persisted_rows_by_session"] = persisted_rows_by_session
    outcome["inserted_rows_by_session"] = {
        value.isoformat(): int(inserted_by_session.get(value.isoformat(), 0))
        for value in sessions
    }
    if unknown_symbol_sessions > 0 or field_unknown_count > 0:
        outcome["status"] = "INCOMPLETE"
    else:
        outcome["status"] = "COMPLETED"
    return outcome
