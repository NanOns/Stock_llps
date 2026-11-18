"""用户手动触发的有界BaoStock日线读取、规范化与PG保存。"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from typing import Any
from uuid import UUID

from stock_a.domain import ContractError, FieldValue
from stock_a.sources.baostock.client import query_daily_range_rows
from stock_a.sources.baostock.daily import BaoUnitContract, normalize_daily_row
from stock_a.sources.baostock.identity import normalize_baostock_symbol
from stock_a.storage.postgres import baostock as postgres_baostock


_FIELD_MAP = {
    "session_date": "date",
    "source_code": "code",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
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
            unique = set(raw_symbols)
            if any(
                not isinstance(symbol, str)
                or normalize_baostock_symbol(symbol) != symbol
                for symbol in unique
            ):
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
        max_queries_per_target_session=max_queries,
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
