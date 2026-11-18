"""用户手动触发的有界BaoStock日线读取、规范化与PG保存。"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from typing import Any
from uuid import UUID

from stock_a.domain import ContractError
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
_MAX_SYMBOLS_PER_RANGE = 20
_MAX_CALENDAR_DAYS_PER_RANGE = 14
_MAX_ROWS_PER_SYMBOL = 20


class BaoStockDailyJobError(ValueError):
    """BaoStock手动来源作业的请求或响应合同错误。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


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


def _request_budget(config: Mapping[str, Any]) -> tuple[int, float]:
    source_policy = config.get("source_policy")
    bao = (
        source_policy.get("baostock_supplement")
        if isinstance(source_policy, Mapping)
        else None
    )
    if not isinstance(bao, Mapping) or bao.get("request_policy") != "MANUAL_REQUESTED":
        raise BaoStockDailyJobError(
            "BAOSTOCK_REQUEST_NOT_ENABLED", "BaoStock来源策略未显式启用用户手动请求"
        )
    budget = bao.get("request_budget")
    if not isinstance(budget, Mapping):
        raise BaoStockDailyJobError("REQUEST_BUDGET_INVALID", "缺少BaoStock请求预算")
    max_logins = budget.get("max_logins")
    max_queries = budget.get("max_queries_per_target_session")
    timeout = budget.get("query_timeout_seconds")
    if (
        type(max_logins) is not int
        or max_logins != 1
        or type(max_queries) is not int
        or not 1 <= max_queries <= _MAX_SYMBOLS_PER_RANGE
        or isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or not 0.2 <= float(timeout) <= 30.0
    ):
        raise BaoStockDailyJobError(
            "REQUEST_BUDGET_INVALID", "BaoStock请求预算超出有界合同"
        )
    return max_queries, float(timeout)


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
    if (
        any(value is None for value in symbols)
        or len(set(symbols)) != len(symbols)
        or len(symbols) > _MAX_SYMBOLS_PER_RANGE
    ):
        raise BaoStockDailyJobError(
            "SOURCE_SYMBOLS_INVALID", "证券代码须有效、不重复且不超过客户端上限"
        )
    max_queries, timeout = _request_budget(config)
    if len(symbols) > max_queries:
        raise BaoStockDailyJobError(
            "SOURCE_REQUEST_BUDGET_EXCEEDED",
            "BaoStock目标池超过当前每范围查询预算；拒绝静默抽样或扩大网络请求",
        )

    response = query_daily_range_rows(
        symbols=symbols,
        start_date=sessions[0].isoformat(),
        end_date=sessions[-1].isoformat(),
        timeout_seconds=timeout,
        max_rows_per_symbol=min(_MAX_ROWS_PER_SYMBOL, len(sessions)),
        transport=transport,
    )
    response_status = response.get("status")
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
    planned_symbol_sessions = len(symbols) * len(sessions)
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
        "rejected_rows": rejected_rows,
        "non_observed_field_values": field_unknown_count,
        "succeeded_symbol_sessions": successful_bar_rows,
        "unknown_count": unknown_symbol_sessions,
        "query_status": response_status,
        "query_calls": response.get("query_calls", 0),
        "retry_count": response.get("retry_count", 0),
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
    reader = readback or postgres_baostock.read_baostock_daily_capture
    try:
        verified = reader(
            config,
            capture_id=capture_id,
            source_symbols=symbols,
            start_session=sessions[0],
            end_session=sessions[-1],
            cutoff=cutoff,
            limit=500,
            offset=0,
        )
    except Exception as error:
        outcome.update(
            {
                "status": "PERSISTED_READBACK_FAILED",
                "readback_error_type": type(error).__name__,
                "readback_error_code": getattr(error, "code", "PG_READ_FAILED"),
            }
        )
        return outcome
    if not isinstance(verified, Mapping):
        raise BaoStockDailyJobError(
            "READBACK_RESULT_INVALID", "BaoStock独立读回未返回typed结果"
        )
    read_rows = verified.get("rows")
    page = verified.get("page")
    if not isinstance(read_rows, list) or not isinstance(page, Mapping):
        raise BaoStockDailyJobError(
            "READBACK_RESULT_INVALID", "BaoStock独立读回缺少分页行和计数"
        )
    expected_keys = {
        (
            record.source_symbol,
            record.observation.session_date.isoformat(),
            units.version,
        )
        for record in records
    }
    expected_digests = {
        (
            record.source_symbol,
            record.observation.session_date.isoformat(),
            units.version,
        ): postgres_baostock.normalized_baostock_record_sha256(
            capture_id, record, units, cutoff=cutoff
        )
        for record in records
    }
    actual_digests = {
        (
            row.get("source_symbol"),
            row.get("session_date"),
            row.get("unit_contract_version"),
        ): str(row.get("normalized_sha256", "")).strip()
        for row in read_rows
        if isinstance(row, Mapping)
    }
    page_count = page.get("count")
    page_total = page.get("total")
    readback_matches = (
        verified.get("status") == "AVAILABLE"
        and type(page_count) is int
        and page_count == len(expected_keys)
        and type(page_total) is int
        and page_total == len(expected_keys)
        and len(read_rows) == len(expected_keys)
        and set(actual_digests) == expected_keys
        and actual_digests == expected_digests
    )
    outcome.update(
        {
            "pg_readback_verified": readback_matches,
            "pg_readback_rows": page_count,
            "pg_readback_total": page_total,
        }
    )
    if not readback_matches:
        outcome["status"] = "PG_READBACK_MISMATCH"
        return outcome
    if unknown_symbol_sessions > 0 or field_unknown_count > 0:
        outcome["status"] = "INCOMPLETE"
    else:
        outcome["status"] = "COMPLETED"
    return outcome
