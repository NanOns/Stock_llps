"""为来源市场宽度CLI和组合阅读命令提供唯一只读装配。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, localcontext
from fractions import Fraction
import re
from typing import Any, Mapping, Sequence

from stock_a.domain.market_presentation import (
    classify_market_breadth,
    market_environment_label,
)
from stock_a.research.entry_context.errors import ResearchEntryError
from stock_a.research.entry_context.sessions import _load_calendar
from stock_a.research.entry_history.support import trade_status_state_from_pg_row
from stock_a.research.source_market_breadth import (
    QfqPriceObservation,
    SourceCohortIdentity,
    SourceMarketBreadthInput,
    SourceMarketBreadthResult,
    SourceMarketBreadthSymbol,
    SOURCE_MARKET_BREADTH_CONTRACT,
    calculate_source_market_breadth,
)
from stock_a.storage.postgres.baostock_price_facts import (
    BaoStockPriceFactsError,
    read_baostock_capture_roster,
    read_baostock_price_history_batch,
)

_ELIGIBLE_CLASSES = (
    "COMPARABLE_GAIN",
    "COMPARABLE_LOSS",
    "COMPARABLE_BOTH_POSITIVE",
    "COMPARABLE_BOTH_NONPOSITIVE",
)


class SourceMarketContextError(Exception):
    """来源市场装配的明确拒绝或不可评价原因。"""

    def __init__(self, code: str, message: str, *, category: str):
        self.code = code
        self.category = category
        super().__init__(message)


@dataclass(frozen=True)
class SourceMarketContextRead:
    """一个精确BaoStock QFQ捕获的市场宽度读取与内核结果。"""

    target_session: date
    research_cutoff: date
    calendar: Mapping[str, Any]
    pg_result: Mapping[str, Any]
    source_version: Mapping[str, Any]
    cohort: SourceCohortIdentity
    calculation: SourceMarketBreadthResult
    pressure_axis: Mapping[str, Any] | None = None


def fraction_payload(value: Fraction | None) -> dict[str, Any] | None:
    """把精确Fraction序列化为现行CLI字段。"""
    if value is None:
        return None
    with localcontext() as context:
        context.prec = 40
        decimal_value = Decimal(value.numerator) / Decimal(value.denominator)
        display = f"{decimal_value:.12f}"
    return {
        "numerator": value.numerator,
        "denominator": value.denominator,
        "fraction": str(value),
        "display_decimal_12dp": display,
        "unit": "fraction",
    }


def compute_source_market_context(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int,
) -> SourceMarketContextRead:
    """按固定官方日历和paired BaoStock捕获读取QFQ市场宽度。"""
    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
    ):
        raise SourceMarketContextError(
            "MARKET_BREADTH_DATE_AFTER_CUTOFF",
            "目标交易会话不得晚于显式研究截止日",
            category="ARGUMENT",
        )
    if type(source_capture_id) is not int or source_capture_id <= 0:
        raise SourceMarketContextError(
            "MARKET_BREADTH_CAPTURE_ID_INVALID",
            "source_capture_id必须是正数BIGINT",
            category="ARGUMENT",
        )

    try:
        payload, metadata, revisions, sessions = _load_calendar(target_session)
        year_metadata = next(
            item for item in metadata if item["year"] == target_session.year
        )
        calendar = {
            "calendar_id": payload["calendar_id"],
            "calendar_revision": payload["revision"],
            "revision_chain": revisions,
            "sessions": sessions,
        }
        target_index = sessions.index(target_session)
    except (ResearchEntryError, ValueError) as error:
        raise SourceMarketContextError(
            "MARKET_BREADTH_CALENDAR_INVALID",
            str(error),
            category="ARGUMENT",
        ) from error
    if target_index < 4:
        raise SourceMarketContextError(
            "MARKET_BREADTH_HISTORY_TOO_SHORT",
            "版本化官方日历中目标日前不足四个会话",
            category="NOT_EVALUABLE",
        )

    session_window = calendar["sessions"][target_index - 4 : target_index + 1]
    try:
        roster = read_baostock_capture_roster(
            config,
            source_capture_id=source_capture_id,
            cutoff=research_cutoff,
        )
    except BaoStockPriceFactsError as error:
        raise SourceMarketContextError(
            error.code, str(error), category="UNAVAILABLE"
        ) from error
    if roster.get("status") != "AVAILABLE":
        raise SourceMarketContextError(
            "MARKET_BREADTH_CAPTURE_UNAVAILABLE",
            "目标BaoStock paired捕获目录不可用或为空",
            category="NOT_EVALUABLE",
        )
    roster_by_symbol = {
        item["source_symbol"]: item for item in roster.get("symbols", [])
    }
    from stock_a.sources.baostock.identity import eligible_research_board

    source_symbols = tuple(
        sorted(
            symbol
            for symbol, item in roster_by_symbol.items()
            if eligible_research_board(symbol) is not None
            and item.get("eligible_start_session")
            and item.get("eligible_end_session")
            and item["eligible_start_session"]
            <= target_session.isoformat()
            <= item["eligible_end_session"]
        )
    )
    if not source_symbols:
        raise SourceMarketContextError(
            "MARKET_BREADTH_CAPTURE_EMPTY_AT_TARGET",
            "paired捕获在目标会话没有符合有效窗口的来源代码",
            category="NOT_EVALUABLE",
        )
    needed_sessions = (
        session_window[0],
        session_window[1],
        session_window[3],
        session_window[4],
    )
    rows_by_key = {}
    offset = 0
    page_size = 5000
    try:
        while True:
            batch = read_baostock_price_history_batch(
                config,
                source_symbols=source_symbols,
                sessions=needed_sessions,
                source_capture_id=source_capture_id,
                price_coordinate="FORWARD_ADJUSTED",
                cutoff=research_cutoff,
                offset=offset,
                limit=page_size,
            )
            for row in batch.get("rows", []):
                key = (row["source_symbol"], row["session_date"])
                if key in rows_by_key:
                    raise SourceMarketContextError(
                        "MARKET_BREADTH_PRICE_DUPLICATE",
                        "QFQ历史批量Reader返回重复来源代码和会话",
                        category="UNAVAILABLE",
                    )
                rows_by_key[key] = row
            page = batch.get("page", {})
            next_offset = page.get("next_offset")
            if next_offset is None:
                break
            if type(next_offset) is not int or next_offset <= offset:
                raise SourceMarketContextError(
                    "MARKET_BREADTH_PRICE_PAGE_INVALID",
                    "QFQ历史批量Reader分页游标无效",
                    category="UNAVAILABLE",
                )
            offset = next_offset
    except BaoStockPriceFactsError as error:
        raise SourceMarketContextError(
            error.code, str(error), category="UNAVAILABLE"
        ) from error

    def missing_status(symbol: str, session: date) -> tuple[str, str]:
        if not (
            roster_by_symbol[symbol]["eligible_start_session"]
            <= session.isoformat()
            <= roster_by_symbol[symbol]["eligible_end_session"]
        ):
            return "TARGET_MISSING", "SOURCE_SYMBOL_OUTSIDE_ELIGIBLE_SESSION_WINDOW"
        request = (
            roster_by_symbol[symbol].get("coordinates", {}).get("FORWARD_ADJUSTED", {})
        )
        completed_through = request.get("completed_through_session")
        if request.get("request_status") in {"COMPLETED", "NO_DATA"} and (
            request.get("request_status") == "NO_DATA"
            or completed_through is not None
            and completed_through >= session.isoformat()
        ):
            return "TARGET_MISSING", "QFQ_TARGET_BAR_NOT_RETURNED"
        return "READ_FAILED", request.get(
            "last_error_code"
        ) or "QFQ_REQUEST_NOT_COMPLETE"

    observations = []
    status_counts = {"TARGET_PRESENT": 0, "TARGET_MISSING": 0, "READ_FAILED": 0}
    trade_status_counts = {"ACTUAL": 0, "CONFIRMED_SUSPENSION": 0, "UNKNOWN": 0}
    for symbol in source_symbols:
        for session in needed_sessions:
            row = rows_by_key.get((symbol, session))
            if row is None:
                bar_status, bar_reason = missing_status(symbol, session)
                quality = "UNKNOWN"
                price_reason = bar_reason
                close = None
                trade_status_state, trade_status_reason = (
                    "UNKNOWN",
                    "TRADE_STATUS_HISTORY_ROW_NOT_STORED",
                )
            else:
                bar_status = row.get("qfq_bar_status") or "READ_FAILED"
                bar_reason = row.get("qfq_bar_reason")
                quality = row.get("qfq_price_quality") or "UNKNOWN"
                price_reason = row.get("qfq_price_reason")
                close = row.get("qfq_close")
                if bar_status not in status_counts:
                    bar_status = "READ_FAILED"
                    bar_reason = bar_reason or "PRICE_BAR_STATUS_INVALID"
                trade_status_state, trade_status_reason = (
                    trade_status_state_from_pg_row(
                        row, price_coordinate="FORWARD_ADJUSTED"
                    )
                )
            if session == target_session:
                status_counts[bar_status] += 1
                trade_status_counts[trade_status_state] += 1
            observations.append(
                QfqPriceObservation(
                    source_symbol=symbol,
                    session_date=session,
                    source_capture_id=source_capture_id,
                    selected_close=close,
                    price_quality=quality,
                    price_reason=price_reason or bar_reason,
                    bar_status=bar_status,
                    price_coordinate="FORWARD_ADJUSTED",
                    trade_status_state=trade_status_state,
                    trade_status_reason=trade_status_reason,
                )
            )
    source_version = {
        "source_capture_id": source_capture_id,
        "provider": "BAOSTOCK",
        "price_coordinate": "FORWARD_ADJUSTED",
        "price_coordinates": ["RAW_UNADJUSTED", "FORWARD_ADJUSTED"],
        "source_time_quality": roster["source_time_quality"],
        "source_clock_role": roster["source_clock_role"],
        "capture_read_started_at": roster["captured_at_start"],
        "capture_validated_at": roster["captured_at_validated"],
        "source_available_at": None,
        "price_row_count": len(source_symbols),
        "capture_expected_price_row_count": len(source_symbols),
        "target_status_counts": status_counts,
        "target_trade_status_counts": trade_status_counts,
    }
    cohort = SourceCohortIdentity(
        target_session=target_session,
        source_capture_id=source_capture_id,
        price_coordinate="FORWARD_ADJUSTED",
        source_time_quality=source_version["source_time_quality"],
        source_available_at=None,
        capture_read_started_at=source_version["capture_read_started_at"],
        capture_validated_at=source_version["capture_validated_at"],
        source_clock_role=source_version["source_clock_role"],
        price_row_count=len(source_symbols),
        capture_expected_price_row_count=len(source_symbols),
        target_present_count=status_counts["TARGET_PRESENT"],
        target_missing_count=status_counts["TARGET_MISSING"],
        read_failed_count=status_counts["READ_FAILED"],
        calendar_id=calendar["calendar_id"],
        calendar_revision=calendar["calendar_revision"],
    )
    observations = tuple(observations)
    try:
        calculation = calculate_source_market_breadth(
            SourceMarketBreadthInput(
                cohort=cohort,
                source_symbols=source_symbols,
                research_cutoff=research_cutoff,
                calendar_id=calendar["calendar_id"],
                calendar_revision=calendar["calendar_revision"],
                official_sessions=calendar["sessions"],
                observations=observations,
            )
        )
    except ValueError as error:
        raise SourceMarketContextError(
            "MARKET_BREADTH_INPUT_REJECTED",
            str(error),
            category="UNAVAILABLE",
        ) from error

    return SourceMarketContextRead(
        target_session=target_session,
        research_cutoff=research_cutoff,
        calendar=calendar,
        pg_result={
            "status": "AVAILABLE",
            "transaction": "各Reader分别使用REPEATABLE READ READ ONLY；未共享跨调用快照",
            "source_capture_id": source_capture_id,
            "price_coordinate": "FORWARD_ADJUSTED",
            "source_symbols": source_symbols,
            "history_rows": [
                {
                    "source_symbol": item.source_symbol,
                    "session_date": item.session_date,
                    "source_capture_id": item.source_capture_id,
                    "selected_close": item.selected_close,
                    "price_quality": item.price_quality,
                    "price_reason": item.price_reason,
                    "bar_status": item.bar_status,
                    "price_coordinate": item.price_coordinate,
                }
                for item in observations
            ],
            "target_status_counts": status_counts,
            "history_row_count": len(rows_by_key),
            "calculation_history_row_count": len(observations),
            "storage_status": "COMPUTED_READ_ONLY",
            "source_io_attempted": False,
            "ddl_executed": False,
            "dml_executed": False,
            "database_write_attempted": False,
        },
        source_version=source_version,
        cohort=cohort,
        calculation=calculation,
    )


def _price_limit_calendar_revision(research_cutoff: date) -> str:
    """返回限价生产者按研究截止加载的完整日历修订链。"""
    return _load_calendar(research_cutoff)[2]


def read_source_market_context(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int,
    include_symbols: bool = False,
) -> SourceMarketContextRead:
    """直接读取已保存的精确市场宽度，不在读取时重算来源池。"""
    from stock_a.storage.postgres.market_breadth import read_market_breadth

    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
    ):
        raise SourceMarketContextError(
            "MARKET_BREADTH_DATE_AFTER_CUTOFF",
            "目标会话超过研究截止",
            category="ARGUMENT",
        )
    if type(source_capture_id) is not int or source_capture_id <= 0:
        raise SourceMarketContextError(
            "MARKET_BREADTH_CAPTURE_ID_INVALID",
            "source_capture_id无效",
            category="ARGUMENT",
        )
    try:
        payload, metadata, revisions, sessions = _load_calendar(target_session)
        year_metadata = next(
            item for item in metadata if item["year"] == target_session.year
        )
        index = sessions.index(target_session)
    except (ResearchEntryError, ValueError, StopIteration) as error:
        raise SourceMarketContextError(
            "MARKET_BREADTH_CALENDAR_INVALID", str(error), category="ARGUMENT"
        ) from error
    saved = read_market_breadth(
        config,
        target_session=target_session,
        source_capture_id=source_capture_id,
        calendar_revision=payload["revision"],
        contract_version=SOURCE_MARKET_BREADTH_CONTRACT,
        include_symbols=include_symbols,
    )
    if saved["storage_status"] != "SAVED":
        raise SourceMarketContextError(
            "MARKET_BREADTH_" + saved["storage_status"],
            saved["reason"],
            category="NOT_EVALUABLE",
        )
    day = saved["day"]
    window = tuple(sessions[index - 4 : index + 1]) if index >= 4 else ()
    saved_window = tuple(
        day[name]
        for name in (
            "session_t4",
            "session_t3",
            "session_t2",
            "session_t1",
            "target_session",
        )
    )
    if (
        window != saved_window
        or day["calendar_id"] != payload["calendar_id"]
        or day["calendar_revision"] != payload["revision"]
    ):
        raise SourceMarketContextError(
            "MARKET_BREADTH_INCOMPLETE",
            "已存宽度的官方日历身份或窗口不匹配",
            category="NOT_EVALUABLE",
        )

    def clock(name: str) -> str | None:
        value = day[name]
        return value.isoformat() if value is not None else None

    cohort = SourceCohortIdentity(
        target_session=target_session,
        source_capture_id=int(day["source_capture_id"]),
        price_coordinate="FORWARD_ADJUSTED",
        source_time_quality=day["source_time_quality"],
        source_available_at=clock("source_available_at"),
        capture_read_started_at=clock("capture_read_started_at"),
        capture_validated_at=clock("capture_validated_at"),
        source_clock_role=day["source_clock_role"],
        price_row_count=day["source_count"],
        capture_expected_price_row_count=day["source_count"],
        target_present_count=day["target_present_count"],
        target_missing_count=day["target_missing_count"],
        read_failed_count=day["read_failed_count"],
        calendar_id=day["calendar_id"],
        calendar_revision=day["calendar_revision"],
    )

    def rational(row: Mapping[str, Any], prefix: str) -> Fraction | None:
        numerator, denominator = (
            row[prefix + "_numerator"],
            row[prefix + "_denominator"],
        )
        return None if numerator is None else Fraction(int(numerator), int(denominator))

    symbols = tuple(
        SourceMarketBreadthSymbol(
            source_symbol=row["source_symbol"],
            ret1_t=rational(row, "ret_t"),
            ret1_t_minus_3=rational(row, "ret_t3"),
            positive_t=row["positive_t"],
            positive_t_minus_3=row["positive_t3"],
            delta_indicator=row["delta_indicator"],
            classification=row["classification"],
            primary_unknown_reason=row["primary_unknown_reason"],
            unknown_reason_detail=row["unknown_reason_detail"],
        )
        for row in saved["symbols"]
    )
    counts, n, eligible = saved["counts"], day["source_count"], day["eligible_count"]
    calculation = SourceMarketBreadthResult(
        target_session=target_session,
        research_cutoff=research_cutoff,
        session_window=window,
        contract_version=day["contract_version"],
        cohort=cohort,
        n_source=n,
        n_eligible=eligible,
        counts=counts,
        unknown_reason_counts=saved["unknown_reasons"],
        coverage=Fraction(eligible, n) if n else None,
        b_fixed_source=Fraction(
            counts["COMPARABLE_GAIN"] - counts["COMPARABLE_LOSS"], eligible
        )
        if eligible
        else None,
        symbols=symbols,
    )
    source_version = {
        "source_capture_id": cohort.source_capture_id,
        "price_coordinate": cohort.price_coordinate,
        "price_coordinates": ["RAW_UNADJUSTED", "FORWARD_ADJUSTED"],
        "price_row_count": cohort.price_row_count,
        "capture_expected_price_row_count": cohort.capture_expected_price_row_count,
        "source_time_quality": cohort.source_time_quality,
        "source_available_at": cohort.source_available_at,
        "capture_read_started_at": cohort.capture_read_started_at,
        "capture_validated_at": cohort.capture_validated_at,
        "source_clock_role": cohort.source_clock_role,
    }
    pg_result = {
        "history_row_count": 0,
        "calculation_history_row_count": day["history_row_count"],
        "storage_status": "SAVED",
        "transaction": "各Reader分别使用REPEATABLE READ READ ONLY；未共享跨调用快照",
        "source_io_attempted": False,
        "database_write_attempted": False,
        "ddl_executed": False,
        "dml_executed": False,
    }
    calendar = {
        "calendar_id": payload["calendar_id"],
        "calendar_revision": payload["revision"],
        "revision_chain": revisions,
        "sessions": sessions,
    }
    from stock_a.storage.postgres.price_limits import read_market_pressure_summary

    pressure_calendar_revision = _price_limit_calendar_revision(research_cutoff)
    pressure_axis = read_market_pressure_summary(
        config,
        target_session=target_session,
        source_capture_id=cohort.source_capture_id,
        calendar_revision=pressure_calendar_revision,
    )
    return SourceMarketContextRead(
        target_session,
        research_cutoff,
        calendar,
        pg_result,
        source_version,
        cohort,
        calculation,
        pressure_axis=pressure_axis,
    )


def prepare_source_market_breadth_change(
    config: Mapping[str, Any], market: SourceMarketContextRead
) -> dict[str, Any]:
    """只比较已存前态与本次目标宽度；集合或版本不兼容时拒绝差值。"""
    from stock_a.application.source_relative_position import market_changes
    from stock_a.storage.postgres.market_breadth import read_market_breadth

    calculation = market.calculation
    target_session = market.target_session
    before_session = calculation.session_window[3]
    source_capture_id = market.cohort.source_capture_id
    calendar_id = market.calendar["calendar_id"]
    calendar_revision = market.calendar["calendar_revision"]
    contract_version = calculation.contract_version
    target_by_symbol = {item.source_symbol: item for item in calculation.symbols}
    target_symbols = set(target_by_symbol)
    target_eligible = {
        symbol
        for symbol, item in target_by_symbol.items()
        if item.classification != "UNKNOWN"
    }
    target_unknown = target_symbols - target_eligible

    change: dict[str, Any] = {
        "before_session": before_session,
        "target_session": target_session,
        "before_source_capture_id": None,
        "target_source_capture_id": source_capture_id,
        "before_calendar_id": None,
        "target_calendar_id": calendar_id,
        "before_calendar_revision": None,
        "target_calendar_revision": calendar_revision,
        "before_contract_version": None,
        "target_contract_version": contract_version,
        "before_price_coordinate": None,
        "target_price_coordinate": market.cohort.price_coordinate,
        "comparison_status": "NOT_COMPUTED",
        "reason_code": "PREVIOUS_BREADTH_NOT_COMPUTED",
        "source_sets_equal": False,
        "eligible_sets_equal": False,
        "unknown_sets_equal": False,
        "unknown_reason_mismatch_count": 0,
        "source_count_before": 0,
        "source_count_target": calculation.n_source,
        "common_source_count": 0,
        "eligible_count_before": 0,
        "eligible_count_target": calculation.n_eligible,
        "common_eligible_count": 0,
        "unknown_count_before": 0,
        "unknown_count_target": len(target_unknown),
        "breadth_before": None,
        "breadth_target": calculation.b_fixed_source,
        "breadth_delta": None,
        "breadth_direction": None,
        "improved_symbol_count": 0,
        "deteriorated_symbol_count": 0,
        "unchanged_symbol_count": 0,
        "unknown_symbol_count": calculation.n_source,
    }
    if (
        type(before_session) is not date
        or calculation.session_window[-1] != target_session
        or calculation.session_window[-2] != before_session
        or source_capture_id <= 0
        or calculation.n_source != len(target_symbols)
    ):
        raise SourceMarketContextError(
            "TARGET_BREADTH_PARTITION_INVALID",
            "目标市场宽度证券分区或官方相邻会话不完整",
            category="DATA_OR_CONTRACT",
        )

    saved = read_market_breadth(
        config,
        target_session=before_session,
        source_capture_id=source_capture_id,
        calendar_revision=calendar_revision,
        contract_version=contract_version,
        include_symbols=True,
    )
    if saved.get("storage_status") == "NOT_COMPUTED":
        return change
    if saved.get("storage_status") != "SAVED":
        raise SourceMarketContextError(
            "PREVIOUS_BREADTH_INCOMPLETE",
            "前一会话已存市场宽度分区不完整，拒绝生成变化事实",
            category="DATA_OR_CONTRACT",
        )

    before_day = saved["day"]
    before_rows = saved["symbols"]
    before_by_symbol = {row["source_symbol"]: row for row in before_rows}
    before_symbols = set(before_by_symbol)
    before_eligible = {
        symbol
        for symbol, row in before_by_symbol.items()
        if row["classification"] != "UNKNOWN"
    }
    before_unknown = before_symbols - before_eligible
    before_reasons = {
        symbol: row["primary_unknown_reason"]
        for symbol, row in before_by_symbol.items()
        if row["classification"] == "UNKNOWN"
    }
    target_reasons = {
        symbol: item.primary_unknown_reason
        for symbol, item in target_by_symbol.items()
        if item.classification == "UNKNOWN"
    }
    common_unknown = before_unknown & target_unknown
    unknown_reason_mismatch_count = sum(
        before_reasons.get(symbol) != target_reasons.get(symbol)
        for symbol in common_unknown
    )
    change.update(
        {
            "before_source_capture_id": before_day["source_capture_id"],
            "before_calendar_id": before_day["calendar_id"],
            "before_calendar_revision": before_day["calendar_revision"],
            "before_contract_version": before_day["contract_version"],
            "before_price_coordinate": before_day["price_coordinate"],
            "source_count_before": before_day["source_count"],
            "common_source_count": len(before_symbols & target_symbols),
            "eligible_count_before": before_day["eligible_count"],
            "common_eligible_count": len(before_eligible & target_eligible),
            "unknown_count_before": len(before_unknown),
            "source_sets_equal": before_symbols == target_symbols,
            "eligible_sets_equal": before_eligible == target_eligible,
            "unknown_sets_equal": before_unknown == target_unknown,
            "unknown_reason_mismatch_count": unknown_reason_mismatch_count,
        }
    )
    before_width = None
    if before_day["eligible_count"] > 0:
        before_numerator = sum(
            int(row["delta_indicator"])
            for row in before_rows
            if row["delta_indicator"] is not None
        )
        before_width = Fraction(before_numerator, before_day["eligible_count"])
    change["breadth_before"] = before_width

    if before_day["source_capture_id"] != source_capture_id:
        reason = "SOURCE_CAPTURE_MISMATCH"
    elif (
        before_day["price_coordinate"] != market.cohort.price_coordinate
        or market.cohort.price_coordinate != "FORWARD_ADJUSTED"
    ):
        reason = "PRICE_COORDINATE_MISMATCH"
    elif (
        before_day["calendar_id"] != calendar_id
        or before_day["calendar_revision"] != calendar_revision
    ):
        reason = "CALENDAR_REVISION_MISMATCH"
    elif before_day["contract_version"] != contract_version:
        reason = "ALGORITHM_CONTRACT_MISMATCH"
    elif before_symbols != target_symbols:
        reason = "SOURCE_SYMBOL_SET_MISMATCH"
    elif before_eligible != target_eligible:
        reason = "ELIGIBLE_SYMBOL_SET_MISMATCH"
    elif before_unknown != target_unknown:
        reason = "UNKNOWN_SYMBOL_SET_MISMATCH"
    elif unknown_reason_mismatch_count:
        reason = "UNKNOWN_REASON_MISMATCH"
    elif before_width is None or calculation.b_fixed_source is None:
        change["comparison_status"] = "UNKNOWN"
        change["reason_code"] = "NO_ELIGIBLE_SECURITIES"
        return change
    else:
        reason = None

    if reason is not None:
        change["comparison_status"] = "NOT_COMPARABLE"
        change["reason_code"] = reason
        return change

    def market_day(
        session: date,
        width: Fraction,
        counts: Mapping[str, int],
        source_count: int,
        eligible_count: int,
        unknown_count: int,
    ) -> dict[str, Any]:
        return {
            "session": session.isoformat(),
            "market": {
                "metric": {"exact": {"fraction": str(width)}},
                "denominator": {
                    "N_source": source_count,
                    "N_eligible": eligible_count,
                    "UNKNOWN": unknown_count,
                    "counts": dict(counts),
                },
            },
        }

    before_counts = dict(saved["counts"])
    target_counts = dict(calculation.counts)
    change_result = market_changes(
        [
            market_day(
                before_session,
                before_width,
                before_counts,
                before_day["source_count"],
                before_day["eligible_count"],
                len(before_unknown),
            ),
            market_day(
                target_session,
                calculation.b_fixed_source,
                target_counts,
                calculation.n_source,
                calculation.n_eligible,
                len(target_unknown),
            ),
        ]
    )[-1]
    breadth_delta = Fraction(change_result["breadth_delta"])
    if breadth_delta != calculation.b_fixed_source - before_width:
        raise SourceMarketContextError(
            "MARKET_BREADTH_CHANGE_ORACLE_MISMATCH",
            "市场宽度变化聚合器与精确Fraction端点不一致",
            category="DATA_OR_CONTRACT",
        )
    improved = deteriorated = unchanged = 0
    for symbol in before_eligible:
        delta = int(target_by_symbol[symbol].delta_indicator) - int(
            before_by_symbol[symbol]["delta_indicator"]
        )
        if delta > 0:
            improved += 1
        elif delta < 0:
            deteriorated += 1
        else:
            unchanged += 1
    return {
        **change,
        "comparison_status": "COMPARABLE",
        "reason_code": None,
        "breadth_delta": breadth_delta,
        "breadth_direction": change_result["breadth_direction"],
        "improved_symbol_count": improved,
        "deteriorated_symbol_count": deteriorated,
        "unchanged_symbol_count": unchanged,
        "unknown_symbol_count": len(target_unknown),
    }


def source_market_context_payload(
    market: SourceMarketContextRead,
    *,
    market_participation: Mapping[str, Any] | None = None,
    pressure_axis: Mapping[str, Any] | None = None,
    breadth_change: Mapping[str, Any] | None = None,
    environment_display: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """输出市场宽度摘要，不暴露逐来源代码计算明细。"""
    calculation = market.calculation
    cohort = market.cohort
    source_version = market.source_version
    pg_result = market.pg_result
    window = calculation.session_window
    status = (
        "UNKNOWN_ZERO_ELIGIBLE"
        if calculation.n_eligible == 0
        else "PARTIAL_WITH_UNKNOWN"
        if calculation.counts["UNKNOWN"]
        else "COMPLETE"
    )
    breadth_classification = classify_market_breadth(calculation.b_fixed_source)
    participation_axis = dict(market_participation or {})
    if not participation_axis:
        participation_axis = {
            "status": "NOT_COMPUTED",
            "reason": "此精确日期和来源尚无已保存成交参与事实。",
            "classification": "UNKNOWN",
        }
    participation_metric = participation_axis.get("metric") or {}
    participation_classification = participation_metric.get(
        "classification", participation_axis.get("classification", "UNKNOWN")
    )
    if participation_axis.get("status") in {
        "NOT_COMPUTED",
        "NOT_COMPARABLE",
        "INCOMPLETE",
    }:
        participation_classification = None
    trend_classification: str | None = None
    if (
        isinstance(environment_display, Mapping)
        and environment_display.get("status") == "AVAILABLE"
    ):
        env_trend = (environment_display.get("axes") or {}).get("trend")
        if env_trend in {"STRONG", "NEUTRAL", "WEAK"}:
            trend_classification = str(env_trend)
            trend_axis = {
                "status": "OBSERVED",
                "classification": trend_classification,
                "primary_index_symbol": environment_display.get("primary_index_symbol"),
                "index_capture_id": environment_display.get("index_capture_id"),
                "index_config_version": environment_display.get("index_config_version"),
                "reason": None,
            }
        else:
            trend_axis = {
                "status": "UNKNOWN",
                "classification": "UNKNOWN",
                "primary_index_symbol": environment_display.get("primary_index_symbol"),
                "index_capture_id": environment_display.get("index_capture_id"),
                "index_config_version": environment_display.get("index_config_version"),
                "reason": "主参考指数趋势因子当日为UNKNOWN（窗口不足或点位缺失）。",
            }
    else:
        trend_axis = {
            "status": "NOT_COMPUTED",
            "classification": "UNKNOWN",
            "reason": "尚无与该来源范围绑定的合法PRIMARY_MARKET参考序列。",
        }
    resolved_pressure = dict(
        pressure_axis
        if pressure_axis is not None
        else (getattr(market, "pressure_axis", None) or {})
    )
    if not resolved_pressure:
        resolved_pressure = {
            "status": "NOT_COMPUTED",
            "classification": "UNKNOWN",
            "reason": "目标会话尚未生成已保存的涨跌停限价事实。",
        }
    pressure_axis_payload = resolved_pressure
    pressure_classification = pressure_axis_payload.get("classification", "UNKNOWN")
    if pressure_axis_payload.get("status") != "OBSERVED":
        pressure_classification = None
    composite = market_environment_label(
        trend_classification,
        breadth_classification,
        participation_classification,
        pressure_classification,
    )
    composite["reason"] = (
        "按冻结有序规则枚举未知轴的所有合法补全；仅当所有补全标签唯一时才报告综合标签。"
    )
    change = dict(breadth_change or {})
    stored_change = change.get("change")
    if isinstance(stored_change, Mapping):
        change = dict(stored_change)
        change_storage_status = breadth_change.get("storage_status")
    else:
        change_storage_status = change.get("storage_status")
    comparison_status = (
        change.get("comparison_status")
        if change_storage_status == "SAVED"
        else change_storage_status or "NOT_COMPUTED"
    )

    def change_fraction(name: str) -> dict[str, Any] | None:
        value = change.get(name)
        if isinstance(value, Fraction):
            return fraction_payload(value)
        numerator = change.get(f"{name}_numerator")
        denominator = change.get(f"{name}_denominator")
        if numerator is None or denominator is None:
            return None
        try:
            return fraction_payload(Fraction(int(numerator), int(denominator)))
        except (TypeError, ValueError, ZeroDivisionError):
            return None

    breadth_change_payload = {
        "status": comparison_status,
        "reason_code": change.get("reason_code")
        or (breadth_change or {}).get("reason_code"),
        "before_session": (
            change["before_session"].isoformat()
            if isinstance(change.get("before_session"), date)
            else change.get("before_session") or window[3].isoformat()
        ),
        "target_session": (
            change["target_session"].isoformat()
            if isinstance(change.get("target_session"), date)
            else change.get("target_session", market.target_session.isoformat())
        ),
        "breadth_before": change_fraction("breadth_before"),
        "breadth_target": change_fraction("breadth_target"),
        "breadth_delta": change_fraction("breadth_delta"),
        "direction": change.get("breadth_direction"),
        "source_count_before": change.get("source_count_before"),
        "source_count_target": change.get("source_count_target", calculation.n_source),
        "common_source_count": change.get("common_source_count"),
        "eligible_count_before": change.get("eligible_count_before"),
        "eligible_count_target": change.get(
            "eligible_count_target", calculation.n_eligible
        ),
        "common_eligible_count": change.get("common_eligible_count"),
        "unknown_count_before": change.get("unknown_count_before"),
        "unknown_count_target": change.get(
            "unknown_count_target", calculation.counts["UNKNOWN"]
        ),
        "source_sets_equal": change.get("source_sets_equal"),
        "eligible_sets_equal": change.get("eligible_sets_equal"),
        "unknown_sets_equal": change.get("unknown_sets_equal"),
        "unknown_reason_mismatch_count": change.get("unknown_reason_mismatch_count"),
        "improved_symbol_count": change.get("improved_symbol_count"),
        "deteriorated_symbol_count": change.get("deteriorated_symbol_count"),
        "unchanged_symbol_count": change.get("unchanged_symbol_count"),
        "unknown_symbol_count": change.get("unknown_symbol_count"),
        "source_scope": "SOURCE_SYMBOL_ONLY",
        "price_coordinate": "FORWARD_ADJUSTED",
        "research_mode": "NON_PIT_RETROSPECTIVE",
    }
    payload = {
        "status": status,
        "contract_version": calculation.contract_version,
        "analysis_scope": "SOURCE_SYMBOL_COHORT_FIXED_AT_T",
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "price_coordinate": "FORWARD_ADJUSTED",
        "target_session": market.target_session.isoformat(),
        "research_cutoff": market.research_cutoff.isoformat(),
        "official_calendar": {
            "calendar_id": market.calendar["calendar_id"],
            "revision": market.calendar["calendar_revision"],
            "lookback_sessions": {
                "t_minus_4": window[0].isoformat(),
                "t_minus_3": window[1].isoformat(),
                "t_minus_2": window[2].isoformat(),
                "t_minus_1": window[3].isoformat(),
                "t": window[4].isoformat(),
            },
            "qfq_price_sessions": [
                {"role": "t", "session_date": window[4].isoformat()},
                {"role": "t_minus_1", "session_date": window[3].isoformat()},
                {"role": "t_minus_3", "session_date": window[1].isoformat()},
                {"role": "t_minus_4", "session_date": window[0].isoformat()},
            ],
        },
        "source_cohort": {
            "target_session": market.target_session.isoformat(),
            "source_capture_id": cohort.source_capture_id,
            "price_coordinate": cohort.price_coordinate,
            "price_coordinates": ["RAW_UNADJUSTED", "FORWARD_ADJUSTED"],
            "price_row_count": cohort.price_row_count,
            "target_present_count": cohort.target_present_count,
            "target_missing_count": cohort.target_missing_count,
            "read_failed_count": cohort.read_failed_count,
            "source_time_quality": cohort.source_time_quality,
            "source_available_at": cohort.source_available_at,
            "capture_read_started_at": cohort.capture_read_started_at,
            "capture_validated_at": cohort.capture_validated_at,
            "source_clock_role": cohort.source_clock_role,
        },
        "metric": {
            "name": "B_fixed_source",
            "formula": "MEAN(I(ret1_t>0)-I(ret1_t_minus_3>0))",
            "exact": fraction_payload(calculation.b_fixed_source),
            "unit": "fraction",
            "range": ["-1", "1"],
            "sign_return_difference_is_main_value": False,
            "ma20_profile_breadth_is_main_value": False,
        },
        "market_axes": {
            "breadth": {
                "status": "OBSERVED"
                if calculation.b_fixed_source is not None
                else "UNKNOWN",
                "classification": breadth_classification,
                "metric_name": "B_fixed_source",
                "exact": fraction_payload(calculation.b_fixed_source),
                "source_pool_count": calculation.n_source,
                "assessable_count": calculation.n_eligible,
                "unknown_count": calculation.counts["UNKNOWN"],
            },
            "participation": participation_axis,
            "trend": trend_axis,
            "pressure": pressure_axis_payload,
            "composite": composite,
        },
        "denominator": {
            "N_source": calculation.n_source,
            "N_eligible": calculation.n_eligible,
            "UNKNOWN": calculation.counts["UNKNOWN"],
            "eligible_partition_sum": sum(
                calculation.counts[name] for name in _ELIGIBLE_CLASSES
            ),
            "counts": calculation.counts,
            "coverage": fraction_payload(calculation.coverage),
            "unknown_reason_counts": calculation.unknown_reason_counts,
        },
        "formal_market_breadth": {
            "status": "NOT_READY_HISTORICAL_UNIVERSE",
            "reason": "本值只描述t日BaoStock来源代码集合，不代表t与t-3各自正式上市总体。",
            "missing_dependencies": [
                "t与t-3官方有效上市身份总体及其精确比较集合",
                "稳定security_id和日期有效身份映射",
                "按当时available_at核定的历史成员资格与PIT证据",
            ],
        },
        "database_read": {
            "database": "stock_a",
            "schema": "stock",
            "account": "stock_a",
            "server": "127.0.0.1:5432",
            "transaction": pg_result["transaction"],
            "history_price_row_count": pg_result["history_row_count"],
            "source_history_rows_at_calculation": pg_result.get(
                "calculation_history_row_count", pg_result["history_row_count"]
            ),
            "storage_status": pg_result.get("storage_status", "COMPUTED_READ_ONLY"),
            "source_io_attempted": pg_result["source_io_attempted"],
            "database_write_attempted": pg_result["database_write_attempted"],
            "ddl_executed": pg_result["ddl_executed"],
            "dml_executed": pg_result["dml_executed"],
            "dml_rows_affected": 0,
        },
    }
    if breadth_change is not None:
        payload["breadth_change"] = breadth_change_payload
    return payload


def _clock_identity(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return value
    else:
        return str(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return f"NAIVE:{parsed.isoformat()}"
    return parsed.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _calendar_revision_covers_target(
    observed_revision: Any, target_session: date, expected_revision: str
) -> bool:
    """校验长窗口日历修订串包含目标年份的精确官方修订。"""
    if observed_revision == expected_revision:
        return True
    if not isinstance(observed_revision, str):
        return False
    components: dict[str, str] = {}
    for component in observed_revision.split("+"):
        match = re.fullmatch(r"(\d{4}):(.+)", component)
        if match is None or match.group(1) in components:
            return False
        components[match.group(1)] = match.group(2)
    return components.get(str(target_session.year)) == expected_revision


def validate_source_market_context_binding(
    market: SourceMarketContextRead,
    *,
    strength_results: Sequence[Mapping[str, Any]],
    discovery: Mapping[str, Any],
) -> None:
    """拒绝将不同数字来源包、捕获、会话或日历修订拼入组合结果。"""
    d1_source = discovery.get("source")
    d1_calendar = discovery.get("official_calendar")
    if not isinstance(d1_source, Mapping) or not isinstance(d1_calendar, Mapping):
        raise SourceMarketContextError(
            "SOURCE_MARKET_CONTEXT_IDENTITY_MISMATCH",
            "D1结果缺少封口来源或官方日历身份，拒绝组合输出",
            category="IDENTITY_MISMATCH",
        )

    market_identity = {
        "target_session": market.target_session.isoformat(),
        "price_coordinate": "FORWARD_ADJUSTED",
        "price_row_count": int(market.source_version["price_row_count"]),
        "capture_expected_price_row_count": int(
            market.source_version["capture_expected_price_row_count"]
        ),
    }
    d1_identity = {key: d1_source.get(key) for key in market_identity}
    d1_identity["research_cutoff"] = discovery.get("research_cutoff")
    expected_identity = dict(market_identity)
    expected_identity["research_cutoff"] = market.research_cutoff.isoformat()
    market_calendar = {
        "calendar_id": market.calendar["calendar_id"],
        "calendar_revision": market.calendar["calendar_revision"],
    }
    d1_calendar_identity = {
        "calendar_id": d1_calendar.get("calendar_id"),
        "calendar_revision": d1_calendar.get("calendar_revision"),
    }
    if (
        d1_identity != expected_identity
        or market_calendar != d1_calendar_identity
        or _clock_identity(market.source_version.get("source_available_at"))
        != _clock_identity(d1_source.get("source_available_at"))
    ):
        raise SourceMarketContextError(
            "SOURCE_MARKET_CONTEXT_IDENTITY_MISMATCH",
            "市场宽度与D1捕获、价格坐标、时钟或官方日历修订不一致",
            category="IDENTITY_MISMATCH",
        )

    for strength in strength_results:
        strength_source = strength.get("target_source_version")
        strength_calendar = strength.get("official_calendar")
        if not isinstance(strength_source, Mapping) or not isinstance(
            strength_calendar, Mapping
        ):
            raise SourceMarketContextError(
                "SOURCE_MARKET_CONTEXT_IDENTITY_MISMATCH",
                "行业或GN强弱结果缺少来源身份或官方日历",
                category="IDENTITY_MISMATCH",
            )
        strength_identity = {
            "target_session": strength.get("target_session"),
            "price_coordinate": strength_source.get("price_coordinate"),
        }
        expected_strength_identity = {
            key: market_identity[key] for key in strength_identity
        }
        expected_strength_identity["research_cutoff"] = (
            market.research_cutoff.isoformat()
        )
        strength_identity["research_cutoff"] = strength.get("research_cutoff")
        if (
            strength_identity != expected_strength_identity
            or strength_calendar.get("calendar_id") != market_calendar["calendar_id"]
            or not _calendar_revision_covers_target(
                strength_calendar.get("calendar_revision"),
                market.target_session,
                market_calendar["calendar_revision"],
            )
        ):
            raise SourceMarketContextError(
                "SOURCE_MARKET_CONTEXT_IDENTITY_MISMATCH",
                "市场宽度与行业/GN强弱使用的捕获、坐标或官方日历不一致",
                category="IDENTITY_MISMATCH",
            )


def source_market_not_evaluable_payload(
    *,
    source_identity: Mapping[str, Any],
    official_calendar: Mapping[str, Any],
    target_session: date,
    research_cutoff: date,
    reason_code: str,
    reason: str,
) -> dict[str, Any]:
    """在官方市场窗口不足时保留组合结果并将市场组件标为UNKNOWN。"""
    n_source = int(source_identity["price_row_count"])
    counts = {name: 0 for name in (*_ELIGIBLE_CLASSES, "UNKNOWN")}
    counts["UNKNOWN"] = n_source
    unknown_reasons = {reason_code: n_source} if n_source else {}
    return {
        "status": "NOT_EVALUABLE",
        "contract_version": SOURCE_MARKET_BREADTH_CONTRACT,
        "analysis_scope": "SOURCE_SYMBOL_COHORT_FIXED_AT_T",
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "price_coordinate": "FORWARD_ADJUSTED",
        "target_session": target_session.isoformat(),
        "research_cutoff": research_cutoff.isoformat(),
        "official_calendar": {
            "calendar_id": official_calendar.get("calendar_id"),
            "revision": official_calendar.get("calendar_revision"),
            "lookback_sessions": None,
            "not_evaluable_reason": reason,
        },
        "source_cohort": {
            "target_session": target_session.isoformat(),
            "source_capture_id": source_identity.get("source_capture_id"),
            "price_coordinate": "FORWARD_ADJUSTED",
            "price_row_count": n_source,
            "source_time_quality": source_identity.get("source_time_quality"),
            "source_available_at": source_identity.get("source_available_at"),
        },
        "metric": {
            "name": "B_fixed_source",
            "formula": "MEAN(I(ret1_t>0)-I(ret1_t_minus_3>0))",
            "exact": None,
            "unit": "fraction",
            "range": ["-1", "1"],
            "not_evaluable_reason": reason,
        },
        "denominator": {
            "N_source": n_source,
            "N_eligible": 0,
            "UNKNOWN": n_source,
            "eligible_partition_sum": 0,
            "counts": counts,
            "coverage": fraction_payload(Fraction(0, 1)) if n_source else None,
            "unknown_reason_counts": unknown_reasons,
        },
        "not_evaluable_reason_code": reason_code,
        "formal_market_breadth": {
            "status": "NOT_READY_HISTORICAL_UNIVERSE",
            "reason": "来源级市场指标不可评价；该状态不代表正式市场总体。",
            "missing_dependencies": [
                "t与t-3官方有效上市身份总体及其精确比较集合",
                "稳定security_id和日期有效身份映射",
                "按当时available_at核定的历史成员资格与PIT证据",
            ],
        },
        "database_read": {
            "transaction": "NOT_OPENED_MARKET_WINDOW_NOT_EVALUABLE",
            "database_write_attempted": False,
            "ddl_executed": False,
            "dml_executed": False,
            "dml_rows_affected": 0,
        },
    }


def compute_formal_market_breadth_from_universes(
    *,
    universe_t: Mapping[str, Fraction | None],
    universe_t3: Mapping[str, Fraction | None],
) -> dict[str, Any]:
    """基于 t 与 t-3 各自日期有效 Universe 交集计算正式市场宽度 B_formal。"""
    ids_t = set(universe_t.keys())
    ids_t3 = set(universe_t3.keys())
    common_ids = sorted(ids_t & ids_t3)
    if not common_ids:
        return {
            "status": "UNKNOWN",
            "reason": "COMMON_UNIVERSE_EMPTY",
            "n_t": len(ids_t),
            "n_t3": len(ids_t3),
            "n_common": 0,
            "n_eligible": 0,
            "unknown_count": 0,
            "exact": None,
        }
    eligible_diffs: list[int] = []
    unknown_count = 0
    for eid in common_ids:
        r_t = universe_t.get(eid)
        r_t3 = universe_t3.get(eid)
        if r_t is None or r_t3 is None:
            unknown_count += 1
            continue
        i_t = 1 if r_t > 0 else 0
        i_t3 = 1 if r_t3 > 0 else 0
        eligible_diffs.append(i_t - i_t3)
    n_eligible = len(eligible_diffs)
    if n_eligible == 0:
        return {
            "status": "UNKNOWN",
            "reason": "NO_ELIGIBLE_COMMON_RETURNS",
            "n_t": len(ids_t),
            "n_t3": len(ids_t3),
            "n_common": len(common_ids),
            "n_eligible": 0,
            "unknown_count": unknown_count,
            "exact": None,
        }
    b_val = Fraction(sum(eligible_diffs), n_eligible)
    return {
        "status": "OBSERVED",
        "reason": None,
        "n_t": len(ids_t),
        "n_t3": len(ids_t3),
        "n_common": len(common_ids),
        "n_eligible": n_eligible,
        "unknown_count": unknown_count,
        "exact": fraction_payload(b_val),
    }
