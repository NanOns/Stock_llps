"""Stock A研究history_preview职责模块。"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from statistics import mean
from typing import Any, Iterable, Mapping, Sequence
from stock_a.domain import ContractError, FieldValue, exact_number, unknown
from stock_a.research.core import (
    CoreObservation,
    MarketSessionCalendar,
    _actual_window,
    _column,
    _coordinate_error,
    _extreme_column,
    _known,
    _known_ohlc_error,
    _number,
    _numeric_column,
    _validate_history,
    calculate_core,
    calculate_seed_missing_core,
)
from stock_a.research.breakout import evaluate_raw_breakout_predicate

from stock_a.research.entry_core.fields import (
    _apply_calendar_coverage_to_core,
    _render_field,
    _render_value,
)
from stock_a.research.entry_context.errors import ResearchEntryError
from stock_a.research.entry_core.profile_preview import (
    _build_profile_preview,
)
from stock_a.research.entry_context.sessions import (
    _load_calendar,
    _parse_target,
    load_period_calendar,
)
from stock_a.research.raw_periods import (
    build_source_symbol_raw_periods,
    evaluate_raw_period_trend,
)
from stock_a.research.entry_context.source_discovery import _normalize_symbol
from stock_a.research.entry_history.support import (
    _core_observation_from_pg_price_row,
    _field_from_pg_raw,
    trade_status_state_from_pg_row,
)


@dataclass(frozen=True)
class _SourceD1PreviewContext:
    calendars_by_year: Mapping[
        int,
        tuple[
            dict[str, Any],
            tuple[dict[str, Any], ...],
            str,
            tuple[date, ...],
        ],
    ]
    core_algorithm_version: str
    execution_adapter_version: str
    target_windows: dict[date, _SourceD1TargetWindow] = field(
        default_factory=dict,
        compare=False,
        repr=False,
    )


@dataclass(frozen=True)
class _SourceD1TargetWindow:
    session_vector: tuple[date, ...]
    expected_sessions: tuple[date, ...]
    calendar_payload: Mapping[str, Any]
    calendar_metadata: tuple[Mapping[str, Any], ...]
    calendar_revision: str
    bounded_calendar: MarketSessionCalendar


@dataclass(frozen=True)
class _SourceD1RowFacts:
    """一次D1批量预览中逐会话复用的Raw价格校验事实。"""

    known_ohlc_error: str | None
    numbers: Mapping[str, tuple[Any | None, str | None]]


def _source_d1_row_facts(row: CoreObservation) -> _SourceD1RowFacts:
    """按Core原有顺序校验一次OHLC，并保留字段数值及其缺失原因。"""
    numbers: dict[str, tuple[Any | None, str | None]] = {}
    known: dict[str, Any] = {}
    ohlc_error = None
    for name in ("open", "high", "low", "close"):
        field = getattr(row, name)
        if not isinstance(field, FieldValue) or field.quality != "OBSERVED":
            value, reason = _number(field, f"DATA_INVALID_{name.upper()}")
            numbers[name] = (value, reason)
            continue
        try:
            value = exact_number(field.value)
        except (ValueError, TypeError, ZeroDivisionError):
            value = None
            ohlc_error = "DATA_INVALID_OHLC"
        if value is not None and value <= 0:
            ohlc_error = "DATA_INVALID_OHLC"
        if ohlc_error is None:
            known[name] = value
        numbers[name] = (
            value,
            "DATA_INVALID_OHLC" if ohlc_error else None,
        )
    if ohlc_error is None:
        open_value, high, low, close = (
            known.get(name) for name in ("open", "high", "low", "close")
        )
        if high is not None and low is not None and low > high:
            ohlc_error = "DATA_INVALID_OHLC"
        elif low is not None and any(
            value is not None and low > value for value in (open_value, close)
        ):
            ohlc_error = "DATA_INVALID_OHLC"
        elif high is not None and any(
            value is not None and value > high for value in (open_value, close)
        ):
            ohlc_error = "DATA_INVALID_OHLC"
    if ohlc_error:
        numbers = {
            name: (value, reason or ohlc_error)
            for name, (value, reason) in numbers.items()
        }
    return _SourceD1RowFacts(ohlc_error, numbers)


def _source_d1_column(
    rows: Sequence[CoreObservation],
    name: str,
    *,
    row_facts: Mapping[date, _SourceD1RowFacts],
) -> tuple[list[Any] | None, str | None]:
    coordinates = {row.coordinate_id for row in rows if row.state == "ACTUAL"}
    if len(coordinates) > 1:
        return None, "MIXED_PRICE_COORDINATE"
    values = []
    for row in rows:
        facts = row_facts[row.session]
        if facts.known_ohlc_error:
            return None, facts.known_ohlc_error
        value, reason = facts.numbers[name]
        if reason:
            return None, reason
        if value <= 0:
            return None, f"DATA_INVALID_{name.upper()}"
        values.append(value)
    return values, None


def _qfq_pair_economically_comparable(
    prev_row: Mapping[str, Any], cur_row: Mapping[str, Any]
) -> bool:
    """核验跨捕获QFQ行是否具有合法可比经济坐标证据。"""
    prev_explicit = prev_row.get("qfq_coordinate_id")
    cur_explicit = cur_row.get("qfq_coordinate_id")
    if (
        isinstance(prev_explicit, str)
        and prev_explicit
        and isinstance(cur_explicit, str)
        and cur_explicit
    ):
        return prev_explicit == cur_explicit
    if (
        prev_row.get("qfq_price_quality") != "OBSERVED"
        or cur_row.get("qfq_price_quality") != "OBSERVED"
    ):
        return False
    prev_close_q = prev_row.get("qfq_close_quality", prev_row.get("qfq_price_quality"))
    cur_preclose_q = cur_row.get("qfq_preclose_quality")
    if prev_close_q != "OBSERVED" or cur_preclose_q != "OBSERVED":
        return False
    try:
        prev_close = exact_number(prev_row.get("qfq_close"))
        cur_preclose = exact_number(cur_row.get("qfq_preclose"))
    except (TypeError, ValueError, ZeroDivisionError):
        return False
    return prev_close > 0 and cur_preclose > 0 and prev_close == cur_preclose


def _resolve_qfq_coordinates(
    normalized: str,
    sessions: Sequence[date],
    rows_by_session: Mapping[date, Mapping[str, Any]],
) -> dict[date, str]:
    """按逐行qfq_source_capture_id与可比经济坐标证据解析QFQ坐标ID。"""
    coords: dict[date, str] = {}
    last_actual_row: Mapping[str, Any] | None = None
    last_actual_coord: str | None = None
    has_unknown_gap = False

    for day in sessions:
        row = rows_by_session.get(day)
        capture_id = (
            row.get("qfq_source_capture_id") if isinstance(row, Mapping) else None
        )
        explicit_coord = (
            row.get("qfq_coordinate_id")
            if isinstance(row, Mapping)
            and isinstance(row.get("qfq_coordinate_id"), str)
            and row.get("qfq_coordinate_id")
            else None
        )
        raw_coord = explicit_coord or (
            f"FORWARD_ADJUSTED:BAOSTOCK_SOURCE_CAPTURE:{capture_id}:{normalized}"
            if type(capture_id) is int and capture_id > 0
            else f"FORWARD_ADJUSTED:BAOSTOCK_COORDINATE_UNKNOWN:{normalized}"
        )
        if row is None or row.get("qfq_bar_status") != "TARGET_PRESENT":
            trade_state, _ = trade_status_state_from_pg_row(
                row, price_coordinate="FORWARD_ADJUSTED"
            )
            if trade_state != "CONFIRMED_SUSPENSION":
                has_unknown_gap = True
            coords[day] = raw_coord
            continue

        trade_state, _ = trade_status_state_from_pg_row(
            row, price_coordinate="FORWARD_ADJUSTED"
        )
        if trade_state == "CONFIRMED_SUSPENSION":
            coords[day] = raw_coord
            continue
        if trade_state != "ACTUAL":
            has_unknown_gap = True
            coords[day] = raw_coord
            continue

        if type(capture_id) is int and capture_id > 0:
            if last_actual_row is not None and last_actual_coord is not None:
                prev_cap_id = last_actual_row.get("qfq_source_capture_id")
                if prev_cap_id == capture_id or (
                    not has_unknown_gap
                    and _qfq_pair_economically_comparable(last_actual_row, row)
                ):
                    effective_coord = last_actual_coord
                else:
                    effective_coord = raw_coord
            else:
                effective_coord = raw_coord
            coords[day] = effective_coord
            last_actual_row = row
            last_actual_coord = effective_coord
            has_unknown_gap = False
        else:
            coords[day] = raw_coord
            last_actual_row = row
            last_actual_coord = raw_coord
            has_unknown_gap = True
    return coords


def _calculate_core_d1_inputs(
    history: Sequence[CoreObservation],
    *,
    official_calendar: MarketSessionCalendar,
    row_facts: Mapping[date, _SourceD1RowFacts] | None = None,
) -> dict[str, FieldValue]:
    """复用现役Core原语，生成D1结构所需的7个Core字段。"""
    if not isinstance(official_calendar, MarketSessionCalendar):
        raise ContractError("Core计算必须传入版本化官方市场会话日历")
    _validate_history(history, official_calendar)
    latest = history[-1]
    if latest.state != "ACTUAL":
        reason = {
            "UNKNOWN": "CURRENT_SESSION_UNKNOWN",
            "CONFIRMED_SUSPENSION": "CURRENT_SESSION_SUSPENDED",
            "PRE_LISTING": "CURRENT_SESSION_PRE_LISTING",
            "ADJUSTMENT_UNKNOWN": "PRICE_IDENTITY_UNKNOWN",
            "IDENTITY_UNKNOWN": "SECURITY_IDENTITY_UNKNOWN",
        }[latest.state]
        return {
            name: unknown(reason)
            for name in (
                "core.amount_ratio20",
                "core.prior_high20",
                "core.ma5",
                "core.ma20",
                "core.atr20",
                "core.clv",
                "core.ma_slope20_atr",
            )
        }

    prior_rows, prior_reason = _actual_window(history, 20, exclude_current=True)
    prior_highs, high_reason = (None, prior_reason)
    if not prior_reason:
        prior_highs, high_reason = (
            _source_d1_column(prior_rows, "high", row_facts=row_facts)
            if row_facts is not None
            else _extreme_column(prior_rows, "high")
        )
    prior_high20 = _known(
        max(prior_highs, default=None) if prior_highs else None,
        prior_reason or high_reason,
    )

    ratio_input = (
        "native_amount"
        if any(getattr(row, "native_amount") is not None for row in history)
        else "amount"
    )
    prior_amounts, amount_col_reason = (
        _numeric_column(prior_rows, ratio_input, nonnegative=True)
        if not prior_reason
        else (None, prior_reason)
    )
    current_amount, current_amount_reason = _number(
        getattr(latest, ratio_input), "DATA_INVALID_AMOUNT"
    )
    amount_reason = prior_reason or amount_col_reason or current_amount_reason
    if not amount_reason and current_amount < 0:
        amount_reason = "DATA_INVALID_AMOUNT"
    amount_denom = mean(prior_amounts) if prior_amounts else None
    if not amount_reason and (amount_denom is None or amount_denom <= 0):
        amount_reason = "NONPOSITIVE_DENOMINATOR"
    amount_ratio20 = (
        unknown(amount_reason)
        if amount_reason
        else FieldValue(current_amount / amount_denom)
    )

    ma5_rows, ma5_reason = _actual_window(history, 5)
    ma5_closes, ma5_val_reason = (
        (
            _source_d1_column(ma5_rows, "close", row_facts=row_facts)
            if row_facts is not None
            else _column(ma5_rows, "close")
        )
        if not ma5_reason
        else (None, ma5_reason)
    )
    ma5 = _known(mean(ma5_closes) if ma5_closes else None, ma5_reason or ma5_val_reason)

    ma20_rows, ma20_reason = _actual_window(history, 20)
    ma20_closes, ma20_val_reason = (
        (
            _source_d1_column(ma20_rows, "close", row_facts=row_facts)
            if row_facts is not None
            else _column(ma20_rows, "close")
        )
        if not ma20_reason
        else (None, ma20_reason)
    )
    ma20 = _known(
        mean(ma20_closes) if ma20_closes else None, ma20_reason or ma20_val_reason
    )

    rows, atr_reason = _actual_window(history, 21)
    if not atr_reason:
        atr_reason = _coordinate_error(rows)
    if not atr_reason:
        atr_reason = next(
            (
                error
                for row in rows
                if (
                    error := (
                        row_facts[row.session].known_ohlc_error
                        if row_facts is not None
                        else _known_ohlc_error(row)
                    )
                )
            ),
            None,
        )
    if not atr_reason:
        true_ranges = []
        for previous, current in zip(rows, rows[1:]):
            # 窗口已逐行检查坐标及已知OHLC；此处只读取实际依赖字段。
            if row_facts is None:
                cur_high, high_reason = _number(current.high, "DATA_INVALID_HIGH")
                cur_low, low_reason = _number(current.low, "DATA_INVALID_LOW")
                prev_close, close_reason = _number(previous.close, "DATA_INVALID_CLOSE")
            else:
                cur_high, high_reason = row_facts[current.session].numbers["high"]
                cur_low, low_reason = row_facts[current.session].numbers["low"]
                prev_close, close_reason = row_facts[previous.session].numbers["close"]
            dependency_error = high_reason or low_reason or close_reason
            if dependency_error:
                atr_reason = dependency_error
                break
            true_ranges.append(
                max(
                    cur_high - cur_low,
                    abs(cur_high - prev_close),
                    abs(cur_low - prev_close),
                )
            )
    atr20 = _known(mean(true_ranges), None) if not atr_reason else unknown(atr_reason)

    slope_rows, slope_reason = _actual_window(history, 25)
    slope_values, slope_value_reason = (
        (
            _source_d1_column(slope_rows, "close", row_facts=row_facts)
            if row_facts is not None
            else _column(slope_rows, "close")
        )
        if not slope_reason
        else (None, slope_reason)
    )
    slope_reason = slope_reason or slope_value_reason
    if not slope_reason and atr20.quality != "OBSERVED":
        slope_reason = atr20.reason or "UPSTREAM_UNKNOWN"
    if not slope_reason and atr20.value <= 0:
        slope_reason = "ZERO_ATR"
    slope20 = (
        unknown(slope_reason)
        if slope_reason
        else FieldValue(
            (mean(slope_values[-20:]) - mean(slope_values[-25:-5])) / atr20.value
        )
    )

    if row_facts is None:
        closes, close_reason = _column([latest], "close")
        highs, high_reason = _extreme_column([latest], "high")
        lows, low_reason = _extreme_column([latest], "low")
    else:
        closes, close_reason = _source_d1_column([latest], "close", row_facts=row_facts)
        highs, high_reason = _source_d1_column([latest], "high", row_facts=row_facts)
        lows, low_reason = _source_d1_column([latest], "low", row_facts=row_facts)
    if closes and highs and lows:
        high, low = highs[0], lows[0]
        clv = (
            unknown("ZERO_PRICE_RANGE")
            if high == low
            else FieldValue((closes[0] - low) / (high - low))
        )
    else:
        clv = unknown(close_reason or high_reason or low_reason)
    return {
        "core.amount_ratio20": amount_ratio20,
        "core.prior_high20": prior_high20,
        "core.ma5": ma5,
        "core.ma20": ma20,
        "core.atr20": atr20,
        "core.clv": clv,
        "core.ma_slope20_atr": slope20,
    }


def prepare_source_d1_preview_context(
    target_sessions: Sequence[date],
) -> _SourceD1PreviewContext:
    """为一次D1请求准备精确日历和显式算法版本，不跨请求缓存。"""
    targets = tuple(target_sessions)
    if (
        not targets
        or any(type(day) is not date for day in targets)
        or tuple(sorted(set(targets))) != targets
    ):
        raise ResearchEntryError(
            "PG_HISTORY_CALENDAR_INVALID", "D1批量预览目标会话必须唯一且递增"
        )
    calendars_by_year = {
        year: _load_calendar(next(day for day in targets if day.year == year))
        for year in sorted({day.year for day in targets})
    }
    return _SourceD1PreviewContext(
        calendars_by_year=calendars_by_year,
        core_algorithm_version="STOCK_A_CORE_R1",
        execution_adapter_version="SOURCE_D1_PREVIEW_R1",
    )


def _source_d1_target_window(
    target: date,
    *,
    sessions: tuple[date, ...],
    context: _SourceD1PreviewContext,
) -> _SourceD1TargetWindow:
    cached = context.target_windows.get(target)
    if cached is not None and cached.session_vector == sessions:
        return cached

    expected_sessions = tuple(day for day in sessions if day <= target)
    calendar_data = context.calendars_by_year.get(target.year)
    if calendar_data is None:
        raise ResearchEntryError(
            "PG_HISTORY_CALENDAR_INVALID", "D1预览上下文缺少目标年度日历"
        )
    calendar_payload, calendar_metadata, calendar_revision, all_calendar_sessions = (
        calendar_data
    )
    if (
        not expected_sessions
        or len(expected_sessions) > 75
        or expected_sessions[-1] != target
        or any(day not in all_calendar_sessions for day in expected_sessions)
    ):
        raise ResearchEntryError(
            "PG_HISTORY_CALENDAR_INVALID", "PG历史窗口未与版本化官方日历严格绑定"
        )
    official_window = tuple(
        day for day in all_calendar_sessions if expected_sessions[0] <= day <= target
    )
    if expected_sessions != official_window:
        raise ResearchEntryError(
            "CALENDAR_SESSION_GAP", "PG历史窗口遗漏版本化官方日历中的交易会话"
        )
    window = _SourceD1TargetWindow(
        session_vector=sessions,
        expected_sessions=expected_sessions,
        calendar_payload=calendar_payload,
        calendar_metadata=calendar_metadata,
        calendar_revision=calendar_revision,
        bounded_calendar=MarketSessionCalendar(
            calendar_id=calendar_payload["calendar_id"],
            revision=calendar_revision,
            sessions=expected_sessions,
        ),
    )
    context.target_windows[target] = window
    return window


def _source_d1_preview_for_target(
    *,
    target: date,
    sessions: tuple[date, ...],
    normalized: str,
    source_capture_id: int,
    price_coordinate: str,
    coordinate_id: str,
    rows_by_session: Mapping[date, Mapping[str, Any]],
    observations_by_session: Mapping[date, CoreObservation],
    row_facts: Mapping[date, _SourceD1RowFacts],
    context: _SourceD1PreviewContext,
) -> dict[str, Any]:
    target_source_row = rows_by_session.get(target)
    target_qfq_capture_id = (
        target_source_row.get("qfq_source_capture_id")
        if isinstance(target_source_row, Mapping)
        and type(target_source_row.get("qfq_source_capture_id")) is int
        else source_capture_id
    )
    source_capture_id = target_qfq_capture_id
    coordinate_id = (
        f"FORWARD_ADJUSTED:BAOSTOCK_SOURCE_CAPTURE:{target_qfq_capture_id}:{normalized}"
    )
    target_window = _source_d1_target_window(
        target,
        sessions=sessions,
        context=context,
    )
    expected_sessions = target_window.expected_sessions
    calendar_payload = target_window.calendar_payload
    calendar_metadata = target_window.calendar_metadata
    calendar_revision = target_window.calendar_revision
    history = tuple(observations_by_session[day] for day in expected_sessions)
    core = _calculate_core_d1_inputs(
        history,
        official_calendar=target_window.bounded_calendar,
        row_facts=row_facts,
    )
    _apply_calendar_coverage_to_core(
        core, history, calendar_metadata[-1]["coverage_context"]
    )
    target_row = rows_by_session.get(target)
    close_field = observations_by_session[target].close
    evaluation = evaluate_raw_breakout_predicate(
        close_field,
        core["core.prior_high20"],
        core["core.atr20"],
        core["core.clv"],
    )
    for predicate in (evaluation["p1"], evaluation["p2"]):
        predicate["evidence"] = {
            key: _render_value(value) for key, value in predicate["evidence"].items()
        }
    return {
        "symbol": normalized,
        "target_session": target.isoformat(),
        "source_capture_id": source_capture_id,
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "price_coordinate": price_coordinate,
        "structure_core_fields": {
            name: _render_field(core[name])
            for name in (
                "core.amount_ratio20",
                "core.prior_high20",
                "core.ma5",
                "core.ma20",
                "core.atr20",
                "core.clv",
                "core.ma_slope20_atr",
            )
        },
        "target_bar_status": {
            "TARGET_PRESENT": "BAOSTOCK_TARGET_BAR_PRESENT",
            "TARGET_MISSING": "BAOSTOCK_TARGET_BAR_MISSING",
            "READ_FAILED": "BAOSTOCK_READ_FAILED",
            "HISTORY_ROW_NOT_STORED": "HISTORY_ROW_NOT_STORED",
        }[target_row["bar_status"] if target_row else "HISTORY_ROW_NOT_STORED"],
        "raw_d1_breakout_predicate": {
            "scope": "SOURCE_SYMBOL_ONLY_QFQ_NON_PIT_RETROSPECTIVE",
            "target_session": target.isoformat(),
            "price_coordinate": price_coordinate,
            "coordinate_id": coordinate_id,
            "core_producer_symbol": "stock_a.research.core:calculate_core",
            "core_producer_algorithm_version": context.core_algorithm_version,
            "core_producer_role": "SEMANTIC_CORE_CONTRACT_SOURCE",
            "execution_adapter_symbol": (
                "stock_a.research.entry_history.history_preview:"
                "_calculate_core_d1_inputs"
            ),
            "execution_adapter_version": context.execution_adapter_version,
            "execution_adapter_role": "ACTUAL_D1_FAST_CALCULATION_ADAPTER",
            "core_field_ids": [
                "core.prior_high20",
                "core.atr20",
                "core.clv",
            ],
            "inputs": {
                "C": _render_field(close_field),
                "core.prior_high20": _render_field(core["core.prior_high20"]),
                "core.atr20": _render_field(core["core.atr20"]),
                "core.clv": _render_field(core["core.clv"]),
            },
            "official_dependency_window": {
                "calendar_id": calendar_payload["calendar_id"],
                "calendar_revision": calendar_revision,
                "start_session": expected_sessions[0].isoformat(),
                "end_session": expected_sessions[-1].isoformat(),
                "official_session_count": len(expected_sessions),
                "prior_high20_actual_bars": 20,
                "prior_high20_excludes_target": True,
                "atr20_true_ranges": 20,
                "atr20_dependency_actual_bars": 21,
                "clv_target_bar_only": True,
            },
            "predicates": evaluation,
        },
    }


def research_stock_pg_d1_history_previews(
    *,
    symbol: str,
    event_sessions: Sequence[date],
    followup_session: date,
    session_dates: Sequence[date],
    history_rows: Sequence[Mapping[str, Any]],
    context: _SourceD1PreviewContext,
    source_capture_id: int | None = None,
    price_coordinate: str | None = None,
    **legacy_source_identity: Any,
) -> dict[date, dict[str, Any]]:
    """按paired BaoStock捕获和QFQ坐标计算D1会话与所需后继。"""
    events = tuple(event_sessions)
    sessions = tuple(session_dates)
    normalized = _normalize_symbol(symbol)[0]
    if type(source_capture_id) is not int or source_capture_id <= 0:
        return {
            day: {
                "status": "NOT_COMPUTED",
                "reason_code": "PG_BAOSTOCK_CAPTURE_REQUIRED",
                "symbol": normalized,
                "target_session": day.isoformat(),
                "price_coordinate": price_coordinate,
                "time_role": "NON_PIT_RETROSPECTIVE",
            }
            for day in events
        }
    if price_coordinate != "FORWARD_ADJUSTED":
        return {
            day: {
                "status": "NOT_COMPUTED",
                "reason_code": "D1_REQUIRES_QFQ_PRICE_COORDINATE",
                "symbol": normalized,
                "target_session": day.isoformat(),
                "source_capture_id": source_capture_id,
                "price_coordinate": price_coordinate,
                "time_role": "NON_PIT_RETROSPECTIVE",
            }
            for day in events
        }
    if (
        not events
        or tuple(sorted(set(events))) != events
        or any(type(day) is not date for day in events)
        or type(followup_session) is not date
        or not sessions
        or len(sessions) > 75
        or any(type(day) is not date for day in sessions)
        or tuple(sorted(set(sessions))) != sessions
        or followup_session != sessions[-1]
        or any(day not in sessions or day > followup_session for day in events)
    ):
        raise ResearchEntryError(
            "PG_HISTORY_CALENDAR_INVALID", "D1批量预览会话范围无效"
        )

    rows = tuple(history_rows)
    row_dates = tuple(row.get("session_date") for row in rows)
    if (
        len(set(row_dates)) != len(row_dates)
        or any(day not in sessions for day in row_dates)
        or row_dates != tuple(sorted(row_dates))
    ):
        raise ResearchEntryError(
            "PG_HISTORY_SESSION_ORDER_INVALID", "PG历史Raw行重复、越界或未按会话递增"
        )
    if any(
        row.get("source_symbol") != normalized
        or (
            row.get("session_date") == sessions[-1]
            and row.get("source_capture_id") != source_capture_id
        )
        for row in rows
    ):
        raise ResearchEntryError(
            "PG_HISTORY_REVISION_MISMATCH",
            "PG历史行混入不同来源代码或目标日捕获ID不匹配",
        )

    rows_by_session = {row["session_date"]: row for row in rows}
    coordinate_id = (
        f"FORWARD_ADJUSTED:BAOSTOCK_SOURCE_CAPTURE:{source_capture_id}:{normalized}"
    )
    coords_by_session = _resolve_qfq_coordinates(normalized, sessions, rows_by_session)

    observations_by_session: dict[date, CoreObservation] = {}
    for day in sessions:
        row = rows_by_session.get(day)
        observations_by_session[day] = _core_observation_from_pg_price_row(
            row,
            session=day,
            source_capture_id=source_capture_id if day == sessions[-1] else None,
            price_coordinate=price_coordinate,
            coordinate_id=coords_by_session[day],
        )

    row_facts = {
        day: _source_d1_row_facts(observation)
        for day, observation in observations_by_session.items()
        if observation.state == "ACTUAL"
    }

    result = {
        day: _source_d1_preview_for_target(
            target=day,
            sessions=sessions,
            normalized=normalized,
            source_capture_id=source_capture_id,
            price_coordinate=price_coordinate,
            coordinate_id=coordinate_id,
            rows_by_session=rows_by_session,
            observations_by_session=observations_by_session,
            context=context,
            row_facts=row_facts,
        )
        for day in events
    }
    if followup_session not in result and any(
        preview["raw_d1_breakout_predicate"]["predicates"]["result"]["state"] == "TRUE"
        for preview in result.values()
    ):
        result[followup_session] = _source_d1_preview_for_target(
            target=followup_session,
            sessions=sessions,
            normalized=normalized,
            source_capture_id=source_capture_id,
            price_coordinate=price_coordinate,
            coordinate_id=coordinate_id,
            rows_by_session=rows_by_session,
            observations_by_session=observations_by_session,
            context=context,
            row_facts=row_facts,
        )
    return result


def research_stock_pg_history_preview(
    *,
    symbol: str,
    target_session: str,
    session_dates: Sequence[date],
    history_rows: Sequence[Mapping[str, Any]],
    source_capture_id: int | None = None,
    price_coordinate: str | None = None,
    missing_history_sessions: Sequence[date] | None = None,
    profile_contract: dict[str, Any] | None = None,
    closed_period_bars: Mapping[str, Sequence[object]] | None = None,
    include_profile_preview: bool = True,
    selected_profile_fields: frozenset[str] | None = None,
    seed_core_inputs: Mapping[str, FieldValue] | None = None,
    cross_section_fields: Mapping[str, FieldValue] | None = None,
    cross_section_meta: Mapping[str, Any] | None = None,
    include_raw_breakout: bool = False,
    allow_missing_target_row: bool = False,
    **legacy_source_identity: Any,
) -> dict[str, Any]:
    """把同一paired BaoStock捕获的QFQ窗口接到Core、Profile与D1预览。"""
    target = _parse_target(target_session)
    normalized = _normalize_symbol(symbol)[0]

    def not_computed(reason_code: str, message: str) -> dict[str, Any]:
        return {
            "ok": False,
            "contract": "STOCK_A_PG_BAOSTOCK_CORE_PROFILE_PREVIEW_R2",
            "status": "NOT_COMPUTED",
            "reason_code": reason_code,
            "reason": message,
            "symbol": normalized,
            "target_session": target.isoformat(),
            "source_capture_id": source_capture_id
            if type(source_capture_id) is int and source_capture_id > 0
            else None,
            "price_coordinate": price_coordinate,
            "time_role": "NON_PIT_RETROSPECTIVE",
            "source": {"provider": "BAOSTOCK"},
            "database_write": False,
        }

    if type(source_capture_id) is not int or source_capture_id <= 0:
        return not_computed(
            "PG_BAOSTOCK_CAPTURE_REQUIRED",
            "历史Core/Profile读取必须显式绑定paired BaoStock source_capture_id；不回退TDX或包键。",
        )
    if price_coordinate != "FORWARD_ADJUSTED":
        return not_computed(
            "CORE_REQUIRES_QFQ_PRICE_COORDINATE",
            "跨日Core、MA、ATR、趋势和D1输入必须使用BaoStock QFQ坐标。",
        )
    if include_profile_preview and profile_contract is None:
        return not_computed(
            "PRICE_PROFILE_CONTRACT_REQUIRED", "Profile预览缺少已加载的R2价格字段合同。"
        )
    if (
        not isinstance(session_dates, (tuple, list))
        or not session_dates
        or len(session_dates) > 250
        or any(type(day) is not date for day in session_dates)
        or tuple(sorted(set(session_dates))) != tuple(session_dates)
        or session_dates[-1] != target
    ):
        raise ResearchEntryError(
            "PG_HISTORY_CALENDAR_INVALID",
            "PG历史窗口必须是目标日截止的1至250个官方会话。",
        )

    calendar_payload, calendar_metadata, calendar_revision, all_calendar_sessions = (
        _load_calendar(target)
    )
    coverage_context = calendar_metadata[-1]["coverage_context"]
    expected_sessions = tuple(session_dates)
    official_window = tuple(
        day for day in all_calendar_sessions if expected_sessions[0] <= day <= target
    )
    if expected_sessions != official_window:
        raise ResearchEntryError(
            "CALENDAR_SESSION_GAP", "PG历史窗口遗漏版本化官方日历中的交易会话。"
        )

    rows = tuple(history_rows)
    if any(not isinstance(row, Mapping) for row in rows):
        raise ResearchEntryError(
            "PG_HISTORY_ROW_INVALID", "PG价格历史行必须是字段映射。"
        )
    row_dates = tuple(row.get("session_date") for row in rows)
    if (
        len(set(row_dates)) != len(row_dates)
        or any(day not in expected_sessions for day in row_dates)
        or row_dates != tuple(sorted(row_dates))
        or any(row.get("source_symbol") != normalized for row in rows)
        or (
            (
                target_row := next(
                    (row for row in rows if row.get("session_date") == target), None
                )
            )
            is not None
            and target_row.get("source_capture_id") != source_capture_id
        )
    ):
        raise ResearchEntryError(
            "PG_HISTORY_CAPTURE_BINDING_INVALID",
            "PG历史行必须绑定相同来源代码、目标日paired capture和递增会话；历史捕获仅作逐日溯源。",
        )
    if any(row.get("price_coordinate") not in (None, price_coordinate) for row in rows):
        raise ResearchEntryError(
            "PG_HISTORY_COORDINATE_MISMATCH", "PG历史行含不同价格坐标。"
        )
    rows_by_session = {row["session_date"]: row for row in rows}
    missing_dates = tuple(
        day for day in expected_sessions if day not in rows_by_session
    )
    if (
        missing_history_sessions is not None
        and tuple(missing_history_sessions) != missing_dates
    ):
        raise ResearchEntryError(
            "PG_HISTORY_MISSING_SESSION_BINDING_INVALID",
            "PG历史缺失会话清单与实际QFQ行不一致。",
        )
    if target not in rows_by_session and not allow_missing_target_row:
        return not_computed(
            "PG_HISTORY_TARGET_ROW_MISSING",
            "QFQ历史没有精确目标日行；不回退RAW或最近日期。",
        )

    coords_by_session = _resolve_qfq_coordinates(
        normalized, expected_sessions, rows_by_session
    )
    coordinate_id = coords_by_session.get(
        target, f"FORWARD_ADJUSTED:BAOSTOCK_COORDINATE_UNKNOWN:{normalized}"
    )
    history = tuple(
        _core_observation_from_pg_price_row(
            rows_by_session.get(session),
            session=session,
            source_capture_id=source_capture_id if session == target else None,
            price_coordinate=price_coordinate,
            coordinate_id=coords_by_session[session],
        )
        for session in expected_sessions
    )
    status_counts = {
        "TARGET_PRESENT": 0,
        "TARGET_MISSING": 0,
        "READ_FAILED": 0,
        "HISTORY_ROW_NOT_STORED": 0,
    }
    for session in expected_sessions:
        row = rows_by_session.get(session)
        status = row.get("bar_status") if row is not None else "HISTORY_ROW_NOT_STORED"
        if status not in status_counts:
            raise ResearchEntryError(
                "PG_HISTORY_STATUS_INVALID", "PG行含未登记价格状态。"
            )
        status_counts[status] += 1

    target_row = rows_by_session.get(target)
    target_observation = history[-1]
    if seed_core_inputs is None:
        core = calculate_core(
            history,
            official_calendar=MarketSessionCalendar(
                calendar_id=calendar_payload["calendar_id"],
                revision=calendar_revision,
                sessions=expected_sessions,
            ),
        )
    else:
        if include_raw_breakout or selected_profile_fields != frozenset(
            {"profile.compression", "profile.extension", "profile.ma_structure"}
        ):
            raise ResearchEntryError(
                "PG_SEED_CORE_SCOPE_INVALID", "已存Core复用仅适用于Seed所需画像。"
            )
        core = dict(seed_core_inputs)
        core.update(
            calculate_seed_missing_core(
                history,
                official_calendar=MarketSessionCalendar(
                    calendar_id=calendar_payload["calendar_id"],
                    revision=calendar_revision,
                    sessions=expected_sessions,
                ),
                saved_atr20=core["core.atr20"],
            )
        )
    _apply_calendar_coverage_to_core(core, history, coverage_context)
    rendered_core = {name: _render_field(value) for name, value in sorted(core.items())}
    unknown_reasons: dict[str, int] = {}
    for value in core.values():
        if value.quality != "OBSERVED":
            reason = value.reason or "UNKNOWN_REASON_NOT_PROVIDED"
            unknown_reasons[reason] = unknown_reasons.get(reason, 0) + 1
    if seed_core_inputs is None and len(core) != 43:
        raise ResearchEntryError(
            "PG_CORE_FIELD_COUNT_INVALID", f"Core现役字段数异常：{len(core)}"
        )

    raw_field_specs = {
        "open": ("raw_open", "raw_open_quality", "raw_open_reason"),
        "high": ("raw_high", "raw_high_quality", "raw_high_reason"),
        "low": ("raw_low", "raw_low_quality", "raw_low_reason"),
        "close": ("raw_close", "raw_close_quality", "raw_close_reason"),
        "preclose": ("raw_preclose", "raw_preclose_quality", "raw_preclose_reason"),
        "amount": ("raw_amount", "amount_quality", "amount_reason"),
        "volume": ("raw_volume", "volume_quality", "volume_reason"),
        "turn": ("raw_turn_value", "raw_turn_quality", "raw_turn_reason"),
        "tradestatus": (
            "raw_tradestatus_value",
            "raw_tradestatus_quality",
            "raw_tradestatus_reason",
        ),
        "is_st": ("raw_is_st_value", "raw_is_st_quality", "raw_is_st_reason"),
    }
    raw_fields = {
        name: _render_field(
            _field_from_pg_raw(target_row, value, quality, reason)
            if target_row is not None
            else unknown("TARGET_BAR_MISSING")
        )
        for name, (value, quality, reason) in raw_field_specs.items()
    }
    qfq_fields = {
        name: _render_field(
            _field_from_pg_raw(
                target_row,
                column,
                f"qfq_{name}_quality",
                f"qfq_{name}_reason",
            )
            if target_row is not None
            else unknown("TARGET_BAR_MISSING")
        )
        for name, column in (
            ("open", "qfq_open"),
            ("high", "qfq_high"),
            ("low", "qfq_low"),
            ("close", "qfq_close"),
        )
    }
    actual_bars = sum(row.state == "ACTUAL" for row in history)
    qfq_price_quality_counts: dict[str, int] = {}
    raw_price_quality_counts: dict[str, int] = {}
    raw_amount_quality_counts: dict[str, int] = {}
    raw_volume_quality_counts: dict[str, int] = {}
    for row in rows:
        for counts, key in (
            (qfq_price_quality_counts, "qfq_price_quality"),
            (raw_price_quality_counts, "price_quality"),
            (raw_amount_quality_counts, "raw_amount_quality"),
            (raw_volume_quality_counts, "raw_volume_quality"),
        ):
            quality = str(row.get(key) or "UNKNOWN")
            counts[quality] = counts.get(quality, 0) + 1

    ma20_rows, _ma20_window_reason = _actual_window(history, 20)
    ma20_actual = sum(row.close.quality == "OBSERVED" for row in ma20_rows)
    atr_rows, _atr_window_reason = _actual_window(history, 21)
    atr20_actual = sum(
        previous.close.quality == "OBSERVED"
        and current.high.quality == "OBSERVED"
        and current.low.quality == "OBSERVED"
        for previous, current in zip(atr_rows, atr_rows[1:])
    )
    core_ma20 = core.get("core.ma20")
    core_atr20 = core.get("core.atr20")
    price_window_usable = bool(
        core_ma20 is not None
        and core_atr20 is not None
        and core_ma20.quality == "OBSERVED"
        and core_atr20.quality == "OBSERVED"
    )
    price_window_status = "USABLE" if price_window_usable else "UNKNOWN_DATA"
    price_window_reason = (
        None
        if price_window_usable
        else (
            core_atr20.reason
            if core_atr20 and core_atr20.reason
            else core_ma20.reason
            if core_ma20
            else "QFQ_PRICE_WINDOW_UNAVAILABLE"
        )
    )
    profile_preview = None
    if include_profile_preview and profile_contract is not None:
        raw_period_trends: dict[str, Any] | None = None
        if (
            closed_period_bars is None
            and price_coordinate == "FORWARD_ADJUSTED"
            and isinstance(profile_contract, Mapping)
            and (
                selected_profile_fields is None
                or "profile.closed_week_trend" in selected_profile_fields
                or "profile.closed_month_trend" in selected_profile_fields
            )
        ):
            period_calendar_sessions, period_cov_start, period_cov_end = (
                load_period_calendar(target)
            )
            period_bars = build_source_symbol_raw_periods(
                target_session=target,
                session_dates=expected_sessions,
                calendar_sessions=period_calendar_sessions,
                history_rows=tuple(
                    rows_by_session[s]
                    for s in expected_sessions
                    if s in rows_by_session
                ),
                price_coordinate=price_coordinate,
                calendar_coverage_start=period_cov_start,
                calendar_coverage_end=period_cov_end,
            )
            period_rules = profile_contract.get("profile_expression_registry", {})
            period_registry = profile_contract.get("field_registry", [])
            period_source_cutoff = f"BAOSTOCK_CAPTURE:{source_capture_id}:{price_coordinate}:THROUGH:{target.isoformat()}"
            raw_period_trends = {
                "profile.closed_week_trend": evaluate_raw_period_trend(
                    kind="WEEK",
                    price_coordinate=price_coordinate,
                    periods=period_bars["WEEK"],
                    target_session=target,
                    calendar_id=calendar_payload["calendar_id"],
                    calendar_revision=calendar_revision,
                    source_cutoff=period_source_cutoff,
                    profile_rules=period_rules,
                    field_registry=period_registry,
                ),
                "profile.closed_month_trend": evaluate_raw_period_trend(
                    kind="MONTH",
                    price_coordinate=price_coordinate,
                    periods=period_bars["MONTH"],
                    target_session=target,
                    calendar_id=calendar_payload["calendar_id"],
                    calendar_revision=calendar_revision,
                    source_cutoff=period_source_cutoff,
                    profile_rules=period_rules,
                    field_registry=period_registry,
                ),
            }
        profile_preview = _build_profile_preview(
            selected_fields=selected_profile_fields,
            symbol=normalized,
            target_session=target,
            source_last_session=target,
            source_cutoff=(
                f"BAOSTOCK_CAPTURE:{source_capture_id}:{price_coordinate}:THROUGH:{target.isoformat()}"
            ),
            target_bar=target_row,
            history=history,
            core=core,
            calendar_revision=calendar_revision,
            contract=profile_contract,
            source_capture_id=source_capture_id,
            price_coordinate=price_coordinate,
            closed_period_bars=closed_period_bars,
            raw_period_trends=raw_period_trends,
            cross_section_fields=cross_section_fields,
            cross_section_metadata=cross_section_meta,
            official_sessions=all_calendar_sessions,
            source_snapshot_role="BAOSTOCK_CAPTURE_NON_PIT_NOT_HISTORICAL_AVAILABLE_AT",
        )
        components = profile_preview.get("components", [])
        if (
            len(components) != 11
            if selected_profile_fields is None
            else {item["field_id"] for item in components} != selected_profile_fields
        ):
            raise ResearchEntryError(
                "PG_PROFILE_RULE_COUNT_INVALID", "Profile规则数异常。"
            )

    target_status = (
        target_row.get("bar_status") if target_row else "HISTORY_ROW_NOT_STORED"
    )
    target_bar_status = {
        "TARGET_PRESENT": "BAOSTOCK_TARGET_BAR_PRESENT",
        "TARGET_MISSING": "BAOSTOCK_TARGET_BAR_MISSING",
        "READ_FAILED": "BAOSTOCK_READ_FAILED",
        "HISTORY_ROW_NOT_STORED": "HISTORY_ROW_NOT_STORED",
    }[target_status]
    captures_by_session = [
        {
            "session_date": row["session_date"].isoformat(),
            "source_capture_id": row.get("qfq_source_capture_id"),
            "price_coordinate": price_coordinate,
            "capture_read_started_at": row.get("observed_started_at"),
            "capture_read_completed_at": row.get("observed_completed_at"),
        }
        for row in rows
    ]
    result: dict[str, Any] = {
        "ok": True,
        "contract": "STOCK_A_PG_BAOSTOCK_CORE_PROFILE_PREVIEW_R2",
        "status": "RESEARCH_PREVIEW_NON_PIT",
        "symbol": normalized,
        "target_session": target.isoformat(),
        "source_capture_id": source_capture_id,
        "price_coordinate": price_coordinate,
        "research_mode": "NON_PIT_RETROSPECTIVE",
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "stable_security_id_issued": False,
        "source_object_status": "BAOSTOCK_CAPTURE_OBSERVED",
        "target_bar_status": target_bar_status,
        "price_research_status": "AVAILABLE"
        if target_observation.close.quality == "OBSERVED"
        else "UNKNOWN_DATA",
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "history_status": "RECORDS_COMPLETE"
        if not missing_dates
        else "HISTORY_ROW_NOT_STORED",
        "history_records_complete": not missing_dates,
        "missing_history_sessions": [day.isoformat() for day in missing_dates],
        "source": {
            "provider": "BAOSTOCK",
            "source_capture_id": source_capture_id,
            "price_coordinate": price_coordinate,
            "price_coordinates_in_capture": ["RAW_UNADJUSTED", "FORWARD_ADJUSTED"],
            "source_clock_role": (
                target_row.get("source_clock_role") if target_row else None
            ),
            "source_observed_started_at": (
                target_row.get("observed_started_at") if target_row else None
            ),
            "source_observed_completed_at": (
                target_row.get("observed_completed_at") if target_row else None
            ),
            "source_available_at": None,
            "capture_revisions": captures_by_session,
        },
        "window": {
            "calendar_id": calendar_payload["calendar_id"],
            "calendar_revision": calendar_revision,
            "start_session": expected_sessions[0].isoformat(),
            "end_session": expected_sessions[-1].isoformat(),
            "requested_sessions": len(expected_sessions),
            "evaluated_sessions": len(rows),
            "stored_sessions": len(rows),
            "real_bar_count": actual_bars,
            "gap_count": len(expected_sessions) - actual_bars,
            "session_status_counts": status_counts,
            "future_session_rows_used": 0,
            "price_window_usable": price_window_usable,
            "price_window_status": price_window_status,
            "price_window_reason": price_window_reason,
            "price_window_valid_bar_count": atr20_actual,
            "price_window_expected_bar_count": 20,
            "ma20_window_valid_bar_count": ma20_actual,
            "ma20_window_expected_bar_count": 20,
        },
        "raw": raw_fields,
        "qfq": qfq_fields,
        "quality": {
            "qfq_price_quality_counts": dict(sorted(qfq_price_quality_counts.items())),
            "raw_price_quality_counts": dict(sorted(raw_price_quality_counts.items())),
            "raw_amount_quality_counts": dict(
                sorted(raw_amount_quality_counts.items())
            ),
            "raw_volume_quality_counts": dict(
                sorted(raw_volume_quality_counts.items())
            ),
            "amount_unit_status": target_row.get("amount_unit_status")
            if target_row
            else None,
            "volume_unit_status": target_row.get("volume_unit_status")
            if target_row
            else None,
        },
        "core": {
            "field_count": len(core),
            "unknown_count": sum(
                value.quality != "OBSERVED" for value in core.values()
            ),
            "unknown_field_count": sum(
                value.quality != "OBSERVED" for value in core.values()
            ),
            "unknown_reason_counts": dict(sorted(unknown_reasons.items())),
            "fields": rendered_core,
        },
        "profile_preview": profile_preview,
        "database_write": False,
        "source_file_read": False,
        "network_access": False,
    }
    if include_raw_breakout:
        close_field = target_observation.close
        evaluation = evaluate_raw_breakout_predicate(
            close_field,
            core["core.prior_high20"],
            core["core.atr20"],
            core["core.clv"],
        )
        for predicate in (evaluation["p1"], evaluation["p2"]):
            predicate["evidence"] = {
                key: _render_value(value)
                for key, value in predicate["evidence"].items()
            }
        result["raw_d1_breakout_predicate"] = {
            "scope": "SOURCE_SYMBOL_ONLY_QFQ_NON_PIT_RETROSPECTIVE",
            "target_session": target.isoformat(),
            "source_capture_id": source_capture_id,
            "price_coordinate": price_coordinate,
            "coordinate_id": coordinate_id,
            "core_producer_symbol": "stock_a.research.core:calculate_core",
            "core_field_ids": ["core.prior_high20", "core.atr20", "core.clv"],
            "inputs": {
                "C": _render_field(close_field),
                "core.prior_high20": _render_field(core["core.prior_high20"]),
                "core.atr20": _render_field(core["core.atr20"]),
                "core.clv": _render_field(core["core.clv"]),
            },
            "predicates": evaluation,
        }
    return result
