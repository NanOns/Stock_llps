"""Stock A研究history_preview职责模块。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
import re
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
    _validate_history,
    calculate_core,
)
from stock_a.research.breakout import evaluate_raw_breakout_predicate
from stock_a.sources.tdx.day import DAY_STRUCT, DecodedDay, UnitContract

from stock_a.research.entry_core.fields import (
    _apply_calendar_coverage_to_core,
    _render_field,
    _render_raw,
    _render_value,
)
from stock_a.research.entry_context.errors import ResearchEntryError
from stock_a.research.entry_core.profile_preview import (
    _build_profile_preview,
    _profile_preview_source_digest,
)
from stock_a.research.entry_context.sessions import _load_calendar, _parse_target
from stock_a.research.entry_context.source_discovery import _normalize_symbol
from stock_a.research.entry_history.support import _decoded_day_from_pg_raw


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
    core_source_sha256: str
    execution_adapter_source_sha256: str


def _calculate_core_d1_inputs(
    history: Sequence[CoreObservation],
    *,
    official_calendar: MarketSessionCalendar,
) -> dict[str, FieldValue]:
    """复用现役Core原语，只生成ALG-29消费的三字段。"""
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
            for name in ("core.prior_high20", "core.atr20", "core.clv")
        }

    prior_rows, prior_reason = _actual_window(history, 20, exclude_current=True)
    prior_highs, high_reason = (
        _extreme_column(prior_rows, "high")
        if not prior_reason
        else (None, prior_reason)
    )
    prior_high20 = _known(
        max(prior_highs, default=None) if prior_highs else None,
        prior_reason or high_reason,
    )

    rows, atr_reason = _actual_window(history, 21)
    if not atr_reason:
        atr_reason = _coordinate_error(rows)
    if not atr_reason:
        atr_reason = next(
            (error for row in rows if (error := _known_ohlc_error(row))), None
        )
    if not atr_reason:
        true_ranges = []
        for previous, current in zip(rows, rows[1:]):
            highs, high_reason = _extreme_column([current], "high")
            lows, low_reason = _extreme_column([current], "low")
            closes, close_reason = _column([previous], "close")
            dependency_error = high_reason or low_reason or close_reason
            if dependency_error:
                atr_reason = dependency_error
                break
            cur_high, cur_low, prev_close = highs[0], lows[0], closes[0]
            true_ranges.append(
                max(
                    cur_high - cur_low,
                    abs(cur_high - prev_close),
                    abs(cur_low - prev_close),
                )
            )
    atr20 = _known(mean(true_ranges), None) if not atr_reason else unknown(atr_reason)

    closes, close_reason = _column([latest], "close")
    highs, high_reason = _extreme_column([latest], "high")
    lows, low_reason = _extreme_column([latest], "low")
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
        "core.prior_high20": prior_high20,
        "core.atr20": atr20,
        "core.clv": clv,
    }


def prepare_source_d1_preview_context(
    target_sessions: Sequence[date],
) -> _SourceD1PreviewContext:
    """为一次D1请求准备精确日历和Core来源身份，不跨请求缓存。"""
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
    core_source_sha256 = _profile_preview_source_digest(
        Path(__file__).resolve().parents[1] / "core.py", "calculate_core"
    )
    return _SourceD1PreviewContext(
        calendars_by_year=calendars_by_year,
        core_source_sha256=core_source_sha256,
        execution_adapter_source_sha256=_profile_preview_source_digest(
            Path(__file__).resolve(), "_calculate_core_d1_inputs"
        ),
    )


def _source_d1_preview_for_target(
    *,
    target: date,
    sessions: tuple[date, ...],
    normalized: str,
    coordinate_id: str,
    rows_by_session: Mapping[date, Mapping[str, Any]],
    decoded_by_session: Mapping[date, DecodedDay],
    observations_by_session: Mapping[date, CoreObservation],
    context: _SourceD1PreviewContext,
) -> dict[str, Any]:
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
    history = tuple(observations_by_session[day] for day in expected_sessions)
    bounded_calendar = MarketSessionCalendar(
        calendar_id=calendar_payload["calendar_id"],
        revision=calendar_revision,
        sessions=expected_sessions,
    )
    core = _calculate_core_d1_inputs(history, official_calendar=bounded_calendar)
    _apply_calendar_coverage_to_core(
        core, history, calendar_metadata[-1]["coverage_context"]
    )
    target_row = rows_by_session.get(target)
    target_bar = decoded_by_session.get(target)
    close_field = (
        target_bar.raw_close
        if target_bar is not None
        else unknown(
            "UNKNOWN_HISTORY_SESSION_NOT_STORED"
            if target_row is None
            else "TARGET_BAR_MISSING"
        )
    )
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
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "price_coordinate": "RAW_UNADJUSTED",
        "target_bar_status": {
            "TARGET_PRESENT": "TDX_TARGET_BAR_PRESENT",
            "TARGET_MISSING": "TDX_TARGET_BAR_MISSING",
            "READ_FAILED": "TDX_READ_FAILED",
            "HISTORY_ROW_NOT_STORED": "HISTORY_ROW_NOT_STORED",
        }[target_row["bar_status"] if target_row else "HISTORY_ROW_NOT_STORED"],
        "raw_d1_breakout_predicate": {
            "scope": "SOURCE_SYMBOL_ONLY_RAW_UNADJUSTED_NON_PIT_RETROSPECTIVE",
            "target_session": target.isoformat(),
            "price_coordinate": "RAW_UNADJUSTED",
            "coordinate_id": coordinate_id,
            "core_producer_symbol": "stock_a.research.core:calculate_core",
            "core_producer_source_sha256": context.core_source_sha256,
            "core_producer_role": "SEMANTIC_CORE_CONTRACT_SOURCE",
            "execution_adapter_symbol": (
                "stock_a.research.entry_history.history_preview:"
                "_calculate_core_d1_inputs"
            ),
            "execution_adapter_source_sha256": (
                context.execution_adapter_source_sha256
            ),
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
    package_sha256: str,
    history_rows: Sequence[Mapping[str, Any]],
    namespace_id: str,
    context: _SourceD1PreviewContext,
) -> dict[date, dict[str, Any]]:
    """一次解码同源历史，只计算D1会话及真实D1事件所需的目标后继。"""
    events = tuple(event_sessions)
    sessions = tuple(session_dates)
    normalized = _normalize_symbol(symbol)[0]
    if (
        not isinstance(package_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", package_sha256) is None
        or not isinstance(namespace_id, str)
        or not namespace_id
    ):
        raise ResearchEntryError(
            "PG_HISTORY_BINDING_INVALID", "PG历史预览缺少包SHA或namespace身份"
        )
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
        or any(day not in sessions or day >= followup_session for day in events)
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
        or row.get("package_sha256") != package_sha256
        or row.get("namespace_id") != namespace_id
        for row in rows
    ):
        raise ResearchEntryError(
            "PG_HISTORY_REVISION_MISMATCH",
            "PG历史行混入不同来源代码、包SHA或namespace",
        )

    rows_by_session = {row["session_date"]: row for row in rows}
    decoded_by_session = {
        row["session_date"]: decoded
        for row in rows
        if (decoded := _decoded_day_from_pg_raw(row)) is not None
    }
    coordinate_id = f"RAW:TDX_SOURCE_SYMBOL:{normalized}"
    observations_by_session: dict[date, CoreObservation] = {}
    for day in sessions:
        row = rows_by_session.get(day)
        if row is None:
            observations_by_session[day] = CoreObservation(
                session=day,
                state="UNKNOWN",
                coordinate_id=coordinate_id,
                open=unknown("UNKNOWN_HISTORY_SESSION_NOT_STORED"),
                high=unknown("UNKNOWN_HISTORY_SESSION_NOT_STORED"),
                low=unknown("UNKNOWN_HISTORY_SESSION_NOT_STORED"),
                close=unknown("UNKNOWN_HISTORY_SESSION_NOT_STORED"),
                amount=unknown("UNIT_AUTHORITY_NOT_VERIFIED"),
                volume=unknown("UNIT_AUTHORITY_NOT_VERIFIED"),
            )
            continue
        state = row.get("bar_status")
        if state == "TARGET_PRESENT":
            decoded = decoded_by_session[day]
            observations_by_session[day] = CoreObservation(
                session=day,
                state="ACTUAL",
                coordinate_id=coordinate_id,
                open=decoded.raw_open,
                high=decoded.raw_high,
                low=decoded.raw_low,
                close=decoded.raw_close,
                amount=decoded.amount,
                volume=decoded.volume,
                native_amount=decoded.raw_amount,
                native_volume=decoded.raw_volume,
            )
        elif state in {"TARGET_MISSING", "READ_FAILED"}:
            reason = row.get("bar_reason") or (
                "TARGET_SESSION_BAR_MISSING"
                if state == "TARGET_MISSING"
                else "SOURCE_ENTRY_READ_FAILED"
            )
            observations_by_session[day] = CoreObservation(
                session=day,
                state="UNKNOWN",
                coordinate_id=coordinate_id,
                open=unknown(reason),
                high=unknown(reason),
                low=unknown(reason),
                close=unknown(reason),
                amount=unknown("UNIT_AUTHORITY_NOT_VERIFIED"),
                volume=unknown("UNIT_AUTHORITY_NOT_VERIFIED"),
            )
        else:
            raise ResearchEntryError(
                "PG_HISTORY_STATUS_INVALID", "PG历史行含未登记的Raw来源状态"
            )

    result = {
        day: _source_d1_preview_for_target(
            target=day,
            sessions=sessions,
            normalized=normalized,
            coordinate_id=coordinate_id,
            rows_by_session=rows_by_session,
            decoded_by_session=decoded_by_session,
            observations_by_session=observations_by_session,
            context=context,
        )
        for day in events
    }
    if any(
        preview["raw_d1_breakout_predicate"]["predicates"]["result"]["state"] == "TRUE"
        for preview in result.values()
    ):
        result[followup_session] = _source_d1_preview_for_target(
            target=followup_session,
            sessions=sessions,
            normalized=normalized,
            coordinate_id=coordinate_id,
            rows_by_session=rows_by_session,
            decoded_by_session=decoded_by_session,
            observations_by_session=observations_by_session,
            context=context,
        )
    return result


def research_stock_pg_history_preview(
    *,
    symbol: str,
    target_session: str,
    package_sha256: str,
    session_dates: Sequence[date],
    history_rows: Sequence[Mapping[str, Any]],
    missing_history_sessions: Sequence[date] | None = None,
    namespace_id: str,
    profile_contract: dict[str, Any],
    closed_period_bars: Mapping[str, Sequence[object]] | None = None,
    include_profile_preview: bool = True,
    include_raw_breakout: bool = False,
    allow_missing_target_row: bool = False,
) -> dict[str, Any]:
    """把明确PG Raw窗口适配至现役Core和Profile11研究预览。"""
    target = _parse_target(target_session)
    normalized = _normalize_symbol(symbol)[0]
    if (
        not isinstance(package_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", package_sha256) is None
        or not isinstance(namespace_id, str)
        or not namespace_id
    ):
        raise ResearchEntryError(
            "PG_HISTORY_BINDING_INVALID", "PG历史预览缺少包SHA或namespace身份"
        )
    calendar_payload, calendar_metadata, calendar_revision, all_calendar_sessions = (
        _load_calendar(target)
    )
    coverage_context = calendar_metadata[-1]["coverage_context"]
    expected_sessions = tuple(session_dates)
    if (
        not expected_sessions
        or len(expected_sessions) > 75
        or any(type(day) is not date for day in expected_sessions)
        or tuple(sorted(set(expected_sessions))) != expected_sessions
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

    rows = tuple(history_rows)
    row_dates = tuple(row.get("session_date") for row in rows)
    if (
        len(set(row_dates)) != len(row_dates)
        or any(day not in expected_sessions for day in row_dates)
        or row_dates != tuple(sorted(row_dates))
    ):
        raise ResearchEntryError(
            "PG_HISTORY_SESSION_ORDER_INVALID", "PG历史Raw行重复、越界或未按会话递增"
        )
    present_dates = set(row_dates)
    missing_dates = tuple(day for day in expected_sessions if day not in present_dates)
    if (
        missing_history_sessions is not None
        and tuple(missing_history_sessions) != missing_dates
    ):
        raise ResearchEntryError(
            "PG_HISTORY_MISSING_SESSION_BINDING_INVALID",
            "PG历史缺失会话清单与实际Raw行不一致",
        )
    if target not in present_dates and not allow_missing_target_row:
        raise ResearchEntryError(
            "PG_HISTORY_TARGET_ROW_MISSING", "PG历史Raw没有精确目标日行，拒绝跨日回退"
        )
    if any(
        row.get("source_symbol") != normalized
        or row.get("package_sha256") != package_sha256
        or row.get("namespace_id") != namespace_id
        for row in rows
    ):
        raise ResearchEntryError(
            "PG_HISTORY_REVISION_MISMATCH", "PG历史行混入不同来源代码、包SHA或namespace"
        )
    rows_by_session = {row["session_date"]: row for row in rows}
    target_row = rows_by_session.get(target)
    target_bar = _decoded_day_from_pg_raw(target_row) if target_row else None
    if target_row is None:
        target_row = {"bar_status": "HISTORY_ROW_NOT_STORED"}
    coordinate_id = f"RAW:TDX_SOURCE_SYMBOL:{normalized}"
    history: list[CoreObservation] = []
    status_counts = {
        "TARGET_PRESENT": 0,
        "TARGET_MISSING": 0,
        "READ_FAILED": 0,
        "HISTORY_ROW_NOT_STORED": 0,
    }
    price_quality_counts: dict[str, int] = {}
    raw_amount_quality_counts: dict[str, int] = {}
    raw_volume_quality_counts: dict[str, int] = {}
    for session in expected_sessions:
        row = rows_by_session.get(session)
        if row is None:
            status_counts["HISTORY_ROW_NOT_STORED"] += 1
            history.append(
                CoreObservation(
                    session=session,
                    state="UNKNOWN",
                    coordinate_id=coordinate_id,
                    open=unknown("UNKNOWN_HISTORY_SESSION_NOT_STORED"),
                    high=unknown("UNKNOWN_HISTORY_SESSION_NOT_STORED"),
                    low=unknown("UNKNOWN_HISTORY_SESSION_NOT_STORED"),
                    close=unknown("UNKNOWN_HISTORY_SESSION_NOT_STORED"),
                    amount=unknown("UNIT_AUTHORITY_NOT_VERIFIED"),
                    volume=unknown("UNIT_AUTHORITY_NOT_VERIFIED"),
                )
            )
            continue
        state = row.get("bar_status")
        status_counts[state] = status_counts.get(state, 0) + 1
        for counter, key in (
            (price_quality_counts, "price_quality"),
            (raw_amount_quality_counts, "raw_amount_quality"),
            (raw_volume_quality_counts, "raw_volume_quality"),
        ):
            quality = str(row.get(key) or "UNKNOWN")
            counter[quality] = counter.get(quality, 0) + 1
        if state == "TARGET_PRESENT":
            decoded = _decoded_day_from_pg_raw(row)
            assert decoded is not None
            history.append(
                CoreObservation(
                    session=row["session_date"],
                    state="ACTUAL",
                    coordinate_id=coordinate_id,
                    open=decoded.raw_open,
                    high=decoded.raw_high,
                    low=decoded.raw_low,
                    close=decoded.raw_close,
                    amount=decoded.amount,
                    volume=decoded.volume,
                    native_amount=decoded.raw_amount,
                    native_volume=decoded.raw_volume,
                )
            )
        elif state in {"TARGET_MISSING", "READ_FAILED"}:
            reason = row.get("bar_reason") or (
                "TARGET_SESSION_BAR_MISSING"
                if state == "TARGET_MISSING"
                else "SOURCE_ENTRY_READ_FAILED"
            )
            history.append(
                CoreObservation(
                    session=row["session_date"],
                    state="UNKNOWN",
                    open=unknown(reason),
                    high=unknown(reason),
                    low=unknown(reason),
                    close=unknown(reason),
                    amount=unknown("UNIT_AUTHORITY_NOT_VERIFIED"),
                    volume=unknown("UNIT_AUTHORITY_NOT_VERIFIED"),
                )
            )
        else:
            raise ResearchEntryError(
                "PG_HISTORY_STATUS_INVALID", "PG历史行含未登记的Raw来源状态"
            )
    if sum(status_counts.values()) != len(expected_sessions):
        raise ResearchEntryError(
            "PG_HISTORY_PARTITION_INCOMPLETE", "PG历史目标行状态分母未闭合"
        )

    bounded_calendar = MarketSessionCalendar(
        calendar_id=calendar_payload["calendar_id"],
        revision=calendar_revision,
        sessions=expected_sessions,
    )
    core = calculate_core(tuple(history), official_calendar=bounded_calendar)
    _apply_calendar_coverage_to_core(core, tuple(history), coverage_context)
    rendered_core = {name: _render_field(value) for name, value in sorted(core.items())}
    unknown_reasons: dict[str, int] = {}
    for value in core.values():
        if value.quality != "OBSERVED":
            reason = value.reason or "UNKNOWN_REASON_NOT_PROVIDED"
            unknown_reasons[reason] = unknown_reasons.get(reason, 0) + 1
    if len(core) != 43:
        raise ResearchEntryError(
            "PG_CORE_FIELD_COUNT_INVALID", f"Core现役字段数异常：{len(core)}"
        )

    entry_shas = sorted(
        {
            row.get("source_entry_sha256")
            for row in rows
            if row.get("source_entry_sha256")
        }
    )
    if len(entry_shas) > 1:
        raise ResearchEntryError(
            "PG_HISTORY_ENTRY_SHA_CONFLICT", "同包同来源代码的跨日条目SHA不一致"
        )
    captures_by_session = []
    for row in rows:
        captures_by_session.append(
            {
                "session_date": row["session_date"].isoformat(),
                "source_capture_id": row["source_capture_id"],
                "capture_digest": row["capture_digest"],
                "capture_read_started_at": row["capture_read_started_at"].isoformat(),
                "capture_validated_at": row["capture_validated_at"].isoformat(),
                "capture_normalized_count": row["capture_normalized_count"],
                "source_entry_sha256": row.get("source_entry_sha256"),
            }
        )

    source_snapshot_id = (
        f"STOCK_A_PG_RAW_HISTORY:{namespace_id}:{package_sha256}:{target.isoformat()}"
    )
    profile_preview = None
    if include_profile_preview:
        profile_preview = _build_profile_preview(
            symbol=normalized,
            target_session=target,
            source_last_session=target,
            source_sha256=package_sha256,
            target_bar=target_bar,
            history=tuple(history),
            core=core,
            calendar_revision=calendar_revision,
            contract=profile_contract,
            closed_period_bars=closed_period_bars,
            source_snapshot_id=source_snapshot_id,
            source_snapshot_role="PG_CAPTURE_HISTORY_ID_NOT_CAPTURED_AVAILABLE_AT",
        )
        profile_components = profile_preview.get("components", [])
        if len(profile_components) != 11:
            raise ResearchEntryError(
                "PG_PROFILE_RULE_COUNT_INVALID", "Profile现役规则数异常"
            )
    actual_bars = status_counts["TARGET_PRESENT"]
    gaps = sum(status_counts.values()) - status_counts["TARGET_PRESENT"]
    target_bar_status = {
        "TARGET_PRESENT": "TDX_TARGET_BAR_PRESENT",
        "TARGET_MISSING": "TDX_TARGET_BAR_MISSING",
        "READ_FAILED": "TDX_READ_FAILED",
        "HISTORY_ROW_NOT_STORED": "HISTORY_ROW_NOT_STORED",
    }[target_row["bar_status"]]
    price_window_sessions = expected_sessions[-21:]
    ma20_window_sessions = expected_sessions[-20:]

    def has_valid_price_bar(session: date) -> bool:
        row = rows_by_session.get(session)
        return bool(
            row is not None
            and row.get("bar_status") == "TARGET_PRESENT"
            and row.get("price_quality") == "OBSERVED"
            and all(
                row.get(name) is not None
                for name in ("raw_open", "raw_high", "raw_low", "raw_close")
            )
        )

    ma20_actual_bars = sum(has_valid_price_bar(day) for day in ma20_window_sessions)
    atr20_actual_bars = sum(has_valid_price_bar(day) for day in price_window_sessions)
    core_ma20 = core.get("core.ma20")
    core_atr20 = core.get("core.atr20")
    price_window_usable = bool(
        core_ma20 is not None
        and core_atr20 is not None
        and core_ma20.quality == "OBSERVED"
        and core_atr20.quality == "OBSERVED"
    )
    if price_window_usable:
        price_window_status = "USABLE"
        price_window_reason = None
    elif target_row["bar_status"] == "TARGET_MISSING":
        price_window_status = "TARGET_MISSING"
        price_window_reason = (
            target_row.get("bar_reason") or "TARGET_SESSION_BAR_MISSING"
        )
    elif target_row["bar_status"] == "READ_FAILED":
        price_window_status = "READ_FAILED"
        price_window_reason = target_row.get("bar_reason") or "SOURCE_ENTRY_READ_FAILED"
    elif any(day not in rows_by_session for day in price_window_sessions):
        price_window_status = "HISTORY_ROW_NOT_STORED"
        price_window_reason = "UNKNOWN_HISTORY_SESSION_NOT_STORED"
    elif ma20_actual_bars < 20 or atr20_actual_bars < 21:
        price_window_status = "INSUFFICIENT_VALID_BARS"
        price_window_reason = (
            core_atr20.reason
            if core_atr20 is not None and core_atr20.reason
            else core_ma20.reason
            if core_ma20 is not None
            else "PRICE_WINDOW_UNAVAILABLE"
        )
    else:
        price_window_status = "PRICE_FIELDS_UNKNOWN"
        price_window_reason = (
            core_atr20.reason
            if core_atr20 is not None and core_atr20.reason
            else core_ma20.reason
            if core_ma20 is not None
            else "PRICE_WINDOW_UNAVAILABLE"
        )
    history_records_complete = not missing_dates
    history_records_status = (
        "RECORDS_COMPLETE" if history_records_complete else "HISTORY_ROW_NOT_STORED"
    )
    result = {
        "ok": True,
        "contract": "STOCK_A_PG_SOURCE_SYMBOL_CORE_PROFILE_PREVIEW_R1",
        "status": "RESEARCH_PREVIEW_NON_PIT",
        "symbol": normalized,
        "target_session": target.isoformat(),
        "research_mode": "SOURCE_SYMBOL_NON_PIT_PREVIEW",
        "price_coordinate": "RAW_UNADJUSTED",
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "source_object_status": "TDX_SOURCE_SYMBOL_OBSERVED",
        "target_bar_status": target_bar_status,
        "price_research_status": "AVAILABLE"
        if target_row.get("price_quality") == "OBSERVED"
        else "UNKNOWN_DATA",
        "stable_security_id_status": "STABLE_SECURITY_ID_UNVERIFIED",
        "listing_status": "LISTING_STATUS_UNKNOWN",
        "suspension_status": "SUSPENSION_STATUS_UNKNOWN",
        "official_listing_universe_status": "UNKNOWN",
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "history_status": history_records_status,
        "history_records_complete": history_records_complete,
        "history_records_status": history_records_status,
        "price_window_usable": price_window_usable,
        "price_window_status": price_window_status,
        "price_window_reason": price_window_reason,
        "price_window_valid_bar_count": atr20_actual_bars,
        "price_window_expected_bar_count": len(price_window_sessions),
        "ma20_window_valid_bar_count": ma20_actual_bars,
        "ma20_window_expected_bar_count": len(ma20_window_sessions),
        "missing_history_sessions": [day.isoformat() for day in missing_dates],
        "source": {
            "storage": "POSTGRESQL_TYPED_SOURCE_RAW",
            "namespace_id": namespace_id,
            "package_sha256": package_sha256,
            "dataset_id": target_row.get("dataset_id"),
            "source_content_id": target_row.get("source_content_id"),
            "source_entry_sha256": entry_shas[0] if entry_shas else None,
            "target_source_capture_id": target_row.get("source_capture_id"),
            "source_clock_role": target_row.get("capture_clock_role"),
            "source_time_quality": target_row.get("capture_time_quality"),
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
            "missing_history_sessions": [day.isoformat() for day in missing_dates],
            "history_complete": history_records_complete,
            "history_status": history_records_status,
            "history_records_complete": history_records_complete,
            "history_records_status": history_records_status,
            "price_window_usable": price_window_usable,
            "price_window_status": price_window_status,
            "price_window_reason": price_window_reason,
            "price_window_valid_bar_count": atr20_actual_bars,
            "price_window_expected_bar_count": len(price_window_sessions),
            "ma20_window_valid_bar_count": ma20_actual_bars,
            "ma20_window_expected_bar_count": len(ma20_window_sessions),
            "real_bar_count": actual_bars,
            "gap_count": gaps,
            "source_bar_gap_count": status_counts["TARGET_MISSING"]
            + status_counts["READ_FAILED"],
            "unavailable_history_row_count": status_counts["HISTORY_ROW_NOT_STORED"],
            "session_status_counts": status_counts,
            "unknown_session_dates": [
                session.isoformat()
                for session in expected_sessions
                if session not in present_dates
                or rows_by_session[session]["bar_status"] != "TARGET_PRESENT"
            ],
            "future_session_rows_used": 0,
        },
        "raw": _render_raw(
            target_bar,
            UnitContract("TDX_RAW_SOURCE_VALUES_UNIT_UNVERIFIED_R1", None, None),
        ),
        "quality": {
            "price_quality_counts": dict(sorted(price_quality_counts.items())),
            "raw_amount_quality_counts": dict(
                sorted(raw_amount_quality_counts.items())
            ),
            "raw_volume_quality_counts": dict(
                sorted(raw_volume_quality_counts.items())
            ),
            "amount_unit_status": "UNIT_UNVERIFIED",
            "volume_unit_status": "UNIT_UNVERIFIED",
            "unit_dependent_core_reason": "UNIT_AUTHORITY_NOT_VERIFIED",
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
        "execution": {
            "source_zip_opened": False,
            "source_scan_performed": False,
            "source_recalculated": False,
            "ddl_executed": False,
            "database_write": False,
            "network_access": False,
        },
    }
    if include_raw_breakout:
        close_field = (
            target_bar.raw_close
            if target_bar is not None
            else unknown(
                "UNKNOWN_HISTORY_SESSION_NOT_STORED"
                if target not in present_dates
                else "TARGET_BAR_MISSING"
            )
        )
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
            "scope": "SOURCE_SYMBOL_ONLY_RAW_UNADJUSTED_NON_PIT_RETROSPECTIVE",
            "target_session": target.isoformat(),
            "price_coordinate": "RAW_UNADJUSTED",
            "coordinate_id": coordinate_id,
            "core_producer_symbol": "stock_a.research.core:calculate_core",
            "core_producer_source_sha256": _profile_preview_source_digest(
                Path(__file__).resolve().parents[1] / "core.py", "calculate_core"
            ),
            "core_producer_role": "SEMANTIC_CORE_CONTRACT_SOURCE",
            "execution_adapter_symbol": (
                "stock_a.research.entry_history.history_preview:"
                "_calculate_core_d1_inputs"
            ),
            "execution_adapter_source_sha256": _profile_preview_source_digest(
                Path(__file__).resolve(), "_calculate_core_d1_inputs"
            ),
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
        }
    return result
