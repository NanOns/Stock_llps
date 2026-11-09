"""以只读Raw历史接通现役D1来源结构生成与后继观察。"""

from __future__ import annotations

from collections import Counter
from datetime import date
from fractions import Fraction
import re
import time
from typing import Any, Mapping, Sequence

from stock_a.application.pg_research import (
    PgResearchBindingError,
    read_source_version_context,
)
from stock_a.domain import FieldValue, exact_number, unknown
from stock_a.research.core import (
    CoreObservation,
    MarketSessionCalendar,
    calculate_core,
)
from stock_a.research.entry import _load_calendar
from stock_a.research.entry_core.fields import _render_field, _render_value
from stock_a.research.entry_history.support import _decoded_day_from_pg_raw
from stock_a.research.facts import boolean
from stock_a.research.structure import (
    Anchor,
    Bar,
    CONTRACT as STRUCTURE_CONTRACT,
    gap_fill,
    generate,
    observe_support,
)
from stock_a.storage.postgres.source_raw import read_raw_daily_history_page

STRUCTURE_TYPES = (
    "PIVOT_LOW",
    "PIVOT_HIGH",
    "RANGE_FLOOR",
    "RANGE_CEILING",
    "GAP_ZONE",
)
_RANGE_TYPES = {"RANGE_FLOOR", "RANGE_CEILING"}
_SYMBOL = re.compile(r"(?:SH|SZ)\.[0-9]{6}")


def _field_from_rendered(value: Mapping[str, Any] | None, label: str) -> FieldValue:
    if not isinstance(value, Mapping):
        return unknown(f"{label.upper().replace('.', '_')}_NOT_PROVIDED")
    quality = value.get("quality")
    if quality == "OBSERVED":
        raw = value.get("value")
        if isinstance(raw, Mapping):
            raw = raw.get("exact")
        try:
            if isinstance(raw, bool) or raw is None:
                raise ValueError("非精确数值")
            return FieldValue(Fraction(str(raw)))
        except (TypeError, ValueError, ZeroDivisionError) as error:
            raise PgResearchBindingError(
                "D1_STRUCTURE_CORE_VALUE_INVALID",
                f"现役Core字段{label}不是精确有限数值",
            ) from error
    reason = value.get("reason")
    if not isinstance(reason, str) or not reason:
        reason = f"{label.upper().replace('.', '_')}_QUALITY_UNKNOWN"
    if quality == "NOT_APPLICABLE":
        return FieldValue(None, "NOT_APPLICABLE", reason)
    return unknown(reason)


def _valid_ohlc(values: Sequence[Any]) -> tuple[Fraction, ...] | None:
    try:
        o, h, low, close = tuple(exact_number(value) for value in values)
    except (TypeError, ValueError, ZeroDivisionError):
        return None
    if min(o, h, low, close) <= 0 or not low <= min(o, close) <= max(o, close) <= h:
        return None
    return o, h, low, close


def _history_observations(
    *,
    symbol: str,
    sessions: Sequence[date],
    history_rows: Sequence[Mapping[str, Any]],
    namespace_id: str,
    package_sha256: str,
) -> tuple[tuple[CoreObservation, ...], tuple[Bar, ...], dict[date, Mapping[str, Any]]]:
    normalized = symbol.strip().upper()
    if _SYMBOL.fullmatch(normalized) is None:
        raise PgResearchBindingError(
            "D1_STRUCTURE_SYMBOL_INVALID", "D1结构来源代码必须是SH./SZ.六位代码"
        )
    days = tuple(sessions)
    if (
        not days
        or tuple(sorted(set(days))) != days
        or any(type(day) is not date for day in days)
    ):
        raise PgResearchBindingError(
            "D1_STRUCTURE_SESSION_WINDOW_INVALID", "D1结构历史必须为唯一递增官方会话"
        )
    row_map: dict[date, Mapping[str, Any]] = {}
    for row in history_rows:
        day = row.get("session_date")
        if (
            type(day) is not date
            or day not in days
            or day in row_map
            or row.get("source_symbol") != normalized
            or row.get("namespace_id") != namespace_id
            or row.get("package_sha256") != package_sha256
        ):
            raise PgResearchBindingError(
                "D1_STRUCTURE_RAW_BINDING_INVALID",
                "D1结构历史混入错日、错代码、错namespace或错包",
            )
        row_map[day] = row

    coordinate_id = f"RAW:TDX_SOURCE_SYMBOL:{normalized}"
    observations: list[CoreObservation] = []
    bars: list[Bar] = []
    for ordinal, day in enumerate(days):
        row = row_map.get(day)
        decoded = _decoded_day_from_pg_raw(row) if row is not None else None
        values = (
            _valid_ohlc(decoded.prices.value)
            if decoded is not None and decoded.prices.quality == "OBSERVED"
            else None
        )
        if values is not None:
            o, high, low, close = values
            observations.append(
                CoreObservation(
                    session=day,
                    state="ACTUAL",
                    coordinate_id=coordinate_id,
                    open=FieldValue(o),
                    high=FieldValue(high),
                    low=FieldValue(low),
                    close=FieldValue(close),
                    amount=decoded.amount,
                    volume=decoded.volume,
                    native_amount=decoded.raw_amount,
                    native_volume=decoded.raw_volume,
                )
            )
            bars.append(Bar(ordinal, FieldValue(values)))
            continue

        reason = (
            "DATA_INVALID_OHLC"
            if decoded is not None and decoded.prices.quality == "OBSERVED"
            else decoded.prices.reason
            if decoded is not None
            else row.get("bar_reason") or row.get("bar_status")
            if row is not None
            else "UNKNOWN_HISTORY_SESSION_NOT_STORED"
        )
        observations.append(
            CoreObservation(
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
        )
        bars.append(Bar(ordinal, unknown(reason)))
    return tuple(observations), tuple(bars), row_map


def _core_at(
    observations: Sequence[CoreObservation],
    sessions: Sequence[date],
    *,
    end_index: int,
    calendar_id: str,
    calendar_revision: str,
) -> dict[str, FieldValue]:
    prefix = tuple(sessions[: end_index + 1])
    return calculate_core(
        tuple(observations[: end_index + 1]),
        official_calendar=MarketSessionCalendar(
            calendar_id=calendar_id,
            revision=calendar_revision,
            sessions=prefix,
        ),
    )


def _actual_field(observation: CoreObservation) -> FieldValue:
    return (
        FieldValue(True)
        if observation.state == "ACTUAL"
        else unknown("UNKNOWN_ACTUAL_BAR_OR_PRICE_QUALITY")
    )


def _source_row_identity(row: Mapping[str, Any] | None) -> dict[str, Any]:
    if row is None:
        return {"status": "UNKNOWN_HISTORY_SESSION_NOT_STORED"}
    return {
        "session": row["session_date"].isoformat(),
        "bar_status": row.get("bar_status"),
        "price_quality": row.get("price_quality"),
        "namespace_id": row.get("namespace_id"),
        "package_sha256": row.get("package_sha256"),
        "source_capture_id": str(row["source_capture_id"])
        if row.get("source_capture_id") is not None
        else None,
        "source_content_id": row.get("source_content_id"),
        "source_entry_sha256": row.get("source_entry_sha256"),
        "source_clock_role": row.get("source_clock_role"),
        "source_time_quality": row.get("source_time_quality"),
        "source_available_at": row.get("source_available_at"),
        "observed_started_at": row.get("observed_started_at"),
        "observed_completed_at": row.get("observed_completed_at"),
    }


def _bar_values(bar: Bar) -> dict[str, Any]:
    if bar.ohlc.quality != "OBSERVED":
        return {"quality": bar.ohlc.quality, "reason": bar.ohlc.reason}
    return {
        "quality": "OBSERVED",
        "open": _render_value(bar.ohlc.value[0]),
        "high": _render_value(bar.ohlc.value[1]),
        "low": _render_value(bar.ohlc.value[2]),
        "close": _render_value(bar.ohlc.value[3]),
    }


def _generation_evidence(
    *,
    kind: str,
    index: int,
    sessions: Sequence[date],
    bars: Sequence[Bar],
    row_map: Mapping[date, Mapping[str, Any]],
    core_previous: Mapping[str, FieldValue] | None,
) -> dict[str, Any]:
    def bar_ref(position: int) -> dict[str, Any]:
        return {
            "ordinal": position,
            "bar": _bar_values(bars[position]),
            "source": _source_row_identity(row_map.get(sessions[position])),
        }

    result: dict[str, Any] = {
        "target": bar_ref(index),
        "price_coordinate": "RAW_UNADJUSTED",
        "identity_status": "SOURCE_SYMBOL_ONLY",
    }
    if kind.startswith("PIVOT_"):
        start = max(0, index - 4)
        result["pivot_five_session_window"] = [
            bar_ref(position) for position in range(start, index + 1)
        ]
        result["required_actual_bars"] = 5
        result["right_confirmation_session"] = sessions[index].isoformat()
    elif kind in _RANGE_TYPES:
        start = max(0, index - 20)
        result["strict_prior_twenty_window"] = [
            bar_ref(position) for position in range(start, index)
        ]
        if core_previous is not None:
            result["core_previous_inputs"] = {
                "core.atr20": _render_field(core_previous["core.atr20"]),
                "core.ma_slope20_atr": _render_field(
                    core_previous["core.ma_slope20_atr"]
                ),
            }
        else:
            result["core_previous_inputs"] = {
                "core.atr20": _render_field(unknown("PREVIOUS_CORE_UNAVAILABLE")),
                "core.ma_slope20_atr": _render_field(
                    unknown("PREVIOUS_CORE_UNAVAILABLE")
                ),
            }
        result["required_actual_bars"] = 20
    elif kind == "GAP_ZONE" and index > 0:
        result["previous_official_session"] = bar_ref(index - 1)
        result["intervening_session_status"] = "ADJACENT_OFFICIAL_SESSIONS_ONLY"
    return result


def _anchor_payload(anchor: Anchor, sessions: Sequence[date]) -> dict[str, Any]:
    return {
        "anchor_id": anchor.anchor_id,
        "type": anchor.kind,
        "relation": anchor.relation,
        "created_session": sessions[anchor.created_ordinal].isoformat(),
        "available_from_session": sessions[anchor.available_from].isoformat(),
        "test_from_session": (
            sessions[anchor.test_from].isoformat()
            if anchor.test_from < len(sessions)
            else None
        ),
        "test_from_in_request_window": anchor.test_from < len(sessions),
        "coordinate_id": anchor.coordinate_id,
        "price_coordinate": "RAW_UNADJUSTED",
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "lower": _render_value(anchor.lower),
        "upper": _render_value(anchor.upper),
        "source_event_id": anchor.source_event_id,
        "source_ref": anchor.source_ref,
        "research_mode": "NON_PIT_RETROSPECTIVE",
    }


def _evaluate_generated(
    value: FieldValue,
    *,
    kind: str,
    prior_range_state: FieldValue | None = None,
    prior_range_anchor: Anchor | None = None,
) -> dict[str, Any]:
    if value.quality == "NOT_APPLICABLE":
        return {
            "state": "NOT_GENERATED",
            "reason": value.reason,
            "anchor": None,
        }
    if value.quality != "OBSERVED" or not isinstance(value.value, Anchor):
        return {
            "state": "UNKNOWN",
            "reason": value.reason or "STRUCTURE_GENERATION_UNKNOWN",
            "anchor": None,
        }
    anchor = value.value
    if kind in _RANGE_TYPES:
        if prior_range_state is None or prior_range_state.quality != "OBSERVED":
            return {
                "state": "UNKNOWN",
                "reason": prior_range_state.reason
                if prior_range_state is not None
                else "RANGE_PRIOR_STATE_UNAVAILABLE",
                "anchor": None,
            }
        if prior_range_state.value is True and prior_range_anchor is None:
            return {
                "state": "UNKNOWN",
                "reason": "RANGE_PRIOR_ANCHOR_LEFT_CENSORED",
                "anchor": None,
            }
        if (
            prior_range_state.value is True
            and prior_range_anchor is not None
            and (prior_range_anchor.lower, prior_range_anchor.upper)
            == (anchor.lower, anchor.upper)
        ):
            return {
                "state": "NOT_GENERATED",
                "reason": "RANGE_ANCHOR_REUSED_UNCHANGED_BOUNDS",
                "anchor": None,
            }
    return {"state": "GENERATED", "reason": None, "anchor": anchor}


def _generate_day(
    *,
    symbol: str,
    target_index: int,
    sessions: Sequence[date],
    observations: Sequence[CoreObservation],
    bars: Sequence[Bar],
    row_map: Mapping[date, Mapping[str, Any]],
    namespace_id: str,
    package_sha256: str,
    calendar_id: str,
    calendar_revision: str,
    core_previous: Mapping[str, FieldValue] | None,
    prior_range_results: Mapping[str, dict[str, Any]] | None = None,
    prior_range_states: Mapping[str, FieldValue] | None = None,
    prior_range_anchors: Mapping[str, Anchor | None] | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any], dict[str, Anchor]]:
    day = sessions[target_index]
    coordinate_id = f"RAW:TDX_SOURCE_SYMBOL:{symbol}"
    window_ref = (
        f"{namespace_id}:{package_sha256}:{symbol}:"
        f"{sessions[0].isoformat()}:{day.isoformat()}"
    )
    source_ref = f"stock.source_raw_daily:{namespace_id}:{package_sha256}:{symbol}"
    statuses = tuple(
        (position, "ACTUAL" if item.state == "ACTUAL" else "UNKNOWN")
        for position, item in enumerate(observations[: target_index + 1])
    )
    pivot5 = tuple(bars[max(0, target_index - 4) : target_index + 1])
    prior20 = tuple(bars[max(0, target_index - 20) : target_index])
    immediate_prior = tuple(bars[max(0, target_index - 1) : target_index])
    core_fields = core_previous or {}
    range_inputs = {
        "atr_previous": core_fields.get(
            "core.atr20", unknown("PREVIOUS_CORE_UNAVAILABLE")
        ),
        "slope_previous": core_fields.get(
            "core.ma_slope20_atr", unknown("PREVIOUS_CORE_UNAVAILABLE")
        ),
    }
    actual = _actual_field(observations[target_index])
    previous_values = prior_range_results or {}
    previous_states = prior_range_states or {}
    previous_anchors = prior_range_anchors or {}
    results: dict[str, dict[str, Any]] = {}
    anchor_models: dict[str, Anchor] = {}
    for kind in STRUCTURE_TYPES:
        if kind == "GAP_ZONE" and target_index == 0:
            results[kind] = {
                "state": "UNKNOWN",
                "reason": "GAP_PREVIOUS_OFFICIAL_SESSION_UNAVAILABLE",
                "anchor": None,
                "generation_evidence": _generation_evidence(
                    kind=kind,
                    index=target_index,
                    sessions=sessions,
                    bars=bars,
                    row_map=row_map,
                    core_previous=core_previous,
                ),
            }
            continue
        value = generate(
            kind,
            entity_id=f"SOURCE_SYMBOL_ONLY:{namespace_id}:{symbol}",
            ordinal=target_index,
            coordinate_id=coordinate_id,
            source_ref=source_ref,
            window_ref=window_ref,
            actual=actual,
            today=bars[target_index],
            inputs=range_inputs if kind in _RANGE_TYPES else {},
            prior20=immediate_prior if kind == "GAP_ZONE" else prior20,
            pivot5=pivot5,
            statuses=statuses,
            previous_anchor=(
                previous_anchors.get(kind) if kind in _RANGE_TYPES else None
            ),
            prior_qualified=(
                previous_states.get(kind, unknown("RANGE_PRIOR_STATE_UNAVAILABLE"))
                if kind in _RANGE_TYPES
                else None
            ),
            capability_state="READY",
        )
        prior_state = previous_states.get(kind) if kind in _RANGE_TYPES else None
        prior_anchor = previous_anchors.get(kind) if kind in _RANGE_TYPES else None
        classified = _evaluate_generated(
            value,
            kind=kind,
            prior_range_state=prior_state,
            prior_range_anchor=prior_anchor,
        )
        anchor = classified["anchor"]
        if isinstance(anchor, Anchor):
            anchor_models[kind] = anchor
        results[kind] = {
            **classified,
            "anchor": _anchor_payload(anchor, sessions) if anchor else None,
            "generation_evidence": _generation_evidence(
                kind=kind,
                index=target_index,
                sessions=sessions,
                bars=bars,
                row_map=row_map,
                core_previous=core_previous,
            ),
        }
    return (
        results,
        {
            "prior_range_results": previous_values,
            "source_row": _source_row_identity(row_map.get(day)),
        },
        anchor_models,
    )


def _range_candidates_for_previous_session(
    *,
    symbol: str,
    target_index: int,
    sessions: Sequence[date],
    observations: Sequence[CoreObservation],
    bars: Sequence[Bar],
    namespace_id: str,
    package_sha256: str,
    calendar_id: str,
    calendar_revision: str,
    core_previous: Mapping[str, FieldValue] | None,
) -> tuple[dict[str, dict[str, Any]], dict[str, FieldValue], dict[str, Anchor | None]]:
    results: dict[str, dict[str, Any]] = {}
    states: dict[str, FieldValue] = {}
    anchors: dict[str, Anchor | None] = {}
    if target_index <= 0:
        return (
            {
                kind: {"state": "UNKNOWN", "reason": "RANGE_PRIOR_SESSION_UNAVAILABLE"}
                for kind in _RANGE_TYPES
            },
            {kind: unknown("RANGE_PRIOR_SESSION_UNAVAILABLE") for kind in _RANGE_TYPES},
            {kind: None for kind in _RANGE_TYPES},
        )
    previous_index = target_index - 1
    day = sessions[previous_index]
    coordinate_id = f"RAW:TDX_SOURCE_SYMBOL:{symbol}"
    source_ref = f"stock.source_raw_daily:{namespace_id}:{package_sha256}:{symbol}"
    window_ref = (
        f"{namespace_id}:{package_sha256}:{symbol}:"
        f"{sessions[0].isoformat()}:{day.isoformat()}"
    )
    statuses = tuple(
        (position, "ACTUAL" if item.state == "ACTUAL" else "UNKNOWN")
        for position, item in enumerate(observations[: previous_index + 1])
    )
    prior20 = tuple(bars[max(0, previous_index - 20) : previous_index])
    actual = _actual_field(observations[previous_index])
    for kind in sorted(_RANGE_TYPES):
        inputs = {
            "atr_previous": (core_previous or {}).get(
                "core.atr20", unknown("PREVIOUS_CORE_UNAVAILABLE")
            ),
            "slope_previous": (core_previous or {}).get(
                "core.ma_slope20_atr", unknown("PREVIOUS_CORE_UNAVAILABLE")
            ),
        }
        value = generate(
            kind,
            entity_id=f"SOURCE_SYMBOL_ONLY:{namespace_id}:{symbol}",
            ordinal=previous_index,
            coordinate_id=coordinate_id,
            source_ref=source_ref,
            window_ref=window_ref,
            actual=actual,
            today=bars[previous_index],
            inputs=inputs,
            prior20=prior20,
            statuses=statuses,
            prior_qualified=unknown("PREVIOUS_RANGE_HISTORY_NOT_RECONSTRUCTED"),
            capability_state="READY",
        )
        if value.quality == "OBSERVED" and isinstance(value.value, Anchor):
            results[kind] = {"state": "TRUE", "anchor": value.value}
            states[kind] = FieldValue(True)
            anchors[kind] = value.value
        elif value.quality == "NOT_APPLICABLE":
            results[kind] = {"state": "FALSE", "reason": value.reason}
            states[kind] = FieldValue(False)
            anchors[kind] = None
        else:
            results[kind] = {
                "state": "UNKNOWN",
                "reason": value.reason or "RANGE_PREVIOUS_QUALIFICATION_UNKNOWN",
            }
            states[kind] = unknown(results[kind]["reason"])
            anchors[kind] = None
    return results, states, anchors


def _pool_candidate_result(
    *,
    candidate: Mapping[str, Any],
    sessions: Sequence[date],
    target_index: int,
    namespace_id: str,
    package_sha256: str,
    calendar_id: str,
    calendar_revision: str,
) -> tuple[dict[str, dict[str, Any]], int, int]:
    symbol = candidate["source_symbol"]
    observations, bars, row_map = _history_observations(
        symbol=symbol,
        sessions=sessions,
        history_rows=candidate["history_rows"],
        namespace_id=namespace_id,
        package_sha256=package_sha256,
    )
    previous_core = (
        _core_at(
            observations,
            sessions,
            end_index=target_index - 1,
            calendar_id=calendar_id,
            calendar_revision=calendar_revision,
        )
        if target_index >= 1
        else None
    )
    prior_previous_core = (
        _core_at(
            observations,
            sessions,
            end_index=target_index - 2,
            calendar_id=calendar_id,
            calendar_revision=calendar_revision,
        )
        if target_index >= 2
        else None
    )
    two_sessions_prior_core = (
        _core_at(
            observations,
            sessions,
            end_index=target_index - 3,
            calendar_id=calendar_id,
            calendar_revision=calendar_revision,
        )
        if target_index >= 3
        else None
    )
    prior_range_results, prior_range_states, prior_range_anchors = (
        _range_candidates_for_previous_session(
            symbol=symbol,
            target_index=target_index,
            sessions=sessions,
            observations=observations,
            bars=bars,
            namespace_id=namespace_id,
            package_sha256=package_sha256,
            calendar_id=calendar_id,
            calendar_revision=calendar_revision,
            core_previous=prior_previous_core,
        )
    )
    _older_range_results, older_range_states, older_range_anchors = (
        _range_candidates_for_previous_session(
            symbol=symbol,
            target_index=target_index - 1,
            sessions=sessions,
            observations=observations,
            bars=bars,
            namespace_id=namespace_id,
            package_sha256=package_sha256,
            calendar_id=calendar_id,
            calendar_revision=calendar_revision,
            core_previous=two_sessions_prior_core,
        )
    )
    for kind in _RANGE_TYPES:
        if (
            prior_range_states[kind].quality == "OBSERVED"
            and prior_range_states[kind].value is True
        ):
            prior_anchor = prior_range_anchors[kind]
            older_state = older_range_states[kind]
            older_anchor = older_range_anchors[kind]
            known_origin = (
                older_state.quality == "OBSERVED" and older_state.value is False
            ) or (
                older_state.quality == "OBSERVED"
                and older_state.value is True
                and prior_anchor is not None
                and older_anchor is not None
                and (prior_anchor.lower, prior_anchor.upper)
                != (older_anchor.lower, older_anchor.upper)
            )
            if not known_origin:
                prior_range_states[kind] = unknown("RANGE_ANCHOR_LEFT_CENSORED")
                prior_range_anchors[kind] = None
    results, _, _ = _generate_day(
        symbol=symbol,
        target_index=target_index,
        sessions=sessions,
        observations=observations,
        bars=bars,
        row_map=row_map,
        namespace_id=namespace_id,
        package_sha256=package_sha256,
        calendar_id=calendar_id,
        calendar_revision=calendar_revision,
        core_previous=previous_core,
        prior_range_results=prior_range_results,
        prior_range_states=prior_range_states,
        prior_range_anchors=prior_range_anchors,
    )
    # A prior UNKNOWN cannot prove a new range event; a known current FALSE remains FALSE.
    for kind in _RANGE_TYPES:
        if (
            prior_range_states[kind].quality != "OBSERVED"
            and results[kind]["state"] != "NOT_GENERATED"
        ):
            results[kind] = {
                **results[kind],
                "state": "UNKNOWN",
                "reason": prior_range_states[kind].reason,
                "anchor": None,
            }
    return (
        results,
        len(candidate["history_rows"]),
        int(target_index >= 1) + int(target_index >= 2) + int(target_index >= 3),
    )


def discover_source_d1_structures(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    sessions: Sequence[date],
    package_sha256: str,
    offset: int,
    limit: int,
    structure_symbol: str | None = None,
) -> dict[str, Any]:
    """一遍完整扫描目标日Raw池，统计五种D1结构并按事件分页。"""
    started_wall = time.perf_counter()
    started_cpu = time.process_time()
    days = tuple(sessions)
    if (
        not days
        or days[-1] != target_session
        or target_session > research_cutoff
        or tuple(sorted(set(days))) != days
        or len(days) > 75
        or type(offset) is not int
        or offset < 0
        or type(limit) is not int
        or not 1 <= limit <= 100
        or not isinstance(package_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", package_sha256) is None
    ):
        raise PgResearchBindingError(
            "D1_STRUCTURE_REQUEST_INVALID", "D1结构全池请求的窗口、版本或分页无效"
        )
    if structure_symbol is not None:
        if (
            not isinstance(structure_symbol, str)
            or _SYMBOL.fullmatch(structure_symbol.strip().upper()) is None
        ):
            raise PgResearchBindingError(
                "D1_STRUCTURE_SYMBOL_INVALID", "结构下钻代码必须是SH./SZ.六位代码"
            )
        structure_symbol = structure_symbol.strip().upper()

    calendar_payload, calendar_metadata, calendar_revision, _ = _load_calendar(
        target_session
    )
    calendar_meta = next(
        item for item in calendar_metadata if item["year"] == target_session.year
    )
    calendar_id = calendar_payload["calendar_id"]
    target_calendar_revision = calendar_revision
    target_calendar_sha256 = calendar_meta["resource_sha256"]
    target_index = len(days) - 1
    counts = {
        kind: {"GENERATED": 0, "NOT_GENERATED": 0, "UNKNOWN": 0}
        for kind in STRUCTURE_TYPES
    }
    reason_counts = {
        kind: {"not_generated": Counter(), "unknown": Counter()}
        for kind in STRUCTURE_TYPES
    }
    events: list[dict[str, Any]] = []
    selected_detail = None
    seen_symbols: set[str] = set()
    last_symbol = None
    source_context = None
    source_pool_total = None
    source_offset = 0
    source_page_count = 0
    source_context_read_count = 0
    history_rows_decoded = 0
    core_calculations = 0
    generation_calls = 0

    while True:
        page = read_raw_daily_history_page(
            config,
            target_session=target_session,
            package_sha256=package_sha256,
            sessions=days,
            offset=source_offset,
            limit=100,
            market="ALL",
            board="ALL",
        )
        candidates = page["candidates"]
        target_rows = []
        for candidate in candidates:
            symbol = candidate["source_symbol"]
            rows = candidate["history_rows"]
            dates = [row["session_date"] for row in rows]
            target_matches = [
                row for row in rows if row["session_date"] == target_session
            ]
            if (
                symbol in seen_symbols
                or (last_symbol is not None and symbol <= last_symbol)
                or dates != sorted(set(dates))
                or any(day not in days or day > target_session for day in dates)
                or len(target_matches) != 1
                or any(
                    row.get("source_symbol") != symbol
                    or row.get("package_sha256") != package_sha256
                    or row.get("namespace_id") != page["namespace_id"]
                    for row in rows
                )
            ):
                raise PgResearchBindingError(
                    "D1_STRUCTURE_RAW_HISTORY_INVALID",
                    "来源历史重复、缺唯一目标行或跨代码、包、namespace、截止日",
                )
            seen_symbols.add(symbol)
            last_symbol = symbol
            target_rows.extend(target_matches)

        if source_context is None:
            source_context = read_source_version_context(
                config,
                target_session=target_session,
                package_sha256=package_sha256,
                namespace_id=page["namespace_id"],
                target_raw_rows=target_rows,
                expected_raw_row_count=page["version_row_count"],
            )
            source_context_read_count += 1
            source_pool_total = page["version_row_count"]
        context = source_context
        if (
            page["version_status"] != "CLOSED"
            or not page["partition_closed"]
            or page["total_count"] != page["version_row_count"]
            or page["namespace_id"] != source_context.get("namespace_id")
            or page["target_session"] != target_session.isoformat()
            or page["package_sha256"] != package_sha256
            or source_context.get("status") != "AVAILABLE"
            or context.get("raw_row_count") != page["version_row_count"]
            or page["version_expected_row_count"]
            != context.get("capture_expected_row_count")
            or any(
                row.get("source_content_id") != source_context.get("source_content_id")
                or str(row.get("source_capture_id"))
                != str(source_context.get("source_capture_id"))
                for row in target_rows
            )
        ):
            raise PgResearchBindingError(
                "D1_STRUCTURE_SOURCE_POOL_NOT_CLOSED",
                "Raw分页、目标日捕获或来源版本未闭合",
            )

        for candidate in candidates:
            result, decoded_count, core_count = _pool_candidate_result(
                candidate=candidate,
                sessions=days,
                target_index=target_index,
                namespace_id=page["namespace_id"],
                package_sha256=package_sha256,
                calendar_id=calendar_id,
                calendar_revision=target_calendar_revision,
            )
            history_rows_decoded += decoded_count
            core_calculations += core_count
            generation_calls += (
                5 + (2 if target_index >= 1 else 0) + (2 if target_index >= 2 else 0)
            )
            row = next(
                item
                for item in candidate["history_rows"]
                if item["session_date"] == target_session
            )
            detail = {
                "source_symbol": candidate["source_symbol"],
                "market": candidate.get("market"),
                "board": candidate.get("board"),
                "target_source": _source_row_identity(row),
                "structures": result,
            }
            for kind, outcome in result.items():
                counts[kind][outcome["state"]] += 1
                if outcome["state"] == "UNKNOWN":
                    reason = outcome.get("reason") or "UNKNOWN_REASON_NOT_PROVIDED"
                    reason_counts[kind]["unknown"][reason] += 1
                elif outcome["state"] == "NOT_GENERATED":
                    reason = outcome.get("reason") or "STRUCTURE_PREDICATE_FALSE"
                    reason_counts[kind]["not_generated"][reason] += 1
                elif outcome["state"] == "GENERATED":
                    events.append(
                        {
                            "source_symbol": candidate["source_symbol"],
                            "market": candidate.get("market"),
                            "board": candidate.get("board"),
                            "structure": kind,
                            "relation": outcome["anchor"]["relation"],
                            "anchor": outcome["anchor"],
                            "generation_evidence": outcome["generation_evidence"],
                        }
                    )
            if candidate["source_symbol"] == structure_symbol:
                selected_detail = detail

        source_page_count += 1
        expected_next = source_offset + len(candidates)
        if page["next_offset"] is None:
            if expected_next != page["total_count"]:
                raise PgResearchBindingError(
                    "D1_STRUCTURE_PAGE_INCOMPLETE", "完整来源池扫描分页提前结束"
                )
            break
        if (
            not candidates
            or page["next_offset"] != expected_next
            or page["next_offset"] >= page["total_count"]
        ):
            raise PgResearchBindingError(
                "D1_STRUCTURE_PAGE_INVALID", "来源池扫描分页游标不连续"
            )
        source_offset = page["next_offset"]

    if (
        source_context is None
        or source_pool_total is None
        or len(seen_symbols) != source_pool_total
        or len(seen_symbols) != source_context["raw_row_count"]
    ):
        raise PgResearchBindingError(
            "D1_STRUCTURE_POOL_INCOMPLETE", "目标日来源池未完全评价"
        )
    for kind in STRUCTURE_TYPES:
        if sum(counts[kind].values()) != source_pool_total:
            raise PgResearchBindingError(
                "D1_STRUCTURE_TYPE_PARTITION_INCOMPLETE",
                f"{kind}生成/未生成/未知分母不守恒",
            )
    events.sort(key=lambda item: (item["source_symbol"], item["structure"]))
    selected_events = events[offset : offset + limit]
    return {
        "ok": True,
        "status": "SOURCE_LEVEL_D1_MULTI_STRUCTURE_DISCOVERY",
        "contract": STRUCTURE_CONTRACT,
        "scope": "FIVE_SOURCE_LEVEL_RESEARCH_TYPES_NOT_FORMAL_D1_TEN_TYPE_CLOSURE",
        "research_mode": "NON_PIT_RETROSPECTIVE",
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "price_coordinate": "RAW_UNADJUSTED",
        "target_session": target_session.isoformat(),
        "research_cutoff": research_cutoff.isoformat(),
        "calendar": {
            "calendar_id": calendar_id,
            "calendar_revision": target_calendar_revision,
            "calendar_sha256": target_calendar_sha256,
            "official_session_count": len(days),
            "history_start": days[0].isoformat(),
            "history_end": days[-1].isoformat(),
        },
        "source": source_context,
        "source_pool_count": source_pool_total,
        "evaluated_source_symbol_count": len(seen_symbols),
        "source_pool_partition_closed": len(seen_symbols) == source_pool_total,
        "structure_type_counts": {
            kind: {
                **counts[kind],
                "unknown_reason_counts": dict(
                    sorted(reason_counts[kind]["unknown"].items())
                ),
                "not_generated_reason_counts": dict(
                    sorted(reason_counts[kind]["not_generated"].items())
                ),
                "partition_closed": sum(counts[kind].values()) == source_pool_total,
            }
            for kind in STRUCTURE_TYPES
        },
        "generated_event_total": len(events),
        "page": {
            "offset": offset,
            "limit": limit,
            "result_count": len(selected_events),
            "event_total": len(events),
            "next_offset": offset + limit if offset + limit < len(events) else None,
            "stable_order": "source_symbol ASC, structure ASC",
            "results": selected_events,
        },
        "selected_source_symbol_detail": selected_detail
        if selected_detail is not None
        else {
            "source_symbol": structure_symbol,
            "status": "NOT_IN_TARGET_SOURCE_POOL"
            if structure_symbol is not None
            else "NOT_REQUESTED",
        },
        "support_followup": {
            "status": "NOT_AVAILABLE_AT_TARGET_WINDOW_END",
            "reason": "请求窗口截止于目标日；单股历史模式显示目标日之前已可见的后继状态",
            "resistance_types_excluded": ["PIVOT_HIGH", "RANGE_CEILING"],
        },
        "formal_downstream": {
            "formal_p15_anchor": "NOT_CREATED",
            "formal_d2": "NOT_CREATED",
            "priority": "NOT_EVALUATED",
            "focus": "NOT_EVALUATED",
            "trade_signal": "NOT_EVALUATED",
        },
        "execution": {
            "database_target": "127.0.0.1:5432/stock_a",
            "database_schema": "stock",
            "database_role": "stock_a",
            "database_transaction": "REPEATABLE_READ_READ_ONLY_PER_SOURCE_PAGE",
            "database_write_attempted": False,
            "ddl_executed": False,
            "dml_executed": False,
            "source_zip_opened": False,
            "source_pages_read": source_page_count,
            "raw_history_batch_reads": source_page_count,
            "source_version_context_reads": source_context_read_count,
            "full_pool_scan_passes": 1,
            "evaluated_source_rows": len(seen_symbols),
            "history_raw_rows_decoded_once": history_rows_decoded,
            "core_calculations": core_calculations,
            "structure_kernel_calls": generation_calls,
            "wall_seconds": round(time.perf_counter() - started_wall, 6),
            "cpu_seconds": round(time.process_time() - started_cpu, 6),
        },
    }


def evaluate_source_d1_structure_timeline(
    *,
    symbol: str,
    sessions: Sequence[date],
    history_rows: Sequence[Mapping[str, Any]],
    namespace_id: str,
    package_sha256: str,
    core_previews_by_session: Mapping[date, Mapping[str, Any]],
) -> dict[str, Any]:
    """为单一来源代码沿已存会话生成锚并逐日观察后继，不回写PG。"""
    normalized = symbol.strip().upper()
    observations, bars, row_map = _history_observations(
        symbol=normalized,
        sessions=sessions,
        history_rows=history_rows,
        namespace_id=namespace_id,
        package_sha256=package_sha256,
    )
    if set(core_previews_by_session) != set(sessions):
        raise PgResearchBindingError(
            "D1_STRUCTURE_HISTORY_PREVIEW_INCOMPLETE",
            "单股结构观察需要每个输入会话的精确Core预览",
        )
    range_state = {
        kind: unknown("RANGE_PRIOR_STATE_LEFT_CENSORED") for kind in _RANGE_TYPES
    }
    range_anchor: dict[str, Anchor | None] = {kind: None for kind in _RANGE_TYPES}
    anchors: dict[str, dict[str, Any]] = {}
    state_counts = {
        kind: {"GENERATED": 0, "NOT_GENERATED": 0, "UNKNOWN": 0}
        for kind in STRUCTURE_TYPES
    }
    points = []

    def core_field(day: date, name: str) -> FieldValue:
        preview = core_previews_by_session.get(day)
        fields = (
            preview.get("core", {}).get("fields")
            if isinstance(preview, Mapping)
            else None
        )
        item = fields.get(name) if isinstance(fields, Mapping) else None
        return _field_from_rendered(item, name)

    for index, day in enumerate(sessions):
        previous_core = (
            core_previews_by_session.get(sessions[index - 1]) if index > 0 else None
        )
        previous_fields = (
            {
                "core.atr20": core_field(sessions[index - 1], "core.atr20"),
                "core.ma_slope20_atr": core_field(
                    sessions[index - 1], "core.ma_slope20_atr"
                ),
            }
            if previous_core is not None
            else None
        )
        results, _, anchor_models = _generate_day(
            symbol=normalized,
            target_index=index,
            sessions=sessions,
            observations=observations,
            bars=bars,
            row_map=row_map,
            namespace_id=namespace_id,
            package_sha256=package_sha256,
            calendar_id="OFFICIAL_SESSION_CALENDAR",
            calendar_revision="SOURCE_WINDOW_BOUND",
            core_previous=previous_fields,
            prior_range_states=range_state,
            prior_range_anchors=range_anchor,
        )
        new_anchors: list[Anchor] = []
        for kind, outcome in results.items():
            state_counts[kind][outcome["state"]] += 1
            if kind in _RANGE_TYPES:
                if outcome["state"] == "GENERATED":
                    anchor = anchor_models[kind]
                    new_anchors.append(anchor)
                    range_anchor[kind] = anchor
                    range_state[kind] = FieldValue(True)
                elif outcome["state"] == "NOT_GENERATED":
                    if outcome.get("reason") == "RANGE_ANCHOR_REUSED_UNCHANGED_BOUNDS":
                        range_state[kind] = FieldValue(True)
                    else:
                        range_state[kind] = FieldValue(False)
                        range_anchor[kind] = None
                else:
                    range_state[kind] = unknown(
                        outcome.get("reason") or "RANGE_QUALIFICATION_UNKNOWN"
                    )
                    range_anchor[kind] = None
            elif outcome["state"] == "GENERATED":
                new_anchors.append(anchor_models[kind])

        today = bars[index]
        actual = _actual_field(observations[index])
        clv = core_field(day, "core.clv")
        atr_previous = (
            core_field(sessions[index - 1], "core.atr20")
            if index > 0
            else unknown("PREVIOUS_CORE_UNAVAILABLE")
        )
        observed_anchors = []
        for anchor_id, item in anchors.items():
            anchor = item["anchor"]
            if anchor.relation != "SUPPORT" or index <= anchor.created_ordinal:
                continue
            observation = observe_support(
                anchor,
                today=today,
                prior=item.get("prior_observation"),
                lower=FieldValue(anchor.lower),
                upper=FieldValue(anchor.upper),
                atr_previous=atr_previous,
                clv=clv,
                actual=actual,
                capability_state="READY",
            )
            item["prior_observation"] = observation
            observation_payload = {
                "anchor_id": anchor_id,
                "type": anchor.kind,
                "session": day.isoformat(),
                "state": observation.state,
                "acceptance": observation.acceptance,
                "freshness": observation.freshness,
                "breach_streak": observation.breach_streak,
                "acceptance_streak": observation.acceptance_streak,
                "test_count": observation.test_count,
                "separated_sessions": observation.separated_sessions,
                "retest_qualified": observation.retest_qualified,
                "atr_previous": _render_field(atr_previous),
                "clv": _render_field(clv),
                "source": _source_row_identity(row_map.get(day)),
            }
            item["support_observations"].append(observation_payload)
            observed_anchors.append(observation_payload)

            if anchor.kind == "GAP_ZONE":
                prior_fill = item["gap_fill_state"]
                if prior_fill.quality == "OBSERVED":
                    next_fill = gap_fill(
                        prior_fill.value,
                        low=(
                            observations[index].low
                            if observations[index].state == "ACTUAL"
                            else unknown("GAP_LOW_UNKNOWN")
                        ),
                        lower=anchor.lower,
                        upper=anchor.upper,
                    )
                else:
                    next_fill = prior_fill
                item["gap_fill_state"] = next_fill
                fill_payload = {
                    "anchor_id": anchor_id,
                    "type": anchor.kind,
                    "session": day.isoformat(),
                    "state": next_fill.value
                    if next_fill.quality == "OBSERVED"
                    else "UNKNOWN",
                    "quality": next_fill.quality,
                    "reason": next_fill.reason,
                    "source": _source_row_identity(row_map.get(day)),
                }
                item["gap_fill_observations"].append(fill_payload)
                observed_anchors.append({"gap_fill": fill_payload})

        for anchor in new_anchors:
            if anchor.anchor_id in anchors:
                continue
            result = results[anchor.kind]
            anchors[anchor.anchor_id] = {
                "anchor": anchor,
                "birth": {
                    **_anchor_payload(anchor, sessions),
                    "source_row": _source_row_identity(row_map.get(day)),
                    "generation_evidence": result["generation_evidence"],
                },
                "prior_observation": None,
                "support_observations": [],
                "gap_fill_state": FieldValue("UNTOUCHED"),
                "gap_fill_observations": [],
            }
        points.append(
            {
                "session": day.isoformat(),
                "target_bar_status": "ACTUAL"
                if observations[index].state == "ACTUAL"
                else "UNKNOWN",
                "structures": {
                    kind: {
                        "state": outcome["state"],
                        "reason": outcome.get("reason"),
                        "anchor_id": outcome["anchor"]["anchor_id"]
                        if outcome.get("anchor")
                        else None,
                    }
                    for kind, outcome in results.items()
                },
                "subsequent_support_observations": observed_anchors,
            }
        )

    event_payloads = []
    for item in anchors.values():
        anchor = item["anchor"]
        event_payloads.append(
            {
                **item["birth"],
                "support_observations": item["support_observations"],
                "gap_fill_observations": item["gap_fill_observations"]
                if anchor.kind == "GAP_ZONE"
                else [],
                "support_observation_status": "NOT_APPLICABLE"
                if anchor.relation == "RESISTANCE"
                else "NOT_AVAILABLE_AT_REQUEST_WINDOW_END"
                if anchor.test_from >= len(sessions)
                else "OBSERVED_FROM_NEXT_OFFICIAL_SESSION",
                "gap_fill_is_independent_of_formal_invalidation": (
                    anchor.kind == "GAP_ZONE"
                ),
            }
        )
    event_payloads.sort(
        key=lambda item: (item["created_session"], item["type"], item["anchor_id"])
    )
    return {
        "contract": STRUCTURE_CONTRACT,
        "status": "SOURCE_SYMBOL_D1_STRUCTURE_HISTORY",
        "research_mode": "NON_PIT_RETROSPECTIVE",
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "price_coordinate": "RAW_UNADJUSTED",
        "source_symbol": normalized,
        "window": {
            "start_session": sessions[0].isoformat(),
            "end_session": sessions[-1].isoformat(),
            "official_session_count": len(sessions),
            "future_session_rows_used": 0,
        },
        "type_counts": {
            kind: {
                **state_counts[kind],
                "evaluated_sessions": len(sessions),
                "partition_closed": sum(state_counts[kind].values()) == len(sessions),
            }
            for kind in STRUCTURE_TYPES
        },
        "events": event_payloads,
        "points": points,
        "resistance_types_excluded_from_support": [
            "PIVOT_HIGH",
            "RANGE_CEILING",
        ],
        "formal_downstream": {
            "formal_p15_anchor": "NOT_CREATED",
            "formal_d2": "NOT_CREATED",
            "priority": "NOT_EVALUATED",
            "focus": "NOT_EVALUATED",
            "trade_signal": "NOT_EVALUATED",
        },
    }
