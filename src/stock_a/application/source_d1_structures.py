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
from stock_a.research.entry_history.history_preview import (
    prepare_source_d1_preview_context,
    research_stock_pg_d1_history_previews,
)
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
MATERIAL_CHANGE_REASONS = (
    "SUPPORT_STATE_TRANSITION",
    "SUPPORT_ACCEPTANCE_TRANSITION",
    "SUPPORT_FRESHNESS_TRANSITION",
    "RETEST_QUALIFICATION_TRANSITION",
    "FIRST_VALID_TEST_OBSERVED",
    "FIRST_SEPARATED_SESSION_OBSERVED",
    "GAP_FILL_STAGE_TRANSITION",
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
) -> tuple[
    tuple[CoreObservation, ...],
    tuple[Bar, ...],
    dict[date, Mapping[str, Any]],
    dict[date, Any],
]:
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
    decoded_by_session = {}
    for ordinal, day in enumerate(days):
        row = row_map.get(day)
        decoded = _decoded_day_from_pg_raw(row) if row is not None else None
        if decoded is not None:
            decoded_by_session[day] = decoded
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
    return tuple(observations), tuple(bars), row_map, decoded_by_session


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
        "bar_reason": row.get("bar_reason"),
        "price_quality": row.get("price_quality"),
        "price_reason": row.get("price_reason"),
        "namespace_id": row.get("namespace_id"),
        "package_sha256": row.get("package_sha256"),
        "source_capture_id": str(row["source_capture_id"])
        if row.get("source_capture_id") is not None
        else None,
        "source_content_id": row.get("source_content_id"),
        "source_entry_sha256": row.get("source_entry_sha256"),
        "parser_contract": row.get("parser_contract"),
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
            prior_reason = (
                prior_range_state.reason
                if prior_range_state is not None
                else "RANGE_PRIOR_STATE_UNAVAILABLE"
            )
            if prior_reason == "RANGE_ANCHOR_LEFT_CENSORED":
                if prior_range_anchor is None:
                    return {
                        "state": "UNKNOWN",
                        "reason": "RANGE_ANCHOR_LEFT_CENSORED",
                        "anchor": None,
                        "carry_anchor": anchor,
                    }
                if (prior_range_anchor.lower, prior_range_anchor.upper) == (
                    anchor.lower,
                    anchor.upper,
                ):
                    return {
                        "state": "UNKNOWN",
                        "reason": "RANGE_ANCHOR_LEFT_CENSORED",
                        "anchor": None,
                        "carry_anchor": prior_range_anchor,
                    }
                return {"state": "GENERATED", "reason": None, "anchor": anchor}
            else:
                return {
                    "state": "UNKNOWN",
                    "reason": prior_reason,
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


def _range_outcome_is_left_censored(
    *,
    session_index: int,
    observations: Sequence[CoreObservation],
    reason: str | None,
    core_previous: Mapping[str, FieldValue] | None,
) -> bool:
    if session_index >= 25 or any(
        observation.state != "ACTUAL"
        for observation in observations[: session_index + 1]
    ):
        return False
    if reason == "RANGE_PRIOR_WINDOW_UNKNOWN" and session_index < 20:
        return True
    return reason == "STRUCTURE_INPUT_UNKNOWN" and any(
        core_previous is None
        or core_previous.get(name) is None
        or core_previous[name].quality != "OBSERVED"
        and core_previous[name].reason == "SHORT_HISTORY"
        for name in ("core.atr20", "core.ma_slope20_atr")
    )


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
        if (
            kind in _RANGE_TYPES
            and classified["state"] == "UNKNOWN"
            and _range_outcome_is_left_censored(
                session_index=target_index,
                observations=observations,
                reason=classified.get("reason"),
                core_previous=core_previous,
            )
        ):
            classified["reason"] = "RANGE_ANCHOR_LEFT_CENSORED"
        anchor = classified["anchor"]
        if isinstance(anchor, Anchor):
            anchor_models[kind] = anchor
        elif kind in _RANGE_TYPES and isinstance(
            classified.get("carry_anchor"), Anchor
        ):
            anchor_models[kind] = classified["carry_anchor"]
        results[kind] = {
            **{key: item for key, item in classified.items() if key != "carry_anchor"},
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


def _range_unknown_is_left_censored(
    *,
    session_index: int,
    observations: Sequence[CoreObservation],
    candidate: Mapping[str, Any],
    core_previous: Mapping[str, FieldValue] | None,
) -> bool:
    """区分请求窗口历史不足与窗口内部的真实缺值断点。"""
    reason = candidate.get("reason")
    if session_index >= 25 or any(
        observation.state != "ACTUAL"
        for observation in observations[: session_index + 1]
    ):
        return False
    if reason == "RANGE_PRIOR_WINDOW_UNKNOWN" and session_index < 20:
        return True
    if reason == "STRUCTURE_INPUT_UNKNOWN":
        if core_previous is None:
            return True
        return any(
            core_previous.get(name) is not None
            and core_previous[name].quality != "OBSERVED"
            and core_previous[name].reason == "SHORT_HISTORY"
            for name in ("core.atr20", "core.ma_slope20_atr")
        )
    return False


def _pool_candidate_result(
    *,
    candidate: Mapping[str, Any],
    sessions: Sequence[date],
    target_index: int,
    namespace_id: str,
    package_sha256: str,
    calendar_id: str,
    calendar_revision: str,
    history_inputs=None,
    core_previews_by_session: Mapping[date, Mapping[str, Any]] | None = None,
) -> tuple[dict[str, dict[str, Any]], int, int, int]:
    symbol = candidate["source_symbol"]
    history_inputs_reused = history_inputs is not None
    if history_inputs is None:
        history_inputs = _history_observations(
            symbol=symbol,
            sessions=sessions,
            history_rows=candidate["history_rows"],
            namespace_id=namespace_id,
            package_sha256=package_sha256,
        )
    observations, bars, row_map, _decoded_by_session = history_inputs
    core_cache: dict[int, dict[str, FieldValue]] = {}
    if core_previews_by_session is not None:
        if set(core_previews_by_session) != set(sessions):
            raise PgResearchBindingError(
                "D1_STRUCTURE_CORE_PREVIEW_INCOMPLETE",
                "共享结构Core预览未覆盖完整官方会话",
            )
        for index, day in enumerate(sessions):
            fields = core_previews_by_session[day]["core"]["fields"]
            core_cache[index] = {
                name: _field_from_rendered(fields.get(name), name)
                for name in ("core.atr20", "core.ma_slope20_atr", "core.clv")
            }

    def core_at(end_index: int) -> dict[str, FieldValue] | None:
        if end_index < 0:
            return None
        if end_index not in core_cache:
            core_cache[end_index] = _core_at(
                observations,
                sessions,
                end_index=end_index,
                calendar_id=calendar_id,
                calendar_revision=calendar_revision,
            )
        return core_cache[end_index]

    previous_core = core_at(target_index - 1)
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
            core_previous=core_at(target_index - 2),
        )
    )
    range_history_checks = int(target_index > 0)
    range_history_evidence: dict[str, dict[str, Any]] = {}
    for kind in sorted(_RANGE_TYPES):
        prior_candidate = prior_range_results[kind]
        prior_index = target_index - 1
        if prior_range_states[kind].quality != "OBSERVED":
            left_censored = _range_unknown_is_left_censored(
                session_index=prior_index,
                observations=observations,
                candidate=prior_candidate,
                core_previous=core_at(prior_index - 1),
            )
            status = "LEFT_CENSORED" if left_censored else "UNKNOWN_DATA"
            reason = (
                "RANGE_ANCHOR_LEFT_CENSORED"
                if left_censored
                else prior_range_states[kind].reason
            )
            prior_range_states[kind] = unknown(reason)
            prior_range_anchors[kind] = None
            range_history_evidence[kind] = {
                "status": status,
                "reason": reason,
                "lookback_official_sessions": 1,
                "created_session": None,
                "trace_stop_session": sessions[prior_index].isoformat()
                if prior_index >= 0
                else None,
            }
            continue
        if prior_range_states[kind].value is not True:
            range_history_evidence[kind] = {
                "status": "NO_PRIOR_RANGE",
                "reason": prior_candidate.get("reason"),
                "lookback_official_sessions": 1,
                "created_session": None,
                "trace_stop_session": sessions[prior_index].isoformat(),
            }
            continue

        prior_anchor = prior_range_anchors[kind]
        if prior_anchor is None:
            prior_range_states[kind] = unknown("RANGE_PRIOR_ANCHOR_UNAVAILABLE")
            range_history_evidence[kind] = {
                "status": "UNKNOWN_DATA",
                "reason": "RANGE_PRIOR_ANCHOR_UNAVAILABLE",
                "lookback_official_sessions": 1,
                "created_session": None,
                "trace_stop_session": sessions[prior_index].isoformat(),
            }
            continue

        cursor = target_index - 2
        lookback = 1
        proof = None
        trace_stop_session = None
        while cursor >= 0:
            older_target_index = cursor + 1
            older_core = core_at(cursor - 1)
            older_results, older_states, older_anchors = (
                _range_candidates_for_previous_session(
                    symbol=symbol,
                    target_index=older_target_index,
                    sessions=sessions,
                    observations=observations,
                    bars=bars,
                    namespace_id=namespace_id,
                    package_sha256=package_sha256,
                    calendar_id=calendar_id,
                    calendar_revision=calendar_revision,
                    core_previous=older_core,
                )
            )
            range_history_checks += 1
            lookback += 1
            older_candidate = older_results[kind]
            older_state = older_states[kind]
            trace_stop_session = sessions[cursor].isoformat()
            if older_state.quality != "OBSERVED":
                left_censored = _range_unknown_is_left_censored(
                    session_index=cursor,
                    observations=observations,
                    candidate=older_candidate,
                    core_previous=older_core,
                )
                reason = (
                    "RANGE_ANCHOR_LEFT_CENSORED"
                    if left_censored
                    else older_state.reason or "RANGE_HISTORY_UNKNOWN"
                )
                status = "LEFT_CENSORED" if left_censored else "UNKNOWN_DATA"
                prior_range_states[kind] = unknown(reason)
                # Only a continuous, valid candidate immediately before the
                # target can be carried across the left edge as an identity token.
                prior_range_anchors[kind] = prior_anchor if left_censored else None
                proof = {
                    "status": status,
                    "reason": reason,
                    "created_session": None,
                }
                break
            if older_state.value is False:
                proof = {
                    "status": "PROVEN_AFTER_NOT_QUALIFIED",
                    "reason": "PRIOR_RANGE_PREDICATE_FALSE",
                    "created_session": sessions[prior_index].isoformat(),
                }
                break
            older_anchor = older_anchors[kind]
            if older_anchor is None:
                prior_range_states[kind] = unknown("RANGE_PRIOR_ANCHOR_UNAVAILABLE")
                prior_range_anchors[kind] = None
                proof = {
                    "status": "UNKNOWN_DATA",
                    "reason": "RANGE_PRIOR_ANCHOR_UNAVAILABLE",
                    "created_session": None,
                }
                break
            if (older_anchor.lower, older_anchor.upper) != (
                prior_anchor.lower,
                prior_anchor.upper,
            ):
                proof = {
                    "status": "PROVEN_BOUNDARY_CHANGE",
                    "reason": "RANGE_BOUNDS_CHANGED_FROM_PREVIOUS_SESSION",
                    "created_session": sessions[prior_index].isoformat(),
                }
                break
            cursor -= 1
        if proof is None:
            prior_range_states[kind] = unknown("RANGE_ANCHOR_LEFT_CENSORED")
            prior_range_anchors[kind] = prior_anchor
            proof = {
                "status": "LEFT_CENSORED",
                "reason": "RANGE_ANCHOR_LEFT_CENSORED",
                "created_session": None,
            }
        range_history_evidence[kind] = {
            **proof,
            "lookback_official_sessions": lookback,
            "trace_stop_session": trace_stop_session,
        }
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
            and not (
                prior_range_states[kind].reason == "RANGE_ANCHOR_LEFT_CENSORED"
                and prior_range_anchors[kind] is not None
                and results[kind]["state"] == "GENERATED"
            )
        ):
            results[kind] = {
                **results[kind],
                "state": "UNKNOWN",
                "reason": prior_range_states[kind].reason,
                "anchor": None,
            }
        results[kind]["prior_anchor_evidence"] = range_history_evidence[kind]
    return (
        results,
        0 if history_inputs_reused else len(row_map),
        len(core_cache),
        range_history_checks,
    )


def _structure_history_core_previews(
    *,
    symbol: str,
    sessions: Sequence[date],
    history_rows: Sequence[Mapping[str, Any]],
    namespace_id: str,
    package_sha256: str,
    preview_context,
    decoded_by_session: Mapping[date, Any] | None = None,
) -> dict[date, dict[str, Any]]:
    previews = research_stock_pg_d1_history_previews(
        symbol=symbol,
        event_sessions=sessions,
        followup_session=sessions[-1],
        session_dates=sessions,
        package_sha256=package_sha256,
        history_rows=history_rows,
        namespace_id=namespace_id,
        context=preview_context,
        _decoded_by_session=decoded_by_session,
    )
    return {
        day: {"core": {"fields": preview["structure_core_fields"]}}
        for day, preview in previews.items()
    }


def _observation_quality_reasons(observation: Mapping[str, Any] | None) -> list[str]:
    if not isinstance(observation, Mapping):
        return []
    reasons = []
    for group, fields in (
        (
            "SUPPORT_CORE",
            {
                "atr_previous": observation.get("atr_previous"),
                "clv": observation.get("clv"),
            },
        ),
        ("SUPPORT_PRICE", observation.get("price_inputs", {})),
    ):
        if not isinstance(fields, Mapping):
            continue
        for name, field in fields.items():
            if not isinstance(field, Mapping) or field.get("quality") == "OBSERVED":
                continue
            reason = field.get("reason") or field.get("quality") or "UNKNOWN"
            reasons.append(f"{group}_{name.upper()}:{reason}")
    source = observation.get("source")
    if isinstance(source, Mapping):
        for name in ("bar_reason", "price_reason"):
            reason = source.get(name)
            if reason:
                reasons.append(f"SOURCE_{name.upper()}:{reason}")
    if observation.get("state") == "UNKNOWN" and not reasons:
        reasons.append("SUPPORT_STATE_UNKNOWN_WITHOUT_FIELD_REASON")
    return sorted(set(reasons))


def _structure_timeline_changes(
    *,
    timeline: Mapping[str, Any],
    previous_session: date,
    target_session: date,
) -> list[dict[str, Any]]:
    support_fields = (
        "state",
        "acceptance",
        "freshness",
        "breach_streak",
        "acceptance_streak",
        "test_count",
        "separated_sessions",
        "retest_qualified",
    )
    changes: list[dict[str, Any]] = []
    for anchor_event in timeline.get("events", []):
        created_session = date.fromisoformat(anchor_event["created_session"])
        if created_session >= target_session:
            continue
        support_by_session = {
            item["session"]: item
            for item in anchor_event.get("support_observations", [])
        }
        gap_by_session = {
            item["session"]: item
            for item in anchor_event.get("gap_fill_observations", [])
        }
        before = support_by_session.get(previous_session.isoformat())
        after = support_by_session.get(target_session.isoformat())
        relation = anchor_event.get("relation")
        if relation != "SUPPORT":
            support_status = "NOT_APPLICABLE"
            support_reasons = ["RESISTANCE_ANCHOR_EXCLUDED_FROM_SUPPORT"]
            support_changed_fields: list[str] = []
        elif created_session >= previous_session:
            support_status = "NOT_COMPARABLE"
            support_reasons = ["ANCHOR_BIRTH_DAY_CANNOT_SELF_PROVE"]
            support_changed_fields = []
        elif before is None or after is None:
            support_status = "NOT_EVALUABLE"
            support_reasons = ["ADJACENT_SUPPORT_OBSERVATION_MISSING"]
            support_changed_fields = []
        elif before.get("state") == "UNKNOWN" or after.get("state") == "UNKNOWN":
            support_status = "UNKNOWN"
            support_reasons = ["ADJACENT_SUPPORT_OBSERVATION_UNKNOWN"]
            support_changed_fields = []
        else:
            support_changed_fields = [
                name for name in support_fields if before.get(name) != after.get(name)
            ]
            support_status = "CHANGED" if support_changed_fields else "UNCHANGED"
            support_reasons = [
                f"SUPPORT_{name.upper()}_CHANGED" for name in support_changed_fields
            ] or ["SAME_ANCHOR_SUPPORT_STATE_UNCHANGED"]

        gap_before = gap_by_session.get(previous_session.isoformat())
        gap_after = gap_by_session.get(target_session.isoformat())
        if anchor_event.get("type") != "GAP_ZONE":
            gap_status = "NOT_APPLICABLE"
            gap_reasons = ["NOT_A_GAP_ANCHOR"]
        elif created_session >= previous_session:
            gap_status = "NOT_COMPARABLE"
            gap_reasons = ["GAP_BIRTH_DAY_CANNOT_SELF_PROVE"]
        elif gap_before is None or gap_after is None:
            gap_status = "NOT_EVALUABLE"
            gap_reasons = ["ADJACENT_GAP_FILL_OBSERVATION_MISSING"]
        elif (
            gap_before.get("quality") != "OBSERVED"
            or gap_after.get("quality") != "OBSERVED"
            or gap_before.get("state") == "UNKNOWN"
            or gap_after.get("state") == "UNKNOWN"
        ):
            gap_status = "UNKNOWN"
            gap_reasons = ["ADJACENT_GAP_FILL_OBSERVATION_UNKNOWN"]
        elif gap_before.get("state") != gap_after.get("state"):
            gap_status = "CHANGED"
            gap_reasons = ["GAP_FILL_STATE_CHANGED"]
        else:
            gap_status = "UNCHANGED"
            gap_reasons = ["GAP_FILL_STATE_UNCHANGED"]

        quality_reasons = sorted(
            set(
                _observation_quality_reasons(before)
                + _observation_quality_reasons(after)
                + [
                    f"GAP_FILL_{name.upper()}:{item.get('reason')}"
                    for name, item in (("BEFORE", gap_before), ("AFTER", gap_after))
                    if isinstance(item, Mapping)
                    and item.get("quality") != "OBSERVED"
                    and item.get("reason")
                ]
            )
        )
        material_reasons = []
        if relation != "SUPPORT":
            material_status = "NOT_APPLICABLE"
            material_reasons = ["RESISTANCE_ANCHOR_EXCLUDED_FROM_MATERIAL_SUPPORT"]
        elif support_status in {"NOT_COMPARABLE", "NOT_EVALUABLE"}:
            material_status = support_status
            material_reasons = list(support_reasons)
        else:
            if support_status != "UNKNOWN" and before is not None and after is not None:
                for field, reason in (
                    ("state", "SUPPORT_STATE_TRANSITION"),
                    ("acceptance", "SUPPORT_ACCEPTANCE_TRANSITION"),
                    ("freshness", "SUPPORT_FRESHNESS_TRANSITION"),
                    ("retest_qualified", "RETEST_QUALIFICATION_TRANSITION"),
                ):
                    if before.get(field) != after.get(field):
                        material_reasons.append(reason)
                if before.get("test_count", 0) == 0 and after.get("test_count", 0) > 0:
                    material_reasons.append("FIRST_VALID_TEST_OBSERVED")
                if (
                    before.get("separated_sessions", 0) == 0
                    and after.get("separated_sessions", 0) > 0
                ):
                    material_reasons.append("FIRST_SEPARATED_SESSION_OBSERVED")
            if gap_status == "CHANGED":
                material_reasons.append("GAP_FILL_STAGE_TRANSITION")
            if material_reasons:
                material_status = "MATERIAL_CHANGE"
            elif support_status == "UNKNOWN" or gap_status == "UNKNOWN":
                material_status = "UNKNOWN"
                material_reasons = sorted(set(support_reasons + gap_reasons))
            elif support_status in {"NOT_COMPARABLE", "NOT_EVALUABLE"}:
                material_status = support_status
                material_reasons = list(support_reasons)
            elif support_changed_fields:
                material_status = "COUNT_ONLY"
                material_reasons = [
                    "DIAGNOSTIC_COUNTER_CHANGED_WITHOUT_STATE_TRANSITION"
                ]
            else:
                material_status = "UNCHANGED"
                material_reasons = ["NO_MATERIAL_SUPPORT_OR_GAP_FILL_TRANSITION"]

        if relation != "SUPPORT":
            classification = "NOT_APPLICABLE"
        elif support_status == "CHANGED" or gap_status == "CHANGED":
            classification = "CHANGED"
        elif support_status == "UNKNOWN" or gap_status == "UNKNOWN":
            classification = "UNKNOWN"
        elif support_status in {"NOT_COMPARABLE", "NOT_EVALUABLE"}:
            classification = support_status
        else:
            classification = "UNCHANGED"

        lower = anchor_event.get("lower")
        upper = anchor_event.get("upper")
        changes.append(
            {
                "source_symbol": anchor_event["source_symbol"],
                "type": anchor_event["type"],
                "relation": relation,
                "created_session": anchor_event["created_session"],
                "anchor_id": anchor_event["anchor_id"],
                "anchor_pairing_identity": {
                    "source_symbol": anchor_event["source_symbol"],
                    "type": anchor_event["type"],
                    "created_session": anchor_event["created_session"],
                    "coordinate_id": anchor_event.get("coordinate_id"),
                    "lower": lower,
                    "upper": upper,
                },
                "comparison_context": {
                    "previous_official_session": previous_session.isoformat(),
                    "target_session": target_session.isoformat(),
                    "official_sessions_adjacent": True,
                    "anchor_birth_before_previous_session": created_session
                    < previous_session,
                    "request_window_start": timeline["window"]["start_session"],
                    "anchor_id_used_across_request_windows": False,
                },
                "change_classification": classification,
                "field_change_classification": classification,
                "change_reasons": sorted(set(support_reasons + gap_reasons)),
                "quality_reasons": quality_reasons,
                "material_change": material_status == "MATERIAL_CHANGE",
                "material_change_classification": material_status,
                "material_change_reasons": sorted(set(material_reasons)),
                "support_change": {
                    "classification": support_status,
                    "reason_codes": support_reasons,
                    "changed_fields": support_changed_fields,
                    "before": before,
                    "after": after,
                },
                "gap_fill_change": {
                    "classification": gap_status,
                    "reason_codes": gap_reasons,
                    "before": gap_before,
                    "after": gap_after,
                    "independent_of_support_or_formal_invalidation": True,
                },
                "generation_evidence": anchor_event.get("generation_evidence"),
                "source": {
                    "birth": anchor_event.get("source_row"),
                    "previous": before.get("source") if before else None,
                    "target": after.get("source") if after else None,
                    "price_coordinate": "RAW_UNADJUSTED",
                    "identity_status": "SOURCE_SYMBOL_ONLY",
                },
            }
        )
    return changes


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
    structure_changes: bool = False,
    structure_type: str | None = None,
    material_change_reason: str | None = None,
    structure_change_status: str | None = None,
    material_changes_only: bool = False,
    include_source_symbol_change_summary: bool = False,
    include_source_symbol_d1_details: bool = False,
    include_source_symbol_structure_results: bool = False,
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
        or type(structure_changes) is not bool
        or type(material_changes_only) is not bool
        or type(include_source_symbol_change_summary) is not bool
        or type(include_source_symbol_d1_details) is not bool
        or type(include_source_symbol_structure_results) is not bool
        or (include_source_symbol_d1_details and not structure_changes)
        or (structure_type is not None and structure_type not in STRUCTURE_TYPES)
        or (
            material_change_reason is not None
            and material_change_reason not in MATERIAL_CHANGE_REASONS
        )
        or (
            structure_change_status is not None
            and structure_change_status
            not in {
                "CHANGED",
                "UNCHANGED",
                "UNKNOWN",
                "NOT_COMPARABLE",
                "NOT_EVALUABLE",
                "NOT_APPLICABLE",
            }
        )
        or (not structure_changes and structure_change_status is not None)
        or (not structure_changes and material_change_reason is not None)
        or (not structure_changes and material_changes_only)
        or (not structure_changes and structure_type is not None)
        or (structure_changes and target_session == days[0])
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

    calendar_payload, calendar_metadata, calendar_revision, all_calendar_sessions = (
        _load_calendar(target_session)
    )
    official_window = tuple(
        day for day in all_calendar_sessions if days[0] <= day <= target_session
    )
    if days != official_window:
        raise PgResearchBindingError(
            "D1_STRUCTURE_CALENDAR_WINDOW_INVALID",
            "D1结构历史窗口未与版本化官方日历严格闭合",
        )
    calendar_meta = next(
        item for item in calendar_metadata if item["year"] == target_session.year
    )
    calendar_id = calendar_payload["calendar_id"]
    target_calendar_revision = calendar_revision
    target_calendar_sha256 = calendar_meta["resource_sha256"]
    target_index = len(days) - 1
    previous_session = days[target_index - 1] if target_index >= 1 else None
    structure_preview_context = (
        prepare_source_d1_preview_context(days) if structure_changes else None
    )
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
    historical_change_page: list[dict[str, Any]] = []
    selected_symbol_anchor_changes: list[dict[str, Any]] | None = None
    filtered_anchor_count = 0
    historical_anchor_count_by_type: Counter[str] = Counter()
    change_status_counts: Counter[str] = Counter()
    material_change_status_counts: Counter[str] = Counter()
    material_change_status_counts_by_type = {
        kind: Counter() for kind in STRUCTURE_TYPES
    }
    material_change_reason_anchor_counts: Counter[str] = Counter()
    material_change_reason_anchor_counts_by_type = {
        kind: Counter() for kind in STRUCTURE_TYPES
    }
    material_change_reason_source_symbols = {
        reason: set() for reason in MATERIAL_CHANGE_REASONS
    }
    material_change_reason_source_symbols_by_type = {
        kind: {reason: set() for reason in MATERIAL_CHANGE_REASONS}
        for kind in STRUCTURE_TYPES
    }
    support_status_counts: Counter[str] = Counter()
    changed_source_symbols: set[str] = set()
    material_change_source_symbols: set[str] = set()
    source_symbol_change_summaries: dict[str, dict[str, Any]] = {}
    source_symbol_d1_details: dict[str, list[dict[str, Any]]] = {}
    source_symbol_structure_results: dict[str, dict[str, Any]] = {}
    range_history_status_counts = {kind: Counter() for kind in sorted(_RANGE_TYPES)}
    structure_core_preview_count = 0
    structure_timeline_candidate_count = 0

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
            history_inputs = None
            core_previews = None
            if structure_changes:
                history_inputs = _history_observations(
                    symbol=candidate["source_symbol"],
                    sessions=days,
                    history_rows=candidate["history_rows"],
                    namespace_id=page["namespace_id"],
                    package_sha256=package_sha256,
                )
                history_rows_decoded += len(history_inputs[2])
                core_previews = _structure_history_core_previews(
                    symbol=candidate["source_symbol"],
                    sessions=days,
                    history_rows=candidate["history_rows"],
                    namespace_id=page["namespace_id"],
                    package_sha256=package_sha256,
                    preview_context=structure_preview_context,
                    decoded_by_session=history_inputs[3],
                )
                structure_core_preview_count += len(core_previews)
            result, decoded_count, core_count, range_check_count = (
                _pool_candidate_result(
                    candidate=candidate,
                    sessions=days,
                    target_index=target_index,
                    namespace_id=page["namespace_id"],
                    package_sha256=package_sha256,
                    calendar_id=calendar_id,
                    calendar_revision=target_calendar_revision,
                    history_inputs=history_inputs,
                    core_previews_by_session=core_previews,
                )
            )
            history_rows_decoded += decoded_count
            core_calculations += core_count
            generation_calls += 5 + 2 * range_check_count
            for kind in _RANGE_TYPES:
                evidence = result[kind].get("prior_anchor_evidence", {})
                range_history_status_counts[kind][
                    evidence.get("status", "UNKNOWN_DATA")
                ] += 1
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
            if include_source_symbol_structure_results:
                source_symbol_structure_results[candidate["source_symbol"]] = {
                    "history_row_count": len(candidate["history_rows"]),
                    "market": candidate.get("market"),
                    "board": candidate.get("board"),
                    "target_source": detail["target_source"],
                    "structures": {
                        kind: {
                            "state": outcome["state"],
                            "reason": outcome.get("reason"),
                            "anchor": outcome.get("anchor"),
                        }
                        for kind, outcome in result.items()
                    },
                }

            if structure_changes:
                if structure_preview_context is None or previous_session is None:
                    raise PgResearchBindingError(
                        "D1_STRUCTURE_CHANGE_CONTEXT_UNAVAILABLE",
                        "全池结构变化缺少相邻会话Core上下文",
                    )
                timeline = evaluate_source_d1_structure_timeline(
                    symbol=candidate["source_symbol"],
                    sessions=days,
                    history_rows=candidate["history_rows"],
                    namespace_id=page["namespace_id"],
                    package_sha256=package_sha256,
                    core_previews_by_session=core_previews,
                    history_inputs=history_inputs,
                )
                candidate_changes = _structure_timeline_changes(
                    timeline=timeline,
                    previous_session=previous_session,
                    target_session=target_session,
                )
                candidate_changes.sort(
                    key=lambda item: (
                        item["source_symbol"],
                        item["type"],
                        item["created_session"],
                        str(item["anchor_pairing_identity"]["lower"]),
                        str(item["anchor_pairing_identity"]["upper"]),
                    )
                )
                symbol_change_summary = {
                    "anchor_count": len(candidate_changes),
                    "material_change_anchor_count": 0,
                    "anchor_counts_by_type": Counter(),
                    "material_change_anchor_counts_by_type": Counter(),
                    "material_change_status_counts": Counter(),
                    "material_change_status_counts_by_type": {
                        kind: Counter() for kind in STRUCTURE_TYPES
                    },
                    "material_change_reason_anchor_counts": Counter(),
                    "material_change_reason_anchor_counts_by_type": {
                        kind: Counter() for kind in STRUCTURE_TYPES
                    },
                }
                for change in candidate_changes:
                    change_status_counts[change["change_classification"]] += 1
                    material_status = change["material_change_classification"]
                    symbol_change_summary["anchor_counts_by_type"][change["type"]] += 1
                    symbol_change_summary["material_change_status_counts"][
                        material_status
                    ] += 1
                    symbol_change_summary["material_change_status_counts_by_type"][
                        change["type"]
                    ][material_status] += 1
                    if material_status == "MATERIAL_CHANGE":
                        symbol_change_summary["material_change_anchor_count"] += 1
                        symbol_change_summary["material_change_anchor_counts_by_type"][
                            change["type"]
                        ] += 1
                        for reason in change["material_change_reasons"]:
                            if reason not in MATERIAL_CHANGE_REASONS:
                                continue
                            symbol_change_summary[
                                "material_change_reason_anchor_counts"
                            ][reason] += 1
                            symbol_change_summary[
                                "material_change_reason_anchor_counts_by_type"
                            ][change["type"]][reason] += 1
                            material_change_reason_anchor_counts[reason] += 1
                            material_change_reason_anchor_counts_by_type[
                                change["type"]
                            ][reason] += 1
                            material_change_reason_source_symbols[reason].add(
                                candidate["source_symbol"]
                            )
                            material_change_reason_source_symbols_by_type[
                                change["type"]
                            ][reason].add(candidate["source_symbol"])
                    material_change_status_counts[material_status] += 1
                    material_change_status_counts_by_type[change["type"]][
                        material_status
                    ] += 1
                    support_status_counts[
                        change["support_change"]["classification"]
                    ] += 1
                    historical_anchor_count_by_type[change["type"]] += 1
                    if change["change_classification"] == "CHANGED":
                        changed_source_symbols.add(candidate["source_symbol"])
                    if change["material_change"]:
                        material_change_source_symbols.add(candidate["source_symbol"])
                    if (
                        (structure_type is None or change["type"] == structure_type)
                        and (
                            structure_change_status is None
                            or change["change_classification"]
                            == structure_change_status
                        )
                        and (not material_changes_only or change["material_change"])
                        and (
                            material_change_reason is None
                            or (
                                material_status == "MATERIAL_CHANGE"
                                and material_change_reason
                                in change["material_change_reasons"]
                            )
                        )
                    ):
                        if offset <= filtered_anchor_count < offset + limit:
                            historical_change_page.append(change)
                        filtered_anchor_count += 1
                source_symbol_change_summaries[candidate["source_symbol"]] = (
                    symbol_change_summary
                )
                if include_source_symbol_d1_details:
                    source_symbol_d1_details[candidate["source_symbol"]] = [
                        change
                        for change in candidate_changes
                        if change["material_change"]
                        or change["material_change_classification"]
                        in {"UNKNOWN", "NOT_COMPARABLE", "NOT_EVALUABLE"}
                    ]
                structure_timeline_candidate_count += 1
                generation_calls += 5 * len(days)
                if candidate["source_symbol"] == structure_symbol:
                    selected_symbol_anchor_changes = candidate_changes
                    detail["historical_anchor_changes"] = candidate_changes
                    detail["structure_timeline"] = {
                        key: value for key, value in timeline.items() if key != "points"
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
    page_events = (
        historical_change_page if structure_changes else events[offset : offset + limit]
    )
    page_total = filtered_anchor_count if structure_changes else len(events)
    selected_change_detail = (
        {
            "source_symbol": structure_symbol,
            "status": "AVAILABLE",
            "anchor_changes": selected_symbol_anchor_changes or [],
            "filtered_anchor_changes": [
                item
                for item in selected_symbol_anchor_changes or []
                if (structure_type is None or item["type"] == structure_type)
                and (
                    structure_change_status is None
                    or item["change_classification"] == structure_change_status
                )
                and (not material_changes_only or item["material_change"])
                and (
                    material_change_reason is None
                    or (
                        item["material_change"]
                        and material_change_reason in item["material_change_reasons"]
                    )
                )
            ],
        }
        if selected_symbol_anchor_changes is not None
        else {
            "source_symbol": structure_symbol,
            "status": "NOT_IN_TARGET_SOURCE_POOL"
            if structure_symbol is not None
            else "NOT_REQUESTED",
            "anchor_changes": [],
            "filtered_anchor_changes": [],
        }
        if structure_changes
        else None
    )
    if structure_changes and selected_detail is not None:
        selected_detail["historical_change_detail"] = selected_change_detail
    source_symbol_material_summary = None
    if structure_changes and include_source_symbol_change_summary:
        source_symbol_material_summary = {}
        for symbol in sorted(seen_symbols):
            summary = source_symbol_change_summaries.get(
                symbol,
                {
                    "anchor_count": 0,
                    "material_change_anchor_count": 0,
                    "anchor_counts_by_type": Counter(),
                    "material_change_anchor_counts_by_type": Counter(),
                    "material_change_status_counts": Counter(),
                    "material_change_status_counts_by_type": {
                        kind: Counter() for kind in STRUCTURE_TYPES
                    },
                    "material_change_reason_anchor_counts": Counter(),
                    "material_change_reason_anchor_counts_by_type": {
                        kind: Counter() for kind in STRUCTURE_TYPES
                    },
                },
            )
            statuses = summary["material_change_status_counts"]
            if statuses.get("MATERIAL_CHANGE", 0):
                group = "MATERIAL_CHANGE"
            elif statuses.get("UNKNOWN", 0):
                group = "UNKNOWN"
            elif statuses.get("NOT_COMPARABLE", 0):
                group = "NOT_COMPARABLE"
            elif statuses.get("NOT_EVALUABLE", 0):
                group = "NOT_EVALUABLE"
            elif sum(statuses.get(status, 0) for status in ("COUNT_ONLY", "UNCHANGED")):
                group = "KNOWN_NON_MATERIAL"
            else:
                group = "NO_PROVED_ANCHOR"
            source_symbol_material_summary[symbol] = {
                "group": group,
                "anchor_count": summary["anchor_count"],
                "material_change_anchor_count": summary["material_change_anchor_count"],
                "anchor_counts_by_type": dict(
                    sorted(summary["anchor_counts_by_type"].items())
                ),
                "material_change_anchor_counts_by_type": dict(
                    sorted(summary["material_change_anchor_counts_by_type"].items())
                ),
                "material_change_status_counts": dict(
                    sorted(summary["material_change_status_counts"].items())
                ),
                "material_change_status_counts_by_type": {
                    kind: dict(
                        sorted(
                            summary["material_change_status_counts_by_type"][
                                kind
                            ].items()
                        )
                    )
                    for kind in STRUCTURE_TYPES
                },
                "material_change_reason_anchor_counts": dict(
                    sorted(summary["material_change_reason_anchor_counts"].items())
                ),
                "material_change_reason_anchor_counts_by_type": {
                    kind: dict(
                        sorted(
                            summary["material_change_reason_anchor_counts_by_type"][
                                kind
                            ].items()
                        )
                    )
                    for kind in STRUCTURE_TYPES
                },
            }
    return {
        "ok": True,
        "status": "SOURCE_LEVEL_D1_STRUCTURE_CHANGE_DISCOVERY"
        if structure_changes
        else "SOURCE_LEVEL_D1_MULTI_STRUCTURE_DISCOVERY",
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
        "historical_anchor_changes": {
            "status": "COMPLETE_FULL_POOL_ADJACENT_SESSION_SCAN"
            if structure_changes
            else "NOT_REQUESTED",
            "material_change_contract": "SOURCE_D1_STRUCTURE_MATERIAL_CHANGE_R1",
            "previous_official_session": previous_session.isoformat()
            if previous_session
            else None,
            "target_session": target_session.isoformat(),
            "official_sessions_adjacent": previous_session is not None,
            "source_pool_symbol_denominator": source_pool_total,
            "evaluated_source_symbol_count": structure_timeline_candidate_count,
            "effective_old_anchor_count": sum(historical_anchor_count_by_type.values()),
            "effective_old_anchor_count_by_type": {
                kind: historical_anchor_count_by_type.get(kind, 0)
                for kind in STRUCTURE_TYPES
            },
            "support_comparable_anchor_count": support_status_counts.get("CHANGED", 0)
            + support_status_counts.get("UNCHANGED", 0),
            "support_status_counts": dict(sorted(support_status_counts.items())),
            "change_status_counts": dict(sorted(change_status_counts.items())),
            "field_change_status_counts": dict(sorted(change_status_counts.items())),
            "material_change_status_counts": dict(
                sorted(material_change_status_counts.items())
            ),
            "material_change_status_counts_by_type": {
                kind: dict(sorted(material_change_status_counts_by_type[kind].items()))
                for kind in STRUCTURE_TYPES
            },
            "material_change_reason_anchor_counts": dict(
                sorted(material_change_reason_anchor_counts.items())
            ),
            "material_change_reason_anchor_counts_by_type": {
                kind: dict(
                    sorted(material_change_reason_anchor_counts_by_type[kind].items())
                )
                for kind in STRUCTURE_TYPES
            },
            "material_change_reason_distinct_source_symbol_counts": {
                reason: len(symbols)
                for reason, symbols in sorted(
                    material_change_reason_source_symbols.items()
                )
            },
            "material_change_reason_distinct_source_symbol_counts_by_type": {
                kind: {
                    reason: len(symbols)
                    for reason, symbols in sorted(
                        material_change_reason_source_symbols_by_type[kind].items()
                    )
                }
                for kind in STRUCTURE_TYPES
            },
            "material_change_count": material_change_status_counts.get(
                "MATERIAL_CHANGE", 0
            ),
            "material_change_distinct_source_symbol_count": len(
                material_change_source_symbols
            ),
            "material_change_evaluable_anchor_count": sum(
                material_change_status_counts.get(status, 0)
                for status in ("MATERIAL_CHANGE", "COUNT_ONLY", "UNCHANGED")
            ),
            "material_change_denominator": sum(
                historical_anchor_count_by_type.values()
            ),
            "material_change_partition_closed": sum(
                material_change_status_counts.values()
            )
            == sum(historical_anchor_count_by_type.values()),
            "changed_anchor_count": change_status_counts.get("CHANGED", 0),
            "changed_distinct_source_symbol_count": len(changed_source_symbols),
            "field_changed_anchor_count": change_status_counts.get("CHANGED", 0),
            "field_changed_distinct_source_symbol_count": len(changed_source_symbols),
            "unknown_or_not_applicable_anchor_count": sum(
                change_status_counts.get(state, 0)
                for state in ("UNKNOWN", "NOT_APPLICABLE")
            ),
            "filters": {
                "structure_type": structure_type or "ALL",
                "material_change_reason": material_change_reason or "ALL",
                "change_status": structure_change_status or "ALL",
                "material_changes_only": material_changes_only,
                "applied_after_full_pool_evaluation": True,
            },
            "filtered_anchor_count": filtered_anchor_count,
            "source_symbol_anchor_details": selected_change_detail,
            "material_change_status_counts_by_type": {
                kind: dict(sorted(material_change_status_counts_by_type[kind].items()))
                for kind in STRUCTURE_TYPES
            },
            "source_symbol_material_summary": source_symbol_material_summary,
            "source_symbol_d1_details": (
                source_symbol_d1_details if include_source_symbol_d1_details else None
            ),
            "source_symbol_structure_results": (
                source_symbol_structure_results
                if include_source_symbol_structure_results
                else None
            ),
        },
        "range_previous_anchor_evidence_counts": {
            kind: dict(sorted(range_history_status_counts[kind].items()))
            for kind in sorted(_RANGE_TYPES)
        },
        "page": {
            "offset": offset,
            "limit": limit,
            "result_count": len(page_events),
            "event_total": page_total,
            "next_offset": offset + limit if offset + limit < page_total else None,
            "stable_order": "source_symbol ASC, type ASC, created_session ASC, lower ASC, upper ASC"
            if structure_changes
            else "source_symbol ASC, structure ASC",
            "pagination_unit": "anchor_event",
            "results": page_events,
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
            "status": "OBSERVED_FOR_ADJACENT_SESSIONS"
            if structure_changes
            else "NOT_AVAILABLE_AT_TARGET_WINDOW_END",
            "reason": "变化模式按同一历史锚比较相邻官方会话；填补单列且不构成正式失效"
            if structure_changes
            else "请求窗口截止于目标日；单股历史模式显示目标日之前已可见的后继状态",
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
            "raw_history_rows_decoded": history_rows_decoded,
            "raw_history_decode_passes": 1,
            "target_generation_raw_rows_decoded": history_rows_decoded,
            "structure_history_core_preview_sessions": structure_core_preview_count,
            "structure_timeline_candidates": structure_timeline_candidate_count,
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
    history_inputs=None,
) -> dict[str, Any]:
    """为单一来源代码沿已存会话生成锚并逐日观察后继，不回写PG。"""
    normalized = symbol.strip().upper()
    if history_inputs is None:
        history_inputs = _history_observations(
            symbol=normalized,
            sessions=sessions,
            history_rows=history_rows,
            namespace_id=namespace_id,
            package_sha256=package_sha256,
        )
    observations, bars, row_map, _decoded_by_session = history_inputs
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
                    range_anchor[kind] = (
                        anchor_models.get(kind)
                        if outcome.get("reason") == "RANGE_ANCHOR_LEFT_CENSORED"
                        else None
                    )
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
                "price_inputs": {
                    "actual": _render_field(actual),
                    "open": _render_field(observations[index].open),
                    "high": _render_field(observations[index].high),
                    "low": _render_field(observations[index].low),
                    "close": _render_field(observations[index].close),
                    "price_coordinate": "RAW_UNADJUSTED",
                },
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
                    "low": _render_field(observations[index].low),
                    "price_coordinate": "RAW_UNADJUSTED",
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
                "source_symbol": normalized,
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


def discover_source_d1_prior_support_reference(
    *,
    symbol: str,
    sessions: Sequence[date],
    history_rows: Sequence[Mapping[str, Any]],
    namespace_id: str,
    package_sha256: str,
    source_content_id: str,
    parser_contract: str | None = None,
) -> dict[str, Any]:
    """按单股历史生成截至窗口末会话已可见的来源支撑参考。"""
    days = tuple(sessions)
    if (
        not days
        or tuple(sorted(set(days))) != days
        or len(days) > 75
        or not isinstance(source_content_id, str)
        or not source_content_id
    ):
        raise PgResearchBindingError(
            "D1_SOURCE_PRIOR_REFERENCE_REQUEST_INVALID",
            "单股D1前态需要完整内容身份及最多75个递增官方会话",
        )
    normalized = symbol.strip().upper()
    history_inputs = _history_observations(
        symbol=normalized,
        sessions=days,
        history_rows=history_rows,
        namespace_id=namespace_id,
        package_sha256=package_sha256,
    )
    core_previews = _structure_history_core_previews(
        symbol=normalized,
        sessions=days,
        history_rows=history_rows,
        namespace_id=namespace_id,
        package_sha256=package_sha256,
        preview_context=prepare_source_d1_preview_context(days),
        decoded_by_session=history_inputs[3],
    )
    timeline = evaluate_source_d1_structure_timeline(
        symbol=normalized,
        sessions=days,
        history_rows=history_rows,
        namespace_id=namespace_id,
        package_sha256=package_sha256,
        core_previews_by_session=core_previews,
        history_inputs=history_inputs,
    )
    prior_session = days[-1]
    supported_types = {"PIVOT_LOW", "RANGE_FLOOR", "GAP_ZONE"}
    references = []
    for event in timeline["events"]:
        if (
            event.get("relation") != "SUPPORT"
            or event.get("type") not in supported_types
            or date.fromisoformat(event["created_session"]) >= prior_session
            or event.get("coordinate_id") != f"RAW:TDX_SOURCE_SYMBOL:{normalized}"
        ):
            continue
        observation = next(
            (
                item
                for item in event.get("support_observations", [])
                if item.get("session") == prior_session.isoformat()
            ),
            None,
        )
        if not isinstance(observation, Mapping):
            continue
        quality_reasons = _observation_quality_reasons(observation)
        gap_fill_observation = next(
            (
                item
                for item in event.get("gap_fill_observations", [])
                if item.get("session") == prior_session.isoformat()
            ),
            None,
        )
        gap_fill_quality_reasons = []
        if isinstance(gap_fill_observation, Mapping):
            if gap_fill_observation.get("quality") != "OBSERVED":
                gap_fill_quality_reasons.append(
                    "GAP_FILL_STATE:"
                    + str(
                        gap_fill_observation.get("reason")
                        or gap_fill_observation.get("quality")
                        or "UNKNOWN"
                    )
                )
        source = observation.get("source")
        birth_source = event.get("source_row")
        for label, row in (("OBSERVATION", source), ("BIRTH", birth_source)):
            if not isinstance(row, Mapping):
                quality_reasons.append(f"{label}_SOURCE_IDENTITY_MISSING")
                continue
            if row.get("namespace_id") != namespace_id:
                quality_reasons.append(f"{label}_NAMESPACE_MISMATCH")
            if row.get("package_sha256") != package_sha256:
                quality_reasons.append(f"{label}_PACKAGE_SHA_MISMATCH")
            if row.get("source_content_id") != source_content_id:
                quality_reasons.append(f"{label}_SOURCE_CONTENT_MISMATCH")
            if (
                parser_contract is not None
                and row.get("parser_contract") != parser_contract
            ):
                quality_reasons.append(f"{label}_PARSER_CONTRACT_MISMATCH")
        if source is None or source.get("session") != prior_session.isoformat():
            quality_reasons.append("OBSERVATION_SOURCE_SESSION_MISMATCH")
        quality_reasons = sorted(set(quality_reasons))
        observed = observation.get("state") != "UNKNOWN" and not quality_reasons
        references.append(
            {
                "source_symbol": normalized,
                "anchor_id": event.get("anchor_id"),
                "type": event.get("type"),
                "relation": event.get("relation"),
                "anchor_session": event.get("created_session"),
                "available_from_session": event.get("available_from_session"),
                "observable_session": prior_session.isoformat(),
                "coordinate_id": event.get("coordinate_id"),
                "price_coordinate": event.get("price_coordinate"),
                "identity_status": event.get("identity_status"),
                "lower": event.get("lower"),
                "upper": event.get("upper"),
                "state": observation.get("state", "UNKNOWN"),
                "acceptance": observation.get("acceptance", "UNKNOWN"),
                "freshness": observation.get("freshness", "UNKNOWN"),
                "breach_streak": observation.get("breach_streak"),
                "acceptance_streak": observation.get("acceptance_streak"),
                "test_count": observation.get("test_count"),
                "separated_sessions": observation.get("separated_sessions"),
                "retest_qualified": observation.get("retest_qualified"),
                "broken": observation.get("state") == "BROKEN"
                or observation.get("acceptance") == "BROKEN",
                "gap_fill": (
                    {
                        "state": gap_fill_observation.get("state"),
                        "quality": gap_fill_observation.get("quality"),
                        "reason": gap_fill_observation.get("reason"),
                        "source": gap_fill_observation.get("source"),
                    }
                    if isinstance(gap_fill_observation, Mapping)
                    else None
                ),
                "quality": "OBSERVED" if observed else "UNKNOWN",
                "quality_reasons": quality_reasons,
                "gap_fill_quality_reasons": gap_fill_quality_reasons,
                "generation_evidence": event.get("generation_evidence"),
                "source": source,
                "birth_source": birth_source,
                "research_mode": "NON_PIT_RETROSPECTIVE",
                "price_coordinate_status": "RAW_UNADJUSTED",
                "formal_parent": "NOT_FORMAL_PARENT",
            }
        )
    usable = [
        item
        for item in references
        if item["quality"] == "OBSERVED"
        and item["state"]
        in {
            "IDLE",
            "APPROACHING",
            "TESTING",
            "HELD_TENTATIVE",
            "RETESTING",
            "HELD_CONFIRMED",
            "BREACHED_SHALLOW",
            "RECLAIMED",
        }
    ]
    return {
        "source_symbol": normalized,
        "namespace_id": namespace_id,
        "package_sha256": package_sha256,
        "source_content_id": source_content_id,
        "parser_contract": parser_contract,
        "state": "TRUE" if usable else "UNKNOWN",
        "reason": "SOURCE_SUPPORT_OBSERVED"
        if usable
        else "SOURCE_SUPPORT_REFERENCES_ONLY_BROKEN_OR_UNKNOWN"
        if references
        else "NO_OBSERVED_SOURCE_SUPPORT_IN_WINDOW_LEFT_CENSORED",
        "source_prior_support_observed": "TRUE"
        if any(item["quality"] == "OBSERVED" for item in references)
        else "UNKNOWN",
        "source_prior_support_reason": "OBSERVED_SOURCE_SUPPORT_REFERENCE"
        if any(item["quality"] == "OBSERVED" for item in references)
        else "NO_QUALIFYING_SUPPORT_PROVEN_WITHIN_LEFT_CENSORED_WINDOW",
        "formal_parent": "NOT_FORMAL_PARENT",
        "references": references,
        "window": {
            "start_session": days[0].isoformat(),
            "prior_session": prior_session.isoformat(),
            "official_session_count": len(days),
            "left_censored_at_window_start": len(days) < 75,
            "future_session_rows_used": 0,
        },
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "research_mode": "NON_PIT_RETROSPECTIVE",
        "price_coordinate": "RAW_UNADJUSTED",
        "raw_history_rows_decoded": len(history_inputs[2]),
        "database_write_attempted": False,
        "ddl_executed": False,
        "dml_executed": False,
    }
