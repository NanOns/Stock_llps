"""Stock A研究support职责模块。"""

from __future__ import annotations

from datetime import date, timedelta
from fractions import Fraction
from hashlib import sha256
import re
from typing import Any, Iterable, Mapping, Sequence
from stock_a.domain import ContractError, FieldValue, exact_number, unknown
from stock_a.sources.tdx.day import DAY_STRUCT, DecodedDay, UnitContract

from stock_a.research.entry_core.fields import _render_field, _render_value
from stock_a.research.entry_context.errors import ResearchEntryError
from stock_a.research.entry_context.source_discovery import _normalize_symbol
from stock_a.research.structure import Bar


def _field_from_pg_raw(
    row: Mapping[str, Any],
    name: str,
    quality_column: str,
    reason_column: str,
    *,
    raw_quality: bool = False,
) -> FieldValue:
    value = row.get(name)
    quality = row.get(quality_column)
    if (
        quality
        in ({"RAW_SOURCE_VALUE_OBSERVED", "OBSERVED"} if raw_quality else {"OBSERVED"})
        and value is not None
    ):
        return FieldValue(value)
    reason = row.get(reason_column)
    if not isinstance(reason, str) or not reason:
        reason = "PG_RAW_VALUE_NOT_OBSERVED"
    return unknown(reason)


def _decoded_day_from_pg_raw(row: Mapping[str, Any]) -> DecodedDay | None:
    if row.get("bar_status") != "TARGET_PRESENT":
        return None
    session = row.get("session_date")
    if type(session) is not date:
        raise ResearchEntryError("PG_RAW_SESSION_INVALID", "PG来源Raw行缺少date会话")
    fields = tuple(
        _field_from_pg_raw(row, column, "price_quality", "price_reason")
        for column in ("raw_open", "raw_high", "raw_low", "raw_close")
    )
    if all(item.quality == "OBSERVED" for item in fields):
        prices = FieldValue(tuple(item.value for item in fields))
    else:
        prices = unknown(
            next(
                (item.reason for item in fields if item.quality != "OBSERVED"),
                "PRICE_QUALITY_UNKNOWN",
            )
        )
    raw_amount = _field_from_pg_raw(
        row,
        "raw_amount",
        "raw_amount_quality",
        "raw_amount_reason",
        raw_quality=True,
    )
    raw_volume = _field_from_pg_raw(
        row,
        "raw_volume",
        "raw_volume_quality",
        "raw_volume_reason",
        raw_quality=True,
    )
    amount = (
        unknown("UNIT_AUTHORITY_NOT_VERIFIED")
        if raw_amount.quality == "OBSERVED"
        else unknown(raw_amount.reason or "DATA_INVALID_AMOUNT")
    )
    volume = (
        unknown("UNIT_AUTHORITY_NOT_VERIFIED")
        if raw_volume.quality == "OBSERVED"
        else unknown(raw_volume.reason or "DATA_INVALID_VOLUME")
    )
    return DecodedDay(
        session_date=session,
        prices=prices,
        raw_open=fields[0],
        raw_high=fields[1],
        raw_low=fields[2],
        raw_close=fields[3],
        amount=amount,
        volume=volume,
        reserved=0,
        unit_version="TDX_RAW_SOURCE_VALUES_UNIT_UNVERIFIED_R1",
        raw_amount=raw_amount,
        raw_volume=raw_volume,
    )


def _support_field_from_rendered(value: Mapping[str, Any], label: str) -> FieldValue:
    quality = value.get("quality")
    if quality == "OBSERVED":
        raw = value.get("value")
        if isinstance(raw, Mapping):
            raw = raw.get("exact")
        try:
            if isinstance(raw, bool) or raw is None:
                raise ValueError("invalid exact value")
            return FieldValue(Fraction(str(raw)))
        except (TypeError, ValueError, ZeroDivisionError) as error:
            raise ResearchEntryError(
                "SUPPORT_FIELD_VALUE_INVALID", f"支撑字段{label}不是精确有限数值"
            ) from error
    if quality not in {"UNKNOWN_DATA", "NOT_APPLICABLE"}:
        raise ResearchEntryError(
            "SUPPORT_FIELD_QUALITY_INVALID", f"支撑字段{label}质量状态无效"
        )
    reason = value.get("reason")
    if not isinstance(reason, str) or not reason:
        reason = f"{label.upper().replace('.', '_')}_QUALITY_UNKNOWN"
    return FieldValue(None, quality, reason)


def _support_evidence(
    *,
    lower: Fraction,
    upper: Fraction,
    atr_previous: FieldValue,
    today: Bar,
    clv: FieldValue,
) -> dict[str, Any]:
    """仅投影冻结阈值及原值比较，状态仍完全由structure.observe_support生成。"""
    if atr_previous.quality != "OBSERVED":
        return {
            "quality": atr_previous.quality,
            "reason": atr_previous.reason,
            "atr_previous": _render_field(atr_previous),
        }
    scale = exact_number(atr_previous.value)
    if scale <= 0:
        return {
            "quality": "INVALID_VALUE",
            "reason": "SUPPORT_KERNEL_REQUIRES_POSITIVE_PREVIOUS_ATR",
            "atr_previous": _render_field(atr_previous),
        }
    if today.ohlc.quality != "OBSERVED" or clv.quality != "OBSERVED":
        return {
            "quality": "UNKNOWN_DATA",
            "reason": (
                today.ohlc.reason
                if today.ohlc.quality != "OBSERVED"
                else clv.reason or "CLV_UNKNOWN"
            ),
            "atr_previous": _render_field(atr_previous),
            "clv": _render_field(clv),
        }
    ohlc_values = today.ohlc.value
    if not isinstance(ohlc_values, (tuple, list)) or len(ohlc_values) != 4:
        raise ResearchEntryError(
            "SUPPORT_BAR_OHLC_INVALID", "支撑观察Bar必须含四个有序OHLC字段"
        )
    open_value, high, low, close = (exact_number(value) for value in ohlc_values)
    touch_low = lower - scale / 4
    touch_high = upper + scale / 4
    breach_below = lower - scale / 2
    deep_breach_below = lower - 3 * scale / 2
    distance = min(abs(close - lower), abs(close - upper))
    return {
        "quality": "OBSERVED",
        "reason": None,
        "ohlc": {
            "open": _render_value(open_value),
            "high": _render_value(high),
            "low": _render_value(low),
            "close": _render_value(close),
        },
        "atr_previous": _render_field(atr_previous),
        "clv": _render_field(clv),
        "touch_band": {
            "low": _render_value(touch_low),
            "high": _render_value(touch_high),
        },
        "close_breach_below_strict": _render_value(breach_below),
        "deep_breach_below_strict": _render_value(deep_breach_below),
        "reclaim": {
            "close_at_or_above": _render_value(upper),
            "clv_at_least": _render_value(Fraction(1, 2)),
        },
        "approach": {
            "nearest_zone_distance": _render_value(distance),
            "threshold": _render_value(scale),
        },
    }


def _support_observation_reason(
    observation: Any,
    *,
    prior: Any,
    unknown_reasons: Sequence[str],
) -> str:
    if observation.state == "UNKNOWN":
        return "支撑观察依赖未知，保留UNKNOWN与STALE；" + "；".join(unknown_reasons)
    if observation.state == "BROKEN":
        return "现役核判定BROKEN（深度破位或相邻两次浅破）；本日与相邻日阈值原值见numeric_evidence。"
    if observation.state == "BREACHED_SHALLOW":
        return (
            "现役核判定BREACHED_SHALLOW；本日收盘与浅破、深破阈值见numeric_evidence。"
        )
    if observation.state == "HELD_CONFIRMED":
        return "现役核确认分离后复测；分离会话、复测资格与计数见本观察前态字段。"
    if observation.state == "RECLAIMED":
        return (
            "现役核判定RECLAIMED；本日OHLC、平台上沿与CLV收复门槛见numeric_evidence。"
        )
    if observation.state == "HELD_TENTATIVE":
        return "前态为RECLAIMED，后续会话收盘仍不低于upper。"
    if observation.state == "RETESTING":
        return "前态已进入可复测状态，本会话触碰但未收复。"
    if observation.state == "TESTING":
        return "现役核判定TESTING；本日OHLC及支撑触碰带边界见numeric_evidence。"
    if observation.state == "APPROACHING":
        return "现役核判定APPROACHING；收盘到平台距离及前态ATR见numeric_evidence。"
    if prior is not None and observation.state == prior.state:
        return "本会话未触发更高优先级转换，保留上一会话可用状态。"
    return f"现役支撑观察核输出{observation.state}。"


def research_stock_pg_support_followup_preview(
    *,
    symbol: str,
    target_session: date,
    support_anchor_session: date,
    official_sessions: Sequence[date],
    requested_sessions: Sequence[date],
    history_points: Sequence[Mapping[str, Any]],
    package_sha256: str,
    source_context: Mapping[str, Any],
    official_calendar: Mapping[str, Any],
) -> dict[str, Any]:
    """把单股精确PG历史D1证据接到冻结BREAKOUT_PLATFORM与支撑观察核。"""
    from stock_a.research.structure import generate, observe_support

    normalized = _normalize_symbol(symbol)[0]
    sessions = tuple(official_sessions)
    requested = tuple(requested_sessions)
    points = tuple(history_points)
    if (
        source_context.get("status") != "AVAILABLE"
        or source_context.get("target_session") != target_session.isoformat()
        or source_context.get("source_content_id") is None
        or source_context.get("source_capture_id") is None
        or source_context.get("raw_row_count")
        != source_context.get("capture_expected_row_count")
        or not isinstance(source_context.get("capture_digest"), str)
        or re.fullmatch(r"[0-9a-f]{64}", source_context["capture_digest"]) is None
        or not isinstance(package_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", package_sha256) is None
        or source_context.get("package_sha256") != package_sha256
        or not isinstance(official_calendar, Mapping)
        or not official_calendar.get("calendar_id")
        or not official_calendar.get("calendar_revision")
        or not official_calendar.get("calendar_sha256")
    ):
        raise ResearchEntryError(
            "SUPPORT_SOURCE_BINDING_INVALID", "支撑研究没有绑定同一Raw捕获与包版本"
        )
    if (
        not sessions
        or len(sessions) > 75
        or sessions[-1] != target_session
        or tuple(sorted(set(sessions))) != sessions
        or any(type(day) is not date for day in sessions)
        or support_anchor_session not in sessions
        or support_anchor_session >= target_session
        or not requested
        or requested[-1] != target_session
        or tuple(sorted(set(requested))) != requested
        or any(day not in sessions for day in requested)
        or not requested[0] <= support_anchor_session < target_session
    ):
        raise ResearchEntryError(
            "SUPPORT_SESSION_RANGE_INVALID", "平台出生日或后继会话不在官方历史范围"
        )
    if (
        tuple(day for day in sessions if requested[0] <= day <= target_session)
        != requested
    ):
        raise ResearchEntryError(
            "SUPPORT_SESSION_GAP", "支撑时间线遗漏官方会话，不能跨日推进"
        )
    if tuple(point.get("session") for point in points) != requested:
        raise ResearchEntryError(
            "SUPPORT_HISTORY_POINTS_INVALID", "支撑D1证据没有覆盖请求的连续会话"
        )

    namespace_id = source_context.get("namespace_id")
    coordinate_id = f"RAW:TDX_SOURCE_SYMBOL:{normalized}"
    point_by_session: dict[date, Mapping[str, Any]] = {}
    row_by_session: dict[date, Mapping[str, Any] | None] = {}
    for point in points:
        day = point.get("session")
        preview = point.get("preview")
        row = point.get("row")
        if (
            type(day) is not date
            or not isinstance(preview, Mapping)
            or day in point_by_session
            or preview.get("symbol") != normalized
            or preview.get("target_session") != day.isoformat()
            or preview.get("identity_status") != "SOURCE_SYMBOL_ONLY"
            or preview.get("price_coordinate") != "RAW_UNADJUSTED"
            or (
                point.get("history_max_session") is not None
                and point.get("history_max_session") > day
            )
            or (
                row is not None
                and (
                    row.get("session_date") != day
                    or row.get("source_symbol") != normalized
                    or row.get("package_sha256") != package_sha256
                    or row.get("namespace_id") != namespace_id
                )
            )
        ):
            raise ResearchEntryError(
                "SUPPORT_HISTORY_BINDING_INVALID", "支撑历史混入错日、错包或错来源代码"
            )
        predicate = preview.get("raw_d1_breakout_predicate")
        if (
            not isinstance(predicate, Mapping)
            or predicate.get("target_session") != day.isoformat()
            or predicate.get("coordinate_id") != coordinate_id
            or predicate.get("price_coordinate") != "RAW_UNADJUSTED"
            or predicate.get("official_dependency_window", {}).get("end_session")
            != day.isoformat()
        ):
            raise ResearchEntryError(
                "SUPPORT_D1_BINDING_INVALID", "D1 Core前态越过当前会话或坐标不符"
            )
        point_by_session[day] = point
        row_by_session[day] = row

    anchor_index = sessions.index(support_anchor_session)
    anchor_point = point_by_session[support_anchor_session]
    anchor_preview = anchor_point["preview"]
    anchor_predicate = anchor_preview["raw_d1_breakout_predicate"]
    d1 = anchor_predicate["predicates"]
    d1_state = d1["result"].get("state")
    source_identity = {
        "namespace_id": namespace_id,
        "source_capture_id": source_context.get("source_capture_id"),
        "package_sha256": package_sha256,
        "source_symbol": normalized,
        "anchor_session": support_anchor_session.isoformat(),
    }
    base = {
        "contract": "SOURCE_SYMBOL_BREAKOUT_PLATFORM_SUPPORT_FOLLOWUP_R1",
        "status": "NOT_GENERATED",
        "scope": "SOURCE_SYMBOL_SCOPED_NONFORMAL",
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "stable_security_id_issued": False,
        "source_symbol": normalized,
        "support_anchor_session": support_anchor_session.isoformat(),
        "target_session": target_session.isoformat(),
        "research_mode": "NON_PIT_RETROSPECTIVE",
        "price_coordinate": "RAW_UNADJUSTED",
        "coordinate_id": coordinate_id,
        "future_session_rows_used": 0,
        "formal_anchor_created": False,
        "formal_p15_admission": "NOT_EVALUATED",
        "source": {
            "namespace_id": namespace_id,
            "package_sha256": package_sha256,
            "source_content_id": source_context.get("source_content_id"),
            "source_capture_id": source_context.get("source_capture_id"),
            "source_digest": source_context.get("source_digest"),
            "source_raw_digest": source_context.get("source_raw_digest"),
            "source_clock_role": source_context.get("source_clock_role"),
            "source_time_quality": source_context.get("source_time_quality"),
            "source_available_at": source_context.get("source_available_at"),
            "capture_read_started_at": source_context.get("capture_read_started_at"),
            "capture_validated_at": source_context.get("capture_validated_at"),
        },
        "official_calendar": {
            "calendar_id": official_calendar.get("calendar_id"),
            "source_calendar_revision": official_calendar.get("calendar_revision"),
            "calendar_sha256": official_calendar.get("calendar_sha256"),
            "history_calendar_revision": anchor_predicate[
                "official_dependency_window"
            ].get("calendar_revision"),
            "anchor_window_start": anchor_predicate["official_dependency_window"].get(
                "start_session"
            ),
            "anchor_window_end": anchor_predicate["official_dependency_window"].get(
                "end_session"
            ),
            "requested_session_count": len(requested),
        },
        "d1_birth_evidence": {
            "predicate": d1,
            "inputs": anchor_predicate.get("inputs"),
            "source_capture_id": (
                str(row_by_session[support_anchor_session]["source_capture_id"])
                if row_by_session[support_anchor_session]
                else None
            ),
            "source_entry_sha256": (
                row_by_session[support_anchor_session].get("source_entry_sha256")
                if row_by_session[support_anchor_session]
                else None
            ),
        },
        "formal_downstream": {
            "stable_security_identity": "NOT_ISSUED",
            "p15_anchor": "NOT_CREATED",
            "d2_seed_focus": "NOT_EVALUATED",
            "trade_signal": "NOT_EVALUATED",
        },
        "execution": {
            "database_transaction": "REPEATABLE_READ_READ_ONLY",
            "database_write_attempted": False,
            "ddl_executed": False,
            "dml_executed": False,
            "temporary_computation": "IN_MEMORY_AND_STDOUT_ONLY",
        },
    }
    if d1_state != "TRUE":
        base["reason_code"] = f"D1_BREAKOUT_{d1_state or 'UNKNOWN'}"
        base["reason"] = "显式平台出生会话的现役原始D1谓词不是TRUE；不生成结构锚。"
        return base
    if any(d1.get(key, {}).get("state") != "TRUE" for key in ("p1", "p2")):
        raise ResearchEntryError(
            "SUPPORT_D1_RESULT_INCONSISTENT", "D1 TRUE结果与P1/P2明细不一致"
        )

    inputs = anchor_predicate.get("inputs", {})
    prior_high = _support_field_from_rendered(
        inputs.get("core.prior_high20", {}), "core.prior_high20"
    )
    atr_today = _support_field_from_rendered(inputs.get("core.atr20", {}), "core.atr20")
    clv_today = _support_field_from_rendered(inputs.get("core.clv", {}), "core.clv")
    if any(field.quality != "OBSERVED" for field in (prior_high, atr_today, clv_today)):
        base["status"] = "NOT_EVALUABLE"
        base["reason_code"] = "D1_TRUE_WITH_REQUIRED_STRUCTURE_FIELD_UNKNOWN"
        base["reason"] = "D1与结构生成所需Core字段质量不一致，拒绝补造结构输入。"
        return base
    atr_value = exact_number(atr_today.value)
    if atr_value <= 0:
        base["status"] = "NOT_EVALUABLE"
        base["reason_code"] = "BREAKOUT_PLATFORM_REQUIRES_POSITIVE_ATR"
        base["reason"] = (
            "原始D1可能在ATR20为零时仍为TRUE；冻结结构生成器要求正尺度，"
            "按合同保留D1证据并停止平台消费者，不改写ATR。"
        )
        return base

    anchor_row = row_by_session[support_anchor_session]
    decoded_anchor = _decoded_day_from_pg_raw(anchor_row) if anchor_row else None
    if decoded_anchor is None or decoded_anchor.prices.quality != "OBSERVED":
        base["status"] = "NOT_EVALUABLE"
        base["reason_code"] = "BREAKOUT_PLATFORM_ACTUAL_BAR_REQUIRED"
        base["reason"] = "D1 TRUE出生会话缺少可验证Raw OHLC actual Bar。"
        return base
    symbol_scope = "|".join(str(source_identity[key]) for key in source_identity)
    shadow_entity_id = (
        "SOURCE_SYMBOL_SCOPED_NONFORMAL:"
        + sha256(symbol_scope.encode("utf-8")).hexdigest()
    )
    source_ref = "PG_SOURCE_RAW:" + ":".join(
        (
            str(namespace_id),
            package_sha256,
            str(source_context.get("source_capture_id")),
            normalized,
            support_anchor_session.isoformat(),
            str(anchor_row.get("source_capture_id")),
        )
    )
    dependency = anchor_predicate["official_dependency_window"]
    generated = generate(
        "BREAKOUT_PLATFORM",
        entity_id=shadow_entity_id,
        ordinal=anchor_index,
        coordinate_id=coordinate_id,
        source_ref=source_ref,
        window_ref=(
            "D1_CORE_WINDOW:"
            + str(dependency.get("start_session"))
            + ":"
            + str(dependency.get("end_session"))
            + ":"
            + package_sha256
        ),
        actual=FieldValue(True),
        today=Bar(anchor_index, decoded_anchor.prices),
        inputs={
            "prior_high20": prior_high,
            "atr_today": atr_today,
            "clv": clv_today,
        },
        capability_state="READY",
    )
    if generated.quality != "OBSERVED":
        base["status"] = "NOT_GENERATED"
        base["reason_code"] = generated.reason or generated.quality
        base["reason"] = "现役BREAKOUT_PLATFORM生成器未返回合法平台。"
        return base
    anchor = generated.value
    if anchor.kind != "BREAKOUT_PLATFORM" or anchor.relation != "SUPPORT":
        raise ResearchEntryError(
            "SUPPORT_GENERATED_ANCHOR_INVALID", "结构核返回的锚类型或方向不符"
        )
    lower = FieldValue(exact_number(anchor.lower))
    upper = FieldValue(exact_number(anchor.upper))
    if lower.value != upper.value:
        raise ResearchEntryError(
            "SUPPORT_PLATFORM_NOT_FIXED", "BREAKOUT_PLATFORM价位应固定为严格前高"
        )

    first_test = sessions[anchor_index + 1]
    observations = []
    prior_observation = None
    for day in sessions[anchor_index : sessions.index(target_session) + 1]:
        point = point_by_session[day]
        preview = point["preview"]
        row = row_by_session[day]
        day_inputs = preview["raw_d1_breakout_predicate"].get("inputs", {})
        clv = _support_field_from_rendered(day_inputs.get("core.clv", {}), "core.clv")
        if day == support_anchor_session:
            atr_previous = unknown("ANCHOR_BIRTH_DAY_NOT_EVALUATED")
        else:
            previous_day = sessions[sessions.index(day) - 1]
            previous_inputs = point_by_session[previous_day]["preview"][
                "raw_d1_breakout_predicate"
            ].get("inputs", {})
            atr_previous = _support_field_from_rendered(
                previous_inputs.get("core.atr20", {}), "core.atr20"
            )
        decoded = _decoded_day_from_pg_raw(row) if row else None
        if decoded is None:
            reason = (
                (row or {}).get("bar_reason")
                or (row or {}).get("price_reason")
                or (
                    "UNKNOWN_HISTORY_SESSION_NOT_STORED"
                    if row is None
                    else "TARGET_BAR_MISSING_OR_READ_FAILED"
                )
            )
            ohlc = unknown(reason)
            actual = unknown(reason)
        else:
            ohlc = decoded.prices
            actual = (
                FieldValue(True)
                if decoded.prices.quality == "OBSERVED"
                else unknown(decoded.prices.reason or "PRICE_QUALITY_UNKNOWN")
            )
            if actual.value is True:
                price_values = tuple(
                    exact_number(value) for value in decoded.prices.value
                )
                if min(price_values) <= 0 or not (
                    price_values[2]
                    <= min(price_values[0], price_values[3])
                    <= max(price_values[0], price_values[3])
                    <= price_values[1]
                ):
                    raise ResearchEntryError(
                        "SUPPORT_RAW_OHLC_INVALID", "支撑观察Raw OHLC不满足价格合同"
                    )
        bar = Bar(sessions.index(day), ohlc)
        evidence = (
            {"quality": "NOT_EVALUATED", "reason": "ANCHOR_BIRTH_DAY_NO_SELF_TEST"}
            if day == support_anchor_session
            else _support_evidence(
                lower=exact_number(lower.value),
                upper=exact_number(upper.value),
                atr_previous=atr_previous,
                today=bar,
                clv=clv,
            )
        )
        if day != support_anchor_session and evidence.get("quality") == "INVALID_VALUE":
            raise ResearchEntryError(
                "SUPPORT_ATR_PREVIOUS_NONPOSITIVE",
                "现役observe_support要求正的上一会话ATR；当前零/负值不改标未知或伪造状态",
            )
        observation = observe_support(
            anchor,
            today=bar,
            prior=prior_observation,
            lower=lower,
            upper=upper,
            atr_previous=atr_previous,
            clv=clv,
            actual=actual,
            capability_state="READY",
        )
        unknown_reasons = [
            field.reason
            for field in (actual, atr_previous, clv)
            if field.quality != "OBSERVED" and field.reason
        ]
        observation_reason = _support_observation_reason(
            observation,
            prior=prior_observation,
            unknown_reasons=unknown_reasons,
        )
        if day == support_anchor_session:
            observation_reason = "突破平台出生会话仅建立锚；不进行同日支撑自证。"
        observations.append(
            {
                "session": day.isoformat(),
                "role": "ANCHOR_BIRTH"
                if day == support_anchor_session
                else "FOLLOWUP_OBSERVATION",
                "source_capture_id": str(row["source_capture_id"]) if row else None,
                "source_entry_sha256": row.get("source_entry_sha256") if row else None,
                "source_clock_role": (
                    row.get("capture_clock_role") or row.get("source_clock_role")
                    if row
                    else None
                ),
                "source_available_at": row.get("source_available_at") if row else None,
                "source_observed_started_at": row.get("observed_started_at")
                if row
                else None,
                "source_observed_completed_at": row.get("observed_completed_at")
                if row
                else None,
                "target_bar_status": preview.get("target_bar_status"),
                "price_quality": (row or {}).get("price_quality", "UNKNOWN"),
                "bar_reason": (row or {}).get("bar_reason")
                or (row or {}).get("price_reason"),
                "raw_ohlc": {
                    name: _render_field(getattr(decoded, f"raw_{name}"))
                    if decoded is not None
                    else _render_field(unknown(ohlc.reason or "RAW_BAR_UNAVAILABLE"))
                    for name in ("open", "high", "low", "close")
                },
                "prior_dependencies": {
                    "prior_official_session": None
                    if day == support_anchor_session
                    else sessions[sessions.index(day) - 1].isoformat(),
                    "atr20_previous": _render_field(atr_previous),
                    "clv_today": _render_field(clv),
                    "quality": evidence.get("quality"),
                    "reason": evidence.get("reason"),
                },
                "numeric_evidence": evidence,
                "support_observation": {
                    "state": observation.state,
                    "acceptance": observation.acceptance,
                    "freshness": observation.freshness,
                    "breach_streak": observation.breach_streak,
                    "acceptance_streak": observation.acceptance_streak,
                    "test_count": observation.test_count,
                    "separated_sessions": observation.separated_sessions,
                    "retest_qualified": observation.retest_qualified,
                    "quality": "UNKNOWN_DATA"
                    if observation.state == "UNKNOWN"
                    else "OBSERVED",
                    "reason": observation_reason,
                },
            }
        )
        prior_observation = observation

    base.update(
        {
            "status": "GENERATED_AND_OBSERVED",
            "platform_generation": {
                "state": "GENERATED",
                "kernel": "stock_a.research.structure.generate",
                "kind": anchor.kind,
                "relation": anchor.relation,
                "fixed_price_level": _render_value(exact_number(anchor.lower)),
                "lower": _render_value(exact_number(anchor.lower)),
                "upper": _render_value(exact_number(anchor.upper)),
                "created_session": support_anchor_session.isoformat(),
                "earliest_test_session": first_test.isoformat(),
                "price_coordinate": "RAW_UNADJUSTED",
                "shadow_structure": "SOURCE_SYMBOL_SCOPED_NONFORMAL",
                "formal_entity_id_emitted": False,
                "formal_anchor_id_emitted": False,
            },
            "support_kernel": "stock_a.research.structure.observe_support",
            "observation_count": len(observations),
            "observations": observations,
            "formal_anchor_created": False,
        }
    )
    return base


def classify_source_support_change(
    support_followup: Mapping[str, Any],
    *,
    previous_session: date,
    target_session: date,
    post_anchor_session_count: int,
) -> dict[str, Any]:
    """将同锚支撑历史两端状态归入来源批次变化类别。"""
    if (
        not isinstance(support_followup, Mapping)
        or type(previous_session) is not date
        or type(target_session) is not date
        or previous_session >= target_session
        or type(post_anchor_session_count) is not int
        or post_anchor_session_count < 0
    ):
        raise ResearchEntryError(
            "SUPPORT_CHANGE_INPUT_INVALID", "支撑变化比较输入或官方会话无效"
        )

    status = support_followup.get("status")
    if status == "NOT_EVALUABLE":
        return {
            "classification": "NOT_EVALUABLE",
            "reason_code": support_followup.get("reason_code")
            or "SUPPORT_STRUCTURE_NOT_EVALUABLE",
        }
    if status != "GENERATED_AND_OBSERVED":
        return {
            "classification": "OTHER_EXPLAINED",
            "reason_code": support_followup.get("reason_code")
            or f"SUPPORT_FOLLOWUP_{status or 'STATUS_MISSING'}",
        }

    observations = support_followup.get("observations")
    if not isinstance(observations, (list, tuple)):
        raise ResearchEntryError(
            "SUPPORT_CHANGE_OBSERVATIONS_INVALID", "支撑历史观察列表缺失"
        )
    by_session: dict[str, Mapping[str, Any]] = {}
    for item in observations:
        if not isinstance(item, Mapping):
            raise ResearchEntryError(
                "SUPPORT_CHANGE_OBSERVATION_INVALID", "支撑观察不是有效记录"
            )
        session = item.get("session")
        if not isinstance(session, str) or session in by_session:
            raise ResearchEntryError(
                "SUPPORT_CHANGE_OBSERVATION_DUPLICATE", "支撑观察会话缺失或重复"
            )
        by_session[session] = item

    previous_key = previous_session.isoformat()
    target_key = target_session.isoformat()
    previous = by_session.get(previous_key)
    current = by_session.get(target_key)
    if previous is None or current is None:
        raise ResearchEntryError(
            "SUPPORT_CHANGE_ENDPOINT_MISSING", "支撑变化端点观察不完整"
        )

    previous_result = previous.get("support_observation")
    current_result = current.get("support_observation")
    if not isinstance(previous_result, Mapping) or not isinstance(
        current_result, Mapping
    ):
        raise ResearchEntryError(
            "SUPPORT_CHANGE_ENDPOINT_INVALID", "支撑变化端点缺少状态或质量"
        )
    previous_state = previous_result.get("state")
    current_state = current_result.get("state")
    previous_quality = previous_result.get("quality")
    current_quality = current_result.get("quality")
    states = {
        "IDLE",
        "APPROACHING",
        "TESTING",
        "HELD_TENTATIVE",
        "RETESTING",
        "HELD_CONFIRMED",
        "BREACHED_SHALLOW",
        "RECLAIMED",
        "BROKEN",
        "UNKNOWN",
    }
    if previous_state not in states or current_state not in states:
        raise ResearchEntryError(
            "SUPPORT_CHANGE_STATE_INVALID", "支撑观察状态不在现役状态机枚举内"
        )

    reason = None
    if post_anchor_session_count < 2:
        classification = "NOT_COMPARABLE"
        reason = "ANCHOR_BIRTH_OR_INSUFFICIENT_POST_ANCHOR_SESSIONS"
    elif (
        previous_state == "UNKNOWN"
        or current_state == "UNKNOWN"
        or previous_quality != "OBSERVED"
        or current_quality != "OBSERVED"
    ):
        classification = "UNKNOWN"
        reason = "SUPPORT_ENDPOINT_UNKNOWN_OR_QUALITY_INSUFFICIENT"
    elif previous_state != current_state:
        classification = "CHANGED"
    else:
        classification = "UNCHANGED"

    return {
        "classification": classification,
        "reason_code": reason,
        "previous_session": previous_key,
        "target_session": target_key,
        "previous_observation": previous,
        "current_observation": current,
    }


def select_source_support_change_page(
    results: Sequence[Mapping[str, Any]],
    *,
    offset: int,
    limit: int,
    change_only: bool,
) -> dict[str, Any]:
    """在完整批次分类后按来源代码稳定排序、过滤并分页。"""
    if (
        type(offset) is not int
        or offset < 0
        or type(limit) is not int
        or not 1 <= limit <= 100
        or type(change_only) is not bool
    ):
        raise ResearchEntryError("SUPPORT_CHANGE_PAGE_INVALID", "支撑变化分页参数无效")
    normalized = []
    seen: set[str] = set()
    classifications = {
        "CHANGED",
        "UNCHANGED",
        "UNKNOWN",
        "NOT_COMPARABLE",
        "NOT_EVALUABLE",
        "OTHER_EXPLAINED",
    }
    for item in results:
        if not isinstance(item, Mapping):
            raise ResearchEntryError(
                "SUPPORT_CHANGE_RESULT_INVALID", "支撑变化批次含非法结果"
            )
        symbol = item.get("source_symbol")
        if (
            not isinstance(symbol, str)
            or not symbol
            or symbol in seen
            or item.get("change_classification") not in classifications
        ):
            raise ResearchEntryError(
                "SUPPORT_CHANGE_RESULT_IDENTITY_INVALID",
                "支撑变化批次含重复来源代码或未注册分类",
            )
        seen.add(symbol)
        normalized.append(item)

    ordered = sorted(normalized, key=lambda item: item["source_symbol"])
    filtered = (
        [item for item in ordered if item["change_classification"] == "CHANGED"]
        if change_only
        else ordered
    )
    selected = filtered[offset : offset + limit]
    end_offset = offset + len(selected)
    return {
        "results": selected,
        "page": {
            "offset": offset,
            "limit": limit,
            "page_count": len(selected),
            "filtered_total": len(filtered),
            "next_offset": end_offset if end_offset < len(filtered) else None,
            "stable_order": "source_symbol ASC",
            "symbols": [item["source_symbol"] for item in selected],
        },
    }
