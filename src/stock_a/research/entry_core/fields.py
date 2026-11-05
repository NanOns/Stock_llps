"""Stock A研究core_fields职责模块。"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal, localcontext
from fractions import Fraction
from typing import Any, Iterable, Mapping, Sequence
from stock_a.canonical.selection import CanonicalReadSelection, build_tdx_read_selection
from stock_a.domain import ContractError, FieldValue, exact_number, unknown
from stock_a.research.core import (
    CORE_MAX_ACTUAL_LOOKBACK,
    CoreObservation,
    MarketSessionCalendar,
    calculate_core,
)
from stock_a.sources.tdx.day import DAY_STRUCT, DecodedDay, UnitContract

_RAW_FIELDS = ("open", "high", "low", "close", "amount", "volume")


def _render_value(value: Any) -> Any:
    if isinstance(value, Fraction):
        with localcontext() as context:
            context.prec = 24
            decimal_value = Decimal(value.numerator) / Decimal(value.denominator)
        return {
            "exact": f"{value.numerator}/{value.denominator}",
            "decimal": format(decimal_value, "f"),
        }
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (str, int, bool)) or value is None:
        return value
    if isinstance(value, tuple):
        return [_render_value(item) for item in value]
    return str(value)


def _render_field(value: FieldValue) -> dict[str, Any]:
    if value.quality == "OBSERVED":
        return {
            "value": _render_value(value.value),
            "quality": value.quality,
            "reason": None,
        }
    return {"value": None, "quality": value.quality, "reason": value.reason}


def _unit_trust_status(scale: object) -> str:
    return (
        "CALLER_DECLARED_ASSUMPTION" if scale is not None else "UNKNOWN_UNIT_AUTHORITY"
    )


def _render_raw(decoded: DecodedDay | None, units: UnitContract) -> dict[str, Any]:
    if decoded is None:
        fields = {
            name: _render_field(unknown("TARGET_BAR_MISSING")) for name in _RAW_FIELDS
        }
        raw_amount = unknown("TARGET_BAR_MISSING")
        raw_volume = unknown("TARGET_BAR_MISSING")
    else:
        fields = {
            "open": _render_field(decoded.raw_open),
            "high": _render_field(decoded.raw_high),
            "low": _render_field(decoded.raw_low),
            "close": _render_field(decoded.raw_close),
            "amount": _render_field(decoded.amount),
            "volume": _render_field(decoded.volume),
        }
        raw_amount = decoded.raw_amount or unknown("RAW_AMOUNT_NOT_RETAINED")
        raw_volume = decoded.raw_volume or unknown("RAW_VOLUME_NOT_RETAINED")
    fields["amount"]["raw_stored_value"] = {
        **_render_field(raw_amount),
        "storage_type": "TDX_FLOAT32",
        "unit": "UNVERIFIED_RAW_STORAGE_COORDINATE",
    }
    fields["volume"]["raw_stored_value"] = {
        **_render_field(raw_volume),
        "storage_type": "TDX_UINT32",
        "unit": "UNVERIFIED_RAW_STORAGE_COORDINATE",
    }
    fields["amount"]["unit_trust"] = {
        "status": "CALLER_DECLARED_ASSUMPTION"
        if units.amount_to_cny is not None
        else "UNKNOWN",
        "unit_version": units.version,
        "amount_to_cny": str(units.amount_to_cny)
        if units.amount_to_cny is not None
        else None,
    }
    fields["volume"]["unit_trust"] = {
        "status": "CALLER_DECLARED_ASSUMPTION"
        if units.volume_to_shares is not None
        else "UNKNOWN",
        "unit_version": units.version,
        "volume_to_shares": units.volume_to_shares,
    }
    return fields


def _core_history_from_selection(
    selection: CanonicalReadSelection,
) -> tuple[CoreObservation, ...]:
    """让既有Core直接消费本次规范选择的typed字段行。"""
    return tuple(
        CoreObservation(
            session=row.session,
            state=row.state,
            coordinate_id=row.coordinate_id,
            open=row.open,
            high=row.high,
            low=row.low,
            close=row.close,
            amount=row.amount,
            volume=row.volume,
            native_amount=row.native_amount,
            native_volume=row.native_volume,
        )
        for row in selection.rows
    )


def _render_canonical_selection(selection: CanonicalReadSelection) -> dict[str, Any]:
    capture = selection.capture
    package_entry = capture.source_package_entry
    source_identity = capture.source_identity
    target_fields: dict[str, Any] = {}
    for fact in selection.target_fields:
        payload = _render_field(fact.value)
        payload.update(
            {
                "field_id": fact.field_id,
                "target_session": fact.target_session.isoformat(),
                "source_session": fact.source_session.isoformat()
                if fact.source_session
                else None,
                "unit": fact.unit,
                "unit_basis": fact.unit_basis,
                "source_sha256": fact.source_sha256,
                "field_digest": None,
                "field_digest_status": "BLOCKED_STABLE_SECURITY_ID_UNBOUND",
            }
        )
        target_fields[fact.field_id.removeprefix("raw.")] = payload
    actual_rows = sum(row.state == "ACTUAL" for row in selection.rows)
    missing_rows = len(selection.rows) - actual_rows
    target_present = any(
        fact.source_session == selection.target_session
        for fact in selection.target_fields
    )
    return {
        "contract": "STOCK-A-TDX-CANONICAL-READ-SELECTION/R1",
        "selection_status": "EPHEMERAL_READ_ONLY_SELECTION_COMPLETE",
        "acceptance_status": "NOT_ACCEPTED_NOT_PERSISTED",
        "target_session": selection.target_session.isoformat(),
        "target_bar_status": "PRESENT" if target_present else "UNKNOWN_NO_TDX_BAR",
        "source_candidate_scope": {
            "kind": "ONE_EXPLICIT_SOURCE_SYMBOL_QUERY",
            "candidate_count": 1,
            "selected_count": 1,
            "official_universe_denominator": "UNKNOWN",
            "exclusion_reason": "来源代码不是官方成员或稳定证券身份证明",
        },
        "source": {
            "dataset_id": capture.dataset_id,
            "dataset_version": capture.dataset_version,
            "capture_id": capture.capture_id,
            "capture_id_role": "EPHEMERAL_CURRENT_READ_ONLY_INVOCATION",
            "capture_business_digest_sha256": capture.business_digest_sha256,
            "raw_byte_sha256": capture.content_sha256,
            "record_count": capture.record_count,
            "byte_count": capture.byte_count,
            "record_date_range": [
                capture.first_date.isoformat(),
                capture.last_date.isoformat(),
            ],
            "complete_read": True,
            "source_authority": (
                "VIPDATA_OFFICIAL_ENDPOINT_LOCAL_PACKAGE_AND_ENTRY_BYTES_REVALIDATED"
                if package_entry is not None
                else "UNVERIFIED_LOCAL_TDX_INSTALLATION"
            ),
            "source_identity": (
                {
                    "kind": "VERIFIED_TDX_ZIP_ENTRY",
                    "package_id": package_entry.package_id,
                    "package_sha256": package_entry.package_sha256,
                    "official_package_url": package_entry.official_package_url,
                    "package_file_size": package_entry.package_file_size,
                    "zip_entry_count": package_entry.zip_entry_count,
                    "zip_crc": package_entry.zip_crc,
                    "package_verified_at": package_entry.package_verified_at.isoformat(),
                    "zip_entry_path": package_entry.zip_entry_path,
                    "entry_sha256": package_entry.entry_sha256,
                    "entry_byte_count": package_entry.entry_byte_count,
                    "source_symbol": package_entry.source_symbol,
                    "target_session": package_entry.target_session.isoformat(),
                    "source_clock_role": package_entry.source_clock_role,
                    "published_at": "UNKNOWN",
                    "first_available_at": "UNKNOWN",
                }
                if package_entry is not None
                else {
                    "kind": "READONLY_LOCAL_FILE_HANDLE",
                    "requested_access": source_identity.requested_access,
                    "path_binding_verified": source_identity.path_binding_verified,
                    "handle_path_verified": source_identity.handle_path_verified,
                    "size_stable_during_read": source_identity.size_before
                    == source_identity.size_after,
                    "mtime_stable_during_read": source_identity.mtime_ns_before
                    == source_identity.mtime_ns_after,
                }
            ),
            "observed_started_at": capture.observed_started_at.isoformat(),
            "observed_completed_at": capture.observed_completed_at.isoformat(),
            "observation_clock_role": (
                package_entry.source_clock_role
                if package_entry is not None
                else "LOCAL_FILE_READ_TIME_ONLY"
            ),
            "first_available_at": "UNKNOWN",
            "published_at": "UNKNOWN",
        },
        "identity": (
            {
                **selection.identity_projection.as_dict(),
                "status": selection.identity_projection.identity_status,
                "security_entity_id": selection.identity_projection.entity_id,
                "membership_status": "UNKNOWN_NOT_OFFICIALLY_BOUND",
                "listing_lifecycle_status": selection.identity_projection.classification,
            }
            if selection.identity_projection is not None
            else {
                "source_symbol": selection.source_symbol,
                "status": "SOURCE_SYMBOL_ONLY",
                "security_entity_id": None,
                "membership_status": "UNKNOWN_NOT_OFFICIALLY_BOUND",
                "listing_lifecycle_status": "UNKNOWN_NOT_OFFICIALLY_BOUND",
            }
        ),
        "calendar": {
            "calendar_id": selection.calendar_id,
            "revision": selection.calendar_revision,
            "evidence_status": selection.calendar_evidence_status,
            "target_session_verified": True,
            "missing_bar_semantics": "UNKNOWN; 不推断停牌、退市或上市前",
        },
        "coordinate": {
            "price": "RAW_UNADJUSTED",
            "adjustment_status": "NOT_APPLIED_IDENTITY_AND_EVENT_AVAILABILITY_UNRESOLVED",
        },
        "time_availability": {
            "mode": "NON_PIT_RETROSPECTIVE",
            "observed_at_role": "LOCAL_FILE_READ_TIME_ONLY",
            "available_at": "UNKNOWN",
            "first_available_at": "UNKNOWN",
            "durable_at": "NOT_CREATED",
            "accepted_at": "NOT_CREATED",
            "availability_digest_status": "NOT_COMPUTED_NO_DURABLE_ACCEPTANCE_OR_SOURCE_AVAILABILITY_CLOCK",
        },
        "history_selection": {
            "calendar_session_count": len(selection.rows),
            "actual_source_rows_selected": actual_rows,
            "missing_bar_sessions_unknown": missing_rows,
            "maximum_selected_session": selection.rows[-1].session.isoformat()
            if selection.rows
            else None,
            "future_source_rows_excluded": selection.future_source_rows_excluded,
            "future_rows_used_by_core": False,
            "field_window_digest_status": "BLOCKED_STABLE_SECURITY_ID_UNBOUND",
        },
        "field_identity": {
            "field_digest_status": "BLOCKED_STABLE_SECURITY_ID_UNBOUND",
            "window_digest_status": "BLOCKED_STABLE_SECURITY_ID_UNBOUND",
            "reason": "不得以来源代码冒充稳定security_entity_id",
        },
        "unit_contract": {
            "version": selection.units.version,
            "amount_to_cny": str(selection.units.amount_to_cny)
            if selection.units.amount_to_cny is not None
            else None,
            "volume_to_shares": selection.units.volume_to_shares,
            "amount_authority": _unit_trust_status(selection.units.amount_to_cny),
            "volume_authority": _unit_trust_status(selection.units.volume_to_shares),
            "binding_status": "CALLER_DECLARED_RESEARCH_ASSUMPTION"
            if selection.units.amount_to_cny is not None
            or selection.units.volume_to_shares is not None
            else "UNKNOWN_UNIT_AUTHORITY",
        },
        "fields": target_fields,
        "production_admission": "DENY_SOURCE_AUTHORITY_IDENTITY_AND_PIT_UNBOUND",
        "profile_admission": "DENY_TRUSTED_FOUNDATION_SELECTOR_REQUIRED",
    }


def _field_relation(left: FieldValue, right: FieldValue) -> FieldValue:
    if left.quality != "OBSERVED":
        return unknown(left.reason or "UPSTREAM_UNKNOWN")
    if right.quality != "OBSERVED":
        return unknown(right.reason or "UPSTREAM_UNKNOWN")
    try:
        left_value = exact_number(left.value)
        right_value = exact_number(right.value)
    except (TypeError, ValueError, ZeroDivisionError):
        return unknown("INVALID_OBSERVATION_INPUT")
    if left_value > right_value:
        return FieldValue("ABOVE")
    if left_value < right_value:
        return FieldValue("BELOW")
    return FieldValue("EQUAL")


def _field_greater_than(left: FieldValue, right: FieldValue) -> FieldValue:
    relation = _field_relation(left, right)
    if relation.quality != "OBSERVED":
        return unknown(relation.reason or "UPSTREAM_UNKNOWN")
    return FieldValue(relation.value == "ABOVE")


def _apply_calendar_coverage_to_core(
    core: dict[str, FieldValue],
    history: tuple[CoreObservation, ...],
    coverage_context: dict[str, Any],
) -> None:
    if coverage_context.get("core_lookback_complete") or not history:
        return
    if any(row.state != "ACTUAL" for row in history):
        return
    for name, field in tuple(core.items()):
        if field.quality != "OBSERVED" and field.reason in {
            "SHORT_HISTORY",
            "SHORT_SESSION_HISTORY",
        }:
            core[name] = unknown("CALENDAR_LOOKBACK_NOT_COVERED")


def _observation_summary(
    *,
    symbol: str,
    target_session: date,
    source_last_session: date,
    raw_fields: dict[str, Any],
    raw_close: FieldValue,
    core: dict[str, FieldValue],
) -> dict[str, Any]:
    return {
        "contract": "RESEARCH_PREVIEW_NON_PIT",
        "scope": "OBJECTIVE_OBSERVATIONS_ONLY_NO_PROFILE_SEED_PRIORITY_OR_TRADE_SIGNAL",
        "requested_symbol": symbol,
        "target_session": target_session.isoformat(),
        "source_last_session": source_last_session.isoformat(),
        "price_coordinate": "RAW_UNADJUSTED",
        "fields": {
            "raw_close": raw_fields["close"],
            "ma5": _render_field(core["core.ma5"]),
            "ma20": _render_field(core["core.ma20"]),
            "ma60": _render_field(core["core.ma60"]),
            "atr20": _render_field(core["core.atr20"]),
            "ret5": _render_field(core["core.ret5"]),
            "ret20": _render_field(core["core.ret20"]),
            "amount": raw_fields["amount"],
            "volume": raw_fields["volume"],
            "close_above_ma20": _render_field(
                _field_greater_than(raw_close, core["core.ma20"])
            ),
            "moving_average_position": {
                "close_vs_ma5": _render_field(
                    _field_relation(raw_close, core["core.ma5"])
                ),
                "close_vs_ma20": _render_field(
                    _field_relation(raw_close, core["core.ma20"])
                ),
                "close_vs_ma60": _render_field(
                    _field_relation(raw_close, core["core.ma60"])
                ),
                "ma5_vs_ma20": _render_field(
                    _field_relation(core["core.ma5"], core["core.ma20"])
                ),
                "ma20_vs_ma60": _render_field(
                    _field_relation(core["core.ma20"], core["core.ma60"])
                ),
            },
        },
    }


def _annotated_fact(
    *,
    field_id: str,
    value: dict[str, Any],
    formula_source: str,
    coordinate_role: str,
    coordinate_id: str | None,
) -> dict[str, Any]:
    """为可读摘要标明值的现役生产者和坐标，不重新计算字段。"""
    return {
        "field_id": field_id,
        "value": value,
        "formula_source": formula_source,
        "coordinate_role": coordinate_role,
        "coordinate_id": coordinate_id,
    }


def _basic_fact_dimensions(
    *,
    symbol: str,
    target_session: date,
    source_last_session: date,
    read_time: dict[str, Any],
    source: dict[str, Any],
    raw_fields: dict[str, Any],
    core: dict[str, FieldValue],
    observation_summary: dict[str, Any],
    raw_availability: dict[str, Any],
    core_availability: dict[str, Any],
    window_context: dict[str, Any],
    target_bar_present: bool,
    target_prices_usable: bool,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """把既有Raw/Core字段分组展示；不增加指标或策略阈值。"""
    raw_coordinate = f"RAW:TDX_SOURCE_SYMBOL:{symbol}"
    raw_specs = {
        "raw.close": ("close", "TDX_LOCAL_LDAY_32_BYTE_LAYOUT_R00", "RAW_UNADJUSTED"),
        "raw.amount": (
            "amount",
            "TDX_LOCAL_LDAY_32_BYTE_LAYOUT_R00",
            "SOURCE_RAW_ENCODING",
        ),
        "raw.volume": (
            "volume",
            "TDX_LOCAL_LDAY_32_BYTE_LAYOUT_R00",
            "SOURCE_RAW_ENCODING",
        ),
    }
    core_specs = {
        "core.ma5": ("stock_a.research.core:calculate_core", "RAW_UNADJUSTED"),
        "core.ma20": ("stock_a.research.core:calculate_core", "RAW_UNADJUSTED"),
        "core.ma60": ("stock_a.research.core:calculate_core", "RAW_UNADJUSTED"),
        "core.ma_slope20_atr": (
            "stock_a.research.core:calculate_core",
            "DIMENSIONLESS",
        ),
        "core.ret5": ("stock_a.research.core:calculate_core", "DIMENSIONLESS"),
        "core.ret20": ("stock_a.research.core:calculate_core", "DIMENSIONLESS"),
        "core.atr20": ("stock_a.research.core:calculate_core", "RAW_UNADJUSTED"),
        "core.pos60": ("stock_a.research.core:calculate_core", "DIMENSIONLESS"),
        "core.prior_high20": ("stock_a.research.core:calculate_core", "RAW_UNADJUSTED"),
        "core.prior_low20": ("stock_a.research.core:calculate_core", "RAW_UNADJUSTED"),
        "core.bias20_atr": ("stock_a.research.core:calculate_core", "DIMENSIONLESS"),
        "core.dist_high20_atr": (
            "stock_a.research.core:calculate_core",
            "DIMENSIONLESS",
        ),
        "core.price_damage": ("stock_a.research.core:calculate_core", "DIMENSIONLESS"),
        "core.amount_ratio20": (
            "stock_a.research.core:calculate_core",
            "DIMENSIONLESS",
        ),
        "core.volume_ratio20": (
            "stock_a.research.core:calculate_core",
            "DIMENSIONLESS",
        ),
        "core.liquidity_mean20": (
            "stock_a.research.core:calculate_core",
            "SOURCE_AMOUNT_UNIT_ASSUMPTION",
        ),
    }
    comparison_specs = {
        "close_vs_ma20": "core.ma20",
        "close_vs_ma60": "core.ma60",
    }
    old_fields = observation_summary["fields"]

    trend: dict[str, Any] = {}
    position: dict[str, Any] = {}
    participation: dict[str, Any] = {}
    for field_id, (raw_name, formula, coordinate) in raw_specs.items():
        target = trend if field_id == "raw.close" else participation
        target[field_id] = _annotated_fact(
            field_id=field_id,
            value=raw_fields[raw_name],
            formula_source=formula,
            coordinate_role=coordinate,
            coordinate_id=raw_coordinate if coordinate == "RAW_UNADJUSTED" else None,
        )
    for field_id, (formula, coordinate) in core_specs.items():
        target = (
            trend
            if field_id
            in {
                "core.ma5",
                "core.ma20",
                "core.ma60",
                "core.ma_slope20_atr",
                "core.ret5",
                "core.ret20",
            }
            else position
            if field_id
            in {
                "core.atr20",
                "core.pos60",
                "core.prior_high20",
                "core.prior_low20",
                "core.bias20_atr",
                "core.dist_high20_atr",
                "core.price_damage",
            }
            else participation
        )
        fact = _annotated_fact(
            field_id=field_id,
            value=_render_field(core[field_id]),
            formula_source=formula,
            coordinate_role=coordinate,
            coordinate_id=raw_coordinate if coordinate == "RAW_UNADJUSTED" else None,
        )
        if field_id in {
            "core.amount_ratio20",
            "core.volume_ratio20",
            "core.liquidity_mean20",
        }:
            fact["unit_trust"] = {
                "status": (
                    "SAME_CAPTURE_NATIVE_DIMENSIONLESS"
                    if field_id != "core.liquidity_mean20"
                    else "CALLER_DECLARED_ASSUMPTION"
                ),
                "interpretation": (
                    "金额比率要求窗口内金额倍率一致"
                    if field_id == "core.amount_ratio20"
                    else "数量比率要求窗口内数量倍率一致"
                    if field_id == "core.volume_ratio20"
                    else "绝对金额均值依赖调用方声明的金额倍率"
                ),
            }
        target[field_id] = fact
    for label, core_id in comparison_specs.items():
        item = old_fields["moving_average_position"][label]
        trend[label] = _annotated_fact(
            field_id=f"research.{label}",
            value=item,
            formula_source=f"stock_a.research.entry._field_relation(raw.close, {core_id}); 精确比较，不增加阈值",
            coordinate_role="DIMENSIONLESS_RELATION",
            coordinate_id=None,
        )

    all_facts = {**trend, **position, **participation}
    unknown_facts = {
        group: {
            field_id: fact["value"].get("reason") or fact["value"].get("quality")
            for field_id, fact in fields.items()
            if fact["value"].get("quality") != "OBSERVED"
        }
        for group, fields in (
            ("price_trend", trend),
            ("position_volatility", position),
            ("participation", participation),
        )
    }
    dimensions = {
        "contract": "STOCK-A-BASIC-FACT-DIMENSIONS/R1",
        "scope": "只分组呈现本次查询的既有Raw/Core事实；不代表Profile或策略结论",
        "target_session": target_session.isoformat(),
        "source_last_session": source_last_session.isoformat(),
        "research_mode": "NON_PIT_RETROSPECTIVE",
        "price_coordinate": "RAW_UNADJUSTED",
        "identity": {"status": "SOURCE_SYMBOL_ONLY", "symbol": symbol},
        "dimensions": {
            "price_trend": trend,
            "position_volatility": position,
            "participation": participation,
        },
        "source_quality": {
            "target_bar_present": target_bar_present,
            "target_prices_usable": target_prices_usable,
            "read_time": read_time,
            "read_clock_role": "LOCAL_FILE_READ_TIME_ONLY",
            "source_read_budget": source.get("budgets", {}),
            "window_context": window_context,
            "raw_availability": raw_availability,
            "core_availability": core_availability,
            "unknown_facts_by_dimension": unknown_facts,
            "field_count": len(all_facts),
        },
    }
    availability = {
        "contract": "STOCK-A-BASIC-FACT-AVAILABILITY/R1",
        "compatibility_status": "AVAILABLE"
        if target_prices_usable
        else "PARTIAL_UNKNOWN",
        "compatibility_status_scope": "仅按目标日原始价格组判断；不代表所有Core字段已知",
        "target_bar_present": target_bar_present,
        "raw": raw_availability,
        "core": core_availability,
        "unknown_facts_by_dimension": unknown_facts,
        "missing_target_bar_explanation": (
            None
            if target_bar_present
            else "目标日没有可用bar；不据此推断停牌、上市前或退市。"
        ),
        "research_mode": "NON_PIT_RETROSPECTIVE",
        "price_coordinate": "RAW_UNADJUSTED",
        "identity_status": "SOURCE_SYMBOL_ONLY",
    }
    return dimensions, availability
