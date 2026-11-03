"""Stock A研究foundation职责模块。"""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence
from stock_a.research.core import (
    CORE_MAX_ACTUAL_LOOKBACK,
    CoreObservation,
    MarketSessionCalendar,
    calculate_core,
)

from stock_a.research.entry_context.contracts import (
    FoundationTypedFieldHandoff,
    FoundationTypedHandoff,
)
from stock_a.research.entry_core.profile_preview import _profile_window
from stock_a.research.entry_core.profile_preview import _PROFILE_CORE_WINDOW_SPECS


def _build_foundation_typed_handoff(
    *,
    result: Mapping[str, Any],
    foundation_contract: Mapping[str, Any] | None,
    history: tuple[CoreObservation, ...],
    calendar_revision: str,
) -> FoundationTypedHandoff:
    """装配同一读请求的字段级Foundation交接；不签发证据或生成SQL。"""
    target_session = str(result["target_session"])
    source = result.get("source", {})
    source_package = result.get("source_package", {})
    source_identity = (
        source_package if source_package else source.get("source_identity", {})
    )
    unit_contract = result.get("unit_contract", {})
    identity = result.get("identity", {})
    adjustment = result.get("adjustment", {})
    profile_preview = result.get("profile_preview")

    scope: Mapping[str, Any] = {}
    if isinstance(foundation_contract, Mapping):
        candidate_scope = foundation_contract.get("scope_bindings", {}).get(
            "FOUNDATION_EOD", {}
        )
        if isinstance(candidate_scope, Mapping):
            scope = candidate_scope
    registry_rows = scope.get("field_registry", ())
    field_specs = (
        {
            item["field_id"]: item
            for item in registry_rows
            if isinstance(item, Mapping) and isinstance(item.get("field_id"), str)
        }
        if isinstance(registry_rows, (list, tuple))
        else {}
    )
    profile_rules = scope.get("profile_expression_registry", {})
    profile_rule_by_field = (
        {
            rule.get("field_id"): rule
            for rule in profile_rules.values()
            if isinstance(profile_rules, Mapping)
            and isinstance(rule, Mapping)
            and isinstance(rule.get("field_id"), str)
        }
        if isinstance(profile_rules, Mapping)
        else {}
    )
    preview_components = (
        {
            item.get("field_id"): item
            for item in profile_preview.get("components", ())
            if isinstance(profile_preview, Mapping)
            and isinstance(item, Mapping)
            and isinstance(item.get("field_id"), str)
        }
        if isinstance(profile_preview, Mapping)
        else {}
    )
    raw_fields = result.get("target_raw_fields", {})
    core_fields = result.get("core_fields", {})
    target_bar_present = result.get("target_bar_present") is True
    actual_history = [
        row.session.isoformat() for row in history if row.state == "ACTUAL"
    ]
    history_window = {
        "calendar_revision": calendar_revision,
        "first_selected_session": history[0].session.isoformat() if history else None,
        "last_selected_session": target_session,
        "first_actual_session": actual_history[0] if actual_history else None,
        "last_actual_session": actual_history[-1] if actual_history else None,
        "selected_calendar_sessions": len(history),
        "actual_price_bars": len(actual_history),
        "unknown_sessions": sum(row.state == "UNKNOWN" for row in history),
        "future_rows_used": False,
        "interpretation": "history_policy与字段window_contract定义实际选窗；本范围为目标日及以前的官方会话",
    }

    def unit_authority_for(field_id: str, inputs: tuple[str, ...]) -> str | None:
        pending = [field_id, *inputs]
        visited: set[str] = set()
        uses_amount = uses_volume = False
        while pending:
            name = pending.pop()
            if not isinstance(name, str) or name in visited:
                continue
            visited.add(name)
            uses_amount = uses_amount or "amount" in name or "liquidity" in name
            uses_volume = uses_volume or "volume" in name
            spec = field_specs.get(name)
            if isinstance(spec, Mapping):
                pending.extend(
                    item
                    for item in spec.get("input_fields", ())
                    if isinstance(item, str)
                )
        authorities: list[str] = []
        if uses_amount:
            authorities.append(
                "amount="
                + str(unit_contract.get("amount_authority", "UNKNOWN_UNIT_AUTHORITY"))
            )
        if uses_volume:
            authorities.append(
                "volume="
                + str(unit_contract.get("volume_authority", "UNKNOWN_UNIT_AUTHORITY"))
            )
        if field_id == "raw.amount":
            authorities.append("raw_storage=TDX_FLOAT32_UNVERIFIED")
        if field_id == "raw.volume":
            authorities.append("raw_storage=TDX_UINT32_UNVERIFIED")
        return ";".join(authorities) or None

    fields: list[FoundationTypedFieldHandoff] = []
    price_field_ids = tuple(f"raw.{name}" for name in ("open", "high", "low", "close"))
    for field_id in price_field_ids + ("raw.amount", "raw.volume"):
        name = field_id.removeprefix("raw.")
        spec = field_specs.get(field_id, {})
        payload = raw_fields.get(name, {}) if isinstance(raw_fields, Mapping) else {}
        payload = payload if isinstance(payload, Mapping) else {}
        unit = spec.get("unit") if isinstance(spec, Mapping) else None
        consumers = spec.get("consumers", ()) if isinstance(spec, Mapping) else ()
        fields.append(
            FoundationTypedFieldHandoff(
                field_id=field_id,
                value=payload.get("value"),
                value_type=str(spec.get("value_type", "number"))
                if isinstance(spec, Mapping)
                else "number",
                unit=unit if isinstance(unit, str) else None,
                quality=str(payload.get("quality", "UNKNOWN_DATA")),
                reason=payload.get("reason"),
                status="OBSERVED"
                if payload.get("quality") == "OBSERVED"
                else "UNKNOWN_DATA",
                producer_symbol=str(spec.get("producer_symbol"))
                if isinstance(spec, Mapping) and spec.get("producer_symbol")
                else "stock_a.sources.tdx.day:decode_day_record",
                intended_producer_symbol=str(spec.get("producer_symbol"))
                if isinstance(spec, Mapping) and spec.get("producer_symbol")
                else None,
                input_fields=tuple(spec.get("input_fields", ()))
                if isinstance(spec, Mapping)
                else (),
                consumers=tuple(consumers)
                if isinstance(consumers, (list, tuple))
                else (),
                window_contract=str(spec.get("window_contract", "TARGET_SESSION_R1"))
                if isinstance(spec, Mapping)
                else "TARGET_SESSION_R1",
                window_identity_ref=str(spec.get("window_identity_ref"))
                if isinstance(spec, Mapping) and spec.get("window_identity_ref")
                else None,
                window={
                    "target_session": target_session,
                    "source_session": target_session if target_bar_present else None,
                    "selected_rows": 1 if target_bar_present else 0,
                    "future_rows_used": False,
                },
                unit_authority=unit_authority_for(
                    field_id,
                    tuple(spec.get("input_fields", ()))
                    if isinstance(spec, Mapping)
                    else (),
                ),
            )
        )

    for field_id, payload in (
        sorted(core_fields.items()) if isinstance(core_fields, Mapping) else ()
    ):
        field_spec = field_specs.get(field_id, {})
        inputs = (
            tuple(field_spec.get("input_fields", ()))
            if isinstance(field_spec, Mapping)
            else ()
        )
        registered_window = (
            field_spec.get("window_contract")
            if isinstance(field_spec, Mapping)
            else None
        )
        window_payload: dict[str, Any] = {
            **history_window,
            "field_window_contract": registered_window,
        }
        if field_id in _PROFILE_CORE_WINDOW_SPECS:
            _window_identity, window_payload = _profile_window(
                field_id,
                history,
                window_contract=registered_window
                if isinstance(registered_window, str)
                else "TECH_ACTUAL_R1",
                calendar_revision=calendar_revision,
                quality=str(payload.get("quality", "UNKNOWN_DATA"))
                if isinstance(payload, Mapping)
                else "UNKNOWN_DATA",
            )
        fields.append(
            FoundationTypedFieldHandoff(
                field_id=field_id,
                value=payload.get("value") if isinstance(payload, Mapping) else None,
                value_type=str(field_spec.get("value_type", "number"))
                if isinstance(field_spec, Mapping)
                else "number",
                unit=field_spec.get("unit")
                if isinstance(field_spec, Mapping)
                and isinstance(field_spec.get("unit"), str)
                else None,
                quality=str(payload.get("quality", "UNKNOWN_DATA"))
                if isinstance(payload, Mapping)
                else "UNKNOWN_DATA",
                reason=payload.get("reason")
                if isinstance(payload, Mapping)
                else "CORE_FIELD_NOT_PRODUCED",
                status="OBSERVED"
                if isinstance(payload, Mapping) and payload.get("quality") == "OBSERVED"
                else "UNKNOWN_DATA",
                producer_symbol=str(field_spec.get("producer_symbol"))
                if isinstance(field_spec, Mapping) and field_spec.get("producer_symbol")
                else "stock_a.research.core:calculate_core",
                intended_producer_symbol=str(field_spec.get("producer_symbol"))
                if isinstance(field_spec, Mapping) and field_spec.get("producer_symbol")
                else None,
                input_fields=inputs,
                consumers=tuple(field_spec.get("consumers", ()))
                if isinstance(field_spec, Mapping)
                and isinstance(field_spec.get("consumers", ()), (list, tuple))
                else (),
                window_contract=registered_window
                if isinstance(registered_window, str)
                else None,
                window_identity_ref=str(field_spec.get("window_identity_ref"))
                if isinstance(field_spec, Mapping)
                and field_spec.get("window_identity_ref")
                else None,
                window=window_payload,
                unit_authority=unit_authority_for(field_id, inputs),
            )
        )

    for field_id in sorted(
        field_id for field_id in field_specs if field_id.startswith("profile.")
    ):
        spec = field_specs[field_id]
        component = preview_components.get(field_id)
        rule = profile_rule_by_field.get(field_id, {})
        if isinstance(component, Mapping):
            status = str(component.get("status", "UNKNOWN"))
            quality = str(component.get("quality", status))
            value = component.get("value")
            reason = component.get("reason")
            actual_inputs = tuple(
                component.get("calculation_basis", {}).get(
                    "input_fields", rule.get("input_fields", ())
                )
            )
            input_windows = [
                {"field_id": item.get("source_field_id"), "window": item.get("window")}
                for item in component.get("inputs", ())
                if isinstance(item, Mapping)
            ]
        else:
            status = "NOT_REQUESTED"
            quality = "NOT_EVALUATED"
            value = None
            reason = "PROFILE_PREVIEW_NOT_REQUESTED"
            actual_inputs = (
                tuple(rule.get("input_fields", ())) if isinstance(rule, Mapping) else ()
            )
            input_windows = []
        fields.append(
            FoundationTypedFieldHandoff(
                field_id=field_id,
                value=value,
                value_type=str(spec.get("value_type", "enum")),
                unit=spec.get("unit") if isinstance(spec.get("unit"), str) else None,
                quality=quality,
                reason=reason,
                status=status,
                producer_symbol="stock_a.research.profile:evaluate_profile_kernel"
                if component is not None
                else None,
                intended_producer_symbol=str(spec.get("producer_symbol"))
                if spec.get("producer_symbol")
                else "stock_a.research.profile:profile",
                input_fields=actual_inputs,
                consumers=tuple(spec.get("consumers", ()))
                if isinstance(spec.get("consumers", ()), (list, tuple))
                else (),
                window_contract=str(spec.get("window_contract"))
                if spec.get("window_contract")
                else None,
                window_identity_ref=str(spec.get("window_identity_ref"))
                if spec.get("window_identity_ref")
                else None,
                window={
                    "target_session": target_session,
                    "input_windows": input_windows,
                    "history_policy_ref": spec.get("window_identity_ref"),
                    "profile_preview_only": True,
                },
                unit_authority=unit_authority_for(field_id, actual_inputs),
            )
        )

    def group_quality(field_ids: tuple[str, ...]) -> dict[str, Any]:
        by_id = {field.field_id: field for field in fields}
        selected = [by_id[field_id] for field_id in field_ids]
        if all(field.quality == "OBSERVED" for field in selected):
            quality = "OBSERVED"
        elif any(
            field.reason and field.reason.startswith("DATA_INVALID_")
            for field in selected
        ):
            quality = "UNKNOWN_DATA"
        else:
            quality = "UNKNOWN_DATA"
        return {
            "quality": quality,
            "field_quality": {field.field_id: field.quality for field in selected},
            "field_reasons": {
                field.field_id: field.reason for field in selected if field.reason
            },
        }

    price_quality = group_quality(price_field_ids)
    amount_authority = str(
        unit_contract.get("amount_authority", "UNKNOWN_UNIT_AUTHORITY")
    )
    volume_authority = str(
        unit_contract.get("volume_authority", "UNKNOWN_UNIT_AUTHORITY")
    )
    amount_group = group_quality(("raw.amount",))
    amount_group["unit_authority"] = amount_authority
    amount_group["formal_typed_quality"] = (
        "OBSERVED" if amount_authority == "VERIFIED" else "UNKNOWN_DATA"
    )
    volume_group = group_quality(("raw.volume",))
    volume_group["unit_authority"] = volume_authority
    volume_group["formal_typed_quality"] = (
        "OBSERVED" if volume_authority == "VERIFIED" else "UNKNOWN_DATA"
    )

    typed_reasons: list[str] = []
    stable_identity = identity.get("stable_security_id_issued") is True and bool(
        identity.get("security_id")
    )
    if not stable_identity:
        typed_reasons.append("IDENTITY_NOT_VERIFIED")
    if amount_authority != "VERIFIED":
        typed_reasons.append("AMOUNT_UNIT_UNVERIFIED")
    if volume_authority != "VERIFIED":
        typed_reasons.append("VOLUME_UNIT_UNVERIFIED")
    if price_quality["quality"] != "OBSERVED":
        typed_reasons.append("PRICE_FIELDS_NOT_OBSERVED")
    typed_reasons = list(dict.fromkeys(typed_reasons))

    foundation_reasons = list(typed_reasons)
    if result.get("research_mode") != "STRICT_PIT":
        foundation_reasons.append("PIT_SOURCE_AVAILABILITY_UNVERIFIED")
    if (
        source_identity.get(
            "first_available_at", source.get("first_available_at", "UNKNOWN")
        )
        == "UNKNOWN"
    ):
        if "PIT_SOURCE_AVAILABILITY_UNVERIFIED" not in foundation_reasons:
            foundation_reasons.append("PIT_SOURCE_AVAILABILITY_UNVERIFIED")
    if identity.get("membership_status", "UNKNOWN") in {None, "UNKNOWN", "UNBOUND"}:
        foundation_reasons.append("OFFICIAL_UNIVERSE_MEMBERSHIP_UNVERIFIED")
    if adjustment.get("coordinate") != "ADJUSTED_TARGET_COORDINATE_VERIFIED":
        foundation_reasons.append("FORMAL_PRICE_COORDINATE_NOT_BOUND")
    trusted_selector_bound = result.get("trusted_foundation_selector_bound") is True
    if not trusted_selector_bound:
        foundation_reasons.append("TRUSTED_FOUNDATION_SELECTOR_NOT_BOUND")
    if not target_bar_present:
        foundation_reasons.append("TARGET_BAR_MISSING")
    foundation_reasons = list(dict.fromkeys(foundation_reasons))
    reason_explanations = {
        "IDENTITY_NOT_VERIFIED": "来源代码尚未绑定经过验证的稳定证券身份，不能写入正式Canonical。",
        "AMOUNT_UNIT_UNVERIFIED": "成交额单位只有调用方研究假设，未获得该证券与日期的独立单位权威。",
        "VOLUME_UNIT_UNVERIFIED": "成交量单位只有调用方研究假设，未获得该证券与日期的独立单位权威。",
        "PRICE_FIELDS_NOT_OBSERVED": "目标日OHLC字段未全部通过合法价格质量检查。",
        "PIT_SOURCE_AVAILABILITY_UNVERIFIED": "来源发布与首次可得时钟未知，只能作NON_PIT历史研究。",
        "OFFICIAL_UNIVERSE_MEMBERSHIP_UNVERIFIED": "官方上市/有效成员及目标日交易状态尚未绑定。",
        "FORMAL_PRICE_COORDINATE_NOT_BOUND": "当前为RAW_UNADJUSTED，尚无正式公司行为与目标价格坐标闭包。",
        "TRUSTED_FOUNDATION_SELECTOR_NOT_BOUND": "研究预览未通过可信Foundation生产选择器，不能冒充正式Profile。",
        "TARGET_BAR_MISSING": "该来源代码在目标交易日没有可用Bar；缺Bar不等于停牌或上市前。",
    }
    typed_explanations = {code: reason_explanations[code] for code in typed_reasons}
    foundation_explanations = {
        code: reason_explanations[code] for code in foundation_reasons
    }
    consumers = {
        "research_stock": ("raw", "canonical_selection", "core", "profile_preview"),
        "research_universe_preview": (
            "page_quality",
            "raw",
            "core",
            "profile_preview",
            "foundation_admission",
        ),
        "postgres_typed.prepare_canonical_daily_candidate": (
            "CanonicalReadSelection",
            "StableIdentityBinding",
            "UnitAuthority",
        ),
        "future_foundation_typed_pg": (
            "typed field rows; not implemented or persisted"
        ),
    }
    return FoundationTypedHandoff(
        contract="STOCK-A-FOUNDATION-TYPED-HANDOFF/R1",
        source_symbol=str(result["requested_symbol"]),
        target_session=target_session,
        source_package_id=source_identity.get("package_id")
        if isinstance(source_identity, Mapping)
        else None,
        source_package_sha256=source_identity.get(
            "package_sha256", source_identity.get("sha256")
        )
        if isinstance(source_identity, Mapping)
        else None,
        source_entry_sha256=source_identity.get(
            "entry_sha256", source_identity.get("zip_entry_sha256")
        )
        if isinstance(source_identity, Mapping)
        else None,
        source_byte_sha256=source.get("raw_byte_sha256"),
        source_clock_role=str(
            source.get(
                "read_clock_role", source_identity.get("source_clock_role", "UNKNOWN")
            )
        ),
        published_at=str(source_identity.get("published_at", "UNKNOWN")),
        first_available_at=str(source_identity.get("first_available_at", "UNKNOWN")),
        price_coordinate=str(adjustment.get("coordinate", "RAW_UNADJUSTED")),
        research_mode=str(result.get("research_mode", "NON_PIT_RETROSPECTIVE")),
        trusted_foundation_selector_bound=trusted_selector_bound,
        identity_status=str(identity.get("status", "SOURCE_SYMBOL_ONLY")),
        stable_security_id_issued=stable_identity,
        target_bar_status="PRESENT" if target_bar_present else "TARGET_MISSING",
        research_price_status="AVAILABLE"
        if result.get("target_prices_usable") is True
        else "UNKNOWN_DATA",
        quality_groups={
            "price": price_quality,
            "amount": amount_group,
            "volume": volume_group,
        },
        typed_pg_admission_status="BLOCKED"
        if typed_reasons
        else "READY_FOR_TYPED_ADAPTER_REVIEW",
        typed_pg_admission_reasons=tuple(typed_reasons),
        typed_pg_admission_reason_explanations=typed_explanations,
        foundation_admission_status="BLOCKED"
        if foundation_reasons
        else "READY_FOR_FOUNDATION_REVIEW",
        foundation_admission_reasons=tuple(foundation_reasons),
        foundation_admission_reason_explanations=foundation_explanations,
        fields=tuple(fields),
        consumer_edges=consumers,
    )
