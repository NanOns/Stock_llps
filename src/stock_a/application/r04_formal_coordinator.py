"""R04 正式 D2 生命周期与下游风险/Priority/Radar/Near-Miss 协调器入口。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, timedelta
from decimal import Decimal
from fractions import Fraction
from typing import Any

from stock_a.application.d2_downstream_projections import (
    D0ScenarioNearMissInput,
    D0SoftPredicateSpec,
    StockProjectionInput,
    assemble_near_miss_paths_from_d0,
    assemble_stock_risk_inputs,
    build_d2_downstream_projections,
)
from stock_a.application.d2_formal_lifecycle import (
    D2DayInputs,
    D2DayResult,
    advance_d2_day,
    build_support_bindings_for_parents,
    compute_stock_warm_qualification,
    produce_pb_continue_and_peaks_for_stock,
    resolve_prior_context,
    select_prior_peaks_by_parent,
)
from stock_a.application.source_research_day import (
    prepare_dynamic_ma_backfill_facts,
)
from stock_a.domain import FieldValue, unknown
from stock_a.research.admission import Event, FrozenLine
from stock_a.research.d0_pb_continue import (
    ContinueParentDetail,
    PeakEvent,
    PullbackParentDetail,
)
from stock_a.research.entry_context.sessions import _load_calendar
from stock_a.research.facts import Fact
from stock_a.research.priority_radar import (
    classify_emergence,
    detect_existing_anchor_rank_improvement,
    resolve_episode_improvement_age,
)
from stock_a.research.risk import CONTRACT as RISK_CONTRACT
from stock_a.research.risk import RiskResult
from stock_a.research.state import CONTRACT as D2_CONTRACT
from stock_a.research.state import Rules as D2Rules
from stock_a.research.state import State
from stock_a.research.structure import SupportObservation
from stock_a.storage.postgres import source_raw
from stock_a.storage.postgres.formal_admission import (
    _ADMISSION_CONTRACT,
    _D0_SCENARIO_PRODUCER_VERSIONS,
    _SEED_PRODUCER_VERSION,
)
from stock_a.storage.postgres.r04_formal_facts import (
    persist_d2_and_downstream_day_bundle,
    read_d2_day_states,
    read_d2_parent_peaks_before,
    read_d2_primary_projections,
    read_stock_risk_summary_and_rows,
)
from stock_a.storage.postgres.research_day_facts import (
    persist_d1_active_anchors_and_observations,
    read_d1_active_support_observations,
)


def _mk_fact(
    field: FieldValue,
    *,
    entity_id: str,
    session_date: date,
    coordinate_id: str,
    source_ref: str,
    episode_id: str | None = None,
) -> Fact:
    return Fact(
        field=field,
        entity_id=entity_id,
        session=session_date,
        coordinate_id=coordinate_id,
        source_ref=source_ref,
        episode_id=episode_id,
    )


def _frac(num: Any, den: Any) -> Fraction | None:
    if num is None or den is None:
        return None
    d = Decimal(den)
    if d <= 0:
        return None
    return Fraction(Decimal(num)) / Fraction(d)


def _tri_field(
    state: str | None,
    *,
    reason_if_unknown: str = "GATE_UNKNOWN",
    reason_if_false: str = "GATE_FALSE",
) -> FieldValue:
    if state == "TRUE":
        return FieldValue(True)
    if state == "FALSE":
        return FieldValue(False)
    return unknown(reason_if_unknown)


def _state_from_pg_row(
    row: Mapping[str, Any], *, session_to_ordinal: Mapping[date, int]
) -> State:
    """将PG读回的d2_episode_day_states字典还原为research.state.State对象，保留原T0证据。"""
    fl = row.get("failure_level")
    event_obj: Event | None = None
    eff_session = row.get("effective_from_session")
    if row.get("event_id") and fl is not None and eff_session is not None:
        birth_ord = int(row["birth_ordinal"])
        eff_ord = session_to_ordinal.get(eff_session, birth_ord + 1)
        event_obj = Event(
            event_id=row["event_id"],
            entity_id=row["entity_id"],
            session_ordinal=birth_ord,
            scenario=row["scenario"],
            failure_line=FrozenLine(
                value=fl,
                coordinate_id=row.get("failure_coordinate") or row["coordinate_id"],
                source_ref=row.get("failure_source") or "T0_PHC20",
                source_window=str(
                    row.get("failure_window") or row.get("birth_session") or ""
                ),
                effective_ordinal=eff_ord,
            ),
            linked_anchor_id=row.get("linked_anchor_id"),
            prior_episode_id=row.get("parent_episode_id"),
            evidence_ref=row.get("evidence_ref") or row["input_ref"],
        )
    elig_q = row["effective_eligibility_quality"]
    elig_val = row["effective_eligibility_value"]
    eff_elig = (
        FieldValue(elig_val)
        if elig_q == "OBSERVED"
        else FieldValue(
            None,
            elig_q,
            row.get("effective_eligibility_reason") or "ELIGIBILITY_UNKNOWN",
        )
    )
    val_q = row["validity_quality"]
    validity = (
        FieldValue(row["validity_value"])
        if val_q == "OBSERVED"
        else FieldValue(None, val_q, row.get("validity_reason") or "VALIDITY_UNKNOWN")
    )
    hl_q = row["health_quality"]
    health = (
        FieldValue(row["health_value"])
        if hl_q == "OBSERVED"
        else FieldValue(None, hl_q, row.get("health_reason") or "HEALTH_UNKNOWN")
    )
    return State(
        entity_id=row["entity_id"],
        entity_type=row["entity_type"],
        ordinal=int(row["ordinal"]),
        calendar_id=row["calendar_id"],
        rule_identity=row["rule_identity"],
        episode_id=row["episode_id"],
        event_id=row.get("event_id"),
        scenario=row["scenario"],
        kind=row["kind"],
        birth_ordinal=int(row["birth_ordinal"]),
        parent_episode_id=row.get("parent_episode_id"),
        event=event_obj,
        exact_maturity=row.get("exact_maturity"),
        lower_bound_maturity=row["lower_bound_maturity"],
        display_maturity=row["display_maturity"],
        effective_eligibility=eff_elig,
        effective_bucket=row.get("effective_bucket"),
        validity=validity,
        freshness=row["freshness"],
        health=health,
        tracking=row["tracking"],
        downgrade_candidate=row.get("downgrade_candidate"),
        downgrade_count=int(row["downgrade_count"]),
        expiry_count=int(row["expiry_count"]),
        improvement_baseline=row.get("improvement_baseline"),
        exit_ordinal=row.get("exit_ordinal"),
        exit_reason=row.get("exit_reason"),
        reentry_armed=bool(row["reentry_armed"]),
        transitions=tuple(row.get("transitions") or ()),
        preserved_episode_ids=tuple(row.get("preserved_episode_ids") or ()),
        input_ref=row["input_ref"],
    )


def _state_to_pg_dict(st: State, *, session_date: date) -> dict[str, Any]:
    return {
        "entity_type": st.entity_type,
        "entity_id": st.entity_id,
        "calendar_id": st.calendar_id,
        "session_date": session_date,
        "ordinal": st.ordinal,
        "rule_identity": st.rule_identity,
        "episode_id": st.episode_id,
        "coordinate_id": st.event.failure_line.coordinate_id
        if st.event is not None
        else "FORWARD_ADJUSTED",
        "input_ref": st.input_ref,
        "prior_mode": "PREVIOUS_ACCEPTED"
        if st.ordinal > st.birth_ordinal
        else "FIRST_OBSERVATION",
        "event_id": st.event_id,
        "scenario": st.scenario,
        "kind": st.kind,
        "birth_ordinal": st.birth_ordinal,
        "parent_episode_id": st.parent_episode_id,
        "exact_maturity": st.exact_maturity,
        "lower_bound_maturity": st.lower_bound_maturity,
        "display_maturity": st.display_maturity,
        "effective_eligibility_value": st.effective_eligibility.value
        if st.effective_eligibility.quality == "OBSERVED"
        else None,
        "effective_eligibility_quality": st.effective_eligibility.quality,
        "effective_eligibility_reason": st.effective_eligibility.reason,
        "effective_bucket": st.effective_bucket,
        "validity_value": st.validity.value
        if st.validity.quality == "OBSERVED"
        else None,
        "validity_quality": st.validity.quality,
        "validity_reason": st.validity.reason,
        "freshness": st.freshness,
        "health_value": st.health.value if st.health.quality == "OBSERVED" else None,
        "health_quality": st.health.quality,
        "health_reason": st.health.reason,
        "tracking": st.tracking,
        "downgrade_candidate": st.downgrade_candidate,
        "downgrade_count": st.downgrade_count,
        "expiry_count": st.expiry_count,
        "improvement_baseline": st.improvement_baseline,
        "exit_ordinal": st.exit_ordinal,
        "exit_reason": st.exit_reason,
        "reentry_armed": st.reentry_armed,
        "transitions": st.transitions,
        "preserved_episode_ids": st.preserved_episode_ids,
    }


def _risk_result_from_pg_row(row: Mapping[str, Any] | None) -> RiskResult | None:
    if not row:
        return None
    q = row.get("risk_quality")
    lvl = row.get("risk_level")
    level_fv = (
        FieldValue(lvl)
        if q == "OBSERVED" and lvl is not None
        else unknown(row.get("risk_reason") or "PRIOR_RISK_UNKNOWN")
    )
    return RiskResult(
        level=level_fv,
        possible_levels=(lvl,) if lvl else (),
        lower_bound=str(row.get("lower_bound") or "LOW"),
        upper_bound=str(row.get("upper_bound") or "EXTREME"),
        primary_reason=row.get("primary_reason"),
        primary_reason_candidates=(row["primary_reason"],)
        if row.get("primary_reason")
        else (),
        confirmed_reasons=tuple(row.get("confirmed_reasons") or ()),
        possible_reasons=tuple(row.get("possible_reasons") or ()),
        structure_na=(row.get("structure_applicability") == "ABSENT"),
        support_na=(row.get("support_applicability") == "ABSENT"),
        known_inputs=0,
        applicable_inputs=0,
        contract=RISK_CONTRACT,
    )


def _support_obs_from_pg_rows(
    rows: Sequence[Mapping[str, Any]], *, ordinal: int
) -> tuple[SupportObservation, ...]:
    obs: list[SupportObservation] = []
    for s in rows:
        uid = str(
            s.get("anchor_uid")
            or f"{s.get('d1_structure_id')}:{s['created_session'].isoformat()}"
        )
        obs_ok = (
            s.get("after_quality") == "OBSERVED"
            and s.get("comparability_status", "COMPARABLE") == "COMPARABLE"
            and s.get("observation_status") != "NOT_COMPARABLE"
        )
        obs.append(
            SupportObservation(
                anchor_id=uid,
                ordinal=ordinal,
                state=(s.get("after_state") or "UNKNOWN") if obs_ok else "UNKNOWN",
                acceptance=(s.get("after_acceptance") or "UNKNOWN")
                if obs_ok
                else "UNKNOWN",
                freshness=s.get("after_freshness") or "STALE",
                breach_streak=int(s.get("after_breach_streak") or 0),
                acceptance_streak=int(s.get("after_acceptance_streak") or 0),
                test_count=int(s.get("after_test_count") or 0),
                separated_sessions=int(s.get("after_separated_sessions") or 0),
                retest_qualified=bool(s.get("after_retest_qualified"))
                if obs_ok
                else False,
            )
        )
    return tuple(obs)


_PREDICATE_INPUT_KEYS: dict[str, tuple[str, ...]] = {
    "birth_age": ("birth_age",),
    "peak_age": ("peak_age",),
    "drawdown": ("depth",),
    "contraction": ("ratio",),
    "confirmation_amount": ("ratio",),
    "clv": ("clv",),
    "core_ma_slope20_atr": ("value",),
    "rps20": ("value",),
    "above_core_prior_high20": ("close", "prior_high20"),
    "amr20_band": ("amr20",),
}


def _parse_predicate_fraction(raw_val: Any) -> Fraction | None:
    if raw_val is None or isinstance(raw_val, bool):
        return None
    if isinstance(raw_val, FieldValue):
        if raw_val.quality != "OBSERVED":
            return None
        return _parse_predicate_fraction(raw_val.value)
    if isinstance(raw_val, Mapping):
        if "numerator" in raw_val and "denominator" in raw_val:
            return _frac(raw_val.get("numerator"), raw_val.get("denominator"))
        return None
    if isinstance(raw_val, Fraction):
        return raw_val
    if isinstance(raw_val, Decimal):
        return Fraction(raw_val) if raw_val.is_finite() else None
    if isinstance(raw_val, int):
        return Fraction(raw_val)
    return None


def _extract_predicate_value(
    pid: str,
    info: Mapping[str, Any],
) -> FieldValue:
    """统一从谓词字典提取精确数值（兼容 FieldValue 顶层 value 与 d0_pb_continue._pred 的 inputs payload）。"""
    top_val = info.get("value")
    if isinstance(top_val, FieldValue):
        return top_val
    st_val = str(info.get("state") or "UNKNOWN")
    reason_text = str(info.get("reason") or f"{pid.upper()}_UNKNOWN")
    if top_val is not None:
        parsed = _parse_predicate_fraction(top_val)
        if parsed is not None:
            return FieldValue(parsed)
    if st_val not in ("TRUE", "FALSE"):
        return unknown(reason_text)
    inputs = info.get("inputs")
    if not isinstance(inputs, Mapping):
        return unknown(reason_text)
    keys = _PREDICATE_INPUT_KEYS.get(pid, ("value",))
    if keys == ("close", "prior_high20"):
        close_f = _parse_predicate_fraction(inputs.get("close"))
        ph20_f = _parse_predicate_fraction(inputs.get("prior_high20"))
        if close_f is not None and ph20_f is not None:
            return FieldValue(close_f - ph20_f)
        return unknown(reason_text)
    for k in keys:
        parsed = _parse_predicate_fraction(inputs.get(k))
        if parsed is not None:
            return FieldValue(parsed)
    return unknown(reason_text)


def _extract_common_gate_fvs(
    sample_adm: Mapping[str, Any] | None,
) -> tuple[FieldValue, ...]:
    if not sample_adm:
        return ()
    gate_cols = (
        "research_universe_state",
        "actual_quote_state",
        "coordinate_window_state",
        "d0_liquidity_state",
        "non_core_damage_state",
        "non_extreme_extension_state",
        "non_first_day_damage_state",
        "non_severe_drop_state",
    )
    return tuple(
        _tri_field(sample_adm.get(col), reason_if_unknown=f"{col.upper()}_UNKNOWN")
        for col in gate_cols
    )


def _build_d0_near_miss_inputs(
    adm_by_scenario: Mapping[str, Mapping[str, Any]],
    scen_by_scenario: Mapping[str, Mapping[str, Any]],
    *,
    pb_res: Any | None = None,
    atr_today: FieldValue | None = None,
    common_gate_fvs: Sequence[FieldValue] | None = None,
) -> tuple[tuple[FieldValue, ...], tuple[D0ScenarioNearMissInput, ...]]:
    """从PG正式准入共同门与四场景具名谓词事实组装Near-Miss路径输入（严格按§11.7冻结合同）。"""
    sample_adm = next(iter(adm_by_scenario.values()), None)
    if common_gate_fvs:
        common_gates = tuple(common_gate_fvs)
    elif sample_adm is not None:
        common_gates = _extract_common_gate_fvs(sample_adm)
    else:
        return (), ()
    scenario_inputs: list[D0ScenarioNearMissInput] = []
    for scenario_name, spec_map in (
        (
            "LAUNCH_CONFIRM",
            {
                "clv_at_least_060": ("clv", Fraction(3, 5), ">=", None),
                "amr20_mean_prior_at_least_120": (
                    "amr_lower",
                    Fraction(6, 5),
                    ">=",
                    None,
                ),
                "close_above_phc20": ("breakout_price", Fraction(0), ">", atr_today),
            },
        ),
        (
            "RECOVERY_TURN",
            {
                "clv": ("clv", Fraction(11, 20), ">=", None),
                "rps5_delta3": ("rps_pp", Fraction(0), ">", None),
                "amr": ("amr_lower", Fraction(21, 20), ">=", None),
            },
        ),
    ):
        sc_row = scen_by_scenario.get(scenario_name)
        adm_row = adm_by_scenario.get(scenario_name)
        if not sc_row:
            continue
        raw_state = sc_row.get("scenario_state") or (
            adm_row.get("raw_state") if adm_row else None
        )
        raw_matched = _tri_field(raw_state, reason_if_unknown="RAW_STATE_UNKNOWN")
        p_ids = sc_row.get("predicate_ids") or ()
        p_states = sc_row.get("predicate_states") or ()
        p_quals = sc_row.get("predicate_qualities") or ()
        p_nums = sc_row.get("predicate_value_numerators") or ()
        p_dens = sc_row.get("predicate_value_denominators") or ()
        hard_gates: list[FieldValue] = []
        soft_specs: list[D0SoftPredicateSpec] = []
        for idx, pid in enumerate(p_ids):
            st_val = p_states[idx] if idx < len(p_states) else "UNKNOWN"
            q_val = p_quals[idx] if idx < len(p_quals) else "UNKNOWN"
            num_val = p_nums[idx] if idx < len(p_nums) else None
            den_val = p_dens[idx] if idx < len(p_dens) else None
            if pid in spec_map:
                soft_kind, threshold, op, atr_arg = spec_map[pid]
                val_frac = _frac(num_val, den_val)
                actual_fv = (
                    FieldValue(val_frac)
                    if q_val == "OBSERVED" and val_frac is not None
                    else unknown(f"{pid.upper()}_UNKNOWN")
                )
                soft_specs.append(
                    D0SoftPredicateSpec(
                        predicate_id=pid,
                        soft_kind=soft_kind,
                        actual=actual_fv,
                        threshold=FieldValue(threshold),
                        operator=op,
                        atr_today=atr_arg,
                    )
                )
            else:
                hard_gates.append(
                    _tri_field(st_val, reason_if_unknown=f"{pid.upper()}_UNKNOWN")
                )
        scenario_inputs.append(
            D0ScenarioNearMissInput(
                scenario=scenario_name,
                path_id=f"{scenario_name}:PRIMARY",
                raw_matched=raw_matched,
                hard_gates=tuple(hard_gates),
                soft_specs=tuple(soft_specs),
            )
        )

    if pb_res is not None:
        eligible_parents = (
            getattr(getattr(pb_res, "eligible_parents", None), "parents", ()) or ()
        )
        parent_by_id = {p.episode_id: p for p in eligible_parents}
        pb_batch = pb_res.pb_continue_result.pullback
        for d in pb_batch.details:
            if not isinstance(d, PullbackParentDetail) or not d.predicates:
                continue
            hard_gates = []
            soft_specs = []
            if d.admission.status == "BLOCKED":
                hard_gates.append(FieldValue(False))
            p_ctx = parent_by_id.get(d.parent_episode_id)
            if p_ctx is not None:
                hard_gates.append(
                    FieldValue(False)
                    if p_ctx.support_valid.quality == "NOT_APPLICABLE"
                    else p_ctx.support_valid
                )
            for pid, p_info in d.predicates.items():
                st_val = p_info.get("state")
                actual_fv = _extract_predicate_value(pid, p_info)
                if pid == "drawdown":
                    soft_specs.append(
                        D0SoftPredicateSpec(
                            predicate_id="drawdown_min",
                            soft_kind="pb_drawdown_lower",
                            actual=actual_fv,
                            threshold=FieldValue(Fraction(3, 100)),
                            operator=">=",
                        )
                    )
                    soft_specs.append(
                        D0SoftPredicateSpec(
                            predicate_id="drawdown_max",
                            soft_kind="pb_drawdown_upper",
                            actual=actual_fv,
                            threshold=FieldValue(Fraction(12, 100)),
                            operator="<=",
                        )
                    )
                elif pid == "contraction":
                    soft_specs.append(
                        D0SoftPredicateSpec(
                            predicate_id="contraction",
                            soft_kind="pb_contraction",
                            actual=actual_fv,
                            threshold=FieldValue(Fraction(17, 20)),
                            operator="<=",
                        )
                    )
                elif pid == "clv":
                    soft_specs.append(
                        D0SoftPredicateSpec(
                            predicate_id="clv",
                            soft_kind="clv",
                            actual=actual_fv,
                            threshold=FieldValue(Fraction(11, 20)),
                            operator=">=",
                        )
                    )
                else:
                    hard_gates.append(
                        _tri_field(st_val, reason_if_unknown=f"{pid.upper()}_UNKNOWN")
                    )
            scenario_inputs.append(
                D0ScenarioNearMissInput(
                    scenario="STRONG_PULLBACK",
                    path_id=f"STRONG_PULLBACK:{d.parent_episode_id}:{d.peak_event_id or 'NO_PEAK'}",
                    raw_matched=d.raw,
                    hard_gates=tuple(hard_gates),
                    soft_specs=tuple(soft_specs),
                    parent_episode_id=d.parent_episode_id,
                    peak_id=d.peak_event_id,
                )
            )

        cont_batch = pb_res.pb_continue_result.trend_continue
        for d in cont_batch.details:
            if not isinstance(d, ContinueParentDetail) or not d.predicates:
                continue
            hard_gates = []
            soft_specs = []
            if d.admission.status == "BLOCKED":
                hard_gates.append(FieldValue(False))
            p_ctx = parent_by_id.get(d.parent_episode_id)
            if p_ctx is not None and p_ctx.support_valid.quality != "NOT_APPLICABLE":
                hard_gates.append(p_ctx.support_valid)
            for pid, p_info in d.predicates.items():
                st_val = p_info.get("state")
                actual_fv = _extract_predicate_value(pid, p_info)
                if pid == "rps20":
                    soft_specs.append(
                        D0SoftPredicateSpec(
                            predicate_id="rps20",
                            soft_kind="rps_pp",
                            actual=actual_fv,
                            threshold=FieldValue(Fraction(70)),
                            operator=">=",
                        )
                    )
                elif pid == "clv":
                    soft_specs.append(
                        D0SoftPredicateSpec(
                            predicate_id="clv",
                            soft_kind="clv",
                            actual=actual_fv,
                            threshold=FieldValue(Fraction(11, 20)),
                            operator=">=",
                        )
                    )
                elif pid == "amr20_band":
                    soft_specs.append(
                        D0SoftPredicateSpec(
                            predicate_id="amr20_min",
                            soft_kind="amr_lower",
                            actual=actual_fv,
                            threshold=FieldValue(Fraction(4, 5)),
                            operator=">=",
                        )
                    )
                    soft_specs.append(
                        D0SoftPredicateSpec(
                            predicate_id="amr20_max",
                            soft_kind="amr_upper",
                            actual=actual_fv,
                            threshold=FieldValue(Fraction(5, 2)),
                            operator="<=",
                        )
                    )
                elif pid == "above_core_prior_high20":
                    soft_specs.append(
                        D0SoftPredicateSpec(
                            predicate_id="above_core_prior_high20",
                            soft_kind="breakout_price",
                            actual=actual_fv,
                            threshold=FieldValue(Fraction(0)),
                            operator=">",
                            atr_today=atr_today,
                        )
                    )
                else:
                    hard_gates.append(
                        _tri_field(st_val, reason_if_unknown=f"{pid.upper()}_UNKNOWN")
                    )
            scenario_inputs.append(
                D0ScenarioNearMissInput(
                    scenario="TREND_CONTINUE",
                    path_id=f"TREND_CONTINUE:{d.parent_episode_id}",
                    raw_matched=d.raw,
                    hard_gates=tuple(hard_gates),
                    soft_specs=tuple(soft_specs),
                    parent_episode_id=d.parent_episode_id,
                    peak_id=None,
                )
            )

    return common_gates, tuple(scenario_inputs)


def _predicates_to_pg_arrays(
    predicates: Mapping[str, Mapping[str, Any]],
    *,
    fallback_id: str,
    fallback_fv: FieldValue,
    fallback_reason: str,
) -> tuple[
    list[str],
    list[str],
    list[str],
    list[str],
    list[Decimal | None],
    list[Decimal | None],
]:
    if not predicates:
        st_str = (
            "TRUE"
            if fallback_fv.quality == "OBSERVED" and fallback_fv.value is True
            else ("FALSE" if fallback_fv.quality == "OBSERVED" else "UNKNOWN")
        )
        q_str = "OBSERVED" if fallback_fv.quality == "OBSERVED" else "UNKNOWN"
        return (
            [fallback_id],
            [st_str],
            [q_str],
            [fallback_fv.reason or fallback_reason],
            [None],
            [None],
        )
    p_ids: list[str] = []
    p_states: list[str] = []
    p_quals: list[str] = []
    p_reasons: list[str] = []
    p_nums: list[Decimal | None] = []
    p_dens: list[Decimal | None] = []
    for pid, info in predicates.items():
        p_ids.append(str(pid))
        st = str(info.get("state") or "UNKNOWN")
        p_states.append(st if st in ("TRUE", "FALSE", "UNKNOWN") else "UNKNOWN")
        val_fv = _extract_predicate_value(str(pid), info)
        if val_fv.quality == "OBSERVED":
            p_quals.append("OBSERVED")
            p_reasons.append(str(info.get("reason") or "OK"))
            v = val_fv.value
            if isinstance(v, Fraction):
                p_nums.append(Decimal(v.numerator))
                p_dens.append(Decimal(v.denominator))
            elif isinstance(v, Decimal) and v.is_finite():
                f = Fraction(v)
                p_nums.append(Decimal(f.numerator))
                p_dens.append(Decimal(f.denominator))
            elif isinstance(v, int) and not isinstance(v, bool):
                p_nums.append(Decimal(v))
                p_dens.append(Decimal(1))
            else:
                p_nums.append(None)
                p_dens.append(None)
        else:
            p_quals.append("OBSERVED" if st in ("TRUE", "FALSE") else "UNKNOWN")
            p_reasons.append(
                str(
                    val_fv.reason or info.get("reason") or f"{str(pid).upper()}_UNKNOWN"
                )
            )
            p_nums.append(None)
            p_dens.append(None)
    return p_ids, p_states, p_quals, p_reasons, p_nums, p_dens


def _pg_coord(coord_id: str | None) -> str | None:
    if not coord_id:
        return None
    if coord_id.startswith("BAOSTOCK_QFQ:") or coord_id == "FORWARD_ADJUSTED":
        return "FORWARD_ADJUSTED"
    if coord_id.startswith("BAOSTOCK_RAW:") or coord_id == "RAW_UNADJUSTED":
        return "RAW_UNADJUSTED"
    return coord_id


def _extract_gate_vals_from_adm(
    adm_map: Mapping[str, Mapping[str, Any]],
) -> tuple[Mapping[str, Any], dict[str, Any]]:
    sample_adm = next(iter(adm_map.values()), {})
    gate_vals: dict[str, Any] = {}
    for g in (
        "research_universe",
        "actual_quote",
        "coordinate_window",
        "d0_liquidity",
        "non_core_damage",
        "non_extreme_extension",
        "non_first_day_damage",
        "non_severe_drop",
    ):
        g_state = sample_adm.get(f"{g}_state") or "UNKNOWN"
        gate_vals[f"{g}_state"] = g_state
        gate_vals[f"{g}_reason"] = sample_adm.get(f"{g}_reason") or (
            "GATE_TRUE" if g_state == "TRUE" else f"{g.upper()}_{g_state}"
        )
    return sample_adm, gate_vals


def _serialize_no_parent_pb_cont_for_pg(
    *,
    sym: str,
    seed_row: Mapping[str, Any],
    adm_map: Mapping[str, Mapping[str, Any]],
    prior_gap_reason: str | None,
    source_capture_id: int,
    total_source_count: int = 1,
) -> list[dict[str, Any]]:
    """为研究总体中无昨日FORMAL父的股票生成PB/CONTINUE诚实总事实（已证无父FALSE，前态缺证UNKNOWN，无虚假父明细）。"""
    if not seed_row or seed_row.get("seed_fact_id") is None:
        return []
    sample_adm, gate_vals = _extract_gate_vals_from_adm(adm_map)
    if prior_gap_reason is not None:
        raw_state = "UNKNOWN"
        summary_reason = prior_gap_reason
        adm_status = "NOT_READY"
        pred_state = "UNKNOWN"
        pred_qual = "UNKNOWN"
    else:
        raw_state = "FALSE"
        summary_reason = "NO_ELIGIBLE_FORMAL_PARENT"
        adm_status = "NOT_REQUIRED"
        pred_state = "FALSE"
        pred_qual = "OBSERVED"
    src_count = max(1, int(total_source_count))
    facts: list[dict[str, Any]] = []
    for scenario_name in ("STRONG_PULLBACK", "TREND_CONTINUE"):
        facts.append(
            {
                "source_symbol": sym,
                "seed_fact_id": seed_row["seed_fact_id"],
                "source_capture_id": seed_row.get("source_capture_id")
                or source_capture_id,
                "parser_contract": "STOCK_A_D0_PB_CONTINUE_R1",
                "scenario": scenario_name,
                "producer_version": _D0_SCENARIO_PRODUCER_VERSIONS[scenario_name],
                "algorithm_contract": "STOCK_A_D0_PB_CONTINUE_R1",
                "session_contract": "OFFICIAL_A_SHARE_SESSION",
                "history_window_start": seed_row["history_window_start"],
                "history_session_count": seed_row["history_session_count"],
                "source_count": src_count,
                "scenario_state": raw_state,
                "scenario_reason": summary_reason,
                "predicate_ids": [f"{scenario_name.lower()}_parent"],
                "predicate_states": [pred_state],
                "predicate_qualities": [pred_qual],
                "predicate_reason_codes": [summary_reason],
                "predicate_value_numerators": [None],
                "predicate_value_denominators": [None],
                "source_time_quality": seed_row["source_time_quality"],
                "source_clock_role": seed_row["source_clock_role"],
                "source_available_at": seed_row["source_available_at"],
                "capture_started_at": sample_adm.get("capture_started_at")
                or seed_row["source_available_at"],
                "capture_validated_at": sample_adm.get("capture_validated_at")
                or seed_row["source_available_at"],
                "profile_revision": sample_adm.get("profile_revision")
                or "CORE_PROFILE_R1",
                "seed_producer_version": _SEED_PRODUCER_VERSION,
                "identity_scope": sample_adm.get("identity_scope")
                or "SOURCE_SYMBOL_ONLY",
                "calendar_id": "CN_A_SHARE_OFFICIAL",
                "calendar_revision": seed_row["calendar_revision"],
                "research_mode": sample_adm.get("research_mode")
                or "NON_PIT_EXPLORATORY",
                "price_coordinate": "FORWARD_ADJUSTED",
                "admission_contract": _ADMISSION_CONTRACT,
                "admission_status": adm_status,
                "admission_reason": summary_reason,
                "reason_codes": (summary_reason,),
                **gate_vals,
                "event_id": None,
                "failure_level": None,
                "failure_coordinate": None,
                "failure_source": None,
                "failure_window_start": None,
                "failure_window_session_count": None,
                "failure_source_event_ref": None,
                "effective_from_session": None,
                "prior_episode_id": None,
                "linked_anchor_id": None,
            }
        )
    return facts


def _serialize_pb_cont_for_pg(
    *,
    sym: str,
    pb_res: Any,
    seed_row: Mapping[str, Any],
    adm_map: Mapping[str, Mapping[str, Any]],
    target_session: date,
    next_session: date | None,
    sessions: Sequence[date],
    source_capture_id: int,
    total_source_count: int = 1,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """将单股PB/CONTINUE内存评估结果序列化为PG四场景事实、正式准入及逐父/峰明细。"""
    if not seed_row or seed_row.get("seed_fact_id") is None:
        return [], []
    sample_adm, gate_vals = _extract_gate_vals_from_adm(adm_map)
    src_count = max(1, int(total_source_count))

    facts: list[dict[str, Any]] = []
    details_out: list[dict[str, Any]] = []
    pb_cont = pb_res.pb_continue_result
    for scenario_name, batch in (
        ("STRONG_PULLBACK", pb_cont.pullback),
        ("TREND_CONTINUE", pb_cont.trend_continue),
    ):
        raw_fv = batch.raw_summary
        raw_state = (
            "TRUE"
            if raw_fv.quality == "OBSERVED" and raw_fv.value is True
            else ("FALSE" if raw_fv.quality == "OBSERVED" else "UNKNOWN")
        )
        rep_detail = next(
            (d for d in batch.details if d.admission.status == "READY"),
            batch.details[0] if batch.details else None,
        )
        rep_preds = rep_detail.predicates if rep_detail is not None else {}
        p_ids, p_states, p_quals, p_reasons, p_nums, p_dens = _predicates_to_pg_arrays(
            rep_preds,
            fallback_id=f"{scenario_name.lower()}_summary",
            fallback_fv=raw_fv,
            fallback_reason=batch.summary_reason,
        )
        ready_detail = next(
            (
                d
                for d in batch.details
                if d.admission.status == "READY" and d.admission.event is not None
            ),
            None,
        )
        if ready_detail is not None:
            adm_status = "READY"
            adm_reason = ready_detail.admission.reason
            reason_codes = (ready_detail.admission.reason,)
            ev = ready_detail.admission.event
            eff_ord = ev.failure_line.effective_ordinal
            eff_sess = (
                sessions[eff_ord]
                if 0 <= eff_ord < len(sessions)
                else (next_session or (target_session + timedelta(days=1)))
            )
            ev_fields = {
                "event_id": ev.event_id,
                "failure_level": ev.failure_line.value,
                "failure_coordinate": _pg_coord(ev.failure_line.coordinate_id),
                "failure_source": ev.failure_line.source_ref,
                "failure_window_start": target_session,
                "failure_window_session_count": 1,
                "failure_source_event_ref": ev.evidence_ref,
                "effective_from_session": eff_sess,
                "prior_episode_id": ev.prior_episode_id,
                "linked_anchor_id": ev.linked_anchor_id,
            }
        else:
            if raw_state == "FALSE":
                adm_status = "NOT_REQUIRED"
            elif any(d.admission.status == "BLOCKED" for d in batch.details):
                adm_status = "BLOCKED"
            else:
                adm_status = "NOT_READY"
            adm_reason = (
                rep_detail.admission.reason
                if rep_detail is not None
                else batch.summary_reason
            )
            reason_codes = (adm_reason,)
            ev_fields = {
                "event_id": None,
                "failure_level": None,
                "failure_coordinate": None,
                "failure_source": None,
                "failure_window_start": None,
                "failure_window_session_count": None,
                "failure_source_event_ref": None,
                "effective_from_session": None,
                "prior_episode_id": rep_detail.parent_episode_id
                if rep_detail is not None
                else None,
                "linked_anchor_id": None,
            }

        facts.append(
            {
                "source_symbol": sym,
                "seed_fact_id": seed_row["seed_fact_id"],
                "source_capture_id": seed_row.get("source_capture_id")
                or source_capture_id,
                "parser_contract": "STOCK_A_D0_PB_CONTINUE_R1",
                "scenario": scenario_name,
                "producer_version": _D0_SCENARIO_PRODUCER_VERSIONS[scenario_name],
                "algorithm_contract": "STOCK_A_D0_PB_CONTINUE_R1",
                "session_contract": "OFFICIAL_A_SHARE_SESSION",
                "history_window_start": seed_row["history_window_start"],
                "history_session_count": seed_row["history_session_count"],
                "source_count": src_count,
                "scenario_state": raw_state,
                "scenario_reason": batch.summary_reason,
                "predicate_ids": p_ids,
                "predicate_states": p_states,
                "predicate_qualities": p_quals,
                "predicate_reason_codes": p_reasons,
                "predicate_value_numerators": p_nums,
                "predicate_value_denominators": p_dens,
                "source_time_quality": seed_row["source_time_quality"],
                "source_clock_role": seed_row["source_clock_role"],
                "source_available_at": seed_row["source_available_at"],
                "capture_started_at": sample_adm.get("capture_started_at")
                or seed_row["source_available_at"],
                "capture_validated_at": sample_adm.get("capture_validated_at")
                or seed_row["source_available_at"],
                "profile_revision": sample_adm.get("profile_revision")
                or "CORE_PROFILE_R1",
                "seed_producer_version": _SEED_PRODUCER_VERSION,
                "identity_scope": sample_adm.get("identity_scope")
                or "SOURCE_SYMBOL_ONLY",
                "calendar_id": "CN_A_SHARE_OFFICIAL",
                "calendar_revision": seed_row["calendar_revision"],
                "research_mode": sample_adm.get("research_mode")
                or "NON_PIT_EXPLORATORY",
                "price_coordinate": "FORWARD_ADJUSTED",
                "admission_contract": _ADMISSION_CONTRACT,
                "admission_status": adm_status,
                "admission_reason": adm_reason,
                "reason_codes": reason_codes,
                **gate_vals,
                **ev_fields,
            }
        )

        for d in batch.details:
            d_raw_state = (
                "TRUE"
                if d.raw.quality == "OBSERVED" and d.raw.value is True
                else ("FALSE" if d.raw.quality == "OBSERVED" else "UNKNOWN")
            )
            dp_ids, dp_states, dp_quals, dp_reasons, dp_nums, dp_dens = (
                _predicates_to_pg_arrays(
                    d.predicates,
                    fallback_id=f"{scenario_name.lower()}_parent",
                    fallback_fv=d.raw,
                    fallback_reason=d.reason_code,
                )
            )
            peak_id = getattr(d, "peak_event_id", None)
            peak_ord = getattr(d, "peak_ordinal", None)
            d_ev = d.admission.event
            if d.admission.status == "READY" and d_ev is not None:
                d_eff_ord = d_ev.failure_line.effective_ordinal
                d_eff_sess = (
                    sessions[d_eff_ord]
                    if 0 <= d_eff_ord < len(sessions)
                    else (next_session or (target_session + timedelta(days=1)))
                )
                d_ev_fields = {
                    "event_id": d_ev.event_id,
                    "linked_anchor_id": d_ev.linked_anchor_id,
                    "failure_level": d_ev.failure_line.value,
                    "failure_coordinate": _pg_coord(d_ev.failure_line.coordinate_id),
                    "failure_source": d_ev.failure_line.source_ref,
                    "failure_window_start": target_session,
                    "failure_window_session_count": 1,
                    "effective_from_session": d_eff_sess,
                }
            else:
                d_ev_fields = {
                    "event_id": None,
                    "linked_anchor_id": None,
                    "failure_level": None,
                    "failure_coordinate": None,
                    "failure_source": None,
                    "failure_window_start": None,
                    "failure_window_session_count": None,
                    "effective_from_session": None,
                }
            details_out.append(
                {
                    "source_symbol": sym,
                    "scenario": scenario_name,
                    "parent_episode_id": d.parent_episode_id,
                    "peak_event_id": peak_id,
                    "peak_ordinal_key": int(peak_ord) if peak_ord is not None else -1,
                    "raw_state": d_raw_state,
                    "raw_reason": d.reason_code,
                    "predicate_ids": dp_ids,
                    "predicate_states": dp_states,
                    "predicate_qualities": dp_quals,
                    "predicate_reason_codes": dp_reasons,
                    "predicate_value_numerators": dp_nums,
                    "predicate_value_denominators": dp_dens,
                    "admission_status": d.admission.status,
                    "admission_reason": d.admission.reason,
                    "reason_codes": (d.admission.reason,),
                    **d_ev_fields,
                }
            )
    return facts, details_out


def run_r04_d2_and_downstream_day(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_capture_id: int,
    research_cutoff: date | None = None,
) -> dict[str, Any]:
    """在唯一PG中按当日合法输入顺序执行PB/CONT、D2逐日状态推进与下游风险/Priority/Radar/Near-Miss原子持久化。"""
    effective_cutoff = research_cutoff or target_session
    payload, _meta, _chain, sessions = _load_calendar(target_session)
    calendar_id = str(payload.get("calendar_id") or "CN_A_SHARE_OFFICIAL")
    ordinal = sessions.index(target_session)
    prev_session = sessions[ordinal - 1] if ordinal > 0 else None
    next_session = sessions[ordinal + 1] if ordinal + 1 < len(sessions) else None
    session_to_ordinal = {s: idx for idx, s in enumerate(sessions)}
    rules = D2Rules(D2_CONTRACT, 2, 10, 3, 3)
    rule_identity = rules.identity

    # 1. 读取 t-1 D2 状态、选主、父峰、前日风险与前日 D1 支撑
    prior_rows = (
        read_d2_day_states(
            config,
            session_date=prev_session,
            calendar_id=calendar_id,
        )
        if prev_session is not None
        else []
    )
    prior_states_by_symbol: dict[str, list[State]] = {}
    prior_episode_birth_session: dict[str, date] = {}
    prior_episode_effective_from: dict[str, date | None] = {}
    prior_episode_failure_window: dict[str, str | None] = {}
    prior_episode_evidence_ref: dict[str, str | None] = {}
    for r in prior_rows:
        st_obj = _state_from_pg_row(r, session_to_ordinal=session_to_ordinal)
        prior_states_by_symbol.setdefault(r["entity_id"], []).append(st_obj)
        prior_episode_birth_session[r["episode_id"]] = r["birth_session"]
        prior_episode_effective_from[r["episode_id"]] = r.get("effective_from_session")
        prior_episode_failure_window[r["episode_id"]] = r.get("failure_window")
        prior_episode_evidence_ref[r["episode_id"]] = r.get("evidence_ref")

    prior_primary_rows = (
        read_d2_primary_projections(
            config,
            session_date=prev_session,
            calendar_id=calendar_id,
        )
        if prev_session is not None
        else []
    )
    prior_primary_by_symbol = {
        pr["entity_id"]: pr.get("primary_episode_id") for pr in prior_primary_rows
    }
    prev_session_completed = bool(prior_primary_rows) or bool(prior_rows)

    prior_peaks_rows = read_d2_parent_peaks_before(config, before_ordinal=ordinal)
    prior_peaks_by_ep = select_prior_peaks_by_parent(
        prior_peaks_rows, before_ordinal=ordinal
    )

    prior_risk_by_symbol: dict[str, RiskResult] = {}
    if prev_session is not None:
        prev_risk_summary = read_stock_risk_summary_and_rows(
            config, session_date=prev_session
        )
        for rr in prev_risk_summary.get("items", ()):
            parsed_rr = _risk_result_from_pg_row(rr)
            if parsed_rr is not None:
                prior_risk_by_symbol[rr["source_symbol"]] = parsed_rr

    d1_supports_by_symbol = read_d1_active_support_observations(
        config, target_session=target_session, include_inactive=True
    )
    prior_d1_supports_by_symbol = (
        read_d1_active_support_observations(
            config, target_session=prev_session, include_inactive=True
        )
        if prev_session is not None
        else {}
    )

    # 2. 只读读取当日 Seed、Profile、Raw QFQ、正式准入、D0场景谓词、LOO 及改善历史
    pb_window_start = max(0, ordinal - 28)
    pb_sessions = tuple(sessions[pb_window_start : ordinal + 1])

    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _conn,
        cursor,
        _scope,
    ):
        seed_rows = cursor.execute(
            """
            SELECT id AS seed_fact_id, source_symbol, source_capture_id,
                   formal_seed_state, formal_prewatch_state,
                   core_price_damage, core_price_damage_quality, core_price_damage_reason,
                   profile_extension_value, profile_extension_quality, profile_extension_reason,
                   core_liquidity_mean20_numerator, core_liquidity_mean20_denominator,
                   core_liquidity_mean20_quality,
                   rps5_delta3_numerator, rps5_delta3_denominator, rps5_delta3_quality,
                   rps20_numerator, rps20_denominator, rps20_quality,
                   target_actual_quote, target_close_numerator, target_close_denominator,
                   target_close_quality,
                   calendar_revision,
                   source_time_quality, source_clock_role, source_available_at,
                   research_mode, price_coordinate,
                   history_window_start, history_session_count
            FROM stock.source_pool_seed_day_facts
            WHERE target_session = %s
            ORDER BY source_symbol
            """,
            (target_session,),
        ).fetchall()
        seed_cols = [d[0] for d in cursor.description]
        seed_by_symbol = {r[1]: dict(zip(seed_cols, r)) for r in seed_rows}

        prof_rows = cursor.execute(
            """
            SELECT source_symbol,
                   ma20_exact_numerator, ma20_exact_denominator,
                   ma20_quality, ma20_reason,
                   ma_slope20_atr_exact_numerator, ma_slope20_atr_exact_denominator,
                   ma_slope20_atr_quality, ma_slope20_atr_reason,
                   prior_high20_exact_numerator, prior_high20_exact_denominator,
                   prior_high20_quality, prior_high20_reason,
                   atr20_exact_numerator, atr20_exact_denominator,
                   atr20_quality, atr20_reason
            FROM stock.source_symbol_price_profiles
            WHERE target_session = %s
            """,
            (target_session,),
        ).fetchall()
        prof_cols = [d[0] for d in cursor.description]
        prof_by_symbol = {r[0]: dict(zip(prof_cols, r)) for r in prof_rows}

        cursor.execute(
            """
            SELECT DISTINCT source_symbol
            FROM stock.source_d1_structure_day_facts
            WHERE target_session = %s
            """,
            (target_session,),
        )
        d1_completed_symbols = {str(r[0]) for r in cursor.fetchall()}

        prior_d1_completed_symbols: set[str] = set()
        if prev_session is not None:
            cursor.execute(
                """
                SELECT DISTINCT source_symbol
                FROM stock.source_d1_structure_day_facts
                WHERE target_session = %s
                """,
                (prev_session,),
            )
            prior_d1_completed_symbols = {str(r[0]) for r in cursor.fetchall()}

        raw_qfq_rows = cursor.execute(
            """
            SELECT source_symbol,
                   qfq_close, qfq_close_quality, qfq_price_reason AS qfq_close_reason,
                   qfq_high, qfq_high_quality, qfq_high_reason,
                   qfq_source_capture_id
            FROM stock.source_raw_daily
            WHERE session_date = %s AND source_capture_id = %s
            """,
            (target_session, source_capture_id),
        ).fetchall()
        raw_qfq_cols = [d[0] for d in cursor.description]
        raw_qfq_by_symbol = {r[0]: dict(zip(raw_qfq_cols, r)) for r in raw_qfq_rows}

        adm_rows = cursor.execute(
            """
            SELECT source_symbol, scenario, admission_status, raw_state, event_id,
                   failure_level_numerator, failure_level_denominator,
                   failure_coordinate, failure_source, failure_window_start,
                   effective_from_session, prior_episode_id, linked_anchor_id,
                   profile_revision, capture_started_at, capture_validated_at,
                   identity_scope, research_mode,
                   research_universe_state, research_universe_reason,
                   actual_quote_state, actual_quote_reason,
                   coordinate_window_state, coordinate_window_reason,
                   d0_liquidity_state, d0_liquidity_reason,
                   non_core_damage_state, non_core_damage_reason,
                   non_extreme_extension_state, non_extreme_extension_reason,
                   non_first_day_damage_state, non_first_day_damage_reason,
                   non_severe_drop_state, non_severe_drop_reason
            FROM stock.source_d0_formal_admission_day_facts
            WHERE target_session = %s
              AND scenario IN ('LAUNCH_CONFIRM', 'RECOVERY_TURN')
            ORDER BY source_symbol, scenario
            """,
            (target_session,),
        ).fetchall()
        adm_cols = [d[0] for d in cursor.description]
        events_by_symbol: dict[str, list[Event]] = {}
        adm_by_symbol_scenario: dict[str, dict[str, dict[str, Any]]] = {}
        for r in adm_rows:
            ar = dict(zip(adm_cols, r))
            adm_by_symbol_scenario.setdefault(ar["source_symbol"], {})[
                ar["scenario"]
            ] = ar
            if ar["admission_status"] != "READY":
                continue
            fl_val = _frac(
                ar["failure_level_numerator"], ar["failure_level_denominator"]
            )
            if fl_val is None:
                continue
            eff_sess = ar.get("effective_from_session")
            eff_ord = (
                session_to_ordinal.get(eff_sess, ordinal + 1)
                if eff_sess is not None
                else ordinal + 1
            )
            ev = Event(
                event_id=ar["event_id"],
                entity_id=ar["source_symbol"],
                session_ordinal=ordinal,
                scenario=ar["scenario"],
                failure_line=FrozenLine(
                    value=fl_val,
                    coordinate_id=ar["failure_coordinate"] or "FORWARD_ADJUSTED",
                    source_ref=ar["failure_source"] or "T0_PHC20",
                    source_window=str(ar["failure_window_start"] or target_session),
                    effective_ordinal=eff_ord,
                ),
                linked_anchor_id=ar.get("linked_anchor_id"),
                prior_episode_id=ar.get("prior_episode_id"),
                evidence_ref=f"PG_ADMISSION:{target_session.isoformat()}:{ar['source_symbol']}:{ar['scenario']}",
            )
            events_by_symbol.setdefault(ar["source_symbol"], []).append(ev)

        scen_rows = cursor.execute(
            """
            SELECT source_symbol, scenario, scenario_state,
                   predicate_ids, predicate_states, predicate_qualities,
                   predicate_value_numerators, predicate_value_denominators
            FROM stock.source_d0_scenario_day_facts
            WHERE target_session = %s
              AND scenario IN ('LAUNCH_CONFIRM', 'RECOVERY_TURN')
            """,
            (target_session,),
        ).fetchall()
        scen_cols = [d[0] for d in cursor.description]
        scen_by_symbol_scenario: dict[str, dict[str, dict[str, Any]]] = {}
        for r in scen_rows:
            sr = dict(zip(scen_cols, r))
            scen_by_symbol_scenario.setdefault(sr["source_symbol"], {})[
                sr["scenario"]
            ] = sr

        loo_rows = cursor.execute(
            """
            SELECT f.target_entity_id, f.board_kind, f.support_status, f.reason_code,
                   f.candidate_count, c.board_id, c.confirmed_raw_state,
                   c.reason_code AS cand_reason_code
            FROM stock.r03_sector_loo_support_facts f
            LEFT JOIN stock.r03_sector_loo_candidate_facts c
              ON c.loo_fact_id = f.id
            WHERE f.trade_date = %s
            ORDER BY f.target_entity_id, f.board_kind, c.board_id
            """,
            (target_session,),
        ).fetchall()
        loo_by_symbol: dict[str, list[FieldValue]] = {}
        for (
            sym_id,
            _board_kind,
            sup_status,
            rsn,
            cand_count,
            board_id,
            confirmed_raw,
            cand_rsn,
        ) in loo_rows:
            sym_vals = loo_by_symbol.setdefault(sym_id, [])
            if board_id is not None:
                if confirmed_raw == "TRUE":
                    sym_vals.append(FieldValue(True))
                elif confirmed_raw == "FALSE":
                    sym_vals.append(FieldValue(False))
                else:
                    sym_vals.append(
                        unknown(cand_rsn or rsn or "LOO_CONFIRMED_RAW_UNKNOWN")
                    )
            elif sup_status == "NA" and int(cand_count or 0) == 0:
                continue
            else:
                sym_vals.append(unknown(rsn or "LOO_UNKNOWN"))

        # 读取历史早于 t-1 的 D2 状态标记（用于区分左截尾/缺前态与真正首观测）
        earlier_symbols: set[str] = set()
        any_prior_d2_completed = False
        if prev_session is not None:
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM stock.d2_primary_episode_day_facts
                    WHERE entity_type = 'STOCK' AND calendar_id = %s AND session_date < %s
                )
                """,
                (calendar_id, target_session),
            )
            any_prior_d2_completed = bool(cursor.fetchone()[0])
            cursor.execute(
                """
                SELECT DISTINCT entity_id FROM stock.d2_episode_day_states
                WHERE entity_type = 'STOCK' AND calendar_id = %s AND session_date < %s
                """,
                (calendar_id, prev_session),
            )
            earlier_symbols = {row[0] for row in cursor.fetchall()}

        # 读取最近一次合法改善 ordinal（按 episode_id，直接从 D2 状态迁移历史读取）
        last_improvement_by_ep: dict[str, int] = {}
        if prev_session is not None:
            cursor.execute(
                """
                SELECT episode_id, MAX(ordinal)
                FROM stock.d2_episode_day_states
                WHERE entity_type = 'STOCK'
                  AND calendar_id = %s
                  AND session_date <= %s
                  AND transitions && ARRAY['UPGRADED', 'IMPROVEMENT_RESET', 'ANCHOR_RANK_IMPROVED']::text[]
                GROUP BY episode_id
                """,
                (calendar_id, prev_session),
            )
            for ep_id, max_ord in cursor.fetchall():
                if ep_id and max_ord is not None:
                    last_improvement_by_ep[ep_id] = int(max_ord)

        # 若存在 t-1 合法 FORMAL 父，只为这些证券批量读近 29 个官方会话 Raw 行以生产 PB/CONTINUE
        parent_symbols = sorted(
            sym
            for sym, sts in prior_states_by_symbol.items()
            if any(st.kind == "FORMAL" and st.entity_type == "STOCK" for st in sts)
        )
        pb_raw_by_sym_session: dict[str, dict[Any, Any]] = {}
        if parent_symbols and pb_sessions:
            pb_raw_rows = cursor.execute(
                """
                SELECT source_symbol, session_date, source_capture_id,
                       raw_source_capture_id, qfq_source_capture_id,
                       bar_status, bar_reason, raw_adjustflag,
                       CASE WHEN qfq_adjustflag::text = '2' THEN 'FORWARD_ADJUSTED' ELSE NULL END AS qfq_price_coordinate,
                       qfq_adjustflag,
                       qfq_response_session_date, qfq_response_source_code,
                       qfq_response_fields, qfq_bar_status, qfq_bar_reason,
                       qfq_tradestatus_value, qfq_tradestatus_quality, qfq_tradestatus_reason,
                       qfq_price_reason, COALESCE(qfq_close_reason, qfq_price_reason) AS qfq_close_reason,
                       qfq_open, qfq_high, qfq_low, qfq_close, qfq_price_quality,
                       qfq_open_quality, qfq_high_quality, qfq_low_quality, qfq_close_quality,
                       raw_amount, raw_amount_quality, raw_amount_reason, amount_unit_status,
                       raw_tradestatus_value, raw_tradestatus_quality, raw_tradestatus_reason
                FROM stock.source_raw_daily
                WHERE source_symbol = ANY(%s::text[])
                  AND session_date = ANY(%s::date[])
                ORDER BY source_symbol, session_date
                """,
                (parent_symbols, list(pb_sessions)),
            ).fetchall()
            pb_cols = [d[0] for d in cursor.description]
            for r in pb_raw_rows:
                rd = dict(zip(pb_cols, r))
                sym_rows = pb_raw_by_sym_session.setdefault(
                    rd["source_symbol"], {"_target_session": target_session}
                )
                sym_rows[rd["session_date"]] = rd

    # 3. 逐股生产 PB/CONTINUE 与推进 D2 生命周期
    episodes_to_upsert: list[dict[str, Any]] = []
    day_states_to_persist: list[dict[str, Any]] = []
    primaries_to_persist: list[dict[str, Any]] = []
    peaks_to_persist: list[dict[str, Any]] = []
    d2_results_by_symbol: dict[str, D2DayResult] = {}
    pb_cont_results_by_symbol: dict[str, Any] = {}
    pb_cont_facts_to_persist: list[dict[str, Any]] = []
    pb_cont_details_to_persist: list[dict[str, Any]] = []
    pb_cont_dynamic_ma_events_by_symbol: dict[str, list[dict[str, Any]]] = {}

    all_symbols = sorted(
        set(seed_by_symbol) | set(prior_states_by_symbol) | set(events_by_symbol)
    )
    for sym in all_symbols:
        sd = seed_by_symbol.get(sym, {})
        pr = prof_by_symbol.get(sym, {})
        rq = raw_qfq_by_symbol.get(sym, {})
        p_states = tuple(prior_states_by_symbol.get(sym, ()))
        p_mode, p_gap_reason = resolve_prior_context(
            prior_states=p_states,
            prev_session_exists=any_prior_d2_completed,
            prev_session_completed=prev_session_completed,
            has_earlier_history=(sym in earlier_symbols),
        )
        proj_prior_mode = "MODEL_BOUNDARY" if p_mode == "MISSING_PRIOR" else p_mode

        f_seed = sd.get("formal_seed_state", "UNKNOWN")
        f_pw = sd.get("formal_prewatch_state", "UNKNOWN")
        seed_fv = (
            FieldValue(f_seed == "TRUE")
            if f_seed in ("TRUE", "FALSE")
            else unknown("SEED_UNKNOWN")
        )
        pw_fv = (
            FieldValue(f_pw == "TRUE")
            if f_pw in ("TRUE", "FALSE")
            else unknown("PREWATCH_UNKNOWN")
        )
        actual_fv = (
            FieldValue(bool(sd["target_actual_quote"]))
            if "target_actual_quote" in sd and sd["target_actual_quote"] is not None
            else unknown("ACTUAL_UNKNOWN")
        )
        dmg_fv = (
            FieldValue(bool(sd["core_price_damage"]))
            if sd.get("core_price_damage_quality") == "OBSERVED"
            else unknown(sd.get("core_price_damage_reason") or "DAMAGE_UNKNOWN")
        )
        ext_fv = (
            FieldValue(sd["profile_extension_value"])
            if sd.get("profile_extension_quality") == "OBSERVED"
            else unknown(sd.get("profile_extension_reason") or "EXTENSION_UNKNOWN")
        )
        rps_d3 = _frac(
            sd.get("rps5_delta3_numerator"), sd.get("rps5_delta3_denominator")
        )
        metric_fv = (
            FieldValue(rps_d3)
            if sd.get("rps5_delta3_quality") == "OBSERVED" and rps_d3 is not None
            else unknown("RPS5_DELTA3_UNKNOWN")
        )
        rps20_val = _frac(sd.get("rps20_numerator"), sd.get("rps20_denominator"))
        rps20_fv = (
            FieldValue(rps20_val)
            if sd.get("rps20_quality") == "OBSERVED" and rps20_val is not None
            else unknown("RPS20_UNKNOWN")
        )

        ma20_val = _frac(
            pr.get("ma20_exact_numerator"), pr.get("ma20_exact_denominator")
        )
        ma20_fv = (
            FieldValue(ma20_val)
            if pr.get("ma20_quality") == "OBSERVED" and ma20_val is not None
            else unknown(pr.get("ma20_reason") or "MA20_UNKNOWN")
        )
        slope_val = _frac(
            pr.get("ma_slope20_atr_exact_numerator"),
            pr.get("ma_slope20_atr_exact_denominator"),
        )
        slope_fv = (
            FieldValue(slope_val)
            if pr.get("ma_slope20_atr_quality") == "OBSERVED" and slope_val is not None
            else unknown(pr.get("ma_slope20_atr_reason") or "SLOPE_UNKNOWN")
        )
        ph20_val = _frac(
            pr.get("prior_high20_exact_numerator"),
            pr.get("prior_high20_exact_denominator"),
        )
        ph20_fv = (
            FieldValue(ph20_val)
            if pr.get("prior_high20_quality") == "OBSERVED" and ph20_val is not None
            else unknown(pr.get("prior_high20_reason") or "PRIOR_HIGH20_UNKNOWN")
        )

        if (
            rq.get("qfq_close_quality") == "OBSERVED"
            and rq.get("qfq_close") is not None
        ):
            close_fv = FieldValue(Fraction(Decimal(rq["qfq_close"])))
        elif (
            sd.get("target_close_quality") == "OBSERVED"
            and sd.get("target_close_numerator") is not None
        ):
            c_val = _frac(
                sd.get("target_close_numerator"), sd.get("target_close_denominator")
            )
            close_fv = (
                FieldValue(c_val) if c_val is not None else unknown("CLOSE_UNKNOWN")
            )
        else:
            close_fv = unknown(rq.get("qfq_close_reason") or "CLOSE_UNKNOWN")

        if rq.get("qfq_high_quality") == "OBSERVED" and rq.get("qfq_high") is not None:
            high_fv: FieldValue | None = FieldValue(Fraction(Decimal(rq["qfq_high"])))
        else:
            high_fv = None

        qfq_cap = rq.get("qfq_source_capture_id")
        coord_id = (
            f"BAOSTOCK_QFQ:{qfq_cap}" if qfq_cap is not None else "FORWARD_ADJUSTED"
        )
        coord_comparable = (
            FieldValue(True)
            if qfq_cap is not None and rq.get("qfq_close_quality") == "OBSERVED"
            else unknown("QFQ_BASIS_NOT_COMPARABLE")
        )

        # 关联支撑硬破状态（使用稳定 anchor_uid）
        sym_sups = d1_supports_by_symbol.get(sym, ())
        linked_breaches: dict[str, Fact] = {}
        sups_by_uid: dict[str, Mapping[str, Any]] = {}
        for sup in sym_sups:
            uid = str(
                sup.get("anchor_uid")
                or f"{sup.get('d1_structure_id')}:{sup['created_session'].isoformat()}"
            )
            sups_by_uid[uid] = sup
            st_val = sup.get("after_state")
            q_val = sup.get("after_quality")
            if q_val == "OBSERVED" and st_val in (
                "BROKEN",
                "IDLE",
                "APPROACHING",
                "TESTING",
                "BREACHED_SHALLOW",
                "RECLAIMED",
                "HELD_TENTATIVE",
                "HELD_CONFIRMED",
                "RETESTING",
            ):
                breach_fv = FieldValue(st_val == "BROKEN")
            else:
                breach_fv = unknown(sup.get("reason") or "SUPPORT_STATE_UNKNOWN")
            linked_breaches[uid] = _mk_fact(
                breach_fv,
                entity_id=sym,
                session_date=target_session,
                coordinate_id=sup.get("coordinate_id") or coord_id,
                source_ref=f"D1_SUPPORT:{uid}",
            )

        # C01: 从 t-1 合法 FORMAL 父、冻结峰、旧支撑与当日 LOO 生成 PB/CONTINUE 与今日新峰
        pb_res = None
        ev_list = list(events_by_symbol.get(sym, ()))
        if any(st.kind == "FORMAL" and st.entity_type == "STOCK" for st in p_states):
            adm_map = adm_by_symbol_scenario.get(sym, {})
            sample_adm = next(iter(adm_map.values()), {})
            common_gate_fvs = tuple(
                _tri_field(
                    sample_adm.get(col),
                    reason_if_unknown=f"{col.upper()}_UNKNOWN",
                )
                for col in (
                    "research_universe_state",
                    "actual_quote_state",
                    "coordinate_window_state",
                    "d0_liquidity_state",
                    "non_core_damage_state",
                    "non_extreme_extension_state",
                    "non_first_day_damage_state",
                    "non_severe_drop_state",
                )
            )
            prior_sups_for_sym = prior_d1_supports_by_symbol.get(sym, ())
            prior_sups_by_uid = {
                str(r["anchor_uid"]): r
                for r in prior_sups_for_sym
                if r.get("anchor_uid")
            }
            applicable_prior_anchor_ids = frozenset(
                str(r["anchor_uid"])
                for r in prior_sups_for_sym
                if r.get("anchor_uid")
                and (
                    r.get("available_from_session") is None
                    or (
                        prev_session is not None
                        and r["available_from_session"] <= prev_session
                    )
                )
                and r.get("after_quality") == "OBSERVED"
                and r.get("after_state") not in ("BROKEN", "EXPIRED")
                and r.get("is_active") is True
            )
            prior_bindings = build_support_bindings_for_parents(
                prior_states=p_states,
                d1_supports_by_anchor_id=prior_sups_by_uid,
                applicable_prior_anchor_ids=applicable_prior_anchor_ids,
                target_coordinate_id=coord_id,
                supports_proven_complete=(
                    prev_session is not None and sym in prior_d1_completed_symbols
                ),
            )
            pb_res = produce_pb_continue_and_peaks_for_stock(
                entity_id=sym,
                target_session=target_session,
                target_ordinal=ordinal,
                sessions=pb_sessions,
                session_ordinals=session_to_ordinal,
                rows_by_session=pb_raw_by_sym_session.get(sym, {}),
                source_symbol=sym,
                source_capture_id=source_capture_id,
                coordinate_id=coord_id,
                prior_states=p_states,
                prior_peaks_by_parent=prior_peaks_by_ep,
                prior_supports_by_parent=prior_bindings,
                prior_lineage_unknown=(p_gap_reason is not None),
                gates=common_gate_fvs,
                core_ma_slope20_atr=slope_fv,
                core_prior_high20=ph20_fv,
                rps20=rps20_fv,
                loo_confirmed_raws=loo_by_symbol.get(sym),
                actual=actual_fv,
                high_in_today_coordinate=high_fv
                if high_fv is not None
                else unknown("HIGH_UNKNOWN"),
                close_in_today_coordinate=close_fv,
                evidence_ref_prefix=f"PG_PB_CONT:{target_session.isoformat()}:{sym}",
                peak_comparable=coord_comparable,
                capability_state="READY",
            )
            pb_cont_results_by_symbol[sym] = pb_res
            sym_pb_facts, sym_pb_details = _serialize_pb_cont_for_pg(
                sym=sym,
                pb_res=pb_res,
                seed_row=sd,
                adm_map=adm_map,
                target_session=target_session,
                next_session=next_session,
                sessions=sessions,
                source_capture_id=source_capture_id,
                total_source_count=len(seed_by_symbol),
            )
            pb_cont_facts_to_persist.extend(sym_pb_facts)
            pb_cont_details_to_persist.extend(sym_pb_details)
            existing_ev_ids = {ev.event_id for ev in ev_list}
            for pb_ev in pb_res.pb_continue_result.admitted_events:
                if pb_ev.event_id not in existing_ev_ids:
                    ev_list.append(pb_ev)
                    existing_ev_ids.add(pb_ev.event_id)
                if pb_ev.failure_line.source_ref in (
                    "T0_MA5",
                    "T0_MA20",
                    "T0_MA20_FALLBACK",
                ):
                    pb_cont_dynamic_ma_events_by_symbol.setdefault(sym, []).append(
                        {
                            "source_symbol": sym,
                            "target_session": target_session,
                            "scenario": pb_ev.scenario,
                            "event_id": pb_ev.event_id,
                            "source_capture_id": qfq_cap or source_capture_id,
                            "failure_line_value": pb_ev.failure_line.value,
                            "failure_coordinate": "FORWARD_ADJUSTED",
                            "failure_source": pb_ev.failure_line.source_ref,
                            "failure_window_start": target_session,
                            "failure_source_event_ref": pb_ev.evidence_ref,
                            "admission_status": "READY",
                            "linked_anchor_id": pb_ev.linked_anchor_id,
                            "prior_episode_id": pb_ev.prior_episode_id,
                        }
                    )
        elif sd:
            pb_cont_facts_to_persist.extend(
                _serialize_no_parent_pb_cont_for_pg(
                    sym=sym,
                    seed_row=sd,
                    adm_map=adm_by_symbol_scenario.get(sym, {}),
                    prior_gap_reason=p_gap_reason,
                    source_capture_id=source_capture_id,
                    total_source_count=len(seed_by_symbol),
                )
            )
        if coord_id != "FORWARD_ADJUSTED":
            ev_list = [
                Event(
                    event_id=ev.event_id,
                    entity_id=ev.entity_id,
                    session_ordinal=ev.session_ordinal,
                    scenario=ev.scenario,
                    failure_line=FrozenLine(
                        value=ev.failure_line.value,
                        coordinate_id=coord_id,
                        source_ref=ev.failure_line.source_ref,
                        source_window=ev.failure_line.source_window,
                        effective_ordinal=ev.failure_line.effective_ordinal,
                    ),
                    linked_anchor_id=ev.linked_anchor_id,
                    prior_episode_id=ev.prior_episode_id,
                    evidence_ref=ev.evidence_ref,
                )
                if ev.failure_line.coordinate_id == "FORWARD_ADJUSTED"
                else ev
                for ev in ev_list
            ]

        warm_fv = compute_stock_warm_qualification(
            prewatch=pw_fv,
            rps5_delta3_pp=metric_fv,
            close=close_fv,
            ma20=ma20_fv,
            ma_slope20_atr=slope_fv,
        )
        has_confirmed = bool(ev_list)
        if has_confirmed:
            confirmed_fv = FieldValue(True)
        else:
            sym_adms = adm_by_symbol_scenario.get(sym, {})
            base_computed = {"LAUNCH_CONFIRM", "RECOVERY_TURN"}.issubset(
                sym_adms.keys()
            )
            adm_unknown = any(
                row.get("admission_status") in ("NOT_READY", "UNKNOWN", "DIAGNOSTIC")
                for row in sym_adms.values()
            )
            pb_unknown = False
            if pb_res is not None:
                pb_cont = pb_res.pb_continue_result
                pb_unknown = (
                    pb_cont.pullback.raw_summary.quality != "OBSERVED"
                    or pb_cont.trend_continue.raw_summary.quality != "OBSERVED"
                    or any(
                        d.admission.status == "NOT_READY"
                        and not (
                            d.admission.reason
                            == "PRIOR_SUPPORT_REQUIRED_OR_ABSENCE_UNPROVEN"
                            and (pb_bindings := pb_res.eligible_parents.parents)
                            is not None
                            and any(
                                p.episode_id == d.parent_episode_id
                                and p.support_valid.quality == "NOT_APPLICABLE"
                                for p in pb_bindings
                            )
                        )
                        for d in (
                            pb_cont.pullback.details + pb_cont.trend_continue.details
                        )
                    )
                )
            if adm_unknown or pb_unknown:
                confirmed_fv = unknown("D0_CONFIRMED_UNKNOWN")
            elif not base_computed:
                confirmed_fv = unknown("D0_CONFIRMED_NOT_COMPUTED")
            else:
                confirmed_fv = FieldValue(False)
        quals = (
            ("SEED", seed_fv),
            ("PREWATCH", pw_fv),
            ("WARM", warm_fv),
            ("CONFIRMED", confirmed_fv),
        )
        d2_inp = D2DayInputs(
            entity_id=sym,
            entity_type="STOCK",
            session=target_session,
            ordinal=ordinal,
            calendar_id=calendar_id,
            coordinate_id=coord_id,
            input_ref=f"PG_R04_DAY:{target_session.isoformat()}:{sym}",
            prior_states=p_states,
            prior_mode=p_mode,
            prior_gap_reason=p_gap_reason,
            qualifications=quals,
            actual=actual_fv,
            damage=dmg_fv,
            extension=ext_fv,
            metric=metric_fv,
            followup_complete=unknown("FOLLOWUP_NOT_COMPUTED"),
            close_in_today_coordinate=close_fv,
            high_in_today_coordinate=high_fv,
            coordinate_comparable=coord_comparable,
            linked_hard_breaches=linked_breaches,
            formal_events=tuple(ev_list),
            prior_primary_episode_id=prior_primary_by_symbol.get(sym),
            rules=rules,
            capability_state="READY",
        )
        res = advance_d2_day(d2_inp)
        d2_results_by_symbol[sym] = res

        for st in res.states:
            if st.episode_id is None:
                continue
            birth_sess = (
                target_session
                if st.birth_ordinal == ordinal
                else prior_episode_birth_session.get(
                    st.episode_id,
                    sessions[st.birth_ordinal]
                    if 0 <= st.birth_ordinal < len(sessions)
                    else target_session,
                )
            )
            eff_sess = prior_episode_effective_from.get(st.episode_id)
            if eff_sess is None and st.event is not None:
                eff_ord = st.event.failure_line.effective_ordinal
                if 0 <= eff_ord < len(sessions):
                    eff_sess = sessions[eff_ord]
                elif next_session is not None:
                    eff_sess = next_session
            episodes_to_upsert.append(
                {
                    "episode_id": st.episode_id,
                    "entity_type": st.entity_type,
                    "entity_id": st.entity_id,
                    "calendar_id": st.calendar_id,
                    "rule_identity": st.rule_identity,
                    "scenario": st.scenario,
                    "kind": st.kind,
                    "birth_session": birth_sess,
                    "birth_ordinal": st.birth_ordinal,
                    "parent_episode_id": st.parent_episode_id,
                    "event_id": st.event_id,
                    "failure_level": st.event.failure_line.value if st.event else None,
                    "failure_coordinate": st.event.failure_line.coordinate_id
                    if st.event
                    else None,
                    "failure_source": st.event.failure_line.source_ref
                    if st.event
                    else None,
                    "effective_from_session": eff_sess,
                    "linked_anchor_id": st.event.linked_anchor_id if st.event else None,
                    "failure_window": prior_episode_failure_window.get(st.episode_id)
                    or (st.event.failure_line.source_window if st.event else None),
                    "evidence_ref": prior_episode_evidence_ref.get(st.episode_id)
                    or (st.event.evidence_ref if st.event else None),
                }
            )
            day_states_to_persist.append(
                _state_to_pg_dict(st, session_date=target_session)
            )

        primaries_to_persist.append(
            {
                "entity_type": "STOCK",
                "entity_id": sym,
                "calendar_id": calendar_id,
                "ordinal": ordinal,
                "rule_identity": rule_identity,
                "primary_status": "SELECTED"
                if res.primary_selection.episode_id is not None
                else (
                    "NOT_APPLICABLE"
                    if res.primary_selection.quality == "NOT_APPLICABLE"
                    else "UNKNOWN"
                ),
                "primary_reason": res.primary_selection.reason,
                "primary_episode_id": res.primary_selection.episode_id,
                "candidate_count": len([s for s in res.states if s.episode_id]),
            }
        )
        seen_peak_keys: set[tuple[str, int]] = set()
        for pk in tuple(res.new_peak_events) + (
            tuple(pb_res.peak_result.new_peak_events) if pb_res is not None else ()
        ):
            pk_key = (pk.parent_episode_id, int(pk.peak_ordinal))
            if pk_key in seen_peak_keys:
                continue
            seen_peak_keys.add(pk_key)
            peaks_to_persist.append(
                {
                    "parent_episode_id": pk.parent_episode_id,
                    "entity_id": pk.entity_id,
                    "peak_ordinal": pk.peak_ordinal,
                    "coordinate_id": pk.coordinate_id,
                    "peak_high": pk.peak_high,
                    "peak_close": pk.peak_close,
                }
            )

    # 若 PB/CONTINUE 产生新的动态 MA 事件或存在需随修订清理的旧 PB/CONT 动态 MA 锚，局部刷新当日出生锚并刷新当日支撑观察
    old_pb_dyn_symbols = {
        sym
        for sym, rows in d1_supports_by_symbol.items()
        if any(
            r.get("created_session") == target_session
            and r.get("structure_type") in ("DYNAMIC_MA5", "DYNAMIC_MA20")
            and (
                "STRONG_PULLBACK" in str(r.get("source_event_id") or "")
                or "TREND_CONTINUE" in str(r.get("source_event_id") or "")
            )
            for r in rows
        )
    }
    dyn_ma_scoped_symbols = sorted(
        set(pb_cont_dynamic_ma_events_by_symbol.keys()) | old_pb_dyn_symbols
    )
    if dyn_ma_scoped_symbols:
        if research_cutoff is None:
            with source_raw._transaction(config, write=False) as (
                _cap_conn,
                cap_cur,
                _cap_scope,
            ):
                cap_cur.execute(
                    "SELECT range_end_session FROM stock.source_captures WHERE capture_id = %s",
                    (source_capture_id,),
                )
                cap_row = cap_cur.fetchone()
                if cap_row and isinstance(cap_row[0], date):
                    effective_cutoff = max(effective_cutoff, cap_row[0])
        merged_dyn_events: dict[str, list[dict[str, Any]]] = {
            s: list(evs) for s, evs in pb_cont_dynamic_ma_events_by_symbol.items()
        }
        for s in dyn_ma_scoped_symbols:
            for sc_name, adm_r in adm_by_symbol_scenario.get(s, {}).items():
                if adm_r.get("admission_status") == "READY" and adm_r.get(
                    "failure_source"
                ) in ("T0_MA5", "T0_MA20", "T0_MA20_FALLBACK", "R5_MA5", "R20_MA20"):
                    fl_v = _frac(
                        adm_r.get("failure_level_numerator"),
                        adm_r.get("failure_level_denominator"),
                    )
                    if fl_v is not None and adm_r.get("event_id"):
                        merged_dyn_events.setdefault(s, []).append(
                            {
                                "source_symbol": s,
                                "target_session": target_session,
                                "scenario": sc_name,
                                "event_id": adm_r["event_id"],
                                "source_capture_id": source_capture_id,
                                "failure_line_value": fl_v,
                                "failure_coordinate": "FORWARD_ADJUSTED",
                                "failure_source": adm_r["failure_source"],
                                "failure_window_start": adm_r.get(
                                    "failure_window_start"
                                )
                                or target_session,
                                "failure_source_event_ref": f"PG_ADMISSION:{target_session.isoformat()}:{s}:{sc_name}",
                                "admission_status": "READY",
                                "linked_anchor_id": adm_r.get("linked_anchor_id"),
                                "prior_episode_id": adm_r.get("prior_episode_id"),
                            }
                        )
        ma_backfill = (
            prepare_dynamic_ma_backfill_facts(
                config,
                target_session=target_session,
                research_cutoff=effective_cutoff,
                source_capture_id=source_capture_id,
                d1_history_session_count=max(25, len(sessions)),
                events_by_symbol=merged_dyn_events,
            )
            if merged_dyn_events
            else {"active_anchors": (), "active_observations": ()}
        )
        persist_d1_active_anchors_and_observations(
            config,
            target_session=target_session,
            active_anchors=ma_backfill.get("active_anchors", ()),
            active_observations=ma_backfill.get("active_observations", ()),
            replace_scope="SCOPED",
            scoped_symbols=dyn_ma_scoped_symbols,
            scoped_structure_types=("DYNAMIC_MA5", "DYNAMIC_MA20"),
            scoped_created_session=target_session,
        )
        d1_supports_by_symbol = read_d1_active_support_observations(
            config, target_session=target_session, include_inactive=True
        )

    # 4. 组装下游 Risk / Priority / Radar / Near-Miss
    proj_inputs: list[StockProjectionInput] = []
    for sym in all_symbols:
        sd = seed_by_symbol.get(sym, {})
        pr = prof_by_symbol.get(sym, {})
        rq = raw_qfq_by_symbol.get(sym, {})
        qfq_cap = rq.get("qfq_source_capture_id")
        coord_id = (
            f"BAOSTOCK_QFQ:{qfq_cap}" if qfq_cap is not None else "FORWARD_ADJUSTED"
        )
        d2_res = d2_results_by_symbol[sym]
        p_states = tuple(prior_states_by_symbol.get(sym, ()))
        p_mode, p_gap_reason = resolve_prior_context(
            prior_states=p_states,
            prev_session_exists=any_prior_d2_completed,
            prev_session_completed=prev_session_completed,
            has_earlier_history=(sym in earlier_symbols),
        )
        proj_prior_mode = "MODEL_BOUNDARY" if p_mode == "MISSING_PRIOR" else p_mode

        dmg_fv = (
            FieldValue(bool(sd["core_price_damage"]))
            if sd.get("core_price_damage_quality") == "OBSERVED"
            else unknown(sd.get("core_price_damage_reason") or "DAMAGE_UNKNOWN")
        )
        ext_fv = (
            FieldValue(sd["profile_extension_value"])
            if sd.get("profile_extension_quality") == "OBSERVED"
            else unknown(sd.get("profile_extension_reason") or "EXTENSION_UNKNOWN")
        )

        sel_id = d2_res.primary_selection.episode_id
        sel_state = (
            next((s for s in d2_res.states if s.episode_id == sel_id), None)
            if sel_id is not None
            else None
        )
        sym_sups = d1_supports_by_symbol.get(sym, ())
        cand_obs = _support_obs_from_pg_rows(sym_sups, ordinal=ordinal)
        prior_cand_obs = _support_obs_from_pg_rows(
            prior_d1_supports_by_symbol.get(sym, ()),
            ordinal=max(0, ordinal - 1),
        )
        if sel_state is not None:
            collected_ids: set[str] = set()
            if sel_state.event is not None and sel_state.event.linked_anchor_id:
                collected_ids.add(str(sel_state.event.linked_anchor_id))
            if sel_state.event_id:
                for sup_row in sym_sups:
                    if sup_row.get(
                        "source_event_id"
                    ) == sel_state.event_id and sup_row.get("anchor_uid"):
                        collected_ids.add(str(sup_row["anchor_uid"]))
            sym_d1_done = sym in d1_completed_symbols or bool(sym_sups)
            if collected_ids:
                linked_ids: frozenset[str] | None = frozenset(collected_ids)
                sup_absent = False
                support_scope_known = sym_d1_done
            elif sel_state.event is not None and sym_d1_done:
                linked_ids = frozenset()
                sup_absent = True
                support_scope_known = True
            else:
                linked_ids = None
                sup_absent = False
                support_scope_known = False
        else:
            linked_ids = None
            if d2_res.primary_selection.quality == "NOT_APPLICABLE":
                sup_absent = True
                support_scope_known = True
            else:
                sup_absent = False
                support_scope_known = False

        assembled_risk = assemble_stock_risk_inputs(
            entity_id=sym,
            session=target_session,
            coordinate_id=coord_id,
            damage_field=dmg_fv,
            extension_field=ext_fv,
            primary_selection=d2_res.primary_selection.field,
            selected_state=sel_state,
            candidate_supports=cand_obs,
            linked_anchor_ids=linked_ids,
            support_scope_known=support_scope_known,
            support_applicable_proven_absent=sup_absent,
        )

        rps_d3 = _frac(
            sd.get("rps5_delta3_numerator"), sd.get("rps5_delta3_denominator")
        )
        rps_d3_fv = (
            FieldValue(rps_d3)
            if sd.get("rps5_delta3_quality") == "OBSERVED" and rps_d3 is not None
            else unknown("RPS5_DELTA3_UNKNOWN")
        )
        rps20_val = _frac(sd.get("rps20_numerator"), sd.get("rps20_denominator"))
        rps20_fv = (
            FieldValue(rps20_val)
            if sd.get("rps20_quality") == "OBSERVED" and rps20_val is not None
            else unknown("RPS20_UNKNOWN")
        )
        amt_val = _frac(
            sd.get("core_liquidity_mean20_numerator"),
            sd.get("core_liquidity_mean20_denominator"),
        )
        amt_fv = (
            FieldValue(amt_val)
            if sd.get("core_liquidity_mean20_quality") == "OBSERVED"
            and amt_val is not None
            else unknown("PRIOR20_AMOUNT_MEAN_UNKNOWN")
        )

        # D02: 真实改善年龄
        anchor_rank_improved = False
        if linked_ids:
            cur_linked = tuple(o for o in cand_obs if o.anchor_id in linked_ids)
            pri_linked = tuple(o for o in prior_cand_obs if o.anchor_id in linked_ids)
            anchor_rank_improved = detect_existing_anchor_rank_improvement(
                pri_linked, cur_linked
            )
        if anchor_rank_improved and sel_id:
            for ds_row in day_states_to_persist:
                if ds_row.get("episode_id") == sel_id:
                    existing_trans = tuple(ds_row.get("transitions") or ())
                    if "ANCHOR_RANK_IMPROVED" not in existing_trans:
                        ds_row["transitions"] = existing_trans + (
                            "ANCHOR_RANK_IMPROVED",
                        )
        imp_age = resolve_episode_improvement_age(
            current_ordinal=ordinal,
            episode_id=sel_id,
            primary_selection_quality=d2_res.primary_selection.quality,
            current_state=sel_state,
            prior_last_improvement_ordinal=last_improvement_by_ep.get(sel_id)
            if sel_id
            else None,
            prior_history_complete=(p_gap_reason is None),
            prior_mode=p_mode,
            existing_anchor_rank_improved=anchor_rank_improved,
        )

        # D03: Near-Miss 真实装配
        atr20_val = _frac(
            pr.get("atr20_exact_numerator"), pr.get("atr20_exact_denominator")
        )
        atr20_fv = (
            FieldValue(atr20_val)
            if pr.get("atr20_quality") == "OBSERVED" and atr20_val is not None
            else unknown(pr.get("atr20_reason") or "ATR20_UNKNOWN")
        )
        pb_res_for_sym = pb_cont_results_by_symbol.get(sym)
        common_gates, scen_inputs = _build_d0_near_miss_inputs(
            adm_by_symbol_scenario.get(sym, {}),
            scen_by_symbol_scenario.get(sym, {}),
            pb_res=pb_res_for_sym,
            atr_today=atr20_fv,
        )
        emerg_rank_map = {"HIGH": 3, "MEDIUM": 2, "LOW": 1, "NONE": 0}
        emerg_lv, _ = classify_emergence(rps_d3_fv)
        nm_paths = (
            assemble_near_miss_paths_from_d0(
                entity_id=sym,
                stable_id=sym,
                session_ordinal=ordinal,
                emergence_rank=emerg_rank_map.get(emerg_lv) if emerg_lv else None,
                rps5_delta3_pp=rps_d3_fv,
                common_safety_gates=common_gates,
                scenario_inputs=scen_inputs,
            )
            if scen_inputs
            else ()
        )

        sym_pred_evidence: dict[str, tuple[str, ...]] = {}
        for sc_name, sc_row in scen_by_symbol_scenario.get(sym, {}).items():
            p_ids = sc_row.get("predicate_ids") or ()
            pred_states = sc_row.get("predicate_states") or ()
            failed_preds = tuple(
                f"{pid}:{pred_states[idx] if idx < len(pred_states) else 'UNKNOWN'}"
                for idx, pid in enumerate(p_ids)
                if (pred_states[idx] if idx < len(pred_states) else "UNKNOWN") != "TRUE"
            )
            if failed_preds:
                sym_pred_evidence[sc_name] = failed_preds
        if pb_res_for_sym is not None:
            for sc_name, batch in (
                ("STRONG_PULLBACK", pb_res_for_sym.pb_continue_result.pullback),
                ("TREND_CONTINUE", pb_res_for_sym.pb_continue_result.trend_continue),
            ):
                pb_failed = tuple(
                    f"{d.parent_episode_id}:{pid}:{p_info.get('state', 'UNKNOWN')}"
                    for d in batch.details
                    for pid, p_info in d.predicates.items()
                    if p_info.get("state") != "TRUE"
                )
                if pb_failed:
                    sym_pred_evidence[sc_name] = pb_failed

        proj_inputs.append(
            StockProjectionInput(
                entity_id=sym,
                stable_id=sym,
                session=target_session,
                session_ordinal=ordinal,
                coordinate_id=coord_id,
                prior_mode=proj_prior_mode,
                current_states=tuple(s for s in d2_res.states if s.episode_id),
                prior_states=p_states,
                damage=assembled_risk.damage,
                extension=assembled_risk.extension,
                invalidated=assembled_risk.invalidated,
                shallow_breach=assembled_risk.shallow_breach,
                structure_applicability=assembled_risk.structure_applicability,
                support_applicability=assembled_risk.support_applicability,
                linked_support_observations=assembled_risk.linked_support_observations,
                support_scope_complete=assembled_risk.support_scope_complete,
                rps5_delta3_pp=rps_d3_fv,
                rps20=rps20_fv,
                prior20_amount_mean=amt_fv,
                improvement_age=imp_age,
                current_primary=d2_res.primary_selection.field,
                prior_risk=prior_risk_by_symbol.get(sym),
                near_miss_paths=nm_paths,
                predicate_evidence=sym_pred_evidence or None,
            )
        )

    proj_result = build_d2_downstream_projections(
        proj_inputs,
        session=target_session,
        session_ordinal=ordinal,
        capability_state="READY",
    )

    risk_pg_rows = [
        {
            "source_symbol": r.entity_id,
            "coordinate_id": r.coordinate_id,
            "selected_episode_id": r.selected_episode_id,
            "structure_applicability": "ABSENT"
            if r.risk.structure_na
            else (
                "UNKNOWN"
                if r.invalidated_detail.quality == "UNKNOWN"
                and r.selected_episode_id is None
                else "LINKED"
            ),
            "support_applicability": "ABSENT"
            if r.risk.support_na
            else (
                "UNKNOWN" if r.shallow_breach_detail.quality == "UNKNOWN" else "LINKED"
            ),
            "risk_level": r.risk.level.value
            if r.risk.level.quality == "OBSERVED"
            else None,
            "risk_quality": "OBSERVED"
            if r.risk.level.quality == "OBSERVED"
            else "UNKNOWN",
            "risk_reason": r.risk.level.reason,
            "lower_bound": r.risk.lower_bound,
            "upper_bound": r.risk.upper_bound,
            "primary_reason": r.risk.primary_reason,
            "confirmed_reasons": r.risk.confirmed_reasons,
            "possible_reasons": r.risk.possible_reasons,
            "damage_value": r.damage_detail.value,
            "damage_quality": r.damage_detail.quality,
            "extension_value": r.extension_detail.value,
            "extension_quality": r.extension_detail.quality,
            "invalidated_value": r.invalidated_detail.value,
            "invalidated_quality": r.invalidated_detail.quality,
            "shallow_breach_value": r.shallow_breach_detail.value,
            "shallow_breach_quality": r.shallow_breach_detail.quality,
        }
        for r in proj_result.stock_risk_rows
    ]
    priority_pg_rows = [
        {
            "source_symbol": p.entity_id,
            "priority_status": p.result.status,
            "exclusion_reasons": p.result.reasons,
            "bucket": p.result.bucket,
            "emergence_rank": p.result.emergence_rank,
            "structure_rank": p.result.structure_rank.value
            if p.result.structure_rank.quality == "OBSERVED"
            else None,
            "rps5_delta3_pp": p.result.rps5_delta3_pp.value
            if p.result.rps5_delta3_pp.quality == "OBSERVED"
            else None,
            "risk_rank": p.result.risk_rank,
            "improvement_age": p.result.improvement_age.age_sessions,
            "improvement_age_category": p.result.improvement_age.status,
            "prior20_amount_mean": p.result.prior20_amount_mean.value
            if p.result.prior20_amount_mean.quality == "OBSERVED"
            else None,
            "stable_id": p.stable_id,
            "priority_rank": p.rank,
        }
        for p in proj_result.stock_priority_rows
    ]
    radar_pg_rows = [
        {
            "source_symbol": ev.entity_id,
            "event_type": ev.event_type,
            "episode_id": ev.episode_id or "NONE",
            "display_order": idx,
            "is_primary_event": ev.is_primary_event,
            "why_now": ev.why_now,
            "waiting_for": ev.waiting_for,
            "invalid_if": ev.invalid_if,
            "annotation_flags": ev.annotations,
            "evidence_summary": ev.why_now,
        }
        for idx, ev in enumerate(proj_result.stock_radar_events, start=1)
    ]
    near_miss_pg_rows = [
        {
            "source_symbol": nm.entity_id,
            "scenario": nm.scenario,
            "path_id": nm.path_id,
            "failed_predicate": nm.failed_predicate_id,
            "gap": nm.distance,
            "tolerance": nm.tolerance,
            "gap_ratio": nm.normalized_distance,
            "emergence_rank": nm.emergence_rank,
            "delta3": nm.delta3_pp,
            "stable_id": nm.stable_id,
            "display_order": idx,
            "hard_gates_summary": f"SOFT:{nm.soft_kind}",
        }
        for idx, nm in enumerate(proj_result.stock_near_miss_rows, start=1)
        if nm.delta3_pp is not None
    ]

    d2_persist_report, downstream_report = persist_d2_and_downstream_day_bundle(
        config,
        session_date=target_session,
        calendar_id=calendar_id,
        rule_identity=rule_identity,
        episodes=episodes_to_upsert,
        day_states=day_states_to_persist,
        primary_projections=primaries_to_persist,
        peak_events=peaks_to_persist,
        risk_rows=risk_pg_rows,
        priority_rows=priority_pg_rows,
        radar_events=radar_pg_rows,
        near_miss_rows=near_miss_pg_rows,
        pb_cont_facts=pb_cont_facts_to_persist,
        pb_cont_details=pb_cont_details_to_persist,
    )
    return {
        "status": "PERSISTED",
        "session_date": target_session.isoformat(),
        "calendar_id": calendar_id,
        "d2": d2_persist_report,
        "downstream": downstream_report,
        "expected_counts_by_table": {
            "stock.d2_episode_day_states": len(day_states_to_persist),
            "stock.d2_primary_episode_day_facts": len(primaries_to_persist),
            "stock.stock_risk_day_facts": len(risk_pg_rows),
            "stock.stock_priority_day_facts": len(priority_pg_rows),
            "stock.stock_radar_day_events": len(radar_pg_rows),
            "stock.stock_near_miss_day_facts": len(near_miss_pg_rows),
            "stock.d0_parent_scenario_detail_facts": len(pb_cont_details_to_persist),
            "pb_cont_facts": len(pb_cont_facts_to_persist),
        },
    }
