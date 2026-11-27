"""R04 正式 D2 生命周期与下游风险/Priority/Radar/Near-Miss 协调器入口。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
from decimal import Decimal
from fractions import Fraction
from typing import Any

from stock_a.application.d2_downstream_projections import (
    StockProjectionInput,
    build_d2_downstream_projections,
)
from stock_a.application.d2_formal_lifecycle import (
    D2DayInputs,
    D2DayResult,
    SupportBinding,
    advance_d2_day,
    collect_eligible_formal_parents,
    produce_pb_continue_day,
)
from stock_a.domain import FieldValue, unknown
from stock_a.research.admission import Event, FrozenLine
from stock_a.research.d0_pb_continue import PeakEvent
from stock_a.research.entry_context.sessions import _load_calendar
from stock_a.research.facts import Fact
from stock_a.research.priority_radar import ImprovementAge, NearMissPathInput
from stock_a.research.risk import RiskResult
from stock_a.research.state import CONTRACT as D2_CONTRACT
from stock_a.research.state import Rules as D2Rules
from stock_a.research.state import State
from stock_a.research.structure import SupportObservation
from stock_a.storage.postgres import source_raw
from stock_a.storage.postgres.r04_formal_facts import (
    persist_d2_day_bundle,
    persist_downstream_projections_day,
    read_d2_day_states,
    read_d2_parent_peaks_before,
    read_d2_primary_projections,
    read_stock_risk_summary_and_rows,
)
from stock_a.storage.postgres.research_day_facts import (
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


def _state_from_pg_row(row: Mapping[str, Any]) -> State:
    """将PG读回的d2_episode_day_states字典还原为research.state.State对象。"""
    fl = row.get("failure_level")
    event_obj: Event | None = None
    if row.get("event_id") and fl is not None and row.get("effective_from_session"):
        event_obj = Event(
            event_id=row["event_id"],
            entity_id=row["entity_id"],
            session_ordinal=int(row["birth_ordinal"]),
            scenario=row["scenario"],
            failure_line=FrozenLine(
                value=fl,
                coordinate_id=row.get("failure_coordinate") or row["coordinate_id"],
                source_ref=row.get("failure_source") or "T0_PHC20",
                source_window=str(row.get("birth_session") or ""),
                effective_ordinal=int(row["birth_ordinal"]) + 1,
            ),
            linked_anchor_id=row.get("linked_anchor_id"),
            prior_episode_id=row.get("parent_episode_id"),
            evidence_ref=row["input_ref"],
        )
    elig_q = row["effective_eligibility_quality"]
    elig_val = row["effective_eligibility_value"]
    eff_elig = (
        FieldValue(elig_val, "OBSERVED", row.get("effective_eligibility_reason"))
        if elig_q == "OBSERVED"
        else FieldValue(
            None,
            elig_q,
            row.get("effective_eligibility_reason") or "ELIGIBILITY_UNKNOWN",
        )
    )
    val_q = row["validity_quality"]
    validity = (
        FieldValue(row["validity_value"], "OBSERVED", row.get("validity_reason"))
        if val_q == "OBSERVED"
        else FieldValue(None, val_q, row.get("validity_reason") or "VALIDITY_UNKNOWN")
    )
    hl_q = row["health_quality"]
    health = (
        FieldValue(row["health_value"], "OBSERVED", row.get("health_reason"))
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


def run_r04_d2_and_downstream_day(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_capture_id: int,
) -> dict[str, Any]:
    """在唯一PG中按当日合法输入顺序执行D2逐日状态推进与下游风险/Priority/Radar/Near-Miss持久化。"""
    payload, _meta, _chain, sessions = _load_calendar(target_session)
    calendar_id = str(payload.get("calendar_id") or "CN_A_SHARE_OFFICIAL")
    ordinal = sessions.index(target_session)
    prev_session = sessions[ordinal - 1] if ordinal > 0 else None
    rules = D2Rules(D2_CONTRACT, 2, 10, 3, 3)
    rule_identity = rules.identity

    # 1. 读取 t-1 D2 状态与 t-1 父峰事件
    prior_rows = (
        read_d2_day_states(
            config,
            session_date=prev_session,
            calendar_id=calendar_id,
            rule_identity=rule_identity,
        )
        if prev_session is not None
        else []
    )
    prior_states_by_symbol: dict[str, list[State]] = {}
    for r in prior_rows:
        prior_states_by_symbol.setdefault(r["entity_id"], []).append(
            _state_from_pg_row(r)
        )

    prior_primary_by_symbol: dict[str, str | None] = {}
    if prev_session is not None:
        for pr in read_d2_primary_projections(
            config,
            session_date=prev_session,
            calendar_id=calendar_id,
            rule_identity=rule_identity,
        ):
            prior_primary_by_symbol[pr["entity_id"]] = pr.get("primary_episode_id")

    prior_peaks_rows = read_d2_parent_peaks_before(config, before_ordinal=ordinal)
    prior_peaks_by_ep: dict[str, PeakEvent] = {}
    for pk in prior_peaks_rows:
        pe = PeakEvent(
            peak_event_id=f"{pk['parent_episode_id']}:{pk['peak_session'].isoformat()}",
            entity_id=pk["entity_id"],
            parent_episode_id=pk["parent_episode_id"],
            peak_session=pk["peak_session"],
            peak_ordinal=int(pk["peak_ordinal"]),
            peak_seq=1,
            peak_high=pk["peak_high"],
            peak_close=pk["peak_close"],
            coordinate_id=pk["coordinate_id"],
            source_ref="D1_ACTUAL_HIGH",
            created_from="NEW_HIGH",
        )
        prev_pe = prior_peaks_by_ep.get(pk["parent_episode_id"])
        if prev_pe is None or pe.peak_high > prev_pe.peak_high:
            prior_peaks_by_ep[pk["parent_episode_id"]] = pe

    # 2. 只读读取当日 Seed、Profile、D0正式准入事件、D1活跃支撑与R03 LOO
    d1_supports_by_symbol = read_d1_active_support_observations(
        config, target_session=target_session
    )
    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _conn,
        cursor,
        _scope,
    ):
        seed_rows = cursor.execute(
            """
            SELECT source_symbol, formal_seed_state, formal_prewatch_state,
                   core_price_damage, core_price_damage_quality, core_price_damage_reason,
                   profile_extension_value, profile_extension_quality, profile_extension_reason,
                   core_liquidity_mean20_numerator, core_liquidity_mean20_denominator,
                   core_liquidity_mean20_quality,
                   rps5_delta3_numerator, rps5_delta3_denominator, rps5_delta3_quality,
                   rps20_numerator, rps20_denominator, rps20_quality,
                   target_actual_quote, target_close_numerator, target_close_denominator,
                   target_close_quality
            FROM stock.source_pool_seed_day_facts
            WHERE target_session = %s
            ORDER BY source_symbol
            """,
            (target_session,),
        ).fetchall()
        seed_cols = [d[0] for d in cursor.description]
        seed_by_symbol = {r[0]: dict(zip(seed_cols, r)) for r in seed_rows}

        prof_rows = cursor.execute(
            """
            SELECT source_symbol, close_numerator, close_denominator,
                   high_numerator, high_denominator,
                   ma20_numerator, ma20_denominator,
                   ma_slope20_atr_numerator, ma_slope20_atr_denominator
            FROM stock.source_symbol_price_profiles
            WHERE target_session = %s
            """,
            (target_session,),
        ).fetchall()
        prof_cols = [d[0] for d in cursor.description]
        prof_by_symbol = {r[0]: dict(zip(prof_cols, r)) for r in prof_rows}

        adm_rows = cursor.execute(
            """
            SELECT source_symbol, scenario, event_id,
                   failure_level_numerator, failure_level_denominator,
                   failure_coordinate, failure_source, failure_window_start,
                   effective_from_session, prior_episode_id, linked_anchor_id
            FROM stock.source_d0_formal_admission_day_facts
            WHERE target_session = %s AND admission_status = 'READY'
            ORDER BY source_symbol, scenario
            """,
            (target_session,),
        ).fetchall()
        adm_cols = [d[0] for d in cursor.description]
        events_by_symbol: dict[str, list[Event]] = {}
        for r in adm_rows:
            ar = dict(zip(adm_cols, r))
            fl_val = _frac(
                ar["failure_level_numerator"], ar["failure_level_denominator"]
            )
            if fl_val is None:
                continue
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
                    effective_ordinal=ordinal + 1,
                ),
                linked_anchor_id=ar.get("linked_anchor_id"),
                prior_episode_id=ar.get("prior_episode_id"),
                evidence_ref=f"PG_ADMISSION:{target_session.isoformat()}:{ar['source_symbol']}:{ar['scenario']}",
            )
            events_by_symbol.setdefault(ar["source_symbol"], []).append(ev)

    # 3. 逐股推进 D2 生命周期
    episodes_to_upsert: list[dict[str, Any]] = []
    day_states_to_persist: list[dict[str, Any]] = []
    primaries_to_persist: list[dict[str, Any]] = []
    peaks_to_persist: list[dict[str, Any]] = []
    d2_results_by_symbol: dict[str, D2DayResult] = {}

    all_symbols = sorted(
        set(seed_by_symbol) | set(prior_states_by_symbol) | set(events_by_symbol)
    )
    for sym in all_symbols:
        sd = seed_by_symbol.get(sym, {})
        pr = prof_by_symbol.get(sym, {})
        p_states = tuple(prior_states_by_symbol.get(sym, ()))
        p_mode = "PREVIOUS_ACCEPTED" if p_states else "FIRST_OBSERVATION"

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
            if "target_actual_quote" in sd
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
        close_val = _frac(
            sd.get("target_close_numerator"), sd.get("target_close_denominator")
        )
        if close_val is None:
            close_val = _frac(pr.get("close_numerator"), pr.get("close_denominator"))
        close_fv = (
            FieldValue(close_val) if close_val is not None else unknown("CLOSE_UNKNOWN")
        )
        high_val = _frac(pr.get("high_numerator"), pr.get("high_denominator"))
        high_fv = FieldValue(high_val) if high_val is not None else None

        # 关联支撑硬破状态
        linked_breaches: dict[str, Fact] = {}
        for sup in d1_supports_by_symbol.get(sym, ()):
            aid = f"{sup['d1_structure_id']}:{sup['created_session'].isoformat()}"
            st_val = sup.get("after_state")
            breach_fv = (
                FieldValue(st_val == "BROKEN")
                if st_val
                in (
                    "BROKEN",
                    "IDLE",
                    "APPROACHING",
                    "TESTING",
                    "BREACHED_SHALLOW",
                    "RECLAIMED",
                    "HELD_TENTATIVE",
                    "HELD_CONFIRMED",
                    "RETESTING",
                )
                else unknown("SUPPORT_STATE_UNKNOWN")
            )
            linked_breaches[aid] = _mk_fact(
                breach_fv,
                entity_id=sym,
                session_date=target_session,
                coordinate_id=sup.get("coordinate_id") or "FORWARD_ADJUSTED",
                source_ref=f"D1_SUPPORT:{aid}",
            )

        ev_list = tuple(events_by_symbol.get(sym, ()))
        has_confirmed = bool(ev_list)
        quals = (
            ("SEED", seed_fv),
            ("PREWATCH", pw_fv),
            (
                "WARM",
                unknown("WARM_NOT_COMPUTED")
                if f_pw == "UNKNOWN"
                else FieldValue(False),
            ),
            ("CONFIRMED", FieldValue(True) if has_confirmed else FieldValue(False)),
        )
        d2_inp = D2DayInputs(
            entity_id=sym,
            entity_type="STOCK",
            session=target_session,
            ordinal=ordinal,
            calendar_id=calendar_id,
            coordinate_id="FORWARD_ADJUSTED",
            input_ref=f"PG_R04_DAY:{target_session.isoformat()}:{sym}",
            prior_states=p_states,
            prior_mode=p_mode,
            qualifications=quals,
            actual=actual_fv,
            damage=dmg_fv,
            extension=ext_fv,
            metric=metric_fv,
            followup_complete=FieldValue(True),
            close_in_today_coordinate=close_fv,
            high_in_today_coordinate=high_fv,
            linked_hard_breaches=linked_breaches,
            formal_events=ev_list,
            prior_primary_episode_id=prior_primary_by_symbol.get(sym),
            rules=rules,
            capability_state="READY",
        )
        res = advance_d2_day(d2_inp)
        d2_results_by_symbol[sym] = res

        for st in res.states:
            episodes_to_upsert.append(
                {
                    "episode_id": st.episode_id,
                    "entity_type": st.entity_type,
                    "entity_id": st.entity_id,
                    "calendar_id": st.calendar_id,
                    "rule_identity": st.rule_identity,
                    "scenario": st.scenario,
                    "kind": st.kind,
                    "birth_session": target_session
                    if st.birth_ordinal == ordinal
                    else (prev_session or target_session),
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
                    "effective_from_session": target_session if st.event else None,
                    "linked_anchor_id": st.event.linked_anchor_id if st.event else None,
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
                "candidate_count": len(res.states),
            }
        )
        for pk in res.new_peak_events:
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

    d2_persist_report = persist_d2_day_bundle(
        config,
        session_date=target_session,
        calendar_id=calendar_id,
        rule_identity=rule_identity,
        episodes=episodes_to_upsert,
        day_states=day_states_to_persist,
        primary_projections=primaries_to_persist,
        peak_events=peaks_to_persist,
    )

    # 4. 组装并执行下游 Risk / Priority / Radar / Near-Miss
    proj_inputs: list[StockProjectionInput] = []
    for sym in all_symbols:
        sd = seed_by_symbol.get(sym, {})
        d2_res = d2_results_by_symbol[sym]
        p_states = tuple(prior_states_by_symbol.get(sym, ()))
        p_mode = "PREVIOUS_ACCEPTED" if p_states else "FIRST_OBSERVATION"

        dmg_fact = _mk_fact(
            FieldValue(bool(sd["core_price_damage"]))
            if sd.get("core_price_damage_quality") == "OBSERVED"
            else unknown(sd.get("core_price_damage_reason") or "DAMAGE_UNKNOWN"),
            entity_id=sym,
            session_date=target_session,
            coordinate_id="FORWARD_ADJUSTED",
            source_ref="CORE:PRICE_DAMAGE",
        )
        ext_fact = _mk_fact(
            FieldValue(sd["profile_extension_value"])
            if sd.get("profile_extension_quality") == "OBSERVED"
            else unknown(sd.get("profile_extension_reason") or "EXTENSION_UNKNOWN"),
            entity_id=sym,
            session_date=target_session,
            coordinate_id="FORWARD_ADJUSTED",
            source_ref="PROFILE:EXTENSION",
        )
        sel_id = d2_res.primary_selection.episode_id
        if sel_id is None:
            if d2_res.primary_selection.quality == "NOT_APPLICABLE":
                str_app = "ABSENT"
                sup_app = "ABSENT"
                inv_fact = _mk_fact(
                    FieldValue(None, "NOT_APPLICABLE", "NO_APPLICABLE_EPISODE"),
                    entity_id=sym,
                    session_date=target_session,
                    coordinate_id="FORWARD_ADJUSTED",
                    source_ref="D2:NONE",
                )
                sb_fact = _mk_fact(
                    FieldValue(None, "NOT_APPLICABLE", "NO_APPLICABLE_EPISODE"),
                    entity_id=sym,
                    session_date=target_session,
                    coordinate_id="FORWARD_ADJUSTED",
                    source_ref="D1:NONE",
                )
            else:
                str_app = "UNKNOWN"
                sup_app = "UNKNOWN"
                inv_fact = _mk_fact(
                    unknown("PRIMARY_EPISODE_UNKNOWN"),
                    entity_id=sym,
                    session_date=target_session,
                    coordinate_id="FORWARD_ADJUSTED",
                    source_ref="D2:UNKNOWN",
                )
                sb_fact = _mk_fact(
                    unknown("PRIMARY_EPISODE_UNKNOWN"),
                    entity_id=sym,
                    session_date=target_session,
                    coordinate_id="FORWARD_ADJUSTED",
                    source_ref="D1:UNKNOWN",
                )
            linked_obs: tuple[SupportObservation, ...] = ()
        else:
            sel_state = next(s for s in d2_res.states if s.episode_id == sel_id)
            inv_fact = _mk_fact(
                FieldValue(
                    sel_state.validity.quality == "OBSERVED"
                    and sel_state.validity.value == "INVALIDATED"
                ),
                entity_id=sym,
                session_date=target_session,
                coordinate_id="FORWARD_ADJUSTED",
                source_ref=f"D2:{sel_id}",
                episode_id=sel_id,
            )
            sups = d1_supports_by_symbol.get(sym, [])
            if not sups:
                str_app = "LINKED"
                sup_app = "ABSENT"
                sb_fact = _mk_fact(
                    FieldValue(None, "NOT_APPLICABLE", "SUPPORT_NOT_APPLICABLE"),
                    entity_id=sym,
                    session_date=target_session,
                    coordinate_id="FORWARD_ADJUSTED",
                    source_ref="D1:NONE",
                    episode_id=sel_id,
                )
                linked_obs = ()
            else:
                str_app = "LINKED"
                sup_app = "LINKED"
                any_shallow = any(
                    s.get("after_state") == "BREACHED_SHALLOW" for s in sups
                )
                sb_fact = _mk_fact(
                    FieldValue(any_shallow),
                    entity_id=sym,
                    session_date=target_session,
                    coordinate_id="FORWARD_ADJUSTED",
                    source_ref="D1:SUPPORT",
                    episode_id=sel_id,
                )
                linked_obs = tuple(
                    SupportObservation(
                        anchor_id=f"{s['d1_structure_id']}:{s['created_session'].isoformat()}",
                        ordinal=ordinal,
                        state=s.get("after_state") or "UNKNOWN",
                        acceptance=s.get("after_acceptance") or "UNKNOWN",
                        freshness=s.get("after_freshness") or "STALE",
                        breach_streak=int(s.get("after_breach_streak") or 0),
                        acceptance_streak=int(s.get("after_acceptance_streak") or 0),
                        test_count=int(s.get("after_test_count") or 0),
                        separated_sessions=int(s.get("after_separated_sessions") or 0),
                        retest_qualified=bool(s.get("after_retest_qualified")),
                    )
                    for s in sups
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
        imp_age = ImprovementAge(
            status="NO_PROVEN_IMPROVEMENT" if not p_states else "KNOWN_AGE",
            sessions=None if not p_states else 0,
            reason=None,
        )
        proj_inputs.append(
            StockProjectionInput(
                entity_id=sym,
                stable_id=sym,
                session=target_session,
                session_ordinal=ordinal,
                coordinate_id="FORWARD_ADJUSTED",
                prior_mode=p_mode,
                current_states=d2_res.states,
                prior_states=p_states,
                damage=dmg_fact,
                extension=ext_fact,
                invalidated=inv_fact,
                shallow_breach=sb_fact,
                structure_applicability=str_app,
                support_applicability=sup_app,
                linked_support_observations=linked_obs,
                support_scope_complete=True,
                rps5_delta3_pp=rps_d3_fv,
                rps20=rps20_fv,
                prior20_amount_mean=amt_fv,
                improvement_age=imp_age,
                current_primary=d2_res.primary_selection.field,
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
            "structure_applicability": "ABSENT" if r.risk.structure_na else "LINKED",
            "support_applicability": "ABSENT" if r.risk.support_na else "LINKED",
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
            "damage_value": None,
            "damage_quality": "OBSERVED",
            "extension_value": None,
            "extension_quality": "OBSERVED",
            "invalidated_value": None,
            "invalidated_quality": "OBSERVED",
            "shallow_breach_value": None,
            "shallow_breach_quality": "OBSERVED",
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
            "improvement_age": p.result.improvement_age.sessions,
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
            "is_primary_event": idx == 1,
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
    ]

    downstream_report = persist_downstream_projections_day(
        config,
        session_date=target_session,
        risk_rows=risk_pg_rows,
        priority_rows=priority_pg_rows,
        radar_events=radar_pg_rows,
        near_miss_rows=near_miss_pg_rows,
    )
    return {
        "status": "PERSISTED",
        "session_date": target_session.isoformat(),
        "d2": d2_persist_report,
        "downstream": downstream_report,
    }
