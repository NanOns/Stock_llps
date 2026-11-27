"""D2 正式生命周期编排：合法父峰生产、PB/CONTINUE 正式准入与逐日状态推进。"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date
from fractions import Fraction
from typing import Any, Mapping, Sequence

from stock_a.domain import ContractError, FieldValue, unknown
from stock_a.research.admission import Event, SCENARIOS, line_breached
from stock_a.research.d0_pb_continue import (
    FormalParentContext,
    PeakEvent,
    ScenarioBatchResult,
    create_birth_peak_event,
    evaluate_new_peak_event,
    evaluate_strong_pullback_day,
    evaluate_trend_continue_day,
)
from stock_a.research.facts import Fact, boolean, capability, number
from stock_a.research.priority_radar import classify_emergence
from stock_a.research.state import (
    CONTRACT as D2_STATE_CONTRACT,
    Day,
    Rules,
    State,
    primary_episode,
    reduce_episode,
)


@dataclass(frozen=True)
class SupportBinding:
    """t-1 关联旧 support 锚的冻结绑定。"""

    support_anchor_id: str | None
    support_lower: FieldValue
    support_valid: FieldValue


@dataclass(frozen=True)
class EligibleParentSet:
    """t-1 合法 FORMAL 股票父集及其真实性状态。"""

    parents: tuple[FormalParentContext, ...]
    lineage_unknown: bool


@dataclass(frozen=True)
class PeakProductionResult:
    """当日新产生的父峰事件集合（仅供 t+1 及以后 D0 使用）。"""

    session: date
    ordinal: int
    new_peak_events: tuple[PeakEvent, ...]


@dataclass(frozen=True)
class FormalScenarioProductionResult:
    """STRONG_PULLBACK 与 TREND_CONTINUE 正式计算与准入结果。"""

    pullback: ScenarioBatchResult
    trend_continue: ScenarioBatchResult
    admitted_events: tuple[Event, ...]


@dataclass(frozen=True)
class PBContinueAndPeaksResult:
    """单日 PB/CONTINUE 装配与旧合法父新峰生产结果（供 coordinator 直接接入）。"""

    eligible_parents: EligibleParentSet
    pb_continue_result: FormalScenarioProductionResult
    peak_result: PeakProductionResult


@dataclass(frozen=True)
class BlockedEventApplication:
    """同日退出阻断同谱系重入的审计记录（不回写原始场景事实）。"""

    event: Event
    blocked_by_episode_id: str | None
    reason_code: str
    state: State


@dataclass(frozen=True)
class PrimarySelection:
    """D2 当日主 episode 选主投影。"""

    field: FieldValue
    episode_id: str | None
    quality: str
    reason: str


@dataclass(frozen=True)
class D2DayInputs:
    """advance_d2_day 的显式输入集合（不隐式读库或网络 I/O）。"""

    entity_id: str
    entity_type: str
    session: date
    ordinal: int
    calendar_id: str
    coordinate_id: str
    input_ref: str
    prior_states: tuple[State, ...]
    prior_mode: str
    qualifications: tuple[tuple[str, FieldValue], ...]
    actual: FieldValue
    damage: FieldValue
    extension: FieldValue
    metric: FieldValue
    followup_complete: FieldValue
    close_in_today_coordinate: FieldValue
    high_in_today_coordinate: FieldValue | None = None
    linked_hard_breaches: Mapping[str, Fact] = None  # type: ignore[assignment]
    formal_events: tuple[Event, ...] = ()
    prior_primary_episode_id: str | None = None
    prior_gap_reason: str | None = None
    coordinate_comparable: FieldValue | None = None
    comparable_failure_lines: Mapping[str, FieldValue] | None = None
    rules: Rules = Rules(
        version=D2_STATE_CONTRACT,
        downgrade_sessions=2,
        expiry_sessions=10,
        improvement_pp=3,
        health_deadband_pp=3,
    )
    capability_state: str = "READY"

    def __post_init__(self) -> None:
        if self.linked_hard_breaches is None:
            object.__setattr__(self, "linked_hard_breaches", {})
        if self.comparable_failure_lines is None:
            object.__setattr__(self, "comparable_failure_lines", {})


@dataclass(frozen=True)
class D2DayResult:
    """D2 逐日推进完整输出，供 PG Writer 原子落库。"""

    entity_id: str
    entity_type: str
    session: date
    ordinal: int
    calendar_id: str
    coordinate_id: str
    states: tuple[State, ...]
    primary_selection: PrimarySelection
    new_peak_events: tuple[PeakEvent, ...]
    blocked_event_applications: tuple[BlockedEventApplication, ...]
    exited_episode_ids: tuple[str, ...]
    pb_continue_result: FormalScenarioProductionResult | None = None


def collect_eligible_formal_parents(
    *,
    prior_states: Sequence[State],
    target_ordinal: int,
    prior_peaks_by_parent: Mapping[str, PeakEvent],
    prior_supports_by_parent: Mapping[str, SupportBinding],
    prior_lineage_unknown: bool = False,
) -> EligibleParentSet:
    """筛选 t-1 已接受、FRESH、VALID、未退出、四正式场景之一的 FORMAL 股票 episode。"""
    parents: list[FormalParentContext] = []
    lineage_unknown = prior_lineage_unknown
    for state in prior_states:
        if state.entity_type != "STOCK" or state.kind != "FORMAL":
            continue
        if state.ordinal != target_ordinal - 1:
            raise ValueError("父 episode 前态必须来自 t-1 官方会话")
        if state.exit_ordinal is not None:
            continue
        if state.freshness != "FRESH" or state.validity.quality != "OBSERVED":
            lineage_unknown = True
            continue
        if state.validity.value != "VALID":
            continue
        if (
            not state.episode_id
            or state.birth_ordinal is None
            or state.scenario not in SCENARIOS
            or state.event is None
        ):
            lineage_unknown = True
            continue
        support = prior_supports_by_parent.get(state.episode_id)
        if support is None:
            support = SupportBinding(
                support_anchor_id=None,
                support_lower=unknown("PRIOR_SUPPORT_BINDING_MISSING"),
                support_valid=unknown("PRIOR_SUPPORT_BINDING_MISSING"),
            )
        parents.append(
            FormalParentContext(
                episode_id=state.episode_id,
                entity_id=state.entity_id,
                scenario=state.scenario,
                birth_ordinal=state.birth_ordinal,
                prior_peak=prior_peaks_by_parent.get(state.episode_id),
                support_anchor_id=support.support_anchor_id,
                support_lower=support.support_lower,
                support_valid=support.support_valid,
            )
        )
    parents.sort(key=lambda item: (-item.birth_ordinal, item.episode_id))
    return EligibleParentSet(parents=tuple(parents), lineage_unknown=lineage_unknown)


def produce_peak_events(
    *,
    eligible_parents: Sequence[FormalParentContext],
    session: date,
    ordinal: int,
    actual: FieldValue,
    high: FieldValue,
    close: FieldValue,
    coordinate_id: str,
    source_ref: str,
    comparable: FieldValue | None = None,
) -> PeakProductionResult:
    """基于 t-1 合法父及 t-1 冻结峰，按 D1 实际 high 严格创新高生成新 peak_event。"""
    new_peaks: list[PeakEvent] = []
    for parent in eligible_parents:
        if parent.prior_peak is None:
            continue
        peak = evaluate_new_peak_event(
            entity_id=parent.entity_id,
            parent_episode_id=parent.episode_id,
            prior_peak=parent.prior_peak,
            session=session,
            ordinal=ordinal,
            actual=actual,
            high=high,
            close=close,
            coordinate_id=coordinate_id,
            source_ref=source_ref,
            comparable=comparable,
        )
        if peak is not None:
            new_peaks.append(peak)
    return PeakProductionResult(
        session=session,
        ordinal=ordinal,
        new_peak_events=tuple(new_peaks),
    )


def produce_pb_continue_day(
    *,
    entity_id: str,
    target_session: date,
    target_ordinal: int,
    sessions: Sequence[date],
    session_ordinals: Mapping[date, int],
    rows_by_session: Mapping[date, Mapping[str, Any] | None],
    source_symbol: str,
    source_capture_id: int,
    coordinate_id: str,
    eligible_parent_set: EligibleParentSet,
    gates: tuple[FieldValue, ...],
    core_ma_slope20_atr: FieldValue,
    core_prior_high20: FieldValue,
    rps20: FieldValue,
    loo_confirmed_raws: Sequence[FieldValue] | None,
    evidence_ref_prefix: str,
    capability_state: str = "READY",
) -> FormalScenarioProductionResult:
    """只读 t-1 合法父与 t-1 冻结峰，正式计算 STRONG_PULLBACK 与 TREND_CONTINUE 并准入。"""
    pb_result = evaluate_strong_pullback_day(
        entity_id=entity_id,
        target_session=target_session,
        target_ordinal=target_ordinal,
        sessions=sessions,
        session_ordinals=session_ordinals,
        rows_by_session=rows_by_session,
        source_symbol=source_symbol,
        source_capture_id=source_capture_id,
        coordinate_id=coordinate_id,
        parents=eligible_parent_set.parents,
        prior_lineage_unknown=eligible_parent_set.lineage_unknown,
        gates=gates,
        evidence_ref_prefix=f"{evidence_ref_prefix}:PB",
        capability_state=capability_state,
    )
    cont_result = evaluate_trend_continue_day(
        entity_id=entity_id,
        target_session=target_session,
        target_ordinal=target_ordinal,
        sessions=sessions,
        session_ordinals=session_ordinals,
        rows_by_session=rows_by_session,
        source_symbol=source_symbol,
        source_capture_id=source_capture_id,
        coordinate_id=coordinate_id,
        parents=eligible_parent_set.parents,
        prior_lineage_unknown=eligible_parent_set.lineage_unknown,
        gates=gates,
        core_ma_slope20_atr=core_ma_slope20_atr,
        core_prior_high20=core_prior_high20,
        rps20=rps20,
        loo_confirmed_raws=loo_confirmed_raws,
        evidence_ref_prefix=f"{evidence_ref_prefix}:CONTINUE",
        capability_state=capability_state,
    )
    admitted = pb_result.admitted_events + cont_result.admitted_events
    return FormalScenarioProductionResult(
        pullback=pb_result,
        trend_continue=cont_result,
        admitted_events=admitted,
    )


def select_prior_peaks_by_parent(
    peak_rows: Sequence[Mapping[str, Any]],
    *,
    before_ordinal: int,
) -> dict[str, PeakEvent]:
    """从 PG 读回的历史父峰中，严格取 before_ordinal 之前的最高峰（等高取最早）。"""
    ordered = sorted(
        (pk for pk in peak_rows if int(pk["peak_ordinal"]) < before_ordinal),
        key=lambda pk: (str(pk["parent_episode_id"]), int(pk["peak_ordinal"])),
    )
    seq_counts: dict[str, int] = {}
    selected: dict[str, PeakEvent] = {}
    for pk in ordered:
        parent_id = str(pk["parent_episode_id"])
        seq_counts[parent_id] = seq_counts.get(parent_id, 0) + 1
        peak_seq = int(pk.get("peak_seq") or seq_counts[parent_id])
        pe = PeakEvent(
            peak_event_id=str(
                pk.get("peak_event_id")
                or f"{parent_id}:{pk['peak_session'].isoformat()}"
            ),
            entity_id=str(pk["entity_id"]),
            parent_episode_id=parent_id,
            peak_session=pk["peak_session"],
            peak_ordinal=int(pk["peak_ordinal"]),
            peak_seq=peak_seq,
            peak_high=pk["peak_high"],
            peak_close=pk["peak_close"],
            coordinate_id=str(pk["coordinate_id"]),
            source_ref=str(pk.get("source_ref") or "D1_ACTUAL_HIGH"),
            created_from=str(pk.get("created_from") or "NEW_HIGH"),
        )
        prev = selected.get(parent_id)
        if prev is None or pe.peak_high > prev.peak_high:
            selected[parent_id] = pe
    return selected


def _support_binding_from_observation(
    anchor_id: str,
    obs: Mapping[str, Any] | None,
    *,
    target_coordinate_id: str,
) -> SupportBinding:
    if obs is None:
        return SupportBinding(
            support_anchor_id=anchor_id,
            support_lower=unknown("PRIOR_SUPPORT_OBSERVATION_MISSING"),
            support_valid=unknown("PRIOR_SUPPORT_OBSERVATION_MISSING"),
        )
    obs_coord = obs.get("coordinate_id")
    if obs_coord and obs_coord != target_coordinate_id:
        return SupportBinding(
            support_anchor_id=anchor_id,
            support_lower=unknown("PRIOR_SUPPORT_COORDINATE_MISMATCH"),
            support_valid=unknown("PRIOR_SUPPORT_COORDINATE_MISMATCH"),
        )
    state_val = obs.get("after_state") or obs.get("state")
    if state_val in ("BROKEN", "EXPIRED"):
        return SupportBinding(
            support_anchor_id=anchor_id,
            support_lower=unknown("PRIOR_SUPPORT_BROKEN_OR_EXPIRED"),
            support_valid=FieldValue(False),
        )
    if state_val not in (
        "IDLE",
        "APPROACHING",
        "TESTING",
        "BREACHED_SHALLOW",
        "RECLAIMED",
        "HELD_TENTATIVE",
        "HELD_CONFIRMED",
        "RETESTING",
    ):
        return SupportBinding(
            support_anchor_id=anchor_id,
            support_lower=unknown("PRIOR_SUPPORT_STATE_UNKNOWN"),
            support_valid=unknown("PRIOR_SUPPORT_STATE_UNKNOWN"),
        )
    raw_lower = obs.get("support_lower")
    if raw_lower is None:
        raw_lower = obs.get("lower_bound")
    if isinstance(raw_lower, FieldValue):
        lower_num = number(raw_lower, positive=True)
        lower_fv = (
            raw_lower
            if lower_num is not None
            else unknown("PRIOR_SUPPORT_LOWER_UNKNOWN")
        )
    elif raw_lower is not None and Fraction(raw_lower) > 0:
        lower_fv = FieldValue(Fraction(raw_lower))
        lower_num = Fraction(raw_lower)
    else:
        lower_fv = unknown("PRIOR_SUPPORT_LOWER_UNKNOWN")
        lower_num = None
    if lower_num is None:
        return SupportBinding(
            support_anchor_id=anchor_id,
            support_lower=lower_fv,
            support_valid=unknown("PRIOR_SUPPORT_LOWER_UNKNOWN"),
        )
    return SupportBinding(
        support_anchor_id=anchor_id,
        support_lower=lower_fv,
        support_valid=FieldValue(True),
    )


def build_support_bindings_for_parents(
    *,
    prior_states: Sequence[State],
    d1_supports_by_anchor_id: Mapping[str, Mapping[str, Any]],
    target_coordinate_id: str,
    applicable_prior_anchor_ids: Sequence[str] = (),
    supports_proven_complete: bool = False,
) -> dict[str, SupportBinding]:
    """为 t-1 合法父构造真实旧 support 绑定；不可用旁支/坏锚，无证缺失不可 MA20 回退。"""
    bindings: dict[str, SupportBinding] = {}
    for st in prior_states:
        if st.entity_type != "STOCK" or st.kind != "FORMAL" or not st.episode_id:
            continue
        linked_anchor_id = st.event.linked_anchor_id if st.event is not None else None
        if linked_anchor_id:
            bindings[st.episode_id] = _support_binding_from_observation(
                linked_anchor_id,
                d1_supports_by_anchor_id.get(linked_anchor_id),
                target_coordinate_id=target_coordinate_id,
            )
        elif applicable_prior_anchor_ids:
            anchor_id = applicable_prior_anchor_ids[0]
            bindings[st.episode_id] = _support_binding_from_observation(
                anchor_id,
                d1_supports_by_anchor_id.get(anchor_id),
                target_coordinate_id=target_coordinate_id,
            )
        elif supports_proven_complete:
            # 仅当已证明无任何适用旧 support 时，允许 CONTINUE 走 T0_MA20_FALLBACK
            bindings[st.episode_id] = SupportBinding(
                support_anchor_id=None,
                support_lower=FieldValue(
                    None, "NOT_APPLICABLE", "NO_APPLICABLE_PRIOR_SUPPORT"
                ),
                support_valid=FieldValue(
                    None, "NOT_APPLICABLE", "NO_APPLICABLE_PRIOR_SUPPORT"
                ),
            )
        else:
            bindings[st.episode_id] = SupportBinding(
                support_anchor_id=None,
                support_lower=unknown("PRIOR_SUPPORT_ABSENCE_UNPROVEN"),
                support_valid=unknown("PRIOR_SUPPORT_ABSENCE_UNPROVEN"),
            )
    return bindings


def produce_pb_continue_and_peaks_for_stock(
    *,
    entity_id: str,
    target_session: date,
    target_ordinal: int,
    sessions: Sequence[date],
    session_ordinals: Mapping[date, int],
    rows_by_session: Mapping[date, Mapping[str, Any] | None],
    source_symbol: str,
    source_capture_id: int,
    coordinate_id: str,
    prior_states: Sequence[State],
    prior_peaks_by_parent: Mapping[str, PeakEvent],
    prior_supports_by_parent: Mapping[str, SupportBinding],
    prior_lineage_unknown: bool,
    gates: tuple[FieldValue, ...],
    core_ma_slope20_atr: FieldValue,
    core_prior_high20: FieldValue,
    rps20: FieldValue,
    loo_confirmed_raws: Sequence[FieldValue] | None,
    actual: FieldValue,
    high_in_today_coordinate: FieldValue,
    close_in_today_coordinate: FieldValue,
    evidence_ref_prefix: str,
    peak_comparable: FieldValue | None = None,
    capability_state: str = "READY",
) -> PBContinueAndPeaksResult:
    """一站式装配 t-1 合法父、PB/CONTINUE 正式准入与今日旧合法父新峰（仅供 t+1+ 使用）。"""
    eligible = collect_eligible_formal_parents(
        prior_states=prior_states,
        target_ordinal=target_ordinal,
        prior_peaks_by_parent=prior_peaks_by_parent,
        prior_supports_by_parent=prior_supports_by_parent,
        prior_lineage_unknown=prior_lineage_unknown,
    )
    pb_cont = produce_pb_continue_day(
        entity_id=entity_id,
        target_session=target_session,
        target_ordinal=target_ordinal,
        sessions=sessions,
        session_ordinals=session_ordinals,
        rows_by_session=rows_by_session,
        source_symbol=source_symbol,
        source_capture_id=source_capture_id,
        coordinate_id=coordinate_id,
        eligible_parent_set=eligible,
        gates=gates,
        core_ma_slope20_atr=core_ma_slope20_atr,
        core_prior_high20=core_prior_high20,
        rps20=rps20,
        loo_confirmed_raws=loo_confirmed_raws,
        evidence_ref_prefix=evidence_ref_prefix,
        capability_state=capability_state,
    )
    peaks = produce_peak_events(
        eligible_parents=eligible.parents,
        session=target_session,
        ordinal=target_ordinal,
        actual=actual,
        high=high_in_today_coordinate,
        close=close_in_today_coordinate,
        coordinate_id=coordinate_id,
        source_ref=f"{evidence_ref_prefix}:D1_ACTUAL_HIGH",
        comparable=peak_comparable,
    )
    return PBContinueAndPeaksResult(
        eligible_parents=eligible,
        pb_continue_result=pb_cont,
        peak_result=peaks,
    )


def compute_stock_warm_qualification(
    *,
    prewatch: FieldValue,
    rps5_delta3_pp: FieldValue,
    close: FieldValue,
    ma20: FieldValue,
    ma_slope20_atr: FieldValue,
) -> FieldValue:
    """计算股票 WARM = PREWATCH ∧ ACTIVE_EMERGENCE ∧ C > MA20 ∧ slope > 0（三态 AND）。"""
    p_pw = boolean(prewatch)
    emerg_level, _ = classify_emergence(rps5_delta3_pp)
    p_emerg: bool | None = None if emerg_level is None else (emerg_level != "NONE")
    c_val = number(close, positive=True)
    m_val = number(ma20, positive=True)
    p_c_gt_ma20: bool | None = (
        None if (c_val is None or m_val is None) else (c_val > m_val)
    )
    s_val = number(ma_slope20_atr)
    p_slope: bool | None = None if s_val is None else (s_val > 0)

    if any(p is False for p in (p_pw, p_emerg, p_c_gt_ma20, p_slope)):
        return FieldValue(False)
    if p_pw is None:
        return unknown(prewatch.reason or "PREWATCH_UNKNOWN")
    if p_emerg is None:
        return unknown(rps5_delta3_pp.reason or "EMERGENCE_UNKNOWN")
    if p_c_gt_ma20 is None:
        return unknown(close.reason or ma20.reason or "CLOSE_OR_MA20_UNKNOWN")
    if p_slope is None:
        return unknown(ma_slope20_atr.reason or "SLOPE_UNKNOWN")
    return FieldValue(True)


def resolve_prior_context(
    *,
    prior_states: Sequence[State],
    prev_session_exists: bool,
    prev_session_completed: bool,
    has_earlier_history: bool,
) -> tuple[str, str | None]:
    """区分 PREVIOUS_ACCEPTED / FIRST_OBSERVATION / 缺前态（不把缺口或左截尾当首观测）。"""
    if prior_states:
        return "PREVIOUS_ACCEPTED", None
    if not prev_session_exists:
        if has_earlier_history:
            return "MISSING_PRIOR", "PRIOR_STATE_LEFT_CENSORED"
        return "FIRST_OBSERVATION", None
    if not prev_session_completed:
        return "MISSING_PRIOR", "PRIOR_SESSION_NOT_COMPUTED"
    if has_earlier_history:
        return "MISSING_PRIOR", "PRIOR_STATE_GAP"
    return "FIRST_OBSERVATION", None


def _with_confirmed_qual(
    qualifications: tuple[tuple[str, FieldValue], ...],
    confirmed: FieldValue,
) -> tuple[tuple[str, FieldValue], ...]:
    return tuple(
        (key, confirmed if key == "CONFIRMED" else val) for key, val in qualifications
    )


def _build_invalidation_facts(
    *,
    prior: State,
    inputs: D2DayInputs,
) -> tuple[Fact, Fact]:
    if prior.event is None:
        frozen_fact = Fact(
            field=FieldValue(None, "NOT_APPLICABLE", "NO_FROZEN_LINE"),
            entity_id=inputs.entity_id,
            session=inputs.session,
            coordinate_id=inputs.coordinate_id,
            source_ref=inputs.input_ref,
            episode_id=prior.episode_id,
        )
    else:
        event_ref = prior.event.evidence_ref or prior.event.failure_line.source_ref
        if inputs.ordinal < prior.event.failure_line.effective_ordinal:
            breach_field = FieldValue(False)
        elif (
            prior.episode_id
            and inputs.comparable_failure_lines
            and prior.episode_id in inputs.comparable_failure_lines
        ):
            breach_field = line_breached(
                inputs.close_in_today_coordinate,
                inputs.comparable_failure_lines[prior.episode_id],
                ordinal=inputs.ordinal,
                effective_ordinal=prior.event.failure_line.effective_ordinal,
            )
        elif prior.event.failure_line.coordinate_id != inputs.coordinate_id:
            breach_field = unknown("FAILURE_LINE_COORDINATE_MISMATCH")
        elif (
            inputs.coordinate_comparable is not None
            and boolean(inputs.coordinate_comparable) is not True
        ):
            breach_field = unknown(
                inputs.coordinate_comparable.reason or "QFQ_BASIS_NOT_COMPARABLE"
            )
        else:
            breach_field = line_breached(
                inputs.close_in_today_coordinate,
                FieldValue(prior.event.failure_line.value),
                ordinal=inputs.ordinal,
                effective_ordinal=prior.event.failure_line.effective_ordinal,
            )
        frozen_fact = Fact(
            field=breach_field,
            entity_id=inputs.entity_id,
            session=inputs.session,
            coordinate_id=inputs.coordinate_id,
            source_ref=event_ref,
            episode_id=prior.episode_id,
        )

    if prior.event is None or prior.event.linked_anchor_id is None:
        linked_fact = Fact(
            field=FieldValue(None, "NOT_APPLICABLE", "NO_LINKED_ANCHOR"),
            entity_id=inputs.entity_id,
            session=inputs.session,
            coordinate_id=inputs.coordinate_id,
            source_ref=inputs.input_ref,
            episode_id=prior.episode_id,
        )
    else:
        provided = inputs.linked_hard_breaches.get(
            prior.episode_id or ""
        ) or inputs.linked_hard_breaches.get(prior.event.linked_anchor_id)
        if provided is None:
            linked_fact = Fact(
                field=unknown("LINKED_ANCHOR_HARD_BREACH_UNKNOWN"),
                entity_id=inputs.entity_id,
                session=inputs.session,
                coordinate_id=inputs.coordinate_id,
                source_ref=inputs.input_ref,
                episode_id=prior.episode_id,
            )
        elif provided.coordinate_id != inputs.coordinate_id:
            linked_fact = Fact(
                field=unknown("LINKED_ANCHOR_COORDINATE_MISMATCH"),
                entity_id=inputs.entity_id,
                session=inputs.session,
                coordinate_id=inputs.coordinate_id,
                source_ref=provided.source_ref,
                episode_id=prior.episode_id,
            )
        else:
            linked_fact = replace(provided, episode_id=prior.episode_id)
    return frozen_fact, linked_fact


def _na_facts(inputs: D2DayInputs, episode_id: str | None) -> tuple[Fact, Fact]:
    na_frozen = Fact(
        field=FieldValue(None, "NOT_APPLICABLE", "NEW_EPISODE_ENTRY"),
        entity_id=inputs.entity_id,
        session=inputs.session,
        coordinate_id=inputs.coordinate_id,
        source_ref=inputs.input_ref,
        episode_id=episode_id,
    )
    na_linked = Fact(
        field=FieldValue(None, "NOT_APPLICABLE", "NEW_EPISODE_ENTRY"),
        entity_id=inputs.entity_id,
        session=inputs.session,
        coordinate_id=inputs.coordinate_id,
        source_ref=inputs.input_ref,
        episode_id=episode_id,
    )
    return na_frozen, na_linked


def advance_d2_day(
    inputs: D2DayInputs,
    *,
    existing_new_peak_events: Sequence[PeakEvent] = (),
    pb_continue_result: FormalScenarioProductionResult | None = None,
) -> D2DayResult:
    """严格按顺序执行 D2 逐日推进：先推进旧 episode，再应用当日新事件，最后 primary_episode 选主。"""
    capability(inputs.capability_state)
    prior_by_id: dict[str, State] = {}
    for prior in inputs.prior_states:
        if prior.episode_id:
            if prior.episode_id in prior_by_id:
                raise ValueError("t-1 前态存在重复 episode_id")
            prior_by_id[prior.episode_id] = prior

    # 第一遍：先推进所有旧 episode（不带当日 new_event）
    updated_priors: list[State] = []
    updated_by_id: dict[str, State] = {}
    exited_today_ids: set[str] = set()
    admitted_event_ids = {ev.event_id for ev in inputs.formal_events}
    for prior in inputs.prior_states:
        frozen_fact, linked_fact = _build_invalidation_facts(prior=prior, inputs=inputs)
        quals = inputs.qualifications
        if inputs.entity_type == "STOCK":
            if prior.event is None or prior.event_id not in admitted_event_ids:
                # 旧 CORE 或其他旧 FORMAL episode 不能借今日其它事件升/维持 CONFIRMED
                quals = _with_confirmed_qual(quals, FieldValue(False))
        day = Day(
            entity_id=inputs.entity_id,
            entity_type=inputs.entity_type,
            session=inputs.session,
            ordinal=inputs.ordinal,
            calendar_id=inputs.calendar_id,
            coordinate_id=inputs.coordinate_id,
            input_ref=inputs.input_ref,
            qualifications=quals,
            actual=inputs.actual,
            damage=inputs.damage,
            extension=inputs.extension,
            metric=inputs.metric,
            frozen_failure=frozen_fact,
            linked_hard_breach=linked_fact,
            followup_complete=inputs.followup_complete,
            prior_mode=inputs.prior_mode,
            new_event=None,
            exit_in_lineage_today=False,
        )
        updated = reduce_episode(
            prior,
            day,
            rules=inputs.rules,
            capability_state=inputs.capability_state,
        )
        updated_priors.append(updated)
        if updated.episode_id:
            updated_by_id[updated.episode_id] = updated
        if updated.episode_id and updated.exit_ordinal == inputs.ordinal:
            exited_today_ids.add(updated.episode_id)

    # 第二遍：应用当日有效 FORMAL 事件（并处理同日退出阻断与分叉保留）
    today_states: list[State] = list(updated_priors)
    seen_episode_ids: set[str] = {
        s.episode_id for s in today_states if s.episode_id is not None
    }
    blocked_apps: list[BlockedEventApplication] = []
    birth_peaks: list[PeakEvent] = list(existing_new_peak_events)

    if inputs.formal_events:
        if inputs.entity_type != "STOCK":
            raise ContractError("仅股票实体支持应用 D0 正式事件")
        event_quals = _with_confirmed_qual(inputs.qualifications, FieldValue(True))
        for event in inputs.formal_events:
            if event.prior_episode_id is not None:
                target_prior = prior_by_id.get(event.prior_episode_id)
                if target_prior is None:
                    raise ContractError("延续/回踩事件未找到对应的 t-1 前态 episode")
                lineage_exited = target_prior.episode_id in exited_today_ids
            else:
                if (
                    inputs.prior_primary_episode_id
                    and inputs.prior_primary_episode_id in prior_by_id
                ):
                    target_prior = prior_by_id[inputs.prior_primary_episode_id]
                else:
                    # 主选未知或无适用主选时，绝不任意指定 prior_states[0] 作父
                    target_prior = None
                lineage_exited = (
                    target_prior is not None
                    and target_prior.episode_id in exited_today_ids
                )

            if lineage_exited and target_prior is not None and target_prior.episode_id:
                blocked_apps.append(
                    BlockedEventApplication(
                        event=event,
                        blocked_by_episode_id=target_prior.episode_id,
                        reason_code="EVENT_APPLICATION_BLOCKED_EXIT_SESSION",
                        state=updated_by_id[target_prior.episode_id],
                    )
                )
                continue

            if target_prior is not None:
                frozen_fact, linked_fact = _build_invalidation_facts(
                    prior=target_prior, inputs=inputs
                )
                mode = inputs.prior_mode
            else:
                frozen_fact, linked_fact = _na_facts(inputs, None)
                mode = "FIRST_OBSERVATION"

            day_event = Day(
                entity_id=inputs.entity_id,
                entity_type=inputs.entity_type,
                session=inputs.session,
                ordinal=inputs.ordinal,
                calendar_id=inputs.calendar_id,
                coordinate_id=inputs.coordinate_id,
                input_ref=inputs.input_ref,
                qualifications=event_quals,
                actual=inputs.actual,
                damage=inputs.damage,
                extension=inputs.extension,
                metric=inputs.metric,
                frozen_failure=frozen_fact,
                linked_hard_breach=linked_fact,
                followup_complete=inputs.followup_complete,
                prior_mode=mode,
                new_event=event,
                exit_in_lineage_today=lineage_exited,
            )
            child_state = reduce_episode(
                target_prior,
                day_event,
                rules=inputs.rules,
                capability_state=inputs.capability_state,
            )
            if "EVENT_APPLICATION_BLOCKED_EXIT_SESSION" in child_state.transitions:
                blocked_apps.append(
                    BlockedEventApplication(
                        event=event,
                        blocked_by_episode_id=target_prior.episode_id
                        if target_prior
                        else None,
                        reason_code="EVENT_APPLICATION_BLOCKED_EXIT_SESSION",
                        state=child_state,
                    )
                )
                continue

            if (
                child_state.episode_id is not None
                and child_state.episode_id not in seen_episode_ids
            ):
                today_states.append(child_state)
                seen_episode_ids.add(child_state.episode_id)
                if (
                    child_state.kind == "FORMAL"
                    and child_state.birth_ordinal == inputs.ordinal
                    and inputs.high_in_today_coordinate is not None
                ):
                    h_val = number(inputs.high_in_today_coordinate, positive=True)
                    c_val = number(inputs.close_in_today_coordinate, positive=True)
                    if h_val is not None and c_val is not None:
                        birth_peaks.append(
                            create_birth_peak_event(
                                entity_id=inputs.entity_id,
                                parent_episode_id=child_state.episode_id,
                                birth_session=inputs.session,
                                birth_ordinal=inputs.ordinal,
                                high=h_val,
                                close=c_val,
                                coordinate_id=inputs.coordinate_id,
                                source_ref=inputs.input_ref,
                            )
                        )
    elif not inputs.prior_states:
        if (
            inputs.prior_gap_reason is not None
            or inputs.prior_mode != "FIRST_OBSERVATION"
        ):
            gap_reason = inputs.prior_gap_reason or "PRIOR_STATE_MISSING"
            today_states.append(
                State(
                    entity_id=inputs.entity_id,
                    entity_type=inputs.entity_type,
                    ordinal=inputs.ordinal,
                    calendar_id=inputs.calendar_id,
                    rule_identity=inputs.rules.identity,
                    episode_id=None,
                    event_id=None,
                    scenario="NONE",
                    kind="NONE",
                    birth_ordinal=None,
                    parent_episode_id=None,
                    event=None,
                    exact_maturity=None,
                    lower_bound_maturity="NONE",
                    display_maturity="NONE",
                    effective_eligibility=unknown(gap_reason),
                    effective_bucket=None,
                    validity=unknown(gap_reason),
                    freshness="STALE",
                    health=unknown(gap_reason),
                    tracking="PAUSED",
                    downgrade_candidate=None,
                    downgrade_count=0,
                    expiry_count=0,
                    improvement_baseline=None,
                    exit_ordinal=None,
                    exit_reason=None,
                    reentry_armed=False,
                    transitions=(gap_reason,),
                    preserved_episode_ids=(),
                    input_ref=inputs.input_ref,
                )
            )
        else:
            na_frozen, na_linked = _na_facts(inputs, None)
            day_init = Day(
                entity_id=inputs.entity_id,
                entity_type=inputs.entity_type,
                session=inputs.session,
                ordinal=inputs.ordinal,
                calendar_id=inputs.calendar_id,
                coordinate_id=inputs.coordinate_id,
                input_ref=inputs.input_ref,
                qualifications=inputs.qualifications,
                actual=inputs.actual,
                damage=inputs.damage,
                extension=inputs.extension,
                metric=inputs.metric,
                frozen_failure=na_frozen,
                linked_hard_breach=na_linked,
                followup_complete=inputs.followup_complete,
                prior_mode=inputs.prior_mode,
                new_event=None,
                exit_in_lineage_today=False,
            )
            init_state = reduce_episode(
                None,
                day_init,
                rules=inputs.rules,
                capability_state=inputs.capability_state,
            )
            today_states.append(init_state)

    # 第三步：调用 state.primary_episode 选主
    primary_field = primary_episode(today_states)
    primary_sel = PrimarySelection(
        field=primary_field,
        episode_id=primary_field.value if primary_field.quality == "OBSERVED" else None,
        quality=primary_field.quality,
        reason=primary_field.reason or "PRIMARY_SELECTED",
    )
    return D2DayResult(
        entity_id=inputs.entity_id,
        entity_type=inputs.entity_type,
        session=inputs.session,
        ordinal=inputs.ordinal,
        calendar_id=inputs.calendar_id,
        coordinate_id=inputs.coordinate_id,
        states=tuple(today_states),
        primary_selection=primary_sel,
        new_peak_events=tuple(birth_peaks),
        blocked_event_applications=tuple(blocked_apps),
        exited_episode_ids=tuple(sorted(exited_today_ids)),
        pb_continue_result=pb_continue_result,
    )


__all__ = [
    "BlockedEventApplication",
    "D2DayInputs",
    "D2DayResult",
    "EligibleParentSet",
    "FormalScenarioProductionResult",
    "PBContinueAndPeaksResult",
    "PeakProductionResult",
    "PrimarySelection",
    "SupportBinding",
    "advance_d2_day",
    "build_support_bindings_for_parents",
    "collect_eligible_formal_parents",
    "compute_stock_warm_qualification",
    "produce_pb_continue_and_peaks_for_stock",
    "produce_pb_continue_day",
    "produce_peak_events",
    "resolve_prior_context",
    "select_prior_peaks_by_parent",
]
