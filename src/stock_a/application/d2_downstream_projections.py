"""D2 下游投影编排：股票综合风险、Priority V1、Radar 事件差与 Near-Miss 解释（纯内存，无 PG 写入）。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence, Set as AbstractSet
from dataclasses import dataclass
from datetime import date
from fractions import Fraction
from typing import Any

from stock_a.domain import FieldValue, unknown
from stock_a.research.facts import Fact, capability
from stock_a.research.priority_radar import (
    ImprovementAge,
    NearMissPathInput,
    NearMissRecord,
    PriorityResult,
    RadarEvent,
    RadarEventBatch,
    SoftPredicateCheck,
    detect_existing_anchor_rank_improvement,
    evaluate_stock_near_miss,
    evaluate_stock_priority,
    generate_stock_radar_events,
    resolve_episode_improvement_age,
    sort_radar_events,
    support_observation_to_structure_rank,
)
from stock_a.research.risk import RiskResult, compose as compose_risk
from stock_a.research.state import State, primary_episode
from stock_a.research.structure import SupportObservation


@dataclass(frozen=True)
class StockProjectionInput:
    """单只股票当日下游投影的完整输入绑定。"""

    entity_id: str
    stable_id: str
    session: date
    session_ordinal: int
    coordinate_id: str
    prior_mode: str  # FIRST_OBSERVATION / PREVIOUS_ACCEPTED / MODEL_BOUNDARY
    current_states: tuple[State, ...]
    prior_states: tuple[State, ...]
    damage: Fact
    extension: Fact
    invalidated: Fact
    shallow_breach: Fact
    structure_applicability: str  # ABSENT / LINKED / UNKNOWN
    support_applicability: str  # ABSENT / LINKED / UNKNOWN
    linked_support_observations: tuple[SupportObservation, ...]
    support_scope_complete: bool
    rps5_delta3_pp: FieldValue
    rps20: FieldValue
    prior20_amount_mean: FieldValue
    improvement_age: ImprovementAge | None = None
    expired_anchor_ids: frozenset[str] = frozenset()
    current_primary: FieldValue | None = None
    prior_primary: FieldValue | None = None
    prior_risk: RiskResult | None = None
    near_miss_paths: tuple[NearMissPathInput, ...] = ()
    predicate_evidence: Mapping[str, Any] | None = None
    capability_state: str = "READY"
    prior_last_improvement_ordinal: int | None = None
    prior_history_complete: bool | None = None
    prior_linked_support_observations: tuple[SupportObservation, ...] = ()


@dataclass(frozen=True)
class RiskComponentDetail:
    """单个风险分项（P/E/I/W）的持久化值、质量与原因；绝不允许 value=None 却标 OBSERVED。"""

    value: bool | str | None
    quality: str  # OBSERVED / UNKNOWN / NOT_APPLICABLE
    reason: str | None
    episode_id: str | None = None
    anchor_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.quality not in {"OBSERVED", "UNKNOWN", "NOT_APPLICABLE"}:
            raise ValueError(f"非法的风险分项质量: {self.quality}")
        if self.quality == "OBSERVED" and self.value is None:
            raise ValueError("风险分项 quality='OBSERVED' 时 value 不能为 None")
        if self.quality != "OBSERVED" and self.value is not None:
            raise ValueError("风险分项非 OBSERVED 时 value 必须为 None")


def fact_to_risk_component_detail(
    fact: Fact, *, anchor_ids: Sequence[str] = ()
) -> RiskComponentDetail:
    """将 Fact 安全转换为 PG 可持久化的 RiskComponentDetail。"""
    if fact.field.quality == "OBSERVED" and fact.field.value is not None:
        return RiskComponentDetail(
            value=fact.field.value,
            quality="OBSERVED",
            reason=fact.field.reason,
            episode_id=fact.episode_id,
            anchor_ids=tuple(anchor_ids),
        )
    if fact.field.quality == "NOT_APPLICABLE":
        return RiskComponentDetail(
            value=None,
            quality="NOT_APPLICABLE",
            reason=fact.field.reason or "NOT_APPLICABLE",
            episode_id=fact.episode_id,
            anchor_ids=tuple(anchor_ids),
        )
    return RiskComponentDetail(
        value=None,
        quality="UNKNOWN",
        reason=fact.field.reason or "UNKNOWN_DATA",
        episode_id=fact.episode_id,
        anchor_ids=tuple(anchor_ids),
    )


@dataclass(frozen=True)
class AssembledStockRiskInputs:
    """经主 episode 严格关联过滤与三态校验后的股票风险输入包。"""

    selected_episode_id: str | None
    structure_applicability: str
    support_applicability: str
    damage: Fact
    extension: Fact
    invalidated: Fact
    shallow_breach: Fact
    linked_support_observations: tuple[SupportObservation, ...]
    linked_anchor_ids: tuple[str, ...]
    support_scope_complete: bool


def assemble_stock_risk_inputs(
    *,
    entity_id: str,
    session: date,
    coordinate_id: str,
    damage_field: FieldValue,
    extension_field: FieldValue,
    primary_selection: FieldValue,
    selected_state: State | None,
    candidate_supports: Sequence[SupportObservation],
    linked_anchor_ids: AbstractSet[str] | None,
    support_scope_known: bool,
    support_applicable_proven_absent: bool = False,
    anchor_roles: Mapping[str, str] | None = None,
    expired_anchor_ids: AbstractSet[str] = frozenset(),
    damage_source_ref: str = "PROFILE:DAMAGE",
    extension_source_ref: str = "PROFILE:EXTENSION",
) -> AssembledStockRiskInputs:
    """按当前选主 episode 与直接关联支撑组装 P/E/I/W，隔离旁支坏锚与阻力锚。"""
    dmg_fact = Fact(
        field=damage_field,
        entity_id=entity_id,
        session=session,
        coordinate_id=coordinate_id,
        source_ref=damage_source_ref,
    )
    ext_fact = Fact(
        field=extension_field,
        entity_id=entity_id,
        session=session,
        coordinate_id=coordinate_id,
        source_ref=extension_source_ref,
    )

    if primary_selection.quality == "NOT_APPLICABLE":
        na_reason = primary_selection.reason or "NO_APPLICABLE_EPISODE"
        return AssembledStockRiskInputs(
            selected_episode_id=None,
            structure_applicability="ABSENT",
            support_applicability="ABSENT",
            damage=dmg_fact,
            extension=ext_fact,
            invalidated=Fact(
                field=FieldValue(None, "NOT_APPLICABLE", na_reason),
                entity_id=entity_id,
                session=session,
                coordinate_id=coordinate_id,
                source_ref="D2:NONE",
            ),
            shallow_breach=Fact(
                field=FieldValue(None, "NOT_APPLICABLE", na_reason),
                entity_id=entity_id,
                session=session,
                coordinate_id=coordinate_id,
                source_ref="D1:NONE",
            ),
            linked_support_observations=(),
            linked_anchor_ids=(),
            support_scope_complete=True,
        )

    if primary_selection.quality != "OBSERVED" or selected_state is None:
        unk_reason = primary_selection.reason or "PRIMARY_EPISODE_UNKNOWN"
        return AssembledStockRiskInputs(
            selected_episode_id=None,
            structure_applicability="UNKNOWN",
            support_applicability="UNKNOWN",
            damage=dmg_fact,
            extension=ext_fact,
            invalidated=Fact(
                field=unknown(unk_reason),
                entity_id=entity_id,
                session=session,
                coordinate_id=coordinate_id,
                source_ref="D2:UNKNOWN",
            ),
            shallow_breach=Fact(
                field=unknown(unk_reason),
                entity_id=entity_id,
                session=session,
                coordinate_id=coordinate_id,
                source_ref="D1:UNKNOWN",
            ),
            linked_support_observations=(),
            linked_anchor_ids=(),
            support_scope_complete=False,
        )

    sel_ep_id = selected_state.episode_id
    if not sel_ep_id:
        raise ValueError("已选中的主 State 缺少 episode_id")

    if selected_state.validity.quality == "OBSERVED":
        inv_fv = FieldValue(selected_state.validity.value == "INVALIDATED")
    elif selected_state.validity.quality == "UNKNOWN_DATA":
        inv_fv = unknown(selected_state.validity.reason or "EPISODE_VALIDITY_UNKNOWN")
    else:
        inv_fv = unknown("EPISODE_VALIDITY_UNEXPECTED_NA")

    inv_fact = Fact(
        field=inv_fv,
        entity_id=entity_id,
        session=session,
        coordinate_id=coordinate_id,
        source_ref=f"D2:{sel_ep_id}",
        episode_id=sel_ep_id,
    )

    if linked_anchor_ids is None:
        return AssembledStockRiskInputs(
            selected_episode_id=sel_ep_id,
            structure_applicability="LINKED",
            support_applicability="UNKNOWN",
            damage=dmg_fact,
            extension=ext_fact,
            invalidated=inv_fact,
            shallow_breach=Fact(
                field=unknown("SUPPORT_LINKAGE_UNKNOWN"),
                entity_id=entity_id,
                session=session,
                coordinate_id=coordinate_id,
                source_ref="D1:LINKAGE_UNKNOWN",
                episode_id=sel_ep_id,
            ),
            linked_support_observations=(),
            linked_anchor_ids=(),
            support_scope_complete=False,
        )

    active_linked_ids = {
        aid
        for aid in linked_anchor_ids
        if aid not in expired_anchor_ids
        and (anchor_roles is None or anchor_roles.get(aid, "SUPPORT") == "SUPPORT")
    }
    linked_obs = tuple(
        obs for obs in candidate_supports if obs.anchor_id in active_linked_ids
    )
    resolved_anchor_ids = tuple(sorted(obs.anchor_id for obs in linked_obs))

    if not linked_obs:
        if support_applicable_proven_absent and support_scope_known:
            return AssembledStockRiskInputs(
                selected_episode_id=sel_ep_id,
                structure_applicability="LINKED",
                support_applicability="ABSENT",
                damage=dmg_fact,
                extension=ext_fact,
                invalidated=inv_fact,
                shallow_breach=Fact(
                    field=FieldValue(None, "NOT_APPLICABLE", "SUPPORT_NOT_APPLICABLE"),
                    entity_id=entity_id,
                    session=session,
                    coordinate_id=coordinate_id,
                    source_ref="D1:NONE",
                    episode_id=sel_ep_id,
                ),
                linked_support_observations=(),
                linked_anchor_ids=(),
                support_scope_complete=True,
            )
        return AssembledStockRiskInputs(
            selected_episode_id=sel_ep_id,
            structure_applicability="LINKED",
            support_applicability="UNKNOWN",
            damage=dmg_fact,
            extension=ext_fact,
            invalidated=inv_fact,
            shallow_breach=Fact(
                field=unknown("SUPPORT_OBSERVATION_MISSING"),
                entity_id=entity_id,
                session=session,
                coordinate_id=coordinate_id,
                source_ref="D1:UNKNOWN",
                episode_id=sel_ep_id,
            ),
            linked_support_observations=(),
            linked_anchor_ids=tuple(sorted(active_linked_ids)),
            support_scope_complete=False,
        )

    any_shallow = any(obs.state == "BREACHED_SHALLOW" for obs in linked_obs)
    any_unknown = any(obs.state == "UNKNOWN" for obs in linked_obs)
    observed_ids = {obs.anchor_id for obs in linked_obs}
    all_linked_observed = support_scope_known and active_linked_ids.issubset(
        observed_ids
    )

    if any_shallow:
        sb_fv = FieldValue(True)
    elif any_unknown or not all_linked_observed:
        sb_fv = unknown("SUPPORT_SHALLOW_BREACH_UNKNOWN")
    else:
        sb_fv = FieldValue(False)

    return AssembledStockRiskInputs(
        selected_episode_id=sel_ep_id,
        structure_applicability="LINKED",
        support_applicability="LINKED",
        damage=dmg_fact,
        extension=ext_fact,
        invalidated=inv_fact,
        shallow_breach=Fact(
            field=sb_fv,
            entity_id=entity_id,
            session=session,
            coordinate_id=coordinate_id,
            source_ref="D1:SUPPORT",
            episode_id=sel_ep_id,
        ),
        linked_support_observations=linked_obs,
        linked_anchor_ids=resolved_anchor_ids,
        support_scope_complete=all_linked_observed,
    )


@dataclass(frozen=True)
class StockRiskRow:
    """单股综合风险输出行。"""

    entity_id: str
    stable_id: str
    session: date
    session_ordinal: int
    coordinate_id: str
    selected_episode_id: str | None
    primary_selection_quality: str
    primary_selection_reason: str | None
    risk: RiskResult
    damage_detail: RiskComponentDetail | None = None
    extension_detail: RiskComponentDetail | None = None
    invalidated_detail: RiskComponentDetail | None = None
    shallow_breach_detail: RiskComponentDetail | None = None
    linked_anchor_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class StockPriorityRow:
    """单股 Priority V1 输出行（保留全池，不因显示上限截断）。"""

    entity_id: str
    stable_id: str
    session: date
    session_ordinal: int
    selected_episode_id: str | None
    rank: int | None
    result: PriorityResult


@dataclass(frozen=True)
class FamilyCoverage:
    """单族覆盖率汇总，分母严格为研究池总数。"""

    family: str
    total: int
    known: int
    unknown: int
    coverage: Fraction | None


@dataclass(frozen=True)
class DownstreamFamilyCoverageSummary:
    """下游三族输出覆盖率汇总（risk/priority/structure，非股票/板块/市场三族风险）。"""

    risk: FamilyCoverage
    priority: FamilyCoverage
    structure: FamilyCoverage


ThreeFamilyCoverageSummary = DownstreamFamilyCoverageSummary


@dataclass(frozen=True)
class SingleFamilyRiskSummary:
    """单个风险族（股票/板块/市场）的真实独立覆盖与等级分布汇总。"""

    family: str  # stock / sector / market
    total: int
    known: int
    unknown: int
    coverage: Fraction | None
    level_counts: Mapping[str, int]
    changed_count: int | None = None
    source_contract: str = "UNKNOWN"


@dataclass(frozen=True)
class ThreeFamilyRiskSummary:
    """合同规定的三族风险汇总：股票、板块（R03）、市场（R03）。"""

    stock: SingleFamilyRiskSummary
    sector: SingleFamilyRiskSummary
    market: SingleFamilyRiskSummary


def compose_three_family_risk_summary(
    *,
    stock_risk_rows: Sequence[StockRiskRow],
    stock_radar_events: Sequence[RadarEvent] = (),
    sector_risk_summary: Mapping[str, Any] | None = None,
    market_risk_summary: Mapping[str, Any] | None = None,
) -> ThreeFamilyRiskSummary:
    """组合股票、板块（复用 R03 已存事实）、市场（复用 R03 已存事实）三族真实风险。"""
    stock_total = len(stock_risk_rows)
    stock_known = sum(1 for r in stock_risk_rows if r.risk.level.quality == "OBSERVED")
    stock_levels = {"LOW": 0, "MEDIUM": 0, "HIGH": 0, "EXTREME": 0}
    for r in stock_risk_rows:
        if r.risk.level.quality == "OBSERVED" and r.risk.level.value in stock_levels:
            stock_levels[str(r.risk.level.value)] += 1
    stock_changed = sum(
        1
        for ev in stock_radar_events
        if ev.event_type in {"RISK_WORSENED", "RISK_IMPROVED"}
    )
    stock_summary = SingleFamilyRiskSummary(
        family="stock",
        total=stock_total,
        known=stock_known,
        unknown=stock_total - stock_known,
        coverage=Fraction(stock_known, stock_total) if stock_total > 0 else None,
        level_counts=stock_levels,
        changed_count=stock_changed,
        source_contract="STOCK_A_STOCK_RISK_COMPOSITION_R1",
    )

    def _from_r03_mapping(
        family: str, payload: Mapping[str, Any] | None, default_contract: str
    ) -> SingleFamilyRiskSummary:
        if payload is None:
            return SingleFamilyRiskSummary(
                family=family,
                total=0,
                known=0,
                unknown=0,
                coverage=None,
                level_counts={},
                changed_count=None,
                source_contract=f"{default_contract}:NOT_LOADED",
            )
        total = int(payload.get("total") or 0)
        known = int(payload.get("known") or 0)
        unknown_cnt = int(payload.get("unknown") or max(0, total - known))
        cov = payload.get("coverage")
        if cov is None and total > 0:
            cov = Fraction(known, total)
        return SingleFamilyRiskSummary(
            family=family,
            total=total,
            known=known,
            unknown=unknown_cnt,
            coverage=cov,
            level_counts=dict(payload.get("level_counts") or {}),
            changed_count=payload.get("changed_count"),
            source_contract=str(payload.get("source_contract") or default_contract),
        )

    return ThreeFamilyRiskSummary(
        stock=stock_summary,
        sector=_from_r03_mapping("sector", sector_risk_summary, "R03_SECTOR_RISK"),
        market=_from_r03_mapping("market", market_risk_summary, "R03_MARKET_RISK"),
    )


@dataclass(frozen=True)
class D0SoftPredicateSpec:
    """D0 具名软谓词规格：绑定谓词名、容差种类、实际值、阈值及比较符。"""

    predicate_id: str
    soft_kind: str  # clv / amr_lower / amr_upper / rps_pp / breakout_price / pb_drawdown_lower / pb_drawdown_upper / pb_contraction
    actual: FieldValue
    threshold: FieldValue
    operator: str
    atr_today: FieldValue | None = None


@dataclass(frozen=True)
class D0ScenarioNearMissInput:
    """单条 D0 具名场景 AND 路径的真实谓词与门控证据。"""

    scenario: str
    path_id: str
    raw_matched: FieldValue
    hard_gates: Sequence[FieldValue]
    soft_specs: Sequence[D0SoftPredicateSpec]
    parent_episode_id: str | None = None
    peak_id: str | None = None


def assemble_near_miss_paths_from_d0(
    *,
    entity_id: str,
    stable_id: str,
    session_ordinal: int,
    emergence_rank: int | None,
    rps5_delta3_pp: FieldValue,
    common_safety_gates: Sequence[FieldValue],
    scenario_inputs: Sequence[D0ScenarioNearMissInput],
) -> tuple[NearMissPathInput, ...]:
    """从实际 D0 具名谓词、共同安全门、硬门与数值/阈值组装 NearMissPathInput。

    注意：本函数不自动放行或软化任何硬门；已有原始 TRUE、任一硬门/安全门 FALSE/UNKNOWN、
    任一必需软输入未知、或多于 1 个软失败，均由 evaluate_stock_near_miss 严格排除。
    """
    paths: list[NearMissPathInput] = []
    common_tuple = tuple(common_safety_gates)
    for sc in scenario_inputs:
        soft_checks = tuple(
            SoftPredicateCheck(
                predicate_id=spec.predicate_id,
                soft_kind=spec.soft_kind,
                actual=spec.actual,
                threshold=spec.threshold,
                operator=spec.operator,
                atr_today=spec.atr_today,
            )
            for spec in sc.soft_specs
        )
        paths.append(
            NearMissPathInput(
                entity_id=entity_id,
                session_ordinal=session_ordinal,
                scenario=sc.scenario,
                path_id=sc.path_id,
                parent_episode_id=sc.parent_episode_id,
                peak_id=sc.peak_id,
                common_safety_gates=common_tuple,
                hard_gates=tuple(sc.hard_gates),
                raw_matched=sc.raw_matched,
                soft_predicates=soft_checks,
                emergence_rank=emergence_rank,
                rps5_delta3_pp=rps5_delta3_pp,
                stable_id=stable_id,
            )
        )
    return tuple(paths)


@dataclass(frozen=True)
class D2DownstreamProjectionResult:
    """全市场/研究池当日 D2 下游投影集合。"""

    session: date
    session_ordinal: int
    stock_risk_rows: tuple[StockRiskRow, ...]
    stock_priority_rows: tuple[StockPriorityRow, ...]
    stock_radar_events: tuple[RadarEvent, ...]
    stock_radar_batches: tuple[RadarEventBatch, ...]
    stock_near_miss_rows: tuple[NearMissRecord, ...]
    coverage_summary: DownstreamFamilyCoverageSummary


def _find_state_by_episode(
    states: Sequence[State], episode_id: str | None
) -> State | None:
    if episode_id is None:
        return None
    for state in states:
        if state.episode_id == episode_id:
            return state
    return None


def _build_family_coverage(family: str, total: int, known: int) -> FamilyCoverage:
    if total < 0 or known < 0 or known > total:
        raise ValueError("覆盖率统计计数非法")
    return FamilyCoverage(
        family=family,
        total=total,
        known=known,
        unknown=total - known,
        coverage=Fraction(known, total) if total > 0 else None,
    )


def build_d2_downstream_projections(
    items: Sequence[StockProjectionInput],
    *,
    session: date,
    session_ordinal: int,
    capability_state: str = "READY",
) -> D2DownstreamProjectionResult:
    """编排全池当日股票风险、Priority V1、Radar 事件差与 Near-Miss 解释。"""
    capability(capability_state)
    if type(session) is not date:
        raise ValueError("session必须为date")
    if type(session_ordinal) is not int or session_ordinal < 0:
        raise ValueError("session_ordinal必须为非负整数")

    seen_entities: set[str] = set()
    risk_rows: list[StockRiskRow] = []
    priority_rows_unsorted: list[StockPriorityRow] = []
    radar_batches: list[RadarEventBatch] = []
    all_radar_events: list[RadarEvent] = []
    all_near_miss_paths: list[NearMissPathInput] = []

    risk_known = 0
    priority_known = 0
    structure_known = 0

    for item in items:
        if item.session != session or item.session_ordinal != session_ordinal:
            raise ValueError(f"输入实体 {item.entity_id} 日期/会话序号与批处理不一致")
        if item.entity_id in seen_entities:
            raise ValueError(f"输入实体重复: {item.entity_id}")
        seen_entities.add(item.entity_id)

        curr_primary = (
            item.current_primary
            if item.current_primary is not None
            else primary_episode(item.current_states)
        )
        prev_primary = (
            item.prior_primary
            if item.prior_primary is not None
            else primary_episode(item.prior_states)
        )

        selected_ep_id = (
            str(curr_primary.value) if curr_primary.quality == "OBSERVED" else None
        )
        prev_ep_id = (
            str(prev_primary.value) if prev_primary.quality == "OBSERVED" else None
        )
        curr_state = _find_state_by_episode(item.current_states, selected_ep_id)
        prev_state = _find_state_by_episode(item.prior_states, prev_ep_id)

        # 1. 股票综合风险
        risk_result = compose_risk(
            entity_id=item.entity_id,
            session=item.session,
            coordinate_id=item.coordinate_id,
            selected_episode_id=selected_ep_id,
            structure_applicability=item.structure_applicability,
            support_applicability=item.support_applicability,
            damage=item.damage,
            extension=item.extension,
            invalidated=item.invalidated,
            shallow_breach=item.shallow_breach,
            capability_state=item.capability_state,
        )
        if risk_result.level.quality == "OBSERVED":
            risk_known += 1

        linked_anchor_ids = tuple(
            sorted(
                obs.anchor_id
                for obs in item.linked_support_observations
                if obs.anchor_id not in item.expired_anchor_ids
            )
        )
        risk_rows.append(
            StockRiskRow(
                entity_id=item.entity_id,
                stable_id=item.stable_id,
                session=item.session,
                session_ordinal=item.session_ordinal,
                coordinate_id=item.coordinate_id,
                selected_episode_id=selected_ep_id,
                primary_selection_quality=curr_primary.quality,
                primary_selection_reason=curr_primary.reason,
                risk=risk_result,
                damage_detail=fact_to_risk_component_detail(item.damage),
                extension_detail=fact_to_risk_component_detail(item.extension),
                invalidated_detail=fact_to_risk_component_detail(item.invalidated),
                shallow_breach_detail=fact_to_risk_component_detail(
                    item.shallow_breach, anchor_ids=linked_anchor_ids
                ),
                linked_anchor_ids=linked_anchor_ids,
            )
        )

        # 2. Priority V1
        if curr_primary.quality == "OBSERVED" and curr_state is not None:
            eligibility = curr_state.effective_eligibility
            effective_bucket = curr_state.effective_bucket
        elif curr_primary.quality == "NOT_APPLICABLE":
            eligibility = FieldValue(False)
            effective_bucket = None
        else:
            eligibility = unknown(curr_primary.reason or "PRIMARY_SELECTION_UNKNOWN")
            effective_bucket = None

        linked_ranks: list[FieldValue] = []
        for obs in item.linked_support_observations:
            rank_field = support_observation_to_structure_rank(
                obs, is_expired=obs.anchor_id in item.expired_anchor_ids
            )
            if rank_field is not None:
                linked_ranks.append(rank_field)

        if item.improvement_age is not None:
            resolved_imp_age = item.improvement_age
        else:
            anchor_rank_improved = detect_existing_anchor_rank_improvement(
                item.prior_linked_support_observations,
                item.linked_support_observations,
                expired_anchor_ids=item.expired_anchor_ids,
            )
            resolved_imp_age = resolve_episode_improvement_age(
                current_ordinal=item.session_ordinal,
                episode_id=selected_ep_id,
                primary_selection_quality=curr_primary.quality,
                current_state=curr_state,
                prior_last_improvement_ordinal=item.prior_last_improvement_ordinal,
                prior_history_complete=bool(item.prior_history_complete)
                if item.prior_history_complete is not None
                else False,
                prior_mode=item.prior_mode,
                existing_anchor_rank_improved=anchor_rank_improved,
            )

        prio_result = evaluate_stock_priority(
            eligibility=eligibility,
            risk=risk_result,
            effective_bucket=effective_bucket,
            rps5_delta3_pp=item.rps5_delta3_pp,
            rps20=item.rps20,
            linked_support_ranks=linked_ranks,
            scope_complete=item.support_scope_complete,
            improvement_age=resolved_imp_age,
            prior20_amount_mean=item.prior20_amount_mean,
            stable_id=item.stable_id,
            capability_state=item.capability_state,
        )
        if prio_result.status != "UNKNOWN":
            priority_known += 1
        if prio_result.structure_rank.quality == "OBSERVED":
            structure_known += 1

        priority_rows_unsorted.append(
            StockPriorityRow(
                entity_id=item.entity_id,
                stable_id=item.stable_id,
                session=item.session,
                session_ordinal=item.session_ordinal,
                selected_episode_id=selected_ep_id,
                rank=None,
                result=prio_result,
            )
        )

        # 3. Radar 事件差（传递全股独立 episode 集合，确保被选主过滤的旧 episode 退出/失效事件不丢失）
        batch = generate_stock_radar_events(
            entity_id=item.entity_id,
            session_ordinal=item.session_ordinal,
            current_state=curr_state,
            prior_state=prev_state,
            current_primary=curr_primary,
            prior_primary=prev_primary,
            current_risk=risk_result,
            prior_risk=item.prior_risk,
            priority_result=prio_result,
            prior_mode=item.prior_mode,
            predicate_evidence=item.predicate_evidence,
            capability_state=item.capability_state,
            all_current_states=item.current_states,
            all_prior_states=item.prior_states,
        )
        radar_batches.append(batch)
        all_radar_events.extend(batch.events)

        # 4. 收集 Near-Miss 路径
        all_near_miss_paths.extend(item.near_miss_paths)

    # Priority 排序与分配名次（不截断底层）
    rankable = [r for r in priority_rows_unsorted if r.result.status == "RANKABLE"]
    excluded = [r for r in priority_rows_unsorted if r.result.status == "EXCLUDED"]
    unknown_rows = [r for r in priority_rows_unsorted if r.result.status == "UNKNOWN"]

    rankable.sort(key=lambda r: r.result.sort_key)  # type: ignore[arg-type]
    excluded.sort(key=lambda r: r.stable_id)
    unknown_rows.sort(key=lambda r: r.stable_id)

    sorted_priority_rows: list[StockPriorityRow] = []
    for idx, row in enumerate(rankable, start=1):
        sorted_priority_rows.append(
            StockPriorityRow(
                entity_id=row.entity_id,
                stable_id=row.stable_id,
                session=row.session,
                session_ordinal=row.session_ordinal,
                selected_episode_id=row.selected_episode_id,
                rank=idx,
                result=row.result,
            )
        )
    sorted_priority_rows.extend(excluded)
    sorted_priority_rows.extend(unknown_rows)

    # Near-Miss 评估与排序
    near_miss_rows = evaluate_stock_near_miss(
        all_near_miss_paths, capability_state=capability_state
    )

    total = len(items)
    coverage_summary = DownstreamFamilyCoverageSummary(
        risk=_build_family_coverage("risk", total, risk_known),
        priority=_build_family_coverage("priority", total, priority_known),
        structure=_build_family_coverage("structure", total, structure_known),
    )

    return D2DownstreamProjectionResult(
        session=session,
        session_ordinal=session_ordinal,
        stock_risk_rows=tuple(risk_rows),
        stock_priority_rows=tuple(sorted_priority_rows),
        stock_radar_events=sort_radar_events(all_radar_events),
        stock_radar_batches=tuple(radar_batches),
        stock_near_miss_rows=near_miss_rows,
        coverage_summary=coverage_summary,
    )
