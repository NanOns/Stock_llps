"""D2 下游投影编排：股票综合风险、Priority V1、Radar 事件差与 Near-Miss 解释（纯内存，无 PG 写入）。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from fractions import Fraction
from typing import Any, Mapping, Sequence

from stock_a.domain import FieldValue, unknown
from stock_a.research.facts import Fact, capability
from stock_a.research.priority_radar import (
    ImprovementAge,
    NearMissPathInput,
    NearMissRecord,
    PriorityResult,
    RadarEvent,
    RadarEventBatch,
    evaluate_stock_near_miss,
    evaluate_stock_priority,
    generate_stock_radar_events,
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
    improvement_age: ImprovementAge
    expired_anchor_ids: frozenset[str] = frozenset()
    current_primary: FieldValue | None = None
    prior_primary: FieldValue | None = None
    prior_risk: RiskResult | None = None
    near_miss_paths: tuple[NearMissPathInput, ...] = ()
    predicate_evidence: Mapping[str, Any] | None = None
    capability_state: str = "READY"


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
class ThreeFamilyCoverageSummary:
    """三族覆盖率汇总（不混用榜单/Focus 分母）。"""

    risk: FamilyCoverage
    priority: FamilyCoverage
    structure: FamilyCoverage


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
    coverage_summary: ThreeFamilyCoverageSummary


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

        prio_result = evaluate_stock_priority(
            eligibility=eligibility,
            risk=risk_result,
            effective_bucket=effective_bucket,
            rps5_delta3_pp=item.rps5_delta3_pp,
            rps20=item.rps20,
            linked_support_ranks=linked_ranks,
            scope_complete=item.support_scope_complete,
            improvement_age=item.improvement_age,
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

        # 3. Radar 事件差
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
    coverage_summary = ThreeFamilyCoverageSummary(
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
