"""P17/P18 股票 Priority V1、Radar 事件差与 Near-Miss 解释纯核（无 I/O）。"""

from __future__ import annotations

from dataclasses import dataclass
from fractions import Fraction
from typing import Any, Mapping, Sequence

from stock_a.domain import ContractError, FieldValue, exact_number, unknown
from stock_a.research.admission import SCENARIOS
from stock_a.research.facts import boolean, capability, number
from stock_a.research.risk import LEVELS as RISK_LEVELS, RiskResult, priority_status
from stock_a.research.state import BUCKET, State
from stock_a.research.structure import SupportObservation

PRIORITY_CONTRACT = "STOCK_A_PRIORITY_V1_R1"
RADAR_CONTRACT = "STOCK_A_RADAR_EVENTS_R1"
NEAR_MISS_CONTRACT = "STOCK_A_NEAR_MISS_R1"

BUCKET_NAMES = {v: k for k, v in BUCKET.items()}
EMERGENCE_RANKS = {"NONE": 0, "LOW": 1, "MEDIUM": 2, "HIGH": 3}
STRUCTURE_RANKS = {
    "NONE": 0,
    "TENTATIVE": 1,
    "RECLAIMED": 2,
    "HELD_CONFIRMED": 3,
    "ACCEPTED": 4,
}
RISK_RANKS = {level: idx for idx, level in enumerate(RISK_LEVELS)}

RADAR_PRIMARY_PRIORITY = (
    "INVALIDATED",
    "EXPIRED",
    "EXITED",
    "WEAKENED",
    "UPGRADED",
    "REENTERED",
    "FIRST_ENROLLED",
    "RISK_WORSENED",
    "RISK_IMPROVED",
)
RADAR_ANNOTATIONS = (
    "RULE_CHANGED",
    "SELECTION_CHANGED",
    "FIRST_OBSERVED",
    "BECAME_KNOWN",
)

NEAR_MISS_TOLERANCES = {
    "clv": Fraction(5, 100),
    "amr_lower": Fraction(1, 10),
    "amr_upper": Fraction(1, 10),
    "rps_pp": Fraction(1, 1),
    "pb_drawdown_lower": Fraction(1, 100),
    "pb_drawdown_upper": Fraction(1, 100),
    "pb_contraction": Fraction(5, 100),
}
BREAKOUT_ATR_TOLERANCE_FACTOR = Fraction(1, 10)


@dataclass(frozen=True)
class ImprovementAge:
    """改善年龄三态表达：区分已知年龄、完整首观测无改善与未知。"""

    status: str  # KNOWN_AGE / NO_PROVEN_IMPROVEMENT / UNKNOWN
    age_sessions: int | None = None
    reason: str | None = None

    def __post_init__(self):
        if self.status == "KNOWN_AGE":
            if type(self.age_sessions) is not int or self.age_sessions < 0:
                raise ValueError("KNOWN_AGE必须提供非负整数会话年龄")
            if self.reason is not None:
                raise ValueError("KNOWN_AGE不应携带缺失原因")
        elif self.status == "NO_PROVEN_IMPROVEMENT":
            if self.age_sessions is not None:
                raise ValueError("NO_PROVEN_IMPROVEMENT不能写伪年龄")
        elif self.status == "UNKNOWN":
            if self.age_sessions is not None or not self.reason:
                raise ValueError("UNKNOWN改善年龄必须为空且带原因")
        else:
            raise ValueError(f"不支持的改善年龄状态: {self.status}")

    @property
    def sort_key(self) -> tuple[int, int] | None:
        if self.status == "KNOWN_AGE":
            assert self.age_sessions is not None
            return (0, self.age_sessions)
        if self.status == "NO_PROVEN_IMPROVEMENT":
            return (1, 0)
        return None


@dataclass(frozen=True)
class PriorityResult:
    """股票 Priority V1 评估结果与完整有序键。"""

    stable_id: str
    status: str  # RANKABLE / EXCLUDED / UNKNOWN
    bucket: int | None
    bucket_label: str | None
    emergence_label: str | None
    emergence_rank: int | None
    structure_rank: FieldValue
    rps5_delta3_pp: FieldValue
    rps20: FieldValue
    risk_level: FieldValue
    risk_rank: int | None
    improvement_age: ImprovementAge
    prior20_amount_mean: FieldValue
    sort_key: tuple[Any, ...] | None
    reasons: tuple[str, ...]
    contract: str = PRIORITY_CONTRACT


@dataclass(frozen=True)
class RadarEvent:
    """D2/风险真实变化事件与独立注记。"""

    event_type: str
    entity_id: str
    session_ordinal: int
    episode_id: str | None
    event_id: str | None
    why_now: str
    waiting_for: str
    invalid_if: str
    predicate_evidence: tuple[tuple[str, Any], ...]
    annotations: tuple[str, ...]
    priority_group: int  # 0=RANKABLE, 1=EXCLUDED, 2=UNKNOWN
    priority_sort_key: tuple[Any, ...] | None
    contract: str = RADAR_CONTRACT


@dataclass(frozen=True)
class RadarEventBatch:
    """单股当日 Radar 输出，保留全部真实事件与主展示未知标记。"""

    entity_id: str
    session_ordinal: int
    events: tuple[RadarEvent, ...]
    annotations: tuple[str, ...]
    primary_display_unknown: bool
    primary_display_reason: str | None = None
    contract: str = RADAR_CONTRACT


@dataclass(frozen=True)
class SoftPredicateCheck:
    """Near-Miss 软数值谓词检查输入。"""

    predicate_id: str
    soft_kind: str
    actual: FieldValue
    threshold: FieldValue
    operator: str  # ">=", ">", "<=", "<"
    atr_today: FieldValue | None = None


@dataclass(frozen=True)
class NearMissPathInput:
    """单个具名场景 AND 路径的 Near-Miss 输入。"""

    entity_id: str
    session_ordinal: int
    scenario: str
    path_id: str
    parent_episode_id: str | None
    peak_id: str | None
    common_safety_gates: tuple[FieldValue, ...]
    hard_gates: tuple[FieldValue, ...]
    raw_matched: FieldValue
    soft_predicates: tuple[SoftPredicateCheck, ...]
    emergence_rank: int | None
    rps5_delta3_pp: FieldValue


@dataclass(frozen=True)
class NearMissRecord:
    """Near-Miss 近阈值解释记录。"""

    entity_id: str
    session_ordinal: int
    scenario: str
    path_id: str
    parent_episode_id: str | None
    peak_id: str | None
    failed_predicate_id: str
    soft_kind: str
    actual_value: Fraction
    threshold_value: Fraction
    operator: str
    distance: Fraction
    tolerance: Fraction
    normalized_distance: Fraction
    sort_key: tuple[Any, ...]
    contract: str = NEAR_MISS_CONTRACT


def classify_emergence(rps5_delta3_pp: FieldValue) -> tuple[str | None, int | None]:
    """按 ALG-32 分类股票 emergence 等级。"""
    val = number(rps5_delta3_pp)
    if val is None:
        return None, None
    if val >= 10:
        return "HIGH", EMERGENCE_RANKS["HIGH"]
    if val >= 3:
        return "MEDIUM", EMERGENCE_RANKS["MEDIUM"]
    if val > 0:
        return "LOW", EMERGENCE_RANKS["LOW"]
    return "NONE", EMERGENCE_RANKS["NONE"]


def support_observation_to_structure_rank(
    obs: SupportObservation, *, is_expired: bool = False
) -> FieldValue | None:
    """将直接关联的 D1 SupportObservation 映射为 §11.7 结构 rank（0..4）。"""
    if not isinstance(obs, SupportObservation):
        raise ContractError("结构候选必须为 SupportObservation")
    if (obs.state == "BROKEN") != (obs.acceptance == "BROKEN"):
        raise ValueError("support与acceptance破坏状态矛盾")
    if is_expired or obs.state == "BROKEN":
        return None
    if obs.state == "UNKNOWN" or obs.acceptance == "UNKNOWN":
        return unknown("SUPPORT_OBSERVATION_UNKNOWN")
    if obs.acceptance == "ACCEPTED":
        return FieldValue(4)
    if obs.state == "HELD_CONFIRMED":
        return FieldValue(3)
    if obs.state == "RECLAIMED":
        return FieldValue(2)
    if obs.state in {
        "IDLE",
        "APPROACHING",
        "TESTING",
        "HELD_TENTATIVE",
        "RETESTING",
    }:
        return FieldValue(1)
    if obs.state in {"BREACHED_SHALLOW", "NONE"}:
        return FieldValue(0)
    raise ValueError(f"未注册的支撑观察状态: {obs.state}")


def resolve_structure_rank(
    linked_support_ranks: Sequence[FieldValue],
    *,
    scope_complete: bool,
) -> FieldValue:
    """按 §11.7 解析最高合法结构 rank：4 可支配未知，范围不全按可能有 4 处理。"""
    if type(scope_complete) is not bool:
        raise ValueError("scope_complete必须为bool")
    known_ranks: list[int] = []
    has_unknown = False
    for item in linked_support_ranks:
        if not isinstance(item, FieldValue):
            raise ContractError("结构rank候选必须为FieldValue")
        if item.quality == "OBSERVED":
            if type(item.value) is not int or not 0 <= item.value <= 4:
                raise ValueError("结构rank必须为0..4整数")
            known_ranks.append(item.value)
        elif item.quality == "UNKNOWN_DATA":
            has_unknown = True
        # NOT_APPLICABLE 表示该锚不适用，不参与最高 rank 竞争
    max_known = max(known_ranks) if known_ranks else None
    if max_known == 4:
        return FieldValue(4)
    if not scope_complete:
        return unknown("STRUCTURE_SCOPE_INCOMPLETE")
    if has_unknown:
        return unknown("STRUCTURE_HIGHER_RANK_POSSIBLE")
    return FieldValue(max_known if max_known is not None else 0)


def compute_improvement_age(
    *,
    current_ordinal: int,
    last_improvement_ordinal: int | None,
    has_complete_history: bool,
    is_first_observation: bool,
    unknown_reason: str | None = None,
) -> ImprovementAge:
    """按 §11.7 计算改善年龄，禁止将首观测或历史缺口填为 0/Infinity。"""
    if type(current_ordinal) is not int or current_ordinal < 0:
        raise ValueError("current_ordinal必须为非负整数")
    if unknown_reason is not None:
        return ImprovementAge("UNKNOWN", None, unknown_reason)
    if last_improvement_ordinal is not None:
        if type(last_improvement_ordinal) is not int or last_improvement_ordinal < 0:
            raise ValueError("last_improvement_ordinal必须为非负整数")
        if last_improvement_ordinal > current_ordinal:
            raise ValueError("最近改善会话不能晚于当前会话")
        return ImprovementAge("KNOWN_AGE", current_ordinal - last_improvement_ordinal)
    if has_complete_history or is_first_observation:
        return ImprovementAge(
            "NO_PROVEN_IMPROVEMENT",
            None,
            "FIRST_OBSERVATION_NO_PROVEN_IMPROVEMENT"
            if is_first_observation
            else "NO_PROVEN_IMPROVEMENT_IN_EPISODE",
        )
    return ImprovementAge("UNKNOWN", None, "IMPROVEMENT_HISTORY_INCOMPLETE")


def evaluate_stock_priority(
    *,
    eligibility: FieldValue,
    risk: RiskResult,
    effective_bucket: int | None,
    rps5_delta3_pp: FieldValue,
    rps20: FieldValue,
    linked_support_ranks: Sequence[FieldValue],
    scope_complete: bool,
    improvement_age: ImprovementAge,
    prior20_amount_mean: FieldValue,
    stable_id: str,
    capability_state: str = "READY",
) -> PriorityResult:
    """按 ALG-32 / §10.1 / §11.7 计算股票 Priority V1 状态与排序键。"""
    capability(capability_state)
    if not isinstance(risk, RiskResult):
        raise ContractError("风险输入必须为RiskResult")
    if not isinstance(improvement_age, ImprovementAge):
        raise ContractError("改善年龄必须为ImprovementAge")
    if not isinstance(stable_id, str) or not stable_id:
        raise ValueError("stable_id不能为空")

    reasons: list[str] = []
    eligible = boolean(eligibility)
    if eligible is False:
        reasons.append(eligibility.reason or "ELIGIBILITY_FALSE")
    elif eligible is None:
        reasons.append(eligibility.reason or "ELIGIBILITY_UNKNOWN")

    if effective_bucket is not None:
        if type(effective_bucket) is not int or effective_bucket not in BUCKET_NAMES:
            raise ValueError("effective_bucket必须为0..3或None")
    elif eligible is True:
        reasons.append("EFFECTIVE_BUCKET_UNKNOWN")

    emergence_label, emergence_rank = classify_emergence(rps5_delta3_pp)
    if emergence_rank is None:
        reasons.append(rps5_delta3_pp.reason or "RPS5_DELTA3_PP_UNKNOWN")

    rps20_val = number(rps20)
    if rps20_val is None:
        reasons.append(rps20.reason or "RPS20_UNKNOWN")

    structure_rank = resolve_structure_rank(
        linked_support_ranks, scope_complete=scope_complete
    )
    if structure_rank.quality != "OBSERVED":
        reasons.append(structure_rank.reason or "STRUCTURE_RANK_UNKNOWN")

    risk_rank: int | None = None
    if risk.level.quality == "OBSERVED":
        risk_rank = RISK_RANKS[str(risk.level.value)]
    else:
        reasons.append(risk.level.reason or "RISK_LEVEL_UNKNOWN")

    if improvement_age.sort_key is None:
        reasons.append(improvement_age.reason or "IMPROVEMENT_AGE_UNKNOWN")

    amount_val = number(prior20_amount_mean)
    if amount_val is None:
        reasons.append(prior20_amount_mean.reason or "PRIOR20_AMOUNT_MEAN_UNKNOWN")
    elif amount_val < 0:
        raise ValueError("prior20_amount_mean不能为负数")

    other_keys_ready = (
        effective_bucket is not None
        and emergence_rank is not None
        and rps20_val is not None
        and structure_rank.quality == "OBSERVED"
        and improvement_age.sort_key is not None
        and amount_val is not None
    )
    status = priority_status(eligibility, risk, other_keys_ready=other_keys_ready)

    sort_key: tuple[Any, ...] | None = None
    if status == "RANKABLE":
        delta_val = number(rps5_delta3_pp)
        assert (
            effective_bucket is not None
            and emergence_rank is not None
            and structure_rank.quality == "OBSERVED"
            and delta_val is not None
            and risk_rank is not None
            and improvement_age.sort_key is not None
            and amount_val is not None
        )
        sort_key = (
            effective_bucket,
            -emergence_rank,
            -int(structure_rank.value),
            -delta_val,
            risk_rank,
            improvement_age.sort_key,
            -amount_val,
            stable_id,
        )
        if not reasons:
            reasons.append("ALL_PRIORITY_KEYS_READY")

    return PriorityResult(
        stable_id=stable_id,
        status=status,
        bucket=effective_bucket,
        bucket_label=BUCKET_NAMES.get(effective_bucket)
        if effective_bucket is not None
        else None,
        emergence_label=emergence_label,
        emergence_rank=emergence_rank,
        structure_rank=structure_rank,
        rps5_delta3_pp=rps5_delta3_pp,
        rps20=rps20,
        risk_level=risk.level,
        risk_rank=risk_rank,
        improvement_age=improvement_age,
        prior20_amount_mean=prior20_amount_mean,
        sort_key=sort_key,
        reasons=tuple(dict.fromkeys(reasons)),
    )


def _priority_group_from_status(status: str) -> int:
    if status == "RANKABLE":
        return 0
    if status == "EXCLUDED":
        return 1
    return 2


def _describe_radar_texts(
    event_type: str,
    *,
    current_state: State | None,
    current_risk: RiskResult | None,
    prior_risk: RiskResult | None,
) -> tuple[str, str, str]:
    scenario = current_state.scenario if current_state else "UNKNOWN"
    if event_type == "INVALIDATED":
        return (
            f"当前episode({scenario})触发冻结失效线或关联支撑硬破坏。",
            "终态已失效，等待后续合法新事件重新入组。",
            "已触发硬失效终态。",
        )
    if event_type == "EXPIRED":
        return (
            f"当前episode({scenario})连续无已证改善达到到期上限。",
            "已到期退出，等待新上升沿或新正式事件重入。",
            "已触发到期退出终态。",
        )
    if event_type == "EXITED":
        return (
            f"当前episode({scenario})连续降级达到退出条件。",
            "已正常退出，等待后续新匹配重新入组。",
            "已触发正常退出终态。",
        )
    if event_type == "WEAKENED":
        return (
            f"当前episode({scenario})成熟度确认下调。",
            "等待价格重回均线/支撑或指标恢复改善。",
            "若跌破冻结失效线或继续降级至NONE则退出。",
        )
    if event_type == "UPGRADED":
        return (
            f"当前episode({scenario})成熟度升级至{current_state.display_maturity if current_state else '更高档'}。",
            "观察量价延续及关联支撑确认/接受。",
            "若跌破冻结失效线或关联支撑硬破坏则失效。",
        )
    if event_type == "REENTERED":
        return (
            f"在已证退出后出现新合格信号({scenario})，建立带父谱系的新episode。",
            "等待后续会话量价与结构进一步确认。",
            "若跌破新冻结失效线或关联支撑硬破坏则失效。",
        )
    if event_type == "FIRST_ENROLLED":
        return (
            f"首次由无匹配转为合格({scenario})并建立episode。",
            "等待后续会话推进结构确认与改善验证。",
            "若跌破冻结失效线或持续无改善到期则退出。",
        )
    if event_type == "RISK_WORSENED":
        prev_lvl = prior_risk.level.value if prior_risk else "UNKNOWN"
        curr_lvl = current_risk.level.value if current_risk else "UNKNOWN"
        reason = current_risk.primary_reason if current_risk else None
        return (
            f"同规则可比条件下风险由{prev_lvl}升至{curr_lvl}({reason or '多因叠加'})。",
            "等待价格损伤修复或extension回落。",
            "若触发当前episode失效或关联支撑硬破坏则风险升至EXTREME。",
        )
    if event_type == "RISK_IMPROVED":
        prev_lvl = prior_risk.level.value if prior_risk else "UNKNOWN"
        curr_risk_lvl = current_risk.level.value if current_risk else "UNKNOWN"
        return (
            f"同规则可比条件下风险由{prev_lvl}降至{curr_risk_lvl}。",
            "维持当前结构与风险控制。",
            "若再出现价格损伤、高extension或支撑轻破则风险回升。",
        )
    return ("研究状态发生变化。", "继续按日历会话观察。", "若触发失效条件则终止。")


def sort_radar_events(events: Sequence[RadarEvent]) -> tuple[RadarEvent, ...]:
    """按 §11.7 主展示优先序与同类规则排序 Radar 事件。"""
    order = {name: idx for idx, name in enumerate(RADAR_PRIMARY_PRIORITY)}
    return tuple(
        sorted(
            events,
            key=lambda ev: (
                order.get(ev.event_type, len(RADAR_PRIMARY_PRIORITY)),
                -ev.session_ordinal,
                ev.priority_group,
                ev.priority_sort_key if ev.priority_sort_key is not None else (),
                ev.entity_id,
                ev.episode_id or "",
                ev.event_id or "",
            ),
        )
    )


def generate_stock_radar_events(
    *,
    entity_id: str,
    session_ordinal: int,
    current_state: State | None,
    prior_state: State | None,
    current_primary: FieldValue,
    prior_primary: FieldValue,
    current_risk: RiskResult | None,
    prior_risk: RiskResult | None,
    priority_result: PriorityResult | None = None,
    prior_mode: str = "PREVIOUS_ACCEPTED",
    predicate_evidence: Mapping[str, Any] | None = None,
    capability_state: str = "READY",
) -> RadarEventBatch:
    """对比 t-1 与 t 的 D2 State、主选择、风险与规则版本，生成真实 Radar 事件差。"""
    capability(capability_state)
    if not isinstance(entity_id, str) or not entity_id:
        raise ValueError("entity_id不能为空")
    if type(session_ordinal) is not int or session_ordinal < 0:
        raise ValueError("session_ordinal必须为非负整数")
    if prior_mode not in {"FIRST_OBSERVATION", "PREVIOUS_ACCEPTED", "MODEL_BOUNDARY"}:
        raise ValueError(f"非法的prior_mode: {prior_mode}")

    annotations: list[str] = []
    if prior_mode == "FIRST_OBSERVATION":
        annotations.append("FIRST_OBSERVED")
    if prior_mode == "MODEL_BOUNDARY" or (
        prior_state is not None
        and current_state is not None
        and prior_state.rule_identity != current_state.rule_identity
    ):
        annotations.append("RULE_CHANGED")

    curr_ep = (
        str(current_primary.value)
        if current_primary.quality == "OBSERVED"
        else current_state.episode_id
        if current_state
        else None
    )
    prev_ep = (
        str(prior_primary.value)
        if prior_primary.quality == "OBSERVED"
        else prior_state.episode_id
        if prior_state
        else None
    )
    if (
        "RULE_CHANGED" not in annotations
        and "FIRST_OBSERVED" not in annotations
        and prior_primary.quality == "OBSERVED"
        and current_primary.quality == "OBSERVED"
        and curr_ep != prev_ep
    ):
        annotations.append("SELECTION_CHANGED")

    if (
        "RULE_CHANGED" not in annotations
        and "FIRST_OBSERVED" not in annotations
        and "SELECTION_CHANGED" not in annotations
        and (
            (
                prior_risk is not None
                and current_risk is not None
                and prior_risk.level.quality == "UNKNOWN_DATA"
                and current_risk.level.quality == "OBSERVED"
            )
            or (
                prior_primary.quality == "UNKNOWN_DATA"
                and current_primary.quality == "OBSERVED"
            )
        )
    ):
        annotations.append("BECAME_KNOWN")

    annotation_tuple = tuple(dict.fromkeys(annotations))
    evidence_tuple = tuple(sorted((predicate_evidence or {}).items()))
    p_group = _priority_group_from_status(
        priority_result.status if priority_result is not None else "UNKNOWN"
    )
    p_sort_key = priority_result.sort_key if priority_result is not None else None

    event_types: list[str] = []
    if current_state is not None:
        for tr in current_state.transitions:
            if tr in RADAR_PRIMARY_PRIORITY:
                # 首次左截尾加载不伪装自然启动；仅当非 FIRST_OBSERVATION 或确为合法新事件出生时保留
                if (
                    tr == "FIRST_ENROLLED"
                    and "FIRST_OBSERVED" in annotation_tuple
                    and current_state.kind != "FORMAL"
                ):
                    continue
                event_types.append(tr)

    # 风险变化仅在同规则、非首观测、非换主且可比适用性下比较
    risk_comparable = (
        "FIRST_OBSERVED" not in annotation_tuple
        and "RULE_CHANGED" not in annotation_tuple
        and "SELECTION_CHANGED" not in annotation_tuple
        and prior_risk is not None
        and current_risk is not None
        and prior_risk.level.quality == "OBSERVED"
        and current_risk.level.quality == "OBSERVED"
        and prior_risk.structure_na == current_risk.structure_na
        and prior_risk.support_na == current_risk.support_na
    )
    if risk_comparable:
        assert prior_risk is not None and current_risk is not None
        prev_rank = RISK_RANKS[str(prior_risk.level.value)]
        curr_rank = RISK_RANKS[str(current_risk.level.value)]
        if curr_rank > prev_rank:
            event_types.append("RISK_WORSENED")
        elif curr_rank < prev_rank:
            event_types.append("RISK_IMPROVED")

    events: list[RadarEvent] = []
    for ev_type in dict.fromkeys(event_types):
        why_now, waiting_for, invalid_if = _describe_radar_texts(
            ev_type,
            current_state=current_state,
            current_risk=current_risk,
            prior_risk=prior_risk,
        )
        events.append(
            RadarEvent(
                event_type=ev_type,
                entity_id=entity_id,
                session_ordinal=session_ordinal,
                episode_id=current_state.episode_id if current_state else curr_ep,
                event_id=current_state.event_id if current_state else None,
                why_now=why_now,
                waiting_for=waiting_for,
                invalid_if=invalid_if,
                predicate_evidence=evidence_tuple,
                annotations=annotation_tuple,
                priority_group=p_group,
                priority_sort_key=p_sort_key,
            )
        )

    primary_display_unknown = current_primary.quality == "UNKNOWN_DATA"
    return RadarEventBatch(
        entity_id=entity_id,
        session_ordinal=session_ordinal,
        events=sort_radar_events(events),
        annotations=annotation_tuple,
        primary_display_unknown=primary_display_unknown,
        primary_display_reason=current_primary.reason
        if primary_display_unknown
        else None,
    )


def _soft_tolerance(check: SoftPredicateCheck) -> Fraction | None:
    if check.soft_kind in NEAR_MISS_TOLERANCES:
        return NEAR_MISS_TOLERANCES[check.soft_kind]
    if check.soft_kind == "breakout_price":
        if check.atr_today is None:
            return None
        atr = number(check.atr_today, positive=True)
        if atr is None:
            return None
        return BREAKOUT_ATR_TOLERANCE_FACTOR * atr
    raise ValueError(f"未注册的Near-Miss软谓词类型: {check.soft_kind}")


def _evaluate_soft_predicate(
    check: SoftPredicateCheck,
) -> tuple[bool, Fraction, Fraction, Fraction, Fraction] | None:
    """返回 (passed, actual, threshold, distance_g, tolerance)，任一输入未知返回 None。"""
    tol = _soft_tolerance(check)
    actual_val = number(check.actual)
    threshold_val = number(check.threshold)
    if tol is None or actual_val is None or threshold_val is None:
        return None
    if check.operator == ">=":
        passed = actual_val >= threshold_val
        dist = Fraction(0) if passed else threshold_val - actual_val
    elif check.operator == ">":
        passed = actual_val > threshold_val
        dist = Fraction(0) if passed else threshold_val - actual_val
    elif check.operator == "<=":
        passed = actual_val <= threshold_val
        dist = Fraction(0) if passed else actual_val - threshold_val
    elif check.operator == "<":
        passed = actual_val < threshold_val
        dist = Fraction(0) if passed else actual_val - threshold_val
    else:
        raise ValueError(f"不支持的比较运算符: {check.operator}")
    return passed, actual_val, threshold_val, dist, tol


def evaluate_stock_near_miss(
    paths: Sequence[NearMissPathInput],
    *,
    capability_state: str = "READY",
) -> tuple[NearMissRecord, ...]:
    """按 §11.7 评估具名场景路径 Near-Miss，恰有 1 个软谓词失败且 g<=容差才进入。"""
    capability(capability_state)
    records: list[NearMissRecord] = []
    scenario_order = {name: idx for idx, name in enumerate(SCENARIOS)}

    for path in paths:
        if path.scenario not in scenario_order:
            raise ValueError(f"未注册的D0场景: {path.scenario}")
        if not path.entity_id or not path.path_id:
            raise ValueError("Near-Miss路径缺少entity_id或path_id")

        raw = boolean(path.raw_matched)
        if raw is not False:
            continue

        safety_vals = tuple(boolean(g) for g in path.common_safety_gates)
        hard_vals = tuple(boolean(g) for g in path.hard_gates)
        if any(v is not True for v in safety_vals) or any(
            v is not True for v in hard_vals
        ):
            continue

        # 同一表达在多个地方引用不重复计数
        deduped: list[SoftPredicateCheck] = []
        seen_ids: set[str] = set()
        for sp in path.soft_predicates:
            if sp.predicate_id not in seen_ids:
                seen_ids.add(sp.predicate_id)
                deduped.append(sp)

        if not deduped:
            continue

        incomplete = False
        failed: list[
            tuple[SoftPredicateCheck, Fraction, Fraction, Fraction, Fraction]
        ] = []
        for sp in deduped:
            res = _evaluate_soft_predicate(sp)
            if res is None:
                incomplete = True
                break
            passed, actual_val, threshold_val, dist, tol = res
            if not passed:
                failed.append((sp, actual_val, threshold_val, dist, tol))

        if incomplete or len(failed) != 1:
            continue

        sp, actual_val, threshold_val, dist, tol = failed[0]
        if dist > tol:
            continue

        norm_dist = dist / tol
        delta3_val = number(path.rps5_delta3_pp)
        emerg_rank = path.emergence_rank if path.emergence_rank is not None else -1
        sort_key = (
            norm_dist,
            scenario_order[path.scenario],
            -emerg_rank,
            -(delta3_val if delta3_val is not None else Fraction(-1000)),
            path.entity_id,
            path.parent_episode_id or "",
            path.peak_id or "",
            path.path_id,
        )
        records.append(
            NearMissRecord(
                entity_id=path.entity_id,
                session_ordinal=path.session_ordinal,
                scenario=path.scenario,
                path_id=path.path_id,
                parent_episode_id=path.parent_episode_id,
                peak_id=path.peak_id,
                failed_predicate_id=sp.predicate_id,
                soft_kind=sp.soft_kind,
                actual_value=actual_val,
                threshold_value=threshold_val,
                operator=sp.operator,
                distance=dist,
                tolerance=tol,
                normalized_distance=norm_dist,
                sort_key=sort_key,
            )
        )

    records.sort(key=lambda r: r.sort_key)
    return tuple(records)
