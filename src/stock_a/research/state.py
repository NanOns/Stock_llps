"""P16逐episode纯Reducer；不接收P17风险、Priority或Focus。"""

from dataclasses import asdict, dataclass, replace
from fractions import Fraction

from stock_a.canonical.identity import semantic_digest
from stock_a.domain import ContractError, FieldValue, unknown, exact_number
from stock_a.research.admission import Event, SCENARIOS
from stock_a.research.facts import Fact, boolean, number, enum, completions, capability

CONTRACT = "STOCK_A_D2_REDUCER_R1"
MATURITIES = ("NONE", "SEED", "PREWATCH", "WARM", "CONFIRMED")
EXTENSIONS = ("LOW", "MEDIUM", "HIGH", "EXTREME")
BUCKET = {"CONFIRMED": 0, "WARM": 1, "PREWATCH": 2, "SEED": 3}


@dataclass(frozen=True)
class Rules:
    version: str
    downgrade_sessions: int
    expiry_sessions: int
    improvement_pp: object
    health_deadband_pp: object

    def __post_init__(self):
        if not isinstance(self.version, str) or not self.version:
            raise ContractError("状态规则缺少版本")
        if any(
            type(x) is not int or x <= 0
            for x in (self.downgrade_sessions, self.expiry_sessions)
        ):
            raise ContractError("会话阈值必须为正整数")
        if (
            exact_number(self.improvement_pp) <= 0
            or exact_number(self.health_deadband_pp) < 0
        ):
            raise ContractError("改善/健康阈值非法")
        if self.version == CONTRACT and (
            self.downgrade_sessions,
            self.expiry_sessions,
            self.improvement_pp,
            self.health_deadband_pp,
        ) != (2, 10, 3, 3):
            raise ContractError("不能同版本改状态参数")

    @property
    def identity(self):
        return semantic_digest(asdict(self))


@dataclass(frozen=True)
class Day:
    entity_id: str
    entity_type: str
    session: object
    ordinal: int
    calendar_id: str
    coordinate_id: str
    input_ref: str
    qualifications: tuple[tuple[str, FieldValue], ...]
    actual: FieldValue
    damage: FieldValue
    extension: FieldValue
    metric: FieldValue  # STOCK=rps5_delta3_pp；SECTOR=dq5
    frozen_failure: Fact
    linked_hard_breach: Fact
    followup_complete: FieldValue
    prior_mode: str  # FIRST_OBSERVATION / PREVIOUS_ACCEPTED / MODEL_BOUNDARY
    new_event: Event | None = None
    exit_in_lineage_today: bool = False  # P16第一遍旧episode推进产生，不传回P14


@dataclass(frozen=True)
class State:
    entity_id: str
    entity_type: str
    ordinal: int
    calendar_id: str
    rule_identity: str
    episode_id: str | None
    event_id: str | None
    scenario: str
    kind: str
    birth_ordinal: int | None
    parent_episode_id: str | None
    event: Event | None
    exact_maturity: str | None
    lower_bound_maturity: str
    display_maturity: str
    effective_eligibility: FieldValue
    effective_bucket: int | None
    validity: FieldValue
    freshness: str
    health: FieldValue
    tracking: str
    downgrade_candidate: str | None
    downgrade_count: int
    expiry_count: int
    improvement_baseline: object
    exit_ordinal: int | None
    exit_reason: str | None
    reentry_armed: bool
    transitions: tuple[str, ...]
    preserved_episode_ids: tuple[str, ...]
    input_ref: str

    @property
    def state_id(self):
        return semantic_digest(asdict(self))


def select_maturity(qualifications, entity_type):
    if entity_type not in {"STOCK", "SECTOR"}:
        raise ContractError("成熟度实体类型未注册")
    required = MATURITIES[1:] if entity_type == "STOCK" else MATURITIES[2:]
    pairs = tuple(qualifications)
    if len(pairs) != len(required) or set(k for k, _ in pairs) != set(required):
        raise ContractError("原始资格缺项/重复/实体类型错误")
    truth = {k: boolean(v) for k, v in pairs}
    lower = max((MATURITIES.index(k) for k, v in truth.items() if v is True), default=0)
    upper = max(
        (MATURITIES.index(k) for k, v in truth.items() if v is not False), default=0
    )
    return MATURITIES[lower] if lower == upper else None, MATURITIES[lower]


def health(damage, extension, metric, rules):
    p, e, m = boolean(damage), enum(extension, EXTENSIONS), number(metric)
    if m is not None and not -100 <= m <= 100:
        raise ValueError("健康指标必须为[-100,100]pp")
    possible = set()
    deadband = exact_number(rules.health_deadband_pp)
    samples = (-deadband - 1, Fraction(0), deadband + 1)
    for price_damage, ext, value in completions(
        (p, e, m), ((False, True), EXTENSIONS, samples)
    ):
        possible.add(
            "DAMAGED"
            if price_damage
            else "EXHAUSTED"
            if ext == "EXTREME"
            else "IMPROVING"
            if value > deadband
            else "WEAKENING"
            if value < -deadband
            else "STABLE"
        )
    return (
        FieldValue(next(iter(possible)))
        if len(possible) == 1
        else unknown("HEALTH_COMPLETION_AMBIGUOUS")
    )


def _base(day, rules, prior, exact, lower, h):
    return State(
        day.entity_id,
        day.entity_type,
        day.ordinal,
        day.calendar_id,
        rules.identity,
        prior.episode_id if prior else None,
        prior.event_id if prior else None,
        prior.scenario if prior else "NONE",
        prior.kind if prior else "NONE",
        prior.birth_ordinal if prior else None,
        prior.parent_episode_id if prior else None,
        prior.event if prior else None,
        exact,
        lower,
        prior.display_maturity if prior else "NONE",
        unknown("TODAY_NOT_EVALUATED"),
        None,
        unknown("VALIDITY_NOT_EVALUATED"),
        "STALE",
        h,
        "PAUSED" if prior and prior.episode_id else "CLOSED",
        None,
        0,
        prior.expiry_count if prior else 0,
        prior.improvement_baseline if prior else None,
        prior.exit_ordinal if prior else None,
        prior.exit_reason if prior else None,
        prior.reentry_armed if prior else False,
        (),
        prior.preserved_episode_ids if prior else (),
        day.input_ref,
    )


def reduce_episode(
    prior: State | None, day: Day, *, rules: Rules, capability_state: str
):
    capability(capability_state)
    from datetime import date

    if (
        day.entity_type not in {"STOCK", "SECTOR"}
        or type(day.ordinal) is not int
        or day.ordinal < 0
        or type(day.session) is not date
        or not all((day.entity_id, day.calendar_id, day.coordinate_id, day.input_ref))
        or type(day.exit_in_lineage_today) is not bool
    ):
        raise ValueError("状态实体/日历/输入身份非法")
    if day.prior_mode not in {
        "FIRST_OBSERVATION",
        "PREVIOUS_ACCEPTED",
        "MODEL_BOUNDARY",
    }:
        raise ContractError("前态角色未绑定")
    if (prior is None) != (day.prior_mode == "FIRST_OBSERVATION"):
        raise ContractError("前态缺失不能冒充首观测或模型边界")
    if prior and (
        prior.entity_id != day.entity_id
        or prior.entity_type != day.entity_type
        or prior.ordinal != day.ordinal - 1
        or prior.calendar_id != day.calendar_id
    ):
        raise ValueError(
            "必须使用同实体同日历上一市场会话前态；同日revision也不得读自己"
        )
    if prior:
        allowed = (
            MATURITIES
            if day.entity_type == "STOCK"
            else ("NONE", "PREWATCH", "WARM", "CONFIRMED")
        )
        if (
            prior.display_maturity not in allowed
            or prior.lower_bound_maturity not in allowed
            or prior.exact_maturity not in (*allowed, None)
            or any(
                type(x) is not int or x < 0
                for x in (prior.downgrade_count, prior.expiry_count)
            )
            or (prior.exit_ordinal is not None and prior.exit_ordinal > prior.ordinal)
            or (prior.birth_ordinal is not None and prior.birth_ordinal > prior.ordinal)
        ):
            raise ValueError("前态枚举/计数/时间非法")
        boolean(prior.effective_eligibility)
        enum(prior.validity, ("VALID", "INVALIDATED"))
        if prior.event is not None and (
            prior.event.event_id != prior.event_id
            or prior.event.entity_id != day.entity_id
            or prior.event.scenario != prior.scenario
            or prior.event.failure_line.effective_ordinal
            != prior.event.session_ordinal + 1
        ):
            raise ValueError("前态事件/冻结线引用不一致")
    boundary = day.prior_mode == "MODEL_BOUNDARY"
    if prior and prior.rule_identity != rules.identity and not boundary:
        raise ContractError("规则改变必须显式MODEL_BOUNDARY")
    exact, lower = select_maturity(day.qualifications, day.entity_type)
    if (
        day.new_event is not None
        and boolean(dict(day.qualifications)["CONFIRMED"]) is not True
    ):
        raise ContractError("已准入新正式事件必须具有对应CONFIRMED原始资格")
    actual, metric = boolean(day.actual), number(day.metric)
    if day.entity_type == "STOCK":
        damage, ext = boolean(day.damage), enum(day.extension, EXTENSIONS)
        h = health(day.damage, day.extension, day.metric, rules)
    else:
        if (
            day.damage.quality != "NOT_APPLICABLE"
            or day.extension.quality != "NOT_APPLICABLE"
        ):
            raise ValueError("板块D2不能借股票损伤/extension作安全或健康门")
        damage, ext = False, "LOW"  # 不适用的股票门不参与板块资格；原NA保留在输入引用
        if metric is not None and not -100 <= metric <= 100:
            raise ValueError("板块dq5必须为[-100,100]pp")
        deadband = exact_number(rules.health_deadband_pp)
        h = (
            unknown("SECTOR_HEALTH_METRIC_UNKNOWN")
            if metric is None
            else FieldValue(
                "IMPROVING"
                if metric > deadband
                else "WEAKENING"
                if metric < -deadband
                else "STABLE"
            )
        )
    followup = boolean(day.followup_complete)
    for fact in (day.frozen_failure, day.linked_hard_breach):
        fact.validate(day.entity_id, day.session, day.coordinate_id)
    if day.new_event is not None:
        event = day.new_event
        if (
            day.entity_type != "STOCK"
            or event.entity_id != day.entity_id
            or event.session_ordinal != day.ordinal
            or event.scenario not in SCENARIOS
            or event.failure_line.effective_ordinal != day.ordinal + 1
            or event.failure_line.coordinate_id != day.coordinate_id
            or exact_number(event.failure_line.value) <= 0
        ):
            raise ValueError("新事件不属于当前实体/会话/坐标/冻结合同")
        if event.scenario in {"STRONG_PULLBACK", "TREND_CONTINUE"} and (
            prior is None or event.prior_episode_id != prior.episode_id
        ):
            raise ValueError("延续/回踩事件必须关联传入的精确前episode")
    original = prior
    preserved = prior.preserved_episode_ids if prior else ()
    if boundary:
        preserved = tuple(
            dict.fromkeys(
                (*preserved, *((prior.episode_id,) if prior.episode_id else ()))
            )
        )
        prior = None
    state = _base(day, rules, prior, exact, lower, h)
    state = replace(state, preserved_episode_ids=preserved)
    if boundary:
        state = replace(state, parent_episode_id=original.episode_id)
    events = ["RULE_CHANGED"] if boundary else []
    if original is None:
        events.append("FIRST_OBSERVED")
    # 原终态不可由后来反包复活；新事件/早期上升沿须创建child。
    terminal = prior is not None and prior.exit_ordinal is not None
    if prior and prior.episode_id and not terminal:
        for fact in (day.frozen_failure, day.linked_hard_breach):
            if fact.episode_id != prior.episode_id:
                raise ValueError("失效事实串到其它episode")
        if prior.event is None:
            if day.frozen_failure.field.quality != "NOT_APPLICABLE":
                raise ValueError("无冻结线早期episode的失效线项必须明确NA")
            line_fail = False
        else:
            line_fail = boolean(day.frozen_failure.field)
        if prior.event is None or prior.event.linked_anchor_id is None:
            if day.linked_hard_breach.field.quality != "NOT_APPLICABLE":
                raise ValueError("无关联锚的破坏项必须明确NA")
            anchor_fail = False
        else:
            anchor_fail = boolean(day.linked_hard_breach.field)
        if line_fail is True or anchor_fail is True:
            return replace(
                state,
                display_maturity="NONE",
                effective_eligibility=FieldValue(False),
                validity=FieldValue("INVALIDATED"),
                freshness="FRESH",
                health=FieldValue("DAMAGED"),
                tracking="CLOSED" if followup is True else "FOLLOWUP",
                exit_ordinal=day.ordinal,
                exit_reason="INVALIDATED",
                transitions=tuple(events + ["INVALIDATED"]),
                reentry_armed=False,
            )
        invalidation_unknown = line_fail is None or anchor_fail is None
    else:
        invalidation_unknown = False
    if terminal:
        state = replace(
            state,
            validity=prior.validity,
            display_maturity="NONE",
            freshness="FRESH",
            effective_eligibility=FieldValue(False),
            tracking="CLOSED" if followup is True else "FOLLOWUP",
        )
        if actual is True and exact == "NONE":
            state = replace(state, reentry_armed=True)
        can_enter = (
            actual is True
            and exact not in (None, "NONE")
            and damage is False
            and ext != "EXTREME"
            and ext is not None
        )
        fresh_formal = (
            day.new_event is not None and day.new_event.event_id != prior.event_id
        )
        fresh_core = prior.reentry_armed and exact in {"SEED", "PREWATCH", "WARM"}
        if (
            not can_enter
            or not (fresh_formal or fresh_core)
            or day.exit_in_lineage_today
        ):
            return replace(state, transitions=tuple(events + ["TERMINAL_PRESERVED"]))
        events.append("REENTERED")
        state = replace(
            state,
            preserved_episode_ids=tuple(dict.fromkeys((*preserved, prior.episode_id))),
            parent_episode_id=prior.episode_id,
            episode_id=None,
            event_id=None,
            event=None,
            exit_ordinal=None,
            exit_reason=None,
            expiry_count=0,
            improvement_baseline=None,
        )
        prior = None
    if actual is not True or exact is None or invalidation_unknown:
        return replace(
            state,
            validity=unknown("VALIDITY_INPUT_UNKNOWN"),
            freshness="STALE",
            transitions=tuple(events + ["STALE"]),
        )
    safety = (
        False
        if damage is True or ext == "EXTREME"
        else None
        if damage is None or ext is None
        else True
    )
    if exact == "NONE":
        eligibility, candidate = False, "NONE"
    elif safety is None:
        return replace(
            state,
            validity=unknown("VALIDITY_INPUT_UNKNOWN"),
            transitions=tuple(events + ["STALE"]),
        )
    else:
        eligibility, candidate = safety, exact if safety else "NONE"
    state = replace(
        state,
        effective_eligibility=FieldValue(eligibility),
        effective_bucket=BUCKET.get(candidate) if eligibility else None,
        freshness="FRESH",
        validity=FieldValue("VALID"),
    )
    if (
        eligibility
        and day.new_event is not None
        and state.episode_id is not None
        and day.new_event.event_id != state.event_id
    ):
        if day.exit_in_lineage_today:
            return replace(
                state,
                effective_eligibility=FieldValue(False),
                effective_bucket=None,
                transitions=tuple(events + ["EVENT_APPLICATION_BLOCKED_EXIT_SESSION"]),
            )
        # 每个正式事件独立出生；保留旧episode供编排器独立推进/Forward，不能替换旧线。
        state = replace(
            state,
            parent_episode_id=state.episode_id,
            preserved_episode_ids=tuple(
                dict.fromkeys((*state.preserved_episode_ids, state.episode_id))
            ),
            episode_id=None,
            event_id=None,
            event=None,
            display_maturity="NONE",
            expiry_count=0,
            improvement_baseline=None,
            downgrade_count=0,
            downgrade_candidate=None,
        )
        prior = None
        events.append("FORMAL_EVENT_FORKED")
    if state.episode_id is None and eligibility:
        if day.exit_in_lineage_today:
            return replace(
                state,
                effective_eligibility=FieldValue(False),
                effective_bucket=None,
                transitions=tuple(events + ["EVENT_APPLICATION_BLOCKED_EXIT_SESSION"]),
            )
        if (
            day.entity_type == "STOCK"
            and candidate == "CONFIRMED"
            and day.new_event is None
        ):
            raise ContractError("新正式确认必须绑定已准入事件")
        source_event = day.new_event
        event_id = (
            source_event.event_id
            if source_event
            else semantic_digest(
                ("CORE_ENTRY", day.entity_id, day.ordinal, day.input_ref)
            )
        )
        episode_id = semantic_digest(
            (CONTRACT, rules.identity, day.entity_id, event_id, state.parent_episode_id)
        )
        state = replace(
            state,
            episode_id=episode_id,
            event_id=event_id,
            event=source_event,
            kind="FORMAL" if source_event else "CORE",
            scenario=source_event.scenario if source_event else "CORE_EARLY",
            birth_ordinal=day.ordinal,
            display_maturity=candidate,
            expiry_count=0,
            improvement_baseline=metric,
            tracking="ACTIVE",
        )
        if "REENTERED" not in events:
            events.append(
                "FIRST_ENROLLED" if not boundary else "MODEL_BOUNDARY_ENROLLED"
            )
    if (
        day.entity_type == "STOCK"
        and eligibility
        and candidate == "CONFIRMED"
        and state.event is None
    ):
        raise ContractError(
            "Core早期episode不能借其它事件升为正式确认；应应用该事件创建独立child"
        )
    old = prior.display_maturity if prior else state.display_maturity
    old_rank, rank = MATURITIES.index(old), MATURITIES.index(candidate)
    display = candidate
    if state.episode_id and rank < old_rank:
        count = 1
        if (
            prior
            and prior.freshness == "FRESH"
            and prior.downgrade_candidate is not None
        ):
            if rank <= MATURITIES.index(prior.downgrade_candidate):
                count = prior.downgrade_count + 1
        state = replace(state, downgrade_candidate=candidate, downgrade_count=count)
        if count < rules.downgrade_sessions:
            display = old
            events.append("HYSTERESIS_HOLD")
        else:
            events.append("WEAKENED")
    elif state.episode_id and rank > old_rank:
        events.append("UPGRADED")
    state = replace(state, display_maturity=display)
    if state.episode_id and display in {"SEED", "PREWATCH"} and eligibility:
        entered = (
            prior is None
            or prior.display_maturity not in {"SEED", "PREWATCH"}
            or rank > old_rank
        )
        if metric is None:
            return replace(
                state,
                effective_eligibility=unknown("EXPIRY_IMPROVEMENT_UNKNOWN"),
                effective_bucket=None,
                validity=unknown("VALIDITY_INPUT_UNKNOWN"),
                freshness="STALE",
                tracking="PAUSED",
                downgrade_count=0,
                downgrade_candidate=None,
                transitions=tuple(events + ["EXPIRY_PAUSED_UNKNOWN"]),
            )
        baseline = prior.improvement_baseline if prior else None
        improved = baseline is not None and metric - exact_number(
            baseline
        ) >= exact_number(rules.improvement_pp)
        if entered or baseline is None or improved:
            state = replace(state, expiry_count=0, improvement_baseline=metric)
            if improved:
                events.append("IMPROVEMENT_RESET")
        else:
            state = replace(
                state, expiry_count=(prior.expiry_count if prior else 0) + 1
            )
        if state.expiry_count >= rules.expiry_sessions:
            return replace(
                state,
                display_maturity="NONE",
                effective_eligibility=FieldValue(False),
                effective_bucket=None,
                exit_ordinal=day.ordinal,
                exit_reason="EXPIRED",
                reentry_armed=False,
                tracking="CLOSED" if followup is True else "FOLLOWUP",
                transitions=tuple(events + ["EXPIRED"]),
            )
    elif display not in {"SEED", "PREWATCH"}:
        state = replace(state, expiry_count=0, improvement_baseline=None)
    if state.episode_id and display == "NONE":
        return replace(
            state,
            effective_eligibility=FieldValue(False),
            effective_bucket=None,
            exit_ordinal=day.ordinal,
            exit_reason="EXITED",
            reentry_armed=False,
            tracking="CLOSED" if followup is True else "FOLLOWUP",
            transitions=tuple(events + ["EXITED"]),
        )
    state = replace(
        state,
        tracking="ACTIVE"
        if state.episode_id and eligibility
        else "PAUSED"
        if state.episode_id
        else "CLOSED",
    )
    if state.episode_id is None:
        state = replace(
            state, validity=FieldValue(None, "NOT_APPLICABLE", "NO_EPISODE")
        )
    return replace(state, transitions=tuple(events or ["UNCHANGED"]))


def primary_episode(states):
    """唯一证券投影：按今日证据选择，不用显示hold或任意旧坏锚。"""
    rows = tuple(states)
    if not rows:
        return FieldValue(None, "NOT_APPLICABLE", "NO_APPLICABLE_EPISODE")
    if (
        len(
            {
                (s.entity_id, s.entity_type, s.ordinal, s.calendar_id, s.rule_identity)
                for s in rows
            }
        )
        != 1
    ):
        raise ValueError("主episode候选跨实体/日期/日历/规则")
    ids = [s.episode_id for s in rows if s.episode_id is not None]
    if len(ids) != len(set(ids)):
        raise ValueError("主episode候选重复")
    eligible, uncertain = [], []
    for s in rows:
        if s.episode_id is None:
            if s.effective_eligibility.quality == "UNKNOWN_DATA":
                uncertain.append(((-1, -1, -s.ordinal, ""), s))
            continue
        if s.exit_ordinal is not None or s.effective_eligibility.value is False:
            continue
        kind = (
            0
            if s.kind == "FORMAL" and s.birth_ordinal == s.ordinal
            else 1
            if s.kind == "FORMAL"
            else 2
        )
        scenario_rank = (
            SCENARIOS.index(s.scenario) if s.scenario in SCENARIOS else len(SCENARIOS)
        )
        key = (kind, scenario_rank, -s.birth_ordinal, s.event_id)
        (
            eligible
            if s.effective_eligibility.value is True and s.freshness == "FRESH"
            else uncertain
        ).append((key, s))
    eligible.sort(key=lambda row: row[0])
    if not eligible:
        return (
            unknown("PRIMARY_SELECTION_UNKNOWN")
            if uncertain
            else FieldValue(None, "NOT_APPLICABLE", "NO_APPLICABLE_EPISODE")
        )
    if any(key < eligible[0][0] for key, _ in uncertain):
        return unknown("HIGHER_PRIMARY_CANDIDATE_UNKNOWN")
    return FieldValue(eligible[0][1].episode_id)
