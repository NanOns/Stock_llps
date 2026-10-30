"""P14正式事件准入；同日退出应用门留在P16，不回写原始场景。"""

from dataclasses import dataclass

from stock_a.canonical.identity import semantic_digest
from stock_a.domain import FieldValue
from stock_a.research.facts import boolean, number, capability

CONTRACT = "STOCK_A_D0_EVENT_ADMISSION_R1"
SCENARIOS = ("LAUNCH_CONFIRM", "STRONG_PULLBACK", "RECOVERY_TURN", "TREND_CONTINUE")


@dataclass(frozen=True)
class FrozenLine:
    value: object
    coordinate_id: str
    source_ref: str
    source_window: str
    effective_ordinal: int


@dataclass(frozen=True)
class Event:
    event_id: str
    entity_id: str
    session_ordinal: int
    scenario: str
    failure_line: FrozenLine
    linked_anchor_id: str | None
    prior_episode_id: str | None
    evidence_ref: str


@dataclass(frozen=True)
class Admission:
    raw: FieldValue
    formal: FieldValue | str
    status: str
    reason: str
    event: Event | None


def admit(
    *,
    entity_id,
    ordinal,
    next_ordinal,
    coordinate_id,
    scenario,
    role,
    raw: FieldValue,
    gates: tuple[FieldValue, ...],
    close: FieldValue,
    phc20: FieldValue,
    ma5: FieldValue,
    ma20: FieldValue,
    r5: FieldValue,
    r20: FieldValue,
    prior_episode_id,
    prior_episode_valid: FieldValue,
    support_anchor_id,
    support_lower: FieldValue,
    support_valid: FieldValue,
    evidence_ref,
    window_ref,
    capability_state,
):
    capability(capability_state)
    if scenario not in SCENARIOS or role not in {"FORMAL", "DIAGNOSTIC"}:
        raise ValueError("场景或角色未注册")
    if (
        type(ordinal) is not int
        or ordinal < 0
        or type(next_ordinal) is not int
        or next_ordinal != ordinal + 1
        or not all((entity_id, coordinate_id, evidence_ref, window_ref))
    ):
        raise ValueError("缺少精确业务/官方日历绑定")
    truth = boolean(raw)
    if role == "DIAGNOSTIC":
        return Admission(
            raw, "NOT_ENABLED_IN_SCOPE", "DIAGNOSTIC", "ROLE_DIAGNOSTIC", None
        )
    if truth is False:
        return Admission(raw, FieldValue(False), "NOT_REQUIRED", "RAW_FALSE", None)
    unknown = lambda reason: Admission(
        raw, FieldValue(None, "UNKNOWN_DATA", reason), "NOT_READY", reason, None
    )
    if truth is None:
        return unknown("RAW_UNKNOWN")
    gate_values = tuple(boolean(g) for g in gates)
    if not gate_values or any(g is not True for g in gate_values):
        return unknown("COMMON_ADMISSION_NOT_READY")
    p = number(close, positive=True)
    line, anchor = None, None
    source = None
    if scenario == "LAUNCH_CONFIRM":
        line, source = number(phc20, positive=True), "T0_PHC20"
    elif scenario == "RECOVERY_TURN":
        t20, t5 = boolean(r20), boolean(r5)
        if t20 is None:
            return unknown("R20_PRIORITY_UNRESOLVED")
        if t20:
            line, source = number(ma20, positive=True), "T0_MA20"
        elif t5 is True:
            line, source = number(ma5, positive=True), "T0_MA5"
        else:
            return unknown("RECOVERY_TRIGGER_NOT_READY")
    else:
        if not prior_episode_id or boolean(prior_episode_valid) is not True:
            return unknown("PRIOR_EPISODE_REQUIRED")
        if support_anchor_id:
            if boolean(support_valid) is not True:
                return unknown("PRIOR_SUPPORT_NOT_READY")
            line, anchor, source = (
                number(support_lower, positive=True),
                support_anchor_id,
                "PRIOR_SUPPORT_LOWER",
            )
        elif scenario == "TREND_CONTINUE" and support_valid.quality == "NOT_APPLICABLE":
            line, source = number(ma20, positive=True), "T0_MA20_FALLBACK"
        else:
            return unknown("PRIOR_SUPPORT_REQUIRED_OR_ABSENCE_UNPROVEN")
    if p is None or line is None:
        return unknown("FAILURE_LINE_INPUT_UNKNOWN")
    if p <= line:
        return unknown("BIRTH_NOT_ABOVE_FAILURE_LINE")
    frozen = FrozenLine(line, coordinate_id, source, window_ref, next_ordinal)
    identity = semantic_digest(
        (
            CONTRACT,
            entity_id,
            ordinal,
            scenario,
            line,
            coordinate_id,
            source,
            window_ref,
            anchor,
            prior_episode_id,
            evidence_ref,
        )
    )
    event = Event(
        identity,
        entity_id,
        ordinal,
        scenario,
        frozen,
        anchor,
        prior_episode_id,
        evidence_ref,
    )
    return Admission(raw, FieldValue(True), "READY", "ADMITTED", event)


def line_breached(
    close: FieldValue,
    line_in_today_coordinate: FieldValue,
    *,
    ordinal,
    effective_ordinal,
):
    if (
        type(ordinal) is not int
        or type(effective_ordinal) is not int
        or ordinal < 0
        or effective_ordinal < 0
    ):
        raise ValueError("失效比较缺少官方会话序号")
    if ordinal < effective_ordinal:
        return FieldValue(False)
    p = number(close, positive=True)
    level = number(line_in_today_coordinate, positive=True)
    if p is None or level is None:
        return FieldValue(None, "UNKNOWN_DATA", "FAILURE_LINE_OR_CLOSE_UNKNOWN")
    return FieldValue(p <= level)
