"""P17股票单体研究风险；有限域归并，不反向影响P16。"""

from dataclasses import dataclass

from stock_a.domain import FieldValue
from stock_a.research.facts import Fact, boolean, enum, completions, capability

CONTRACT = "STOCK_A_STOCK_RISK_COMPOSITION_R1"
LEVELS = ("LOW", "MEDIUM", "HIGH", "EXTREME")


@dataclass(frozen=True)
class RiskResult:
    level: FieldValue
    possible_levels: tuple[str, ...]
    lower_bound: str
    upper_bound: str
    primary_reason: str | None
    primary_reason_candidates: tuple[str, ...]
    confirmed_reasons: tuple[str, ...]
    possible_reasons: tuple[str, ...]
    structure_na: bool
    support_na: bool
    known_inputs: int
    applicable_inputs: int
    contract: str = CONTRACT


def _rules(p, e, i, w):
    checks = (
        (i, "EXTREME", "CURRENT_EPISODE_INVALIDATED"),
        (e == "EXTREME", "EXTREME", "EXTREME_EXTENSION"),
        (p and (e == "HIGH" or w), "EXTREME", "DAMAGE_WITH_SECOND_WARNING"),
        (e == "HIGH" and w, "EXTREME", "EXTENSION_WITH_SOFT_BREACH"),
        (p, "HIGH", "CORE_PRICE_DAMAGE"),
        (e == "HIGH", "HIGH", "HIGH_EXTENSION"),
        (e == "MEDIUM" and w, "HIGH", "MULTIPLE_MODERATE_WARNINGS"),
        (e == "MEDIUM" or w, "MEDIUM", "SINGLE_MODERATE_WARNING"),
    )
    matched = tuple(reason for passed, _, reason in checks if passed)
    for passed, level, reason in checks:
        if passed:
            return level, reason, matched
    return "LOW", "NO_REGISTERED_WARNING", ("NO_REGISTERED_WARNING",)


def compose(
    *,
    entity_id,
    session,
    coordinate_id,
    selected_episode_id,
    structure_applicability,
    support_applicability,
    damage: Fact,
    extension: Fact,
    invalidated: Fact,
    shallow_breach: Fact,
    capability_state: str,
) -> RiskResult:
    capability(capability_state)
    facts = (damage, extension, invalidated, shallow_breach)
    for fact in facts:
        if not isinstance(fact, Fact):
            raise ValueError("风险输入缺少事实绑定")
        fact.validate(entity_id, session, coordinate_id)
    if structure_applicability not in {"ABSENT", "LINKED", "UNKNOWN"}:
        raise ValueError("结构适用性非法")
    if support_applicability not in {"ABSENT", "LINKED", "UNKNOWN"}:
        raise ValueError("关联support适用性非法")
    if structure_applicability == "LINKED":
        if not selected_episode_id or any(
            f.episode_id != selected_episode_id for f in facts[2:]
        ):
            raise ValueError("结构事实不属于所选episode")
        i = boolean(invalidated.field)
        if support_applicability == "ABSENT":
            if shallow_breach.field.quality != "NOT_APPLICABLE":
                raise ValueError("已证无关联support必须明确NA")
            w = False
        elif support_applicability == "UNKNOWN":
            if shallow_breach.field.quality != "UNKNOWN_DATA":
                raise ValueError("关联support适用性未定必须UNKNOWN")
            w = None
        else:
            w = boolean(shallow_breach.field)
    else:
        if selected_episode_id is not None or any(
            f.episode_id is not None for f in facts[2:]
        ):
            raise ValueError("无已选episode时不能绑定其它episode事实")
        expected = (
            "NOT_APPLICABLE" if structure_applicability == "ABSENT" else "UNKNOWN_DATA"
        )
        if support_applicability != structure_applicability:
            raise ValueError("未选定episode不能自证关联support适用性")
        if any(f.field.quality != expected for f in facts[2:]):
            raise ValueError("结构适用性与NA/UNKNOWN不一致")
        i, w = (False, False) if expected == "NOT_APPLICABLE" else (None, None)
    if any(f.field.quality == "NOT_APPLICABLE" for f in facts[:2]):
        raise ValueError("价格损伤与extension是必需适用输入")
    values = boolean(damage.field), enum(extension.field, LEVELS), i, w
    results = tuple(
        _rules(*row)
        for row in completions(
            values, ((False, True), LEVELS, (False, True), (False, True))
        )
    )
    levels = tuple(x for x in LEVELS if any(row[0] == x for row in results))
    primary = tuple(dict.fromkeys(row[1] for row in results))
    all_sets = [set(row[2]) for row in results]
    confirmed = tuple(sorted(set.intersection(*all_sets)))
    possible = tuple(sorted(set.union(*all_sets)))
    applicable = (
        2
        if structure_applicability == "ABSENT"
        else 3
        if support_applicability == "ABSENT"
        else 4
    )
    known = sum(f.field.quality == "OBSERVED" for f in facts)
    field = (
        FieldValue(levels[0])
        if len(levels) == 1
        else FieldValue(None, "UNKNOWN_DATA", "RISK_COMPLETION_AMBIGUOUS")
    )
    return RiskResult(
        field,
        levels,
        levels[0],
        levels[-1],
        primary[0] if len(primary) == 1 else None,
        primary,
        confirmed,
        possible,
        structure_applicability == "ABSENT",
        support_applicability == "ABSENT",
        known,
        applicable,
    )


def priority_status(
    eligibility: FieldValue, risk: RiskResult, *, other_keys_ready: bool
):
    if type(other_keys_ready) is not bool:
        raise ValueError("排序完整性门必须为bool")
    eligible = boolean(eligibility)
    if eligible is False:
        return "EXCLUDED"
    if eligible is None or risk.level.quality != "OBSERVED" or not other_keys_ready:
        return "UNKNOWN"
    return "RANKABLE"
