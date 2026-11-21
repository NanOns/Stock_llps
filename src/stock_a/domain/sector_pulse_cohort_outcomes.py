"""按T0 PULSE三态与已存相邻变化汇总固定篮后继结果。"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date
from fractions import Fraction
from typing import Any, Mapping, Sequence


_HORIZONS = (1, 3, 5)
_KINDS = ("INDUSTRY", "CONCEPT")
_PULSE_STATES = ("TRUE", "FALSE", "UNKNOWN")
_TRANSITION_STATES = (
    "PULSE_APPEARED",
    "PULSE_PERSISTED",
    "PULSE_CEASED",
    "PULSE_NOT_PRESENT_BOTH",
    "PULSE_TRANSITION_UNKNOWN",
    "NO_STORED_TRANSITION",
)
_OUTCOME_STATES = (
    "OBSERVED",
    "UNKNOWN",
    "NOT_COMPUTED",
    "NOT_DUE",
    "INCOMPLETE",
)
_TRANSITION_CONTRACT = "SOURCE_CROSS_PERIOD_PULSE_TRANSITION_R1"


def _reason_codes(value: Any) -> tuple[str, ...]:
    if not isinstance(value, (tuple, list)):
        return ()
    return tuple(sorted({item for item in value if isinstance(item, str) and item}))


def _transition_for_pulse(
    pulse: Mapping[str, Any],
    rows_by_target: Mapping[int, Sequence[Mapping[str, Any]]],
    *,
    calendar_id: str,
    calendar_revision: str,
) -> tuple[str, tuple[str, ...]]:
    pulse_id = pulse.get("id")
    if type(pulse_id) is not int or pulse_id <= 0:
        raise ValueError("T0 PULSE缺少有效数据库身份")
    rows = rows_by_target.get(pulse_id, ())
    if not rows:
        return "NO_STORED_TRANSITION", ("NO_STORED_TRANSITION_PARENT",)
    if len(rows) != 1:
        raise ValueError("相邻PULSE父事实重复，不能形成唯一转移组")
    row = rows[0]
    state = row.get("transition_state")
    if (
        row.get("target_pulse_id") != pulse_id
        or row.get("transition_contract") != _TRANSITION_CONTRACT
        or row.get("calculation_status") != "COMPLETE"
        or row.get("research_mode") != "NON_PIT_RETROSPECTIVE"
        or row.get("price_coordinate") != "RAW_UNADJUSTED"
        or row.get("formal_rotation_state") != "NOT_EVALUATED"
        or row.get("calendar_id") != calendar_id
        or row.get("calendar_revision") != calendar_revision
        or state not in _TRANSITION_STATES[:-1]
    ):
        raise ValueError("相邻PULSE事实版本或质量与当前T0坐标不一致")
    reasons = _reason_codes(row.get("previous_reason_codes")) + _reason_codes(
        row.get("target_reason_codes")
    )
    return state, tuple(sorted(set(reasons)))


def _outcome_for_board(
    item: Mapping[str, Any] | None,
    *,
    due: bool,
    partition_closed: bool,
    expected_member_count: int,
) -> tuple[str, Fraction | None, int, int, tuple[str, ...]]:
    if not due:
        return (
            "NOT_DUE",
            None,
            0,
            0,
            ("HORIZON_NOT_DUE_BY_RESEARCH_CUTOFF",),
        )
    if not partition_closed:
        return (
            "NOT_COMPUTED",
            None,
            0,
            0,
            ("FOLLOWUP_RAW_SESSION_PARTITION_NOT_CLOSED",),
        )
    if item is None:
        return (
            "NOT_COMPUTED",
            None,
            0,
            0,
            ("FOLLOWUP_BOARD_OUTCOME_NOT_SAVED",),
        )

    outcome = item.get("outcome")
    if not isinstance(outcome, Mapping):
        return "INCOMPLETE", None, 0, 0, ("FOLLOWUP_OUTCOME_INVALID",)
    status = outcome.get("outcome_status")
    member_denominator = outcome.get("fixed_member_denominator")
    observed = outcome.get("observed_member_count")
    unknown = outcome.get("unknown_member_count")
    reasons = _reason_codes(outcome.get("reason_codes"))
    if (
        status not in {"OBSERVED", "UNKNOWN"}
        or member_denominator != expected_member_count
        or type(observed) is not int
        or type(unknown) is not int
        or observed < 0
        or unknown < 0
        or observed + unknown != expected_member_count
    ):
        return "INCOMPLETE", None, 0, 0, reasons or ("FOLLOWUP_COVERAGE_INVALID",)
    if status == "OBSERVED":
        value = outcome.get("equal_weight_mean_return")
        if (
            not isinstance(value, Fraction)
            or observed != expected_member_count
            or unknown != 0
        ):
            return "INCOMPLETE", None, 0, 0, ("FOLLOWUP_EXACT_RETURN_INVALID",)
        return "OBSERVED", value, observed, unknown, reasons
    if expected_member_count == 0 and observed == 0 and unknown == 0:
        return (
            "UNKNOWN",
            None,
            0,
            0,
            reasons or ("EMPTY_FIXED_MEMBER_BASKET",),
        )
    if observed == expected_member_count or unknown == 0:
        return "INCOMPLETE", None, 0, 0, reasons or ("FOLLOWUP_UNKNOWN_STATE_INVALID",)
    return (
        "UNKNOWN",
        None,
        observed,
        unknown,
        reasons or ("FOLLOWUP_MEMBER_OR_PRICE_UNKNOWN",),
    )


def _summary_status(counts: Mapping[str, int], denominator: int) -> str:
    if denominator == 0:
        return "NO_GROUP_MEMBERS"
    if counts["INCOMPLETE"]:
        return "INCOMPLETE"
    if counts["OBSERVED"]:
        return "AVAILABLE"
    if counts["UNKNOWN"]:
        return "UNKNOWN"
    if counts["NOT_COMPUTED"]:
        return "NOT_COMPUTED"
    if counts["NOT_DUE"] == denominator:
        return "NOT_DUE"
    return "INCOMPLETE"


def _fraction_parts(value: Fraction | None) -> tuple[int | None, int | None]:
    if value is None:
        return None, None
    return value.numerator, value.denominator


def build_sector_pulse_cohort_outcomes(
    *,
    pulse_rows: Sequence[Mapping[str, Any]],
    transition_rows: Sequence[Mapping[str, Any]],
    outcomes: Sequence[Mapping[str, Any]],
    horizon_targets: Mapping[int, date | None],
    target_partition_statuses: Mapping[int, Mapping[str, Any]],
    research_cutoff: date,
    calendar_identity: Mapping[str, str],
) -> list[dict[str, Any]]:
    """为一个闭合T0批次形成按板块关系分组的精确typed统计。"""
    if (
        not pulse_rows
        or type(research_cutoff) is not date
        or set(horizon_targets) != set(_HORIZONS)
        or any(
            target is not None and type(target) is not date
            for target in horizon_targets.values()
        )
    ):
        raise ValueError("T0分组统计缺少完整板块、日历或截止输入")
    calendar_id = calendar_identity.get("calendar_id")
    calendar_revision = calendar_identity.get("calendar_revision")
    if not isinstance(calendar_id, str) or not calendar_id:
        raise ValueError("官方日历业务标识缺失")
    if not isinstance(calendar_revision, str) or not calendar_revision:
        raise ValueError("官方日历修订缺失")

    transitions_by_target: dict[int, list[Mapping[str, Any]]] = defaultdict(list)
    for row in transition_rows:
        target_id = row.get("target_pulse_id")
        if type(target_id) is not int or target_id <= 0:
            raise ValueError("相邻PULSE目标事实键无效")
        transitions_by_target[target_id].append(row)

    pulse_by_board: dict[tuple[str, str], Mapping[str, Any]] = {}
    grouped_boards: dict[tuple[str, str, str], list[Mapping[str, Any]]] = defaultdict(
        list
    )
    grouping_reasons: dict[tuple[str, str, str], dict[str, Counter[str]]] = defaultdict(
        lambda: defaultdict(Counter)
    )
    for pulse in pulse_rows:
        kind, board_id, pulse_state = (
            pulse.get("board_kind"),
            pulse.get("board_native_id"),
            pulse.get("pulse_state"),
        )
        if (
            kind not in _KINDS
            or not isinstance(board_id, str)
            or not board_id
            or pulse_state not in _PULSE_STATES
            or type(pulse.get("member_count")) is not int
            or pulse["member_count"] < 0
            or pulse.get("calculation_status") != "COMPLETE"
            or pulse.get("research_mode") != "NON_PIT_RETROSPECTIVE"
            or pulse.get("membership_mode")
            != "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT"
            or pulse.get("price_coordinate") != "FORWARD_ADJUSTED"
            or pulse.get("formal_rotation_state") != "NOT_EVALUATED"
        ):
            raise ValueError("T0 PULSE三态、板块目录或质量字段不合法")
        board_key = (kind, board_id)
        if board_key in pulse_by_board:
            raise ValueError("T0 PULSE板块业务键重复")
        pulse_by_board[board_key] = pulse
        grouped_boards[(kind, "PULSE_STATE", pulse_state)].append(pulse)
        transition_state, transition_reasons = _transition_for_pulse(
            pulse,
            transitions_by_target,
            calendar_id=calendar_id,
            calendar_revision=calendar_revision,
        )
        grouped_boards[(kind, "ADJACENT_TRANSITION", transition_state)].append(pulse)
        if transition_state == "NO_STORED_TRANSITION":
            grouping_reasons[(kind, "ADJACENT_TRANSITION", transition_state)][
                "NOT_COMPUTED"
            ]["NO_STORED_TRANSITION_PARENT"] += 1
        elif transition_state == "PULSE_TRANSITION_UNKNOWN":
            codes = transition_reasons or ("PULSE_TRANSITION_INPUT_UNKNOWN",)
            for code in codes:
                grouping_reasons[(kind, "ADJACENT_TRANSITION", transition_state)][
                    "UNKNOWN"
                ][code] += 1
        if pulse_state == "UNKNOWN":
            codes = _reason_codes(pulse.get("pulse_reason_codes")) or (
                "PULSE_STATE_UNKNOWN",
            )
            for code in codes:
                grouping_reasons[(kind, "PULSE_STATE", pulse_state)]["UNKNOWN"][
                    code
                ] += 1

    outcome_by_key: dict[tuple[str, str, int], Mapping[str, Any]] = {}
    for item in outcomes:
        kind = item.get("board_kind")
        board_id = item.get("board_native_id")
        horizon = item.get("horizon_sessions")
        if (
            (kind, board_id) not in pulse_by_board
            or type(horizon) is not int
            or horizon not in _HORIZONS
            or item.get("target_session") != horizon_targets[horizon]
            or horizon_targets[horizon] is None
            or horizon_targets[horizon] > research_cutoff
        ):
            raise ValueError("已计算后继超出T0、日历或研究截止范围")
        key = (kind, board_id, horizon)
        if key in outcome_by_key:
            raise ValueError("T0板块及期限的后继事实重复")
        outcome_by_key[key] = item

    result: list[dict[str, Any]] = []
    axis_states = {
        "PULSE_STATE": _PULSE_STATES,
        "ADJACENT_TRANSITION": _TRANSITION_STATES,
    }
    for kind in _KINDS:
        kind_pulses = [pulse for pulse in pulse_rows if pulse["board_kind"] == kind]
        if not kind_pulses:
            raise ValueError(f"{kind}的T0板块目录为空")
        for horizon in _HORIZONS:
            target = horizon_targets[horizon]
            due = target is not None and target <= research_cutoff
            target_status = target_partition_statuses.get(horizon, {})
            partition_closed = due and target_status.get("status") == "CLOSED"
            for axis, states in axis_states.items():
                for state in states:
                    boards = grouped_boards.get((kind, axis, state), [])
                    counts = Counter({status: 0 for status in _OUTCOME_STATES})
                    values: list[Fraction] = []
                    fixed_member_denominator_sum = 0
                    evaluable_member_denominator_sum = 0
                    observed_member_count_sum = 0
                    unknown_member_count_sum = 0
                    reason_counts: Counter[tuple[str, str, str]] = Counter()

                    for board in boards:
                        board_id = board["board_native_id"]
                        member_count = int(board["member_count"])
                        fixed_member_denominator_sum += member_count
                        item = outcome_by_key.get((kind, board_id, horizon))
                        status, value, observed, unknown, reasons = _outcome_for_board(
                            item,
                            due=due,
                            partition_closed=partition_closed,
                            expected_member_count=member_count,
                        )
                        counts[status] += 1
                        if status in {"OBSERVED", "UNKNOWN"}:
                            evaluable_member_denominator_sum += member_count
                            observed_member_count_sum += observed
                            unknown_member_count_sum += unknown
                        if status == "OBSERVED" and value is not None:
                            values.append(value)
                        for reason in reasons:
                            reason_counts[("OUTCOME", status, reason)] += 1

                    for quality_status, codes in grouping_reasons.get(
                        (kind, axis, state), {}
                    ).items():
                        for reason, count in codes.items():
                            reason_counts[("GROUPING", quality_status, reason)] += count

                    denominator = len(boards)
                    ordered = sorted(values)
                    distribution: dict[str, Any]
                    if ordered:
                        middle = len(ordered) // 2
                        median = (
                            ordered[middle]
                            if len(ordered) % 2
                            else (ordered[middle - 1] + ordered[middle]) / 2
                        )
                        mean = sum(ordered, Fraction()) / len(ordered)
                        minimum = ordered[0]
                        maximum = ordered[-1]
                        positive = sum(value > 0 for value in ordered)
                        nonpositive = len(ordered) - positive
                        mean_num, mean_den = _fraction_parts(mean)
                        median_num, median_den = _fraction_parts(median)
                        minimum_num, minimum_den = _fraction_parts(minimum)
                        maximum_num, maximum_den = _fraction_parts(maximum)
                        distribution = {
                            "sample_count": len(ordered),
                            "positive_board_count": positive,
                            "nonpositive_board_count": nonpositive,
                            "mean_return_numerator": mean_num,
                            "mean_return_denominator": mean_den,
                            "median_return_numerator": median_num,
                            "median_return_denominator": median_den,
                            "minimum_return_numerator": minimum_num,
                            "minimum_return_denominator": minimum_den,
                            "maximum_return_numerator": maximum_num,
                            "maximum_return_denominator": maximum_den,
                        }
                    else:
                        distribution = {
                            "sample_count": 0,
                            "positive_board_count": None,
                            "nonpositive_board_count": None,
                            "mean_return_numerator": None,
                            "mean_return_denominator": None,
                            "median_return_numerator": None,
                            "median_return_denominator": None,
                            "minimum_return_numerator": None,
                            "minimum_return_denominator": None,
                            "maximum_return_numerator": None,
                            "maximum_return_denominator": None,
                        }
                    if sum(counts.values()) != denominator:
                        raise ValueError("T0分组的完整板块分母未闭合")
                    result.append(
                        {
                            "board_kind": kind,
                            "cohort_axis": axis,
                            "cohort_state": state,
                            "horizon_sessions": horizon,
                            "target_session": target.isoformat()
                            if due and target is not None
                            else None,
                            "group_status": _summary_status(counts, denominator),
                            "board_relation_denominator": denominator,
                            "outcome_status_counts": dict(counts),
                            "fixed_member_denominator_sum": fixed_member_denominator_sum,
                            "evaluable_fixed_member_denominator_sum": (
                                evaluable_member_denominator_sum
                            ),
                            "observed_member_count_sum": observed_member_count_sum,
                            "unknown_member_count_sum": unknown_member_count_sum,
                            "distribution": distribution,
                            "reason_counts": [
                                {
                                    "reason_scope": scope,
                                    "quality_status": quality_status,
                                    "reason_code": reason,
                                    "board_relation_count": count,
                                }
                                for (scope, quality_status, reason), count in sorted(
                                    reason_counts.items()
                                )
                            ],
                        }
                    )
    expected_count = (
        len(_KINDS) * (len(_PULSE_STATES) + len(_TRANSITION_STATES)) * len(_HORIZONS)
    )
    if len(result) != expected_count:
        raise ValueError("T0分组统计未覆盖所有状态与期限")
    return result


__all__ = ["build_sector_pulse_cohort_outcomes"]
