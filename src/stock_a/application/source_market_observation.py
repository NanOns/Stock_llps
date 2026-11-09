"""来源级市场变化与后验观察所需的只读事实归并。"""

from __future__ import annotations

from collections import Counter
from datetime import date
from fractions import Fraction
from statistics import median
import time
from typing import Any, Mapping

from stock_a.application.source_d0_raw import query_source_d0_raw_launch
from stock_a.application.source_market_context import (
    SourceMarketContextError,
    read_source_market_context,
    source_market_context_payload,
)
from stock_a.application.source_pool_seed import (
    _board_native,
    _cross_period_boards,
    _loo_differences,
    read_source_pool_seed,
    read_board_seed_facts,
)
from stock_a.application.pg_price_profiles import PgPriceProfileError
from stock_a.application.research_window import resolve_pg_history_window
from stock_a.application.source_d1_support_changes import (
    discover_source_d1_support_changes,
)
from stock_a.application.source_d1_structures import (
    STRUCTURE_TYPES,
    discover_source_d1_structures,
)
from stock_a.research.entry_context.sessions import _load_calendar
from stock_a.research.entry_history.history_timeline import post_signal_price_facts
from stock_a.research.admission import line_breached
from stock_a.domain import FieldValue
from stock_a.storage.postgres.source_raw import (
    read_raw_daily_catalog,
    read_raw_daily_history_batch,
    read_raw_daily_history_page,
    read_raw_daily_history_stock,
)
from stock_a.application.tdx_sector_members import (
    read_tdx_sector_strength,
    tdx_sector_strength_read_context,
)
from stock_a.application.source_sector_change_research import (
    read_retrospective_rotation,
)
from stock_a.storage.postgres.tdx_sector_members import read_rotation_snapshot_members


class SourceObservationError(ValueError):
    """实际来源身份、分母或版本不能闭合。"""

    code = "OBSERVATION_SOURCE_MISMATCH"


def classify_source_transition(before: str | None, after: str | None) -> str:
    """只把已知假到真认作新增；缺前态不能推断上升沿。"""
    states = {"TRUE", "FALSE", "UNKNOWN", None}
    if before not in states or after not in states or after is None:
        raise ValueError("来源状态必须为TRUE、FALSE或UNKNOWN")
    if before is None:
        return "FIRST_OBSERVED_LEFT_CENSORED"
    return f"{before}_TO_{after}"


def _first_known_event(points, *, state_key, event_state, unknown_states):
    """未知会话之前发生过的事件不能从后来的首次证明倒推。"""
    unknown_before = False
    for point in points:
        state = point[state_key]
        if state in unknown_states:
            unknown_before = True
        elif state == event_state:
            session = point["session"]
            return (None if unknown_before else session), session
    return None, None


def _source_recovery_after_false(points, *, false_at_t0=False):
    """来源谓词转回真仅是诊断；未知间隔只能证明恢复的上界。"""
    false_seen = false_at_t0
    unknown_before_recovery = False
    for point in points:
        state = point["state"]
        if state == "FALSE":
            false_seen = True
        elif state in {"UNKNOWN", "NOT_AVAILABLE"}:
            unknown_before_recovery = True
        elif false_seen and state == "TRUE":
            session = point["session"]
            return (None if unknown_before_recovery else session), session
    return None, None


def read_market_observation_sessions(
    config: Mapping[str, Any],
    *,
    sessions: tuple[date, ...],
    research_cutoff: date,
    package_sha256: str,
    profile_revision: str,
    profile_contract: dict[str, Any],
    history_session_count: int,
    allow_missing_market: bool = False,
) -> list[dict[str, Any]]:
    """先完整计算每个来源池，再按相邻会话和来源代码归并三态。"""
    if not sessions or len(sessions) > 75 or tuple(sorted(set(sessions))) != sessions:
        raise ValueError("研究会话必须非空、唯一且递增")
    if sessions[-1] > research_cutoff:
        raise ValueError("研究会话不得晚于截止日")
    _, _, _, calendar_sessions = _load_calendar(sessions[-1])
    try:
        positions = [calendar_sessions.index(day) for day in sessions]
    except ValueError as error:
        raise ValueError("研究会话必须属于官方日历") from error
    if any(b != a + 1 for a, b in zip(positions, positions[1:])):
        raise ValueError("研究会话必须连续")

    previous: dict[str, dict[str, str]] = {}
    previous_namespace = None
    previous_scenarios = None
    previous_profile_revision = None
    outputs = []
    for session in sessions:
        scenarios = {}
        namespace = None
        for scenario in ("LAUNCH_CONFIRM", "RECOVERY_TURN"):
            page = query_source_d0_raw_launch(
                config,
                scenario=scenario,
                target_session=session,
                research_cutoff=research_cutoff,
                package_sha256=package_sha256,
                limit=5000,
                expected_namespace_id=namespace,
            )
            namespace = page["namespace_id"]
            while page["selection"]["next_offset"] is not None:
                offset = page["selection"]["next_offset"]
                following = query_source_d0_raw_launch(
                    config,
                    scenario=scenario,
                    target_session=session,
                    research_cutoff=research_cutoff,
                    package_sha256=package_sha256,
                    offset=offset,
                    limit=5000,
                    expected_namespace_id=namespace,
                )
                if following["source_pool"] != page["source_pool"]:
                    raise SourceObservationError("来源池分页分母或状态发生变化")
                page["results"].extend(following["results"])
                page["selection"]["next_offset"] = following["selection"]["next_offset"]
            scenarios[scenario] = page
        if previous_namespace is not None and namespace != previous_namespace:
            raise SourceObservationError("相邻会话来源namespace不一致")
        if previous_scenarios is not None:
            for scenario, current_result in scenarios.items():
                prior_result = previous_scenarios[scenario]
                if (
                    prior_result["source_version"]["source_content_id"]
                    != current_result["source_version"]["source_content_id"]
                    or prior_result["official_calendar"]
                    != current_result["official_calendar"]
                    or prior_result["session_contract"]
                    != current_result["session_contract"]
                    or prior_result["algorithm_contract"]
                    != current_result["algorithm_contract"]
                    or prior_result["producer_version"]
                    != current_result["producer_version"]
                ):
                    raise SourceObservationError(
                        "相邻会话来源内容、日历或算法身份不兼容"
                    )
        indexes = {
            name: {item["source_symbol"]: item for item in page["results"]}
            for name, page in scenarios.items()
        }
        try:
            seed = read_source_pool_seed(
                config,
                target_session=session,
                research_cutoff=research_cutoff,
                package_sha256=package_sha256,
                namespace_id=namespace,
                profile_revision=profile_revision,
                profile_contract=profile_contract,
                history_session_count=history_session_count,
                include_rps_read=True,
                allow_raw_profile_recompute=True,
            )
        except PgPriceProfileError as error:
            if error.code != "PROFILE_CHANGE_SNAPSHOT_MISSING":
                raise
            unavailable = {"state": "UNKNOWN", "reasons": [error.code]}
            seed = {
                "status": "UNKNOWN_DATA",
                "reason": error.code,
                "facts": {
                    symbol: {name: dict(unavailable) for name in ("s1", "s2", "seed")}
                    for symbol in indexes["LAUNCH_CONFIRM"]
                },
            }
        runtime_profile_revision = seed.get("runtime_profile_revision")
        if (
            previous_profile_revision is not None
            and runtime_profile_revision is not None
            and previous_profile_revision != runtime_profile_revision
        ):
            raise SourceObservationError("相邻会话即时画像计算修订不一致")
        symbols = set(seed["facts"])
        if any(set(index) != symbols for index in indexes.values()):
            raise SourceObservationError("D0和Seed来源池分母不一致")
        before_only_symbols = sorted(set(previous) - symbols)
        current = {}
        transition_counts: dict[str, Counter[str]] = {
            name: Counter()
            for name in ("LAUNCH_CONFIRM", "RECOVERY_TURN", "s1", "s2", "seed")
        }
        if before_only_symbols:
            for counter in transition_counts.values():
                counter["BEFORE_ONLY"] = len(before_only_symbols)
        for symbol in sorted(symbols):
            states = {name: indexes[name][symbol]["scenario_raw"] for name in indexes}
            states.update(
                {
                    name: seed["facts"][symbol][name]["state"]
                    for name in ("s1", "s2", "seed")
                }
            )
            transitions = {
                name: classify_source_transition(
                    previous.get(symbol, {}).get(name), state
                )
                for name, state in states.items()
            }
            for name, transition in transitions.items():
                transition_counts[name][transition] += 1
            current[symbol] = states
        try:
            market = read_source_market_context(
                config,
                target_session=session,
                research_cutoff=research_cutoff,
                package_sha256=package_sha256,
            )
        except SourceMarketContextError as error:
            if not allow_missing_market or error.category != "NOT_EVALUABLE":
                raise
            market = None
            market_payload = {
                "status": "UNKNOWN",
                "reason": error.code,
                "metric": {"exact": None},
            }
        source_version = scenarios["LAUNCH_CONFIRM"]["source_version"]
        if market is not None and (
            market.cohort.namespace_id != namespace
            or market.cohort.source_content_id != source_version["source_content_id"]
            or market.cohort.source_capture_id != source_version["source_capture_id"]
            or market.pg_result["target_raw_digest"]
            != source_version["source_raw_digest"]
        ):
            raise SourceObservationError("市场宽度与D0来源捕获身份不一致")
        if market is not None:
            market_payload = source_market_context_payload(market)
        outputs.append(
            {
                "session": session,
                "namespace_id": namespace,
                "market": market_payload,
                "scenarios": scenarios,
                "seed": seed,
                "states": current,
                "transition_counts": {
                    name: dict(counts) for name, counts in transition_counts.items()
                },
                "before_only_symbols": before_only_symbols,
                "prior_states": previous,
                "formal_pb_continue_d2": "NOT_EVALUATED",
                "pit_status": "NON_PIT_RETROSPECTIVE",
            }
        )
        previous, previous_namespace, previous_scenarios = current, namespace, scenarios
        previous_profile_revision = runtime_profile_revision
    return outputs


def assemble_observation_changes(
    config: Mapping[str, Any],
    days: list[dict[str, Any]],
    *,
    industry_snapshot_id: str,
    concept_snapshot_id: str,
    d1_by_session: Mapping[date, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """从完整池的已知转换生成观察事件并按完整后验成员归并。"""
    snapshots = [
        read_rotation_snapshot_members(config, snapshot_id=snapshot, board_kind=kind)
        for kind, snapshot in (
            ("INDUSTRY", industry_snapshot_id),
            ("CONCEPT", concept_snapshot_id),
        )
    ]
    if any(
        snapshot["namespace_id"] != days[0]["namespace_id"] for snapshot in snapshots
    ):
        raise SourceObservationError("板块成员与来源池namespace不一致")
    memberships: dict[str, list[dict[str, str]]] = {}
    boards = []
    for snapshot in snapshots:
        for board_id, board in sorted(snapshot["boards"].items()):
            identity = {
                "board_kind": snapshot["board_kind"],
                "board_native_id": board_id,
                "board_name": board["name"],
            }
            for symbol in board["members"]:
                memberships.setdefault(symbol, []).append(identity)
            boards.append((identity, board["members"]))

    events = []
    daily_boards = []
    for day in days:
        states, prior = day["states"], day["prior_states"]
        day_events = []
        d1_details = None
        for symbol, current in states.items():
            for signal, state in current.items():
                transition = classify_source_transition(
                    prior.get(symbol, {}).get(signal), state
                )
                is_event = transition == "FALSE_TO_TRUE" or (
                    signal in {"s1", "s2", "seed"} and transition == "TRUE_TO_FALSE"
                )
                if is_event:
                    day_events.append(
                        {
                            "session": day["session"],
                            "source_symbol": symbol,
                            "signal": signal,
                            "transition": transition,
                            "source_binding": {
                                "namespace_id": day["namespace_id"],
                                "package_sha256": day["scenarios"]["LAUNCH_CONFIRM"][
                                    "source_version"
                                ]["package_sha256"],
                                "source_content_id": day["scenarios"]["LAUNCH_CONFIRM"][
                                    "source_version"
                                ]["source_content_id"],
                                "profile_revision": day["seed"].get("profile_revision"),
                                "runtime_profile_revision": day["seed"].get(
                                    "runtime_profile_revision"
                                ),
                                "stored_profile_status": day["seed"].get(
                                    "stored_profile_status"
                                ),
                            },
                            "algorithm_contract": day["scenarios"][signal][
                                "algorithm_contract"
                            ]
                            if signal in day["scenarios"]
                            else "ALG-19/20",
                            "producer_version": day["scenarios"][signal][
                                "producer_version"
                            ]
                            if signal in day["scenarios"]
                            else day["seed"].get("runtime_profile_revision"),
                            "memberships": memberships.get(symbol, []),
                            "membership_reason": None
                            if symbol in memberships
                            else "NO_PROVED_MEMBERSHIP",
                            "formal_eligibility": "UNKNOWN",
                            "formal_authorization": "FORMAL_NOT_AUTHORIZED",
                            "formal_eligibility_reason": "FORMAL_EPISODE_AND_QUALIFICATION_UNPROVED",
                        }
                    )
        if d1_by_session is not None:
            discovery = d1_by_session[day["session"]]
            if (
                discovery["source_pool_count"] != len(states)
                or discovery["source"]["namespace_id"] != day["namespace_id"]
                or discovery["source"]["package_sha256"]
                != day["scenarios"]["LAUNCH_CONFIRM"]["source_version"][
                    "package_sha256"
                ]
                or discovery["source"]["source_content_id"]
                != day["scenarios"]["LAUNCH_CONFIRM"]["source_version"][
                    "source_content_id"
                ]
                or discovery["source"]["source_capture_id"]
                != day["scenarios"]["LAUNCH_CONFIRM"]["source_version"][
                    "source_capture_id"
                ]
                or discovery["source"]["source_raw_digest"]
                != day["scenarios"]["LAUNCH_CONFIRM"]["source_version"][
                    "source_raw_digest"
                ]
            ):
                raise SourceObservationError("D1与来源池分母或namespace不一致")
            d1_details = {
                detail["source_symbol"]: detail
                for detail in discovery["complete_symbol_details"]
            }
            if len(d1_details) != discovery["source_pool_count"] or set(
                d1_details
            ) != set(states):
                raise SourceObservationError("D1来源代码集合与市场来源池不一致")
            for detail in discovery["complete_symbol_details"]:
                symbol = detail["source_symbol"]
                for anchor in detail["anchor_events"]:
                    if anchor["change_classification"] != "CHANGED":
                        continue
                    day_events.append(
                        {
                            "session": day["session"],
                            "source_symbol": symbol,
                            "signal": "D1_SUPPORT_CHANGE",
                            "transition": "CHANGED",
                            "d1_source_event": anchor,
                            "memberships": memberships.get(symbol, []),
                            "membership_reason": None
                            if symbol in memberships
                            else "NO_PROVED_MEMBERSHIP",
                            "formal_eligibility": "UNKNOWN",
                            "formal_authorization": "FORMAL_NOT_AUTHORIZED",
                            "formal_eligibility_reason": "FORMAL_EPISODE_AND_QUALIFICATION_UNPROVED",
                        }
                    )
        events.extend(day_events)
        market_changed_symbols = {event["source_symbol"] for event in day_events}
        for identity, members in boards:
            member_set = set(members)
            board_events = [
                event for event in day_events if event["source_symbol"] in member_set
            ]
            board_changed_symbols = {event["source_symbol"] for event in board_events}
            counts = {}
            for signal in ("LAUNCH_CONFIRM", "RECOVERY_TURN", "s1", "s2", "seed"):
                state_counts = Counter(
                    states.get(symbol, {}).get(signal, "UNKNOWN") for symbol in members
                )
                changes = Counter(
                    classify_source_transition(
                        prior.get(symbol, {}).get(signal), states[symbol][signal]
                    )
                    for symbol in members
                    if symbol in states
                )
                changes["BEFORE_ONLY"] += sum(
                    symbol in prior and symbol not in states for symbol in members
                )
                counts[signal] = {
                    "states": {
                        key: state_counts[key] for key in ("TRUE", "FALSE", "UNKNOWN")
                    },
                    "transitions": dict(changes),
                }
            if d1_details is not None:
                classifications = Counter(
                    d1_details[symbol]["summary_state"]
                    if symbol in d1_details
                    else "MEMBER_NOT_IN_SOURCE_POOL"
                    for symbol in members
                )
                raw_counts = Counter()
                anchor_changes = Counter()
                for symbol in members:
                    detail = d1_details.get(symbol)
                    if detail is None:
                        continue
                    raw_counts.update(detail["raw_d1_state_counts"])
                    anchor_changes.update(
                        anchor["change_classification"]
                        for anchor in detail["anchor_events"]
                    )
                counts["D1_SUPPORT"] = {
                    "member_summary_states": dict(classifications),
                    "source_pool_mapped_members": sum(
                        symbol in d1_details for symbol in members
                    ),
                    "source_pool_unmapped_members": sum(
                        symbol not in d1_details for symbol in members
                    ),
                    "raw_d1_session_counts": {
                        state: raw_counts[state]
                        for state in ("TRUE", "FALSE", "UNKNOWN")
                    },
                    "anchor_change_counts": dict(anchor_changes),
                    "raw_count_scope": "MEMBER_BY_EVENT_WINDOW_SESSION",
                    "anchor_count_scope": "SOURCE_ANCHOR_EVENTS",
                }
            daily_boards.append(
                {
                    "session": day["session"],
                    **identity,
                    "declared_member_count": len(members),
                    "mapped_source_count": sum(symbol in states for symbol in members),
                    "unknown_mapping_count": sum(
                        symbol not in states for symbol in members
                    ),
                    "signals": counts,
                    "event_count": len(board_events),
                    "distinct_changed_symbol_count": len(board_changed_symbols),
                    "market_changed_symbol_denominator": len(market_changed_symbols),
                    "share_of_market_changed_symbols": str(
                        Fraction(
                            len(board_changed_symbols), len(market_changed_symbols)
                        )
                    )
                    if market_changed_symbols
                    else None,
                    "native_detail": "NOT_EVALUATED_SELECT_BOARD",
                    "loo_detail": "NOT_EVALUATED_SELECT_BOARD_AND_SYMBOL",
                    "rotation_detail": "NOT_EVALUATED_SELECT_BOARD",
                    "membership_status": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
                }
            )
    return {
        "events": events,
        "event_count": len(events),
        "distinct_symbol_count": len({event["source_symbol"] for event in events}),
        "boards": daily_boards,
        "memberships_by_symbol": memberships,
        "snapshot_metadata": [snapshot["snapshot"] for snapshot in snapshots],
        "membership_status": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
    }


def attach_observation_state_followup(
    days: list[dict[str, Any]],
    observation: dict[str, Any],
    *,
    d1_by_session: Mapping[date, Mapping[str, Any]] | None = None,
    horizon: int | None = None,
) -> None:
    """仅投影请求中已经计算的后续状态，不推造正式退出状态。"""
    unknown_reasons = {}
    for day in days:
        for signal, scenario in day.get("scenarios", {}).items():
            unknown_reasons[(day["session"], signal)] = {
                row["source_symbol"]: row.get("main_reason") or "SOURCE_STATE_UNKNOWN"
                for row in scenario["results"]
                if row["scenario_raw"] == "UNKNOWN"
            }
    for event in observation["events"]:
        signal = event["signal"]
        if signal == "D1_SUPPORT_CHANGE":
            points = []
            if d1_by_session is not None:
                future_days = [day for day in days if day["session"] > event["session"]]
                for day in future_days[:horizon]:
                    details = d1_by_session[day["session"]]["complete_symbol_details"]
                    detail = next(
                        (
                            row
                            for row in details
                            if row["source_symbol"] == event["source_symbol"]
                        ),
                        None,
                    )
                    anchor = next(
                        (
                            row
                            for row in (detail or {}).get("anchor_events", [])
                            if row["support_anchor_session"]
                            == event["d1_source_event"]["support_anchor_session"]
                        ),
                        None,
                    )
                    points.append(
                        {
                            "session": day["session"],
                            "classification": anchor["change_classification"]
                            if anchor
                            else "UNKNOWN",
                            "observation": anchor.get("support_observation_after")
                            if anchor
                            else None,
                            "reason": anchor.get("reason_code")
                            if anchor
                            else "D1_ANCHOR_NOT_AVAILABLE",
                        }
                    )
            if horizon is not None:
                for _ in range(len(points), horizon):
                    points.append(
                        {
                            "session": None,
                            "classification": "NOT_AVAILABLE",
                            "observation": None,
                            "reason": "NO_FUTURE_SESSION_IN_SAVED_WINDOW",
                        }
                    )
            observable = 0
            for point in points:
                if point["classification"] not in {"CHANGED", "UNCHANGED"}:
                    break
                observable += 1
            first_unknown = next(
                (
                    point
                    for point in points
                    if point["classification"]
                    in {"UNKNOWN", "NOT_COMPARABLE", "NOT_EVALUABLE", "OTHER_EXPLAINED"}
                ),
                None,
            )
            break_points = []
            for point in points:
                support = (point.get("observation") or {}).get(
                    "support_observation"
                ) or {}
                observed = (
                    point["classification"] in {"CHANGED", "UNCHANGED"}
                    and support.get("quality") == "OBSERVED"
                )
                break_points.append(
                    {
                        "session": point["session"],
                        "state": "UNKNOWN"
                        if not observed
                        else "TRUE"
                        if support.get("state") == "BROKEN"
                        else "FALSE",
                    }
                )
            first_broken, first_broken_upper_bound = _first_known_event(
                break_points,
                state_key="state",
                event_state="TRUE",
                unknown_states={"UNKNOWN"},
            )
            event["source_state_followup"] = {
                "status": "COMPUTED"
                if any(point["session"] for point in points)
                else "NOT_EVALUATED",
                "points": points,
                "first_unknown_session": first_unknown["session"]
                if first_unknown
                else None,
                "first_unknown_reason": first_unknown["reason"]
                if first_unknown
                else None,
                "consecutive_observable_session_count": observable,
                "first_broken_session": first_broken,
                "first_broken_upper_bound": first_broken_upper_bound,
                "formal_exit_status": "NOT_EVALUATED",
            }
            continue
        points = []
        future_days = [day for day in days if day["session"] > event["session"]]
        for day in future_days[:horizon]:
            symbol = event["source_symbol"]
            state = day["states"].get(symbol, {}).get(signal)
            if state is None:
                reason = "SOURCE_SYMBOL_NOT_IN_POOL"
            elif state == "FALSE":
                reason = "SOURCE_SIGNAL_FALSE_NOT_FORMAL_EXIT"
            elif state == "UNKNOWN":
                reason = unknown_reasons.get((day["session"], signal), {}).get(symbol)
                if reason is None:
                    fact = (
                        day.get("seed", {})
                        .get("facts", {})
                        .get(symbol, {})
                        .get(signal, {})
                    )
                    reasons = fact.get("reasons") or (
                        [fact["reason"]] if fact.get("reason") else []
                    )
                    reason = (
                        ";".join(str(item) for item in reasons)
                        or "SOURCE_STATE_UNKNOWN"
                    )
            else:
                reason = "SOURCE_SIGNAL_TRUE"
            points.append(
                {
                    "session": day["session"],
                    "state": state or "UNKNOWN",
                    "reason": reason,
                }
            )
        if horizon is not None:
            for _ in range(len(points), horizon):
                points.append(
                    {
                        "session": None,
                        "state": "NOT_AVAILABLE",
                        "reason": "NO_FUTURE_SESSION_IN_SAVED_WINDOW",
                    }
                )
        if event["transition"] == "TRUE_TO_FALSE":
            first_false = first_false_upper_bound = event["session"]
        else:
            first_false, first_false_upper_bound = _first_known_event(
                points,
                state_key="state",
                event_state="FALSE",
                unknown_states={"UNKNOWN", "NOT_AVAILABLE"},
            )
        first_recovery, recovery_upper = _source_recovery_after_false(
            points, false_at_t0=event["transition"] == "TRUE_TO_FALSE"
        )
        first_unknown = next(
            (point["session"] for point in points if point["state"] == "UNKNOWN"), None
        )
        observable = 0
        for point in points:
            if point["state"] not in {"TRUE", "FALSE"}:
                break
            observable += 1
        consecutive_true = 1 if event["transition"] == "FALSE_TO_TRUE" else 0
        if consecutive_true:
            for point in points:
                if point["state"] != "TRUE":
                    break
                consecutive_true += 1
        event["source_state_followup"] = {
            "status": "COMPUTED"
            if any(point["session"] for point in points)
            else "NOT_EVALUATED",
            "t0_state": "TRUE" if event["transition"] == "FALSE_TO_TRUE" else "FALSE",
            "points": points,
            "consecutive_true_session_count_including_t0": consecutive_true,
            "first_proved_false_session": first_false,
            "first_proved_false_upper_bound": first_false_upper_bound,
            "first_proved_recovery_session": first_recovery,
            "first_proved_recovery_upper_bound": recovery_upper,
            "recovery_scope": "SOURCE_PREDICATE_NOT_FORMAL_REENTRY",
            "first_unknown_session": first_unknown,
            "first_unknown_reason": next(
                (point["reason"] for point in points if point["state"] == "UNKNOWN"),
                None,
            ),
            "consecutive_observable_session_count": observable,
            "formal_exit_status": "NOT_EVALUATED",
        }


def attach_board_strengths(
    config: dict[str, Any],
    days: list[dict[str, Any]],
    observation: dict[str, Any],
    *,
    industry_snapshot_id: str,
    concept_snapshot_id: str,
    research_cutoff: date,
    package_sha256: str,
    profile_revision: str,
) -> None:
    """在完整行业/GN成员上复用现役跨期强弱，缺画像时局部未知。"""
    boards = {
        (row["session"], row["board_kind"], row["board_native_id"]): row
        for row in observation["boards"]
    }
    for day in days:
        if day["seed"].get("status") == "UNKNOWN_DATA":
            for key, row in boards.items():
                if key[0] == day["session"]:
                    row["strength"] = {
                        "status": "UNKNOWN_DATA",
                        "reason": day["seed"]["reason"],
                    }
                    row["native_detail"] = {
                        "status": "UNKNOWN_DATA",
                        "reason": day["seed"]["reason"],
                    }
                    row["rotation_detail"] = {
                        "status": "UNKNOWN_DATA",
                        "reason": day["seed"]["reason"],
                    }
            continue
        if day["seed"].get("stored_profile_status") == "MISSING_RAW_RECOMPUTED":
            for kind, snapshot_id in (
                ("INDUSTRY", industry_snapshot_id),
                ("CONCEPT", concept_snapshot_id),
            ):
                snapshot = read_rotation_snapshot_members(
                    config, snapshot_id=snapshot_id, board_kind=kind
                )
                if snapshot["namespace_id"] != day["namespace_id"]:
                    raise SourceObservationError("板块成员与来源池namespace不一致")
                peers = {key: row["members"] for key, row in snapshot["boards"].items()}
                cache = {}
                representative = next(
                    (key for key, members in peers.items() if members), None
                )
                if representative is None:
                    raise SourceObservationError("板块快照没有可用成员")
                rotation = read_retrospective_rotation(
                    config,
                    snapshot_id=snapshot_id,
                    board_kind=kind,
                    board_native_id=representative,
                    start_session=day["session"],
                    target_session=day["session"],
                    research_cutoff=research_cutoff,
                    package_sha256=package_sha256,
                    peer_target_only=True,
                )
                if rotation["namespace_id"] != day["namespace_id"]:
                    raise SourceObservationError("板块轮动与来源池namespace不一致")
                for board_id, members in peers.items():
                    row = boards[(day["session"], kind, board_id)]
                    row["native_detail"] = _board_native(
                        day["seed"], members, peers, board_id, peer_cache=cache
                    )
                    row["native_detail"]["cross_period"] = rotation["peer_target"][
                        board_id
                    ]
                    row["strength"] = {
                        "status": "UNKNOWN_DATA",
                        "reason": "STORED_PROFILE_SNAPSHOT_MISSING",
                    }
                    row["rotation_detail"] = rotation["peer_target"][board_id][
                        "rotation"
                    ]
            continue
        with tdx_sector_strength_read_context(config) as context:
            for kind, snapshot in (
                ("INDUSTRY", industry_snapshot_id),
                ("CONCEPT", concept_snapshot_id),
            ):
                strength = read_tdx_sector_strength(
                    config,
                    snapshot_id=snapshot,
                    board_kind=kind,
                    board_native_id=None,
                    target_session=day["session"],
                    research_cutoff=research_cutoff,
                    package_sha256=package_sha256,
                    profile_revision=profile_revision,
                    changed_only=False,
                    sort_by="delta20",
                    direction="STRONGER",
                    offset=0,
                    limit=1,
                    member_offset=0,
                    member_limit=0,
                    include_all_boards=True,
                    include_all_member_symbols=True,
                    include_cross_period_inputs=True,
                    read_context=context,
                )
                if (
                    strength["namespace_id"] != day["namespace_id"]
                    or strength["target_source_version"]["source_capture_id"]
                    != day["scenarios"]["LAUNCH_CONFIRM"]["source_version"][
                        "source_capture_id"
                    ]
                ):
                    raise SourceObservationError("板块强弱与来源池namespace不一致")
                peer_members = strength["all_member_symbols_for_linkage"]
                cross = _cross_period_boards(
                    peer_members, strength["cross_period_inputs_for_linkage"]
                )
                peer_cache = {}
                for summary in strength["all_boards_for_linkage"]:
                    key = (day["session"], kind, summary["board_native_id"])
                    if key not in boards:
                        raise SourceObservationError("板块强弱与成员快照不一致")
                    boards[key]["strength"] = summary
                    native = _board_native(
                        day["seed"],
                        peer_members[key[2]],
                        peer_members,
                        key[2],
                        peer_cache=peer_cache,
                    )
                    native["cross_period"] = cross[key[2]]
                    native["cross_period"]["sessions"] = {
                        "target": strength["target_session"],
                        "three_official_sessions_before": strength[
                            "three_official_sessions_before"
                        ],
                        "previous_official_session": strength["window_sessions"][-2],
                    }
                    boards[key]["native_detail"] = native
                    boards[key]["rotation_detail"] = cross[key[2]]["rotation"]


def read_observation_d1(
    config: Mapping[str, Any],
    config_path: str,
    *,
    sessions: tuple[date, ...],
    research_cutoff: date,
    package_sha256: str,
    event_window_start: date,
    history_session_count: int,
) -> dict[date, dict[str, Any]]:
    """复用现役D1来源支撑发现，不构造正式事件。"""
    result = {}
    for target in sessions:
        _, history, revision = resolve_pg_history_window(
            target.isoformat(), history_session_count
        )
        result[target] = discover_source_d1_support_changes(
            config,
            config_path,
            target,
            research_cutoff,
            history,
            revision,
            package_sha256,
            event_window_start,
            0,
            1,
            include_symbol_details=True,
        )
    return result


def _d1_material_board_summaries(
    snapshots: list[dict[str, Any]],
    symbol_summaries: Mapping[str, Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, set[str]]]:
    source_symbols = set(symbol_summaries)
    board_rows = []
    mapped_by_kind: dict[str, set[str]] = {}
    for snapshot in snapshots:
        kind = snapshot["board_kind"]
        mapped: set[str] = set()
        for board_id, board in snapshot["boards"].items():
            members = set(board["members"])
            mapped.update(members)
            in_source = members & source_symbols
            group_counts: Counter[str] = Counter()
            status_counts: Counter[str] = Counter()
            status_counts_by_type = {name: Counter() for name in STRUCTURE_TYPES}
            anchor_counts_by_type: Counter[str] = Counter()
            material_anchor_counts_by_type: Counter[str] = Counter()
            material_symbols = []
            material_anchor_count = 0
            for symbol in sorted(in_source):
                item = symbol_summaries[symbol]
                group_counts[item["group"]] += 1
                for status, count in item["material_change_status_counts"].items():
                    status_counts[status] += count
                    status_counts_by_type_total = item[
                        "material_change_status_counts_by_type"
                    ]
                    for structure, values in status_counts_by_type_total.items():
                        status_counts_by_type[structure][status] += values.get(
                            status, 0
                        )
                for structure, count in item["anchor_counts_by_type"].items():
                    anchor_counts_by_type[structure] += count
                for structure, count in item[
                    "material_change_anchor_counts_by_type"
                ].items():
                    material_anchor_counts_by_type[structure] += count
                if item["group"] == "MATERIAL_CHANGE":
                    material_anchor_count += item["material_change_anchor_count"]
                    material_symbols.append(
                        {
                            "source_symbol": symbol,
                            "material_change_anchor_count": item[
                                "material_change_anchor_count"
                            ],
                            "material_change_anchor_counts_by_type": item[
                                "material_change_anchor_counts_by_type"
                            ],
                            "material_change_status_counts": item[
                                "material_change_status_counts"
                            ],
                            "formal_eligibility": "UNKNOWN/NOT_EVALUATED",
                            "membership_status": (
                                "NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT"
                            ),
                        }
                    )
            board_rows.append(
                {
                    "board_kind": kind,
                    "board_native_id": board_id,
                    "board_name": board["name"],
                    "snapshot_id": snapshot["snapshot"]["snapshot_id"],
                    "snapshot_observed_at": snapshot["snapshot"]["observed_at"],
                    "snapshot_membership_mode": snapshot["snapshot"]["research_mode"],
                    "membership_status": ("NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT"),
                    "snapshot_member_count": len(members),
                    "members_in_source_pool": len(in_source),
                    "members_outside_source_pool": len(members - source_symbols),
                    "source_pool_group_counts": dict(sorted(group_counts.items())),
                    "material_change_distinct_symbol_count": len(material_symbols),
                    "material_change_anchor_count": material_anchor_count,
                    "material_change_status_counts": dict(
                        sorted(status_counts.items())
                    ),
                    "material_change_status_counts_by_type": {
                        structure: dict(sorted(counts.items()))
                        for structure, counts in status_counts_by_type.items()
                    },
                    "anchor_counts_by_type": dict(
                        sorted(anchor_counts_by_type.items())
                    ),
                    "material_change_anchor_counts_by_type": dict(
                        sorted(material_anchor_counts_by_type.items())
                    ),
                    "material_change_stocks": material_symbols,
                    "board_member_counts_are_non_additive": True,
                }
            )
        mapped_by_kind[kind] = mapped
    board_rows.sort(
        key=lambda row: (
            -row["material_change_distinct_symbol_count"],
            row["board_kind"],
            row["board_native_id"],
        )
    )
    return board_rows, mapped_by_kind


def _d1_material_outcomes(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    package_sha256: str,
    horizon: int,
    source: Mapping[str, Any],
    source_symbol_summary: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    started_wall = time.perf_counter()
    started_cpu = time.process_time()
    _calendar_payload, _calendar_metadata, _calendar_revision, all_sessions = (
        _load_calendar(research_cutoff)
    )
    cutoff_sessions = tuple(day for day in all_sessions if day <= research_cutoff)
    if target_session not in cutoff_sessions:
        raise SourceObservationError("后续比较目标不在截止内的官方日历")
    horizon_sessions = tuple(
        (target_session,)
        + tuple(day for day in cutoff_sessions if day > target_session)[:horizon]
    )
    if len(horizon_sessions) < horizon + 1:
        return {
            "status": "NOT_MATURED",
            "scope": "NON_PIT_DESCRIPTIVE_NOT_RETURN_VALIDATION",
            "target_session": target_session.isoformat(),
            "horizon": horizon,
            "mature_session_count": len(horizon_sessions) - 1,
            "required_session_count": horizon,
            "history_rows_read": 0,
            "raw_batch_reads": 0,
            "reason": "NO_SAVED_POST_SIGNAL_SESSION_WITHIN_RESEARCH_CUTOFF",
            "database_write_attempted": False,
            "post_cutoff_rows_read": 0,
        }

    catalog = read_raw_daily_catalog(
        config,
        max_target_session=research_cutoff,
        year=target_session.year,
        offset=0,
        limit=200,
    )
    if catalog["namespace_id"] != source.get("namespace_id"):
        raise SourceObservationError("后续Raw目录与D1来源namespace不一致")
    versions = {
        (date.fromisoformat(row["session_date"]), row["package_sha256"]): row
        for row in catalog["versions"]
    }
    for session in horizon_sessions:
        version = versions.get((session, package_sha256))
        if (
            version is None
            or version["version_status"] != "CLOSED"
            or version["row_count"] != source["raw_row_count"]
            or version["expected_row_count"] != source["raw_row_count"]
        ):
            return {
                "status": "SOURCE_VERSION_NOT_CLOSED",
                "scope": "NON_PIT_DESCRIPTIVE_NOT_RETURN_VALIDATION",
                "target_session": target_session.isoformat(),
                "horizon": horizon,
                "session_versions_checked": len(horizon_sessions),
                "failed_session": session.isoformat(),
                "failed_version": version,
                "history_rows_read": 0,
                "raw_batch_reads": 0,
                "database_write_attempted": False,
                "post_cutoff_rows_read": 0,
            }

    cohort_symbols = sorted(
        symbol
        for symbol, item in source_symbol_summary.items()
        if item["group"] in {"MATERIAL_CHANGE", "KNOWN_NON_MATERIAL"}
    )
    rows_by_symbol: dict[str, list[dict[str, Any]]] = {}
    raw_history_rows_read = 0
    raw_batch_reads = 0
    expected_content_id = source.get("source_content_id")
    target_capture_id = str(source.get("source_capture_id"))
    for offset in range(0, len(cohort_symbols), 100):
        batch_symbols = cohort_symbols[offset : offset + 100]
        batch = read_raw_daily_history_batch(
            config,
            source_symbols=batch_symbols,
            target_session=target_session,
            research_cutoff=research_cutoff,
            package_sha256=package_sha256,
            sessions=horizon_sessions,
            expected_namespace_id=source["namespace_id"],
            expected_source_content_id=expected_content_id,
            expected_target_capture_id=target_capture_id,
            starts_at_target=True,
        )
        if any(day > research_cutoff for day in horizon_sessions):
            raise SourceObservationError("后续Raw请求越过显式研究截止")
        rows_by_symbol.update(batch["history_rows_by_symbol"])
        raw_history_rows_read += batch["raw_history_rows_read"]
        raw_batch_reads += batch["raw_history_batch_reads"]

    points_by_group: dict[str, list[dict[str, Any]]] = {
        "MATERIAL_CHANGE": [],
        "KNOWN_NON_MATERIAL": [],
    }
    for symbol in cohort_symbols:
        facts = post_signal_price_facts(
            horizon_sessions, rows_by_symbol.get(symbol, []), horizon
        )
        t0_quality = facts["t0"]["ohlc"]["quality"]
        endpoint = facts["points"][horizon - 1]
        endpoint_quality = endpoint["ohlc"]["quality"]
        change = endpoint["raw_close_relative_change"]
        if change.get("quality") == "OBSERVED":
            raw_value = Fraction(change["value"]["exact"])
            raw_status = "OBSERVED_RAW_CLOSE_CHANGE"
            reason = None
        else:
            raw_value = None
            raw_status = "UNKNOWN_PRICE_QUALITY"
            reason = (
                "T0:" + str(facts["t0"]["ohlc"].get("reason"))
                if t0_quality != "OBSERVED"
                else "T_PLUS_N:" + str(endpoint["ohlc"].get("reason"))
            )
        points_by_group[source_symbol_summary[symbol]["group"]].append(
            {
                "source_symbol": symbol,
                "raw_status": raw_status,
                "raw_close_change": raw_value,
                "raw_reason": reason,
                "raw_quality": change.get("quality"),
                "corporate_action_continuity": "UNKNOWN_NOT_EVALUATED",
            }
        )

    groups = []
    for group in ("MATERIAL_CHANGE", "KNOWN_NON_MATERIAL"):
        symbols = [
            symbol
            for symbol, item in source_symbol_summary.items()
            if item["group"] == group
        ]
        group_points = points_by_group[group]
        observed = [
            item["raw_close_change"]
            for item in group_points
            if item["raw_close_change"] is not None
        ]
        reasons = Counter(
            item["raw_reason"]
            for item in group_points
            if item["raw_reason"] is not None
        )
        positive_count = sum(value > 0 for value in observed)
        groups.append(
            {
                "group": group,
                "source_symbol_count": len(symbols),
                "outcome_symbols_read": len(group_points),
                "raw_close_change_observed_count": len(observed),
                "raw_close_change_unknown_count": len(group_points) - len(observed),
                "raw_close_change_unknown_reasons": dict(sorted(reasons.items())),
                "non_comparable_count": len(observed),
                "non_comparable_reason": (
                    "CORPORATE_ACTION_CONTINUITY_NOT_PROVED_FOR_RAW_CLOSE_RATIO"
                ),
                "median_raw_close_change_unadjusted": (
                    str(median(observed)) if observed else None
                ),
                "positive_raw_close_ratio_unadjusted": (
                    str(Fraction(positive_count, len(observed))) if observed else None
                ),
                "median_comparable_outcome": None,
                "positive_comparable_ratio": None,
                "corporate_action_continuity": "UNKNOWN_NOT_EVALUATED",
                "result_scope": "RAW_CLOSE_DESCRIPTION_ONLY_NOT_RETURN_VALIDATION",
            }
        )
    return {
        "status": "MATURED_RAW_DESCRIPTIVE_PARTIAL_COMPARABILITY",
        "scope": "NON_PIT_DESCRIPTIVE_NOT_RETURN_VALIDATION",
        "selection_time_role": "T0_D1_ONLY_FUTURE_OUTCOMES_NOT_USED_FOR_GROUPING",
        "target_session": target_session.isoformat(),
        "horizon": horizon,
        "sessions": [day.isoformat() for day in horizon_sessions],
        "source_pool_denominator": len(source_symbol_summary),
        "comparison_group_denominator": len(cohort_symbols),
        "groups": groups,
        "price_coordinate": "RAW_UNADJUSTED",
        "raw_close_formula": "C[T0+N]/C[T0]-1",
        "corporate_action_continuity": "UNKNOWN_NOT_EVALUATED",
        "history_rows_read": raw_history_rows_read,
        "raw_batch_reads": raw_batch_reads,
        "rows_read_after_cutoff": 0,
        "database_transaction": "REPEATABLE_READ_READ_ONLY",
        "database_write_attempted": False,
        "ddl_executed": False,
        "dml_executed": False,
        "wall_seconds": round(time.perf_counter() - started_wall, 6),
        "cpu_seconds": round(time.process_time() - started_cpu, 6),
    }


def read_d1_material_observation(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    package_sha256: str,
    d1_history_session_count: int,
    industry_snapshot_id: str,
    concept_snapshot_id: str,
    horizon: int,
    structure_type: str | None = None,
    board_kind: str | None = None,
    board_native_id: str | None = None,
    symbol: str | None = None,
    offset: int = 0,
    limit: int = 30,
) -> dict[str, Any]:
    """单次全池D1后生成市场、当前板块与股票研究视图。"""
    started_wall = time.perf_counter()
    _target, sessions, _calendar_revision = resolve_pg_history_window(
        target_session.isoformat(), d1_history_session_count
    )
    d1 = discover_source_d1_structures(
        config,
        target_session=target_session,
        research_cutoff=research_cutoff,
        sessions=sessions,
        package_sha256=package_sha256,
        offset=offset,
        limit=limit,
        structure_symbol=symbol,
        structure_changes=True,
        structure_type=structure_type,
        material_changes_only=True,
        include_source_symbol_change_summary=True,
    )
    changes = d1["historical_anchor_changes"]
    source_symbol_summary = changes.get("source_symbol_material_summary")
    if (
        not isinstance(source_symbol_summary, Mapping)
        or len(source_symbol_summary) != d1["source_pool_count"]
        or not changes["material_change_partition_closed"]
        or not d1["source_pool_partition_closed"]
    ):
        raise SourceObservationError("D1材料变化来源池分母未闭合")

    snapshots = [
        read_rotation_snapshot_members(config, snapshot_id=snapshot_id, board_kind=kind)
        for kind, snapshot_id in (
            ("INDUSTRY", industry_snapshot_id),
            ("CONCEPT", concept_snapshot_id),
        )
    ]
    if any(
        snapshot["namespace_id"] != d1["source"]["namespace_id"]
        for snapshot in snapshots
    ):
        raise SourceObservationError("板块当前成员快照与D1来源namespace不一致")
    board_rows, mapped_by_kind = _d1_material_board_summaries(
        snapshots, source_symbol_summary
    )
    if board_kind is not None:
        board_rows = [row for row in board_rows if row["board_kind"] == board_kind]
    if board_native_id is not None:
        board_rows = [
            row for row in board_rows if row["board_native_id"] == board_native_id
        ]
        if not board_rows:
            raise SourceObservationError("所选精确板块不在当前成员快照中")
    selected_board = board_rows[0] if board_native_id is not None else None
    board_page = board_rows[offset : offset + limit]
    member_page = (
        selected_board["material_change_stocks"][offset : offset + limit]
        if selected_board is not None
        else []
    )

    group_counts = Counter(item["group"] for item in source_symbol_summary.values())
    if sum(group_counts.values()) != d1["source_pool_count"]:
        raise SourceObservationError("D1股票归类分母未守恒")
    kind_summaries = {snapshot["board_kind"]: snapshot for snapshot in snapshots}
    market_summary = {
        "source_pool_symbol_count": d1["source_pool_count"],
        "evaluated_source_symbol_count": d1["evaluated_source_symbol_count"],
        "historical_anchor_count": changes["effective_old_anchor_count"],
        "historical_anchor_count_by_type": changes[
            "effective_old_anchor_count_by_type"
        ],
        "material_change_anchor_count": changes["material_change_count"],
        "material_change_distinct_symbol_count": changes[
            "material_change_distinct_source_symbol_count"
        ],
        "material_change_status_counts": changes["material_change_status_counts"],
        "material_change_status_counts_by_type": changes[
            "material_change_status_counts_by_type"
        ],
        "source_symbol_group_counts": dict(sorted(group_counts.items())),
        "source_symbol_group_partition_closed": sum(group_counts.values())
        == d1["source_pool_count"],
        "unmapped_source_symbol_count_by_snapshot_kind": {
            kind: len(set(source_symbol_summary) - mapped_by_kind.get(kind, set()))
            for kind in ("INDUSTRY", "CONCEPT")
        },
        "unmapped_source_symbol_count_across_snapshots": len(
            set(source_symbol_summary) - set().union(*mapped_by_kind.values())
        ),
        "board_material_counts_are_non_additive": True,
    }
    comparison = _d1_material_outcomes(
        config,
        target_session=target_session,
        research_cutoff=research_cutoff,
        package_sha256=package_sha256,
        horizon=horizon,
        source=d1["source"],
        source_symbol_summary=source_symbol_summary,
    )
    selected_stock_summary = (
        source_symbol_summary.get(symbol) if symbol is not None else None
    )
    selected_stock_detail = d1.get("selected_source_symbol_detail")
    selected_stock = None
    if symbol is not None:
        memberships = []
        for snapshot in snapshots:
            for board_id, board in sorted(snapshot["boards"].items()):
                if symbol in board["members"]:
                    memberships.append(
                        {
                            "board_kind": snapshot["board_kind"],
                            "board_native_id": board_id,
                            "board_name": board["name"],
                            "snapshot_id": snapshot["snapshot"]["snapshot_id"],
                            "membership_status": (
                                "NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT"
                            ),
                        }
                    )
        selected_stock = {
            "source_symbol": symbol,
            "source_pool_status": (
                "AVAILABLE"
                if selected_stock_summary is not None
                and selected_stock_detail.get("source_symbol") == symbol
                else selected_stock_detail.get("status")
            ),
            "material_group": selected_stock_summary.get("group")
            if selected_stock_summary
            else "NOT_IN_SOURCE_POOL",
            "source_material_summary": selected_stock_summary,
            "current_snapshot_memberships": memberships,
            "selected_board_membership_status": (
                "MEMBER_OF_SELECTED_CURRENT_SNAPSHOT"
                if selected_board
                and any(
                    row["board_kind"] == selected_board["board_kind"]
                    and row["board_native_id"] == selected_board["board_native_id"]
                    for row in memberships
                )
                else "NOT_MEMBER_OF_SELECTED_CURRENT_SNAPSHOT"
                if selected_board
                else "NOT_REQUESTED"
            ),
            "formal_eligibility": "UNKNOWN/NOT_EVALUATED",
            "pit_status": "NON_PIT_RETROSPECTIVE",
            "d1_detail": selected_stock_detail,
        }

    return {
        "ok": True,
        "view": "d1-material",
        "scope": "NON_PIT_DESCRIPTIVE_NOT_RETURN_VALIDATION",
        "qualification_scope": "SOURCE_D1_MATERIAL_CHANGE_NOT_FORMAL_D2_OR_TRADING_SIGNAL",
        "target_session": target_session.isoformat(),
        "research_cutoff": research_cutoff.isoformat(),
        "package_sha256": package_sha256,
        "profile_revision": "NOT_REQUIRED_FOR_D1_RAW_MATERIAL_VIEW",
        "d1_history_session_count": len(sessions),
        "d1_history_start": sessions[0].isoformat(),
        "d1_history_end": sessions[-1].isoformat(),
        "source": {
            key: d1["source"].get(key)
            for key in (
                "namespace_id",
                "package_sha256",
                "source_content_id",
                "source_capture_id",
                "source_raw_digest",
                "raw_row_count",
                "capture_expected_row_count",
                "capture_read_started_at",
                "capture_validated_at",
                "source_time_quality",
                "source_clock_role",
            )
        },
        "market": market_summary,
        "snapshots": [
            {
                "board_kind": snapshot["board_kind"],
                "snapshot": snapshot["snapshot"],
                "membership_status": ("NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT"),
            }
            for snapshot in snapshots
        ],
        "boards": {
            "offset": offset,
            "limit": limit,
            "total_count": len(board_rows),
            "next_offset": offset + limit if offset + limit < len(board_rows) else None,
            "summaries": [
                {
                    key: value
                    for key, value in row.items()
                    if key != "material_change_stocks"
                }
                for row in board_page
            ],
            "selected": (
                {
                    key: value
                    for key, value in selected_board.items()
                    if key != "material_change_stocks"
                }
                if selected_board
                else None
            ),
            "selected_material_stocks": member_page,
            "selected_material_stock_count": len(
                selected_board["material_change_stocks"]
            )
            if selected_board
            else 0,
        },
        "selected_structure_type": structure_type or "ALL",
        "selected_stock": selected_stock,
        "d1": {
            "status": d1["status"],
            "contract": d1["contract"],
            "research_mode": d1["research_mode"],
            "price_coordinate": d1["price_coordinate"],
            "historical_anchor_changes": changes,
            "material_change_page": d1["page"],
            "formal_downstream": d1["formal_downstream"],
            "execution": d1["execution"],
        },
        "outcome_comparison": comparison,
        "execution": {
            "database_target": "127.0.0.1:5432/stock_a",
            "database_schema": "stock",
            "database_role": "stock_a",
            "database_mode": "READ_ONLY",
            "database_write_attempted": False,
            "ddl_executed": False,
            "dml_executed": False,
            "d1_full_pool_scan_passes": d1["execution"]["full_pool_scan_passes"],
            "wall_seconds": round(time.perf_counter() - started_wall, 6),
        },
    }


def read_observation_board_details(
    config: dict[str, Any],
    day: Mapping[str, Any],
    *,
    snapshot_id: str,
    board_kind: str,
    board_native_id: str,
    package_sha256: str,
    profile_revision: str,
    research_cutoff: date,
    rotation_start: date,
    profile_contract: dict[str, Any],
    loo_symbol: str | None = None,
) -> dict[str, Any]:
    """按精确板块复用完整同类成员计算Native、LOO和后验Rotation。"""
    if day["seed"].get("status") == "UNKNOWN_DATA":
        return {"status": "UNKNOWN_DATA", "reason": day["seed"]["reason"]}
    snapshot = read_rotation_snapshot_members(
        config, snapshot_id=snapshot_id, board_kind=board_kind
    )
    if snapshot["namespace_id"] != day["namespace_id"]:
        raise SourceObservationError("板块快照与来源池namespace不一致")
    peer_members = {key: row["members"] for key, row in snapshot["boards"].items()}
    if board_native_id not in peer_members:
        return {"status": "UNKNOWN_DATA", "reason": "BOARD_NOT_IN_EXACT_SNAPSHOT"}
    if day["seed"].get("stored_profile_status") == "MISSING_RAW_RECOMPUTED":
        members = peer_members[board_native_id]
        native = _board_native(day["seed"], members, peer_members, board_native_id)
        loo = {"state": "NOT_REQUESTED"}
        if loo_symbol is not None:
            if loo_symbol not in members:
                loo = {"state": "NOT_APPLICABLE", "source_symbol": loo_symbol}
            else:
                reduced = {
                    key: [symbol for symbol in values if symbol != loo_symbol]
                    for key, values in peer_members.items()
                }
                after = _board_native(
                    day["seed"], reduced[board_native_id], reduced, board_native_id
                )
                loo = {
                    "state": "APPLIED",
                    "source_symbol": loo_symbol,
                    "before": native,
                    "after": after,
                    "differences": _loo_differences(native, after),
                }
        rotation = read_retrospective_rotation(
            config,
            snapshot_id=snapshot_id,
            board_kind=board_kind,
            board_native_id=board_native_id,
            start_session=rotation_start,
            target_session=day["session"],
            research_cutoff=research_cutoff,
            package_sha256=package_sha256,
        )
        return {
            "status": "COMPUTED",
            "native": {"native": native, "loo": loo},
            "strength": {
                "status": "UNKNOWN_DATA",
                "reason": "STORED_PROFILE_SNAPSHOT_MISSING",
            },
            "rotation": rotation,
            "membership_status": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
        }
    strength = read_tdx_sector_strength(
        config,
        snapshot_id=snapshot_id,
        board_kind=board_kind,
        board_native_id=None,
        target_session=day["session"],
        research_cutoff=research_cutoff,
        package_sha256=package_sha256,
        profile_revision=profile_revision,
        changed_only=False,
        sort_by="delta20",
        direction="STRONGER",
        offset=0,
        limit=1,
        member_offset=0,
        member_limit=0,
        include_all_boards=True,
        include_all_member_symbols=True,
        include_cross_period_inputs=True,
    )
    if strength["namespace_id"] != day["namespace_id"]:
        raise SourceObservationError("板块强弱与来源池namespace不一致")
    native = read_board_seed_facts(
        config,
        day["seed"],
        snapshot_id=snapshot_id,
        board_kind=board_kind,
        board_native_id=board_native_id,
        peer_members=peer_members,
        cross_period_inputs=strength["cross_period_inputs_for_linkage"],
        cross_period_sessions={
            "target": strength["target_session"],
            "three_official_sessions_before": strength[
                "three_official_sessions_before"
            ],
            "previous_official_session": strength["window_sessions"][-2],
        },
        loo_symbol=loo_symbol,
    )
    rotation = read_retrospective_rotation(
        config,
        snapshot_id=snapshot_id,
        board_kind=board_kind,
        board_native_id=board_native_id,
        start_session=rotation_start,
        target_session=day["session"],
        research_cutoff=research_cutoff,
        package_sha256=package_sha256,
        profile_revision=profile_revision,
        profile_contract=profile_contract,
        profile_history_session_count=day["seed"]["history_session_count"],
    )
    return {
        "status": "COMPUTED",
        "strength": strength["all_boards_for_linkage"],
        "native": native,
        "rotation": rotation,
        "membership_status": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
    }


def attach_observation_outcomes(
    config: Mapping[str, Any],
    days: list[dict[str, Any]],
    observation: dict[str, Any],
    *,
    research_cutoff: date,
    package_sha256: str,
    horizon: int,
    history_cache: Mapping[tuple[date, str], dict[str, Any]] | None = None,
    read_metrics: dict[str, int] | None = None,
) -> None:
    """对完整事件组读取同包后继；后继不参与事件选择。"""
    if type(horizon) is not int or not 1 <= horizon <= 10:
        raise ValueError("后续会话数须为1至10")
    _, _, _, calendar = _load_calendar(research_cutoff)
    available = tuple(day for day in calendar if day <= research_cutoff)
    day_index = {day["session"]: day for day in days}
    histories = {}
    for event in observation["events"]:
        target, symbol = event["session"], event["source_symbol"]
        key = (target, symbol)
        if key not in histories and history_cache is not None and key in history_cache:
            histories[key] = history_cache[key]
            if read_metrics is not None:
                read_metrics["event_cache_hits"] += 1
        if key not in histories:
            if read_metrics is not None:
                read_metrics["event_stock_reads"] += 1
            start = available.index(target)
            sessions = available[start : start + horizon + 1]
            history = read_raw_daily_history_stock(
                config,
                source_symbol=symbol,
                target_session=target,
                package_sha256=package_sha256,
                sessions=sessions,
                expected_namespace_id=day_index[target]["namespace_id"],
                starts_at_target=True,
                research_cutoff=research_cutoff,
            )
            rows = history["history_rows"]
            birth = next(row for row in rows if row["session_date"] == target)
            safe_rows = []
            for row in rows:
                if row["session_date"] != target and (
                    not birth.get("source_entry_sha256")
                    or row.get("source_entry_sha256")
                    != birth.get("source_entry_sha256")
                    or row.get("source_content_id") != birth.get("source_content_id")
                    or row.get("price_coordinate") != "RAW_UNADJUSTED"
                ):
                    safe_rows.append(
                        {
                            "session_date": row["session_date"],
                            "bar_status": "FOLLOWUP_COORDINATE_OR_SOURCE_MISMATCH",
                        }
                    )
                else:
                    safe_rows.append(row)
            histories[key] = post_signal_price_facts(sessions, safe_rows, horizon)
        facts = histories[key]
        line = None
        signal = event["signal"]
        if signal in {"LAUNCH_CONFIRM", "RECOVERY_TURN"}:
            items = day_index[target]["scenarios"][signal]["results"]
            raw = next(item for item in items if item["source_symbol"] == symbol)
            candidate = raw["failure_line_candidate"]["value"]
            if candidate is not None:
                line = Fraction(
                    int(candidate["numerator"]), int(candidate["denominator"])
                )
        comparisons = []
        for point in facts["points"]:
            close = point["ohlc"]["close"]
            if point["session"] is None:
                comparisons.append(
                    {
                        "session": None,
                        "state": "NOT_AVAILABLE",
                        "reason": "NO_FUTURE_SESSION_IN_SAVED_WINDOW",
                    }
                )
                continue
            if line is None or close["quality"] != "OBSERVED":
                comparisons.append(
                    {
                        "session": point["session"],
                        "state": "UNKNOWN",
                        "reason": (
                            (
                                "FAILURE_LINE_UNAVAILABLE"
                                if signal in {"LAUNCH_CONFIRM", "RECOVERY_TURN"}
                                else "FAILURE_LINE_NOT_DEFINED_FOR_SIGNAL"
                            )
                            if line is None
                            else close.get("reason") or "FOLLOWUP_CLOSE_UNKNOWN"
                        ),
                    }
                )
                continue
            value = close["value"]
            value = value["exact"] if isinstance(value, dict) else value
            result = line_breached(
                FieldValue(Fraction(value)),
                FieldValue(line),
                ordinal=point["offset"],
                effective_ordinal=1,
            )
            comparisons.append(
                {
                    "session": point["session"],
                    "state": "TRUE" if result.value else "FALSE",
                    "reason": None,
                }
            )
        event["post_signal_price_facts"] = facts
        event["failure_line_comparisons"] = comparisons
        first_touch, first_touch_upper_bound = _first_known_event(
            comparisons,
            state_key="state",
            event_state="TRUE",
            unknown_states={"UNKNOWN", "NOT_AVAILABLE"},
        )
        event["first_proved_failure_session"] = first_touch
        event["first_candidate_line_touch_session"] = first_touch
        event["first_candidate_line_touch_upper_bound"] = first_touch_upper_bound
        event["failure_line_status"] = (
            "TRUE"
            if first_touch_upper_bound is not None
            else "UNKNOWN"
            if not comparisons
            or any(
                point["state"] in {"UNKNOWN", "NOT_AVAILABLE"} for point in comparisons
            )
            else "FALSE"
        )


def attach_comparison_continuity(
    comparisons: list[dict[str, Any]], observation: Mapping[str, Any]
) -> None:
    """按同一事件组汇总后续状态；组间可重叠，锚点仍各自计权。"""
    grouped: dict[tuple[date, str], list[dict[str, Any]]] = {}
    for event in observation["events"]:
        base = f"{event['signal']}:{event['transition']}"
        names = {base}
        for membership in event.get("memberships", []):
            kind = membership["board_kind"]
            names.add(f"{base}|{kind}_MAPPED_NON_PIT")
            names.add(f"{base}|BOARD:{kind}:{membership['board_native_id']}")
        for name in names:
            grouped.setdefault((event["session"], name), []).append(event)
    for comparison in comparisons:
        events = grouped.get((comparison["session"], comparison["group"]))
        if events is None:
            continue
        n = comparison["horizon"]
        states: Counter[str] = Counter()
        state_unknown_reasons: Counter[str] = Counter()
        support_states: Counter[str] = Counter()
        lines: Counter[str] = Counter()
        line_unknown_reasons: Counter[str] = Counter()
        joint: Counter[str] = Counter()
        joint_signs: Counter[str] = Counter()
        first_false_by_horizon = 0
        line_touched_by_horizon = 0
        for event in events:
            points = event["source_state_followup"]["points"]
            point = points[n - 1] if len(points) >= n else {}
            state = point.get("state") or point.get("classification") or "NOT_AVAILABLE"
            states[state] += 1
            if state in {
                "UNKNOWN",
                "NOT_COMPARABLE",
                "NOT_EVALUABLE",
                "OTHER_EXPLAINED",
            }:
                state_unknown_reasons[
                    point.get("reason") or "SOURCE_STATE_UNKNOWN"
                ] += 1
            if event["signal"] == "D1_SUPPORT_CHANGE":
                support = (point.get("observation") or {}).get(
                    "support_observation"
                ) or {}
                support_state = (
                    support.get("state")
                    if support.get("quality") == "OBSERVED"
                    else None
                )
                support_states[
                    "NOT_AVAILABLE"
                    if state == "NOT_AVAILABLE"
                    else support_state or "UNKNOWN"
                ] += 1
            if event["transition"] == "TRUE_TO_FALSE" or any(
                row.get("state") == "FALSE" for row in points[:n]
            ):
                first_false_by_horizon += 1
            line_points = event["failure_line_comparisons"]
            if any(row["state"] == "TRUE" for row in line_points[:n]):
                line_touched_by_horizon += 1
            line = (
                line_points[n - 1]["state"]
                if len(line_points) >= n
                else "NOT_AVAILABLE"
            )
            lines[line] += 1
            if line == "UNKNOWN":
                line_unknown_reasons[
                    line_points[n - 1].get("reason") or "FAILURE_LINE_UNKNOWN"
                ] += 1
            facts = event["post_signal_price_facts"]["points"]
            change = (
                facts[n - 1]["raw_close_relative_change"] if len(facts) >= n else {}
            )
            if change.get("quality") == "OBSERVED" and state in {
                "TRUE",
                "FALSE",
                "CHANGED",
                "UNCHANGED",
            }:
                value = Fraction(change["value"]["exact"])
                joint[f"{state}_{'POSITIVE' if value > 0 else 'NON_POSITIVE'}"] += 1
                sign = "POSITIVE" if value > 0 else "NEGATIVE" if value < 0 else "ZERO"
                joint_signs[f"{state}_{sign}"] += 1
        comparison["event_continuity"] = {
            "state_counts": dict(states),
            "state_unknown_reasons": dict(state_unknown_reasons),
            "d1_support_state_counts": dict(support_states),
            "candidate_line_touch_counts": dict(lines),
            "candidate_line_unknown_reasons": dict(line_unknown_reasons),
            "state_and_price_counts": dict(joint),
            "state_and_price_sign_counts": dict(joint_signs),
            "source_false_by_horizon_count": (
                None
                if events[0]["signal"] == "D1_SUPPORT_CHANGE"
                else first_false_by_horizon
            ),
            "source_false_reason": (
                "D1_SUPPORT_IS_NOT_BOOLEAN"
                if events[0]["signal"] == "D1_SUPPORT_CHANGE"
                else None
            ),
            "candidate_line_touched_by_horizon_count": line_touched_by_horizon,
            "scope": "EVENT_WEIGHTED_NOT_FORMAL_EXIT",
        }


def _comparison_price_point(facts: Mapping[str, Any], horizon: int) -> dict[str, Any]:
    point = facts["points"][horizon - 1]
    value = dict(point["raw_close_relative_change"])
    if value.get("reason") == "T0_OR_ENDPOINT_BAR_UNKNOWN":
        base = facts["t0"]["ohlc"]
        endpoint = point["ohlc"]
        if base.get("quality") != "OBSERVED":
            value["reason"] = "T0:" + str(base.get("reason") or "RAW_BAR_UNKNOWN")
        elif endpoint.get("quality") != "OBSERVED":
            value["reason"] = "FOLLOWUP:" + str(
                endpoint.get("reason") or "RAW_BAR_UNKNOWN"
            )
    return value


def compare_source_outcomes(
    config: Mapping[str, Any],
    days: list[dict[str, Any]],
    observation: Mapping[str, Any],
    *,
    research_cutoff: date,
    package_sha256: str,
    horizon: int,
    history_cache: dict[tuple[date, str], dict[str, Any]] | None = None,
    read_metrics: dict[str, int] | None = None,
) -> list[dict[str, Any]]:
    """对同日事件与双D0已知未触发对象描述后验价格，不用于筛选T0。"""
    if type(horizon) is not int or not 1 <= horizon <= 10:
        raise ValueError("后续会话数须为1至10")
    _, _, _, calendar = _load_calendar(research_cutoff)
    available = tuple(day for day in calendar if day <= research_cutoff)
    output = []
    for day in days:
        target = day["session"]
        start = available.index(target)
        sessions = available[start : start + horizon + 1]
        histories = {}
        cached = history_cache is not None and all(
            (target, symbol) in history_cache for symbol in day["states"]
        )
        offset = 0
        if read_metrics is not None:
            read_metrics[
                "comparison_cache_reuses" if cached else "comparison_pool_scans"
            ] += 1
        while not cached:
            if read_metrics is not None:
                read_metrics["comparison_page_reads"] += 1
            page = read_raw_daily_history_page(
                config,
                target_session=sessions[-1],
                package_sha256=package_sha256,
                sessions=sessions,
                offset=offset,
                limit=100,
                market="ALL",
                board="ALL",
            )
            if page["namespace_id"] != day["namespace_id"]:
                raise SourceObservationError("后继来源namespace不一致")
            for candidate in page["candidates"]:
                symbol = candidate["source_symbol"]
                rows = candidate["history_rows"]
                birth = next(
                    (row for row in rows if row["session_date"] == target), None
                )
                safe = []
                for row in rows:
                    if row["session_date"] != target and (
                        birth is None
                        or not birth.get("source_entry_sha256")
                        or row.get("source_entry_sha256")
                        != birth.get("source_entry_sha256")
                        or row.get("source_content_id")
                        != birth.get("source_content_id")
                        or row.get("price_coordinate") != "RAW_UNADJUSTED"
                    ):
                        safe.append(
                            {
                                "session_date": row["session_date"],
                                "bar_status": "FOLLOWUP_COORDINATE_OR_SOURCE_MISMATCH",
                            }
                        )
                    else:
                        safe.append(row)
                facts = post_signal_price_facts(sessions, safe, horizon)
                histories[symbol] = _comparison_price_point(facts, horizon)
                if history_cache is not None:
                    history_cache[(target, symbol)] = facts
            offset = page["next_offset"]
            if offset is None:
                break
        if history_cache is not None:
            for symbol in day["states"]:
                facts = history_cache.get((target, symbol))
                if facts is None:
                    # 最新会话可能没有T0代码；补读该代码，不能把其T0误判为缺失。
                    if read_metrics is not None:
                        read_metrics["comparison_missing_stock_reads"] += 1
                    history = read_raw_daily_history_stock(
                        config,
                        source_symbol=symbol,
                        target_session=target,
                        package_sha256=package_sha256,
                        sessions=sessions,
                        expected_namespace_id=day["namespace_id"],
                        starts_at_target=True,
                        research_cutoff=research_cutoff,
                    )
                    rows = history["history_rows"]
                    birth = next(
                        (row for row in rows if row["session_date"] == target), None
                    )
                    safe = [
                        row
                        if row["session_date"] == target
                        or (
                            birth is not None
                            and birth.get("source_entry_sha256")
                            and row.get("source_entry_sha256")
                            == birth.get("source_entry_sha256")
                            and row.get("source_content_id")
                            == birth.get("source_content_id")
                            and row.get("price_coordinate") == "RAW_UNADJUSTED"
                        )
                        else {
                            "session_date": row["session_date"],
                            "bar_status": "FOLLOWUP_COORDINATE_OR_SOURCE_MISMATCH",
                        }
                        for row in rows
                    ]
                    facts = post_signal_price_facts(sessions, safe, horizon)
                    history_cache[(target, symbol)] = facts
                histories[symbol] = _comparison_price_point(facts, horizon)
        groups: dict[str, list[str]] = {}
        for name in ("LAUNCH_CONFIRM", "RECOVERY_TURN", "s1", "s2", "seed"):
            groups[f"{name}:FALSE_TO_TRUE"] = []
        for name in ("s1", "s2", "seed"):
            groups[f"{name}:TRUE_TO_FALSE"] = []
        groups["D1_SUPPORT_CHANGE:CHANGED"] = []
        for event in observation["events"]:
            if event["session"] == target:
                group = f"{event['signal']}:{event['transition']}"
                groups.setdefault(group, []).append(event["source_symbol"])
                for kind in ("INDUSTRY", "CONCEPT"):
                    if any(
                        membership["board_kind"] == kind
                        for membership in event.get("memberships", [])
                    ):
                        groups.setdefault(f"{group}|{kind}_MAPPED_NON_PIT", []).append(
                            event["source_symbol"]
                        )
                for membership in event.get("memberships", []):
                    board_group = (
                        f"{membership['board_kind']}:{membership['board_native_id']}"
                    )
                    groups.setdefault(f"{group}|BOARD:{board_group}", []).append(
                        event["source_symbol"]
                    )
        groups["CONTROL_BOTH_D0_KNOWN_FALSE"] = [
            symbol
            for symbol, state in day["states"].items()
            if state["LAUNCH_CONFIRM"] == state["RECOVERY_TURN"] == "FALSE"
        ]
        changed_symbols = {
            event["source_symbol"]
            for event in observation["events"]
            if event["session"] == target
        }
        groups["CONTROL_D0_FALSE_NO_DISCOVERED_EVENT"] = [
            symbol
            for symbol in groups["CONTROL_BOTH_D0_KNOWN_FALSE"]
            if symbol not in changed_symbols
        ]
        memberships = observation.get("memberships_by_symbol", {})
        for control in (
            "CONTROL_BOTH_D0_KNOWN_FALSE",
            "CONTROL_D0_FALSE_NO_DISCOVERED_EVENT",
        ):
            for kind in ("INDUSTRY", "CONCEPT"):
                groups[f"{control}|{kind}_MAPPED_NON_PIT"] = [
                    symbol
                    for symbol in groups[control]
                    if any(
                        row["board_kind"] == kind for row in memberships.get(symbol, [])
                    )
                ]
            for symbol in groups[control]:
                for membership in memberships.get(symbol, []):
                    board_group = (
                        f"{membership['board_kind']}:{membership['board_native_id']}"
                    )
                    groups.setdefault(f"{control}|BOARD:{board_group}", []).append(
                        symbol
                    )
        for group, symbols in groups.items():

            def summarize(items):
                values = []
                reasons: Counter[str] = Counter()
                for symbol in items:
                    fact = histories.get(symbol)
                    if fact is None or fact["quality"] != "OBSERVED":
                        reasons[
                            (fact or {}).get("reason") or "FOLLOWUP_POOL_SYMBOL_MISSING"
                        ] += 1
                    else:
                        values.append(Fraction(fact["value"]["exact"]))
                positive = sum(value > 0 for value in values)
                return {
                    "count": len(items),
                    "known_count": len(values),
                    "unknown_count": len(items) - len(values),
                    "unknown_reasons": dict(reasons),
                    "median": str(median(values)) if values else None,
                    "positive_count": positive,
                    "non_positive_count": len(values) - positive,
                    "positive_fraction": str(Fraction(positive, len(values)))
                    if values
                    else None,
                }

            event_stats = summarize(symbols)
            stock_stats = summarize(list(dict.fromkeys(symbols)))
            values_count = event_stats["known_count"]
            output.append(
                {
                    "session": target,
                    "group": group,
                    "horizon": horizon,
                    "event_or_control_count": len(symbols),
                    "distinct_symbol_count": len(set(symbols)),
                    "known_count": event_stats["known_count"],
                    "unknown_count": event_stats["unknown_count"],
                    "unknown_reasons": event_stats["unknown_reasons"],
                    "median": event_stats["median"],
                    "positive_fraction": event_stats["positive_fraction"],
                    "event_weighted": event_stats,
                    "distinct_stock_weighted": stock_stats,
                    "weighting_note": "事件级按事件计权；股票级按同日来源代码去重；各组允许重叠",
                    "status": "COMPUTED" if values_count else "NOT_EVALUABLE",
                    "scope": "NON_PIT_DESCRIPTIVE_NOT_RETURN_VALIDATION",
                    "control_definition": (
                        "D0两场景均已知FALSE；可与其他事件类别重叠"
                        if group.startswith("CONTROL_BOTH_D0_KNOWN_FALSE")
                        else "D0两场景均已知FALSE，且本次未发现任何已证变化；未发现不代表全部模式已知FALSE"
                        if group.startswith("CONTROL_D0_FALSE_NO_DISCOVERED_EVENT")
                        else None
                    ),
                    "market_context": day["market"],
                }
            )
    return output
