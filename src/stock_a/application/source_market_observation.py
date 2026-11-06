"""来源级市场变化与后验观察所需的只读事实归并。"""

from __future__ import annotations

from collections import Counter
from datetime import date
from fractions import Fraction
from statistics import median
from typing import Any, Mapping

from stock_a.application.source_d0_raw import query_source_d0_raw_launch
from stock_a.application.source_market_context import (
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
from stock_a.research.entry_context.sessions import _load_calendar
from stock_a.research.entry_history.history_timeline import post_signal_price_facts
from stock_a.research.admission import line_breached
from stock_a.domain import FieldValue
from stock_a.storage.postgres.source_raw import (
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


def read_market_observation_sessions(
    config: Mapping[str, Any],
    *,
    sessions: tuple[date, ...],
    research_cutoff: date,
    package_sha256: str,
    profile_revision: str,
    profile_contract: dict[str, Any],
    history_session_count: int,
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
        market = read_source_market_context(
            config,
            target_session=session,
            research_cutoff=research_cutoff,
            package_sha256=package_sha256,
        )
        source_version = scenarios["LAUNCH_CONFIRM"]["source_version"]
        if (
            market.cohort.namespace_id != namespace
            or market.cohort.source_content_id != source_version["source_content_id"]
            or market.cohort.source_capture_id != source_version["source_capture_id"]
            or market.pg_result["target_raw_digest"]
            != source_version["source_raw_digest"]
        ):
            raise SourceObservationError("市场宽度与D0来源捕获身份不一致")
        outputs.append(
            {
                "session": session,
                "namespace_id": namespace,
                "market": source_market_context_payload(market),
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
) -> None:
    """仅投影请求中已经计算的后续三态，不推造正式退出状态。"""
    for event in observation["events"]:
        signal = event["signal"]
        if signal == "D1_SUPPORT_CHANGE":
            points = []
            if d1_by_session is not None:
                for day in days:
                    if day["session"] <= event["session"]:
                        continue
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
            event["source_state_followup"] = {
                "status": "COMPUTED" if points else "NOT_EVALUATED",
                "points": points,
                "formal_exit_status": "NOT_EVALUATED",
            }
            continue
        points = []
        for day in days:
            if day["session"] <= event["session"]:
                continue
            state = day["states"].get(event["source_symbol"], {}).get(signal)
            points.append(
                {
                    "session": day["session"],
                    "state": state or "UNKNOWN",
                    "reason": (
                        "SOURCE_SYMBOL_NOT_IN_POOL"
                        if state is None
                        else "SOURCE_SIGNAL_FALSE_NOT_FORMAL_EXIT"
                        if state == "FALSE"
                        else "SOURCE_STATE_UNKNOWN"
                        if state == "UNKNOWN"
                        else "SOURCE_SIGNAL_TRUE"
                    ),
                }
            )
        event["source_state_followup"] = {
            "status": "COMPUTED" if points else "NOT_EVALUATED",
            "points": points,
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
        if key not in histories:
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
            if line is None or close["quality"] != "OBSERVED":
                comparisons.append(
                    {
                        "session": point["session"],
                        "state": "UNKNOWN",
                        "reason": "FAILURE_LINE_OR_CLOSE_UNKNOWN",
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
        event["first_proved_failure_session"] = next(
            (point["session"] for point in comparisons if point["state"] == "TRUE"),
            None,
        )
        event["failure_line_status"] = (
            "TRUE"
            if event["first_proved_failure_session"] is not None
            else "UNKNOWN"
            if not comparisons
            or any(point["state"] == "UNKNOWN" for point in comparisons)
            else "FALSE"
        )


def compare_source_outcomes(
    config: Mapping[str, Any],
    days: list[dict[str, Any]],
    observation: Mapping[str, Any],
    *,
    research_cutoff: date,
    package_sha256: str,
    horizon: int,
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
        offset = 0
        while True:
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
                histories[symbol] = post_signal_price_facts(sessions, safe, horizon)[
                    "points"
                ][-1]["raw_close_relative_change"]
            offset = page["next_offset"]
            if offset is None:
                break
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
            values = []
            reasons: Counter[str] = Counter()
            for symbol in symbols:
                fact = histories.get(symbol)
                if fact is None or fact["quality"] != "OBSERVED":
                    reasons[
                        (fact or {}).get("reason") or "FOLLOWUP_POOL_SYMBOL_MISSING"
                    ] += 1
                else:
                    values.append(Fraction(fact["value"]["exact"]))
            output.append(
                {
                    "session": target,
                    "group": group,
                    "horizon": horizon,
                    "event_or_control_count": len(symbols),
                    "distinct_symbol_count": len(set(symbols)),
                    "known_count": len(values),
                    "unknown_count": len(symbols) - len(values),
                    "unknown_reasons": dict(reasons),
                    "median": str(median(values)) if values else None,
                    "positive_fraction": str(
                        Fraction(sum(value > 0 for value in values), len(values))
                    )
                    if values
                    else None,
                    "status": "COMPUTED" if values else "NOT_EVALUABLE",
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
