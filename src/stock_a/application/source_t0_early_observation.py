"""从已存T0、Seed、板块成员和PULSE事实生产可证伪的板内观察。"""

from __future__ import annotations

from collections import defaultdict
from datetime import date
import time
from typing import Any, Mapping

from stock_a.application.research_window import resolve_pg_history_window
from stock_a.storage.postgres.research_enrollment import (
    read_enrollment_followups,
)
from stock_a.storage.postgres.sector_pulse_facts import (
    read_source_sector_pulse_for_boards,
)
from stock_a.storage.postgres.source_launch_watch_evidence import (
    read_source_launch_watch_board_rps_comparisons,
)
from stock_a.storage.postgres.source_raw import read_source_version_context
from stock_a.storage.postgres.t0_early_observations import (
    persist_source_t0_early_observation_facts,
    read_source_t0_early_observation_inputs,
    read_source_t0_pulse_transition_facts,
)


HYPOTHESIS_VERSION = "EARLY_BOARD_RELATIVE_T0_R1"
_RPS_METRICS = ("RPS5", "RPS20")
_PULSE_TRANSITION_STATES = {
    "PULSE_APPEARED",
    "PULSE_PERSISTED",
    "PULSE_CEASED",
    "PULSE_NOT_PRESENT_BOTH",
    "PULSE_TRANSITION_UNKNOWN",
}


class SourceT0EarlyObservationError(ValueError):
    """已存T0板内观察生产边界错误。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def _metric_position(metric: Mapping[str, Any]) -> str:
    first = metric.get("rank_first")
    last = metric.get("rank_last")
    valid_count = metric.get("valid_member_count")
    if (
        metric.get("status") != "AVAILABLE"
        or type(first) is not int
        or type(last) is not int
        or type(valid_count) is not int
        or valid_count <= 0
    ):
        return "UNKNOWN"
    midpoint = (valid_count + 1) // 2
    if last <= midpoint:
        return "FRONT_HALF"
    if first > midpoint:
        return "BACK_HALF"
    return "MIDPOINT_TIE"


def _relative_position(rps5_position: str, rps20_position: str) -> str:
    if "MIDPOINT_TIE" in {rps5_position, rps20_position}:
        return "MIDPOINT_TIE"
    if rps5_position == rps20_position == "FRONT_HALF":
        return "BOTH_FRONT_HALF"
    if rps5_position == rps20_position == "BACK_HALF":
        return "BOTH_BACK_HALF"
    if {rps5_position, rps20_position} == {"FRONT_HALF", "BACK_HALF"}:
        return "MIXED"
    return "UNKNOWN"


def _derive_interpretation(
    *,
    mapped: bool,
    d0_transition: str | None,
    pulse_status: str,
    pulse_state: str,
    transition_status: str,
    transition_state: str,
    rps5_position: str,
    rps20_position: str,
) -> dict[str, Any]:
    support: list[str] = []
    counter: list[str] = []
    unknown: list[str] = []
    d0_new = d0_transition == "FALSE_TO_TRUE"

    if not mapped:
        unknown.append("MEMBERSHIP_UNKNOWN")
    if d0_new:
        support.append("D0_FALSE_TO_TRUE")
    if pulse_state == "TRUE":
        support.append("PULSE_TRUE")
    elif pulse_state == "FALSE":
        counter.append("PULSE_FALSE")
    elif pulse_state == "UNKNOWN":
        unknown.append("PULSE_CURRENT_UNKNOWN")
    else:
        unknown.append("PULSE_NOT_COMPUTED")

    if transition_status == "AVAILABLE" and transition_state == "PULSE_APPEARED":
        support.append("PULSE_APPEARED")
    elif transition_status == "AVAILABLE" and transition_state == "PULSE_PERSISTED":
        support.append("PULSE_PERSISTED")
    elif transition_status == "AVAILABLE" and transition_state == "PULSE_CEASED":
        counter.append("PULSE_CEASED")
    elif transition_status != "AVAILABLE":
        unknown.append("PULSE_TRANSITION_NOT_COMPUTED")

    for code, position, supported, opposed in (
        ("RPS5", rps5_position, "RPS5_FRONT_HALF", "RPS5_BACK_HALF"),
        ("RPS20", rps20_position, "RPS20_FRONT_HALF", "RPS20_BACK_HALF"),
    ):
        if position == "FRONT_HALF":
            support.append(supported)
        elif position == "BACK_HALF":
            counter.append(opposed)
        elif position in {"UNKNOWN", "NOT_COMPUTED"}:
            unknown.append(f"{code}_UNKNOWN")
    relative = _relative_position(rps5_position, rps20_position)
    if relative == "MIXED":
        counter.append("RPS5_RPS20_MIXED")

    if not mapped:
        alignment = "UNKNOWN"
        research_case = "NO_BOARD_MAPPING"
    elif pulse_status != "AVAILABLE" or pulse_state in {"UNKNOWN", "NOT_COMPUTED"}:
        alignment = "UNKNOWN"
        research_case = "PULSE_UNKNOWN"
    elif d0_new and (transition_state == "PULSE_CEASED" or pulse_state == "FALSE"):
        alignment = "CONFLICT"
        research_case = "D0_PULSE_FALSE_CONFLICT"
    elif (
        d0_new
        and transition_state == "PULSE_APPEARED"
        and relative == "BOTH_FRONT_HALF"
    ):
        alignment = "SUPPORT"
        research_case = "D0_PULSE_APPEARED_FRONT"
    elif d0_new and pulse_state == "TRUE" and relative == "BOTH_FRONT_HALF":
        alignment = "SUPPORT"
        research_case = "D0_PULSE_TRUE_FRONT"
    elif d0_new and pulse_state == "TRUE" and relative == "BOTH_BACK_HALF":
        alignment = "MIXED"
        research_case = "D0_PULSE_TRUE_BACK"
    elif d0_new and pulse_state == "TRUE" and relative == "MIXED":
        alignment = "MIXED"
        research_case = "D0_PULSE_TRUE_MIXED"
    elif pulse_state == "TRUE" and relative == "BOTH_BACK_HALF":
        alignment = "CONFLICT"
        research_case = "PULSE_TRUE_BACK"
    else:
        alignment = "NEUTRAL"
        research_case = "BOARD_CONTEXT"

    if not mapped:
        next_check = "WAIT_FOR_MISSING_CONTEXT"
    elif transition_status != "AVAILABLE":
        next_check = "CHECK_NEXT_PULSE_TRANSITION"
    elif "UNKNOWN" in {rps5_position, rps20_position}:
        next_check = "CHECK_RPS_POSITION_NEXT_SESSION"
    elif alignment == "CONFLICT":
        next_check = "VERIFY_CONFLICT_RESOLUTION"
    elif relative != "UNKNOWN":
        next_check = "CHECK_RPS_POSITION_NEXT_SESSION"
    else:
        next_check = "WAIT_FOR_NEXT_STORED_SESSION"

    return {
        "relative_position": relative,
        "alignment_state": alignment,
        "research_case": research_case,
        "support_codes": sorted(set(support)),
        "counter_codes": sorted(set(counter)),
        "unknown_codes": sorted(set(unknown)),
        "next_check_code": next_check,
    }


def persist_source_t0_early_observations_from_saved(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    source_package_id: int,
    snapshot_ids: Mapping[str, int],
) -> dict[str, Any]:
    """只从已存事实生成T0板内观察；不采集或重算Raw、Seed、D0和D1。"""
    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
        or type(source_package_id) is not int
        or source_package_id <= 0
    ):
        raise SourceT0EarlyObservationError(
            "T0_EARLY_TARGET_INVALID", "T0观察目标日、截止日或来源包无效"
        )

    started = time.perf_counter()
    source_version = read_source_version_context(
        config,
        target_session=target_session,
        source_package_id=source_package_id,
    )
    if (
        source_version.get("status") != "AVAILABLE"
        or source_version.get("source_package_id") != source_package_id
        or type(source_version.get("source_capture_id")) is not int
        or source_version.get("raw_row_count")
        != source_version.get("capture_expected_row_count")
    ):
        raise SourceT0EarlyObservationError(
            "T0_EARLY_SOURCE_NOT_CLOSED", "目标日Raw来源版本不唯一或分区未闭合"
        )

    inputs = read_source_t0_early_observation_inputs(
        config,
        target_session=target_session,
        research_cutoff=research_cutoff,
        source_package_id=source_package_id,
        source_capture_id=source_version["source_capture_id"],
        expected_source_count=source_version["capture_expected_row_count"],
        snapshot_ids=snapshot_ids,
    )
    registrations = inputs["registrations"]
    boards_by_key: dict[tuple[int, str, str], dict[str, Any]] = {}
    symbols_by_board: dict[tuple[int, str, str], set[str]] = defaultdict(set)
    for registration in registrations:
        for membership in registration["memberships"]:
            key = (
                membership["snapshot_id"],
                membership["board_kind"],
                membership["board_native_id"],
            )
            boards_by_key[key] = membership
            symbols_by_board[key].add(registration["source_symbol"])

    boards = [
        {
            "snapshot_id": key[0],
            "board_kind": key[1],
            "board_native_id": key[2],
        }
        for key in sorted(boards_by_key)
    ]
    pulse_result = read_source_sector_pulse_for_boards(
        config,
        target_session=target_session,
        source_package_id=source_package_id,
        source_capture_id=source_version["source_capture_id"],
        source_pool_count=source_version["capture_expected_row_count"],
        boards=boards,
    )
    pulse_by_board = pulse_result.get("by_board", {})

    resolved, adjacent_sessions, _ = resolve_pg_history_window(
        target_session.isoformat(), 2
    )
    if resolved != target_session or len(adjacent_sessions) != 2:
        raise SourceT0EarlyObservationError(
            "T0_EARLY_CALENDAR_WINDOW_UNAVAILABLE", "目标T0没有可验证的相邻官方会话"
        )
    previous_session = adjacent_sessions[0]
    previous_source = read_source_version_context(
        config,
        target_session=previous_session,
        source_package_id=source_package_id,
    )
    previous_capture_id = (
        previous_source.get("source_capture_id")
        if previous_source.get("status") == "AVAILABLE"
        and previous_source.get("source_package_id") == source_package_id
        else None
    )
    transition_targets = [
        int(item["pulse_fact_id"])
        for item in pulse_by_board.values()
        if item.get("status") == "AVAILABLE" and type(item.get("pulse_fact_id")) is int
    ]
    transitions_by_pulse = read_source_t0_pulse_transition_facts(
        config,
        target_pulse_ids=transition_targets,
        previous_session=previous_session,
        target_session=target_session,
        source_package_id=source_package_id,
        previous_source_capture_id=previous_capture_id,
        previous_profile_revision=inputs["seed_scope"]["profile_revision"],
    )

    comparison_started = time.perf_counter()
    comparison_by_board_symbol: dict[
        tuple[tuple[int, str, str], str], dict[str, Any]
    ] = {}
    comparison_contexts = [
        (*board_key, symbol)
        for board_key in sorted(symbols_by_board)
        for symbol in sorted(symbols_by_board[board_key])
    ]
    comparison_batches = 0
    if comparison_contexts:
        compared = read_source_launch_watch_board_rps_comparisons(
            config,
            target_session=target_session,
            source_package_id=source_package_id,
            source_capture_id=source_version["source_capture_id"],
            board_symbol_contexts=comparison_contexts,
            seed_scope=inputs["seed_scope"],
        )
        comparison_batches = 1
        comparison_by_board_symbol = {
            ((snapshot_id, board_kind, board_native_id), symbol): metrics
            for (
                snapshot_id,
                board_kind,
                board_native_id,
                symbol,
            ), metrics in compared.items()
        }
    comparison_seconds = time.perf_counter() - comparison_started

    snapshot_complete = {
        kind: inputs["snapshots"][kind]["source_quality"].startswith("COMPLETE_NON_PIT")
        for kind in ("INDUSTRY", "CONCEPT")
    }
    observation_rows: list[dict[str, Any]] = []
    pulse_counts = {state: 0 for state in ("TRUE", "FALSE", "UNKNOWN", "NOT_COMPUTED")}
    transition_counts = {state: 0 for state in ("AVAILABLE", "NOT_COMPUTED", "UNKNOWN")}
    rps5_unknown_rows = 0
    rps20_unknown_rows = 0

    for registration in registrations:
        memberships = registration["memberships"]
        contexts: list[Mapping[str, Any] | None] = (
            list(memberships) if memberships else [None]
        )
        if not memberships:
            membership_status = (
                "NO_STORED_FACT" if all(snapshot_complete.values()) else "UNKNOWN"
            )
            contexts = [
                {
                    "membership_status": membership_status,
                    "membership_quality": None,
                }
            ]

        for membership in contexts:
            mapped = membership.get("catalog_id") is not None
            board_key = (
                (
                    int(membership["snapshot_id"]),
                    str(membership["board_kind"]),
                    str(membership["board_native_id"]),
                )
                if mapped
                else None
            )
            pulse = (
                pulse_by_board.get(f"{board_key[1]}:{board_key[2]}", {})
                if board_key is not None
                else {}
            )
            if pulse.get("status") == "AVAILABLE":
                pulse_status = "AVAILABLE"
                pulse_state = pulse.get("state")
                pulse_fact_id = pulse.get("pulse_fact_id")
            elif pulse.get("status") == "NOT_COMPUTED":
                pulse_status = "NOT_COMPUTED"
                pulse_state = "NOT_COMPUTED"
                pulse_fact_id = None
            else:
                pulse_status = "UNKNOWN"
                pulse_state = "UNKNOWN"
                pulse_fact_id = None
            if pulse_state not in {"TRUE", "FALSE", "UNKNOWN", "NOT_COMPUTED"}:
                pulse_state = "UNKNOWN"

            transition = (
                transitions_by_pulse.get(int(pulse_fact_id), {})
                if type(pulse_fact_id) is int
                else {}
            )
            if transition.get("status") == "AVAILABLE":
                transition_status = "AVAILABLE"
                transition_state = transition.get("transition_state")
                transition_id = transition.get("transition_id")
                transition_reasons = (
                    ["PULSE_TRANSITION_UNKNOWN"]
                    if transition_state == "PULSE_TRANSITION_UNKNOWN"
                    else []
                )
            elif transition.get("status") == "UNKNOWN":
                transition_status = "UNKNOWN"
                transition_state = "PULSE_TRANSITION_UNKNOWN"
                transition_id = None
                transition_reasons = list(transition.get("reason_codes", ()))
            elif transition.get("status") == "INCOMPLETE":
                transition_status = "UNKNOWN"
                transition_state = "PULSE_TRANSITION_UNKNOWN"
                transition_id = None
                transition_reasons = list(transition.get("reason_codes", ()))
            else:
                transition_status = "NOT_COMPUTED"
                transition_state = "NOT_COMPUTED"
                transition_id = None
                transition_reasons = [
                    "PULSE_TRANSITION_FACTS_NOT_SAVED_FOR_EXACT_INPUT"
                ]

            metric_details = {}
            for metric_name, quality_name, reason_name in (
                ("RPS5", "rps5_quality", "rps5_reason"),
                ("RPS20", "rps20_quality", "rps20_reason"),
            ):
                comparison = (
                    comparison_by_board_symbol.get(
                        (board_key, registration["source_symbol"]), {}
                    )
                    if board_key is not None
                    else {}
                )
                metric = comparison.get(metric_name, {})
                status = metric.get("status", "NOT_COMPUTED")
                metric_details[metric_name] = {
                    "quality": metric.get("quality")
                    or registration.get(quality_name)
                    or "UNKNOWN",
                    "reason": metric.get("reason") or registration.get(reason_name),
                    "rank_first": metric.get("rank_first")
                    if status == "AVAILABLE"
                    else None,
                    "rank_last": metric.get("rank_last")
                    if status == "AVAILABLE"
                    else None,
                    "valid_member_count": int(metric.get("valid_member_count") or 0),
                    "unknown_member_count": int(
                        metric.get("unknown_member_count") or 0
                    ),
                    "source_pool_member_count": int(
                        metric.get("source_pool_member_count") or 0
                    ),
                    "snapshot_member_count": int(
                        metric.get("snapshot_member_count") or 0
                    ),
                    "out_of_source_pool_member_count": int(
                        metric.get("out_of_source_pool_member_count") or 0
                    ),
                    "position": (
                        _metric_position(metric)
                        if board_key is not None
                        else "NOT_COMPUTED"
                    ),
                }

            rps5 = metric_details["RPS5"]
            rps20 = metric_details["RPS20"]
            interpretation = _derive_interpretation(
                mapped=mapped,
                d0_transition=registration.get("d0_transition"),
                pulse_status=pulse_status,
                pulse_state=pulse_state,
                transition_status=transition_status,
                transition_state=transition_state,
                rps5_position=rps5["position"],
                rps20_position=rps20["position"],
            )
            if pulse_state in pulse_counts:
                pulse_counts[pulse_state] += 1
            transition_counts[transition_status] += 1
            rps5_unknown_rows += rps5["position"] in {"UNKNOWN", "NOT_COMPUTED"}
            rps20_unknown_rows += rps20["position"] in {"UNKNOWN", "NOT_COMPUTED"}

            snapshot_member_count = rps5["snapshot_member_count"] if mapped else None
            source_pool_member_count = (
                rps5["source_pool_member_count"] if mapped else None
            )
            snapshot_id = int(membership["snapshot_id"]) if mapped else None
            board_kind = membership.get("board_kind") if mapped else None
            board_native_id = membership.get("board_native_id") if mapped else None
            catalog_id = int(membership["catalog_id"]) if mapped else None
            context_key = (
                f"{snapshot_id}:{board_kind}:{board_native_id}"
                if mapped
                else "UNMAPPED"
            )
            seed_fact_id = int(registration["seed_fact_id"])
            evidence_revision_key = (
                f"seed:{seed_fact_id}|pulse:{pulse_fact_id or 'NONE'}|"
                f"transition:{transition_id or 'NONE'}"
            )
            observation_rows.append(
                {
                    "enrollment_id": int(registration["enrollment_id"]),
                    "target_session": target_session,
                    "source_package_id": source_package_id,
                    "source_capture_id": source_version["source_capture_id"],
                    "source_symbol": registration["source_symbol"],
                    "d0_fact_id": registration.get("d0_fact_id"),
                    "rule_fact_id": registration.get("rule_fact_id"),
                    "seed_fact_id": seed_fact_id,
                    "membership_snapshot_id": snapshot_id,
                    "catalog_id": catalog_id,
                    "board_kind": board_kind,
                    "board_native_id": board_native_id,
                    "board_context_key": context_key,
                    "membership_status": (
                        "MAPPED"
                        if mapped
                        else membership.get("membership_status", "UNKNOWN")
                    ),
                    "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
                    "pulse_fact_id": pulse_fact_id,
                    "pulse_status": pulse_status,
                    "pulse_state": pulse_state,
                    "pulse_transition_id": transition_id,
                    "transition_status": transition_status,
                    "transition_state": transition_state,
                    "transition_reason_codes": transition_reasons,
                    "snapshot_member_count": snapshot_member_count,
                    "source_pool_member_count": source_pool_member_count,
                    "rps5_quality": rps5["quality"],
                    "rps5_reason": rps5["reason"],
                    "rps5_rank_first": rps5["rank_first"],
                    "rps5_rank_last": rps5["rank_last"],
                    "rps5_valid_member_count": rps5["valid_member_count"],
                    "rps5_unknown_member_count": rps5["unknown_member_count"],
                    "rps5_out_of_source_pool_member_count": (
                        rps5["out_of_source_pool_member_count"] if mapped else None
                    ),
                    "rps5_position": rps5["position"],
                    "rps20_quality": rps20["quality"],
                    "rps20_reason": rps20["reason"],
                    "rps20_rank_first": rps20["rank_first"],
                    "rps20_rank_last": rps20["rank_last"],
                    "rps20_valid_member_count": rps20["valid_member_count"],
                    "rps20_unknown_member_count": rps20["unknown_member_count"],
                    "rps20_out_of_source_pool_member_count": (
                        rps20["out_of_source_pool_member_count"] if mapped else None
                    ),
                    "rps20_position": rps20["position"],
                    "relative_position": interpretation["relative_position"],
                    "alignment_state": interpretation["alignment_state"],
                    "research_case": interpretation["research_case"],
                    "support_codes": interpretation["support_codes"],
                    "counter_codes": interpretation["counter_codes"],
                    "unknown_codes": interpretation["unknown_codes"],
                    "next_check_code": interpretation["next_check_code"],
                    "hypothesis_version": HYPOTHESIS_VERSION,
                    "evidence_revision_key": evidence_revision_key,
                    "research_mode": "NON_PIT_EXPLORATORY",
                    "price_coordinate": "RAW_UNADJUSTED",
                }
            )

    persistence = persist_source_t0_early_observation_facts(
        config, rows=observation_rows
    )
    return {
        "status": persistence["status"],
        "target_session": target_session.isoformat(),
        "research_cutoff": research_cutoff.isoformat(),
        "source_package_id": source_package_id,
        "source_capture_id": source_version["source_capture_id"],
        "source_pool_count": source_version["capture_expected_row_count"],
        "profile_revision": inputs["seed_scope"]["profile_revision"],
        "seed_scope": inputs["seed_scope"],
        "snapshot_ids": dict(snapshot_ids),
        "registration_count": inputs["registration_count"],
        "unique_symbol_count": inputs["unique_symbol_count"],
        "membership_relation_count": inputs["membership_relation_count"],
        "context_rows_expected": len(observation_rows),
        "pulse_state_counts": pulse_counts,
        "pulse_transition_status_counts": transition_counts,
        "rps5_position_unknown_rows": rps5_unknown_rows,
        "rps20_position_unknown_rows": rps20_unknown_rows,
        "comparison_batch_count": comparison_batches,
        "comparison_seconds": comparison_seconds,
        "persistence": persistence,
        "execution": {
            "raw_source_collection": False,
            "raw_rows_materialized_in_python": 0,
            "full_source_pool_recomputed": False,
            "seed_d0_d1_recomputed": False,
            "existing_exact_board_rps_comparator_reused": True,
            "source_sector_pulse_recalculated": False,
            "http_write_or_ddl": False,
            "database_write_attempted": True,
            "elapsed_seconds": time.perf_counter() - started,
        },
    }


__all__ = [
    "HYPOTHESIS_VERSION",
    "SourceT0EarlyObservationError",
    "persist_source_t0_early_observations_from_saved",
]
