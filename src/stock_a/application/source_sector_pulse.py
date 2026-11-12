"""现役单日研究链生成并保存完整同类板块跨日PULSE。"""

from __future__ import annotations

from datetime import date
from typing import Any, Mapping

from stock_a.application.source_sector_change_research import (
    read_retrospective_rotation,
)
from stock_a.storage.postgres.sector_pulse_facts import (
    persist_source_sector_pulse_day,
    read_source_sector_pulse_day,
)
from stock_a.storage.postgres.sector_pulse_transitions import (
    persist_source_sector_pulse_transitions,
    read_source_sector_pulse_transitions,
)
from stock_a.storage.postgres.source_raw import read_source_version_context
from stock_a.storage.postgres.tdx_sector_members import read_rotation_snapshot_members


class SourceSectorPulseError(ValueError):
    """单日板块PULSE生产边界错误。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def run_source_sector_pulse_day(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    source_package_id: int,
    profile_revision: str,
    industry_snapshot_id: int,
    concept_snapshot_id: int,
) -> dict[str, Any]:
    """读取精确已存Raw，单次扫描全池并原子保存行业与GN概念全集。"""
    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
        or not isinstance(profile_revision, str)
        or profile_revision not in {f"PROFILE_R{i:02d}" for i in range(1, 100)}
        or type(industry_snapshot_id) is not int
        or industry_snapshot_id <= 0
        or type(concept_snapshot_id) is not int
        or concept_snapshot_id <= 0
        or industry_snapshot_id == concept_snapshot_id
    ):
        raise SourceSectorPulseError(
            "SECTOR_PULSE_SCOPE_INVALID", "目标会话、画像修订或双板块快照绑定无效"
        )
    source_version = read_source_version_context(
        config,
        target_session=target_session,
        source_package_id=source_package_id,
    )
    if source_version.get("status") != "AVAILABLE":
        raise SourceSectorPulseError(
            "SECTOR_PULSE_RAW_SOURCE_UNAVAILABLE", "目标日没有唯一闭合的已存Raw来源"
        )
    industry = read_rotation_snapshot_members(
        config, snapshot_id=industry_snapshot_id, board_kind="INDUSTRY"
    )
    concept = read_rotation_snapshot_members(
        config, snapshot_id=concept_snapshot_id, board_kind="CONCEPT"
    )
    if not industry.get("boards") or not concept.get("boards"):
        raise SourceSectorPulseError(
            "SECTOR_PULSE_SNAPSHOT_BINDING_INVALID",
            "行业与GN概念目录必须均非空",
        )
    industry_board_id = sorted(industry["boards"])[0]
    calculated = read_retrospective_rotation(
        config,
        snapshot_id=industry_snapshot_id,
        board_kind="INDUSTRY",
        board_native_id=industry_board_id,
        start_session=target_session,
        target_session=target_session,
        research_cutoff=research_cutoff,
        source_package_id=source_package_id,
        peer_target_only=True,
        peer_snapshot_ids={
            "INDUSTRY": industry_snapshot_id,
            "CONCEPT": concept_snapshot_id,
        },
    )
    persisted = persist_source_sector_pulse_day(
        config,
        target_session=target_session,
        source_package_id=source_package_id,
        source_version=source_version,
        profile_revision=profile_revision,
        target_result=calculated,
    )
    pulse_counts = {state: 0 for state in ("TRUE", "FALSE", "UNKNOWN")}
    unknown_reasons: dict[str, int] = {}
    for boards in calculated["peer_target_by_kind"].values():
        for board in boards.values():
            pulse = board["rotation"]["pulse"]
            pulse_counts[pulse["state"]] += 1
            for reason in pulse.get("reasons", ()):
                unknown_reasons[reason] = unknown_reasons.get(reason, 0) + 1
    return {
        "status": persisted["status"],
        "target_session": target_session.isoformat(),
        "source_package_id": source_package_id,
        "source_capture_id": source_version["source_capture_id"],
        "source_pool_count": source_version["raw_row_count"],
        "profile_revision": profile_revision,
        "snapshot_ids": {
            "INDUSTRY": industry_snapshot_id,
            "CONCEPT": concept_snapshot_id,
        },
        "board_count_by_kind": {
            kind: len(rows) for kind, rows in calculated["peer_target_by_kind"].items()
        },
        "pulse_state_counts": pulse_counts,
        "pulse_unknown_reason_counts": dict(sorted(unknown_reasons.items())),
        "membership_mode": calculated["membership_mode"],
        "price_coordinate": "RAW_UNADJUSTED",
        "calculation": "EXISTING_CROSS_PERIOD_BOARDS_SINGLE_SOURCE_POOL_SCAN",
        "persistence": persisted,
    }


def run_source_sector_pulse_transition_day(
    config: Mapping[str, Any],
    *,
    previous_session: date,
    target_session: date,
    research_cutoff: date,
    previous_source_package_id: int,
    target_source_package_id: int,
    previous_profile_revision: str,
    target_profile_revision: str,
    industry_snapshot_id: int,
    concept_snapshot_id: int,
) -> dict[str, Any]:
    """补齐必要的缺失日PULSE，并原子保存严格相邻的板块变化。"""
    from stock_a.application.source_d0_raw import _official_calendar

    inputs = (
        previous_session,
        target_session,
        research_cutoff,
    )
    if (
        any(type(item) is not date for item in inputs)
        or previous_session >= target_session
        or target_session > research_cutoff
        or type(previous_source_package_id) is not int
        or previous_source_package_id <= 0
        or type(target_source_package_id) is not int
        or target_source_package_id <= 0
        or previous_profile_revision not in {f"PROFILE_R{i:02d}" for i in range(1, 100)}
        or target_profile_revision not in {f"PROFILE_R{i:02d}" for i in range(1, 100)}
        or type(industry_snapshot_id) is not int
        or industry_snapshot_id <= 0
        or type(concept_snapshot_id) is not int
        or concept_snapshot_id <= 0
        or industry_snapshot_id == concept_snapshot_id
    ):
        raise SourceSectorPulseError(
            "SECTOR_PULSE_TRANSITION_SCOPE_INVALID", "相邻变化日期、来源或快照参数无效"
        )
    calendar = _official_calendar(target_session)
    sessions = calendar["sessions"]
    if target_session not in sessions:
        raise SourceSectorPulseError(
            "SECTOR_PULSE_TRANSITION_TARGET_NOT_OFFICIAL",
            "目标日期不在版本化官方交易日历中",
        )
    index = sessions.index(target_session)
    if index == 0 or sessions[index - 1] != previous_session:
        raise SourceSectorPulseError(
            "SECTOR_PULSE_TRANSITION_NOT_ADJACENT",
            "只允许严格相邻的两个官方交易会话",
        )
    previous_calendar = _official_calendar(previous_session)
    calendar_identity = {
        key: calendar[key] for key in ("calendar_id", "calendar_revision")
    }
    if any(
        previous_calendar.get(key) != calendar_identity[key]
        for key in calendar_identity
    ):
        raise SourceSectorPulseError(
            "SECTOR_PULSE_TRANSITION_CALENDAR_REVISION_MISMATCH",
            "相邻两端必须使用同一版本化官方日历",
        )

    snapshots = {
        "INDUSTRY": industry_snapshot_id,
        "CONCEPT": concept_snapshot_id,
    }
    generated_pulses: dict[str, Any] = {}

    def load_exact_day(
        session: date, source_package_id: int, expected_profile_revision: str
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        source_version = read_source_version_context(
            config,
            target_session=session,
            source_package_id=source_package_id,
        )
        if (
            source_version.get("status") != "AVAILABLE"
            or source_version.get("version_status") != "COMPLETE"
            or source_version.get("raw_row_count")
            != source_version.get("capture_expected_row_count")
        ):
            raise SourceSectorPulseError(
                "SECTOR_PULSE_TRANSITION_RAW_NOT_CLOSED",
                f"{session}缺少唯一且闭合的精确Raw来源",
            )
        pulse = read_source_sector_pulse_day(
            config,
            target_session=session,
            source_package_id=source_package_id,
            source_capture_id=source_version["source_capture_id"],
            source_pool_count=source_version["raw_row_count"],
            snapshot_ids=snapshots,
        )
        pulse["target_session"] = session.isoformat()
        pulse["source_version"] = source_version
        if pulse.get("profile_revision") != expected_profile_revision:
            raise SourceSectorPulseError(
                "SECTOR_PULSE_TRANSITION_PROFILE_REVISION_MISMATCH",
                f"{session}PULSE必须绑定显式指定的Seed/Profile修订",
            )
        return source_version, pulse

    previous_source, previous_pulse = load_exact_day(
        previous_session,
        previous_source_package_id,
        previous_profile_revision,
    )
    target_source, target_pulse = load_exact_day(
        target_session,
        target_source_package_id,
        target_profile_revision,
    )
    for session, source_version, pulse, profile_revision in (
        (
            previous_session,
            previous_source,
            previous_pulse,
            previous_profile_revision,
        ),
        (target_session, target_source, target_pulse, target_profile_revision),
    ):
        if pulse.get("status") == "AVAILABLE":
            continue
        reasons = {
            reason
            for group in pulse.get("by_kind", {}).values()
            if isinstance(group, Mapping)
            for reason in group.get("reason_codes", ())
        }
        if pulse.get("status") != "NOT_COMPUTED" or reasons != {
            "PULSE_FACTS_NOT_SAVED_FOR_EXACT_INPUT"
        }:
            raise SourceSectorPulseError(
                "SECTOR_PULSE_TRANSITION_EXISTING_PARTITION_NOT_REPAIRABLE",
                f"{session}脉冲分区不是完整可补算缺口：{sorted(reasons)}",
            )
        generated_pulses[session.isoformat()] = run_source_sector_pulse_day(
            config,
            target_session=session,
            research_cutoff=session,
            source_package_id=source_version["source_package_id"],
            profile_revision=profile_revision,
            industry_snapshot_id=industry_snapshot_id,
            concept_snapshot_id=concept_snapshot_id,
        )
        _, refreshed = load_exact_day(
            session, source_version["source_package_id"], profile_revision
        )
        if refreshed.get("status") != "AVAILABLE":
            raise SourceSectorPulseError(
                "SECTOR_PULSE_TRANSITION_RECALCULATION_NOT_CLOSED",
                f"{session}脉冲补算后行业与GN分区未完整闭合",
            )
        if session == previous_session:
            previous_pulse = refreshed
        else:
            target_pulse = refreshed

    persistence = persist_source_sector_pulse_transitions(
        config,
        previous_result=previous_pulse,
        target_result=target_pulse,
        calendar_identity=calendar_identity,
    )
    readback = read_source_sector_pulse_transitions(
        config,
        previous_result=previous_pulse,
        target_result=target_pulse,
        calendar_identity=calendar_identity,
    )
    if readback.get("status") != "AVAILABLE":
        raise SourceSectorPulseError(
            "SECTOR_PULSE_TRANSITION_READBACK_FAILED",
            "相邻变化事实已写入但异事务PG读回未完整",
        )
    transition_counts = {kind: {} for kind in ("INDUSTRY", "CONCEPT")}
    for kind, group in readback["by_kind"].items():
        for fact in group["facts"].values():
            state = fact["transition_state"]
            transition_counts[kind][state] = transition_counts[kind].get(state, 0) + 1
    return {
        "status": persistence["status"],
        "previous_session": previous_session.isoformat(),
        "target_session": target_session.isoformat(),
        "calendar_identity": calendar_identity,
        "source_identities": {
            "previous": {
                key: previous_source[key]
                for key in (
                    "source_package_id",
                    "source_capture_id",
                    "raw_row_count",
                )
            },
            "target": {
                key: target_source[key]
                for key in (
                    "source_package_id",
                    "source_capture_id",
                    "raw_row_count",
                )
            },
            "previous_profile_revision": previous_profile_revision,
            "target_profile_revision": target_profile_revision,
        },
        "pulse_rows": {
            "previous": {
                kind: group.get("read_board_count", 0)
                for kind, group in previous_pulse["by_kind"].items()
            },
            "target": {
                kind: group.get("read_board_count", 0)
                for kind, group in target_pulse["by_kind"].items()
            },
        },
        "pulse_generated": generated_pulses,
        "transition_rows_expected": persistence["rows_expected"],
        "transition_rows_inserted": persistence["rows_inserted"],
        "transition_counts": transition_counts,
        "persistence": persistence,
        "readback_status": readback["status"],
        "readback": readback,
        "execution": {
            "raw_source_collection": False,
            "profile_seed_d0_d1_recomputed": False,
            "sector_pulse_calculated_only_for_missing_exact_day": True,
            "sector_pulse_cross_period_recalculated_by_http": False,
            "database_write_attempted": bool(
                persistence["database_write_attempted"]
                or any(
                    item.get("persistence", {}).get("database_write_attempted")
                    for item in generated_pulses.values()
                )
            ),
            "ddl_executed": False,
        },
    }


__all__ = [
    "SourceSectorPulseError",
    "run_source_sector_pulse_day",
    "run_source_sector_pulse_transition_day",
]
