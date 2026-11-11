"""现役单日研究链生成并保存完整同类板块跨日PULSE。"""

from __future__ import annotations

from datetime import date
from typing import Any, Mapping

from stock_a.application.source_sector_change_research import (
    read_retrospective_rotation,
)
from stock_a.storage.postgres.sector_pulse_facts import (
    persist_source_sector_pulse_day,
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
    package_sha256: str,
    profile_revision: str,
    industry_snapshot_id: str,
    concept_snapshot_id: str,
) -> dict[str, Any]:
    """读取精确已存Raw，单次扫描全池并原子保存行业与GN概念全集。"""
    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
        or not isinstance(profile_revision, str)
        or len(profile_revision) != 64
        or not isinstance(industry_snapshot_id, str)
        or not isinstance(concept_snapshot_id, str)
        or industry_snapshot_id == concept_snapshot_id
    ):
        raise SourceSectorPulseError(
            "SECTOR_PULSE_SCOPE_INVALID", "目标会话、画像修订或双板块快照绑定无效"
        )
    source_version = read_source_version_context(
        config,
        target_session=target_session,
        package_sha256=package_sha256,
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
    if (
        industry.get("namespace_id") != source_version.get("namespace_id")
        or concept.get("namespace_id") != source_version.get("namespace_id")
        or not industry.get("boards")
        or not concept.get("boards")
    ):
        raise SourceSectorPulseError(
            "SECTOR_PULSE_SNAPSHOT_BINDING_INVALID",
            "行业与GN概念必须绑定同一Raw命名空间且目录均非空",
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
        package_sha256=package_sha256,
        peer_target_only=True,
        peer_snapshot_ids={
            "INDUSTRY": industry_snapshot_id,
            "CONCEPT": concept_snapshot_id,
        },
    )
    persisted = persist_source_sector_pulse_day(
        config,
        target_session=target_session,
        package_sha256=package_sha256,
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
        "namespace_id": source_version["namespace_id"],
        "package_sha256": package_sha256,
        "source_capture_id": source_version["source_capture_id"],
        "source_content_id": source_version["source_content_id"],
        "source_raw_digest": source_version["capture_digest"],
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


__all__ = ["SourceSectorPulseError", "run_source_sector_pulse_day"]
