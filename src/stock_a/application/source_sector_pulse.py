"""现役单日研究链生成并保存完整同类板块跨日PULSE。"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Mapping, Sequence

from stock_a.application.source_sector_change_research import (
    read_retrospective_rotation,
)
from stock_a.application.pg_research import read_source_version_context
from stock_a.storage.postgres.sector_pulse_facts import (
    persist_source_sector_pulse_day,
    read_source_sector_pulse_day,
)
from stock_a.storage.postgres.sector_pulse_transitions import (
    persist_source_sector_pulse_transitions,
    read_source_sector_pulse_transitions,
)
from stock_a.storage.postgres.tdx_sector_members import (
    query_tdx_sector_source_snapshot_catalog,
    read_rotation_snapshot_members,
)


class SourceSectorPulseError(ValueError):
    """单日板块PULSE生产边界错误。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


_SNAPSHOT_SOURCE_KIND = {
    "INDUSTRY": "TDX_TDXHY_CFG",
    "CONCEPT": "TDX_INFOHARBOR_BLOCK_DAT",
}
_NON_PIT_SOURCE_QUALITIES = frozenset({"COMPLETE_NON_PIT", "PARTIAL_NON_PIT"})


def _snapshot_catalog_record(
    row: Mapping[str, Any], board_kind: str
) -> dict[str, Any] | None:
    """解析并校验单个来源快照的类型、质量、时钟及typed成员统计。"""
    if row.get("source_kind") != _SNAPSHOT_SOURCE_KIND.get(board_kind):
        return None
    try:
        snapshot_id = int(row["snapshot_id"])
        observed_at = datetime.fromisoformat(str(row["observed_at"]))
        catalog_rows = int(row.get(f"{board_kind.lower()}_catalog_rows") or 0)
        member_rows = int(row.get(f"{board_kind.lower()}_member_rows") or 0)
        other_kind = "CONCEPT" if board_kind == "INDUSTRY" else "INDUSTRY"
        other_catalog_rows = int(row.get(f"{other_kind.lower()}_catalog_rows") or 0)
        other_member_rows = int(row.get(f"{other_kind.lower()}_member_rows") or 0)
        excluded_catalog_rows = int(row.get("excluded_catalog_rows") or 0)
        invalid_members = int(row.get("invalid_membership_quality_rows") or 0)
    except (KeyError, TypeError, ValueError):
        return None
    source_quality = row.get("source_quality")
    quality_parts = source_quality.split(";") if isinstance(source_quality, str) else []
    quality_code = quality_parts[0] if quality_parts else ""
    quality_counts: dict[str, int] = {}
    for part in quality_parts[1:]:
        name, separator, raw_value = part.partition("=")
        if separator and raw_value.isdecimal():
            quality_counts[name] = int(raw_value)
    unmapped_rows = quality_counts.get("UNMAPPED_SOURCE_ROWS")
    if (
        snapshot_id <= 0
        or observed_at.tzinfo is None
        or observed_at.utcoffset() is None
        or row.get("research_mode") != "NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT"
        or row.get("clock_quality") != "UNKNOWN"
        or row.get("source_valid_on") is not None
        or row.get("source_known_at") is not None
        or row.get("source_available_at") is not None
        or quality_code not in _NON_PIT_SOURCE_QUALITIES
        or unmapped_rows is None
        or (quality_code == "COMPLETE_NON_PIT" and unmapped_rows != 0)
        or (quality_code == "PARTIAL_NON_PIT" and unmapped_rows == 0)
        or catalog_rows <= 0
        or member_rows <= 0
        or other_catalog_rows != 0
        or other_member_rows != 0
        or excluded_catalog_rows != 0
        or invalid_members != 0
    ):
        return None
    return {
        "snapshot_id": snapshot_id,
        "source_kind": row["source_kind"],
        "observed_at": observed_at,
        "observed_at_utc": observed_at.astimezone(timezone.utc),
        "research_mode": row["research_mode"],
        "clock_quality": row["clock_quality"],
        "source_quality": source_quality,
        "catalog_rows": catalog_rows,
        "member_rows": member_rows,
        "invalid_membership_quality_rows": invalid_members,
    }


def _require_source_sector_snapshot_pair(
    config: Mapping[str, Any], *, industry_snapshot_id: int, concept_snapshot_id: int
) -> dict[str, Any]:
    """在正式板块生产入口重新验证显式快照对，而不依赖上游计划校验。"""
    if (
        type(industry_snapshot_id) is not int
        or industry_snapshot_id <= 0
        or type(concept_snapshot_id) is not int
        or concept_snapshot_id <= 0
        or industry_snapshot_id == concept_snapshot_id
    ):
        raise SourceSectorPulseError(
            "SECTOR_SNAPSHOT_PAIR_SCOPE_INVALID", "行业与GN快照ID必须是不同的正整数"
        )
    catalog = query_tdx_sector_source_snapshot_catalog(dict(config))
    if catalog.get("status") != "AVAILABLE":
        raise SourceSectorPulseError(
            "SECTOR_SNAPSHOT_PAIR_NOT_AVAILABLE", "PG中没有可验证的TDX成员快照目录"
        )
    rows = catalog.get("snapshots", ())
    by_id: dict[int, Mapping[str, Any]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        try:
            snapshot_id = int(row.get("snapshot_id"))
        except (TypeError, ValueError):
            continue
        by_id[snapshot_id] = row

    selected: dict[str, dict[str, Any]] = {}
    for board_kind, snapshot_id in (
        ("INDUSTRY", industry_snapshot_id),
        ("CONCEPT", concept_snapshot_id),
    ):
        row = by_id.get(snapshot_id)
        parsed = _snapshot_catalog_record(row, board_kind) if row is not None else None
        if parsed is None:
            raise SourceSectorPulseError(
                "SECTOR_SNAPSHOT_PAIR_IDENTITY_INVALID",
                f"快照{snapshot_id}的来源类型、质量或NON_PIT时钟不符合{board_kind}合同",
            )
        selected[board_kind] = parsed
    if (
        selected["INDUSTRY"]["observed_at_utc"]
        != selected["CONCEPT"]["observed_at_utc"]
    ):
        raise SourceSectorPulseError(
            "SECTOR_SNAPSHOT_PAIR_OBSERVATION_MISMATCH",
            "行业与GN概念快照不是同一次来源观察",
        )

    public_snapshots = [
        {
            key: value.isoformat() if isinstance(value, datetime) else value
            for key, value in selected[kind].items()
            if key != "observed_at_utc"
        }
        for kind in ("INDUSTRY", "CONCEPT")
    ]
    return {
        "status": "AVAILABLE",
        "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
        "selection_rule": "EXPLICIT_IDS_VALIDATED_SAME_OBSERVATION",
        "observed_at": selected["INDUSTRY"]["observed_at"].isoformat(),
        "snapshot_ids": {
            "INDUSTRY": industry_snapshot_id,
            "CONCEPT": concept_snapshot_id,
        },
        "snapshots": public_snapshots,
        "selected_by_kind": selected,
    }


def _validate_loaded_snapshot(
    members: Mapping[str, Any], expected: Mapping[str, Any], board_kind: str
) -> None:
    snapshot = members.get("snapshot")
    if not isinstance(snapshot, Mapping):
        raise SourceSectorPulseError(
            "SECTOR_SNAPSHOT_READBACK_INVALID", f"{board_kind}成员读取缺少快照身份"
        )
    try:
        observed_at = datetime.fromisoformat(str(snapshot["observed_at"]))
        snapshot_id = int(snapshot["snapshot_id"])
    except (KeyError, TypeError, ValueError) as error:
        raise SourceSectorPulseError(
            "SECTOR_SNAPSHOT_READBACK_INVALID", f"{board_kind}成员快照身份字段非法"
        ) from error
    expected_time = expected["observed_at_utc"]
    if (
        snapshot_id != expected["snapshot_id"]
        or snapshot.get("source_kind") != _SNAPSHOT_SOURCE_KIND[board_kind]
        or snapshot.get("research_mode") != "NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT"
        or snapshot.get("clock_quality") != "UNKNOWN"
        or snapshot.get("source_valid_on") is not None
        or snapshot.get("source_known_at") is not None
        or snapshot.get("source_available_at") is not None
        or snapshot.get("source_quality") != expected["source_quality"]
        or observed_at.tzinfo is None
        or observed_at.utcoffset() is None
        or observed_at.astimezone(timezone.utc) != expected_time
    ):
        raise SourceSectorPulseError(
            "SECTOR_SNAPSHOT_READBACK_MISMATCH",
            f"{board_kind}成员读取身份与已验证的NON_PIT快照不一致",
        )


def resolve_source_sector_snapshot_pair(config: Mapping[str, Any]) -> dict[str, Any]:
    """从PG选择同次观察的行业与GN快照，并保留其NON_PIT身份。"""
    catalog = query_tdx_sector_source_snapshot_catalog(dict(config))
    if catalog.get("status") != "AVAILABLE":
        return {
            "status": "NOT_AVAILABLE",
            "reason_code": "SECTOR_SNAPSHOT_CATALOG_EMPTY",
            "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
            "snapshot_ids": None,
            "snapshots": [],
        }

    by_kind: dict[str, list[dict[str, Any]]] = {
        "INDUSTRY": [],
        "CONCEPT": [],
    }
    for row in catalog.get("snapshots", ()):
        if not isinstance(row, Mapping):
            continue
        source_kind = row.get("source_kind")
        board_kind = (
            "INDUSTRY"
            if source_kind == "TDX_TDXHY_CFG"
            else "CONCEPT"
            if source_kind == "TDX_INFOHARBOR_BLOCK_DAT"
            else None
        )
        if board_kind is None:
            continue
        parsed = _snapshot_catalog_record(row, board_kind)
        if parsed is None:
            continue
        by_kind[board_kind].append(parsed)

    candidates = []
    concept_by_observation = {row["observed_at_utc"]: row for row in by_kind["CONCEPT"]}
    for industry in by_kind["INDUSTRY"]:
        concept = concept_by_observation.get(industry["observed_at_utc"])
        if concept is not None:
            candidates.append((industry, concept))
    if not candidates:
        return {
            "status": "NOT_AVAILABLE",
            "reason_code": "SECTOR_SNAPSHOT_PAIR_NOT_FOUND",
            "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
            "snapshot_ids": None,
            "snapshots": [],
        }

    industry, concept = max(
        candidates,
        key=lambda pair: (
            pair[0]["observed_at_utc"],
            pair[0]["snapshot_id"],
            pair[1]["snapshot_id"],
        ),
    )

    def public_snapshot(row: Mapping[str, Any]) -> dict[str, Any]:
        return {
            key: value.isoformat() if isinstance(value, datetime) else value
            for key, value in row.items()
            if key != "observed_at_utc"
        }

    return {
        "status": "AVAILABLE",
        "reason_code": None,
        "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
        "selection_rule": "LATEST_COMMON_OBSERVED_AT_WITH_BOTH_TYPED_MEMBER_SETS",
        "observed_at": industry["observed_at"].isoformat(),
        "snapshot_ids": {
            "INDUSTRY": industry["snapshot_id"],
            "CONCEPT": concept["snapshot_id"],
        },
        "snapshots": [public_snapshot(industry), public_snapshot(concept)],
    }


def resolve_source_sector_snapshot_pairs_by_session(
    config: Mapping[str, Any],
    sessions: Sequence[date],
) -> dict[str, dict[str, int]]:
    """按目标官方会话日期解析同次观察的行业与GN快照对；优先精确匹配会话日，否则回退最新合法配对。"""
    from datetime import timedelta

    catalog = query_tdx_sector_source_snapshot_catalog(dict(config))
    if catalog.get("status") != "AVAILABLE":
        return {}
    by_kind: dict[str, list[dict[str, Any]]] = {"INDUSTRY": [], "CONCEPT": []}
    for row in catalog.get("snapshots", ()):
        if not isinstance(row, Mapping):
            continue
        source_kind = row.get("source_kind")
        board_kind = (
            "INDUSTRY"
            if source_kind == "TDX_TDXHY_CFG"
            else "CONCEPT"
            if source_kind == "TDX_INFOHARBOR_BLOCK_DAT"
            else None
        )
        if board_kind is None:
            continue
        parsed = _snapshot_catalog_record(row, board_kind)
        if parsed is not None:
            by_kind[board_kind].append(parsed)
    concept_by_obs = {row["observed_at_utc"]: row for row in by_kind["CONCEPT"]}
    candidates = [
        (ind, concept_by_obs[ind["observed_at_utc"]])
        for ind in by_kind["INDUSTRY"]
        if ind["observed_at_utc"] in concept_by_obs
    ]
    if not candidates:
        return {}
    shanghai_tz = timezone(timedelta(hours=8))
    by_day: dict[date, list[tuple[dict[str, Any], dict[str, Any]]]] = {}
    for ind, con in candidates:
        obs_dt = ind["observed_at_utc"]
        obs_day = (
            obs_dt.astimezone(shanghai_tz).date()
            if getattr(obs_dt, "tzinfo", None) is not None
            else obs_dt.date()
        )
        by_day.setdefault(obs_day, []).append((ind, con))
    latest_ind, latest_con = max(
        candidates,
        key=lambda p: (
            p[0]["observed_at_utc"],
            p[0]["snapshot_id"],
            p[1]["snapshot_id"],
        ),
    )
    default_pair = {
        "INDUSTRY": int(latest_ind["snapshot_id"]),
        "CONCEPT": int(latest_con["snapshot_id"]),
    }
    out: dict[str, dict[str, int]] = {}
    for day in sessions:
        day_cands = by_day.get(day)
        if day_cands:
            ind_d, con_d = max(
                day_cands,
                key=lambda p: (
                    p[0]["observed_at_utc"],
                    p[0]["snapshot_id"],
                    p[1]["snapshot_id"],
                ),
            )
            out[day.isoformat()] = {
                "INDUSTRY": int(ind_d["snapshot_id"]),
                "CONCEPT": int(con_d["snapshot_id"]),
            }
        else:
            out[day.isoformat()] = dict(default_pair)
    return out


def run_source_sector_pulse_day(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int,
    industry_snapshot_id: int,
    concept_snapshot_id: int,
) -> dict[str, Any]:
    """读取精确BaoStock paired capture，扫描全池并原子保存行业与GN概念全集。"""
    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
        or type(industry_snapshot_id) is not int
        or industry_snapshot_id <= 0
        or type(concept_snapshot_id) is not int
        or concept_snapshot_id <= 0
        or industry_snapshot_id == concept_snapshot_id
    ):
        raise SourceSectorPulseError(
            "SECTOR_PULSE_SCOPE_INVALID", "目标会话或双板块快照绑定无效"
        )
    snapshot_pair = _require_source_sector_snapshot_pair(
        config,
        industry_snapshot_id=industry_snapshot_id,
        concept_snapshot_id=concept_snapshot_id,
    )
    source_version = read_source_version_context(
        config,
        target_session=target_session,
        research_cutoff=research_cutoff,
        source_capture_id=source_capture_id,
        price_coordinate="FORWARD_ADJUSTED",
    )
    if (
        source_version.get("status") != "AVAILABLE"
        or source_version.get("version_status") != "COMPLETE"
        or source_version.get("price_coordinate") != "FORWARD_ADJUSTED"
        or source_version.get("time_role") != "NON_PIT_RETROSPECTIVE"
    ):
        raise SourceSectorPulseError(
            "SECTOR_PULSE_QFQ_SOURCE_UNAVAILABLE",
            "目标日没有唯一闭合的paired QFQ价格来源",
        )
    industry = read_rotation_snapshot_members(
        config, snapshot_id=industry_snapshot_id, board_kind="INDUSTRY"
    )
    concept = read_rotation_snapshot_members(
        config, snapshot_id=concept_snapshot_id, board_kind="CONCEPT"
    )
    _validate_loaded_snapshot(
        industry,
        snapshot_pair["selected_by_kind"]["INDUSTRY"],
        "INDUSTRY",
    )
    _validate_loaded_snapshot(
        concept,
        snapshot_pair["selected_by_kind"]["CONCEPT"],
        "CONCEPT",
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
        source_capture_id=source_capture_id,
        price_coordinate="FORWARD_ADJUSTED",
        peer_target_only=True,
        peer_snapshot_ids={
            "INDUSTRY": industry_snapshot_id,
            "CONCEPT": concept_snapshot_id,
        },
    )
    persisted = persist_source_sector_pulse_day(
        config,
        target_session=target_session,
        source_capture_id=source_capture_id,
        source_version=source_version,
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
        "source_capture_id": source_version["source_capture_id"],
        "source_pool_count": source_version["raw_row_count"],
        "snapshot_ids": {
            "INDUSTRY": industry_snapshot_id,
            "CONCEPT": concept_snapshot_id,
        },
        "snapshot_provenance": [
            industry["snapshot"],
            concept["snapshot"],
        ],
        "board_count_by_kind": {
            kind: len(rows) for kind, rows in calculated["peer_target_by_kind"].items()
        },
        "pulse_state_counts": pulse_counts,
        "pulse_unknown_reason_counts": dict(sorted(unknown_reasons.items())),
        "membership_mode": calculated["membership_mode"],
        "price_coordinate": "FORWARD_ADJUSTED",
        "time_role": "NON_PIT_RETROSPECTIVE",
        "calculation": "EXISTING_CROSS_PERIOD_BOARDS_SINGLE_SOURCE_POOL_SCAN",
        "persistence": persisted,
    }


def run_source_sector_pulse_transition_day(
    config: Mapping[str, Any],
    *,
    previous_session: date,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int,
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
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or type(industry_snapshot_id) is not int
        or industry_snapshot_id <= 0
        or type(concept_snapshot_id) is not int
        or concept_snapshot_id <= 0
        or industry_snapshot_id == concept_snapshot_id
    ):
        raise SourceSectorPulseError(
            "SECTOR_PULSE_TRANSITION_SCOPE_INVALID", "相邻变化日期、来源或快照参数无效"
        )
    _require_source_sector_snapshot_pair(
        config,
        industry_snapshot_id=industry_snapshot_id,
        concept_snapshot_id=concept_snapshot_id,
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
    if previous_calendar.get("calendar_id") != calendar_identity["calendar_id"]:
        raise SourceSectorPulseError(
            "SECTOR_PULSE_TRANSITION_CALENDAR_ID_MISMATCH",
            "相邻两端必须使用同一官方日历合同",
        )

    snapshots = {
        "INDUSTRY": industry_snapshot_id,
        "CONCEPT": concept_snapshot_id,
    }
    generated_pulses: dict[str, Any] = {}

    def load_exact_day(
        session: date, source_capture_id: int
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        source_version = read_source_version_context(
            config,
            target_session=session,
            research_cutoff=research_cutoff,
            source_capture_id=source_capture_id,
            price_coordinate="FORWARD_ADJUSTED",
        )
        if (
            source_version.get("status") != "AVAILABLE"
            or source_version.get("version_status") != "COMPLETE"
            or source_version.get("raw_row_count")
            != source_version.get("capture_expected_row_count")
            or source_version.get("price_coordinate") != "FORWARD_ADJUSTED"
            or source_version.get("time_role") != "NON_PIT_RETROSPECTIVE"
        ):
            raise SourceSectorPulseError(
                "SECTOR_PULSE_TRANSITION_CAPTURE_NOT_CLOSED",
                f"{session}缺少唯一且闭合的BaoStock paired capture",
            )
        pulse = read_source_sector_pulse_day(
            config,
            target_session=session,
            source_capture_id=source_capture_id,
            source_pool_count=source_version["raw_row_count"],
            snapshot_ids=snapshots,
        )
        pulse["target_session"] = session.isoformat()
        pulse["source_version"] = source_version
        return source_version, pulse

    previous_source, previous_pulse = load_exact_day(
        previous_session,
        source_capture_id,
    )
    target_source, target_pulse = load_exact_day(
        target_session,
        source_capture_id,
    )
    for session, source_version, pulse in (
        (
            previous_session,
            previous_source,
            previous_pulse,
        ),
        (target_session, target_source, target_pulse),
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
            research_cutoff=research_cutoff,
            source_capture_id=source_version["source_capture_id"],
            industry_snapshot_id=industry_snapshot_id,
            concept_snapshot_id=concept_snapshot_id,
        )
        _, refreshed = load_exact_day(session, source_version["source_capture_id"])
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
        "source_capture_id": source_capture_id,
        "previous_source_capture_id": previous_source["source_capture_id"],
        "target_source_capture_id": target_source["source_capture_id"],
        "price_coordinate": "FORWARD_ADJUSTED",
        "time_role": "NON_PIT_RETROSPECTIVE",
        "calendar_identity": calendar_identity,
        "source_identities": {
            "previous": {
                key: previous_source[key]
                for key in (
                    "source_capture_id",
                    "raw_row_count",
                )
            },
            "target": {
                key: target_source[key]
                for key in (
                    "source_capture_id",
                    "raw_row_count",
                )
            },
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
    "resolve_source_sector_snapshot_pair",
    "run_source_sector_pulse_day",
    "run_source_sector_pulse_transition_day",
]
