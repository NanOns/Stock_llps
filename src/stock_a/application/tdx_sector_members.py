"""TDX行业与概念成员来源接入和只读查询用例。"""

from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo
from typing import Any

from stock_a.sources.tdx.sector_members import (
    TdxSectorSourceError,
    parse_sector_source_inputs,
    read_sector_source_inputs,
)
from stock_a.research.entry import ResearchEntryError, _load_calendar
from stock_a.storage.postgres.tdx_sector_members import (
    TdxSectorMembersPgError,
    persist_sector_source_snapshots,
    query_sector_catalog,
    query_sector_breadth,
    query_sector_members,
)


class TdxSectorMembersApplicationError(ValueError):
    """TDX板块来源应用用例拒绝无效配置或参数。"""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def ingest_tdx_sector_members(config: dict[str, Any]) -> dict[str, Any]:
    roots = config.get("source_policy", {}).get("configured_tdx_source_roots")
    if (
        config.get("source_policy", {}).get("all_source_roots_read_only") is not True
        or not isinstance(roots, list)
        or len(roots) != 1
        or not isinstance(roots[0], str)
        or not roots[0]
    ):
        raise TdxSectorMembersApplicationError(
            "TDX_SOURCE_ROOT_NOT_BOUND",
            "接入要求唯一登记的只读TDX来源根，拒绝猜测安装路径",
        )
    try:
        inputs = read_sector_source_inputs(roots[0])
        observed_at = datetime.now(ZoneInfo("Asia/Shanghai"))
        snapshots = parse_sector_source_inputs(inputs, observed_at=observed_at)
    except TdxSectorSourceError:
        raise
    source_files = [
        {
            "relative_path": item.relative_path,
            "absolute_path": item.absolute_path,
            "sha256": item.sha256,
            "size_bytes": item.size_bytes,
            "last_write_time_utc": item.last_write_time_utc,
            "last_write_time_is_source_clock": False,
        }
        for item in inputs.files.values()
    ]
    persisted = persist_sector_source_snapshots(config, snapshots)
    return {
        "status": persisted["status"],
        "source_root": inputs.root,
        "observed_at": observed_at.isoformat(),
        "source_clock_quality": "UNKNOWN",
        "research_mode": "NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT",
        "source_files": source_files,
        **persisted,
        "source_downloaded": False,
        "source_modified": False,
    }


def read_tdx_sector_catalog(
    config: dict[str, Any],
    *,
    snapshot_id: str,
    board_kind: str | None,
    search: str | None,
    offset: int,
    limit: int,
) -> dict[str, Any]:
    return query_sector_catalog(
        config,
        snapshot_id=snapshot_id,
        board_kind=board_kind,
        search=search,
        offset=offset,
        limit=limit,
    )


def read_tdx_sector_members(
    config: dict[str, Any],
    *,
    snapshot_id: str,
    board_kind: str,
    board_native_id: str,
    target_session: date,
    research_cutoff: date,
    source_set_id: str,
    package_sha256: str,
    profile_revision: str | None,
    offset: int,
    limit: int,
) -> dict[str, Any]:
    return query_sector_members(
        config,
        snapshot_id=snapshot_id,
        board_kind=board_kind,
        board_native_id=board_native_id,
        target_session=target_session,
        research_cutoff=research_cutoff,
        source_set_id=source_set_id,
        package_sha256=package_sha256,
        profile_revision=profile_revision,
        offset=offset,
        limit=limit,
    )


def read_tdx_sector_breadth(
    config: dict[str, Any],
    *,
    snapshot_id: str,
    board_kind: str,
    board_native_id: str | None,
    before_session: date,
    after_session: date,
    research_cutoff: date,
    before_source_set_id: str,
    before_package_sha256: str,
    before_profile_revision: str,
    after_source_set_id: str,
    after_package_sha256: str,
    after_profile_revision: str,
    offset: int,
    limit: int,
    member_offset: int,
    member_limit: int,
) -> dict[str, Any]:
    try:
        calendar, _metadata, calendar_revision, sessions = _load_calendar(after_session)
    except ResearchEntryError as error:
        raise TdxSectorMembersApplicationError(
            "OFFICIAL_CALENDAR_UNAVAILABLE", str(error)
        ) from error
    session_indexes = {
        session
        if isinstance(session, date)
        else date.fromisoformat(str(session)): index
        for index, session in enumerate(sessions)
    }
    before_index = session_indexes.get(before_session)
    after_index = session_indexes.get(after_session)
    if before_index is None or after_index is None:
        raise TdxSectorMembersApplicationError(
            "PROFILE_SESSION_NOT_IN_OFFICIAL_CALENDAR",
            "两端画像日期必须是所选官方日历中的真实交易会话",
        )
    result = query_sector_breadth(
        config,
        snapshot_id=snapshot_id,
        board_kind=board_kind,
        board_native_id=board_native_id,
        before_session=before_session,
        after_session=after_session,
        research_cutoff=research_cutoff,
        before_source_set_id=before_source_set_id,
        before_package_sha256=before_package_sha256,
        before_profile_revision=before_profile_revision,
        after_source_set_id=after_source_set_id,
        after_package_sha256=after_package_sha256,
        after_profile_revision=after_profile_revision,
        official_calendar_id=str(calendar["calendar_id"]),
        official_calendar_revision=str(calendar_revision),
        offset=offset,
        limit=limit,
        member_offset=member_offset,
        member_limit=member_limit,
    )
    result["session_relation"] = {
        "status": (
            "CONSECUTIVE_OFFICIAL_SESSIONS"
            if after_index == before_index + 1
            else "NONCONSECUTIVE_OFFICIAL_SESSIONS"
        ),
        "before_session": before_session.isoformat(),
        "after_session": after_session.isoformat(),
        "calendar_id": str(calendar["calendar_id"]),
        "calendar_revision": str(calendar_revision),
        "reason": None,
    }
    return result


__all__ = [
    "TdxSectorMembersApplicationError",
    "TdxSectorMembersPgError",
    "TdxSectorSourceError",
    "ingest_tdx_sector_members",
    "read_tdx_sector_catalog",
    "read_tdx_sector_breadth",
    "read_tdx_sector_members",
]
