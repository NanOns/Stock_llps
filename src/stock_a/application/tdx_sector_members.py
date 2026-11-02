"""TDX行业与概念成员来源接入和只读查询用例。"""

from __future__ import annotations

from datetime import date, datetime
import hashlib
import json
import re
from zoneinfo import ZoneInfo
from typing import Any

from stock_a.sources.tdx.sector_members import (
    SectorSourceInputs,
    TdxSectorSourceError,
    parse_remote_sector_source_inputs_with_status,
    parse_sector_source_inputs,
    read_sector_source_inputs,
)
from stock_a.sources.tdx.remote_sector import (
    TdxRemoteSectorError,
    fetch_remote_sector_files,
    validate_remote_sector_files,
)
from stock_a.research.entry import ResearchEntryError, _load_calendar
from stock_a.storage.postgres.tdx_sector_members import (
    TdxSectorMembersPgError,
    inspect_remote_sector_source_revisions,
    persist_sector_source_snapshots,
    query_sector_catalog,
    query_sector_breadth,
    query_sector_members,
    verify_remote_sector_source_readback,
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


def refresh_remote_tdx_sector_members(
    config: dict[str, Any],
    *,
    validate_only: bool,
    validated_source_sha256: list[str] | None = None,
    oracle_set_sha256: list[str] | None = None,
) -> dict[str, Any]:
    """从有限TDX远端候选校验成员来源，并按已有身份键追加新版本。"""
    try:
        remote = fetch_remote_sector_files(config)
    except TdxRemoteSectorError:
        raise
    except (OSError, TimeoutError) as error:
        raise TdxRemoteSectorError(
            "BLOCKED_REMOTE_SOURCE", "TDX远端成员来源当前不可读取"
        ) from error
    validate_remote_sector_files(remote)

    inputs = SectorSourceInputs(
        root=f"TDX_REMOTE_SERVER/{remote.host}:7709",
        files={item.relative_path: item for item in remote.files.values()},
    )
    observed_at = datetime.now(ZoneInfo("Asia/Shanghai"))
    parse_outcome = parse_remote_sector_source_inputs_with_status(
        inputs, observed_at=observed_at
    )
    snapshots = parse_outcome.snapshots
    if not snapshots:
        if parse_outcome.rejected_sources:
            first_error = parse_outcome.rejected_sources[0]
            raise TdxSectorSourceError(first_error["code"], first_error["message"])
        raise TdxRemoteSectorError(
            "BLOCKED_REMOTE_SOURCE", "TDX远端没有可完整验证的行业或概念成员来源"
        )
    if not 1 <= len(snapshots) <= 2:
        raise TdxSectorMembersApplicationError(
            "REMOTE_SNAPSHOT_COUNT_INVALID", "远端类别快照必须在一至两个之间"
        )

    preflight = inspect_remote_sector_source_revisions(config, snapshots)
    decisions = {item["snapshot_id"]: item for item in preflight["revisions"]}
    insert_candidates = [
        snapshot
        for snapshot in snapshots
        if decisions[snapshot.snapshot_id]["decision"] == "NEW_VERSION"
    ]
    blocked = [
        item
        for item in preflight["revisions"]
        if item["decision"] == "BLOCKED_REVISION_KEY"
    ]

    source_artifacts = {name: item.sha256 for name, item in remote.files.items()}
    if remote.archive_sha256 is not None:
        source_artifacts["zhb.zip"] = remote.archive_sha256
    expected_source_hashes = _parse_hash_bindings(
        validated_source_sha256,
        set(source_artifacts),
        "SOURCE_ORACLE_SHA_INVALID",
    )
    expected_set_hashes = _parse_hash_bindings(
        oracle_set_sha256,
        {"INDUSTRY", "CONCEPT"},
        "INDEPENDENT_ORACLE_SHA_INVALID",
    )
    snapshot_oracle_status: dict[str, str] = {}
    for snapshot in snapshots:
        board_kind = (
            "INDUSTRY" if snapshot.source_kind == "TDX_TDXHY_CFG" else "CONCEPT"
        )
        expected = expected_set_hashes.get(board_kind)
        snapshot_oracle_status[snapshot.snapshot_id] = (
            "MATCHED_EXTERNAL_ORACLE_SHA"
            if expected is not None and expected == _snapshot_set_sha256(snapshot)
            else "EXTERNAL_TRF02_NOT_BOUND"
        )
    if expected_source_hashes and expected_source_hashes != source_artifacts:
        raise TdxRemoteSectorError(
            "REMOTE_SOURCE_ORACLE_SHA_MISMATCH",
            "本次远端原件与TRF独立Oracle核验的文件SHA不一致",
        )
    if expected_set_hashes and any(
        snapshot_oracle_status[item.snapshot_id] != "MATCHED_EXTERNAL_ORACLE_SHA"
        for item in snapshots
        if item.source_kind == "TDX_TDXHY_CFG"
        and "INDUSTRY" in expected_set_hashes
        or item.source_kind == "TDX_INFOHARBOR_BLOCK_DAT"
        and "CONCEPT" in expected_set_hashes
    ):
        raise TdxRemoteSectorError(
            "INDEPENDENT_ORACLE_SHA_MISMATCH",
            "本次解析成员集合与TRF独立Oracle摘要不一致",
        )

    inserted: dict[str, Any] | None = None
    if not validate_only and insert_candidates:
        required_kinds = {
            "INDUSTRY" if item.source_kind == "TDX_TDXHY_CFG" else "CONCEPT"
            for item in insert_candidates
        }
        if (
            expected_source_hashes != source_artifacts
            or not required_kinds.issubset(expected_set_hashes)
            or any(
                snapshot_oracle_status[item.snapshot_id]
                != "MATCHED_EXTERNAL_ORACLE_SHA"
                for item in insert_candidates
            )
        ):
            raise TdxRemoteSectorError(
                "INDEPENDENT_ORACLE_REQUIRED",
                "新增PG来源版本前必须绑定同一原件SHA和外部独立成员集合Oracle SHA",
            )
        insert_catalog = sum(len(item.catalog_rows) for item in insert_candidates)
        insert_members = sum(len(item.member_rows) for item in insert_candidates)
        if (
            len(insert_candidates) > 2
            or insert_catalog > 12_000
            or insert_members > 500_000
        ):
            raise TdxSectorMembersPgError(
                "PG_INSERT_BUDGET_EXCEEDED",
                "本批远端来源新增行数超过已批准的2/12000/500000上限",
            )
        inserted = persist_sector_source_snapshots(config, insert_candidates)

    reusable = [
        snapshot
        for snapshot in snapshots
        if decisions[snapshot.snapshot_id]["decision"] == "REUSE_IDENTICAL_VERSION"
    ]
    readback: dict[str, Any] | None = None
    if not validate_only and reusable:
        readback = verify_remote_sector_source_readback(config, reusable)

    consumer_queries: dict[str, Any] = {}
    for snapshot in snapshots:
        decision = decisions[snapshot.snapshot_id]["decision"]
        if decision == "BLOCKED_REVISION_KEY":
            continue
        if validate_only and decision == "NEW_VERSION":
            continue
        result = query_sector_catalog(
            config,
            snapshot_id=snapshot.snapshot_id,
            board_kind=None,
            search=None,
            offset=0,
            limit=1,
        )
        consumer_queries[snapshot.snapshot_id] = {
            "status": result["status"],
            "total_boards": result["total_count"],
            "snapshot_members": len(snapshot.member_rows),
            "first_board": result["rows"][0] if result["rows"] else None,
            "database_write_attempted": result["database_write_attempted"],
            "ddl_executed": result["ddl_executed"],
        }

    per_snapshot: list[dict[str, Any]] = []
    inserted_by_id = {
        item["snapshot_id"]: item for item in (inserted or {}).get("snapshots", [])
    }
    for snapshot in snapshots:
        decision = decisions[snapshot.snapshot_id]
        if decision["decision"] == "BLOCKED_REVISION_KEY":
            result_status = "BLOCKED_REVISION_KEY"
        elif validate_only:
            result_status = decision["decision"]
        elif snapshot.snapshot_id in inserted_by_id:
            result_status = "INSERTED"
        else:
            result_status = "IDEMPOTENT_ZERO_WRITE"
        per_snapshot.append(
            {
                "snapshot_id": snapshot.snapshot_id,
                "source_kind": snapshot.source_kind,
                "source_file_sha256": snapshot.source_file_sha256,
                "source_file_size": snapshot.source_file_size,
                "source_path_role": snapshot.source_path_role,
                "parse_contract_version": snapshot.parse_contract_version,
                "rows_declared": snapshot.rows_declared,
                "catalog_rows": len(snapshot.catalog_rows),
                "member_rows": len(snapshot.member_rows),
                "unmapped_source_rows": sum(
                    count for _reason, count in snapshot.unresolved_rows
                ),
                "source_quality": snapshot.source_quality,
                "clock_quality": snapshot.clock_quality,
                "research_mode": snapshot.research_mode,
                "independent_oracle_status": snapshot_oracle_status[
                    snapshot.snapshot_id
                ],
                "catalog_membership_set_sha256": _snapshot_set_sha256(snapshot),
                "preflight_decision": decision["decision"],
                "result": result_status,
                "reason_code": decision.get("reason_code"),
                "catalog_query_recheck": consumer_queries.get(snapshot.snapshot_id),
                "followup_catalog_command": (
                    "python -m stock_a source-tdx-sector-members catalog "
                    f"--snapshot-id {snapshot.snapshot_id} --limit 100"
                ),
                "followup_members_command_template": (
                    "python -m stock_a source-tdx-sector-members members "
                    f"--snapshot-id {snapshot.snapshot_id} --board-kind <INDUSTRY|CONCEPT> "
                    "--board-native-id <原生板块ID> --target-session <YYYY-MM-DD> "
                    "--research-cutoff <YYYY-MM-DD> --source-set-id <SEALED_SOURCE_SET_ID> "
                    "--package-sha256 <PACKAGE_SHA256> [--profile-revision <REVISION>]"
                ),
                "followup_breadth_command_template": (
                    "python -m stock_a source-tdx-sector-members breadth "
                    f"--snapshot-id {snapshot.snapshot_id} --board-kind <INDUSTRY|CONCEPT> "
                    "--before-session <YYYY-MM-DD> --after-session <YYYY-MM-DD> "
                    "--research-cutoff <YYYY-MM-DD> --before-source-set-id <SEALED_SOURCE_SET_ID> "
                    "--before-package-sha256 <PACKAGE_SHA256> --before-profile-revision <REVISION> "
                    "--after-source-set-id <SEALED_SOURCE_SET_ID> "
                    "--after-package-sha256 <PACKAGE_SHA256> --after-profile-revision <REVISION>"
                ),
            }
        )

    source_files = []
    for name in ("tdxhy.cfg", "tdxzs.cfg", "infoharbor_block.dat"):
        item = remote.files.get(name)
        if item is not None:
            source_files.append(
                {
                    "name": name,
                    "source": item.source_label,
                    "protocol": remote.methods.get(name),
                    "complete_original_bytes_in_memory": True,
                    "size_bytes": item.size_bytes,
                    "sha256": item.sha256,
                    "archive_sha256": item.source_archive_sha256,
                }
            )
    partial_source = (
        len(snapshots) < 2
        or bool(remote.missing_files)
        or bool(remote.source_failures)
        or bool(parse_outcome.rejected_sources)
    )
    result = {
        "status": (
            "BLOCKED_REVISION_KEY"
            if blocked and len(blocked) == len(snapshots)
            else "PARTIAL_VALIDATED"
            if blocked and validate_only
            else "PARTIAL_REFRESHED"
            if blocked
            else "PARTIAL_VALIDATED"
            if validate_only and partial_source
            else "PARTIAL_REFRESHED"
            if partial_source
            else "VALIDATED_ONLY"
            if validate_only
            else "REFRESHED"
        ),
        "operation": "refresh-remote",
        "validate_only": validate_only,
        "remote_host": remote.host,
        "remote_port": 7709,
        "remote_candidate_sources": list(remote.candidate_sources),
        "candidate_total": remote.candidate_count,
        "candidate_content_verified_count": sum(
            item.get("status") in {"COMPLETE", "PARTIAL"}
            for item in remote.candidate_attempts
        ),
        "candidate_full_service_verified_count": sum(
            item.get("status") == "COMPLETE" for item in remote.candidate_attempts
        ),
        "candidate_skipped_over_budget": remote.skipped_candidate_count,
        "candidate_discovery_sources": list(remote.discovery_sources),
        "candidate_attempts": list(remote.candidate_attempts),
        "missing_files": list(remote.missing_files),
        "source_failures": list(remote.source_failures),
        "source_parse_rejections": list(parse_outcome.rejected_sources),
        "source_files": source_files,
        "source_artifacts_sha256": source_artifacts,
        "zhb_zip": (
            {
                "source": f"TDX_REMOTE_SERVER/{remote.host}:7709/zhb.zip",
                "protocol": "GET_REPORT_FILE_0X06B9",
                "complete_original_bytes_in_memory": True,
                "size_bytes": remote.archive_size_bytes,
                "sha256": remote.archive_sha256,
                "tdxzs_member_crc32": remote.archive_member_crc32,
            }
            if remote.archive_sha256 is not None
            else None
        ),
        "observed_at": observed_at.isoformat(),
        "source_clock_quality": "UNKNOWN",
        "research_mode": "NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT",
        "independent_oracle_status": "EXTERNAL_TRF02_REQUIRED",
        "preflight": preflight,
        "snapshots": per_snapshot,
        "write_summary": (
            {
                "inserted_snapshot_rows": inserted["inserted_snapshot_rows"],
                "inserted_catalog_rows": inserted["inserted_catalog_rows"],
                "inserted_member_rows": inserted["inserted_member_rows"],
                "update_count": 0,
                "delete_count": 0,
                "other_business_table_writes": 0,
                "transaction_count": inserted["transaction_count"],
                "independent_readback_transaction": inserted[
                    "independent_readback_transaction"
                ],
                "independent_readback": inserted["independent_readback"],
            }
            if inserted is not None
            else {
                "inserted_snapshot_rows": 0,
                "inserted_catalog_rows": 0,
                "inserted_member_rows": 0,
                "update_count": 0,
                "delete_count": 0,
                "other_business_table_writes": 0,
                "transaction_count": 0,
                "independent_readback_transaction": bool(readback),
                "independent_readback": (
                    readback["independent_readback"] if readback else {}
                ),
            }
        ),
        "database_write_attempted": bool(inserted),
        "ddl_executed": False,
        "migration_executed": False,
        "source_downloaded": True,
        "source_files_persisted_to_disk": False,
        "tdx_install_modified": False,
    }
    if blocked and result["snapshots"]:
        result["independent_categories_continue"] = True
    if snapshots and all(
        snapshot_oracle_status[item.snapshot_id] == "MATCHED_EXTERNAL_ORACLE_SHA"
        for item in snapshots
    ):
        result["independent_oracle_status"] = "MATCHED_EXTERNAL_ORACLE_SHA"
    return result


def _parse_hash_bindings(
    values: list[str] | None, allowed_names: set[str], error_code: str
) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values or []:
        name, separator, digest = value.partition("=")
        if (
            not separator
            or name not in allowed_names
            or re.fullmatch(r"[0-9a-f]{64}", digest) is None
            or name in result
        ):
            raise TdxRemoteSectorError(
                error_code, "独立来源或成员集合SHA参数必须是唯一的NAME=64位小写SHA"
            )
        result[name] = digest
    return result


def _snapshot_set_sha256(snapshot: Any) -> str:
    canonical = {
        "catalog": sorted(
            [row.board_kind, row.board_native_id, row.board_name]
            for row in snapshot.catalog_rows
        ),
        "members": sorted(
            [
                row.board_kind,
                row.board_native_id,
                row.source_symbol,
                row.membership_quality,
                row.source_record_ordinal,
            ]
            for row in snapshot.member_rows
        ),
    }
    encoded = json.dumps(canonical, ensure_ascii=False, separators=(",", ":")).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).hexdigest()


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
