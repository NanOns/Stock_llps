"""TDX行业与概念成员来源接入和只读查询用例。"""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_EVEN, localcontext
from fractions import Fraction
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
from stock_a.domain import FieldValue, unknown
from stock_a.research.core import (
    CoreObservation,
    MarketSessionCalendar,
    calculate_core,
    calculate_core_return,
)
from stock_a.research.entry import ResearchEntryError, _load_calendar
from stock_a.storage.postgres.tdx_sector_members import (
    TdxSectorMembersPgError,
    TdxSectorStrengthReadContext,
    inspect_remote_sector_source_revisions,
    persist_sector_source_snapshots,
    query_sector_catalog,
    query_sector_breadth,
    query_sector_members,
    query_sector_strength,
    query_latest_sector_source_observation,
    query_tdx_sector_source_snapshot_catalog,
    tdx_sector_strength_read_context,
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
                    "--research-cutoff <YYYY-MM-DD> "
                    "--package-sha256 <PACKAGE_SHA256> [--profile-revision <REVISION>]"
                ),
                "followup_breadth_command_template": (
                    "python -m stock_a source-tdx-sector-members breadth "
                    f"--snapshot-id {snapshot.snapshot_id} --board-kind <INDUSTRY|CONCEPT> "
                    "--before-session <YYYY-MM-DD> --after-session <YYYY-MM-DD> "
                    "--research-cutoff <YYYY-MM-DD> "
                    "--before-package-sha256 <PACKAGE_SHA256> --before-profile-revision <REVISION> "
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


def read_latest_tdx_sector_snapshot_observation(
    config: dict[str, Any],
    *,
    target_session: date,
    package_sha256: str,
    source_profile: dict[str, Any] | None,
) -> dict[str, Any]:
    """只读投影最新行业/GN快照及其与精确画像的成员交集。"""
    try:
        observation = query_latest_sector_source_observation(
            config,
            target_session=target_session,
            package_sha256=package_sha256,
            source_profile=source_profile,
        )
    except TdxSectorMembersPgError as error:
        raise TdxSectorMembersApplicationError(error.code, str(error)) from error
    for snapshot in observation["snapshots"]:
        observed_at = datetime.fromisoformat(snapshot["observed_at"])
        snapshot["observed_after_target"] = observed_at.date() > target_session
        snapshot["source_time_role"] = (
            "OBSERVED_AT_IS_LOCAL_OBSERVATION_NOT_SOURCE_AVAILABILITY"
        )
        snapshot["temporal_relationship"] = (
            "OBSERVED_AFTER_TARGET"
            if snapshot["observed_after_target"]
            else "OBSERVED_ON_OR_BEFORE_TARGET"
        )
        snapshot["membership_mode"] = "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT"
    return observation


def read_tdx_sector_source_snapshot_catalog(
    config: dict[str, Any],
) -> dict[str, Any]:
    """读取全部已保存行业与GN快照，供页面显式绑定来源身份。"""
    try:
        return query_tdx_sector_source_snapshot_catalog(config)
    except TdxSectorMembersPgError as error:
        raise TdxSectorMembersApplicationError(error.code, str(error)) from error


def read_tdx_sector_members(
    config: dict[str, Any],
    *,
    snapshot_id: str,
    board_kind: str,
    board_native_id: str,
    target_session: date,
    research_cutoff: date,
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
    before_package_sha256: str,
    before_profile_revision: str,
    after_package_sha256: str,
    after_profile_revision: str,
    offset: int,
    limit: int,
    member_offset: int,
    member_limit: int,
    include_core_transitions: bool = False,
    core_transition_kind: str = "ALL",
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
        before_package_sha256=before_package_sha256,
        before_profile_revision=before_profile_revision,
        after_package_sha256=after_package_sha256,
        after_profile_revision=after_profile_revision,
        official_calendar_id=str(calendar["calendar_id"]),
        official_calendar_revision=str(calendar_revision),
        offset=offset,
        limit=limit,
        member_offset=member_offset,
        member_limit=member_limit,
        include_core_transitions=include_core_transitions,
        core_transition_kind=core_transition_kind,
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


def _fraction_from_raw(value: Any) -> Fraction:
    if isinstance(value, Fraction):
        return value
    return Fraction(Decimal(str(value)))


def _fraction_payload(value: Fraction | None) -> dict[str, Any] | None:
    if value is None:
        return None
    with localcontext() as context:
        context.prec = 40
        decimal_value = Decimal(value.numerator) / Decimal(value.denominator)
        decimal_value = decimal_value.quantize(
            Decimal("0.00000001"), rounding=ROUND_HALF_EVEN
        )
    return {
        "numerator": value.numerator,
        "denominator": value.denominator,
        "decimal_8dp": format(decimal_value, ".8f"),
    }


def _median_fraction(values: list[Fraction]) -> Fraction | None:
    if not values:
        return None
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2


def _raw_window_observation(
    *,
    source_symbol: str,
    session: date,
    raw_by_key: dict[tuple[str, date], dict[str, Any]],
    revision_counts: dict[tuple[str, date], dict[str, int]],
    target_session: date,
    target_capture_id: str,
) -> tuple[CoreObservation, str | None]:
    key = (source_symbol, session)
    revisions = revision_counts.get(key)
    row = raw_by_key.get(key)
    if revisions is None or revisions["row_count"] == 0:
        reason = "RAW_HISTORY_SESSION_NOT_STORED"
        return (
            CoreObservation(
                session=session,
                state="UNKNOWN",
                coordinate_id="RAW_UNADJUSTED",
                close=unknown(reason),
            ),
            reason,
        )
    if (
        revisions["row_count"] != 1
        or revisions["package_count"] != 1
        or revisions["capture_count"] != 1
    ):
        reason = "SOURCE_REVISION_OR_CAPTURE_AMBIGUOUS"
        return (
            CoreObservation(
                session=session,
                state="UNKNOWN",
                coordinate_id="RAW_UNADJUSTED",
                close=unknown(reason),
            ),
            reason,
        )
    if row is None:
        reason = "SOURCE_PACKAGE_MISMATCH"
        return (
            CoreObservation(
                session=session,
                state="UNKNOWN",
                coordinate_id="RAW_UNADJUSTED",
                close=unknown(reason),
            ),
            reason,
        )
    if session == target_session and row["source_capture_id"] != target_capture_id:
        reason = "TARGET_SOURCE_CAPTURE_MISMATCH"
        return (
            CoreObservation(
                session=session,
                state="UNKNOWN",
                coordinate_id="RAW_UNADJUSTED",
                close=unknown(reason),
            ),
            reason,
        )
    if row.get("price_coordinate") != "RAW_UNADJUSTED":
        reason = "PRICE_COORDINATE_INCOMPATIBLE"
        return (
            CoreObservation(
                session=session,
                state="UNKNOWN",
                coordinate_id=str(row.get("price_coordinate") or "UNKNOWN"),
                close=unknown(reason),
            ),
            reason,
        )
    status = row.get("bar_status")
    if status != "TARGET_PRESENT":
        reason = row.get("bar_reason") or (
            "TARGET_BAR_MISSING"
            if status == "TARGET_MISSING"
            else "SOURCE_ENTRY_READ_FAILED"
        )
        return (
            CoreObservation(
                session=session,
                state="UNKNOWN",
                coordinate_id="RAW_UNADJUSTED",
                close=unknown(str(reason)),
            ),
            str(reason),
        )
    if row.get("price_quality") != "OBSERVED":
        reason = row.get("price_reason") or "RAW_PRICE_QUALITY_UNKNOWN"
        return (
            CoreObservation(
                session=session,
                state="UNKNOWN",
                coordinate_id="RAW_UNADJUSTED",
                close=unknown(str(reason)),
            ),
            str(reason),
        )

    fields: dict[str, FieldValue] = {}
    for name in ("open", "high", "low", "close"):
        value = row.get(f"raw_{name}")
        if value is None:
            fields[name] = unknown(f"RAW_{name.upper()}_MISSING")
        else:
            try:
                fields[name] = FieldValue(_fraction_from_raw(value))
            except (ArithmeticError, TypeError, ValueError):
                fields[name] = unknown(f"DATA_INVALID_{name.upper()}")
    observation = CoreObservation(
        session=session,
        state="ACTUAL",
        coordinate_id="RAW_UNADJUSTED",
        open=fields["open"],
        high=fields["high"],
        low=fields["low"],
        close=fields["close"],
        amount=unknown("UNIT_AUTHORITY_NOT_USED_BY_RETURN"),
        volume=unknown("UNIT_AUTHORITY_NOT_USED_BY_RETURN"),
    )
    return observation, None


def _return_reason(
    result: FieldValue,
    *,
    observations: list[CoreObservation],
    issues: list[str | None],
    periods: int,
) -> str | None:
    if result.quality == "OBSERVED":
        return None
    if len(observations) >= periods + 1:
        window_issues = issues[-periods - 1 :]
        first_issue = next((reason for reason in window_issues if reason), None)
        if first_issue:
            return first_issue
    return result.reason or "CORE_RETURN_UNKNOWN"


def _metric_endpoint_summary(
    members: list[dict[str, Any]],
    member_returns: dict[str, dict[str, Any]],
    *,
    endpoint: str,
    field: str,
) -> dict[str, Any]:
    values: list[Fraction] = []
    unknown_reasons: Counter[str] = Counter()
    for member in members:
        item = member_returns[member["source_symbol"]][endpoint][field]
        if item["value"] is None:
            unknown_reasons[item["reason"] or "CORE_RETURN_UNKNOWN"] += 1
        else:
            values.append(item["value"])
    total = len(members)
    median = _median_fraction(values)
    return {
        "source_member_total": total,
        "evaluated_count": len(values),
        "unknown_count": total - len(values),
        "coverage": {
            "numerator": len(values),
            "denominator": total,
            "fraction": _fraction_payload(
                Fraction(len(values), total) if total else None
            ),
        },
        "median": _fraction_payload(median),
        "median_unknown_reason": "NO_EVALUABLE_MEMBERS" if median is None else None,
        "unknown_reason_counts": [
            {"reason": reason, "count": count}
            for reason, count in sorted(unknown_reasons.items())
        ],
    }


def _summarize_strength_board(
    board: dict[str, Any],
    *,
    member_returns: dict[str, dict[str, Any]],
    target_profile_symbols: set[str],
    target_endpoint: str,
    prior_endpoint: str,
    member_offset: int,
    member_limit: int,
) -> tuple[dict[str, Any], dict[str, Fraction | None]]:
    members = board["members"]
    member_symbols = [member["source_symbol"] for member in members]
    fields = {"ret5": "ret5", "ret20": "ret20"}
    metrics: dict[str, Any] = {}
    sort_values: dict[str, Fraction | None] = {}
    metric_values: dict[str, dict[str, Fraction | None]] = {}
    for label, field in fields.items():
        target_summary = _metric_endpoint_summary(
            members,
            member_returns,
            endpoint=target_endpoint,
            field=field,
        )
        prior_summary = _metric_endpoint_summary(
            members,
            member_returns,
            endpoint=prior_endpoint,
            field=field,
        )
        common = [
            symbol
            for symbol in member_symbols
            if member_returns[symbol][target_endpoint][field]["value"] is not None
            and member_returns[symbol][prior_endpoint][field]["value"] is not None
        ]
        target_common = _median_fraction(
            [
                member_returns[symbol][target_endpoint][field]["value"]
                for symbol in common
            ]
        )
        prior_common = _median_fraction(
            [
                member_returns[symbol][prior_endpoint][field]["value"]
                for symbol in common
            ]
        )
        delta = target_common - prior_common if common else None
        metrics[label] = {
            "target": target_summary,
            "three_official_sessions_before": prior_summary,
            "common_evaluable": {
                "count": len(common),
                "coverage": {
                    "numerator": len(common),
                    "denominator": len(members),
                    "fraction": _fraction_payload(
                        Fraction(len(common), len(members)) if members else None
                    ),
                },
                "target_median": _fraction_payload(target_common),
                "three_official_sessions_before_median": _fraction_payload(
                    prior_common
                ),
                "delta": _fraction_payload(delta),
                "unknown_reason": "NO_COMMON_EVALUABLE_MEMBERS" if not common else None,
            },
        }
        sort_values[f"delta{label[3:]}"] = delta
        sort_values[label] = _median_fraction(
            [
                member_returns[symbol][target_endpoint][field]["value"]
                for symbol in member_symbols
                if member_returns[symbol][target_endpoint][field]["value"] is not None
            ]
        )
        metric_values[label] = {
            "target": sort_values[label],
            "prior": _median_fraction(
                [
                    member_returns[symbol][prior_endpoint][field]["value"]
                    for symbol in member_symbols
                    if member_returns[symbol][prior_endpoint][field]["value"]
                    is not None
                ]
            ),
            "delta": delta,
        }

    target_ret5 = metrics["ret5"]["target"]
    target_ret20 = metrics["ret20"]["target"]
    member_total = len(members)
    ret5_coverage_met = bool(
        member_total and 5 * target_ret5["evaluated_count"] >= 4 * member_total
    )
    ret20_coverage_met = bool(
        member_total and 5 * target_ret20["evaluated_count"] >= 4 * member_total
    )
    meets_minimum = member_total >= 5 and ret5_coverage_met and ret20_coverage_met
    mapped_count = sum(symbol in target_profile_symbols for symbol in member_symbols)
    changed = any(
        metric_values[field]["delta"] is not None and metric_values[field]["delta"] != 0
        for field in ("ret5", "ret20")
    )
    ordered_members = sorted(members, key=lambda row: row["source_symbol"])
    selected_members = ordered_members[member_offset : member_offset + member_limit]
    member_items = []
    for member in selected_members:
        symbol = member["source_symbol"]
        member_metrics = {}
        for label in fields:
            target_value = member_returns[symbol][target_endpoint][label]
            prior_value = member_returns[symbol][prior_endpoint][label]
            member_metrics[label] = {
                "target": {
                    "value": _fraction_payload(target_value["value"]),
                    "quality": "OBSERVED"
                    if target_value["value"] is not None
                    else "UNKNOWN",
                    "reason": target_value["reason"],
                    "value_source": target_value["value_source"],
                },
                "three_official_sessions_before": {
                    "value": _fraction_payload(prior_value["value"]),
                    "quality": "OBSERVED"
                    if prior_value["value"] is not None
                    else "UNKNOWN",
                    "reason": prior_value["reason"],
                    "value_source": prior_value["value_source"],
                },
                "in_common_evaluable_set": (
                    target_value["value"] is not None
                    and prior_value["value"] is not None
                ),
            }
        member_items.append(
            {
                **member,
                "exact_target_core_profile_present": symbol in target_profile_symbols,
                "returns": member_metrics,
            }
        )
    output = {
        "board_kind": board["board_kind"],
        "board_native_id": board["board_native_id"],
        "board_name": board["board_name"],
        "source_member_total": member_total,
        "target_core_profile_mapped_member_count": mapped_count,
        "target_core_profile_unmapped_member_count": member_total - mapped_count,
        "minimum_quality_requirements": {
            "alg21_minimum_source_members": 5,
            "alg21_minimum_coverage": "0.8",
            "meets_source_member_minimum": member_total >= 5,
            "meets_target_ret5_coverage_minimum": ret5_coverage_met,
            "meets_target_ret20_coverage_minimum": ret20_coverage_met,
            "research_fact_gate": (
                "MEETS_ALG21_MINIMUM_THRESHOLDS"
                if meets_minimum
                else "BELOW_ALG21_MINIMUM_THRESHOLDS"
            ),
            "formal_sector_native_qualification": "NOT_EVALUATED_BY_THIS_ACTION",
        },
        "returns": metrics,
        "changed": changed,
        "member_page": {
            "offset": member_offset,
            "limit": member_limit,
            "count": len(member_items),
            "total": member_total,
            "next_offset": (
                member_offset + len(member_items)
                if member_offset + len(member_items) < member_total
                else None
            ),
            "members_omitted_by_request": member_limit == 0,
            "members": member_items,
        },
    }
    return output, sort_values


def _rank_strength_boards(
    summaries: list[dict[str, Any]],
    sort_values: dict[str, dict[str, Fraction | None]],
    *,
    sort_by: str,
    direction: str,
    changed_only: bool,
    offset: int,
    limit: int,
) -> tuple[list[dict[str, Any]], int, int]:
    descending = direction == "STRONGER"

    def sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
        value = sort_values[item["board_native_id"]][sort_by]
        if value is None:
            return True, Fraction(0), item["board_native_id"]
        return False, -value if descending else value, item["board_native_id"]

    ordered = sorted(summaries, key=sort_key)
    filtered = (
        [item for item in ordered if item["changed"]] if changed_only else ordered
    )
    return filtered[offset : offset + limit], len(ordered), len(filtered)


def read_tdx_sector_strength(
    config: dict[str, Any],
    *,
    snapshot_id: str,
    board_kind: str,
    board_native_id: str | None,
    target_session: date,
    research_cutoff: date,
    package_sha256: str,
    profile_revision: str | None,
    changed_only: bool,
    sort_by: str,
    direction: str,
    offset: int,
    limit: int,
    member_offset: int,
    member_limit: int,
    include_all_boards: bool = False,
    include_all_member_symbols: bool = False,
    include_cross_period_inputs: bool = False,
    read_context: TdxSectorStrengthReadContext | None = None,
) -> dict[str, Any]:
    """按完整TDX成员集合计算来源级Core收益中位数及三会话变化。"""
    if sort_by not in {"ret5", "ret20", "delta5", "delta20"}:
        raise TdxSectorMembersApplicationError(
            "STRENGTH_SORT_FIELD_INVALID",
            "强弱排序字段必须为ret5、ret20、delta5或delta20",
        )
    if direction not in {"STRONGER", "WEAKER"}:
        raise TdxSectorMembersApplicationError(
            "STRENGTH_SORT_DIRECTION_INVALID", "排序方向必须为STRONGER或WEAKER"
        )
    if (
        type(offset) is not int
        or offset < 0
        or type(limit) is not int
        or not 1 <= limit <= 100
        or type(member_offset) is not int
        or member_offset < 0
        or type(member_limit) is not int
        or not 0 <= member_limit <= 100
        or type(include_all_boards) is not bool
        or type(include_all_member_symbols) is not bool
        or type(include_cross_period_inputs) is not bool
        or (include_all_member_symbols and not include_all_boards)
    ):
        raise TdxSectorMembersApplicationError(
            "STRENGTH_PAGE_INVALID",
            "板块页须为1至100行；成员页须为0至100行；offset不得为负",
        )
    if type(target_session) is not date or type(research_cutoff) is not date:
        raise TdxSectorMembersApplicationError(
            "SESSION_DATE_INVALID", "目标日与研究截止日必须是有效官方会话日期"
        )
    if target_session > research_cutoff:
        raise TdxSectorMembersApplicationError(
            "SESSION_AFTER_CUTOFF", "目标交易日不得晚于显式研究截止日"
        )
    try:
        calendar, _metadata, calendar_revision, calendar_sessions = _load_calendar(
            target_session
        )
    except ResearchEntryError as error:
        raise TdxSectorMembersApplicationError(error.code, str(error)) from error
    target_index = calendar_sessions.index(target_session)
    if target_index < 23:
        raise TdxSectorMembersApplicationError(
            "STRENGTH_WINDOW_LEFT_TRUNCATED",
            "目标日之前不足24个官方会话，不能同时计算20会话收益及三会话前态",
        )
    window_sessions = tuple(calendar_sessions[target_index - 23 : target_index + 1])
    prior_session = window_sessions[-4]
    if any(session > research_cutoff for session in window_sessions):
        raise TdxSectorMembersApplicationError(
            "SESSION_AFTER_CUTOFF", "价格窗口包含晚于显式研究截止日的官方会话"
        )
    data = query_sector_strength(
        config,
        snapshot_id=snapshot_id,
        board_kind=board_kind,
        board_native_id=board_native_id,
        target_session=target_session,
        research_cutoff=research_cutoff,
        package_sha256=package_sha256,
        profile_revision=profile_revision,
        official_calendar_id=str(calendar["calendar_id"]),
        official_calendar_revision=str(calendar_revision),
        window_sessions=window_sessions,
        read_context=read_context,
    )
    expected_source_kind = (
        "TDX_TDXHY_CFG" if board_kind == "INDUSTRY" else "TDX_INFOHARBOR_BLOCK_DAT"
    )
    if data["snapshot"]["source_kind"] != expected_source_kind:
        raise TdxSectorMembersApplicationError(
            "SNAPSHOT_BOARD_KIND_MISMATCH",
            "TDX成员快照原始来源类型与板块类型不匹配",
        )
    source_version = data["source_version"]
    target_capture_id = source_version["source_capture_id"]
    raw_by_key = {
        (str(row["source_symbol"]), row["session_date"]): row
        for row in data["raw_rows"]
    }
    profile_by_symbol = {str(row["source_symbol"]): row for row in data["profile_rows"]}
    symbols = sorted(
        {
            member["source_symbol"]
            for board in data["boards"]
            for member in board["members"]
        }
    )
    source_entry_sha_by_symbol: dict[str, set[str]] = {
        symbol: set() for symbol in symbols
    }
    for row in data["raw_rows"]:
        entry_sha = row.get("source_entry_sha256")
        if entry_sha:
            source_entry_sha_by_symbol[str(row["source_symbol"])].add(str(entry_sha))
    official_calendar = MarketSessionCalendar(
        calendar_id=str(calendar["calendar_id"]),
        revision=str(calendar_revision),
        sessions=window_sessions,
    )
    target_endpoint = "target"
    prior_endpoint = "three_official_sessions_before"
    endpoint_indexes = {
        target_endpoint: len(window_sessions) - 1,
        prior_endpoint: len(window_sessions) - 4,
    }
    if include_cross_period_inputs:
        endpoint_indexes["previous_official_session"] = len(window_sessions) - 2
    member_returns: dict[str, dict[str, Any]] = {}
    cross_period_inputs: dict[str, dict[str, Any]] = {}
    core_fields_calculated: set[str] = set()
    for symbol in symbols:
        observations: list[CoreObservation] = []
        observation_issues: list[str | None] = []
        for session in window_sessions:
            observation, issue = _raw_window_observation(
                source_symbol=symbol,
                session=session,
                raw_by_key=raw_by_key,
                revision_counts=data["raw_revision_counts"],
                target_session=target_session,
                target_capture_id=target_capture_id,
            )
            observations.append(observation)
            observation_issues.append(issue)
        if len(source_entry_sha_by_symbol[symbol]) > 1:
            observation_issues = [
                issue or "SOURCE_ENTRY_SHA_CONFLICT" for issue in observation_issues
            ]
            observations = [
                CoreObservation(
                    session=row.session,
                    state="UNKNOWN",
                    coordinate_id="RAW_UNADJUSTED",
                    close=unknown("SOURCE_ENTRY_SHA_CONFLICT"),
                )
                for row in observations
            ]
        endpoint_values: dict[str, dict[str, Any]] = {}
        if include_cross_period_inputs:
            cross_period_inputs[symbol] = {"mapped": symbol in profile_by_symbol}
        for endpoint, end_index in endpoint_indexes.items():
            endpoint_history = observations[: end_index + 1]
            endpoint_issues = observation_issues[: end_index + 1]
            endpoint_calendar = MarketSessionCalendar(
                calendar_id=official_calendar.calendar_id,
                revision=official_calendar.revision,
                sessions=window_sessions[: end_index + 1],
            )
            endpoint_values[endpoint] = {}
            if include_cross_period_inputs:
                bar = endpoint_history[-1]
                fields = (bar.open, bar.high, bar.low, bar.close)
                quote = bar.state == "ACTUAL" and all(
                    item is not None and item.quality == "OBSERVED" for item in fields
                )
                if quote:
                    opening, high, low, close = (
                        Fraction(item.value) for item in fields
                    )
                    quote = (
                        min(opening, high, low, close) > 0
                        and low <= min(opening, close)
                        and high >= max(opening, low, close)
                    )
                core = calculate_core(
                    endpoint_history, official_calendar=endpoint_calendar
                )
                core_fields_calculated.update(core)
                ret1, ma20 = core["core.ret1"], core["core.ma20"]
                cross_period_inputs[symbol][endpoint] = {
                    "actual_quote": quote,
                    "ret1": str(ret1.value)
                    if quote and ret1.quality == "OBSERVED"
                    else None,
                    "above_ma20": (
                        close > Fraction(ma20.value)
                        if quote and ma20.quality == "OBSERVED"
                        else None
                    ),
                }
            for periods in (5, 20):
                field = f"ret{periods}"
                result = calculate_core_return(
                    endpoint_history,
                    periods=periods,
                    official_calendar=endpoint_calendar,
                )
                value = result.value if result.quality == "OBSERVED" else None
                reason = _return_reason(
                    result,
                    observations=endpoint_history,
                    issues=endpoint_issues,
                    periods=periods,
                )
                value_source = "CORE_RETURN_KERNEL_FROM_EXACT_PACKAGE_RAW_WINDOW"
                if endpoint == target_endpoint and periods == 20:
                    profile = profile_by_symbol.get(symbol)
                    if profile is None:
                        value = None
                        reason = "CORE_PROFILE_NOT_IN_EXACT_TARGET_SOURCE_SET"
                        value_source = "EXACT_TARGET_CORE_PROFILE_REQUIRED"
                    elif profile["price_coordinate"] != "RAW_UNADJUSTED":
                        value = None
                        reason = "PRICE_COORDINATE_INCOMPATIBLE"
                        value_source = "EXACT_TARGET_CORE_PROFILE_REQUIRED"
                    elif profile["ret20_quality"] != "OBSERVED":
                        value = None
                        reason = profile["ret20_reason"] or (
                            "CORE_RET20_QUALITY_"
                            + str(profile["ret20_quality"] or "UNKNOWN")
                        )
                        value_source = "EXACT_TARGET_CORE_PROFILE"
                    else:
                        numerator = profile["ret20_exact_numerator"]
                        denominator = profile["ret20_exact_denominator"]
                        try:
                            stored_value = Fraction(int(numerator), int(denominator))
                        except (
                            ArithmeticError,
                            TypeError,
                            ValueError,
                            ZeroDivisionError,
                        ):
                            stored_value = None
                        if (
                            stored_value is None
                            or denominator is None
                            or int(denominator) <= 0
                        ):
                            value = None
                            reason = "CORE_RET20_EXACT_VALUE_UNAVAILABLE"
                            value_source = "EXACT_TARGET_CORE_PROFILE"
                        elif (
                            result.quality != "OBSERVED" or stored_value != result.value
                        ):
                            value = None
                            reason = "CORE_PROFILE_RET20_RAW_WINDOW_MISMATCH"
                            value_source = "EXACT_TARGET_CORE_PROFILE_VERIFIED_BY_RAW"
                        else:
                            value = stored_value
                            reason = None
                            value_source = "EXACT_TARGET_CORE_PROFILE_VERIFIED_BY_RAW"
                endpoint_values[endpoint][field] = {
                    "value": value,
                    "reason": reason,
                    "value_source": value_source,
                }
                if include_cross_period_inputs:
                    cross_period_inputs[symbol][endpoint][field] = (
                        str(value) if quote and value is not None else None
                    )
        member_returns[symbol] = endpoint_values

    summaries: list[dict[str, Any]] = []
    sort_values: dict[str, dict[str, Fraction | None]] = {}
    target_profile_symbols = set(profile_by_symbol)
    for board in data["boards"]:
        summary, values = _summarize_strength_board(
            board,
            member_returns=member_returns,
            target_profile_symbols=target_profile_symbols,
            target_endpoint=target_endpoint,
            prior_endpoint=prior_endpoint,
            member_offset=member_offset,
            member_limit=member_limit,
        )
        summaries.append(summary)
        sort_values[board["board_native_id"]] = values
    page, full_board_count, filtered_board_count = _rank_strength_boards(
        summaries,
        sort_values,
        sort_by=sort_by,
        direction=direction,
        changed_only=changed_only,
        offset=offset,
        limit=limit,
    )
    snapshot = data["snapshot"]
    mapped_total = sum(
        summary["target_core_profile_mapped_member_count"] for summary in summaries
    )
    output = {
        "status": "AVAILABLE" if page else ("NO_MATCH" if changed_only else "EMPTY"),
        "analysis_scope": "NON_PIT_RETROSPECTIVE_CURRENT_TDX_MEMBER_CORE_RETURN_STRENGTH",
        "snapshot": {
            "snapshot_id": snapshot["snapshot_id"],
            "source_kind": snapshot["source_kind"],
            "source_file_sha256": snapshot["source_file_sha256"],
            "source_observed_at": snapshot["observed_at"],
            "source_valid_on": snapshot["source_valid_on"],
            "source_known_at": snapshot["source_known_at"],
            "source_available_at": snapshot["source_available_at"],
            "source_clock_quality": snapshot["clock_quality"],
            "source_quality": snapshot["source_quality"],
            "quality_counts": {
                **{
                    key: value
                    for key, value in snapshot.items()
                    if key in {"rows_declared", "parse_contract_version"}
                },
            },
            "research_mode": snapshot["research_mode"],
        },
        "target_source_version": source_version,
        "namespace_id": data["namespace_id"],
        "target_profile_snapshot": data["profile_snapshot"],
        "official_calendar": data["official_calendar"],
        "target_session": target_session.isoformat(),
        "three_official_sessions_before": prior_session.isoformat(),
        "window_sessions": [session.isoformat() for session in window_sessions],
        "research_cutoff": research_cutoff.isoformat(),
        "price_coordinate": "RAW_UNADJUSTED",
        "entity_identity": "SOURCE_SYMBOL_ONLY",
        "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "formal_sector_native_qualification": "NOT_EVALUATED_BY_THIS_ACTION",
        "formal_rps_percentile_created": False,
        "rotation_state_created": False,
        "filter": {
            "board_kind": board_kind,
            "board_native_id": board_native_id,
            "changed_only": changed_only,
            "sort_by": sort_by,
            "direction": direction,
        },
        "board_total_count": full_board_count,
        "board_changed_count": sum(bool(item["changed"]) for item in summaries),
        "board_filtered_count": filtered_board_count,
        "member_relationship_count": data["member_relationship_count"],
        "distinct_source_symbol_count": data["distinct_source_symbol_count"],
        "target_core_profile_mapped_member_relationships": mapped_total,
        "page": {
            "offset": offset,
            "limit": limit,
            "count": len(page),
            "total": filtered_board_count,
            "next_offset": offset + len(page)
            if offset + len(page) < filtered_board_count
            else None,
            "stable_tie_order": "board_native_id ASC COLLATE C",
            "summaries_computed_before_pagination": True,
        },
        "boards": page,
        "return_provenance": {
            "target_ret20": "目标日已存价格画像的Core ret20；仅当精确值与同包Raw Core收益内核一致时接受。",
            "ret5_and_prior_endpoint_returns": "Existing Core return kernel over exact same-package RAW official-session window; no other Core fields recalculated.",
            "comparison": "Each horizon uses its own fixed common evaluable source-symbol set; target and prior medians are both recomputed on that same set.",
            "membership_is_post_hoc": True,
        },
        "database_read": data["database_read"],
        "database_write_attempted": False,
        "ddl_executed": False,
        "dml_executed": False,
        "dml_rows_affected": 0,
        "return_fields_calculated": ["core.ret5", "core.ret20"],
        "non_return_core_factors_recalculated": [],
    }
    if include_all_boards:
        output["all_boards_for_linkage"] = summaries
        output["strength_sort_values_for_linkage"] = {
            native_id: {
                field: (
                    {"numerator": value.numerator, "denominator": value.denominator}
                    if value is not None
                    else None
                )
                for field, value in values.items()
            }
            for native_id, values in sort_values.items()
        }
    if include_cross_period_inputs:
        output["cross_period_inputs_for_linkage"] = cross_period_inputs
        output["return_fields_calculated"] = sorted(
            field for field in core_fields_calculated if field.startswith("core.ret")
        )
        output["non_return_core_factors_recalculated"] = sorted(
            field
            for field in core_fields_calculated
            if not field.startswith("core.ret")
        )
        output["return_provenance"]["ret5_and_prior_endpoint_returns"] = (
            "同包Raw官方会话窗口复用Core收益内核；额外计算ret1和MA20供跨期宽度研究。"
        )
    if include_all_member_symbols:
        output["all_member_symbols_for_linkage"] = {
            board["board_native_id"]: sorted(
                {str(member["source_symbol"]) for member in board["members"]}
            )
            for board in data["boards"]
        }
    return output


__all__ = [
    "TdxSectorMembersApplicationError",
    "TdxSectorMembersPgError",
    "TdxSectorSourceError",
    "ingest_tdx_sector_members",
    "read_tdx_sector_catalog",
    "read_latest_tdx_sector_snapshot_observation",
    "read_tdx_sector_source_snapshot_catalog",
    "read_tdx_sector_breadth",
    "read_tdx_sector_members",
    "read_tdx_sector_strength",
    "TdxSectorStrengthReadContext",
    "tdx_sector_strength_read_context",
]
