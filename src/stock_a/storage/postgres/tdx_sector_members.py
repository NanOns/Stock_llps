"""保存并查询TDX行业、概念成员快照的typed PostgreSQL对象。"""

from __future__ import annotations

from collections import Counter
from datetime import date
from decimal import Decimal
from fractions import Fraction
import re
from typing import Any, Iterable, Mapping, Sequence
import uuid

from stock_a.sources.tdx.sector_members import (
    SectorCatalogRow,
    SectorMemberRow,
    SectorSourceSnapshot,
    TdxSectorSourceError,
)
from stock_a.storage.postgres.source_raw import (
    RawSourcePgError,
    _read_history_rows,
    _transaction,
)

from .price_profiles import (
    PriceProfilePgError,
    _PROFILE_CHANGE_CTES,
    _profile_change_snapshot_parameters,
    _snapshot_for_session,
)


MAX_SNAPSHOT_INSERTS = 8
MAX_CATALOG_INSERTS = 12_000
MAX_MEMBER_INSERTS = 500_000
MAX_PAGE_SIZE = 100
MAX_BREADTH_PAGE_SIZE = 30
MAX_STRENGTH_BOARD_PAGE_SIZE = 100
MAX_STRENGTH_MEMBER_PAGE_SIZE = 100
MAX_STRENGTH_HISTORY_ROWS = 500_000
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_CORE_TRANSITION_KIND_TO_STATUS = {
    "DAMAGE_NEW": ("price_damage_transition", "FALSE_TO_TRUE"),
    "DAMAGE_RECOVERED": ("price_damage_transition", "TRUE_TO_FALSE"),
    "SLOPE_NEG_TO_POS": (
        "ma_slope20_transition_status",
        "NEGATIVE_TO_POSITIVE",
    ),
    "SLOPE_POS_TO_NEG": (
        "ma_slope20_transition_status",
        "POSITIVE_TO_NEGATIVE",
    ),
}
_CORE_TRANSITION_KINDS = ("ALL", *_CORE_TRANSITION_KIND_TO_STATUS)
_CORE_TRANSITION_ORDER = {
    kind: index for index, kind in enumerate(_CORE_TRANSITION_KIND_TO_STATUS)
}


class TdxSectorMembersPgError(ValueError):
    """板块成员PG读写违反固定身份、键或范围合同。"""

    preserve_transaction_error = True

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def _snapshot_key(snapshot: SectorSourceSnapshot) -> str:
    key = (
        f"STOCK_A_TDX_SECTOR|{snapshot.source_kind}|"
        f"{snapshot.source_file_sha256}|{snapshot.parse_contract_version}"
    )
    return str(uuid.uuid5(uuid.NAMESPACE_URL, key))


def _validate_snapshot(snapshot: SectorSourceSnapshot) -> None:
    if not isinstance(snapshot, SectorSourceSnapshot):
        raise TdxSectorMembersPgError("SNAPSHOT_INPUT_INVALID", "板块来源快照类型无效")
    if snapshot.snapshot_id != _snapshot_key(snapshot):
        raise TdxSectorMembersPgError(
            "SNAPSHOT_ID_MISMATCH", "来源快照UUID不匹配来源类型、文件SHA与解析合同"
        )
    if (
        snapshot.source_kind not in {"TDX_TDXHY_CFG", "TDX_INFOHARBOR_BLOCK_DAT"}
        or _SHA256.fullmatch(snapshot.source_file_sha256) is None
        or snapshot.source_file_size < 0
        or snapshot.rows_declared < 0
        or snapshot.clock_quality != "UNKNOWN"
        or snapshot.research_mode != "NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT"
        or snapshot.source_valid_on is not None
        or snapshot.source_known_at is not None
        or snapshot.source_available_at is not None
        or snapshot.observed_at.tzinfo is None
    ):
        raise TdxSectorMembersPgError(
            "SNAPSHOT_INPUT_INVALID", "来源快照字段违反本批非PIT合同"
        )
    if len(
        {(row.board_kind, row.board_native_id) for row in snapshot.catalog_rows}
    ) != len(snapshot.catalog_rows):
        raise TdxSectorMembersPgError(
            "CATALOG_KEY_DUPLICATE", "同一来源快照的板块目录业务键重复"
        )
    catalog_keys = {
        (row.board_kind, row.board_native_id) for row in snapshot.catalog_rows
    }
    member_keys = [
        (row.board_kind, row.board_native_id, row.source_symbol)
        for row in snapshot.member_rows
    ]
    if len(member_keys) != len(set(member_keys)):
        raise TdxSectorMembersPgError(
            "MEMBER_KEY_DUPLICATE", "同一来源快照的板块成员业务键重复"
        )
    for row in snapshot.catalog_rows:
        if row.board_kind not in {"INDUSTRY", "CONCEPT"} or not row.board_name.strip():
            raise TdxSectorMembersPgError(
                "CATALOG_INPUT_INVALID", "只接受有真实名称的行业或概念目录行"
            )
    for row in snapshot.member_rows:
        if (
            row.board_kind not in {"INDUSTRY", "CONCEPT"}
            or (row.board_kind, row.board_native_id) not in catalog_keys
            or re.fullmatch(r"(?:SH|SZ|BJ)\.[0-9]{6}", row.source_symbol) is None
            or row.membership_quality != "OBSERVED_SOURCE_RELATION"
            or type(row.source_record_ordinal) is not int
            or row.source_record_ordinal <= 0
        ):
            raise TdxSectorMembersPgError(
                "MEMBER_INPUT_INVALID", "成员行不符合来源板块、代码或记录序号合同"
            )


def _validate_limits(snapshots: Sequence[SectorSourceSnapshot]) -> tuple[int, int]:
    if not snapshots or len(snapshots) > MAX_SNAPSHOT_INSERTS:
        raise TdxSectorMembersPgError(
            "PG_INSERT_BUDGET_EXCEEDED", "来源快照行数必须在1至8之间"
        )
    catalog_count = sum(len(item.catalog_rows) for item in snapshots)
    member_count = sum(len(item.member_rows) for item in snapshots)
    if catalog_count > MAX_CATALOG_INSERTS or member_count > MAX_MEMBER_INSERTS:
        raise TdxSectorMembersPgError(
            "PG_INSERT_BUDGET_EXCEEDED", "目录或成员写入超过本批固定行数上限"
        )
    if len({item.snapshot_id for item in snapshots}) != len(snapshots):
        raise TdxSectorMembersPgError(
            "SNAPSHOT_KEY_DUPLICATE", "本次输入的来源快照键重复"
        )
    return catalog_count, member_count


def _read_snapshot_rows(cursor: Any, snapshot_id: str) -> tuple[dict | None, set, set]:
    cursor.execute(
        """SELECT snapshot_id::text, source_kind, source_file_sha256,
       source_path_role, source_file_size, observed_at, source_valid_on,
       source_known_at, source_available_at, clock_quality, research_mode,
       parse_contract_version, rows_declared, source_quality
FROM stock.tdx_sector_source_snapshots WHERE snapshot_id=%s::uuid""",
        (snapshot_id,),
    )
    row = cursor.fetchone()
    if row is None:
        return None, set(), set()
    snapshot = dict(
        zip(
            (
                "snapshot_id",
                "source_kind",
                "source_file_sha256",
                "source_path_role",
                "source_file_size",
                "observed_at",
                "source_valid_on",
                "source_known_at",
                "source_available_at",
                "clock_quality",
                "research_mode",
                "parse_contract_version",
                "rows_declared",
                "source_quality",
            ),
            row,
        )
    )
    cursor.execute(
        """SELECT board_kind, board_native_id, board_name
FROM stock.tdx_sector_catalog WHERE snapshot_id=%s::uuid""",
        (snapshot_id,),
    )
    catalog = set(cursor.fetchall())
    cursor.execute(
        """SELECT board_kind, board_native_id, source_symbol,
       membership_quality, source_record_ordinal
FROM stock.tdx_sector_members WHERE snapshot_id=%s::uuid""",
        (snapshot_id,),
    )
    members = set(cursor.fetchall())
    return snapshot, catalog, members


def _expected_catalog(snapshot: SectorSourceSnapshot) -> set[tuple[str, str, str]]:
    return {
        (row.board_kind, row.board_native_id, row.board_name)
        for row in snapshot.catalog_rows
    }


def _expected_members(
    snapshot: SectorSourceSnapshot,
) -> set[tuple[str, str, str, str, int]]:
    return {
        (
            row.board_kind,
            row.board_native_id,
            row.source_symbol,
            row.membership_quality,
            row.source_record_ordinal,
        )
        for row in snapshot.member_rows
    }


def _stable_snapshot_matches(actual: dict, expected: SectorSourceSnapshot) -> bool:
    return (
        actual["snapshot_id"] == expected.snapshot_id
        and actual["source_kind"] == expected.source_kind
        and actual["source_file_sha256"].strip() == expected.source_file_sha256
        and actual["source_path_role"] == expected.source_path_role
        and actual["source_file_size"] == expected.source_file_size
        and actual["source_valid_on"] == expected.source_valid_on
        and actual["source_known_at"] == expected.source_known_at
        and actual["source_available_at"] == expected.source_available_at
        and actual["clock_quality"] == expected.clock_quality
        and actual["research_mode"] == expected.research_mode
        and actual["parse_contract_version"] == expected.parse_contract_version
        and actual["rows_declared"] == expected.rows_declared
        and actual["source_quality"] == expected.source_quality
    )


def _assert_existing_snapshot(cursor: Any, snapshot: SectorSourceSnapshot) -> bool:
    actual, catalog, members = _read_snapshot_rows(cursor, snapshot.snapshot_id)
    if actual is None:
        cursor.execute(
            """SELECT snapshot_id::text FROM stock.tdx_sector_source_snapshots
WHERE source_kind=%s AND source_file_sha256=%s AND parse_contract_version=%s""",
            (
                snapshot.source_kind,
                snapshot.source_file_sha256,
                snapshot.parse_contract_version,
            ),
        )
        if cursor.fetchone() is not None:
            raise TdxSectorMembersPgError(
                "SOURCE_REVISION_KEY_CONFLICT",
                "来源类型、文件SHA和解析合同已绑定其它快照UUID",
            )
        return False
    if not _stable_snapshot_matches(actual, snapshot):
        raise TdxSectorMembersPgError(
            "SOURCE_REVISION_CONTENT_CONFLICT",
            "同一来源修订键对应不同目录或时态内容；拒绝覆盖原快照",
        )
    if catalog != _expected_catalog(snapshot) or members != _expected_members(snapshot):
        raise TdxSectorMembersPgError(
            "SOURCE_REVISION_CONTENT_CONFLICT",
            "同一来源修订键的PG目录或成员集合与本次独立解析不一致",
        )
    return True


def _stable_snapshot_matches_without_source_path(
    actual: dict, expected: SectorSourceSnapshot
) -> bool:
    return (
        actual["snapshot_id"] == expected.snapshot_id
        and actual["source_kind"] == expected.source_kind
        and actual["source_file_sha256"].strip() == expected.source_file_sha256
        and actual["source_file_size"] == expected.source_file_size
        and actual["source_valid_on"] == expected.source_valid_on
        and actual["source_known_at"] == expected.source_known_at
        and actual["source_available_at"] == expected.source_available_at
        and actual["clock_quality"] == expected.clock_quality
        and actual["research_mode"] == expected.research_mode
        and actual["parse_contract_version"] == expected.parse_contract_version
        and actual["rows_declared"] == expected.rows_declared
        and actual["source_quality"] == expected.source_quality
    )


def _directory_sha256(source_path_role: str) -> str | None:
    match = re.search(
        r"(?:^|;)DIRECTORY_SHA256=([0-9a-f]{64})(?:;|$)", source_path_role
    )
    return match.group(1) if match else None


def inspect_remote_sector_source_revisions(
    config: dict[str, Any], snapshots: Sequence[SectorSourceSnapshot]
) -> dict[str, Any]:
    """只读核对远端修订键、PG成员集合和三张表当前容量。"""
    if not snapshots or len(snapshots) > 2:
        raise TdxSectorMembersPgError(
            "PG_INSERT_BUDGET_EXCEEDED", "远端板块来源核对最多接受两个类别"
        )
    for snapshot in snapshots:
        _validate_snapshot(snapshot)
    revisions: list[dict[str, Any]] = []
    try:
        with _transaction(config, write=False) as (_connection, cursor, _namespace_id):
            cursor.execute(
                """SELECT
    (SELECT count(*) FROM stock.tdx_sector_source_snapshots),
    (SELECT count(*) FROM stock.tdx_sector_catalog),
    (SELECT count(*) FROM stock.tdx_sector_members)"""
            )
            snapshot_count, catalog_count, member_count = map(int, cursor.fetchone())
            for snapshot in snapshots:
                actual, catalog, members = _read_snapshot_rows(
                    cursor, snapshot.snapshot_id
                )
                if actual is None:
                    cursor.execute(
                        """SELECT snapshot_id::text FROM stock.tdx_sector_source_snapshots
WHERE source_kind=%s AND source_file_sha256=%s AND parse_contract_version=%s""",
                        (
                            snapshot.source_kind,
                            snapshot.source_file_sha256,
                            snapshot.parse_contract_version,
                        ),
                    )
                    collision = cursor.fetchone()
                    revisions.append(
                        {
                            "snapshot_id": snapshot.snapshot_id,
                            "source_kind": snapshot.source_kind,
                            "decision": (
                                "BLOCKED_REVISION_KEY" if collision else "NEW_VERSION"
                            ),
                            "existing_snapshot_id": collision[0] if collision else None,
                            "reason_code": (
                                "SOURCE_REVISION_UNIQUE_KEY_COLLISION"
                                if collision
                                else None
                            ),
                        }
                    )
                    continue

                directory_sha_changed = (
                    snapshot.source_kind == "TDX_TDXHY_CFG"
                    and _directory_sha256(actual["source_path_role"])
                    != _directory_sha256(snapshot.source_path_role)
                )
                content_matches = (
                    _stable_snapshot_matches_without_source_path(actual, snapshot)
                    and catalog == _expected_catalog(snapshot)
                    and members == _expected_members(snapshot)
                )
                if directory_sha_changed:
                    decision = "BLOCKED_REVISION_KEY"
                    reason_code = "INDUSTRY_DIRECTORY_SHA_CHANGED_WITH_SAME_PRIMARY_KEY"
                elif not content_matches:
                    decision = "BLOCKED_REVISION_KEY"
                    reason_code = "SOURCE_REVISION_CONTENT_CONFLICT"
                else:
                    decision = "REUSE_IDENTICAL_VERSION"
                    reason_code = None
                revisions.append(
                    {
                        "snapshot_id": snapshot.snapshot_id,
                        "source_kind": snapshot.source_kind,
                        "decision": decision,
                        "reason_code": reason_code,
                        "existing_source_path_role": actual["source_path_role"],
                        "existing_catalog_rows": len(catalog),
                        "existing_member_rows": len(members),
                    }
                )
    except TdxSectorMembersPgError:
        raise
    except RawSourcePgError as error:
        raise TdxSectorMembersPgError(error.code, str(error)) from error
    return {
        "database_counts": {
            "source_snapshots": snapshot_count,
            "catalog_rows": catalog_count,
            "member_rows": member_count,
        },
        "revisions": revisions,
        "database_write_attempted": False,
        "dml_executed": False,
        "ddl_executed": False,
    }


def verify_remote_sector_source_readback(
    config: dict[str, Any], snapshots: Sequence[SectorSourceSnapshot]
) -> dict[str, Any]:
    """以新只读事务重新核验已存在的来源行与完整集合。"""
    readback: dict[str, dict[str, int | str]] = {}
    try:
        with _transaction(config, write=False) as (_connection, cursor, _namespace_id):
            for snapshot in snapshots:
                actual, catalog, members = _read_snapshot_rows(
                    cursor, snapshot.snapshot_id
                )
                same_directory = (
                    snapshot.source_kind != "TDX_TDXHY_CFG"
                    or _directory_sha256(actual["source_path_role"])
                    == _directory_sha256(snapshot.source_path_role)
                    if actual is not None
                    else False
                )
                if (
                    actual is None
                    or not same_directory
                    or not _stable_snapshot_matches_without_source_path(
                        actual, snapshot
                    )
                    or catalog != _expected_catalog(snapshot)
                    or members != _expected_members(snapshot)
                ):
                    raise TdxSectorMembersPgError(
                        "REMOTE_SOURCE_READBACK_MISMATCH",
                        "远端成员来源经独立只读事务回读不匹配",
                    )
                readback[snapshot.snapshot_id] = {
                    "catalog_rows": len(catalog),
                    "member_rows": len(members),
                    "source_file_sha256": snapshot.source_file_sha256,
                }
    except TdxSectorMembersPgError:
        raise
    except RawSourcePgError as error:
        raise TdxSectorMembersPgError(error.code, str(error)) from error
    return {
        "status": "PASS",
        "independent_readback_transaction": True,
        "independent_readback": readback,
        "database_write_attempted": False,
        "dml_executed": False,
        "ddl_executed": False,
    }


def _insert_snapshot(cursor: Any, snapshot: SectorSourceSnapshot) -> None:
    cursor.execute(
        """INSERT INTO stock.tdx_sector_source_snapshots (
    snapshot_id, source_kind, source_file_sha256, source_path_role,
    source_file_size, observed_at, source_valid_on, source_known_at,
    source_available_at, clock_quality, research_mode,
    parse_contract_version, rows_declared, source_quality
) VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            snapshot.snapshot_id,
            snapshot.source_kind,
            snapshot.source_file_sha256,
            snapshot.source_path_role,
            snapshot.source_file_size,
            snapshot.observed_at,
            snapshot.source_valid_on,
            snapshot.source_known_at,
            snapshot.source_available_at,
            snapshot.clock_quality,
            snapshot.research_mode,
            snapshot.parse_contract_version,
            snapshot.rows_declared,
            snapshot.source_quality,
        ),
    )
    if snapshot.catalog_rows:
        cursor.executemany(
            """INSERT INTO stock.tdx_sector_catalog (
    snapshot_id, board_kind, board_native_id, board_name
) VALUES (%s::uuid, %s, %s, %s)""",
            [
                (
                    snapshot.snapshot_id,
                    row.board_kind,
                    row.board_native_id,
                    row.board_name,
                )
                for row in snapshot.catalog_rows
            ],
        )
    if snapshot.member_rows:
        cursor.executemany(
            """INSERT INTO stock.tdx_sector_members (
    snapshot_id, board_kind, board_native_id, source_symbol,
    membership_quality, source_record_ordinal
) VALUES (%s::uuid, %s, %s, %s, %s, %s)""",
            [
                (
                    snapshot.snapshot_id,
                    row.board_kind,
                    row.board_native_id,
                    row.source_symbol,
                    row.membership_quality,
                    row.source_record_ordinal,
                )
                for row in snapshot.member_rows
            ],
        )


def persist_sector_source_snapshots(
    config: dict[str, Any], snapshots: Sequence[SectorSourceSnapshot]
) -> dict[str, Any]:
    """在单个SERIALIZABLE事务中原子插入最多两个已验证来源快照。"""
    catalog_budget, member_budget = _validate_limits(snapshots)
    for snapshot in snapshots:
        _validate_snapshot(snapshot)
    existed: set[str] = set()
    try:
        with _transaction(config, write=True) as (_connection, cursor, _namespace_id):
            cursor.execute(
                "SELECT source_sha FROM stock.migration_ledger WHERE migration_id=9"
            )
            migration_row = cursor.fetchone()
            if migration_row is None:
                raise TdxSectorMembersPgError(
                    "MIGRATION_0009_REQUIRED", "必须先按本批任务范围应用迁移0009"
                )
            cursor.execute("SELECT count(*) FROM stock.tdx_sector_source_snapshots")
            current_snapshot_count = int(cursor.fetchone()[0])
            if current_snapshot_count > 8:
                raise TdxSectorMembersPgError(
                    "SNAPSHOT_ROW_LIMIT_EXCEEDED", "现有来源快照已超过本批8行预算"
                )
            for snapshot in snapshots:
                if _assert_existing_snapshot(cursor, snapshot):
                    existed.add(snapshot.snapshot_id)
            to_insert = [
                snapshot
                for snapshot in snapshots
                if snapshot.snapshot_id not in existed
            ]
            new_catalog_count = sum(
                len(snapshot.catalog_rows) for snapshot in to_insert
            )
            new_member_count = sum(len(snapshot.member_rows) for snapshot in to_insert)
            if current_snapshot_count + len(to_insert) > MAX_SNAPSHOT_INSERTS:
                raise TdxSectorMembersPgError(
                    "PG_INSERT_BUDGET_EXCEEDED", "本批新增来源快照将超过8行上限"
                )
            cursor.execute(
                """SELECT
    (SELECT count(*) FROM stock.tdx_sector_catalog),
    (SELECT count(*) FROM stock.tdx_sector_members)"""
            )
            current_catalog_count, current_member_count = map(int, cursor.fetchone())
            if (
                current_catalog_count + new_catalog_count > MAX_CATALOG_INSERTS
                or current_member_count + new_member_count > MAX_MEMBER_INSERTS
            ):
                raise TdxSectorMembersPgError(
                    "PG_INSERT_BUDGET_EXCEEDED",
                    "本批新增目录或成员将超过对应正式表行数上限",
                )
            for snapshot in to_insert:
                _insert_snapshot(cursor, snapshot)
    except TdxSectorMembersPgError:
        raise
    except RawSourcePgError as error:
        raise TdxSectorMembersPgError(error.code, str(error)) from error

    readback: dict[str, dict] = {}
    try:
        with _transaction(config, write=False) as (_connection, cursor, _namespace_id):
            for snapshot in snapshots:
                actual, catalog, members = _read_snapshot_rows(
                    cursor, snapshot.snapshot_id
                )
                if (
                    actual is None
                    or not _stable_snapshot_matches(actual, snapshot)
                    or catalog != _expected_catalog(snapshot)
                    or members != _expected_members(snapshot)
                ):
                    raise TdxSectorMembersPgError(
                        "POST_COMMIT_READBACK_MISMATCH",
                        "提交后独立只读事务读回的来源快照或成员集合不匹配",
                    )
                readback[snapshot.snapshot_id] = {
                    "catalog_rows": len(catalog),
                    "member_rows": len(members),
                    "source_file_sha256": snapshot.source_file_sha256,
                }
    except TdxSectorMembersPgError:
        raise
    except RawSourcePgError as error:
        raise TdxSectorMembersPgError(error.code, str(error)) from error

    inserted_snapshots = len(snapshots) - len(existed)
    inserted_catalog = sum(
        len(item.catalog_rows) for item in snapshots if item.snapshot_id not in existed
    )
    inserted_members = sum(
        len(item.member_rows) for item in snapshots if item.snapshot_id not in existed
    )
    return {
        "status": "IDEMPOTENT_READBACK" if not inserted_snapshots else "INSERTED",
        "snapshots": [
            {
                "snapshot_id": item.snapshot_id,
                "source_kind": item.source_kind,
                "source_file_sha256": item.source_file_sha256,
                "source_file_size": item.source_file_size,
                "rows_declared": item.rows_declared,
                "source_quality": item.source_quality,
                "catalog_rows": len(item.catalog_rows),
                "member_rows": len(item.member_rows),
                "unmapped_source_rows": sum(
                    count for _reason, count in item.unresolved_rows
                ),
                "unmapped_reasons": dict(item.unresolved_rows),
            }
            for item in snapshots
        ],
        "inserted_snapshot_rows": inserted_snapshots,
        "inserted_catalog_rows": inserted_catalog,
        "inserted_member_rows": inserted_members,
        "update_count": 0,
        "delete_count": 0,
        "other_business_table_writes": 0,
        "preflight_catalog_rows": catalog_budget,
        "preflight_member_rows": member_budget,
        "transaction_count": 1,
        "independent_readback_transaction": True,
        "independent_readback": readback,
    }


def _snapshot_payload(row: Sequence[Any]) -> dict[str, Any]:
    fields = (
        "snapshot_id",
        "source_kind",
        "source_file_sha256",
        "source_path_role",
        "source_file_size",
        "observed_at",
        "source_valid_on",
        "source_known_at",
        "source_available_at",
        "clock_quality",
        "research_mode",
        "parse_contract_version",
        "rows_declared",
        "source_quality",
    )
    payload = dict(zip(fields, row))
    payload["source_file_sha256"] = str(payload["source_file_sha256"]).strip()
    payload["observed_at"] = payload["observed_at"].isoformat()
    for name in ("source_valid_on",):
        if payload[name] is not None:
            payload[name] = payload[name].isoformat()
    for name in ("source_known_at", "source_available_at"):
        if payload[name] is not None:
            payload[name] = payload[name].isoformat()
    return payload


def _source_quality_counts(value: str) -> dict[str, int | str]:
    parts = value.split(";")
    result: dict[str, int | str] = {"quality": parts[0]}
    for part in parts[1:]:
        if "=" not in part:
            continue
        name, raw_count = part.split("=", 1)
        if raw_count.isdecimal():
            result[name.lower()] = int(raw_count)
    return result


def query_latest_sector_source_observation(
    config: dict[str, Any],
    *,
    target_session: date,
    package_sha256: str,
    source_profile: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """读取最新TDX行业/GN快照摘要，并可选核对其与精确画像的交集。"""
    if (
        type(target_session) is not date
        or not isinstance(package_sha256, str)
        or _SHA256.fullmatch(package_sha256) is None
    ):
        raise TdxSectorMembersPgError(
            "SECTOR_OBSERVATION_TARGET_INVALID", "板块观察必须绑定目标日和完整来源包SHA"
        )
    profile = source_profile if isinstance(source_profile, Mapping) else {}
    selected_profile = profile.get("selected_revision")
    set_comparison = profile.get("set_comparison")
    has_exact_profile = bool(
        profile.get("status") == "PG_SOURCE_PROFILE_STORED"
        and isinstance(selected_profile, Mapping)
        and isinstance(set_comparison, Mapping)
        and set_comparison.get("symbol_sets_match") is True
        and profile.get("target_session") == target_session.isoformat()
        and profile.get("package_sha256") == package_sha256
    )
    source_set_id = profile.get("source_set_id") if has_exact_profile else None
    source_set_revision = (
        profile.get("source_set_revision") if has_exact_profile else None
    )
    source_capture_id = profile.get("source_capture_id") if has_exact_profile else None
    profile_revision = (
        selected_profile.get("profile_revision") if has_exact_profile else None
    )
    try:
        if has_exact_profile:
            source_set_id = str(uuid.UUID(source_set_id))
            source_capture_id = str(uuid.UUID(source_capture_id))
        with _transaction(config, write=False) as (_connection, cursor, namespace_id):
            cursor.execute(
                """WITH latest AS (
    SELECT DISTINCT ON (source_kind)
           snapshot_id,source_kind,source_file_sha256,observed_at,source_valid_on,
           source_known_at,source_available_at,clock_quality,research_mode,
           parse_contract_version,rows_declared,source_quality
    FROM stock.tdx_sector_source_snapshots
    WHERE source_kind IN ('TDX_TDXHY_CFG','TDX_INFOHARBOR_BLOCK_DAT')
    ORDER BY source_kind,observed_at DESC,snapshot_id DESC
)
SELECT s.snapshot_id::text AS snapshot_id,s.source_kind,
       s.source_file_sha256,s.observed_at,s.source_valid_on,
       s.source_known_at,s.source_available_at,s.clock_quality,s.research_mode,
       s.parse_contract_version,s.rows_declared,s.source_quality,
       c.catalog_rows,c.industry_catalog_rows,c.concept_catalog_rows,
       c.excluded_catalog_rows,m.member_rows,m.industry_member_rows,
       m.concept_member_rows,m.distinct_member_symbols,
       m.invalid_membership_quality_rows,
       o.profile_overlap_symbols,o.history_24_session_overlap_symbols,
       o.history_75_session_overlap_symbols
FROM latest s
LEFT JOIN LATERAL (
    SELECT count(*)::bigint AS catalog_rows,
           count(*) FILTER (WHERE board_kind='INDUSTRY')::bigint
               AS industry_catalog_rows,
           count(*) FILTER (WHERE board_kind='CONCEPT')::bigint
               AS concept_catalog_rows,
           count(*) FILTER (WHERE board_kind NOT IN ('INDUSTRY','CONCEPT'))::bigint
               AS excluded_catalog_rows
    FROM stock.tdx_sector_catalog
    WHERE snapshot_id=s.snapshot_id
) c ON TRUE
LEFT JOIN LATERAL (
    SELECT count(*)::bigint AS member_rows,
           count(*) FILTER (WHERE board_kind='INDUSTRY')::bigint
               AS industry_member_rows,
           count(*) FILTER (WHERE board_kind='CONCEPT')::bigint
               AS concept_member_rows,
           count(DISTINCT source_symbol)::bigint AS distinct_member_symbols,
           count(*) FILTER (WHERE membership_quality<>'OBSERVED_SOURCE_RELATION')::bigint
               AS invalid_membership_quality_rows
    FROM stock.tdx_sector_members
    WHERE snapshot_id=s.snapshot_id
) m ON TRUE
LEFT JOIN LATERAL (
    SELECT count(DISTINCT m.source_symbol)::bigint AS profile_overlap_symbols,
           count(DISTINCT m.source_symbol) FILTER (
               WHERE p.history_stored_session_count>=24
                 AND p.history_requested_session_count>=24
           )::bigint AS history_24_session_overlap_symbols,
           count(DISTINCT m.source_symbol) FILTER (
               WHERE p.history_stored_session_count>=75
                 AND p.history_requested_session_count>=75
           )::bigint AS history_75_session_overlap_symbols
    FROM stock.tdx_sector_members m
    JOIN stock.source_symbol_price_profiles p
      ON p.namespace_id=%s::uuid AND p.source_set_id=%s::uuid
     AND p.target_session=%s::date AND p.package_sha256=%s
     AND p.source_set_revision=%s AND p.source_capture_id=%s::uuid
     AND p.profile_revision=%s
     AND p.source_symbol=m.source_symbol
    WHERE m.snapshot_id=s.snapshot_id
) o ON TRUE
ORDER BY s.source_kind""",
                (
                    namespace_id,
                    source_set_id,
                    target_session,
                    package_sha256,
                    source_set_revision,
                    source_capture_id,
                    profile_revision,
                ),
            )
            columns = [column.name for column in cursor.description]
            snapshots = [
                dict(zip(columns, row, strict=True)) for row in cursor.fetchall()
            ]
    except TdxSectorMembersPgError:
        raise
    except (RawSourcePgError, PriceProfilePgError) as error:
        raise TdxSectorMembersPgError(error.code, str(error)) from error
    except Exception as error:
        raise TdxSectorMembersPgError(
            "SECTOR_OBSERVATION_READ_FAILED",
            f"最新TDX板块快照只读摘要失败（{type(error).__name__}）",
        ) from error

    for snapshot in snapshots:
        snapshot["source_file_sha256"] = str(snapshot["source_file_sha256"]).strip()
        snapshot["snapshot_id"] = str(snapshot["snapshot_id"])
        snapshot["observed_at"] = snapshot["observed_at"].isoformat()
        for name in ("source_valid_on",):
            if snapshot[name] is not None:
                snapshot[name] = snapshot[name].isoformat()
        for name in ("source_known_at", "source_available_at"):
            if snapshot[name] is not None:
                snapshot[name] = snapshot[name].isoformat()
        for name, value in tuple(snapshot.items()):
            if name.endswith("_rows") or name.endswith("_symbols"):
                snapshot[name] = int(value or 0)
        snapshot["source_quality_counts"] = _source_quality_counts(
            snapshot["source_quality"]
        )
        snapshot["profile_overlap_checked"] = has_exact_profile

    return {
        "status": "AVAILABLE" if snapshots else "NO_MATCH",
        "namespace_id": namespace_id,
        "target_session": target_session.isoformat(),
        "package_sha256": package_sha256,
        "source_profile_revision": profile_revision,
        "selection_rule": "MAX_OBSERVED_AT_PER_EXACT_TDX_SOURCE_KIND",
        "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
        "snapshots": snapshots,
        "database_write": False,
        "ddl_executed": False,
    }


def query_sector_catalog(
    config: dict[str, Any],
    *,
    snapshot_id: str,
    board_kind: str | None,
    search: str | None,
    offset: int,
    limit: int,
) -> dict[str, Any]:
    _validate_page(offset, limit)
    _validate_snapshot_uuid(snapshot_id)
    if board_kind is not None and board_kind not in {"INDUSTRY", "CONCEPT"}:
        raise TdxSectorMembersPgError(
            "BOARD_KIND_INVALID", "板块类型必须为INDUSTRY或CONCEPT"
        )
    normalized_search = search.strip() if isinstance(search, str) else None
    if normalized_search == "":
        normalized_search = None
    if normalized_search is not None and len(normalized_search) > 100:
        raise TdxSectorMembersPgError(
            "SEARCH_TOO_LONG", "板块名称或编号搜索长度不得超过100"
        )
    kind_filter = board_kind
    search_filter = normalized_search
    try:
        with _transaction(config, write=False) as (_connection, cursor, namespace_id):
            cursor.execute(
                """SELECT snapshot_id::text, source_kind, source_file_sha256,
       source_path_role, source_file_size, observed_at, source_valid_on,
       source_known_at, source_available_at, clock_quality, research_mode,
       parse_contract_version, rows_declared, source_quality
FROM stock.tdx_sector_source_snapshots WHERE snapshot_id=%s::uuid""",
                (snapshot_id,),
            )
            snapshot_row = cursor.fetchone()
            if snapshot_row is None:
                raise TdxSectorMembersPgError(
                    "SECTOR_SNAPSHOT_NOT_FOUND", "未找到精确TDX成员来源快照"
                )
            snapshot = _snapshot_payload(snapshot_row)
            cursor.execute(
                """SELECT count(*)::bigint
FROM stock.tdx_sector_catalog c
WHERE c.snapshot_id=%s::uuid
  AND (%s::text IS NULL OR c.board_kind=%s)
  AND (%s::text IS NULL OR position(lower(%s) in lower(c.board_name))>0
       OR position(%s in c.board_native_id)>0)""",
                (
                    snapshot_id,
                    kind_filter,
                    kind_filter,
                    search_filter,
                    search_filter,
                    search_filter,
                ),
            )
            filtered_count = int(cursor.fetchone()[0])
            cursor.execute(
                """SELECT count(*)::bigint FROM stock.tdx_sector_members
WHERE snapshot_id=%s::uuid""",
                (snapshot_id,),
            )
            snapshot_member_count = int(cursor.fetchone()[0])
            cursor.execute(
                """SELECT c.board_kind, c.board_native_id, c.board_name,
       count(m.source_symbol)::bigint AS source_member_count
FROM stock.tdx_sector_catalog c
LEFT JOIN stock.tdx_sector_members m
  ON m.snapshot_id=c.snapshot_id AND m.board_kind=c.board_kind
 AND m.board_native_id=c.board_native_id
WHERE c.snapshot_id=%s::uuid
  AND (%s::text IS NULL OR c.board_kind=%s)
  AND (%s::text IS NULL OR position(lower(%s) in lower(c.board_name))>0
       OR position(%s in c.board_native_id)>0)
GROUP BY c.board_kind, c.board_native_id, c.board_name
ORDER BY c.board_kind COLLATE "C", c.board_native_id COLLATE "C"
LIMIT %s OFFSET %s""",
                (
                    snapshot_id,
                    kind_filter,
                    kind_filter,
                    search_filter,
                    search_filter,
                    search_filter,
                    limit,
                    offset,
                ),
            )
            rows = [
                {
                    "board_kind": row[0],
                    "board_native_id": row[1],
                    "board_name": row[2],
                    "source_member_count": int(row[3]),
                }
                for row in cursor.fetchall()
            ]
    except TdxSectorMembersPgError:
        raise
    except RawSourcePgError as error:
        raise TdxSectorMembersPgError(error.code, str(error)) from error
    return {
        "status": "AVAILABLE" if filtered_count else "NO_MATCH",
        "namespace_id": namespace_id,
        "snapshot": snapshot,
        "source_quality_counts": _source_quality_counts(snapshot["source_quality"]),
        "snapshot_unmapped_source_rows": max(
            0, int(snapshot["rows_declared"]) - snapshot_member_count
        ),
        "board_kind_filter": kind_filter,
        "search": search_filter,
        "offset": offset,
        "limit": limit,
        "total_count": int(filtered_count),
        "display_count": len(rows),
        "empty_page_out_of_range": bool(filtered_count and offset >= filtered_count),
        "rows": rows,
        "database_write_attempted": False,
        "dml_executed": False,
        "ddl_executed": False,
    }


def query_sector_members(
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
    _validate_page(offset, limit)
    _validate_snapshot_uuid(snapshot_id)
    _validate_source_set_uuid(source_set_id)
    if board_kind not in {"INDUSTRY", "CONCEPT"}:
        raise TdxSectorMembersPgError(
            "BOARD_KIND_INVALID", "板块类型必须为INDUSTRY或CONCEPT"
        )
    if not isinstance(board_native_id, str) or not board_native_id.strip():
        raise TdxSectorMembersPgError("BOARD_ID_INVALID", "必须提供精确TDX原生板块编号")
    if not isinstance(target_session, date) or not isinstance(research_cutoff, date):
        raise TdxSectorMembersPgError(
            "SESSION_DATE_INVALID", "目标日与研究截止日必须是有效日期"
        )
    if target_session > research_cutoff:
        raise TdxSectorMembersPgError(
            "SESSION_AFTER_CUTOFF", "目标交易日不得晚于显式研究截止日"
        )
    if not isinstance(package_sha256, str) or _SHA256.fullmatch(package_sha256) is None:
        raise TdxSectorMembersPgError(
            "PACKAGE_SHA_INVALID", "来源包SHA必须是64位小写十六进制值"
        )
    if profile_revision is not None and _SHA256.fullmatch(profile_revision) is None:
        raise TdxSectorMembersPgError(
            "PROFILE_REVISION_INVALID", "画像修订SHA必须是64位小写十六进制值"
        )

    try:
        with _transaction(config, write=False) as (_connection, cursor, namespace_id):
            cursor.execute(
                """SELECT snapshot_id::text, source_kind, source_file_sha256,
       source_path_role, source_file_size, observed_at, source_valid_on,
       source_known_at, source_available_at, clock_quality, research_mode,
       parse_contract_version, rows_declared, source_quality
FROM stock.tdx_sector_source_snapshots WHERE snapshot_id=%s::uuid""",
                (snapshot_id,),
            )
            snapshot_row = cursor.fetchone()
            if snapshot_row is None:
                raise TdxSectorMembersPgError(
                    "SECTOR_SNAPSHOT_NOT_FOUND", "未找到精确TDX成员来源快照"
                )
            snapshot = _snapshot_payload(snapshot_row)
            if snapshot["research_mode"] != "NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT":
                raise TdxSectorMembersPgError(
                    "SECTOR_SNAPSHOT_MODE_REJECTED", "不接受未经声明的历史PIT成员快照"
                )
            cursor.execute(
                """SELECT source_set_id::text, revision_id, target_session,
       package_sha256, source_capture_id::text, raw_row_count, seal_status,
       durability_status, source_time_quality, source_available_at
FROM stock.source_sets WHERE namespace_id=%s::uuid AND source_set_id=%s::uuid
  AND target_session=%s::date AND package_sha256=%s""",
                (namespace_id, source_set_id, target_session, package_sha256),
            )
            source_set = cursor.fetchone()
            if source_set is None or source_set[6:8] != (
                "SEALED",
                "VERIFIED_POST_COMMIT_READBACK",
            ):
                raise TdxSectorMembersPgError(
                    "SEALED_SOURCE_SET_NOT_FOUND", "未找到完全匹配的SEALED P03来源集合"
                )
            cursor.execute(
                """SELECT count(*)::bigint, count(DISTINCT source_symbol)::bigint,
       count(*) FILTER (WHERE bar_status='TARGET_PRESENT')::bigint,
       count(*) FILTER (WHERE bar_status='TARGET_MISSING')::bigint,
       count(*) FILTER (WHERE bar_status='READ_FAILED')::bigint
FROM stock.source_raw_daily WHERE namespace_id=%s::uuid
  AND source_capture_id=%s::uuid AND session_date=%s::date AND package_sha256=%s""",
                (namespace_id, source_set[4], target_session, package_sha256),
            )
            raw_counts = cursor.fetchone()
            if int(raw_counts[0]) != int(source_set[5]) or int(raw_counts[1]) != int(
                source_set[5]
            ):
                raise TdxSectorMembersPgError(
                    "SEALED_SOURCE_RAW_COUNT_MISMATCH",
                    "精确P03来源集合与Raw代码分母不一致",
                )
            cursor.execute(
                """SELECT DISTINCT profile_revision COLLATE "C" AS profile_revision
FROM stock.source_symbol_price_profiles
WHERE namespace_id=%s::uuid AND source_set_id=%s::uuid
  AND target_session=%s::date AND package_sha256=%s
ORDER BY profile_revision""",
                (namespace_id, source_set_id, target_session, package_sha256),
            )
            profile_revisions = [str(row[0]).strip() for row in cursor.fetchall()]
            selected_profile = profile_revision
            if selected_profile is None and len(profile_revisions) == 1:
                selected_profile = profile_revisions[0]
            elif selected_profile is None and len(profile_revisions) > 1:
                raise TdxSectorMembersPgError(
                    "PROFILE_REVISION_REQUIRED",
                    "该来源集合有多个画像修订；必须显式选择profile revision",
                )
            elif (
                selected_profile is not None
                and selected_profile not in profile_revisions
            ):
                raise TdxSectorMembersPgError(
                    "PROFILE_REVISION_NOT_FOUND", "精确来源集合中没有指定画像修订"
                )
            cursor.execute(
                """SELECT count(*)::bigint,
       count(r.source_symbol)::bigint,
       count(r.source_symbol) FILTER (WHERE r.bar_status='TARGET_PRESENT')::bigint,
       count(r.source_symbol) FILTER (WHERE r.bar_status='TARGET_MISSING')::bigint,
       count(r.source_symbol) FILTER (WHERE r.bar_status='READ_FAILED')::bigint,
       count(p.source_symbol)::bigint
FROM stock.tdx_sector_members m
LEFT JOIN stock.source_raw_daily r ON r.namespace_id=%s::uuid
  AND r.source_capture_id=%s::uuid AND r.source_symbol=m.source_symbol
  AND r.session_date=%s::date AND r.package_sha256=%s
LEFT JOIN stock.source_symbol_price_profiles p ON p.namespace_id=%s::uuid
  AND p.source_set_id=%s::uuid AND p.source_capture_id=%s::uuid
  AND p.target_session=%s::date AND p.package_sha256=%s
  AND p.source_symbol=m.source_symbol AND p.profile_revision=%s
WHERE m.snapshot_id=%s::uuid AND m.board_kind=%s AND m.board_native_id=%s""",
                (
                    namespace_id,
                    source_set[4],
                    target_session,
                    package_sha256,
                    namespace_id,
                    source_set_id,
                    source_set[4],
                    target_session,
                    package_sha256,
                    selected_profile,
                    snapshot_id,
                    board_kind,
                    board_native_id,
                ),
            )
            counts = cursor.fetchone()
            source_member_count = int(counts[0])
            if source_member_count == 0:
                cursor.execute(
                    """SELECT 1 FROM stock.tdx_sector_catalog
WHERE snapshot_id=%s::uuid AND board_kind=%s AND board_native_id=%s""",
                    (snapshot_id, board_kind, board_native_id),
                )
                if cursor.fetchone() is None:
                    raise TdxSectorMembersPgError(
                        "SECTOR_BOARD_NOT_FOUND",
                        "指定板块类型与原生编号不在精确快照目录中",
                    )
            cursor.execute(
                """SELECT m.source_symbol, m.membership_quality,
       m.source_record_ordinal,
       (r.source_symbol IS NOT NULL) AS matched_b_t,
       r.bar_status, r.bar_reason, r.price_quality, r.price_reason,
       r.raw_open::text, r.raw_high::text, r.raw_low::text, r.raw_close::text,
       p.profile_revision, p.price_window_status, p.price_window_reason,
       p.target_bar_status, p.target_price_quality,
       p.ma5::text, p.ma5_quality, p.ma5_reason,
       p.ma10::text, p.ma10_quality, p.ma10_reason,
       p.ma20::text, p.ma20_quality, p.ma20_reason,
       p.ma60::text, p.ma60_quality, p.ma60_reason,
       p.atr20::text, p.atr20_quality, p.atr20_reason,
       p.clv::text, p.clv_quality, p.clv_reason,
       p.prior_high20::text, p.prior_high20_quality, p.prior_high20_reason,
       p.ret20::text, p.ret20_quality, p.ret20_reason,
       p.pos60::text, p.pos60_quality, p.pos60_reason,
       p.bias20_atr::text, p.bias20_atr_quality, p.bias20_atr_reason,
       p.profile_field_ids, p.profile_statuses, p.profile_qualities,
       p.profile_reasons
FROM stock.tdx_sector_members m
LEFT JOIN stock.source_raw_daily r ON r.namespace_id=%s::uuid
  AND r.source_capture_id=%s::uuid AND r.source_symbol=m.source_symbol
  AND r.session_date=%s::date AND r.package_sha256=%s
LEFT JOIN stock.source_symbol_price_profiles p ON p.namespace_id=%s::uuid
  AND p.source_set_id=%s::uuid AND p.source_capture_id=%s::uuid
  AND p.target_session=%s::date AND p.package_sha256=%s
  AND p.source_symbol=m.source_symbol AND p.profile_revision=%s
WHERE m.snapshot_id=%s::uuid AND m.board_kind=%s AND m.board_native_id=%s
ORDER BY m.source_symbol COLLATE "C" LIMIT %s OFFSET %s""",
                (
                    namespace_id,
                    source_set[4],
                    target_session,
                    package_sha256,
                    namespace_id,
                    source_set_id,
                    source_set[4],
                    target_session,
                    package_sha256,
                    selected_profile,
                    snapshot_id,
                    board_kind,
                    board_native_id,
                    limit,
                    offset,
                ),
            )
            page_rows = cursor.fetchall()
    except TdxSectorMembersPgError:
        raise
    except RawSourcePgError as error:
        raise TdxSectorMembersPgError(error.code, str(error)) from error

    rows = [_member_payload(row) for row in page_rows]
    (
        total_member_count,
        matched_count,
        present_count,
        missing_count,
        failed_count,
        profile_count,
    ) = (
        int(counts[0]),
        int(counts[1]),
        int(counts[2]),
        int(counts[3]),
        int(counts[4]),
        int(counts[5]),
    )
    unmatched_count = total_member_count - matched_count
    return {
        "status": "AVAILABLE" if total_member_count else "NO_MATCH",
        "namespace_id": namespace_id,
        "snapshot": snapshot,
        "source_set": {
            "source_set_id": str(source_set[0]),
            "revision_id": int(source_set[1]),
            "target_session": source_set[2].isoformat(),
            "package_sha256": str(source_set[3]).strip(),
            "source_capture_id": str(source_set[4]),
            "raw_row_count": int(source_set[5]),
            "seal_status": source_set[6],
            "durability_status": source_set[7],
            "source_time_quality": source_set[8],
            "source_available_at": source_set[9].isoformat() if source_set[9] else None,
        },
        "profile_revision": selected_profile,
        "profile_revision_selection": (
            "EXPLICIT"
            if profile_revision is not None
            else "UNIQUE_EXISTING_REVISION"
            if selected_profile is not None
            else "NO_PERSISTED_PROFILE"
        ),
        "target_session": target_session.isoformat(),
        "research_cutoff": research_cutoff.isoformat(),
        "membership_mode": "NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT",
        "source_member_count": total_member_count,
        "matched_b_t": matched_count,
        "unmatched_b_t": unmatched_count,
        "unmatched_b_t_reason": "NOT_IN_EXACT_SEALED_B_T;不推断停牌、上市或退市",
        "target_bar_counts_within_matched_b_t": {
            "TARGET_PRESENT": present_count,
            "TARGET_MISSING": missing_count,
            "READ_FAILED": failed_count,
        },
        "profile_available_count": profile_count,
        "profile_missing_count": total_member_count - profile_count,
        "unmapped_source_rows": _source_quality_counts(snapshot["source_quality"]),
        "offset": offset,
        "limit": limit,
        "display_count": len(rows),
        "empty_page_out_of_range": bool(
            total_member_count and offset >= total_member_count
        ),
        "rows": rows,
        "database_write_attempted": False,
        "dml_executed": False,
        "ddl_executed": False,
        "source_zip_opened": False,
        "source_downloaded": False,
    }


def _breadth_source_set(
    cursor: Any,
    *,
    namespace_id: str,
    target_session: date,
    source_set_id: str,
    package_sha256: str,
    profile_revision: str,
) -> dict[str, Any]:
    cursor.execute(
        """SELECT source_set_id::text, target_session, package_sha256,
       revision_id, source_capture_id::text, raw_row_count, seal_status,
       durability_status, source_time_quality, source_available_at
FROM stock.source_sets
WHERE namespace_id=%s::uuid AND source_set_id=%s::uuid
  AND target_session=%s::date AND package_sha256=%s""",
        (namespace_id, source_set_id, target_session, package_sha256),
    )
    row = cursor.fetchone()
    if row is None or row[6:8] != (
        "SEALED",
        "VERIFIED_POST_COMMIT_READBACK",
    ):
        raise TdxSectorMembersPgError(
            "SEALED_SOURCE_SET_NOT_FOUND",
            "未找到完全匹配且已验证耐久性的SEALED P03来源集合",
        )
    raw_count = int(row[5])
    if raw_count > MAX_MEMBER_INSERTS:
        raise TdxSectorMembersPgError(
            "BREADTH_SOURCE_ROW_BUDGET_EXCEEDED",
            "精确SEALED来源集合超过广度读取行数上限",
        )
    cursor.execute(
        """SELECT count(*)::bigint,
       count(DISTINCT source_symbol)::bigint,
       count(*) FILTER (WHERE bar_status='TARGET_PRESENT')::bigint,
       count(*) FILTER (WHERE bar_status='TARGET_MISSING')::bigint,
       count(*) FILTER (WHERE bar_status='READ_FAILED')::bigint
FROM stock.source_raw_daily
WHERE namespace_id=%s::uuid AND source_capture_id=%s::uuid
  AND session_date=%s::date AND package_sha256=%s""",
        (namespace_id, row[4], target_session, package_sha256),
    )
    raw_counts = cursor.fetchone()
    if int(raw_counts[0]) != raw_count or int(raw_counts[1]) != raw_count:
        raise TdxSectorMembersPgError(
            "SEALED_SOURCE_RAW_COUNT_MISMATCH",
            "精确P03来源集合与Raw代码集合分母不一致",
        )
    cursor.execute(
        """SELECT count(*)::bigint,
       count(*) FILTER (WHERE source_set_revision=%s)::bigint,
       count(*) FILTER (WHERE source_capture_id=%s::uuid)::bigint
FROM stock.source_symbol_price_profiles
WHERE namespace_id=%s::uuid AND source_set_id=%s::uuid
  AND target_session=%s::date AND package_sha256=%s
  AND profile_revision=%s""",
        (
            row[3],
            row[4],
            namespace_id,
            source_set_id,
            target_session,
            package_sha256,
            profile_revision,
        ),
    )
    profile_counts = cursor.fetchone()
    if any(int(value) != raw_count for value in profile_counts):
        raise TdxSectorMembersPgError(
            "PROFILE_SOURCE_IDENTITY_MISMATCH",
            "画像行分母、P03修订或source_capture_id与精确SEALED来源集合不一致",
        )
    return {
        "source_set_id": str(row[0]),
        "target_session": row[1],
        "package_sha256": str(row[2]).strip(),
        "revision_id": int(row[3]),
        "source_capture_id": str(row[4]),
        "source_symbol_count": raw_count,
        "seal_status": str(row[6]),
        "durability_status": str(row[7]),
        "source_time_quality": str(row[8]),
        "source_available_at": row[9].isoformat() if row[9] else None,
        "profile_revision": profile_revision,
    }


def _validate_breadth_request(
    *,
    snapshot_id: str,
    board_kind: str,
    board_native_id: str | None,
    before_session: date,
    after_session: date,
    research_cutoff: date,
    identities: Sequence[str],
    offset: int,
    limit: int,
    member_offset: int,
    member_limit: int,
    include_core_transitions: bool,
    core_transition_kind: str,
) -> None:
    _validate_snapshot_uuid(snapshot_id)
    if board_kind not in {"INDUSTRY", "CONCEPT"}:
        raise TdxSectorMembersPgError(
            "BOARD_KIND_INVALID", "板块类型必须为INDUSTRY或CONCEPT"
        )
    if board_native_id is not None and (
        not isinstance(board_native_id, str) or not board_native_id.strip()
    ):
        raise TdxSectorMembersPgError(
            "BOARD_ID_INVALID", "原生板块编号必须是非空精确字符串"
        )
    if type(include_core_transitions) is not bool:
        raise TdxSectorMembersPgError(
            "CORE_TRANSITION_OPTION_INVALID", "Core变化开关必须为布尔值"
        )
    if core_transition_kind not in _CORE_TRANSITION_KINDS:
        raise TdxSectorMembersPgError(
            "CORE_TRANSITION_KIND_INVALID",
            "Core变化类型必须为ALL、DAMAGE_NEW、DAMAGE_RECOVERED、SLOPE_NEG_TO_POS或SLOPE_POS_TO_NEG",
        )
    if core_transition_kind != "ALL" and not include_core_transitions:
        raise TdxSectorMembersPgError(
            "CORE_TRANSITION_OPTION_REQUIRED",
            "指定Core变化类型时必须同时启用--include-core-transitions",
        )
    if core_transition_kind != "ALL" and board_native_id is None:
        raise TdxSectorMembersPgError(
            "CORE_TRANSITION_BOARD_REQUIRED",
            "指定Core变化类型分页时必须提供--board-native-id",
        )
    if (
        type(before_session) is not date
        or type(after_session) is not date
        or type(research_cutoff) is not date
        or before_session >= after_session
        or after_session > research_cutoff
    ):
        raise TdxSectorMembersPgError(
            "BREADTH_SESSION_RANGE_INVALID",
            "对照会话必须递增且结束日不晚于显式研究截止日",
        )
    if len(identities) != 6 or any(not isinstance(value, str) for value in identities):
        raise TdxSectorMembersPgError(
            "BREADTH_PROFILE_IDENTITY_INVALID",
            "两端必须提供有效source_set UUID、包SHA和profile revision SHA",
        )
    _validate_source_set_uuid(identities[0])
    _validate_source_set_uuid(identities[3])
    if any(_SHA256.fullmatch(identities[index]) is None for index in (1, 2, 4, 5)):
        raise TdxSectorMembersPgError(
            "BREADTH_PROFILE_IDENTITY_INVALID",
            "两端必须提供有效source_set UUID、包SHA和profile revision SHA",
        )
    if (
        type(offset) is not int
        or not 0 <= offset <= MAX_CATALOG_INSERTS
        or type(limit) is not int
        or not 1 <= limit <= MAX_BREADTH_PAGE_SIZE
        or type(member_offset) is not int
        or not 0 <= member_offset <= MAX_MEMBER_INSERTS
        or type(member_limit) is not int
        or not 1 <= member_limit <= MAX_BREADTH_PAGE_SIZE
    ):
        raise TdxSectorMembersPgError(
            "BREADTH_PAGE_INVALID",
            "板块和变化成员分页偏移或大小超出固定范围（每页最多30）",
        )


def _reason_counts(counter: Counter[str]) -> list[dict[str, Any]]:
    return [
        {"reason": reason, "count": count} for reason, count in sorted(counter.items())
    ]


def _core_transition_summary(
    *,
    board: dict[str, Any],
    members: dict[str, dict[str, Any]],
    source_snapshot: dict[str, Any],
    before_source: dict[str, Any],
    after_source: dict[str, Any],
    member_offset: int,
    member_limit: int,
    core_transition_kind: str,
    include_member_page: bool,
) -> dict[str, Any]:
    damage_transitions = (
        "FALSE_TO_TRUE",
        "TRUE_TO_FALSE",
        "UNCHANGED_FALSE",
        "UNCHANGED_TRUE",
    )
    signs = ("NEGATIVE", "ZERO", "POSITIVE")
    slope_transitions = tuple(
        f"{before}_TO_{after}" for before in signs for after in signs
    )
    before_profile_count = 0
    after_profile_count = 0
    both_profile_count = 0
    absent_reasons: Counter[str] = Counter()
    metric_counts: dict[str, dict[str, Any]] = {
        "price_damage": {
            "transition_counts": dict.fromkeys(damage_transitions, 0),
            "pair_unknown_count": 0,
            "pair_incomparable_count": 0,
            "not_computed_count": 0,
            "reason_counts": Counter(),
        },
        "ma_slope20_atr": {
            "transition_counts": dict.fromkeys(slope_transitions, 0),
            "pair_unknown_count": 0,
            "pair_incomparable_count": 0,
            "not_computed_count": 0,
            "reason_counts": Counter(),
        },
    }

    for row in members.values():
        before_present = row.get("before_profile_present") is True
        after_present = row.get("after_profile_present") is True
        before_profile_count += int(before_present)
        after_profile_count += int(after_present)
        both_present = before_present and after_present
        both_profile_count += int(both_present)
        if not both_present:
            if before_present:
                absent_reasons["AFTER_PROFILE_ABSENT"] += 1
            elif after_present:
                absent_reasons["BEFORE_PROFILE_ABSENT"] += 1
            else:
                absent_reasons["BOTH_PROFILES_ABSENT"] += 1
            continue

        for name, compatibility_field, reason_field, transition_field, state_fields in (
            (
                "price_damage",
                "price_damage_pair_compatibility",
                "price_damage_pair_reason",
                "price_damage_transition",
                (
                    "before_price_damage_state_reason",
                    "after_price_damage_state_reason",
                ),
            ),
            (
                "ma_slope20_atr",
                "ma_slope20_pair_compatibility",
                "ma_slope20_pair_reason",
                "ma_slope20_transition_status",
                (
                    "before_ma_slope20_atr_state_reason",
                    "after_ma_slope20_atr_state_reason",
                ),
            ),
        ):
            counts = metric_counts[name]
            compatibility = row.get(compatibility_field)
            if compatibility == "PAIR_COMPARABLE":
                transition = row.get(transition_field)
                if transition not in counts["transition_counts"]:
                    raise TdxSectorMembersPgError(
                        "CORE_TRANSITION_STATUS_INVALID",
                        f"{name}可比状态缺少受支持的转移分类",
                    )
                counts["transition_counts"][transition] += 1
            elif compatibility == "PAIR_UNKNOWN":
                counts["pair_unknown_count"] += 1
                reason = row.get(reason_field) or compatibility
                counts["reason_counts"][str(reason)] += 1
            elif compatibility == "PAIR_INCOMPARABLE":
                counts["pair_incomparable_count"] += 1
                reason = row.get(reason_field) or compatibility
                counts["reason_counts"][str(reason)] += 1
            else:
                raise TdxSectorMembersPgError(
                    "CORE_TRANSITION_COMPATIBILITY_INVALID",
                    f"{name}双端画像存在时缺少有效兼容性分类",
                )
            if any(
                row.get(field) == "NOT_COMPUTED_FOR_REVISION" for field in state_fields
            ):
                counts["not_computed_count"] += 1

    absent_count = len(members) - both_profile_count
    if (
        before_profile_count > len(members)
        or after_profile_count > len(members)
        or both_profile_count + absent_count != len(members)
        or sum(absent_reasons.values()) != absent_count
    ):
        raise TdxSectorMembersPgError(
            "CORE_TRANSITION_PROFILE_DENOMINATOR_FAILED",
            "板块双端画像覆盖与缺失分母未闭合",
        )

    metric_payload: dict[str, Any] = {}
    for name, counts in metric_counts.items():
        transitions = counts["transition_counts"]
        comparable_count = sum(transitions.values())
        unknown_count = counts["pair_unknown_count"]
        incomparable_count = counts["pair_incomparable_count"]
        unknown_or_incomparable_count = unknown_count + incomparable_count
        if comparable_count + unknown_or_incomparable_count != both_profile_count:
            raise TdxSectorMembersPgError(
                "CORE_TRANSITION_PAIR_DENOMINATOR_FAILED",
                f"{name}可比、未知/不可比与双端画像分母未闭合",
            )
        payload = {
            "n_both_profile": both_profile_count,
            "n_comparable": comparable_count,
            "n_unknown_or_incomparable": unknown_or_incomparable_count,
            "n_unknown": unknown_count,
            "n_incomparable": incomparable_count,
            "n_not_computed_for_revision": counts["not_computed_count"],
            "n_absent": absent_count,
            "unknown_is_subset_of_unknown_or_incomparable": True,
            "not_computed_is_subset_of_unknown_or_incomparable": True,
            "pair_reason_counts": _reason_counts(counts["reason_counts"]),
        }
        if name == "price_damage":
            payload["transition_counts"] = transitions
        else:
            payload["sign_transition_matrix"] = {
                before: {after: transitions[f"{before}_TO_{after}"] for after in signs}
                for before in signs
            }
        metric_payload[name] = payload

    event_rows: list[dict[str, Any]] = []
    if include_member_page:
        for symbol, row in members.items():
            event_kind = None
            for kind, (
                field,
                expected_status,
            ) in _CORE_TRANSITION_KIND_TO_STATUS.items():
                pair_field = (
                    "price_damage_pair_compatibility"
                    if field == "price_damage_transition"
                    else "ma_slope20_pair_compatibility"
                )
                if (
                    row.get(pair_field) == "PAIR_COMPARABLE"
                    and row.get(field) == expected_status
                    and (core_transition_kind == "ALL" or kind == core_transition_kind)
                ):
                    event_kind = kind
                    break
            if event_kind is None:
                continue

            def endpoint(side: str, source: dict[str, Any]) -> dict[str, Any]:
                return {
                    "target_session": source["target_session"].isoformat(),
                    "source_set_id": source["source_set_id"],
                    "source_capture_id": source["source_capture_id"],
                    "package_sha256": source["package_sha256"],
                    "profile_revision": source["profile_revision"],
                    "raw_close": row.get(f"{side}_raw_close"),
                    "raw_price_quality": row.get(f"{side}_raw_price_quality"),
                    "raw_price_reason": row.get(f"{side}_raw_price_reason"),
                    "price_damage": {
                        "value": row.get(f"{side}_price_damage"),
                        "quality": row.get(f"{side}_price_damage_quality"),
                        "reason": row.get(f"{side}_price_damage_reason"),
                        "state_reason": row.get(f"{side}_price_damage_state_reason"),
                    },
                    "ma_slope20_atr": {
                        "value": row.get(f"{side}_ma_slope20_atr"),
                        "exact_numerator": row.get(
                            f"{side}_ma_slope20_atr_exact_numerator"
                        ),
                        "exact_denominator": row.get(
                            f"{side}_ma_slope20_atr_exact_denominator"
                        ),
                        "quality": row.get(f"{side}_ma_slope20_atr_quality"),
                        "reason": row.get(f"{side}_ma_slope20_atr_reason"),
                        "exact_sign": row.get(f"{side}_ma_slope20_atr_sign"),
                        "state_reason": row.get(f"{side}_ma_slope20_atr_state_reason"),
                    },
                }

            event_rows.append(
                {
                    "core_transition_kind": event_kind,
                    "source_symbol": symbol,
                    "transition": (
                        row["price_damage_transition"]
                        if event_kind.startswith("DAMAGE_")
                        else row["ma_slope20_transition_status"]
                    ),
                    "membership": {
                        "snapshot_id": source_snapshot["snapshot_id"],
                        "board_kind": board["board_kind"],
                        "board_native_id": board["board_native_id"],
                        "membership_quality": row.get("membership_quality"),
                        "mode": source_snapshot["research_mode"],
                        "observed_at": source_snapshot["observed_at"],
                    },
                    "before": endpoint("before", before_source),
                    "after": endpoint("after", after_source),
                    "pair": {
                        "compatibility": (
                            row["price_damage_pair_compatibility"]
                            if event_kind.startswith("DAMAGE_")
                            else row["ma_slope20_pair_compatibility"]
                        ),
                        "reason": (
                            row["price_damage_pair_reason"]
                            if event_kind.startswith("DAMAGE_")
                            else row["ma_slope20_pair_reason"]
                        ),
                    },
                }
            )

        event_rows.sort(
            key=lambda item: (
                _CORE_TRANSITION_ORDER[item["core_transition_kind"]],
                item["source_symbol"],
            )
        )
    selected_total = len(event_rows)
    selected_page = event_rows[member_offset : member_offset + member_limit]
    return {
        "n_members": len(members),
        "n_before_profile": before_profile_count,
        "n_after_profile": after_profile_count,
        "n_both_profile": both_profile_count,
        "n_absent": absent_count,
        "absent_profile_reason_counts": _reason_counts(absent_reasons),
        "price_damage": metric_payload["price_damage"],
        "ma_slope20_atr": metric_payload["ma_slope20_atr"],
        "transition_members": (
            {
                "requested_kind": core_transition_kind,
                "ordering": "CORE_TRANSITION_KIND_THEN_SOURCE_SYMBOL_ASC_C",
                "offset": member_offset,
                "limit": member_limit,
                "total": selected_total,
                "count": len(selected_page),
                "next_offset": (
                    member_offset + len(selected_page)
                    if member_offset + len(selected_page) < selected_total
                    else None
                ),
                "rows": selected_page,
            }
            if include_member_page
            else None
        ),
    }


def _summarize_sector_breadth_rows(
    rows: Sequence[dict[str, Any]],
    *,
    board_native_id: str | None,
    source_snapshot: dict[str, Any],
    before_source: dict[str, Any],
    after_source: dict[str, Any],
    member_offset: int,
    member_limit: int,
    include_core_transitions: bool,
    core_transition_kind: str,
) -> list[dict[str, Any]]:
    relation_names = ("ABOVE", "EQUAL", "BELOW", "UNKNOWN")
    transition_names = tuple(
        f"{before}_TO_{after}"
        for before in ("BELOW", "EQUAL", "ABOVE")
        for after in ("BELOW", "EQUAL", "ABOVE")
    )
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        key = (str(row["board_kind"]), str(row["board_native_id"]))
        item = grouped.setdefault(
            key,
            {
                "board_kind": key[0],
                "board_native_id": key[1],
                "board_name": str(row["board_name"]),
                "members": {},
            },
        )
        symbol = row["source_symbol"]
        if symbol is None:
            continue
        symbol = str(symbol)
        if symbol in item["members"]:
            raise TdxSectorMembersPgError(
                "BREADTH_MEMBER_DUPLICATE",
                "精确板块成员与两日画像连接产生重复来源代码",
            )
        item["members"][symbol] = row

    summaries: list[dict[str, Any]] = []
    for key in sorted(grouped, key=lambda value: (value[0], value[1])):
        board = grouped[key]
        members = board["members"]
        side_counts = {
            side: {
                "source_member_total": len(members),
                "matched_b_t": 0,
                "profile_available": 0,
                "positions": dict.fromkeys(relation_names, 0),
                "unknown_reasons": Counter(),
            }
            for side in ("before", "after")
        }
        transitions = dict.fromkeys(transition_names, 0)
        both_days_matched = 0
        before_only = 0
        after_only = 0
        incompatible_common = 0
        changed_members: list[dict[str, Any]] = []

        for symbol in sorted(members):
            row = members[symbol]
            observations: dict[str, str] = {}
            for side, field_prefix in (("before", "before"), ("after", "after")):
                counts = side_counts[side]
                matched = bool(row[f"{field_prefix}_raw_matched"])
                present = bool(row[f"{field_prefix}_profile_present"])
                if matched:
                    counts["matched_b_t"] += 1
                if present:
                    counts["profile_available"] += 1
                relation = row[f"{field_prefix}_relation"]
                if not matched:
                    relation = "UNKNOWN"
                    reason = "NOT_IN_EXACT_SEALED_B_T"
                elif not present:
                    relation = "UNKNOWN"
                    reason = "PROFILE_NOT_STORED"
                elif relation not in {"ABOVE", "EQUAL", "BELOW"}:
                    relation = "UNKNOWN"
                    reason = row[f"{field_prefix}_unknown_reason"] or "RELATION_UNKNOWN"
                else:
                    reason = None
                counts["positions"][relation] += 1
                if relation == "UNKNOWN":
                    counts["unknown_reasons"][str(reason)] += 1
                else:
                    observations[side] = str(relation)

            before_match = bool(row["before_raw_matched"])
            after_match = bool(row["after_raw_matched"])
            if before_match and after_match:
                both_days_matched += 1
            elif before_match:
                before_only += 1
            elif after_match:
                after_only += 1

            before_relation = observations.get("before")
            after_relation = observations.get("after")
            pair_compatible = (
                before_match
                and after_match
                and bool(row["before_profile_present"])
                and bool(row["after_profile_present"])
                and bool(row["pair_compatible"])
                and before_relation is not None
                and after_relation is not None
            )
            if before_match and after_match and not pair_compatible:
                incompatible_common += 1
            if not pair_compatible:
                continue
            transition = f"{before_relation}_TO_{after_relation}"
            transitions[transition] += 1
            if before_relation != after_relation:
                change_type = (
                    "STRICT_CROSS_UP"
                    if transition == "BELOW_TO_ABOVE"
                    else "STRICT_CROSS_DOWN"
                    if transition == "ABOVE_TO_BELOW"
                    else "EQUAL_BOUNDARY_CHANGE"
                    if "EQUAL" in transition
                    else "RELATION_CHANGED"
                )
                changed_members.append(
                    {
                        "source_symbol": symbol,
                        "before_relation": before_relation,
                        "after_relation": after_relation,
                        "transition": transition,
                        "change_type": change_type,
                        "before_raw_close": row["before_raw_close"],
                        "before_ma20": row["before_ma20"],
                        "before_ma20_quality": row["before_ma20_quality"],
                        "after_raw_close": row["after_raw_close"],
                        "after_ma20": row["after_ma20"],
                        "after_ma20_quality": row["after_ma20_quality"],
                        "membership_quality": row["membership_quality"],
                    }
                )

        member_total = len(members)
        per_day: dict[str, dict[str, Any]] = {}
        for side, counts in side_counts.items():
            positions = counts["positions"]
            if sum(positions.values()) != member_total:
                raise TdxSectorMembersPgError(
                    "BREADTH_POSITION_INVARIANT_FAILED",
                    f"{side}端四类MA20状态未覆盖完整板块成员分母",
                )
            if sum(counts["unknown_reasons"].values()) != positions["UNKNOWN"]:
                raise TdxSectorMembersPgError(
                    "BREADTH_UNKNOWN_REASON_INVARIANT_FAILED",
                    f"{side}端UNKNOWN主原因未互斥覆盖",
                )
            evaluated = sum(positions[name] for name in ("ABOVE", "EQUAL", "BELOW"))
            per_day[side] = {
                "target_session": (
                    before_source["target_session"].isoformat()
                    if side == "before"
                    else after_source["target_session"].isoformat()
                ),
                "source_member_total": member_total,
                "matched_b_t": counts["matched_b_t"],
                "unmatched_b_t": member_total - counts["matched_b_t"],
                "profile_available_count": counts["profile_available"],
                "profile_missing_count": member_total - counts["profile_available"],
                "evaluated_count": evaluated,
                "positions": positions,
                "eligible_coverage_fraction": {
                    "numerator": evaluated,
                    "denominator": member_total,
                },
                "unknown_reasons": [
                    {"reason": reason, "count": count}
                    for reason, count in sorted(counts["unknown_reasons"].items())
                ],
            }

        compatible_count = sum(transitions.values())
        if compatible_count != both_days_matched - incompatible_common:
            raise TdxSectorMembersPgError(
                "BREADTH_COMMON_SET_INVARIANT_FAILED",
                "共同可比集合不等于两端共同Raw成员扣除不可比项",
            )
        before_above = sum(
            count
            for transition, count in transitions.items()
            if transition.startswith("ABOVE_TO_")
        )
        after_above = sum(
            count
            for transition, count in transitions.items()
            if transition.endswith("_TO_ABOVE")
        )
        delta = (
            Fraction(100 * (after_above - before_above), compatible_count)
            if compatible_count
            else None
        )
        changed_members.sort(key=lambda value: value["source_symbol"])
        changed_total = len(changed_members)
        page = changed_members[member_offset : member_offset + member_limit]
        core_transitions = (
            _core_transition_summary(
                board=board,
                members=members,
                source_snapshot=source_snapshot,
                before_source=before_source,
                after_source=after_source,
                member_offset=member_offset,
                member_limit=member_limit,
                core_transition_kind=core_transition_kind,
                include_member_page=board_native_id is not None,
            )
            if include_core_transitions
            else None
        )
        summaries.append(
            {
                "board_kind": board["board_kind"],
                "board_native_id": board["board_native_id"],
                "board_name": board["board_name"],
                "source_member_total": member_total,
                "empty_board": member_total == 0,
                "before": per_day["before"],
                "after": per_day["after"],
                "paired": {
                    "both_days_matched_count": both_days_matched,
                    "before_only_b_t_count": before_only,
                    "after_only_b_t_count": after_only,
                    "incompatible_common_count": incompatible_common,
                    "compatible_count": compatible_count,
                    "transition_matrix": transitions,
                    "strict_cross_up_count": transitions["BELOW_TO_ABOVE"],
                    "strict_cross_down_count": transitions["ABOVE_TO_BELOW"],
                    "equal_transition_count": sum(
                        count
                        for transition, count in transitions.items()
                        if "EQUAL" in transition
                    ),
                    "paired_above_before_count": before_above,
                    "paired_above_after_count": after_above,
                    "delta_above_percentage_points": (
                        {
                            "numerator": 100 * (after_above - before_above),
                            "denominator": compatible_count,
                            "decimal_4dp": f"{Decimal(delta.numerator) / Decimal(delta.denominator):.4f}",
                        }
                        if delta is not None
                        else None
                    ),
                    "zero_common_set_is_unknown": compatible_count == 0,
                },
                "changed_members": page,
                "changed_member_count": changed_total,
                "changed_member_offset": member_offset,
                "changed_member_limit": member_limit,
                "changed_member_display_count": len(page),
                "changed_member_next_offset": (
                    member_offset + len(page)
                    if member_offset + len(page) < changed_total
                    else None
                ),
                "members_action_identity": {
                    "operation": "source-tdx-sector-members members",
                    "snapshot_id": source_snapshot["snapshot_id"],
                    "board_kind": board["board_kind"],
                    "board_native_id": board["board_native_id"],
                    "target_session": after_source["target_session"].isoformat(),
                    "research_cutoff": after_source["research_cutoff"].isoformat(),
                    "source_set_id": after_source["source_set_id"],
                    "package_sha256": after_source["package_sha256"],
                    "profile_revision": after_source["profile_revision"],
                    "offset": 0,
                    "limit": 30,
                },
                **(
                    {"core_transitions": core_transitions}
                    if include_core_transitions
                    else {}
                ),
            }
        )
    return summaries


def query_sector_breadth(
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
    official_calendar_id: str,
    official_calendar_revision: str,
    offset: int,
    limit: int,
    member_offset: int,
    member_limit: int,
    include_core_transitions: bool = False,
    core_transition_kind: str = "ALL",
) -> dict[str, Any]:
    identities = (
        before_source_set_id,
        before_package_sha256,
        before_profile_revision,
        after_source_set_id,
        after_package_sha256,
        after_profile_revision,
    )
    _validate_breadth_request(
        snapshot_id=snapshot_id,
        board_kind=board_kind,
        board_native_id=board_native_id,
        before_session=before_session,
        after_session=after_session,
        research_cutoff=research_cutoff,
        identities=identities,
        offset=offset,
        limit=limit,
        member_offset=member_offset,
        member_limit=member_limit,
        include_core_transitions=include_core_transitions,
        core_transition_kind=core_transition_kind,
    )
    if not official_calendar_id or not official_calendar_revision:
        raise TdxSectorMembersPgError(
            "OFFICIAL_CALENDAR_IDENTITY_INVALID",
            "必须绑定正式官方日历身份及修订",
        )

    try:
        with _transaction(config, write=False) as (_connection, cursor, namespace_id):
            cursor.execute(
                """SELECT snapshot_id::text, source_kind, source_file_sha256,
       source_path_role, source_file_size, observed_at, source_valid_on,
       source_known_at, source_available_at, clock_quality, research_mode,
       parse_contract_version, rows_declared, source_quality
FROM stock.tdx_sector_source_snapshots WHERE snapshot_id=%s::uuid""",
                (snapshot_id,),
            )
            snapshot_row = cursor.fetchone()
            if snapshot_row is None:
                raise TdxSectorMembersPgError(
                    "SECTOR_SNAPSHOT_NOT_FOUND", "未找到精确TDX成员来源快照"
                )
            source_snapshot = _snapshot_payload(snapshot_row)
            if (
                source_snapshot["research_mode"]
                != "NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT"
            ):
                raise TdxSectorMembersPgError(
                    "SECTOR_SNAPSHOT_MODE_REJECTED",
                    "只接受明确标注为当前成员后验回看的TDX来源快照",
                )
            try:
                before_profile = _snapshot_for_session(
                    cursor,
                    namespace_id=namespace_id,
                    target_session=before_session,
                    max_target_session=research_cutoff,
                    source_set_id=before_source_set_id,
                    package_sha256=before_package_sha256,
                    profile_revision=before_profile_revision,
                )
                after_profile = _snapshot_for_session(
                    cursor,
                    namespace_id=namespace_id,
                    target_session=after_session,
                    max_target_session=research_cutoff,
                    source_set_id=after_source_set_id,
                    package_sha256=after_package_sha256,
                    profile_revision=after_profile_revision,
                )
            except PriceProfilePgError as error:
                raise TdxSectorMembersPgError(error.code, str(error)) from error

            for profile in (before_profile, after_profile):
                if (
                    profile["history_calendar_id"] != official_calendar_id
                    or profile["history_calendar_revision"]
                    != official_calendar_revision
                ):
                    raise TdxSectorMembersPgError(
                        "PROFILE_CALENDAR_IDENTITY_MISMATCH",
                        "画像快照官方日历身份或修订与现行官方日历不一致",
                    )

            before_source = _breadth_source_set(
                cursor,
                namespace_id=namespace_id,
                target_session=before_session,
                source_set_id=before_source_set_id,
                package_sha256=before_package_sha256,
                profile_revision=before_profile_revision,
            )
            after_source = _breadth_source_set(
                cursor,
                namespace_id=namespace_id,
                target_session=after_session,
                source_set_id=after_source_set_id,
                package_sha256=after_package_sha256,
                profile_revision=after_profile_revision,
            )
            before_source["research_cutoff"] = research_cutoff
            after_source["research_cutoff"] = research_cutoff

            filter_sql = ""
            filter_params: tuple[Any, ...] = ()
            if board_native_id is not None:
                filter_sql = " AND c.board_native_id=%s"
                filter_params = (board_native_id,)
            cursor.execute(
                """SELECT count(DISTINCT c.board_native_id)::bigint,
       count(m.source_symbol)::bigint
FROM stock.tdx_sector_catalog c
LEFT JOIN stock.tdx_sector_members m
  ON m.snapshot_id=c.snapshot_id AND m.board_kind=c.board_kind
 AND m.board_native_id=c.board_native_id
WHERE c.snapshot_id=%s::uuid AND c.board_kind=%s"""
                + filter_sql,
                (snapshot_id, board_kind, *filter_params),
            )
            catalog_count, member_count = (int(value) for value in cursor.fetchone())
            if board_native_id is not None and catalog_count == 0:
                raise TdxSectorMembersPgError(
                    "SECTOR_BOARD_NOT_FOUND",
                    "指定板块类型和原生编号不在精确成员快照目录中",
                )
            if catalog_count > MAX_CATALOG_INSERTS or member_count > MAX_MEMBER_INSERTS:
                raise TdxSectorMembersPgError(
                    "BREADTH_MEMBER_BUDGET_EXCEEDED",
                    "所选板块目录或完整成员关系超过固定只读预算",
                )

            cursor.execute(
                """SELECT c.board_native_id
FROM stock.tdx_sector_catalog c
WHERE c.snapshot_id=%s::uuid AND c.board_kind=%s"""
                + filter_sql
                + ' ORDER BY c.board_native_id COLLATE "C" OFFSET %s LIMIT %s',
                (
                    snapshot_id,
                    board_kind,
                    *filter_params,
                    offset,
                    limit,
                ),
            )
            page_board_ids = [str(row[0]) for row in cursor.fetchall()]

            member_filter_sql = " AND c.board_native_id=ANY(%s::text[])"
            query = (
                _PROFILE_CHANGE_CTES
                + """
SELECT c.board_kind, c.board_native_id, c.board_name,
       m.source_symbol, m.membership_quality, m.source_record_ordinal,
       (rb.source_symbol IS NOT NULL) AS before_raw_matched,
       rb.raw_close::text AS before_raw_close,
       (ra.source_symbol IS NOT NULL) AS after_raw_matched,
       ra.raw_close::text AS after_raw_close,
       x.before_present AS before_profile_present,
       x.after_present AS after_profile_present,
       x.before_source_pool_close_ma20_relation AS before_relation,
       x.before_source_pool_unknown_reason AS before_unknown_reason,
       x.after_source_pool_close_ma20_relation AS after_relation,
       x.after_source_pool_unknown_reason AS after_unknown_reason,
       x.source_pool_pair_compatible AS pair_compatible,
       x.before_ma20::text AS before_ma20,
       x.before_ma20_quality AS before_ma20_quality,
       x.before_ma20_reason AS before_ma20_reason,
       x.after_ma20::text AS after_ma20,
       x.after_ma20_quality AS after_ma20_quality,
       x.after_ma20_reason AS after_ma20_reason,
       x.before_raw_price_quality AS before_raw_price_quality,
       x.before_raw_price_reason AS before_raw_price_reason,
       x.after_raw_price_quality AS after_raw_price_quality,
       x.after_raw_price_reason AS after_raw_price_reason,
       x.before_price_damage AS before_price_damage,
       x.before_price_damage_quality AS before_price_damage_quality,
       x.before_price_damage_reason AS before_price_damage_reason,
       x.before_price_damage_state_reason AS before_price_damage_state_reason,
       x.after_price_damage AS after_price_damage,
       x.after_price_damage_quality AS after_price_damage_quality,
       x.after_price_damage_reason AS after_price_damage_reason,
       x.after_price_damage_state_reason AS after_price_damage_state_reason,
       x.price_damage_pair_compatibility AS price_damage_pair_compatibility,
       x.price_damage_pair_reason AS price_damage_pair_reason,
       x.price_damage_transition AS price_damage_transition,
       x.before_ma_slope20_atr::text AS before_ma_slope20_atr,
       x.before_ma_slope20_atr_exact_numerator::text
           AS before_ma_slope20_atr_exact_numerator,
       x.before_ma_slope20_atr_exact_denominator::text
           AS before_ma_slope20_atr_exact_denominator,
       x.before_ma_slope20_atr_quality AS before_ma_slope20_atr_quality,
       x.before_ma_slope20_atr_reason AS before_ma_slope20_atr_reason,
       x.before_ma_slope20_atr_sign AS before_ma_slope20_atr_sign,
       x.before_ma_slope20_atr_state_reason
           AS before_ma_slope20_atr_state_reason,
       x.after_ma_slope20_atr::text AS after_ma_slope20_atr,
       x.after_ma_slope20_atr_exact_numerator::text
           AS after_ma_slope20_atr_exact_numerator,
       x.after_ma_slope20_atr_exact_denominator::text
           AS after_ma_slope20_atr_exact_denominator,
       x.after_ma_slope20_atr_quality AS after_ma_slope20_atr_quality,
       x.after_ma_slope20_atr_reason AS after_ma_slope20_atr_reason,
       x.after_ma_slope20_atr_sign AS after_ma_slope20_atr_sign,
       x.after_ma_slope20_atr_state_reason
           AS after_ma_slope20_atr_state_reason,
       x.ma_slope20_pair_compatibility AS ma_slope20_pair_compatibility,
       x.ma_slope20_pair_reason AS ma_slope20_pair_reason,
       x.ma_slope20_transition_status AS ma_slope20_transition_status
FROM stock.tdx_sector_catalog c
LEFT JOIN stock.tdx_sector_members m
  ON m.snapshot_id=c.snapshot_id AND m.board_kind=c.board_kind
 AND m.board_native_id=c.board_native_id
LEFT JOIN classified x ON x.source_symbol=m.source_symbol
LEFT JOIN stock.source_raw_daily rb
  ON rb.namespace_id=%s::uuid AND rb.source_capture_id=%s::uuid
 AND rb.source_symbol=m.source_symbol AND rb.session_date=%s::date
 AND rb.package_sha256=%s
LEFT JOIN stock.source_raw_daily ra
  ON ra.namespace_id=%s::uuid AND ra.source_capture_id=%s::uuid
 AND ra.source_symbol=m.source_symbol AND ra.session_date=%s::date
 AND ra.package_sha256=%s
WHERE c.snapshot_id=%s::uuid AND c.board_kind=%s"""
                + member_filter_sql
                + """
ORDER BY c.board_native_id COLLATE "C", m.source_symbol COLLATE "C"
"""
            )
            parameters = (
                *_profile_change_snapshot_parameters(namespace_id, before_profile),
                *_profile_change_snapshot_parameters(namespace_id, after_profile),
                namespace_id,
                before_source["source_capture_id"],
                before_session,
                before_package_sha256,
                namespace_id,
                after_source["source_capture_id"],
                after_session,
                after_package_sha256,
                snapshot_id,
                board_kind,
                page_board_ids,
            )
            cursor.execute(query, parameters)
            names = [column.name for column in cursor.description]
            member_rows = [
                dict(zip(names, row, strict=True)) for row in cursor.fetchall()
            ]
    except TdxSectorMembersPgError:
        raise
    except RawSourcePgError as error:
        raise TdxSectorMembersPgError(error.code, str(error)) from error

    summaries = _summarize_sector_breadth_rows(
        member_rows,
        board_native_id=board_native_id,
        source_snapshot=source_snapshot,
        before_source=before_source,
        after_source=after_source,
        member_offset=member_offset,
        member_limit=member_limit,
        include_core_transitions=include_core_transitions,
        core_transition_kind=core_transition_kind,
    )
    page = summaries
    return {
        "status": "AVAILABLE" if catalog_count else "EMPTY",
        "analysis_scope": "PERSISTED_TDX_SECTOR_MEMBER_MA20_BREADTH",
        "snapshot": {
            "snapshot_id": source_snapshot["snapshot_id"],
            "source_kind": source_snapshot["source_kind"],
            "source_file_sha256": source_snapshot["source_file_sha256"],
            "source_path_role": source_snapshot["source_path_role"],
            "source_file_size": source_snapshot["source_file_size"],
            "source_observed_at": source_snapshot["observed_at"],
            "source_valid_on": source_snapshot["source_valid_on"],
            "source_known_at": source_snapshot["source_known_at"],
            "source_available_at": source_snapshot["source_available_at"],
            "source_clock_quality": source_snapshot["clock_quality"],
            "research_mode": source_snapshot["research_mode"],
            "source_quality": source_snapshot["source_quality"],
            "quality_counts": _source_quality_counts(source_snapshot["source_quality"]),
        },
        "before_snapshot": before_profile,
        "after_snapshot": after_profile,
        "before_source_set": before_source,
        "after_source_set": after_source,
        "filter": {"board_kind": board_kind, "board_native_id": board_native_id},
        "board_total_count": catalog_count,
        "offset": offset,
        "limit": limit,
        "display_count": len(page),
        "empty_page_out_of_range": bool(catalog_count and offset >= catalog_count),
        "member_offset": member_offset,
        "member_limit": member_limit,
        "boards": page,
        "research_scope": "SOURCE_SYMBOL_ONLY",
        "price_coordinate": "RAW_UNADJUSTED",
        "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE/NON_PIT_RETROSPECTIVE",
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "formal_historical_sector_qualification": "UNKNOWN_NOT_CONNECTED",
        "rotation_score_or_rank_created": False,
        "database_read": {
            "database": "stock_a",
            "schema": "stock",
            "account": "stock_a",
            "server": "127.0.0.1:5432",
            "transaction": "REPEATABLE READ READ ONLY",
            "database_write_attempted": False,
            "ddl_executed": False,
            "dml_executed": False,
            "dml_rows_affected": 0,
            "core_recalculated": False,
            "source_files_read": False,
            "source_downloaded": False,
        },
    }


def query_sector_strength(
    config: dict[str, Any],
    *,
    snapshot_id: str,
    board_kind: str,
    board_native_id: str | None,
    target_session: date,
    research_cutoff: date,
    source_set_id: str,
    package_sha256: str,
    profile_revision: str | None,
    official_calendar_id: str,
    official_calendar_revision: str,
    window_sessions: Sequence[date],
) -> dict[str, Any]:
    """在一个只读快照中读取精确板块、目标P03与同包Raw历史。"""
    _validate_snapshot_uuid(snapshot_id)
    _validate_source_set_uuid(source_set_id)
    if board_kind not in {"INDUSTRY", "CONCEPT"}:
        raise TdxSectorMembersPgError(
            "BOARD_KIND_INVALID", "板块类型必须为INDUSTRY或CONCEPT"
        )
    if board_native_id is not None and (
        not isinstance(board_native_id, str)
        or not board_native_id.strip()
        or len(board_native_id) > 100
    ):
        raise TdxSectorMembersPgError(
            "BOARD_ID_INVALID", "原生板块编号必须是非空且不超过100字的精确值"
        )
    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
    ):
        raise TdxSectorMembersPgError(
            "SESSION_DATE_INVALID", "目标官方会话必须不晚于显式研究截止日"
        )
    if _SHA256.fullmatch(package_sha256) is None:
        raise TdxSectorMembersPgError(
            "PACKAGE_SHA_INVALID", "来源包SHA必须是64位小写十六进制值"
        )
    if profile_revision is not None and _SHA256.fullmatch(profile_revision) is None:
        raise TdxSectorMembersPgError(
            "PROFILE_REVISION_INVALID", "画像修订SHA必须是64位小写十六进制值"
        )
    if (
        not official_calendar_id
        or not official_calendar_revision
        or not isinstance(window_sessions, (tuple, list))
        or not window_sessions
        or any(type(session) is not date for session in window_sessions)
        or tuple(sorted(set(window_sessions))) != tuple(window_sessions)
        or window_sessions[-1] != target_session
        or any(session > research_cutoff for session in window_sessions)
    ):
        raise TdxSectorMembersPgError(
            "OFFICIAL_CALENDAR_IDENTITY_INVALID",
            "必须绑定有序官方会话窗、日历身份及研究截止日",
        )

    try:
        with _transaction(config, write=False) as (_connection, cursor, namespace_id):
            cursor.execute(
                """SELECT snapshot_id::text, source_kind, source_file_sha256,
       source_path_role, source_file_size, observed_at, source_valid_on,
       source_known_at, source_available_at, clock_quality, research_mode,
       parse_contract_version, rows_declared, source_quality
FROM stock.tdx_sector_source_snapshots WHERE snapshot_id=%s::uuid""",
                (snapshot_id,),
            )
            snapshot_row = cursor.fetchone()
            if snapshot_row is None:
                raise TdxSectorMembersPgError(
                    "SECTOR_SNAPSHOT_NOT_FOUND", "未找到精确TDX成员来源快照"
                )
            source_snapshot = _snapshot_payload(snapshot_row)
            if (
                source_snapshot["research_mode"]
                != "NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT"
            ):
                raise TdxSectorMembersPgError(
                    "SECTOR_SNAPSHOT_MODE_REJECTED",
                    "当前TDX成员快照仅可作为NON_PIT后验研究输入",
                )

            cursor.execute(
                """SELECT DISTINCT profile_revision
FROM stock.source_symbol_price_profiles
WHERE namespace_id=%s::uuid AND source_set_id=%s::uuid
  AND target_session=%s::date AND package_sha256=%s
ORDER BY profile_revision""",
                (namespace_id, source_set_id, target_session, package_sha256),
            )
            profile_revisions = [str(row[0]).strip() for row in cursor.fetchall()]
            selected_profile = profile_revision
            if selected_profile is None and len(profile_revisions) == 1:
                selected_profile = profile_revisions[0]
            elif selected_profile is None and len(profile_revisions) > 1:
                raise TdxSectorMembersPgError(
                    "PROFILE_REVISION_REQUIRED",
                    "目标P03来源集合有多个画像修订，必须显式选择profile revision",
                )
            elif selected_profile is None:
                raise TdxSectorMembersPgError(
                    "PROFILE_REVISION_NOT_FOUND",
                    "目标P03来源集合没有已保存的Core画像修订",
                )
            elif selected_profile not in profile_revisions:
                raise TdxSectorMembersPgError(
                    "PROFILE_REVISION_NOT_FOUND",
                    "精确目标P03来源集合中不存在指定画像修订",
                )

            try:
                profile_snapshot = _snapshot_for_session(
                    cursor,
                    namespace_id=namespace_id,
                    target_session=target_session,
                    max_target_session=research_cutoff,
                    source_set_id=source_set_id,
                    package_sha256=package_sha256,
                    profile_revision=selected_profile,
                )
            except PriceProfilePgError as error:
                raise TdxSectorMembersPgError(error.code, str(error)) from error
            if (
                profile_snapshot["history_calendar_id"] != official_calendar_id
                or profile_snapshot["history_calendar_revision"]
                != official_calendar_revision
            ):
                raise TdxSectorMembersPgError(
                    "PROFILE_CALENDAR_IDENTITY_MISMATCH",
                    "目标Core画像绑定的官方日历身份或修订与现行官方日历不一致",
                )
            source_set = _breadth_source_set(
                cursor,
                namespace_id=namespace_id,
                target_session=target_session,
                source_set_id=source_set_id,
                package_sha256=package_sha256,
                profile_revision=selected_profile,
            )

            board_filter_sql = ""
            board_filter_params: tuple[Any, ...] = ()
            if board_native_id is not None:
                board_filter_sql = " AND c.board_native_id=%s"
                board_filter_params = (board_native_id,)
            cursor.execute(
                """SELECT count(DISTINCT c.board_native_id)::bigint,
       count(m.source_symbol)::bigint
FROM stock.tdx_sector_catalog c
LEFT JOIN stock.tdx_sector_members m
  ON m.snapshot_id=c.snapshot_id AND m.board_kind=c.board_kind
 AND m.board_native_id=c.board_native_id
WHERE c.snapshot_id=%s::uuid AND c.board_kind=%s"""
                + board_filter_sql,
                (snapshot_id, board_kind, *board_filter_params),
            )
            board_count, member_count = (int(value) for value in cursor.fetchone())
            if board_native_id is not None and board_count == 0:
                raise TdxSectorMembersPgError(
                    "SECTOR_BOARD_NOT_FOUND",
                    "指定板块类型和原生编号不在精确成员快照目录中",
                )
            if board_count > MAX_CATALOG_INSERTS or member_count > MAX_MEMBER_INSERTS:
                raise TdxSectorMembersPgError(
                    "STRENGTH_MEMBER_BUDGET_EXCEEDED",
                    "所选板块目录或完整成员关系超过固定只读预算",
                )
            cursor.execute(
                """SELECT c.board_kind, c.board_native_id, c.board_name,
       m.source_symbol, m.membership_quality, m.source_record_ordinal
FROM stock.tdx_sector_catalog c
LEFT JOIN stock.tdx_sector_members m
  ON m.snapshot_id=c.snapshot_id AND m.board_kind=c.board_kind
 AND m.board_native_id=c.board_native_id
WHERE c.snapshot_id=%s::uuid AND c.board_kind=%s"""
                + board_filter_sql
                + """
ORDER BY c.board_native_id COLLATE "C", m.source_symbol COLLATE "C" """,
                (snapshot_id, board_kind, *board_filter_params),
            )
            boards: list[dict[str, Any]] = []
            boards_by_id: dict[str, dict[str, Any]] = {}
            symbols: set[str] = set()
            for kind, native_id, name, symbol, quality, ordinal in cursor.fetchall():
                native_id = str(native_id)
                board = boards_by_id.get(native_id)
                if board is None:
                    board = {
                        "board_kind": kind,
                        "board_native_id": native_id,
                        "board_name": name,
                        "members": [],
                    }
                    boards_by_id[native_id] = board
                    boards.append(board)
                if symbol is None:
                    continue
                symbol = str(symbol)
                board["members"].append(
                    {
                        "source_symbol": symbol,
                        "membership_quality": quality,
                        "source_record_ordinal": int(ordinal),
                    }
                )
                symbols.add(symbol)

            window_slots = len(symbols) * len(window_sessions)
            if window_slots > MAX_STRENGTH_HISTORY_ROWS:
                raise TdxSectorMembersPgError(
                    "STRENGTH_HISTORY_BUDGET_EXCEEDED",
                    "板块成员乘官方会话窗超过固定Raw历史读取预算",
                )
            profile_rows: list[dict[str, Any]] = []
            raw_rows: list[dict[str, Any]] = []
            revision_counts: dict[tuple[str, date], dict[str, int]] = {}
            if symbols:
                ordered_symbols = sorted(symbols)
                cursor.execute(
                    """SELECT p.source_symbol, p.source_capture_id::text,
       p.price_coordinate, p.calculation_scope, p.producer_contract,
       p.producer_version, p.producer_contract_sha256::text,
       p.ret20::text, p.ret20_exact_numerator::text,
       p.ret20_exact_denominator::text, p.ret20_quality, p.ret20_reason
FROM stock.source_symbol_price_profiles p
WHERE p.namespace_id=%s::uuid AND p.source_set_id=%s::uuid
  AND p.target_session=%s::date AND p.package_sha256=%s
  AND p.profile_revision=%s AND p.source_symbol=ANY(%s)
ORDER BY p.source_symbol COLLATE "C" """,
                    (
                        namespace_id,
                        source_set_id,
                        target_session,
                        package_sha256,
                        selected_profile,
                        ordered_symbols,
                    ),
                )
                profile_columns = [item.name for item in cursor.description]
                profile_rows = [
                    dict(zip(profile_columns, row, strict=True))
                    for row in cursor.fetchall()
                ]
                cursor.execute(
                    """SELECT source_symbol, session_date,
       count(*)::bigint, count(DISTINCT package_sha256)::bigint,
       count(DISTINCT source_capture_id)::bigint
FROM stock.source_raw_daily
WHERE namespace_id=%s::uuid AND source_symbol=ANY(%s)
  AND session_date=ANY(%s::date[])
GROUP BY source_symbol, session_date""",
                    (namespace_id, ordered_symbols, list(window_sessions)),
                )
                revision_rows = cursor.fetchall()
                revision_total = sum(int(row[2]) for row in revision_rows)
                if revision_total > MAX_STRENGTH_HISTORY_ROWS:
                    raise TdxSectorMembersPgError(
                        "STRENGTH_HISTORY_BUDGET_EXCEEDED",
                        "所选成员官方会话窗的Raw修订总行数超过固定只读预算",
                    )
                revision_counts = {
                    (str(row[0]), row[1]): {
                        "row_count": int(row[2]),
                        "package_count": int(row[3]),
                        "capture_count": int(row[4]),
                    }
                    for row in revision_rows
                }
                raw_rows = _read_history_rows(
                    cursor,
                    namespace_id=namespace_id,
                    source_symbols=ordered_symbols,
                    sessions=window_sessions,
                    package_sha256=package_sha256,
                )
            profile_capture_ids = {
                str(row["source_capture_id"]) for row in profile_rows
            }
            if profile_capture_ids and profile_capture_ids != {
                source_set["source_capture_id"]
            }:
                raise TdxSectorMembersPgError(
                    "PROFILE_SOURCE_IDENTITY_MISMATCH",
                    "TDX板块成员命中的Core画像source_capture_id与SEALED P03不一致",
                )
            supported_profile_producers = {
                "STOCK_A_PG_SOURCE_SYMBOL_CORE_PROFILE_PREVIEW_R1",
                "STOCK_A_PG_SOURCE_SYMBOL_CORE_PROFILE_PREVIEW_R2",
            }
            profile_contract_identities = {
                (
                    row["calculation_scope"],
                    row["producer_contract"],
                    row["producer_version"],
                    row["producer_contract_sha256"],
                )
                for row in profile_rows
            }
            if profile_rows and (
                len(profile_contract_identities) != 1
                or any(
                    row["source_capture_id"] is None
                    or row["calculation_scope"] != "SOURCE_SYMBOL_NON_PIT_PRICE_PROFILE"
                    or row["producer_contract"] not in supported_profile_producers
                    for row in profile_rows
                )
            ):
                raise TdxSectorMembersPgError(
                    "PROFILE_CORE_CONTRACT_MISMATCH",
                    "目标Core画像不属于当前固定来源代码Core合同",
                )
    except TdxSectorMembersPgError:
        raise
    except RawSourcePgError as error:
        raise TdxSectorMembersPgError(error.code, str(error)) from error

    return {
        "namespace_id": namespace_id,
        "snapshot": source_snapshot,
        "source_set": source_set,
        "profile_snapshot": profile_snapshot,
        "official_calendar": {
            "calendar_id": official_calendar_id,
            "calendar_revision": official_calendar_revision,
        },
        "window_sessions": tuple(window_sessions),
        "boards": boards,
        "member_relationship_count": member_count,
        "distinct_source_symbol_count": len(symbols),
        "profile_rows": profile_rows,
        "raw_rows": raw_rows,
        "raw_revision_counts": revision_counts,
        "database_read": {
            "database": "stock_a",
            "schema": "stock",
            "account": "stock_a",
            "server": "127.0.0.1:5432",
            "transaction": "REPEATABLE READ READ ONLY",
            "source_raw_rows_read": len(raw_rows),
            "raw_revision_groups_read": len(revision_counts),
            "profile_rows_read": len(profile_rows),
            "database_write_attempted": False,
            "dml_executed": False,
            "dml_rows_affected": 0,
            "ddl_executed": False,
            "migration_executed": False,
        },
    }


def _member_payload(row: Sequence[Any]) -> dict[str, Any]:
    (
        source_symbol,
        membership_quality,
        source_record_ordinal,
        matched_b_t,
        raw_bar_status,
        raw_bar_reason,
        raw_price_quality,
        raw_price_reason,
        raw_open,
        raw_high,
        raw_low,
        raw_close,
        profile_revision,
        price_window_status,
        price_window_reason,
        target_bar_status,
        target_price_quality,
        ma5,
        ma5_quality,
        ma5_reason,
        ma10,
        ma10_quality,
        ma10_reason,
        ma20,
        ma20_quality,
        ma20_reason,
        ma60,
        ma60_quality,
        ma60_reason,
        atr20,
        atr20_quality,
        atr20_reason,
        clv,
        clv_quality,
        clv_reason,
        prior_high20,
        prior_high20_quality,
        prior_high20_reason,
        ret20,
        ret20_quality,
        ret20_reason,
        pos60,
        pos60_quality,
        pos60_reason,
        bias20_atr,
        bias20_atr_quality,
        bias20_atr_reason,
        profile_field_ids,
        profile_statuses,
        profile_qualities,
        profile_reasons,
    ) = row
    profile = None
    if profile_revision is not None:
        profile = {
            "revision": str(profile_revision).strip(),
            "price_window_status": price_window_status,
            "price_window_reason": price_window_reason,
            "target_bar_status": target_bar_status,
            "target_price_quality": target_price_quality,
            "core_fields": {
                "ma5": _quality_value(ma5, ma5_quality, ma5_reason),
                "ma10": _quality_value(ma10, ma10_quality, ma10_reason),
                "ma20": _quality_value(ma20, ma20_quality, ma20_reason),
                "ma60": _quality_value(ma60, ma60_quality, ma60_reason),
                "atr20": _quality_value(atr20, atr20_quality, atr20_reason),
                "clv": _quality_value(clv, clv_quality, clv_reason),
                "prior_high20": _quality_value(
                    prior_high20, prior_high20_quality, prior_high20_reason
                ),
                "ret20": _quality_value(ret20, ret20_quality, ret20_reason),
                "pos60": _quality_value(pos60, pos60_quality, pos60_reason),
                "bias20_atr": _quality_value(
                    bias20_atr, bias20_atr_quality, bias20_atr_reason
                ),
            },
            "profile_fields": _profile_fields(
                profile_field_ids,
                profile_statuses,
                profile_qualities,
                profile_reasons,
            ),
        }
    return {
        "source_symbol": source_symbol,
        "identity_status": "SOURCE_SYMBOL_ONLY;STABLE_SECURITY_ID_UNVERIFIED",
        "membership_quality": membership_quality,
        "source_record_ordinal": int(source_record_ordinal),
        "b_t_membership_status": "MATCHED_B_T" if matched_b_t else "UNMATCHED_B_T",
        "b_t_unmatched_reason": None if matched_b_t else "NOT_IN_EXACT_SEALED_B_T",
        "raw": {
            "bar_status": raw_bar_status if matched_b_t else "NOT_IN_B_T",
            "bar_reason": raw_bar_reason,
            "price_quality": raw_price_quality,
            "price_reason": raw_price_reason,
            "open": raw_open,
            "high": raw_high,
            "low": raw_low,
            "close": raw_close,
        },
        "profile_status": "AVAILABLE" if profile is not None else "NOT_STORED",
        "profile": profile,
        "absence_semantics": "缺少B_t或Bar不推断停牌、未上市或退市",
    }


def _quality_value(value: Any, quality: Any, reason: Any) -> dict[str, Any]:
    return {"value": value, "quality": quality, "reason": reason}


def _profile_fields(
    field_ids: Iterable[str] | None,
    statuses: Iterable[str] | None,
    qualities: Iterable[str] | None,
    reasons: Iterable[str] | None,
) -> dict[str, dict[str, Any]]:
    values = [field_ids, statuses, qualities, reasons]
    if any(value is None for value in values):
        return {}
    field_list, status_list, quality_list, reason_list = [
        list(value) for value in values
    ]
    if not (
        len(field_list) == len(status_list) == len(quality_list) == len(reason_list)
    ):
        raise TdxSectorMembersPgError(
            "PROFILE_ARRAY_ALIGNMENT_INVALID", "画像字段ID、状态、质量与原因长度不一致"
        )
    if len(set(field_list)) != len(field_list):
        raise TdxSectorMembersPgError(
            "PROFILE_FIELD_ID_DUPLICATE", "画像字段ID数组包含重复值"
        )
    return {
        str(field_id): {
            "status": status,
            "quality": quality,
            "reason": reason,
        }
        for field_id, status, quality, reason in zip(
            field_list, status_list, quality_list, reason_list
        )
    }


def _validate_page(offset: int, limit: int) -> None:
    if type(offset) is not int or not 0 <= offset <= MAX_MEMBER_INSERTS:
        raise TdxSectorMembersPgError(
            "PAGE_OFFSET_INVALID", "分页偏移必须在0至500000之间"
        )
    if type(limit) is not int or not 1 <= limit <= MAX_PAGE_SIZE:
        raise TdxSectorMembersPgError("PAGE_LIMIT_INVALID", "分页大小必须在1至100之间")


def _validate_snapshot_uuid(value: str) -> None:
    try:
        uuid.UUID(value)
    except (AttributeError, ValueError) as error:
        raise TdxSectorMembersPgError(
            "SNAPSHOT_ID_INVALID", "snapshot_id必须是UUID"
        ) from error


def _validate_source_set_uuid(value: str) -> None:
    try:
        uuid.UUID(value)
    except (AttributeError, ValueError) as error:
        raise TdxSectorMembersPgError(
            "SOURCE_SET_ID_INVALID", "source_set_id必须是UUID"
        ) from error
