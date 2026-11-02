"""保存并查询TDX行业、概念成员快照的typed PostgreSQL对象。"""

from __future__ import annotations

from collections import Counter
from datetime import date
import re
from typing import Any, Iterable, Sequence
import uuid

from stock_a.sources.tdx.sector_members import (
    SectorCatalogRow,
    SectorMemberRow,
    SectorSourceSnapshot,
    TdxSectorSourceError,
)
from stock_a.storage.postgres.source_raw import RawSourcePgError, _transaction


MAX_SNAPSHOT_INSERTS = 8
MAX_CATALOG_INSERTS = 12_000
MAX_MEMBER_INSERTS = 500_000
MAX_PAGE_SIZE = 100
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


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
