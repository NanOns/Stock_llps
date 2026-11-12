"""保存并查询TDX行业、概念成员快照的typed PostgreSQL对象。"""

from __future__ import annotations

from collections import Counter
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from fractions import Fraction
import re
import time
from typing import Any, Iterable, Iterator, Mapping, Sequence

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

from . import source_raw
from .d0_raw import MAX_SOURCE_SYMBOLS
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


@dataclass
class TdxSectorStrengthReadContext:
    """单次请求内共享只读快照及精确来源价格读取结果。"""

    connection: Any
    cursor: Any
    active: bool = True
    validated_source_versions: dict[tuple[Any, ...], dict[str, Any]] = field(
        default_factory=dict
    )
    validated_profile_snapshots: dict[tuple[Any, ...], dict[str, Any]] = field(
        default_factory=dict
    )
    strength_input_cache: dict[tuple[Any, ...], dict[str, Any]] = field(
        default_factory=dict
    )


@contextmanager
def tdx_sector_strength_read_context(
    config: dict[str, Any],
) -> Iterator[TdxSectorStrengthReadContext]:
    """为一次多板块查询持有同一个REPEATABLE READ READ ONLY事务。"""
    with _transaction(config, write=False) as (connection, cursor, _scope):
        context = TdxSectorStrengthReadContext(
            connection=connection,
            cursor=cursor,
        )
        try:
            yield context
        finally:
            context.active = False


def _read_strength_inputs(
    cursor: Any,
    *,
    source_capture_id: int,
    source_package_id: int,
    target_session: date,
    profile_revision: str,
    official_calendar_id: str,
    official_calendar_revision: str,
    window_sessions: Sequence[date],
    symbols: Sequence[str],
    read_context: TdxSectorStrengthReadContext | None,
) -> tuple[
    list[dict[str, Any]],
    dict[tuple[str, date], dict[str, int]],
    list[dict[str, Any]],
    dict[str, int],
]:
    """读取或在同一事务内复用精确来源键的成员历史。"""
    ordered_symbols = tuple(sorted(set(symbols)))
    cache_key = (
        source_capture_id,
        source_package_id,
        target_session,
        profile_revision,
        official_calendar_id,
        official_calendar_revision,
        tuple(window_sessions),
    )
    cache = (
        read_context.strength_input_cache.setdefault(
            cache_key,
            {
                "profile_loaded_symbols": set(),
                "profiles_by_symbol": {},
                "revision_loaded_symbols": set(),
                "revision_counts_by_symbol": {},
                "raw_loaded_symbols": set(),
                "raw_rows_by_symbol": {},
            },
        )
        if read_context is not None
        else {
            "profile_loaded_symbols": set(),
            "profiles_by_symbol": {},
            "revision_loaded_symbols": set(),
            "revision_counts_by_symbol": {},
            "raw_loaded_symbols": set(),
            "raw_rows_by_symbol": {},
        }
    )
    fetched = {
        "profile_rows": 0,
        "revision_groups": 0,
        "raw_rows": 0,
        "profile_queries": 0,
        "revision_queries": 0,
        "raw_queries": 0,
    }

    missing_profile_symbols = [
        symbol
        for symbol in ordered_symbols
        if symbol not in cache["profile_loaded_symbols"]
    ]
    if missing_profile_symbols:
        fetched["profile_queries"] = 1
        cursor.execute(
            """SELECT p.source_symbol, p.source_capture_id,
       p.price_coordinate, p.calculation_scope, p.producer_contract,
       p.producer_version,
       p.ret20::text, p.ret20_exact_numerator::text,
       p.ret20_exact_denominator::text, p.ret20_quality, p.ret20_reason
FROM source_symbol_price_profiles p
WHERE p.target_session=%s::date
  AND p.source_capture_id=%s AND p.source_package_id=%s
  AND p.profile_revision=%s AND p.source_symbol=ANY(%s)
ORDER BY p.source_symbol COLLATE "C" """,
            (
                target_session,
                source_capture_id,
                source_package_id,
                profile_revision,
                list(missing_profile_symbols),
            ),
        )
        profile_columns = [item.name for item in cursor.description]
        profile_records = [
            dict(zip(profile_columns, row, strict=True)) for row in cursor.fetchall()
        ]
        fetched["profile_rows"] = len(profile_records)
        for row in profile_records:
            cache["profiles_by_symbol"].setdefault(
                str(row["source_symbol"]), []
            ).append(row)
        cache["profile_loaded_symbols"].update(missing_profile_symbols)
    profile_rows = [
        dict(row)
        for symbol in ordered_symbols
        for row in cache["profiles_by_symbol"].get(symbol, ())
    ]

    missing_revision_symbols = [
        symbol
        for symbol in ordered_symbols
        if symbol not in cache["revision_loaded_symbols"]
    ]
    if missing_revision_symbols:
        fetched["revision_queries"] = 1
        cursor.execute(
            """SELECT source_symbol, session_date,
       count(*)::bigint, count(DISTINCT source_package_id)::bigint,
       count(DISTINCT source_capture_id)::bigint
FROM source_raw_daily
WHERE source_symbol=ANY(%s)
  AND session_date=ANY(%s::date[])
GROUP BY source_symbol, session_date""",
            (list(missing_revision_symbols), list(window_sessions)),
        )
        revision_rows = cursor.fetchall()
        fetched["revision_groups"] = len(revision_rows)
        for row in revision_rows:
            symbol = str(row[0])
            cache["revision_counts_by_symbol"].setdefault(symbol, {})[row[1]] = {
                "row_count": int(row[2]),
                "package_count": int(row[3]),
                "capture_count": int(row[4]),
            }
        cache["revision_loaded_symbols"].update(missing_revision_symbols)
    revision_counts = {
        (symbol, session): dict(counts)
        for symbol in ordered_symbols
        for session, counts in cache["revision_counts_by_symbol"]
        .get(symbol, {})
        .items()
    }

    missing_raw_symbols = [
        symbol
        for symbol in ordered_symbols
        if symbol not in cache["raw_loaded_symbols"]
    ]
    if missing_raw_symbols:
        fetched["raw_queries"] = 1
        fetched_rows = _read_history_rows(
            cursor,
            source_symbols=missing_raw_symbols,
            sessions=window_sessions,
            source_package_id=source_package_id,
        )
        fetched["raw_rows"] = len(fetched_rows)
        for row in fetched_rows:
            cache["raw_rows_by_symbol"].setdefault(
                str(row["source_symbol"]), []
            ).append(row)
        cache["raw_loaded_symbols"].update(missing_raw_symbols)
    raw_rows = [
        dict(row)
        for symbol in ordered_symbols
        for row in cache["raw_rows_by_symbol"].get(symbol, ())
    ]
    return profile_rows, revision_counts, raw_rows, fetched


def _validate_snapshot(snapshot: SectorSourceSnapshot) -> None:
    if not isinstance(snapshot, SectorSourceSnapshot):
        raise TdxSectorMembersPgError("SNAPSHOT_INPUT_INVALID", "板块来源快照类型无效")
    if (
        snapshot.source_kind not in {"TDX_TDXHY_CFG", "TDX_INFOHARBOR_BLOCK_DAT"}
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
    if len({item.source_kind for item in snapshots}) != len(snapshots):
        raise TdxSectorMembersPgError(
            "SNAPSHOT_KEY_DUPLICATE", "本次输入包含重复的TDX来源类别"
        )
    return catalog_count, member_count


def _read_snapshot_rows(cursor: Any, snapshot_id: int) -> tuple[dict | None, set, set]:
    if type(snapshot_id) is not int or snapshot_id <= 0:
        raise TdxSectorMembersPgError("SNAPSHOT_ID_INVALID", "快照ID必须为正BIGINT")
    cursor.execute(
        """SELECT snapshot_id, source_kind,
       source_path_role, source_file_size, observed_at, source_valid_on,
       source_known_at, source_available_at, clock_quality, research_mode,
       parse_contract_version, rows_declared, source_quality
FROM tdx_sector_source_snapshots WHERE snapshot_id=%s""",
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
FROM tdx_sector_catalog WHERE snapshot_id=%s""",
        (snapshot_id,),
    )
    catalog = set(cursor.fetchall())
    cursor.execute(
        """SELECT m.board_kind, m.board_native_id, m.source_symbol,
       m.membership_quality, m.source_record_ordinal
FROM tdx_sector_members m
JOIN tdx_sector_catalog c ON c.id=m.catalog_id
WHERE c.snapshot_id=%s""",
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
        actual["source_kind"] == expected.source_kind
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


def _assert_existing_snapshot(
    cursor: Any, snapshot: SectorSourceSnapshot
) -> int | None:
    cursor.execute(
        """SELECT snapshot_id
FROM tdx_sector_source_snapshots
WHERE source_kind=%s AND source_file_size=%s
  AND parse_contract_version=%s AND rows_declared=%s
ORDER BY observed_at DESC, snapshot_id DESC""",
        (
            snapshot.source_kind,
            snapshot.source_file_size,
            snapshot.parse_contract_version,
            snapshot.rows_declared,
        ),
    )
    for (snapshot_id,) in cursor.fetchall():
        actual, catalog, members = _read_snapshot_rows(cursor, int(snapshot_id))
        if (
            actual is not None
            and _stable_snapshot_matches(actual, snapshot)
            and catalog == _expected_catalog(snapshot)
            and members == _expected_members(snapshot)
        ):
            return int(snapshot_id)
    return None


def _stable_snapshot_matches_without_source_path(
    actual: dict, expected: SectorSourceSnapshot
) -> bool:
    return (
        actual["source_kind"] == expected.source_kind
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
        with _transaction(config, write=False) as (_connection, cursor, _scope):
            cursor.execute(
                """SELECT
    (SELECT count(*) FROM tdx_sector_source_snapshots),
    (SELECT count(*) FROM tdx_sector_catalog),
    (SELECT count(*) FROM tdx_sector_members)"""
            )
            snapshot_count, catalog_count, member_count = map(int, cursor.fetchone())
            for snapshot in snapshots:
                existing_id = _assert_existing_snapshot(cursor, snapshot)
                revisions.append(
                    {
                        "source_input_key": [
                            snapshot.source_kind,
                            snapshot.source_file_size,
                            snapshot.parse_contract_version,
                            snapshot.rows_declared,
                        ],
                        "source_kind": snapshot.source_kind,
                        "decision": (
                            "REUSE_IDENTICAL_VERSION"
                            if existing_id is not None
                            else "NEW_VERSION"
                        ),
                        "existing_snapshot_id": existing_id,
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
    config: dict[str, Any],
    snapshots: Sequence[SectorSourceSnapshot],
    snapshot_ids: Mapping[str, int] | None = None,
) -> dict[str, Any]:
    """以新只读事务重新核验已存在的来源行与完整集合。"""
    readback: dict[str, dict[str, int | str]] = {}
    try:
        with _transaction(config, write=False) as (_connection, cursor, _scope):
            for snapshot in snapshots:
                snapshot_id = (
                    snapshot_ids.get(snapshot.source_kind)
                    if isinstance(snapshot_ids, Mapping)
                    else _assert_existing_snapshot(cursor, snapshot)
                )
                if type(snapshot_id) is not int or snapshot_id <= 0:
                    raise TdxSectorMembersPgError(
                        "REMOTE_SOURCE_READBACK_MISMATCH",
                        "远端来源没有可读回的BIGINT快照ID",
                    )
                actual, catalog, members = _read_snapshot_rows(cursor, snapshot_id)
                if (
                    actual is None
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
                readback[str(snapshot_id)] = {
                    "catalog_rows": len(catalog),
                    "member_rows": len(members),
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


def _insert_snapshot(cursor: Any, snapshot: SectorSourceSnapshot) -> int:
    cursor.execute(
        """INSERT INTO tdx_sector_source_snapshots (
    source_kind, source_path_role,
    source_file_size, observed_at, source_valid_on, source_known_at,
    source_available_at, clock_quality, research_mode,
    parse_contract_version, rows_declared, source_quality
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
RETURNING snapshot_id""",
        (
            snapshot.source_kind,
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
    row = cursor.fetchone()
    if row is None or type(row[0]) is not int or row[0] <= 0:
        raise TdxSectorMembersPgError(
            "SNAPSHOT_IDENTITY_INSERT_FAILED", "PostgreSQL未分配正BIGINT快照ID"
        )
    snapshot_id = int(row[0])
    catalog_ids = {}
    if snapshot.catalog_rows:
        for row in snapshot.catalog_rows:
            cursor.execute(
                """INSERT INTO tdx_sector_catalog (
    snapshot_id, board_kind, board_native_id, board_name
) VALUES (%s, %s, %s, %s) RETURNING id""",
                (
                    snapshot_id,
                    row.board_kind,
                    row.board_native_id,
                    row.board_name,
                ),
            )
            inserted_catalog = cursor.fetchone()
            if inserted_catalog is None or type(inserted_catalog[0]) is not int:
                raise TdxSectorMembersPgError(
                    "CATALOG_IDENTITY_INSERT_FAILED",
                    "PostgreSQL未分配正BIGINT板块目录ID",
                )
            catalog_ids[(row.board_kind, row.board_native_id)] = int(
                inserted_catalog[0]
            )
    if snapshot.member_rows:
        cursor.executemany(
            """INSERT INTO tdx_sector_members (
    catalog_id, source_symbol, membership_quality, source_record_ordinal
) VALUES (%s, %s, %s, %s)""",
            [
                (
                    catalog_ids[(row.board_kind, row.board_native_id)],
                    row.source_symbol,
                    row.membership_quality,
                    row.source_record_ordinal,
                )
                for row in snapshot.member_rows
            ],
        )
    return snapshot_id


def persist_sector_source_snapshots(
    config: dict[str, Any], snapshots: Sequence[SectorSourceSnapshot]
) -> dict[str, Any]:
    """在单个SERIALIZABLE事务中原子插入最多两个已验证来源快照。"""
    catalog_budget, member_budget = _validate_limits(snapshots)
    for snapshot in snapshots:
        _validate_snapshot(snapshot)
    snapshot_ids: dict[str, int] = {}
    new_source_kinds: set[str] = set()
    try:
        with _transaction(config, write=True) as (_connection, cursor, _scope):
            cursor.execute(
                "LOCK TABLE tdx_sector_source_snapshots IN SHARE ROW EXCLUSIVE MODE"
            )
            cursor.execute("SELECT 1 FROM migration_ledger WHERE migration_id=9")
            migration_row = cursor.fetchone()
            if migration_row is None:
                raise TdxSectorMembersPgError(
                    "MIGRATION_0009_REQUIRED", "必须先按本批任务范围应用迁移0009"
                )
            cursor.execute("SELECT count(*) FROM tdx_sector_source_snapshots")
            current_snapshot_count = int(cursor.fetchone()[0])
            if current_snapshot_count > 8:
                raise TdxSectorMembersPgError(
                    "SNAPSHOT_ROW_LIMIT_EXCEEDED", "现有来源快照已超过本批8行预算"
                )
            for snapshot in snapshots:
                existing_id = _assert_existing_snapshot(cursor, snapshot)
                if existing_id is not None:
                    snapshot_ids[snapshot.source_kind] = existing_id
            to_insert = [
                item for item in snapshots if item.source_kind not in snapshot_ids
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
    (SELECT count(*) FROM tdx_sector_catalog),
    (SELECT count(*) FROM tdx_sector_members)"""
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
                snapshot_ids[snapshot.source_kind] = _insert_snapshot(cursor, snapshot)
                new_source_kinds.add(snapshot.source_kind)
    except TdxSectorMembersPgError:
        raise
    except RawSourcePgError as error:
        raise TdxSectorMembersPgError(error.code, str(error)) from error

    readback: dict[str, dict] = {}
    try:
        with _transaction(config, write=False) as (_connection, cursor, _scope):
            for snapshot in snapshots:
                snapshot_id = snapshot_ids[snapshot.source_kind]
                actual, catalog, members = _read_snapshot_rows(cursor, snapshot_id)
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
                readback[str(snapshot_id)] = {
                    "catalog_rows": len(catalog),
                    "member_rows": len(members),
                }
    except TdxSectorMembersPgError:
        raise
    except RawSourcePgError as error:
        raise TdxSectorMembersPgError(error.code, str(error)) from error

    inserted_snapshots = len(new_source_kinds)
    inserted_catalog = sum(
        len(item.catalog_rows)
        for item in snapshots
        if item.source_kind in new_source_kinds
    )
    inserted_members = sum(
        len(item.member_rows)
        for item in snapshots
        if item.source_kind in new_source_kinds
    )
    return {
        "status": "IDEMPOTENT_READBACK" if not inserted_snapshots else "INSERTED",
        "snapshots": [
            {
                "snapshot_id": snapshot_ids[item.source_kind],
                "source_kind": item.source_kind,
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
    source_package_id: int,
    source_profile: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """读取最新TDX行业/GN快照摘要，并可选核对其与精确画像的交集。"""
    if (
        type(target_session) is not date
        or type(source_package_id) is not int
        or source_package_id <= 0
    ):
        raise TdxSectorMembersPgError(
            "SECTOR_OBSERVATION_TARGET_INVALID", "板块观察必须绑定目标日和正数来源包ID"
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
        and profile.get("source_package_id") == source_package_id
    )
    source_capture_id = profile.get("source_capture_id") if has_exact_profile else None
    profile_revision = (
        selected_profile.get("profile_revision") if has_exact_profile else None
    )
    try:
        with _transaction(config, write=False) as (_connection, cursor, _scope):
            cursor.execute(
                """WITH latest AS (
    SELECT DISTINCT ON (source_kind)
           snapshot_id,source_kind,observed_at,source_valid_on,
           source_known_at,source_available_at,clock_quality,research_mode,
           parse_contract_version,rows_declared,source_quality
    FROM tdx_sector_source_snapshots
    WHERE source_kind IN ('TDX_TDXHY_CFG','TDX_INFOHARBOR_BLOCK_DAT')
    ORDER BY source_kind,observed_at DESC,snapshot_id DESC
)
SELECT s.snapshot_id,s.source_kind,
       s.observed_at,s.source_valid_on,
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
    FROM tdx_sector_catalog
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
    FROM tdx_sector_members member
    JOIN tdx_sector_catalog catalog ON catalog.id=member.catalog_id
    WHERE catalog.snapshot_id=s.snapshot_id
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
    FROM tdx_sector_members m
    JOIN source_symbol_price_profiles p
      ON p.target_session=%s::date AND p.source_package_id=%s
     AND p.source_capture_id=%s
     AND p.profile_revision=%s
     AND p.source_symbol=m.source_symbol
    JOIN tdx_sector_catalog c ON c.id=m.catalog_id
    WHERE c.snapshot_id=s.snapshot_id
) o ON TRUE
ORDER BY s.source_kind""",
                (
                    target_session,
                    source_package_id,
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
        snapshot["snapshot_id"] = int(snapshot["snapshot_id"])
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
        "target_session": target_session.isoformat(),
        "source_package_id": source_package_id,
        "source_profile_revision": profile_revision,
        "selection_rule": "MAX_OBSERVED_AT_PER_EXACT_TDX_SOURCE_KIND",
        "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
        "snapshots": snapshots,
        "database_write": False,
        "ddl_executed": False,
    }


def query_tdx_sector_source_snapshot_catalog(config: dict[str, Any]) -> dict[str, Any]:
    """读取PG内全部行业与GN概念快照身份，供用户显式选择。"""
    try:
        with _transaction(config, write=False) as (_connection, cursor, _scope):
            cursor.execute(
                """SELECT s.snapshot_id,s.source_kind,
       s.observed_at,s.source_valid_on,
       s.source_known_at,s.source_available_at,s.clock_quality,
       s.research_mode,s.parse_contract_version,s.rows_declared,s.source_quality,
       c.catalog_rows,c.industry_catalog_rows,c.concept_catalog_rows,
       c.excluded_catalog_rows,m.member_rows,m.industry_member_rows,
       m.concept_member_rows,m.distinct_member_symbols,
       m.invalid_membership_quality_rows
FROM tdx_sector_source_snapshots s
LEFT JOIN LATERAL (
    SELECT count(*)::bigint AS catalog_rows,
           count(*) FILTER (WHERE board_kind='INDUSTRY')::bigint
               AS industry_catalog_rows,
           count(*) FILTER (WHERE board_kind='CONCEPT')::bigint
               AS concept_catalog_rows,
           count(*) FILTER (WHERE board_kind NOT IN ('INDUSTRY','CONCEPT'))::bigint
               AS excluded_catalog_rows
    FROM tdx_sector_catalog
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
    FROM tdx_sector_members member
    JOIN tdx_sector_catalog catalog ON catalog.id=member.catalog_id
    WHERE catalog.snapshot_id=s.snapshot_id
) m ON TRUE
WHERE s.source_kind IN ('TDX_TDXHY_CFG','TDX_INFOHARBOR_BLOCK_DAT')
ORDER BY s.source_kind,s.observed_at,s.snapshot_id"""
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
            "SECTOR_SNAPSHOT_CATALOG_READ_FAILED",
            f"TDX板块快照目录只读查询失败（{type(error).__name__}）",
        ) from error

    date_fields = ("source_valid_on",)
    datetime_fields = ("observed_at", "source_known_at", "source_available_at")
    integer_fields = {
        "rows_declared",
        "catalog_rows",
        "industry_catalog_rows",
        "concept_catalog_rows",
        "excluded_catalog_rows",
        "member_rows",
        "industry_member_rows",
        "concept_member_rows",
        "distinct_member_symbols",
        "invalid_membership_quality_rows",
    }
    for snapshot in snapshots:
        snapshot["snapshot_id"] = int(snapshot["snapshot_id"])
        for field in date_fields:
            if snapshot[field] is not None:
                snapshot[field] = snapshot[field].isoformat()
        for field in datetime_fields:
            if snapshot[field] is not None:
                snapshot[field] = snapshot[field].isoformat()
        for field in integer_fields:
            snapshot[field] = int(snapshot[field] or 0)
        snapshot["membership_mode"] = "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT"
        snapshot["source_time_role"] = (
            "OBSERVED_AT_IS_LOCAL_OBSERVATION_NOT_SOURCE_AVAILABILITY"
        )

    return {
        "status": "AVAILABLE" if snapshots else "NO_MATCH",
        "snapshots": snapshots,
        "database_write": False,
        "ddl_executed": False,
    }


def query_tdx_sector_symbol_membership(
    config: dict[str, Any],
    *,
    exchange: str,
    source_symbol: str,
    target_session: date,
    industry_snapshot_id: int,
    concept_snapshot_id: int,
) -> dict[str, Any]:
    """按显式行业和概念快照读取单个来源代码的当前成员关系。"""
    if exchange not in {"SH", "SZ"} or not isinstance(source_symbol, str):
        raise TdxSectorMembersPgError(
            "SOURCE_SYMBOL_INVALID", "来源股票代码必须绑定SH或SZ交易所前缀"
        )
    if re.fullmatch(r"(SH|SZ)\.[0-9]{6}", source_symbol) is None:
        raise TdxSectorMembersPgError(
            "SOURCE_SYMBOL_INVALID", "来源股票代码格式必须为SH.六位代码或SZ.六位代码"
        )
    if source_symbol.split(".", 1)[0] != exchange:
        raise TdxSectorMembersPgError(
            "SOURCE_SYMBOL_EXCHANGE_MISMATCH", "交易所与精确来源股票代码前缀不一致"
        )
    if type(target_session) is not date:
        raise TdxSectorMembersPgError(
            "TARGET_SESSION_INVALID", "研究目标日必须是有效交易日期"
        )
    if (
        type(industry_snapshot_id) is not int
        or industry_snapshot_id <= 0
        or type(concept_snapshot_id) is not int
        or concept_snapshot_id <= 0
    ):
        raise TdxSectorMembersPgError(
            "SNAPSHOT_ID_INVALID", "行业及概念快照ID必须为正BIGINT"
        )
    if industry_snapshot_id == concept_snapshot_id:
        raise TdxSectorMembersPgError(
            "SECTOR_SNAPSHOT_IDS_NOT_DISTINCT", "行业和概念必须使用两个独立快照"
        )
    expected_kinds = {
        industry_snapshot_id: "TDX_TDXHY_CFG",
        concept_snapshot_id: "TDX_INFOHARBOR_BLOCK_DAT",
    }
    try:
        with _transaction(config, write=False) as (
            _connection,
            cursor,
            _scope,
        ):
            requested_ids = [industry_snapshot_id, concept_snapshot_id]
            cursor.execute(
                """SELECT snapshot_id, source_kind,
       source_path_role, source_file_size, observed_at, source_valid_on,
       source_known_at, source_available_at, clock_quality, research_mode,
       parse_contract_version, rows_declared, source_quality
FROM tdx_sector_source_snapshots
WHERE snapshot_id=ANY(%s::bigint[])
ORDER BY snapshot_id""",
                (requested_ids,),
            )
            snapshot_columns = [item.name for item in cursor.description]
            snapshot_rows = [
                dict(zip(snapshot_columns, row, strict=True))
                for row in cursor.fetchall()
            ]
            snapshots_by_id = {int(row["snapshot_id"]): row for row in snapshot_rows}
            if set(snapshots_by_id) != set(expected_kinds):
                raise TdxSectorMembersPgError(
                    "SECTOR_SNAPSHOT_NOT_FOUND",
                    "未找到显式绑定的行业及概念TDX快照",
                )
            for snapshot_id, expected_kind in expected_kinds.items():
                row = snapshots_by_id[snapshot_id]
                if row["source_kind"] != expected_kind:
                    raise TdxSectorMembersPgError(
                        "SECTOR_SNAPSHOT_KIND_MISMATCH",
                        "显式快照类型与行业/概念请求角色不匹配",
                    )
                if row["research_mode"] != "NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT":
                    raise TdxSectorMembersPgError(
                        "SECTOR_SNAPSHOT_MODE_REJECTED",
                        "不接受未声明为当前后验快照的板块成员来源",
                    )

            cursor.execute(
                """SELECT c.snapshot_id, m.board_kind,
       m.board_native_id, c.board_name, m.source_symbol,
       m.membership_quality, m.source_record_ordinal
FROM tdx_sector_members m
JOIN tdx_sector_catalog c
  ON c.id=m.catalog_id
WHERE c.snapshot_id=ANY(%s::bigint[]) AND m.source_symbol=%s
  AND m.board_kind IN ('INDUSTRY','CONCEPT')
ORDER BY m.board_kind COLLATE "C", m.board_native_id COLLATE "C"
""",
                (requested_ids, source_symbol),
            )
            membership_rows = cursor.fetchall()
    except TdxSectorMembersPgError:
        raise
    except RawSourcePgError as error:
        raise TdxSectorMembersPgError(error.code, str(error)) from error
    except Exception as error:
        raise TdxSectorMembersPgError(
            "SECTOR_SYMBOL_MEMBERSHIP_READ_FAILED",
            f"单股TDX成员关系只读读取失败（{type(error).__name__}）",
        ) from error

    expected_kind_by_snapshot = {
        industry_snapshot_id: "INDUSTRY",
        concept_snapshot_id: "CONCEPT",
    }
    snapshot_payloads: list[dict[str, Any]] = []
    for snapshot_id in (industry_snapshot_id, concept_snapshot_id):
        payload = _snapshot_payload(
            tuple(snapshots_by_id[snapshot_id][name] for name in snapshot_columns)
        )
        observed_at = datetime.fromisoformat(payload["observed_at"])
        payload["source_quality_counts"] = _source_quality_counts(
            payload["source_quality"]
        )
        payload["source_observed_at"] = payload["observed_at"]
        payload["source_clock_quality"] = payload["clock_quality"]
        payload["source_time_role"] = (
            "LOCAL_CAPTURE_OBSERVED_AT_NOT_SOURCE_AVAILABILITY"
        )
        payload["temporal_relationship"] = (
            "OBSERVED_AFTER_TARGET"
            if observed_at.date() > target_session
            else "OBSERVED_ON_OR_BEFORE_TARGET"
        )
        payload["membership_mode"] = "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT"
        payload["membership_count"] = 0
        snapshot_payloads.append(payload)

    snapshots_by_payload_id = {
        payload["snapshot_id"]: payload for payload in snapshot_payloads
    }
    memberships: list[dict[str, Any]] = []
    for row in membership_rows:
        snapshot_id = int(row[0])
        board_kind = str(row[1])
        if (
            snapshot_id not in expected_kind_by_snapshot
            or board_kind != expected_kind_by_snapshot[snapshot_id]
            or row[5] != "OBSERVED_SOURCE_RELATION"
        ):
            raise TdxSectorMembersPgError(
                "SECTOR_MEMBERSHIP_QUALITY_INVALID",
                "单股成员行与快照角色或OBSERVED关系质量不匹配",
            )
        snapshot = snapshots_by_payload_id[snapshot_id]
        snapshot["membership_count"] += 1
        memberships.append(
            {
                "snapshot_id": snapshot_id,
                "source_kind": snapshot["source_kind"],
                "board_kind": board_kind,
                "board_native_id": str(row[2]),
                "board_name": str(row[3]),
                "source_symbol": str(row[4]),
                "membership_quality": str(row[5]),
                "source_record_ordinal": int(row[6]),
                "observed_at": snapshot["observed_at"],
                "source_observed_at": snapshot["source_observed_at"],
                "source_valid_on": snapshot["source_valid_on"],
                "source_known_at": snapshot["source_known_at"],
                "source_available_at": snapshot["source_available_at"],
                "clock_quality": snapshot["clock_quality"],
                "source_clock_quality": snapshot["source_clock_quality"],
                "source_quality": snapshot["source_quality"],
                "research_mode": snapshot["research_mode"],
                "source_time_role": snapshot["source_time_role"],
                "temporal_relationship": snapshot["temporal_relationship"],
                "membership_mode": snapshot["membership_mode"],
            }
        )

    status = "AVAILABLE" if memberships else "AVAILABLE_EMPTY"
    return {
        "status": status,
        "exchange": exchange,
        "source_symbol": source_symbol,
        "target_session": target_session.isoformat(),
        "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
        "snapshots": snapshot_payloads,
        "memberships": memberships,
        "empty_reason": (
            "NO_OBSERVED_RELATION_IN_SELECTED_CURRENT_SNAPSHOTS"
            if not memberships
            else None
        ),
        "database_write_attempted": False,
        "dml_executed": False,
        "ddl_executed": False,
    }


def query_tdx_sector_memberships_for_symbols(
    config: dict[str, Any],
    *,
    source_symbols: Sequence[str],
    target_session: date,
    industry_snapshot_id: int | None,
    concept_snapshot_id: int | None,
) -> dict[str, Any]:
    """在一个只读事务内批量读取FT来源代码的可用TDX成员快照。"""
    if isinstance(source_symbols, (str, bytes)) or not isinstance(
        source_symbols, Sequence
    ):
        raise TdxSectorMembersPgError(
            "SOURCE_SYMBOLS_INVALID", "批量来源代码必须是明确的代码序列"
        )
    normalized_symbols: list[str] = []
    for source_symbol in source_symbols:
        if (
            not isinstance(source_symbol, str)
            or re.fullmatch(r"(?:SH|SZ)\.[0-9]{6}", source_symbol) is None
        ):
            raise TdxSectorMembersPgError(
                "SOURCE_SYMBOL_INVALID", "来源股票代码必须是SH/SZ六位代码"
            )
        normalized_symbols.append(source_symbol)
        if len(normalized_symbols) > MAX_SOURCE_SYMBOLS:
            raise TdxSectorMembersPgError(
                "SOURCE_SYMBOLS_LIMIT_EXCEEDED",
                f"批量来源代码不得超过已封口来源池上限{MAX_SOURCE_SYMBOLS}条",
            )
    if len(set(normalized_symbols)) != len(normalized_symbols):
        raise TdxSectorMembersPgError(
            "SOURCE_SYMBOL_DUPLICATED", "批量来源代码不能包含重复值"
        )
    normalized_symbols.sort()
    if type(target_session) is not date:
        raise TdxSectorMembersPgError(
            "TARGET_SESSION_INVALID", "研究目标日必须是有效交易日期"
        )
    if any(
        value is not None and (type(value) is not int or value <= 0)
        for value in (industry_snapshot_id, concept_snapshot_id)
    ):
        raise TdxSectorMembersPgError(
            "SECTOR_IDENTITY_INVALID", "TDX成员快照ID必须为空或正BIGINT"
        )
    if industry_snapshot_id is not None and industry_snapshot_id == concept_snapshot_id:
        raise TdxSectorMembersPgError(
            "SECTOR_SNAPSHOT_IDS_NOT_DISTINCT", "行业和概念必须使用两个独立快照"
        )

    expected_snapshots = tuple(
        item
        for item in (
            (industry_snapshot_id, "TDX_TDXHY_CFG", "INDUSTRY"),
            (concept_snapshot_id, "TDX_INFOHARBOR_BLOCK_DAT", "CONCEPT"),
        )
        if item[0] is not None
    )
    transaction_started = time.perf_counter()
    select_client_seconds: float | None = None
    rows: list[dict[str, Any]] = []
    try:
        with _transaction(config, write=False) as (
            _connection,
            cursor,
            _scope,
        ):
            select_started = time.perf_counter()
            cursor.execute(
                """WITH requested(snapshot_id, expected_source_kind, expected_board_kind) AS (
    VALUES (%s::bigint, %s::text, %s::text), (%s::bigint, %s::text, %s::text)
)
SELECT requested.snapshot_id AS requested_snapshot_id,
       requested.expected_source_kind, requested.expected_board_kind,
       snapshot.snapshot_id AS snapshot_row_id,
       snapshot.source_kind,
       snapshot.source_path_role, snapshot.source_file_size,
       snapshot.observed_at, snapshot.source_valid_on,
       snapshot.source_known_at, snapshot.source_available_at,
       snapshot.clock_quality, snapshot.research_mode,
       snapshot.parse_contract_version, snapshot.rows_declared,
       snapshot.source_quality, member.board_kind AS member_board_kind,
       member.board_native_id AS member_board_native_id, catalog.board_name,
       member.source_symbol AS member_source_symbol,
       member.membership_quality, member.source_record_ordinal
FROM requested
LEFT JOIN tdx_sector_source_snapshots AS snapshot
  ON snapshot.snapshot_id=requested.snapshot_id
LEFT JOIN tdx_sector_members AS member
  ON member.catalog_id IN (
      SELECT catalog.id FROM tdx_sector_catalog catalog
      WHERE catalog.snapshot_id=snapshot.snapshot_id
  )
 AND member.board_kind=requested.expected_board_kind
 AND member.source_symbol=ANY(%s::text[])
LEFT JOIN tdx_sector_catalog AS catalog
  ON catalog.id=member.catalog_id
WHERE requested.snapshot_id IS NOT NULL
ORDER BY requested.expected_board_kind COLLATE "C",
         member.board_native_id COLLATE "C",
         member.source_symbol COLLATE "C\"""",
                (
                    industry_snapshot_id,
                    "TDX_TDXHY_CFG",
                    "INDUSTRY",
                    concept_snapshot_id,
                    "TDX_INFOHARBOR_BLOCK_DAT",
                    "CONCEPT",
                    normalized_symbols,
                ),
            )
            columns = [item.name for item in cursor.description]
            rows = [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
            select_client_seconds = time.perf_counter() - select_started
    except TdxSectorMembersPgError:
        raise
    except RawSourcePgError as error:
        raise TdxSectorMembersPgError(error.code, str(error)) from error
    except Exception as error:
        raise TdxSectorMembersPgError(
            "SECTOR_BATCH_MEMBERSHIP_READ_FAILED",
            f"批量TDX成员关系只读读取失败（{type(error).__name__}）",
        ) from error

    snapshots_by_id: dict[int, dict[str, Any]] = {}
    memberships: list[dict[str, Any]] = []
    seen_relations: set[tuple[str, str, str]] = set()
    expected_by_id = {
        snapshot_id: (source_kind, board_kind)
        for snapshot_id, source_kind, board_kind in expected_snapshots
    }
    for row in rows:
        requested_id = int(row["requested_snapshot_id"])
        if row["snapshot_row_id"] is None:
            raise TdxSectorMembersPgError(
                "SECTOR_SNAPSHOT_NOT_FOUND", "未找到显式绑定的行业及概念TDX快照"
            )
        if int(row["snapshot_row_id"]) != requested_id:
            raise TdxSectorMembersPgError(
                "SECTOR_SNAPSHOT_IDENTITY_MISMATCH",
                "成员行返回的快照身份与请求ID不一致",
            )
        expected_source_kind, expected_board_kind = expected_by_id[requested_id]
        if (
            row["expected_source_kind"] != expected_source_kind
            or row["expected_board_kind"] != expected_board_kind
            or row["source_kind"] != expected_source_kind
        ):
            raise TdxSectorMembersPgError(
                "SECTOR_SNAPSHOT_KIND_MISMATCH",
                "显式快照类型与行业/概念请求角色不匹配",
            )
        if row["research_mode"] != "NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT":
            raise TdxSectorMembersPgError(
                "SECTOR_SNAPSHOT_MODE_REJECTED",
                "不接受未声明为当前后验快照的板块成员来源",
            )
        snapshot = snapshots_by_id.get(requested_id)
        if snapshot is None:
            snapshot = {
                "snapshot_id": requested_id,
                "source_kind": row["source_kind"],
                "board_kind": expected_board_kind,
                "native_category": "GN"
                if expected_board_kind == "CONCEPT"
                else "INDUSTRY",
                "source_path_role": row["source_path_role"],
                "source_file_size": int(row["source_file_size"]),
                "observed_at": row["observed_at"].isoformat(),
                "source_valid_on": (
                    row["source_valid_on"].isoformat()
                    if row["source_valid_on"] is not None
                    else None
                ),
                "source_known_at": (
                    row["source_known_at"].isoformat()
                    if row["source_known_at"] is not None
                    else None
                ),
                "source_available_at": (
                    row["source_available_at"].isoformat()
                    if row["source_available_at"] is not None
                    else None
                ),
                "clock_quality": row["clock_quality"],
                "research_mode": row["research_mode"],
                "parse_contract_version": row["parse_contract_version"],
                "rows_declared": int(row["rows_declared"]),
                "source_quality": row["source_quality"],
                "source_quality_counts": _source_quality_counts(row["source_quality"]),
                "source_time_role": "LOCAL_CAPTURE_OBSERVED_AT_NOT_SOURCE_AVAILABILITY",
                "temporal_relationship": (
                    "OBSERVED_AFTER_TARGET"
                    if row["observed_at"].date() > target_session
                    else "OBSERVED_ON_OR_BEFORE_TARGET"
                ),
                "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
                "requested_symbol_membership_count": 0,
            }
            snapshots_by_id[requested_id] = snapshot
        if row["member_source_symbol"] is None:
            continue
        if (
            row["member_board_kind"] != expected_board_kind
            or row["member_board_native_id"] is None
            or row["board_name"] is None
            or row["membership_quality"] != "OBSERVED_SOURCE_RELATION"
        ):
            raise TdxSectorMembersPgError(
                "SECTOR_MEMBERSHIP_QUALITY_INVALID",
                "批量成员行与快照角色或OBSERVED关系质量不匹配",
            )
        relation_key = (
            requested_id,
            str(row["member_board_native_id"]),
            row["member_source_symbol"],
        )
        if relation_key in seen_relations:
            raise TdxSectorMembersPgError(
                "SECTOR_MEMBERSHIP_DUPLICATED", "TDX快照中出现重复板块成员关系"
            )
        seen_relations.add(relation_key)
        snapshot["requested_symbol_membership_count"] += 1
        memberships.append(
            {
                "snapshot_id": requested_id,
                "source_kind": row["source_kind"],
                "board_kind": row["member_board_kind"],
                "native_category": "GN"
                if row["member_board_kind"] == "CONCEPT"
                else "INDUSTRY",
                "board_native_id": str(row["member_board_native_id"]),
                "board_name": str(row["board_name"]),
                "source_symbol": row["member_source_symbol"],
                "membership_quality": row["membership_quality"],
                "source_record_ordinal": int(row["source_record_ordinal"]),
                "observed_at": snapshots_by_id[requested_id]["observed_at"],
                "source_observed_at": snapshots_by_id[requested_id]["observed_at"],
                "source_valid_on": snapshots_by_id[requested_id]["source_valid_on"],
                "source_known_at": snapshots_by_id[requested_id]["source_known_at"],
                "source_available_at": snapshots_by_id[requested_id][
                    "source_available_at"
                ],
                "clock_quality": row["clock_quality"],
                "source_clock_quality": row["clock_quality"],
                "source_quality": row["source_quality"],
                "research_mode": row["research_mode"],
                "source_time_role": snapshots_by_id[requested_id]["source_time_role"],
                "temporal_relationship": snapshots_by_id[requested_id][
                    "temporal_relationship"
                ],
                "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
            }
        )
    if set(snapshots_by_id) != set(expected_by_id):
        raise TdxSectorMembersPgError(
            "SECTOR_SNAPSHOT_NOT_FOUND", "未读取到完整的显式TDX双快照"
        )

    memberships.sort(
        key=lambda item: (
            item["board_kind"],
            item["board_native_id"],
            item["source_symbol"],
        )
    )
    return {
        "status": "AVAILABLE" if memberships else "AVAILABLE_EMPTY",
        "target_session": target_session.isoformat(),
        "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
        "source_symbols_requested": normalized_symbols,
        "source_symbol_request_count": len(normalized_symbols),
        "matched_source_symbol_count": len(
            {row["source_symbol"] for row in memberships}
        ),
        "membership_relation_count": len(memberships),
        "execution": {
            "pg_membership_read_seconds": select_client_seconds,
            "snapshot_membership_select_fetch_seconds": select_client_seconds,
            "transaction_wall_seconds": time.perf_counter() - transaction_started,
            "select_statement_count": 1,
            "select_rows_returned": len(rows),
            "source_symbol_parameter_count": len(normalized_symbols),
            "membership_relation_rows_returned": len(memberships),
            "fetched_relation_rows": len(memberships),
            "transaction": "REPEATABLE READ READ ONLY",
            "database_write_attempted": False,
            "ddl_executed": False,
            "dml_executed": False,
            "dml_rows_affected": 0,
        },
        "snapshots": [
            snapshots_by_id[snapshot_id]
            for snapshot_id, _source_kind, _board_kind in expected_snapshots
        ],
        "memberships": memberships,
        "database_write_attempted": False,
        "dml_executed": False,
        "ddl_executed": False,
    }


def read_rotation_snapshot_members(
    config: dict[str, Any], *, snapshot_id: int, board_kind: str
) -> dict[str, Any]:
    """读取同类板块的完整后验快照成员，不依赖当日画像。"""
    if type(snapshot_id) is not int or snapshot_id <= 0:
        raise TdxSectorMembersPgError("SNAPSHOT_ID_INVALID", "快照ID必须为正BIGINT")
    if board_kind not in {"INDUSTRY", "CONCEPT"}:
        raise TdxSectorMembersPgError(
            "BOARD_KIND_INVALID", "板块类型必须为INDUSTRY或CONCEPT"
        )
    with _transaction(config, write=False) as (_connection, cursor, _scope):
        cursor.execute(
            """SELECT snapshot_id, source_kind,
       source_path_role, source_file_size, observed_at, source_valid_on,
       source_known_at, source_available_at, clock_quality, research_mode,
       parse_contract_version, rows_declared, source_quality
FROM tdx_sector_source_snapshots WHERE snapshot_id=%s""",
            (snapshot_id,),
        )
        snapshot_row = cursor.fetchone()
        if snapshot_row is None:
            raise TdxSectorMembersPgError(
                "SECTOR_SNAPSHOT_NOT_FOUND", "未找到精确TDX成员来源快照"
            )
        snapshot = _snapshot_payload(snapshot_row)
        expected_kind = (
            "TDX_TDXHY_CFG" if board_kind == "INDUSTRY" else "TDX_INFOHARBOR_BLOCK_DAT"
        )
        if (
            snapshot["source_kind"] != expected_kind
            or snapshot["research_mode"] != "NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT"
        ):
            raise TdxSectorMembersPgError(
                "SECTOR_SNAPSHOT_MODE_REJECTED", "成员快照来源或后验研究模式不匹配"
            )
        cursor.execute(
            """SELECT c.id, c.board_native_id, c.board_name, m.source_symbol
FROM tdx_sector_catalog c LEFT JOIN tdx_sector_members m
ON m.catalog_id=c.id
WHERE c.snapshot_id=%s AND c.board_kind=%s
ORDER BY c.board_native_id COLLATE "C", m.source_symbol COLLATE "C"
""",
            (snapshot_id, board_kind),
        )
        boards: dict[str, dict[str, Any]] = {}
        for catalog_id, board_id, board_name, symbol in cursor.fetchall():
            board = boards.setdefault(
                board_id,
                {
                    "catalog_id": int(catalog_id),
                    "name": board_name,
                    "members": [],
                },
            )
            if board["name"] != board_name:
                raise TdxSectorMembersPgError(
                    "SECTOR_SNAPSHOT_INCONSISTENT", "板块名称不一致"
                )
            if symbol is not None:
                if symbol in board["members"]:
                    raise TdxSectorMembersPgError(
                        "SECTOR_MEMBER_DUPLICATE", "成员快照包含重复代码"
                    )
                board["members"].append(symbol)
    return {
        "snapshot": snapshot,
        "board_kind": board_kind,
        "boards": boards,
    }


def query_sector_catalog(
    config: dict[str, Any],
    *,
    snapshot_id: int,
    board_kind: str | None,
    search: str | None,
    offset: int,
    limit: int,
) -> dict[str, Any]:
    _validate_page(offset, limit)
    if type(snapshot_id) is not int or snapshot_id <= 0:
        raise TdxSectorMembersPgError("SNAPSHOT_ID_INVALID", "快照ID必须为正BIGINT")
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
        with _transaction(config, write=False) as (_connection, cursor, _scope):
            cursor.execute(
                """SELECT snapshot_id, source_kind,
       source_path_role, source_file_size, observed_at, source_valid_on,
       source_known_at, source_available_at, clock_quality, research_mode,
       parse_contract_version, rows_declared, source_quality
FROM tdx_sector_source_snapshots WHERE snapshot_id=%s""",
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
FROM tdx_sector_catalog c
WHERE c.snapshot_id=%s
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
                """SELECT count(*)::bigint
FROM tdx_sector_members m
JOIN tdx_sector_catalog c ON c.id=m.catalog_id
WHERE c.snapshot_id=%s""",
                (snapshot_id,),
            )
            snapshot_member_count = int(cursor.fetchone()[0])
            cursor.execute(
                """SELECT c.board_kind, c.board_native_id, c.board_name,
       count(m.source_symbol)::bigint AS source_member_count
FROM tdx_sector_catalog c
LEFT JOIN tdx_sector_members m
  ON m.catalog_id=c.id
WHERE c.snapshot_id=%s
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
    snapshot_id: int,
    board_kind: str,
    board_native_id: str,
    target_session: date,
    research_cutoff: date,
    source_package_id: int,
    profile_revision: str | None,
    offset: int,
    limit: int,
) -> dict[str, Any]:
    _validate_page(offset, limit)
    if type(snapshot_id) is not int or snapshot_id <= 0:
        raise TdxSectorMembersPgError("SNAPSHOT_ID_INVALID", "快照ID必须为正BIGINT")
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
    if type(source_package_id) is not int or source_package_id <= 0:
        raise TdxSectorMembersPgError(
            "SOURCE_PACKAGE_ID_INVALID", "来源包ID必须是正BIGINT"
        )
    if (
        profile_revision is not None
        and re.fullmatch(r"PROFILE_R[0-9]{2}", profile_revision) is None
    ):
        raise TdxSectorMembersPgError(
            "PROFILE_REVISION_INVALID", "画像修订必须是显式PROFILE_RNN版本标签"
        )

    try:
        with _transaction(config, write=False) as (_connection, cursor, _scope):
            cursor.execute(
                """SELECT snapshot_id, source_kind,
       source_path_role, source_file_size, observed_at, source_valid_on,
       source_known_at, source_available_at, clock_quality, research_mode,
       parse_contract_version, rows_declared, source_quality
FROM tdx_sector_source_snapshots WHERE snapshot_id=%s""",
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
                """SELECT DISTINCT r.source_package_id, r.source_capture_id,
       c.read_started_at, c.validated_at, c.source_available_at,
       c.source_time_quality, c.source_clock_role, c.parser_contract,
       c.normalized_count, s.dataset_id, s.byte_count
FROM source_raw_daily r
JOIN source_captures c ON c.capture_id=r.source_capture_id
  AND c.source_package_id=r.source_package_id AND c.target_session=r.session_date
JOIN source_packages s ON s.source_package_id=r.source_package_id
WHERE r.session_date=%s::date AND r.source_package_id=%s
ORDER BY r.source_capture_id, r.source_package_id LIMIT 2""",
                (target_session, source_package_id),
            )
            source_version = cursor.fetchall()
            if len(source_version) != 1:
                raise TdxSectorMembersPgError(
                    "SOURCE_VERSION_NOT_UNIQUE",
                    "目标日与来源包数字ID没有唯一Raw捕获引用",
                )
            (
                source_package_id,
                source_capture_id,
                _capture_started_at,
                _capture_validated_at,
                source_available_at,
                source_time_quality,
                source_clock_role,
                parser_contract,
                expected_row_count,
                source_dataset_id,
                source_byte_count,
            ) = source_version[0]
            if (
                source_dataset_id != source_raw._DATASET_ID
                or type(expected_row_count) is not int
                or expected_row_count <= 0
            ):
                raise TdxSectorMembersPgError(
                    "SOURCE_VERSION_INVALID", "Raw来源捕获或内容元数据不合法"
                )
            cursor.execute(
                """SELECT count(*)::bigint, count(DISTINCT source_symbol)::bigint,
       count(*) FILTER (WHERE bar_status='TARGET_PRESENT')::bigint,
       count(*) FILTER (WHERE bar_status='TARGET_MISSING')::bigint,
       count(*) FILTER (WHERE bar_status='READ_FAILED')::bigint
FROM source_raw_daily
WHERE source_capture_id=%s AND session_date=%s::date AND source_package_id=%s""",
                (source_capture_id, target_session, source_package_id),
            )
            raw_counts = cursor.fetchone()
            if (
                int(raw_counts[0]) != expected_row_count
                or int(raw_counts[1]) != expected_row_count
            ):
                raise TdxSectorMembersPgError(
                    "SOURCE_RAW_COUNT_MISMATCH",
                    "目标日Raw行数与capture记录的来源代码数不一致",
                )
            cursor.execute(
                """SELECT DISTINCT profile_revision COLLATE "C" AS profile_revision
FROM source_symbol_price_profiles
WHERE source_capture_id=%s AND target_session=%s::date AND source_package_id=%s
ORDER BY 1""",
                (
                    source_capture_id,
                    target_session,
                    source_package_id,
                ),
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
FROM tdx_sector_members m
JOIN tdx_sector_catalog c ON c.id=m.catalog_id
LEFT JOIN source_raw_daily r ON r.source_capture_id=%s
  AND r.source_symbol=m.source_symbol
  AND r.session_date=%s::date AND r.source_package_id=%s
LEFT JOIN source_symbol_price_profiles p ON p.source_capture_id=%s
  AND p.target_session=%s::date AND p.source_package_id=%s
  AND p.source_symbol=m.source_symbol AND p.profile_revision=%s
WHERE c.snapshot_id=%s AND m.board_kind=%s AND m.board_native_id=%s""",
                (
                    source_capture_id,
                    target_session,
                    source_package_id,
                    source_capture_id,
                    target_session,
                    source_package_id,
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
                    """SELECT 1 FROM tdx_sector_catalog
WHERE snapshot_id=%s AND board_kind=%s AND board_native_id=%s""",
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
FROM tdx_sector_members m
JOIN tdx_sector_catalog c ON c.id=m.catalog_id
LEFT JOIN source_raw_daily r ON r.source_capture_id=%s
  AND r.source_symbol=m.source_symbol
  AND r.session_date=%s::date AND r.source_package_id=%s
LEFT JOIN source_symbol_price_profiles p ON p.source_capture_id=%s
  AND p.target_session=%s::date AND p.source_package_id=%s
  AND p.source_symbol=m.source_symbol AND p.profile_revision=%s
WHERE c.snapshot_id=%s AND m.board_kind=%s AND m.board_native_id=%s
ORDER BY m.source_symbol COLLATE "C" LIMIT %s OFFSET %s""",
                (
                    source_capture_id,
                    target_session,
                    source_package_id,
                    source_capture_id,
                    target_session,
                    source_package_id,
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
        "snapshot": snapshot,
        "source_version": {
            "target_session": target_session.isoformat(),
            "source_package_id": source_package_id,
            "source_capture_id": int(source_capture_id),
            "parser_contract": parser_contract,
            "raw_row_count": expected_row_count,
            "source_byte_count": int(source_byte_count),
            "source_time_quality": source_time_quality,
            "source_clock_role": source_clock_role,
            "source_available_at": source_available_at.isoformat()
            if source_available_at
            else None,
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
        "unmatched_b_t_reason": "NOT_IN_SELECTED_B_T;不推断停牌、上市或退市",
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


def _breadth_source_version(
    cursor: Any,
    *,
    target_session: date,
    source_package_id: int,
    profile_revision: str,
) -> dict[str, Any]:
    cursor.execute(
        """SELECT DISTINCT r.source_package_id, r.source_capture_id,
       c.source_available_at, c.source_time_quality, c.source_clock_role,
       c.parser_contract, c.normalized_count, s.dataset_id, s.byte_count
FROM source_raw_daily r
JOIN source_captures c ON c.capture_id=r.source_capture_id
  AND c.source_package_id=r.source_package_id AND c.target_session=r.session_date
JOIN source_packages s ON s.source_package_id=r.source_package_id
WHERE r.session_date=%s::date AND r.source_package_id=%s
ORDER BY r.source_capture_id, r.source_package_id LIMIT 2""",
        (target_session, source_package_id),
    )
    rows = cursor.fetchall()
    if len(rows) != 1:
        raise TdxSectorMembersPgError(
            "SOURCE_VERSION_NOT_UNIQUE",
            "目标日与来源包数字ID没有唯一Raw捕获",
        )
    (
        source_package_id,
        source_capture_id,
        source_available_at,
        source_time_quality,
        source_clock_role,
        parser_contract,
        raw_count,
        dataset_id,
        source_byte_count,
    ) = rows[0]
    if (
        dataset_id != source_raw._DATASET_ID
        or type(raw_count) is not int
        or raw_count <= 0
    ):
        raise TdxSectorMembersPgError(
            "SOURCE_VERSION_INVALID", "Raw来源捕获或内容元数据不合法"
        )
    if raw_count > MAX_MEMBER_INSERTS:
        raise TdxSectorMembersPgError(
            "BREADTH_SOURCE_ROW_BUDGET_EXCEEDED",
            "目标日Raw来源超过广度读取行数上限",
        )
    cursor.execute(
        """SELECT count(*)::bigint,
       count(DISTINCT source_symbol)::bigint,
       count(*) FILTER (WHERE bar_status='TARGET_PRESENT')::bigint,
       count(*) FILTER (WHERE bar_status='TARGET_MISSING')::bigint,
       count(*) FILTER (WHERE bar_status='READ_FAILED')::bigint
FROM source_raw_daily
WHERE source_capture_id=%s AND session_date=%s::date AND source_package_id=%s""",
        (source_capture_id, target_session, source_package_id),
    )
    raw_counts = cursor.fetchone()
    if int(raw_counts[0]) != raw_count or int(raw_counts[1]) != raw_count:
        raise TdxSectorMembersPgError(
            "SOURCE_RAW_COUNT_MISMATCH",
            "capture记录与Raw代码集合分母不一致",
        )
    cursor.execute(
        """SELECT count(*)::bigint,
       count(DISTINCT source_symbol)::bigint,
       count(DISTINCT source_capture_id)::bigint
FROM source_symbol_price_profiles
WHERE source_capture_id=%s AND target_session=%s::date AND source_package_id=%s
  AND profile_revision=%s""",
        (
            source_capture_id,
            target_session,
            source_package_id,
            profile_revision,
        ),
    )
    profile_counts = cursor.fetchone()
    if (
        int(profile_counts[0]) != raw_count
        or int(profile_counts[1]) != raw_count
        or int(profile_counts[2]) != 1
    ):
        raise TdxSectorMembersPgError(
            "PROFILE_SOURCE_IDENTITY_MISMATCH",
            "画像行分母、唯一捕获或capture与Raw来源版本不一致",
        )
    return {
        "target_session": target_session,
        "source_package_id": int(source_package_id),
        "source_capture_id": int(source_capture_id),
        "source_symbol_count": raw_count,
        "parser_contract": parser_contract,
        "source_time_quality": str(source_time_quality),
        "source_clock_role": str(source_clock_role),
        "source_available_at": source_available_at.isoformat()
        if source_available_at
        else None,
        "source_byte_count": int(source_byte_count),
        "profile_revision": profile_revision,
    }


def _validate_breadth_request(
    *,
    snapshot_id: int,
    board_kind: str,
    board_native_id: str | None,
    before_session: date,
    after_session: date,
    research_cutoff: date,
    identities: Sequence[Any],
    offset: int,
    limit: int,
    member_offset: int,
    member_limit: int,
    include_core_transitions: bool,
    core_transition_kind: str,
) -> None:
    if type(snapshot_id) is not int or snapshot_id <= 0:
        raise TdxSectorMembersPgError("SNAPSHOT_ID_INVALID", "快照ID必须为正BIGINT")
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
    if (
        len(identities) != 4
        or type(identities[0]) is not int
        or type(identities[2]) is not int
        or identities[0] <= 0
        or identities[2] <= 0
        or any(
            not isinstance(value, str)
            or re.fullmatch(r"PROFILE_R[0-9]{2}", value) is None
            for value in (identities[1], identities[3])
        )
    ):
        raise TdxSectorMembersPgError(
            "BREADTH_PROFILE_IDENTITY_INVALID",
            "两端必须提供正数来源包ID及显式PROFILE_RNN画像修订",
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
                    "source_capture_id": source["source_capture_id"],
                    "source_package_id": source["source_package_id"],
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
                    reason = "NOT_IN_EXACT_B_T"
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
                    "source_package_id": after_source["source_package_id"],
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
    snapshot_id: int,
    board_kind: str,
    board_native_id: str | None,
    before_session: date,
    after_session: date,
    research_cutoff: date,
    before_source_package_id: int,
    before_profile_revision: str,
    after_source_package_id: int,
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
        before_source_package_id,
        before_profile_revision,
        after_source_package_id,
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
        with _transaction(config, write=False) as (_connection, cursor, _unused):
            cursor.execute(
                """SELECT snapshot_id, source_kind,
       source_path_role, source_file_size, observed_at, source_valid_on,
       source_known_at, source_available_at, clock_quality, research_mode,
       parse_contract_version, rows_declared, source_quality
FROM tdx_sector_source_snapshots WHERE snapshot_id=%s""",
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
                    target_session=before_session,
                    max_target_session=research_cutoff,
                    source_package_id=before_source_package_id,
                    profile_revision=before_profile_revision,
                )
                after_profile = _snapshot_for_session(
                    cursor,
                    target_session=after_session,
                    max_target_session=research_cutoff,
                    source_package_id=after_source_package_id,
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

            before_source = _breadth_source_version(
                cursor,
                target_session=before_session,
                source_package_id=before_source_package_id,
                profile_revision=before_profile_revision,
            )
            after_source = _breadth_source_version(
                cursor,
                target_session=after_session,
                source_package_id=after_source_package_id,
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
FROM tdx_sector_catalog c
LEFT JOIN tdx_sector_members m
  ON m.catalog_id=c.id
WHERE c.snapshot_id=%s AND c.board_kind=%s"""
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
FROM tdx_sector_catalog c
WHERE c.snapshot_id=%s AND c.board_kind=%s"""
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
FROM tdx_sector_catalog c
LEFT JOIN tdx_sector_members m
  ON m.catalog_id=c.id
LEFT JOIN classified x ON x.source_symbol=m.source_symbol
LEFT JOIN source_raw_daily rb
  ON rb.source_capture_id=%s
 AND rb.source_symbol=m.source_symbol AND rb.session_date=%s::date
 AND rb.source_package_id=%s
LEFT JOIN source_raw_daily ra
  ON ra.source_capture_id=%s
 AND ra.source_symbol=m.source_symbol AND ra.session_date=%s::date
 AND ra.source_package_id=%s
WHERE c.snapshot_id=%s AND c.board_kind=%s"""
                + member_filter_sql
                + """
ORDER BY c.board_native_id COLLATE "C", m.source_symbol COLLATE "C"
"""
            )
            parameters = (
                *_profile_change_snapshot_parameters(before_profile),
                *_profile_change_snapshot_parameters(after_profile),
                before_source["source_capture_id"],
                before_session,
                before_source_package_id,
                after_source["source_capture_id"],
                after_session,
                after_source_package_id,
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
        "before_source_version": before_source,
        "after_source_version": after_source,
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
    snapshot_id: int,
    board_kind: str,
    board_native_id: str | None,
    target_session: date,
    research_cutoff: date,
    source_package_id: int,
    profile_revision: str | None,
    official_calendar_id: str,
    official_calendar_revision: str,
    window_sessions: Sequence[date],
    read_context: TdxSectorStrengthReadContext | None = None,
) -> dict[str, Any]:
    """在一个只读快照中读取精确板块、目标价格画像与同包Raw历史。"""
    if type(snapshot_id) is not int or snapshot_id <= 0:
        raise TdxSectorMembersPgError("SNAPSHOT_ID_INVALID", "快照ID必须为正BIGINT")
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
    if type(source_package_id) is not int or source_package_id <= 0:
        raise TdxSectorMembersPgError(
            "SOURCE_PACKAGE_ID_INVALID", "来源包ID必须是正BIGINT"
        )
    if (
        profile_revision is not None
        and re.fullmatch(r"PROFILE_R[0-9]{2}", profile_revision) is None
    ):
        raise TdxSectorMembersPgError(
            "PROFILE_REVISION_INVALID", "画像修订必须为显式PROFILE_RNN标签"
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

    if read_context is not None and (
        not isinstance(read_context, TdxSectorStrengthReadContext)
        or not read_context.active
    ):
        raise TdxSectorMembersPgError(
            "STRENGTH_READ_CONTEXT_INVALID",
            "共享板块读取上下文必须来自仍开启的只读事务",
        )
    transaction_context = (
        nullcontext(
            (
                read_context.connection,
                read_context.cursor,
                None,
            )
        )
        if read_context is not None
        else _transaction(config, write=False)
    )
    try:
        with transaction_context as (_connection, cursor, _unused):
            cursor.execute(
                """SELECT snapshot_id, source_kind,
       source_path_role, source_file_size, observed_at, source_valid_on,
       source_known_at, source_available_at, clock_quality, research_mode,
       parse_contract_version, rows_declared, source_quality
FROM tdx_sector_source_snapshots WHERE snapshot_id=%s""",
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
                """SELECT DISTINCT source_capture_id
FROM source_raw_daily
WHERE session_date=%s::date
  AND source_package_id=%s
ORDER BY source_capture_id LIMIT 2""",
                (target_session, source_package_id),
            )
            raw_capture_ids = [int(row[0]) for row in cursor.fetchall()]
            if len(raw_capture_ids) != 1:
                raise TdxSectorMembersPgError(
                    "SOURCE_VERSION_NOT_UNIQUE",
                    "目标日与来源包ID没有唯一Raw捕获",
                )
            source_capture_id = raw_capture_ids[0]
            cursor.execute(
                """SELECT DISTINCT profile_revision
FROM source_symbol_price_profiles
WHERE target_session=%s::date AND source_capture_id=%s AND source_package_id=%s
ORDER BY profile_revision""",
                (target_session, source_capture_id, source_package_id),
            )
            profile_revisions = [str(row[0]).strip() for row in cursor.fetchall()]
            selected_profile = profile_revision
            if selected_profile is None and len(profile_revisions) == 1:
                selected_profile = profile_revisions[0]
            elif selected_profile is None and len(profile_revisions) > 1:
                raise TdxSectorMembersPgError(
                    "PROFILE_REVISION_REQUIRED",
                    "目标日画像有多个修订，必须显式选择profile revision",
                )
            elif selected_profile is None:
                raise TdxSectorMembersPgError(
                    "PROFILE_REVISION_NOT_FOUND",
                    "目标日画像没有已保存的Core修订",
                )
            elif selected_profile not in profile_revisions:
                raise TdxSectorMembersPgError(
                    "PROFILE_REVISION_NOT_FOUND",
                    "目标日精确来源版本中不存在指定画像修订",
                )

            validation_key = (
                source_capture_id,
                source_package_id,
                target_session,
                research_cutoff,
                selected_profile,
                official_calendar_id,
                official_calendar_revision,
                tuple(window_sessions),
            )
            profile_snapshot = (
                read_context.validated_profile_snapshots.get(validation_key)
                if read_context is not None
                else None
            )
            if profile_snapshot is None:
                try:
                    profile_snapshot = _snapshot_for_session(
                        cursor,
                        target_session=target_session,
                        max_target_session=research_cutoff,
                        source_package_id=source_package_id,
                        profile_revision=selected_profile,
                    )
                except PriceProfilePgError as error:
                    raise TdxSectorMembersPgError(error.code, str(error)) from error
                if read_context is not None:
                    read_context.validated_profile_snapshots[validation_key] = (
                        profile_snapshot
                    )
            if (
                profile_snapshot["history_calendar_id"] != official_calendar_id
                or profile_snapshot["history_calendar_revision"]
                != official_calendar_revision
            ):
                raise TdxSectorMembersPgError(
                    "PROFILE_CALENDAR_IDENTITY_MISMATCH",
                    "目标Core画像绑定的官方日历身份或修订与现行官方日历不一致",
                )
            source_version = (
                read_context.validated_source_versions.get(validation_key)
                if read_context is not None
                else None
            )
            if source_version is None:
                source_version = _breadth_source_version(
                    cursor,
                    target_session=target_session,
                    source_package_id=source_package_id,
                    profile_revision=selected_profile,
                )
                if read_context is not None:
                    read_context.validated_source_versions[validation_key] = (
                        source_version
                    )

            board_filter_sql = ""
            board_filter_params: tuple[Any, ...] = ()
            if board_native_id is not None:
                board_filter_sql = " AND c.board_native_id=%s"
                board_filter_params = (board_native_id,)
            cursor.execute(
                """SELECT count(DISTINCT c.board_native_id)::bigint,
       count(m.source_symbol)::bigint
FROM tdx_sector_catalog c
LEFT JOIN tdx_sector_members m
  ON m.catalog_id=c.id
WHERE c.snapshot_id=%s AND c.board_kind=%s"""
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
FROM tdx_sector_catalog c
LEFT JOIN tdx_sector_members m
  ON m.catalog_id=c.id
WHERE c.snapshot_id=%s AND c.board_kind=%s"""
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
            if symbols:
                (
                    profile_rows,
                    revision_counts,
                    raw_rows,
                    input_read_counts,
                ) = _read_strength_inputs(
                    cursor,
                    source_capture_id=source_version["source_capture_id"],
                    source_package_id=source_package_id,
                    target_session=target_session,
                    profile_revision=selected_profile,
                    official_calendar_id=official_calendar_id,
                    official_calendar_revision=official_calendar_revision,
                    window_sessions=window_sessions,
                    symbols=tuple(sorted(symbols)),
                    read_context=read_context,
                )
                revision_total = sum(
                    counts["row_count"] for counts in revision_counts.values()
                )
                if revision_total > MAX_STRENGTH_HISTORY_ROWS:
                    raise TdxSectorMembersPgError(
                        "STRENGTH_HISTORY_BUDGET_EXCEEDED",
                        "所选成员官方会话窗的Raw修订总行数超过固定只读预算",
                    )
            else:
                profile_rows = []
                raw_rows = []
                revision_counts = {}
                input_read_counts = {
                    "profile_rows": 0,
                    "revision_groups": 0,
                    "raw_rows": 0,
                    "profile_queries": 0,
                    "revision_queries": 0,
                    "raw_queries": 0,
                }
            profile_capture_ids = {
                str(row["source_capture_id"]) for row in profile_rows
            }
            if profile_capture_ids and profile_capture_ids != {
                source_version["source_capture_id"]
            }:
                raise TdxSectorMembersPgError(
                    "PROFILE_SOURCE_IDENTITY_MISMATCH",
                    "TDX板块成员命中的Core画像source_capture_id与Raw捕获不一致",
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
        "snapshot": source_snapshot,
        "source_version": source_version,
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
            "source_raw_rows_fetched_this_call": input_read_counts["raw_rows"],
            "source_raw_rows_reused_within_request": (
                len(raw_rows) - input_read_counts["raw_rows"]
            ),
            "raw_revision_groups_fetched_this_call": input_read_counts[
                "revision_groups"
            ],
            "profile_rows_fetched_this_call": input_read_counts["profile_rows"],
            "strength_input_queries_executed": {
                "profile": input_read_counts["profile_queries"],
                "raw_revision_groups": input_read_counts["revision_queries"],
                "raw_history": input_read_counts["raw_queries"],
            },
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
        "b_t_unmatched_reason": None if matched_b_t else "NOT_IN_EXACT_B_T",
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
