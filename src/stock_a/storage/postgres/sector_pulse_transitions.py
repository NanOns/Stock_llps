"""相邻官方会话板块PULSE变化的数字父键PG读写。"""

from __future__ import annotations

from datetime import date
from typing import Any, Mapping, Sequence

from stock_a.domain.sector_pulse_transitions import classify_pulse_transition
from stock_a.storage.postgres import source_raw
from stock_a.storage.postgres.research_day_facts import _insert_rows, _row_matches

_TABLE = "source_sector_pulse_adjacent_transitions"
_PULSE_TABLE = "source_sector_cross_period_pulse_facts"
_PULSE_ALGORITHM = "SOURCE_CROSS_PERIOD_PULSE_R1"
_TRANSITION_CONTRACT = "SOURCE_CROSS_PERIOD_PULSE_TRANSITION_R1"
_KINDS = ("INDUSTRY", "CONCEPT")
_COLUMNS = (
    "previous_pulse_id",
    "target_pulse_id",
    "calendar_id",
    "calendar_revision",
    "board_kind",
    "board_native_id",
    "transition_contract",
    "previous_pulse_state",
    "target_pulse_state",
    "transition_state",
    "previous_reason_codes",
    "target_reason_codes",
    "calculation_status",
    "research_mode",
    "price_coordinate",
    "formal_rotation_state",
)
_KEY = (
    "previous_pulse_id",
    "target_pulse_id",
    "transition_contract",
    "calendar_id",
    "calendar_revision",
)


class SectorPulseTransitionPgError(ValueError):
    """相邻PULSE事实的稳定PG错误。"""

    def __init__(self, code: str, message: str):
        self.code = code
        self.preserve_transaction_error = True
        super().__init__(message)


def _day_identity(day: Mapping[str, Any], label: str) -> dict[str, Any]:
    source = day.get("source_version")
    session_value = day.get("target_session")
    try:
        session = date.fromisoformat(session_value)
    except (TypeError, ValueError) as error:
        raise SectorPulseTransitionPgError(
            "PULSE_TRANSITION_SESSION_INVALID", f"{label}会话日期无效"
        ) from error
    if not isinstance(source, Mapping):
        raise SectorPulseTransitionPgError(
            "PULSE_TRANSITION_SOURCE_VERSION_MISSING", f"{label}来源版本缺失"
        )
    package_id = source.get("source_package_id")
    capture_id = source.get("source_capture_id")
    row_count = source.get("raw_row_count")
    profile_revision = day.get("profile_revision")
    if (
        type(package_id) is not int
        or package_id <= 0
        or type(capture_id) is not int
        or capture_id <= 0
        or type(row_count) is not int
        or row_count < 1
        or not isinstance(profile_revision, str)
        or not profile_revision.startswith("PROFILE_R")
    ):
        raise SectorPulseTransitionPgError(
            "PULSE_TRANSITION_SOURCE_IDENTITY_INVALID",
            f"{label}来源包、捕获、Profile或分母无效",
        )
    return {
        "target_session": session,
        "source_package_id": package_id,
        "source_capture_id": capture_id,
        "raw_row_count": row_count,
        "profile_revision": profile_revision,
        "source_version": source,
        "day_result": day,
    }


def _verify_source_partition(cursor: Any, day: Mapping[str, Any], label: str) -> None:
    identity = _day_identity(day, label)
    cursor.execute(
        "SELECT count(*)::bigint, count(DISTINCT source_symbol)::bigint, "
        "count(DISTINCT source_capture_id)::bigint, count(DISTINCT source_package_id)::bigint "
        "FROM source_raw_daily WHERE session_date=%s AND source_package_id=%s "
        "AND source_capture_id=%s",
        (
            identity["target_session"],
            identity["source_package_id"],
            identity["source_capture_id"],
        ),
    )
    raw = tuple(map(int, cursor.fetchone()))
    if raw != (identity["raw_row_count"], identity["raw_row_count"], 1, 1):
        raise SectorPulseTransitionPgError(
            "PULSE_TRANSITION_RAW_PARTITION_MISMATCH",
            f"{label}Raw总体未闭合到精确数据包和捕获ID",
        )
    cursor.execute(
        "SELECT count(*)::bigint, count(DISTINCT source_symbol)::bigint, "
        "count(DISTINCT profile_revision)::bigint, min(producer_version), max(producer_version) "
        "FROM source_pool_seed_day_facts WHERE target_session=%s AND source_package_id=%s "
        "AND source_capture_id=%s AND profile_revision=%s",
        (
            identity["target_session"],
            identity["source_package_id"],
            identity["source_capture_id"],
            identity["profile_revision"],
        ),
    )
    seed = cursor.fetchone()
    if (
        int(seed[0]) != identity["raw_row_count"]
        or int(seed[1]) != identity["raw_row_count"]
        or int(seed[2]) != 1
        or seed[3] != "SOURCE_POOL_SEED_CORE_PROFILE_R1"
        or seed[4] != seed[3]
    ):
        raise SectorPulseTransitionPgError(
            "PULSE_TRANSITION_PROFILE_PARTITION_MISMATCH",
            f"{label}Seed/Profile分区未闭合到精确业务键",
        )


def _pulse_parent_id(
    cursor: Any,
    *,
    identity: Mapping[str, Any],
    kind: str,
    board_id: str,
    fact: Mapping[str, Any],
) -> int:
    catalog_id = fact.get("catalog_id")
    if type(catalog_id) is not int or catalog_id <= 0:
        raise SectorPulseTransitionPgError(
            "PULSE_TRANSITION_CATALOG_ID_INVALID", f"{kind}板块缺少数字目录ID"
        )
    cursor.execute(
        f"SELECT p.id, p.pulse_state, p.pulse_reason_codes, p.profile_revision, "
        f"p.target_session, p.source_package_id, p.source_capture_id, "
        f"p.algorithm_contract, p.calculation_status, p.formal_rotation_state, "
        f"p.research_mode, p.membership_mode, p.price_coordinate, "
        f"c.snapshot_id, c.board_kind, c.board_native_id "
        f"FROM {_PULSE_TABLE} p JOIN tdx_sector_catalog c ON c.id=p.catalog_id "
        "WHERE p.target_session=%s AND p.source_package_id=%s "
        "AND p.source_capture_id=%s AND p.profile_revision=%s "
        "AND p.catalog_id=%s AND p.algorithm_contract=%s",
        (
            identity["target_session"],
            identity["source_package_id"],
            identity["source_capture_id"],
            identity["profile_revision"],
            catalog_id,
            _PULSE_ALGORITHM,
        ),
    )
    row = cursor.fetchone()
    if row is None or row[1] != fact.get("state") or row[2] != fact.get("reason_codes"):
        raise SectorPulseTransitionPgError(
            "PULSE_TRANSITION_PARENT_FACT_MISMATCH",
            f"{kind}.{board_id}PULSE父事实缺失或与独立读回不一致",
        )
    if (
        str(row[3]).strip() != identity["profile_revision"]
        or row[4] != identity["target_session"]
        or int(row[5]) != identity["source_package_id"]
        or int(row[6]) != identity["source_capture_id"]
        or row[7] != _PULSE_ALGORITHM
        or row[8] != "COMPLETE"
        or row[9] != "NOT_EVALUATED"
        or row[10] != "NON_PIT_RETROSPECTIVE"
        or row[11] != "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT"
        or row[12] != "RAW_UNADJUSTED"
        or int(row[13]) != fact.get("snapshot_id")
        or row[14] != kind
        or row[15] != board_id
    ):
        raise SectorPulseTransitionPgError(
            "PULSE_TRANSITION_PARENT_SCOPE_MISMATCH",
            f"{kind}.{board_id}PULSE父事实来源、快照或业务键不一致",
        )
    parent_id = row[0]
    if type(parent_id) is not int or parent_id <= 0:
        raise SectorPulseTransitionPgError(
            "PULSE_TRANSITION_PARENT_ID_INVALID", "PULSE父事实主键不是正BIGINT"
        )
    return parent_id


def _expected_rows(
    cursor: Any,
    *,
    previous_result: Mapping[str, Any],
    target_result: Mapping[str, Any],
    calendar_identity: Mapping[str, Any],
) -> tuple[dict[str, list[tuple[Any, ...]]], dict[str, dict[str, Any]]]:
    previous = _day_identity(previous_result, "前一会话")
    target = _day_identity(target_result, "目标会话")
    calendar_id = calendar_identity.get("calendar_id")
    calendar_revision = calendar_identity.get("calendar_revision")
    if (
        not isinstance(calendar_id, str)
        or not calendar_id
        or not isinstance(calendar_revision, str)
        or not calendar_revision
        or set(calendar_identity) != {"calendar_id", "calendar_revision"}
    ):
        raise SectorPulseTransitionPgError(
            "PULSE_TRANSITION_CALENDAR_INVALID", "日历身份必须由日历ID和显式修订组成"
        )
    if previous["target_session"] >= target["target_session"]:
        raise SectorPulseTransitionPgError(
            "PULSE_TRANSITION_SESSION_ORDER_INVALID", "PULSE变化两端会话顺序无效"
        )
    _verify_source_partition(cursor, previous_result, "前一会话")
    _verify_source_partition(cursor, target_result, "目标会话")
    rows_by_kind: dict[str, list[tuple[Any, ...]]] = {}
    expected_by_kind: dict[str, dict[str, Any]] = {}
    for kind in _KINDS:
        previous_group = previous_result.get("by_kind", {}).get(kind)
        target_group = target_result.get("by_kind", {}).get(kind)
        if (
            not isinstance(previous_group, Mapping)
            or previous_group.get("status") != "AVAILABLE"
            or not isinstance(target_group, Mapping)
            or target_group.get("status") != "AVAILABLE"
        ):
            raise SectorPulseTransitionPgError(
                "PULSE_TRANSITION_DAY_NOT_COMPLETE", f"{kind}板块两端没有完整PULSE事实"
            )
        before_facts = previous_group.get("facts")
        after_facts = target_group.get("facts")
        if (
            not isinstance(before_facts, Mapping)
            or not before_facts
            or not isinstance(after_facts, Mapping)
            or set(before_facts) != set(after_facts)
            or previous_group.get("snapshot_id") != target_group.get("snapshot_id")
        ):
            raise SectorPulseTransitionPgError(
                "PULSE_TRANSITION_BOARD_PARTITION_MISMATCH",
                f"{kind}板块两端成员快照或板块集合不同",
            )
        snapshot_id = previous_group.get("snapshot_id")
        if type(snapshot_id) is not int or snapshot_id <= 0:
            raise SectorPulseTransitionPgError(
                "PULSE_TRANSITION_SNAPSHOT_INVALID", f"{kind}板块快照不是正整数ID"
            )
        expected_facts = {}
        rows = []
        for board_id in sorted(before_facts):
            before = before_facts[board_id]
            after = after_facts[board_id]
            if not isinstance(before, Mapping) or not isinstance(after, Mapping):
                raise SectorPulseTransitionPgError(
                    "PULSE_TRANSITION_FACT_INVALID", "板块PULSE事实结构无效"
                )
            previous_id = _pulse_parent_id(
                cursor, identity=previous, kind=kind, board_id=board_id, fact=before
            )
            target_id = _pulse_parent_id(
                cursor, identity=target, kind=kind, board_id=board_id, fact=after
            )
            previous_state = before.get("state")
            target_state = after.get("state")
            if previous_state not in {
                "TRUE",
                "FALSE",
                "UNKNOWN",
            } or target_state not in {
                "TRUE",
                "FALSE",
                "UNKNOWN",
            }:
                raise SectorPulseTransitionPgError(
                    "PULSE_TRANSITION_STATE_INVALID", "板块脉冲状态不是有效三态"
                )
            previous_reasons = before.get("reason_codes")
            target_reasons = after.get("reason_codes")
            if (
                not isinstance(previous_reasons, list)
                or any(
                    not isinstance(item, str) or not item for item in previous_reasons
                )
                or not isinstance(target_reasons, list)
                or any(not isinstance(item, str) or not item for item in target_reasons)
            ):
                raise SectorPulseTransitionPgError(
                    "PULSE_TRANSITION_REASON_INVALID", "PULSE原因不是字符串数组"
                )
            transition_state = classify_pulse_transition(previous_state, target_state)
            record = {
                "status": "AVAILABLE",
                "previous_state": previous_state,
                "target_state": target_state,
                "transition_state": transition_state,
                "previous_reason_codes": list(previous_reasons),
                "target_reason_codes": list(target_reasons),
                "snapshot_id": snapshot_id,
                "formal_rotation_state": "NOT_EVALUATED",
                "research_mode": "NON_PIT_RETROSPECTIVE",
                "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
                "price_coordinate": "RAW_UNADJUSTED",
            }
            expected_facts[board_id] = record
            values = {
                "previous_pulse_id": previous_id,
                "target_pulse_id": target_id,
                "calendar_id": calendar_id,
                "calendar_revision": calendar_revision,
                "board_kind": kind,
                "board_native_id": board_id,
                "transition_contract": _TRANSITION_CONTRACT,
                "previous_pulse_state": previous_state,
                "target_pulse_state": target_state,
                "transition_state": transition_state,
                "previous_reason_codes": list(previous_reasons),
                "target_reason_codes": list(target_reasons),
                "calculation_status": "COMPLETE",
                "research_mode": "NON_PIT_RETROSPECTIVE",
                "price_coordinate": "RAW_UNADJUSTED",
                "formal_rotation_state": "NOT_EVALUATED",
            }
            rows.append(tuple(values[name] for name in _COLUMNS))
        rows_by_kind[kind] = rows
        expected_by_kind[kind] = {
            "snapshot_id": snapshot_id,
            "facts": expected_facts,
        }
    return rows_by_kind, expected_by_kind


def _scope_rows(cursor: Any, rows: Sequence[Sequence[Any]]) -> list[tuple[Any, ...]]:
    if not rows:
        return []
    previous_ids = [row[_COLUMNS.index("previous_pulse_id")] for row in rows]
    target_ids = [row[_COLUMNS.index("target_pulse_id")] for row in rows]
    first = rows[0]
    cursor.execute(
        f"SELECT {', '.join(_COLUMNS)} FROM {_TABLE} "
        "WHERE previous_pulse_id=ANY(%s) AND target_pulse_id=ANY(%s) "
        "AND transition_contract=%s AND calendar_id=%s AND calendar_revision=%s "
        'ORDER BY board_native_id COLLATE "C"',
        (
            previous_ids,
            target_ids,
            first[_COLUMNS.index("transition_contract")],
            first[_COLUMNS.index("calendar_id")],
            first[_COLUMNS.index("calendar_revision")],
        ),
    )
    return cursor.fetchall()


def persist_source_sector_pulse_transitions(
    config: Mapping[str, Any],
    *,
    previous_result: Mapping[str, Any],
    target_result: Mapping[str, Any],
    calendar_identity: Mapping[str, Any],
) -> dict[str, Any]:
    """以两端PULSE数字父键原子保存完整行业与GN变化。"""
    for attempt in range(5):
        try:
            with source_raw._transaction(
                config, write=True, statement_timeout_ms=30000
            ) as (_, cursor, _connection_identity):
                cursor.execute("SELECT to_regclass(%s) IS NOT NULL", (_TABLE,))
                if cursor.fetchone() != (True,):
                    raise SectorPulseTransitionPgError(
                        "PULSE_TRANSITION_MIGRATION_MISSING",
                        "R06 PULSE变化迁移尚未应用",
                    )
                rows_by_kind, expected_by_kind = _expected_rows(
                    cursor,
                    previous_result=previous_result,
                    target_result=target_result,
                    calendar_identity=calendar_identity,
                )
                scopes = {}
                for kind, rows in rows_by_kind.items():
                    actual = _scope_rows(cursor, rows)
                    if actual and (
                        len(actual) != len(rows)
                        or any(
                            not _row_matches(expected, stored)
                            for expected, stored in zip(rows, actual, strict=True)
                        )
                    ):
                        raise SectorPulseTransitionPgError(
                            "PULSE_TRANSITION_CONTENT_CONFLICT",
                            f"{kind}相同父事实和业务合同已存在异内容或不完整行",
                        )
                    scopes[kind] = len(actual) == len(rows)
                if any(scopes.values()) and not all(scopes.values()):
                    raise SectorPulseTransitionPgError(
                        "PULSE_TRANSITION_PARTIAL_SCOPE",
                        "行业与GN变化事实没有处于同一原子业务批次",
                    )
                inserted = {}
                if not all(scopes.values()):
                    all_rows = rows_by_kind["INDUSTRY"] + rows_by_kind["CONCEPT"]
                    inserted[_TABLE] = _insert_rows(
                        cursor, _TABLE, _COLUMNS, _KEY, all_rows
                    )
                for kind, rows in rows_by_kind.items():
                    actual = _scope_rows(cursor, rows)
                    if len(actual) != len(rows) or any(
                        not _row_matches(expected, stored)
                        for expected, stored in zip(rows, actual, strict=True)
                    ):
                        raise SectorPulseTransitionPgError(
                            "PULSE_TRANSITION_READBACK_MISMATCH",
                            f"{kind}变化事实事务内读回不一致",
                        )
            already_present = all(scopes.values())
            return {
                "status": "IDEMPOTENT_ALREADY_PRESENT"
                if already_present
                else "WRITTEN_AND_TYPED_READBACK_VERIFIED",
                "rows_expected": {
                    kind: len(rows) for kind, rows in rows_by_kind.items()
                },
                "rows_inserted": inserted,
                "same_input_retry": "ZERO_INSERTS_TYPED_EQUALITY_VERIFIED"
                if already_present
                else None,
                "transaction": "SERIALIZABLE_ATOMIC_INDUSTRY_AND_CONCEPT_TRANSITIONS",
                "database_write_attempted": not already_present,
                "ddl_executed": False,
                "dml_executed": not already_present,
                "calendar_identity": dict(calendar_identity),
                "snapshot_ids": {
                    kind: group["snapshot_id"]
                    for kind, group in expected_by_kind.items()
                },
            }
        except source_raw.RawSourcePgError as error:
            if getattr(error.__cause__, "sqlstate", None) != "40001" or attempt == 4:
                raise
            time.sleep(0.1 * (2**attempt))
    raise AssertionError("不可达的PULSE变化事务重试状态")


def read_source_sector_pulse_transitions(
    config: Mapping[str, Any],
    *,
    previous_result: Mapping[str, Any],
    target_result: Mapping[str, Any],
    calendar_identity: Mapping[str, Any],
) -> dict[str, Any]:
    """只读校验两端PULSE父事实与相邻变化typed结果。"""
    with source_raw._transaction(config, write=False, statement_timeout_ms=20000) as (
        _,
        cursor,
        _connection_identity,
    ):
        rows_by_kind, expected_by_kind = _expected_rows(
            cursor,
            previous_result=previous_result,
            target_result=target_result,
            calendar_identity=calendar_identity,
        )
        by_kind = {}
        for kind, rows in rows_by_kind.items():
            actual = _scope_rows(cursor, rows)
            if not actual:
                by_kind[kind] = {
                    "status": "NOT_COMPUTED",
                    "reason_codes": ["PULSE_TRANSITION_FACTS_NOT_SAVED"],
                    "snapshot_id": expected_by_kind[kind]["snapshot_id"],
                    "facts": {},
                }
                continue
            if len(actual) != len(rows) or any(
                not _row_matches(expected, stored)
                for expected, stored in zip(rows, actual, strict=True)
            ):
                by_kind[kind] = {
                    "status": "INCOMPLETE",
                    "reason_codes": ["PULSE_TRANSITION_PARTITION_NOT_CLOSED"],
                    "snapshot_id": expected_by_kind[kind]["snapshot_id"],
                    "facts": {},
                }
                continue
            facts = {}
            for board_id, record in expected_by_kind[kind]["facts"].items():
                facts[board_id] = record
            by_kind[kind] = {
                "status": "AVAILABLE",
                "reason_codes": [],
                "snapshot_id": expected_by_kind[kind]["snapshot_id"],
                "expected_board_count": len(rows),
                "read_board_count": len(actual),
                "facts": facts,
            }
    statuses = [by_kind[kind]["status"] for kind in _KINDS]
    overall = (
        "AVAILABLE"
        if all(status == "AVAILABLE" for status in statuses)
        else "INCOMPLETE"
        if any(status in {"AVAILABLE", "INCOMPLETE"} for status in statuses)
        else "NOT_COMPUTED"
    )
    return {
        "status": overall,
        "by_kind": by_kind,
        "calendar_identity": dict(calendar_identity),
        "database_write_attempted": False,
        "ddl_executed": False,
        "dml_executed": False,
    }


__all__ = [
    "SectorPulseTransitionPgError",
    "persist_source_sector_pulse_transitions",
    "read_source_sector_pulse_transitions",
]
