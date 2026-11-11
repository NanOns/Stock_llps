"""相邻官方会话板块PULSE变化的最小typed PG读写。"""

from __future__ import annotations

from datetime import date
import time
from typing import Any, Mapping, Sequence
from uuid import UUID

from stock_a.domain.sector_pulse_transitions import classify_pulse_transition
from stock_a.storage.postgres import source_raw
from stock_a.storage.postgres.research_day_facts import (
    _insert_rows,
    _normalize_value,
    _row_matches,
)

_TABLE = "stock.source_sector_pulse_adjacent_transitions"
_PULSE_TABLE = "stock.source_sector_cross_period_pulse_facts"
_PULSE_ALGORITHM = "SOURCE_CROSS_PERIOD_PULSE_R1"
_TRANSITION_CONTRACT = "SOURCE_CROSS_PERIOD_PULSE_TRANSITION_R1"
_KINDS = ("INDUSTRY", "CONCEPT")
_COLUMNS = (
    "namespace_id",
    "previous_session",
    "target_session",
    "previous_package_sha256",
    "previous_source_capture_id",
    "previous_source_content_id",
    "previous_source_raw_digest",
    "previous_profile_revision",
    "target_package_sha256",
    "target_source_capture_id",
    "target_source_content_id",
    "target_source_raw_digest",
    "target_profile_revision",
    "calendar_id",
    "calendar_revision",
    "calendar_sha256",
    "snapshot_id",
    "board_kind",
    "board_native_id",
    "pulse_algorithm_contract",
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
    "namespace_id",
    "previous_session",
    "target_session",
    "previous_package_sha256",
    "previous_source_capture_id",
    "previous_source_content_id",
    "previous_source_raw_digest",
    "previous_profile_revision",
    "target_package_sha256",
    "target_source_capture_id",
    "target_source_content_id",
    "target_source_raw_digest",
    "target_profile_revision",
    "snapshot_id",
    "board_kind",
    "board_native_id",
    "pulse_algorithm_contract",
    "transition_contract",
    "calendar_revision",
    "calendar_sha256",
    "calendar_id",
)
_SCOPE = tuple(column for column in _KEY if column != "board_native_id")


class SectorPulseTransitionPgError(ValueError):
    """相邻PULSE事实的稳定PG错误。"""

    def __init__(self, code: str, message: str):
        self.code = code
        self.preserve_transaction_error = True
        super().__init__(message)


def _uuid(value: Any, label: str) -> str:
    try:
        normalized = str(UUID(str(value)))
    except (ValueError, TypeError, AttributeError) as error:
        raise SectorPulseTransitionPgError(
            "PULSE_TRANSITION_UUID_INVALID", f"{label}必须是规范UUID"
        ) from error
    if normalized != str(value):
        raise SectorPulseTransitionPgError(
            "PULSE_TRANSITION_UUID_INVALID", f"{label}必须为小写规范UUID"
        )
    return normalized


def _sha(value: Any, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(char not in "0123456789abcdef" for char in value)
    ):
        raise SectorPulseTransitionPgError(
            "PULSE_TRANSITION_SHA_INVALID", f"{label}必须是小写SHA-256"
        )
    return value


def _reasons(value: Any) -> list[str]:
    if not isinstance(value, (list, tuple)) or any(
        not isinstance(item, str) or not item for item in value
    ):
        raise SectorPulseTransitionPgError(
            "PULSE_TRANSITION_REASON_INVALID", "PULSE原因必须是非空字符串数组"
        )
    return list(value)


def _day_group(day_result: Mapping[str, Any], kind: str) -> Mapping[str, Any]:
    group = day_result.get("by_kind", {}).get(kind)
    if not isinstance(group, Mapping) or group.get("status") != "AVAILABLE":
        raise SectorPulseTransitionPgError(
            "PULSE_TRANSITION_DAY_NOT_COMPLETE",
            f"{kind}两端PULSE分区均须为完整AVAILABLE",
        )
    return group


def _day_identity(day_result: Mapping[str, Any], label: str) -> dict[str, Any]:
    source = day_result.get("source_version")
    if not isinstance(source, Mapping):
        raise SectorPulseTransitionPgError(
            "PULSE_TRANSITION_SOURCE_IDENTITY_MISSING", f"{label}缺少Raw来源身份"
        )
    profile_revision = _sha(day_result.get("profile_revision"), f"{label}Profile修订")
    identity = {
        "namespace_id": _uuid(source.get("namespace_id"), f"{label}namespace"),
        "target_session": day_result.get("target_session"),
        "package_sha256": _sha(source.get("package_sha256"), f"{label}包SHA"),
        "source_capture_id": _uuid(source.get("source_capture_id"), f"{label}capture"),
        "source_content_id": _sha(source.get("source_content_id"), f"{label}content"),
        "source_raw_digest": _sha(source.get("capture_digest"), f"{label}Raw摘要"),
        "profile_revision": profile_revision,
        "raw_row_count": source.get("raw_row_count"),
    }
    try:
        identity["target_session"] = date.fromisoformat(str(identity["target_session"]))
    except (ValueError, TypeError) as error:
        raise SectorPulseTransitionPgError(
            "PULSE_TRANSITION_SESSION_INVALID", f"{label}日期无效"
        ) from error
    if type(identity["raw_row_count"]) is not int or identity["raw_row_count"] < 1:
        raise SectorPulseTransitionPgError(
            "PULSE_TRANSITION_SOURCE_COUNT_INVALID", f"{label}Raw行数无效"
        )
    return identity


def _expected_rows(
    *,
    previous_result: Mapping[str, Any],
    target_result: Mapping[str, Any],
    calendar_identity: Mapping[str, str],
) -> tuple[list[tuple[Any, ...]], dict[str, dict[str, Any]]]:
    previous = _day_identity(previous_result, "前一会话")
    target = _day_identity(target_result, "目标会话")
    if previous["namespace_id"] != target["namespace_id"]:
        raise SectorPulseTransitionPgError(
            "PULSE_TRANSITION_NAMESPACE_MISMATCH", "两端PG命名空间不一致"
        )
    if previous["target_session"] >= target["target_session"]:
        raise SectorPulseTransitionPgError(
            "PULSE_TRANSITION_SESSION_ORDER_INVALID", "相邻会话日期顺序无效"
        )
    calendar = {
        "calendar_id": calendar_identity.get("calendar_id"),
        "calendar_revision": calendar_identity.get("calendar_revision"),
        "calendar_sha256": _sha(
            calendar_identity.get("calendar_sha256"), "官方日历SHA"
        ),
    }
    if any(
        not isinstance(calendar[name], str) or not calendar[name]
        for name in ("calendar_id", "calendar_revision")
    ):
        raise SectorPulseTransitionPgError(
            "PULSE_TRANSITION_CALENDAR_INVALID", "官方日历身份不完整"
        )
    rows: list[tuple[Any, ...]] = []
    expected_by_kind: dict[str, dict[str, Any]] = {}
    for kind in _KINDS:
        before = _day_group(previous_result, kind)
        after = _day_group(target_result, kind)
        before_snapshot = _uuid(before.get("snapshot_id"), f"前一会话{kind}快照")
        after_snapshot = _uuid(after.get("snapshot_id"), f"目标会话{kind}快照")
        if before_snapshot != after_snapshot:
            raise SectorPulseTransitionPgError(
                "PULSE_TRANSITION_SNAPSHOT_MISMATCH",
                f"{kind}两端必须使用同一成员快照",
            )
        before_facts = before.get("facts")
        after_facts = after.get("facts")
        if not isinstance(before_facts, Mapping) or not isinstance(
            after_facts, Mapping
        ):
            raise SectorPulseTransitionPgError(
                "PULSE_TRANSITION_FACTS_INVALID", f"{kind}PULSE板块事实无效"
            )
        before_ids = set(before_facts)
        after_ids = set(after_facts)
        expected_count = before.get("expected_board_count")
        if (
            type(expected_count) is not int
            or expected_count < 1
            or expected_count != after.get("expected_board_count")
            or before.get("read_board_count") != expected_count
            or after.get("read_board_count") != expected_count
            or len(before_ids) != expected_count
            or before_ids != after_ids
        ):
            raise SectorPulseTransitionPgError(
                "PULSE_TRANSITION_BOARD_PARTITION_MISMATCH",
                f"{kind}两端板块完整分区或共同分母不一致",
            )
        kind_expected: dict[str, Any] = {
            "status": "AVAILABLE",
            "snapshot_id": before_snapshot,
            "expected_board_count": expected_count,
            "read_board_count": expected_count,
            "facts": {},
        }
        for board_id in sorted(before_ids):
            if not isinstance(board_id, str) or not board_id:
                raise SectorPulseTransitionPgError(
                    "PULSE_TRANSITION_BOARD_ID_INVALID", f"{kind}原生板块编号无效"
                )
            before_fact = before_facts[board_id]
            after_fact = after_facts[board_id]
            if not isinstance(before_fact, Mapping) or not isinstance(
                after_fact, Mapping
            ):
                raise SectorPulseTransitionPgError(
                    "PULSE_TRANSITION_FACT_INVALID", f"{kind}板块PULSE事实无效"
                )
            shared_values = (
                (before_fact, "snapshot_id", before_snapshot),
                (after_fact, "snapshot_id", after_snapshot),
                (before_fact, "algorithm_contract", _PULSE_ALGORITHM),
                (after_fact, "algorithm_contract", _PULSE_ALGORITHM),
                (before_fact, "calculation_status", "COMPLETE"),
                (after_fact, "calculation_status", "COMPLETE"),
                (before_fact, "research_mode", "NON_PIT_RETROSPECTIVE"),
                (after_fact, "research_mode", "NON_PIT_RETROSPECTIVE"),
                (
                    before_fact,
                    "membership_mode",
                    "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
                ),
                (
                    after_fact,
                    "membership_mode",
                    "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
                ),
                (before_fact, "price_coordinate", "RAW_UNADJUSTED"),
                (after_fact, "price_coordinate", "RAW_UNADJUSTED"),
                (before_fact, "formal_rotation_state", "NOT_EVALUATED"),
                (after_fact, "formal_rotation_state", "NOT_EVALUATED"),
            )
            if any(
                actual.get(field) != expected
                for actual, field, expected in shared_values
            ):
                raise SectorPulseTransitionPgError(
                    "PULSE_TRANSITION_ALGORITHM_OR_QUALITY_MISMATCH",
                    f"{kind}板块两端算法、快照或质量语义不一致",
                )
            previous_state = before_fact.get("state")
            target_state = after_fact.get("state")
            transition = classify_pulse_transition(previous_state, target_state)
            previous_reasons = _reasons(before_fact.get("reason_codes", []))
            target_reasons = _reasons(after_fact.get("reason_codes", []))
            record = {
                "status": "AVAILABLE",
                "previous_state": previous_state,
                "target_state": target_state,
                "transition_state": transition,
                "previous_reason_codes": previous_reasons,
                "target_reason_codes": target_reasons,
                "snapshot_id": before_snapshot,
                "formal_rotation_state": "NOT_EVALUATED",
                "research_mode": "NON_PIT_RETROSPECTIVE",
                "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
                "price_coordinate": "RAW_UNADJUSTED",
            }
            kind_expected["facts"][board_id] = record
            rows.append(
                (
                    previous["namespace_id"],
                    previous["target_session"],
                    target["target_session"],
                    previous["package_sha256"],
                    previous["source_capture_id"],
                    previous["source_content_id"],
                    previous["source_raw_digest"],
                    previous["profile_revision"],
                    target["package_sha256"],
                    target["source_capture_id"],
                    target["source_content_id"],
                    target["source_raw_digest"],
                    target["profile_revision"],
                    calendar["calendar_id"],
                    calendar["calendar_revision"],
                    calendar["calendar_sha256"],
                    before_snapshot,
                    kind,
                    board_id,
                    _PULSE_ALGORITHM,
                    _TRANSITION_CONTRACT,
                    previous_state,
                    target_state,
                    transition,
                    previous_reasons,
                    target_reasons,
                    "COMPLETE",
                    "NON_PIT_RETROSPECTIVE",
                    "RAW_UNADJUSTED",
                    "NOT_EVALUATED",
                )
            )
        expected_by_kind[kind] = kind_expected
    return rows, expected_by_kind


def _query_scope(cursor: Any, row: Sequence[Any]) -> list[tuple[Any, ...]]:
    values = dict(zip(_COLUMNS, row, strict=True))
    cursor.execute(
        f"SELECT {', '.join(_COLUMNS)} FROM {_TABLE} WHERE "
        "namespace_id=%s::uuid AND previous_session=%s AND target_session=%s "
        "AND previous_package_sha256=%s AND previous_source_capture_id=%s::uuid "
        "AND previous_source_content_id=%s AND previous_source_raw_digest=%s "
        "AND previous_profile_revision=%s AND target_package_sha256=%s "
        "AND target_source_capture_id=%s::uuid AND target_source_content_id=%s "
        "AND target_source_raw_digest=%s AND target_profile_revision=%s "
        "AND snapshot_id=%s::uuid AND board_kind=%s "
        "AND pulse_algorithm_contract=%s AND transition_contract=%s "
        "AND calendar_revision=%s AND calendar_sha256=%s AND calendar_id=%s "
        'ORDER BY board_native_id COLLATE "C"',
        tuple(values[name] for name in _SCOPE),
    )
    return cursor.fetchall()


def _verify_parent_partitions(
    cursor: Any,
    *,
    previous_result: Mapping[str, Any],
    target_result: Mapping[str, Any],
    expected_by_kind: Mapping[str, Mapping[str, Any]],
) -> None:
    previous = _day_identity(previous_result, "前一会话")
    target = _day_identity(target_result, "目标会话")
    for day, label in ((previous, "前一会话"), (target, "目标会话")):
        cursor.execute(
            "SELECT count(*)::bigint, count(DISTINCT source_symbol)::bigint, "
            "count(DISTINCT source_capture_id)::bigint, "
            "count(DISTINCT source_content_id)::bigint "
            "FROM stock.source_raw_daily WHERE namespace_id=%s::uuid "
            "AND session_date=%s AND package_sha256=%s",
            (day["namespace_id"], day["target_session"], day["package_sha256"]),
        )
        raw_count, symbol_count, capture_count, content_count = cursor.fetchone()
        if (
            int(raw_count),
            int(symbol_count),
            int(capture_count),
            int(content_count),
        ) != (
            day["raw_row_count"],
            day["raw_row_count"],
            1,
            1,
        ):
            raise SectorPulseTransitionPgError(
                "PULSE_TRANSITION_RAW_PARTITION_MISMATCH",
                f"{label}Raw分区已变化或不完整",
            )
        cursor.execute(
            "SELECT count(*)::bigint, count(DISTINCT source_symbol)::bigint "
            "FROM stock.source_pool_seed_day_facts WHERE namespace_id=%s::uuid "
            "AND target_session=%s AND package_sha256=%s "
            "AND source_capture_id=%s::uuid AND source_content_id=%s "
            "AND profile_revision=%s AND producer_version=%s",
            (
                day["namespace_id"],
                day["target_session"],
                day["package_sha256"],
                day["source_capture_id"],
                day["source_content_id"],
                day["profile_revision"],
                "SOURCE_POOL_SEED_CORE_PROFILE_R1",
            ),
        )
        seed_count, seed_symbols = cursor.fetchone()
        if (int(seed_count), int(seed_symbols)) != (
            day["raw_row_count"],
            day["raw_row_count"],
        ):
            raise SectorPulseTransitionPgError(
                "PULSE_TRANSITION_PROFILE_PARTITION_MISMATCH",
                f"{label}Seed/Profile修订未闭合到Raw来源池",
            )
    for kind in _KINDS:
        group = expected_by_kind[kind]
        snapshot_id = group["snapshot_id"]
        expected_count = group["expected_board_count"]
        for day in (previous, target):
            cursor.execute(
                f"SELECT board_native_id, pulse_state, pulse_reason_codes, "
                f"calculation_status, research_mode, membership_mode, price_coordinate, "
                f"formal_rotation_state, member_count, ret5_denominator, breadth_denominator "
                f"FROM {_PULSE_TABLE} WHERE namespace_id=%s::uuid AND target_session=%s "
                "AND package_sha256=%s AND source_capture_id=%s::uuid "
                "AND source_content_id=%s AND source_raw_digest=%s "
                "AND profile_revision=%s AND snapshot_id=%s::uuid AND board_kind=%s "
                'AND algorithm_contract=%s ORDER BY board_native_id COLLATE "C"',
                (
                    day["namespace_id"],
                    day["target_session"],
                    day["package_sha256"],
                    day["source_capture_id"],
                    day["source_content_id"],
                    day["source_raw_digest"],
                    day["profile_revision"],
                    snapshot_id,
                    kind,
                    _PULSE_ALGORITHM,
                ),
            )
            actual = cursor.fetchall()
            input_group = _day_group(
                previous_result if day is previous else target_result, kind
            )
            input_facts = input_group["facts"]
            if len(actual) != expected_count:
                raise SectorPulseTransitionPgError(
                    "PULSE_TRANSITION_PARENT_PARTITION_INCOMPLETE",
                    f"{kind}两端PULSE PG分区行数与完整板块目录不一致",
                )
            for (
                board_id,
                state,
                reasons,
                calculation_status,
                research_mode,
                membership_mode,
                price_coordinate,
                rotation_state,
                member_count,
                ret5_denominator,
                breadth_denominator,
            ) in actual:
                fact = input_facts.get(board_id)
                if (
                    not isinstance(fact, Mapping)
                    or fact.get("state") != state
                    or list(fact.get("reason_codes", [])) != list(reasons)
                    or calculation_status != "COMPLETE"
                    or research_mode != "NON_PIT_RETROSPECTIVE"
                    or membership_mode != "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT"
                    or price_coordinate != "RAW_UNADJUSTED"
                    or rotation_state != "NOT_EVALUATED"
                    or int(member_count) != int(ret5_denominator)
                    or int(member_count) != int(breadth_denominator)
                ):
                    raise SectorPulseTransitionPgError(
                        "PULSE_TRANSITION_PARENT_FACT_MISMATCH",
                        f"{kind}PULSE源行、状态、原因或共同成员分母不匹配",
                    )


def _persist_once(
    config: Mapping[str, Any],
    *,
    rows: Sequence[Sequence[Any]],
    expected_by_kind: Mapping[str, Mapping[str, Any]],
    previous_result: Mapping[str, Any],
    target_result: Mapping[str, Any],
) -> dict[str, Any]:
    scopes: list[tuple[str, list[tuple[Any, ...]], bool]] = []
    inserted = 0
    with source_raw._transaction(config, write=True, statement_timeout_ms=30000) as (
        _connection,
        cursor,
        namespace_id,
    ):
        previous = _day_identity(previous_result, "前一会话")
        target = _day_identity(target_result, "目标会话")
        if (
            namespace_id != previous["namespace_id"]
            or namespace_id != target["namespace_id"]
        ):
            raise SectorPulseTransitionPgError(
                "PULSE_TRANSITION_NAMESPACE_MISMATCH", "PG连接namespace与两端事实不一致"
            )
        cursor.execute(
            "SELECT pg_backend_pid(), current_setting('transaction_isolation')"
        )
        backend_pid, isolation = cursor.fetchone()
        cursor.execute("SELECT to_regclass(%s) IS NOT NULL", (_TABLE,))
        if cursor.fetchone() != (True,):
            raise SectorPulseTransitionPgError(
                "PULSE_TRANSITION_MIGRATION_MISSING", "0020相邻变化迁移尚未应用"
            )
        _verify_parent_partitions(
            cursor,
            previous_result=previous_result,
            target_result=target_result,
            expected_by_kind=expected_by_kind,
        )
        for kind in _KINDS:
            group = [row for row in rows if row[_COLUMNS.index("board_kind")] == kind]
            if not group:
                raise SectorPulseTransitionPgError(
                    "PULSE_TRANSITION_EMPTY_KIND", f"{kind}不得省略完整变化分区"
                )
            existing = _query_scope(cursor, group[0])
            if existing and (
                len(existing) != len(group)
                or any(
                    not _row_matches(expected, actual)
                    for expected, actual in zip(group, existing, strict=True)
                )
            ):
                raise SectorPulseTransitionPgError(
                    "PULSE_TRANSITION_CONTENT_CONFLICT",
                    f"{kind}相同业务键已有异内容或不完整分区，事务回滚",
                )
            scopes.append((kind, group, len(existing) == len(group)))
        if any(scope[2] for scope in scopes) and not all(scope[2] for scope in scopes):
            raise SectorPulseTransitionPgError(
                "PULSE_TRANSITION_CROSS_KIND_PARTIAL_SCOPE",
                "行业与概念变化必须在同一事务中完整持久化",
            )
        already_present = all(scope[2] for scope in scopes)
        if not already_present:
            inserted = _insert_rows(cursor, _TABLE, _COLUMNS, _KEY, rows)
        for kind, group, _ in scopes:
            actual = _query_scope(cursor, group[0])
            mismatches = (
                [
                    [
                        column
                        for column, expected_value, persisted_value in zip(
                            _COLUMNS, expected, persisted, strict=True
                        )
                        if _normalize_value(expected_value)
                        != _normalize_value(persisted_value)
                    ]
                    for expected, persisted in zip(group, actual, strict=True)
                    if not _row_matches(expected, persisted)
                ]
                if len(actual) == len(group)
                else []
            )
            if len(actual) != len(group) or mismatches:
                raise SectorPulseTransitionPgError(
                    "PULSE_TRANSITION_READBACK_MISMATCH",
                    f"{kind}变化事实事务内读回不匹配；expected={len(group)} actual={len(actual)} inserted={inserted} fields={mismatches[:3]}",
                )
    already_present = all(scope[2] for scope in scopes)
    return {
        "status": "IDEMPOTENT_ALREADY_PRESENT"
        if already_present
        else "WRITTEN_AND_TYPED_READBACK_VERIFIED",
        "rows_expected": {kind: len(group) for kind, group, _ in scopes},
        "rows_inserted": 0 if already_present else inserted,
        "same_input_retry": "ZERO_INSERTS_TYPED_EQUALITY_VERIFIED"
        if already_present
        else None,
        "transaction": "SERIALIZABLE_ATOMIC_INDUSTRY_AND_CONCEPT_TRANSITIONS",
        "transaction_isolation": isolation,
        "backend_pid": int(backend_pid),
        "database_write_attempted": not already_present,
        "ddl_executed": False,
        "dml_executed": not already_present,
    }


def persist_source_sector_pulse_transitions(
    config: Mapping[str, Any],
    *,
    previous_result: Mapping[str, Any],
    target_result: Mapping[str, Any],
    calendar_identity: Mapping[str, str],
) -> dict[str, Any]:
    """原子保存两端完整行业与GN PULSE变化；拒绝错误身份或部分分区。"""
    if not isinstance(previous_result, Mapping) or not isinstance(
        target_result, Mapping
    ):
        raise SectorPulseTransitionPgError(
            "PULSE_TRANSITION_ARGUMENT_INVALID", "变化事实必须来自两个完整PULSE读取结果"
        )
    rows, expected_by_kind = _expected_rows(
        previous_result=previous_result,
        target_result=target_result,
        calendar_identity=calendar_identity,
    )
    for attempt in range(5):
        try:
            return _persist_once(
                config,
                rows=rows,
                expected_by_kind=expected_by_kind,
                previous_result=previous_result,
                target_result=target_result,
            )
        except source_raw.RawSourcePgError as error:
            if getattr(error.__cause__, "sqlstate", None) != "40001" or attempt == 4:
                raise
            time.sleep(0.1 * (2**attempt))
    raise AssertionError("不可达的相邻PULSE写入重试状态")


def read_source_sector_pulse_transitions(
    config: Mapping[str, Any],
    *,
    previous_result: Mapping[str, Any],
    target_result: Mapping[str, Any],
    calendar_identity: Mapping[str, str],
) -> dict[str, Any]:
    """按两端精确来源修订与共同快照批量只读完整相邻变化事实。"""
    unavailable_groups: dict[str, dict[str, Any]] = {}
    for kind in _KINDS:
        prior = previous_result.get("by_kind", {}).get(kind, {})
        target = target_result.get("by_kind", {}).get(kind, {})
        if (
            isinstance(prior, Mapping)
            and prior.get("status") == "AVAILABLE"
            and isinstance(target, Mapping)
            and target.get("status") == "AVAILABLE"
        ):
            continue
        statuses = [
            item.get("status") if isinstance(item, Mapping) else "NOT_COMPUTED"
            for item in (prior, target)
        ]
        status = "INCOMPLETE" if "INCOMPLETE" in statuses else "NOT_COMPUTED"
        reasons = sorted(
            {
                str(reason)
                for item in (prior, target)
                if isinstance(item, Mapping)
                for reason in item.get("reason_codes", ())
            }
        )
        unavailable_groups[kind] = {
            "status": status,
            "reason_codes": reasons or ["PULSE_TRANSITION_INPUT_NOT_AVAILABLE"],
            "snapshot_id": (
                prior.get("snapshot_id") if isinstance(prior, Mapping) else None
            ),
            "facts": {},
        }
    if unavailable_groups:
        for kind in _KINDS:
            unavailable_groups.setdefault(
                kind,
                {
                    "status": "INCOMPLETE",
                    "reason_codes": ["PULSE_TRANSITION_PEER_PARTITION_NOT_COMPLETE"],
                    "snapshot_id": None,
                    "facts": {},
                },
            )
        return {
            "status": "INCOMPLETE"
            if any(
                item["status"] == "INCOMPLETE" for item in unavailable_groups.values()
            )
            else "NOT_COMPUTED",
            "by_kind": unavailable_groups,
            "database_write_attempted": False,
            "cross_period_recalculated": False,
        }
    try:
        previous = _day_identity(previous_result, "前一会话")
        target = _day_identity(target_result, "目标会话")
        if previous["namespace_id"] != target["namespace_id"]:
            raise SectorPulseTransitionPgError(
                "PULSE_TRANSITION_NAMESPACE_MISMATCH", "两端PG命名空间不一致"
            )
        if previous["target_session"] >= target["target_session"]:
            raise SectorPulseTransitionPgError(
                "PULSE_TRANSITION_SESSION_ORDER_INVALID", "相邻会话日期顺序无效"
            )
        calendar_sha256 = _sha(calendar_identity.get("calendar_sha256"), "官方日历SHA")
        calendar_id = calendar_identity.get("calendar_id")
        calendar_revision = calendar_identity.get("calendar_revision")
        if not all(
            isinstance(value, str) and value
            for value in (calendar_id, calendar_revision)
        ):
            raise SectorPulseTransitionPgError(
                "PULSE_TRANSITION_CALENDAR_INVALID", "官方日历身份不完整"
            )
        groups: dict[str, dict[str, Any]] = {}
        with source_raw._transaction(
            config, write=False, statement_timeout_ms=15000
        ) as (_, cursor, connected_namespace):
            if connected_namespace != previous["namespace_id"]:
                raise SectorPulseTransitionPgError(
                    "PULSE_TRANSITION_NAMESPACE_MISMATCH", "变化读取namespace不一致"
                )
            for kind in _KINDS:
                previous_group = _day_group(previous_result, kind)
                target_group = _day_group(target_result, kind)
                previous_snapshot = _uuid(
                    previous_group.get("snapshot_id"), f"前一会话{kind}快照"
                )
                target_snapshot = _uuid(
                    target_group.get("snapshot_id"), f"目标会话{kind}快照"
                )
                previous_facts = previous_group.get("facts")
                target_facts = target_group.get("facts")
                previous_ids = (
                    set(previous_facts)
                    if isinstance(previous_facts, Mapping)
                    else set()
                )
                target_ids = (
                    set(target_facts) if isinstance(target_facts, Mapping) else set()
                )
                count = previous_group.get("expected_board_count")
                if (
                    previous_snapshot != target_snapshot
                    or type(count) is not int
                    or count < 1
                    or count != target_group.get("expected_board_count")
                    or previous_group.get("read_board_count") != count
                    or target_group.get("read_board_count") != count
                    or len(previous_ids) != count
                    or previous_ids != target_ids
                ):
                    groups[kind] = {
                        "status": "INCOMPLETE",
                        "reason_codes": [
                            "PULSE_TRANSITION_INPUT_PARTITION_NOT_COMPARABLE"
                        ],
                        "snapshot_id": previous_snapshot,
                        "expected_board_count": count,
                        "facts": {},
                    }
                    continue
                cursor.execute(
                    f"SELECT {', '.join(_COLUMNS)} FROM {_TABLE} WHERE "
                    "namespace_id=%s::uuid AND previous_session=%s AND target_session=%s "
                    "AND previous_package_sha256=%s AND previous_source_capture_id=%s::uuid "
                    "AND previous_source_content_id=%s AND previous_source_raw_digest=%s "
                    "AND previous_profile_revision=%s AND target_package_sha256=%s "
                    "AND target_source_capture_id=%s::uuid AND target_source_content_id=%s "
                    "AND target_source_raw_digest=%s AND target_profile_revision=%s "
                    "AND snapshot_id=%s::uuid AND board_kind=%s "
                    "AND pulse_algorithm_contract=%s AND transition_contract=%s "
                    "AND calendar_revision=%s AND calendar_sha256=%s AND calendar_id=%s "
                    'ORDER BY board_native_id COLLATE "C"',
                    (
                        previous["namespace_id"],
                        previous["target_session"],
                        target["target_session"],
                        previous["package_sha256"],
                        previous["source_capture_id"],
                        previous["source_content_id"],
                        previous["source_raw_digest"],
                        previous["profile_revision"],
                        target["package_sha256"],
                        target["source_capture_id"],
                        target["source_content_id"],
                        target["source_raw_digest"],
                        target["profile_revision"],
                        previous_snapshot,
                        kind,
                        _PULSE_ALGORITHM,
                        _TRANSITION_CONTRACT,
                        calendar_revision,
                        calendar_sha256,
                        calendar_id,
                    ),
                )
                actual = cursor.fetchall()
                if not actual:
                    cursor.execute(
                        f"SELECT count(*)::bigint FROM {_TABLE} WHERE "
                        "namespace_id=%s::uuid AND previous_session=%s AND target_session=%s "
                        "AND snapshot_id=%s::uuid AND board_kind=%s AND transition_contract=%s",
                        (
                            previous["namespace_id"],
                            previous["target_session"],
                            target["target_session"],
                            previous_snapshot,
                            kind,
                            _TRANSITION_CONTRACT,
                        ),
                    )
                    stale = int(cursor.fetchone()[0]) > 0
                    groups[kind] = {
                        "status": "NOT_COMPUTED",
                        "reason_codes": [
                            "PULSE_TRANSITION_SOURCE_REVISION_MISMATCH"
                            if stale
                            else "PULSE_TRANSITION_FACTS_NOT_SAVED_FOR_EXACT_INPUT"
                        ],
                        "snapshot_id": previous_snapshot,
                        "facts": {},
                    }
                    continue
                if len(actual) != count:
                    groups[kind] = {
                        "status": "INCOMPLETE",
                        "reason_codes": ["PULSE_TRANSITION_BOARD_PARTITION_NOT_CLOSED"],
                        "snapshot_id": previous_snapshot,
                        "expected_board_count": count,
                        "read_board_count": len(actual),
                        "facts": {},
                    }
                    continue
                facts: dict[str, dict[str, Any]] = {}
                for row in actual:
                    stored = dict(zip(_COLUMNS, row, strict=True))
                    board_id = stored["board_native_id"]
                    previous_fact = previous_facts.get(board_id)
                    target_fact = target_facts.get(board_id)
                    if (
                        not isinstance(previous_fact, Mapping)
                        or not isinstance(target_fact, Mapping)
                        or stored["previous_pulse_state"] != previous_fact.get("state")
                        or stored["target_pulse_state"] != target_fact.get("state")
                        or list(stored["previous_reason_codes"])
                        != list(previous_fact.get("reason_codes", []))
                        or list(stored["target_reason_codes"])
                        != list(target_fact.get("reason_codes", []))
                        or stored["calculation_status"] != "COMPLETE"
                        or stored["formal_rotation_state"] != "NOT_EVALUATED"
                    ):
                        raise SectorPulseTransitionPgError(
                            "PULSE_TRANSITION_TYPED_CONTENT_MISMATCH",
                            f"{kind}已存变化来源键或两端状态与PULSE事实不一致",
                        )
                    facts[board_id] = {
                        "status": "AVAILABLE",
                        "previous_state": stored["previous_pulse_state"],
                        "target_state": stored["target_pulse_state"],
                        "transition_state": stored["transition_state"],
                        "previous_reason_codes": list(stored["previous_reason_codes"]),
                        "target_reason_codes": list(stored["target_reason_codes"]),
                        "snapshot_id": str(stored["snapshot_id"]),
                        "formal_rotation_state": stored["formal_rotation_state"],
                        "research_mode": stored["research_mode"],
                        "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
                        "price_coordinate": stored["price_coordinate"],
                    }
                groups[kind] = {
                    "status": "AVAILABLE",
                    "reason_codes": [],
                    "snapshot_id": previous_snapshot,
                    "expected_board_count": count,
                    "read_board_count": len(actual),
                    "facts": facts,
                }
    except (SectorPulseTransitionPgError, source_raw.RawSourcePgError) as error:
        return {
            "status": "READ_ERROR",
            "reason_codes": [getattr(error, "code", "PULSE_TRANSITION_PG_READ_FAILED")],
            "by_kind": groups,
            "database_write_attempted": False,
            "cross_period_recalculated": False,
        }
    statuses = [groups[kind]["status"] for kind in _KINDS]
    overall = (
        "AVAILABLE"
        if all(status == "AVAILABLE" for status in statuses)
        else "INCOMPLETE"
        if any(status == "INCOMPLETE" for status in statuses)
        else "NOT_COMPUTED"
    )
    return {
        "status": overall,
        "previous_session": previous["target_session"].isoformat(),
        "target_session": target["target_session"].isoformat(),
        "previous_profile_revision": previous["profile_revision"],
        "target_profile_revision": target["profile_revision"],
        "calendar_identity": {
            "calendar_id": calendar_id,
            "calendar_revision": calendar_revision,
            "calendar_sha256": calendar_sha256,
        },
        "by_kind": groups,
        "database_write_attempted": False,
        "cross_period_recalculated": False,
    }


__all__ = [
    "SectorPulseTransitionPgError",
    "persist_source_sector_pulse_transitions",
    "read_source_sector_pulse_transitions",
]
