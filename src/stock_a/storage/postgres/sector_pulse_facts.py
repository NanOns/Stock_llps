"""板块跨日PULSE typed事实的PG原子写入与只读批量读取。"""

from __future__ import annotations

from datetime import date
from decimal import Decimal, ROUND_HALF_UP, localcontext
from fractions import Fraction
import re
import time
from typing import Any, Mapping, Sequence
from uuid import UUID

from stock_a.storage.postgres import source_raw
from stock_a.storage.postgres.research_day_facts import _insert_rows, _row_matches

_TABLE = "stock.source_sector_cross_period_pulse_facts"
_ALGORITHM = "SOURCE_CROSS_PERIOD_PULSE_R1"
_SHA = re.compile(r"^[0-9a-f]{64}$")
_KINDS = ("INDUSTRY", "CONCEPT")
_COLUMNS = (
    "namespace_id",
    "target_session",
    "package_sha256",
    "source_capture_id",
    "source_content_id",
    "source_raw_digest",
    "profile_revision",
    "snapshot_id",
    "board_kind",
    "board_native_id",
    "board_name",
    "algorithm_contract",
    "prior_ret5_session",
    "prior_breadth_session",
    "source_pool_count",
    "snapshot_board_count",
    "snapshot_source_quality",
    "member_count",
    "mapped_count",
    "unmapped_count",
    "target_quote_count",
    "prior_ret5_quote_count",
    "prior_breadth_quote_count",
    "ret5_target_numerator",
    "ret5_target_denominator",
    "ret5_prior_numerator",
    "ret5_prior_denominator",
    "ret5_target_percentile_numerator",
    "ret5_target_percentile_denominator",
    "ret5_prior_percentile_numerator",
    "ret5_prior_percentile_denominator",
    "dq5_pp_numerator",
    "dq5_pp_denominator",
    "ret5_common_count",
    "ret5_denominator",
    "ret5_target_known_count",
    "ret5_prior_known_count",
    "ret5_target_peer_count",
    "ret5_prior_peer_count",
    "ret5_reason_codes",
    "breadth_target_numerator",
    "breadth_target_denominator",
    "breadth_prior_numerator",
    "breadth_prior_denominator",
    "breadth_delta1_numerator",
    "breadth_delta1_denominator",
    "breadth_common_count",
    "breadth_denominator",
    "breadth_target_known_count",
    "breadth_prior_known_count",
    "breadth_reason_codes",
    "pulse_state",
    "pulse_reason_codes",
    "calculation_status",
    "research_mode",
    "membership_mode",
    "price_coordinate",
    "source_time_quality",
    "source_clock_role",
    "source_available_at",
    "formal_rotation_state",
)
_KEY = (
    "namespace_id",
    "target_session",
    "package_sha256",
    "source_capture_id",
    "source_content_id",
    "source_raw_digest",
    "profile_revision",
    "snapshot_id",
    "board_kind",
    "board_native_id",
    "algorithm_contract",
)


class SectorPulsePgError(ValueError):
    """板块脉冲事实的稳定PG读写错误。"""

    def __init__(self, code: str, message: str):
        self.code = code
        self.preserve_transaction_error = True
        super().__init__(message)


def _parts(value: Any) -> tuple[int | None, int | None]:
    if value is None:
        return None, None
    fraction = value if isinstance(value, Fraction) else Fraction(str(value))
    return fraction.numerator, fraction.denominator


def _codes(values: Any) -> list[str]:
    if not isinstance(values, (list, tuple)):
        return []
    return sorted({item for item in values if isinstance(item, str) and item})


def _unknown_reasons(
    metric: Mapping[str, Any],
    endpoints: Mapping[str, Any],
    *,
    comparison_endpoint: str,
    label: str,
) -> list[str]:
    if metric.get("delta") is not None:
        return []
    denominator = int(metric.get("denominator") or 0)
    common_count = int(metric.get("common_count") or 0)
    reasons: set[str] = set()
    if denominator < 5:
        reasons.add("BOARD_MEMBER_COUNT_BELOW_FIVE")
    if common_count < 5:
        reasons.add("COMMON_MEMBER_COUNT_BELOW_FIVE")
    if denominator > 0 and 5 * common_count < 4 * denominator:
        reasons.add("COMMON_MEMBER_COVERAGE_BELOW_80_PERCENT")
    for endpoint_name, endpoint_label in (
        ("target", "TARGET"),
        (comparison_endpoint, "COMPARISON"),
    ):
        endpoint = endpoints.get(endpoint_name, {})
        quote_count = int(endpoint.get("actual_quote_count") or 0)
        if quote_count < 5:
            reasons.add(f"{endpoint_label}_QUOTE_COUNT_BELOW_FIVE")
        if denominator > 0 and 5 * quote_count < 4 * denominator:
            reasons.add(f"{endpoint_label}_QUOTE_COVERAGE_BELOW_80_PERCENT")
    if metric.get("target") is None:
        reasons.add(f"{label}_TARGET_VALUE_UNKNOWN")
    if metric.get("prior") is None:
        reasons.add(f"{label}_COMPARISON_VALUE_UNKNOWN")
    return sorted(reasons)


def _pulse_rows(
    *,
    target_session: date,
    package_sha256: str,
    source_version: Mapping[str, Any],
    profile_revision: str,
    target_result: Mapping[str, Any],
) -> tuple[tuple[Any, ...], ...]:
    namespace_id = str(source_version.get("namespace_id") or "")
    capture_id = str(source_version.get("source_capture_id") or "")
    content_id = str(source_version.get("source_content_id") or "")
    raw_digest = str(source_version.get("capture_digest") or "")
    source_count = source_version.get("raw_row_count")
    if (
        not namespace_id
        or not capture_id
        or not _SHA.fullmatch(content_id)
        or not _SHA.fullmatch(raw_digest)
        or not _SHA.fullmatch(profile_revision)
        or type(source_count) is not int
        or source_count < 1
        or source_version.get("package_sha256") != package_sha256
        or target_result.get("namespace_id") != namespace_id
        or target_result.get("package_sha256") != package_sha256
        or target_result.get("target_session") != target_session.isoformat()
        or target_result.get("membership_mode")
        != "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT"
    ):
        raise SectorPulsePgError(
            "SECTOR_PULSE_SOURCE_BINDING_INVALID", "脉冲事实缺少唯一来源或后验快照绑定"
        )
    snapshots = target_result.get("snapshots_by_kind")
    names = target_result.get("board_names_by_kind")
    all_results = target_result.get("peer_target_by_kind")
    comparison_sessions = target_result.get("comparison_sessions")
    if (
        not isinstance(snapshots, Mapping)
        or set(snapshots) != set(_KINDS)
        or not isinstance(names, Mapping)
        or not isinstance(all_results, Mapping)
        or set(all_results) != set(_KINDS)
        or not isinstance(comparison_sessions, Mapping)
        or not isinstance(comparison_sessions.get("prior_ret5"), date)
        or not isinstance(comparison_sessions.get("prior_breadth"), date)
    ):
        raise SectorPulsePgError(
            "SECTOR_PULSE_PEER_SET_INCOMPLETE", "行业与GN同类板块全集或会话端点未闭合"
        )
    raw_rows: list[tuple[Any, ...]] = []
    for kind in _KINDS:
        snapshot = snapshots[kind]
        kind_results = all_results[kind]
        kind_names = names.get(kind)
        snapshot_id = snapshot.get("snapshot_id")
        source_quality = snapshot.get("source_quality")
        if (
            not isinstance(snapshot_id, str)
            or not isinstance(source_quality, str)
            or not source_quality
            or snapshot.get("research_mode") != "NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT"
            or not isinstance(kind_results, Mapping)
            or not kind_results
            or not isinstance(kind_names, Mapping)
            or set(kind_results) != set(kind_names)
        ):
            raise SectorPulsePgError(
                "SECTOR_PULSE_PEER_SET_INCOMPLETE", f"{kind}同行目录或来源质量未闭合"
            )
        snapshot_board_count = len(kind_results)
        for board_id in sorted(kind_results):
            result = kind_results[board_id]
            if not isinstance(result, Mapping):
                raise SectorPulsePgError(
                    "SECTOR_PULSE_BOARD_RESULT_INVALID", "板块算法结果结构无效"
                )
            ret5 = result.get("ret5")
            breadth = result.get("ret1_delta1")
            rotation = result.get("rotation")
            endpoint_facts = result.get("endpoints")
            if not all(
                isinstance(item, Mapping)
                for item in (ret5, breadth, rotation, endpoint_facts)
            ):
                raise SectorPulsePgError(
                    "SECTOR_PULSE_BOARD_RESULT_INVALID",
                    "板块脉冲缺少跨日指标或端点质量",
                )
            pulse = rotation.get("pulse")
            if not isinstance(pulse, Mapping) or pulse.get("state") not in {
                "TRUE",
                "FALSE",
                "UNKNOWN",
            }:
                raise SectorPulsePgError(
                    "SECTOR_PULSE_STATE_INVALID", "现役板块算法没有返回合法PULSE三态"
                )
            target_quotes = int(endpoint_facts["target"]["actual_quote_count"])
            prior_ret5_quotes = int(
                endpoint_facts["three_official_sessions_before"]["actual_quote_count"]
            )
            prior_breadth_quotes = int(
                endpoint_facts["previous_official_session"]["actual_quote_count"]
            )
            ret5_reasons = _unknown_reasons(
                ret5,
                endpoint_facts,
                comparison_endpoint="three_official_sessions_before",
                label="RET5",
            )
            if ret5.get("target_percentile") is None and ret5.get("target") is not None:
                if int(ret5.get("target_peer_count") or 0) < 2:
                    ret5_reasons.append("TARGET_PEER_BOARD_COUNT_BELOW_TWO")
            if ret5.get("prior_percentile") is None and ret5.get("prior") is not None:
                if int(ret5.get("prior_peer_count") or 0) < 2:
                    ret5_reasons.append("COMPARISON_PEER_BOARD_COUNT_BELOW_TWO")
            if rotation.get("dq5_pp") is None and not ret5_reasons:
                ret5_reasons.append("DQ5_INPUT_UNKNOWN")
            breadth_reasons = _unknown_reasons(
                breadth,
                endpoint_facts,
                comparison_endpoint="previous_official_session",
                label="BREADTH_DELTA1",
            )
            if breadth.get("delta") is None and not breadth_reasons:
                breadth_reasons.append("BREADTH_DELTA1_INPUT_UNKNOWN")
            ret5_reasons = sorted(set(ret5_reasons))
            breadth_reasons = sorted(set(breadth_reasons))
            pulse_reasons = _codes(pulse.get("reasons"))
            if pulse.get("state") == "UNKNOWN":
                pulse_reasons = sorted(
                    set(pulse_reasons + ret5_reasons + breadth_reasons)
                )
            ret5_target = _parts(ret5.get("target"))
            ret5_prior = _parts(ret5.get("prior"))
            ret5_target_rank = _parts(ret5.get("target_percentile"))
            ret5_prior_rank = _parts(ret5.get("prior_percentile"))
            dq5 = _parts(rotation.get("dq5_pp"))
            breadth_target = _parts(breadth.get("target"))
            breadth_prior = _parts(breadth.get("prior"))
            breadth_delta = _parts(rotation.get("breadth_delta1"))
            mapped_count = int(result.get("mapped_count") or 0)
            member_count = int(result.get("member_count") or 0)
            unmapped_count = int(result.get("unmapped_count") or 0)
            values = {
                "namespace_id": namespace_id,
                "target_session": target_session,
                "package_sha256": package_sha256,
                "source_capture_id": capture_id,
                "source_content_id": content_id,
                "source_raw_digest": raw_digest,
                "profile_revision": profile_revision,
                "snapshot_id": snapshot_id,
                "board_kind": kind,
                "board_native_id": board_id,
                "board_name": kind_names[board_id],
                "algorithm_contract": _ALGORITHM,
                "prior_ret5_session": comparison_sessions["prior_ret5"],
                "prior_breadth_session": comparison_sessions["prior_breadth"],
                "source_pool_count": source_count,
                "snapshot_board_count": snapshot_board_count,
                "snapshot_source_quality": source_quality,
                "member_count": member_count,
                "mapped_count": mapped_count,
                "unmapped_count": unmapped_count,
                "target_quote_count": target_quotes,
                "prior_ret5_quote_count": prior_ret5_quotes,
                "prior_breadth_quote_count": prior_breadth_quotes,
                "ret5_target_numerator": ret5_target[0],
                "ret5_target_denominator": ret5_target[1],
                "ret5_prior_numerator": ret5_prior[0],
                "ret5_prior_denominator": ret5_prior[1],
                "ret5_target_percentile_numerator": ret5_target_rank[0],
                "ret5_target_percentile_denominator": ret5_target_rank[1],
                "ret5_prior_percentile_numerator": ret5_prior_rank[0],
                "ret5_prior_percentile_denominator": ret5_prior_rank[1],
                "dq5_pp_numerator": dq5[0],
                "dq5_pp_denominator": dq5[1],
                "ret5_common_count": int(ret5.get("common_count") or 0),
                "ret5_denominator": int(ret5.get("denominator") or 0),
                "ret5_target_known_count": int(ret5.get("target_known") or 0),
                "ret5_prior_known_count": int(ret5.get("prior_known") or 0),
                "ret5_target_peer_count": int(ret5.get("target_peer_count") or 0),
                "ret5_prior_peer_count": int(ret5.get("prior_peer_count") or 0),
                "ret5_reason_codes": ret5_reasons,
                "breadth_target_numerator": breadth_target[0],
                "breadth_target_denominator": breadth_target[1],
                "breadth_prior_numerator": breadth_prior[0],
                "breadth_prior_denominator": breadth_prior[1],
                "breadth_delta1_numerator": breadth_delta[0],
                "breadth_delta1_denominator": breadth_delta[1],
                "breadth_common_count": int(breadth.get("common_count") or 0),
                "breadth_denominator": int(breadth.get("denominator") or 0),
                "breadth_target_known_count": int(breadth.get("target_known") or 0),
                "breadth_prior_known_count": int(breadth.get("prior_known") or 0),
                "breadth_reason_codes": breadth_reasons,
                "pulse_state": pulse["state"],
                "pulse_reason_codes": pulse_reasons,
                "calculation_status": "COMPLETE",
                "research_mode": "NON_PIT_RETROSPECTIVE",
                "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
                "price_coordinate": "RAW_UNADJUSTED",
                "source_time_quality": source_version.get("source_time_quality"),
                "source_clock_role": source_version.get("source_clock_role"),
                "source_available_at": source_version.get("source_available_at"),
                "formal_rotation_state": "NOT_EVALUATED",
            }
            if mapped_count + unmapped_count != member_count:
                raise SectorPulsePgError(
                    "SECTOR_PULSE_MEMBER_DENOMINATOR_INVALID",
                    "板块成员映射计数与完整成员分母不闭合",
                )
            raw_rows.append(tuple(values[name] for name in _COLUMNS))
    if not raw_rows:
        raise SectorPulsePgError(
            "SECTOR_PULSE_EMPTY_PEER_SET", "行业与GN同行目录没有可保存板块"
        )
    return tuple(raw_rows)


def _validate_write_scope(
    cursor: Any,
    *,
    namespace_id: str,
    target_session: date,
    package_sha256: str,
    source_version: Mapping[str, Any],
    profile_revision: str,
    rows: Sequence[Sequence[Any]],
    target_result: Mapping[str, Any],
) -> None:
    if namespace_id != source_version.get("namespace_id"):
        raise SectorPulsePgError(
            "SECTOR_PULSE_NAMESPACE_MISMATCH", "PG连接namespace与离线研究来源不一致"
        )
    cursor.execute(
        "SELECT capture_id::text, content_id, canonical_digest, normalized_count, "
        "source_time_quality, source_clock_role, source_available_at "
        "FROM stock.source_captures WHERE namespace_id=%s::uuid AND capture_id=%s::uuid",
        (namespace_id, source_version["source_capture_id"]),
    )
    capture = cursor.fetchone()
    if capture is None or (
        str(capture[0]) != source_version["source_capture_id"]
        or str(capture[1]).strip() != source_version["source_content_id"]
        or str(capture[2]).strip() != source_version["capture_digest"]
        or int(capture[3]) != source_version["capture_expected_row_count"]
        or capture[4] != source_version["source_time_quality"]
        or capture[5] != source_version["source_clock_role"]
        or source_raw._db_scalar(capture[6])
        != source_version.get("source_available_at")
    ):
        raise SectorPulsePgError(
            "SECTOR_PULSE_CAPTURE_REVISION_MISMATCH", "目标Raw捕获身份或时钟修订已变化"
        )
    cursor.execute(
        "SELECT count(*)::bigint, count(DISTINCT source_symbol)::bigint, "
        "count(DISTINCT source_capture_id)::bigint, count(DISTINCT source_content_id)::bigint "
        "FROM stock.source_raw_daily WHERE namespace_id=%s::uuid AND session_date=%s "
        "AND package_sha256=%s",
        (namespace_id, target_session, package_sha256),
    )
    raw_count, unique_symbols, capture_count, content_count = cursor.fetchone()
    expected_count = int(source_version["raw_row_count"])
    if (
        int(raw_count),
        int(unique_symbols),
        int(capture_count),
        int(content_count),
    ) != (
        expected_count,
        expected_count,
        1,
        1,
    ):
        raise SectorPulsePgError(
            "SECTOR_PULSE_RAW_PARTITION_OPEN", "目标Raw来源池分区已变化或未完整闭合"
        )
    cursor.execute(
        "SELECT count(*)::bigint, count(DISTINCT source_symbol)::bigint, "
        "count(DISTINCT profile_revision)::bigint FROM stock.source_pool_seed_day_facts "
        "WHERE namespace_id=%s::uuid AND target_session=%s AND package_sha256=%s "
        "AND source_capture_id=%s::uuid AND source_content_id=%s AND profile_revision=%s",
        (
            namespace_id,
            target_session,
            package_sha256,
            source_version["source_capture_id"],
            source_version["source_content_id"],
            profile_revision,
        ),
    )
    seed_count, seed_symbols, seed_revisions = cursor.fetchone()
    if (int(seed_count), int(seed_symbols), int(seed_revisions)) != (
        expected_count,
        expected_count,
        1,
    ):
        raise SectorPulsePgError(
            "SECTOR_PULSE_PROFILE_PARTITION_OPEN",
            "目标画像修订没有闭合到唯一完整Seed来源池",
        )
    snapshots = target_result.get("snapshots_by_kind")
    all_results = target_result.get("peer_target_by_kind")
    names = target_result.get("board_names_by_kind")
    for kind in _KINDS:
        snapshot = snapshots[kind]
        snapshot_id = snapshot["snapshot_id"]
        expected_source_kind = (
            "TDX_TDXHY_CFG" if kind == "INDUSTRY" else "TDX_INFOHARBOR_BLOCK_DAT"
        )
        cursor.execute(
            "SELECT source_kind, research_mode, source_quality FROM "
            "stock.tdx_sector_source_snapshots WHERE snapshot_id=%s::uuid",
            (snapshot_id,),
        )
        actual_snapshot = cursor.fetchone()
        if actual_snapshot != (
            expected_source_kind,
            "NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT",
            snapshot.get("source_quality"),
        ):
            raise SectorPulsePgError(
                "SECTOR_PULSE_SNAPSHOT_REVISION_MISMATCH",
                f"{kind}成员快照身份或质量已变化",
            )
        cursor.execute(
            "SELECT c.board_native_id, c.board_name, count(m.source_symbol)::integer "
            "FROM stock.tdx_sector_catalog c LEFT JOIN stock.tdx_sector_members m "
            "ON m.snapshot_id=c.snapshot_id AND m.board_kind=c.board_kind "
            "AND m.board_native_id=c.board_native_id "
            "WHERE c.snapshot_id=%s::uuid AND c.board_kind=%s "
            'GROUP BY c.board_native_id, c.board_name ORDER BY c.board_native_id COLLATE "C"',
            (snapshot_id, kind),
        )
        actual_boards = cursor.fetchall()
        expected_boards = all_results[kind]
        expected_names = names[kind]
        if len(actual_boards) != len(expected_boards):
            raise SectorPulsePgError(
                "SECTOR_PULSE_SNAPSHOT_PARTITION_OPEN", f"{kind}完整板块目录行数不匹配"
            )
        for board_id, board_name, member_count in actual_boards:
            actual = expected_boards.get(board_id)
            if (
                actual is None
                or expected_names.get(board_id) != board_name
                or int(actual.get("member_count", -1)) != int(member_count)
            ):
                raise SectorPulsePgError(
                    "SECTOR_PULSE_SNAPSHOT_MEMBER_MISMATCH",
                    f"{kind}板块名称或成员快照与生产结果不匹配",
                )
    if any(len(row) != len(_COLUMNS) for row in rows):
        raise SectorPulsePgError(
            "SECTOR_PULSE_TYPED_ROW_INVALID", "脉冲typed行列数不符合现役schema"
        )


def _persist_once(
    config: Mapping[str, Any],
    *,
    target_session: date,
    package_sha256: str,
    source_version: Mapping[str, Any],
    profile_revision: str,
    target_result: Mapping[str, Any],
    rows: Sequence[Sequence[Any]],
) -> dict[str, Any]:
    inserted: dict[str, int] = {}
    scopes = []
    with source_raw._transaction(config, write=True, statement_timeout_ms=30000) as (
        _,
        cursor,
        namespace_id,
    ):
        _validate_write_scope(
            cursor,
            namespace_id=namespace_id,
            target_session=target_session,
            package_sha256=package_sha256,
            source_version=source_version,
            profile_revision=profile_revision,
            rows=rows,
            target_result=target_result,
        )
        cursor.execute("SELECT to_regclass(%s) IS NOT NULL", (_TABLE,))
        if cursor.fetchone() != (True,):
            raise SectorPulsePgError(
                "SECTOR_PULSE_MIGRATION_MISSING", "0019板块脉冲事实迁移尚未应用"
            )
        for kind in _KINDS:
            group = [row for row in rows if row[_COLUMNS.index("board_kind")] == kind]
            group = sorted(
                group,
                key=lambda row: (
                    row[_COLUMNS.index("snapshot_id")],
                    row[_COLUMNS.index("board_native_id")],
                ),
            )
            if not group:
                raise SectorPulsePgError(
                    "SECTOR_PULSE_PEER_SET_EMPTY", f"{kind}没有板块事实，拒绝部分写入"
                )
            predicate = (
                "namespace_id=%s::uuid AND target_session=%s AND package_sha256=%s "
            )
            predicate += "AND source_capture_id=%s::uuid AND source_content_id=%s "
            predicate += "AND source_raw_digest=%s AND profile_revision=%s "
            predicate += (
                "AND snapshot_id=%s::uuid AND board_kind=%s AND algorithm_contract=%s"
            )
            first = group[0]
            params = (
                namespace_id,
                target_session,
                package_sha256,
                source_version["source_capture_id"],
                source_version["source_content_id"],
                source_version["capture_digest"],
                profile_revision,
                first[_COLUMNS.index("snapshot_id")],
                kind,
                _ALGORITHM,
            )
            cursor.execute(
                f"SELECT {', '.join(_COLUMNS)} FROM {_TABLE} WHERE {predicate} "
                'ORDER BY board_native_id COLLATE "C"',
                params,
            )
            existing = cursor.fetchall()
            if existing and (
                len(existing) != len(group)
                or any(
                    not _row_matches(expected, actual)
                    for expected, actual in zip(group, existing, strict=True)
                )
            ):
                raise SectorPulsePgError(
                    "SECTOR_PULSE_CONTENT_CONFLICT",
                    "相同板块脉冲业务键已有异内容或不完整行，事务回滚",
                )
            scopes.append((kind, group, len(existing) == len(group)))
        if any(scope[2] for scope in scopes) and not all(scope[2] for scope in scopes):
            raise SectorPulsePgError(
                "SECTOR_PULSE_PARTIAL_SCOPE",
                "行业和GN完整同行快照未处于同一原子业务批次，拒绝补写",
            )
        if not all(scope[2] for scope in scopes):
            inserted[_TABLE.rsplit(".", 1)[1]] = _insert_rows(
                cursor, _TABLE, _COLUMNS, _KEY, rows
            )
        for kind, group, _ in scopes:
            first = group[0]
            cursor.execute(
                f"SELECT {', '.join(_COLUMNS)} FROM {_TABLE} WHERE "
                "namespace_id=%s::uuid AND target_session=%s AND package_sha256=%s "
                "AND source_capture_id=%s::uuid AND source_content_id=%s "
                "AND source_raw_digest=%s AND profile_revision=%s AND snapshot_id=%s::uuid "
                'AND board_kind=%s AND algorithm_contract=%s ORDER BY board_native_id COLLATE "C"',
                (
                    namespace_id,
                    target_session,
                    package_sha256,
                    source_version["source_capture_id"],
                    source_version["source_content_id"],
                    source_version["capture_digest"],
                    profile_revision,
                    first[_COLUMNS.index("snapshot_id")],
                    kind,
                    _ALGORITHM,
                ),
            )
            persisted = cursor.fetchall()
            if len(persisted) != len(group) or any(
                not _row_matches(expected, actual)
                for expected, actual in zip(group, persisted, strict=True)
            ):
                raise SectorPulsePgError(
                    "SECTOR_PULSE_READBACK_MISMATCH",
                    "同一事务内板块脉冲typed事实读回不匹配",
                )
    already_present = all(scope[2] for scope in scopes)
    return {
        "status": "IDEMPOTENT_ALREADY_PRESENT"
        if already_present
        else "WRITTEN_AND_TYPED_READBACK_VERIFIED",
        "rows_expected": {kind: len(group) for kind, group, _ in scopes},
        "rows_inserted": inserted,
        "same_input_retry": "ZERO_INSERTS_TYPED_EQUALITY_VERIFIED"
        if already_present
        else None,
        "transaction": "SERIALIZABLE_ATOMIC_INDUSTRY_AND_CONCEPT_BOARD_SNAPSHOTS",
        "database_write_attempted": not already_present,
        "ddl_executed": False,
        "dml_executed": not already_present,
    }


def persist_source_sector_pulse_day(
    config: Mapping[str, Any],
    *,
    target_session: date,
    package_sha256: str,
    source_version: Mapping[str, Any],
    profile_revision: str,
    target_result: Mapping[str, Any],
) -> dict[str, Any]:
    """原子保存完整行业与GN同类板块集合；同键异内容拒绝回滚。"""
    if (
        type(target_session) is not date
        or not isinstance(package_sha256, str)
        or not _SHA.fullmatch(package_sha256)
        or not isinstance(source_version, Mapping)
        or not isinstance(target_result, Mapping)
    ):
        raise SectorPulsePgError(
            "SECTOR_PULSE_ARGUMENT_INVALID", "目标日期、来源版本或同行结果不合法"
        )
    rows = _pulse_rows(
        target_session=target_session,
        package_sha256=package_sha256,
        source_version=source_version,
        profile_revision=profile_revision,
        target_result=target_result,
    )
    for attempt in range(5):
        try:
            return _persist_once(
                config,
                target_session=target_session,
                package_sha256=package_sha256,
                source_version=source_version,
                profile_revision=profile_revision,
                target_result=target_result,
                rows=rows,
            )
        except source_raw.RawSourcePgError as error:
            if getattr(error.__cause__, "sqlstate", None) != "40001" or attempt == 4:
                raise
            time.sleep(0.1 * (2**attempt))
    raise AssertionError("不可达的板块脉冲事务重试状态")


def _fraction_payload(row: Mapping[str, Any], prefix: str) -> dict[str, Any] | None:
    numerator = row.get(f"{prefix}_numerator")
    denominator = row.get(f"{prefix}_denominator")
    if numerator is None or denominator is None:
        return None
    exact = Fraction(int(numerator), int(denominator))
    with localcontext() as context:
        context.prec = 40
        decimal_4dp = str(
            (Decimal(exact.numerator) / Decimal(exact.denominator)).quantize(
                Decimal("0.0001"), rounding=ROUND_HALF_UP
            )
        )
    return {
        "numerator": str(exact.numerator),
        "denominator": str(exact.denominator),
        "fraction": str(exact),
        "decimal_4dp": decimal_4dp,
    }


def _pulse_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "status": "AVAILABLE",
        "state": row["pulse_state"],
        "dq5_pp": _fraction_payload(row, "dq5_pp"),
        "breadth_delta1": _fraction_payload(row, "breadth_delta1"),
        "ret5_target": _fraction_payload(row, "ret5_target"),
        "ret5_prior": _fraction_payload(row, "ret5_prior"),
        "breadth_target": _fraction_payload(row, "breadth_target"),
        "breadth_prior": _fraction_payload(row, "breadth_prior"),
        "member_count": int(row["member_count"]),
        "mapped_count": int(row["mapped_count"]),
        "unmapped_count": int(row["unmapped_count"]),
        "ret5_common_count": int(row["ret5_common_count"]),
        "ret5_denominator": int(row["ret5_denominator"]),
        "ret5_target_peer_count": int(row["ret5_target_peer_count"]),
        "ret5_prior_peer_count": int(row["ret5_prior_peer_count"]),
        "breadth_common_count": int(row["breadth_common_count"]),
        "breadth_denominator": int(row["breadth_denominator"]),
        "ret5_reason_codes": list(row["ret5_reason_codes"]),
        "breadth_reason_codes": list(row["breadth_reason_codes"]),
        "reason_codes": list(row["pulse_reason_codes"]),
        "snapshot_source_quality": row["snapshot_source_quality"],
        "snapshot_id": str(row["snapshot_id"]),
        "profile_revision": str(row["profile_revision"]).strip(),
        "algorithm_contract": row["algorithm_contract"],
        "research_mode": row["research_mode"],
        "membership_mode": row["membership_mode"],
        "price_coordinate": row["price_coordinate"],
        "formal_rotation_state": row["formal_rotation_state"],
        "calculation_status": row["calculation_status"],
    }


def read_source_sector_pulse_day(
    config: Mapping[str, Any],
    *,
    target_session: date,
    package_sha256: str,
    namespace_id: str,
    source_capture_id: str,
    source_content_id: str,
    source_raw_digest: str,
    source_pool_count: int,
    snapshot_ids: Mapping[str, str | None],
) -> dict[str, Any]:
    """按精确Raw、Profile和板块快照版本批量只读已存PULSE事实。"""
    if (
        type(target_session) is not date
        or not _SHA.fullmatch(package_sha256 or "")
        or not _SHA.fullmatch(source_content_id or "")
        or not _SHA.fullmatch(source_raw_digest or "")
        or type(source_pool_count) is not int
        or source_pool_count < 1
        or set(snapshot_ids) != set(_KINDS)
    ):
        raise SectorPulsePgError(
            "SECTOR_PULSE_READ_ARGUMENT_INVALID",
            "PG脉冲读取必须绑定精确日期、来源和双快照",
        )
    try:
        namespace_id = str(UUID(namespace_id))
        source_capture_id = str(UUID(source_capture_id))
        normalized_snapshots = {
            kind: str(UUID(snapshot_ids[kind]))
            if snapshot_ids[kind] is not None
            else None
            for kind in _KINDS
        }
    except (ValueError, TypeError, AttributeError) as error:
        raise SectorPulsePgError(
            "SECTOR_PULSE_READ_UUID_INVALID",
            "PG脉冲读取的namespace、capture或snapshot UUID无效",
        ) from error
    active_snapshots = [
        normalized_snapshots[kind]
        for kind in _KINDS
        if normalized_snapshots[kind] is not None
    ]
    if len(set(active_snapshots)) != len(active_snapshots):
        raise SectorPulsePgError(
            "SECTOR_PULSE_READ_SNAPSHOTS_NOT_DISTINCT", "行业与GN概念必须绑定不同快照"
        )
    result_by_kind: dict[str, dict[str, Any]] = {
        kind: {
            "status": "NOT_COMPUTED",
            "reason_codes": ["PULSE_FACTS_NOT_SAVED_FOR_EXACT_INPUT"],
            "snapshot_id": normalized_snapshots[kind],
            "facts": {},
        }
        for kind in _KINDS
    }
    if not active_snapshots:
        return {
            "status": "NOT_COMPUTED",
            "profile_revision": None,
            "by_kind": result_by_kind,
            "database_write_attempted": False,
            "cross_period_recalculated": False,
        }
    with source_raw._transaction(config, write=False, statement_timeout_ms=15000) as (
        _,
        cursor,
        connected_namespace,
    ):
        if connected_namespace != namespace_id:
            raise SectorPulsePgError(
                "SECTOR_PULSE_READ_NAMESPACE_MISMATCH", "API脉冲读取namespace与PG不一致"
            )
        cursor.execute(
            "SELECT profile_revision, count(*)::bigint, count(DISTINCT source_symbol)::bigint "
            "FROM stock.source_pool_seed_day_facts WHERE namespace_id=%s::uuid "
            "AND target_session=%s AND package_sha256=%s AND source_capture_id=%s::uuid "
            "AND source_content_id=%s AND producer_version IS NOT NULL "
            "GROUP BY profile_revision ORDER BY profile_revision",
            (
                namespace_id,
                target_session,
                package_sha256,
                source_capture_id,
                source_content_id,
            ),
        )
        seed_versions = cursor.fetchall()
        if len(seed_versions) != 1 or (
            int(seed_versions[0][1]) != source_pool_count
            or int(seed_versions[0][2]) != source_pool_count
        ):
            reason = (
                "SOURCE_PROFILE_REVISION_AMBIGUOUS"
                if len(seed_versions) > 1
                else "SOURCE_PROFILE_FACTS_NOT_COMPLETE"
            )
            for kind in _KINDS:
                if normalized_snapshots[kind] is not None:
                    result_by_kind[kind] = {
                        "status": "NOT_COMPUTED",
                        "reason_codes": [reason],
                        "snapshot_id": normalized_snapshots[kind],
                        "facts": {},
                    }
            return {
                "status": "NOT_COMPUTED",
                "profile_revision": None,
                "by_kind": result_by_kind,
                "database_write_attempted": False,
                "cross_period_recalculated": False,
            }
        profile_revision = str(seed_versions[0][0]).strip()
        cursor.execute(
            f"SELECT {', '.join(_COLUMNS)} FROM {_TABLE} WHERE namespace_id=%s::uuid "
            "AND target_session=%s AND package_sha256=%s AND source_capture_id=%s::uuid "
            "AND source_content_id=%s AND source_raw_digest=%s AND profile_revision=%s "
            "AND snapshot_id=ANY(%s::uuid[]) AND algorithm_contract=%s "
            'ORDER BY snapshot_id, board_kind, board_native_id COLLATE "C"',
            (
                namespace_id,
                target_session,
                package_sha256,
                source_capture_id,
                source_content_id,
                source_raw_digest,
                profile_revision,
                active_snapshots,
                _ALGORITHM,
            ),
        )
        columns = [item.name for item in cursor.description]
        rows = [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
        for kind in _KINDS:
            snapshot_id = normalized_snapshots[kind]
            if snapshot_id is None:
                result_by_kind[kind] = {
                    "status": "NOT_COMPUTED",
                    "reason_codes": ["SECTOR_SNAPSHOT_UNAVAILABLE"],
                    "snapshot_id": None,
                    "facts": {},
                }
                continue
            group = [
                row
                for row in rows
                if str(row["snapshot_id"]) == snapshot_id and row["board_kind"] == kind
            ]
            if not group:
                cursor.execute(
                    f"SELECT count(*)::bigint FROM {_TABLE} WHERE namespace_id=%s::uuid "
                    "AND target_session=%s AND snapshot_id=%s::uuid AND board_kind=%s "
                    "AND algorithm_contract=%s",
                    (namespace_id, target_session, snapshot_id, kind, _ALGORITHM),
                )
                has_other_revision = int(cursor.fetchone()[0]) > 0
                result_by_kind[kind] = {
                    "status": "NOT_COMPUTED",
                    "reason_codes": [
                        "PULSE_SOURCE_REVISION_MISMATCH"
                        if has_other_revision
                        else "PULSE_FACTS_NOT_SAVED_FOR_EXACT_INPUT"
                    ],
                    "snapshot_id": snapshot_id,
                    "facts": {},
                }
                continue
            expected_counts = {int(row["snapshot_board_count"]) for row in group}
            unique_boards = {row["board_native_id"] for row in group}
            valid_group = (
                len(expected_counts) == 1
                and len(group) == next(iter(expected_counts))
                and len(unique_boards) == len(group)
                and all(
                    row["calculation_status"] == "COMPLETE"
                    and row["formal_rotation_state"] == "NOT_EVALUATED"
                    and row["research_mode"] == "NON_PIT_RETROSPECTIVE"
                    and row["membership_mode"]
                    == "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT"
                    and row["price_coordinate"] == "RAW_UNADJUSTED"
                    and int(row["ret5_denominator"]) == int(row["member_count"])
                    and int(row["breadth_denominator"]) == int(row["member_count"])
                    for row in group
                )
            )
            if not valid_group:
                result_by_kind[kind] = {
                    "status": "INCOMPLETE",
                    "reason_codes": ["PULSE_SNAPSHOT_BOARD_PARTITION_NOT_CLOSED"],
                    "snapshot_id": snapshot_id,
                    "facts": {},
                }
                continue
            result_by_kind[kind] = {
                "status": "AVAILABLE",
                "reason_codes": [],
                "snapshot_id": snapshot_id,
                "expected_board_count": next(iter(expected_counts)),
                "read_board_count": len(group),
                "facts": {row["board_native_id"]: _pulse_payload(row) for row in group},
            }
    statuses = [result_by_kind[kind]["status"] for kind in _KINDS]
    overall = (
        "AVAILABLE"
        if all(item == "AVAILABLE" for item in statuses)
        else "INCOMPLETE"
        if any(item in {"AVAILABLE", "INCOMPLETE"} for item in statuses)
        else "NOT_COMPUTED"
    )
    return {
        "status": overall,
        "profile_revision": profile_revision,
        "by_kind": result_by_kind,
        "database_write_attempted": False,
        "cross_period_recalculated": False,
    }


__all__ = [
    "SectorPulsePgError",
    "persist_source_sector_pulse_day",
    "read_source_sector_pulse_day",
]
