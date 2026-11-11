"""板块PULSE真实后继结果的PG读取、原子写入与幂等核验。"""

from __future__ import annotations

from datetime import date
from decimal import Decimal, ROUND_HALF_UP, localcontext
from fractions import Fraction
from typing import Any, Mapping, Sequence

from stock_a.storage.postgres import source_raw
from stock_a.storage.postgres.research_day_facts import _insert_rows, _row_matches

_TABLE = "stock.source_sector_pulse_followup_facts"
_PULSE_TABLE = "stock.source_sector_cross_period_pulse_facts"
_FOLLOWUP = "SOURCE_SECTOR_PULSE_FOLLOWUP_CLOSE_R1"
_PULSE_ALGORITHM = "SOURCE_CROSS_PERIOD_PULSE_R1"
_KINDS = ("INDUSTRY", "CONCEPT")
_HORIZONS = (1, 3, 5)
_PULSE_COLUMNS = (
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
    "pulse_state",
    "pulse_reason_codes",
    "calculation_status",
    "research_mode",
    "membership_mode",
    "price_coordinate",
    "source_time_quality",
    "source_clock_role",
    "source_available_at",
    "member_count",
    "snapshot_board_count",
    "source_pool_count",
)
_COLUMNS = (
    "namespace_id",
    "t0_session",
    "t0_package_sha256",
    "t0_source_capture_id",
    "t0_source_content_id",
    "t0_source_raw_digest",
    "t0_profile_revision",
    "snapshot_id",
    "board_kind",
    "board_native_id",
    "board_name",
    "pulse_algorithm_contract",
    "pulse_state",
    "pulse_reason_codes",
    "followup_contract",
    "horizon_sessions",
    "target_session",
    "target_package_sha256",
    "target_source_capture_id",
    "target_source_content_id",
    "target_source_raw_digest",
    "calendar_id",
    "calendar_revision",
    "calendar_sha256",
    "fixed_member_denominator",
    "observed_member_count",
    "unknown_member_count",
    "equal_weight_return_numerator",
    "equal_weight_return_denominator",
    "outcome_status",
    "reason_codes",
    "research_mode",
    "membership_mode",
    "price_coordinate",
    "t0_source_time_quality",
    "t0_source_clock_role",
    "t0_source_available_at",
    "target_source_time_quality",
    "target_source_clock_role",
    "target_source_available_at",
)
_KEY = (
    "namespace_id",
    "t0_session",
    "t0_package_sha256",
    "t0_source_capture_id",
    "t0_source_content_id",
    "t0_source_raw_digest",
    "t0_profile_revision",
    "snapshot_id",
    "board_kind",
    "board_native_id",
    "pulse_algorithm_contract",
    "followup_contract",
    "horizon_sessions",
    "target_session",
    "calendar_id",
    "calendar_revision",
    "calendar_sha256",
)


class SectorPulseFollowupPgError(ValueError):
    """板块PULSE后继结果的稳定PG错误。"""

    def __init__(self, code: str, message: str):
        self.code = code
        self.preserve_transaction_error = True
        super().__init__(message)


def _uuid(value: Any, label: str) -> str:
    import uuid

    try:
        normalized = str(uuid.UUID(str(value)))
    except (ValueError, TypeError, AttributeError) as error:
        raise SectorPulseFollowupPgError(
            "PULSE_FOLLOWUP_UUID_INVALID", f"{label}不是有效UUID"
        ) from error
    return normalized


def _text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SectorPulseFollowupPgError(
            "PULSE_FOLLOWUP_SOURCE_BINDING_INVALID", f"{label}不能为空"
        )
    return value.strip()


def _codes(value: Any) -> list[str]:
    return sorted(
        {item for item in value if isinstance(item, str) and item}
        if isinstance(value, (list, tuple))
        else set()
    )


def _pulse_rows(
    cursor: Any, *, t0_session: date, package_sha256: str
) -> list[dict[str, Any]]:
    cursor.execute(
        f"SELECT {', '.join(_PULSE_COLUMNS)} FROM {_PULSE_TABLE} "
        "WHERE target_session=%s AND package_sha256=%s "
        'ORDER BY board_kind, board_native_id COLLATE "C"',
        (t0_session, package_sha256),
    )
    names = [item.name for item in cursor.description]
    rows = [dict(zip(names, values, strict=True)) for values in cursor.fetchall()]
    if not rows:
        raise SectorPulseFollowupPgError(
            "PULSE_FOLLOWUP_T0_NOT_SAVED", "指定T0与来源包没有已存PULSE事实"
        )
    for row in rows:
        for key in (
            "namespace_id",
            "source_capture_id",
            "snapshot_id",
        ):
            row[key] = str(row[key])
        for key in (
            "package_sha256",
            "source_content_id",
            "source_raw_digest",
            "profile_revision",
        ):
            row[key] = str(row[key]).strip()
        if (
            row["board_kind"] not in _KINDS
            or row["pulse_state"] not in {"TRUE", "FALSE", "UNKNOWN"}
            or row["algorithm_contract"] != _PULSE_ALGORITHM
            or row["calculation_status"] != "COMPLETE"
            or row["research_mode"] != "NON_PIT_RETROSPECTIVE"
            or row["membership_mode"] != "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT"
            or row["price_coordinate"] != "RAW_UNADJUSTED"
        ):
            raise SectorPulseFollowupPgError(
                "PULSE_FOLLOWUP_T0_PARTITION_INVALID",
                "T0 PULSE存在未知状态、版本或时间/价格语义",
            )
    identities = {
        tuple(
            row[key]
            for key in (
                "namespace_id",
                "target_session",
                "package_sha256",
                "source_capture_id",
                "source_content_id",
                "source_raw_digest",
                "profile_revision",
                "algorithm_contract",
                "source_pool_count",
                "source_time_quality",
                "source_clock_role",
                "source_available_at",
            )
        )
        for row in rows
    }
    snapshots = {
        kind: {row["snapshot_id"] for row in rows if row["board_kind"] == kind}
        for kind in _KINDS
    }
    if (
        len(identities) != 1
        or any(len(snapshots[kind]) != 1 for kind in _KINDS)
        or len({next(iter(snapshots[kind])) for kind in _KINDS}) != len(_KINDS)
    ):
        raise SectorPulseFollowupPgError(
            "PULSE_FOLLOWUP_T0_IDENTITY_AMBIGUOUS",
            "T0 PULSE的Raw、Profile或双成员快照身份不唯一",
        )
    for kind in _KINDS:
        group = [row for row in rows if row["board_kind"] == kind]
        expected = {int(row["snapshot_board_count"]) for row in group}
        if (
            not group
            or len(expected) != 1
            or len(group) != next(iter(expected))
            or len({row["board_native_id"] for row in group}) != len(group)
        ):
            raise SectorPulseFollowupPgError(
                "PULSE_FOLLOWUP_T0_PARTITION_INCOMPLETE",
                f"{kind}的T0 PULSE板块分区不完整",
            )
    return rows


def _source_version(
    cursor: Any,
    *,
    namespace_id: str,
    session: date,
    package_sha256: str,
) -> tuple[str, dict[str, Any] | None]:
    status, actual, expected = source_raw._page_version_status(
        cursor, namespace_id, session, package_sha256
    )
    if status != "CLOSED":
        return status, None
    cursor.execute(
        """SELECT c.capture_id::text, c.content_id, c.canonical_digest,
       c.normalized_count, c.source_time_quality, c.source_clock_role,
       c.source_available_at, count(r.*)::bigint,
       count(DISTINCT r.source_symbol)::bigint,
       count(DISTINCT r.source_content_id)::bigint,
       count(DISTINCT r.package_sha256)::bigint
FROM stock.source_raw_daily r
JOIN stock.source_captures c
  ON c.namespace_id=r.namespace_id AND c.capture_id=r.source_capture_id
WHERE r.namespace_id=%s::uuid AND r.session_date=%s AND r.package_sha256=%s
GROUP BY c.capture_id, c.content_id, c.canonical_digest, c.normalized_count,
         c.source_time_quality, c.source_clock_role, c.source_available_at""",
        (namespace_id, session, package_sha256),
    )
    rows = cursor.fetchall()
    if len(rows) != 1:
        return "INCOMPLETE", None
    (
        capture_id,
        content_id,
        raw_digest,
        normalized_count,
        time_quality,
        clock_role,
        available_at,
        row_count,
        symbol_count,
        content_count,
        package_count,
    ) = rows[0]
    if (
        actual != expected
        or int(row_count) != actual
        or int(symbol_count) != actual
        or int(content_count) != 1
        or int(package_count) != 1
        or int(normalized_count) != actual
        or not content_id
        or not raw_digest
        or not time_quality
        or not clock_role
    ):
        return "INCOMPLETE", None
    return status, {
        "session_date": session,
        "namespace_id": namespace_id,
        "package_sha256": package_sha256,
        "source_capture_id": str(capture_id),
        "source_content_id": str(content_id).strip(),
        "source_raw_digest": str(raw_digest).strip(),
        "normalized_count": int(normalized_count),
        "source_time_quality": time_quality,
        "source_clock_role": clock_role,
        "source_available_at": available_at,
        "row_count": int(row_count),
    }


def read_source_sector_pulse_followup_inputs(
    config: Mapping[str, Any],
    *,
    t0_session: date,
    research_cutoff: date,
    package_sha256: str,
    due_targets: Mapping[int, date],
) -> dict[str, Any]:
    """在一个只读快照中读T0完整PULSE、固定成员和已闭合后继Raw。"""
    if (
        type(t0_session) is not date
        or type(research_cutoff) is not date
        or t0_session > research_cutoff
        or not isinstance(package_sha256, str)
        or len(package_sha256) != 64
        or any(
            type(horizon) is not int
            or horizon not in _HORIZONS
            or type(target) is not date
            or target <= t0_session
            or target > research_cutoff
            for horizon, target in due_targets.items()
        )
    ):
        raise SectorPulseFollowupPgError(
            "PULSE_FOLLOWUP_INPUT_INVALID", "后继生产输入日期、期限或来源包无效"
        )

    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _connection,
        cursor,
        namespace_id,
    ):
        pulses = _pulse_rows(
            cursor, t0_session=t0_session, package_sha256=package_sha256
        )
        pulse_namespaces = {row["namespace_id"] for row in pulses}
        if pulse_namespaces != {namespace_id}:
            raise SectorPulseFollowupPgError(
                "PULSE_FOLLOWUP_NAMESPACE_MISMATCH", "T0 PULSE与PG命名空间不一致"
            )
        identity = pulses[0]

        t0_status, t0_source = _source_version(
            cursor,
            namespace_id=namespace_id,
            session=t0_session,
            package_sha256=package_sha256,
        )
        if (
            t0_status != "CLOSED"
            or t0_source is None
            or t0_source["source_capture_id"] != identity["source_capture_id"]
            or t0_source["source_content_id"] != identity["source_content_id"]
            or t0_source["source_raw_digest"] != identity["source_raw_digest"]
            or t0_source["normalized_count"] != int(identity["source_pool_count"])
        ):
            raise SectorPulseFollowupPgError(
                "PULSE_FOLLOWUP_T0_RAW_BINDING_INVALID",
                "T0 PULSE没有绑定完整且一致的Raw来源分区",
            )
        cursor.execute(
            """SELECT count(*)::bigint, count(DISTINCT source_symbol)::bigint,
       count(DISTINCT profile_revision)::bigint,
       min(producer_version), max(producer_version)
FROM stock.source_pool_seed_day_facts
WHERE namespace_id=%s::uuid AND target_session=%s AND package_sha256=%s
  AND source_capture_id=%s::uuid AND source_content_id=%s AND profile_revision=%s""",
            (
                namespace_id,
                t0_session,
                package_sha256,
                identity["source_capture_id"],
                identity["source_content_id"],
                identity["profile_revision"],
            ),
        )
        seed_count, seed_symbols, profile_count, producer_min, producer_max = (
            cursor.fetchone()
        )
        if (
            int(seed_count) != int(identity["source_pool_count"])
            or int(seed_symbols) != int(identity["source_pool_count"])
            or int(profile_count) != 1
            or producer_min != "SOURCE_POOL_SEED_CORE_PROFILE_R1"
            or producer_max != producer_min
        ):
            raise SectorPulseFollowupPgError(
                "PULSE_FOLLOWUP_PROFILE_PARTITION_INCOMPLETE",
                "T0画像修订没有闭合到唯一完整的已存Seed来源池",
            )

        members_by_board: dict[tuple[str, str], tuple[str, ...]] = {}
        pulse_by_board = {}
        for kind in _KINDS:
            snapshot_id = next(
                row["snapshot_id"] for row in pulses if row["board_kind"] == kind
            )
            cursor.execute(
                """SELECT c.board_native_id, c.board_name,
       count(m.source_symbol)::integer,
       COALESCE(array_agg(m.source_symbol ORDER BY m.source_symbol)
           FILTER (WHERE m.source_symbol IS NOT NULL), ARRAY[]::text[])
FROM stock.tdx_sector_catalog c
LEFT JOIN stock.tdx_sector_members m
  ON m.snapshot_id=c.snapshot_id AND m.board_kind=c.board_kind
 AND m.board_native_id=c.board_native_id
WHERE c.snapshot_id=%s::uuid AND c.board_kind=%s
GROUP BY c.board_native_id,c.board_name
ORDER BY c.board_native_id COLLATE "C" """,
                (snapshot_id, kind),
            )
            directory = cursor.fetchall()
            group = [row for row in pulses if row["board_kind"] == kind]
            if len(directory) != len(group):
                raise SectorPulseFollowupPgError(
                    "PULSE_FOLLOWUP_SNAPSHOT_PARTITION_INCOMPLETE",
                    f"{kind}的T0成员目录与PULSE板块分区不一致",
                )
            expected = {row["board_native_id"]: row for row in group}
            for board_id, board_name, member_count, symbols in directory:
                pulse = expected.get(board_id)
                symbols = tuple(symbols or ())
                if (
                    pulse is None
                    or pulse["board_name"] != board_name
                    or int(pulse["member_count"]) != int(member_count)
                    or len(symbols) != int(member_count)
                    or len(set(symbols)) != len(symbols)
                ):
                    raise SectorPulseFollowupPgError(
                        "PULSE_FOLLOWUP_MEMBER_DENOMINATOR_MISMATCH",
                        f"{kind}板块固定成员分母与已存PULSE不一致",
                    )
                members_by_board[(kind, board_id)] = symbols
                pulse_by_board[(kind, board_id)] = pulse

        target_versions: dict[int, dict[str, Any]] = {}
        target_statuses: dict[int, dict[str, Any]] = {}
        readable_sessions = [t0_session]
        for horizon, target in sorted(due_targets.items()):
            status, version = _source_version(
                cursor,
                namespace_id=namespace_id,
                session=target,
                package_sha256=package_sha256,
            )
            target_statuses[horizon] = {
                "status": status,
                "target_session": target,
                "row_count": version["row_count"] if version else None,
                "expected_count": version["normalized_count"] if version else None,
            }
            if status == "CLOSED" and version is not None:
                target_versions[horizon] = version
                readable_sessions.append(target)

        symbols = tuple(
            sorted(
                {symbol for members in members_by_board.values() for symbol in members}
            )
        )
        raw_rows = source_raw._read_history_rows(
            cursor,
            namespace_id=namespace_id,
            source_symbols=symbols,
            sessions=tuple(sorted(set(readable_sessions))),
            package_sha256=package_sha256,
        )
        cursor.execute(
            "SELECT max(session_date) FROM stock.source_raw_daily "
            "WHERE namespace_id=%s::uuid AND package_sha256=%s AND session_date<=%s",
            (namespace_id, package_sha256, research_cutoff),
        )
        saved_through = cursor.fetchone()[0]

    rows_by_symbol: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in symbols}
    for row in raw_rows:
        rows_by_symbol[row["source_symbol"]].append(row)
    return {
        "namespace_id": namespace_id,
        "t0_session": t0_session,
        "research_cutoff": research_cutoff,
        "package_sha256": package_sha256,
        "pulse_rows": pulses,
        "pulse_by_board": pulse_by_board,
        "members_by_board": members_by_board,
        "rows_by_symbol": rows_by_symbol,
        "t0_source_version": t0_source,
        "target_source_versions": target_versions,
        "target_partition_statuses": target_statuses,
        "calendar_identity": None,
        "saved_through": saved_through,
        "raw_history_row_count": len(raw_rows),
        "transaction": "REPEATABLE READ READ ONLY",
        "database_write_attempted": False,
    }


def _fraction_parts(value: Any) -> tuple[int | None, int | None]:
    if value is None:
        return None, None
    fraction = value if isinstance(value, Fraction) else Fraction(value)
    return fraction.numerator, fraction.denominator


def _prepare_rows(
    *,
    context: Mapping[str, Any],
    outcomes: Sequence[Mapping[str, Any]],
    calendar_identity: Mapping[str, str],
) -> list[tuple[Any, ...]]:
    identity = context["pulse_rows"][0]
    calendar_id = _text(calendar_identity.get("calendar_id"), "官方日历")
    calendar_revision = _text(
        calendar_identity.get("calendar_revision"), "官方日历修订"
    )
    calendar_sha256 = _text(calendar_identity.get("calendar_sha256"), "官方日历SHA")
    rows = []
    seen = set()
    for item in outcomes:
        pulse = item.get("pulse")
        outcome = item.get("outcome")
        horizon = item.get("horizon_sessions")
        target_session = item.get("target_session")
        target_version = context["target_source_versions"].get(horizon)
        if (
            not isinstance(pulse, Mapping)
            or not isinstance(outcome, Mapping)
            or type(horizon) is not int
            or horizon not in _HORIZONS
            or type(target_session) is not date
            or target_version is None
            or target_version["session_date"] != target_session
            or outcome.get("outcome_status") not in {"OBSERVED", "UNKNOWN"}
            or outcome.get("research_mode") != "NON_PIT_RETROSPECTIVE"
            or outcome.get("membership_mode")
            != "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT"
            or outcome.get("price_coordinate") != "RAW_UNADJUSTED"
            or outcome.get("fixed_member_denominator") != pulse["member_count"]
        ):
            raise SectorPulseFollowupPgError(
                "PULSE_FOLLOWUP_TYPED_ROW_INVALID",
                "后继PG typed行缺少精确来源或质量字段",
            )
        numerator, denominator = _fraction_parts(
            outcome.get("equal_weight_mean_return")
        )
        key = (
            pulse["board_kind"],
            pulse["board_native_id"],
            horizon,
        )
        if key in seen:
            raise SectorPulseFollowupPgError(
                "PULSE_FOLLOWUP_TYPED_KEY_DUPLICATE", "后继板块业务键重复"
            )
        seen.add(key)
        rows.append(
            (
                identity["namespace_id"],
                identity["target_session"],
                identity["package_sha256"],
                identity["source_capture_id"],
                identity["source_content_id"],
                identity["source_raw_digest"],
                identity["profile_revision"],
                pulse["snapshot_id"],
                pulse["board_kind"],
                pulse["board_native_id"],
                pulse["board_name"],
                pulse["algorithm_contract"],
                pulse["pulse_state"],
                _codes(pulse["pulse_reason_codes"]),
                _FOLLOWUP,
                horizon,
                target_session,
                identity["package_sha256"],
                target_version["source_capture_id"],
                target_version["source_content_id"],
                target_version["source_raw_digest"],
                calendar_id,
                calendar_revision,
                calendar_sha256,
                int(outcome["fixed_member_denominator"]),
                int(outcome["observed_member_count"]),
                int(outcome["unknown_member_count"]),
                numerator,
                denominator,
                outcome["outcome_status"],
                _codes(outcome.get("reason_codes")),
                outcome["research_mode"],
                outcome["membership_mode"],
                outcome["price_coordinate"],
                identity["source_time_quality"],
                identity["source_clock_role"],
                identity["source_available_at"],
                target_version["source_time_quality"],
                target_version["source_clock_role"],
                target_version["source_available_at"],
            )
        )
    expected = {
        (kind, board_id, horizon)
        for kind, board_id in context["pulse_by_board"]
        for horizon in context["target_source_versions"]
    }
    if seen != expected or not rows:
        raise SectorPulseFollowupPgError(
            "PULSE_FOLLOWUP_BATCH_INCOMPLETE",
            "离线后继批次未覆盖每个T0板块与每个可用期限",
        )
    rows.sort(
        key=lambda row: (
            row[_COLUMNS.index("board_kind")],
            row[_COLUMNS.index("horizon_sessions")],
            row[_COLUMNS.index("board_native_id")],
        )
    )
    return rows


def _validate_write_scope(cursor: Any, context: Mapping[str, Any]) -> None:
    identity = context["pulse_rows"][0]
    current = _pulse_rows(
        cursor,
        t0_session=identity["target_session"],
        package_sha256=identity["package_sha256"],
    )
    expected_map = {
        (row["board_kind"], row["board_native_id"]): row
        for row in context["pulse_rows"]
    }
    current_map = {(row["board_kind"], row["board_native_id"]): row for row in current}
    stable_fields = (
        "namespace_id",
        "target_session",
        "package_sha256",
        "source_capture_id",
        "source_content_id",
        "source_raw_digest",
        "profile_revision",
        "snapshot_id",
        "board_name",
        "algorithm_contract",
        "pulse_state",
        "pulse_reason_codes",
        "member_count",
        "source_time_quality",
        "source_clock_role",
        "source_available_at",
    )
    if current_map.keys() != expected_map.keys() or any(
        any(
            current_map[key][field] != expected_map[key][field]
            for field in stable_fields
        )
        for key in expected_map
    ):
        raise SectorPulseFollowupPgError(
            "PULSE_FOLLOWUP_T0_CHANGED", "T0 PULSE业务分区在生产期间发生变化"
        )
    dates = {context["t0_session"]: context["t0_source_version"]}
    dates.update(
        {
            item["session_date"]: item
            for item in context["target_source_versions"].values()
        }
    )
    for session, expected in dates.items():
        status, actual = _source_version(
            cursor,
            namespace_id=context["namespace_id"],
            session=session,
            package_sha256=context["package_sha256"],
        )
        if status != "CLOSED" or actual != expected:
            raise SectorPulseFollowupPgError(
                "PULSE_FOLLOWUP_RAW_CHANGED", "T0或后继Raw来源修订在生产期间发生变化"
            )
    for (kind, snapshot_id), group in (
        (
            (
                kind,
                next(
                    row["snapshot_id"] for row in current if row["board_kind"] == kind
                ),
            ),
            [row for row in current if row["board_kind"] == kind],
        )
        for kind in _KINDS
    ):
        cursor.execute(
            """SELECT c.board_native_id, count(m.source_symbol)::integer
FROM stock.tdx_sector_catalog c
LEFT JOIN stock.tdx_sector_members m
  ON m.snapshot_id=c.snapshot_id AND m.board_kind=c.board_kind
 AND m.board_native_id=c.board_native_id
WHERE c.snapshot_id=%s::uuid AND c.board_kind=%s
GROUP BY c.board_native_id ORDER BY c.board_native_id COLLATE "C" """,
            (snapshot_id, kind),
        )
        members = {board_id: int(count) for board_id, count in cursor.fetchall()}
        expected_members = {
            row["board_native_id"]: int(row["member_count"]) for row in group
        }
        if members != expected_members:
            raise SectorPulseFollowupPgError(
                "PULSE_FOLLOWUP_SNAPSHOT_CHANGED",
                f"{kind}后验成员快照在生产期间发生变化",
            )


def _scope_query(cursor: Any, row: Sequence[Any]) -> list[tuple[Any, ...]]:
    values = dict(zip(_COLUMNS, row, strict=True))
    scope = (
        "namespace_id=%s::uuid AND t0_session=%s AND t0_package_sha256=%s "
        "AND t0_source_capture_id=%s::uuid AND t0_source_content_id=%s "
        "AND t0_source_raw_digest=%s AND t0_profile_revision=%s "
        "AND snapshot_id=%s::uuid AND board_kind=%s AND pulse_algorithm_contract=%s "
        "AND followup_contract=%s AND horizon_sessions=%s AND target_session=%s "
        "AND calendar_id=%s AND calendar_revision=%s AND calendar_sha256=%s"
    )
    cursor.execute(
        f"SELECT {', '.join(_COLUMNS)} FROM {_TABLE} WHERE {scope} "
        'ORDER BY board_native_id COLLATE "C"',
        tuple(values[name] for name in _KEY if name != "board_native_id"),
    )
    return cursor.fetchall()


def persist_source_sector_pulse_followups(
    config: Mapping[str, Any],
    *,
    context: Mapping[str, Any],
    outcomes: Sequence[Mapping[str, Any]],
    calendar_identity: Mapping[str, str],
) -> dict[str, Any]:
    """以一个SERIALIZABLE事务原子写入全部行业、概念与期限结果。"""
    rows = _prepare_rows(
        context=context,
        outcomes=outcomes,
        calendar_identity=calendar_identity,
    )
    scopes = []
    with source_raw._transaction(config, write=True, statement_timeout_ms=30000) as (
        _connection,
        cursor,
        namespace_id,
    ):
        if namespace_id != context["namespace_id"]:
            raise SectorPulseFollowupPgError(
                "PULSE_FOLLOWUP_NAMESPACE_MISMATCH", "写入PG命名空间与生产输入不一致"
            )
        _validate_write_scope(cursor, context)
        cursor.execute("SELECT to_regclass(%s) IS NOT NULL", (_TABLE,))
        if cursor.fetchone() != (True,):
            raise SectorPulseFollowupPgError(
                "PULSE_FOLLOWUP_MIGRATION_MISSING", "板块PULSE后继结果迁移尚未应用"
            )
        by_scope: dict[tuple[str, int], list[tuple[Any, ...]]] = {}
        for row in rows:
            by_scope.setdefault(
                (
                    row[_COLUMNS.index("board_kind")],
                    row[_COLUMNS.index("horizon_sessions")],
                ),
                [],
            ).append(row)
        already = []
        for key, group in sorted(by_scope.items()):
            existing = _scope_query(cursor, group[0])
            if existing and (
                len(existing) != len(group)
                or any(
                    not _row_matches(expected, actual)
                    for expected, actual in zip(group, existing, strict=False)
                )
            ):
                raise SectorPulseFollowupPgError(
                    "PULSE_FOLLOWUP_CONTENT_CONFLICT",
                    "相同T0、成员快照与期限已有异内容或不完整PG结果",
                )
            already.append(len(existing) == len(group))
            scopes.append((key, group, existing))
        if any(already) and not all(already):
            raise SectorPulseFollowupPgError(
                "PULSE_FOLLOWUP_PARTIAL_BATCH",
                "PG中存在部分期限或板块已写结果；拒绝留下半批事实",
            )
        if not all(already):
            inserted = _insert_rows(cursor, _TABLE, _COLUMNS, _KEY, rows)
        else:
            inserted = 0
        for _key, group, _existing in scopes:
            readback = _scope_query(cursor, group[0])
            if len(readback) != len(group) or any(
                not _row_matches(expected, actual)
                for expected, actual in zip(group, readback, strict=True)
            ):
                raise SectorPulseFollowupPgError(
                    "PULSE_FOLLOWUP_TYPED_READBACK_MISMATCH",
                    "同一PG事务中的后继typed事实读回不匹配",
                )
    already_present = bool(already) and all(already)
    return {
        "status": "IDEMPOTENT_ALREADY_PRESENT"
        if already_present
        else "WRITTEN_AND_TYPED_READBACK_VERIFIED",
        "rows_expected": len(rows),
        "rows_inserted": inserted,
        "same_input_retry": "ZERO_INSERTS_TYPED_EQUALITY_VERIFIED"
        if already_present
        else None,
        "transaction": "SERIALIZABLE_ATOMIC_ALL_BOARDS_AND_DUE_HORIZONS",
        "database_write_attempted": not already_present,
        "ddl_executed": False,
        "dml_executed": not already_present,
    }


def _result_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    numerator = row["equal_weight_return_numerator"]
    denominator = row["equal_weight_return_denominator"]
    percent_4dp = None
    exact = None
    if numerator is not None and denominator is not None:
        exact = Fraction(numerator) / Fraction(denominator)
        with localcontext() as context:
            context.prec = 50
            percent_4dp = str(
                (Decimal(exact.numerator * 100) / Decimal(exact.denominator)).quantize(
                    Decimal("0.0001"), rounding=ROUND_HALF_UP
                )
            )
    return {
        "status": row["outcome_status"],
        "horizon_sessions": int(row["horizon_sessions"]),
        "target_session": row["target_session"].isoformat(),
        "pulse_state": row["pulse_state"],
        "fixed_member_denominator": int(row["fixed_member_denominator"]),
        "observed_member_count": int(row["observed_member_count"]),
        "unknown_member_count": int(row["unknown_member_count"]),
        "equal_weight_return": str(exact) if exact is not None else None,
        "equal_weight_return_percent_4dp": percent_4dp,
        "reason_codes": list(row["reason_codes"]),
        "research_mode": row["research_mode"],
        "membership_mode": row["membership_mode"],
        "price_coordinate": row["price_coordinate"],
        "t0_source_time_quality": row["t0_source_time_quality"],
        "t0_source_clock_role": row["t0_source_clock_role"],
        "t0_source_available_at": (
            row["t0_source_available_at"].isoformat()
            if row["t0_source_available_at"] is not None
            else None
        ),
        "target_source_time_quality": row["target_source_time_quality"],
        "target_source_clock_role": row["target_source_clock_role"],
        "target_source_available_at": (
            row["target_source_available_at"].isoformat()
            if row["target_source_available_at"] is not None
            else None
        ),
    }


def read_source_sector_pulse_followups(
    config: Mapping[str, Any],
    *,
    pulse_day: Mapping[str, Any],
    research_cutoff: date,
    horizon_targets: Mapping[int, date | None],
    calendar_identity: Mapping[str, str],
) -> dict[str, Any]:
    """批量PG只读已存后继事实，并区分到期、未生产和输入未闭合。"""
    groups: dict[str, dict[str, Any]] = {
        kind: {"status": "NOT_COMPUTED", "reason_codes": [], "facts": {}}
        for kind in _KINDS
    }
    if pulse_day.get("status") != "AVAILABLE":
        status = pulse_day.get("status", "NOT_COMPUTED")
        reasons = pulse_day.get("reason_codes", ["PULSE_INPUT_NOT_AVAILABLE"])
        for kind in _KINDS:
            groups[kind] = {"status": status, "reason_codes": reasons, "facts": {}}
        return {"status": status, "by_kind": groups, "database_write_attempted": False}

    source = pulse_day.get("source_version")
    t0_session = date.fromisoformat(str(pulse_day.get("target_session")))
    package_sha256 = _text(source.get("package_sha256"), "T0来源包")
    namespace_id = _uuid(source.get("namespace_id"), "T0 namespace")
    capture_id = _uuid(source.get("source_capture_id"), "T0捕获")
    content_id = _text(source.get("source_content_id"), "T0内容")
    raw_digest = _text(
        source.get("source_raw_digest") or source.get("capture_digest"), "T0 Raw摘要"
    )
    profile_revision = _text(pulse_day.get("profile_revision"), "T0画像修订")
    snapshot_ids = {
        kind: _uuid(
            pulse_day.get("by_kind", {}).get(kind, {}).get("snapshot_id"),
            f"{kind}成员快照",
        )
        for kind in _KINDS
    }
    rows_by_scope: dict[tuple[str, int], list[dict[str, Any]]] = {}
    with source_raw._transaction(config, write=False, statement_timeout_ms=15000) as (
        _connection,
        cursor,
        connected_namespace,
    ):
        if connected_namespace != namespace_id:
            raise SectorPulseFollowupPgError(
                "PULSE_FOLLOWUP_NAMESPACE_MISMATCH", "API后继读取namespace与PG不一致"
            )
        cursor.execute(
            f"SELECT {', '.join(_COLUMNS)} FROM {_TABLE} WHERE "
            "namespace_id=%s::uuid AND t0_session=%s AND t0_package_sha256=%s "
            "AND t0_source_capture_id=%s::uuid AND t0_source_content_id=%s "
            "AND t0_source_raw_digest=%s AND t0_profile_revision=%s "
            "AND snapshot_id=ANY(%s::uuid[]) AND followup_contract=%s "
            "AND calendar_id=%s AND calendar_revision=%s AND calendar_sha256=%s "
            'ORDER BY board_kind,board_native_id COLLATE "C",horizon_sessions',
            (
                namespace_id,
                t0_session,
                package_sha256,
                capture_id,
                content_id,
                raw_digest,
                profile_revision,
                list(snapshot_ids.values()),
                _FOLLOWUP,
                calendar_identity["calendar_id"],
                calendar_identity["calendar_revision"],
                calendar_identity["calendar_sha256"],
            ),
        )
        names = [item.name for item in cursor.description]
        saved = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
        for row in saved:
            rows_by_scope.setdefault(
                (row["board_kind"], int(row["horizon_sessions"])), []
            ).append(row)
        cursor.execute(
            "SELECT max(session_date) FROM stock.source_raw_daily "
            "WHERE namespace_id=%s::uuid AND package_sha256=%s AND session_date<=%s",
            (namespace_id, package_sha256, research_cutoff),
        )
        saved_through = cursor.fetchone()[0]
        for kind in _KINDS:
            pulse_group = pulse_day.get("by_kind", {}).get(kind, {})
            expected_count = pulse_group.get("expected_board_count")
            pulse_facts = pulse_group.get("facts", {})
            if (
                pulse_group.get("status") != "AVAILABLE"
                or type(expected_count) is not int
                or expected_count < 1
                or len(pulse_facts) != expected_count
            ):
                groups[kind] = {
                    "status": "INCOMPLETE",
                    "reason_codes": ["PULSE_SNAPSHOT_BOARD_PARTITION_NOT_CLOSED"],
                    "facts": {},
                }
                continue
            kind_facts = {}
            kind_statuses = []
            for board_id, pulse_fact in pulse_facts.items():
                horizons = []
                for horizon in _HORIZONS:
                    target = horizon_targets.get(horizon)
                    persisted = [
                        row
                        for row in rows_by_scope.get((kind, horizon), ())
                        if row["board_native_id"] == board_id
                    ]
                    scope_rows = rows_by_scope.get((kind, horizon), ())
                    if target is None:
                        item = {
                            "status": "INCOMPLETE",
                            "horizon_sessions": horizon,
                            "target_session": None,
                            "fixed_member_denominator": pulse_fact.get("member_count"),
                            "observed_member_count": None,
                            "unknown_member_count": None,
                            "reason_codes": ["OFFICIAL_CALENDAR_WINDOW_UNAVAILABLE"],
                        }
                    elif persisted:
                        row = persisted[0]
                        if row["target_session"] != target:
                            item = {
                                "status": "INCOMPLETE",
                                "horizon_sessions": horizon,
                                "target_session": target.isoformat(),
                                "fixed_member_denominator": int(
                                    pulse_fact["member_count"]
                                ),
                                "observed_member_count": None,
                                "unknown_member_count": None,
                                "reason_codes": ["FOLLOWUP_TARGET_SESSION_MISMATCH"],
                            }
                        else:
                            item = _result_payload(row)
                    elif len(scope_rows) > 0:
                        item = {
                            "status": "INCOMPLETE",
                            "horizon_sessions": horizon,
                            "target_session": target.isoformat(),
                            "fixed_member_denominator": int(pulse_fact["member_count"]),
                            "observed_member_count": None,
                            "unknown_member_count": None,
                            "reason_codes": ["FOLLOWUP_BOARD_PARTITION_INCOMPLETE"],
                        }
                    elif target > research_cutoff:
                        item = {
                            "status": "NOT_DUE",
                            "horizon_sessions": horizon,
                            "target_session": target.isoformat(),
                            "fixed_member_denominator": int(pulse_fact["member_count"]),
                            "observed_member_count": None,
                            "unknown_member_count": None,
                            "reason_codes": ["FOLLOWUP_HORIZON_BEYOND_RESEARCH_CUTOFF"],
                        }
                    else:
                        try:
                            version_status, _actual, _expected = (
                                source_raw._page_version_status(
                                    cursor, namespace_id, target, package_sha256
                                )
                            )
                        except source_raw.RawSourcePgError:
                            version_status = "INCOMPLETE"
                        item = {
                            "status": "NOT_COMPUTED"
                            if version_status == "CLOSED"
                            else "INCOMPLETE",
                            "horizon_sessions": horizon,
                            "target_session": target.isoformat(),
                            "fixed_member_denominator": int(pulse_fact["member_count"]),
                            "observed_member_count": None,
                            "unknown_member_count": None,
                            "reason_codes": [
                                "FOLLOWUP_RESULT_NOT_SAVED_FOR_EXACT_INPUT"
                                if version_status == "CLOSED"
                                else "FOLLOWUP_RAW_SESSION_PARTITION_NOT_CLOSED"
                            ],
                        }
                    item.setdefault("pulse_state", pulse_fact.get("state"))
                    item.setdefault("research_mode", "NON_PIT_RETROSPECTIVE")
                    item.setdefault(
                        "membership_mode", "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT"
                    )
                    item.setdefault("price_coordinate", "RAW_UNADJUSTED")
                    horizons.append(item)
                    kind_statuses.append(item["status"])
                kind_facts[board_id] = {
                    "pulse_state": pulse_fact.get("state"),
                    "snapshot_id": snapshot_ids[kind],
                    "horizons": horizons,
                }
            group_status = (
                "INCOMPLETE"
                if "INCOMPLETE" in kind_statuses
                else "NOT_COMPUTED"
                if "NOT_COMPUTED" in kind_statuses
                else "NOT_DUE"
                if "NOT_DUE" in kind_statuses
                else "AVAILABLE"
            )
            groups[kind] = {
                "status": group_status,
                "reason_codes": [],
                "expected_board_count": expected_count,
                "read_board_count": len(kind_facts),
                "facts": kind_facts,
            }
    statuses = [groups[kind]["status"] for kind in _KINDS]
    overall = (
        "INCOMPLETE"
        if "INCOMPLETE" in statuses
        else "NOT_COMPUTED"
        if "NOT_COMPUTED" in statuses
        else "NOT_DUE"
        if "NOT_DUE" in statuses
        else "AVAILABLE"
    )
    return {
        "status": overall,
        "by_kind": groups,
        "database_write_attempted": False,
        "cross_session_recalculated": False,
        "saved_through": saved_through.isoformat() if saved_through else None,
    }


__all__ = [
    "SectorPulseFollowupPgError",
    "persist_source_sector_pulse_followups",
    "read_source_sector_pulse_followup_inputs",
    "read_source_sector_pulse_followups",
]
