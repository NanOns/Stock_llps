"""板块PULSE后继事实的BIGINT父键读写与幂等核验。"""

from __future__ import annotations

from datetime import date
from decimal import Decimal, ROUND_HALF_UP, localcontext
from fractions import Fraction
from typing import Any, Mapping, Sequence

from stock_a.storage.postgres import source_raw
from stock_a.storage.postgres.research_day_facts import _insert_rows, _row_matches

_TABLE = "source_sector_pulse_followup_facts"
_PULSE_TABLE = "source_sector_cross_period_pulse_facts"
_FOLLOWUP = "SOURCE_SECTOR_PULSE_FOLLOWUP_CLOSE_R1"
_PULSE_ALGORITHM = "SOURCE_CROSS_PERIOD_PULSE_R1"
_KINDS = ("INDUSTRY", "CONCEPT")
_HORIZONS = (1, 3, 5)
_PULSE_STATES = ("TRUE", "FALSE", "UNKNOWN")
_OUTCOME_STATES = ("OBSERVED", "UNKNOWN", "NOT_DUE", "NOT_COMPUTED", "INCOMPLETE")
_PULSE_COLUMNS = (
    "id",
    "target_session",
    "source_package_id",
    "source_capture_id",
    "profile_revision",
    "catalog_id",
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
    "pulse_fact_id",
    "followup_contract",
    "horizon_sessions",
    "target_session",
    "target_source_capture_id",
    "calendar_id",
    "calendar_revision",
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
    "target_source_time_quality",
    "target_source_clock_role",
    "target_source_available_at",
)
_KEY = (
    "pulse_fact_id",
    "target_source_capture_id",
    "horizon_sessions",
    "followup_contract",
    "calendar_id",
    "calendar_revision",
)


class SectorPulseFollowupPgError(ValueError):
    """板块PULSE后继结果的稳定PG错误。"""

    def __init__(self, code: str, message: str):
        self.code = code
        self.preserve_transaction_error = True
        super().__init__(message)


def _required_text(value: Any, label: str) -> str:
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
    cursor: Any, *, t0_session: date, source_package_id: int
) -> list[dict[str, Any]]:
    selected = ["p." + name for name in _PULSE_COLUMNS if name != "snapshot_id"]
    cursor.execute(
        f"SELECT {', '.join(selected)}, c.snapshot_id "
        f"FROM {_PULSE_TABLE} p JOIN tdx_sector_catalog c ON c.id=p.catalog_id "
        "WHERE p.target_session=%s AND p.source_package_id=%s AND p.algorithm_contract=%s "
        'ORDER BY c.board_kind, c.board_native_id COLLATE "C"',
        (t0_session, source_package_id, _PULSE_ALGORITHM),
    )
    names = [item.name for item in cursor.description]
    rows = [dict(zip(names, values, strict=True)) for values in cursor.fetchall()]
    if not rows:
        raise SectorPulseFollowupPgError(
            "PULSE_FOLLOWUP_T0_NOT_SAVED", "指定T0与来源包没有已存PULSE事实"
        )
    for row in rows:
        if (
            type(row["id"]) is not int
            or row["id"] <= 0
            or type(row["source_capture_id"]) is not int
            or row["source_capture_id"] <= 0
            or type(row["source_package_id"]) is not int
            or row["source_package_id"] != source_package_id
            or type(row["catalog_id"]) is not int
            or type(row["snapshot_id"]) is not int
            or row["board_kind"] not in _KINDS
            or row["pulse_state"] not in _PULSE_STATES
            or row["algorithm_contract"] != _PULSE_ALGORITHM
            or row["calculation_status"] != "COMPLETE"
            or row["research_mode"] != "NON_PIT_RETROSPECTIVE"
            or row["membership_mode"] != "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT"
            or row["price_coordinate"] != "RAW_UNADJUSTED"
            or not str(row["profile_revision"]).startswith("PROFILE_R")
        ):
            raise SectorPulseFollowupPgError(
                "PULSE_FOLLOWUP_T0_PARTITION_INVALID",
                "T0 PULSE存在未知状态、版本或时间/价格语义",
            )
    identities = {
        (
            row["target_session"],
            row["source_package_id"],
            row["source_capture_id"],
            row["profile_revision"],
            row["source_pool_count"],
            row["source_time_quality"],
            row["source_clock_role"],
            row["source_available_at"],
        )
        for row in rows
    }
    snapshots = {
        kind: {row["snapshot_id"] for row in rows if row["board_kind"] == kind}
        for kind in _KINDS
    }
    if len(identities) != 1 or any(len(snapshots[kind]) != 1 for kind in _KINDS):
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
    cursor: Any, *, session: date, source_package_id: int
) -> tuple[str, dict[str, Any] | None]:
    status, actual, expected = source_raw._page_version_status(
        cursor, session, source_package_id
    )
    if status != "CLOSED":
        return status, None
    cursor.execute(
        """SELECT c.capture_id, c.source_package_id, c.normalized_count,
       c.source_time_quality, c.source_clock_role, c.source_available_at,
       count(r.id)::bigint, count(DISTINCT r.source_symbol)::bigint,
       count(DISTINCT r.source_capture_id)::bigint
FROM source_captures c
LEFT JOIN source_raw_daily r ON r.source_capture_id=c.capture_id
  AND r.source_package_id=c.source_package_id AND r.session_date=c.target_session
WHERE c.source_package_id=%s AND c.target_session=%s
GROUP BY c.capture_id, c.source_package_id, c.normalized_count,
         c.source_time_quality, c.source_clock_role, c.source_available_at""",
        (source_package_id, session),
    )
    rows = cursor.fetchall()
    if len(rows) != 1:
        return "INCOMPLETE", None
    (
        capture,
        package,
        normalized,
        quality,
        role,
        available,
        row_count,
        symbol_count,
        capture_count,
    ) = rows[0]
    if (
        int(package) != source_package_id
        or actual != expected
        or int(row_count) != actual
        or int(symbol_count) != actual
        or int(capture_count) != 1
        or int(normalized) != actual
        or not quality
        or not role
    ):
        return "INCOMPLETE", None
    return status, {
        "session_date": session,
        "source_package_id": int(package),
        "source_capture_id": int(capture),
        "normalized_count": int(normalized),
        "source_time_quality": quality,
        "source_clock_role": role,
        "source_available_at": available,
        "row_count": int(row_count),
    }


def read_source_sector_pulse_followup_inputs(
    config: Mapping[str, Any],
    *,
    t0_session: date,
    research_cutoff: date,
    source_package_id: int,
    due_targets: Mapping[int, date],
) -> dict[str, Any]:
    """在一个只读快照中读T0完整PULSE、固定成员和已闭合后继Raw。"""
    if (
        type(t0_session) is not date
        or type(research_cutoff) is not date
        or t0_session > research_cutoff
        or type(source_package_id) is not int
        or source_package_id <= 0
        or any(
            type(h) is not int
            or h not in _HORIZONS
            or type(d) is not date
            or d <= t0_session
            or d > research_cutoff
            for h, d in due_targets.items()
        )
    ):
        raise SectorPulseFollowupPgError(
            "PULSE_FOLLOWUP_INPUT_INVALID", "后继生产输入日期、期限或来源包无效"
        )
    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _connection,
        cursor,
        _identity,
    ):
        pulses = _pulse_rows(
            cursor, t0_session=t0_session, source_package_id=source_package_id
        )
        identity = pulses[0]
        t0_status, t0_source = _source_version(
            cursor, session=t0_session, source_package_id=source_package_id
        )
        if (
            t0_status != "CLOSED"
            or t0_source is None
            or t0_source["source_capture_id"] != identity["source_capture_id"]
            or t0_source["normalized_count"] != int(identity["source_pool_count"])
        ):
            raise SectorPulseFollowupPgError(
                "PULSE_FOLLOWUP_T0_RAW_BINDING_INVALID",
                "T0 PULSE没有绑定完整且一致的Raw来源分区",
            )
        cursor.execute(
            """SELECT count(*)::bigint, count(DISTINCT source_symbol)::bigint,
       count(DISTINCT profile_revision)::bigint, min(producer_version), max(producer_version)
FROM source_pool_seed_day_facts
WHERE target_session=%s AND source_package_id=%s AND source_capture_id=%s AND profile_revision=%s""",
            (
                t0_session,
                source_package_id,
                identity["source_capture_id"],
                identity["profile_revision"],
            ),
        )
        seed_count, seed_symbols, revision_count, producer_min, producer_max = (
            cursor.fetchone()
        )
        if (
            int(seed_count) != int(identity["source_pool_count"])
            or int(seed_symbols) != int(identity["source_pool_count"])
            or int(revision_count) != 1
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
                """SELECT c.board_native_id, c.board_name, count(m.source_symbol)::integer,
       COALESCE(array_agg(m.source_symbol ORDER BY m.source_symbol)
           FILTER (WHERE m.source_symbol IS NOT NULL), ARRAY[]::text[])
FROM tdx_sector_catalog c LEFT JOIN tdx_sector_members m ON m.catalog_id=c.id
WHERE c.snapshot_id=%s AND c.board_kind=%s
GROUP BY c.board_native_id,c.board_name ORDER BY c.board_native_id COLLATE "C""",
                (snapshot_id, kind),
            )
            directory = cursor.fetchall()
            group = [row for row in pulses if row["board_kind"] == kind]
            expected = {row["board_native_id"]: row for row in group}
            if len(directory) != len(group):
                raise SectorPulseFollowupPgError(
                    "PULSE_FOLLOWUP_SNAPSHOT_PARTITION_INCOMPLETE",
                    f"{kind}的T0成员目录与PULSE板块分区不一致",
                )
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
        target_versions, target_statuses = {}, {}
        readable_sessions = [t0_session]
        for horizon, target in sorted(due_targets.items()):
            status, version = _source_version(
                cursor, session=target, source_package_id=source_package_id
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
            source_symbols=symbols,
            sessions=tuple(sorted(set(readable_sessions))),
            source_package_id=source_package_id,
        )
        cursor.execute(
            "SELECT max(session_date) FROM source_raw_daily WHERE source_package_id=%s AND session_date<=%s",
            (source_package_id, research_cutoff),
        )
        saved_through = cursor.fetchone()[0]
    rows_by_symbol: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in symbols}
    for row in raw_rows:
        rows_by_symbol[row["source_symbol"]].append(row)
    return {
        "t0_session": t0_session,
        "research_cutoff": research_cutoff,
        "source_package_id": source_package_id,
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
    calendar_id = _required_text(calendar_identity.get("calendar_id"), "官方日历")
    calendar_revision = _required_text(
        calendar_identity.get("calendar_revision"), "官方日历修订"
    )
    rows, seen = [], set()
    for item in outcomes:
        pulse, outcome = item.get("pulse"), item.get("outcome")
        horizon, target = item.get("horizon_sessions"), item.get("target_session")
        target_version = context["target_source_versions"].get(horizon)
        if (
            not isinstance(pulse, Mapping)
            or not isinstance(outcome, Mapping)
            or type(horizon) is not int
            or horizon not in _HORIZONS
            or type(target) is not date
            or target_version is None
            or target_version["session_date"] != target
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
        pulse_id = pulse.get("pulse_fact_id")
        if type(pulse_id) is not int or pulse_id <= 0:
            raise SectorPulseFollowupPgError(
                "PULSE_FOLLOWUP_PULSE_ID_INVALID", "后继结果缺少已存PULSE父记录数字ID"
            )
        numerator, denominator = _fraction_parts(
            outcome.get("equal_weight_mean_return")
        )
        key = (
            pulse_id,
            target_version["source_capture_id"],
            horizon,
            calendar_id,
            calendar_revision,
        )
        if key in seen:
            raise SectorPulseFollowupPgError(
                "PULSE_FOLLOWUP_TYPED_KEY_DUPLICATE", "后继板块业务键重复"
            )
        seen.add(key)
        rows.append(
            (
                pulse_id,
                _FOLLOWUP,
                horizon,
                target,
                target_version["source_capture_id"],
                calendar_id,
                calendar_revision,
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
                target_version["source_time_quality"],
                target_version["source_clock_role"],
                target_version["source_available_at"],
            )
        )
    expected = {
        (
            pulse["id"],
            context["target_source_versions"][h]["source_capture_id"],
            h,
            calendar_id,
            calendar_revision,
        )
        for pulse in context["pulse_rows"]
        for h in context["target_source_versions"]
    }
    if seen != expected or not rows:
        raise SectorPulseFollowupPgError(
            "PULSE_FOLLOWUP_BATCH_INCOMPLETE",
            "离线后继批次未覆盖每个T0板块与每个可用期限",
        )
    return sorted(rows, key=lambda row: (row[0], row[2]))


def _validate_write_scope(cursor: Any, context: Mapping[str, Any]) -> None:
    identity = context["pulse_rows"][0]
    current = _pulse_rows(
        cursor,
        t0_session=identity["target_session"],
        source_package_id=identity["source_package_id"],
    )
    stable = (
        "id",
        "source_capture_id",
        "source_package_id",
        "profile_revision",
        "catalog_id",
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
    expected_map = {
        (r["board_kind"], r["board_native_id"]): r for r in context["pulse_rows"]
    }
    current_map = {(r["board_kind"], r["board_native_id"]): r for r in current}
    if current_map.keys() != expected_map.keys() or any(
        any(current_map[k].get(n) != expected_map[k].get(n) for n in stable)
        for k in expected_map
    ):
        raise SectorPulseFollowupPgError(
            "PULSE_FOLLOWUP_T0_CHANGED", "T0 PULSE业务分区在生产期间发生变化"
        )
    versions = {context["t0_session"]: context["t0_source_version"]}
    versions.update(
        {v["session_date"]: v for v in context["target_source_versions"].values()}
    )
    for session, expected in versions.items():
        status, actual = _source_version(
            cursor, session=session, source_package_id=context["source_package_id"]
        )
        if status != "CLOSED" or actual != expected:
            raise SectorPulseFollowupPgError(
                "PULSE_FOLLOWUP_RAW_CHANGED", "T0或后继Raw来源分区在生产期间发生变化"
            )
    for kind in _KINDS:
        snapshot_id = next(r["snapshot_id"] for r in current if r["board_kind"] == kind)
        cursor.execute(
            """SELECT c.board_native_id, c.board_name,
       COALESCE(array_agg(m.source_symbol ORDER BY m.source_symbol)
           FILTER (WHERE m.source_symbol IS NOT NULL), ARRAY[]::text[])
FROM tdx_sector_catalog c LEFT JOIN tdx_sector_members m ON m.catalog_id=c.id
WHERE c.snapshot_id=%s AND c.board_kind=%s
GROUP BY c.board_native_id,c.board_name ORDER BY c.board_native_id COLLATE "C""",
            (snapshot_id, kind),
        )
        actual = {(b, n): tuple(s or ()) for b, n, s in cursor.fetchall()}
        expected = {
            (r["board_native_id"], r["board_name"]): context["members_by_board"][
                (kind, r["board_native_id"])
            ]
            for r in current
            if r["board_kind"] == kind
        }
        if actual != expected:
            raise SectorPulseFollowupPgError(
                "PULSE_FOLLOWUP_SNAPSHOT_CHANGED",
                f"{kind}后验成员快照在生产期间发生变化",
            )


def _read_scope(
    cursor: Any,
    pulse_ids: Sequence[int],
    capture_id: int,
    horizon: int,
    calendar_id: str,
    calendar_revision: str,
) -> list[tuple[Any, ...]]:
    cursor.execute(
        f"SELECT {', '.join(_COLUMNS)} FROM {_TABLE} WHERE pulse_fact_id=ANY(%s::bigint[]) AND target_source_capture_id=%s AND horizon_sessions=%s AND followup_contract=%s AND calendar_id=%s AND calendar_revision=%s ORDER BY pulse_fact_id",
        (
            list(pulse_ids),
            capture_id,
            horizon,
            _FOLLOWUP,
            calendar_id,
            calendar_revision,
        ),
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
        context=context, outcomes=outcomes, calendar_identity=calendar_identity
    )
    calendar_id = _required_text(calendar_identity.get("calendar_id"), "官方日历")
    calendar_revision = _required_text(
        calendar_identity.get("calendar_revision"), "官方日历修订"
    )
    grouped: dict[tuple[int, int], list[tuple[Any, ...]]] = {}
    for row in rows:
        grouped.setdefault((row[4], row[2]), []).append(row)
    scopes, already = [], []
    with source_raw._transaction(config, write=True, statement_timeout_ms=30000) as (
        _connection,
        cursor,
        _identity,
    ):
        _validate_write_scope(cursor, context)
        cursor.execute("SELECT 1 FROM migration_ledger WHERE migration_id=22")
        if cursor.fetchone() is None:
            raise SectorPulseFollowupPgError(
                "PULSE_FOLLOWUP_MIGRATION_MISSING", "R06板块PULSE后继结果迁移尚未应用"
            )
        for key, group in sorted(grouped.items()):
            expected = sorted(group, key=lambda row: row[0])
            ids = [row[0] for row in expected]
            existing = _read_scope(
                cursor, ids, key[0], key[1], calendar_id, calendar_revision
            )
            equal = len(existing) == len(expected) and all(
                _row_matches(a, b) for a, b in zip(expected, existing, strict=True)
            )
            if existing and not equal:
                raise SectorPulseFollowupPgError(
                    "PULSE_FOLLOWUP_CONTENT_CONFLICT",
                    "相同PULSE父行、目标捕获与期限已有异内容或不完整PG结果",
                )
            already.append(equal)
            scopes.append((key, expected, ids))
        if any(already) and not all(already):
            raise SectorPulseFollowupPgError(
                "PULSE_FOLLOWUP_PARTIAL_BATCH",
                "PG中存在部分期限或板块已写结果；拒绝留下半批事实",
            )
        inserted = (
            0 if all(already) else _insert_rows(cursor, _TABLE, _COLUMNS, _KEY, rows)
        )
        for key, expected, ids in scopes:
            actual = _read_scope(
                cursor, ids, key[0], key[1], calendar_id, calendar_revision
            )
            if len(actual) != len(expected) or any(
                not _row_matches(a, b) for a, b in zip(expected, actual, strict=True)
            ):
                raise SectorPulseFollowupPgError(
                    "PULSE_FOLLOWUP_TYPED_READBACK_MISMATCH",
                    "同一PG事务中的后继typed事实读回不匹配",
                )
    same = bool(already) and all(already)
    return {
        "status": "IDEMPOTENT_ALREADY_PRESENT"
        if same
        else "WRITTEN_AND_TYPED_READBACK_VERIFIED",
        "rows_expected": len(rows),
        "rows_inserted": inserted,
        "same_input_retry": "ZERO_INSERTS_TYPED_EQUALITY_VERIFIED" if same else None,
        "transaction": "SERIALIZABLE_ATOMIC_ALL_BOARDS_AND_DUE_HORIZONS",
        "database_write_attempted": not same,
        "ddl_executed": False,
        "dml_executed": not same,
    }


def _result_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    numerator, denominator = (
        row["equal_weight_return_numerator"],
        row["equal_weight_return_denominator"],
    )
    exact = (
        Fraction(numerator) / Fraction(denominator)
        if numerator is not None and denominator is not None
        else None
    )
    percent = None
    if exact is not None:
        with localcontext() as context:
            context.prec = 50
            percent = str(
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
        "equal_weight_return_percent_4dp": percent,
        "reason_codes": list(row["reason_codes"]),
        "research_mode": row["research_mode"],
        "membership_mode": row["membership_mode"],
        "price_coordinate": row["price_coordinate"],
        "t0_source_time_quality": row["source_time_quality"],
        "t0_source_clock_role": row["source_clock_role"],
        "t0_source_available_at": row["source_available_at"].isoformat()
        if row["source_available_at"] is not None
        else None,
        "target_source_time_quality": row["target_source_time_quality"],
        "target_source_clock_role": row["target_source_clock_role"],
        "target_source_available_at": row["target_source_available_at"].isoformat()
        if row["target_source_available_at"] is not None
        else None,
    }


def _percent_4dp(value: Fraction) -> str:
    with localcontext() as context:
        context.prec = 50
        return str(
            (Decimal(value.numerator * 100) / Decimal(value.denominator)).quantize(
                Decimal("0.0001"), rounding=ROUND_HALF_UP
            )
        )


def _followup_cohort_comparison(
    kind_facts: Mapping[str, Mapping[str, Any]], *, expected_count: int
) -> dict[str, Any]:
    """按已绑定PULSE与后继事实形成板块关系级描述性分组。"""
    horizon_groups = []
    incomplete_reasons: dict[tuple[int, str], int] = {}
    for horizon in _HORIZONS:
        state_groups = {}
        state_mean_values: dict[str, Fraction] = {}
        for pulse_state in _PULSE_STATES:
            items = []
            for board in kind_facts.values():
                if board.get("pulse_state") != pulse_state:
                    continue
                item = next(
                    (
                        row
                        for row in board.get("horizons", ())
                        if row.get("horizon_sessions") == horizon
                    ),
                    None,
                )
                items.append(item)

            counts = {status: 0 for status in _OUTCOME_STATES}
            unknown_reasons: dict[str, int] = {}
            incomplete_reason_group: dict[str, int] = {}
            values: list[Fraction] = []
            fixed_member_denominator = 0
            observed_member_count = 0
            unknown_member_count = 0
            for item in items:
                if not isinstance(item, Mapping):
                    counts["INCOMPLETE"] += 1
                    reason = "FOLLOWUP_HORIZON_FACT_MISSING"
                    incomplete_reason_group[reason] = (
                        incomplete_reason_group.get(reason, 0) + 1
                    )
                    incomplete_reasons[(horizon, "FOLLOWUP_HORIZON_FACT_MISSING")] = (
                        incomplete_reasons.get(
                            (horizon, "FOLLOWUP_HORIZON_FACT_MISSING"), 0
                        )
                        + 1
                    )
                    continue
                status = item.get("status")
                if status not in counts:
                    status = "INCOMPLETE"
                counts[status] += 1
                denominator = item.get("fixed_member_denominator")
                if type(denominator) is int and denominator >= 0:
                    fixed_member_denominator += denominator
                observed = item.get("observed_member_count")
                if type(observed) is int and observed >= 0:
                    observed_member_count += observed
                unknown = item.get("unknown_member_count")
                if type(unknown) is int and unknown >= 0:
                    unknown_member_count += unknown
                if status == "UNKNOWN":
                    for reason in set(item.get("reason_codes") or ()):
                        if isinstance(reason, str) and reason:
                            unknown_reasons[reason] = unknown_reasons.get(reason, 0) + 1
                elif status == "INCOMPLETE":
                    for reason in set(item.get("reason_codes") or ()):
                        if isinstance(reason, str) and reason:
                            incomplete_reason_group[reason] = (
                                incomplete_reason_group.get(reason, 0) + 1
                            )
                            key = (horizon, reason)
                            incomplete_reasons[key] = incomplete_reasons.get(key, 0) + 1
                if status == "OBSERVED":
                    try:
                        exact = Fraction(str(item["equal_weight_return"]))
                    except (KeyError, ValueError, ZeroDivisionError):
                        counts["INCOMPLETE"] += 1
                        counts["OBSERVED"] -= 1
                        key = (horizon, "FOLLOWUP_EXACT_RETURN_INVALID")
                        incomplete_reasons[key] = incomplete_reasons.get(key, 0) + 1
                    else:
                        values.append(exact)

            total = len(items)
            observed_count = counts["OBSERVED"]
            incomplete_count = counts["INCOMPLETE"]
            if total == 0:
                status = "NOT_COMPARABLE"
            elif incomplete_count:
                status = "INCOMPLETE"
            elif not observed_count and counts["NOT_DUE"] == total:
                status = "NOT_DUE"
            elif not observed_count and counts["NOT_COMPUTED"] == total:
                status = "NOT_COMPUTED"
            elif observed_count:
                status = "AVAILABLE"
            else:
                status = "NOT_COMPARABLE"

            distribution = {
                "status": "NOT_COMPARABLE",
                "sample_scope": "OBSERVED_BOARD_RELATIONS_ONLY",
                "numeric_method": "EXACT_FRACTION_THEN_PERCENT_4DP",
                "sample_count": 0,
                "mean_return_percent_4dp": None,
                "median_return_percent_4dp": None,
                "positive_board_relation_count": None,
                "nonpositive_board_relation_count": None,
                "minimum_return_percent_4dp": None,
                "maximum_return_percent_4dp": None,
            }
            if status == "AVAILABLE" and len(values) == observed_count:
                ordered = sorted(values)
                middle = len(ordered) // 2
                median_value = (
                    ordered[middle]
                    if len(ordered) % 2
                    else (ordered[middle - 1] + ordered[middle]) / 2
                )
                mean_value = sum(ordered, Fraction()) / len(ordered)
                state_mean_values[pulse_state] = mean_value
                distribution.update(
                    {
                        "status": "AVAILABLE",
                        "sample_count": len(ordered),
                        "mean_return_percent_4dp": _percent_4dp(mean_value),
                        "median_return_percent_4dp": _percent_4dp(median_value),
                        "positive_board_relation_count": sum(
                            value > 0 for value in ordered
                        ),
                        "nonpositive_board_relation_count": sum(
                            value <= 0 for value in ordered
                        ),
                        "minimum_return_percent_4dp": _percent_4dp(ordered[0]),
                        "maximum_return_percent_4dp": _percent_4dp(ordered[-1]),
                    }
                )

            coverage_percent = (
                _percent_4dp(Fraction(observed_count, total)) if total else None
            )
            state_groups[pulse_state] = {
                "status": status,
                "pulse_state": pulse_state,
                "total_board_relations": total,
                "outcome_status_counts": counts,
                "partition_closed": sum(counts.values()) == total,
                "complete_basket_coverage": {
                    "observed_board_relations": observed_count,
                    "all_board_relations": total,
                    "percent_4dp": coverage_percent,
                },
                "fixed_member_denominator_sum": fixed_member_denominator,
                "observed_member_count_sum": observed_member_count,
                "unknown_member_count_sum": unknown_member_count,
                "unknown_reason_scope": "BOARD_RELATIONS_WITH_REASON; REASONS_MAY_OVERLAP",
                "unknown_reason_counts": [
                    {
                        "reason_code": reason,
                        "board_relation_count": count,
                    }
                    for reason, count in sorted(unknown_reasons.items())
                ],
                "incomplete_reason_counts": [
                    {
                        "reason_code": reason,
                        "board_relation_count": count,
                    }
                    for reason, count in sorted(incomplete_reason_group.items())
                ],
                "distribution": distribution,
            }

        horizon_total = sum(
            group["total_board_relations"] for group in state_groups.values()
        )
        horizon_groups.append(
            {
                "horizon_sessions": horizon,
                "total_board_relations": horizon_total,
                "pulse_state_partition_closed": (
                    horizon_total == expected_count
                    and all(
                        group["partition_closed"] for group in state_groups.values()
                    )
                ),
                "true_minus_false_observed_coverage_percentage_points": (
                    _percent_4dp(
                        Fraction(
                            state_groups["TRUE"]["complete_basket_coverage"][
                                "observed_board_relations"
                            ],
                            state_groups["TRUE"]["complete_basket_coverage"][
                                "all_board_relations"
                            ],
                        )
                        - Fraction(
                            state_groups["FALSE"]["complete_basket_coverage"][
                                "observed_board_relations"
                            ],
                            state_groups["FALSE"]["complete_basket_coverage"][
                                "all_board_relations"
                            ],
                        )
                    )
                    if state_groups["TRUE"]["total_board_relations"]
                    and state_groups["FALSE"]["total_board_relations"]
                    else None
                ),
                "true_minus_false_mean_return_difference_percentage_points": (
                    _percent_4dp(state_mean_values["TRUE"] - state_mean_values["FALSE"])
                    if "TRUE" in state_mean_values and "FALSE" in state_mean_values
                    else None
                ),
                "pulse_states": state_groups,
            }
        )

    all_statuses = [
        group["status"]
        for item in horizon_groups
        for group in item["pulse_states"].values()
    ]
    overall_status = (
        "INCOMPLETE"
        if any(value == "INCOMPLETE" for value in all_statuses)
        else "AVAILABLE"
        if any(value == "AVAILABLE" for value in all_statuses)
        else "NOT_COMPUTED"
        if any(value == "NOT_COMPUTED" for value in all_statuses)
        else "NOT_DUE"
        if any(value == "NOT_DUE" for value in all_statuses)
        else "NOT_COMPARABLE"
    )
    return {
        "status": overall_status,
        "scope": "SAME_T0_SOURCE_SNAPSHOT_WINDOW_AND_BOARD_KIND",
        "unit": "BOARD_RELATION; MEMBERS_MAY_OVERLAP_ACROSS_BOARDS",
        "research_mode": "NON_PIT_EXPLORATORY",
        "predictive_validation": "NO_PREDICTIVE_VALIDATION",
        "reason_code_counts": [
            {
                "horizon_sessions": horizon,
                "reason_code": reason,
                "board_relation_count": count,
            }
            for (horizon, reason), count in sorted(incomplete_reasons.items())
        ],
        "horizons": horizon_groups,
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
    groups = {
        kind: {
            "status": "NOT_COMPUTED",
            "reason_codes": [],
            "facts": {},
            "comparison": {"status": "NOT_COMPUTED", "horizons": []},
        }
        for kind in _KINDS
    }
    if pulse_day.get("status") != "AVAILABLE":
        status = pulse_day.get("status", "NOT_COMPUTED")
        reasons = pulse_day.get("reason_codes", ["PULSE_INPUT_NOT_AVAILABLE"])
        return {
            "status": status,
            "by_kind": {
                kind: {
                    "status": status,
                    "reason_codes": reasons,
                    "facts": {},
                    "comparison": {
                        "status": status,
                        "reason_codes": reasons,
                        "horizons": [],
                    },
                }
                for kind in _KINDS
            },
            "database_write_attempted": False,
        }
    source = pulse_day.get("source_version")
    if not isinstance(source, Mapping):
        raise SectorPulseFollowupPgError(
            "PULSE_FOLLOWUP_SOURCE_BINDING_INVALID", "T0缺少来源捕获上下文"
        )
    t0_session = date.fromisoformat(str(pulse_day.get("target_session")))
    package_id, capture_id = (
        source.get("source_package_id"),
        source.get("source_capture_id"),
    )
    if (
        type(package_id) is not int
        or package_id <= 0
        or type(capture_id) is not int
        or capture_id <= 0
    ):
        raise SectorPulseFollowupPgError(
            "PULSE_FOLLOWUP_SOURCE_BINDING_INVALID", "T0来源包或捕获ID无效"
        )
    profile_revision = _required_text(pulse_day.get("profile_revision"), "T0画像修订")
    calendar_id = _required_text(calendar_identity.get("calendar_id"), "官方日历")
    calendar_revision = _required_text(
        calendar_identity.get("calendar_revision"), "官方日历修订"
    )
    snapshot_ids = {
        kind: pulse_day.get("by_kind", {}).get(kind, {}).get("snapshot_id")
        for kind in _KINDS
    }
    if any(type(value) is not int or value <= 0 for value in snapshot_ids.values()):
        raise SectorPulseFollowupPgError(
            "PULSE_FOLLOWUP_SNAPSHOT_ID_INVALID", "T0成员快照必须是正BIGINT ID"
        )
    saved_by_board: dict[str, dict[int, dict[str, Any]]] = {kind: {} for kind in _KINDS}
    with source_raw._transaction(config, write=False, statement_timeout_ms=15000) as (
        _connection,
        cursor,
        _identity,
    ):
        cursor.execute(
            f"""SELECT f.{", f.".join(_COLUMNS)}, p.pulse_state, p.pulse_reason_codes,
       p.source_time_quality, p.source_clock_role, p.source_available_at,
       c.board_kind, c.board_native_id, c.board_name, p.member_count, p.catalog_id
FROM {_TABLE} f JOIN {_PULSE_TABLE} p ON p.id=f.pulse_fact_id
JOIN tdx_sector_catalog c ON c.id=p.catalog_id
WHERE p.source_package_id=%s AND p.source_capture_id=%s AND p.profile_revision=%s
  AND p.algorithm_contract=%s AND c.snapshot_id=ANY(%s::bigint[])
  AND f.followup_contract=%s AND f.calendar_id=%s AND f.calendar_revision=%s
ORDER BY c.board_kind,c.board_native_id COLLATE "C",f.horizon_sessions""",
            (
                package_id,
                capture_id,
                profile_revision,
                _PULSE_ALGORITHM,
                list(snapshot_ids.values()),
                _FOLLOWUP,
                calendar_id,
                calendar_revision,
            ),
        )
        names = [item.name for item in cursor.description]
        saved = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
        for row in saved:
            bucket = saved_by_board[row["board_kind"]].setdefault(
                row["board_native_id"], {}
            )
            horizon = int(row["horizon_sessions"])
            if horizon in bucket:
                raise SectorPulseFollowupPgError(
                    "PULSE_FOLLOWUP_DUPLICATE_RESULT",
                    "同一板块期限存在多个已存后继结果",
                )
            bucket[horizon] = row
        cursor.execute(
            "SELECT max(session_date) FROM source_raw_daily WHERE source_package_id=%s AND session_date<=%s",
            (package_id, research_cutoff),
        )
        saved_through = cursor.fetchone()[0]
        target_versions, target_statuses = {}, {}
        for horizon, target in horizon_targets.items():
            if target is None or target > research_cutoff:
                continue
            status, version = _source_version(
                cursor, session=target, source_package_id=package_id
            )
            target_statuses[horizon] = status
            if version is not None:
                target_versions[horizon] = version
        for kind in _KINDS:
            pulse_group = pulse_day.get("by_kind", {}).get(kind, {})
            expected_count, pulse_facts = (
                pulse_group.get("expected_board_count"),
                pulse_group.get("facts", {}),
            )
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
                    "comparison": {
                        "status": "INCOMPLETE",
                        "reason_codes": ["PULSE_SNAPSHOT_BOARD_PARTITION_NOT_CLOSED"],
                        "horizons": [],
                    },
                }
                continue
            kind_facts, statuses = {}, []
            for board_id, pulse in pulse_facts.items():
                horizons = []
                for horizon in _HORIZONS:
                    target = horizon_targets.get(horizon)
                    row = saved_by_board[kind].get(board_id, {}).get(horizon)
                    if target is None:
                        item = {
                            "status": "INCOMPLETE",
                            "horizon_sessions": horizon,
                            "target_session": None,
                            "fixed_member_denominator": pulse.get("member_count"),
                            "observed_member_count": None,
                            "unknown_member_count": None,
                            "reason_codes": ["OFFICIAL_CALENDAR_WINDOW_UNAVAILABLE"],
                        }
                    elif target > research_cutoff:
                        item = {
                            "status": "NOT_DUE",
                            "horizon_sessions": horizon,
                            "target_session": target.isoformat(),
                            "fixed_member_denominator": int(pulse["member_count"]),
                            "observed_member_count": None,
                            "unknown_member_count": None,
                            "reason_codes": ["FOLLOWUP_HORIZON_BEYOND_RESEARCH_CUTOFF"],
                        }
                    elif row is not None:
                        version = target_versions.get(horizon)
                        if (
                            row["target_session"] != target
                            or row["target_source_capture_id"]
                            != (version or {}).get("source_capture_id")
                            or row["pulse_state"] != pulse.get("state")
                            or row["fixed_member_denominator"]
                            != pulse.get("member_count")
                            or row["catalog_id"] != pulse.get("catalog_id")
                        ):
                            item = {
                                "status": "INCOMPLETE",
                                "horizon_sessions": horizon,
                                "target_session": target.isoformat(),
                                "fixed_member_denominator": int(pulse["member_count"]),
                                "observed_member_count": None,
                                "unknown_member_count": None,
                                "reason_codes": ["FOLLOWUP_SOURCE_OR_PARENT_MISMATCH"],
                            }
                        else:
                            item = _result_payload(row)
                    else:
                        version_status = target_statuses.get(horizon, "INCOMPLETE")
                        item = {
                            "status": "NOT_COMPUTED"
                            if version_status == "CLOSED"
                            else "INCOMPLETE",
                            "horizon_sessions": horizon,
                            "target_session": target.isoformat(),
                            "fixed_member_denominator": int(pulse["member_count"]),
                            "observed_member_count": None,
                            "unknown_member_count": None,
                            "reason_codes": [
                                "FOLLOWUP_RESULT_NOT_SAVED_FOR_EXACT_INPUT"
                                if version_status == "CLOSED"
                                else "FOLLOWUP_RAW_SESSION_PARTITION_NOT_CLOSED"
                            ],
                        }
                    item.setdefault("pulse_state", pulse.get("state"))
                    item.setdefault("research_mode", "NON_PIT_RETROSPECTIVE")
                    item.setdefault(
                        "membership_mode", "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT"
                    )
                    item.setdefault("price_coordinate", "RAW_UNADJUSTED")
                    horizons.append(item)
                    statuses.append(item["status"])
                kind_facts[board_id] = {
                    "pulse_state": pulse.get("state"),
                    "snapshot_id": snapshot_ids[kind],
                    "horizons": horizons,
                }
            group_status = (
                "INCOMPLETE"
                if "INCOMPLETE" in statuses
                else "NOT_COMPUTED"
                if "NOT_COMPUTED" in statuses
                else "NOT_DUE"
                if "NOT_DUE" in statuses
                else "AVAILABLE"
            )
            groups[kind] = {
                "status": group_status,
                "reason_codes": [],
                "expected_board_count": expected_count,
                "read_board_count": len(kind_facts),
                "facts": kind_facts,
                "comparison": _followup_cohort_comparison(
                    kind_facts, expected_count=expected_count
                ),
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
        "saved_through": saved_through.isoformat() if saved_through else None,
        "database_write_attempted": False,
        "ddl_executed": False,
        "dml_executed": False,
        "read_only_transaction": "REPEATABLE READ READ ONLY",
        "profile_revision": profile_revision,
        "source_package_id": package_id,
        "source_capture_id": capture_id,
    }
