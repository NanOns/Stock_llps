"""板块PULSE T0分组后继事实的typed持久化与有界读取。"""

from __future__ import annotations

from datetime import date
from fractions import Fraction
from typing import Any, Mapping, Sequence

from stock_a.domain.sector_pulse_cohort_outcomes import (
    build_sector_pulse_cohort_outcomes,
)
from stock_a.storage.postgres import source_raw


_GROUP_TABLE = "source_sector_pulse_cohort_group_facts"
_REASON_TABLE = "source_sector_pulse_cohort_reason_facts"
_COHORT_CONTRACT = "SOURCE_SECTOR_PULSE_COHORT_OUTCOMES_R1"
_FOLLOWUP_CONTRACT = "SOURCE_SECTOR_PULSE_FOLLOWUP_CLOSE_R1"
_PULSE_CONTRACT = "SOURCE_CROSS_PERIOD_PULSE_R1"
_KINDS = ("INDUSTRY", "CONCEPT")
_AXIS_STATES = {
    "PULSE_STATE": ("TRUE", "FALSE", "UNKNOWN"),
    "ADJACENT_TRANSITION": (
        "PULSE_APPEARED",
        "PULSE_PERSISTED",
        "PULSE_CEASED",
        "PULSE_NOT_PRESENT_BOTH",
        "PULSE_TRANSITION_UNKNOWN",
        "NO_STORED_TRANSITION",
    ),
}
_HORIZONS = (1, 3, 5)
_GROUP_COLUMNS = (
    "t0_session",
    "research_cutoff",
    "t0_source_capture_id",
    "t0_profile_revision",
    "snapshot_id",
    "pulse_algorithm_contract",
    "cohort_contract",
    "followup_contract",
    "board_kind",
    "cohort_axis",
    "cohort_state",
    "horizon_sessions",
    "target_session",
    "target_source_capture_id",
    "calendar_id",
    "calendar_revision",
    "group_status",
    "board_relation_denominator",
    "observed_board_count",
    "unknown_board_count",
    "not_computed_board_count",
    "not_due_board_count",
    "incomplete_board_count",
    "fixed_member_denominator_sum",
    "evaluable_fixed_member_denominator_sum",
    "observed_member_count_sum",
    "unknown_member_count_sum",
    "positive_board_count",
    "nonpositive_board_count",
    "mean_return_numerator",
    "mean_return_denominator",
    "median_return_numerator",
    "median_return_denominator",
    "minimum_return_numerator",
    "minimum_return_denominator",
    "maximum_return_numerator",
    "maximum_return_denominator",
    "research_mode",
    "membership_mode",
    "price_coordinate",
    "source_time_quality",
    "source_clock_role",
    "source_available_at",
    "formal_rotation_state",
)
_GROUP_INSERT_COLUMNS = _GROUP_COLUMNS
_GROUP_BUSINESS_COLUMNS = _GROUP_COLUMNS
_GROUP_KEY = (
    "t0_session",
    "research_cutoff",
    "t0_source_capture_id",
    "t0_profile_revision",
    "snapshot_id",
    "pulse_algorithm_contract",
    "cohort_contract",
    "followup_contract",
    "board_kind",
    "cohort_axis",
    "cohort_state",
    "horizon_sessions",
    "calendar_id",
    "calendar_revision",
)


class SectorPulseCohortPgError(ValueError):
    """板块PULSE分组统计不符合PG业务键或完整性约束。"""

    def __init__(self, code: str, message: str):
        self.code = code
        self.preserve_transaction_error = True
        super().__init__(message)


def _fraction(numerator: Any, denominator: Any) -> Fraction | None:
    if numerator is None and denominator is None:
        return None
    try:
        num = Fraction(str(numerator))
        den = Fraction(str(denominator))
    except (TypeError, ValueError, ZeroDivisionError) as error:
        raise SectorPulseCohortPgError(
            "PULSE_COHORT_EXACT_VALUE_INVALID", "分组统计中的精确有理数无效"
        ) from error
    if den <= 0:
        raise SectorPulseCohortPgError(
            "PULSE_COHORT_EXACT_VALUE_INVALID", "分组统计的精确分母必须为正"
        )
    return num / den


def prepare_sector_pulse_cohort_rows(
    *,
    context: Mapping[str, Any],
    outcomes: Sequence[Mapping[str, Any]],
    horizon_targets: Mapping[int, date | None],
    calendar_identity: Mapping[str, str],
) -> tuple[list[tuple[Any, ...]], list[dict[str, Any]]]:
    """用一个T0的已闭合PULSE风险集和固定篮结果生成typed行。"""
    pulses = context.get("pulse_rows")
    t0_source = context.get("t0_source_version")
    t0_session = context.get("t0_session")
    research_cutoff = context.get("research_cutoff")
    if (
        not isinstance(pulses, Sequence)
        or not pulses
        or not isinstance(t0_source, Mapping)
        or type(t0_session) is not date
        or type(research_cutoff) is not date
    ):
        raise SectorPulseCohortPgError(
            "PULSE_COHORT_T0_SCOPE_INVALID", "板块PULSE分组缺少精确T0来源范围"
        )
    try:
        grouped = build_sector_pulse_cohort_outcomes(
            pulse_rows=pulses,
            transition_rows=context.get("transition_rows", ()),
            outcomes=outcomes,
            horizon_targets=horizon_targets,
            target_partition_statuses=context.get("target_partition_statuses", {}),
            research_cutoff=research_cutoff,
            calendar_identity=calendar_identity,
        )
    except ValueError as error:
        raise SectorPulseCohortPgError(
            "PULSE_COHORT_INPUT_INCOMPLETE", str(error)
        ) from error

    pulse_identity = pulses[0]
    source_capture_id = t0_source.get("source_capture_id")
    profile_revision = pulse_identity.get("profile_revision")
    if (
        type(source_capture_id) is not int
        or source_capture_id <= 0
        or not isinstance(profile_revision, str)
        or not profile_revision
        or pulse_identity.get("source_capture_id") != source_capture_id
    ):
        raise SectorPulseCohortPgError(
            "PULSE_COHORT_SOURCE_IDENTITY_INVALID", "分组统计T0来源身份无效"
        )

    pulse_by_kind = {}
    for kind in _KINDS:
        kind_rows = [row for row in pulses if row.get("board_kind") == kind]
        if not kind_rows:
            raise SectorPulseCohortPgError(
                "PULSE_COHORT_T0_KIND_MISSING", f"T0缺少{kind}板块目录"
            )
        snapshots = {row.get("snapshot_id") for row in kind_rows}
        versions = {
            (
                row.get("profile_revision"),
                row.get("source_capture_id"),
                row.get("algorithm_contract"),
                row.get("source_time_quality"),
                row.get("source_clock_role"),
                row.get("source_available_at"),
            )
            for row in kind_rows
        }
        if len(snapshots) != 1 or len(versions) != 1:
            raise SectorPulseCohortPgError(
                "PULSE_COHORT_T0_KIND_VERSION_CONFLICT",
                f"{kind}的T0目录或来源版本不唯一",
            )
        pulse_by_kind[kind] = kind_rows[0]

    target_versions = context.get("target_source_versions", {})
    status_keys = (
        "observed_board_count",
        "unknown_board_count",
        "not_computed_board_count",
        "not_due_board_count",
        "incomplete_board_count",
    )
    group_rows: list[tuple[Any, ...]] = []
    reason_rows: list[dict[str, Any]] = []
    for group in grouped:
        kind = group["board_kind"]
        identity = pulse_by_kind[kind]
        horizon = group["horizon_sessions"]
        target_version = target_versions.get(horizon)
        counts = group["outcome_status_counts"]
        distribution = group["distribution"]
        group_row = {
            "t0_session": t0_session,
            "research_cutoff": research_cutoff,
            "t0_source_capture_id": source_capture_id,
            "t0_profile_revision": profile_revision,
            "snapshot_id": identity["snapshot_id"],
            "pulse_algorithm_contract": _PULSE_CONTRACT,
            "cohort_contract": _COHORT_CONTRACT,
            "followup_contract": _FOLLOWUP_CONTRACT,
            "board_kind": kind,
            "cohort_axis": group["cohort_axis"],
            "cohort_state": group["cohort_state"],
            "horizon_sessions": horizon,
            "target_session": (
                date.fromisoformat(group["target_session"])
                if group["target_session"] is not None
                else None
            ),
            "target_source_capture_id": (
                target_version.get("source_capture_id")
                if target_version is not None
                else None
            ),
            "calendar_id": calendar_identity["calendar_id"],
            "calendar_revision": calendar_identity["calendar_revision"],
            "group_status": group["group_status"],
            "board_relation_denominator": group["board_relation_denominator"],
            "observed_board_count": counts["OBSERVED"],
            "unknown_board_count": counts["UNKNOWN"],
            "not_computed_board_count": counts["NOT_COMPUTED"],
            "not_due_board_count": counts["NOT_DUE"],
            "incomplete_board_count": counts["INCOMPLETE"],
            "fixed_member_denominator_sum": group["fixed_member_denominator_sum"],
            "evaluable_fixed_member_denominator_sum": group[
                "evaluable_fixed_member_denominator_sum"
            ],
            "observed_member_count_sum": group["observed_member_count_sum"],
            "unknown_member_count_sum": group["unknown_member_count_sum"],
            "positive_board_count": distribution["positive_board_count"],
            "nonpositive_board_count": distribution["nonpositive_board_count"],
            "mean_return_numerator": distribution["mean_return_numerator"],
            "mean_return_denominator": distribution["mean_return_denominator"],
            "median_return_numerator": distribution["median_return_numerator"],
            "median_return_denominator": distribution["median_return_denominator"],
            "minimum_return_numerator": distribution["minimum_return_numerator"],
            "minimum_return_denominator": distribution["minimum_return_denominator"],
            "maximum_return_numerator": distribution["maximum_return_numerator"],
            "maximum_return_denominator": distribution["maximum_return_denominator"],
            "research_mode": "NON_PIT_RETROSPECTIVE",
            "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
            "price_coordinate": "FORWARD_ADJUSTED",
            "source_time_quality": identity["source_time_quality"],
            "source_clock_role": identity["source_clock_role"],
            "source_available_at": identity["source_available_at"],
            "formal_rotation_state": "NOT_EVALUATED",
        }
        if any(type(group_row[name]) is not int for name in status_keys):
            raise SectorPulseCohortPgError(
                "PULSE_COHORT_STATUS_COUNT_INVALID", "分组质量计数缺失或非整数"
            )
        group_rows.append(tuple(group_row[name] for name in _GROUP_INSERT_COLUMNS))
        for item in group["reason_counts"]:
            reason_rows.append(
                {
                    "group_key": tuple(group_row[name] for name in _GROUP_KEY),
                    "reason_scope": item["reason_scope"],
                    "quality_status": item["quality_status"],
                    "reason_code": item["reason_code"],
                    "board_relation_count": item["board_relation_count"],
                }
            )
    if len(group_rows) != 54:
        raise SectorPulseCohortPgError(
            "PULSE_COHORT_BATCH_INCOMPLETE",
            "分组行未覆盖两类目录、两轴、全部状态与三期限",
        )
    return group_rows, reason_rows


def _read_group_batch(
    cursor: Any,
    *,
    t0_session: date,
    research_cutoff: date,
    t0_source_capture_id: int,
    t0_profile_revision: str,
    snapshot_ids: Mapping[str, int],
    calendar_id: str,
    calendar_revision: str,
) -> list[dict[str, Any]]:
    cursor.execute(
        f"""SELECT id,{", ".join(_GROUP_COLUMNS)}
FROM {_GROUP_TABLE}
WHERE t0_session=%s AND research_cutoff=%s
  AND t0_source_capture_id=%s
  AND t0_profile_revision=%s AND snapshot_id=ANY(%s::bigint[])
  AND pulse_algorithm_contract=%s AND cohort_contract=%s
  AND followup_contract=%s AND calendar_id=%s AND calendar_revision=%s
ORDER BY board_kind,cohort_axis,horizon_sessions,cohort_state""",
        (
            t0_session,
            research_cutoff,
            t0_source_capture_id,
            t0_profile_revision,
            list(snapshot_ids.values()),
            _PULSE_CONTRACT,
            _COHORT_CONTRACT,
            _FOLLOWUP_CONTRACT,
            calendar_id,
            calendar_revision,
        ),
    )
    names = [column.name for column in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def _group_identity(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(row[name] for name in _GROUP_KEY)


def _group_matches(expected: Sequence[Any], actual: Mapping[str, Any]) -> bool:
    return all(actual[name] == value for name, value in zip(_GROUP_COLUMNS, expected))


def persist_sector_pulse_cohort_rows(
    cursor: Any,
    *,
    context: Mapping[str, Any],
    group_rows: Sequence[Sequence[Any]],
    reason_rows: Sequence[Mapping[str, Any]],
    calendar_identity: Mapping[str, str],
) -> dict[str, Any]:
    """在现有PULSE后继事务中原子幂等写入分组事实及原因计数。"""
    pulse_rows = context["pulse_rows"]
    identity = pulse_rows[0]
    snapshot_ids = {
        kind: next(
            row["snapshot_id"] for row in pulse_rows if row["board_kind"] == kind
        )
        for kind in _KINDS
    }
    expected_by_key = {
        tuple(
            row[index]
            for index, name in enumerate(_GROUP_COLUMNS)
            if name in _GROUP_KEY
        ): row
        for row in group_rows
    }
    if len(expected_by_key) != len(group_rows) or len(group_rows) != 54:
        raise SectorPulseCohortPgError(
            "PULSE_COHORT_TYPED_KEY_DUPLICATE", "分组统计typed业务键重复或批次不完整"
        )
    existing = _read_group_batch(
        cursor,
        t0_session=context["t0_session"],
        research_cutoff=context["research_cutoff"],
        t0_source_capture_id=context["t0_source_version"]["source_capture_id"],
        t0_profile_revision=identity["profile_revision"],
        snapshot_ids=snapshot_ids,
        calendar_id=calendar_identity["calendar_id"],
        calendar_revision=calendar_identity["calendar_revision"],
    )
    if existing and len(existing) != len(group_rows):
        raise SectorPulseCohortPgError(
            "PULSE_COHORT_PARTIAL_BATCH", "PG已有部分分组统计；拒绝覆盖不完整批次"
        )
    if existing and any(
        not _group_matches(expected_by_key[_group_identity(row)], row)
        for row in existing
    ):
        raise SectorPulseCohortPgError(
            "PULSE_COHORT_CONTENT_CONFLICT", "同一T0分组业务键已有不同统计内容"
        )
    inserted_groups = 0
    if not existing:
        placeholders = ",".join(["%s"] * len(_GROUP_INSERT_COLUMNS))
        columns = ",".join(_GROUP_INSERT_COLUMNS)
        cursor.executemany(
            f"INSERT INTO {_GROUP_TABLE} ({columns}) VALUES ({placeholders}) "
            f"ON CONFLICT ({','.join(_GROUP_KEY)}) DO NOTHING",
            list(group_rows),
        )
        inserted_groups = cursor.rowcount
    actual = _read_group_batch(
        cursor,
        t0_session=context["t0_session"],
        research_cutoff=context["research_cutoff"],
        t0_source_capture_id=context["t0_source_version"]["source_capture_id"],
        t0_profile_revision=identity["profile_revision"],
        snapshot_ids=snapshot_ids,
        calendar_id=calendar_identity["calendar_id"],
        calendar_revision=calendar_identity["calendar_revision"],
    )
    if len(actual) != len(group_rows) or any(
        not _group_matches(expected_by_key[_group_identity(row)], row) for row in actual
    ):
        raise SectorPulseCohortPgError(
            "PULSE_COHORT_TYPED_READBACK_MISMATCH", "分组统计写入后同事务读回不一致"
        )

    ids_by_key = {_group_identity(row): row["id"] for row in actual}
    expected_reasons = {
        (
            ids_by_key[item["group_key"]],
            item["reason_scope"],
            item["quality_status"],
            item["reason_code"],
        ): item["board_relation_count"]
        for item in reason_rows
    }
    if len(expected_reasons) != len(reason_rows):
        raise SectorPulseCohortPgError(
            "PULSE_COHORT_REASON_KEY_DUPLICATE", "分组原因统计typed业务键重复"
        )
    parent_ids = list(ids_by_key.values())
    cursor.execute(
        f"SELECT cohort_group_fact_id,reason_scope,quality_status,reason_code,board_relation_count "
        f"FROM {_REASON_TABLE} WHERE cohort_group_fact_id=ANY(%s::bigint[])",
        (parent_ids,),
    )
    existing_reasons = {
        (fact_id, scope, status, code): count
        for fact_id, scope, status, code, count in cursor.fetchall()
    }
    if existing and existing_reasons != expected_reasons:
        raise SectorPulseCohortPgError(
            "PULSE_COHORT_REASON_CONTENT_CONFLICT", "同一分组原因业务键已有不同计数"
        )
    inserted_reasons = 0
    if not existing and expected_reasons:
        cursor.executemany(
            f"INSERT INTO {_REASON_TABLE} "
            "(cohort_group_fact_id,reason_scope,quality_status,reason_code,board_relation_count) "
            "VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            [
                (*key, count)
                for key, count in sorted(
                    expected_reasons.items(), key=lambda item: item[0]
                )
            ],
        )
        inserted_reasons = cursor.rowcount
    cursor.execute(
        f"SELECT cohort_group_fact_id,reason_scope,quality_status,reason_code,board_relation_count "
        f"FROM {_REASON_TABLE} WHERE cohort_group_fact_id=ANY(%s::bigint[])",
        (parent_ids,),
    )
    actual_reasons = {
        (fact_id, scope, status, code): count
        for fact_id, scope, status, code, count in cursor.fetchall()
    }
    if actual_reasons != expected_reasons:
        raise SectorPulseCohortPgError(
            "PULSE_COHORT_REASON_READBACK_MISMATCH", "分组原因写入后同事务读回不一致"
        )
    return {
        "group_rows_expected": len(group_rows),
        "group_rows_inserted": inserted_groups,
        "reason_rows_expected": len(reason_rows),
        "reason_rows_inserted": inserted_reasons,
        "same_input_retry": bool(existing)
        and inserted_groups == 0
        and inserted_reasons == 0,
    }


def _percent_4dp(value: Fraction | None) -> str | None:
    if value is None:
        return None
    from decimal import Decimal, ROUND_HALF_UP, localcontext

    with localcontext() as context:
        context.prec = 60
        return str(
            (Decimal(value.numerator * 100) / Decimal(value.denominator)).quantize(
                Decimal("0.0001"), rounding=ROUND_HALF_UP
            )
        )


def _stored_group_payload(
    row: Mapping[str, Any], reasons: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    observed = int(row["observed_board_count"])
    denominator = int(row["board_relation_denominator"])
    member_observed = int(row["observed_member_count_sum"])
    member_known_denominator = int(row["evaluable_fixed_member_denominator_sum"])
    distribution = {}
    for label, prefix in (
        ("mean", "mean"),
        ("median", "median"),
        ("minimum", "minimum"),
        ("maximum", "maximum"),
    ):
        value = _fraction(
            row[f"{prefix}_return_numerator"],
            row[f"{prefix}_return_denominator"],
        )
        distribution[f"{label}_return"] = str(value) if value is not None else None
        distribution[f"{label}_return_percent_exact"] = (
            str(value * 100) if value is not None else None
        )
        distribution[f"{label}_return_percent_4dp"] = _percent_4dp(value)
    distribution.update(
        {
            "status": "AVAILABLE"
            if observed
            else row["group_status"]
            if row["group_status"] in {"NOT_DUE", "NOT_COMPUTED", "INCOMPLETE"}
            else "NOT_COMPARABLE",
            "sample_scope": "OBSERVED_COMPLETE_FIXED_BASKETS_ONLY",
            "sample_count": observed,
            "positive_board_count": row["positive_board_count"],
            "nonpositive_board_count": row["nonpositive_board_count"],
            "positive_board_relation_count": row["positive_board_count"],
            "nonpositive_board_relation_count": row["nonpositive_board_count"],
        }
    )
    status_counts = {
        "OBSERVED": observed,
        "UNKNOWN": int(row["unknown_board_count"]),
        "NOT_COMPUTED": int(row["not_computed_board_count"]),
        "NOT_DUE": int(row["not_due_board_count"]),
        "INCOMPLETE": int(row["incomplete_board_count"]),
    }
    reason_payload = [
        {
            "reason_scope": item["reason_scope"],
            "quality_status": item["quality_status"],
            "reason_code": item["reason_code"],
            "board_relation_count": int(item["board_relation_count"]),
        }
        for item in reasons
    ]
    return {
        "status": row["group_status"],
        "cohort_axis": row["cohort_axis"],
        "cohort_state": row["cohort_state"],
        "pulse_state": row["cohort_state"]
        if row["cohort_axis"] == "PULSE_STATE"
        else None,
        "horizon_sessions": int(row["horizon_sessions"]),
        "target_session": row["target_session"].isoformat()
        if row["target_session"] is not None
        else None,
        "total_board_relations": denominator,
        "outcome_status_counts": status_counts,
        "partition_closed": sum(status_counts.values()) == denominator,
        "complete_basket_coverage": {
            "observed_board_relations": observed,
            "all_board_relations": denominator,
            "percent_4dp": _percent_4dp(Fraction(observed, denominator))
            if denominator
            else None,
        },
        "fixed_member_quality": {
            "fixed_member_denominator_sum": int(row["fixed_member_denominator_sum"]),
            "evaluable_fixed_member_denominator_sum": member_known_denominator,
            "observed_member_count_sum": member_observed,
            "unknown_member_count_sum": int(row["unknown_member_count_sum"]),
            "coverage_percent_4dp": _percent_4dp(
                Fraction(member_observed, member_known_denominator)
            )
            if member_known_denominator
            else None,
        },
        "fixed_member_denominator_sum": int(row["fixed_member_denominator_sum"]),
        "observed_member_count_sum": member_observed,
        "unknown_member_count_sum": int(row["unknown_member_count_sum"]),
        "unknown_reason_counts": [
            item for item in reason_payload if item["quality_status"] == "UNKNOWN"
        ],
        "incomplete_reason_counts": [
            item for item in reason_payload if item["quality_status"] == "INCOMPLETE"
        ],
        "distribution": distribution,
        "reason_counts": reason_payload,
    }


def read_source_sector_pulse_cohort_groups(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int,
    profile_revision: str,
    snapshot_ids: Mapping[str, int],
    expected_board_counts: Mapping[str, int],
    calendar_id: str,
    calendar_revision: str,
    board_kind: str = "ALL",
) -> dict[str, Any]:
    """只读离线已存的T0分组事实，不在GET中遍历板块或重算分布。"""
    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
        or any(type(value) is not int or value <= 0 for value in (source_capture_id,))
        or not isinstance(profile_revision, str)
        or set(snapshot_ids) != set(_KINDS)
        or set(expected_board_counts) != set(_KINDS)
        or any(
            type(value) is not int or value < 0
            for value in expected_board_counts.values()
        )
        or board_kind not in {"ALL", *_KINDS}
    ):
        raise SectorPulseCohortPgError(
            "PULSE_COHORT_READ_ARGUMENT_INVALID", "板块PULSE分组查询业务键无效"
        )
    selected_kinds = _KINDS if board_kind == "ALL" else (board_kind,)
    by_kind: dict[str, Any] = {}
    with source_raw._transaction(config, write=False, statement_timeout_ms=12000) as (
        _connection,
        cursor,
        _identity,
    ):
        rows = _read_group_batch(
            cursor,
            t0_session=target_session,
            research_cutoff=research_cutoff,
            t0_source_capture_id=source_capture_id,
            t0_profile_revision=profile_revision,
            snapshot_ids={kind: snapshot_ids[kind] for kind in selected_kinds},
            calendar_id=calendar_id,
            calendar_revision=calendar_revision,
        )
        fact_ids = [row["id"] for row in rows]
        reasons_by_fact: dict[int, list[dict[str, Any]]] = {
            fact_id: [] for fact_id in fact_ids
        }
        if fact_ids:
            cursor.execute(
                f"SELECT cohort_group_fact_id,reason_scope,quality_status,reason_code,board_relation_count "
                f"FROM {_REASON_TABLE} WHERE cohort_group_fact_id=ANY(%s::bigint[]) "
                "ORDER BY cohort_group_fact_id,reason_scope,quality_status,reason_code",
                (fact_ids,),
            )
            for fact_id, scope, status, code, count in cursor.fetchall():
                reasons_by_fact[fact_id].append(
                    {
                        "reason_scope": scope,
                        "quality_status": status,
                        "reason_code": code,
                        "board_relation_count": count,
                    }
                )
    if not rows:
        return {
            "status": "NOT_COMPUTED",
            "reason_codes": ["SECTOR_PULSE_COHORT_GROUP_FACTS_NOT_SAVED"],
            "scope": "SAME_T0_SOURCE_VERSION_SNAPSHOT_KIND_AND_HORIZON",
            "unit": "BOARD_RELATION; OVERLAPPING_MEMBERS_AND_DATES_ARE_NOT_INDEPENDENT",
            "research_mode": "NON_PIT_RETROSPECTIVE",
            "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
            "price_coordinate": "FORWARD_ADJUSTED",
            "predictive_validation": "NO_PREDICTIVE_VALIDATION",
            "by_kind": {
                kind: {"status": "NOT_COMPUTED", "horizons": []}
                for kind in selected_kinds
            },
        }

    for kind in selected_kinds:
        kind_rows = [row for row in rows if row["board_kind"] == kind]
        expected_rows = sum(len(states) for states in _AXIS_STATES.values()) * len(
            _HORIZONS
        )
        identities = {
            (row["cohort_axis"], row["cohort_state"], int(row["horizon_sessions"]))
            for row in kind_rows
        }
        expected_identities = {
            (axis, state, horizon)
            for axis, states in _AXIS_STATES.items()
            for state in states
            for horizon in _HORIZONS
        }
        reasons_lookup = reasons_by_fact
        horizon_groups = []
        kind_complete = (
            len(kind_rows) == expected_rows and identities == expected_identities
        )
        for horizon in _HORIZONS:
            axis_payload: dict[str, dict[str, Any]] = {}
            for axis, states in _AXIS_STATES.items():
                groups = {
                    row["cohort_state"]: _stored_group_payload(
                        row, reasons_lookup[row["id"]]
                    )
                    for row in kind_rows
                    if row["cohort_axis"] == axis
                    and int(row["horizon_sessions"]) == horizon
                }
                if (
                    set(groups) != set(states)
                    or sum(group["total_board_relations"] for group in groups.values())
                    != expected_board_counts[kind]
                ):
                    kind_complete = False
                axis_payload[axis] = groups
            pulse_states = axis_payload["PULSE_STATE"]
            true_group = pulse_states.get("TRUE", {})
            false_group = pulse_states.get("FALSE", {})
            true_coverage = true_group.get("complete_basket_coverage", {})
            false_coverage = false_group.get("complete_basket_coverage", {})
            true_mean = true_group.get("distribution", {}).get("mean_return")
            false_mean = false_group.get("distribution", {}).get("mean_return")
            try:
                coverage_difference = (
                    Fraction(
                        int(true_coverage["observed_board_relations"]),
                        int(true_coverage["all_board_relations"]),
                    )
                    - Fraction(
                        int(false_coverage["observed_board_relations"]),
                        int(false_coverage["all_board_relations"]),
                    )
                    if true_coverage.get("all_board_relations")
                    and false_coverage.get("all_board_relations")
                    else None
                )
                mean_difference = (
                    Fraction(true_mean) - Fraction(false_mean)
                    if true_mean is not None and false_mean is not None
                    else None
                )
            except (TypeError, ValueError, ZeroDivisionError):
                coverage_difference = None
                mean_difference = None
            horizon_groups.append(
                {
                    "horizon_sessions": horizon,
                    "total_board_relations": expected_board_counts[kind],
                    "true_minus_false_observed_coverage_percentage_points": _percent_4dp(
                        coverage_difference
                    ),
                    "true_minus_false_mean_return_difference_percentage_points": _percent_4dp(
                        mean_difference
                    ),
                    "pulse_state_partition_closed": (
                        sum(
                            group["total_board_relations"]
                            for group in axis_payload["PULSE_STATE"].values()
                        )
                        == expected_board_counts[kind]
                    ),
                    "transition_partition_closed": (
                        sum(
                            group["total_board_relations"]
                            for group in axis_payload["ADJACENT_TRANSITION"].values()
                        )
                        == expected_board_counts[kind]
                    ),
                    "pulse_states": axis_payload["PULSE_STATE"],
                    "cohorts": axis_payload,
                }
            )
        by_kind[kind] = {
            "status": "AVAILABLE" if kind_complete else "INCOMPLETE",
            "expected_board_count": expected_board_counts[kind],
            "group_fact_count": len(kind_rows),
            "horizons": horizon_groups,
        }
    overall = (
        "AVAILABLE"
        if all(item["status"] == "AVAILABLE" for item in by_kind.values())
        else "INCOMPLETE"
    )
    return {
        "status": overall,
        "scope": "SAME_T0_SOURCE_VERSION_SNAPSHOT_KIND_AND_HORIZON",
        "unit": "BOARD_RELATION; OVERLAPPING_MEMBERS_AND_DATES_ARE_NOT_INDEPENDENT",
        "research_mode": "NON_PIT_RETROSPECTIVE",
        "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
        "price_coordinate": "FORWARD_ADJUSTED",
        "predictive_validation": "NO_PREDICTIVE_VALIDATION",
        "by_kind": by_kind,
    }


__all__ = [
    "SectorPulseCohortPgError",
    "persist_sector_pulse_cohort_rows",
    "prepare_sector_pulse_cohort_rows",
    "read_source_sector_pulse_cohort_groups",
]
