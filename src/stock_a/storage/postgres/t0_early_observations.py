"""固定stock_a.stock内T0板内观察事实的输入、写入与读取。"""

from __future__ import annotations

from datetime import date
import re
from typing import Any, Mapping, Sequence

from stock_a.contracts.profile_revision import PROFILE_REVISION_PATTERN
from stock_a.storage.postgres import source_raw


class T0EarlyObservationPgError(ValueError):
    """T0板内观察PG输入、唯一键或读回不符合合同。"""

    preserve_transaction_error = True

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


_SYMBOL = re.compile(r"^(SH|SZ)\.[0-9]{6}$")
_SNAPSHOT_ROLES = {
    "INDUSTRY": "TDX_TDXHY_CFG",
    "CONCEPT": "TDX_INFOHARBOR_BLOCK_DAT",
}
_SEED_SCOPE_FIELDS = (
    "profile_revision",
    "producer_contract",
    "producer_version",
    "history_session_count",
    "calendar_id",
    "calendar_revision",
    "research_mode",
    "price_coordinate",
)
_OBSERVATION_COLUMNS = (
    "enrollment_id",
    "target_session",
    "source_package_id",
    "source_capture_id",
    "source_symbol",
    "d0_fact_id",
    "rule_fact_id",
    "seed_fact_id",
    "membership_snapshot_id",
    "catalog_id",
    "board_kind",
    "board_native_id",
    "board_context_key",
    "membership_status",
    "membership_mode",
    "pulse_fact_id",
    "pulse_status",
    "pulse_state",
    "pulse_transition_id",
    "transition_status",
    "transition_state",
    "transition_reason_codes",
    "snapshot_member_count",
    "source_pool_member_count",
    "rps5_quality",
    "rps5_reason",
    "rps5_rank_first",
    "rps5_rank_last",
    "rps5_valid_member_count",
    "rps5_unknown_member_count",
    "rps5_out_of_source_pool_member_count",
    "rps5_position",
    "rps20_quality",
    "rps20_reason",
    "rps20_rank_first",
    "rps20_rank_last",
    "rps20_valid_member_count",
    "rps20_unknown_member_count",
    "rps20_out_of_source_pool_member_count",
    "rps20_position",
    "relative_position",
    "alignment_state",
    "research_case",
    "support_codes",
    "counter_codes",
    "unknown_codes",
    "next_check_code",
    "hypothesis_version",
    "evidence_revision_key",
    "research_mode",
    "price_coordinate",
)


def read_source_t0_early_observation_inputs(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    source_package_id: int,
    source_capture_id: int,
    expected_source_count: int,
    snapshot_ids: Mapping[str, int],
) -> dict[str, Any]:
    """仅读取指定来源版本的T0登记、Seed和快照板块关系。"""
    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
        or type(source_package_id) is not int
        or source_package_id <= 0
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or type(expected_source_count) is not int
        or expected_source_count <= 0
        or not isinstance(snapshot_ids, Mapping)
        or set(snapshot_ids) != set(_SNAPSHOT_ROLES)
        or any(
            type(snapshot_ids[kind]) is not int or snapshot_ids[kind] <= 0
            for kind in _SNAPSHOT_ROLES
        )
        or snapshot_ids["INDUSTRY"] == snapshot_ids["CONCEPT"]
    ):
        raise T0EarlyObservationPgError(
            "T0_EARLY_INPUT_SCOPE_INVALID", "T0观察目标日、来源版本或成员快照键无效"
        )

    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT s.profile_revision,s.producer_contract,s.producer_version,
                      s.history_session_count,s.calendar_id,s.calendar_revision,
                      s.research_mode,s.price_coordinate,count(*)::bigint,
                      count(DISTINCT s.source_symbol)::bigint
               FROM source_pool_seed_day_facts s
               WHERE s.target_session=%s AND s.source_package_id=%s
                 AND s.source_capture_id=%s
               GROUP BY s.profile_revision,s.producer_contract,s.producer_version,
                        s.history_session_count,s.calendar_id,s.calendar_revision,
                        s.research_mode,s.price_coordinate""",
            (target_session, source_package_id, source_capture_id),
        )
        seed_scopes = cursor.fetchall()
        if (
            len(seed_scopes) != 1
            or int(seed_scopes[0][-2]) != expected_source_count
            or int(seed_scopes[0][-1]) != expected_source_count
        ):
            raise T0EarlyObservationPgError(
                "T0_EARLY_SEED_SCOPE_NOT_CLOSED",
                "指定来源版本的Seed/Profile分区不唯一或未闭合到Raw来源池分母",
            )
        seed_scope = dict(zip(_SEED_SCOPE_FIELDS, seed_scopes[0][:-2], strict=True))

        cursor.execute(
            """SELECT snapshot_id,source_kind,research_mode,source_quality,rows_declared
               FROM tdx_sector_source_snapshots
               WHERE snapshot_id=ANY(%s) ORDER BY snapshot_id""",
            ([snapshot_ids["INDUSTRY"], snapshot_ids["CONCEPT"]],),
        )
        snapshot_rows = cursor.fetchall()
        snapshot_by_id = {int(row[0]): row for row in snapshot_rows}
        for kind, expected_source_kind in _SNAPSHOT_ROLES.items():
            snapshot_id = snapshot_ids[kind]
            row = snapshot_by_id.get(snapshot_id)
            if (
                row is None
                or row[1] != expected_source_kind
                or row[2] != "NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT"
            ):
                raise T0EarlyObservationPgError(
                    "T0_EARLY_SNAPSHOT_IDENTITY_MISMATCH",
                    f"{kind}成员快照身份、来源或NON_PIT语义不匹配",
                )

        cursor.execute(
            """SELECT count(*)::bigint
               FROM source_research_enrollments e
               LEFT JOIN source_d0_launch_day_facts d ON d.id=e.d0_fact_id
               LEFT JOIN source_rule_evaluations r ON r.id=e.rule_fact_id
               WHERE COALESCE(d.target_session,r.target_session)=%s
                 AND COALESCE(d.source_package_id,r.source_package_id)=%s
                 AND COALESCE(d.target_capture_id,r.source_capture_id)=%s""",
            (target_session, source_package_id, source_capture_id),
        )
        expected_registration_count = int(cursor.fetchone()[0])

        cursor.execute(
            """WITH parents AS MATERIALIZED (
    SELECT e.id AS enrollment_id,e.enrollment_kind,e.reason_code,
           e.d0_fact_id AS parent_d0_fact_id,e.rule_fact_id,
           COALESCE(d.target_session,r.target_session) AS target_session,
           COALESCE(d.source_package_id,r.source_package_id) AS source_package_id,
           COALESCE(d.target_capture_id,r.source_capture_id) AS source_capture_id,
           COALESCE(d.source_symbol,r.source_symbol) AS source_symbol
    FROM source_research_enrollments e
    LEFT JOIN source_d0_launch_day_facts d ON d.id=e.d0_fact_id
    LEFT JOIN source_rule_evaluations r ON r.id=e.rule_fact_id
    WHERE COALESCE(d.target_session,r.target_session)=%s
      AND COALESCE(d.source_package_id,r.source_package_id)=%s
      AND COALESCE(d.target_capture_id,r.source_capture_id)=%s
), memberships AS MATERIALIZED (
    SELECT m.source_symbol,c.id AS catalog_id,c.snapshot_id,
           c.board_kind,c.board_native_id,
           min(m.membership_quality) AS membership_quality
    FROM tdx_sector_members m
    JOIN tdx_sector_catalog c ON c.id=m.catalog_id
    WHERE c.snapshot_id=ANY(%s)
    GROUP BY m.source_symbol,c.id,c.snapshot_id,c.board_kind,c.board_native_id
), d0_scope AS MATERIALIZED (
    SELECT d.source_symbol,count(*)::integer AS fact_count,
           min(d.id) AS fact_id,min(d.transition) AS transition,
           min(d.before_state) AS before_state,min(d.after_state) AS after_state
    FROM source_d0_launch_day_facts d
    WHERE d.target_session=%s AND d.source_package_id=%s
      AND d.target_capture_id=%s
    GROUP BY d.source_symbol
)
SELECT p.enrollment_id,p.enrollment_kind,p.reason_code,
       p.parent_d0_fact_id,p.rule_fact_id,p.target_session,
       p.source_package_id,p.source_capture_id,p.source_symbol,
       s.id AS seed_fact_id,s.profile_revision,s.producer_contract,
       s.producer_version,s.history_session_count,s.calendar_id,
       s.calendar_revision,s.research_mode AS seed_research_mode,
       s.price_coordinate AS seed_price_coordinate,
       s.rps5_quality,s.rps5_reason,s.rps20_quality,s.rps20_reason,
       s.rps5_delta3_quality,s.rps5_delta3_reason,
       CASE WHEN d.fact_count=1 THEN d.fact_id END AS d0_fact_id,
       CASE WHEN d.fact_count=1 THEN d.transition END AS d0_transition,
       CASE WHEN d.fact_count=1 THEN d.before_state END AS d0_before_state,
       CASE WHEN d.fact_count=1 THEN d.after_state END AS d0_after_state,
       d.fact_count AS d0_context_fact_count,
       m.catalog_id,m.snapshot_id,m.board_kind,m.board_native_id,
       m.membership_quality
FROM parents p
JOIN source_pool_seed_day_facts s
  ON s.target_session=p.target_session
 AND s.source_package_id=p.source_package_id
 AND s.source_capture_id=p.source_capture_id
 AND s.source_symbol=p.source_symbol
LEFT JOIN d0_scope d ON d.source_symbol=p.source_symbol
LEFT JOIN memberships m ON m.source_symbol=p.source_symbol
ORDER BY p.enrollment_id,m.board_kind,m.board_native_id COLLATE "C" """,
            (
                target_session,
                source_package_id,
                source_capture_id,
                [snapshot_ids["INDUSTRY"], snapshot_ids["CONCEPT"]],
                target_session,
                source_package_id,
                source_capture_id,
            ),
        )
        raw_rows = cursor.fetchall()
        column_names = [item.name for item in cursor.description]
        registrations_by_id: dict[int, dict[str, Any]] = {}
        for values in raw_rows:
            record = dict(zip(column_names, values, strict=True))
            enrollment_id = int(record["enrollment_id"])
            registration = registrations_by_id.get(enrollment_id)
            if registration is None:
                registration = {
                    key: record[key]
                    for key in column_names
                    if key
                    not in {
                        "catalog_id",
                        "snapshot_id",
                        "board_kind",
                        "board_native_id",
                        "membership_quality",
                    }
                }
                registrations_by_id[enrollment_id] = registration
                registration["memberships"] = []
            if record["catalog_id"] is not None:
                registration["memberships"].append(
                    {
                        "catalog_id": int(record["catalog_id"]),
                        "snapshot_id": int(record["snapshot_id"]),
                        "board_kind": record["board_kind"],
                        "board_native_id": record["board_native_id"],
                        "membership_quality": record["membership_quality"],
                    }
                )

    if not registrations_by_id:
        raise T0EarlyObservationPgError(
            "T0_EARLY_NO_STORED_ENROLLMENTS", "指定目标日和来源版本没有已存T0登记"
        )
    if len(registrations_by_id) != expected_registration_count:
        raise T0EarlyObservationPgError(
            "T0_EARLY_ENROLLMENT_SEED_LINK_MISSING",
            "有已存T0登记未能精确联接到同版本Seed事实；拒绝静默遗漏登记",
        )
    if any(
        item["d0_context_fact_count"] not in {None, 0, 1}
        for item in registrations_by_id.values()
    ):
        raise T0EarlyObservationPgError(
            "T0_EARLY_D0_CONTEXT_AMBIGUOUS", "同一来源代码在指定D0来源版本内有多个事实"
        )
    if any(
        item["seed_research_mode"] != "NON_PIT_RETROSPECTIVE"
        or item["seed_price_coordinate"] != "RAW_UNADJUSTED"
        for item in registrations_by_id.values()
    ):
        raise T0EarlyObservationPgError(
            "T0_EARLY_SEED_SEMANTICS_MISMATCH", "Seed研究模式或价格坐标不符合本轮合同"
        )

    return {
        "target_session": target_session,
        "source_package_id": source_package_id,
        "source_capture_id": source_capture_id,
        "expected_source_count": expected_source_count,
        "seed_scope": seed_scope,
        "snapshots": {
            kind: {
                "snapshot_id": snapshot_ids[kind],
                "source_kind": snapshot_by_id[snapshot_ids[kind]][1],
                "research_mode": snapshot_by_id[snapshot_ids[kind]][2],
                "source_quality": snapshot_by_id[snapshot_ids[kind]][3],
                "rows_declared": int(snapshot_by_id[snapshot_ids[kind]][4]),
            }
            for kind in _SNAPSHOT_ROLES
        },
        "registrations": list(registrations_by_id.values()),
        "registration_count": len(registrations_by_id),
        "unique_symbol_count": len(
            {item["source_symbol"] for item in registrations_by_id.values()}
        ),
        "membership_relation_count": sum(
            len(item["memberships"]) for item in registrations_by_id.values()
        ),
    }


def read_source_t0_pulse_transition_facts(
    config: Mapping[str, Any],
    *,
    target_pulse_ids: Sequence[int],
    previous_session: date,
    target_session: date,
    source_package_id: int,
    previous_source_capture_id: int | None,
    target_source_capture_id: int,
    previous_profile_revision: str | None,
    target_profile_revision: str,
) -> dict[int, dict[str, Any]]:
    """读取绑定两端独立来源捕获和Profile修订的既存PULSE转移。"""
    pulse_ids = tuple(sorted(set(target_pulse_ids)))
    if (
        type(previous_session) is not date
        or type(target_session) is not date
        or previous_session >= target_session
        or type(source_package_id) is not int
        or source_package_id <= 0
        or type(target_source_capture_id) is not int
        or target_source_capture_id <= 0
        or any(type(item) is not int or item <= 0 for item in pulse_ids)
        or len(pulse_ids) > 2000
    ):
        raise T0EarlyObservationPgError(
            "T0_EARLY_TRANSITION_SCOPE_INVALID", "相邻PULSE转移读取键无效"
        )
    if (
        not pulse_ids
        or previous_source_capture_id is None
        or previous_profile_revision is None
    ):
        return {}
    if type(previous_source_capture_id) is not int or previous_source_capture_id <= 0:
        raise T0EarlyObservationPgError(
            "T0_EARLY_PREVIOUS_CAPTURE_INVALID", "前一Raw捕获ID无效"
        )
    if (
        not isinstance(previous_profile_revision, str)
        or re.fullmatch(PROFILE_REVISION_PATTERN, previous_profile_revision) is None
        or not isinstance(target_profile_revision, str)
        or re.fullmatch(PROFILE_REVISION_PATTERN, target_profile_revision) is None
    ):
        raise T0EarlyObservationPgError(
            "T0_EARLY_PROFILE_REVISION_INVALID", "相邻两端Profile修订必须分别显式绑定"
        )

    with source_raw._transaction(config, write=False, statement_timeout_ms=15000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT t.target_pulse_id,t.id,t.transition_state,
                      t.previous_pulse_state,t.target_pulse_state,
                      t.previous_reason_codes,t.target_reason_codes,
                      t.calculation_status,t.transition_contract,
                      t.previous_pulse_id
               FROM source_sector_pulse_adjacent_transitions t
               JOIN source_sector_cross_period_pulse_facts target
                 ON target.id=t.target_pulse_id
               JOIN source_sector_cross_period_pulse_facts previous
                 ON previous.id=t.previous_pulse_id
               WHERE t.target_pulse_id=ANY(%s)
                 AND previous.target_session=%s
                 AND previous.source_package_id=%s
                 AND previous.source_capture_id=%s
                 AND previous.profile_revision=%s
                 AND target.target_session=%s
                 AND target.source_package_id=%s
                 AND target.source_capture_id=%s
                 AND target.profile_revision=%s
                 AND t.transition_contract='SOURCE_CROSS_PERIOD_PULSE_TRANSITION_R1'
                 AND t.research_mode='NON_PIT_RETROSPECTIVE'
                 AND t.price_coordinate='RAW_UNADJUSTED'
               ORDER BY t.target_pulse_id,t.id""",
            (
                list(pulse_ids),
                previous_session,
                source_package_id,
                previous_source_capture_id,
                previous_profile_revision,
                target_session,
                source_package_id,
                target_source_capture_id,
                target_profile_revision,
            ),
        )
        rows = cursor.fetchall()

    grouped: dict[int, list[tuple[Any, ...]]] = {}
    for row in rows:
        grouped.setdefault(int(row[0]), []).append(row)
    output = {}
    for pulse_id, candidates in grouped.items():
        if len(candidates) != 1:
            output[pulse_id] = {
                "status": "UNKNOWN",
                "reason_codes": ["PULSE_TRANSITION_FACT_AMBIGUOUS"],
            }
            continue
        (
            _,
            transition_id,
            state,
            previous_state,
            target_state,
            previous_reasons,
            target_reasons,
            calculation_status,
            contract,
            previous_pulse_id,
        ) = candidates[0]
        if calculation_status != "COMPLETE":
            output[pulse_id] = {
                "status": "INCOMPLETE",
                "reason_codes": ["PULSE_TRANSITION_PARTITION_INCOMPLETE"],
            }
            continue
        output[pulse_id] = {
            "status": "AVAILABLE",
            "transition_id": int(transition_id),
            "transition_state": state,
            "previous_state": previous_state,
            "target_state": target_state,
            "previous_reason_codes": list(previous_reasons or ()),
            "target_reason_codes": list(target_reasons or ()),
            "transition_contract": contract,
            "previous_pulse_id": int(previous_pulse_id),
            "reason_codes": [],
        }
    return output


def persist_source_t0_early_observation_facts(
    config: Mapping[str, Any], *, rows: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    """单事务幂等保存并逐键独立核对全部T0板内观察行。"""
    if not rows:
        raise T0EarlyObservationPgError(
            "T0_EARLY_ROWS_EMPTY", "没有可保存的T0板内观察事实"
        )
    normalized = []
    identity_keys = set()
    for row in rows:
        if not isinstance(row, Mapping) or any(
            name not in row for name in _OBSERVATION_COLUMNS
        ):
            raise T0EarlyObservationPgError(
                "T0_EARLY_ROW_INVALID", "T0板内观察行缺少正式typed字段"
            )
        values = tuple(row[name] for name in _OBSERVATION_COLUMNS)
        key = (
            row["enrollment_id"],
            row["board_context_key"],
            row["evidence_revision_key"],
        )
        if key in identity_keys:
            raise T0EarlyObservationPgError(
                "T0_EARLY_ROW_DUPLICATED", "同一T0登记和证据修订出现重复板块上下文"
            )
        identity_keys.add(key)
        if (
            type(row["enrollment_id"]) is not int
            or row["enrollment_id"] <= 0
            or type(row["target_session"]) is not date
            or not isinstance(row["source_symbol"], str)
            or _SYMBOL.fullmatch(row["source_symbol"]) is None
            or row["hypothesis_version"] != "EARLY_BOARD_RELATIVE_T0_R1"
            or row["research_mode"] != "NON_PIT_EXPLORATORY"
            or row["price_coordinate"] != "RAW_UNADJUSTED"
        ):
            raise T0EarlyObservationPgError(
                "T0_EARLY_ROW_SEMANTICS_INVALID", "观察行身份、版本或NON_PIT语义无效"
            )
        normalized.append(values)

    columns = ",".join(_OBSERVATION_COLUMNS)
    placeholders = ",".join(["%s"] * len(_OBSERVATION_COLUMNS))
    insert_sql = (
        f"INSERT INTO source_t0_early_observation_facts ({columns}) "
        f"VALUES ({placeholders}) ON CONFLICT DO NOTHING"
    )
    readback_columns = ",".join(f"o.{name}" for name in _OBSERVATION_COLUMNS)
    expected_order = sorted(
        normalized,
        key=lambda item: (item[0], item[12], item[48]),
    )
    with source_raw._transaction(config, write=True, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        cursor.executemany(insert_sql, normalized)
        inserted = int(cursor.rowcount)
        cursor.execute(
            f"""WITH expected(enrollment_id,board_context_key,evidence_revision_key) AS (
    SELECT * FROM unnest(%s::bigint[],%s::text[],%s::text[])
)
SELECT {readback_columns} FROM expected e
JOIN source_t0_early_observation_facts o
  ON o.enrollment_id=e.enrollment_id
 AND o.board_context_key=e.board_context_key
 AND o.evidence_revision_key=e.evidence_revision_key
ORDER BY o.enrollment_id,o.board_context_key COLLATE "C",o.evidence_revision_key COLLATE "C" """,
            (
                [item[0] for item in expected_order],
                [item[12] for item in expected_order],
                [item[48] for item in expected_order],
            ),
        )
        readback = list(cursor.fetchall())
        if readback != expected_order:
            raise T0EarlyObservationPgError(
                "T0_EARLY_READBACK_MISMATCH",
                "T0板内观察事实同事务读回与期望内容不一致；事务已回滚",
            )

    return {
        "status": "PERSISTED",
        "rows_expected": len(normalized),
        "rows_inserted": inserted,
        "readback_rows": len(readback),
        "readback_status": "AVAILABLE",
        "database_write_attempted": True,
        "ddl_executed": False,
        "dml_executed": True,
        "transaction": "SERIALIZABLE",
    }


_READ_SELECT = """WITH latest AS (
    SELECT DISTINCT ON (o.enrollment_id,o.board_context_key)
           o.*
    FROM source_t0_early_observation_facts o
    WHERE o.enrollment_id=ANY(%s)
    ORDER BY o.enrollment_id,o.board_context_key,o.recorded_at DESC,o.id DESC
)
SELECT o.id,o.enrollment_id,e.enrollment_kind,e.reason_code AS enrollment_reason_code,
       o.target_session,o.source_package_id,o.source_capture_id,o.source_symbol,
       o.d0_fact_id,o.rule_fact_id,r.rule_id,r.rule_version,r.state AS rule_state,
       d.before_state AS d0_before_state,d.after_state AS d0_after_state,
       d.transition AS d0_transition,o.seed_fact_id,
       o.membership_snapshot_id,o.catalog_id,c.board_kind,c.board_native_id,
       c.board_name,ss.source_quality AS snapshot_source_quality,
       m.membership_quality,o.board_context_key,o.membership_status,o.membership_mode,
       o.pulse_fact_id,o.pulse_status,o.pulse_state,
       p.dq5_pp_numerator::text AS pulse_dq5_numerator,
       p.dq5_pp_denominator::text AS pulse_dq5_denominator,
       p.breadth_delta1_numerator::text AS pulse_breadth_delta1_numerator,
       p.breadth_delta1_denominator::text AS pulse_breadth_delta1_denominator,
       p.pulse_reason_codes,
       o.pulse_transition_id,o.transition_status,o.transition_state,
       o.transition_reason_codes,t.previous_pulse_state,t.target_pulse_state,
       t.previous_pulse_state AS previous_state,
       t.target_pulse_state AS target_state,
       t.previous_reason_codes,t.target_reason_codes,
       o.snapshot_member_count,o.source_pool_member_count,
       o.rps5_quality,o.rps5_reason,o.rps5_rank_first,o.rps5_rank_last,
       o.rps5_valid_member_count,o.rps5_unknown_member_count,
       o.rps5_out_of_source_pool_member_count,o.rps5_position,
       s.rps5_numerator::text AS rps5_numerator,
       s.rps5_denominator::text AS rps5_denominator,
       s.rps5_delta3_numerator::text AS rps5_delta3_numerator,
       s.rps5_delta3_denominator::text AS rps5_delta3_denominator,
       s.rps5_delta3_quality,s.rps5_delta3_reason,
       o.rps20_quality,o.rps20_reason,o.rps20_rank_first,o.rps20_rank_last,
       o.rps20_valid_member_count,o.rps20_unknown_member_count,
       o.rps20_out_of_source_pool_member_count,o.rps20_position,
       s.rps20_numerator::text AS rps20_numerator,
       s.rps20_denominator::text AS rps20_denominator,
       o.relative_position,o.alignment_state,o.research_case,
       o.support_codes,o.counter_codes,o.unknown_codes,o.next_check_code,
       (
           o.research_case IN (
               'D0_PULSE_TRUE_FRONT','D0_PULSE_APPEARED_FRONT'
           )
           OR (
               o.pulse_status='AVAILABLE'
               AND o.pulse_state='TRUE'
               AND o.rps5_position='FRONT_HALF'
               AND o.rps20_position='FRONT_HALF'
           )
       ) AS driver_same_direction_candidate,
       CASE
           WHEN o.research_case IN (
               'D0_PULSE_TRUE_FRONT','D0_PULSE_APPEARED_FRONT'
           ) THEN 'D0_PULSE_SAME_DIRECTION'
           WHEN o.pulse_status='AVAILABLE'
            AND o.pulse_state='TRUE'
            AND o.rps5_position='FRONT_HALF'
            AND o.rps20_position='FRONT_HALF'
           THEN 'PULSE_TRUE_RPS5_RPS20_FRONT'
       END AS driver_candidate_basis,
       NOT (
           o.research_case IN (
               'D0_PULSE_TRUE_FRONT','D0_PULSE_APPEARED_FRONT'
           )
           OR (
               o.pulse_status='AVAILABLE'
               AND o.pulse_state='TRUE'
               AND o.rps5_position='FRONT_HALF'
               AND o.rps20_position='FRONT_HALF'
           )
       ) AS driver_other_board_background,
       ARRAY(
           SELECT DISTINCT code
           FROM unnest(o.counter_codes) AS item(code)
           WHERE code IN (
               'RPS5_BACK_HALF','RPS20_BACK_HALF','RPS5_RPS20_MIXED'
           )
           ORDER BY code
       ) || CASE
           WHEN o.transition_status='AVAILABLE'
            AND o.transition_state='PULSE_CEASED'
           THEN ARRAY['PULSE_CEASED']::text[]
           ELSE ARRAY[]::text[]
       END AS driver_named_conflict_codes,
       o.pulse_state='FALSE' AS driver_pulse_false_background,
       (
           o.membership_status IN ('UNKNOWN','NO_STORED_FACT')
           OR o.pulse_status IN ('UNKNOWN','NOT_COMPUTED')
           OR o.pulse_state IN ('UNKNOWN','NOT_COMPUTED')
           OR o.rps5_position IN ('UNKNOWN','NOT_COMPUTED')
           OR o.rps20_position IN ('UNKNOWN','NOT_COMPUTED')
           OR cardinality(array_remove(
               o.unknown_codes,'PULSE_TRANSITION_NOT_COMPUTED'
           )) > 0
       ) AS driver_has_unknown,
       array_remove(
           o.unknown_codes,'PULSE_TRANSITION_NOT_COMPUTED'
       ) AS driver_unknown_reason_codes,
       o.hypothesis_version,o.evidence_revision_key,o.research_mode,
       o.price_coordinate,o.recorded_at
FROM latest o
JOIN source_research_enrollments e ON e.id=o.enrollment_id
LEFT JOIN source_rule_evaluations r ON r.id=o.rule_fact_id
LEFT JOIN source_d0_launch_day_facts d ON d.id=o.d0_fact_id
JOIN source_pool_seed_day_facts s ON s.id=o.seed_fact_id
LEFT JOIN tdx_sector_catalog c ON c.id=o.catalog_id
LEFT JOIN tdx_sector_source_snapshots ss ON ss.snapshot_id=o.membership_snapshot_id
LEFT JOIN LATERAL (
    SELECT min(member.membership_quality) AS membership_quality
    FROM tdx_sector_members member
    WHERE member.catalog_id=o.catalog_id
      AND member.source_symbol=o.source_symbol
) m ON TRUE
LEFT JOIN source_sector_cross_period_pulse_facts p ON p.id=o.pulse_fact_id
LEFT JOIN source_sector_pulse_adjacent_transitions t ON t.id=o.pulse_transition_id
ORDER BY o.enrollment_id,c.board_kind,c.board_native_id COLLATE "C" """


def _read_observation_rows(
    cursor: Any, enrollment_ids: Sequence[int]
) -> list[dict[str, Any]]:
    cursor.execute(_READ_SELECT, (list(enrollment_ids),))
    names = [item.name for item in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def read_source_t0_early_observations_for_enrollments(
    config: Mapping[str, Any], *, enrollment_ids: Sequence[int]
) -> dict[int, list[dict[str, Any]]]:
    """按当前页面登记ID有界读取最新同版本板块上下文。"""
    ids = tuple(sorted(set(enrollment_ids)))
    if (
        isinstance(enrollment_ids, (str, bytes))
        or len(ids) != len(enrollment_ids)
        or len(ids) > 300
        or any(type(item) is not int or item <= 0 for item in ids)
    ):
        raise T0EarlyObservationPgError(
            "T0_EARLY_READER_IDS_INVALID", "T0观察登记读取范围须为不超过300个不同数字ID"
        )
    if not ids:
        return {}
    with source_raw._transaction(config, write=False, statement_timeout_ms=15000) as (
        _,
        cursor,
        _,
    ):
        rows = _read_observation_rows(cursor, ids)
    result: dict[int, list[dict[str, Any]]] = {item: [] for item in ids}
    for row in rows:
        result[int(row["enrollment_id"])].append(row)
    return result


def read_source_t0_early_observations_for_symbols(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_package_id: int,
    source_capture_id: int,
    source_symbols: Sequence[str],
    board_kind: str | None = None,
    board_native_id: str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """按同一目标日来源版本分页读取个股的T0观察；可限当前下钻板块。"""
    symbols = tuple(sorted(set(source_symbols)))
    if (
        type(target_session) is not date
        or type(source_package_id) is not int
        or source_package_id <= 0
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or isinstance(source_symbols, (str, bytes))
        or len(symbols) != len(source_symbols)
        or len(symbols) > 50
        or any(
            not isinstance(item, str) or _SYMBOL.fullmatch(item) is None
            for item in symbols
        )
        or (board_kind is None) != (board_native_id is None)
        or (board_kind is not None and board_kind not in {"INDUSTRY", "CONCEPT"})
        or (
            board_native_id is not None
            and (not board_native_id or len(board_native_id) > 64)
        )
    ):
        raise T0EarlyObservationPgError(
            "T0_EARLY_SYMBOL_SCOPE_INVALID", "T0个股观察读取范围无效"
        )
    if not symbols:
        return {}

    with source_raw._transaction(config, write=False, statement_timeout_ms=15000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT DISTINCT e.id
               FROM source_research_enrollments e
               LEFT JOIN source_d0_launch_day_facts d ON d.id=e.d0_fact_id
               LEFT JOIN source_rule_evaluations r ON r.id=e.rule_fact_id
               WHERE COALESCE(d.target_session,r.target_session)=%s
                 AND COALESCE(d.source_package_id,r.source_package_id)=%s
                 AND COALESCE(d.target_capture_id,r.source_capture_id)=%s
                 AND COALESCE(d.source_symbol,r.source_symbol)=ANY(%s)
               ORDER BY e.id""",
            (target_session, source_package_id, source_capture_id, list(symbols)),
        )
        enrollment_ids = [int(row[0]) for row in cursor.fetchall()]
        if not enrollment_ids:
            return {symbol: [] for symbol in symbols}
        rows = _read_observation_rows(cursor, enrollment_ids)

    selected = []
    for row in rows:
        if row["target_session"] != target_session:
            continue
        if (
            row["source_package_id"] != source_package_id
            or row["source_capture_id"] != source_capture_id
        ):
            continue
        if row["source_symbol"] not in symbols:
            continue
        if board_kind is not None and (
            row["board_kind"] != board_kind or row["board_native_id"] != board_native_id
        ):
            continue
        selected.append(row)

    result: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in symbols}
    for row in selected:
        result[row["source_symbol"]].append(row)
    return result


def read_source_t0_early_triage_summaries_for_symbols(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_package_id: int,
    source_capture_id: int,
    source_symbols: Sequence[str],
) -> dict[str, dict[str, Any]]:
    """有界汇总T0旧分型与独立板块驱动/早期性解释视角。"""
    symbols = tuple(sorted(set(source_symbols)))
    if (
        type(target_session) is not date
        or type(source_package_id) is not int
        or source_package_id <= 0
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or isinstance(source_symbols, (str, bytes))
        or len(symbols) != len(source_symbols)
        or len(symbols) > 50
        or any(
            not isinstance(item, str) or _SYMBOL.fullmatch(item) is None
            for item in symbols
        )
    ):
        raise T0EarlyObservationPgError(
            "T0_EARLY_SUMMARY_SCOPE_INVALID",
            "T0研判摘要必须限定到不超过50只精确来源代码",
        )
    if not symbols:
        return {}

    with source_raw._transaction(config, write=False, statement_timeout_ms=15000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """WITH latest AS MATERIALIZED (
    SELECT DISTINCT ON (o.enrollment_id,o.board_context_key)
           o.enrollment_id,o.source_symbol,o.seed_fact_id,
           o.membership_snapshot_id,
           o.board_kind,o.board_native_id,o.board_context_key,
           o.membership_mode,o.hypothesis_version,o.research_mode,
           o.price_coordinate,o.research_case,o.pulse_status,o.pulse_state,
           o.transition_status,o.transition_state,o.next_check_code,
           o.membership_status,o.rps5_quality,o.rps5_position,
           o.rps5_rank_first,o.rps5_rank_last,o.rps5_valid_member_count,
           o.rps5_unknown_member_count,o.rps5_out_of_source_pool_member_count,
           o.rps20_quality,o.rps20_position,
           o.rps20_rank_first,o.rps20_rank_last,o.rps20_valid_member_count,
           o.rps20_unknown_member_count,o.rps20_out_of_source_pool_member_count,
           o.counter_codes,o.unknown_codes
    FROM source_t0_early_observation_facts o
    WHERE o.target_session=%s AND o.source_package_id=%s
      AND o.source_capture_id=%s AND o.source_symbol=ANY(%s)
    ORDER BY o.enrollment_id,o.board_context_key,o.recorded_at DESC,o.id DESC
), evidence AS MATERIALIZED (
    SELECT o.*,COALESCE(c.board_name,o.board_native_id) AS board_name,
           o.research_case IN ('D0_PULSE_TRUE_FRONT','D0_PULSE_APPEARED_FRONT')
               AS has_support,
           (
               o.research_case IN (
                   'D0_PULSE_TRUE_FRONT','D0_PULSE_APPEARED_FRONT'
               )
               OR (
                   o.pulse_status='AVAILABLE'
                   AND o.pulse_state='TRUE'
                   AND o.rps5_position='FRONT_HALF'
                   AND o.rps20_position='FRONT_HALF'
               )
           ) AS driver_same_direction_candidate,
           cardinality(o.counter_codes)>0 AS has_counter,
           (cardinality(o.unknown_codes)>0
            OR o.membership_status='UNKNOWN'
            OR o.pulse_status<>'AVAILABLE'
            OR o.pulse_state IN ('UNKNOWN','NOT_COMPUTED')
            OR o.rps5_position IN ('UNKNOWN','NOT_COMPUTED')
            OR o.rps20_position IN ('UNKNOWN','NOT_COMPUTED')) AS has_unknown,
           ARRAY(
               SELECT DISTINCT code
               FROM unnest(o.counter_codes) AS item(code)
               WHERE code IN (
                   'RPS5_BACK_HALF','RPS20_BACK_HALF','RPS5_RPS20_MIXED'
               )
               ORDER BY code
           ) || CASE
               WHEN o.transition_status='AVAILABLE'
                AND o.transition_state='PULSE_CEASED'
               THEN ARRAY['PULSE_CEASED']::text[]
               ELSE ARRAY[]::text[]
           END AS driver_conflict_codes,
           (
               o.membership_status IN ('UNKNOWN','NO_STORED_FACT')
               OR o.pulse_status IN ('UNKNOWN','NOT_COMPUTED')
               OR o.pulse_state IN ('UNKNOWN','NOT_COMPUTED')
               OR o.rps5_quality IN ('UNKNOWN','BAD_VALUE','NOT_APPLICABLE','NOT_COMPUTED')
               OR o.rps20_quality IN ('UNKNOWN','BAD_VALUE','NOT_APPLICABLE','NOT_COMPUTED')
               OR cardinality(array_remove(
                   o.unknown_codes,'PULSE_TRANSITION_NOT_COMPUTED'
               )) > 0
           ) AS driver_unknown
    FROM latest o
    LEFT JOIN tdx_sector_catalog c
      ON c.snapshot_id=o.membership_snapshot_id
     AND c.board_kind=o.board_kind
     AND c.board_native_id=o.board_native_id
), registration AS (
    SELECT enrollment_id,source_symbol,
           bool_or(has_support) AS has_support,
           bool_or(has_counter) AS has_counter,
           bool_or(has_unknown) AS has_unknown
    FROM evidence
    GROUP BY enrollment_id,source_symbol
), symbol_state AS (
    SELECT source_symbol,
           bool_or(has_support) AS has_support,
           bool_or(has_counter) AS has_counter,
           bool_or(has_unknown) AS has_unknown,
           count(*)::integer AS registration_count,
           count(*) FILTER(WHERE has_support AND has_counter)::integer
               AS mixed_registration_count
    FROM registration
    GROUP BY source_symbol
), evidence_counts AS (
    SELECT source_symbol,count(*)::integer AS relation_count,
           count(*) FILTER(WHERE has_support)::integer AS support_relation_count,
           count(*) FILTER(WHERE driver_same_direction_candidate)::integer
               AS driver_support_relation_count,
           count(*) FILTER(WHERE has_counter)::integer AS counter_relation_count,
           count(*) FILTER(WHERE has_unknown)::integer AS unknown_relation_count,
           count(*) FILTER(WHERE NOT driver_same_direction_candidate)::integer
               AS background_relation_count,
           count(*) FILTER(WHERE pulse_state='FALSE')::integer
               AS background_pulse_false_relation_count,
           count(*) FILTER(WHERE cardinality(driver_conflict_codes)>0)::integer
               AS direct_conflict_relation_count,
           count(*) FILTER(WHERE driver_unknown)::integer
               AS driver_unknown_relation_count,
           count(*) FILTER(WHERE transition_status='NOT_COMPUTED')::integer
               AS transition_not_computed_relation_count,
           array_agg(DISTINCT research_case ORDER BY research_case)
               AS research_cases,
           array_agg(DISTINCT next_check_code ORDER BY next_check_code)
               AS next_check_codes,
           array(
               SELECT DISTINCT code
               FROM evidence e2
               CROSS JOIN LATERAL unnest(e2.driver_conflict_codes) AS item(code)
               WHERE e2.source_symbol=e.source_symbol
               ORDER BY code
           ) AS direct_conflict_codes,
           min(hypothesis_version) AS hypothesis_version,
           min(membership_mode) AS membership_mode,
           min(research_mode) AS research_mode,
           min(price_coordinate) AS price_coordinate
    FROM evidence e
    GROUP BY source_symbol
), symbol_boards AS (
    SELECT source_symbol,board_context_key,
           max(board_kind) AS board_kind,
           max(board_native_id) AS board_native_id,
           max(board_name) AS board_name,
           bool_or(has_support) AS has_support,
           bool_or(driver_same_direction_candidate) AS has_driver_support,
           bool_or(driver_same_direction_candidate AND NOT has_support)
               AS has_board_only_support,
           bool_or(NOT driver_same_direction_candidate) AS has_background,
           bool_or(pulse_state='FALSE') AS has_pulse_false,
           bool_or(cardinality(driver_conflict_codes)>0) AS has_direct_conflict,
           bool_or(driver_unknown) AS has_driver_unknown,
           bool_or(has_counter) AS has_counter,
           bool_or(has_unknown) AS has_unknown,
           array_remove(array_agg(DISTINCT pulse_state),NULL) AS pulse_states,
           array_remove(array_agg(DISTINCT code),NULL) AS direct_conflict_codes,
           CASE WHEN count(DISTINCT ROW(
               rps5_position,rps5_rank_first,rps5_rank_last,
               rps5_valid_member_count,rps5_unknown_member_count,
               rps5_out_of_source_pool_member_count
           ))=1 THEN min(rps5_position) END AS rps5_position,
           CASE WHEN count(DISTINCT ROW(
               rps5_position,rps5_rank_first,rps5_rank_last,
               rps5_valid_member_count,rps5_unknown_member_count,
               rps5_out_of_source_pool_member_count
           ))=1 THEN min(rps5_rank_first) END AS rps5_rank_first,
           CASE WHEN count(DISTINCT ROW(
               rps5_position,rps5_rank_first,rps5_rank_last,
               rps5_valid_member_count,rps5_unknown_member_count,
               rps5_out_of_source_pool_member_count
           ))=1 THEN min(rps5_rank_last) END AS rps5_rank_last,
           CASE WHEN count(DISTINCT ROW(
               rps5_position,rps5_rank_first,rps5_rank_last,
               rps5_valid_member_count,rps5_unknown_member_count,
               rps5_out_of_source_pool_member_count
           ))=1 THEN min(rps5_valid_member_count) END AS rps5_valid_member_count,
           CASE WHEN count(DISTINCT ROW(
               rps5_position,rps5_rank_first,rps5_rank_last,
               rps5_valid_member_count,rps5_unknown_member_count,
               rps5_out_of_source_pool_member_count
           ))=1 THEN min(rps5_unknown_member_count) END AS rps5_unknown_member_count,
           CASE WHEN count(DISTINCT ROW(
               rps5_position,rps5_rank_first,rps5_rank_last,
               rps5_valid_member_count,rps5_unknown_member_count,
               rps5_out_of_source_pool_member_count
           ))=1 THEN min(rps5_out_of_source_pool_member_count)
           END AS rps5_out_of_source_pool_member_count,
           CASE WHEN count(DISTINCT ROW(
               rps20_position,rps20_rank_first,rps20_rank_last,
               rps20_valid_member_count,rps20_unknown_member_count,
               rps20_out_of_source_pool_member_count
           ))=1 THEN min(rps20_position) END AS rps20_position,
           CASE WHEN count(DISTINCT ROW(
               rps20_position,rps20_rank_first,rps20_rank_last,
               rps20_valid_member_count,rps20_unknown_member_count,
               rps20_out_of_source_pool_member_count
           ))=1 THEN min(rps20_rank_first) END AS rps20_rank_first,
           CASE WHEN count(DISTINCT ROW(
               rps20_position,rps20_rank_first,rps20_rank_last,
               rps20_valid_member_count,rps20_unknown_member_count,
               rps20_out_of_source_pool_member_count
           ))=1 THEN min(rps20_rank_last) END AS rps20_rank_last,
           CASE WHEN count(DISTINCT ROW(
               rps20_position,rps20_rank_first,rps20_rank_last,
               rps20_valid_member_count,rps20_unknown_member_count,
               rps20_out_of_source_pool_member_count
           ))=1 THEN min(rps20_valid_member_count) END AS rps20_valid_member_count,
           CASE WHEN count(DISTINCT ROW(
               rps20_position,rps20_rank_first,rps20_rank_last,
               rps20_valid_member_count,rps20_unknown_member_count,
               rps20_out_of_source_pool_member_count
           ))=1 THEN min(rps20_unknown_member_count) END AS rps20_unknown_member_count,
           CASE WHEN count(DISTINCT ROW(
               rps20_position,rps20_rank_first,rps20_rank_last,
               rps20_valid_member_count,rps20_unknown_member_count,
               rps20_out_of_source_pool_member_count
           ))=1 THEN min(rps20_out_of_source_pool_member_count)
           END AS rps20_out_of_source_pool_member_count
    FROM evidence e
    LEFT JOIN LATERAL unnest(e.driver_conflict_codes) AS conflict(code) ON TRUE
    WHERE e.board_kind IS NOT NULL AND e.board_native_id IS NOT NULL
    GROUP BY e.source_symbol,e.board_context_key
), board_summary AS (
    SELECT source_symbol,
           count(*) FILTER(WHERE has_support)::integer AS support_board_count,
           count(*) FILTER(WHERE has_driver_support)::integer
               AS driver_support_board_count,
           count(*) FILTER(WHERE has_background)::integer AS background_board_count,
           count(*) FILTER(WHERE has_pulse_false)::integer
               AS background_pulse_false_board_count,
           count(*) FILTER(WHERE has_direct_conflict)::integer
               AS direct_conflict_board_count,
           count(*) FILTER(WHERE has_driver_unknown)::integer
               AS driver_unknown_board_count,
           count(*) FILTER(WHERE has_counter)::integer AS counter_board_count,
           count(*) FILTER(WHERE has_unknown)::integer AS unknown_board_count,
           (array_agg(board_name ORDER BY board_name COLLATE "C")
               FILTER(WHERE has_support))[1:2] AS support_board_names,
           (array_agg(board_name ORDER BY board_name COLLATE "C")
               FILTER(WHERE has_counter))[1:2] AS counter_board_names
    FROM symbol_boards
    GROUP BY source_symbol
), categorized_boards AS (
    SELECT b.*,category
    FROM symbol_boards b
    CROSS JOIN LATERAL (VALUES
        ('SAME_DIRECTION_CANDIDATE',b.has_driver_support),
        ('OTHER_BOARD_BACKGROUND',b.has_background),
        ('NAMED_CONDITION_CONFLICT',b.has_direct_conflict),
        ('CURRENT_EVIDENCE_UNKNOWN',b.has_driver_unknown)
    ) AS category_rows(category,include_row)
    WHERE include_row
), ranked_board_examples AS (
    SELECT b.*,row_number() OVER (
        PARTITION BY source_symbol,category
        ORDER BY board_name COLLATE "C",board_kind COLLATE "C",
                 board_native_id COLLATE "C",board_context_key COLLATE "C"
    ) AS example_rank
    FROM categorized_boards b
), board_examples AS (
    SELECT source_symbol,
           jsonb_agg(jsonb_build_object(
               'board_name',board_name,'pulse_states',pulse_states,
               'candidate_basis',CASE WHEN has_support
                   THEN 'D0_PULSE_SAME_DIRECTION'
                   ELSE 'PULSE_TRUE_RPS5_RPS20_FRONT' END,
               'rps5_rank_first',rps5_rank_first,
               'rps5_rank_last',rps5_rank_last,
               'rps5_valid_member_count',rps5_valid_member_count,
               'rps20_rank_first',rps20_rank_first,
               'rps20_rank_last',rps20_rank_last,
               'rps20_valid_member_count',rps20_valid_member_count
           ) ORDER BY board_name COLLATE "C")
               FILTER(WHERE category='SAME_DIRECTION_CANDIDATE')
               AS same_direction_examples,
           jsonb_agg(jsonb_build_object(
               'board_name',board_name,'pulse_states',pulse_states,
               'board_kind',board_kind,'board_native_id',board_native_id
           ) ORDER BY board_name COLLATE "C")
               FILTER(WHERE category='OTHER_BOARD_BACKGROUND')
               AS background_examples,
           jsonb_agg(jsonb_build_object(
               'board_name',board_name,'board_kind',board_kind,
               'pulse_states',pulse_states,
               'direct_conflict_codes',direct_conflict_codes
           ) ORDER BY board_name COLLATE "C")
               FILTER(WHERE category='NAMED_CONDITION_CONFLICT')
               AS conflict_examples,
           jsonb_agg(jsonb_build_object(
               'board_name',board_name,'pulse_states',pulse_states
           ) ORDER BY board_name COLLATE "C")
               FILTER(WHERE category='CURRENT_EVIDENCE_UNKNOWN')
               AS unknown_examples
    FROM ranked_board_examples
    WHERE example_rank=1
    GROUP BY source_symbol
), reason_summary AS (
    SELECT source_symbol,
           (array_agg(code ORDER BY code COLLATE "C"))[1:4]
               AS unknown_reason_codes
    FROM (
        SELECT DISTINCT e.source_symbol,u.code
        FROM evidence e
        CROSS JOIN LATERAL unnest(e.unknown_codes) AS u(code)
    ) codes
    GROUP BY source_symbol
), driver_reason_summary AS (
    SELECT source_symbol,
           (array_agg(code ORDER BY code COLLATE "C"))[1:4]
               AS driver_unknown_reason_codes
    FROM (
        SELECT DISTINCT e.source_symbol,u.code
        FROM evidence e
        CROSS JOIN LATERAL unnest(e.unknown_codes) AS u(code)
        WHERE u.code<>'PULSE_TRANSITION_NOT_COMPUTED'
    ) codes
    GROUP BY source_symbol
), seed_inputs AS (
    SELECT DISTINCT source_symbol,seed_fact_id FROM latest
), seed_rows AS (
    SELECT i.source_symbol,s.id,
           s.core_ret5_numerator,s.core_ret5_denominator,
           s.core_ret5_quality,s.core_ret5_reason,
           s.core_ret20_numerator,s.core_ret20_denominator,
           s.core_ret20_quality,s.core_ret20_reason,
           s.profile_extension_value,s.profile_extension_quality,
           s.profile_extension_reason,
           s.core_bias20_atr_numerator,s.core_bias20_atr_denominator,
           s.core_bias20_atr_quality,s.core_bias20_atr_reason,
           s.core_price_damage,s.core_price_damage_quality,
           s.core_price_damage_reason
    FROM seed_inputs i
    JOIN source_pool_seed_day_facts s ON s.id=i.seed_fact_id
), seed_metrics AS (
    SELECT source_symbol,count(DISTINCT id)::integer AS seed_fact_count,
           count(DISTINCT ROW(
               core_ret5_numerator,core_ret5_denominator,
               core_ret5_quality,core_ret5_reason
           ))::integer AS ret5_variants,
           min(core_ret5_quality) AS ret5_quality,
           min(core_ret5_reason) AS ret5_reason,
           min(core_ret5_numerator)::text AS ret5_numerator,
           min(core_ret5_denominator)::text AS ret5_denominator,
           count(DISTINCT ROW(
               core_ret20_numerator,core_ret20_denominator,
               core_ret20_quality,core_ret20_reason
           ))::integer AS ret20_variants,
           min(core_ret20_quality) AS ret20_quality,
           min(core_ret20_reason) AS ret20_reason,
           min(core_ret20_numerator)::text AS ret20_numerator,
           min(core_ret20_denominator)::text AS ret20_denominator,
           count(DISTINCT ROW(
               profile_extension_value,profile_extension_quality,
               profile_extension_reason
           ))::integer AS extension_variants,
           min(profile_extension_quality) AS extension_quality,
           min(profile_extension_reason) AS extension_reason,
           min(profile_extension_value) AS extension_value,
           count(DISTINCT ROW(
               core_bias20_atr_numerator,core_bias20_atr_denominator,
               core_bias20_atr_quality,core_bias20_atr_reason
           ))::integer AS bias_variants,
           min(core_bias20_atr_quality) AS bias_quality,
           min(core_bias20_atr_reason) AS bias_reason,
           min(core_bias20_atr_numerator)::text AS bias_numerator,
           min(core_bias20_atr_denominator)::text AS bias_denominator,
           count(DISTINCT ROW(
               core_price_damage,core_price_damage_quality,
               core_price_damage_reason
           ))::integer AS damage_variants,
           min(core_price_damage_quality) AS damage_quality,
           min(core_price_damage_reason) AS damage_reason,
           bool_and(core_price_damage) AS damage_value
    FROM seed_rows
    GROUP BY source_symbol
), seed_summary AS (
    SELECT source_symbol,jsonb_build_object(
        'seed_fact_count',seed_fact_count,
        'core_ret5',jsonb_build_object(
            'quality',CASE WHEN ret5_variants=1 THEN ret5_quality
                           ELSE 'CONFLICTING_STORED_VALUES' END,
            'numerator',CASE WHEN ret5_variants=1 AND ret5_quality='OBSERVED'
                             THEN ret5_numerator END,
            'denominator',CASE WHEN ret5_variants=1 AND ret5_quality='OBSERVED'
                               THEN ret5_denominator END,
            'window_sessions',5,'includes_target_session',TRUE,
            'reason',CASE WHEN ret5_variants=1 THEN ret5_reason
                          ELSE 'SEED_VALUE_VARIANTS' END
        ),
        'core_ret20',jsonb_build_object(
            'quality',CASE WHEN ret20_variants=1 THEN ret20_quality
                           ELSE 'CONFLICTING_STORED_VALUES' END,
            'numerator',CASE WHEN ret20_variants=1 AND ret20_quality='OBSERVED'
                             THEN ret20_numerator END,
            'denominator',CASE WHEN ret20_variants=1 AND ret20_quality='OBSERVED'
                               THEN ret20_denominator END,
            'window_sessions',20,'includes_target_session',TRUE,
            'reason',CASE WHEN ret20_variants=1 THEN ret20_reason
                          ELSE 'SEED_VALUE_VARIANTS' END
        ),
        'profile_extension',jsonb_build_object(
            'quality',CASE WHEN extension_variants=1 THEN extension_quality
                           ELSE 'CONFLICTING_STORED_VALUES' END,
            'value',CASE WHEN extension_variants=1
                              AND extension_quality='OBSERVED'
                         THEN extension_value END,
            'reason',CASE WHEN extension_variants=1 THEN extension_reason
                          ELSE 'SEED_VALUE_VARIANTS' END
        ),
        'core_bias20_atr',jsonb_build_object(
            'quality',CASE WHEN bias_variants=1 THEN bias_quality
                           ELSE 'CONFLICTING_STORED_VALUES' END,
            'numerator',CASE WHEN bias_variants=1 AND bias_quality='OBSERVED'
                             THEN bias_numerator END,
            'denominator',CASE WHEN bias_variants=1 AND bias_quality='OBSERVED'
                               THEN bias_denominator END,
            'reason',CASE WHEN bias_variants=1 THEN bias_reason
                          ELSE 'SEED_VALUE_VARIANTS' END
        ),
        'core_price_damage',jsonb_build_object(
            'quality',CASE WHEN damage_variants=1 THEN damage_quality
                           ELSE 'CONFLICTING_STORED_VALUES' END,
            'value',CASE WHEN damage_variants=1 AND damage_quality='OBSERVED'
                         THEN damage_value END,
            'reason',CASE WHEN damage_variants=1 THEN damage_reason
                          ELSE 'SEED_VALUE_VARIANTS' END
        )
    ) AS price_at_t0
    FROM seed_metrics
)
SELECT s.source_symbol,
       CASE WHEN s.has_support AND s.has_counter THEN 'MIXED'
            WHEN s.has_support THEN 'CURRENT_SAME_DIRECTION'
            WHEN s.has_counter THEN 'CURRENT_COUNTER'
            WHEN s.has_unknown THEN 'UNKNOWN'
            ELSE 'NO_PROVED_CASE' END AS triage_state,
       s.registration_count,s.mixed_registration_count,
       e.relation_count,e.support_relation_count,e.counter_relation_count,
       e.unknown_relation_count,
       COALESCE(b.support_board_count,0),COALESCE(b.counter_board_count,0),
       COALESCE(b.unknown_board_count,0),
       COALESCE(b.support_board_names,'{}'::text[]),
       COALESCE(b.counter_board_names,'{}'::text[]),
       COALESCE(r.unknown_reason_codes,'{}'::text[]),e.research_cases,
       e.hypothesis_version,e.membership_mode,e.research_mode,e.price_coordinate,
       e.background_relation_count,e.background_pulse_false_relation_count,
       e.driver_support_relation_count,
       e.direct_conflict_relation_count,e.driver_unknown_relation_count,
       e.transition_not_computed_relation_count,e.next_check_codes,
       e.direct_conflict_codes,
       COALESCE(b.background_board_count,0),
       COALESCE(b.driver_support_board_count,0),
       COALESCE(b.background_pulse_false_board_count,0),
       COALESCE(b.direct_conflict_board_count,0),
       COALESCE(b.driver_unknown_board_count,0),
       COALESCE(be.same_direction_examples,'[]'::jsonb),
       COALESCE(be.background_examples,'[]'::jsonb),
       COALESCE(be.conflict_examples,'[]'::jsonb),
       COALESCE(be.unknown_examples,'[]'::jsonb),
       COALESCE(dr.driver_unknown_reason_codes,'{}'::text[]),
       COALESCE(sm.price_at_t0,'{}'::jsonb)
FROM symbol_state s
JOIN evidence_counts e USING (source_symbol)
LEFT JOIN board_summary b USING (source_symbol)
LEFT JOIN reason_summary r USING (source_symbol)
LEFT JOIN driver_reason_summary dr USING (source_symbol)
LEFT JOIN board_examples be USING (source_symbol)
LEFT JOIN seed_summary sm USING (source_symbol)
ORDER BY s.source_symbol COLLATE "C"
            """,
            (target_session, source_package_id, source_capture_id, list(symbols)),
        )
        rows = cursor.fetchall()

    summaries = {
        symbol: {
            "status": "AVAILABLE",
            "triage_state": triage_state,
            "registration_count": int(registration_count),
            "mixed_registration_count": int(mixed_registration_count),
            "relation_count": int(relation_count),
            "support_relation_count": int(support_relation_count),
            "counter_relation_count": int(counter_relation_count),
            "unknown_relation_count": int(unknown_relation_count),
            "support_board_count": int(support_board_count),
            "counter_board_count": int(counter_board_count),
            "unknown_board_count": int(unknown_board_count),
            "support_board_names": list(support_board_names or []),
            "counter_board_names": list(counter_board_names or []),
            "unknown_reason_codes": list(unknown_reason_codes or []),
            "research_cases": list(research_cases or []),
            "hypothesis_version": hypothesis_version,
            "membership_mode": membership_mode,
            "scope": research_mode,
            "price_coordinate": price_coordinate,
            "source_package_id": source_package_id,
            "source_capture_id": source_capture_id,
            "t0_driver_and_earliness": {
                "version": "T0_DRIVER_AND_EARLINESS_R1",
                "registration_count": int(registration_count),
                "relation_count": int(relation_count),
                "same_direction_candidate_relation_count": int(
                    driver_support_relation_count
                ),
                "same_direction_candidate_board_count": int(driver_support_board_count),
                "same_direction_candidate_examples": list(
                    same_direction_examples or []
                ),
                "other_background_relation_count": int(background_relation_count),
                "other_background_board_count": int(background_board_count),
                "background_pulse_false_relation_count": int(
                    background_pulse_false_relation_count
                ),
                "background_pulse_false_board_count": int(
                    background_pulse_false_board_count
                ),
                "other_background_examples": list(background_examples or []),
                "named_conflict_relation_count": int(direct_conflict_relation_count),
                "named_conflict_board_count": int(direct_conflict_board_count),
                "named_conflict_codes": list(direct_conflict_codes or []),
                "named_conflict_examples": list(conflict_examples or []),
                "unknown_relation_count": int(driver_unknown_relation_count),
                "unknown_board_count": int(driver_unknown_board_count),
                "unknown_reason_codes": list(driver_unknown_reason_codes or []),
                "pulse_transition_not_computed_relation_count": int(
                    transition_not_computed_relation_count
                ),
                "next_check_codes": list(next_check_codes or []),
                "price_at_t0": price_at_t0 or {},
                "scope": research_mode,
                "price_coordinate": price_coordinate,
            },
        }
        for (
            symbol,
            triage_state,
            registration_count,
            mixed_registration_count,
            relation_count,
            support_relation_count,
            counter_relation_count,
            unknown_relation_count,
            support_board_count,
            counter_board_count,
            unknown_board_count,
            support_board_names,
            counter_board_names,
            unknown_reason_codes,
            research_cases,
            hypothesis_version,
            membership_mode,
            research_mode,
            price_coordinate,
            background_relation_count,
            background_pulse_false_relation_count,
            driver_support_relation_count,
            direct_conflict_relation_count,
            driver_unknown_relation_count,
            transition_not_computed_relation_count,
            next_check_codes,
            direct_conflict_codes,
            background_board_count,
            driver_support_board_count,
            background_pulse_false_board_count,
            direct_conflict_board_count,
            driver_unknown_board_count,
            same_direction_examples,
            background_examples,
            conflict_examples,
            unknown_examples,
            driver_unknown_reason_codes,
            price_at_t0,
        ) in rows
    }
    for symbol in symbols:
        summaries.setdefault(
            symbol,
            {
                "status": "NO_STORED_FACT",
                "triage_state": "NO_T0_FACT",
                "registration_count": 0,
                "relation_count": 0,
                "source_package_id": source_package_id,
                "source_capture_id": source_capture_id,
            },
        )
    return summaries


__all__ = [
    "T0EarlyObservationPgError",
    "persist_source_t0_early_observation_facts",
    "read_source_t0_early_observation_inputs",
    "read_source_t0_early_observations_for_enrollments",
    "read_source_t0_early_observations_for_symbols",
    "read_source_t0_early_triage_summaries_for_symbols",
    "read_source_t0_pulse_transition_facts",
]
