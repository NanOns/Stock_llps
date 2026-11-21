"""按真实PG分页读取当前会话全部已存板块PULSE目录。"""

from __future__ import annotations

from datetime import date
from decimal import Decimal, ROUND_HALF_UP, localcontext
from fractions import Fraction
from typing import Any, Mapping

from stock_a.storage.postgres import source_raw
from stock_a.storage.postgres.sector_pulse_facts import (
    _ALGORITHM,
    _pulse_payload,
    SectorPulsePgError,
)
from stock_a.storage.postgres.sector_pulse_cohorts import (
    read_source_sector_pulse_cohort_groups,
)


def _percent(value: Fraction | None) -> str | None:
    if value is None:
        return None
    with localcontext() as context:
        context.prec = 50
        return str(
            (
                Decimal(value.numerator) * Decimal(100) / Decimal(value.denominator)
            ).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        )


def read_source_sector_pulse_directory_page(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int,
    source_pool_count: int,
    profile_revision: str,
    snapshot_ids: Mapping[str, int],
    calendar_id: str,
    calendar_revision: str,
    horizon_targets: Mapping[int, date | None],
    board_kind: str = "ALL",
    search: str = "",
    offset: int = 0,
    limit: int = 15,
) -> dict[str, Any]:
    """读取精确日期和快照的单页PULSE；不扫描Raw或执行PULSE计算。"""
    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
        or any(
            type(value) is not int or value <= 0
            for value in (source_capture_id, source_pool_count)
        )
        or not isinstance(profile_revision, str)
        or not profile_revision.startswith("PROFILE_R")
        or not isinstance(snapshot_ids, Mapping)
        or set(snapshot_ids) != {"INDUSTRY", "CONCEPT"}
        or any(type(value) is not int or value <= 0 for value in snapshot_ids.values())
        or snapshot_ids["INDUSTRY"] == snapshot_ids["CONCEPT"]
        or not isinstance(calendar_id, str)
        or not calendar_id
        or not isinstance(calendar_revision, str)
        or not calendar_revision
        or not isinstance(horizon_targets, Mapping)
        or set(horizon_targets) != {1, 3, 5}
        or any(
            value is not None and type(value) is not date
            for value in horizon_targets.values()
        )
        or any(
            value is not None and value <= target_session
            for value in horizon_targets.values()
        )
        or board_kind not in {"ALL", "INDUSTRY", "CONCEPT"}
        or not isinstance(search, str)
        or len(search) > 80
        or type(offset) is not int
        or not 0 <= offset <= 2**31 - 1
        or type(limit) is not int
        or not 1 <= limit <= 50
    ):
        raise SectorPulsePgError(
            "SECTOR_PULSE_DIRECTORY_ARGUMENT_INVALID", "全体PULSE分页键无效"
        )

    snapshots = [snapshot_ids["INDUSTRY"], snapshot_ids["CONCEPT"]]
    with source_raw._transaction(config, write=False, statement_timeout_ms=20000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT count(*)::bigint,count(DISTINCT source_symbol)::bigint,
       count(DISTINCT profile_revision)::bigint,min(producer_version),max(producer_version)
FROM source_pool_seed_day_facts
WHERE target_session=%s AND source_capture_id=%s
  AND profile_revision=%s AND producer_version=%s""",
            (
                target_session,
                source_capture_id,
                profile_revision,
                "SOURCE_POOL_SEED_CORE_PROFILE_R2",
            ),
        )
        seed = cursor.fetchone()
        if (
            int(seed[0]) != source_pool_count
            or int(seed[1]) != source_pool_count
            or int(seed[2]) != 1
            or seed[3] != "SOURCE_POOL_SEED_CORE_PROFILE_R2"
            or seed[4] != seed[3]
        ):
            raise SectorPulsePgError(
                "SECTOR_PULSE_DIRECTORY_SEED_SCOPE_INCOMPLETE",
                "当前日期精确Seed/Profile全集未闭合",
            )

        cursor.execute(
            """SELECT snapshot_id,board_kind,count(*)::int
FROM tdx_sector_catalog
WHERE snapshot_id=ANY(%s) GROUP BY snapshot_id,board_kind""",
            (snapshots,),
        )
        catalog_counts = {
            (int(snapshot), kind): int(count)
            for snapshot, kind, count in cursor.fetchall()
        }
        cursor.execute(
            """SELECT c.snapshot_id,c.board_kind,count(*)::int,
       count(DISTINCT p.catalog_id)::int,min(p.snapshot_board_count)::int,
       max(p.snapshot_board_count)::int,
       count(*) FILTER (WHERE p.calculation_status='COMPLETE'
         AND p.formal_rotation_state='NOT_EVALUATED'
         AND p.research_mode='NON_PIT_RETROSPECTIVE'
         AND p.membership_mode='CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT'
         AND p.price_coordinate='FORWARD_ADJUSTED')::int
FROM source_sector_cross_period_pulse_facts p
JOIN tdx_sector_catalog c ON c.id=p.catalog_id
WHERE p.target_session=%s AND p.source_capture_id=%s
  AND p.profile_revision=%s
  AND p.algorithm_contract=%s AND c.snapshot_id=ANY(%s)
GROUP BY c.snapshot_id,c.board_kind""",
            (
                target_session,
                source_capture_id,
                profile_revision,
                _ALGORITHM,
                snapshots,
            ),
        )
        partitions = {}
        pulse_total = 0
        for (
            snapshot,
            kind,
            rows,
            catalogs,
            min_expected,
            max_expected,
            valid_rows,
        ) in cursor.fetchall():
            snapshot_id = int(snapshot)
            expected = catalog_counts.get((snapshot_id, kind), 0)
            pulse_total += int(rows)
            partitions[kind] = {
                "snapshot_id": snapshot_id,
                "actual_board_count": int(rows),
                "catalog_board_count": expected,
                "complete": (
                    expected > 0
                    and int(rows) == expected
                    and int(catalogs) == expected
                    and min_expected == expected
                    and max_expected == expected
                    and int(valid_rows) == int(rows)
                ),
            }
        partition_complete = all(
            partitions.get(kind, {}).get("complete") is True
            and partitions[kind]["snapshot_id"] == snapshot_ids[kind]
            for kind in ("INDUSTRY", "CONCEPT")
        )

        base_where = (
            "p.target_session=%s AND p.source_capture_id=%s "
            "AND p.profile_revision=%s "
            "AND p.algorithm_contract=%s AND c.snapshot_id=ANY(%s)"
        )
        base_params: list[Any] = [
            target_session,
            source_capture_id,
            profile_revision,
            _ALGORITHM,
            snapshots,
        ]
        if board_kind != "ALL":
            base_where += " AND c.board_kind=%s"
            base_params.append(board_kind)
        if search:
            base_where += (
                " AND (strpos(lower(c.board_name),lower(%s))>0 "
                "OR strpos(lower(c.board_native_id),lower(%s))>0)"
            )
            base_params.extend((search, search))
        cursor.execute(
            f"""SELECT count(*)::int FROM source_sector_cross_period_pulse_facts p
JOIN tdx_sector_catalog c ON c.id=p.catalog_id WHERE {base_where}""",
            tuple(base_params),
        )
        filtered_total = int(cursor.fetchone()[0])
        cursor.execute(
            """SELECT p.*,c.snapshot_id AS catalog_snapshot_id,
       c.board_kind AS catalog_board_kind,c.board_native_id AS catalog_native_id,
       c.board_name AS catalog_board_name,s.observed_at,s.source_quality
FROM source_sector_cross_period_pulse_facts p
JOIN tdx_sector_catalog c ON c.id=p.catalog_id
JOIN tdx_sector_source_snapshots s ON s.snapshot_id=c.snapshot_id
WHERE """
            + base_where
            + """
ORDER BY c.board_kind,c.board_name COLLATE "C",c.board_native_id COLLATE "C"
LIMIT %s OFFSET %s""",
            tuple(base_params + [limit, offset]),
        )
        page_columns = [column.name for column in cursor.description]
        page_facts = [
            dict(zip(page_columns, values, strict=True)) for values in cursor.fetchall()
        ]

        cursor.execute(
            """SELECT p.pulse_state,count(*)::int
FROM source_sector_cross_period_pulse_facts p
JOIN tdx_sector_catalog c ON c.id=p.catalog_id
WHERE p.target_session=%s AND p.source_capture_id=%s
  AND p.profile_revision=%s
  AND p.algorithm_contract=%s AND c.snapshot_id=ANY(%s)
GROUP BY p.pulse_state""",
            (
                target_session,
                source_capture_id,
                profile_revision,
                _ALGORITHM,
                snapshots,
            ),
        )
        pulse_state_counts = {state: int(count) for state, count in cursor.fetchall()}

        t1_target = horizon_targets[1]
        if t1_target is None:
            t1_status_counts = {"NOT_COMPUTED": pulse_total}
        elif t1_target > research_cutoff:
            t1_status_counts = {"NOT_DUE": pulse_total}
        else:
            cursor.execute(
                """WITH scoped AS (
  SELECT p.id FROM source_sector_cross_period_pulse_facts p
  JOIN tdx_sector_catalog c ON c.id=p.catalog_id
  WHERE p.target_session=%s AND p.source_capture_id=%s
    AND p.profile_revision=%s
    AND p.algorithm_contract=%s AND c.snapshot_id=ANY(%s)
), outcomes AS (
  SELECT p.id,CASE WHEN count(f.id)=0 THEN 'NOT_COMPUTED'
       WHEN count(f.id)>1 THEN 'INCOMPLETE'
       ELSE min(f.outcome_status) END AS outcome_status
  FROM scoped p LEFT JOIN source_sector_pulse_followup_facts f
    ON f.pulse_fact_id=p.id AND f.horizon_sessions=1 AND f.target_session=%s
   AND f.followup_contract='SOURCE_SECTOR_PULSE_FOLLOWUP_CLOSE_R1'
   AND f.calendar_id=%s AND f.calendar_revision=%s
  GROUP BY p.id
)
SELECT outcome_status,count(*)::int FROM outcomes GROUP BY outcome_status""",
                (
                    target_session,
                    source_capture_id,
                    profile_revision,
                    _ALGORITHM,
                    snapshots,
                    t1_target,
                    calendar_id,
                    calendar_revision,
                ),
            )
            t1_status_counts = {state: int(count) for state, count in cursor.fetchall()}

        pulse_ids = [int(row["id"]) for row in page_facts]
        followups_by_key: dict[tuple[int, int], list[dict[str, Any]]] = {}
        due_horizons = [
            horizon
            for horizon, due in horizon_targets.items()
            if due is not None and due <= research_cutoff
        ]
        if pulse_ids and due_horizons:
            cursor.execute(
                """SELECT pulse_fact_id,horizon_sessions,target_session,
       target_source_capture_id,fixed_member_denominator,
       observed_member_count,unknown_member_count,equal_weight_return_numerator,
       equal_weight_return_denominator,outcome_status,reason_codes,research_mode,
       membership_mode,price_coordinate,calendar_id,calendar_revision
FROM source_sector_pulse_followup_facts
WHERE pulse_fact_id=ANY(%s) AND target_session<=%s
  AND horizon_sessions=ANY(%s)
  AND followup_contract='SOURCE_SECTOR_PULSE_FOLLOWUP_CLOSE_R1'
  AND calendar_id=%s AND calendar_revision=%s
ORDER BY pulse_fact_id,horizon_sessions,target_session,id""",
                (
                    pulse_ids,
                    research_cutoff,
                    due_horizons,
                    calendar_id,
                    calendar_revision,
                ),
            )
            followup_columns = [column.name for column in cursor.description]
            for values in cursor.fetchall():
                fact = dict(zip(followup_columns, values, strict=True))
                key = (int(fact["pulse_fact_id"]), int(fact["horizon_sessions"]))
                followups_by_key.setdefault(key, []).append(fact)

        boards = []
        for row in page_facts:
            row["snapshot_id"] = int(row["catalog_snapshot_id"])
            payload = _pulse_payload(row)
            typed_valid = (
                row["calculation_status"] == "COMPLETE"
                and row["formal_rotation_state"] == "NOT_EVALUATED"
                and row["research_mode"] == "NON_PIT_RETROSPECTIVE"
                and row["membership_mode"] == "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT"
                and row["price_coordinate"] == "FORWARD_ADJUSTED"
                and int(row["source_pool_count"]) == source_pool_count
            )
            pulse = {
                **payload,
                "status": "AVAILABLE" if typed_valid else "INCOMPLETE",
                "reason_codes": []
                if typed_valid
                else ["PULSE_TYPED_FACT_CONTRACT_MISMATCH"],
            }
            horizon_facts = []
            for horizon in (1, 3, 5):
                due_target = horizon_targets[horizon]
                if due_target is None:
                    horizon_facts.append(
                        {
                            "horizon_sessions": horizon,
                            "status": "NOT_COMPUTED",
                            "reason_codes": ["OFFICIAL_HORIZON_SESSION_UNAVAILABLE"],
                        }
                    )
                    continue
                if due_target > research_cutoff:
                    horizon_facts.append(
                        {
                            "horizon_sessions": horizon,
                            "status": "NOT_DUE",
                            "fixed_member_denominator": pulse["member_count"],
                            "observed_member_count": None,
                            "unknown_member_count": None,
                            "reason_codes": ["WAITING_FOR_FUTURE_OFFICIAL_SESSION"],
                        }
                    )
                    continue
                candidates = followups_by_key.get((int(row["id"]), horizon), [])
                exact = [
                    fact for fact in candidates if fact["target_session"] == due_target
                ]
                if not exact:
                    horizon_facts.append(
                        {
                            "horizon_sessions": horizon,
                            "target_session": due_target.isoformat(),
                            "status": "INCOMPLETE" if candidates else "NOT_COMPUTED",
                            "fixed_member_denominator": pulse["member_count"],
                            "reason_codes": [
                                "FOLLOWUP_FACT_TARGET_SESSION_MISMATCH"
                                if candidates
                                else "FOLLOWUP_FACT_NOT_SAVED_FOR_EXACT_HORIZON"
                            ],
                        }
                    )
                    continue
                if len(exact) != 1:
                    horizon_facts.append(
                        {
                            "horizon_sessions": horizon,
                            "target_session": due_target.isoformat(),
                            "status": "INCOMPLETE",
                            "reason_codes": ["FOLLOWUP_FACT_KEY_AMBIGUOUS"],
                        }
                    )
                    continue
                fact = exact[0]
                fixed = int(fact["fixed_member_denominator"])
                observed = int(fact["observed_member_count"])
                unknown = int(fact["unknown_member_count"])
                numerator = fact["equal_weight_return_numerator"]
                denominator = fact["equal_weight_return_denominator"]
                fraction = (
                    Fraction(int(numerator), int(denominator))
                    if numerator is not None
                    and denominator is not None
                    and int(denominator) > 0
                    else None
                )
                valid = (
                    fact["calendar_id"] == calendar_id
                    and fact["calendar_revision"] == calendar_revision
                    and fact["research_mode"] == "NON_PIT_RETROSPECTIVE"
                    and fact["membership_mode"]
                    == "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT"
                    and fact["price_coordinate"] == "FORWARD_ADJUSTED"
                    and fixed == pulse["member_count"]
                    and observed + unknown == fixed
                    and (denominator is None or int(denominator) > 0)
                )
                horizon_facts.append(
                    {
                        "horizon_sessions": horizon,
                        "target_session": due_target.isoformat(),
                        "status": fact["outcome_status"] if valid else "INCOMPLETE",
                        "fixed_member_denominator": fixed,
                        "observed_member_count": observed,
                        "unknown_member_count": unknown,
                        "equal_weight_return": str(fraction)
                        if fraction is not None
                        else None,
                        "equal_weight_return_percent_4dp": _percent(fraction),
                        "reason_codes": list(fact["reason_codes"] or [])
                        if valid
                        else ["PULSE_FOLLOWUP_TYPED_FACT_CONTRACT_MISMATCH"],
                    }
                )
            followup = {
                "status": "AVAILABLE",
                "horizons": horizon_facts,
                "research_mode": "NON_PIT_RETROSPECTIVE",
                "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
                "price_coordinate": "FORWARD_ADJUSTED",
            }
            quality = str(row["snapshot_source_quality"] or "UNKNOWN")
            quality_label = quality.split(";", 1)[0]
            try:
                unmapped_source_rows = int(
                    next(
                        part.split("=", 1)[1]
                        for part in quality.split(";")
                        if part.startswith("UNMAPPED_SOURCE_ROWS=")
                    )
                )
            except (StopIteration, ValueError):
                unmapped_source_rows = None
            boards.append(
                {
                    "board_kind": row["catalog_board_kind"],
                    "board_native_id": row["catalog_native_id"],
                    "board_name": row["catalog_board_name"],
                    "snapshot_id": int(row["catalog_snapshot_id"]),
                    "observed_at": row["observed_at"].isoformat()
                    if row["observed_at"] is not None
                    else None,
                    "membership_source_quality": {
                        "quality": quality_label,
                        "source_unmapped_rows": unmapped_source_rows,
                    },
                    "observation_basis": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
                    "pulse_followup_only": True,
                    "cross_period_pulse": pulse,
                    "cross_period_pulse_followup": followup,
                }
            )

    cohort_comparison = read_source_sector_pulse_cohort_groups(
        config,
        target_session=target_session,
        research_cutoff=research_cutoff,
        source_capture_id=source_capture_id,
        profile_revision=profile_revision,
        snapshot_ids=snapshot_ids,
        expected_board_counts={
            kind: partitions[kind]["actual_board_count"]
            for kind in ("INDUSTRY", "CONCEPT")
        },
        calendar_id=calendar_id,
        calendar_revision=calendar_revision,
        board_kind=board_kind,
    )
    if not partition_complete and cohort_comparison.get("status") == "AVAILABLE":
        cohort_comparison["status"] = "INCOMPLETE"
    return {
        "status": "AVAILABLE" if partition_complete else "PARTIAL",
        "reason_codes": []
        if partition_complete
        else ["PULSE_DIRECTORY_PARTITION_INCOMPLETE"],
        "target_session": target_session.isoformat(),
        "source_capture_id": source_capture_id,
        "profile_revision": profile_revision,
        "snapshot_ids": dict(snapshot_ids),
        "total_board_count": pulse_total,
        "filtered_total": filtered_total,
        "offset": offset,
        "limit": limit,
        "board_kind": board_kind,
        "search": search,
        "pulse_state_counts": pulse_state_counts,
        "t1_status_counts": t1_status_counts,
        "partitions": partitions,
        "cohort_comparison": cohort_comparison,
        "boards": boards,
        "execution": {
            "transaction": "REPEATABLE_READ_READ_ONLY",
            "database_write_attempted": False,
            "raw_rows_read": False,
            "whole_market_recalculated": False,
            "limit": limit,
            "offset": offset,
            "rows_returned": len(boards),
        },
    }


__all__ = ["read_source_sector_pulse_directory_page"]
