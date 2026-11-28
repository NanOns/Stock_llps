"""13 类正式 Cohort 评价、事件子行、T0 冻结观察登记的 PG 存储与读取（ALG-34 / §11.8）。"""

from __future__ import annotations

from datetime import date
from fractions import Fraction
from statistics import median
from typing import Any, Mapping, Sequence

from stock_a.storage.postgres.source_raw import _transaction

FORMAL_STOCK_RULES = (
    "STOCK_SEED",
    "STOCK_WARM",
    "STOCK_CONFIRMED",
    "STOCK_D0_LAUNCH_CONFIRM",
    "STOCK_D0_RECOVERY_TURN",
    "STOCK_D0_STRONG_PULLBACK",
    "STOCK_D0_TREND_CONTINUE",
)
FORMAL_SECTOR_RULES = (
    "SECTOR_PREWATCH",
    "SECTOR_WARM",
    "SECTOR_CONFIRMED_RAW",
    "ROTATION_ACCEPTED",
    "ROTATION_EXPANDING",
    "ROTATION_REACCELERATING",
)
ALL_FORMAL_COHORT_RULES = FORMAL_STOCK_RULES + FORMAL_SECTOR_RULES

_EVAL_COLUMNS = (
    "target_session",
    "rule_id",
    "entity_type",
    "entity_id",
    "source_symbol",
    "board_kind",
    "is_st",
    "universe_status",
    "state",
    "reasons",
    "event_count",
    "source_capture_id",
    "calendar_revision",
    "research_mode",
)

_SUBROW_COLUMNS = (
    "evaluation_id",
    "target_session",
    "rule_id",
    "entity_id",
    "sub_event_key",
    "parent_episode_id",
    "peak_event_id",
    "event_id",
    "linked_anchor_id",
    "sub_state",
    "admission_status",
    "reasons",
    "failure_level_numerator",
    "failure_level_denominator",
    "failure_coordinate",
)

_OBS_COLUMNS = (
    "t0_session",
    "time_lane",
    "rule_id",
    "entity_type",
    "entity_id",
    "observation_sub_key",
    "source_symbol",
    "enrollment_kind",
    "parent_observation_id",
    "episode_id",
    "event_id",
    "linked_anchor_id",
    "t0_p0_numerator",
    "t0_p0_denominator",
    "t0_qfq_coordinate_id",
    "t0_rps20_num",
    "t0_rps20_den",
    "t0_pos60_num",
    "t0_pos60_den",
    "t0_amt20_num",
    "t0_amt20_den",
    "t0_industry_id",
    "t0_board_type",
    "t0_is_st",
    "t0_safety_gate_passed",
    "source_available_at",
    "first_captured_at",
    "reason_codes",
)


def persist_formal_cohort_day(
    config: Mapping[str, Any],
    *,
    target_session: date,
    evaluations: Sequence[tuple[Any, ...]],
    subrows_by_eval_key: Mapping[tuple[str, str, str], Sequence[tuple[Any, ...]]],
    observations: Sequence[tuple[Any, ...]],
    time_lane: str = "H",
) -> dict[str, Any]:
    """在单事务内按 target_session 原子替换写入 13 类 Cohort 评价、事件子行与 T0 观察登记。"""
    with _transaction(config, write=True, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        # 清理当日旧评价（CASCADE 删除子行）
        cursor.execute(
            "DELETE FROM stock.formal_cohort_evaluations WHERE target_session=%s",
            (target_session,),
        )
        deleted_evals = cursor.rowcount

        eval_id_by_key: dict[tuple[str, str, str], int] = {}
        if evaluations:
            cols_sql = ", ".join(_EVAL_COLUMNS)
            placeholders = ", ".join(["%s"] * len(_EVAL_COLUMNS))
            for row in evaluations:
                cursor.execute(
                    f"""INSERT INTO stock.formal_cohort_evaluations ({cols_sql})
                        VALUES ({placeholders})
                        RETURNING id, rule_id, entity_type, entity_id""",
                    row,
                )
                eid, r_id, e_type, e_id = cursor.fetchone()
                eval_id_by_key[(r_id, e_type, e_id)] = int(eid)

        subrow_count = 0
        if subrows_by_eval_key:
            sub_cols_sql = ", ".join(_SUBROW_COLUMNS)
            sub_placeholders = ", ".join(["%s"] * len(_SUBROW_COLUMNS))
            sub_rows_flat: list[tuple[Any, ...]] = []
            for key, items in subrows_by_eval_key.items():
                eid = eval_id_by_key.get(key)
                if eid is None:
                    continue
                for item in items:
                    sub_rows_flat.append((eid, *item))
            if sub_rows_flat:
                cursor.executemany(
                    f"INSERT INTO stock.formal_cohort_event_subrows ({sub_cols_sql}) VALUES ({sub_placeholders})",
                    sub_rows_flat,
                )
                subrow_count = len(sub_rows_flat)

        # 幂等替换当日指定 time_lane 的观察与登记（不破坏其他日期与其他 lane）
        cursor.execute(
            "DELETE FROM stock.formal_research_observations WHERE t0_session=%s AND time_lane=%s",
            (target_session, time_lane),
        )
        obs_count = 0
        if observations:
            obs_cols_sql = ", ".join(_OBS_COLUMNS)
            obs_placeholders = ", ".join(["%s"] * len(_OBS_COLUMNS))
            cursor.executemany(
                f"INSERT INTO stock.formal_research_observations ({obs_cols_sql}) VALUES ({obs_placeholders})",
                observations,
            )
            obs_count = len(observations)

    return {
        "target_session": target_session.isoformat(),
        "time_lane": time_lane,
        "deleted_evaluations": deleted_evals,
        "inserted_evaluations": len(eval_id_by_key),
        "inserted_subrows": subrow_count,
        "inserted_observations": obs_count,
        "status": "PERSISTED",
    }


def read_formal_cohort_summary(
    config: Mapping[str, Any],
    *,
    target_session: date,
    rule_id: str | None = None,
    entity_type: str | None = None,
) -> dict[str, Any]:
    """只读查询 13 类正式 Cohort 评价汇总（区分 MATCH/NO_MATCH/UNKNOWN 及隔离未映射分母）。"""
    with _transaction(config, write=False, statement_timeout_ms=15000) as (
        _,
        cursor,
        _,
    ):
        where = ["target_session=%s"]
        params: list[Any] = [target_session]
        if rule_id is not None:
            where.append("rule_id=%s")
            params.append(rule_id)
        if entity_type is not None:
            where.append("entity_type=%s")
            params.append(entity_type)
        cursor.execute(
            f"""SELECT rule_id, entity_type, universe_status, state, count(*), sum(event_count)
                FROM stock.formal_cohort_evaluations
                WHERE {" AND ".join(where)}
                GROUP BY 1, 2, 3, 4
                ORDER BY 1, 2, 3, 4""",
            tuple(params),
        )
        rows = cursor.fetchall()
        if not rows:
            return {
                "status": "NOT_COMPUTED",
                "target_session": target_session.isoformat(),
                "rules": [],
            }
        by_rule: dict[tuple[str, str], dict[str, Any]] = {}
        for r_id, e_type, u_status, state, cnt, ev_sum in rows:
            entry = by_rule.setdefault(
                (r_id, e_type),
                {
                    "rule_id": r_id,
                    "entity_type": e_type,
                    "total": 0,
                    "match_count": 0,
                    "no_match_count": 0,
                    "unknown_count": 0,
                    "unmapped_unknown_count": 0,
                    "excluded_count": 0,
                    "event_count": 0,
                },
            )
            c_int = int(cnt)
            entry["total"] += c_int
            entry["event_count"] += int(ev_sum or 0)
            if state == "TRUE":
                entry["match_count"] += c_int
            elif state == "FALSE":
                entry["no_match_count"] += c_int
            else:
                entry["unknown_count"] += c_int
            if u_status == "UNMAPPED_UNKNOWN":
                entry["unmapped_unknown_count"] += c_int
            elif u_status == "EXCLUDED":
                entry["excluded_count"] += c_int
        return {
            "status": "AVAILABLE",
            "target_session": target_session.isoformat(),
            "rules": list(by_rule.values()),
        }


def read_formal_cohort_members(
    config: Mapping[str, Any],
    *,
    target_session: date,
    rule_id: str,
    entity_type: str | None = None,
    state_filter: str | None = None,
    offset: int = 0,
    limit: int = 30,
) -> dict[str, Any]:
    """只读分页查询指定正式规则的成员评价及 D0 子行明细。"""
    with _transaction(config, write=False, statement_timeout_ms=15000) as (
        _,
        cursor,
        _,
    ):
        where = ["target_session=%s", "rule_id=%s"]
        params: list[Any] = [target_session, rule_id]
        if entity_type is not None:
            where.append("entity_type=%s")
            params.append(entity_type)
        if state_filter is not None:
            where.append("state=%s")
            params.append(state_filter)
        where_sql = " AND ".join(where)
        cursor.execute(
            f"SELECT count(*) FROM stock.formal_cohort_evaluations WHERE {where_sql}",
            tuple(params),
        )
        total = int(cursor.fetchone()[0] or 0)
        cursor.execute(
            f"""SELECT id, entity_type, entity_id, source_symbol, board_kind, is_st,
                       universe_status, state, reasons, event_count
                FROM stock.formal_cohort_evaluations
                WHERE {where_sql}
                ORDER BY entity_type, entity_id
                LIMIT %s OFFSET %s""",
            (*params, limit, offset),
        )
        eval_rows = cursor.fetchall()
        eval_ids = [int(r[0]) for r in eval_rows]
        subrows_by_eval: dict[int, list[dict[str, Any]]] = {}
        if eval_ids:
            cursor.execute(
                """SELECT evaluation_id, sub_event_key, parent_episode_id, peak_event_id,
                          event_id, linked_anchor_id, sub_state, admission_status, reasons,
                          failure_level_numerator, failure_level_denominator, failure_coordinate
                   FROM stock.formal_cohort_event_subrows
                   WHERE evaluation_id=ANY(%s)
                   ORDER BY id""",
                (eval_ids,),
            )
            for s in cursor.fetchall():
                subrows_by_eval.setdefault(int(s[0]), []).append(
                    {
                        "sub_event_key": s[1],
                        "parent_episode_id": s[2],
                        "peak_event_id": s[3],
                        "event_id": s[4],
                        "linked_anchor_id": s[5],
                        "sub_state": s[6],
                        "admission_status": s[7],
                        "reasons": list(s[8] or ()),
                        "failure_level": (
                            {"numerator": str(s[9]), "denominator": str(s[10])}
                            if s[9] is not None and s[10] is not None
                            else None
                        ),
                        "failure_coordinate": s[11],
                    }
                )
        items = [
            {
                "evaluation_id": int(r[0]),
                "entity_type": r[1],
                "entity_id": r[2],
                "source_symbol": r[3],
                "board_kind": r[4],
                "is_st": r[5],
                "universe_status": r[6],
                "state": r[7],
                "reasons": list(r[8] or ()),
                "event_count": int(r[9]),
                "subrows": subrows_by_eval.get(int(r[0]), []),
            }
            for r in eval_rows
        ]
    return {
        "status": "AVAILABLE"
        if total > 0 or rule_id in ALL_FORMAL_COHORT_RULES
        else "NOT_COMPUTED",
        "target_session": target_session.isoformat(),
        "rule_id": rule_id,
        "total": total,
        "items": items,
        "page": {
            "offset": offset,
            "limit": limit,
            "next_offset": offset + limit if offset + limit < total else None,
        },
    }


def read_formal_observations(
    config: Mapping[str, Any],
    *,
    t0_session: date,
    rule_id: str | None = None,
    time_lane: str | None = None,
    entity_id: str | None = None,
    offset: int = 0,
    limit: int = 50,
) -> dict[str, Any]:
    """只读查询冻结 T0 观察与登记明细（含父子重入、左截断、暂停、退出与冻结特征）。"""
    with _transaction(config, write=False, statement_timeout_ms=15000) as (
        _,
        cursor,
        _,
    ):
        where = ["t0_session=%s"]
        params: list[Any] = [t0_session]
        if rule_id is not None:
            where.append("rule_id=%s")
            params.append(rule_id)
        if time_lane is not None:
            where.append("time_lane=%s")
            params.append(time_lane)
        if entity_id is not None:
            where.append("entity_id=%s")
            params.append(entity_id)
        where_sql = " AND ".join(where)
        cursor.execute(
            f"SELECT count(*) FROM stock.formal_research_observations WHERE {where_sql}",
            tuple(params),
        )
        total = int(cursor.fetchone()[0] or 0)
        cursor.execute(
            f"""SELECT id, time_lane, rule_id, entity_type, entity_id, observation_sub_key,
                       source_symbol, enrollment_kind, parent_observation_id, episode_id,
                       event_id, linked_anchor_id, t0_p0_numerator, t0_p0_denominator,
                       t0_qfq_coordinate_id, t0_industry_id, t0_board_type, t0_is_st,
                       t0_safety_gate_passed, source_available_at, first_captured_at, reason_codes
                FROM stock.formal_research_observations
                WHERE {where_sql}
                ORDER BY rule_id, entity_type, entity_id, observation_sub_key
                LIMIT %s OFFSET %s""",
            (*params, limit, offset),
        )
        items = [
            {
                "observation_id": int(r[0]),
                "t0_session": t0_session.isoformat(),
                "time_lane": r[1],
                "rule_id": r[2],
                "entity_type": r[3],
                "entity_id": r[4],
                "observation_sub_key": r[5],
                "source_symbol": r[6],
                "enrollment_kind": r[7],
                "parent_observation_id": int(r[8]) if r[8] is not None else None,
                "episode_id": r[9],
                "event_id": r[10],
                "linked_anchor_id": r[11],
                "t0_p0": (
                    {"numerator": str(r[12]), "denominator": str(r[13])}
                    if r[12] is not None and r[13] is not None
                    else None
                ),
                "t0_qfq_coordinate_id": r[14],
                "t0_industry_id": r[15],
                "t0_board_type": r[16],
                "t0_is_st": r[17],
                "t0_safety_gate_passed": r[18],
                "source_available_at": r[19].isoformat() if r[19] else None,
                "first_captured_at": r[20].isoformat() if r[20] else None,
                "reason_codes": list(r[21] or ()),
            }
            for r in cursor.fetchall()
        ]
    return {
        "status": "AVAILABLE" if total > 0 else "NO_STORED_FACT",
        "t0_session": t0_session.isoformat(),
        "total": total,
        "items": items,
        "page": {
            "offset": offset,
            "limit": limit,
            "next_offset": offset + limit if offset + limit < total else None,
        },
    }
