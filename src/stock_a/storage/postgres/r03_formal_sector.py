"""R03 正式板块 Native、Rotation、Overlap 与 LOO 的 PG 存储、只读查询与有界清理。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from fractions import Fraction
from typing import Any

import psycopg


class R03FormalSectorPgError(RuntimeError):
    """R03 正式 PG 存储边界异常。"""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def _num(value: Fraction | Decimal | int | float | str | None) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, Fraction):
        return Decimal(value.numerator) / Decimal(value.denominator)
    return Decimal(str(value))


@contextmanager
def _tx(config: Mapping[str, Any], *, write: bool):
    pg = config["postgres"]
    conn = psycopg.connect(
        host=pg["host"],
        port=int(pg["port"]),
        dbname=pg["database"],
        user=pg["user"],
        password=pg.get("password", ""),
        options="-c search_path=stock,public",
    )
    try:
        if not write:
            conn.execute("SET TRANSACTION READ ONLY")
        with conn.cursor() as cur:
            yield conn, cur
        if write:
            conn.commit()
        else:
            conn.rollback()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def write_r03_native_day_facts(
    config: Mapping[str, Any],
    *,
    trade_date: date,
    scope_key: str,
    member_version: str,
    contract_version: str,
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, int]:
    """幂等写入单日全板块正式 Native、资格、前态、风险与 Priority 事实。"""
    inserted = 0
    skipped = 0
    with _tx(config, write=True) as (_conn, cur):
        for row in rows:
            cur.execute(
                """
                INSERT INTO stock.r03_sector_native_day_facts (
                    trade_date, board_kind, board_id, scope_key, member_version,
                    contract_version, member_count, actual_quote_count,
                    suspended_count, unknown_quote_count, rs5, rs20, rs5_pct,
                    rs20_pct, dq5, breadth, ma20_width, amr20_median,
                    breadth_delta1, breadth_delta3, ma20_delta3,
                    seed_wilson_point, seed_wilson_lower, seed_wilson_upper,
                    top1_concentration, top3_concentration, safety_state,
                    prewatch_raw_state, warm_raw_state, confirmed_raw_state,
                    effective_maturity, display_maturity, downgrade_streak,
                    prewatch_no_improve_count, health_state, exhaustion_level,
                    emergence_level, priority_status, priority_rank, reason_code
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s
                )
                ON CONFLICT (trade_date, board_kind, board_id, scope_key, member_version, contract_version)
                DO NOTHING
                """,
                (
                    trade_date,
                    row["board_kind"],
                    row["board_id"],
                    scope_key,
                    member_version,
                    contract_version,
                    int(row["n"]),
                    int(row["actual_quote_count"]),
                    int(row["suspended_count"]),
                    int(row["unknown_quote_count"]),
                    _num(row["rs5"]["value"]),
                    _num(row["rs20"]["value"]),
                    _num(row.get("rs5_pct")),
                    _num(row.get("rs20_pct")),
                    _num(row.get("dq5")),
                    _num(row["breadth"]["value"]),
                    _num(row["ma20_width"]["value"]),
                    _num(row["amr20_median"]["value"]),
                    _num(row.get("breadth_delta1")),
                    _num(row.get("breadth_delta3")),
                    _num(row.get("ma20_delta3")),
                    _num(row["seed_wilson"].get("point_value")),
                    _num(row["seed_wilson"].get("lower")),
                    _num(row["seed_wilson"].get("upper")),
                    _num(row["concentration"].get("top1_concentration")),
                    _num(row["concentration"].get("top3_concentration")),
                    row["raw_qualification"]["safety"]["state"],
                    row["raw_qualification"]["prewatch_raw"]["state"],
                    row["raw_qualification"]["warm_raw"]["state"],
                    row["raw_qualification"]["confirmed_raw"]["state"],
                    row["lifecycle"]["effective_maturity"],
                    row["lifecycle"]["display_maturity"],
                    int(row["lifecycle"]["downgrade_streak"]),
                    int(row["lifecycle"]["prewatch_no_improve_count"]),
                    row["lifecycle"]["health"],
                    row["exhaustion"]["level"],
                    row["emergence"]["level"],
                    row["priority"]["priority_status"],
                    row["priority"].get("rank_position"),
                    row["priority"].get("reason"),
                ),
            )
            if cur.rowcount == 1:
                inserted += 1
            else:
                skipped += 1
    return {"inserted": inserted, "skipped": skipped}


def read_r03_native_day_facts(
    config: Mapping[str, Any],
    *,
    trade_date: date,
    scope_key: str,
    member_version: str,
    contract_version: str,
    board_kind: str | None = None,
) -> list[dict[str, Any]]:
    """只读精确 scope/version 的 R03 Native 日级事实，不跨版兜底。"""
    with _tx(config, write=False) as (_conn, cur):
        sql = """
            SELECT board_kind, board_id, member_count, actual_quote_count,
                   rs5_pct, rs20_pct, dq5, breadth, ma20_width,
                   safety_state, prewatch_raw_state, warm_raw_state, confirmed_raw_state,
                   effective_maturity, display_maturity, health_state,
                   exhaustion_level, emergence_level, priority_status, priority_rank
            FROM stock.r03_sector_native_day_facts
            WHERE trade_date=%s AND scope_key=%s AND member_version=%s AND contract_version=%s
        """
        params: list[Any] = [trade_date, scope_key, member_version, contract_version]
        if board_kind is not None:
            sql += " AND board_kind=%s"
            params.append(board_kind)
        sql += " ORDER BY priority_rank ASC NULLS LAST, board_id ASC"
        cur.execute(sql, tuple(params))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def write_r03_overlap_clusters(
    config: Mapping[str, Any],
    *,
    trade_date: date,
    scope_key: str,
    contract_version: str,
    clusters: Sequence[Mapping[str, Any]],
) -> dict[str, int]:
    """幂等写入单日 Overlap 主题簇与代表。"""
    inserted = 0
    skipped = 0
    with _tx(config, write=True) as (_conn, cur):
        for c in clusters:
            cur.execute(
                """
                INSERT INTO stock.r03_sector_overlap_clusters (
                    trade_date, board_kind, cluster_id, scope_key, contract_version,
                    cluster_status, member_board_ids, parent_cluster_ids,
                    representative_board_id, representative_status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (trade_date, board_kind, cluster_id, scope_key, contract_version)
                DO NOTHING
                """,
                (
                    trade_date,
                    c["board_kind"],
                    c["cluster_id"],
                    scope_key,
                    contract_version,
                    c["cluster_status"],
                    list(c["member_board_ids"]),
                    list(c["parent_cluster_ids"]),
                    c["representative_board_id"],
                    c["representative_status"],
                ),
            )
            if cur.rowcount == 1:
                inserted += 1
            else:
                skipped += 1
    return {"inserted": inserted, "skipped": skipped}


def write_r03_loo_support_fact(
    config: Mapping[str, Any],
    *,
    trade_date: date,
    board_kind: str,
    scope_key: str,
    contract_version: str,
    loo_result: Mapping[str, Any],
) -> bool:
    """幂等写入单股 LOO 算法支持板块结果。"""
    with _tx(config, write=True) as (_conn, cur):
        cur.execute(
            """
            INSERT INTO stock.r03_sector_loo_support_facts (
                trade_date, target_entity_id, board_kind, scope_key, contract_version,
                primary_industry_id, algorithm_support_board_id, support_status, reason_code
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (trade_date, target_entity_id, board_kind, scope_key, contract_version)
            DO NOTHING
            """,
            (
                trade_date,
                loo_result["target_entity_id"],
                board_kind,
                scope_key,
                contract_version,
                loo_result.get("primary_industry_id"),
                loo_result.get("algorithm_support_board_id"),
                loo_result["support_status"],
                loo_result.get("reason"),
            ),
        )
        return cur.rowcount == 1


def cleanup_r03_scope_day(
    config: Mapping[str, Any],
    *,
    trade_date: date,
    scope_key: str,
) -> dict[str, int]:
    """按精确日期与 scope 有界清理 R03 正式事实，不触碰其他捕获、日期或来源层。"""
    deleted: dict[str, int] = {}
    with _tx(config, write=True) as (_conn, cur):
        for table in (
            "stock.r03_sector_loo_support_facts",
            "stock.r03_sector_overlap_clusters",
            "stock.r03_sector_native_day_facts",
        ):
            cur.execute(
                f"DELETE FROM {table} WHERE trade_date=%s AND scope_key=%s",
                (trade_date, scope_key),
            )
            deleted[table] = cur.rowcount
        cur.execute(
            """
            DELETE FROM stock.r03_sector_rotation_day_states s
            USING stock.r03_sector_rotation_episodes e
            WHERE s.episode_id = e.id AND s.trade_date=%s AND e.scope_key=%s
            """,
            (trade_date, scope_key),
        )
        deleted["stock.r03_sector_rotation_day_states"] = cur.rowcount
    return deleted
