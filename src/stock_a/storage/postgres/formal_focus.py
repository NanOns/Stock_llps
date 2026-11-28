"""正式 Focus 每日跟踪队列的 PG 持久化与只读查询（ALG-33 / §11.8）。"""

from __future__ import annotations

from datetime import date
from typing import Any, Mapping, Sequence

from stock_a.storage.postgres.source_raw import _transaction

_FOCUS_COLUMNS = (
    "target_session",
    "entity_id",
    "source_symbol",
    "episode_id",
    "parent_episode_id",
    "scenario",
    "display_maturity",
    "tracking_status",
    "birth_session",
    "last_verified_event_date",
    "last_verified_event_type",
    "exit_session",
    "exit_reason",
    "linked_anchor_id",
    "failure_level_numerator",
    "failure_level_denominator",
    "risk_tier",
    "is_archived",
    "is_pinned",
    "research_mode",
)


def persist_formal_focus_day_facts(
    config: Mapping[str, Any],
    *,
    target_session: date,
    rows: Sequence[tuple[Any, ...]],
) -> dict[str, Any]:
    """在单事务内按 target_session 原子替换写入正式 Focus 每日队列事实。"""
    with _transaction(config, write=True, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            "DELETE FROM stock.formal_focus_episode_day_facts WHERE target_session=%s",
            (target_session,),
        )
        deleted = cursor.rowcount
        inserted = 0
        if rows:
            placeholders = ", ".join(["%s"] * len(_FOCUS_COLUMNS))
            cols_sql = ", ".join(_FOCUS_COLUMNS)
            cursor.executemany(
                f"INSERT INTO stock.formal_focus_episode_day_facts ({cols_sql}) VALUES ({placeholders})",
                rows,
            )
            inserted = len(rows)
    return {
        "target_session": target_session.isoformat(),
        "inserted_rows": inserted,
        "deleted_rows": deleted,
        "status": "PERSISTED",
    }


def read_formal_focus_page(
    config: Mapping[str, Any],
    *,
    target_session: date,
    include_exited: bool = False,
    include_archived: bool = False,
    symbol: str | None = None,
    offset: int = 0,
    limit: int = 30,
) -> dict[str, Any]:
    """按 FOCUS_RECENCY_ORDER_R1 只读查询指定历史或当前交易日的正式 Focus 队列。"""
    if offset < 0 or not 1 <= limit <= 100:
        raise ValueError("正式Focus分页参数无效")
    with _transaction(config, write=False, statement_timeout_ms=15000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            "SELECT count(*) FROM stock.d2_episode_day_states WHERE session_date=%s",
            (target_session,),
        )
        d2_count = int(cursor.fetchone()[0] or 0)
        if d2_count == 0:
            return {
                "status": "NOT_COMPUTED",
                "reason_code": "D2_LIFECYCLE_NOT_COMPUTED",
                "target_session": target_session.isoformat(),
                "order": "FOCUS_RECENCY_ORDER_R1",
                "total": 0,
                "items": [],
                "page": {"offset": offset, "limit": limit, "next_offset": None},
                "non_pit_disclosure": "NON_PIT_RETROSPECTIVE",
            }

        where_parts = ["target_session=%s"]
        params: list[Any] = [target_session]
        if not include_exited:
            where_parts.append("tracking_status='ACTIVE'")
        if not include_archived:
            where_parts.append("is_archived IS FALSE")
        if symbol is not None:
            where_parts.append("source_symbol=%s")
            params.append(symbol)
        where_sql = " AND ".join(where_parts)

        cursor.execute(
            f"SELECT count(*) FROM stock.formal_focus_episode_day_facts WHERE {where_sql}",
            tuple(params),
        )
        total = int(cursor.fetchone()[0] or 0)
        cursor.execute(
            f"""SELECT entity_id, source_symbol, episode_id, parent_episode_id,
                       scenario, display_maturity, tracking_status,
                       birth_session, last_verified_event_date, last_verified_event_type,
                       exit_session, exit_reason, linked_anchor_id,
                       failure_level_numerator, failure_level_denominator,
                       risk_tier, is_archived, is_pinned, research_mode
                FROM stock.formal_focus_episode_day_facts
                WHERE {where_sql}
                ORDER BY last_verified_event_date DESC, entity_id ASC, episode_id ASC
                LIMIT %s OFFSET %s""",
            (*params, limit, offset),
        )
        items = [
            {
                "entity_id": row[0],
                "source_symbol": row[1],
                "episode_id": row[2],
                "parent_episode_id": row[3],
                "scenario": row[4],
                "display_maturity": row[5],
                "tracking_status": row[6],
                "birth_session": row[7].isoformat(),
                "last_verified_event_date": row[8].isoformat(),
                "last_verified_event_type": row[9],
                "exit_session": row[10].isoformat() if row[10] else None,
                "exit_reason": row[11],
                "linked_anchor_id": row[12],
                "failure_level": (
                    {"numerator": str(row[13]), "denominator": str(row[14])}
                    if row[13] is not None and row[14] is not None
                    else None
                ),
                "risk_tier": row[15],
                "is_archived": bool(row[16]),
                "is_pinned": bool(row[17]),
                "research_mode": row[18],
            }
            for row in cursor.fetchall()
        ]
    return {
        "status": "AVAILABLE",
        "target_session": target_session.isoformat(),
        "order": "FOCUS_RECENCY_ORDER_R1",
        "total": total,
        "items": items,
        "page": {
            "offset": offset,
            "limit": limit,
            "next_offset": offset + limit if offset + limit < total else None,
        },
        "non_pit_disclosure": "NON_PIT_RETROSPECTIVE",
    }
