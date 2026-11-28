"""正式 Focus 自动跟踪生产器（消费 R04 有效 WARM/CONFIRMED 与真实 Episode）。"""

from __future__ import annotations

from datetime import date
from typing import Any, Mapping

from stock_a.storage.postgres.formal_focus import persist_formal_focus_day_facts
from stock_a.storage.postgres.source_raw import _transaction

_VERIFIED_EVENT_WHITELIST = frozenset(
    {
        "FIRST_ENROLLED",
        "UPGRADED",
        "REENTERED",
        "WEAKENED",
        "RISK_WORSENED",
        "RISK_IMPROVED",
        "INVALIDATED",
        "EXPIRED",
        "EXITED",
        "NEW_FORMAL_BIRTH",
        "EPISODE_CONFIRMED",
        "ANCHOR_CREATED",
        "ANCHOR_ACCEPTED",
        "HELD_CONFIRMED",
        "HELD_TENTATIVE",
        "SUPPORT_ACCEPTANCE_UPGRADED",
    }
)


def produce_formal_focus_day(
    config: Mapping[str, Any],
    *,
    target_session: date,
) -> dict[str, Any]:
    """从 R04 D2 Episode 与 Radar 真实事件生成并持久化指定交易日的正式 Focus 队列。"""
    with _transaction(config, write=False, statement_timeout_ms=20000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT entity_id, episode_id, parent_episode_id, scenario,
                      display_maturity, effective_eligibility_value, tracking,
                      validity_value, exit_ordinal, exit_reason, transitions
               FROM stock.d2_episode_day_states
               WHERE session_date=%s
               ORDER BY entity_id, episode_id""",
            (target_session,),
        )
        d2_rows = cursor.fetchall()
        if not d2_rows:
            return {
                "status": "NOT_COMPUTED",
                "reason_code": "D2_LIFECYCLE_NOT_COMPUTED",
                "target_session": target_session.isoformat(),
                "count": 0,
            }

        cursor.execute(
            """SELECT episode_id, event_type
               FROM stock.stock_radar_day_events
               WHERE target_session=%s""",
            (target_session,),
        )
        radar_by_episode: dict[str, list[str]] = {}
        for ep_id, ev_type in cursor.fetchall():
            radar_by_episode.setdefault(ep_id, []).append(ev_type)

        cursor.execute(
            """SELECT source_symbol, risk_level
               FROM stock.stock_risk_day_facts
               WHERE target_session=%s""",
            (target_session,),
        )
        risk_by_symbol = {row[0]: row[1] for row in cursor.fetchall()}

        # 取最近一个早于 target_session 的已存 Focus 日，用于继承 birth_session、last_verified_event_date 及活跃退出判断
        cursor.execute(
            """SELECT max(target_session)
               FROM stock.formal_focus_episode_day_facts
               WHERE target_session < %s""",
            (target_session,),
        )
        prev_session = cursor.fetchone()[0]
        prev_by_episode: dict[str, tuple[Any, ...]] = {}
        if prev_session is not None:
            cursor.execute(
                """SELECT episode_id, entity_id, source_symbol, parent_episode_id,
                          scenario, birth_session, last_verified_event_date,
                          last_verified_event_type, tracking_status, is_archived, is_pinned
                   FROM stock.formal_focus_episode_day_facts
                   WHERE target_session=%s""",
                (prev_session,),
            )
            for row in cursor.fetchall():
                prev_by_episode[row[0]] = row

    rows_to_write: list[tuple[Any, ...]] = []
    seen_episodes: set[str] = set()
    for (
        entity_id,
        episode_id,
        parent_episode_id,
        scenario,
        display_maturity,
        effective_elig,
        tracking,
        validity_value,
        exit_ordinal,
        exit_reason,
        transitions,
    ) in d2_rows:
        source_symbol = entity_id.split(":", 1)[-1] if ":" in entity_id else entity_id
        prev = prev_by_episode.get(episode_id)
        is_eligible_focus = bool(
            effective_elig is True and display_maturity in ("WARM", "CONFIRMED")
        )
        verified_events = [
            ev
            for ev in (
                *radar_by_episode.get(episode_id, ()),
                *(transitions or ()),
            )
            if ev in _VERIFIED_EVENT_WHITELIST
        ]
        if is_eligible_focus:
            seen_episodes.add(episode_id)
            birth_session = prev[5] if prev is not None else target_session
            if verified_events:
                last_date = target_session
                last_type = verified_events[0]
            elif prev is not None:
                last_date = prev[6]
                last_type = prev[7]
            else:
                last_date = target_session
                last_type = "NEW_FORMAL_BIRTH"
            rows_to_write.append(
                (
                    target_session,
                    entity_id,
                    source_symbol,
                    episode_id,
                    parent_episode_id,
                    scenario,
                    display_maturity,
                    "ACTIVE",
                    birth_session,
                    last_date,
                    last_type,
                    None,
                    None,
                    None,
                    None,
                    None,
                    risk_by_symbol.get(source_symbol),
                    bool(prev[9]) if prev is not None else False,
                    bool(prev[10]) if prev is not None else False,
                    "NON_PIT_RETROSPECTIVE",
                )
            )
        elif prev is not None and prev[8] == "ACTIVE":
            seen_episodes.add(episode_id)
            if validity_value == "INVALIDATED":
                t_status = "INVALIDATED"
                d_mat = "INVALIDATED"
            elif display_maturity == "EXPIRED":
                t_status = "EXPIRED"
                d_mat = "EXPIRED"
            else:
                t_status = "EXITED"
                d_mat = "EXITED"
            rows_to_write.append(
                (
                    target_session,
                    entity_id,
                    source_symbol,
                    episode_id,
                    parent_episode_id,
                    scenario,
                    d_mat,
                    t_status,
                    prev[5],
                    target_session if verified_events else prev[6],
                    verified_events[0] if verified_events else prev[7],
                    target_session,
                    exit_reason or t_status,
                    None,
                    None,
                    None,
                    risk_by_symbol.get(source_symbol),
                    bool(prev[9]),
                    bool(prev[10]),
                    "NON_PIT_RETROSPECTIVE",
                )
            )

    persist_res = persist_formal_focus_day_facts(
        config, target_session=target_session, rows=rows_to_write
    )
    return {
        "status": "PERSISTED",
        "target_session": target_session.isoformat(),
        "count": len(rows_to_write),
        **persist_res,
    }
