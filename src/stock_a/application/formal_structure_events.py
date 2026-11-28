"""原 T0 结构首事件生产、PG 持久化与只读读取（E01 / §11.9）。"""

from __future__ import annotations

from datetime import date
from typing import Any, Mapping

from stock_a.domain.formal_structure_events import evaluate_structure_first_events
from stock_a.research.entry_context.sessions import _load_calendar
from stock_a.storage.postgres.source_raw import _transaction


def produce_formal_structure_first_events_for_t0(
    config: Mapping[str, Any],
    *,
    t0_session: date,
    research_cutoff: date,
) -> dict[str, Any]:
    """消费 R04 D2 状态、Radar 事件与冻结线，计算并保存原 T0 观察的结构首事件。"""
    if t0_session > research_cutoff:
        raise ValueError("T0不得晚于研究截止日")
    _, _, _, sessions = _load_calendar(research_cutoff)
    if t0_session not in sessions:
        raise ValueError("T0不在官方交易日历中")
    t0_idx = sessions.index(t0_session)
    due_horizons = [
        (h, sessions[t0_idx + 1 : t0_idx + h + 1])
        for h in (1, 3, 5, 10, 20)
        if t0_idx + h < len(sessions) and sessions[t0_idx + h] <= research_cutoff
    ]
    if not due_horizons:
        return {
            "status": "NOT_DUE",
            "t0_session": t0_session.isoformat(),
            "inserted_rows": 0,
        }
    max_days = due_horizons[-1][1]

    with _transaction(config, write=False, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT id, entity_id, episode_id, source_symbol
               FROM stock.formal_research_observations
               WHERE t0_session=%s AND entity_type='STOCK'
                 AND enrollment_kind IN ('ENTRY', 'DAILY_LANDMARK', 'REENTRY', 'FIRST_OBSERVED_LEFT_CENSORED')""",
            (t0_session,),
        )
        obs_rows = cursor.fetchall()
        if not obs_rows:
            return {
                "status": "NO_OBSERVATIONS",
                "t0_session": t0_session.isoformat(),
                "inserted_rows": 0,
            }

        entity_ids = sorted({r[1] for r in obs_rows})
        cursor.execute(
            """SELECT session_date, entity_id, episode_id, validity_value,
                      display_maturity, tracking, exit_ordinal, transitions
               FROM stock.d2_episode_day_states
               WHERE session_date=ANY(%s) AND entity_id=ANY(%s)""",
            (list(max_days), entity_ids),
        )
        d2_by_key: dict[tuple[date, str, str], dict[str, Any]] = {}
        d2_by_entity_day: dict[tuple[date, str], list[dict[str, Any]]] = {}
        for s_date, e_id, ep_id, val_st, d_mat, trk, ex_ord, trans in cursor.fetchall():
            events: list[str] = []
            if val_st == "INVALIDATED" or "INVALIDATED" in (trans or ()):
                events.append("INVALIDATED")
            if d_mat == "EXPIRED" or "EXPIRED" in (trans or ()):
                events.append("EXPIRED")
            if trk == "EXITED" or ex_ord is not None or "EXITED" in (trans or ()):
                events.append("EXITED")
            for t in trans or ():
                if t not in events:
                    events.append(t)
            item = {"status": "OBSERVED", "events": events}
            d2_by_key[(s_date, e_id, ep_id)] = item
            d2_by_entity_day.setdefault((s_date, e_id), []).append(item)

        cursor.execute(
            """SELECT target_session, source_symbol, episode_id, event_type
               FROM stock.stock_radar_day_events
               WHERE target_session=ANY(%s)""",
            (list(max_days),),
        )
        radar_by_sym_day: dict[tuple[date, str, str], list[str]] = {}
        for s_date, sym, ep_id, ev_type in cursor.fetchall():
            radar_by_sym_day.setdefault((s_date, sym, ep_id), []).append(ev_type)

    rows_to_write: list[tuple[Any, ...]] = []
    obs_ids = [int(o[0]) for o in obs_rows]
    for obs_id, entity_id, episode_id, sym in obs_rows:
        ep_key = episode_id or "PRIMARY"
        for h, h_days in due_horizons:
            day_map: dict[date, dict[str, Any]] = {}
            for d in h_days:
                if episode_id:
                    info = d2_by_key.get((d, entity_id, episode_id))
                else:
                    lst = d2_by_entity_day.get((d, entity_id), ())
                    if lst:
                        merged = [ev for it in lst for ev in it["events"]]
                        info = {"status": "OBSERVED", "events": merged}
                    else:
                        info = None
                if info is not None:
                    ev_list = list(info["events"])
                    for r_ev in radar_by_sym_day.get((d, sym or "", ep_key), ()):
                        if r_ev not in ev_list:
                            ev_list.append(r_ev)
                    day_map[d] = {"status": "OBSERVED", "events": ev_list}
            res = evaluate_structure_first_events(
                sessions=h_days,
                day_events_by_session=day_map,
                horizon_complete=True,
            )
            rows_to_write.append(
                (
                    obs_id,
                    t0_session,
                    entity_id,
                    ep_key,
                    h,
                    res["first_exit_type"],
                    res["first_exit_session"],
                    res["first_exit_quality"],
                    res["known_upper_bound_session"],
                    res["known_upper_bound_type"],
                    res["first_structure_event_type"],
                    res["first_structure_event_session"],
                    res["any_events"],
                    res["reason_codes"],
                )
            )

    with _transaction(config, write=True, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            "DELETE FROM stock.formal_structure_first_events WHERE observation_id=ANY(%s)",
            (obs_ids,),
        )
        if rows_to_write:
            cursor.executemany(
                """INSERT INTO stock.formal_structure_first_events (
                    observation_id, t0_session, entity_id, episode_id, horizon,
                    first_exit_type, first_exit_session, first_exit_quality,
                    known_upper_bound_session, known_upper_bound_type,
                    first_structure_event_type, first_structure_event_session,
                    any_events, reason_codes
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                rows_to_write,
            )
    return {
        "status": "PERSISTED",
        "t0_session": t0_session.isoformat(),
        "inserted_rows": len(rows_to_write),
    }


def read_formal_structure_first_events(
    config: Mapping[str, Any],
    *,
    t0_session: date,
    observation_id: int | None = None,
) -> dict[str, Any]:
    """只读查询原 T0 结构首事件与首退出结果。"""
    with _transaction(config, write=False, statement_timeout_ms=15000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT observation_id, entity_id, episode_id, horizon,
                      first_exit_type, first_exit_session, first_exit_quality,
                      known_upper_bound_session, known_upper_bound_type,
                      first_structure_event_type, first_structure_event_session,
                      any_events, reason_codes
               FROM stock.formal_structure_first_events
               WHERE t0_session=%s
                 AND (%s::bigint IS NULL OR observation_id=%s)
               ORDER BY observation_id, horizon""",
            (t0_session, observation_id, observation_id),
        )
        items = [
            {
                "observation_id": int(r[0]),
                "entity_id": r[1],
                "episode_id": r[2],
                "horizon": int(r[3]),
                "first_exit_type": r[4],
                "first_exit_session": r[5].isoformat() if r[5] else None,
                "first_exit_quality": r[6],
                "known_upper_bound_session": r[7].isoformat() if r[7] else None,
                "known_upper_bound_type": r[8],
                "first_structure_event_type": r[9],
                "first_structure_event_session": r[10].isoformat() if r[10] else None,
                "any_events": list(r[11] or ()),
                "reason_codes": list(r[12] or ()),
            }
            for r in cursor.fetchall()
        ]
    return {
        "status": "AVAILABLE" if items else "NO_STORED_FACT",
        "t0_session": t0_session.isoformat(),
        "items": items,
    }
