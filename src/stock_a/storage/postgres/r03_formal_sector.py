"""R03 正式板块 Native、Rotation、Overlap 与 LOO 的 PG 存储、只读查询与有界清理。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
from decimal import Decimal
from fractions import Fraction
from typing import Any

from stock_a.research.sector_rotation import FrozenRotationBasket
from stock_a.storage.postgres import source_raw


class R03FormalSectorPgError(RuntimeError):
    """R03 正式 PG 存储边界异常。"""

    preserve_transaction_error = True

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


def _dec_eq(a: Any, b: Any) -> bool:
    da = _num(a)
    db = _num(b)
    if da is None and db is None:
        return True
    if da is None or db is None:
        return False
    return da.normalize() == db.normalize()


def write_r03_native_day_facts(
    config: Mapping[str, Any],
    *,
    trade_date: date,
    scope_key: str,
    member_version: str,
    contract_version: str,
    rows: Sequence[Mapping[str, Any]],
    allow_revision: bool = False,
) -> dict[str, int]:
    """幂等写入单日全板块正式 Native、资格、前态、风险与 Priority 事实；校验同键内容一致性。"""
    inserted = 0
    updated = 0
    skipped = 0
    with source_raw._transaction(config, write=True, statement_timeout_ms=15000) as (
        _conn,
        cur,
        _,
    ):
        for row in rows:
            vals = (
                int(row["n"]),
                int(row["actual_quote_count"]),
                int(row["suspended_count"]),
                int(row["unknown_quote_count"]),
                int(row.get("isolated_count", 0)),
                int(row.get("joined_count", 0)),
                int(row.get("exited_count", 0)),
                int(row["rs5"]["known"]),
                int(row["rs20"]["known"]),
                int(row["breadth"]["known"]),
                int(row["ma20_width"]["known"]),
                int(row["amr20_median"]["known"]),
                int(row.get("peer_count", 0)),
                _num(row["rs5"]["value"]),
                _num(row["rs20"]["value"]),
                _num(row.get("rs5_pct")),
                _num(row.get("rs5_pct_t3")),
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
                row["lifecycle"]["maturity_lower"],
                row["lifecycle"]["maturity_upper"],
                row["lifecycle"]["display_maturity"],
                row["lifecycle"].get("last_known_maturity"),
                row["lifecycle"]["display_quality"],
                int(row["lifecycle"]["downgrade_streak"]),
                row["lifecycle"].get("downgrade_target"),
                int(row["lifecycle"]["prewatch_no_improve_count"]),
                _num(row["lifecycle"].get("prewatch_dq5_baseline")),
                bool(row["lifecycle"].get("prewatch_exited", False)),
                row["lifecycle"]["health"],
                row["exhaustion"]["level"],
                row["emergence"]["level"],
                row["priority"]["priority_status"],
                row["priority"].get("rank_position"),
                row["priority"].get("reason"),
            )
            cur.execute(
                """
                SELECT member_count, actual_quote_count, suspended_count, unknown_quote_count,
                       isolated_count, joined_count, exited_count, rs5_known_count,
                       rs20_known_count, breadth_known_count, ma20_known_count, amr20_known_count,
                       peer_count, rs5, rs20, rs5_pct, rs5_pct_t3, rs20_pct, dq5, breadth,
                       ma20_width, amr20_median, breadth_delta1, breadth_delta3, ma20_delta3,
                       seed_wilson_point, seed_wilson_lower, seed_wilson_upper,
                       top1_concentration, top3_concentration, safety_state, prewatch_raw_state,
                       warm_raw_state, confirmed_raw_state, effective_maturity, maturity_lower,
                       maturity_upper, display_maturity, last_known_maturity, display_quality,
                       downgrade_streak, downgrade_target, prewatch_no_improve_count,
                       prewatch_dq5_baseline, prewatch_exited, health_state, exhaustion_level,
                       emergence_level, priority_status, priority_rank, reason_code
                FROM stock.r03_sector_native_day_facts
                WHERE trade_date=%s AND board_kind=%s AND board_id=%s
                  AND scope_key=%s AND member_version=%s AND contract_version=%s
                """,
                (
                    trade_date,
                    row["board_kind"],
                    row["board_id"],
                    scope_key,
                    member_version,
                    contract_version,
                ),
            )
            existing = cur.fetchone()
            if existing is not None:
                same = all(
                    _dec_eq(e, v) if isinstance(v, Decimal) or isinstance(e, Decimal) else e == v
                    for e, v in zip(existing, vals, strict=True)
                )
                if same:
                    skipped += 1
                    continue
                if not allow_revision:
                    raise R03FormalSectorPgError(
                        "R03_NATIVE_KEY_CONTENT_CONFLICT",
                        f"同业务键({trade_date}, {row['board_kind']}, {row['board_id']})已存在不同内容且未授权修订。",
                    )
                cur.execute(
                    """
                    UPDATE stock.r03_sector_native_day_facts SET
                        member_count=%s, actual_quote_count=%s, suspended_count=%s, unknown_quote_count=%s,
                        isolated_count=%s, joined_count=%s, exited_count=%s, rs5_known_count=%s,
                        rs20_known_count=%s, breadth_known_count=%s, ma20_known_count=%s, amr20_known_count=%s,
                        peer_count=%s, rs5=%s, rs20=%s, rs5_pct=%s, rs5_pct_t3=%s, rs20_pct=%s, dq5=%s,
                        breadth=%s, ma20_width=%s, amr20_median=%s, breadth_delta1=%s, breadth_delta3=%s,
                        ma20_delta3=%s, seed_wilson_point=%s, seed_wilson_lower=%s, seed_wilson_upper=%s,
                        top1_concentration=%s, top3_concentration=%s, safety_state=%s, prewatch_raw_state=%s,
                        warm_raw_state=%s, confirmed_raw_state=%s, effective_maturity=%s, maturity_lower=%s,
                        maturity_upper=%s, display_maturity=%s, last_known_maturity=%s, display_quality=%s,
                        downgrade_streak=%s, downgrade_target=%s, prewatch_no_improve_count=%s,
                        prewatch_dq5_baseline=%s, prewatch_exited=%s, health_state=%s, exhaustion_level=%s,
                        emergence_level=%s, priority_status=%s, priority_rank=%s, reason_code=%s,
                        recorded_at=now()
                    WHERE trade_date=%s AND board_kind=%s AND board_id=%s
                      AND scope_key=%s AND member_version=%s AND contract_version=%s
                    """,
                    vals
                    + (
                        trade_date,
                        row["board_kind"],
                        row["board_id"],
                        scope_key,
                        member_version,
                        contract_version,
                    ),
                )
                updated += 1
                continue

            cur.execute(
                """
                INSERT INTO stock.r03_sector_native_day_facts (
                    trade_date, board_kind, board_id, scope_key, member_version, contract_version,
                    member_count, actual_quote_count, suspended_count, unknown_quote_count,
                    isolated_count, joined_count, exited_count, rs5_known_count,
                    rs20_known_count, breadth_known_count, ma20_known_count, amr20_known_count,
                    peer_count, rs5, rs20, rs5_pct, rs5_pct_t3, rs20_pct, dq5, breadth,
                    ma20_width, amr20_median, breadth_delta1, breadth_delta3, ma20_delta3,
                    seed_wilson_point, seed_wilson_lower, seed_wilson_upper,
                    top1_concentration, top3_concentration, safety_state, prewatch_raw_state,
                    warm_raw_state, confirmed_raw_state, effective_maturity, maturity_lower,
                    maturity_upper, display_maturity, last_known_maturity, display_quality,
                    downgrade_streak, downgrade_target, prewatch_no_improve_count,
                    prewatch_dq5_baseline, prewatch_exited, health_state, exhaustion_level,
                    emergence_level, priority_status, priority_rank, reason_code
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    trade_date,
                    row["board_kind"],
                    row["board_id"],
                    scope_key,
                    member_version,
                    contract_version,
                )
                + vals,
            )
            inserted += 1
    return {"inserted": inserted, "updated": updated, "skipped": skipped}


def read_r03_native_day_facts(
    config: Mapping[str, Any],
    *,
    trade_date: date,
    scope_key: str,
    member_version: str,
    contract_version: str,
    board_kind: str | None = None,
    limit: int | None = None,
    offset: int = 0,
) -> dict[str, Any]:
    """只读精确 scope/version 的 R03 Native 日级事实，支持稳定分页并返回状态与总分母。"""
    with source_raw._transaction(config, write=False, statement_timeout_ms=10000) as (
        _conn,
        cur,
        _,
    ):
        where_sql = (
            "WHERE trade_date=%s AND scope_key=%s AND member_version=%s AND contract_version=%s"
        )
        params: list[Any] = [trade_date, scope_key, member_version, contract_version]
        if board_kind is not None:
            where_sql += " AND board_kind=%s"
            params.append(board_kind)

        cur.execute(
            f"SELECT count(*)::bigint FROM stock.r03_sector_native_day_facts {where_sql}",
            tuple(params),
        )
        total_count = int(cur.fetchone()[0])
        if total_count == 0:
            return {
                "status": "NOT_COMPUTED",
                "total_count": 0,
                "items": [],
                "reason": "该日期与精确口径尚无已保存的 R03 Native 事实。",
            }

        sql = f"""
            SELECT board_kind, board_id, member_count, actual_quote_count, suspended_count,
                   unknown_quote_count, isolated_count, joined_count, exited_count,
                   rs5_known_count, rs20_known_count, breadth_known_count, ma20_known_count,
                   amr20_known_count, peer_count, rs5, rs20, rs5_pct, rs5_pct_t3, rs20_pct, dq5,
                   breadth, ma20_width, amr20_median, breadth_delta1, breadth_delta3, ma20_delta3,
                   seed_wilson_point, seed_wilson_lower, seed_wilson_upper,
                   top1_concentration, top3_concentration, safety_state, prewatch_raw_state,
                   warm_raw_state, confirmed_raw_state, effective_maturity, maturity_lower,
                   maturity_upper, display_maturity, last_known_maturity, display_quality,
                   downgrade_streak, downgrade_target, prewatch_no_improve_count,
                   prewatch_dq5_baseline, prewatch_exited, health_state, exhaustion_level,
                   emergence_level, priority_status, priority_rank, reason_code
            FROM stock.r03_sector_native_day_facts
            {where_sql}
            ORDER BY priority_rank ASC NULLS LAST, board_kind ASC, board_id ASC
        """
        query_params = list(params)
        if limit is not None:
            sql += " LIMIT %s OFFSET %s"
            query_params.extend([int(limit), int(offset)])
        cur.execute(sql, tuple(query_params))
        cols = [d[0] for d in cur.description]
        items = [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]
        return {
            "status": "AVAILABLE",
            "total_count": total_count,
            "items": items,
            "reason": None,
        }


def read_r03_previous_lifecycle_states(
    config: Mapping[str, Any],
    *,
    previous_trade_date: date,
    scope_key: str,
    member_version: str,
    contract_version: str,
) -> dict[tuple[str, str], dict[str, Any]]:
    """从 PG 恢复上一官方交易日的板块成熟、降级目标与 PREWATCH 基准前态。"""
    res = read_r03_native_day_facts(
        config,
        trade_date=previous_trade_date,
        scope_key=scope_key,
        member_version=member_version,
        contract_version=contract_version,
    )
    if res["status"] != "AVAILABLE":
        return {}
    return {
        (item["board_kind"], item["board_id"]): item for item in res["items"]
    }


def write_r03_rotation_day(
    config: Mapping[str, Any],
    *,
    trade_date: date,
    scope_key: str,
    member_version: str,
    contract_version: str,
    records: Sequence[Mapping[str, Any]],
    allow_revision: bool = False,
) -> dict[str, int]:
    """在单事务中写入 Rotation Episode（含冻结篮子成员）及逐日状态推进。"""
    episodes_inserted = 0
    states_inserted = 0
    states_updated = 0
    states_skipped = 0
    with source_raw._transaction(config, write=True, statement_timeout_ms=15000) as (
        _conn,
        cur,
        _,
    ):
        for rec in records:
            basket: FrozenRotationBasket | None = rec.get("basket")
            episode_pg_id: int | None = rec.get("episode_pg_id")
            if episode_pg_id is None and basket is not None:
                cur.execute(
                    """
                    SELECT id, frozen_member_count, s0_count, u0_count, h0_count
                    FROM stock.r03_sector_rotation_episodes
                    WHERE episode_key=%s AND scope_key=%s AND contract_version=%s
                    """,
                    (basket.episode_id, scope_key, contract_version),
                )
                ep_row = cur.fetchone()
                if ep_row is None:
                    cur.execute(
                        """
                        INSERT INTO stock.r03_sector_rotation_episodes (
                            episode_key, board_kind, board_id, t0_session, scope_key,
                            member_version, contract_version, parent_episode_id,
                            frozen_member_count, s0_count, u0_count, h0_count,
                            s0_known, u0_known, h0_known, left_censored
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                        """,
                        (
                            basket.episode_id,
                            basket.board_kind,
                            basket.board_id,
                            date.fromisoformat(basket.t0_session),
                            scope_key,
                            member_version,
                            contract_version,
                            rec.get("parent_episode_id"),
                            len(basket.members),
                            len(basket.s0_seed),
                            len(basket.u0_up),
                            len(basket.h0_strong),
                            bool(basket.s0_known),
                            bool(basket.u0_known),
                            bool(basket.h0_known),
                            bool(basket.left_censored),
                        ),
                    )
                    episode_pg_id = int(cur.fetchone()[0])
                    episodes_inserted += 1
                    for eid in basket.members:
                        cur.execute(
                            """
                            INSERT INTO stock.r03_sector_rotation_basket_members (
                                episode_id, entity_id, weight, b_price, p_price,
                                in_s0, in_u0, in_h0
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                episode_pg_id,
                                eid,
                                _num(basket.weights[eid]),
                                _num(basket.b_prices[eid]),
                                _num(basket.p_prices[eid]),
                                eid in basket.s0_seed,
                                eid in basket.u0_up,
                                eid in basket.h0_strong,
                            ),
                        )
                else:
                    episode_pg_id = int(ep_row[0])

            if episode_pg_id is None:
                continue

            st = rec["state_result"]
            bm = rec.get("basket_metrics") or {}
            vals = (
                st["state"],
                st.get("last_known_state"),
                bool(st.get("stale", False)),
                int(st.get("market_age", 0)),
                int(st.get("eval_age", 0)),
                bool(st.get("accepted_once", False)),
                bool(st.get("armed", False)),
                int(st.get("out_neg_streak", 0)),
                bool(st.get("prev_mature_true", False)),
                _num(st.get("dq5")),
                _num(bm.get("formation_r")),
                _num(bm.get("post_r")),
                _num((bm.get("seed_retention") or {}).get("value")),
                _num((bm.get("breadth_retention") or {}).get("value")),
                _num((bm.get("strong_retention") or {}).get("value")),
                (bm.get("diffusion") or {}).get("state", "UNKNOWN"),
                (bm.get("early") or {}).get("state", "UNKNOWN"),
                (bm.get("mature") or {}).get("state", "UNKNOWN"),
                st.get("reason"),
            )
            cur.execute(
                """
                SELECT rotation_state, last_known_state, stale, market_age, eval_age,
                       accepted_once, armed, out_neg_streak, prev_mature_true, dq5,
                       formation_r, post_r, seed_retention, breadth_retention,
                       strong_retention, diffusion_state, early_state, mature_state, reason_code
                FROM stock.r03_sector_rotation_day_states
                WHERE episode_id=%s AND trade_date=%s
                """,
                (episode_pg_id, trade_date),
            )
            existing = cur.fetchone()
            if existing is not None:
                same = all(
                    _dec_eq(e, v) if isinstance(v, Decimal) or isinstance(e, Decimal) else e == v
                    for e, v in zip(existing, vals, strict=True)
                )
                if same:
                    states_skipped += 1
                    continue
                if not allow_revision:
                    raise R03FormalSectorPgError(
                        "R03_ROTATION_KEY_CONTENT_CONFLICT",
                        f"Rotation 日状态({episode_pg_id}, {trade_date})已存在不同内容。",
                    )
                cur.execute(
                    """
                    UPDATE stock.r03_sector_rotation_day_states SET
                        rotation_state=%s, last_known_state=%s, stale=%s, market_age=%s,
                        eval_age=%s, accepted_once=%s, armed=%s, out_neg_streak=%s,
                        prev_mature_true=%s, dq5=%s, formation_r=%s, post_r=%s,
                        seed_retention=%s, breadth_retention=%s, strong_retention=%s,
                        diffusion_state=%s, early_state=%s, mature_state=%s, reason_code=%s,
                        recorded_at=now()
                    WHERE episode_id=%s AND trade_date=%s
                    """,
                    vals + (episode_pg_id, trade_date),
                )
                states_updated += 1
                continue

            cur.execute(
                """
                INSERT INTO stock.r03_sector_rotation_day_states (
                    episode_id, trade_date, rotation_state, last_known_state, stale,
                    market_age, eval_age, accepted_once, armed, out_neg_streak,
                    prev_mature_true, dq5, formation_r, post_r, seed_retention,
                    breadth_retention, strong_retention, diffusion_state, early_state,
                    mature_state, reason_code
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (episode_pg_id, trade_date) + vals,
            )
            states_inserted += 1
    return {
        "episodes_inserted": episodes_inserted,
        "states_inserted": states_inserted,
        "states_updated": states_updated,
        "states_skipped": states_skipped,
    }


def read_r03_rotation_recovery_states(
    config: Mapping[str, Any],
    *,
    before_trade_date: date,
    scope_key: str,
    contract_version: str,
) -> dict[tuple[str, str], dict[str, Any]]:
    """跨重启从 PG 恢复各板块最新 Rotation Episode、冻结篮子与最近日状态前态。"""
    with source_raw._transaction(config, write=False, statement_timeout_ms=10000) as (
        _conn,
        cur,
        _,
    ):
        cur.execute(
            """
            SELECT DISTINCT ON (e.board_kind, e.board_id)
                   e.id, e.episode_key, e.board_kind, e.board_id, e.t0_session,
                   e.parent_episode_id, e.s0_known, e.u0_known, e.h0_known, e.left_censored,
                   s.trade_date, s.rotation_state, s.last_known_state, s.stale,
                   s.market_age, s.eval_age, s.accepted_once, s.armed, s.out_neg_streak,
                   s.prev_mature_true, s.dq5
            FROM stock.r03_sector_rotation_episodes e
            JOIN stock.r03_sector_rotation_day_states s ON s.episode_id = e.id
            WHERE e.scope_key=%s AND e.contract_version=%s AND s.trade_date < %s
            ORDER BY e.board_kind, e.board_id, s.trade_date DESC, e.id DESC
            """,
            (scope_key, contract_version, before_trade_date),
        )
        rows = cur.fetchall()
        out: dict[tuple[str, str], dict[str, Any]] = {}
        for r in rows:
            ep_id = int(r[0])
            cur.execute(
                """
                SELECT entity_id, weight, b_price, p_price, in_s0, in_u0, in_h0
                FROM stock.r03_sector_rotation_basket_members
                WHERE episode_id=%s
                ORDER BY entity_id
                """,
                (ep_id,),
            )
            m_rows = cur.fetchall()
            members = tuple(mr[0] for mr in m_rows)
            n_members = len(m_rows)
            exact_eq_weight = Fraction(1, n_members) if n_members > 0 else Fraction(0)
            weights = {
                mr[0]: (
                    exact_eq_weight
                    if n_members > 0 and abs(Fraction(mr[1]) - exact_eq_weight) <= Fraction(1, 10**6)
                    else Fraction(mr[1])
                )
                for mr in m_rows
            }
            b_prices = {mr[0]: Fraction(mr[2]) for mr in m_rows}
            p_prices = {mr[0]: Fraction(mr[3]) for mr in m_rows}
            s0 = frozenset(mr[0] for mr in m_rows if mr[4])
            u0 = frozenset(mr[0] for mr in m_rows if mr[5])
            h0 = frozenset(mr[0] for mr in m_rows if mr[6])
            basket = FrozenRotationBasket(
                episode_id=str(r[1]),
                board_id=str(r[3]),
                board_kind=str(r[2]),
                t0_session=r[4].isoformat(),
                members=members,
                weights=weights,
                b_prices=b_prices,
                p_prices=p_prices,
                s0_seed=s0,
                u0_up=u0,
                h0_strong=h0,
                s0_known=bool(r[6]),
                u0_known=bool(r[7]),
                h0_known=bool(r[8]),
                left_censored=bool(r[9]),
            )
            out[(str(r[2]), str(r[3]))] = {
                "episode_pg_id": ep_id,
                "parent_episode_id": r[5],
                "basket": basket,
                "prev_trade_date": r[10],
                "state_dict": {
                    "state": str(r[11]),
                    "last_known_state": r[12],
                    "stale": bool(r[13]),
                    "market_age": int(r[14]),
                    "eval_age": int(r[15]),
                    "accepted_once": bool(r[16]),
                    "armed": bool(r[17]),
                    "out_neg_streak": int(r[18]),
                    "prev_mature_true": bool(r[19]),
                    "dq5": Fraction(r[20]) if r[20] is not None else None,
                },
            }
        return out


def read_r03_rotation_timeline(
    config: Mapping[str, Any],
    *,
    board_kind: str,
    board_id: str,
    scope_key: str,
    contract_version: str,
) -> dict[str, Any]:
    """只读板块完整 Rotation Episode、冻结篮子成员与逐日状态时间线。"""
    with source_raw._transaction(config, write=False, statement_timeout_ms=10000) as (
        _conn,
        cur,
        _,
    ):
        cur.execute(
            """
            SELECT id, episode_key, t0_session, parent_episode_id,
                   frozen_member_count, s0_count, u0_count, h0_count,
                   s0_known, u0_known, h0_known, left_censored
            FROM stock.r03_sector_rotation_episodes
            WHERE board_kind=%s AND board_id=%s AND scope_key=%s AND contract_version=%s
            ORDER BY t0_session ASC, id ASC
            """,
            (board_kind, board_id, scope_key, contract_version),
        )
        ep_rows = cur.fetchall()
        if not ep_rows:
            return {
                "status": "NOT_COMPUTED",
                "episodes": [],
                "reason": "该板块尚无正式 Rotation Episode 事实。",
            }
        episodes = []
        for ep in ep_rows:
            ep_id = int(ep[0])
            cur.execute(
                """
                SELECT entity_id, weight, b_price, p_price, in_s0, in_u0, in_h0
                FROM stock.r03_sector_rotation_basket_members
                WHERE episode_id=%s ORDER BY entity_id
                """,
                (ep_id,),
            )
            m_cols = [d[0] for d in cur.description]
            members = [dict(zip(m_cols, mr, strict=True)) for mr in cur.fetchall()]
            cur.execute(
                """
                SELECT trade_date, rotation_state, last_known_state, stale, market_age,
                       eval_age, accepted_once, armed, out_neg_streak, formation_r, post_r,
                       seed_retention, breadth_retention, strong_retention,
                       diffusion_state, early_state, mature_state, reason_code
                FROM stock.r03_sector_rotation_day_states
                WHERE episode_id=%s ORDER BY trade_date ASC
                """,
                (ep_id,),
            )
            s_cols = [d[0] for d in cur.description]
            states = [dict(zip(s_cols, sr, strict=True)) for sr in cur.fetchall()]
            episodes.append(
                {
                    "episode_pg_id": ep_id,
                    "episode_key": ep[1],
                    "t0_session": ep[2].isoformat(),
                    "parent_episode_id": ep[3],
                    "frozen_member_count": ep[4],
                    "s0_count": ep[5],
                    "u0_count": ep[6],
                    "h0_count": ep[7],
                    "s0_known": ep[8],
                    "u0_known": ep[9],
                    "h0_known": ep[10],
                    "left_censored": ep[11],
                    "frozen_members": members,
                    "day_states": states,
                }
            )
        return {"status": "AVAILABLE", "episodes": episodes, "reason": None}


def write_r03_overlap_results(
    config: Mapping[str, Any],
    *,
    trade_date: date,
    scope_key: str,
    member_version: str,
    contract_version: str,
    overlap: Mapping[str, Any],
    native_rows: Sequence[Mapping[str, Any]],
    allow_revision: bool = False,
) -> dict[str, int]:
    """幂等写入单日 Overlap 主题簇、代表及每个合格板块的 unique_share 与摘要标记。"""
    clusters = overlap["clusters"]
    unique_shares = overlap["unique_shares"]
    overview_selected = overlap["overview_selected"]
    cluster_by_board = {
        (c["board_kind"], bid): c["cluster_id"]
        for c in clusters
        for bid in c["member_board_ids"]
    }

    c_ins = c_upd = c_skip = 0
    b_ins = b_upd = b_skip = 0
    with source_raw._transaction(config, write=True, statement_timeout_ms=15000) as (
        _conn,
        cur,
        _,
    ):
        for c in clusters:
            vals = (
                c["cluster_status"],
                list(c["member_board_ids"]),
                list(c["parent_cluster_ids"]),
                c["representative_board_id"],
                c["representative_status"],
            )
            cur.execute(
                """
                SELECT cluster_status, member_board_ids, parent_cluster_ids,
                       representative_board_id, representative_status
                FROM stock.r03_sector_overlap_clusters
                WHERE trade_date=%s AND board_kind=%s AND cluster_id=%s
                  AND scope_key=%s AND member_version=%s AND contract_version=%s
                """,
                (
                    trade_date,
                    c["board_kind"],
                    c["cluster_id"],
                    scope_key,
                    member_version,
                    contract_version,
                ),
            )
            existing = cur.fetchone()
            if existing is not None:
                if existing == vals:
                    c_skip += 1
                    continue
                if not allow_revision:
                    raise R03FormalSectorPgError(
                        "R03_OVERLAP_KEY_CONTENT_CONFLICT",
                        f"Overlap簇({trade_date}, {c['cluster_id']})已存在不同内容。",
                    )
                cur.execute(
                    """
                    UPDATE stock.r03_sector_overlap_clusters SET
                        cluster_status=%s, member_board_ids=%s, parent_cluster_ids=%s,
                        representative_board_id=%s, representative_status=%s, recorded_at=now()
                    WHERE trade_date=%s AND board_kind=%s AND cluster_id=%s
                      AND scope_key=%s AND member_version=%s AND contract_version=%s
                    """,
                    vals
                    + (
                        trade_date,
                        c["board_kind"],
                        c["cluster_id"],
                        scope_key,
                        member_version,
                        contract_version,
                    ),
                )
                c_upd += 1
                continue
            cur.execute(
                """
                INSERT INTO stock.r03_sector_overlap_clusters (
                    trade_date, board_kind, cluster_id, scope_key, member_version,
                    contract_version, cluster_status, member_board_ids,
                    parent_cluster_ids, representative_board_id, representative_status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    trade_date,
                    c["board_kind"],
                    c["cluster_id"],
                    scope_key,
                    member_version,
                    contract_version,
                )
                + vals,
            )
            c_ins += 1

        for nr in native_rows:
            bk = str(nr["board_kind"])
            bid = str(nr["board_id"])
            if (bk, bid) not in unique_shares and bid not in unique_shares and (bk, bid) not in cluster_by_board:
                continue
            us = unique_shares.get(
                (bk, bid),
                unique_shares.get(bid, {"status": "NA", "value": None, "reason": "SAFETY_NOT_TRUE"}),
            )
            vals_b = (
                us["status"],
                _num(us.get("value")),
                cluster_by_board.get((bk, bid)),
                bid in overview_selected.get(bk, []),
                us.get("reason"),
            )
            cur.execute(
                """
                SELECT unique_share_status, unique_share, cluster_id,
                       in_overview_summary, reason_code
                FROM stock.r03_sector_overlap_board_facts
                WHERE trade_date=%s AND board_kind=%s AND board_id=%s
                  AND scope_key=%s AND member_version=%s AND contract_version=%s
                """,
                (trade_date, bk, bid, scope_key, member_version, contract_version),
            )
            existing_b = cur.fetchone()
            if existing_b is not None:
                same_b = all(
                    _dec_eq(e, v) if isinstance(v, Decimal) or isinstance(e, Decimal) else e == v
                    for e, v in zip(existing_b, vals_b, strict=True)
                )
                if same_b:
                    b_skip += 1
                    continue
                if not allow_revision:
                    raise R03FormalSectorPgError(
                        "R03_OVERLAP_BOARD_CONFLICT",
                        f"Overlap板块({trade_date}, {bk}, {bid})已存在不同内容。",
                    )
                cur.execute(
                    """
                    UPDATE stock.r03_sector_overlap_board_facts SET
                        unique_share_status=%s, unique_share=%s, cluster_id=%s,
                        in_overview_summary=%s, reason_code=%s, recorded_at=now()
                    WHERE trade_date=%s AND board_kind=%s AND board_id=%s
                      AND scope_key=%s AND member_version=%s AND contract_version=%s
                    """,
                    vals_b + (trade_date, bk, bid, scope_key, member_version, contract_version),
                )
                b_upd += 1
                continue
            cur.execute(
                """
                INSERT INTO stock.r03_sector_overlap_board_facts (
                    trade_date, board_kind, board_id, scope_key, member_version,
                    contract_version, unique_share_status, unique_share,
                    cluster_id, in_overview_summary, reason_code
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (trade_date, bk, bid, scope_key, member_version, contract_version)
                + vals_b,
            )
            b_ins += 1
    return {
        "clusters_inserted": c_ins,
        "clusters_updated": c_upd,
        "clusters_skipped": c_skip,
        "board_facts_inserted": b_ins,
        "board_facts_updated": b_upd,
        "board_facts_skipped": b_skip,
    }


def read_r03_overlap_day(
    config: Mapping[str, Any],
    *,
    trade_date: date,
    scope_key: str,
    member_version: str,
    contract_version: str,
    board_kind: str | None = None,
) -> dict[str, Any]:
    """只读精确口径的 Overlap 簇、代表与板块 unique_share 事实。"""
    with source_raw._transaction(config, write=False, statement_timeout_ms=10000) as (
        _conn,
        cur,
        _,
    ):
        where_sql = (
            "WHERE trade_date=%s AND scope_key=%s AND member_version=%s AND contract_version=%s"
        )
        params: list[Any] = [trade_date, scope_key, member_version, contract_version]
        if board_kind is not None:
            where_sql += " AND board_kind=%s"
            params.append(board_kind)
        cur.execute(
            f"""
            SELECT board_kind, cluster_id, cluster_status, member_board_ids,
                   parent_cluster_ids, representative_board_id, representative_status
            FROM stock.r03_sector_overlap_clusters
            {where_sql}
            ORDER BY board_kind ASC, cluster_id ASC
            """,
            tuple(params),
        )
        c_cols = [d[0] for d in cur.description]
        clusters = [dict(zip(c_cols, r, strict=True)) for r in cur.fetchall()]
        cur.execute(
            f"""
            SELECT board_kind, board_id, unique_share_status, unique_share,
                   cluster_id, in_overview_summary, reason_code
            FROM stock.r03_sector_overlap_board_facts
            {where_sql}
            ORDER BY board_kind ASC, board_id ASC
            """,
            tuple(params),
        )
        b_cols = [d[0] for d in cur.description]
        board_facts = [dict(zip(b_cols, r, strict=True)) for r in cur.fetchall()]
        if not clusters and not board_facts:
            return {
                "status": "NOT_COMPUTED",
                "clusters": [],
                "board_facts": [],
                "reason": "该日期与口径尚无已保存的 Overlap 事实。",
            }
        return {
            "status": "AVAILABLE",
            "clusters": clusters,
            "board_facts": board_facts,
            "reason": None,
        }


def write_r03_loo_support_fact(
    config: Mapping[str, Any],
    *,
    trade_date: date,
    board_kind: str,
    scope_key: str,
    member_version: str,
    contract_version: str,
    loo_result: Mapping[str, Any],
    allow_revision: bool = False,
) -> dict[str, int]:
    """幂等写入单股 LOO 算法支持板块结果及全部候选板块重算证据。"""
    inserted = updated = skipped = 0
    with source_raw._transaction(config, write=True, statement_timeout_ms=15000) as (
        _conn,
        cur,
        _,
    ):
        vals = (
            loo_result.get("primary_industry_id"),
            loo_result.get("algorithm_support_board_id"),
            loo_result["support_status"],
            loo_result.get("reason"),
        )
        cur.execute(
            """
            SELECT id, primary_industry_id, algorithm_support_board_id, support_status, reason_code
            FROM stock.r03_sector_loo_support_facts
            WHERE trade_date=%s AND target_entity_id=%s AND board_kind=%s
              AND scope_key=%s AND member_version=%s AND contract_version=%s
            """,
            (
                trade_date,
                loo_result["target_entity_id"],
                board_kind,
                scope_key,
                member_version,
                contract_version,
            ),
        )
        existing = cur.fetchone()
        if existing is not None:
            loo_id = int(existing[0])
            cur.execute(
                """
                SELECT board_id, safety_state, prewatch_raw_state, warm_raw_state, confirmed_raw_state
                FROM stock.r03_sector_loo_candidate_facts
                WHERE loo_fact_id=%s
                ORDER BY board_id
                """,
                (loo_id,),
            )
            existing_cands = [
                (str(r[0]), str(r[1]), str(r[2]), str(r[3]), str(r[4])) for r in cur.fetchall()
            ]
            new_cands = sorted(
                (
                    str(c["board_id"]),
                    str(c["raw_qualification"]["safety"]["state"]),
                    str(c["raw_qualification"]["prewatch_raw"]["state"]),
                    str(c["raw_qualification"]["warm_raw"]["state"]),
                    str(c["raw_qualification"]["confirmed_raw"]["state"]),
                )
                for c in loo_result.get("loo_boards", ())
            )
            if existing[1:] == vals and existing_cands == new_cands:
                skipped += 1
            elif not allow_revision:
                raise R03FormalSectorPgError(
                    "R03_LOO_KEY_CONTENT_CONFLICT",
                    f"LOO支持事实({trade_date}, {loo_result['target_entity_id']}, {board_kind})已存在不同内容。",
                )
            else:
                cur.execute(
                    """
                    UPDATE stock.r03_sector_loo_support_facts SET
                        primary_industry_id=%s, algorithm_support_board_id=%s,
                        support_status=%s, reason_code=%s, recorded_at=now()
                    WHERE id=%s
                    """,
                    vals + (loo_id,),
                )
                cur.execute(
                    "DELETE FROM stock.r03_sector_loo_candidate_facts WHERE loo_fact_id=%s",
                    (loo_id,),
                )
                updated += 1
        else:
            cur.execute(
                """
                INSERT INTO stock.r03_sector_loo_support_facts (
                    trade_date, target_entity_id, board_kind, scope_key, member_version,
                    contract_version, primary_industry_id, algorithm_support_board_id,
                    support_status, reason_code
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    trade_date,
                    loo_result["target_entity_id"],
                    board_kind,
                    scope_key,
                    member_version,
                    contract_version,
                )
                + vals,
            )
            loo_id = int(cur.fetchone()[0])
            inserted += 1

        if inserted or updated:
            for cand in loo_result.get("loo_boards", ()):
                rq = cand["raw_qualification"]
                cur.execute(
                    """
                    INSERT INTO stock.r03_sector_loo_candidate_facts (
                        loo_fact_id, board_id, safety_state, prewatch_raw_state,
                        warm_raw_state, confirmed_raw_state, emergence_level,
                        seed_wilson_point, rs5_pct, dq5, reason_code
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        loo_id,
                        cand["board_id"],
                        rq["safety"]["state"],
                        rq["prewatch_raw"]["state"],
                        rq["warm_raw"]["state"],
                        rq["confirmed_raw"]["state"],
                        cand["emergence"]["level"],
                        _num(cand["seed_wilson"].get("point_value")),
                        _num(cand.get("rs5_pct")),
                        _num(cand.get("dq5")),
                        cand["priority"].get("reason"),
                    ),
                )
    return {"inserted": inserted, "updated": updated, "skipped": skipped}


def read_r03_loo_support_fact(
    config: Mapping[str, Any],
    *,
    trade_date: date,
    target_entity_id: str,
    board_kind: str,
    scope_key: str,
    member_version: str,
    contract_version: str,
) -> dict[str, Any]:
    """只读单股精确口径的 LOO 算法支持板块与候选证据（供 R04 TREND_CONTINUE 等消费者使用）。"""
    with source_raw._transaction(config, write=False, statement_timeout_ms=10000) as (
        _conn,
        cur,
        _,
    ):
        cur.execute(
            """
            SELECT id, primary_industry_id, algorithm_support_board_id,
                   support_status, reason_code
            FROM stock.r03_sector_loo_support_facts
            WHERE trade_date=%s AND target_entity_id=%s AND board_kind=%s
              AND scope_key=%s AND member_version=%s AND contract_version=%s
            """,
            (
                trade_date,
                target_entity_id,
                board_kind,
                scope_key,
                member_version,
                contract_version,
            ),
        )
        row = cur.fetchone()
        if row is None:
            return {
                "status": "NOT_COMPUTED",
                "target_entity_id": target_entity_id,
                "board_kind": board_kind,
                "candidates": [],
                "reason": "该目标证券与口径尚无已保存的 LOO 支持板块事实。",
            }
        loo_id = int(row[0])
        cur.execute(
            """
            SELECT board_id, safety_state, prewatch_raw_state, warm_raw_state,
                   confirmed_raw_state, emergence_level, seed_wilson_point,
                   rs5_pct, dq5, reason_code
            FROM stock.r03_sector_loo_candidate_facts
            WHERE loo_fact_id=%s ORDER BY board_id ASC
            """,
            (loo_id,),
        )
        c_cols = [d[0] for d in cur.description]
        candidates = [dict(zip(c_cols, cr, strict=True)) for cr in cur.fetchall()]
        return {
            "status": "AVAILABLE",
            "target_entity_id": target_entity_id,
            "board_kind": board_kind,
            "primary_industry_id": row[1],
            "algorithm_support_board_id": row[2],
            "support_status": row[3],
            "reason": row[4],
            "candidates": candidates,
        }


def cleanup_r03_scope_day(
    config: Mapping[str, Any],
    *,
    trade_date: date,
    scope_key: str,
) -> dict[str, int]:
    """按精确日期与 scope 有界清理 R03 正式事实，不触碰其他捕获、日期或来源层。"""
    deleted: dict[str, int] = {}
    with source_raw._transaction(config, write=True, statement_timeout_ms=15000) as (
        _conn,
        cur,
        _,
    ):
        for table in (
            "stock.r03_sector_loo_support_facts",
            "stock.r03_sector_overlap_board_facts",
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
        # 清理已无任何 day_states 的孤儿 episode（若 t0_session == trade_date）
        cur.execute(
            """
            DELETE FROM stock.r03_sector_rotation_episodes e
            WHERE e.t0_session=%s AND e.scope_key=%s
              AND NOT EXISTS (
                  SELECT 1 FROM stock.r03_sector_rotation_day_states s
                  WHERE s.episode_id = e.id
              )
            """,
            (trade_date, scope_key),
        )
        deleted["stock.r03_sector_rotation_episodes"] = cur.rowcount
    return deleted
