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
    rows: Sequence[Mapping[str, Any]],
    allow_revision: bool = False,
) -> dict[str, int]:
    """幂等写入单日全板块正式 Native、资格、前态、风险与 Priority 事实（单一有效业务键替换）。"""
    inserted = 0
    updated = 0
    skipped = 0
    with source_raw._transaction(config, write=True, statement_timeout_ms=15000) as (
        _conn,
        cur,
        _,
    ):
        for row in rows:
            conc = row.get("concentration") or {}
            rq = row["raw_qualification"]
            lc = row["lifecycle"]
            sw = row["seed_wilson"]
            sg_raw = row.get("session_gap")
            if sg_raw is None:
                sg_raw = lc.get("session_gap")
            prev_td = row.get("prev_trade_date") or lc.get("prev_trade_date")
            j_cnt = row.get("joined_count")
            e_cnt = row.get("exited_count")
            m_delta_status = row.get("membership_delta_status") or (
                "OBSERVED" if (j_cnt is not None and e_cnt is not None) else "UNKNOWN"
            )
            vals = (
                int(row["n"]),
                int(row["actual_quote_count"]),
                int(row["suspended_count"]),
                int(row["unknown_quote_count"]),
                int(row.get("isolated_count", 0)),
                int(j_cnt) if j_cnt is not None else None,
                int(e_cnt) if e_cnt is not None else None,
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
                _num(sw.get("point_value")),
                _num(sw.get("lower")),
                _num(sw.get("upper")),
                _num(conc.get("top1_concentration")),
                _num(conc.get("top3_concentration")),
                rq["safety"]["state"],
                rq["prewatch_raw"]["state"],
                rq["warm_raw"]["state"],
                rq["confirmed_raw"]["state"],
                lc["effective_maturity"],
                lc["maturity_lower"],
                lc["maturity_upper"],
                lc["display_maturity"],
                lc.get("last_known_maturity"),
                lc["display_quality"],
                int(lc["downgrade_streak"]),
                lc.get("downgrade_target"),
                int(lc["prewatch_no_improve_count"]),
                _num(lc.get("prewatch_dq5_baseline")),
                bool(lc.get("prewatch_exited", False)),
                lc["health"],
                row["exhaustion"]["level"],
                row["emergence"]["level"],
                row["priority"]["priority_status"],
                row["priority"].get("rank_position"),
                row["priority"].get("reason"),
                bool(row.get("membership_complete", True)),
                str(m_delta_status),
                int(row.get("rs5_peer_count", row.get("peer_count", 0)) or 0),
                int(row.get("rs20_peer_count", row.get("peer_count", 0)) or 0),
                int(row.get("seed_k", sw.get("k", 0)) or 0),
                int(row.get("seed_f", sw.get("f", 0)) or 0),
                int(row.get("seed_u", sw.get("u", 0)) or 0),
                _num(row.get("quote_coverage")),
                int(row["breadth_delta1_common_count"])
                if row.get("breadth_delta1_common_count") is not None
                else None,
                int(row["breadth_delta3_common_count"])
                if row.get("breadth_delta3_common_count") is not None
                else None,
                int(row["ma20_delta3_common_count"])
                if row.get("ma20_delta3_common_count") is not None
                else None,
                str(conc.get("status", "UNKNOWN")),
                conc.get("reason"),
                rq["safety"].get("reason"),
                rq["prewatch_raw"].get("reason"),
                rq["warm_raw"].get("reason"),
                rq["confirmed_raw"].get("reason"),
                row["exhaustion"].get("reason"),
                row["emergence"].get("reason"),
                row["rs5"].get("reason"),
                row["rs20"].get("reason"),
                row["breadth"].get("reason"),
                row["ma20_width"].get("reason"),
                row["amr20_median"].get("reason"),
                prev_td,
                int(sg_raw) if sg_raw is not None else None,
                row.get("prev_exhaustion_level"),
                lc.get("last_known_display") or lc.get("last_known_maturity"),
                lc.get("last_known_effective") or lc.get("last_known_maturity"),
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
                       emergence_level, priority_status, priority_rank, reason_code,
                       membership_complete, membership_delta_status, rs5_peer_count, rs20_peer_count,
                       seed_k, seed_f, seed_u, quote_coverage,
                       breadth_delta1_common_count, breadth_delta3_common_count, ma20_delta3_common_count,
                       concentration_status, concentration_reason, safety_reason,
                       prewatch_raw_reason, warm_raw_reason, confirmed_raw_reason,
                       exhaustion_reason, emergence_reason, rs5_reason, rs20_reason,
                       breadth_reason, ma20_reason, amr20_reason,
                       prev_trade_date, session_gap, prev_exhaustion_level,
                       last_known_display, last_known_effective
                FROM stock.r03_sector_native_day_facts
                WHERE trade_date=%s AND board_kind=%s AND board_id=%s
                """,
                (trade_date, row["board_kind"], row["board_id"]),
            )
            existing = cur.fetchone()
            if existing is not None:
                same = all(
                    _dec_eq(e, v)
                    if isinstance(v, Decimal) or isinstance(e, Decimal)
                    else e == v
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
                INSERT INTO stock.r03_sector_native_day_facts (
                    trade_date, board_kind, board_id,
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
                    emergence_level, priority_status, priority_rank, reason_code,
                    membership_complete, membership_delta_status, rs5_peer_count, rs20_peer_count,
                    seed_k, seed_f, seed_u, quote_coverage,
                    breadth_delta1_common_count, breadth_delta3_common_count, ma20_delta3_common_count,
                    concentration_status, concentration_reason, safety_reason,
                    prewatch_raw_reason, warm_raw_reason, confirmed_raw_reason,
                    exhaustion_reason, emergence_reason, rs5_reason, rs20_reason,
                    breadth_reason, ma20_reason, amr20_reason,
                    prev_trade_date, session_gap, prev_exhaustion_level,
                    last_known_display, last_known_effective
                ) VALUES (
                    %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (trade_date, board_kind, board_id) DO UPDATE SET
                    member_count=EXCLUDED.member_count,
                    actual_quote_count=EXCLUDED.actual_quote_count,
                    suspended_count=EXCLUDED.suspended_count,
                    unknown_quote_count=EXCLUDED.unknown_quote_count,
                    isolated_count=EXCLUDED.isolated_count,
                    joined_count=EXCLUDED.joined_count,
                    exited_count=EXCLUDED.exited_count,
                    rs5_known_count=EXCLUDED.rs5_known_count,
                    rs20_known_count=EXCLUDED.rs20_known_count,
                    breadth_known_count=EXCLUDED.breadth_known_count,
                    ma20_known_count=EXCLUDED.ma20_known_count,
                    amr20_known_count=EXCLUDED.amr20_known_count,
                    peer_count=EXCLUDED.peer_count,
                    rs5=EXCLUDED.rs5,
                    rs20=EXCLUDED.rs20,
                    rs5_pct=EXCLUDED.rs5_pct,
                    rs5_pct_t3=EXCLUDED.rs5_pct_t3,
                    rs20_pct=EXCLUDED.rs20_pct,
                    dq5=EXCLUDED.dq5,
                    breadth=EXCLUDED.breadth,
                    ma20_width=EXCLUDED.ma20_width,
                    amr20_median=EXCLUDED.amr20_median,
                    breadth_delta1=EXCLUDED.breadth_delta1,
                    breadth_delta3=EXCLUDED.breadth_delta3,
                    ma20_delta3=EXCLUDED.ma20_delta3,
                    seed_wilson_point=EXCLUDED.seed_wilson_point,
                    seed_wilson_lower=EXCLUDED.seed_wilson_lower,
                    seed_wilson_upper=EXCLUDED.seed_wilson_upper,
                    top1_concentration=EXCLUDED.top1_concentration,
                    top3_concentration=EXCLUDED.top3_concentration,
                    safety_state=EXCLUDED.safety_state,
                    prewatch_raw_state=EXCLUDED.prewatch_raw_state,
                    warm_raw_state=EXCLUDED.warm_raw_state,
                    confirmed_raw_state=EXCLUDED.confirmed_raw_state,
                    effective_maturity=EXCLUDED.effective_maturity,
                    maturity_lower=EXCLUDED.maturity_lower,
                    maturity_upper=EXCLUDED.maturity_upper,
                    display_maturity=EXCLUDED.display_maturity,
                    last_known_maturity=EXCLUDED.last_known_maturity,
                    display_quality=EXCLUDED.display_quality,
                    downgrade_streak=EXCLUDED.downgrade_streak,
                    downgrade_target=EXCLUDED.downgrade_target,
                    prewatch_no_improve_count=EXCLUDED.prewatch_no_improve_count,
                    prewatch_dq5_baseline=EXCLUDED.prewatch_dq5_baseline,
                    prewatch_exited=EXCLUDED.prewatch_exited,
                    health_state=EXCLUDED.health_state,
                    exhaustion_level=EXCLUDED.exhaustion_level,
                    emergence_level=EXCLUDED.emergence_level,
                    priority_status=EXCLUDED.priority_status,
                    priority_rank=EXCLUDED.priority_rank,
                    reason_code=EXCLUDED.reason_code,
                    membership_complete=EXCLUDED.membership_complete,
                    membership_delta_status=EXCLUDED.membership_delta_status,
                    rs5_peer_count=EXCLUDED.rs5_peer_count,
                    rs20_peer_count=EXCLUDED.rs20_peer_count,
                    seed_k=EXCLUDED.seed_k,
                    seed_f=EXCLUDED.seed_f,
                    seed_u=EXCLUDED.seed_u,
                    quote_coverage=EXCLUDED.quote_coverage,
                    breadth_delta1_common_count=EXCLUDED.breadth_delta1_common_count,
                    breadth_delta3_common_count=EXCLUDED.breadth_delta3_common_count,
                    ma20_delta3_common_count=EXCLUDED.ma20_delta3_common_count,
                    concentration_status=EXCLUDED.concentration_status,
                    concentration_reason=EXCLUDED.concentration_reason,
                    safety_reason=EXCLUDED.safety_reason,
                    prewatch_raw_reason=EXCLUDED.prewatch_raw_reason,
                    warm_raw_reason=EXCLUDED.warm_raw_reason,
                    confirmed_raw_reason=EXCLUDED.confirmed_raw_reason,
                    exhaustion_reason=EXCLUDED.exhaustion_reason,
                    emergence_reason=EXCLUDED.emergence_reason,
                    rs5_reason=EXCLUDED.rs5_reason,
                    rs20_reason=EXCLUDED.rs20_reason,
                    breadth_reason=EXCLUDED.breadth_reason,
                    ma20_reason=EXCLUDED.ma20_reason,
                    amr20_reason=EXCLUDED.amr20_reason,
                    prev_trade_date=EXCLUDED.prev_trade_date,
                    session_gap=EXCLUDED.session_gap,
                    prev_exhaustion_level=EXCLUDED.prev_exhaustion_level,
                    last_known_display=EXCLUDED.last_known_display,
                    last_known_effective=EXCLUDED.last_known_effective,
                    recorded_at=now()
                """,
                (trade_date, row["board_kind"], row["board_id"]) + vals,
            )
            if existing is not None:
                updated += 1
            else:
                inserted += 1
    return {"inserted": inserted, "updated": updated, "skipped": skipped}


def _read_r03_partition_progress_state(
    cur: Any,
    *,
    trade_date: date,
    business_table: str,
) -> tuple[str | None, int | None]:
    """查询目标日业务表在 data_job_progress 的最新状态。"""
    cur.execute(
        """
        SELECT status, expected_rows
        FROM stock.data_job_progress
        WHERE target_session=%s AND business_table=%s
        ORDER BY updated_at DESC, job_id DESC
        LIMIT 1
        """,
        (trade_date, business_table),
    )
    row = cur.fetchone()
    if row is None:
        return None, None
    return str(row[0]), (int(row[1]) if row[1] is not None else None)


def read_r03_native_day_facts(
    config: Mapping[str, Any],
    *,
    trade_date: date,
    board_kind: str | None = None,
    limit: int | None = None,
    offset: int = 0,
) -> dict[str, Any]:
    """只读目标日单一有效 R03 Native 日级事实，支持稳定分页并返回状态与总分母。"""
    with source_raw._transaction(config, write=False, statement_timeout_ms=10000) as (
        _conn,
        cur,
        _,
    ):
        full_where_sql = "WHERE trade_date=%s"
        full_params: list[Any] = [trade_date]
        cur.execute(
            f"SELECT count(*)::bigint FROM stock.r03_sector_native_day_facts {full_where_sql}",
            tuple(full_params),
        )
        full_partition_count = int(cur.fetchone()[0])

        where_sql = full_where_sql
        params: list[Any] = list(full_params)
        if board_kind is not None:
            where_sql += " AND board_kind=%s"
            params.append(board_kind)

        if board_kind is not None:
            cur.execute(
                f"SELECT count(*)::bigint FROM stock.r03_sector_native_day_facts {where_sql}",
                tuple(params),
            )
            total_count = int(cur.fetchone()[0])
        else:
            total_count = full_partition_count
        prog_status, prog_expected = _read_r03_partition_progress_state(
            cur,
            trade_date=trade_date,
            business_table="stock.r03_sector_native_day_facts",
        )
        if prog_status in {"FAILED", "INCOMPLETE", "RUNNING"}:
            return {
                "status": "INCOMPLETE",
                "total_count": 0,
                "items": [],
                "reason": "该日期 R03 Native 任务状态未完成，禁止读取半成品。",
            }
        if prog_status not in {"COMPLETED", "ALREADY_COMPLETE", "NO_CHANGE"}:
            if full_partition_count == 0:
                return {
                    "status": "NOT_COMPUTED",
                    "total_count": 0,
                    "items": [],
                    "reason": "该日期尚无已保存的 R03 Native 事实。",
                }
            return {
                "status": "INCOMPLETE",
                "total_count": 0,
                "items": [],
                "reason": "该日期缺少已验证完成进度记录，禁止读取未闭合半成品。",
            }
        if prog_expected is not None and full_partition_count != prog_expected:
            return {
                "status": "INCOMPLETE",
                "total_count": 0,
                "items": [],
                "reason": "该日期实际行数与预期行数不符，禁止读取半成品。",
            }
        if total_count == 0:
            return {
                "status": "AVAILABLE_EMPTY",
                "total_count": 0,
                "items": [],
                "reason": "该日期已完成计算且合法结果为空（0行）。",
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
                   emergence_level, priority_status, priority_rank, reason_code,
                   membership_complete, membership_delta_status, rs5_peer_count, rs20_peer_count,
                   seed_k, seed_f, seed_u, quote_coverage,
                   breadth_delta1_common_count, breadth_delta3_common_count, ma20_delta3_common_count,
                   concentration_status, concentration_reason, safety_reason,
                   prewatch_raw_reason, warm_raw_reason, confirmed_raw_reason,
                   exhaustion_reason, emergence_reason, rs5_reason, rs20_reason,
                   breadth_reason, ma20_reason, amr20_reason,
                   prev_trade_date, session_gap, prev_exhaustion_level,
                   last_known_display, last_known_effective
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
    return_metadata: bool = False,
) -> dict[Any, Any]:
    """从 PG 恢复上一官方交易日（或最近已完成历史日）的板块成熟、降级目标与 PREWATCH 基准前态；区分首次冷启动与中间缺日。"""
    actual_prev_td: date | None = previous_trade_date
    res = read_r03_native_day_facts(
        config,
        trade_date=previous_trade_date,
    )
    previous_session_missing = res["status"] not in {"AVAILABLE", "AVAILABLE_EMPTY"}
    if previous_session_missing:
        with source_raw._transaction(
            config, write=False, statement_timeout_ms=10000
        ) as (
            _conn,
            cur,
            _,
        ):
            cur.execute(
                """
                SELECT DISTINCT trade_date
                FROM stock.r03_sector_native_day_facts
                WHERE trade_date < %s
                ORDER BY trade_date DESC
                """,
                (previous_trade_date,),
            )
            cand_dates = [r[0] for r in cur.fetchall()]
        for cand_td in cand_dates:
            cand_res = read_r03_native_day_facts(
                config,
                trade_date=cand_td,
            )
            if cand_res["status"] in {"AVAILABLE", "AVAILABLE_EMPTY"}:
                res = cand_res
                actual_prev_td = cand_td
                break
    items = (
        {(item["board_kind"], item["board_id"]): item for item in res["items"]}
        if res["status"] == "AVAILABLE"
        else {}
    )
    has_prior_history = res["status"] in {"AVAILABLE", "AVAILABLE_EMPTY"}
    if not has_prior_history:
        actual_prev_td = None
    if return_metadata:
        return {
            "status": res["status"],
            "previous_trade_date": actual_prev_td,
            "previous_session_missing": previous_session_missing,
            "has_prior_history": has_prior_history,
            "items": items,
            "reason": res.get("reason"),
        }
    return items


def write_r03_rotation_day(
    config: Mapping[str, Any],
    *,
    trade_date: date,
    records: Sequence[Mapping[str, Any]],
    allow_revision: bool = False,
) -> dict[str, int]:
    """在单事务中写入 Rotation Episode（含冻结篮子成员）及逐日状态推进（单一有效业务键替换）。"""
    episodes_inserted = 0
    states_inserted = 0
    states_updated = 0
    states_skipped = 0
    board_days_inserted = 0
    board_days_updated = 0
    board_days_skipped = 0
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
                    SELECT id, parent_episode_id, frozen_member_count, s0_count, u0_count, h0_count,
                           s0_known, u0_known, h0_known, left_censored
                    FROM stock.r03_sector_rotation_episodes
                    WHERE episode_key=%s
                    """,
                    (basket.episode_id,),
                )
                ep_row = cur.fetchone()
                if ep_row is None:
                    cur.execute(
                        """
                        INSERT INTO stock.r03_sector_rotation_episodes (
                            episode_key, board_kind, board_id, t0_session, parent_episode_id,
                            frozen_member_count, s0_count, u0_count, h0_count,
                            s0_known, u0_known, h0_known, left_censored
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                        """,
                        (
                            basket.episode_id,
                            basket.board_kind,
                            basket.board_id,
                            date.fromisoformat(basket.t0_session),
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
                    ep_meta_expected = (
                        rec.get("parent_episode_id"),
                        len(basket.members),
                        len(basket.s0_seed),
                        len(basket.u0_up),
                        len(basket.h0_strong),
                        bool(basket.s0_known),
                        bool(basket.u0_known),
                        bool(basket.h0_known),
                        bool(basket.left_censored),
                    )
                    cur.execute(
                        """
                        SELECT entity_id, weight, b_price, p_price, in_s0, in_u0, in_h0
                        FROM stock.r03_sector_rotation_basket_members
                        WHERE episode_id=%s
                        ORDER BY entity_id
                        """,
                        (episode_pg_id,),
                    )
                    existing_members = cur.fetchall()
                    expected_members = [
                        (
                            eid,
                            _num(basket.weights[eid]),
                            _num(basket.b_prices[eid]),
                            _num(basket.p_prices[eid]),
                            eid in basket.s0_seed,
                            eid in basket.u0_up,
                            eid in basket.h0_strong,
                        )
                        for eid in sorted(basket.members)
                    ]
                    members_same = len(existing_members) == len(
                        expected_members
                    ) and all(
                        em[0] == xm[0]
                        and _dec_eq(em[1], xm[1])
                        and _dec_eq(em[2], xm[2])
                        and _dec_eq(em[3], xm[3])
                        and bool(em[4]) == bool(xm[4])
                        and bool(em[5]) == bool(xm[5])
                        and bool(em[6]) == bool(xm[6])
                        for em, xm in zip(
                            existing_members, expected_members, strict=True
                        )
                    )
                    if tuple(ep_row[1:]) != ep_meta_expected or not members_same:
                        raise R03FormalSectorPgError(
                            "R03_ROTATION_T0_EVIDENCE_IMMUTABLE",
                            f"Rotation Episode({basket.episode_id})已存在且原已接受 T0 冻结篮子/基准不可被修订覆盖；如需重算请先清理对应版本。",
                        )

            st = rec["state_result"]
            rec_prev_td = rec.get("prev_trade_date") or st.get("prev_trade_date")
            rec_sg_raw = rec.get("session_gap")
            if rec_sg_raw is None:
                rec_sg_raw = st.get("session_gap")
            bd_session_gap = int(rec_sg_raw) if rec_sg_raw is not None else None
            st_session_gap = int(rec_sg_raw) if rec_sg_raw is not None else 1
            b_kind = str(rec.get("board_kind") or (basket.board_kind if basket else ""))
            b_id = str(rec.get("board_id") or (basket.board_id if basket else ""))
            if b_kind and b_id:
                raw_pulse = rec.get("pulse_raw", "UNKNOWN")
                if isinstance(raw_pulse, Mapping):
                    pulse_raw_str = str(raw_pulse.get("state", "UNKNOWN"))
                else:
                    pulse_raw_str = str(raw_pulse or "UNKNOWN")
                if pulse_raw_str not in {"TRUE", "FALSE", "UNKNOWN"}:
                    pulse_raw_str = "UNKNOWN"
                bd_vals = (
                    episode_pg_id,
                    st["state"],
                    pulse_raw_str,
                    st.get("last_known_state"),
                    st.get("reason"),
                    rec_prev_td,
                    bd_session_gap,
                )
                cur.execute(
                    """
                    SELECT episode_id, rotation_state, pulse_raw_state, last_known_state,
                           reason_code, prev_trade_date, session_gap
                    FROM stock.r03_sector_rotation_board_day_facts
                    WHERE trade_date=%s AND board_kind=%s AND board_id=%s
                    """,
                    (trade_date, b_kind, b_id),
                )
                bd_existing = cur.fetchone()
                if bd_existing is not None:
                    if tuple(bd_existing) == bd_vals:
                        board_days_skipped += 1
                    elif not allow_revision:
                        raise R03FormalSectorPgError(
                            "R03_ROTATION_BOARD_DAY_CONTENT_CONFLICT",
                            f"Rotation 板块日状态({trade_date}, {b_kind}, {b_id})已存在不同内容。",
                        )
                if bd_existing is None or tuple(bd_existing) != bd_vals:
                    cur.execute(
                        """
                        INSERT INTO stock.r03_sector_rotation_board_day_facts (
                            trade_date, board_kind, board_id,
                            episode_id, rotation_state, pulse_raw_state,
                            last_known_state, reason_code, prev_trade_date, session_gap
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (trade_date, board_kind, board_id) DO UPDATE SET
                            episode_id=EXCLUDED.episode_id,
                            rotation_state=EXCLUDED.rotation_state,
                            pulse_raw_state=EXCLUDED.pulse_raw_state,
                            last_known_state=EXCLUDED.last_known_state,
                            reason_code=EXCLUDED.reason_code,
                            prev_trade_date=EXCLUDED.prev_trade_date,
                            session_gap=EXCLUDED.session_gap,
                            recorded_at=now()
                        """,
                        (trade_date, b_kind, b_id) + bd_vals,
                    )
                    if bd_existing is not None:
                        board_days_updated += 1
                    else:
                        board_days_inserted += 1

            if episode_pg_id is None:
                continue
            bm = rec.get("basket_metrics") or {}
            pr_obj = bm.get("price_retention")
            if isinstance(pr_obj, Mapping):
                pr_val = _num(pr_obj.get("value"))
                pr_state = str(pr_obj.get("state") or "UNKNOWN")
                pr_reason = pr_obj.get("reason")
            else:
                pr_val = _num(pr_obj)
                pr_state = str(bm.get("price_retention_state") or "UNKNOWN")
                pr_reason = bm.get("price_retention_reason")
            sr_obj = bm.get("seed_retention") or {}
            br_obj = bm.get("breadth_retention") or {}
            hr_obj = bm.get("strong_retention") or {}
            diff_obj = bm.get("diffusion") or {}
            early_obj = bm.get("early") or {}
            mature_obj = bm.get("mature") or {}
            ent_c = bm.get("entered_count")
            ex_c = bm.get("exited_count")
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
                _num(sr_obj.get("value")),
                _num(br_obj.get("value")),
                _num(hr_obj.get("value")),
                diff_obj.get("state", "UNKNOWN"),
                early_obj.get("state", "UNKNOWN"),
                mature_obj.get("state", "UNKNOWN"),
                bm.get("valuation_quality", "UNKNOWN"),
                bm.get("max_quote_age"),
                st.get("reason"),
                rec_prev_td,
                st_session_gap,
                pr_val,
                pr_state,
                pr_reason,
                bm.get("valuation_reason"),
                sr_obj.get("state", "UNKNOWN"),
                sr_obj.get("reason"),
                br_obj.get("state", "UNKNOWN"),
                br_obj.get("reason"),
                hr_obj.get("state", "UNKNOWN"),
                hr_obj.get("reason"),
                int(ent_c) if ent_c is not None else None,
                int(ex_c) if ex_c is not None else None,
                diff_obj.get("reason"),
                early_obj.get("reason"),
                mature_obj.get("reason"),
            )
            cur.execute(
                """
                SELECT rotation_state, last_known_state, stale, market_age, eval_age,
                       accepted_once, armed, out_neg_streak, prev_mature_true, dq5,
                       formation_r, post_r, seed_retention, breadth_retention,
                       strong_retention, diffusion_state, early_state, mature_state,
                       valuation_quality, max_quote_age, reason_code,
                       prev_trade_date, session_gap, price_retention, price_retention_state,
                       price_retention_reason, valuation_reason,
                       seed_retention_state, seed_retention_reason,
                       breadth_retention_state, breadth_retention_reason,
                       strong_retention_state, strong_retention_reason,
                       entered_count, exited_count,
                       diffusion_reason, early_reason, mature_reason
                FROM stock.r03_sector_rotation_day_states
                WHERE episode_id=%s AND trade_date=%s
                """,
                (episode_pg_id, trade_date),
            )
            existing = cur.fetchone()
            if existing is not None:
                same = all(
                    _dec_eq(e, v)
                    if isinstance(v, Decimal) or isinstance(e, Decimal)
                    else e == v
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
                        diffusion_state=%s, early_state=%s, mature_state=%s,
                        valuation_quality=%s, max_quote_age=%s, reason_code=%s,
                        prev_trade_date=%s, session_gap=%s, price_retention=%s, price_retention_state=%s,
                        price_retention_reason=%s, valuation_reason=%s,
                        seed_retention_state=%s, seed_retention_reason=%s,
                        breadth_retention_state=%s, breadth_retention_reason=%s,
                        strong_retention_state=%s, strong_retention_reason=%s,
                        entered_count=%s, exited_count=%s,
                        diffusion_reason=%s, early_reason=%s, mature_reason=%s,
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
                    mature_state, valuation_quality, max_quote_age, reason_code,
                    prev_trade_date, session_gap, price_retention, price_retention_state,
                    price_retention_reason, valuation_reason,
                    seed_retention_state, seed_retention_reason,
                    breadth_retention_state, breadth_retention_reason,
                    strong_retention_state, strong_retention_reason,
                    entered_count, exited_count,
                    diffusion_reason, early_reason, mature_reason
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
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
        "board_days_inserted": board_days_inserted,
        "board_days_updated": board_days_updated,
        "board_days_skipped": board_days_skipped,
    }


def read_r03_rotation_recovery_states(
    config: Mapping[str, Any],
    *,
    before_trade_date: date | None = None,
    previous_trade_date: date | None = None,
) -> dict[tuple[str, str], dict[str, Any]]:
    """跨重启从 PG 恢复各板块上一官方会话或历史最新已完成 Rotation Episode、冻结篮子与最近日状态前态。"""
    if previous_trade_date is None and before_trade_date is None:
        return {}
    complete_statuses = {"COMPLETED", "ALREADY_COMPLETE", "NO_CHANGE"}

    with source_raw._transaction(config, write=False, statement_timeout_ms=10000) as (
        _conn,
        cur,
        _,
    ):
        prog_cache: dict[date, bool] = {}

        def _is_partition_complete(td: date) -> bool:
            if td not in prog_cache:
                pst, _ = _read_r03_partition_progress_state(
                    cur,
                    trade_date=td,
                    business_table="stock.r03_sector_rotation_board_day_facts",
                )
                prog_cache[td] = pst in complete_statuses
            return prog_cache[td]

        def _build_basket(
            ep_id: int,
            ep_key: str,
            bk: str,
            bid: str,
            t0_s: date,
            s0k: bool,
            u0k: bool,
            h0k: bool,
            lc: bool,
            exp_fc: int,
        ) -> FrozenRotationBasket:
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
            if exp_fc <= 0 or len(m_rows) != exp_fc:
                raise R03FormalSectorPgError(
                    "R03_ROTATION_BASKET_CORRUPT",
                    f"Episode({ep_id})冻结成员数声明{exp_fc}，实际子行{len(m_rows)}，拒绝静默重算权重。",
                )
            exact_eq_weight = Fraction(1, exp_fc)
            weights: dict[str, Fraction] = {}
            for mr in m_rows:
                stored_w = Fraction(mr[1])
                if abs(stored_w - exact_eq_weight) > Fraction(1, 10**6):
                    raise R03FormalSectorPgError(
                        "R03_ROTATION_WEIGHT_CORRUPT",
                        f"Episode({ep_id})成员{mr[0]}存储权重{stored_w}偏离契约等权{exact_eq_weight}。",
                    )
                weights[mr[0]] = exact_eq_weight
            return FrozenRotationBasket(
                episode_id=ep_key,
                board_id=bid,
                board_kind=bk,
                t0_session=t0_s.isoformat(),
                members=tuple(mr[0] for mr in m_rows),
                weights=weights,
                b_prices={mr[0]: Fraction(mr[2]) for mr in m_rows},
                p_prices={mr[0]: Fraction(mr[3]) for mr in m_rows},
                s0_seed=frozenset(mr[0] for mr in m_rows if mr[4]),
                u0_up=frozenset(mr[0] for mr in m_rows if mr[5]),
                h0_strong=frozenset(mr[0] for mr in m_rows if mr[6]),
                s0_known=bool(s0k),
                u0_known=bool(u0k),
                h0_known=bool(h0k),
                left_censored=bool(lc),
            )

        out: dict[tuple[str, str], dict[str, Any]] = {}
        cutoff_op = "<=" if previous_trade_date is not None else "<"
        cutoff_date = (
            previous_trade_date
            if previous_trade_date is not None
            else before_trade_date
        )
        cur.execute(
            f"""
            SELECT b.board_kind, b.board_id, b.trade_date, b.episode_id,
                   b.rotation_state, b.last_known_state, b.reason_code,
                   e.id, e.episode_key, e.t0_session, e.parent_episode_id,
                   e.s0_known, e.u0_known, e.h0_known, e.left_censored,
                   e.frozen_member_count,
                   s.stale, s.market_age, s.eval_age, s.accepted_once,
                   s.armed, s.out_neg_streak, s.prev_mature_true, s.dq5,
                   s0.formation_r
            FROM stock.r03_sector_rotation_board_day_facts b
            LEFT JOIN stock.r03_sector_rotation_episodes e ON e.id = b.episode_id
            LEFT JOIN stock.r03_sector_rotation_day_states s
              ON s.episode_id = e.id AND s.trade_date = b.trade_date
            LEFT JOIN stock.r03_sector_rotation_day_states s0
              ON s0.episode_id = e.id AND s0.trade_date = e.t0_session
            WHERE b.trade_date {cutoff_op} %s
            ORDER BY b.board_kind, b.board_id, b.trade_date DESC, b.id DESC
            """,
            (cutoff_date,),
        )
        for r in cur.fetchall():
            key = (str(r[0]), str(r[1]))
            b_td = r[2]
            if key in out or not _is_partition_complete(b_td):
                continue
            bd_ep_id = r[3]
            missing_prev = bool(
                previous_trade_date is not None and b_td < previous_trade_date
            )
            if bd_ep_id is not None and r[7] is not None and r[16] is not None:
                basket = _build_basket(
                    int(r[7]),
                    str(r[8]),
                    str(r[0]),
                    str(r[1]),
                    r[9],
                    bool(r[11]),
                    bool(r[12]),
                    bool(r[13]),
                    bool(r[14]),
                    int(r[15]),
                )
                out[key] = {
                    "episode_pg_id": int(r[7]),
                    "parent_episode_id": r[10],
                    "basket": basket,
                    "prev_trade_date": b_td,
                    "missing_previous_session": missing_prev,
                    "formation_r_t0": Fraction(r[24]) if r[24] is not None else None,
                    "state_dict": {
                        "state": str(r[4]),
                        "last_known_state": r[5],
                        "stale": bool(r[16]),
                        "market_age": int(r[17]),
                        "eval_age": int(r[18]),
                        "accepted_once": bool(r[19]),
                        "armed": bool(r[20]),
                        "out_neg_streak": int(r[21]),
                        "prev_mature_true": bool(r[22]),
                        "dq5": Fraction(r[23]) if r[23] is not None else None,
                    },
                }
            elif bd_ep_id is None:
                out[key] = {
                    "episode_pg_id": None,
                    "parent_episode_id": None,
                    "basket": None,
                    "prev_trade_date": b_td,
                    "missing_previous_session": missing_prev,
                    "formation_r_t0": Fraction(r[24]) if r[24] is not None else None,
                    "state_dict": {
                        "state": str(r[4]),
                        "last_known_state": r[5],
                        "stale": False,
                        "market_age": 0,
                        "eval_age": 0,
                        "accepted_once": False,
                        "armed": False,
                        "out_neg_streak": 0,
                        "prev_mature_true": False,
                        "dq5": None,
                        "reason": r[6],
                    },
                }
        return out


def read_r03_rotation_timeline(
    config: Mapping[str, Any],
    *,
    board_kind: str,
    board_id: str,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    """只读板块完整 Rotation Episode、冻结篮子成员与逐日状态时间线（按日校验完成态）。"""
    complete_statuses = {"COMPLETED", "ALREADY_COMPLETE", "NO_CHANGE"}
    with source_raw._transaction(config, write=False, statement_timeout_ms=10000) as (
        _conn,
        cur,
        _,
    ):
        bd_sql = """
            SELECT b.trade_date, b.episode_id, b.rotation_state, b.pulse_raw_state,
                   b.last_known_state, b.reason_code,
                   b.prev_trade_date, b.session_gap
            FROM stock.r03_sector_rotation_board_day_facts b
            WHERE b.board_kind=%s AND b.board_id=%s
        """
        bd_params: list[Any] = [board_kind, board_id]
        if as_of_date is not None:
            bd_sql += " AND b.trade_date <= %s"
            bd_params.append(as_of_date)
        bd_sql += " ORDER BY b.trade_date ASC"
        cur.execute(bd_sql, tuple(bd_params))
        bd_cols = [d[0] for d in cur.description]
        raw_bd_rows = cur.fetchall()
        if not raw_bd_rows:
            return {
                "status": "NOT_COMPUTED",
                "episodes": [],
                "board_day_facts": [],
                "reason": "该板块尚无正式 Rotation Episode 或日级状态事实。",
            }

        prog_cache: dict[date, str | None] = {}

        def _prog_status(td: date) -> str | None:
            if td not in prog_cache:
                pst, _ = _read_r03_partition_progress_state(
                    cur,
                    trade_date=td,
                    business_table="stock.r03_sector_rotation_board_day_facts",
                )
                prog_cache[td] = pst
            return prog_cache[td]

        latest_row = raw_bd_rows[-1]
        latest_pst = _prog_status(latest_row[0])
        if latest_pst not in complete_statuses:
            return {
                "status": "INCOMPLETE",
                "episodes": [],
                "board_day_facts": [],
                "reason": "该板块 Rotation 目标日尚未完成验证，禁止读取半成品。",
            }

        ordered_bd_rows = [
            br for br in raw_bd_rows if _prog_status(br[0]) in complete_statuses
        ]
        family_ep_ids: set[int] = {
            int(br[1]) for br in ordered_bd_rows if br[1] is not None
        }
        frontier = set(family_ep_ids)
        while frontier:
            cur.execute(
                """
                SELECT parent_episode_id
                FROM stock.r03_sector_rotation_episodes
                WHERE id = ANY(%s) AND parent_episode_id IS NOT NULL
                """,
                (sorted(frontier),),
            )
            parents = {
                int(r[0]) for r in cur.fetchall() if r[0] is not None
            } - family_ep_ids
            if not parents:
                break
            family_ep_ids.update(parents)
            frontier = parents

        board_day_facts = [
            {
                **dict(zip(bd_cols, br, strict=True)),
                "trade_date": br[0].isoformat(),
            }
            for br in ordered_bd_rows
        ]
        completed_days_by_ep: dict[int, set[date]] = {}
        for br in ordered_bd_rows:
            if br[1] is not None:
                completed_days_by_ep.setdefault(int(br[1]), set()).add(br[0])

        ep_rows: list[Any] = []
        if family_ep_ids:
            ep_sql = """
                SELECT id, episode_key, t0_session, parent_episode_id,
                       frozen_member_count, s0_count, u0_count, h0_count,
                       s0_known, u0_known, h0_known, left_censored
                FROM stock.r03_sector_rotation_episodes
                WHERE id = ANY(%s)
            """
            ep_params: list[Any] = [sorted(family_ep_ids)]
            if as_of_date is not None:
                ep_sql += " AND t0_session <= %s"
                ep_params.append(as_of_date)
            ep_sql += " ORDER BY t0_session ASC, id ASC"
            cur.execute(ep_sql, tuple(ep_params))
            ep_rows = cur.fetchall()
            found_ep_ids = {int(ep[0]) for ep in ep_rows}
            if found_ep_ids != family_ep_ids:
                return {
                    "status": "INCOMPLETE",
                    "episodes": [],
                    "board_day_facts": [],
                    "reason": "Rotation 父子表不闭合：部分关联 Episode 主记录缺失。",
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
            if int(ep[4]) <= 0 or len(members) != int(ep[4]):
                return {
                    "status": "INCOMPLETE",
                    "episodes": [],
                    "board_day_facts": [],
                    "reason": f"Episode({ep_id})冻结篮子成员子表不闭合，禁止读取半成品。",
                }
            st_sql = """
                SELECT trade_date, rotation_state, last_known_state, stale, market_age,
                       eval_age, accepted_once, armed, out_neg_streak, formation_r, post_r,
                       seed_retention, breadth_retention, strong_retention,
                       diffusion_state, early_state, mature_state,
                       valuation_quality, max_quote_age, reason_code,
                       prev_trade_date, session_gap, price_retention, price_retention_state,
                       price_retention_reason, valuation_reason,
                       seed_retention_state, seed_retention_reason,
                       breadth_retention_state, breadth_retention_reason,
                       strong_retention_state, strong_retention_reason,
                       entered_count, exited_count,
                       diffusion_reason, early_reason, mature_reason
                FROM stock.r03_sector_rotation_day_states
                WHERE episode_id=%s
            """
            st_params: list[Any] = [ep_id]
            if as_of_date is not None:
                st_sql += " AND trade_date <= %s"
                st_params.append(as_of_date)
            st_sql += " ORDER BY trade_date ASC"
            cur.execute(st_sql, tuple(st_params))
            s_cols = [d[0] for d in cur.description]
            allowed_days = completed_days_by_ep.get(ep_id, set())
            states = [
                dict(zip(s_cols, sr, strict=True))
                for sr in cur.fetchall()
                if not allowed_days or sr[0] in allowed_days
            ]
            state_days = {sr["trade_date"] for sr in states}
            if allowed_days and not allowed_days.issubset(state_days):
                return {
                    "status": "INCOMPLETE",
                    "episodes": [],
                    "board_day_facts": [],
                    "reason": f"Episode({ep_id})缺少已完成日的 day_states 子表记录，禁止读取半成品。",
                }
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
        return {
            "status": "AVAILABLE",
            "episodes": episodes,
            "board_day_facts": board_day_facts,
            "reason": None,
        }


def read_r03_rotation_board_day_summary(
    config: Mapping[str, Any],
    *,
    trade_date: date,
) -> dict[str, Any]:
    """按完成状态只读当日 Rotation 板块日表摘要，禁止把未完成半成品挂进普通目录。"""
    with source_raw._transaction(config, write=False, statement_timeout_ms=10000) as (
        _conn,
        cur,
        _,
    ):
        prog_status, prog_expected = _read_r03_partition_progress_state(
            cur,
            trade_date=trade_date,
            business_table="stock.r03_sector_rotation_board_day_facts",
        )
        if prog_status in {"FAILED", "INCOMPLETE", "RUNNING"}:
            return {"status": "INCOMPLETE", "items": {}}
        cur.execute(
            """
            SELECT board_kind, board_id, rotation_state, last_known_state, reason_code
            FROM stock.r03_sector_rotation_board_day_facts
            WHERE trade_date=%s
            """,
            (trade_date,),
        )
        rows = cur.fetchall()
        if not rows:
            if (
                prog_status in {"COMPLETED", "ALREADY_COMPLETE", "NO_CHANGE"}
                and prog_expected == 0
            ):
                return {"status": "AVAILABLE_EMPTY", "items": {}}
            return {"status": "NOT_COMPUTED", "items": {}}
        if prog_status not in {"COMPLETED", "ALREADY_COMPLETE", "NO_CHANGE"}:
            return {"status": "INCOMPLETE", "items": {}}
        items = {
            (str(bk), str(bid)): {
                "rotation_state": str(r_st),
                "last_known_state": lk_st,
                "reason_code": rsn,
            }
            for bk, bid, r_st, lk_st, rsn in rows
        }
        return {"status": "AVAILABLE", "items": items}


def write_r03_overlap_results(
    config: Mapping[str, Any],
    *,
    trade_date: date,
    overlap: Mapping[str, Any],
    native_rows: Sequence[Mapping[str, Any]],
    allow_revision: bool = False,
) -> dict[str, int]:
    """幂等写入单日 Overlap 主题簇、代表及每个合格板块的 unique_share 与摘要标记（单一有效业务键替换）。"""
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
                c.get("cluster_reason"),
                c.get("representative_reason"),
                int(c.get("unknown_edge_count", 0) or 0),
            )
            cur.execute(
                """
                SELECT cluster_status, member_board_ids, parent_cluster_ids,
                       representative_board_id, representative_status,
                       cluster_reason, representative_reason, unknown_edge_count
                FROM stock.r03_sector_overlap_clusters
                WHERE trade_date=%s AND board_kind=%s AND cluster_id=%s
                """,
                (trade_date, c["board_kind"], c["cluster_id"]),
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
                INSERT INTO stock.r03_sector_overlap_clusters (
                    trade_date, board_kind, cluster_id, cluster_status, member_board_ids,
                    parent_cluster_ids, representative_board_id, representative_status,
                    cluster_reason, representative_reason, unknown_edge_count
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (trade_date, board_kind, cluster_id) DO UPDATE SET
                    cluster_status=EXCLUDED.cluster_status,
                    member_board_ids=EXCLUDED.member_board_ids,
                    parent_cluster_ids=EXCLUDED.parent_cluster_ids,
                    representative_board_id=EXCLUDED.representative_board_id,
                    representative_status=EXCLUDED.representative_status,
                    cluster_reason=EXCLUDED.cluster_reason,
                    representative_reason=EXCLUDED.representative_reason,
                    unknown_edge_count=EXCLUDED.unknown_edge_count,
                    recorded_at=now()
                """,
                (trade_date, c["board_kind"], c["cluster_id"]) + vals,
            )
            if existing is not None:
                c_upd += 1
            else:
                c_ins += 1

        for nr in native_rows:
            bk = str(nr["board_kind"])
            bid = str(nr["board_id"])
            if (
                (bk, bid) not in unique_shares
                and bid not in unique_shares
                and (bk, bid) not in cluster_by_board
            ):
                continue
            us = unique_shares.get(
                (bk, bid),
                unique_shares.get(
                    bid, {"status": "NA", "value": None, "reason": "SAFETY_NOT_TRUE"}
                ),
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
                """,
                (trade_date, bk, bid),
            )
            existing_b = cur.fetchone()
            if existing_b is not None:
                same_b = all(
                    _dec_eq(e, v)
                    if isinstance(v, Decimal) or isinstance(e, Decimal)
                    else e == v
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
                INSERT INTO stock.r03_sector_overlap_board_facts (
                    trade_date, board_kind, board_id, unique_share_status, unique_share,
                    cluster_id, in_overview_summary, reason_code
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (trade_date, board_kind, board_id) DO UPDATE SET
                    unique_share_status=EXCLUDED.unique_share_status,
                    unique_share=EXCLUDED.unique_share,
                    cluster_id=EXCLUDED.cluster_id,
                    in_overview_summary=EXCLUDED.in_overview_summary,
                    reason_code=EXCLUDED.reason_code,
                    recorded_at=now()
                """,
                (trade_date, bk, bid) + vals_b,
            )
            if existing_b is not None:
                b_upd += 1
            else:
                b_ins += 1
        if allow_revision:
            current_cluster_keys = {
                (str(c["board_kind"]), str(c["cluster_id"])) for c in clusters
            }
            cur.execute(
                """
                SELECT board_kind, cluster_id
                FROM stock.r03_sector_overlap_clusters
                WHERE trade_date=%s
                """,
                (trade_date,),
            )
            for old_bk, old_cid in cur.fetchall():
                if (str(old_bk), str(old_cid)) not in current_cluster_keys:
                    cur.execute(
                        """
                        DELETE FROM stock.r03_sector_overlap_clusters
                        WHERE trade_date=%s AND board_kind=%s AND cluster_id=%s
                        """,
                        (trade_date, old_bk, old_cid),
                    )
        # 幂等写入 Overlap 板块对 Jaccard 边事实（修订时清理过期旧边）
        pair_rows = overlap.get("pairs", ())
        current_pair_keys = {
            (str(p["board_kind"]), str(p["board_id_a"]), str(p["board_id_b"]))
            for p in pair_rows
        }
        if allow_revision:
            cur.execute(
                """
                SELECT board_kind, board_id_a, board_id_b
                FROM stock.r03_sector_overlap_pair_facts
                WHERE trade_date=%s
                """,
                (trade_date,),
            )
            for old_bk, old_a, old_b in cur.fetchall():
                if (str(old_bk), str(old_a), str(old_b)) not in current_pair_keys:
                    cur.execute(
                        """
                        DELETE FROM stock.r03_sector_overlap_pair_facts
                        WHERE trade_date=%s AND board_kind=%s AND board_id_a=%s AND board_id_b=%s
                        """,
                        (trade_date, old_bk, old_a, old_b),
                    )
        for pr in pair_rows:
            p_vals = (
                str(pr["jaccard_status"]),
                _num(pr.get("jaccard_value")),
                pr.get("reason"),
            )
            cur.execute(
                """
                SELECT jaccard_status, jaccard_value, reason_code
                FROM stock.r03_sector_overlap_pair_facts
                WHERE trade_date=%s AND board_kind=%s AND board_id_a=%s AND board_id_b=%s
                """,
                (trade_date, pr["board_kind"], pr["board_id_a"], pr["board_id_b"]),
            )
            ex_p = cur.fetchone()
            if ex_p is not None:
                same_p = all(
                    _dec_eq(e, v)
                    if isinstance(v, Decimal) or isinstance(e, Decimal)
                    else e == v
                    for e, v in zip(ex_p, p_vals, strict=True)
                )
                if same_p:
                    continue
                if not allow_revision:
                    raise R03FormalSectorPgError(
                        "R03_OVERLAP_PAIR_CONFLICT",
                        f"Overlap边({trade_date}, {pr['board_kind']}, {pr['board_id_a']}, {pr['board_id_b']})已存在不同内容。",
                    )
            cur.execute(
                """
                INSERT INTO stock.r03_sector_overlap_pair_facts (
                    trade_date, board_kind, board_id_a, board_id_b,
                    jaccard_status, jaccard_value, reason_code
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (trade_date, board_kind, board_id_a, board_id_b) DO UPDATE SET
                    jaccard_status=EXCLUDED.jaccard_status,
                    jaccard_value=EXCLUDED.jaccard_value,
                    reason_code=EXCLUDED.reason_code,
                    recorded_at=now()
                """,
                (trade_date, pr["board_kind"], pr["board_id_a"], pr["board_id_b"])
                + p_vals,
            )
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
    board_kind: str | None = None,
) -> dict[str, Any]:
    """只读目标日单一有效 Overlap 簇、代表与板块 unique_share 事实。"""
    with source_raw._transaction(config, write=False, statement_timeout_ms=10000) as (
        _conn,
        cur,
        _,
    ):
        where_sql = "WHERE trade_date=%s"
        params: list[Any] = [trade_date]
        if board_kind is not None:
            where_sql += " AND board_kind=%s"
            params.append(board_kind)
        cur.execute(
            f"""
            SELECT board_kind, cluster_id, cluster_status, member_board_ids,
                   parent_cluster_ids, representative_board_id, representative_status,
                   cluster_reason, representative_reason, unknown_edge_count
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
        cur.execute(
            f"""
            SELECT board_kind, board_id_a, board_id_b, jaccard_status, jaccard_value, reason_code
            FROM stock.r03_sector_overlap_pair_facts
            {where_sql}
            ORDER BY board_kind ASC, board_id_a ASC, board_id_b ASC
            """,
            tuple(params),
        )
        p_cols = [d[0] for d in cur.description]
        pairs = [dict(zip(p_cols, r, strict=True)) for r in cur.fetchall()]
        ov_status, ov_expected = _read_r03_partition_progress_state(
            cur,
            trade_date=trade_date,
            business_table="stock.r03_sector_overlap_clusters",
        )
        if ov_status in {"FAILED", "INCOMPLETE", "RUNNING"}:
            return {
                "status": "INCOMPLETE",
                "clusters": [],
                "board_facts": [],
                "pairs": [],
                "reason": "该精确分区 Overlap 任务状态未完成，禁止读取半成品。",
            }
        if not clusters and not board_facts:
            if ov_status in {"COMPLETED", "ALREADY_COMPLETE", "NO_CHANGE"} and (
                ov_expected == 0 or board_kind is not None
            ):
                return {
                    "status": "AVAILABLE_EMPTY",
                    "clusters": [],
                    "board_facts": [],
                    "pairs": [],
                    "reason": "该日期 Overlap 已完成计算且合法结果为空（0簇）。",
                }
            if ov_status in {"COMPLETED", "ALREADY_COMPLETE", "NO_CHANGE"}:
                return {
                    "status": "INCOMPLETE",
                    "clusters": [],
                    "board_facts": [],
                    "pairs": [],
                    "reason": "该精确分区 Overlap 进度为已完成但实际行数为 0（与预期不符），禁止读取半成品。",
                }
            return {
                "status": "NOT_COMPUTED",
                "clusters": [],
                "board_facts": [],
                "pairs": [],
                "reason": "该日期与口径尚无已保存的 Overlap 事实。",
            }
        if ov_status not in {"COMPLETED", "ALREADY_COMPLETE", "NO_CHANGE"}:
            return {
                "status": "INCOMPLETE",
                "clusters": [],
                "board_facts": [],
                "pairs": [],
                "reason": "该精确分区 Overlap 缺少已验证完成进度记录，禁止读取半成品。",
            }
        # 校验 clusters、board_facts 与 pair_facts 多表闭合
        cluster_keys = {(str(c["board_kind"]), str(c["cluster_id"])) for c in clusters}
        board_keys = {
            (str(bf["board_kind"]), str(bf["board_id"])) for bf in board_facts
        }
        for bf in board_facts:
            cid = bf.get("cluster_id")
            if (
                cid is not None
                and (str(bf["board_kind"]), str(cid)) not in cluster_keys
            ):
                return {
                    "status": "INCOMPLETE",
                    "clusters": [],
                    "board_facts": [],
                    "pairs": [],
                    "reason": "Overlap 板块事实引用了缺失的簇记录，禁止读取半成品。",
                }
        boards_in_clusters_by_kind: dict[str, set[str]] = {}
        for c in clusters:
            bk = str(c["board_kind"])
            for bid in c.get("member_board_ids") or ():
                if (bk, str(bid)) not in board_keys:
                    return {
                        "status": "INCOMPLETE",
                        "clusters": [],
                        "board_facts": [],
                        "pairs": [],
                        "reason": "Overlap 簇成员在 board_facts 子表中缺失，禁止读取半成品。",
                    }
                boards_in_clusters_by_kind.setdefault(bk, set()).add(str(bid))
        pair_kinds = {str(p["board_kind"]) for p in pairs}
        for bk, bids in boards_in_clusters_by_kind.items():
            if len(bids) >= 2 and bk not in pair_kinds:
                return {
                    "status": "INCOMPLETE",
                    "clusters": [],
                    "board_facts": [],
                    "pairs": [],
                    "reason": f"Overlap {bk} 存在多板块簇但缺少 pair_facts 边事实，禁止读取半成品。",
                }
        return {
            "status": "AVAILABLE",
            "clusters": clusters,
            "board_facts": board_facts,
            "pairs": pairs,
            "reason": None,
        }


def write_r03_loo_day_batch(
    config: Mapping[str, Any],
    *,
    trade_date: date,
    targets_and_results: Sequence[tuple[str, Mapping[str, Any]]],
    allow_revision: bool = False,
) -> dict[str, int]:
    """在单事务中原子写入当日全部 LOO 目标支持事实及候选明细，任一目标失败整日回滚不暴露部分分区。"""
    inserted = updated = skipped = 0
    with source_raw._transaction(config, write=True, statement_timeout_ms=30000) as (
        _conn,
        cur,
        _,
    ):
        for bk, loo_result in targets_and_results:
            res = _write_r03_loo_support_in_cursor(
                cur,
                trade_date=trade_date,
                board_kind=bk,
                loo_result=loo_result,
                allow_revision=allow_revision,
            )
            inserted += res["inserted"]
            updated += res["updated"]
            skipped += res["skipped"]
    return {"inserted": inserted, "updated": updated, "skipped": skipped}


def _write_r03_loo_support_in_cursor(
    cur: Any,
    *,
    trade_date: date,
    board_kind: str,
    loo_result: Mapping[str, Any],
    allow_revision: bool = False,
) -> dict[str, int]:
    inserted = updated = skipped = 0
    loo_boards = loo_result.get("loo_boards", ())
    cand_count = int(loo_result.get("candidate_count", len(loo_boards)) or 0)
    if loo_result.get("qualified_candidate_count") is not None:
        qual_cand_count = int(loo_result["qualified_candidate_count"])
    else:
        qual_cand_count = sum(
            1
            for c in loo_boards
            if c.get("raw_qualification", {}).get("safety", {}).get("state") == "TRUE"
            and any(
                c.get("raw_qualification", {}).get(k, {}).get("state") == "TRUE"
                for k in ("confirmed_raw", "warm_raw", "prewatch_raw")
            )
        )
    if "matches_primary_industry" in loo_result:
        matches_primary = loo_result.get("matches_primary_industry")
    elif (
        board_kind == "INDUSTRY"
        and loo_result.get("support_status") == "SELECTED"
        and loo_result.get("primary_industry_id") is not None
        and loo_result.get("algorithm_support_board_id") is not None
    ):
        matches_primary = str(loo_result["algorithm_support_board_id"]) == str(
            loo_result["primary_industry_id"]
        )
    else:
        matches_primary = None
    selected_bid = (
        str(loo_result["algorithm_support_board_id"])
        if loo_result.get("support_status") == "SELECTED"
        and loo_result.get("algorithm_support_board_id") is not None
        else None
    )
    vals = (
        loo_result.get("primary_industry_id"),
        loo_result.get("algorithm_support_board_id"),
        loo_result["support_status"],
        loo_result.get("reason"),
        cand_count,
        qual_cand_count,
        matches_primary,
    )
    cur.execute(
        """
        SELECT id, primary_industry_id, algorithm_support_board_id, support_status, reason_code,
               candidate_count, qualified_candidate_count, matches_primary_industry
        FROM stock.r03_sector_loo_support_facts
        WHERE trade_date=%s AND target_entity_id=%s AND board_kind=%s
        """,
        (trade_date, loo_result["target_entity_id"], board_kind),
    )
    existing = cur.fetchone()
    if existing is not None:
        loo_id = int(existing[0])
        cur.execute(
            """
            SELECT board_id, safety_state, prewatch_raw_state, warm_raw_state,
                   confirmed_raw_state, emergence_level, seed_wilson_point,
                   rs5_pct, dq5, reason_code,
                   is_selected_support, breadth_delta3, ma20_delta3, top1_concentration
            FROM stock.r03_sector_loo_candidate_facts
            WHERE loo_fact_id=%s
            ORDER BY board_id
            """,
            (loo_id,),
        )
        existing_cands = [
            (
                str(r[0]),
                str(r[1]),
                str(r[2]),
                str(r[3]),
                str(r[4]),
                str(r[5]) if r[5] is not None else None,
                _num(r[6]),
                _num(r[7]),
                _num(r[8]),
                str(r[9]) if r[9] is not None else None,
                bool(r[10]),
                _num(r[11]),
                _num(r[12]),
                _num(r[13]),
            )
            for r in cur.fetchall()
        ]
        new_cands = sorted(
            (
                str(c["board_id"]),
                str(c["raw_qualification"]["safety"]["state"]),
                str(c["raw_qualification"]["prewatch_raw"]["state"]),
                str(c["raw_qualification"]["warm_raw"]["state"]),
                str(c["raw_qualification"]["confirmed_raw"]["state"]),
                str(c["emergence"]["level"])
                if c.get("emergence", {}).get("level") is not None
                else None,
                _num(c.get("seed_wilson", {}).get("point_value")),
                _num(c.get("rs5_pct")),
                _num(c.get("dq5")),
                str(c["priority"]["reason"])
                if c.get("priority", {}).get("reason") is not None
                else None,
                bool(c.get("is_selected_support"))
                if "is_selected_support" in c
                else (selected_bid is not None and str(c["board_id"]) == selected_bid),
                _num(c.get("breadth_delta3")),
                _num(c.get("ma20_delta3")),
                _num(
                    c.get(
                        "top1_concentration",
                        (c.get("concentration") or {}).get("top1_concentration"),
                    )
                ),
            )
            for c in loo_boards
        )
        cands_same = len(existing_cands) == len(new_cands) and all(
            all(
                _dec_eq(ec, nc)
                if isinstance(nc, Decimal) or isinstance(ec, Decimal)
                else ec == nc
                for ec, nc in zip(erow, nrow, strict=True)
            )
            for erow, nrow in zip(existing_cands, new_cands, strict=True)
        )
        if existing[1:] == vals and cands_same:
            skipped += 1
        elif not allow_revision:
            raise R03FormalSectorPgError(
                "R03_LOO_KEY_CONTENT_CONFLICT",
                f"LOO支持事实({trade_date}, {loo_result['target_entity_id']}, {board_kind})已存在不同内容。",
            )
        else:
            cur.execute(
                """
                INSERT INTO stock.r03_sector_loo_support_facts (
                    trade_date, target_entity_id, board_kind,
                    primary_industry_id, algorithm_support_board_id,
                    support_status, reason_code, candidate_count, qualified_candidate_count,
                    matches_primary_industry
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (trade_date, target_entity_id, board_kind) DO UPDATE SET
                    primary_industry_id=EXCLUDED.primary_industry_id,
                    algorithm_support_board_id=EXCLUDED.algorithm_support_board_id,
                    support_status=EXCLUDED.support_status,
                    reason_code=EXCLUDED.reason_code,
                    candidate_count=EXCLUDED.candidate_count,
                    qualified_candidate_count=EXCLUDED.qualified_candidate_count,
                    matches_primary_industry=EXCLUDED.matches_primary_industry,
                    recorded_at=now()
                RETURNING id
                """,
                (trade_date, loo_result["target_entity_id"], board_kind) + vals,
            )
            loo_id = int(cur.fetchone()[0])
            cur.execute(
                "DELETE FROM stock.r03_sector_loo_candidate_facts WHERE loo_fact_id=%s",
                (loo_id,),
            )
            updated += 1
    else:
        cur.execute(
            """
            INSERT INTO stock.r03_sector_loo_support_facts (
                trade_date, target_entity_id, board_kind,
                primary_industry_id, algorithm_support_board_id,
                support_status, reason_code, candidate_count, qualified_candidate_count,
                matches_primary_industry
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (trade_date, target_entity_id, board_kind) DO UPDATE SET
                primary_industry_id=EXCLUDED.primary_industry_id,
                algorithm_support_board_id=EXCLUDED.algorithm_support_board_id,
                support_status=EXCLUDED.support_status,
                reason_code=EXCLUDED.reason_code,
                candidate_count=EXCLUDED.candidate_count,
                qualified_candidate_count=EXCLUDED.qualified_candidate_count,
                matches_primary_industry=EXCLUDED.matches_primary_industry,
                recorded_at=now()
            RETURNING id
            """,
            (trade_date, loo_result["target_entity_id"], board_kind) + vals,
        )
        loo_id = int(cur.fetchone()[0])
        inserted += 1

    if inserted or updated:
        for cand in loo_boards:
            rq = cand["raw_qualification"]
            is_sel = (
                bool(cand.get("is_selected_support"))
                if "is_selected_support" in cand
                else (
                    selected_bid is not None and str(cand["board_id"]) == selected_bid
                )
            )
            cur.execute(
                """
                INSERT INTO stock.r03_sector_loo_candidate_facts (
                    loo_fact_id, board_id, safety_state, prewatch_raw_state,
                    warm_raw_state, confirmed_raw_state, emergence_level,
                    seed_wilson_point, rs5_pct, dq5, reason_code,
                    is_selected_support, breadth_delta3, ma20_delta3, top1_concentration
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    is_sel,
                    _num(cand.get("breadth_delta3")),
                    _num(cand.get("ma20_delta3")),
                    _num(
                        cand.get(
                            "top1_concentration",
                            (cand.get("concentration") or {}).get("top1_concentration"),
                        )
                    ),
                ),
            )
    return {"inserted": inserted, "updated": updated, "skipped": skipped}


def write_r03_loo_support_fact(
    config: Mapping[str, Any],
    *,
    trade_date: date,
    board_kind: str,
    loo_result: Mapping[str, Any],
    allow_revision: bool = False,
) -> dict[str, int]:
    """幂等写入单股 LOO 算法支持板块结果及全部候选板块重算证据。"""
    with source_raw._transaction(config, write=True, statement_timeout_ms=15000) as (
        _conn,
        cur,
        _,
    ):
        return _write_r03_loo_support_in_cursor(
            cur,
            trade_date=trade_date,
            board_kind=board_kind,
            loo_result=loo_result,
            allow_revision=allow_revision,
        )


def verify_r03_partition_keys(
    config: Mapping[str, Any],
    *,
    module_code: str,
    trade_date: date,
    expected_keys: set[tuple[str, str]],
    expected_sub_keys: Mapping[str, set[tuple[str, ...]]] | None = None,
) -> dict[str, Any]:
    """独立只读连接核验 R03 目标日父子实际业务键集合与预期严格相等（防少行、同数错键、额外或预期0陈旧）。"""
    sub_missing: dict[str, list[Any]] = {}
    sub_extra: dict[str, list[Any]] = {}
    with source_raw._transaction(config, write=False, statement_timeout_ms=10000) as (
        _conn,
        cur,
        _,
    ):
        if module_code == "R03_SECTOR_NATIVE":
            cur.execute(
                """
                SELECT board_kind, board_id
                FROM stock.r03_sector_native_day_facts
                WHERE trade_date=%s
                """,
                (trade_date,),
            )
            actual_keys = {(str(r[0]), str(r[1])) for r in cur.fetchall()}
        elif module_code == "R03_SECTOR_ROTATION":
            cur.execute(
                """
                SELECT board_kind, board_id, episode_id
                FROM stock.r03_sector_rotation_board_day_facts
                WHERE trade_date=%s
                """,
                (trade_date,),
            )
            rot_rows = cur.fetchall()
            actual_keys = {(str(r[0]), str(r[1])) for r in rot_rows}
            ep_ids = sorted({int(r[2]) for r in rot_rows if r[2] is not None})
            if ep_ids:
                cur.execute(
                    """
                    SELECT episode_id FROM stock.r03_sector_rotation_day_states
                    WHERE trade_date=%s AND episode_id = ANY(%s)
                    """,
                    (trade_date, ep_ids),
                )
                present_ep_states = {int(r[0]) for r in cur.fetchall()}
                miss_ep_states = sorted(set(ep_ids) - present_ep_states)
                if miss_ep_states:
                    sub_missing["rotation_day_states"] = miss_ep_states[:10]
                cur.execute(
                    """
                    SELECT e.id, e.frozen_member_count, count(bm.id)::bigint
                    FROM stock.r03_sector_rotation_episodes e
                    LEFT JOIN stock.r03_sector_rotation_basket_members bm ON bm.episode_id = e.id
                    WHERE e.id = ANY(%s)
                    GROUP BY e.id, e.frozen_member_count
                    HAVING count(bm.id) <> e.frozen_member_count
                    """,
                    (ep_ids,),
                )
                corrupt_eps = [int(r[0]) for r in cur.fetchall()]
                if corrupt_eps:
                    sub_missing["rotation_basket_members"] = corrupt_eps[:10]
        elif module_code == "R03_SECTOR_OVERLAP":
            cur.execute(
                """
                SELECT board_kind, cluster_id
                FROM stock.r03_sector_overlap_clusters
                WHERE trade_date=%s
                """,
                (trade_date,),
            )
            actual_keys = {(str(r[0]), str(r[1])) for r in cur.fetchall()}
            if expected_sub_keys is not None:
                if "board_facts" in expected_sub_keys:
                    cur.execute(
                        """
                        SELECT board_kind, board_id
                        FROM stock.r03_sector_overlap_board_facts
                        WHERE trade_date=%s
                        """,
                        (trade_date,),
                    )
                    act_bf = {(str(r[0]), str(r[1])) for r in cur.fetchall()}
                    exp_bf = set(expected_sub_keys["board_facts"])
                    if exp_bf - act_bf:
                        sub_missing["overlap_board_facts"] = sorted(exp_bf - act_bf)[
                            :10
                        ]
                    if act_bf - exp_bf:
                        sub_extra["overlap_board_facts"] = sorted(act_bf - exp_bf)[:10]
                if "pair_facts" in expected_sub_keys:
                    cur.execute(
                        """
                        SELECT board_kind, board_id_a, board_id_b
                        FROM stock.r03_sector_overlap_pair_facts
                        WHERE trade_date=%s
                        """,
                        (trade_date,),
                    )
                    act_pf = {(str(r[0]), str(r[1]), str(r[2])) for r in cur.fetchall()}
                    exp_pf = set(expected_sub_keys["pair_facts"])
                    if exp_pf - act_pf:
                        sub_missing["overlap_pair_facts"] = sorted(exp_pf - act_pf)[:10]
                    if act_pf - exp_pf:
                        sub_extra["overlap_pair_facts"] = sorted(act_pf - exp_pf)[:10]
        elif module_code == "R03_SECTOR_LOO":
            cur.execute(
                """
                SELECT target_entity_id, board_kind
                FROM stock.r03_sector_loo_support_facts
                WHERE trade_date=%s
                """,
                (trade_date,),
            )
            actual_keys = {(str(r[0]), str(r[1])) for r in cur.fetchall()}
            if expected_sub_keys is not None and "candidates" in expected_sub_keys:
                cur.execute(
                    """
                    SELECT f.target_entity_id, f.board_kind, c.board_id
                    FROM stock.r03_sector_loo_candidate_facts c
                    JOIN stock.r03_sector_loo_support_facts f ON f.id = c.loo_fact_id
                    WHERE f.trade_date=%s
                    """,
                    (trade_date,),
                )
                act_cand = {(str(r[0]), str(r[1]), str(r[2])) for r in cur.fetchall()}
                exp_cand = set(expected_sub_keys["candidates"])
                if exp_cand - act_cand:
                    sub_missing["loo_candidate_facts"] = sorted(exp_cand - act_cand)[
                        :10
                    ]
                if act_cand - exp_cand:
                    sub_extra["loo_candidate_facts"] = sorted(act_cand - exp_cand)[:10]
        else:
            return {"matched": True, "missing": [], "extra": []}
    missing = sorted(expected_keys - actual_keys)
    extra = sorted(actual_keys - expected_keys)
    return {
        "matched": (not missing and not extra and not sub_missing and not sub_extra),
        "actual_count": len(actual_keys),
        "expected_count": len(expected_keys),
        "missing": missing[:10],
        "extra": extra[:10],
        "sub_missing": sub_missing,
        "sub_extra": sub_extra,
    }


def read_r03_loo_support_fact(
    config: Mapping[str, Any],
    *,
    trade_date: date,
    target_entity_id: str,
    board_kind: str,
) -> dict[str, Any]:
    """只读单股目标日 LOO 算法支持板块与候选证据（供 R04 TREND_CONTINUE 等消费者使用）。"""
    with source_raw._transaction(config, write=False, statement_timeout_ms=10000) as (
        _conn,
        cur,
        _,
    ):
        loo_status, loo_expected = _read_r03_partition_progress_state(
            cur,
            trade_date=trade_date,
            business_table="stock.r03_sector_loo_support_facts",
        )
        if loo_status in {"FAILED", "INCOMPLETE", "RUNNING"}:
            return {
                "status": "INCOMPLETE",
                "target_entity_id": target_entity_id,
                "board_kind": board_kind,
                "candidates": [],
                "reason": "该日期 LOO 任务状态未完成，禁止读取半成品。",
            }
        cur.execute(
            """
            SELECT id, primary_industry_id, algorithm_support_board_id,
                   support_status, reason_code,
                   candidate_count, qualified_candidate_count, matches_primary_industry
            FROM stock.r03_sector_loo_support_facts
            WHERE trade_date=%s AND target_entity_id=%s AND board_kind=%s
            """,
            (trade_date, target_entity_id, board_kind),
        )
        row = cur.fetchone()
        if row is None:
            if (
                loo_status in {"COMPLETED", "ALREADY_COMPLETE", "NO_CHANGE"}
                and loo_expected == 0
            ):
                return {
                    "status": "AVAILABLE_EMPTY",
                    "target_entity_id": target_entity_id,
                    "board_kind": board_kind,
                    "candidates": [],
                    "reason": "该日期 LOO 已完成计算且合法结果为空（0行）。",
                }
            return {
                "status": "NOT_COMPUTED",
                "target_entity_id": target_entity_id,
                "board_kind": board_kind,
                "candidates": [],
                "reason": "该目标证券与口径尚无已保存的 LOO 支持板块事实。",
            }
        if loo_status not in {"COMPLETED", "ALREADY_COMPLETE", "NO_CHANGE"}:
            return {
                "status": "INCOMPLETE",
                "target_entity_id": target_entity_id,
                "board_kind": board_kind,
                "candidates": [],
                "reason": "该精确分区 LOO 缺少已验证完成进度记录，禁止读取半成品。",
            }
        loo_id = int(row[0])
        cand_count = int(row[5]) if row[5] is not None else 0
        cur.execute(
            """
            SELECT board_id, safety_state, prewatch_raw_state, warm_raw_state,
                   confirmed_raw_state, emergence_level, seed_wilson_point,
                   rs5_pct, dq5, reason_code,
                   is_selected_support, breadth_delta3, ma20_delta3, top1_concentration
            FROM stock.r03_sector_loo_candidate_facts
            WHERE loo_fact_id=%s ORDER BY board_id ASC
            """,
            (loo_id,),
        )
        c_cols = [d[0] for d in cur.description]
        candidates = [dict(zip(c_cols, cr, strict=True)) for cr in cur.fetchall()]
        if len(candidates) != cand_count:
            return {
                "status": "INCOMPLETE",
                "target_entity_id": target_entity_id,
                "board_kind": board_kind,
                "candidates": [],
                "reason": "LOO 候选子表与支持父表数量不闭合，禁止读取半成品。",
            }
        selected_ids = [
            str(c["board_id"]) for c in candidates if bool(c.get("is_selected_support"))
        ]
        if row[3] == "SELECTED":
            if (
                row[2] is None
                or str(row[2]) not in {str(c["board_id"]) for c in candidates}
                or selected_ids != [str(row[2])]
            ):
                return {
                    "status": "INCOMPLETE",
                    "target_entity_id": target_entity_id,
                    "board_kind": board_kind,
                    "candidates": [],
                    "reason": "LOO 选中支持板块与候选子表选中标记不一致，父子表不闭合。",
                }
        elif selected_ids:
            return {
                "status": "INCOMPLETE",
                "target_entity_id": target_entity_id,
                "board_kind": board_kind,
                "candidates": [],
                "reason": "非 SELECTED 状态存在 is_selected_support=True 脏子行，父子表不闭合。",
            }
        return {
            "status": "AVAILABLE",
            "target_entity_id": target_entity_id,
            "board_kind": board_kind,
            "primary_industry_id": row[1],
            "algorithm_support_board_id": row[2],
            "support_status": row[3],
            "reason": row[4],
            "candidate_count": cand_count,
            "qualified_candidate_count": int(row[6]) if row[6] is not None else 0,
            "matches_primary_industry": row[7],
            "candidates": candidates,
        }


def write_r03_formal_market_breadth(
    config: Mapping[str, Any],
    *,
    trade_date: date,
    result: Mapping[str, Any],
    allow_revision: bool = False,
) -> dict[str, int]:
    """幂等写入 R03 正式市场宽度日级事实（独立于固定来源代码池，单一有效业务键替换）。"""
    exact = result.get("exact")
    num = (
        int(exact["numerator"])
        if isinstance(exact, Mapping) and exact.get("numerator") is not None
        else None
    )
    den = (
        int(exact["denominator"])
        if isinstance(exact, Mapping) and exact.get("denominator") is not None
        else None
    )
    val = (
        (Decimal(num) / Decimal(den)).quantize(Decimal("0.0000000001"))
        if num is not None and den is not None and den > 0
        else None
    )
    vals = (
        str(result["status"]),
        result.get("reason"),
        int(result["n_t"]),
        int(result["n_t3"]),
        int(result["n_common"]),
        int(result["n_eligible"]),
        int(result["unknown_count"]),
        num,
        den,
        val,
    )
    with source_raw._transaction(config, write=True, statement_timeout_ms=10000) as (
        _conn,
        cur,
        _,
    ):
        cur.execute(
            """
            SELECT status, reason_code, n_t, n_t3, n_common, n_eligible, unknown_count,
                   exact_numerator, exact_denominator, breadth_value
            FROM stock.r03_formal_market_breadth_facts
            WHERE trade_date=%s
            """,
            (trade_date,),
        )
        existing = cur.fetchone()
        if existing is not None:
            same = all(
                _dec_eq(e, v)
                if isinstance(v, Decimal) or isinstance(e, Decimal)
                else e == v
                for e, v in zip(existing, vals, strict=True)
            )
            if same:
                return {"inserted": 0, "updated": 0, "skipped": 1}
            if not allow_revision:
                raise R03FormalSectorPgError(
                    "R03_FORMAL_MARKET_BREADTH_CONFLICT",
                    f"正式市场宽度({trade_date})内容冲突：new={vals}, existing={existing}。",
                )
        cur.execute(
            """
            INSERT INTO stock.r03_formal_market_breadth_facts (
                trade_date, status, reason_code, n_t, n_t3, n_common,
                n_eligible, unknown_count, exact_numerator, exact_denominator,
                breadth_value
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (trade_date) DO UPDATE SET
                status=EXCLUDED.status,
                reason_code=EXCLUDED.reason_code,
                n_t=EXCLUDED.n_t,
                n_t3=EXCLUDED.n_t3,
                n_common=EXCLUDED.n_common,
                n_eligible=EXCLUDED.n_eligible,
                unknown_count=EXCLUDED.unknown_count,
                exact_numerator=EXCLUDED.exact_numerator,
                exact_denominator=EXCLUDED.exact_denominator,
                breadth_value=EXCLUDED.breadth_value,
                recorded_at=now()
            """,
            (trade_date,) + vals,
        )
        if existing is not None:
            return {"inserted": 0, "updated": 1, "skipped": 0}
        return {"inserted": 1, "updated": 0, "skipped": 0}


def read_r03_formal_market_breadth(
    config: Mapping[str, Any],
    *,
    trade_date: date,
    previous_trade_date: date | None = None,
) -> dict[str, Any]:
    """只读已保存的 R03 正式市场宽度日级事实；严格校验紧邻上一官方交易日完成态以计算相邻变化。"""
    resolved_prev_td = previous_trade_date
    if resolved_prev_td is None:
        from stock_a.application.data_jobs import _load_official_sessions

        _cal_id, _cal_rev, off_sessions = _load_official_sessions(trade_date)
        prev_candidates = [d for d in off_sessions if d < trade_date]
        resolved_prev_td = prev_candidates[-1] if prev_candidates else None

    with source_raw._transaction(config, write=False, statement_timeout_ms=10000) as (
        _conn,
        cur,
        _,
    ):
        cur.execute(
            """
            SELECT status, reason_code,
                   n_t, n_t3, n_common, n_eligible, unknown_count,
                   exact_numerator, exact_denominator, breadth_value
            FROM stock.r03_formal_market_breadth_facts
            WHERE trade_date=%s
            """,
            (trade_date,),
        )
        r = cur.fetchone()
        if r is None:
            return {
                "status": "NOT_READY_HISTORICAL_UNIVERSE",
                "reason": "该日期尚无已保存的正式市场宽度日级事实。",
            }
        mkt_status, _mkt_expected = _read_r03_partition_progress_state(
            cur,
            trade_date=trade_date,
            business_table="stock.r03_formal_market_breadth_facts",
        )
        if mkt_status not in {"COMPLETED", "ALREADY_COMPLETE", "NO_CHANGE"}:
            return {
                "status": "INCOMPLETE"
                if mkt_status in {"FAILED", "INCOMPLETE", "RUNNING"}
                else "NOT_READY_HISTORICAL_UNIVERSE",
                "reason": "该日期正式市场宽度尚未完成验证，禁止读取未闭合数据。",
            }
        from stock_a.domain.market_presentation import classify_market_breadth

        exact_obj = (
            {"numerator": int(r[7]), "denominator": int(r[8])}
            if r[7] is not None and r[8] is not None
            else None
        )
        classification = "UNKNOWN"
        if (
            str(r[0]) == "OBSERVED"
            and exact_obj is not None
            and exact_obj["denominator"] > 0
        ):
            classification = classify_market_breadth(
                Fraction(exact_obj["numerator"], exact_obj["denominator"])
            )
        n_common_val = int(r[4])
        n_eligible_val = int(r[5])
        coverage = (
            float(Fraction(n_eligible_val, n_common_val)) if n_common_val > 0 else None
        )
        # 严格校验紧邻上一官方交易日及该日 data_job_progress 完成态；不拿更早旧日冒充相邻变化
        if resolved_prev_td is None:
            breadth_change: dict[str, Any] = {
                "change_status": "FIRST_OBSERVED",
                "delta": None,
                "previous_trade_date": None,
            }
        else:
            prev_mkt_status, _ = _read_r03_partition_progress_state(
                cur,
                trade_date=resolved_prev_td,
                business_table="stock.r03_formal_market_breadth_facts",
            )
            cur.execute(
                """
                SELECT status, exact_numerator, exact_denominator
                FROM stock.r03_formal_market_breadth_facts
                WHERE trade_date = %s
                """,
                (resolved_prev_td,),
            )
            prev_b_row = cur.fetchone()
            if (
                prev_mkt_status in {"COMPLETED", "ALREADY_COMPLETE", "NO_CHANGE"}
                and prev_b_row is not None
            ):
                if (
                    str(r[0]) == "OBSERVED"
                    and exact_obj is not None
                    and exact_obj["denominator"] > 0
                    and str(prev_b_row[0]) == "OBSERVED"
                    and prev_b_row[1] is not None
                    and prev_b_row[2] is not None
                    and int(prev_b_row[2]) > 0
                ):
                    delta_frac = Fraction(
                        exact_obj["numerator"], exact_obj["denominator"]
                    ) - Fraction(int(prev_b_row[1]), int(prev_b_row[2]))
                    breadth_change = {
                        "change_status": "OBSERVED",
                        "delta": float(delta_frac),
                        "previous_trade_date": resolved_prev_td.isoformat(),
                    }
                else:
                    breadth_change = {
                        "change_status": "UNKNOWN",
                        "delta": None,
                        "previous_trade_date": resolved_prev_td.isoformat(),
                    }
            else:
                cur.execute(
                    """
                    SELECT 1 FROM stock.r03_formal_market_breadth_facts
                    WHERE trade_date < %s
                    LIMIT 1
                    """,
                    (trade_date,),
                )
                has_earlier = cur.fetchone() is not None
                if not has_earlier:
                    breadth_change = {
                        "change_status": "FIRST_OBSERVED",
                        "delta": None,
                        "previous_trade_date": None,
                    }
                else:
                    breadth_change = {
                        "change_status": "UNKNOWN",
                        "delta": None,
                        "previous_trade_date": resolved_prev_td.isoformat(),
                    }
        return {
            "status": str(r[0]),
            "reason": r[1],
            "n_t": int(r[2]),
            "n_t3": int(r[3]),
            "n_common": n_common_val,
            "n_eligible": n_eligible_val,
            "unknown_count": int(r[6]),
            "exact": exact_obj,
            "breadth_value": str(r[9]) if r[9] is not None else None,
            "classification": classification,
            "coverage": coverage,
            "breadth_change": breadth_change,
        }


def cleanup_r03_scope_day(
    config: Mapping[str, Any],
    *,
    trade_date: date,
    include_formal_market: bool = False,
) -> dict[str, int]:
    """按精确日期有界清理 R03 正式事实，不触碰其他日期或来源层，默认不误删独立市场宽度。"""
    deleted: dict[str, int] = {}
    with source_raw._transaction(config, write=True, statement_timeout_ms=15000) as (
        _conn,
        cur,
        _,
    ):
        cur.execute(
            """
            DELETE FROM stock.r03_sector_loo_candidate_facts c
            USING stock.r03_sector_loo_support_facts f
            WHERE c.loo_fact_id = f.id AND f.trade_date=%s
            """,
            (trade_date,),
        )
        deleted["stock.r03_sector_loo_candidate_facts"] = cur.rowcount
        tables: list[str] = [
            "stock.r03_sector_loo_support_facts",
            "stock.r03_sector_overlap_pair_facts",
            "stock.r03_sector_overlap_board_facts",
            "stock.r03_sector_overlap_clusters",
            "stock.r03_sector_rotation_board_day_facts",
            "stock.r03_sector_native_day_facts",
        ]
        for table in tables:
            cur.execute(
                f"DELETE FROM {table} WHERE trade_date=%s",
                (trade_date,),
            )
            deleted[table] = cur.rowcount
        if include_formal_market:
            cur.execute(
                "DELETE FROM stock.r03_formal_market_breadth_facts WHERE trade_date=%s",
                (trade_date,),
            )
            deleted["stock.r03_formal_market_breadth_facts"] = cur.rowcount
        cur.execute(
            """
            DELETE FROM stock.r03_sector_rotation_day_states
            WHERE trade_date=%s
            """,
            (trade_date,),
        )
        deleted["stock.r03_sector_rotation_day_states"] = cur.rowcount
        cur.execute(
            """
            DELETE FROM stock.r03_sector_rotation_basket_members bm
            USING stock.r03_sector_rotation_episodes e
            WHERE bm.episode_id = e.id AND e.t0_session=%s
              AND NOT EXISTS (
                  SELECT 1 FROM stock.r03_sector_rotation_day_states s
                  WHERE s.episode_id = e.id
              )
            """,
            (trade_date,),
        )
        deleted["stock.r03_sector_rotation_basket_members"] = cur.rowcount
        cur.execute(
            """
            UPDATE stock.r03_sector_rotation_episodes e
            SET parent_episode_id = NULL
            WHERE e.parent_episode_id IN (
                SELECT ep.id FROM stock.r03_sector_rotation_episodes ep
                WHERE ep.t0_session=%s
                  AND NOT EXISTS (
                      SELECT 1 FROM stock.r03_sector_rotation_day_states s
                      WHERE s.episode_id = ep.id
                  )
            )
              AND e.t0_session=%s
              AND NOT EXISTS (
                  SELECT 1 FROM stock.r03_sector_rotation_day_states s
                  WHERE s.episode_id = e.id
              )
            """,
            (trade_date, trade_date),
        )
        cur.execute(
            """
            DELETE FROM stock.r03_sector_rotation_episodes e
            WHERE e.t0_session=%s
              AND NOT EXISTS (
                  SELECT 1 FROM stock.r03_sector_rotation_day_states s
                  WHERE s.episode_id = e.id
              )
            """,
            (trade_date,),
        )
        deleted["stock.r03_sector_rotation_episodes"] = cur.rowcount
    return deleted


def invalidate_r03_from_date(
    config: Mapping[str, Any],
    *,
    from_trade_date: date,
    include_formal_market: bool = False,
) -> dict[str, int]:
    """上游输入在 from_trade_date 修订后，级联失效 from_trade_date 及全部后继 R03 事实，保留更早日期、独立市场宽度与真实 Raw。"""
    deleted: dict[str, int] = {}
    with source_raw._transaction(config, write=True, statement_timeout_ms=15000) as (
        _conn,
        cur,
        _,
    ):
        cur.execute(
            """
            DELETE FROM stock.r03_sector_loo_candidate_facts c
            USING stock.r03_sector_loo_support_facts f
            WHERE c.loo_fact_id = f.id AND f.trade_date >= %s
            """,
            (from_trade_date,),
        )
        deleted["stock.r03_sector_loo_candidate_facts"] = cur.rowcount
        for table in (
            "stock.r03_sector_loo_support_facts",
            "stock.r03_sector_overlap_pair_facts",
            "stock.r03_sector_overlap_board_facts",
            "stock.r03_sector_overlap_clusters",
            "stock.r03_sector_rotation_board_day_facts",
            "stock.r03_sector_native_day_facts",
        ):
            cur.execute(
                f"DELETE FROM {table} WHERE trade_date >= %s",
                (from_trade_date,),
            )
            deleted[table] = cur.rowcount
        if include_formal_market:
            cur.execute(
                "DELETE FROM stock.r03_formal_market_breadth_facts WHERE trade_date >= %s",
                (from_trade_date,),
            )
            deleted["stock.r03_formal_market_breadth_facts"] = cur.rowcount
        cur.execute(
            """
            DELETE FROM stock.r03_sector_rotation_day_states
            WHERE trade_date >= %s
            """,
            (from_trade_date,),
        )
        deleted["stock.r03_sector_rotation_day_states"] = cur.rowcount
        cur.execute(
            """
            DELETE FROM stock.r03_sector_rotation_basket_members bm
            USING stock.r03_sector_rotation_episodes e
            WHERE bm.episode_id = e.id AND e.t0_session >= %s
              AND NOT EXISTS (
                  SELECT 1 FROM stock.r03_sector_rotation_day_states s
                  WHERE s.episode_id = e.id
              )
            """,
            (from_trade_date,),
        )
        deleted["stock.r03_sector_rotation_basket_members"] = cur.rowcount
        cur.execute(
            """
            UPDATE stock.r03_sector_rotation_episodes e
            SET parent_episode_id = NULL
            WHERE e.parent_episode_id IN (
                SELECT ep.id FROM stock.r03_sector_rotation_episodes ep
                WHERE ep.t0_session >= %s
                  AND NOT EXISTS (
                      SELECT 1 FROM stock.r03_sector_rotation_day_states s
                      WHERE s.episode_id = ep.id
                  )
            )
              AND e.t0_session >= %s
              AND NOT EXISTS (
                  SELECT 1 FROM stock.r03_sector_rotation_day_states s
                  WHERE s.episode_id = e.id
              )
            """,
            (from_trade_date, from_trade_date),
        )
        cur.execute(
            """
            DELETE FROM stock.r03_sector_rotation_episodes e
            WHERE e.t0_session >= %s
              AND NOT EXISTS (
                  SELECT 1 FROM stock.r03_sector_rotation_day_states s
                  WHERE s.episode_id = e.id
              )
            """,
            (from_trade_date,),
        )
        deleted["stock.r03_sector_rotation_episodes"] = cur.rowcount
    return deleted
