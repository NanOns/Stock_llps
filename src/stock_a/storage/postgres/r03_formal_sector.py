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


def _parse_r03_partition_progress_filters(
    *,
    scope_key: str,
    member_version: str | None,
) -> tuple[int | None, str | None, int | None, int | None]:
    """从 R03 scope_key 与 member_version 解析精确 data_job_progress 过滤条件。"""
    cap_id: int | None = None
    prof_rev: str | None = None
    for part in str(scope_key).split(":"):
        if part.startswith("BAOSTOCK_CAP_"):
            raw_cap = part[len("BAOSTOCK_CAP_") :]
            if raw_cap.isdigit():
                cap_id = int(raw_cap)
        elif part.startswith("PROF_"):
            prof_rev = part[len("PROF_") :] or None
    ind_id: int | None = None
    con_id: int | None = None
    if member_version:
        for part in str(member_version).split(":"):
            if part.startswith("IND_"):
                raw_ind = part[len("IND_") :]
                if raw_ind.isdigit():
                    ind_id = int(raw_ind)
            elif part.startswith("CON_"):
                raw_con = part[len("CON_") :]
                if raw_con.isdigit():
                    con_id = int(raw_con)
    return cap_id, prof_rev, ind_id, con_id


def _read_r03_partition_progress_state(
    cur: Any,
    *,
    trade_date: date,
    business_table: str,
    scope_key: str,
    member_version: str | None,
    contract_version: str,
) -> tuple[str | None, int | None]:
    """精确查询 R03 业务分区在 data_job_progress 的最新状态，绝不跨独立版本串扰或通配缺失键。"""
    cap_id, prof_rev, ind_id, con_id = _parse_r03_partition_progress_filters(
        scope_key=scope_key, member_version=member_version
    )
    is_sector_table = business_table != "stock.r03_formal_market_breadth_facts"
    if cap_id is None:
        return None, None
    if is_sector_table and (prof_rev is None or ind_id is None or con_id is None):
        return None, None
    sql = """
        SELECT status, expected_rows
        FROM stock.data_job_progress
        WHERE target_session=%s AND business_table=%s
          AND producer_version=%s
          AND source_capture_id=%s
    """
    params: list[Any] = [trade_date, business_table, contract_version, cap_id]
    if prof_rev is not None:
        sql += " AND profile_revision=%s"
        params.append(prof_rev)
    elif not is_sector_table:
        sql += " AND profile_revision IS NULL"
    if is_sector_table:
        sql += " AND industry_snapshot_id=%s AND concept_snapshot_id=%s"
        params.extend([ind_id, con_id])
    sql += " ORDER BY updated_at DESC LIMIT 1"
    cur.execute(sql, tuple(params))
    row = cur.fetchone()
    if row is None:
        return None, None
    return str(row[0]), (int(row[1]) if row[1] is not None else None)


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
        where_sql = "WHERE trade_date=%s AND scope_key=%s AND member_version=%s AND contract_version=%s"
        params: list[Any] = [trade_date, scope_key, member_version, contract_version]
        if board_kind is not None:
            where_sql += " AND board_kind=%s"
            params.append(board_kind)

        cur.execute(
            f"SELECT count(*)::bigint FROM stock.r03_sector_native_day_facts {where_sql}",
            tuple(params),
        )
        total_count = int(cur.fetchone()[0])
        prog_status, prog_expected = _read_r03_partition_progress_state(
            cur,
            trade_date=trade_date,
            business_table="stock.r03_sector_native_day_facts",
            scope_key=scope_key,
            member_version=member_version,
            contract_version=contract_version,
        )
        if prog_status in {"FAILED", "INCOMPLETE", "RUNNING"}:
            return {
                "status": "INCOMPLETE",
                "total_count": 0,
                "items": [],
                "reason": "该精确分区 R03 Native 任务状态未完成，禁止读取半成品。",
            }
        if total_count == 0:
            if (
                prog_status in {"COMPLETED", "ALREADY_COMPLETE", "NO_CHANGE"}
                and prog_expected == 0
            ):
                return {
                    "status": "AVAILABLE_EMPTY",
                    "total_count": 0,
                    "items": [],
                    "reason": "该日期已完成计算且合法结果为空（0行）。",
                }
            return {
                "status": "NOT_COMPUTED",
                "total_count": 0,
                "items": [],
                "reason": "该日期与精确口径尚无已保存的 R03 Native 事实。",
            }
        if prog_status not in {"COMPLETED", "ALREADY_COMPLETE", "NO_CHANGE"}:
            return {
                "status": "INCOMPLETE",
                "total_count": 0,
                "items": [],
                "reason": "该精确分区缺少已验证完成进度记录，禁止读取未闭合半成品。",
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
    previous_scope_key: str | None = None,
    previous_member_version: str | None = None,
) -> dict[tuple[str, str], dict[str, Any]]:
    """从 PG 恢复上一官方交易日的板块成熟、降级目标与 PREWATCH 基准前态；仅按显式合法版本连续，绝不盲目全局回退。"""
    target_scope = previous_scope_key or scope_key
    target_mem = previous_member_version or member_version
    res = read_r03_native_day_facts(
        config,
        trade_date=previous_trade_date,
        scope_key=target_scope,
        member_version=target_mem,
        contract_version=contract_version,
    )
    if res["status"] == "AVAILABLE":
        return {(item["board_kind"], item["board_id"]): item for item in res["items"]}
    return {}


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
                    WHERE episode_key=%s AND scope_key=%s AND member_version=%s AND contract_version=%s
                    """,
                    (basket.episode_id, scope_key, member_version, contract_version),
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
                )
                cur.execute(
                    """
                    SELECT episode_id, rotation_state, pulse_raw_state, last_known_state, reason_code
                    FROM stock.r03_sector_rotation_board_day_facts
                    WHERE trade_date=%s AND board_kind=%s AND board_id=%s
                      AND scope_key=%s AND member_version=%s AND contract_version=%s
                    """,
                    (
                        trade_date,
                        b_kind,
                        b_id,
                        scope_key,
                        member_version,
                        contract_version,
                    ),
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
                    else:
                        cur.execute(
                            """
                            UPDATE stock.r03_sector_rotation_board_day_facts SET
                                episode_id=%s, rotation_state=%s, pulse_raw_state=%s,
                                last_known_state=%s, reason_code=%s, recorded_at=now()
                            WHERE trade_date=%s AND board_kind=%s AND board_id=%s
                              AND scope_key=%s AND member_version=%s AND contract_version=%s
                            """,
                            bd_vals
                            + (
                                trade_date,
                                b_kind,
                                b_id,
                                scope_key,
                                member_version,
                                contract_version,
                            ),
                        )
                        board_days_updated += 1
                else:
                    cur.execute(
                        """
                        INSERT INTO stock.r03_sector_rotation_board_day_facts (
                            trade_date, board_kind, board_id, scope_key, member_version,
                            contract_version, episode_id, rotation_state, pulse_raw_state,
                            last_known_state, reason_code
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            trade_date,
                            b_kind,
                            b_id,
                            scope_key,
                            member_version,
                            contract_version,
                        )
                        + bd_vals,
                    )
                    board_days_inserted += 1

            if episode_pg_id is None:
                continue
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
                bm.get("valuation_quality", "UNKNOWN"),
                bm.get("max_quote_age"),
                st.get("reason"),
            )
            cur.execute(
                """
                SELECT rotation_state, last_known_state, stale, market_age, eval_age,
                       accepted_once, armed, out_neg_streak, prev_mature_true, dq5,
                       formation_r, post_r, seed_retention, breadth_retention,
                       strong_retention, diffusion_state, early_state, mature_state,
                       valuation_quality, max_quote_age, reason_code
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
                    mature_state, valuation_quality, max_quote_age, reason_code
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
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
    scope_key: str,
    contract_version: str,
    previous_scope_key: str | None = None,
    member_version: str | None = None,
    previous_member_version: str | None = None,
    allow_cross_version_recovery: bool = False,
) -> dict[tuple[str, str], dict[str, Any]]:
    """跨重启从 PG 恢复各板块精确上一官方会话或历史最新 Rotation Episode、冻结篮子与最近日状态前态（严格校验已完成进度）。"""
    if previous_trade_date is None and before_trade_date is None:
        return {}
    prev_scope = previous_scope_key
    prev_mem = previous_member_version or member_version
    complete_statuses = {"COMPLETED", "ALREADY_COMPLETE", "NO_CHANGE"}

    with source_raw._transaction(config, write=False, statement_timeout_ms=10000) as (
        _conn,
        cur,
        _,
    ):
        prog_cache: dict[tuple[date, str, str], bool] = {}

        def _is_partition_complete(td: date, sk: str, mv: str) -> bool:
            ck = (td, sk, mv)
            if ck not in prog_cache:
                pst, _ = _read_r03_partition_progress_state(
                    cur,
                    trade_date=td,
                    business_table="stock.r03_sector_rotation_board_day_facts",
                    scope_key=sk,
                    member_version=mv,
                    contract_version=contract_version,
                )
                prog_cache[ck] = pst in complete_statuses
            return prog_cache[ck]

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
        if previous_trade_date is not None:
            if prev_scope is not None:
                mem_filter = " AND b.member_version=%s" if prev_mem is not None else ""
                params_ep: list[Any] = [
                    prev_scope,
                    contract_version,
                    previous_trade_date,
                ]
                if prev_mem is not None:
                    params_ep.append(prev_mem)
                cur.execute(
                    f"""
                    SELECT e.id, e.episode_key, e.board_kind, e.board_id, e.t0_session,
                           e.parent_episode_id, e.s0_known, e.u0_known, e.h0_known, e.left_censored,
                           s.trade_date, s.rotation_state, s.last_known_state, s.stale,
                           s.market_age, s.eval_age, s.accepted_once, s.armed, s.out_neg_streak,
                           s.prev_mature_true, s.dq5, e.frozen_member_count,
                           b.scope_key, b.member_version, e.scope_key
                    FROM stock.r03_sector_rotation_board_day_facts b
                    JOIN stock.r03_sector_rotation_episodes e ON e.id = b.episode_id
                    JOIN stock.r03_sector_rotation_day_states s
                      ON s.episode_id = e.id AND s.trade_date = b.trade_date
                    WHERE b.scope_key=%s AND b.contract_version=%s AND b.trade_date=%s
                    {mem_filter}
                    ORDER BY b.board_kind, b.board_id, e.id DESC
                    """,
                    tuple(params_ep),
                )
                for r in cur.fetchall():
                    key = (str(r[2]), str(r[3]))
                    if key in out or not _is_partition_complete(
                        r[10], str(r[22]), str(r[23])
                    ):
                        continue
                    basket = _build_basket(
                        int(r[0]),
                        str(r[1]),
                        str(r[2]),
                        str(r[3]),
                        r[4],
                        bool(r[6]),
                        bool(r[7]),
                        bool(r[8]),
                        bool(r[9]),
                        int(r[21]),
                    )
                    out[key] = {
                        "episode_pg_id": int(r[0]),
                        "episode_scope_key": str(r[24]),
                        "parent_episode_id": r[5],
                        "basket": basket,
                        "prev_trade_date": r[10],
                        "missing_previous_session": False,
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
                bd_sql = """
                    SELECT DISTINCT ON (board_kind, board_id)
                           board_kind, board_id, episode_id, rotation_state, last_known_state,
                           reason_code, scope_key, member_version
                    FROM stock.r03_sector_rotation_board_day_facts
                    WHERE scope_key=%s AND contract_version=%s AND trade_date = %s
                """
                bd_params: list[Any] = [
                    prev_scope,
                    contract_version,
                    previous_trade_date,
                ]
                if prev_mem is not None:
                    bd_sql += " AND member_version=%s"
                    bd_params.append(prev_mem)
                bd_sql += " ORDER BY board_kind, board_id, id DESC"
                cur.execute(bd_sql, tuple(bd_params))
                for bk, bid, bd_ep_id, rot_st, lk_st, rsn, b_sk, b_mv in cur.fetchall():
                    key = (str(bk), str(bid))
                    if key in out or bd_ep_id is not None:
                        continue
                    if not _is_partition_complete(
                        previous_trade_date, str(b_sk), str(b_mv)
                    ):
                        continue
                    out[key] = {
                        "episode_pg_id": None,
                        "episode_scope_key": None,
                        "parent_episode_id": None,
                        "basket": None,
                        "prev_trade_date": previous_trade_date,
                        "missing_previous_session": False,
                        "state_dict": {
                            "state": str(rot_st),
                            "last_known_state": lk_st,
                            "stale": False,
                            "market_age": 0,
                            "eval_age": 0,
                            "accepted_once": False,
                            "armed": False,
                            "out_neg_streak": 0,
                            "prev_mature_true": False,
                            "dq5": None,
                            "reason": rsn,
                        },
                    }

            # 缺中间日或跨版本前态恢复：先查同 prev_scope，若 allow_cross_version_recovery 则允许沿同 contract_version 最近已完成日接续
            miss_cond = (
                "b.contract_version=%s AND b.trade_date <= %s"
                if allow_cross_version_recovery
                else "b.scope_key=%s AND b.contract_version=%s AND b.trade_date < %s"
            )
            miss_params: list[Any] = (
                [contract_version, previous_trade_date]
                if allow_cross_version_recovery
                else [prev_scope or scope_key, contract_version, previous_trade_date]
            )
            miss_mem = (
                ""
                if (allow_cross_version_recovery or prev_mem is None)
                else " AND b.member_version=%s"
            )
            if miss_mem:
                miss_params.append(prev_mem)
            cur.execute(
                f"""
                SELECT e.id, e.episode_key, e.board_kind, e.board_id, e.t0_session,
                       e.parent_episode_id, e.s0_known, e.u0_known, e.h0_known, e.left_censored,
                       s.trade_date, s.rotation_state, s.last_known_state, s.stale,
                       s.market_age, s.eval_age, s.accepted_once, s.armed, s.out_neg_streak,
                       s.prev_mature_true, s.dq5, e.frozen_member_count,
                       b.scope_key, b.member_version, e.scope_key
                FROM stock.r03_sector_rotation_board_day_facts b
                JOIN stock.r03_sector_rotation_episodes e ON e.id = b.episode_id
                JOIN stock.r03_sector_rotation_day_states s
                  ON s.episode_id = e.id AND s.trade_date = b.trade_date
                WHERE {miss_cond} {miss_mem}
                ORDER BY b.board_kind, b.board_id, b.trade_date DESC,
                         (b.scope_key = %s) DESC,
                         (b.member_version = %s) DESC,
                         e.id DESC
                """,
                tuple(miss_params) + (prev_scope or scope_key, prev_mem or ""),
            )
            for mr_row in cur.fetchall():
                key = (str(mr_row[2]), str(mr_row[3]))
                if key in out:
                    continue
                if not _is_partition_complete(
                    mr_row[10], str(mr_row[22]), str(mr_row[23])
                ):
                    continue
                basket_rec = _build_basket(
                    int(mr_row[0]),
                    str(mr_row[1]),
                    str(mr_row[2]),
                    str(mr_row[3]),
                    mr_row[4],
                    bool(mr_row[6]),
                    bool(mr_row[7]),
                    bool(mr_row[8]),
                    bool(mr_row[9]),
                    int(mr_row[21]),
                )
                out[key] = {
                    "episode_pg_id": int(mr_row[0]),
                    "episode_scope_key": str(mr_row[24]),
                    "parent_episode_id": mr_row[5],
                    "basket": basket_rec,
                    "prev_trade_date": mr_row[10],
                    "missing_previous_session": mr_row[10] < previous_trade_date,
                    "state_dict": {
                        "state": str(mr_row[11]),
                        "last_known_state": mr_row[12],
                        "stale": bool(mr_row[13]),
                        "market_age": int(mr_row[14]),
                        "eval_age": int(mr_row[15]),
                        "accepted_once": bool(mr_row[16]),
                        "armed": bool(mr_row[17]),
                        "out_neg_streak": int(mr_row[18]),
                        "prev_mature_true": bool(mr_row[19]),
                        "dq5": Fraction(mr_row[20]) if mr_row[20] is not None else None,
                    },
                }
        else:
            mem_filter = " AND e.member_version=%s" if prev_mem is not None else ""
            params_hist: list[Any] = [
                prev_scope or scope_key,
                contract_version,
                before_trade_date,
            ]
            if prev_mem is not None:
                params_hist.append(prev_mem)
            cur.execute(
                f"""
                SELECT e.id, e.episode_key, e.board_kind, e.board_id, e.t0_session,
                       e.parent_episode_id, e.s0_known, e.u0_known, e.h0_known, e.left_censored,
                       s.trade_date, s.rotation_state, s.last_known_state, s.stale,
                       s.market_age, s.eval_age, s.accepted_once, s.armed, s.out_neg_streak,
                       s.prev_mature_true, s.dq5, e.frozen_member_count,
                       e.scope_key, e.member_version
                FROM stock.r03_sector_rotation_episodes e
                JOIN stock.r03_sector_rotation_day_states s ON s.episode_id = e.id
                WHERE e.scope_key=%s AND e.contract_version=%s AND s.trade_date < %s
                {mem_filter}
                ORDER BY e.board_kind, e.board_id, s.trade_date DESC, e.id DESC
                """,
                tuple(params_hist),
            )
            for r in cur.fetchall():
                key = (str(r[2]), str(r[3]))
                if key in out or not _is_partition_complete(
                    r[10], str(r[22]), str(r[23])
                ):
                    continue
                basket = _build_basket(
                    int(r[0]),
                    str(r[1]),
                    str(r[2]),
                    str(r[3]),
                    r[4],
                    bool(r[6]),
                    bool(r[7]),
                    bool(r[8]),
                    bool(r[9]),
                    int(r[21]),
                )
                out[key] = {
                    "episode_pg_id": int(r[0]),
                    "episode_scope_key": str(r[22]),
                    "parent_episode_id": r[5],
                    "basket": basket,
                    "prev_trade_date": r[10],
                    "missing_previous_session": False,
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
    member_version: str | None = None,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    """只读板块完整 Rotation Episode、冻结篮子成员、逐日状态及跨版已完成状态时间线（按日逐分区校验完成态）。"""
    complete_statuses = {"COMPLETED", "ALREADY_COMPLETE", "NO_CHANGE"}
    with source_raw._transaction(config, write=False, statement_timeout_ms=10000) as (
        _conn,
        cur,
        _,
    ):
        bd_sql = """
            SELECT b.trade_date, b.episode_id, b.rotation_state, b.pulse_raw_state,
                   b.last_known_state, b.reason_code, b.scope_key, b.member_version
            FROM stock.r03_sector_rotation_board_day_facts b
            WHERE b.board_kind=%s AND b.board_id=%s AND b.scope_key=%s AND b.contract_version=%s
        """
        bd_params: list[Any] = [board_kind, board_id, scope_key, contract_version]
        if member_version is not None:
            bd_sql += " AND b.member_version=%s"
            bd_params.append(member_version)
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

        prog_cache: dict[tuple[date, str, str], str | None] = {}

        def _prog_status(td: date, sk: str, mv: str) -> str | None:
            ck = (td, sk, mv)
            if ck not in prog_cache:
                pst, _ = _read_r03_partition_progress_state(
                    cur,
                    trade_date=td,
                    business_table="stock.r03_sector_rotation_board_day_facts",
                    scope_key=sk,
                    member_version=mv,
                    contract_version=contract_version,
                )
                prog_cache[ck] = pst
            return prog_cache[ck]

        latest_row = raw_bd_rows[-1]
        latest_pst = _prog_status(latest_row[0], str(latest_row[6]), str(latest_row[7]))
        if latest_pst not in complete_statuses:
            return {
                "status": "INCOMPLETE",
                "episodes": [],
                "board_day_facts": [],
                "reason": "该板块 Rotation 目标日分区尚未完成验证，禁止读取半成品。",
            }

        completed_bd_rows = [
            br
            for br in raw_bd_rows
            if _prog_status(br[0], str(br[6]), str(br[7])) in complete_statuses
        ]
        # 沿 episode_id -> parent_episode_id 收集跨版家族 episode_id，并回溯更早版本的已完成 board_day_facts
        family_ep_ids: set[int] = {
            int(br[1]) for br in completed_bd_rows if br[1] is not None
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

        all_bd_by_day: dict[date, Any] = {br[0]: br for br in completed_bd_rows}
        if family_ep_ids:
            hist_sql = """
                SELECT b.trade_date, b.episode_id, b.rotation_state, b.pulse_raw_state,
                       b.last_known_state, b.reason_code, b.scope_key, b.member_version
                FROM stock.r03_sector_rotation_board_day_facts b
                WHERE b.board_kind=%s AND b.board_id=%s AND b.contract_version=%s
                  AND b.episode_id = ANY(%s)
            """
            hist_params: list[Any] = [
                board_kind,
                board_id,
                contract_version,
                sorted(family_ep_ids),
            ]
            if as_of_date is not None:
                hist_sql += " AND b.trade_date <= %s"
                hist_params.append(as_of_date)
            hist_sql += " ORDER BY b.trade_date ASC"
            cur.execute(hist_sql, tuple(hist_params))
            for hbr in cur.fetchall():
                if (
                    hbr[0] not in all_bd_by_day
                    and _prog_status(hbr[0], str(hbr[6]), str(hbr[7]))
                    in complete_statuses
                ):
                    all_bd_by_day[hbr[0]] = hbr

        ordered_bd_rows = [all_bd_by_day[d] for d in sorted(all_bd_by_day)]
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
            st_sql = """
                SELECT trade_date, rotation_state, last_known_state, stale, market_age,
                       eval_age, accepted_once, armed, out_neg_streak, formation_r, post_r,
                       seed_retention, breadth_retention, strong_retention,
                       diffusion_state, early_state, mature_state,
                       valuation_quality, max_quote_age, reason_code
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
    scope_key: str,
    member_version: str,
    contract_version: str,
) -> dict[str, Any]:
    """按精确分区完成状态只读当日 Rotation 板块日表摘要，禁止把未完成半成品挂进普通目录。"""
    with source_raw._transaction(config, write=False, statement_timeout_ms=10000) as (
        _conn,
        cur,
        _,
    ):
        prog_status, prog_expected = _read_r03_partition_progress_state(
            cur,
            trade_date=trade_date,
            business_table="stock.r03_sector_rotation_board_day_facts",
            scope_key=scope_key,
            member_version=member_version,
            contract_version=contract_version,
        )
        if prog_status in {"FAILED", "INCOMPLETE", "RUNNING"}:
            return {"status": "INCOMPLETE", "items": {}}
        cur.execute(
            """
            SELECT board_kind, board_id, rotation_state, last_known_state, reason_code
            FROM stock.r03_sector_rotation_board_day_facts
            WHERE trade_date=%s AND scope_key=%s AND member_version=%s AND contract_version=%s
            """,
            (trade_date, scope_key, member_version, contract_version),
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
                  AND scope_key=%s AND member_version=%s AND contract_version=%s
                """,
                (trade_date, bk, bid, scope_key, member_version, contract_version),
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
                    UPDATE stock.r03_sector_overlap_board_facts SET
                        unique_share_status=%s, unique_share=%s, cluster_id=%s,
                        in_overview_summary=%s, reason_code=%s, recorded_at=now()
                    WHERE trade_date=%s AND board_kind=%s AND board_id=%s
                      AND scope_key=%s AND member_version=%s AND contract_version=%s
                    """,
                    vals_b
                    + (
                        trade_date,
                        bk,
                        bid,
                        scope_key,
                        member_version,
                        contract_version,
                    ),
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
        if allow_revision:
            current_cluster_keys = {
                (str(c["board_kind"]), str(c["cluster_id"])) for c in clusters
            }
            cur.execute(
                """
                SELECT board_kind, cluster_id
                FROM stock.r03_sector_overlap_clusters
                WHERE trade_date=%s AND scope_key=%s AND member_version=%s AND contract_version=%s
                """,
                (trade_date, scope_key, member_version, contract_version),
            )
            for old_bk, old_cid in cur.fetchall():
                if (str(old_bk), str(old_cid)) not in current_cluster_keys:
                    cur.execute(
                        """
                        DELETE FROM stock.r03_sector_overlap_clusters
                        WHERE trade_date=%s AND board_kind=%s AND cluster_id=%s
                          AND scope_key=%s AND member_version=%s AND contract_version=%s
                        """,
                        (
                            trade_date,
                            old_bk,
                            old_cid,
                            scope_key,
                            member_version,
                            contract_version,
                        ),
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
                WHERE trade_date=%s AND scope_key=%s AND member_version=%s AND contract_version=%s
                """,
                (trade_date, scope_key, member_version, contract_version),
            )
            for old_bk, old_a, old_b in cur.fetchall():
                if (str(old_bk), str(old_a), str(old_b)) not in current_pair_keys:
                    cur.execute(
                        """
                        DELETE FROM stock.r03_sector_overlap_pair_facts
                        WHERE trade_date=%s AND board_kind=%s AND board_id_a=%s AND board_id_b=%s
                          AND scope_key=%s AND member_version=%s AND contract_version=%s
                        """,
                        (
                            trade_date,
                            old_bk,
                            old_a,
                            old_b,
                            scope_key,
                            member_version,
                            contract_version,
                        ),
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
                  AND scope_key=%s AND member_version=%s AND contract_version=%s
                """,
                (
                    trade_date,
                    pr["board_kind"],
                    pr["board_id_a"],
                    pr["board_id_b"],
                    scope_key,
                    member_version,
                    contract_version,
                ),
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
                    UPDATE stock.r03_sector_overlap_pair_facts SET
                        jaccard_status=%s, jaccard_value=%s, reason_code=%s, recorded_at=now()
                    WHERE trade_date=%s AND board_kind=%s AND board_id_a=%s AND board_id_b=%s
                      AND scope_key=%s AND member_version=%s AND contract_version=%s
                    """,
                    p_vals
                    + (
                        trade_date,
                        pr["board_kind"],
                        pr["board_id_a"],
                        pr["board_id_b"],
                        scope_key,
                        member_version,
                        contract_version,
                    ),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO stock.r03_sector_overlap_pair_facts (
                        trade_date, board_kind, board_id_a, board_id_b, scope_key,
                        member_version, contract_version, jaccard_status, jaccard_value, reason_code
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        trade_date,
                        pr["board_kind"],
                        pr["board_id_a"],
                        pr["board_id_b"],
                        scope_key,
                        member_version,
                        contract_version,
                    )
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
        where_sql = "WHERE trade_date=%s AND scope_key=%s AND member_version=%s AND contract_version=%s"
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
            scope_key=scope_key,
            member_version=member_version,
            contract_version=contract_version,
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
            if (
                ov_status in {"COMPLETED", "ALREADY_COMPLETE", "NO_CHANGE"}
                and ov_expected == 0
            ):
                return {
                    "status": "AVAILABLE_EMPTY",
                    "clusters": [],
                    "board_facts": [],
                    "pairs": [],
                    "reason": "该日期 Overlap 已完成计算且合法结果为空（0簇）。",
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
    scope_key: str,
    member_version: str,
    contract_version: str,
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
                scope_key=scope_key,
                member_version=member_version,
                contract_version=contract_version,
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
    scope_key: str,
    member_version: str,
    contract_version: str,
    loo_result: Mapping[str, Any],
    allow_revision: bool = False,
) -> dict[str, int]:
    inserted = updated = skipped = 0
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
            SELECT board_id, safety_state, prewatch_raw_state, warm_raw_state,
                   confirmed_raw_state, emergence_level, seed_wilson_point,
                   rs5_pct, dq5, reason_code
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
                Decimal(str(r[6])) if r[6] is not None else None,
                Decimal(str(r[7])) if r[7] is not None else None,
                Decimal(str(r[8])) if r[8] is not None else None,
                str(r[9]) if r[9] is not None else None,
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
    with source_raw._transaction(config, write=True, statement_timeout_ms=15000) as (
        _conn,
        cur,
        _,
    ):
        return _write_r03_loo_support_in_cursor(
            cur,
            trade_date=trade_date,
            board_kind=board_kind,
            scope_key=scope_key,
            member_version=member_version,
            contract_version=contract_version,
            loo_result=loo_result,
            allow_revision=allow_revision,
        )


def verify_r03_partition_keys(
    config: Mapping[str, Any],
    *,
    module_code: str,
    trade_date: date,
    scope_key: str,
    member_version: str,
    contract_version: str,
    expected_keys: set[tuple[str, str]],
    expected_sub_keys: Mapping[str, set[tuple[str, ...]]] | None = None,
) -> dict[str, Any]:
    """独立只读连接核验 R03 分区父子实际业务键集合与预期严格相等（防少行、同数错键、额外或预期0陈旧）。"""
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
                WHERE trade_date=%s AND scope_key=%s AND member_version=%s AND contract_version=%s
                """,
                (trade_date, scope_key, member_version, contract_version),
            )
            actual_keys = {(str(r[0]), str(r[1])) for r in cur.fetchall()}
        elif module_code == "R03_SECTOR_ROTATION":
            cur.execute(
                """
                SELECT board_kind, board_id, episode_id
                FROM stock.r03_sector_rotation_board_day_facts
                WHERE trade_date=%s AND scope_key=%s AND member_version=%s AND contract_version=%s
                """,
                (trade_date, scope_key, member_version, contract_version),
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
                WHERE trade_date=%s AND scope_key=%s AND member_version=%s AND contract_version=%s
                """,
                (trade_date, scope_key, member_version, contract_version),
            )
            actual_keys = {(str(r[0]), str(r[1])) for r in cur.fetchall()}
            if expected_sub_keys is not None:
                if "board_facts" in expected_sub_keys:
                    cur.execute(
                        """
                        SELECT board_kind, board_id
                        FROM stock.r03_sector_overlap_board_facts
                        WHERE trade_date=%s AND scope_key=%s AND member_version=%s AND contract_version=%s
                        """,
                        (trade_date, scope_key, member_version, contract_version),
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
                        WHERE trade_date=%s AND scope_key=%s AND member_version=%s AND contract_version=%s
                        """,
                        (trade_date, scope_key, member_version, contract_version),
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
                WHERE trade_date=%s AND scope_key=%s AND member_version=%s AND contract_version=%s
                """,
                (trade_date, scope_key, member_version, contract_version),
            )
            actual_keys = {(str(r[0]), str(r[1])) for r in cur.fetchall()}
            if expected_sub_keys is not None and "candidates" in expected_sub_keys:
                cur.execute(
                    """
                    SELECT f.target_entity_id, f.board_kind, c.board_id
                    FROM stock.r03_sector_loo_candidate_facts c
                    JOIN stock.r03_sector_loo_support_facts f ON f.id = c.loo_fact_id
                    WHERE f.trade_date=%s AND f.scope_key=%s AND f.member_version=%s AND f.contract_version=%s
                    """,
                    (trade_date, scope_key, member_version, contract_version),
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
        loo_status, _loo_expected = _read_r03_partition_progress_state(
            cur,
            trade_date=trade_date,
            business_table="stock.r03_sector_loo_support_facts",
            scope_key=scope_key,
            member_version=member_version,
            contract_version=contract_version,
        )
        if loo_status in {"FAILED", "INCOMPLETE", "RUNNING"}:
            return {
                "status": "INCOMPLETE",
                "target_entity_id": target_entity_id,
                "board_kind": board_kind,
                "candidates": [],
                "reason": "该精确分区 LOO 任务状态未完成，禁止读取半成品。",
            }
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
        if loo_status not in {"COMPLETED", "ALREADY_COMPLETE", "NO_CHANGE"}:
            return {
                "status": "INCOMPLETE",
                "target_entity_id": target_entity_id,
                "board_kind": board_kind,
                "candidates": [],
                "reason": "该精确分区 LOO 缺少已验证完成进度记录，禁止读取半成品。",
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


def write_r03_formal_market_breadth(
    config: Mapping[str, Any],
    *,
    trade_date: date,
    scope_key: str,
    contract_version: str,
    result: Mapping[str, Any],
    allow_revision: bool = False,
) -> dict[str, int]:
    """幂等写入 R03 正式市场宽度日级事实（独立于固定来源代码池）。"""
    u_t = str(result["universe_version_t"])
    u_t3 = str(result["universe_version_t3"])
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
        _num(Fraction(num, den))
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
            WHERE trade_date=%s AND scope_key=%s
              AND universe_version_t=%s AND universe_version_t3=%s
              AND contract_version=%s
            """,
            (trade_date, scope_key, u_t, u_t3, contract_version),
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
                    f"正式市场宽度({trade_date}, {scope_key}, {u_t}, {u_t3})已存在不同内容且未授权修订。",
                )
            cur.execute(
                """
                UPDATE stock.r03_formal_market_breadth_facts SET
                    status=%s, reason_code=%s, n_t=%s, n_t3=%s, n_common=%s,
                    n_eligible=%s, unknown_count=%s, exact_numerator=%s,
                    exact_denominator=%s, breadth_value=%s, recorded_at=now()
                WHERE trade_date=%s AND scope_key=%s
                  AND universe_version_t=%s AND universe_version_t3=%s
                  AND contract_version=%s
                """,
                vals + (trade_date, scope_key, u_t, u_t3, contract_version),
            )
            return {"inserted": 0, "updated": 1, "skipped": 0}
        cur.execute(
            """
            INSERT INTO stock.r03_formal_market_breadth_facts (
                trade_date, scope_key, universe_version_t, universe_version_t3,
                contract_version, status, reason_code, n_t, n_t3, n_common,
                n_eligible, unknown_count, exact_numerator, exact_denominator,
                breadth_value
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (trade_date, scope_key, u_t, u_t3, contract_version) + vals,
        )
        return {"inserted": 1, "updated": 0, "skipped": 0}


def read_r03_formal_market_breadth(
    config: Mapping[str, Any],
    *,
    trade_date: date,
    scope_key: str | None = None,
    universe_version_t: str | None = None,
    universe_version_t3: str | None = None,
    contract_version: str = "R03_FORMAL_SECTOR_V1",
) -> dict[str, Any]:
    """只读已保存的 R03 正式市场宽度日级事实；缺失或同口径多版本歧义时诚实返回状态。"""
    with source_raw._transaction(config, write=False, statement_timeout_ms=10000) as (
        _conn,
        cur,
        _,
    ):
        sql = """
            SELECT scope_key, universe_version_t, universe_version_t3, status, reason_code,
                   n_t, n_t3, n_common, n_eligible, unknown_count,
                   exact_numerator, exact_denominator, breadth_value
            FROM stock.r03_formal_market_breadth_facts
            WHERE trade_date=%s AND contract_version=%s
        """
        params: list[Any] = [trade_date, contract_version]
        if scope_key is not None:
            sql += " AND scope_key=%s"
            params.append(scope_key)
        if universe_version_t is not None:
            sql += " AND universe_version_t=%s"
            params.append(universe_version_t)
        if universe_version_t3 is not None:
            sql += " AND universe_version_t3=%s"
            params.append(universe_version_t3)
        sql += " ORDER BY recorded_at DESC, id DESC"
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
        if not rows:
            return {
                "status": "NOT_READY_HISTORICAL_UNIVERSE",
                "reason": "该日期尚无已保存的正式市场宽度日级事实。",
            }
        if len(rows) > 1:
            return {
                "status": "AMBIGUOUS_SCOPE",
                "reason": f"该日期匹配到 {len(rows)} 条不同口径/Universe版本的正式市场宽度事实，须指定精确 scope 与两端 Universe 版本。",
            }
        r = rows[0]
        mkt_status, _mkt_expected = _read_r03_partition_progress_state(
            cur,
            trade_date=trade_date,
            business_table="stock.r03_formal_market_breadth_facts",
            scope_key=str(r[0]),
            member_version=None,
            contract_version=contract_version,
        )
        if mkt_status not in {"COMPLETED", "ALREADY_COMPLETE", "NO_CHANGE"}:
            return {
                "status": "INCOMPLETE"
                if mkt_status in {"FAILED", "INCOMPLETE", "RUNNING"}
                else "NOT_READY_HISTORICAL_UNIVERSE",
                "reason": "该精确分区正式市场宽度尚未完成验证，禁止读取未闭合数据。",
            }
        return {
            "status": str(r[3]),
            "scope_key": str(r[0]),
            "universe_version_t": str(r[1]),
            "universe_version_t3": str(r[2]),
            "reason": r[4],
            "n_t": int(r[5]),
            "n_t3": int(r[6]),
            "n_common": int(r[7]),
            "n_eligible": int(r[8]),
            "unknown_count": int(r[9]),
            "exact": (
                {"numerator": int(r[10]), "denominator": int(r[11])}
                if r[10] is not None and r[11] is not None
                else None
            ),
            "breadth_value": str(r[12]) if r[12] is not None else None,
        }


def cleanup_r03_scope_day(
    config: Mapping[str, Any],
    *,
    trade_date: date,
    scope_key: str,
    member_version: str | None = None,
    contract_version: str | None = None,
    include_formal_market: bool = False,
) -> dict[str, int]:
    """按精确日期、scope 及可选成员/合同版本有界清理 R03 正式事实，不触碰其他捕获、日期或来源层，默认不误删独立市场宽度。"""
    deleted: dict[str, int] = {}
    ver_sql = ""
    ver_params: tuple[Any, ...] = ()
    if member_version is not None:
        ver_sql += " AND member_version=%s"
        ver_params += (member_version,)
    if contract_version is not None:
        ver_sql += " AND contract_version=%s"
        ver_params += (contract_version,)

    ep_ver_sql = ""
    if member_version is not None:
        ep_ver_sql += " AND e.member_version=%s"
    if contract_version is not None:
        ep_ver_sql += " AND e.contract_version=%s"

    with source_raw._transaction(config, write=True, statement_timeout_ms=15000) as (
        _conn,
        cur,
        _,
    ):
        cur.execute(
            f"""
            DELETE FROM stock.r03_sector_loo_candidate_facts c
            USING stock.r03_sector_loo_support_facts f
            WHERE c.loo_fact_id = f.id AND f.trade_date=%s AND f.scope_key=%s
            {ep_ver_sql.replace("e.", "f.")}
            """,
            (trade_date, scope_key, *ver_params),
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
                f"DELETE FROM {table} WHERE trade_date=%s AND scope_key=%s {ver_sql}",
                (trade_date, scope_key, *ver_params),
            )
            deleted[table] = cur.rowcount
        if include_formal_market:
            mkt_sql = "DELETE FROM stock.r03_formal_market_breadth_facts WHERE trade_date=%s AND scope_key=%s"
            mkt_params: tuple[Any, ...] = (trade_date, scope_key)
            if contract_version is not None:
                mkt_sql += " AND contract_version=%s"
                mkt_params += (contract_version,)
            cur.execute(mkt_sql, mkt_params)
            deleted["stock.r03_formal_market_breadth_facts"] = cur.rowcount
        cur.execute(
            f"""
            DELETE FROM stock.r03_sector_rotation_day_states s
            USING stock.r03_sector_rotation_episodes e
            WHERE s.episode_id = e.id AND s.trade_date=%s AND e.scope_key=%s {ep_ver_sql}
            """,
            (trade_date, scope_key, *ver_params),
        )
        deleted["stock.r03_sector_rotation_day_states"] = cur.rowcount
        cur.execute(
            f"""
            DELETE FROM stock.r03_sector_rotation_basket_members bm
            USING stock.r03_sector_rotation_episodes e
            WHERE bm.episode_id = e.id AND e.t0_session=%s AND e.scope_key=%s {ep_ver_sql}
              AND NOT EXISTS (
                  SELECT 1 FROM stock.r03_sector_rotation_day_states s
                  WHERE s.episode_id = e.id
              )
            """,
            (trade_date, scope_key, *ver_params),
        )
        deleted["stock.r03_sector_rotation_basket_members"] = cur.rowcount
        cur.execute(
            f"""
            UPDATE stock.r03_sector_rotation_episodes e
            SET parent_episode_id = NULL
            WHERE e.parent_episode_id IN (
                SELECT ep.id FROM stock.r03_sector_rotation_episodes ep
                WHERE ep.t0_session=%s AND ep.scope_key=%s {ep_ver_sql.replace("e.", "ep.")}
                  AND NOT EXISTS (
                      SELECT 1 FROM stock.r03_sector_rotation_day_states s
                      WHERE s.episode_id = ep.id
                  )
            )
              AND e.t0_session=%s AND e.scope_key=%s {ep_ver_sql}
              AND NOT EXISTS (
                  SELECT 1 FROM stock.r03_sector_rotation_day_states s
                  WHERE s.episode_id = e.id
              )
            """,
            (trade_date, scope_key, *ver_params, trade_date, scope_key, *ver_params),
        )
        cur.execute(
            f"""
            DELETE FROM stock.r03_sector_rotation_episodes e
            WHERE e.t0_session=%s AND e.scope_key=%s {ep_ver_sql}
              AND NOT EXISTS (
                  SELECT 1 FROM stock.r03_sector_rotation_day_states s
                  WHERE s.episode_id = e.id
              )
            """,
            (trade_date, scope_key, *ver_params),
        )
        deleted["stock.r03_sector_rotation_episodes"] = cur.rowcount
    return deleted


def invalidate_r03_from_date(
    config: Mapping[str, Any],
    *,
    from_trade_date: date,
    scope_key: str,
    member_version: str | None = None,
    contract_version: str | None = None,
    include_formal_market: bool = False,
) -> dict[str, int]:
    """上游输入在 from_trade_date 修订后，按同 scope（及可选成员/合同版本）级联失效 from_trade_date 及全部后继 R03 事实，保留更早日期、其他 scope/成员版本、独立市场宽度与真实 Raw。"""
    deleted: dict[str, int] = {}
    ver_sql = ""
    ver_params: tuple[Any, ...] = ()
    if member_version is not None:
        ver_sql += " AND member_version=%s"
        ver_params += (member_version,)
    if contract_version is not None:
        ver_sql += " AND contract_version=%s"
        ver_params += (contract_version,)

    ep_ver_sql = ""
    if member_version is not None:
        ep_ver_sql += " AND e.member_version=%s"
    if contract_version is not None:
        ep_ver_sql += " AND e.contract_version=%s"

    with source_raw._transaction(config, write=True, statement_timeout_ms=15000) as (
        _conn,
        cur,
        _,
    ):
        cur.execute(
            f"""
            DELETE FROM stock.r03_sector_loo_candidate_facts c
            USING stock.r03_sector_loo_support_facts f
            WHERE c.loo_fact_id = f.id AND f.trade_date >= %s AND f.scope_key=%s
            {ep_ver_sql.replace("e.", "f.")}
            """,
            (from_trade_date, scope_key, *ver_params),
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
                f"DELETE FROM {table} WHERE trade_date >= %s AND scope_key=%s {ver_sql}",
                (from_trade_date, scope_key, *ver_params),
            )
            deleted[table] = cur.rowcount
        if include_formal_market:
            mkt_sql = "DELETE FROM stock.r03_formal_market_breadth_facts WHERE trade_date >= %s AND scope_key=%s"
            mkt_params: tuple[Any, ...] = (from_trade_date, scope_key)
            if contract_version is not None:
                mkt_sql += " AND contract_version=%s"
                mkt_params += (contract_version,)
            cur.execute(mkt_sql, mkt_params)
            deleted["stock.r03_formal_market_breadth_facts"] = cur.rowcount
        cur.execute(
            f"""
            DELETE FROM stock.r03_sector_rotation_day_states s
            USING stock.r03_sector_rotation_episodes e
            WHERE s.episode_id = e.id AND s.trade_date >= %s AND e.scope_key=%s {ep_ver_sql}
            """,
            (from_trade_date, scope_key, *ver_params),
        )
        deleted["stock.r03_sector_rotation_day_states"] = cur.rowcount
        cur.execute(
            f"""
            DELETE FROM stock.r03_sector_rotation_basket_members bm
            USING stock.r03_sector_rotation_episodes e
            WHERE bm.episode_id = e.id AND e.t0_session >= %s AND e.scope_key=%s {ep_ver_sql}
              AND NOT EXISTS (
                  SELECT 1 FROM stock.r03_sector_rotation_day_states s
                  WHERE s.episode_id = e.id
              )
            """,
            (from_trade_date, scope_key, *ver_params),
        )
        deleted["stock.r03_sector_rotation_basket_members"] = cur.rowcount
        cur.execute(
            f"""
            UPDATE stock.r03_sector_rotation_episodes e
            SET parent_episode_id = NULL
            WHERE e.parent_episode_id IN (
                SELECT ep.id FROM stock.r03_sector_rotation_episodes ep
                WHERE ep.t0_session >= %s AND ep.scope_key=%s {ep_ver_sql.replace("e.", "ep.")}
                  AND NOT EXISTS (
                      SELECT 1 FROM stock.r03_sector_rotation_day_states s
                      WHERE s.episode_id = ep.id
                  )
            )
              AND e.t0_session >= %s AND e.scope_key=%s {ep_ver_sql}
              AND NOT EXISTS (
                  SELECT 1 FROM stock.r03_sector_rotation_day_states s
                  WHERE s.episode_id = e.id
              )
            """,
            (
                from_trade_date,
                scope_key,
                *ver_params,
                from_trade_date,
                scope_key,
                *ver_params,
            ),
        )
        cur.execute(
            f"""
            DELETE FROM stock.r03_sector_rotation_episodes e
            WHERE e.t0_session >= %s AND e.scope_key=%s {ep_ver_sql}
              AND NOT EXISTS (
                  SELECT 1 FROM stock.r03_sector_rotation_day_states s
                  WHERE s.episode_id = e.id
              )
            """,
            (from_trade_date, scope_key, *ver_params),
        )
        deleted["stock.r03_sector_rotation_episodes"] = cur.rowcount
    return deleted
