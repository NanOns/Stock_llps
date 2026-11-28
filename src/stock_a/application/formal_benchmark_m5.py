"""固定基准、五期限板块收盘路径、M5_R1 对照与 ITT 离线生产及 PG 读写（D01/D03）。"""

from __future__ import annotations

from datetime import date
from fractions import Fraction
from typing import Any, Mapping

from stock_a.domain.formal_forward_benchmarks import (
    M5Candidate,
    compute_fixed_basket_path,
    evaluate_market_benchmark_eligibility,
    match_m5_controls_for_t0,
)
from stock_a.research.entry_context.sessions import _load_calendar
from stock_a.storage.postgres.source_raw import _transaction


def _to_fraction(num: Any, den: Any) -> Fraction | None:
    if num is None or den is None or int(den) <= 0:
        return None
    return Fraction(int(num), int(den))


def produce_formal_benchmarks_and_m5_for_t0(
    config: Mapping[str, Any],
    *,
    t0_session: date,
    research_cutoff: date,
) -> dict[str, Any]:
    """为指定 T0 交易日的正式观察生成五期限固定基准、相对收益、M5 对照与 ITT/Clean 结果。"""
    if t0_session > research_cutoff:
        raise ValueError("T0不得晚于研究截止日")
    _, _, _, sessions = _load_calendar(research_cutoff)
    if t0_session not in sessions:
        raise ValueError("T0不在官方交易日历中")
    t0_idx = sessions.index(t0_session)
    due_horizons = [
        (h, sessions[t0_idx + h])
        for h in (1, 3, 5, 10, 20)
        if t0_idx + h < len(sessions) and sessions[t0_idx + h] <= research_cutoff
    ]

    with _transaction(config, write=False, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT id, rule_id, entity_type, entity_id, source_symbol,
                      enrollment_kind, t0_p0_numerator, t0_p0_denominator,
                      t0_qfq_coordinate_id, t0_rps20_num, t0_rps20_den,
                      t0_pos60_num, t0_pos60_den, t0_amt20_num, t0_amt20_den,
                      t0_industry_id, t0_board_type, t0_is_st, t0_safety_gate_passed
               FROM stock.formal_research_observations
               WHERE t0_session=%s
               ORDER BY id""",
            (t0_session,),
        )
        obs_rows = cursor.fetchall()
        if not obs_rows:
            return {
                "status": "NO_OBSERVATIONS",
                "t0_session": t0_session.isoformat(),
                "benchmarks_inserted": 0,
                "controls_inserted": 0,
                "outcomes_inserted": 0,
            }

        # 读取当日完整股票评价（用于M5 NO_MATCH候选与市场基准分母）
        cursor.execute(
            """SELECT e.rule_id, e.entity_id, e.source_symbol, e.board_kind, e.is_st,
                      e.universe_status, e.state,
                      s.safety_state, r.qfq_close, r.qfq_price_quality, r.qfq_source_capture_id,
                      p.pos60_exact_numerator, p.pos60_exact_denominator,
                      p.liquidity_mean20_exact_numerator, p.liquidity_mean20_exact_denominator,
                      c.rps20_exact
               FROM stock.formal_cohort_evaluations e
               LEFT JOIN stock.source_pool_seed_day_facts s
                 ON s.target_session=e.target_session AND s.source_symbol=e.source_symbol
                AND s.producer_version='SOURCE_POOL_SEED_CORE_PROFILE_R2'
               LEFT JOIN stock.source_raw_daily r
                 ON r.session_date=e.target_session AND r.source_symbol=e.source_symbol
               LEFT JOIN stock.source_symbol_price_profiles p
                 ON p.target_session=e.target_session AND p.source_symbol=e.source_symbol
               LEFT JOIN stock.source_cross_section_day_facts c
                 ON c.target_session=e.target_session AND c.source_symbol=e.source_symbol
               WHERE e.target_session=%s AND e.entity_type='STOCK'""",
            (t0_session,),
        )
        eval_pool_rows = cursor.fetchall()

        # 读取最新官方行业成员映射
        cursor.execute(
            """SELECT m.source_symbol, m.board_native_id
               FROM stock.tdx_sector_members m
               JOIN stock.tdx_sector_catalog c ON c.id = m.catalog_id
               WHERE m.board_kind = 'INDUSTRY'
                 AND c.snapshot_id = (
                     SELECT max(snapshot_id)
                     FROM stock.tdx_sector_source_snapshots
                     WHERE source_kind = 'TDX_TDXHY_CFG'
                 )"""
        )
        sym_to_ind: dict[str, str] = {}
        ind_to_syms: dict[str, list[str]] = {}
        for sym, ind_id in cursor.fetchall():
            sym_to_ind.setdefault(sym, ind_id)
            ind_to_syms.setdefault(ind_id, []).append(sym)

        # 读取所需全窗口 QFQ 收盘价格
        max_h = max((h for h, _ in due_horizons), default=0)
        window_sessions = tuple(sessions[t0_idx : t0_idx + max_h + 1])
        closes_by_sym_sess: dict[
            str, dict[date, tuple[Fraction | None, str | None, str]]
        ] = {}
        if window_sessions:
            cursor.execute(
                """SELECT source_symbol, session_date, qfq_close, qfq_price_quality,
                          qfq_source_capture_id
                   FROM stock.source_raw_daily
                   WHERE session_date=ANY(%s)""",
                (list(window_sessions),),
            )
            for sym, s_date, q_close, q_qual, q_cap in cursor.fetchall():
                c_val = (
                    Fraction(str(q_close))
                    if q_close is not None and q_qual == "OBSERVED"
                    else None
                )
                coord = (
                    f"FORWARD_ADJUSTED:BAOSTOCK_SOURCE_CAPTURE:{q_cap}:{sym}"
                    if q_cap
                    else None
                )
                closes_by_sym_sess.setdefault(sym, {})[s_date] = (
                    c_val,
                    coord,
                    str(q_qual or "UNKNOWN"),
                )

        # 读取后继会话哪些对照转为 MATCH（用于 CLEAN_SENSITIVITY 口径）
        later_sessions = [d for d in window_sessions[1:]]
        later_matches: dict[tuple[str, str], date] = {}
        if later_sessions:
            cursor.execute(
                """SELECT rule_id, entity_id, min(target_session)
                   FROM stock.formal_cohort_evaluations
                   WHERE target_session=ANY(%s) AND state='TRUE'
                   GROUP BY 1, 2""",
                (later_sessions,),
            )
            for r_id, e_id, m_date in cursor.fetchall():
                later_matches[(r_id, e_id)] = m_date

    # 组织 M5 候选池与市场基准成员
    m5_pool_by_rule: dict[str, list[M5Candidate]] = {}
    market_eligible_syms: set[str] = set()
    known_universe_syms: set[str] = set()
    for row in eval_pool_rows:
        (
            r_id,
            e_id,
            sym,
            board,
            is_st,
            u_status,
            st,
            safety_st,
            q_close,
            q_qual,
            q_cap,
            pos_n,
            pos_d,
            amt_n,
            amt_d,
            rps20_ex,
        ) = row
        if u_status in ("ELIGIBLE", "EXCLUDED") and sym:
            known_universe_syms.add(sym)
        if (
            u_status == "ELIGIBLE"
            and safety_st == "TRUE"
            and q_qual == "OBSERVED"
            and q_close is not None
        ):
            market_eligible_syms.add(sym)
        rps_val = Fraction(str(rps20_ex)) if rps20_ex is not None else None
        cand = M5Candidate(
            entity_id=e_id,
            source_symbol=sym or "",
            rule_state=st,
            safety_gate_passed=(safety_st == "TRUE") if safety_st is not None else None,
            board_type=board,
            is_st=is_st,
            industry_id=sym_to_ind.get(sym or ""),
            p0=Fraction(str(q_close))
            if q_close is not None and q_qual == "OBSERVED"
            else None,
            rps20=rps_val,
            pos60=_to_fraction(pos_n, pos_d),
            amt20=_to_fraction(amt_n, amt_d),
        )
        m5_pool_by_rule.setdefault(r_id, []).append(cand)

    benchmark_rows: list[tuple[Any, ...]] = []
    control_rows: list[tuple[Any, ...]] = []
    outcome_rows: list[tuple[Any, ...]] = []

    # 计算市场基准（所有股票共享同一市场固定篮子）
    mkt_ok, mkt_cov, mkt_err = evaluate_market_benchmark_eligibility(
        known_research_universe_count=len(known_universe_syms),
        eligible_basket_count=len(market_eligible_syms),
    )
    mkt_weights = (
        {
            s: Fraction(1, len(market_eligible_syms))
            for s in sorted(market_eligible_syms)
        }
        if mkt_ok
        else {}
    )

    for obs in obs_rows:
        (
            obs_id,
            rule_id,
            entity_type,
            entity_id,
            sym,
            enr_kind,
            p0_n,
            p0_d,
            coord_id,
            rps_n,
            rps_d,
            pos_n,
            pos_d,
            amt_n,
            amt_d,
            ind_id,
            board_type,
            is_st,
            safety_ok,
        ) = obs
        if enr_kind not in (
            "ENTRY",
            "DAILY_LANDMARK",
            "REENTRY",
            "FIRST_OBSERVED_LEFT_CENSORED",
        ):
            continue
        t_cand = M5Candidate(
            entity_id=entity_id,
            source_symbol=sym or "",
            rule_state="TRUE",
            safety_gate_passed=safety_ok,
            board_type=board_type,
            is_st=is_st,
            industry_id=ind_id or sym_to_ind.get(sym or ""),
            p0=_to_fraction(p0_n, p0_d),
            rps20=_to_fraction(rps_n, rps_d),
            pos60=_to_fraction(pos_n, pos_d),
            amt20=_to_fraction(amt_n, amt_d),
        )
        m5_res = match_m5_controls_for_t0(t_cand, m5_pool_by_rule.get(rule_id, ()))
        if not m5_res["controls"]:
            control_rows.append(
                (
                    obs_id,
                    t0_session,
                    rule_id,
                    entity_id,
                    0,
                    "NONE",
                    "NONE",
                    m5_res["match_status"],
                    0,
                    None,
                    None,
                    None,
                    None,
                )
            )
        else:
            for c in m5_res["controls"]:
                w = c["weight"]
                d = c["distance"]
                control_rows.append(
                    (
                        obs_id,
                        t0_session,
                        rule_id,
                        entity_id,
                        c["rank"],
                        c["entity_id"],
                        c["source_symbol"],
                        m5_res["match_status"],
                        m5_res["match_k"],
                        d.numerator,
                        d.denominator,
                        w.numerator if w is not None else None,
                        w.denominator if w is not None else None,
                    )
                )

        # 为该观察的到期期限计算 M5 ITT / CLEAN_SENSITIVITY 以及 MARKET 基准
        for h, ep_day in due_horizons:
            path_days = tuple(sessions[t0_idx + 1 : t0_idx + h + 1])
            target_r: Fraction | None = None
            if sym:
                t0_item = closes_by_sym_sess.get(sym, {}).get(t0_session)
                ep_item = closes_by_sym_sess.get(sym, {}).get(ep_day)
                if (
                    t0_item
                    and ep_item
                    and t0_item[0]
                    and ep_item[0]
                    and t0_item[2] == "OBSERVED"
                    and ep_item[2] == "OBSERVED"
                    and t0_item[1] == ep_item[1]
                ):
                    target_r = ep_item[0] / t0_item[0] - 1

            # MARKET 基准
            if mkt_ok:
                mkt_path = compute_fixed_basket_path(
                    member_weights=mkt_weights,
                    closes_by_symbol_session=closes_by_sym_sess,
                    t0_session=t0_session,
                    path_sessions=path_days,
                )
                b_ret = mkt_path["endpoint_return"]
                rel_ret = (
                    (target_r - b_ret)
                    if (target_r is not None and b_ret is not None)
                    else None
                )
                benchmark_rows.append(
                    (
                        t0_session,
                        f"OBS:{obs_id}",
                        h,
                        "MARKET",
                        "MARKET_UNIVERSE",
                        ep_day,
                        mkt_path["basket_member_count"],
                        mkt_path["evaluable_member_count"],
                        mkt_cov.numerator if mkt_cov else None,
                        mkt_cov.denominator if mkt_cov else None,
                        b_ret.numerator if b_ret is not None else None,
                        b_ret.denominator if b_ret is not None else None,
                        rel_ret.numerator if rel_ret is not None else None,
                        rel_ret.denominator if rel_ret is not None else None,
                        mkt_path["mfe_close"].numerator
                        if mkt_path["mfe_close"] is not None
                        else None,
                        mkt_path["mfe_close"].denominator
                        if mkt_path["mfe_close"] is not None
                        else None,
                        mkt_path["mae_close"].numerator
                        if mkt_path["mae_close"] is not None
                        else None,
                        mkt_path["mae_close"].denominator
                        if mkt_path["mae_close"] is not None
                        else None,
                        mkt_path["mdd_close"].numerator
                        if mkt_path["mdd_close"] is not None
                        else None,
                        mkt_path["mdd_close"].denominator
                        if mkt_path["mdd_close"] is not None
                        else None,
                        mkt_path["outcome_quality"],
                        mkt_path["reason_codes"],
                    )
                )
            else:
                benchmark_rows.append(
                    (
                        t0_session,
                        f"OBS:{obs_id}",
                        h,
                        "MARKET",
                        "MARKET_UNIVERSE",
                        ep_day,
                        len(market_eligible_syms),
                        0,
                        mkt_cov.numerator if mkt_cov else None,
                        mkt_cov.denominator if mkt_cov else None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        "UNKNOWN",
                        [mkt_err or "MARKET_BENCHMARK_UNKNOWN"],
                    )
                )

            # M5 ITT & CLEAN_SENSITIVITY
            if m5_res["match_status"] != "MATCHED":
                for variant in ("ITT", "CLEAN_SENSITIVITY"):
                    outcome_rows.append(
                        (
                            obs_id,
                            h,
                            variant,
                            m5_res["match_status"],
                            m5_res["match_k"],
                            None,
                            None,
                            None,
                            None,
                            "UNKNOWN",
                            [m5_res["reason"] or "M5_NOT_MATCHED"],
                        )
                    )
            else:
                itt_weights = {
                    c["source_symbol"]: c["weight"]
                    for c in m5_res["controls"]
                    if c["weight"] is not None
                }
                itt_path = compute_fixed_basket_path(
                    member_weights=itt_weights,
                    closes_by_symbol_session=closes_by_sym_sess,
                    t0_session=t0_session,
                    path_sessions=path_days,
                )
                c_ret = itt_path["endpoint_return"]
                ex_ret = (
                    (target_r - c_ret)
                    if (target_r is not None and c_ret is not None)
                    else None
                )
                outcome_rows.append(
                    (
                        obs_id,
                        h,
                        "ITT",
                        "MATCHED",
                        m5_res["match_k"],
                        c_ret.numerator if c_ret is not None else None,
                        c_ret.denominator if c_ret is not None else None,
                        ex_ret.numerator if ex_ret is not None else None,
                        ex_ret.denominator if ex_ret is not None else None,
                        itt_path["outcome_quality"],
                        itt_path["reason_codes"],
                    )
                )

                clean_controls = [
                    c
                    for c in m5_res["controls"]
                    if not (
                        (rule_id, c["entity_id"]) in later_matches
                        and later_matches[(rule_id, c["entity_id"])] <= ep_day
                    )
                ]
                if not clean_controls:
                    outcome_rows.append(
                        (
                            obs_id,
                            h,
                            "CLEAN_SENSITIVITY",
                            "MATCHED",
                            0,
                            None,
                            None,
                            None,
                            None,
                            "UNKNOWN",
                            ["ALL_CONTROLS_CONVERTED_TO_MATCH"],
                        )
                    )
                else:
                    clean_weights = {
                        c["source_symbol"]: Fraction(1, len(clean_controls))
                        for c in clean_controls
                    }
                    clean_path = compute_fixed_basket_path(
                        member_weights=clean_weights,
                        closes_by_symbol_session=closes_by_sym_sess,
                        t0_session=t0_session,
                        path_sessions=path_days,
                    )
                    cc_ret = clean_path["endpoint_return"]
                    cex_ret = (
                        (target_r - cc_ret)
                        if (target_r is not None and cc_ret is not None)
                        else None
                    )
                    outcome_rows.append(
                        (
                            obs_id,
                            h,
                            "CLEAN_SENSITIVITY",
                            "MATCHED",
                            len(clean_controls),
                            cc_ret.numerator if cc_ret is not None else None,
                            cc_ret.denominator if cc_ret is not None else None,
                            cex_ret.numerator if cex_ret is not None else None,
                            cex_ret.denominator if cex_ret is not None else None,
                            clean_path["outcome_quality"],
                            clean_path["reason_codes"],
                        )
                    )

    obs_ids = [int(o[0]) for o in obs_rows]
    target_keys = [f"OBS:{oid}" for oid in obs_ids]
    with _transaction(config, write=True, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            "DELETE FROM stock.formal_forward_benchmarks WHERE t0_session=%s AND target_key=ANY(%s)",
            (t0_session, target_keys),
        )
        if benchmark_rows:
            cursor.executemany(
                """INSERT INTO stock.formal_forward_benchmarks (
                    t0_session, target_key, horizon, benchmark_kind, benchmark_ref_id,
                    endpoint_session, basket_member_count, evaluable_member_count,
                    coverage_ratio_num, coverage_ratio_den, benchmark_return_num,
                    benchmark_return_den, relative_return_num, relative_return_den,
                    mfe_close_num, mfe_close_den, mae_close_num, mae_close_den,
                    mdd_close_num, mdd_close_den, outcome_quality, reason_codes
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                benchmark_rows,
            )
        cursor.execute(
            "DELETE FROM stock.formal_m5_matched_controls WHERE observation_id=ANY(%s)",
            (obs_ids,),
        )
        if control_rows:
            cursor.executemany(
                """INSERT INTO stock.formal_m5_matched_controls (
                    observation_id, t0_session, rule_id, target_entity_id,
                    control_rank, control_entity_id, control_symbol,
                    match_status, match_k, distance_num, distance_den,
                    weight_num, weight_den
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                control_rows,
            )
        cursor.execute(
            "DELETE FROM stock.formal_m5_control_outcomes WHERE observation_id=ANY(%s)",
            (obs_ids,),
        )
        if outcome_rows:
            cursor.executemany(
                """INSERT INTO stock.formal_m5_control_outcomes (
                    observation_id, horizon, variant, match_status, match_k,
                    control_return_num, control_return_den, excess_return_num,
                    excess_return_den, outcome_quality, reason_codes
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                outcome_rows,
            )

    return {
        "status": "PERSISTED",
        "t0_session": t0_session.isoformat(),
        "benchmarks_inserted": len(benchmark_rows),
        "controls_inserted": len(control_rows),
        "outcomes_inserted": len(outcome_rows),
    }


def read_formal_benchmarks_and_m5(
    config: Mapping[str, Any],
    *,
    t0_session: date,
    observation_id: int | None = None,
) -> dict[str, Any]:
    """只读查询指定 T0 的固定基准与 M5 对照/ITT 结果。"""
    with _transaction(config, write=False, statement_timeout_ms=15000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT target_key, horizon, benchmark_kind, benchmark_ref_id,
                      basket_member_count, evaluable_member_count,
                      benchmark_return_num, benchmark_return_den,
                      relative_return_num, relative_return_den,
                      mfe_close_num, mfe_close_den, mae_close_num, mae_close_den,
                      mdd_close_num, mdd_close_den, outcome_quality, reason_codes
               FROM stock.formal_forward_benchmarks
               WHERE t0_session=%s
                 AND (%s::text IS NULL OR target_key=%s)
               ORDER BY target_key, horizon, benchmark_kind""",
            (
                t0_session,
                f"OBS:{observation_id}" if observation_id else None,
                f"OBS:{observation_id}" if observation_id else None,
            ),
        )
        benchmarks = [
            {
                "target_key": r[0],
                "horizon": int(r[1]),
                "benchmark_kind": r[2],
                "benchmark_ref_id": r[3],
                "basket_member_count": int(r[4]),
                "evaluable_member_count": int(r[5]),
                "benchmark_return": str(Fraction(int(r[6]), int(r[7])))
                if r[6] is not None and r[7]
                else None,
                "relative_return": str(Fraction(int(r[8]), int(r[9])))
                if r[8] is not None and r[9]
                else None,
                "mfe_close": str(Fraction(int(r[10]), int(r[11])))
                if r[10] is not None and r[11]
                else None,
                "mae_close": str(Fraction(int(r[12]), int(r[13])))
                if r[12] is not None and r[13]
                else None,
                "mdd_close": str(Fraction(int(r[14]), int(r[15])))
                if r[14] is not None and r[15]
                else None,
                "outcome_quality": r[16],
                "reason_codes": list(r[17] or ()),
            }
            for r in cursor.fetchall()
        ]
        cursor.execute(
            """SELECT observation_id, control_rank, control_entity_id, control_symbol,
                      match_status, match_k, distance_num, distance_den, weight_num, weight_den
               FROM stock.formal_m5_matched_controls
               WHERE t0_session=%s
                 AND (%s::bigint IS NULL OR observation_id=%s)
               ORDER BY observation_id, control_rank""",
            (t0_session, observation_id, observation_id),
        )
        controls = [
            {
                "observation_id": int(r[0]),
                "control_rank": int(r[1]),
                "control_entity_id": r[2],
                "control_symbol": r[3],
                "match_status": r[4],
                "match_k": int(r[5]),
                "distance": str(Fraction(int(r[6]), int(r[7])))
                if r[6] is not None and r[7]
                else None,
                "weight": str(Fraction(int(r[8]), int(r[9])))
                if r[8] is not None and r[9]
                else None,
            }
            for r in cursor.fetchall()
        ]
        cursor.execute(
            """SELECT o.observation_id, o.horizon, o.variant, o.match_status, o.match_k,
                      o.control_return_num, o.control_return_den,
                      o.excess_return_num, o.excess_return_den, o.outcome_quality, o.reason_codes
               FROM stock.formal_m5_control_outcomes o
               JOIN stock.formal_research_observations ob ON ob.id = o.observation_id
               WHERE ob.t0_session=%s
                 AND (%s::bigint IS NULL OR o.observation_id=%s)
               ORDER BY o.observation_id, o.horizon, o.variant""",
            (t0_session, observation_id, observation_id),
        )
        outcomes = [
            {
                "observation_id": int(r[0]),
                "horizon": int(r[1]),
                "variant": r[2],
                "match_status": r[3],
                "match_k": int(r[4]),
                "control_return": str(Fraction(int(r[5]), int(r[6])))
                if r[5] is not None and r[6]
                else None,
                "excess_return": str(Fraction(int(r[7]), int(r[8])))
                if r[7] is not None and r[8]
                else None,
                "outcome_quality": r[9],
                "reason_codes": list(r[10] or ()),
            }
            for r in cursor.fetchall()
        ]
    return {
        "status": "AVAILABLE" if benchmarks or controls else "NO_STORED_FACT",
        "t0_session": t0_session.isoformat(),
        "benchmarks": benchmarks,
        "m5_controls": controls,
        "m5_outcomes": outcomes,
    }
