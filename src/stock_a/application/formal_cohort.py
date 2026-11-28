"""13 类正式 Cohort 评价、事件子行、T0 冻结与观察登记生产器（ALG-34 / §11.8）。"""

from __future__ import annotations

from datetime import date, datetime
from fractions import Fraction
from typing import Any, Mapping

from stock_a.research.entry_context.sessions import _load_calendar
from stock_a.storage.postgres.formal_cohort import (
    ALL_FORMAL_COHORT_RULES,
    FORMAL_SECTOR_RULES,
    FORMAL_STOCK_RULES,
    persist_formal_cohort_day,
)
from stock_a.storage.postgres.source_raw import _transaction


def _to_fraction_pair(val: Any) -> tuple[int | None, int | None]:
    if val is None:
        return None, None
    try:
        f = Fraction(str(val))
        return int(f.numerator), int(f.denominator)
    except (TypeError, ValueError, ZeroDivisionError):
        return None, None


def _or_reduce_states(states: list[str]) -> str:
    if any(s == "TRUE" for s in states):
        return "TRUE"
    if states and all(s == "FALSE" for s in states):
        return "FALSE"
    return "UNKNOWN"


def produce_formal_cohort_day(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    time_lane: str = "H",
    first_captured_at: datetime | None = None,
    source_available_at: datetime | None = None,
    selected_rules: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    """基于 PG 已存 R02/R03/R04 正式上游生产 13 类正式 Cohort 评价与 T0 冻结登记。"""
    if target_session > research_cutoff:
        raise ValueError("正式Cohort目标日不得晚于研究截止日")
    if time_lane not in {"H", "L", "LATE"}:
        raise ValueError("time_lane 必须为 H/L/LATE")
    _, _, calendar_revision, calendar_sessions = _load_calendar(research_cutoff)
    if target_session not in calendar_sessions:
        raise ValueError("目标日不在官方交易日历中")
    t_idx = calendar_sessions.index(target_session)
    prev_session = calendar_sessions[t_idx - 1] if t_idx > 0 else None

    target_rules = tuple(selected_rules) if selected_rules else ALL_FORMAL_COHORT_RULES
    want_stock = any(r in FORMAL_STOCK_RULES for r in target_rules)
    want_sector = any(r in FORMAL_SECTOR_RULES for r in target_rules)

    with _transaction(config, write=False, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        raw_stocks: list[tuple[Any, ...]] = []
        seed_by_symbol: dict[str, tuple[Any, ...]] = {}
        d2_primary_by_symbol: dict[str, tuple[Any, ...]] = {}
        d2_states_by_symbol: dict[str, list[tuple[Any, ...]]] = {}
        admission_by_key: dict[tuple[str, str], tuple[Any, ...]] = {}
        pb_cont_by_key: dict[tuple[str, str], list[tuple[Any, ...]]] = {}
        profile_by_symbol: dict[str, tuple[Any, ...]] = {}
        cross_by_symbol: dict[str, tuple[Any, ...]] = {}
        industry_by_symbol: dict[str, str] = {}

        if want_stock:
            cursor.execute(
                """SELECT source_symbol, board, raw_is_st_value, raw_is_st_quality,
                          qfq_close, qfq_price_quality, qfq_source_capture_id, source_capture_id
                   FROM stock.source_raw_daily
                   WHERE session_date=%s
                   ORDER BY source_symbol""",
                (target_session,),
            )
            raw_stocks = cursor.fetchall()

            cursor.execute(
                """SELECT source_symbol, formal_seed_state, formal_seed_reasons,
                          safety_state, safety_reasons, formal_universe_state,
                          formal_universe_reason, source_capture_id
                   FROM stock.source_pool_seed_day_facts
                   WHERE target_session=%s
                     AND producer_version='SOURCE_POOL_SEED_CORE_PROFILE_R2'""",
                (target_session,),
            )
            for r in cursor.fetchall():
                seed_by_symbol[r[0]] = r

            cursor.execute(
                """SELECT entity_id, primary_status, primary_reason, primary_episode_id
                   FROM stock.d2_primary_episode_day_facts
                   WHERE session_date=%s""",
                (target_session,),
            )
            for r in cursor.fetchall():
                sym = r[0].split(":", 1)[-1] if ":" in r[0] else r[0]
                d2_primary_by_symbol[sym] = r

            cursor.execute(
                """SELECT entity_id, episode_id, effective_eligibility_value,
                          effective_eligibility_quality, effective_bucket,
                          display_maturity, tracking, exit_ordinal
                   FROM stock.d2_episode_day_states
                   WHERE session_date=%s""",
                (target_session,),
            )
            for r in cursor.fetchall():
                sym = r[0].split(":", 1)[-1] if ":" in r[0] else r[0]
                d2_states_by_symbol.setdefault(sym, []).append(r)

            cursor.execute(
                """SELECT source_symbol, scenario, admission_status, admission_reason,
                          event_id, linked_anchor_id, failure_level_numerator,
                          failure_level_denominator, failure_coordinate
                   FROM stock.source_d0_formal_admission_day_facts
                   WHERE target_session=%s""",
                (target_session,),
            )
            for r in cursor.fetchall():
                admission_by_key[(r[0], r[1])] = r

            cursor.execute(
                """SELECT source_symbol, scenario, parent_episode_id, peak_event_id,
                          peak_ordinal_key, raw_state, admission_status, admission_reason,
                          event_id, linked_anchor_id, failure_level_numerator,
                          failure_level_denominator, failure_coordinate
                   FROM stock.d0_parent_scenario_detail_facts
                   WHERE target_session=%s
                   ORDER BY source_symbol, scenario, parent_episode_id, peak_ordinal_key""",
                (target_session,),
            )
            for r in cursor.fetchall():
                pb_cont_by_key.setdefault((r[0], r[1]), []).append(r)

            cursor.execute(
                """SELECT source_symbol, pos60_exact_numerator, pos60_exact_denominator,
                          liquidity_mean20_exact_numerator, liquidity_mean20_exact_denominator
                   FROM stock.source_symbol_price_profiles
                   WHERE target_session=%s""",
                (target_session,),
            )
            for r in cursor.fetchall():
                profile_by_symbol[r[0]] = r

            cursor.execute(
                """SELECT source_symbol, rps20_exact
                   FROM stock.source_cross_section_day_facts
                   WHERE target_session=%s""",
                (target_session,),
            )
            for r in cursor.fetchall():
                cross_by_symbol[r[0]] = r

            # 读取最新官方行业快照成员（若有，标为NON_PIT主行业）
            cursor.execute(
                """SELECT m.source_symbol, min(m.board_native_id)
                   FROM stock.tdx_sector_members m
                   JOIN stock.tdx_sector_catalog c ON c.id = m.catalog_id
                   JOIN stock.tdx_sector_source_snapshots s ON s.snapshot_id = c.snapshot_id
                   WHERE m.board_kind = 'INDUSTRY'
                     AND s.snapshot_id = (
                         SELECT max(snapshot_id)
                         FROM stock.tdx_sector_source_snapshots
                         WHERE source_kind = 'TDX_TDXHY_CFG'
                     )
                   GROUP BY m.source_symbol"""
            )
            for sym, ind_id in cursor.fetchall():
                industry_by_symbol[sym] = ind_id

        native_sectors: list[tuple[Any, ...]] = []
        rotation_by_board: dict[tuple[str, str], tuple[Any, ...]] = {}
        if want_sector:
            cursor.execute(
                """SELECT board_kind, board_id, safety_state, prewatch_raw_state,
                          warm_raw_state, confirmed_raw_state,
                          prewatch_raw_reason, warm_raw_reason, confirmed_raw_reason
                   FROM stock.r03_sector_native_day_facts
                   WHERE trade_date=%s
                   ORDER BY board_kind, board_id""",
                (target_session,),
            )
            native_sectors = cursor.fetchall()

            cursor.execute(
                """SELECT e.board_kind, e.board_id, e.episode_key, s.rotation_state, s.reason_code
                   FROM stock.r03_sector_rotation_day_states s
                   JOIN stock.r03_sector_rotation_episodes e ON e.id = s.episode_id
                   WHERE s.trade_date=%s""",
                (target_session,),
            )
            for r in cursor.fetchall():
                rotation_by_board[(r[0], r[1])] = r

        # 读取前一官方会话的评价与登记状态（用于状态机 ENTRY/LANDMARK/PAUSED/EXIT/REENTRY）
        prev_eval_state: dict[tuple[str, str, str], str] = {}
        prev_obs_by_key: dict[tuple[str, str, str, str], tuple[Any, ...]] = {}
        ever_exited_keys: dict[tuple[str, str, str], int] = {}
        if prev_session is not None:
            cursor.execute(
                """SELECT rule_id, entity_type, entity_id, state
                   FROM stock.formal_cohort_evaluations
                   WHERE target_session=%s""",
                (prev_session,),
            )
            for r_id, e_type, e_id, st in cursor.fetchall():
                prev_eval_state[(r_id, e_type, e_id)] = st

            cursor.execute(
                """SELECT id, rule_id, entity_type, entity_id, observation_sub_key,
                          enrollment_kind, parent_observation_id
                   FROM stock.formal_research_observations
                   WHERE t0_session=%s AND time_lane=%s""",
                (prev_session, time_lane),
            )
            for r in cursor.fetchall():
                prev_obs_by_key[(r[1], r[2], r[3], r[4])] = r

        cursor.execute(
            """SELECT rule_id, entity_type, entity_id, max(id)
               FROM stock.formal_research_observations
               WHERE t0_session < %s AND time_lane=%s AND enrollment_kind='EXIT_PROVED'
               GROUP BY 1, 2, 3""",
            (target_session, time_lane),
        )
        for r_id, e_type, e_id, obs_id in cursor.fetchall():
            ever_exited_keys[(r_id, e_type, e_id)] = int(obs_id)

    evaluations: list[tuple[Any, ...]] = []
    subrows_by_eval_key: dict[tuple[str, str, str], list[tuple[Any, ...]]] = {}
    observations: list[tuple[Any, ...]] = []

    # 1. 评估股票 7 类规则
    for (
        sym,
        board,
        raw_is_st_val,
        raw_is_st_qual,
        qfq_close,
        qfq_qual,
        qfq_cap,
        raw_cap,
    ) in raw_stocks:
        is_st = (
            (raw_is_st_val == "1")
            if raw_is_st_qual == "OBSERVED" and raw_is_st_val in ("0", "1")
            else None
        )
        cap_id = qfq_cap or raw_cap
        seed = seed_by_symbol.get(sym)
        if board in ("STAR_BOARD", "BEIJING_BOARD", "STAR", "BSE"):
            u_status = "EXCLUDED"
            entity_id = f"STOCK:{sym}"
        elif board not in ("MAIN_BOARD", "GEM"):
            u_status = "UNMAPPED_UNKNOWN"
            entity_id = f"UNMAPPED:{sym}"
        else:
            u_status = "ELIGIBLE"
            entity_id = f"STOCK:{sym}"

        for rule_id in target_rules:
            if rule_id not in FORMAL_STOCK_RULES:
                continue
            if u_status == "EXCLUDED":
                eval_state, reasons, ev_cnt = (
                    "FALSE",
                    ["BOARD_EXCLUDED_FROM_FORMAL_STOCK_COHORT"],
                    0,
                )
            elif u_status == "UNMAPPED_UNKNOWN":
                eval_state, reasons, ev_cnt = "UNKNOWN", ["UNMAPPED_IDENTITY_SLOT"], 0
            elif rule_id == "STOCK_SEED":
                if seed is None:
                    eval_state, reasons, ev_cnt = (
                        "UNKNOWN",
                        ["SEED_FACT_NOT_COMPUTED"],
                        0,
                    )
                else:
                    eval_state = seed[1] or "UNKNOWN"
                    reasons = list(seed[2] or ())
                    ev_cnt = 1 if eval_state == "TRUE" else 0
            elif rule_id in ("STOCK_WARM", "STOCK_CONFIRMED"):
                eps = d2_states_by_symbol.get(sym, [])
                if not eps and not d2_primary_by_symbol:
                    eval_state, reasons, ev_cnt = "UNKNOWN", ["D2_NOT_COMPUTED"], 0
                else:
                    req_bucket = 1 if rule_id == "STOCK_WARM" else 0
                    matched_eps = [
                        e
                        for e in eps
                        if e[2] is True and e[4] is not None and int(e[4]) <= req_bucket
                    ]
                    unknown_eps = [e for e in eps if e[3] != "OBSERVED"]
                    if matched_eps:
                        eval_state, reasons, ev_cnt = "TRUE", [], len(matched_eps)
                    elif unknown_eps:
                        eval_state, reasons, ev_cnt = (
                            "UNKNOWN",
                            ["D2_ELIGIBILITY_UNKNOWN"],
                            0,
                        )
                    else:
                        eval_state, reasons, ev_cnt = "FALSE", ["D2_BUCKET_NOT_MET"], 0
            elif rule_id in ("STOCK_D0_LAUNCH_CONFIRM", "STOCK_D0_RECOVERY_TURN"):
                scen = (
                    "LAUNCH_CONFIRM"
                    if rule_id == "STOCK_D0_LAUNCH_CONFIRM"
                    else "RECOVERY_TURN"
                )
                adm = admission_by_key.get((sym, scen))
                if adm is None:
                    eval_state, reasons, ev_cnt = (
                        "UNKNOWN",
                        ["D0_ADMISSION_NOT_COMPUTED"],
                        0,
                    )
                else:
                    adm_status = adm[2]
                    if adm_status == "READY":
                        eval_state, reasons, ev_cnt = "TRUE", [], 1
                        subrows_by_eval_key.setdefault(
                            (rule_id, "STOCK", entity_id), []
                        ).append(
                            (
                                target_session,
                                rule_id,
                                entity_id,
                                str(adm[4] or "PRIMARY"),
                                None,
                                None,
                                adm[4],
                                adm[5],
                                "TRUE",
                                adm_status,
                                [],
                                adm[6],
                                adm[7],
                                adm[8],
                            )
                        )
                    elif adm_status in ("NOT_REQUIRED", "NOT_READY", "BLOCKED"):
                        eval_state, reasons, ev_cnt = (
                            "FALSE",
                            [str(adm[3] or adm_status)],
                            0,
                        )
                    else:
                        eval_state, reasons, ev_cnt = (
                            "UNKNOWN",
                            [str(adm[3] or "ADMISSION_UNKNOWN")],
                            0,
                        )
            else:
                scen = (
                    "STRONG_PULLBACK"
                    if rule_id == "STOCK_D0_STRONG_PULLBACK"
                    else "TREND_CONTINUE"
                )
                details = pb_cont_by_key.get((sym, scen), [])
                if not details:
                    # 当日 D2 存在但无 t-1 父 episode -> 已证无事件 FALSE
                    if d2_primary_by_symbol:
                        eval_state, reasons, ev_cnt = (
                            "FALSE",
                            ["NO_ELIGIBLE_PARENT_EPISODE"],
                            0,
                        )
                    else:
                        eval_state, reasons, ev_cnt = (
                            "UNKNOWN",
                            ["PB_CONT_NOT_COMPUTED"],
                            0,
                        )
                else:
                    sub_states: list[str] = []
                    sub_list: list[tuple[Any, ...]] = []
                    ready_cnt = 0
                    for d in details:
                        # (source_symbol, scenario, parent_episode_id, peak_event_id, peak_ordinal_key,
                        #  raw_state, admission_status, admission_reason, event_id, linked_anchor_id,
                        #  failure_level_numerator, failure_level_denominator, failure_coordinate)
                        (
                            p_ep,
                            pk_ev,
                            pk_ord,
                            r_st,
                            a_st,
                            a_rsn,
                            ev_id,
                            lnk_id,
                            f_num,
                            f_den,
                            f_coord,
                        ) = d[2:]
                        sub_key = f"{p_ep}:{pk_ord}"
                        if a_st == "READY":
                            s_state = "TRUE"
                            ready_cnt += 1
                        elif r_st == "FALSE" or a_st in (
                            "NOT_READY",
                            "BLOCKED",
                            "NOT_REQUIRED",
                        ):
                            s_state = "FALSE"
                        else:
                            s_state = "UNKNOWN"
                        sub_states.append(s_state)
                        sub_list.append(
                            (
                                target_session,
                                rule_id,
                                entity_id,
                                sub_key,
                                p_ep,
                                pk_ev,
                                ev_id,
                                lnk_id,
                                s_state,
                                a_st,
                                [a_rsn] if a_rsn else [],
                                f_num,
                                f_den,
                                f_coord,
                            )
                        )
                    eval_state = _or_reduce_states(sub_states)
                    reasons = (
                        []
                        if eval_state == "TRUE"
                        else [
                            sub_list[0][10][0] if sub_list[0][10] else "NO_READY_SUBROW"
                        ]
                    )
                    ev_cnt = ready_cnt
                    subrows_by_eval_key[(rule_id, "STOCK", entity_id)] = sub_list

            evaluations.append(
                (
                    target_session,
                    rule_id,
                    "STOCK",
                    entity_id,
                    sym,
                    board,
                    is_st,
                    u_status,
                    eval_state,
                    reasons,
                    ev_cnt,
                    cap_id,
                    calendar_revision,
                    "NON_PIT_RETROSPECTIVE",
                )
            )

            # T0 观察与登记（仅 ELIGIBLE 总体）
            if u_status == "ELIGIBLE":
                p0_num, p0_den = _to_fraction_pair(
                    qfq_close if qfq_qual == "OBSERVED" else None
                )
                coord_id = (
                    f"FORWARD_ADJUSTED:BAOSTOCK_SOURCE_CAPTURE:{qfq_cap}:{sym}"
                    if qfq_cap
                    else None
                )
                prof = profile_by_symbol.get(sym)
                pos_num = prof[1] if prof else None
                pos_den = prof[2] if prof else None
                amt_num = prof[3] if prof else None
                amt_den = prof[4] if prof else None
                cross = cross_by_symbol.get(sym)
                rps_num, rps_den = _to_fraction_pair(cross[1] if cross else None)
                safety_ok = (
                    bool(seed[3] == "TRUE") if seed and seed[3] is not None else None
                )
                ind_id = industry_by_symbol.get(sym)

                prev_st = prev_eval_state.get((rule_id, "STOCK", entity_id))
                prev_obs = prev_obs_by_key.get((rule_id, "STOCK", entity_id, "PRIMARY"))
                parent_obs_id = int(prev_obs[0]) if prev_obs else None

                if eval_state == "TRUE":
                    if prev_st is None:
                        kind = "FIRST_OBSERVED_LEFT_CENSORED"
                        rsn = ["LEFT_CENSORED_NO_PRIOR_SESSION"]
                    elif prev_st == "FALSE":
                        exited_id = ever_exited_keys.get((rule_id, "STOCK", entity_id))
                        if exited_id is not None:
                            kind = "REENTRY"
                            parent_obs_id = exited_id
                            rsn = ["MATCH_AFTER_PROVED_EXIT"]
                        else:
                            kind = "ENTRY"
                            parent_obs_id = None
                            rsn = ["ADJACENT_FALSE_TO_TRUE"]
                    else:
                        kind = "DAILY_LANDMARK"
                        rsn = ["CONTINUED_OR_RESUMED_MATCH"]
                    observations.append(
                        (
                            target_session,
                            time_lane,
                            rule_id,
                            "STOCK",
                            entity_id,
                            "PRIMARY",
                            sym,
                            kind,
                            parent_obs_id,
                            None,
                            None,
                            None,
                            p0_num,
                            p0_den,
                            coord_id,
                            rps_num,
                            rps_den,
                            pos_num,
                            pos_den,
                            amt_num,
                            amt_den,
                            ind_id,
                            board,
                            is_st,
                            safety_ok,
                            source_available_at,
                            first_captured_at,
                            rsn,
                        )
                    )
                elif (
                    eval_state == "FALSE"
                    and prev_obs is not None
                    and prev_obs[5]
                    in (
                        "ENTRY",
                        "DAILY_LANDMARK",
                        "REENTRY",
                        "FIRST_OBSERVED_LEFT_CENSORED",
                        "UNKNOWN_PAUSED",
                    )
                ):
                    observations.append(
                        (
                            target_session,
                            time_lane,
                            rule_id,
                            "STOCK",
                            entity_id,
                            "PRIMARY",
                            sym,
                            "EXIT_PROVED",
                            parent_obs_id,
                            None,
                            None,
                            None,
                            p0_num,
                            p0_den,
                            coord_id,
                            rps_num,
                            rps_den,
                            pos_num,
                            pos_den,
                            amt_num,
                            amt_den,
                            ind_id,
                            board,
                            is_st,
                            safety_ok,
                            source_available_at,
                            first_captured_at,
                            ["TRUE_TO_FALSE_EXIT"],
                        )
                    )
                elif (
                    eval_state == "UNKNOWN"
                    and prev_obs is not None
                    and prev_obs[5]
                    in (
                        "ENTRY",
                        "DAILY_LANDMARK",
                        "REENTRY",
                        "FIRST_OBSERVED_LEFT_CENSORED",
                        "UNKNOWN_PAUSED",
                    )
                ):
                    observations.append(
                        (
                            target_session,
                            time_lane,
                            rule_id,
                            "STOCK",
                            entity_id,
                            "PRIMARY",
                            sym,
                            "UNKNOWN_PAUSED",
                            parent_obs_id,
                            None,
                            None,
                            None,
                            p0_num,
                            p0_den,
                            coord_id,
                            rps_num,
                            rps_den,
                            pos_num,
                            pos_den,
                            amt_num,
                            amt_den,
                            ind_id,
                            board,
                            is_st,
                            safety_ok,
                            source_available_at,
                            first_captured_at,
                            ["UNKNOWN_PAUSE_KEEP_ENROLLMENT"],
                        )
                    )

    # 2. 评估板块 6 类规则（行业与概念严格独立）
    for (
        b_kind,
        b_id,
        safety_st,
        pw_st,
        warm_st,
        conf_st,
        pw_rsn,
        warm_rsn,
        conf_rsn,
    ) in native_sectors:
        e_type = "INDUSTRY" if b_kind == "INDUSTRY" else "CONCEPT"
        rot = rotation_by_board.get((b_kind, b_id))
        for rule_id in target_rules:
            if rule_id not in FORMAL_SECTOR_RULES:
                continue
            if rule_id == "SECTOR_PREWATCH":
                eval_state = pw_st or "UNKNOWN"
                reasons = [pw_rsn] if pw_rsn and eval_state != "TRUE" else []
            elif rule_id == "SECTOR_WARM":
                eval_state = warm_st or "UNKNOWN"
                reasons = [warm_rsn] if warm_rsn and eval_state != "TRUE" else []
            elif rule_id == "SECTOR_CONFIRMED_RAW":
                eval_state = conf_st or "UNKNOWN"
                reasons = [conf_rsn] if conf_rsn and eval_state != "TRUE" else []
            else:
                expected_rot = rule_id.replace("ROTATION_", "")
                if rot is None:
                    eval_state = "FALSE"
                    reasons = ["NO_ACTIVE_ROTATION_EPISODE"]
                elif rot[3] == "UNKNOWN":
                    eval_state = "UNKNOWN"
                    reasons = [rot[4] or "ROTATION_STATE_UNKNOWN"]
                elif rot[3] == expected_rot:
                    eval_state = "TRUE"
                    reasons = []
                else:
                    eval_state = "FALSE"
                    reasons = [f"ROTATION_STATE_{rot[3]}"]
            evaluations.append(
                (
                    target_session,
                    rule_id,
                    e_type,
                    b_id,
                    None,
                    b_kind,
                    None,
                    "ELIGIBLE",
                    eval_state,
                    reasons,
                    1 if eval_state == "TRUE" else 0,
                    None,
                    calendar_revision,
                    "NON_PIT_RETROSPECTIVE",
                )
            )

    persist_res = persist_formal_cohort_day(
        config,
        target_session=target_session,
        evaluations=evaluations,
        subrows_by_eval_key=subrows_by_eval_key,
        observations=observations,
        time_lane=time_lane,
    )
    return {
        "status": "PERSISTED",
        "target_session": target_session.isoformat(),
        "rule_count": len(target_rules),
        **persist_res,
    }
