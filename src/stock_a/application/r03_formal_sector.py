"""R03 正式市场、板块、轮动、LOO 与 Overlap 离线协调器入口。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
from fractions import Fraction
from typing import Any

from stock_a.application.source_market_context import (
    compute_formal_market_breadth_from_universes,
)
from stock_a.research.sector_native import (
    MemberSlot,
    compute_sector_loo_and_support,
    compute_sector_native_day,
)
from stock_a.research.sector_overlap import compute_sector_overlap
from stock_a.research.sector_rotation import (
    FrozenRotationBasket,
    create_frozen_basket,
    evaluate_basket_metrics,
    evaluate_pulse_raw,
    step_rotation_state_machine,
)
from stock_a.storage.postgres import source_raw
from stock_a.storage.postgres.r03_formal_sector import (
    R03FormalSectorPgError,
    read_r03_overlap_day,
    read_r03_previous_lifecycle_states,
    read_r03_rotation_recovery_states,
    write_r03_formal_market_breadth,
    write_r03_loo_support_fact,
    write_r03_native_day_facts,
    write_r03_overlap_results,
    write_r03_rotation_day,
)

R03_CONTRACT_VERSION = "R03_FORMAL_SECTOR_V1"


def _strict_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text[0] in "+-":
            if len(text) == 1 or not text[1:].isdigit():
                return None
        elif not text.isdigit():
            return None
        return int(text)
    from decimal import Decimal

    if isinstance(value, Decimal):
        if not value.is_finite() or value != value.to_integral_value():
            return None
        return int(value)
    return None


def _frac_from_pair(num: Any, den: Any, quality: Any) -> Fraction | None:
    if quality != "OBSERVED" or num is None or den is None:
        return None
    num_int = _strict_int(num)
    den_int = _strict_int(den)
    if num_int is None or den_int is None or den_int <= 0:
        return None
    return Fraction(num_int, den_int)


def _load_endpoint_observations(
    cur: Any,
    *,
    session: date,
    source_capture_id: int,
    profile_revision: str | None,
    symbols: Sequence[str],
    price_coordinate: str = "FORWARD_ADJUSTED",
    universe_version: str | None = None,
) -> dict[str, dict[str, Any]]:
    if not symbols:
        return {}
    sym_list = sorted(set(symbols))
    cur.execute(
        """
        SELECT source_symbol, bar_status, raw_close, raw_amount, raw_amount_quality,
               amount_quality, raw_tradestatus_value, raw_tradestatus_quality,
               qfq_bar_status, qfq_close, qfq_price_quality
        FROM stock.source_raw_daily
        WHERE source_capture_id=%s AND session_date=%s AND source_symbol=ANY(%s)
        """,
        (source_capture_id, session, sym_list),
    )
    raw_by_sym = {r[0]: r for r in cur.fetchall()}

    prof_by_sym: dict[str, Any] = {}
    prof_ambiguous: set[str] = set()
    seed_by_sym: dict[str, str] = {}
    seed_ambiguous: set[str] = set()
    if profile_revision is not None:
        prof_sql = """
            SELECT source_symbol, ret1_exact_numerator, ret1_exact_denominator, ret1_quality,
                   ret5_exact_numerator, ret5_exact_denominator, ret5_quality,
                   ret20_exact_numerator, ret20_exact_denominator, ret20_quality,
                   amount_ratio20_exact_numerator, amount_ratio20_exact_denominator, amount_ratio20_quality,
                   ma20_exact_numerator, ma20_exact_denominator, ma20_quality
            FROM stock.source_symbol_price_profiles
            WHERE source_capture_id=%s AND target_session=%s AND profile_revision=%s
              AND price_coordinate=%s AND source_symbol=ANY(%s)
        """
        prof_params: list[Any] = [source_capture_id, session, profile_revision, price_coordinate, sym_list]
        if universe_version is not None:
            prof_sql += " AND cross_section_universe_version=%s"
            prof_params.append(universe_version)
        cur.execute(prof_sql, tuple(prof_params))
        for row in cur.fetchall():
            sym = row[0]
            if sym in prof_ambiguous:
                continue
            if sym in prof_by_sym:
                prof_ambiguous.add(sym)
                prof_by_sym.pop(sym, None)
            else:
                prof_by_sym[sym] = row

        seed_sql = """
            SELECT source_symbol, source_seed_state, universe_match_status
            FROM stock.source_pool_seed_day_facts
            WHERE source_capture_id=%s AND target_session=%s AND profile_revision=%s
              AND price_coordinate=%s AND source_symbol=ANY(%s)
        """
        seed_params: list[Any] = [source_capture_id, session, profile_revision, price_coordinate, sym_list]
        if universe_version is not None:
            seed_sql += " AND cross_section_universe_version=%s"
            seed_params.append(universe_version)
        cur.execute(seed_sql, tuple(seed_params))
        for sym, s_state, u_match in cur.fetchall():
            if sym in seed_ambiguous:
                continue
            if sym in seed_by_sym:
                seed_ambiguous.add(sym)
                seed_by_sym[sym] = "UNKNOWN"
            elif u_match != "EXACT_MATCH":
                seed_by_sym[sym] = "UNKNOWN"
            else:
                seed_by_sym[sym] = s_state

    cs_sql = """
        SELECT source_symbol, rps20, rps20_exact, rps20_quality
        FROM stock.source_cross_section_day_facts
        WHERE price_capture_id=%s AND target_session=%s AND price_coordinate=%s
          AND source_symbol=ANY(%s)
    """
    cs_params: list[Any] = [source_capture_id, session, price_coordinate, sym_list]
    if universe_version is not None:
        cs_sql += " AND universe_version=%s"
        cs_params.append(universe_version)
    cur.execute(cs_sql, tuple(cs_params))
    cs_by_sym: dict[str, Any] = {}
    cs_ambiguous: set[str] = set()
    for row in cur.fetchall():
        sym = row[0]
        if sym in cs_ambiguous:
            continue
        if sym in cs_by_sym:
            cs_ambiguous.add(sym)
            cs_by_sym.pop(sym, None)
        else:
            cs_by_sym[sym] = row

    obs: dict[str, dict[str, Any]] = {}
    for sym in sym_list:
        raw = raw_by_sym.get(sym)
        prof = prof_by_sym.get(sym)
        cs = cs_by_sym.get(sym)
        seed_state = seed_by_sym.get(sym, "UNKNOWN")

        if raw is None:
            q_state = "UNKNOWN"
            raw_close = None
            raw_amount = None
            qfq_close = None
        else:
            (
                bar_status,
                r_close,
                r_amt,
                r_amt_q,
                amt_q,
                r_ts,
                r_ts_q,
                qfq_bs,
                q_close,
                q_pq,
            ) = raw[1:]
            if (
                bar_status == "TARGET_PRESENT"
                and r_ts_q == "OBSERVED"
                and str(r_ts) == "1"
                and r_close is not None
                and Fraction(str(r_close)) > 0
            ):
                q_state = "TRUE"
                raw_close = Fraction(str(r_close))
            elif r_ts_q == "OBSERVED" and str(r_ts) == "0":
                q_state = "CONFIRMED_SUSPENSION"
                raw_close = None
            else:
                q_state = "UNKNOWN"
                raw_close = None
            eff_amt_q = amt_q or r_amt_q
            raw_amount = (
                Fraction(str(r_amt))
                if r_amt is not None
                and eff_amt_q in {"OBSERVED", "RAW_SOURCE_VALUE_OBSERVED"}
                and Fraction(str(r_amt)) >= 0
                else None
            )
            qfq_close = (
                Fraction(str(q_close))
                if qfq_bs == "TARGET_PRESENT"
                and q_pq == "OBSERVED"
                and q_close is not None
                and Fraction(str(q_close)) > 0
                else None
            )

        ret1 = ret5 = ret20 = amr20 = above_ma20 = None
        if prof is not None:
            ret1 = _frac_from_pair(prof[1], prof[2], prof[3])
            ret5 = _frac_from_pair(prof[4], prof[5], prof[6])
            ret20 = _frac_from_pair(prof[7], prof[8], prof[9])
            amr20 = _frac_from_pair(prof[10], prof[11], prof[12])
            ma20_val = _frac_from_pair(prof[13], prof[14], prof[15])
            if ma20_val is not None and qfq_close is not None:
                above_ma20 = qfq_close > ma20_val

        rps20 = None
        if cs is not None and cs[3] == "OBSERVED":
            if cs[2] is not None:
                rps20 = Fraction(str(cs[2]))
            elif cs[1] is not None:
                rps20 = Fraction(str(cs[1]))
        obs[sym] = {
            "actual_quote_state": q_state,
            "raw_close": raw_close,
            "qfq_close": qfq_close,
            "raw_amount": raw_amount,
            "ret1": ret1,
            "ret5": ret5,
            "ret20": ret20,
            "above_ma20": above_ma20,
            "amr20": amr20,
            "seed_state": seed_state if seed_state in {"TRUE", "FALSE"} else "UNKNOWN",
            "rps20": rps20,
        }
    return obs


def _load_snapshot_boards(
    cur: Any,
    snap_map: Mapping[str, int] | None,
) -> tuple[list[tuple[str, str, list[MemberSlot], bool]], set[str], dict[str, str | None]]:
    boards_meta: list[tuple[str, str, list[MemberSlot], bool]] = []
    all_symbols: set[str] = set()
    primary_industry: dict[str, str | None] = {}
    industry_conflicts: set[str] = set()
    if not snap_map:
        return boards_meta, all_symbols, primary_industry

    for kind in ("INDUSTRY", "CONCEPT"):
        snap_id = snap_map.get(kind)
        if snap_id is None:
            continue
        cur.execute(
            """
            SELECT c.board_native_id, m.source_symbol, m.membership_quality
            FROM stock.tdx_sector_catalog c
            LEFT JOIN stock.tdx_sector_members m ON m.catalog_id = c.id
            WHERE c.snapshot_id=%s AND c.board_kind=%s
            ORDER BY c.board_native_id, m.source_record_ordinal, m.source_symbol
            """,
            (int(snap_id), kind),
        )
        by_board: dict[str, list[MemberSlot]] = {}
        seen_slots: set[tuple[str, str]] = set()
        for bid, sym, mq in cur.fetchall():
            bid_str = str(bid)
            by_board.setdefault(bid_str, [])
            if sym is None:
                continue
            if (bid_str, sym) in seen_slots:
                continue
            seen_slots.add((bid_str, sym))
            mapped_eid = sym if mq != "UNMAPPED" else None
            slot = MemberSlot(slot_id=f"{kind}:{bid_str}:{sym}", entity_id=mapped_eid, symbol=sym)
            by_board[bid_str].append(slot)
            if mapped_eid is not None:
                all_symbols.add(mapped_eid)
                if kind == "INDUSTRY":
                    if mapped_eid in industry_conflicts:
                        continue
                    if mapped_eid in primary_industry and primary_industry[mapped_eid] != bid_str:
                        primary_industry[mapped_eid] = None
                        industry_conflicts.add(mapped_eid)
                    else:
                        primary_industry[mapped_eid] = bid_str
        for bid_str, slots in sorted(by_board.items()):
            has_unmapped = any(s.entity_id is None for s in slots)
            boards_meta.append((kind, bid_str, slots, not has_unmapped))
    return boards_meta, all_symbols, primary_industry


def _resolve_session_universe_members(
    cur: Any,
    *,
    session: date,
    price_capture_id: int,
    price_coordinate: str = "FORWARD_ADJUSTED",
    universe_version: str | None = None,
) -> tuple[str | None, set[str]]:
    """按日期与捕获解析日期有效 Research Universe 版本及全部成员（含无归属股票）；多版本未指定时拒绝。"""
    if universe_version is not None:
        cur.execute(
            """
            SELECT DISTINCT source_symbol
            FROM stock.source_cross_section_day_facts
            WHERE price_capture_id=%s AND target_session=%s
              AND price_coordinate=%s AND universe_version=%s
            """,
            (price_capture_id, session, price_coordinate, universe_version),
        )
        return universe_version, {str(r[0]) for r in cur.fetchall() if r[0]}
    cur.execute(
        """
        SELECT DISTINCT universe_version
        FROM stock.source_cross_section_day_facts
        WHERE price_capture_id=%s AND target_session=%s AND price_coordinate=%s
          AND universe_version IS NOT NULL
        ORDER BY universe_version
        """,
        (price_capture_id, session, price_coordinate),
    )
    versions = [str(r[0]) for r in cur.fetchall() if r[0]]
    if not versions:
        return None, set()
    if len(versions) > 1:
        raise R03FormalSectorPgError(
            "R03_UNIVERSE_VERSION_AMBIGUOUS",
            f"交易日 {session} 捕获 {price_capture_id} 存在多个 universe_version({versions})，拒绝静默选首行。",
        )
    resolved_ver = versions[0]
    cur.execute(
        """
        SELECT DISTINCT source_symbol
        FROM stock.source_cross_section_day_facts
        WHERE price_capture_id=%s AND target_session=%s
          AND price_coordinate=%s AND universe_version=%s
        """,
        (price_capture_id, session, price_coordinate, resolved_ver),
    )
    return resolved_ver, {str(r[0]) for r in cur.fetchall() if r[0]}


def assemble_r03_session_inputs(
    config: Mapping[str, Any],
    *,
    target_session: date,
    t1_session: date | None,
    t3_session: date | None,
    source_capture_id: int,
    profile_revision: str,
    snapshot_ids: Mapping[str, int],
    snapshot_ids_by_session: Mapping[str, Mapping[str, int]] | None = None,
    source_capture_id_t1: int | None = None,
    source_capture_id_t3: int | None = None,
    profile_revision_t1: str | None = None,
    profile_revision_t3: str | None = None,
    universe_version: str | None = None,
    universe_version_t1: str | None = None,
    universe_version_t3: str | None = None,
    extra_research_entities: Sequence[str] | None = None,
) -> dict[str, Any]:
    """从现役 PG 表组装 R03 板块三端独立输入、成员槽位、日期有效 Universe 与 LOO 目标列表（只读事务）。"""
    cap_t = int(source_capture_id)
    cap_t1 = int(source_capture_id_t1) if source_capture_id_t1 is not None else cap_t
    cap_t3 = int(source_capture_id_t3) if source_capture_id_t3 is not None else cap_t
    prof_t = str(profile_revision)
    prof_t1 = str(profile_revision_t1) if profile_revision_t1 is not None else None
    prof_t3 = str(profile_revision_t3) if profile_revision_t3 is not None else None

    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _conn,
        cur,
        _,
    ):
        snaps_t = (snapshot_ids_by_session or {}).get("t", snapshot_ids)
        snaps_t1 = (snapshot_ids_by_session or {}).get("t1", snapshot_ids) if t1_session is not None else None
        snaps_t3 = (snapshot_ids_by_session or {}).get("t3", snapshot_ids) if t3_session is not None else None

        meta_t, syms_t, primary_industry = _load_snapshot_boards(cur, snaps_t)
        meta_t1, syms_t1, _ = _load_snapshot_boards(cur, snaps_t1)
        meta_t3, syms_t3, _ = _load_snapshot_boards(cur, snaps_t3)

        u_ver_t, u_syms_t = _resolve_session_universe_members(
            cur, session=target_session, price_capture_id=cap_t, universe_version=universe_version
        )
        u_ver_t1, u_syms_t1 = (
            _resolve_session_universe_members(
                cur,
                session=t1_session,
                price_capture_id=cap_t1,
                universe_version=universe_version_t1 if universe_version_t1 is not None else universe_version,
            )
            if t1_session is not None
            else (None, set())
        )
        u_ver_t3, u_syms_t3 = (
            _resolve_session_universe_members(
                cur,
                session=t3_session,
                price_capture_id=cap_t3,
                universe_version=universe_version_t3 if universe_version_t3 is not None else universe_version,
            )
            if t3_session is not None
            else (None, set())
        )

        all_entities = set(syms_t) | set(syms_t1) | set(syms_t3) | set(u_syms_t) | set(u_syms_t1) | set(u_syms_t3)
        if extra_research_entities:
            all_entities.update(str(e) for e in extra_research_entities if e)
        sym_list = sorted(all_entities)

        obs_t = _load_endpoint_observations(
            cur,
            session=target_session,
            source_capture_id=cap_t,
            profile_revision=prof_t,
            symbols=sym_list,
            universe_version=u_ver_t,
        )
        obs_t1 = (
            _load_endpoint_observations(
                cur,
                session=t1_session,
                source_capture_id=cap_t1,
                profile_revision=prof_t1,
                symbols=sym_list,
                universe_version=u_ver_t1,
            )
            if t1_session is not None
            else {}
        )
        obs_t3 = (
            _load_endpoint_observations(
                cur,
                session=t3_session,
                source_capture_id=cap_t3,
                profile_revision=prof_t3,
                symbols=sym_list,
                universe_version=u_ver_t3,
            )
            if t3_session is not None
            else {}
        )

        boards_t = [
            {
                "board_id": bid,
                "board_kind": kind,
                "members": slots,
                "observations": obs_t,
                "coordinate_ready": True,
                "membership_complete": m_complete,
            }
            for kind, bid, slots, m_complete in meta_t
        ]
        boards_t1 = (
            {
                (kind, bid): {
                    "board_id": bid,
                    "board_kind": kind,
                    "members": slots,
                    "observations": obs_t1,
                    "coordinate_ready": True,
                    "membership_complete": m_complete,
                }
                for kind, bid, slots, m_complete in meta_t1
            }
            if t1_session is not None
            else {}
        )
        boards_t3 = (
            {
                (kind, bid): {
                    "board_id": bid,
                    "board_kind": kind,
                    "members": slots,
                    "observations": obs_t3,
                    "coordinate_ready": True,
                    "membership_complete": m_complete,
                }
                for kind, bid, slots, m_complete in meta_t3
            }
            if t3_session is not None
            else {}
        )
        present_kinds = ("INDUSTRY", "CONCEPT")
        target_entities = sorted(u_syms_t) if u_syms_t else sorted(set(extra_research_entities or ()))
        loo_targets = [
            {
                "entity_id": eid,
                "primary_industry_id": primary_industry.get(eid),
                "board_kind": bk,
            }
            for eid in target_entities
            for bk in present_kinds
        ]
        universe_ret1_t = {eid: obs_t.get(eid, {}).get("ret1") for eid in sorted(u_syms_t)}
        universe_ret1_t3 = {eid: obs_t3.get(eid, {}).get("ret1") for eid in sorted(u_syms_t3)}
        return {
            "boards_t": boards_t,
            "boards_t1": boards_t1,
            "boards_t3": boards_t3,
            "loo_targets": loo_targets,
            "universe_version_t": u_ver_t,
            "universe_version_t3": u_ver_t3,
            "universe_ret1_t": universe_ret1_t,
            "universe_ret1_t3": universe_ret1_t3,
        }


def _extract_rotation_inputs(
    board_t: Mapping[str, Any],
    board_t1: Mapping[str, Any] | None,
    prev_basket: FrozenRotationBasket | None = None,
) -> dict[str, Any]:
    obs_t = board_t["observations"]
    obs_t1 = board_t1["observations"] if board_t1 is not None else {}
    t1_entities = {
        s.entity_id
        for s in (board_t1["members"] if board_t1 is not None else board_t["members"])
        if s.entity_id is not None
    }
    t_entities = {s.entity_id for s in board_t["members"] if s.entity_id is not None} | t1_entities
    if prev_basket is not None:
        t_entities.update(prev_basket.members)
        t1_entities.update(prev_basket.members)
    prices_t = {eid: obs_t.get(eid, {}).get("qfq_close") for eid in sorted(t_entities)}
    prices_t1 = {eid: obs_t1.get(eid, {}).get("qfq_close") for eid in sorted(t1_entities)}
    seed_t = {eid: obs_t.get(eid, {}).get("seed_state", "UNKNOWN") for eid in sorted(t_entities)}
    seed_t1 = {eid: obs_t1.get(eid, {}).get("seed_state", "UNKNOWN") for eid in sorted(t1_entities)}
    ret1_t = {eid: obs_t.get(eid, {}).get("ret1") for eid in sorted(t_entities)}
    rps20_t = {eid: obs_t.get(eid, {}).get("rps20") for eid in sorted(t_entities)}
    rps20_t1 = {eid: obs_t1.get(eid, {}).get("rps20") for eid in sorted(t1_entities)}
    return {
        "prices_t": prices_t,
        "prices_t1": prices_t1,
        "seed_t": seed_t,
        "seed_t1": seed_t1,
        "ret1_t": ret1_t,
        "rps20_t": rps20_t,
        "rps20_t1": rps20_t1,
    }


def run_r03_formal_sector_day(
    config: Mapping[str, Any],
    *,
    trade_date: date,
    scope_key: str,
    member_version: str,
    boards_t: Sequence[Mapping[str, Any]],
    boards_t1: Mapping[Any, Mapping[str, Any]],
    boards_t3: Mapping[Any, Mapping[str, Any]],
    previous_states: Mapping[Any, Mapping[str, Any]] | None = None,
    previous_clusters: Sequence[Mapping[str, Any]] | None = None,
    rotation_recovery: Mapping[tuple[str, str], Mapping[str, Any]] | None = None,
    loo_targets: Sequence[Mapping[str, Any]] | None = None,
    modules: set[str] | None = None,
    allow_revision: bool = False,
) -> dict[str, Any]:
    """执行单日 R03 正式 Native、资格、前态、Rotation、Overlap 与 LOO 离线生产并幂等落库。"""
    active_modules = modules or {
        "R03_SECTOR_NATIVE",
        "R03_SECTOR_ROTATION",
        "R03_SECTOR_OVERLAP",
        "R03_SECTOR_LOO",
    }

    native_rows = compute_sector_native_day(
        boards_t=boards_t,
        boards_t1=boards_t1,
        boards_t3=boards_t3,
        previous_states=previous_states,
    )
    native_pg = {"inserted": 0, "updated": 0, "skipped": 0}
    if "R03_SECTOR_NATIVE" in active_modules:
        native_pg = write_r03_native_day_facts(
            config,
            trade_date=trade_date,
            scope_key=scope_key,
            member_version=member_version,
            contract_version=R03_CONTRACT_VERSION,
            rows=native_rows,
            allow_revision=allow_revision,
        )

    rotation_pg = {
        "episodes_inserted": 0,
        "states_inserted": 0,
        "states_updated": 0,
        "states_skipped": 0,
        "board_days_inserted": 0,
        "board_days_updated": 0,
        "board_days_skipped": 0,
    }
    expected_rotation_rows = 0
    if "R03_SECTOR_ROTATION" in active_modules:
        t_by_key = {(str(b["board_kind"]), str(b["board_id"])): b for b in boards_t}
        t1_by_key = {(str(b["board_kind"]), str(b["board_id"])): b for b in boards_t1.values()}
        rot_records: list[dict[str, Any]] = []
        for nr in native_rows:
            bkey = (str(nr["board_kind"]), str(nr["board_id"]))
            bt = t_by_key[bkey]
            bt1 = t1_by_key.get(bkey)
            rec_prev = (rotation_recovery or {}).get(bkey)
            missing_prev_session = bool(rec_prev.get("missing_previous_session", False)) if rec_prev else False
            prev_ep_pg_id = rec_prev.get("episode_pg_id") if rec_prev else None
            prev_basket: FrozenRotationBasket | None = (
                rec_prev.get("basket") if rec_prev else None
            )
            prev_state_dict = rec_prev.get("state_dict") if rec_prev else None
            prev_d2 = None
            if previous_states:
                ps = previous_states.get(bkey) or previous_states.get(nr["board_id"])
                if ps is not None:
                    prev_d2 = ps.get("effective_maturity")

            pulse_raw = evaluate_pulse_raw(
                dq5=nr.get("dq5"),
                breadth_delta1=nr.get("breadth_delta1"),
            )
            rot_in = _extract_rotation_inputs(bt, bt1, prev_basket=prev_basket)
            active_prev = (
                prev_state_dict is not None
                and prev_state_dict.get("last_known_state", prev_state_dict.get("state"))
                not in {None, "NONE", "FAILED", "OUT"}
            )
            basket_metrics = None
            if active_prev and prev_basket is not None:
                basket_metrics = evaluate_basket_metrics(
                    prev_basket,
                    prices_u=rot_in["prices_t"],
                    seed_u=rot_in["seed_t"],
                    seed_prev=rot_in["seed_t1"],
                    rps20_u=rot_in["rps20_t"],
                    breadth_delta1=nr.get("breadth_delta1"),
                    top1_concentration=nr["concentration"].get("top1_concentration"),
                )

            contract_ok = bool(bt.get("coordinate_ready", True)) and not missing_prev_session
            st_res = step_rotation_state_machine(
                contract_valid=contract_ok,
                previous_episode_state=prev_state_dict,
                t_minus_1_d2_maturity=prev_d2 if active_prev else "NONE",
                dq5=nr.get("dq5"),
                breadth_delta1=nr.get("breadth_delta1"),
                pulse_raw=pulse_raw,
                basket_metrics=basket_metrics,
            )
            if missing_prev_session:
                st_res = {
                    **st_res,
                    "state": "UNKNOWN",
                    "reason": "ROTATION_MISSING_PREVIOUS_SESSION",
                }

            new_basket = None
            parent_ep_id = None
            if st_res["state"] == "PULSE" and not active_prev:
                prev_lk = (
                    prev_state_dict.get("last_known_state")
                    if prev_state_dict is not None
                    else None
                )
                if bt1 is None:
                    st_res = {
                        **st_res,
                        "state": "UNKNOWN",
                        "last_known_state": prev_lk,
                        "reason": "BIRTH_T1_MISSING",
                    }
                elif not bt1.get("membership_complete", True):
                    st_res = {
                        **st_res,
                        "state": "UNKNOWN",
                        "last_known_state": prev_lk,
                        "reason": "BIRTH_MEMBERSHIP_INCOMPLETE",
                    }
                else:
                    members_t1 = [
                        s.entity_id for s in bt1["members"] if s.entity_id is not None
                    ]
                    new_basket, err_reason = create_frozen_basket(
                        episode_id=f"{nr['board_kind']}:{nr['board_id']}:{trade_date.isoformat()}",
                        board_id=str(nr["board_id"]),
                        board_kind=str(nr["board_kind"]),
                        t0_session=trade_date.isoformat(),
                        members_t0_minus_1=members_t1,
                        prices_t0_minus_1=rot_in["prices_t1"],
                        prices_t0=rot_in["prices_t"],
                        seed_t0=rot_in["seed_t"],
                        ret1_t0=rot_in["ret1_t"],
                        rps20_t0_minus_1=rot_in["rps20_t1"],
                        left_censored=(prev_state_dict is None),
                    )
                    if new_basket is None:
                        st_res = {
                            **st_res,
                            "state": "UNKNOWN",
                            "last_known_state": prev_lk,
                            "reason": err_reason or "BIRTH_BASKET_INVALID",
                        }
                    else:
                        parent_ep_id = prev_ep_pg_id
                        prev_ep_pg_id = None
                        basket_metrics = evaluate_basket_metrics(
                            new_basket,
                            prices_u=rot_in["prices_t"],
                            seed_u=rot_in["seed_t"],
                            seed_prev=rot_in["seed_t1"],
                            rps20_u=rot_in["rps20_t"],
                            breadth_delta1=nr.get("breadth_delta1"),
                            top1_concentration=nr["concentration"].get("top1_concentration"),
                        )

            rot_records.append(
                {
                    "board_kind": str(nr["board_kind"]),
                    "board_id": str(nr["board_id"]),
                    "pulse_raw": pulse_raw,
                    "episode_pg_id": prev_ep_pg_id,
                    "parent_episode_id": parent_ep_id,
                    "basket": new_basket,
                    "state_result": st_res,
                    "basket_metrics": basket_metrics,
                }
            )
        expected_rotation_rows = len(rot_records)
        if rot_records:
            rotation_pg = write_r03_rotation_day(
                config,
                trade_date=trade_date,
                scope_key=scope_key,
                member_version=member_version,
                contract_version=R03_CONTRACT_VERSION,
                records=rot_records,
                allow_revision=allow_revision,
            )

    overlap_pg = {
        "clusters_inserted": 0,
        "clusters_updated": 0,
        "clusters_skipped": 0,
        "board_facts_inserted": 0,
        "board_facts_updated": 0,
        "board_facts_skipped": 0,
    }
    overlap_clusters_count = 0
    if "R03_SECTOR_OVERLAP" in active_modules:
        priority_rows = [r["priority"] for r in native_rows]
        overlap = compute_sector_overlap(
            boards=native_rows,
            priority_rows=priority_rows,
            previous_clusters=previous_clusters,
        )
        overlap_clusters_count = len(overlap["clusters"])
        overlap_pg = write_r03_overlap_results(
            config,
            trade_date=trade_date,
            scope_key=scope_key,
            member_version=member_version,
            contract_version=R03_CONTRACT_VERSION,
            overlap=overlap,
            native_rows=native_rows,
            allow_revision=allow_revision,
        )

    loo_target_list = list(loo_targets or ())
    expected_loo_rows = (
        len({(str(t["entity_id"]), str(t.get("board_kind", "CONCEPT"))) for t in loo_target_list})
        if "R03_SECTOR_LOO" in active_modules
        else 0
    )
    loo_written = 0
    if "R03_SECTOR_LOO" in active_modules:
        for target in loo_target_list:
            bk = str(target.get("board_kind", "CONCEPT"))
            loo_res = compute_sector_loo_and_support(
                target_entity_id=str(target["entity_id"]),
                boards_t=boards_t,
                boards_t1=boards_t1,
                boards_t3=boards_t3,
                primary_industry_id=target.get("primary_industry_id"),
                board_kind=bk,
            )
            res = write_r03_loo_support_fact(
                config,
                trade_date=trade_date,
                board_kind=bk,
                scope_key=scope_key,
                member_version=member_version,
                contract_version=R03_CONTRACT_VERSION,
                loo_result=loo_res,
                allow_revision=allow_revision,
            )
            loo_written += res["inserted"] + res["updated"]

    return {
        "trade_date": trade_date.isoformat(),
        "scope_key": scope_key,
        "member_version": member_version,
        "contract_version": R03_CONTRACT_VERSION,
        "native_count": len(native_rows),
        "native_pg": native_pg,
        "rotation_pg": rotation_pg,
        "overlap_clusters_count": overlap_clusters_count,
        "overlap_pg": overlap_pg,
        "loo_written": loo_written,
        "expected_rows": {
            "R03_SECTOR_NATIVE": len(boards_t) if "R03_SECTOR_NATIVE" in active_modules else 0,
            "R03_SECTOR_ROTATION": expected_rotation_rows if "R03_SECTOR_ROTATION" in active_modules else 0,
            "R03_SECTOR_OVERLAP": overlap_clusters_count if "R03_SECTOR_OVERLAP" in active_modules else 0,
            "R03_SECTOR_LOO": expected_loo_rows,
        },
    }


def produce_formal_market_breadth_for_session(
    config: Mapping[str, Any],
    *,
    target_session: date,
    t3_session: date | None,
    source_capture_id: int,
    profile_revision: str,
    profile_revision_t3: str | None = None,
    source_capture_id_t3: int | None = None,
    universe_version_t: str | None = None,
    universe_version_t3: str | None = None,
    scope_key: str | None = None,
    allow_revision: bool = False,
) -> dict[str, Any] | None:
    """独立生产并幂等持久化 R03 正式市场宽度（不依赖板块研究模块）。"""
    if t3_session is None:
        return None
    resolved_scope = scope_key or (
        f"BAOSTOCK_CAP_{source_capture_id}:PROF_{profile_revision}:NON_PIT"
    )
    cap_t = int(source_capture_id)
    cap_t3 = int(source_capture_id_t3) if source_capture_id_t3 is not None else cap_t
    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _conn,
        cur,
        _,
    ):
        u_ver_t, u_syms_t = _resolve_session_universe_members(
            cur, session=target_session, price_capture_id=cap_t, universe_version=universe_version_t
        )
        u_ver_t3, u_syms_t3 = _resolve_session_universe_members(
            cur, session=t3_session, price_capture_id=cap_t3, universe_version=universe_version_t3
        )
        if u_ver_t is None or u_ver_t3 is None:
            return None
        sym_list = sorted(u_syms_t | u_syms_t3)
        obs_t = _load_endpoint_observations(
            cur,
            session=target_session,
            source_capture_id=cap_t,
            profile_revision=profile_revision,
            symbols=sym_list,
            universe_version=u_ver_t,
        )
        obs_t3 = _load_endpoint_observations(
            cur,
            session=t3_session,
            source_capture_id=cap_t3,
            profile_revision=profile_revision_t3,
            symbols=sym_list,
            universe_version=u_ver_t3,
        )
    res = compute_formal_market_breadth_from_universes(
        universe_t={eid: obs_t.get(eid, {}).get("ret1") for eid in sorted(u_syms_t)},
        universe_t3={eid: obs_t3.get(eid, {}).get("ret1") for eid in sorted(u_syms_t3)},
        universe_version_t=str(u_ver_t),
        universe_version_t3=str(u_ver_t3),
    )
    write_r03_formal_market_breadth(
        config,
        trade_date=target_session,
        scope_key=resolved_scope,
        contract_version=R03_CONTRACT_VERSION,
        result=res,
        allow_revision=allow_revision,
    )
    return res


def run_r03_formal_sector_for_session(
    config: Mapping[str, Any],
    *,
    target_session: date,
    t1_session: date | None,
    t3_session: date | None,
    source_capture_id: int,
    profile_revision: str,
    snapshot_ids: Mapping[str, int],
    snapshot_ids_by_session: Mapping[str, Mapping[str, int]] | None = None,
    profile_revision_t1: str | None = None,
    profile_revision_t3: str | None = None,
    source_capture_id_t1: int | None = None,
    source_capture_id_t3: int | None = None,
    universe_version: str | None = None,
    universe_version_t1: str | None = None,
    universe_version_t3: str | None = None,
    scope_key: str | None = None,
    modules: set[str] | None = None,
    allow_revision: bool = False,
) -> dict[str, Any]:
    """正常业务入口：从 PG 读取组装全部上游三端独立输入并恢复前态后执行 R03 正式生产与正式市场宽度落库。"""
    resolved_scope = scope_key or (
        f"BAOSTOCK_CAP_{source_capture_id}:PROF_{profile_revision}:NON_PIT"
    )
    member_ver = (
        f"IND_{snapshot_ids.get('INDUSTRY', 0)}:CON_{snapshot_ids.get('CONCEPT', 0)}"
    )
    rot_recovery = (
        read_r03_rotation_recovery_states(
            config,
            previous_trade_date=t1_session,
            scope_key=resolved_scope,
            contract_version=R03_CONTRACT_VERSION,
        )
        if t1_session is not None
        else {}
    )
    active_frozen_entities = sorted(
        {
            eid
            for rec in rot_recovery.values()
            if rec.get("basket") is not None
            for eid in rec["basket"].members
        }
    )
    assembled = assemble_r03_session_inputs(
        config,
        target_session=target_session,
        t1_session=t1_session,
        t3_session=t3_session,
        source_capture_id=source_capture_id,
        profile_revision=profile_revision,
        snapshot_ids=snapshot_ids,
        snapshot_ids_by_session=snapshot_ids_by_session,
        source_capture_id_t1=source_capture_id_t1,
        source_capture_id_t3=source_capture_id_t3,
        profile_revision_t1=profile_revision_t1,
        profile_revision_t3=profile_revision_t3,
        universe_version=universe_version,
        universe_version_t1=universe_version_t1,
        universe_version_t3=universe_version_t3,
        extra_research_entities=active_frozen_entities,
    )
    prev_states = (
        read_r03_previous_lifecycle_states(
            config,
            previous_trade_date=t1_session,
            scope_key=resolved_scope,
            member_version=member_ver,
            contract_version=R03_CONTRACT_VERSION,
        )
        if t1_session is not None
        else {}
    )
    prev_clusters_res = (
        read_r03_overlap_day(
            config,
            trade_date=t1_session,
            scope_key=resolved_scope,
            member_version=member_ver,
            contract_version=R03_CONTRACT_VERSION,
        )
        if t1_session is not None
        else {"status": "NOT_COMPUTED", "clusters": []}
    )
    formal_market_breadth = None
    if (
        t3_session is not None
        and assembled.get("universe_version_t") is not None
        and assembled.get("universe_version_t3") is not None
    ):
        formal_market_breadth = compute_formal_market_breadth_from_universes(
            universe_t=assembled["universe_ret1_t"],
            universe_t3=assembled["universe_ret1_t3"],
            universe_version_t=str(assembled["universe_version_t"]),
            universe_version_t3=str(assembled["universe_version_t3"]),
        )
        write_r03_formal_market_breadth(
            config,
            trade_date=target_session,
            scope_key=resolved_scope,
            contract_version=R03_CONTRACT_VERSION,
            result=formal_market_breadth,
            allow_revision=allow_revision,
        )

    day_res = run_r03_formal_sector_day(
        config,
        trade_date=target_session,
        scope_key=resolved_scope,
        member_version=member_ver,
        boards_t=assembled["boards_t"],
        boards_t1=assembled["boards_t1"],
        boards_t3=assembled["boards_t3"],
        previous_states=prev_states,
        previous_clusters=prev_clusters_res.get("clusters", []),
        rotation_recovery=rot_recovery,
        loo_targets=assembled["loo_targets"],
        modules=modules,
        allow_revision=allow_revision,
    )
    day_res["formal_market_breadth"] = formal_market_breadth
    return day_res
