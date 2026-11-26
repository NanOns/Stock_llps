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
    write_r03_loo_day_batch,
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


def _is_valid_ohlc(o: Any, h: Any, l: Any, c: Any) -> bool:
    if o is None or h is None or l is None or c is None:
        return False
    fo, fh, fl, fc = (
        Fraction(str(o)),
        Fraction(str(h)),
        Fraction(str(l)),
        Fraction(str(c)),
    )
    if fo <= 0 or fh <= 0 or fl <= 0 or fc <= 0:
        return False
    return fh >= fo and fh >= fc and fl <= fo and fl <= fc and fh >= fl


def _check_capture_dual_coordinates(cur: Any, source_capture_id: int) -> bool:
    cur.execute(
        """
        SELECT price_coordinates
        FROM stock.source_captures
        WHERE capture_id=%s
        """,
        (source_capture_id,),
    )
    row = cur.fetchone()
    if row is not None and row[0] is not None:
        coords = {str(x) for x in row[0]}
        if {"RAW_UNADJUSTED", "FORWARD_ADJUSTED"}.issubset(coords):
            return True
    return False


def _check_captures_qfq_comparable(
    cur: Any,
    cap_a: int,
    cap_b: int,
    *,
    overlap_session: date | None,
    symbols: Sequence[str] | None = None,
) -> bool:
    """检查两个捕获的前复权坐标在共同端点会话与必要证券集合上是否价格重基一致（同捕获直接返回True）。"""
    if cap_a == cap_b:
        return True
    if overlap_session is None:
        return False
    sql = """
        SELECT a.source_symbol, a.qfq_close, b.qfq_close
        FROM stock.source_raw_daily a
        JOIN stock.source_raw_daily b
          ON b.source_symbol = a.source_symbol
         AND b.session_date = a.session_date
        WHERE a.source_capture_id = %s
          AND b.source_capture_id = %s
          AND a.session_date = %s
          AND a.qfq_bar_status = 'TARGET_PRESENT'
          AND b.qfq_bar_status = 'TARGET_PRESENT'
          AND a.qfq_price_quality = 'OBSERVED'
          AND b.qfq_price_quality = 'OBSERVED'
          AND a.qfq_close > 0
          AND b.qfq_close > 0
    """
    params: list[Any] = [cap_a, cap_b, overlap_session]
    required: set[str] | None = None
    if symbols is not None:
        required = {str(s) for s in symbols if s}
        if not required:
            return False
        sql += " AND a.source_symbol = ANY(%s)"
        params.append(sorted(required))
    cur.execute(sql, tuple(params))
    rows = cur.fetchall()
    if not rows:
        return False
    if required is not None:
        present = {str(r[0]) for r in rows}
        if present != required:
            return False
    return all(Fraction(str(r[1])) == Fraction(str(r[2])) for r in rows)


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
        SELECT source_symbol, bar_status, raw_open, raw_high, raw_low, raw_close,
               raw_amount, raw_amount_quality, amount_quality,
               raw_tradestatus_value, raw_tradestatus_quality,
               qfq_bar_status, qfq_open, qfq_high, qfq_low, qfq_close, qfq_price_quality
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
        prof_params: list[Any] = [
            source_capture_id,
            session,
            profile_revision,
            price_coordinate,
            sym_list,
        ]
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
            SELECT source_symbol, source_seed_state, universe_match_status,
                   cross_section_universe_version
            FROM stock.source_pool_seed_day_facts
            WHERE source_capture_id=%s AND target_session=%s AND profile_revision=%s
              AND price_coordinate=%s AND source_symbol=ANY(%s)
        """
        seed_params: list[Any] = [
            source_capture_id,
            session,
            profile_revision,
            price_coordinate,
            sym_list,
        ]
        if universe_version is not None:
            seed_sql += " AND cross_section_universe_version=%s"
            seed_params.append(universe_version)
        cur.execute(seed_sql, tuple(seed_params))
        for sym, s_state, u_match, _cs_u_ver in cur.fetchall():
            if universe_version is None:
                seed_by_sym[sym] = "UNKNOWN"
                continue
            if sym in seed_ambiguous:
                continue
            if sym in seed_by_sym:
                seed_ambiguous.add(sym)
                seed_by_sym[sym] = "UNKNOWN"
            elif u_match != "EXACT_MATCH":
                seed_by_sym[sym] = "UNKNOWN"
            else:
                seed_by_sym[sym] = s_state

    cs_by_sym: dict[str, Any] = {}
    cs_ambiguous: set[str] = set()
    if universe_version is not None:
        cs_sql = """
            SELECT source_symbol, rps20, rps20_exact, rps20_quality
            FROM stock.source_cross_section_day_facts
            WHERE price_capture_id=%s AND target_session=%s AND price_coordinate=%s
              AND universe_version=%s AND source_symbol=ANY(%s)
        """
        cur.execute(
            cs_sql,
            (source_capture_id, session, price_coordinate, universe_version, sym_list),
        )
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
                r_open,
                r_high,
                r_low,
                r_close,
                r_amt,
                r_amt_q,
                amt_q,
                r_ts,
                r_ts_q,
                qfq_bs,
                q_open,
                q_high,
                q_low,
                q_close,
                q_pq,
            ) = raw[1:]
            raw_ohlc_ok = (
                _is_valid_ohlc(r_open, r_high, r_low, r_close)
                if (r_open is not None and r_high is not None and r_low is not None)
                else (r_close is not None and Fraction(str(r_close)) > 0)
            )
            if (
                bar_status == "TARGET_PRESENT"
                and r_ts_q == "OBSERVED"
                and str(r_ts) == "1"
                and raw_ohlc_ok
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
            qfq_ohlc_ok = (
                _is_valid_ohlc(q_open, q_high, q_low, q_close)
                if (q_open is not None and q_high is not None and q_low is not None)
                else (q_close is not None and Fraction(str(q_close)) > 0)
            )
            qfq_close = (
                Fraction(str(q_close))
                if qfq_bs == "TARGET_PRESENT" and q_pq == "OBSERVED" and qfq_ohlc_ok
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
        last_actual_qfq_close = None
        quote_age: int | None = (
            0 if q_state == "TRUE" and qfq_close is not None else None
        )
        if q_state == "CONFIRMED_SUSPENSION":
            from stock_a.application.data_jobs import _load_official_sessions

            _cal_id, _cal_rev, off_sessions = _load_official_sessions(session)
            prev_official = [d for d in off_sessions if d < session][-20:]
            expected_desc = list(reversed(prev_official))
            cur.execute(
                """
                SELECT session_date, bar_status, raw_tradestatus_value, raw_tradestatus_quality,
                       qfq_bar_status, qfq_close, qfq_price_quality
                FROM stock.source_raw_daily
                WHERE source_capture_id=%s AND source_symbol=%s AND session_date < %s
                ORDER BY session_date DESC
                LIMIT 21
                """,
                (source_capture_id, sym, session),
            )
            hist_by_day = {r[0]: r for r in cur.fetchall()}
            for idx_age, exp_day in enumerate(expected_desc, start=1):
                h_row = hist_by_day.get(exp_day)
                if h_row is None:
                    # 官方会话存在真实缺行（非已证停牌），禁止跨缺日寻找旧价
                    break
                _h_day, h_bs, h_ts, h_ts_q, h_qbs, h_qc, h_qpq = h_row
                if (
                    h_bs == "TARGET_PRESENT"
                    and h_ts_q == "OBSERVED"
                    and str(h_ts) == "1"
                    and h_qbs == "TARGET_PRESENT"
                    and h_qpq == "OBSERVED"
                    and h_qc is not None
                    and Fraction(str(h_qc)) > 0
                ):
                    last_actual_qfq_close = Fraction(str(h_qc))
                    quote_age = idx_age
                    break
                if not (h_ts_q == "OBSERVED" and str(h_ts) == "0"):
                    # 区间存在非已证停牌的未知缺口，禁止估值
                    break
        obs[sym] = {
            "actual_quote_state": q_state,
            "raw_close": raw_close,
            "qfq_close": qfq_close,
            "last_actual_qfq_close": last_actual_qfq_close,
            "quote_age": quote_age,
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


def _load_known_basic_symbols(cur: Any, source_capture_id: int) -> set[str]:
    cur.execute(
        """
        SELECT source_symbol
        FROM stock.source_baostock_stock_basic
        WHERE source_capture_id=%s
        """,
        (int(source_capture_id),),
    )
    return {str(r[0]) for r in cur.fetchall() if r[0]}


def _load_snapshot_boards(
    cur: Any,
    snap_map: Mapping[str, int] | None,
    *,
    known_symbols: set[str] | None = None,
) -> tuple[
    list[tuple[str, str, list[MemberSlot], bool]], set[str], dict[str, str | None]
]:
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
            SELECT source_quality
            FROM stock.tdx_sector_source_snapshots
            WHERE snapshot_id=%s
            """,
            (int(snap_id),),
        )
        snap_row = cur.fetchone()
        snap_quality = (
            str(snap_row[0]) if snap_row and snap_row[0] is not None else None
        )
        quality_head = (
            snap_quality.split(";", 1)[0].strip().upper()
            if snap_quality is not None
            else ""
        )
        snapshot_complete = snap_row is not None and quality_head in {
            "COMPLETE",
            "COMPLETE_NON_PIT",
            "PARTIAL_NON_PIT",
            "VALID",
            "VERIFIED",
            "AVAILABLE",
            "PRESENT",
            "OK",
        }
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
            mapped_eid = (
                sym
                if (mq != "UNMAPPED" and (not known_symbols or sym in known_symbols))
                else None
            )
            slot = MemberSlot(
                slot_id=f"{kind}:{bid_str}:{sym}", entity_id=mapped_eid, symbol=sym
            )
            by_board[bid_str].append(slot)
            if mapped_eid is not None:
                all_symbols.add(mapped_eid)
                if kind == "INDUSTRY":
                    if mapped_eid in industry_conflicts:
                        continue
                    if (
                        mapped_eid in primary_industry
                        and primary_industry[mapped_eid] != bid_str
                    ):
                        primary_industry[mapped_eid] = None
                        industry_conflicts.add(mapped_eid)
                    else:
                        primary_industry[mapped_eid] = bid_str
        for bid_str, slots in sorted(by_board.items()):
            # 保留完整名单中的 UNMAPPED 槽位，不因单个成员未映射而直接判整份快照不完整
            boards_meta.append((kind, bid_str, slots, snapshot_complete))
    return boards_meta, all_symbols, primary_industry


def _resolve_session_universe_members(
    cur: Any,
    *,
    config: Mapping[str, Any] | None = None,
    session: date,
    price_capture_id: int,
    price_coordinate: str = "FORWARD_ADJUSTED",
    universe_version: str | None = None,
    universe_capture_id: int | None = None,
) -> tuple[str | None, set[str]]:
    """按日期与捕获解析日期有效 Research Universe 版本及全部成员（含无归属股票）；优先使用权威日期有效总体，多版本未指定时拒绝。"""
    if config is not None:
        from stock_a.storage.postgres.baostock_universe import (
            resolve_date_effective_universe,
        )

        dir_cap = universe_capture_id
        if dir_cap is None:
            cur.execute(
                """
                SELECT DISTINCT source_capture_id
                FROM stock.source_baostock_session_directory
                WHERE session_date=%s
                ORDER BY source_capture_id
                """,
                (session,),
            )
            dir_caps = [int(r[0]) for r in cur.fetchall() if r[0] is not None]
            if len(dir_caps) > 1:
                if int(price_capture_id) in dir_caps:
                    dir_cap = int(price_capture_id)
                else:
                    raise R03FormalSectorPgError(
                        "R03_UNIVERSE_CAPTURE_AMBIGUOUS",
                        f"交易日 {session} 存在多个 session_directory 捕获({dir_caps})，拒绝静默选首行。",
                    )
            else:
                dir_cap = dir_caps[0] if len(dir_caps) == 1 else None
        if dir_cap is not None:
            resolved_u = resolve_date_effective_universe(
                config,
                session_date=session,
                directory_capture_id=dir_cap,
                stock_basic_capture_id=dir_cap,
                research_eligible_only=True,
            )
            if resolved_u.get("status") == "AVAILABLE" and resolved_u.get(
                "universe_version"
            ):
                auth_ver = str(resolved_u["universe_version"])
                if universe_version is not None and universe_version != auth_ver:
                    raise R03FormalSectorPgError(
                        "R03_UNIVERSE_VERSION_MISMATCH",
                        f"交易日 {session} 指定 universe_version={universe_version} 与权威日期有效版本 {auth_ver} 不一致。",
                    )
                return auth_ver, {str(m) for m in resolved_u.get("members", ()) if m}

    # 权威日期有效 Universe 不可用时，禁止从 cross_section 现存行或虚构空总体推正式总体
    return None, set()


def assemble_r03_session_inputs(
    config: Mapping[str, Any],
    *,
    target_session: date,
    t1_session: date | None,
    t3_session: date | None,
    source_capture_id: int,
    profile_revision: str,
    snapshot_ids: Mapping[str, int],
    snapshot_ids_by_session: Mapping[str, Mapping[str, int] | None] | None = None,
    source_capture_id_t1: int | None = None,
    source_capture_id_t3: int | None = None,
    profile_revision_t1: str | None = None,
    profile_revision_t3: str | None = None,
    universe_version: str | None = None,
    universe_version_t1: str | None = None,
    universe_version_t3: str | None = None,
    universe_capture_id: int | None = None,
    universe_capture_id_t1: int | None = None,
    universe_capture_id_t3: int | None = None,
    strict_endpoints: bool = False,
    extra_research_entities: Sequence[str] | None = None,
    extra_entities_by_board: Mapping[tuple[str, str], Sequence[str]] | None = None,
) -> dict[str, Any]:
    """从现役 PG 表组装 R03 板块三端独立输入、成员槽位、日期有效 Universe 与 LOO 目标列表（只读事务）。"""
    cap_t = int(source_capture_id)
    cap_t1 = (
        int(source_capture_id_t1)
        if source_capture_id_t1 is not None
        else (None if strict_endpoints else cap_t)
    )
    cap_t3 = (
        int(source_capture_id_t3)
        if source_capture_id_t3 is not None
        else (None if strict_endpoints else cap_t)
    )
    prof_t = str(profile_revision)
    prof_t1 = str(profile_revision_t1) if profile_revision_t1 is not None else None
    prof_t3 = str(profile_revision_t3) if profile_revision_t3 is not None else None

    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _conn,
        cur,
        _,
    ):
        snaps_t = (
            snapshot_ids_by_session.get("t", snapshot_ids)
            if snapshot_ids_by_session is not None
            else snapshot_ids
        )
        snaps_t1 = (
            (
                snapshot_ids_by_session.get("t1")
                if snapshot_ids_by_session is not None
                else snapshot_ids
            )
            if t1_session is not None
            else None
        )
        snaps_t3 = (
            (
                snapshot_ids_by_session.get("t3")
                if snapshot_ids_by_session is not None
                else snapshot_ids
            )
            if t3_session is not None
            else None
        )

        known_t = _load_known_basic_symbols(cur, cap_t)
        known_t1 = (
            _load_known_basic_symbols(cur, cap_t1)
            if t1_session is not None and cap_t1 is not None
            else set()
        )
        known_t3 = (
            _load_known_basic_symbols(cur, cap_t3)
            if t3_session is not None and cap_t3 is not None
            else set()
        )

        meta_t, syms_t, primary_industry = _load_snapshot_boards(
            cur, snaps_t, known_symbols=known_t
        )
        meta_t1, syms_t1, _ = _load_snapshot_boards(
            cur, snaps_t1, known_symbols=known_t1
        )
        meta_t3, syms_t3, _ = _load_snapshot_boards(
            cur, snaps_t3, known_symbols=known_t3
        )

        u_ver_t, u_syms_t = _resolve_session_universe_members(
            cur,
            config=config,
            session=target_session,
            price_capture_id=cap_t,
            universe_version=universe_version,
            universe_capture_id=universe_capture_id,
        )
        u_ver_t1, u_syms_t1 = (
            _resolve_session_universe_members(
                cur,
                config=config,
                session=t1_session,
                price_capture_id=cap_t1,
                universe_version=universe_version_t1,
                universe_capture_id=universe_capture_id_t1,
            )
            if t1_session is not None and cap_t1 is not None
            else (None, set())
        )
        u_ver_t3, u_syms_t3 = (
            _resolve_session_universe_members(
                cur,
                config=config,
                session=t3_session,
                price_capture_id=cap_t3,
                universe_version=universe_version_t3,
                universe_capture_id=universe_capture_id_t3,
            )
            if t3_session is not None and cap_t3 is not None
            else (None, set())
        )

        all_entities = (
            set(syms_t)
            | set(syms_t1)
            | set(syms_t3)
            | set(u_syms_t)
            | set(u_syms_t1)
            | set(u_syms_t3)
        )
        if extra_research_entities:
            all_entities.update(str(e) for e in extra_research_entities if e)
        sym_list = sorted(all_entities)

        coord_t = _check_capture_dual_coordinates(cur, cap_t)
        coord_t1 = (
            _check_capture_dual_coordinates(cur, cap_t1)
            if t1_session is not None and cap_t1 is not None
            else False
        )
        coord_t3 = (
            _check_capture_dual_coordinates(cur, cap_t3)
            if t3_session is not None and cap_t3 is not None
            else False
        )
        qfq_comparable_t1 = (
            _check_captures_qfq_comparable(
                cur,
                cap_t,
                cap_t1,
                overlap_session=t1_session,
                symbols=sym_list,
            )
            if t1_session is not None and cap_t1 is not None and sym_list
            else (False if t1_session is not None else True)
        )
        t1_slots_by_board = {
            (str(kind), str(bid)): [
                s.entity_id for s in slots if s.entity_id is not None
            ]
            for kind, bid, slots, _mc in meta_t1
        }
        qfq_comparable_t1_by_board: dict[tuple[str, str], bool] = {}
        for kind, bid, _slots, _mc in meta_t:
            bk_key = (str(kind), str(bid))
            if t1_session is None:
                qfq_comparable_t1_by_board[bk_key] = True
                continue
            if cap_t1 is None:
                qfq_comparable_t1_by_board[bk_key] = False
                continue
            if cap_t == cap_t1:
                qfq_comparable_t1_by_board[bk_key] = True
                continue
            req_syms = set(t1_slots_by_board.get(bk_key, ()))
            if extra_entities_by_board and bk_key in extra_entities_by_board:
                req_syms.update(str(e) for e in extra_entities_by_board[bk_key] if e)
            if not req_syms:
                qfq_comparable_t1_by_board[bk_key] = True
            else:
                qfq_comparable_t1_by_board[bk_key] = _check_captures_qfq_comparable(
                    cur,
                    cap_t,
                    cap_t1,
                    overlap_session=t1_session,
                    symbols=sorted(req_syms),
                )
        qfq_comparable_t3 = (
            _check_captures_qfq_comparable(
                cur,
                cap_t,
                cap_t3,
                overlap_session=t3_session,
                symbols=sym_list,
            )
            if t3_session is not None and cap_t3 is not None and sym_list
            else (False if t3_session is not None else True)
        )

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
            if t1_session is not None and cap_t1 is not None
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
            if t3_session is not None and cap_t3 is not None
            else {}
        )

        boards_t = [
            {
                "board_id": bid,
                "board_kind": kind,
                "members": slots,
                "observations": obs_t,
                "coordinate_ready": coord_t,
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
                    "coordinate_ready": coord_t1,
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
                    "coordinate_ready": coord_t3,
                    "membership_complete": m_complete,
                }
                for kind, bid, slots, m_complete in meta_t3
            }
            if t3_session is not None
            else {}
        )
        present_kinds = ("INDUSTRY", "CONCEPT")
        # 仅使用真实日期有效 Research Universe 作为正式 LOO 目标（固定 B 额外实体仅用于估值）
        target_entities = sorted(u_syms_t)
        loo_targets = [
            {
                "entity_id": eid,
                "primary_industry_id": primary_industry.get(eid),
                "board_kind": bk,
            }
            for eid in target_entities
            for bk in present_kinds
        ]
        universe_ret1_t = {
            eid: obs_t.get(eid, {}).get("ret1") for eid in sorted(u_syms_t)
        }
        universe_ret1_t3 = {
            eid: obs_t3.get(eid, {}).get("ret1") for eid in sorted(u_syms_t3)
        }
        return {
            "boards_t": boards_t,
            "boards_t1": boards_t1,
            "boards_t3": boards_t3,
            "loo_targets": loo_targets,
            "universe_ready_t": u_ver_t is not None,
            "universe_version_t": u_ver_t,
            "universe_version_t1": u_ver_t1,
            "universe_version_t3": u_ver_t3,
            "universe_ret1_t": universe_ret1_t,
            "universe_ret1_t3": universe_ret1_t3,
            "qfq_comparable_t1": qfq_comparable_t1,
            "qfq_comparable_t1_by_board": qfq_comparable_t1_by_board,
            "qfq_comparable_t3": qfq_comparable_t3,
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
    t_entities = {
        s.entity_id for s in board_t["members"] if s.entity_id is not None
    } | t1_entities
    if prev_basket is not None:
        t_entities.update(prev_basket.members)
        t1_entities.update(prev_basket.members)
    # 出生基准严格只取 ACTUAL 行情收盘价
    prices_t = {
        eid: (
            obs_t[eid].get("qfq_close")
            if eid in obs_t and obs_t[eid].get("actual_quote_state", "TRUE") == "TRUE"
            else None
        )
        for eid in sorted(t_entities)
    }
    prices_t1 = {
        eid: (
            obs_t1[eid].get("qfq_close")
            if eid in obs_t1 and obs_t1[eid].get("actual_quote_state", "TRUE") == "TRUE"
            else None
        )
        for eid in sorted(t1_entities)
    }
    quote_states_t = {
        eid: str(obs_t.get(eid, {}).get("actual_quote_state", "UNKNOWN"))
        for eid in sorted(t_entities)
    }
    last_actual_quotes_t = {
        eid: (obs_t[eid]["last_actual_qfq_close"], int(obs_t[eid]["quote_age"]))
        for eid in sorted(t_entities)
        if obs_t.get(eid, {}).get("last_actual_qfq_close") is not None
        and obs_t.get(eid, {}).get("quote_age") is not None
    }
    seed_t = {
        eid: obs_t.get(eid, {}).get("seed_state", "UNKNOWN")
        for eid in sorted(t_entities)
    }
    seed_t1 = {
        eid: obs_t1.get(eid, {}).get("seed_state", "UNKNOWN")
        for eid in sorted(t1_entities)
    }
    ret1_t = {eid: obs_t.get(eid, {}).get("ret1") for eid in sorted(t_entities)}
    rps20_t = {eid: obs_t.get(eid, {}).get("rps20") for eid in sorted(t_entities)}
    rps20_t1 = {eid: obs_t1.get(eid, {}).get("rps20") for eid in sorted(t1_entities)}
    return {
        "prices_t": prices_t,
        "prices_t1": prices_t1,
        "quote_states_t": quote_states_t,
        "last_actual_quotes_t": last_actual_quotes_t,
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
    coordinate_comparable_t1: bool = True,
    coordinate_comparable_t1_by_board: Mapping[tuple[str, str], bool] | None = None,
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
        t1_by_key = {
            (str(b["board_kind"]), str(b["board_id"])): b for b in boards_t1.values()
        }
        rot_records: list[dict[str, Any]] = []
        for nr in native_rows:
            bkey = (str(nr["board_kind"]), str(nr["board_id"]))
            bt = t_by_key[bkey]
            bt1 = t1_by_key.get(bkey)
            rec_prev = (rotation_recovery or {}).get(bkey)
            missing_prev_session = (
                bool(rec_prev.get("missing_previous_session", False))
                if rec_prev
                else False
            )
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
            active_prev = prev_state_dict is not None and prev_state_dict.get(
                "last_known_state", prev_state_dict.get("state")
            ) not in {None, "NONE", "FAILED", "OUT", "REJECTED"}
            basket_metrics = None
            origin_comparable = (
                bool(rec_prev.get("origin_coordinate_comparable", True))
                if rec_prev
                else True
            )
            board_comp_t1 = (
                bool(coordinate_comparable_t1_by_board[bkey])
                if coordinate_comparable_t1_by_board is not None
                and bkey in coordinate_comparable_t1_by_board
                else bool(coordinate_comparable_t1)
            )
            contract_ok = (
                bool(bt.get("coordinate_ready", True))
                and board_comp_t1
                and origin_comparable
            )
            if (
                active_prev
                and prev_basket is not None
                and not missing_prev_session
                and contract_ok
            ):
                basket_metrics = evaluate_basket_metrics(
                    prev_basket,
                    prices_u=rot_in["prices_t"],
                    seed_u=rot_in["seed_t"],
                    seed_prev=rot_in["seed_t1"],
                    rps20_u=rot_in["rps20_t"],
                    breadth_delta1=nr.get("breadth_delta1"),
                    top1_concentration=nr["concentration"].get("top1_concentration"),
                    quote_states_u=rot_in["quote_states_t"],
                    last_actual_quotes_u=rot_in["last_actual_quotes_t"],
                )

            if missing_prev_session and prev_state_dict is not None:
                st_res = step_rotation_state_machine(
                    contract_valid=contract_ok,
                    previous_episode_state=prev_state_dict,
                    t_minus_1_d2_maturity=prev_d2 if active_prev else "NONE",
                    dq5=None,
                    breadth_delta1=None,
                    pulse_raw={
                        "state": "UNKNOWN",
                        "reason": "ROTATION_MISSING_PREVIOUS_SESSION",
                    },
                    basket_metrics=None,
                    input_missing_reason="ROTATION_MISSING_PREVIOUS_SESSION",
                )
            else:
                st_res = step_rotation_state_machine(
                    contract_valid=contract_ok,
                    previous_episode_state=prev_state_dict,
                    t_minus_1_d2_maturity=prev_d2 if active_prev else "NONE",
                    dq5=nr.get("dq5"),
                    breadth_delta1=nr.get("breadth_delta1"),
                    pulse_raw=pulse_raw,
                    basket_metrics=basket_metrics,
                )
            if not origin_comparable and st_res.get("state") == "REJECTED":
                st_res = {**st_res, "reason": "ROTATION_COORDINATE_INCOMPARABLE"}
            if (
                st_res.get("state") == "REJECTED"
                and st_res.get("preserve_prior")
                and prev_state_dict is not None
            ):
                prev_lk = prev_state_dict.get(
                    "last_known_state"
                ) or prev_state_dict.get("state")
                st_res = {
                    **st_res,
                    "last_known_state": prev_lk,
                    "stale": True,
                    "market_age": int(prev_state_dict.get("market_age", 0)),
                    "eval_age": int(prev_state_dict.get("eval_age", 0)),
                    "accepted_once": bool(prev_state_dict.get("accepted_once", False)),
                    "armed": bool(prev_state_dict.get("armed", False)),
                    "out_neg_streak": int(prev_state_dict.get("out_neg_streak", 0)),
                    "prev_mature_true": bool(
                        prev_state_dict.get("prev_mature_true", False)
                    ),
                    "dq5": prev_state_dict.get("dq5"),
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
                elif any(s.entity_id is None for s in bt1["members"]):
                    st_res = {
                        **st_res,
                        "state": "UNKNOWN",
                        "last_known_state": prev_lk,
                        "reason": "BIRTH_UNMAPPED_MEMBER",
                    }
                elif not board_comp_t1:
                    st_res = {
                        **st_res,
                        "state": "UNKNOWN",
                        "last_known_state": prev_lk,
                        "reason": "ROTATION_COORDINATE_INCOMPARABLE",
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
                            top1_concentration=nr["concentration"].get(
                                "top1_concentration"
                            ),
                            quote_states_u=rot_in["quote_states_t"],
                            last_actual_quotes_u=rot_in["last_actual_quotes_t"],
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
        seen_rot_keys = {(rec["board_kind"], rec["board_id"]) for rec in rot_records}
        for bkey, rec_prev in (rotation_recovery or {}).items():
            if bkey in seen_rot_keys:
                continue
            prev_sd = rec_prev.get("state_dict")
            if prev_sd is None:
                continue
            prev_lk = prev_sd.get("last_known_state", prev_sd.get("state"))
            if prev_lk in {None, "NONE", "FAILED", "OUT"}:
                continue
            # 整个板块缺目录时属于合法输入缺失 UNKNOWN，不走合同错误拒绝分支，保留原 last_known_state、accepted_once、market_age 与原篮子
            miss_res = step_rotation_state_machine(
                contract_valid=True,
                previous_episode_state=prev_sd,
                t_minus_1_d2_maturity="NONE",
                dq5=None,
                breadth_delta1=None,
                pulse_raw={"state": "UNKNOWN", "reason": "BOARD_CATALOG_MISSING"},
                basket_metrics=None,
                input_missing_reason="BOARD_CATALOG_MISSING",
            )
            rot_records.append(
                {
                    "board_kind": bkey[0],
                    "board_id": bkey[1],
                    "pulse_raw": {
                        "state": "UNKNOWN",
                        "reason": "BOARD_CATALOG_MISSING",
                    },
                    "episode_pg_id": rec_prev.get("episode_pg_id"),
                    "parent_episode_id": None,
                    "basket": None,
                    "state_result": miss_res,
                    "basket_metrics": None,
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
        len(
            {
                (str(t["entity_id"]), str(t.get("board_kind", "CONCEPT")))
                for t in loo_target_list
            }
        )
        if "R03_SECTOR_LOO" in active_modules
        else 0
    )
    loo_written = 0
    if "R03_SECTOR_LOO" in active_modules:
        loo_batch: list[tuple[str, Mapping[str, Any]]] = []
        for target in loo_target_list:
            bk = str(target.get("board_kind", "CONCEPT"))
            scoped_kind_boards = [b for b in boards_t if str(b["board_kind"]) == bk]
            kind_membership_known = (
                all(
                    bool(b.get("membership_complete", True)) for b in scoped_kind_boards
                )
                if scoped_kind_boards
                else False
            )
            loo_res = compute_sector_loo_and_support(
                target_entity_id=str(target["entity_id"]),
                boards_t=boards_t,
                boards_t1=boards_t1,
                boards_t3=boards_t3,
                primary_industry_id=target.get("primary_industry_id"),
                board_kind=bk,
                membership_known=kind_membership_known,
            )
            loo_batch.append((bk, loo_res))
        if loo_batch:
            res = write_r03_loo_day_batch(
                config,
                trade_date=trade_date,
                scope_key=scope_key,
                member_version=member_version,
                contract_version=R03_CONTRACT_VERSION,
                targets_and_results=loo_batch,
                allow_revision=allow_revision,
            )
            loo_written = res["inserted"] + res["updated"]

    expected_keys = {
        "R03_SECTOR_NATIVE": (
            {(str(b["board_kind"]), str(b["board_id"])) for b in boards_t}
            if "R03_SECTOR_NATIVE" in active_modules
            else set()
        ),
        "R03_SECTOR_ROTATION": (
            {(str(rec["board_kind"]), str(rec["board_id"])) for rec in rot_records}
            if "R03_SECTOR_ROTATION" in active_modules
            else set()
        ),
        "R03_SECTOR_OVERLAP": (
            {(str(c["board_kind"]), str(c["cluster_id"])) for c in overlap["clusters"]}
            if "R03_SECTOR_OVERLAP" in active_modules
            else set()
        ),
        "R03_SECTOR_LOO": (
            {
                (str(t["entity_id"]), str(t.get("board_kind", "CONCEPT")))
                for t in loo_target_list
            }
            if "R03_SECTOR_LOO" in active_modules
            else set()
        ),
    }

    expected_sub_keys: dict[str, dict[str, set[tuple[str, ...]]]] = {}
    if "R03_SECTOR_OVERLAP" in active_modules:
        ov_unique = overlap.get("unique_shares") or {}
        ov_cluster_boards = {
            (str(c["board_kind"]), str(mb))
            for c in overlap.get("clusters", ())
            for mb in c.get("member_board_ids", ())
        }
        expected_sub_keys["R03_SECTOR_OVERLAP"] = {
            "board_facts": {
                (str(nr["board_kind"]), str(nr["board_id"]))
                for nr in native_rows
                if (str(nr["board_kind"]), str(nr["board_id"])) in ov_unique
                or str(nr["board_id"]) in ov_unique
                or (str(nr["board_kind"]), str(nr["board_id"])) in ov_cluster_boards
            },
            "pair_facts": {
                (str(pf["board_kind"]), str(pf["board_id_a"]), str(pf["board_id_b"]))
                for pf in overlap.get("pairs", ())
            },
        }
    if "R03_SECTOR_LOO" in active_modules:
        expected_sub_keys["R03_SECTOR_LOO"] = {
            "candidates": {
                (str(res["target_entity_id"]), str(bk), str(cand["board_id"]))
                for bk, res in loo_batch
                for cand in res.get("loo_boards", ())
            }
        }

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
        "expected_keys": expected_keys,
        "expected_sub_keys": expected_sub_keys,
        "expected_rows": {
            "R03_SECTOR_NATIVE": len(boards_t)
            if "R03_SECTOR_NATIVE" in active_modules
            else 0,
            "R03_SECTOR_ROTATION": expected_rotation_rows
            if "R03_SECTOR_ROTATION" in active_modules
            else 0,
            "R03_SECTOR_OVERLAP": overlap_clusters_count
            if "R03_SECTOR_OVERLAP" in active_modules
            else 0,
            "R03_SECTOR_LOO": expected_loo_rows,
        },
    }


def _load_qfq_ret1_from_raw(
    cur: Any,
    *,
    session: date,
    prev_session: date | None,
    source_capture_id: int,
    symbols: Sequence[str],
) -> dict[str, Fraction | None]:
    """当 Profile 缺失时，直接从现役 source_raw_daily 的合法 QFQ 收盘价计算相邻官方会话 ret1。"""
    if prev_session is None or not symbols:
        return {}
    sym_list = sorted(set(symbols))
    cur.execute(
        """
        SELECT source_symbol, session_date, qfq_bar_status, qfq_close, qfq_price_quality,
               raw_tradestatus_value, raw_tradestatus_quality
        FROM stock.source_raw_daily
        WHERE source_capture_id=%s AND session_date=ANY(%s) AND source_symbol=ANY(%s)
        """,
        (source_capture_id, [prev_session, session], sym_list),
    )
    by_sym_day: dict[tuple[str, date], tuple[str, Any, str, Any, Any]] = {}
    for sym, s_day, q_bs, q_c, q_pq, r_ts, r_ts_q in cur.fetchall():
        by_sym_day[(str(sym), s_day)] = (str(q_bs), q_c, str(q_pq), r_ts, r_ts_q)
    out: dict[str, Fraction | None] = {}
    for sym in sym_list:
        curr = by_sym_day.get((sym, session))
        prev = by_sym_day.get((sym, prev_session))
        if curr is None or prev is None:
            out[sym] = None
            continue
        if (
            curr[0] == "TARGET_PRESENT"
            and curr[2] == "OBSERVED"
            and curr[1] is not None
            and curr[4] == "OBSERVED"
            and str(curr[3]) == "1"
            and prev[0] == "TARGET_PRESENT"
            and prev[2] == "OBSERVED"
            and prev[1] is not None
        ):
            c_val = Fraction(str(curr[1]))
            p_val = Fraction(str(prev[1]))
            if c_val > 0 and p_val > 0:
                out[sym] = (c_val - p_val) / p_val
                continue
        out[sym] = None
    return out


def produce_formal_market_breadth_for_session(
    config: Mapping[str, Any],
    *,
    target_session: date,
    t3_session: date | None,
    source_capture_id: int,
    profile_revision: str | None = None,
    profile_revision_t3: str | None = None,
    source_capture_id_t3: int | None = None,
    prev_session_t: date | None = None,
    prev_session_t3: date | None = None,
    universe_version_t: str | None = None,
    universe_version_t3: str | None = None,
    universe_capture_id: int | None = None,
    scope_key: str | None = None,
    allow_revision: bool = False,
) -> dict[str, Any]:
    """独立生产并幂等持久化 R03 正式市场宽度（不依赖板块研究模块；缺 Profile 时复用合法 QFQ ret1，缺端点时诚实保存 UNKNOWN）。"""
    resolved_scope = scope_key or (
        f"BAOSTOCK_CAP_{source_capture_id}:PROF_{profile_revision}:NON_PIT"
        if profile_revision is not None
        else f"BAOSTOCK_CAP_{source_capture_id}:MARKET_QFQ:NON_PIT"
    )
    cap_t = int(source_capture_id)
    cap_t3 = int(source_capture_id_t3) if source_capture_id_t3 is not None else cap_t
    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _conn,
        cur,
        _,
    ):
        u_ver_t, u_syms_t = _resolve_session_universe_members(
            cur,
            config=config,
            session=target_session,
            price_capture_id=cap_t,
            universe_version=universe_version_t,
            universe_capture_id=universe_capture_id,
        )
        if t3_session is None:
            res = {
                "status": "UNKNOWN",
                "reason": "MISSING_T3_SESSION",
                "universe_version_t": str(u_ver_t or "MISSING"),
                "universe_version_t3": "MISSING",
                "n_t": len(u_syms_t),
                "n_t3": 0,
                "n_common": 0,
                "n_eligible": 0,
                "unknown_count": 0,
                "exact": None,
                "breadth_value": None,
            }
        else:
            u_ver_t3, u_syms_t3 = _resolve_session_universe_members(
                cur,
                config=config,
                session=t3_session,
                price_capture_id=cap_t3,
                universe_version=universe_version_t3,
                universe_capture_id=universe_capture_id,
            )
            if u_ver_t is None or u_ver_t3 is None:
                res = {
                    "status": "UNKNOWN",
                    "reason": "MISSING_UNIVERSE_ENDPOINT",
                    "universe_version_t": str(u_ver_t or "MISSING"),
                    "universe_version_t3": str(u_ver_t3 or "MISSING"),
                    "n_t": len(u_syms_t),
                    "n_t3": len(u_syms_t3),
                    "n_common": 0,
                    "n_eligible": 0,
                    "unknown_count": 0,
                    "exact": None,
                    "breadth_value": None,
                }
            else:
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
                ret1_t = {
                    eid: obs_t.get(eid, {}).get("ret1") for eid in sorted(u_syms_t)
                }
                ret1_t3 = {
                    eid: obs_t3.get(eid, {}).get("ret1") for eid in sorted(u_syms_t3)
                }
                if (
                    any(v is None for v in ret1_t.values())
                    and prev_session_t is not None
                    and profile_revision is None
                ):
                    raw_ret1_t = _load_qfq_ret1_from_raw(
                        cur,
                        session=target_session,
                        prev_session=prev_session_t,
                        source_capture_id=cap_t,
                        symbols=sorted(u_syms_t),
                    )
                    for eid, val in raw_ret1_t.items():
                        if ret1_t.get(eid) is None and val is not None:
                            ret1_t[eid] = val
                if (
                    any(v is None for v in ret1_t3.values())
                    and prev_session_t3 is not None
                    and profile_revision_t3 is None
                ):
                    raw_ret1_t3 = _load_qfq_ret1_from_raw(
                        cur,
                        session=t3_session,
                        prev_session=prev_session_t3,
                        source_capture_id=cap_t3,
                        symbols=sorted(u_syms_t3),
                    )
                    for eid, val in raw_ret1_t3.items():
                        if ret1_t3.get(eid) is None and val is not None:
                            ret1_t3[eid] = val
                res = compute_formal_market_breadth_from_universes(
                    universe_t=ret1_t,
                    universe_t3=ret1_t3,
                    universe_version_t=str(u_ver_t),
                    universe_version_t3=str(u_ver_t3),
                )
    pg_res = write_r03_formal_market_breadth(
        config,
        trade_date=target_session,
        scope_key=resolved_scope,
        contract_version=R03_CONTRACT_VERSION,
        result=res,
        allow_revision=allow_revision,
    )
    return {**res, "scope_key": resolved_scope, "pg_result": pg_res}


def run_r03_formal_sector_for_session(
    config: Mapping[str, Any],
    *,
    target_session: date,
    t1_session: date | None,
    t3_session: date | None,
    source_capture_id: int,
    profile_revision: str,
    snapshot_ids: Mapping[str, int],
    snapshot_ids_by_session: Mapping[str, Mapping[str, int] | None] | None = None,
    profile_revision_t1: str | None = None,
    profile_revision_t3: str | None = None,
    source_capture_id_t1: int | None = None,
    source_capture_id_t3: int | None = None,
    universe_version: str | None = None,
    universe_version_t1: str | None = None,
    universe_version_t3: str | None = None,
    universe_capture_id: int | None = None,
    universe_capture_id_t1: int | None = None,
    universe_capture_id_t3: int | None = None,
    strict_endpoints: bool = False,
    scope_key: str | None = None,
    modules: set[str] | None = None,
    allow_revision: bool = False,
    prior_version_chain: Sequence[tuple[date, str, str]] | None = None,
) -> dict[str, Any]:
    """正常业务入口：从 PG 读取组装全部上游三端独立输入并恢复前态后执行 R03 正式生产。"""
    resolved_scope = scope_key or (
        f"BAOSTOCK_CAP_{source_capture_id}:PROF_{profile_revision}:NON_PIT"
    )
    member_ver = (
        f"IND_{snapshot_ids.get('INDUSTRY', 0)}:CON_{snapshot_ids.get('CONCEPT', 0)}"
    )
    prev_cap = int(source_capture_id_t1) if source_capture_id_t1 is not None else None
    prev_prof = profile_revision_t1
    prev_scope_key = (
        scope_key
        if scope_key is not None
        else (
            f"BAOSTOCK_CAP_{prev_cap}:PROF_{prev_prof}:NON_PIT"
            if prev_cap is not None and prev_prof is not None
            else None
        )
    )
    snaps_t1 = (
        snapshot_ids_by_session.get("t1")
        if snapshot_ids_by_session is not None
        else snapshot_ids
    )
    prev_member_ver = (
        f"IND_{snaps_t1.get('INDUSTRY', 0)}:CON_{snaps_t1.get('CONCEPT', 0)}"
        if isinstance(snaps_t1, Mapping)
        else None
    )
    rot_recovery = (
        read_r03_rotation_recovery_states(
            config,
            previous_trade_date=t1_session,
            scope_key=resolved_scope,
            contract_version=R03_CONTRACT_VERSION,
            previous_scope_key=prev_scope_key,
            member_version=member_ver,
            previous_member_version=prev_member_ver,
            prior_version_chain=prior_version_chain,
            allow_cross_version_recovery=False,
        )
        if t1_session is not None
        else {}
    )
    if rot_recovery:
        with source_raw._transaction(
            config, write=False, statement_timeout_ms=10000
        ) as (
            _conn,
            _cur_chk,
            _,
        ):
            for _bk_k, _rec in rot_recovery.items():
                _bask = _rec.get("basket")
                _ep_scope = _rec.get("episode_scope_key")
                if _bask is None or not _ep_scope:
                    continue
                _ep_cap = None
                for _part in str(_ep_scope).split(":"):
                    if (
                        _part.startswith("BAOSTOCK_CAP_")
                        and _part[len("BAOSTOCK_CAP_") :].isdigit()
                    ):
                        _ep_cap = int(_part[len("BAOSTOCK_CAP_") :])
                        break
                if _ep_cap is not None and _ep_cap != int(source_capture_id):
                    _t0_d = date.fromisoformat(_bask.t0_session)
                    _comp = _check_captures_qfq_comparable(
                        _cur_chk,
                        _ep_cap,
                        int(source_capture_id),
                        overlap_session=_t0_d,
                        symbols=list(_bask.members),
                    )
                    _rec["origin_coordinate_comparable"] = _comp

    extra_entities_by_board = {
        _bk_k: tuple(_rec["basket"].members)
        for _bk_k, _rec in rot_recovery.items()
        if _rec.get("basket") is not None
    }
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
        universe_capture_id=universe_capture_id,
        universe_capture_id_t1=universe_capture_id_t1,
        universe_capture_id_t3=universe_capture_id_t3,
        strict_endpoints=strict_endpoints,
        extra_research_entities=active_frozen_entities,
        extra_entities_by_board=extra_entities_by_board,
    )
    active_mods = modules or {
        "R03_SECTOR_NATIVE",
        "R03_SECTOR_ROTATION",
        "R03_SECTOR_OVERLAP",
        "R03_SECTOR_LOO",
    }
    if "R03_SECTOR_LOO" in active_mods and not assembled.get("universe_ready_t", False):
        raise R03FormalSectorPgError(
            "R03_UNIVERSE_MISSING",
            f"交易日 {target_session} 缺少权威日期有效 Research Universe，禁止将 LOO 空结果标记为已完成。",
        )
    prev_states = (
        read_r03_previous_lifecycle_states(
            config,
            previous_trade_date=t1_session,
            scope_key=resolved_scope,
            member_version=member_ver,
            contract_version=R03_CONTRACT_VERSION,
            previous_scope_key=prev_scope_key,
            previous_member_version=prev_member_ver,
        )
        if t1_session is not None
        and prev_scope_key is not None
        and prev_member_ver is not None
        else {}
    )
    prev_clusters_res = (
        read_r03_overlap_day(
            config,
            trade_date=t1_session,
            scope_key=prev_scope_key,
            member_version=prev_member_ver,
            contract_version=R03_CONTRACT_VERSION,
        )
        if t1_session is not None
        and prev_scope_key is not None
        and prev_member_ver is not None
        else {"status": "NOT_COMPUTED", "clusters": []}
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
        coordinate_comparable_t1=bool(assembled.get("qfq_comparable_t1", True)),
        coordinate_comparable_t1_by_board=assembled.get("qfq_comparable_t1_by_board"),
    )
    day_res["formal_market_breadth"] = None
    day_res["universe_version_t"] = assembled.get("universe_version_t")
    day_res["universe_version_t1"] = assembled.get("universe_version_t1")
    day_res["universe_version_t3"] = assembled.get("universe_version_t3")
    day_res["prev_scope_key"] = prev_scope_key
    day_res["prev_member_version"] = prev_member_ver
    return day_res
