"""全市场10项横截面因子正式离线生产链（GAP-02）：共享QFQ基础收益→日期有效总体→批量10因子与10端点落库。"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from decimal import Decimal
from fractions import Fraction
from typing import Any

from stock_a.domain import FieldValue, unknown
from stock_a.research.core import MarketSessionCalendar
from stock_a.research.cross_section import (
    CROSS_SECTION_CONTRACT_VERSION,
    PriceCoordinate,
    ReturnEndpoint,
    calculate_cross_section_batch,
)
from stock_a.research.entry_context.sessions import _load_calendar
from stock_a.storage.postgres.baostock_universe import resolve_date_effective_universe
from stock_a.storage.postgres.cross_section import persist_cross_section_day_facts
from stock_a.storage.postgres.source_raw import _transaction


_RANK_SPECS = (
    ("rps5_current", 5, 0),
    ("rps5_minus1", 5, 1),
    ("rps5_minus3", 5, 3),
    ("rps20_current", 20, 0),
    ("rps20_minus1", 20, 1),
    ("rps20_minus3", 20, 3),
)
_REL_SPECS = (
    ("rel_market_1", 1),
    ("rel_market_3", 3),
    ("rel_market_5", 5),
    ("rel_market_20", 20),
)


def _compute_return_fraction(
    close_end: Decimal | None,
    close_start: Decimal | None,
) -> FieldValue:
    """按 Core 精确有理数公式计算跨日收益 C[end] / C[start] - 1。"""
    if close_end is None or close_start is None:
        return unknown("QFQ_CLOSE_MISSING_OR_UNKNOWN")
    if close_end <= 0 or close_start <= 0:
        return unknown("QFQ_CLOSE_NON_POSITIVE")
    return FieldValue(Fraction(close_end) / Fraction(close_start) - Fraction(1))


def persist_source_cross_section_for_session(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int,
    directory_capture_id: int | None = None,
    stock_basic_capture_id: int | None = None,
) -> dict[str, Any]:
    """基于单一 paired BaoStock QFQ 捕获与日期有效总体，计算并落库目标日全市场10项横截面因子及10端点。"""
    if target_session > research_cutoff:
        raise ValueError("横截面目标会话不得晚于研究截止日")
    calendar_data, _, calendar_revision, calendar_sessions = _load_calendar(
        research_cutoff
    )
    if target_session not in calendar_sessions:
        raise ValueError("横截面目标会话不在官方日历中")
    calendar_id = str(calendar_data["calendar_id"])
    target_idx = calendar_sessions.index(target_session)
    calendar_obj = MarketSessionCalendar(
        calendar_id=calendar_id,
        revision=calendar_revision,
        sessions=tuple(calendar_sessions),
    )

    # 1. 解析目标日及所需历史端点日的日期有效总体（短窗不被长窗缺失阻断）
    needed_universe_offsets = (0, 1, 3, 5, 20)
    needed_price_offsets: set[int] = {0}
    for _, period, offset in _RANK_SPECS:
        needed_price_offsets.add(offset)
        needed_price_offsets.add(offset + period)
    for _, period in _REL_SPECS:
        needed_price_offsets.add(period)

    date_by_offset: dict[int, date] = {}
    for offset in sorted(needed_price_offsets | set(needed_universe_offsets)):
        idx = target_idx - offset
        if idx >= 0:
            date_by_offset[offset] = calendar_sessions[idx]

    target_universe_info = resolve_date_effective_universe(
        config,
        session_date=target_session,
        directory_capture_id=directory_capture_id,
        stock_basic_capture_id=stock_basic_capture_id,
        research_eligible_only=True,
    )
    if target_universe_info.get("status") != "AVAILABLE":
        raise ValueError(
            f"CROSS_SECTION_TARGET_UNIVERSE_UNAVAILABLE:{target_universe_info.get('reason')}"
        )
    target_members = tuple(sorted(target_universe_info["members"]))
    if not target_members:
        raise ValueError("CROSS_SECTION_TARGET_UNIVERSE_EMPTY")

    universe_capture_id = target_universe_info.get("universe_capture_id")
    universe_version = str(target_universe_info["universe_version"])
    pit_status = str(target_universe_info["pit_status"])

    universe_info_by_date: dict[date, dict[str, Any]] = {
        target_session: target_universe_info
    }
    for offset in needed_universe_offsets:
        if offset == 0 or offset not in date_by_offset:
            continue
        ep_date = date_by_offset[offset]
        ep_info = resolve_date_effective_universe(
            config,
            session_date=ep_date,
            directory_capture_id=directory_capture_id,
            stock_basic_capture_id=stock_basic_capture_id,
            research_eligible_only=True,
        )
        if ep_info.get("status") == "AVAILABLE" and ep_info.get("members"):
            universe_info_by_date[ep_date] = ep_info

    # 2. 一次性只读加载所需全部会话的 QFQ 收盘价（共享基础输入，避免重复读库与重复算收益）
    earliest_idx = max(0, target_idx - max(needed_price_offsets))
    price_sessions = list(calendar_sessions[earliest_idx : target_idx + 1])
    all_symbols: set[str] = set()
    for info in universe_info_by_date.values():
        all_symbols.update(info["members"])

    qfq_close_by_session: dict[date, dict[str, Decimal]] = {
        sess: {} for sess in price_sessions
    }
    session_status_by_session: dict[date, dict[str, str]] = {
        sess: {} for sess in price_sessions
    }
    with _transaction(config, write=False, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT session_date, source_symbol,
                      qfq_close, qfq_bar_status, qfq_close_quality,
                      raw_tradestatus_value, raw_tradestatus_quality
               FROM stock.source_raw_daily
               WHERE source_capture_id = %s
                 AND session_date = ANY(%s)
                 AND source_symbol = ANY(%s)""",
            (source_capture_id, price_sessions, sorted(all_symbols)),
        )
        for (
            sess,
            sym,
            qfq_close,
            qfq_status,
            qfq_quality,
            raw_ts_val,
            raw_ts_qual,
        ) in cursor.fetchall():
            symbol = str(sym)
            # C02：严格区分 ACTUAL / CONFIRMED_SUSPENSION / UNKNOWN，不以缺bar推断停牌、不以停牌保留价当ACTUAL
            if raw_ts_qual == "OBSERVED" and raw_ts_val == "0":
                session_status_by_session[sess][symbol] = "CONFIRMED_SUSPENSION"
                continue
            if (
                raw_ts_qual == "OBSERVED"
                and raw_ts_val == "1"
                and qfq_status == "TARGET_PRESENT"
                and qfq_quality == "OBSERVED"
                and qfq_close is not None
                and Decimal(qfq_close) > 0
            ):
                qfq_close_by_session[sess][symbol] = Decimal(qfq_close)
                session_status_by_session[sess][symbol] = "ACTUAL"
            else:
                session_status_by_session[sess][symbol] = "UNKNOWN"

    # 若逐日名录有确证停牌且 RAW 行缺失，补充为 CONFIRMED_SUSPENSION（优先以同源 RAW tradestatus 为准）
    for u_date, u_info in universe_info_by_date.items():
        if u_date not in session_status_by_session:
            continue
        ts_map = u_info.get("trade_status_by_symbol") or {}
        day_status = session_status_by_session[u_date]
        for sym, ts in ts_map.items():
            if sym not in day_status and ts == "SUSPENDED":
                day_status[sym] = "CONFIRMED_SUSPENSION"

    # 3. 构造共同目标坐标身份与各 ReturnEndpoint（每个端点只算一次）
    coord_rev = f"BAOSTOCK_CAPTURE:{source_capture_id}:FORWARD_ADJUSTED"
    target_coordinates: dict[str, PriceCoordinate] = {
        sym: PriceCoordinate(
            entity_id=sym,
            target_date=target_session,
            coordinate_id=f"FORWARD_ADJUSTED:BAOSTOCK_SOURCE_CAPTURE:{source_capture_id}:{sym}",
            revision=coord_rev,
        )
        for sym in sorted(all_symbols)
    }

    def _build_endpoint(
        *,
        ep_offset: int,
        period: int,
        relative: bool,
    ) -> ReturnEndpoint | None:
        start_offset = ep_offset + period
        if ep_offset not in date_by_offset or start_offset not in date_by_offset:
            return None
        ep_date = date_by_offset[ep_offset]
        start_date = date_by_offset[start_offset]
        u_date = start_date if relative else ep_date
        u_info = universe_info_by_date.get(u_date)
        if u_info is None:
            return None
        members = tuple(sorted(u_info["members"]))
        if not members:
            return None
        ep_close_map = qfq_close_by_session.get(ep_date, {})
        start_close_map = qfq_close_by_session.get(start_date, {})
        start_idx = calendar_sessions.index(start_date)
        ep_idx = calendar_sessions.index(ep_date)
        window_days = calendar_sessions[start_idx : ep_idx + 1]

        returns_map: dict[str, FieldValue] = {}
        win_statuses: dict[str, tuple[str, ...]] = {}
        coords_map: dict[str, PriceCoordinate] = {}
        for sym in members:
            statuses = tuple(
                session_status_by_session.get(d, {}).get(sym, "UNKNOWN")
                for d in window_days
            )
            win_statuses[sym] = statuses
            # C01：先校验逐日窗口状态（两端必须 ACTUAL，内部仅允许 ACTUAL/CONFIRMED_SUSPENSION），
            # 缺口局部降级为 UNKNOWN，绝不向纯核传入非法已知收益导致整批失败
            endpoints_actual = (
                len(statuses) >= 2
                and statuses[0] == "ACTUAL"
                and statuses[-1] == "ACTUAL"
            )
            interior_valid = all(
                s in {"ACTUAL", "CONFIRMED_SUSPENSION"} for s in statuses[1:-1]
            )
            if endpoints_actual and interior_valid:
                returns_map[sym] = _compute_return_fraction(
                    ep_close_map.get(sym), start_close_map.get(sym)
                )
            elif not endpoints_actual:
                returns_map[sym] = unknown("CROSS_ENDPOINT_NOT_ACTUAL")
            else:
                returns_map[sym] = unknown("CROSS_INTERIOR_SESSION_UNKNOWN")
            coords_map[sym] = target_coordinates[sym]
        return ReturnEndpoint(
            endpoint_date=ep_date,
            return_period=period,
            return_start_date=start_date,
            universe_date=u_date,
            universe_id=f"BAOSTOCK_DATE_EFFECTIVE:{u_date.isoformat()}",
            universe_revision=str(u_info["universe_version"]),
            producer_version=CROSS_SECTION_CONTRACT_VERSION,
            universe_members=members,
            returns=returns_map,
            coordinates=coords_map,
            window_statuses=win_statuses,
        )

    rank_endpoints: dict[str, ReturnEndpoint | None] = {
        key: _build_endpoint(ep_offset=offset, period=period, relative=False)
        for key, period, offset in _RANK_SPECS
    }
    relative_endpoints: dict[str, ReturnEndpoint | None] = {
        key: _build_endpoint(ep_offset=0, period=period, relative=True)
        for key, period in _REL_SPECS
    }

    # 4. 批量计算全市场10项横截面因子与10端点元数据，并单事务落库 PG
    batch_result = calculate_cross_section_batch(
        target_date=target_session,
        calendar=calendar_obj,
        target_universe_members=target_members,
        target_coordinates=target_coordinates,
        rank_endpoints=rank_endpoints,
        relative_endpoints=relative_endpoints,
    )
    persist_report = persist_cross_section_day_facts(
        config,
        batch_result=batch_result,
        price_capture_id=source_capture_id,
        universe_capture_id=universe_capture_id,
        universe_version=universe_version,
        calendar_id=calendar_id,
        calendar_revision=calendar_revision,
        pit_status=pit_status,
    )
    return {
        "status": persist_report["status"],
        "target_session": target_session.isoformat(),
        "row_count": persist_report["row_count"],
        "endpoint_count": persist_report["endpoint_count"],
        "universe_capture_id": universe_capture_id,
        "universe_version": universe_version,
        "calendar_revision": calendar_revision,
        "contract_version": CROSS_SECTION_CONTRACT_VERSION,
    }
