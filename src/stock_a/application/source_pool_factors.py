"""只读组装完整来源池的非PIT横截面价格事实。"""

from __future__ import annotations

from datetime import date
from typing import Any, Mapping

from stock_a.application.research_window import resolve_pg_history_window
from stock_a.domain import FieldValue, unknown
from stock_a.research.core import (
    CoreObservation,
    MarketSessionCalendar,
    calculate_core_return,
)
from stock_a.research.cross_section import (
    PriceCoordinate,
    ReturnEndpoint,
    rank_return_endpoint,
)
from stock_a.research.entry import _load_calendar
from stock_a.storage.postgres.baostock_price_facts import (
    read_baostock_price_history_batch,
    read_baostock_price_page,
)


def read_source_pool_rps(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int,
    page_size: int = 5000,
) -> dict[str, Any]:
    """按paired BaoStock capture读取两个完整QFQ横截面端点。"""
    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or type(page_size) is not int
        or not 1 <= page_size <= 5000
    ):
        raise ValueError("RPS目标、截止、paired capture或分页上限无效")
    from stock_a.research.entry_history.support import (
        _core_observation_from_pg_price_row,
    )

    _, sessions, revision = resolve_pg_history_window(target_session.isoformat(), 24)
    calendar_data, _, _, _ = _load_calendar(target_session)
    calendar = MarketSessionCalendar(calendar_data["calendar_id"], revision, sessions)
    coordinate_id = f"FORWARD_ADJUSTED:BAOSTOCK_SOURCE_CAPTURE:{source_capture_id}"
    endpoint_members: dict[int, tuple[str, ...]] = {}
    for offset in (0, 3):
        endpoint = sessions[-1 - offset]
        symbols: list[str] = []
        page_offset = 0
        total = None
        raw_seen = 0
        while True:
            page = read_baostock_price_page(
                config,
                target_session=endpoint,
                cutoff=research_cutoff,
                source_capture_id=source_capture_id if offset == 0 else None,
                price_coordinate="FORWARD_ADJUSTED",
                offset=page_offset,
                limit=page_size,
                market="ALL",
                board="ALL",
                allow_full_pool_page=True,
            )
            if offset == 0 and page.get("source_capture_id") != source_capture_id:
                raise ValueError("RPS端点没有绑定请求的paired capture")
            if (
                page.get("version_status") != "CLOSED"
                or page.get("partition_closed") is not True
            ):
                raise ValueError("RPS端点QFQ分区或证券总体未闭合")
            if total is None:
                total = int(page["total_count"])
            elif int(page["total_count"]) != total:
                raise ValueError("RPS端点分页期间分母发生变化")
            from stock_a.sources.baostock.identity import eligible_research_board

            page_rows = page.get("rows", ())
            raw_seen += len(page_rows)
            symbols.extend(
                str(row["source_symbol"])
                for row in page_rows
                if isinstance(row, Mapping)
                and eligible_research_board(row.get("source_symbol")) is not None
            )
            next_offset = page.get("next_offset")
            if next_offset is None:
                break
            page_offset = int(next_offset)
        if (
            total is None
            or raw_seen != total
            or not symbols
            or len(set(symbols)) != len(symbols)
        ):
            raise ValueError("RPS端点代码总体与分页分母不闭合")
        endpoint_members[offset] = tuple(sorted(symbols))

    all_symbols = tuple(sorted(set(endpoint_members[0]) | set(endpoint_members[3])))
    history_rows: dict[tuple[str, date], Mapping[str, Any]] = {}
    page_offset = 0
    total_rows = None
    while True:
        batch = read_baostock_price_history_batch(
            config,
            source_symbols=all_symbols,
            sessions=sessions,
            source_capture_id=None,
            price_coordinate="FORWARD_ADJUSTED",
            cutoff=research_cutoff,
            offset=page_offset,
            limit=page_size,
        )
        page = batch.get("page", {})
        if batch.get("price_coordinate") != "FORWARD_ADJUSTED" or batch.get(
            "target_sessions"
        ) != [item.isoformat() for item in sessions]:
            raise ValueError("RPS历史批次响应与QFQ capture窗口不一致")
        if total_rows is None:
            total_rows = int(page.get("total", -1))
        elif int(page.get("total", -1)) != total_rows:
            raise ValueError("RPS历史分页期间行数分母发生变化")
        for item in batch.get("coverage", ()):
            if (
                not isinstance(item, Mapping)
                or item.get("coverage_status") != "COMPLETE"
            ):
                raise ValueError("RPS QFQ历史会话覆盖未完整闭合")
        for row in batch.get("rows", ()):
            if not isinstance(row, Mapping):
                raise ValueError("RPS历史行不是字段映射")
            key = (str(row["source_symbol"]), row["session_date"])
            if key in history_rows or (
                row.get("session_date") == target_session
                and row.get("source_capture_id") != source_capture_id
            ):
                raise ValueError("RPS历史行重复或source_capture_id不匹配")
            history_rows[key] = row
        next_offset = page.get("next_offset")
        if next_offset is None:
            break
        page_offset = int(next_offset)
    if total_rows is None or len(history_rows) != total_rows:
        raise ValueError("RPS历史行数与PG分页分母不一致")

    target_coordinates = {
        symbol: PriceCoordinate(
            symbol, target_session, coordinate_id, str(source_capture_id)
        )
        for symbol in all_symbols
    }
    ranked: dict[str, dict[str, FieldValue]] = {}
    counts: dict[str, dict[str, Any]] = {}
    for offset in (0, 3):
        end = sessions[-1 - offset]
        endpoint_window = sessions[: len(sessions) - offset][-21:]
        members = endpoint_members[offset]
        endpoint_calendar = MarketSessionCalendar(
            calendar.calendar_id, calendar.revision, endpoint_window
        )
        histories: dict[str, tuple[CoreObservation, ...]] = {}
        window_statuses: dict[str, tuple[str, ...]] = {}
        for symbol in members:
            observations = tuple(
                _core_observation_from_pg_price_row(
                    history_rows.get((symbol, day)),
                    session=day,
                    source_capture_id=source_capture_id
                    if day == target_session
                    else None,
                    price_coordinate="FORWARD_ADJUSTED",
                    coordinate_id=coordinate_id,
                )
                for day in endpoint_window
            )
            histories[symbol] = observations
            window_statuses[symbol] = tuple(
                observation.state for observation in observations
            )

        for period in (5, 20):
            period_returns = {
                symbol: calculate_core_return(
                    histories[symbol],
                    periods=period,
                    official_calendar=endpoint_calendar,
                )
                for symbol in members
            }
            key = f"rps{period}_{offset}"
            period_statuses = {
                symbol: window_statuses[symbol][-(period + 1) :] for symbol in members
            }
            endpoint = ReturnEndpoint(
                endpoint_date=end,
                return_period=period,
                return_start_date=endpoint_window[-1 - period],
                universe_date=end,
                universe_id="BAOSTOCK_SOURCE_POOL_NON_PIT",
                universe_revision=str(source_capture_id),
                producer_version="CORE_QFQ_SOURCE_RPS_R2",
                universe_members=members,
                returns=period_returns,
                coordinates={symbol: target_coordinates[symbol] for symbol in members},
                window_statuses=period_statuses,
            )
            ranked[key] = rank_return_endpoint(
                endpoint,
                target_date=target_session,
                calendar=calendar,
                target_coordinates=target_coordinates,
            )
            known = sum(
                value.quality == "OBSERVED" for value in period_returns.values()
            )
            counts[key] = {
                "total": len(members),
                "known": known,
                "unknown": len(members) - known,
                "session": end.isoformat(),
            }

    facts: dict[str, dict[str, FieldValue]] = {}
    for symbol in endpoint_members[0]:
        fact: dict[str, FieldValue] = {}
        for period in (5, 20):
            current = ranked[f"rps{period}_0"][symbol]
            previous = ranked[f"rps{period}_3"].get(
                symbol, unknown("PRIOR_SOURCE_POOL_MEMBER_MISSING")
            )
            delta = (
                FieldValue(current.value - previous.value)
                if current.quality == previous.quality == "OBSERVED"
                else unknown(current.reason or previous.reason or "RPS_UNKNOWN")
            )
            fact[f"rps{period}"] = current
            fact[f"rps{period}_delta3_pp"] = delta
        facts[symbol] = fact
    return {
        "target_session": target_session,
        "research_cutoff": research_cutoff,
        "source_capture_id": source_capture_id,
        "scope": "SOURCE_POOL_NON_PIT_EXPLORATORY",
        "coordinate": "FORWARD_ADJUSTED",
        "price_coordinate": "FORWARD_ADJUSTED",
        "time_role": "NON_PIT_RETROSPECTIVE",
        "corporate_action_continuity": "NON_PIT_RETROSPECTIVE",
        "formal_research_universe": "UNKNOWN",
        "calendar_revision": revision,
        "endpoint_counts": counts,
        "facts": facts,
    }
