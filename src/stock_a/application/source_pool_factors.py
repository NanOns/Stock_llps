"""只读组装完整来源池的非PIT横截面价格事实。"""

from __future__ import annotations

from datetime import date
from typing import Any, Mapping

from stock_a.application.research_window import resolve_pg_history_window
from stock_a.application.tdx_sector_members import _raw_window_observation
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
from stock_a.storage.postgres.source_raw import read_raw_daily_history_page


def read_source_pool_rps(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    package_sha256: str,
    expected_namespace_id: str,
) -> dict[str, Any]:
    """各精确端点分别读取完整来源池；RAW结果仅作后验描述。"""
    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
    ):
        raise ValueError("目标会话必须不晚于研究截止")
    _, sessions, revision = resolve_pg_history_window(target_session.isoformat(), 24)
    calendar_data, _, _, _ = _load_calendar(target_session)
    calendar = MarketSessionCalendar(calendar_data["calendar_id"], revision, sessions)
    all_coordinates: dict[str, PriceCoordinate] = {}
    endpoint_data = {}
    for offset in (0, 3):
        end = sessions[-1 - offset]
        window = sessions[: len(sessions) - offset][-21:]
        page_offset = 0
        members = []
        histories = {}
        total = None
        while True:
            page = read_raw_daily_history_page(
                config,
                target_session=end,
                package_sha256=package_sha256,
                sessions=window,
                offset=page_offset,
                limit=100,
                market="ALL",
                board="ALL",
            )
            if page["namespace_id"] != expected_namespace_id:
                raise ValueError("来源池namespace与请求不一致")
            if total is None:
                total = page["total_count"]
                if total and (
                    page["version_status"] != "CLOSED"
                    or total != page["version_expected_row_count"]
                ):
                    raise ValueError("来源池分母不完整")
            elif total != page["total_count"] or page["version_status"] != "CLOSED":
                raise ValueError("来源池分页分母发生变化")
            for candidate in page["candidates"]:
                symbol = candidate["source_symbol"]
                if symbol in histories:
                    raise ValueError("来源池证券重复")
                rows = candidate["history_rows"]
                by_key = {(symbol, row["session_date"]): row for row in rows}
                if len(by_key) != len(rows):
                    raise ValueError("来源历史会话重复")
                counts = {
                    key: {"row_count": 1, "package_count": 1, "capture_count": 1}
                    for key in by_key
                }
                target_rows = [row for row in rows if row["session_date"] == end]
                if len(target_rows) != 1:
                    raise ValueError("来源池目标日缺少唯一Raw行")
                capture = target_rows[0]["source_capture_id"]
                observations = tuple(
                    _raw_window_observation(
                        source_symbol=symbol,
                        session=day,
                        raw_by_key=by_key,
                        revision_counts=counts,
                        target_session=end,
                        target_capture_id=capture,
                    )[0]
                    for day in window
                )
                entry_shas = {
                    row.get("source_entry_sha256")
                    for row in rows
                    if row.get("source_entry_sha256")
                }
                if len(entry_shas) > 1:
                    observations = tuple(
                        CoreObservation(
                            session=day,
                            state="UNKNOWN",
                            coordinate_id="RAW_UNADJUSTED",
                            close=unknown("SOURCE_ENTRY_SHA_CONFLICT"),
                        )
                        for day in window
                    )
                histories[symbol] = observations
                members.append(symbol)
                all_coordinates[symbol] = PriceCoordinate(
                    symbol, target_session, "RAW_UNADJUSTED", package_sha256
                )
            if page["next_offset"] is None:
                break
            page_offset = page["next_offset"]
        if len(members) != total or members != sorted(set(members)):
            raise ValueError("来源池分页未闭合")
        endpoint_data[offset] = (end, window, tuple(members), histories)
    target_members = endpoint_data[0][2]
    if not target_members:
        raise ValueError("目标日没有精确来源池")
    ranked = {}
    counts = {}
    for offset, (end, window, members, histories) in endpoint_data.items():
        for period in (5, 20):
            key = f"rps{period}_{offset}"
            if not members:
                ranked[key] = {}
                counts[key] = {
                    "total": 0,
                    "known": 0,
                    "unknown": 0,
                    "session": end.isoformat(),
                }
                continue
            endpoint_calendar = MarketSessionCalendar(
                calendar.calendar_id, calendar.revision, window
            )
            returns = {
                symbol: calculate_core_return(
                    rows, periods=period, official_calendar=endpoint_calendar
                )
                for symbol, rows in histories.items()
            }
            endpoint = ReturnEndpoint(
                endpoint_date=end,
                return_period=period,
                return_start_date=window[-1 - period],
                universe_date=end,
                universe_id="SOURCE_POOL_NON_PIT_EXPLORATORY",
                universe_revision=package_sha256,
                producer_version="CORE_RAW_SOURCE_RPS_R1",
                universe_members=members,
                returns=returns,
                coordinates={symbol: all_coordinates[symbol] for symbol in members},
                window_statuses={
                    symbol: tuple(row.state for row in histories[symbol][-period - 1 :])
                    for symbol in members
                },
            )
            ranked[key] = rank_return_endpoint(
                endpoint,
                target_date=target_session,
                calendar=calendar,
                target_coordinates=all_coordinates,
            )
            known = sum(item.quality == "OBSERVED" for item in returns.values())
            counts[key] = {
                "total": len(members),
                "known": known,
                "unknown": len(members) - known,
                "session": end.isoformat(),
            }
    facts = {}
    for symbol in target_members:
        fact = {}
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
        "namespace_id": expected_namespace_id,
        "package_sha256": package_sha256,
        "scope": "SOURCE_POOL_NON_PIT_EXPLORATORY",
        "coordinate": "RAW_UNADJUSTED",
        "corporate_action_continuity": "UNKNOWN",
        "formal_research_universe": "UNKNOWN",
        "calendar_revision": revision,
        "endpoint_counts": counts,
        "facts": facts,
    }
