"""日线涨跌停限价事实PG持久化与市场压力/个股读取。"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections.abc import Mapping
from datetime import date
from decimal import Decimal, localcontext
from fractions import Fraction
from typing import Any

from stock_a.research.entry_context.sessions import _load_calendar
from stock_a.research.price_limits import (
    PRICE_LIMIT_CONTRACT_VERSION,
    evaluate_daily_price_limit,
)
from stock_a.storage.postgres.source_raw import _transaction


def fraction_payload(value: Fraction | None) -> dict[str, Any] | None:
    """把精确Fraction序列化为市场压力字段。"""
    if value is None:
        return None
    with localcontext() as context:
        context.prec = 40
        decimal_value = Decimal(value.numerator) / Decimal(value.denominator)
        display = f"{decimal_value:.12f}"
    return {
        "numerator": value.numerator,
        "denominator": value.denominator,
        "fraction": str(value),
        "display_decimal_12dp": display,
        "unit": "fraction",
    }


def _compute_listing_session_ordinal(
    calendar_sessions: tuple[date, ...] | list[date],
    *,
    ipo_date: date | None,
    target_session: date,
) -> int | None:
    """按官方交易日历精确计算上市后交易会话序号（ipo_date早于日历起点或晚于目标日时返回None）。"""
    if ipo_date is None or not calendar_sessions:
        return None
    if target_session < ipo_date or ipo_date < calendar_sessions[0]:
        return None
    start_idx = bisect_left(calendar_sessions, ipo_date)
    end_idx = bisect_right(calendar_sessions, target_session)
    if end_idx <= start_idx:
        return None
    return end_idx - start_idx


def persist_source_price_limits_for_session(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int | None = None,
    stock_basic_capture_id: int | None = None,
) -> dict[str, Any]:
    """在单事务内计算并幂等写入目标交易日的日线涨跌停限价事实。"""
    if target_session > research_cutoff:
        raise ValueError("限价目标会话不得晚于研究截止日")
    calendar_data, _, calendar_revision, calendar_sessions = _load_calendar(
        research_cutoff
    )
    if target_session not in calendar_sessions:
        raise ValueError("限价目标会话不在官方日历中")
    calendar_id = str(calendar_data["calendar_id"])

    with _transaction(config, write=True, statement_timeout_ms=30000) as (_, cursor, _):
        cursor.execute(
            """WITH target_symbols AS (
                   SELECT d.source_symbol
                   FROM stock.source_raw_daily d
                   JOIN stock.source_captures c
                     ON c.capture_id = d.raw_source_capture_id
                   JOIN stock.source_capture_requests raw_request
                     ON raw_request.capture_id = d.raw_source_capture_id
                    AND raw_request.source_code = d.source_code
                    AND raw_request.adjustment_flag = '3'
                   WHERE d.session_date = %s
                     AND d.raw_source_capture_id IS NOT NULL
                     AND c.provider = 'BAOSTOCK'
                     AND c.range_end_session <= %s
                     AND raw_request.request_status IN ('COMPLETED','NO_DATA')
                     AND (raw_request.request_status = 'NO_DATA'
                          OR raw_request.completed_through_session >= raw_request.range_end_session)
               ),
               hist AS (
                   SELECT d.source_symbol, d.session_date, d.market, d.board,
                          d.raw_source_capture_id AS row_source_capture_id,
                          d.bar_status, d.raw_tradestatus_value, d.raw_is_st_value,
                          d.raw_preclose, d.raw_open, d.raw_high, d.raw_low, d.raw_close,
                          d.raw_preclose_quality, d.raw_open_quality, d.raw_high_quality,
                          d.raw_low_quality, d.raw_close_quality,
                          d.raw_tradestatus_quality, d.raw_is_st_quality,
                          LAG(d.raw_close) OVER (
                              PARTITION BY d.source_symbol ORDER BY d.session_date
                          ) AS prev_raw_close,
                          COUNT(*) FILTER (
                              WHERE d.bar_status = 'TARGET_PRESENT'
                          ) OVER (
                              PARTITION BY d.source_symbol
                              ORDER BY d.session_date
                              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                          ) AS hist_present_cnt
                   FROM stock.source_raw_daily d
                   JOIN stock.source_captures c
                     ON c.capture_id = d.raw_source_capture_id
                   JOIN stock.source_capture_requests raw_request
                     ON raw_request.capture_id = d.raw_source_capture_id
                    AND raw_request.source_code = d.source_code
                    AND raw_request.adjustment_flag = '3'
                   JOIN target_symbols ts ON ts.source_symbol = d.source_symbol
                   WHERE d.session_date <= %s
                     AND d.raw_source_capture_id IS NOT NULL
                     AND c.provider = 'BAOSTOCK'
                     AND c.range_end_session <= %s
                     AND raw_request.request_status IN ('COMPLETED','NO_DATA')
                     AND (raw_request.request_status = 'NO_DATA'
                          OR raw_request.completed_through_session >= raw_request.range_end_session)
               )
               SELECT source_symbol, market, board, row_source_capture_id, bar_status,
                      raw_tradestatus_value, raw_is_st_value,
                      raw_preclose, prev_raw_close, hist_present_cnt,
                      raw_open, raw_high, raw_low, raw_close,
                      raw_preclose_quality, raw_open_quality, raw_high_quality,
                      raw_low_quality, raw_close_quality,
                      raw_tradestatus_quality, raw_is_st_quality
               FROM hist
               WHERE session_date = %s
               ORDER BY source_symbol""",
            (
                target_session,
                research_cutoff,
                target_session,
                research_cutoff,
                target_session,
            ),
        )
        raw_rows = cursor.fetchall()
        if not raw_rows:
            raise ValueError("目标会话没有可用的BaoStock RAW来源行")

        ipo_date_by_symbol: dict[str, date] = {}
        if stock_basic_capture_id is not None:
            cursor.execute(
                """SELECT source_symbol, ipo_date
                   FROM stock.source_baostock_stock_basic
                   WHERE source_capture_id = %s
                     AND field_quality <> 'CONFLICT_QUARANTINED'
                     AND ipo_date IS NOT NULL""",
                (stock_basic_capture_id,),
            )
        else:
            cursor.execute(
                """SELECT source_symbol, ipo_date
                   FROM stock.source_baostock_stock_basic
                   WHERE field_quality <> 'CONFLICT_QUARANTINED'
                     AND ipo_date IS NOT NULL"""
            )
        ipo_date_by_symbol = {str(sym): ipo for sym, ipo in cursor.fetchall()}

        records = []
        status_counts: dict[str, int] = {
            "ASSESSABLE": 0,
            "NA_SUSPENDED": 0,
            "NA_NO_PRICE_LIMIT": 0,
            "UNKNOWN": 0,
        }
        for (
            symbol,
            market,
            board,
            row_source_capture_id,
            bar_status,
            tradestatus,
            is_st,
            raw_preclose,
            prev_raw_close,
            hist_cnt,
            raw_open,
            raw_high,
            raw_low,
            raw_close,
            raw_preclose_quality,
            raw_open_quality,
            raw_high_quality,
            raw_low_quality,
            raw_close_quality,
            raw_tradestatus_quality,
            raw_is_st_quality,
        ) in raw_rows:
            symbol_ipo_date = ipo_date_by_symbol.get(symbol)
            listing_ordinal = _compute_listing_session_ordinal(
                calendar_sessions,
                ipo_date=symbol_ipo_date,
                target_session=target_session,
            )
            fact = evaluate_daily_price_limit(
                source_symbol=symbol,
                market=market,
                board=board,
                target_session=target_session,
                price_coordinate="RAW_UNADJUSTED",
                bar_status=bar_status,
                raw_tradestatus_value=tradestatus,
                raw_is_st_value=is_st,
                raw_preclose=raw_preclose,
                prev_raw_close=prev_raw_close,
                history_present_count=int(hist_cnt or 0),
                raw_open=raw_open,
                raw_high=raw_high,
                raw_low=raw_low,
                raw_close=raw_close,
                ipo_date=symbol_ipo_date,
                listing_session_ordinal=listing_ordinal,
                raw_preclose_quality=raw_preclose_quality,
                raw_open_quality=raw_open_quality,
                raw_high_quality=raw_high_quality,
                raw_low_quality=raw_low_quality,
                raw_close_quality=raw_close_quality,
                raw_tradestatus_quality=raw_tradestatus_quality,
                raw_is_st_quality=raw_is_st_quality,
            )
            status_counts[fact["limit_status"]] = (
                status_counts.get(fact["limit_status"], 0) + 1
            )
            records.append(
                (
                    target_session,
                    fact["source_symbol"],
                    fact["market"],
                    fact["board"],
                    fact["research_eligible"],
                    int(row_source_capture_id),
                    fact["price_coordinate"],
                    calendar_id,
                    calendar_revision,
                    fact["contract_version"],
                    fact["rule_id"],
                    fact["limit_status"],
                    fact["limit_pct"],
                    fact["reference_price"],
                    fact["upper_limit"],
                    fact["lower_limit"],
                    fact["close_limit_up"],
                    fact["close_limit_down"],
                    fact["touched_limit_up"],
                    fact["touched_limit_down"],
                    fact["one_word_limit_up"],
                    fact["one_word_limit_down"],
                    fact["quality"],
                    fact["reason"],
                )
            )

        cursor.executemany(
            """INSERT INTO stock.source_price_limit_day_facts (
                   target_session, source_symbol, market, board, research_eligible,
                   source_capture_id, price_coordinate, calendar_id, calendar_revision,
                   contract_version, rule_id, limit_status, limit_pct, reference_price,
                   upper_limit, lower_limit, close_limit_up, close_limit_down,
                   touched_limit_up, touched_limit_down, one_word_limit_up,
                   one_word_limit_down, quality, reason
               ) VALUES (
                   %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                   %s, %s, %s, %s, %s, %s, %s, %s
               )
               ON CONFLICT (
                   target_session, source_symbol
               ) DO UPDATE SET
                   market = EXCLUDED.market,
                   board = EXCLUDED.board,
                   research_eligible = EXCLUDED.research_eligible,
                   source_capture_id = EXCLUDED.source_capture_id,
                   calendar_id = EXCLUDED.calendar_id,
                   calendar_revision = EXCLUDED.calendar_revision,
                   contract_version = EXCLUDED.contract_version,
                   rule_id = EXCLUDED.rule_id,
                   limit_status = EXCLUDED.limit_status,
                   limit_pct = EXCLUDED.limit_pct,
                   reference_price = EXCLUDED.reference_price,
                   upper_limit = EXCLUDED.upper_limit,
                   lower_limit = EXCLUDED.lower_limit,
                   close_limit_up = EXCLUDED.close_limit_up,
                   close_limit_down = EXCLUDED.close_limit_down,
                   touched_limit_up = EXCLUDED.touched_limit_up,
                   touched_limit_down = EXCLUDED.touched_limit_down,
                   one_word_limit_up = EXCLUDED.one_word_limit_up,
                   one_word_limit_down = EXCLUDED.one_word_limit_down,
                   quality = EXCLUDED.quality,
                   reason = EXCLUDED.reason
               WHERE (
                   source_price_limit_day_facts.board,
                   source_price_limit_day_facts.research_eligible,
                   source_price_limit_day_facts.source_capture_id,
                   source_price_limit_day_facts.calendar_revision,
                   source_price_limit_day_facts.contract_version,
                   source_price_limit_day_facts.rule_id,
                   source_price_limit_day_facts.limit_status,
                   source_price_limit_day_facts.limit_pct,
                   source_price_limit_day_facts.reference_price,
                   source_price_limit_day_facts.upper_limit,
                   source_price_limit_day_facts.lower_limit,
                   source_price_limit_day_facts.close_limit_up,
                   source_price_limit_day_facts.close_limit_down,
                   source_price_limit_day_facts.touched_limit_up,
                   source_price_limit_day_facts.touched_limit_down,
                   source_price_limit_day_facts.one_word_limit_up,
                   source_price_limit_day_facts.one_word_limit_down,
                   source_price_limit_day_facts.quality,
                   source_price_limit_day_facts.reason
               ) IS DISTINCT FROM (
                   EXCLUDED.board,
                   EXCLUDED.research_eligible,
                   EXCLUDED.source_capture_id,
                   EXCLUDED.calendar_revision,
                   EXCLUDED.contract_version,
                   EXCLUDED.rule_id,
                   EXCLUDED.limit_status,
                   EXCLUDED.limit_pct,
                   EXCLUDED.reference_price,
                   EXCLUDED.upper_limit,
                   EXCLUDED.lower_limit,
                   EXCLUDED.close_limit_up,
                   EXCLUDED.close_limit_down,
                   EXCLUDED.touched_limit_up,
                   EXCLUDED.touched_limit_down,
                   EXCLUDED.one_word_limit_up,
                   EXCLUDED.one_word_limit_down,
                   EXCLUDED.quality,
                   EXCLUDED.reason
               )""",
            records,
        )
    raw_capture_ids = sorted({int(row[3]) for row in raw_rows})
    return {
        "status": "PERSISTED",
        "target_session": target_session.isoformat(),
        "source_capture_id": raw_capture_ids[0] if len(raw_capture_ids) == 1 else None,
        "raw_source_capture_ids": raw_capture_ids,
        "requested_source_capture_id": source_capture_id,
        "calendar_revision": calendar_revision,
        "contract_version": PRICE_LIMIT_CONTRACT_VERSION,
        "row_count": len(records),
        "status_counts": status_counts,
    }


def read_market_pressure_summary(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_capture_id: int | None = None,
    calendar_revision: str | None = None,
) -> dict[str, Any]:
    """读取正式研究范围（主板+创业板）的日线涨跌停市场压力轴汇总。"""
    with _transaction(config, write=False, statement_timeout_ms=15000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT
                   count(*)::bigint AS total_count,
                   count(*) FILTER (WHERE limit_status = 'ASSESSABLE')::bigint AS assessable_count,
                   count(*) FILTER (WHERE limit_status = 'NA_SUSPENDED')::bigint AS suspended_count,
                   count(*) FILTER (WHERE limit_status = 'UNKNOWN')::bigint AS unknown_count,
                   count(*) FILTER (WHERE limit_status = 'ASSESSABLE' AND close_limit_up = 'TRUE')::bigint AS limit_up_count,
                   count(*) FILTER (WHERE limit_status = 'ASSESSABLE' AND close_limit_down = 'TRUE')::bigint AS limit_down_count,
                   count(*) FILTER (WHERE limit_status = 'ASSESSABLE' AND touched_limit_up = 'TRUE')::bigint AS touched_up_count,
                   count(*) FILTER (WHERE limit_status = 'ASSESSABLE' AND touched_limit_down = 'TRUE')::bigint AS touched_down_count,
                   count(*) FILTER (WHERE limit_status = 'ASSESSABLE' AND one_word_limit_up = 'TRUE')::bigint AS one_word_up_count,
                   count(*) FILTER (WHERE limit_status = 'ASSESSABLE' AND one_word_limit_down = 'TRUE')::bigint AS one_word_down_count
               FROM stock.source_price_limit_day_facts
               WHERE target_session = %s
                 AND research_eligible = TRUE""",
            (target_session,),
        )
        row = cursor.fetchone()
        (
            total_count,
            assessable_count,
            suspended_count,
            unknown_count,
            limit_up_count,
            limit_down_count,
            touched_up_count,
            touched_down_count,
            one_word_up_count,
            one_word_down_count,
        ) = (int(v or 0) for v in row)
        if total_count == 0:
            return {
                "status": "NOT_COMPUTED",
                "classification": "UNKNOWN",
                "reason": "目标会话尚未生成已保存的涨跌停限价事实。",
            }
        cursor.execute(
            """SELECT reason, count(*)::bigint
               FROM stock.source_price_limit_day_facts
               WHERE target_session = %s
                 AND research_eligible = TRUE
                 AND limit_status = 'UNKNOWN'
               GROUP BY reason
               ORDER BY count(*) DESC, reason ASC""",
            (target_session,),
        )
        unknown_reason_counts = {str(r): int(c) for r, c in cursor.fetchall()}

    active_count = total_count - suspended_count
    if active_count <= 0 or assessable_count <= 0:
        return {
            "status": "UNKNOWN",
            "classification": "UNKNOWN",
            "reason": "有效可判定涨跌停样本数为0，压力轴保持UNKNOWN。",
            "research_pool_count": total_count,
            "assessable_count": assessable_count,
            "suspended_count": suspended_count,
            "unknown_count": unknown_count,
            "unknown_reason_counts": unknown_reason_counts,
        }
    coverage = Fraction(assessable_count, active_count)
    if coverage < Fraction(4, 5):
        return {
            "status": "UNKNOWN",
            "classification": "UNKNOWN",
            "reason": "涨跌停可判定覆盖率低于80%，压力轴保持UNKNOWN。",
            "research_pool_count": total_count,
            "assessable_count": assessable_count,
            "suspended_count": suspended_count,
            "unknown_count": unknown_count,
            "coverage": fraction_payload(coverage),
            "unknown_reason_counts": unknown_reason_counts,
        }
    pressure_ratio = Fraction(limit_down_count, assessable_count)
    if pressure_ratio >= Fraction(5, 100):
        classification = "HIGH"
    elif pressure_ratio >= Fraction(1, 100):
        classification = "ELEVATED"
    else:
        classification = "LOW"
    return {
        "status": "OBSERVED",
        "classification": classification,
        "metric_name": "limit_down_ratio_raw_assessable",
        "exact": fraction_payload(pressure_ratio),
        "coverage": fraction_payload(coverage),
        "price_coordinate": "RAW_UNADJUSTED",
        "contract_version": PRICE_LIMIT_CONTRACT_VERSION,
        "research_pool_count": total_count,
        "active_non_suspended_count": active_count,
        "assessable_count": assessable_count,
        "suspended_count": suspended_count,
        "unknown_count": unknown_count,
        "limit_up_count": limit_up_count,
        "limit_down_count": limit_down_count,
        "touched_limit_up_count": touched_up_count,
        "touched_limit_down_count": touched_down_count,
        "one_word_limit_up_count": one_word_up_count,
        "one_word_limit_down_count": one_word_down_count,
        "unknown_reason_counts": unknown_reason_counts,
        "reason": "仅对非ST且非除权/非上市特殊期的已证实制度子域判定，ST与缺参考价证券保留UNKNOWN。",
    }


def read_stock_price_limit_fact(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_symbol: str,
    source_capture_id: int | None = None,
    calendar_revision: str,
) -> dict[str, Any]:
    """读取单只股票在目标交易日的已保存涨跌停限价事实。"""
    with _transaction(config, write=False, statement_timeout_ms=10000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT target_session, source_symbol, market, board, research_eligible,
                      source_capture_id, price_coordinate, calendar_id, calendar_revision,
                      contract_version, rule_id, limit_status, limit_pct, reference_price,
                      upper_limit, lower_limit, close_limit_up, close_limit_down,
                      touched_limit_up, touched_limit_down, one_word_limit_up,
                      one_word_limit_down, quality, reason
               FROM stock.source_price_limit_day_facts
               WHERE target_session = %s
                 AND source_symbol = %s
                 AND contract_version = %s
                 AND calendar_revision = %s""",
            (
                target_session,
                source_symbol,
                PRICE_LIMIT_CONTRACT_VERSION,
                calendar_revision,
            ),
        )
        row = cursor.fetchone()
        if row is None:
            return {
                "status": "NOT_COMPUTED",
                "target_session": target_session.isoformat(),
                "source_symbol": source_symbol,
                "source_capture_id": source_capture_id,
                "price_coordinate": "RAW_UNADJUSTED",
                "reason": "目标会话尚未生成该证券的涨跌停限价事实。",
            }
        names = [column.name for column in cursor.description]
        data = dict(zip(names, row, strict=True))
        for key in ("limit_pct", "reference_price", "upper_limit", "lower_limit"):
            if data[key] is not None:
                data[key] = str(data[key])
        data["target_session"] = data["target_session"].isoformat()
        data["status"] = "AVAILABLE"
        return data
