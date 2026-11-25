"""日线连板计数事实PG持久化与市场连板梯队只读查询（MKT-04 / §11.4）。"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from datetime import date
from typing import Any

from stock_a.research.entry_context.sessions import _load_calendar
from stock_a.research.limit_up_ladder import (
    LIMIT_UP_LADDER_CONTRACT_VERSION,
    DailyLimitObservation,
    evaluate_limit_up_streak_series,
    summarize_market_limit_up_ladder,
)
from stock_a.storage.postgres.price_limits import PRICE_LIMIT_CONTRACT_VERSION
from stock_a.storage.postgres.source_raw import _transaction


def persist_limit_up_streaks_for_session(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int,
) -> dict[str, Any]:
    """在单事务内基于已存相邻官方会话限价事实计算并幂等写入目标日连板事实。"""
    if target_session > research_cutoff:
        raise ValueError("连板目标会话不得晚于研究截止日")
    calendar_data, _, calendar_revision, calendar_sessions = _load_calendar(
        research_cutoff
    )
    if target_session not in calendar_sessions:
        raise ValueError("连板目标会话不在官方日历中")
    calendar_id = str(calendar_data["calendar_id"])
    target_idx = calendar_sessions.index(target_session)

    with _transaction(config, write=True, statement_timeout_ms=30000) as (_, cursor, _):
        # 查找同一捕获下 <= target_session 已存限价事实的会话集合，并向前回溯连续相邻官方会话段
        cursor.execute(
            """SELECT DISTINCT target_session
               FROM stock.source_price_limit_day_facts
               WHERE source_capture_id = %s
                 AND contract_version = %s
                 AND calendar_revision = %s
                 AND target_session <= %s""",
            (
                source_capture_id,
                PRICE_LIMIT_CONTRACT_VERSION,
                calendar_revision,
                target_session,
            ),
        )
        available_sessions = {row[0] for row in cursor.fetchall()}
        if target_session not in available_sessions:
            raise ValueError("目标交易日缺少已落库的日线涨跌停限价事实")

        contiguous: list[date] = [target_session]
        idx = target_idx - 1
        while idx >= 0 and calendar_sessions[idx] in available_sessions:
            contiguous.append(calendar_sessions[idx])
            idx -= 1
        contiguous.reverse()
        window_sessions = tuple(contiguous)

        cursor.execute(
            """SELECT target_session, source_symbol, market, board, research_eligible,
                      limit_status, close_limit_up, touched_limit_up, one_word_limit_up, rule_id
               FROM stock.source_price_limit_day_facts
               WHERE source_capture_id = %s
                 AND contract_version = %s
                 AND calendar_revision = %s
                 AND target_session = ANY(%s)""",
            (
                source_capture_id,
                PRICE_LIMIT_CONTRACT_VERSION,
                calendar_revision,
                list(window_sessions),
            ),
        )
        obs_by_symbol: dict[str, dict[date, DailyLimitObservation]] = defaultdict(dict)
        meta_at_target: dict[str, tuple[str, str, bool]] = {}
        for row in cursor.fetchall():
            (
                sess,
                sym,
                market,
                board,
                eligible,
                limit_status,
                close_up,
                touched_up,
                one_word_up,
                rule_id,
            ) = row
            obs_by_symbol[sym][sess] = DailyLimitObservation(
                session_date=sess,
                limit_status=limit_status,
                close_limit_up=close_up,
                touched_limit_up=touched_up,
                one_word_limit_up=one_word_up,
                rule_id=rule_id,
            )
            if sess == target_session:
                meta_at_target[sym] = (market, board, bool(eligible))

        if not meta_at_target:
            raise ValueError("目标交易日限价事实为空")

        records: list[tuple[Any, ...]] = []
        for sym in sorted(meta_at_target):
            market, board, eligible = meta_at_target[sym]
            series = evaluate_limit_up_streak_series(
                official_sessions=window_sessions,
                observations_by_session=obs_by_symbol[sym],
            )
            final = series[-1]
            records.append(
                (
                    target_session,
                    sym,
                    market,
                    board,
                    eligible,
                    source_capture_id,
                    "RAW_UNADJUSTED",
                    LIMIT_UP_LADDER_CONTRACT_VERSION,
                    calendar_id,
                    calendar_revision,
                    final.streak_status,
                    final.exact_streak,
                    final.lower_bound_streak,
                    final.is_first_board,
                    final.prior_session_status,
                    final.close_limit_up,
                    final.touched_limit_up,
                    final.one_word_limit_up,
                    final.reason,
                )
            )

        # 检查同键幂等性：已存在则比对全部业务列，同内容 NO_CHANGE，异内容拒绝回滚
        cursor.execute(
            """SELECT source_symbol, market, board, research_eligible,
                      streak_status, exact_streak, lower_bound_streak,
                      is_first_board, prior_session_status,
                      close_limit_up, touched_limit_up, one_word_limit_up, reason
               FROM stock.source_limit_up_streak_day_facts
               WHERE target_session = %s
                 AND source_capture_id = %s
                 AND contract_version = %s
                 AND calendar_revision = %s""",
            (
                target_session,
                source_capture_id,
                LIMIT_UP_LADDER_CONTRACT_VERSION,
                calendar_revision,
            ),
        )
        existing = {row[0]: row for row in cursor.fetchall()}
        if existing:
            expected = {
                rec[1]: (
                    rec[1],
                    rec[2],
                    rec[3],
                    rec[4],
                    rec[10],
                    rec[11],
                    rec[12],
                    rec[13],
                    rec[14],
                    rec[15],
                    rec[16],
                    rec[17],
                    rec[18],
                )
                for rec in records
            }
            if existing == expected:
                return {
                    "status": "NO_CHANGE",
                    "target_session": target_session.isoformat(),
                    "row_count": len(records),
                    "contiguous_session_count": len(window_sessions),
                    "calendar_revision": calendar_revision,
                    "contract_version": LIMIT_UP_LADDER_CONTRACT_VERSION,
                }
            raise ValueError("LIMIT_UP_STREAK_SAME_KEY_DIFFERENT_CONTENT")

        cursor.executemany(
            """INSERT INTO stock.source_limit_up_streak_day_facts (
                   target_session, source_symbol, market, board, research_eligible,
                   source_capture_id, price_coordinate, contract_version,
                   calendar_id, calendar_revision,
                   streak_status, exact_streak, lower_bound_streak,
                   is_first_board, prior_session_status,
                   close_limit_up, touched_limit_up, one_word_limit_up, reason
               ) VALUES (
                   %s, %s, %s, %s, %s,
                   %s, %s, %s,
                   %s, %s,
                   %s, %s, %s,
                   %s, %s,
                   %s, %s, %s, %s
               )""",
            records,
        )

    return {
        "status": "PERSISTED",
        "target_session": target_session.isoformat(),
        "row_count": len(records),
        "contiguous_session_count": len(window_sessions),
        "calendar_revision": calendar_revision,
        "contract_version": LIMIT_UP_LADDER_CONTRACT_VERSION,
    }


def read_market_limit_up_ladder_summary(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_capture_id: int,
    calendar_revision: str | None = None,
    research_eligible_only: bool = True,
) -> dict[str, Any]:
    """只读查询目标交易日全市场日线连板梯队汇总。

    强制绑定精确 calendar_revision（防止多日历修订重复计数），并与同日同捕获的
    source_price_limit_day_facts 目标证券集合逐一核对完整度；未落库返回 NOT_COMPUTED，
    残缺返回 INCOMPLETE，完整闭合才返回 AVAILABLE。
    """
    if calendar_revision is None:
        _, _, calendar_revision, _ = _load_calendar(target_session)

    with _transaction(config, write=False, statement_timeout_ms=15000) as (
        _,
        cursor,
        _,
    ):
        eligible_clause = (
            " AND research_eligible = TRUE" if research_eligible_only else ""
        )
        cursor.execute(
            f"""SELECT source_symbol, streak_status, exact_streak, lower_bound_streak,
                       is_first_board, close_limit_up, touched_limit_up, one_word_limit_up
                FROM stock.source_limit_up_streak_day_facts
                WHERE target_session = %s
                  AND source_capture_id = %s
                  AND contract_version = %s
                  AND calendar_revision = %s{eligible_clause}""",
            (
                target_session,
                source_capture_id,
                LIMIT_UP_LADDER_CONTRACT_VERSION,
                calendar_revision,
            ),
        )
        rows = cursor.fetchall()

        cursor.execute(
            f"""SELECT source_symbol
                FROM stock.source_price_limit_day_facts
                WHERE target_session = %s
                  AND source_capture_id = %s
                  AND contract_version = %s
                  AND calendar_revision = %s{eligible_clause}""",
            (
                target_session,
                source_capture_id,
                PRICE_LIMIT_CONTRACT_VERSION,
                calendar_revision,
            ),
        )
        expected_symbols = {r[0] for r in cursor.fetchall()}

    if not rows:
        return {
            "status": "NOT_COMPUTED",
            "target_session": target_session.isoformat(),
            "source_capture_id": source_capture_id,
            "calendar_revision": calendar_revision,
            "contract_version": LIMIT_UP_LADDER_CONTRACT_VERSION,
            "reason": "LIMIT_UP_STREAK_FACTS_NOT_PERSISTED",
        }

    actual_symbols = {r[0] for r in rows}
    if not expected_symbols:
        return {
            "status": "INCOMPLETE",
            "target_session": target_session.isoformat(),
            "source_capture_id": source_capture_id,
            "calendar_revision": calendar_revision,
            "contract_version": LIMIT_UP_LADDER_CONTRACT_VERSION,
            "expected_symbol_count": 0,
            "actual_symbol_count": len(actual_symbols),
            "missing_symbol_count": 0,
            "extra_symbol_count": len(actual_symbols),
            "reason": "UPSTREAM_PRICE_LIMITS_MISSING",
        }
    if actual_symbols != expected_symbols:
        return {
            "status": "INCOMPLETE",
            "target_session": target_session.isoformat(),
            "source_capture_id": source_capture_id,
            "calendar_revision": calendar_revision,
            "contract_version": LIMIT_UP_LADDER_CONTRACT_VERSION,
            "expected_symbol_count": len(expected_symbols),
            "actual_symbol_count": len(actual_symbols),
            "missing_symbol_count": len(expected_symbols - actual_symbols),
            "extra_symbol_count": len(actual_symbols - expected_symbols),
            "reason": "LIMIT_UP_STREAK_TARGET_SYMBOL_SET_INCOMPLETE",
        }

    items = [
        {
            "streak_status": r[1],
            "exact_streak": r[2],
            "lower_bound_streak": r[3],
            "is_first_board": r[4],
            "close_limit_up": r[5],
            "touched_limit_up": r[6],
            "one_word_limit_up": r[7],
        }
        for r in rows
    ]
    summary = summarize_market_limit_up_ladder(
        target_session=target_session,
        symbol_results=items,
    )
    summary["status"] = "AVAILABLE"
    summary["source_capture_id"] = source_capture_id
    summary["calendar_revision"] = calendar_revision
    summary["research_eligible_only"] = research_eligible_only
    return summary
