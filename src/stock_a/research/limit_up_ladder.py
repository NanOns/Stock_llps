"""MATH:MKT-04 + §11.4 严格日线连板计数与日线连板梯队纯算法引擎。"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping, Sequence


LIMIT_UP_LADDER_CONTRACT_VERSION = "STOCK_A_LIMIT_UP_LADDER_R1"


@dataclass(frozen=True)
class DailyLimitObservation:
    """单个官方交易日、单只证券的日线限价事实输入。"""

    session_date: date
    limit_status: str
    close_limit_up: str
    touched_limit_up: str = "UNKNOWN"
    one_word_limit_up: str = "UNKNOWN"
    rule_id: str | None = None


@dataclass(frozen=True)
class DailyLimitUpStreakResult:
    """单只证券在目标交易日的严格日线连板计数结果。"""

    target_session: date
    streak_status: str
    exact_streak: int | None
    lower_bound_streak: int | None
    is_first_board: str
    prior_session_status: str
    close_limit_up: str
    touched_limit_up: str
    one_word_limit_up: str
    reason: str


def evaluate_limit_up_streak_series(
    *,
    official_sessions: Sequence[date],
    observations_by_session: Mapping[date, DailyLimitObservation | None],
) -> tuple[DailyLimitUpStreakResult, ...]:
    """按相邻官方市场会话严格递推连板计数（§11.4）。

    规则：
    - 停牌/无价格限制当日计数 NA，以明确 NA_RESET 供下一会话；未知缺日不能当 RESET。
    - 当日收盘明确非涨停（close_limit_up == 'FALSE'）→ 精确计数 0（EXACT）。
    - 当日收盘涨停（close_limit_up == 'TRUE'）：
      - 前会话精确计数 k >= 0 → 精确计数 k + 1（EXACT）；
      - 前会话明确 NA_RESET（停牌/无涨跌停）→ 重新从 1 起（EXACT, 1）；
      - 前会话左截尾或 UNKNOWN → 精确值 UNKNOWN，仅递推下界 >= 1（LOWER_BOUND_ONLY）；
      - 前会话为 LOWER_BOUND_ONLY(lb) → 保持 LOWER_BOUND_ONLY，下界递推为 lb + 1。
    - 首板（is_first_board == 'TRUE'）仅在当日涨停且前会话确定为 0 或 NA_RESET 时成立。
    """
    if not official_sessions:
        raise ValueError("OFFICIAL_SESSIONS_REQUIRED")
    sessions = tuple(official_sessions)
    for prev_s, cur_s in zip(sessions, sessions[1:]):
        if cur_s <= prev_s:
            raise ValueError("OFFICIAL_SESSIONS_MUST_BE_STRICTLY_INCREASING")

    results: list[DailyLimitUpStreakResult] = []
    # 状态机记录上一官方会话的连板状态
    prev_state: str = "LEFT_CENSORED"
    prev_exact: int | None = None
    prev_lower: int | None = None

    for session in sessions:
        obs = observations_by_session.get(session)
        if obs is None:
            res = DailyLimitUpStreakResult(
                target_session=session,
                streak_status="UNKNOWN",
                exact_streak=None,
                lower_bound_streak=None,
                is_first_board="UNKNOWN",
                prior_session_status=prev_state,
                close_limit_up="UNKNOWN",
                touched_limit_up="UNKNOWN",
                one_word_limit_up="UNKNOWN",
                reason="LIMIT_FACT_MISSING_FOR_SESSION",
            )
            results.append(res)
            prev_state = "UNKNOWN"
            prev_exact = None
            prev_lower = None
            continue

        if obs.limit_status in {"NA_SUSPENDED", "NA_NO_PRICE_LIMIT"}:
            res = DailyLimitUpStreakResult(
                target_session=session,
                streak_status="NA_RESET",
                exact_streak=None,
                lower_bound_streak=None,
                is_first_board="NA",
                prior_session_status=prev_state,
                close_limit_up="NA",
                touched_limit_up="NA",
                one_word_limit_up="NA",
                reason=f"RESET_BY_{obs.limit_status}",
            )
            results.append(res)
            prev_state = "NA_RESET"
            prev_exact = None
            prev_lower = None
            continue

        if obs.limit_status != "ASSESSABLE" or obs.close_limit_up not in {
            "TRUE",
            "FALSE",
        }:
            res = DailyLimitUpStreakResult(
                target_session=session,
                streak_status="UNKNOWN",
                exact_streak=None,
                lower_bound_streak=None,
                is_first_board="UNKNOWN",
                prior_session_status=prev_state,
                close_limit_up=obs.close_limit_up,
                touched_limit_up=obs.touched_limit_up,
                one_word_limit_up=obs.one_word_limit_up,
                reason="LIMIT_STATUS_OR_CLOSE_LIMIT_UP_UNKNOWN",
            )
            results.append(res)
            prev_state = "UNKNOWN"
            prev_exact = None
            prev_lower = None
            continue

        if obs.close_limit_up == "FALSE":
            res = DailyLimitUpStreakResult(
                target_session=session,
                streak_status="EXACT",
                exact_streak=0,
                lower_bound_streak=0,
                is_first_board="FALSE",
                prior_session_status=prev_state,
                close_limit_up="FALSE",
                touched_limit_up=obs.touched_limit_up,
                one_word_limit_up=obs.one_word_limit_up,
                reason="NOT_CLOSE_LIMIT_UP",
            )
            results.append(res)
            prev_state = "ZERO"
            prev_exact = 0
            prev_lower = 0
            continue

        # obs.close_limit_up == "TRUE"
        if prev_state in {"ZERO", "NA_RESET"}:
            exact_k = 1
            res = DailyLimitUpStreakResult(
                target_session=session,
                streak_status="EXACT",
                exact_streak=exact_k,
                lower_bound_streak=exact_k,
                is_first_board="TRUE",
                prior_session_status=prev_state,
                close_limit_up="TRUE",
                touched_limit_up=obs.touched_limit_up,
                one_word_limit_up=obs.one_word_limit_up,
                reason=(
                    "FIRST_BOARD_AFTER_RESET"
                    if prev_state == "NA_RESET"
                    else "FIRST_BOARD_AFTER_NON_LIMIT"
                ),
            )
            results.append(res)
            prev_state = "EXACT_STREAK"
            prev_exact = exact_k
            prev_lower = exact_k
        elif prev_state == "EXACT_STREAK" and prev_exact is not None:
            exact_k = prev_exact + 1
            res = DailyLimitUpStreakResult(
                target_session=session,
                streak_status="EXACT",
                exact_streak=exact_k,
                lower_bound_streak=exact_k,
                is_first_board="FALSE",
                prior_session_status=prev_state,
                close_limit_up="TRUE",
                touched_limit_up=obs.touched_limit_up,
                one_word_limit_up=obs.one_word_limit_up,
                reason="CONSECUTIVE_EXACT_STREAK",
            )
            results.append(res)
            prev_state = "EXACT_STREAK"
            prev_exact = exact_k
            prev_lower = exact_k
        elif prev_state == "LOWER_BOUND_ONLY" and prev_lower is not None:
            next_lb = prev_lower + 1
            res = DailyLimitUpStreakResult(
                target_session=session,
                streak_status="LOWER_BOUND_ONLY",
                exact_streak=None,
                lower_bound_streak=next_lb,
                is_first_board="FALSE",
                prior_session_status=prev_state,
                close_limit_up="TRUE",
                touched_limit_up=obs.touched_limit_up,
                one_word_limit_up=obs.one_word_limit_up,
                reason="CONSECUTIVE_LOWER_BOUND_STREAK",
            )
            results.append(res)
            prev_state = "LOWER_BOUND_ONLY"
            prev_exact = None
            prev_lower = next_lb
        else:
            # LEFT_CENSORED 或 UNKNOWN：精确值未知，下界至少为 1，首板未知（不塞进一板）
            res = DailyLimitUpStreakResult(
                target_session=session,
                streak_status="LOWER_BOUND_ONLY",
                exact_streak=None,
                lower_bound_streak=1,
                is_first_board="UNKNOWN",
                prior_session_status=prev_state,
                close_limit_up="TRUE",
                touched_limit_up=obs.touched_limit_up,
                one_word_limit_up=obs.one_word_limit_up,
                reason=(
                    "LEFT_CENSORED_HISTORY_LOWER_BOUND"
                    if prev_state == "LEFT_CENSORED"
                    else "PRIOR_SESSION_UNKNOWN_LOWER_BOUND"
                ),
            )
            results.append(res)
            prev_state = "LOWER_BOUND_ONLY"
            prev_exact = None
            prev_lower = 1

    return tuple(results)


def summarize_market_limit_up_ladder(
    *,
    target_session: date,
    symbol_results: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """按 §11.4 汇总当日正式研究池日线连板梯队。

    梯队只统计精确计数（EXACT 且 exact_streak >= 1），未知下界（LOWER_BOUND_ONLY）单独列示，
    绝不塞进一板；日线不输出封板顺序或炸板次数。
    """
    exact_counts: Counter[int] = Counter()
    lower_bound_counts: Counter[int] = Counter()
    first_board_count = 0
    close_limit_up_count = 0
    one_word_limit_up_count = 0
    touched_only_count = 0
    na_reset_count = 0
    unknown_count = 0

    for item in symbol_results:
        status = item.get("streak_status")
        close_up = item.get("close_limit_up")
        touched_up = item.get("touched_limit_up")
        one_word_up = item.get("one_word_limit_up")
        if close_up == "TRUE":
            close_limit_up_count += 1
        if one_word_up == "TRUE":
            one_word_limit_up_count += 1
        if touched_up == "TRUE" and close_up == "FALSE":
            touched_only_count += 1
        if item.get("is_first_board") == "TRUE":
            first_board_count += 1

        if status == "EXACT":
            exact_k = item.get("exact_streak")
            if isinstance(exact_k, int) and exact_k >= 1:
                exact_counts[exact_k] += 1
        elif status == "LOWER_BOUND_ONLY":
            lb = item.get("lower_bound_streak")
            if isinstance(lb, int) and lb >= 1:
                lower_bound_counts[lb] += 1
        elif status == "NA_RESET":
            na_reset_count += 1
        else:
            unknown_count += 1

    exact_ladder = [
        {"boards": boards, "count": count}
        for boards, count in sorted(exact_counts.items())
    ]
    lower_bound_buckets = [
        {"min_boards": min_b, "count": count}
        for min_b, count in sorted(lower_bound_counts.items())
    ]
    max_exact_boards = max(exact_counts.keys()) if exact_counts else 0

    return {
        "target_session": target_session.isoformat(),
        "contract_version": LIMIT_UP_LADDER_CONTRACT_VERSION,
        "denominator_count": len(symbol_results),
        "close_limit_up_count": close_limit_up_count,
        "first_board_count": first_board_count,
        "one_word_limit_up_count": one_word_limit_up_count,
        "touched_limit_up_not_closed_count": touched_only_count,
        "max_exact_boards": max_exact_boards,
        "exact_ladder_total_count": sum(exact_counts.values()),
        "exact_ladder": exact_ladder,
        "lower_bound_only_total_count": sum(lower_bound_counts.values()),
        "lower_bound_only_buckets": lower_bound_buckets,
        "na_reset_count": na_reset_count,
        "unknown_count": unknown_count,
        "intraday_seal_order_supported": False,
        "intraday_open_count_supported": False,
    }
