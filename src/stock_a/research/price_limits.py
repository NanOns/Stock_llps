"""MATH:ALG-04 日线涨跌停限价纯引擎；严格绑定RAW_UNADJUSTED与已证实制度子域。"""

from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any


PRICE_LIMIT_CONTRACT_VERSION = "STOCK_A_PRICE_LIMIT_ENGINE_R1"
_TICK = Decimal("0.01")
# 非 ST 普通股票涨跌幅制度生效分段表：(生效起日元组(Y,M,D)或None, 涨跌幅比例, 规则ID)
_NON_ST_LIMIT_REGIMES: dict[
    str, tuple[tuple[tuple[int, int, int] | None, Decimal, str], ...]
] = {
    "MAIN_BOARD": ((None, Decimal("0.1000"), "CN_MAIN_NON_ST_10PCT_R1"),),
    "GEM": (
        ((2020, 8, 24), Decimal("0.2000"), "CN_GEM_NON_ST_20PCT_R1"),
        (None, Decimal("0.1000"), "CN_GEM_NON_ST_10PCT_PRE_20200824_R1"),
    ),
    "STAR_BOARD": (((2019, 7, 22), Decimal("0.2000"), "CN_STAR_NON_ST_20PCT_R1"),),
}
# 交易所风险警示股票（ST/*ST）涨跌幅制度生效分段表
_ST_LIMIT_REGIMES: dict[
    str, tuple[tuple[tuple[int, int, int] | None, Decimal, str], ...]
] = {
    "MAIN_BOARD": (
        ((2026, 7, 6), Decimal("0.1000"), "CN_MAIN_ST_10PCT_20260706_R1"),
        (None, Decimal("0.0500"), "CN_MAIN_ST_5PCT_PRE_20260706_R1"),
    ),
    "GEM": (
        ((2020, 8, 24), Decimal("0.2000"), "CN_GEM_ST_20PCT_R1"),
        (None, Decimal("0.0500"), "CN_GEM_ST_5PCT_PRE_20200824_R1"),
    ),
    "STAR_BOARD": (((2019, 7, 22), Decimal("0.2000"), "CN_STAR_ST_20PCT_R1"),),
}
# 注册制首次公开发行前5个交易日无涨跌幅限制的各板块生效起日（Y,M,D）
_IPO_NO_LIMIT_5D_EFFECTIVE: dict[str, tuple[int, int, int]] = {
    "STAR_BOARD": (2019, 7, 22),
    "GEM": (2020, 8, 24),
    "MAIN_BOARD": (2023, 4, 10),
}


def _resolve_regime_rule(
    table: dict[str, tuple[tuple[tuple[int, int, int] | None, Decimal, str], ...]],
    board: str,
    target_session: date,
) -> tuple[Decimal, str] | None:
    """按日期有效交易所制度分段表解析当日限价规则。"""
    regimes = table.get(board)
    if regimes is None:
        return None
    target_ymd = (target_session.year, target_session.month, target_session.day)
    for effective_from, limit_pct, rule_id in regimes:
        if effective_from is None or target_ymd >= effective_from:
            return (limit_pct, rule_id)
    return None


def _to_decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None
    if not parsed.is_finite() or parsed <= 0:
        return None
    return parsed


def _unknown_result(
    *,
    source_symbol: str,
    market: str,
    board: str,
    target_session: date,
    rule_id: str,
    reason: str,
    limit_status: str = "UNKNOWN",
    flag_value: str = "UNKNOWN",
    quality: str = "UNKNOWN",
) -> dict[str, Any]:
    return {
        "target_session": target_session,
        "source_symbol": source_symbol,
        "market": market,
        "board": board,
        "research_eligible": board in {"MAIN_BOARD", "GEM"},
        "price_coordinate": "RAW_UNADJUSTED",
        "contract_version": PRICE_LIMIT_CONTRACT_VERSION,
        "rule_id": rule_id,
        "limit_status": limit_status,
        "limit_pct": None,
        "reference_price": None,
        "upper_limit": None,
        "lower_limit": None,
        "close_limit_up": flag_value,
        "close_limit_down": flag_value,
        "touched_limit_up": flag_value,
        "touched_limit_down": flag_value,
        "one_word_limit_up": flag_value,
        "one_word_limit_down": flag_value,
        "quality": quality,
        "reason": reason,
    }


def evaluate_daily_price_limit(
    *,
    source_symbol: str,
    market: str,
    board: str,
    target_session: date,
    price_coordinate: str,
    bar_status: str,
    raw_tradestatus_value: str | None,
    raw_is_st_value: str | None,
    raw_preclose: Any,
    prev_raw_close: Any,
    history_present_count: int,
    raw_open: Any,
    raw_high: Any,
    raw_low: Any,
    raw_close: Any,
    ipo_date: date | None = None,
    listing_session_ordinal: int | None = None,
) -> dict[str, Any]:
    """按日期有效板别、ST、停牌、上市阶段与RAW参考价计算日线涨跌停可判定事实。"""
    if price_coordinate != "RAW_UNADJUSTED":
        raise ValueError("PRICE_LIMIT_REQUIRES_RAW_UNADJUSTED")
    if bar_status != "TARGET_PRESENT":
        return _unknown_result(
            source_symbol=source_symbol,
            market=market,
            board=board,
            target_session=target_session,
            rule_id="UNKNOWN_MISSING_BAR",
            reason="RAW_BAR_NOT_PRESENT",
        )
    if raw_tradestatus_value == "0":
        return _unknown_result(
            source_symbol=source_symbol,
            market=market,
            board=board,
            target_session=target_session,
            rule_id="NA_SUSPENDED",
            reason="CONFIRMED_SUSPENSION_NO_TRADING",
            limit_status="NA_SUSPENDED",
            flag_value="NA",
            quality="NA_SUSPENDED",
        )
    if raw_tradestatus_value != "1":
        return _unknown_result(
            source_symbol=source_symbol,
            market=market,
            board=board,
            target_session=target_session,
            rule_id="UNKNOWN_TRADESTATUS",
            reason="TRADESTATUS_UNKNOWN",
        )
    if raw_is_st_value not in {"0", "1"}:
        return _unknown_result(
            source_symbol=source_symbol,
            market=market,
            board=board,
            target_session=target_session,
            rule_id="UNKNOWN_ST_STATUS",
            reason="IS_ST_STATUS_UNKNOWN",
        )
    if raw_is_st_value == "1":
        board_rule = _resolve_regime_rule(_ST_LIMIT_REGIMES, board, target_session)
        if board_rule is None:
            return _unknown_result(
                source_symbol=source_symbol,
                market=market,
                board=board,
                target_session=target_session,
                rule_id="UNKNOWN_ST_REGIME",
                reason="ST_LIMIT_REGIME_UNVERIFIED",
            )
    else:
        board_rule = _resolve_regime_rule(_NON_ST_LIMIT_REGIMES, board, target_session)
        if board_rule is None:
            return _unknown_result(
                source_symbol=source_symbol,
                market=market,
                board=board,
                target_session=target_session,
                rule_id="UNKNOWN_BOARD_REGIME",
                reason="BOARD_LIMIT_REGIME_UNSUPPORTED",
            )
    limit_pct, rule_id = board_rule

    # 上市特殊期判定：优先依据权威 ipo_date 与官方交易会话序号；无 ipo_date 时保留左截尾保护
    if ipo_date is not None:
        if target_session < ipo_date:
            return _unknown_result(
                source_symbol=source_symbol,
                market=market,
                board=board,
                target_session=target_session,
                rule_id="UNKNOWN_PRE_LISTING",
                reason="TARGET_SESSION_BEFORE_IPO_DATE",
            )
        ipo_ymd = (ipo_date.year, ipo_date.month, ipo_date.day)
        reg_eff = _IPO_NO_LIMIT_5D_EFFECTIVE.get(board)
        if listing_session_ordinal is not None:
            if type(listing_session_ordinal) is not int or listing_session_ordinal < 1:
                return _unknown_result(
                    source_symbol=source_symbol,
                    market=market,
                    board=board,
                    target_session=target_session,
                    rule_id="UNKNOWN_IPO_ORDINAL",
                    reason="LISTING_SESSION_ORDINAL_INVALID",
                )
            if listing_session_ordinal <= 5:
                if reg_eff is not None and ipo_ymd >= reg_eff:
                    return _unknown_result(
                        source_symbol=source_symbol,
                        market=market,
                        board=board,
                        target_session=target_session,
                        rule_id="CN_IPO_FIRST_5_SESSIONS_NO_LIMIT_R1",
                        reason="IPO_FIRST_5_SESSIONS_NO_PRICE_LIMIT",
                        limit_status="NA_NO_PRICE_LIMIT",
                        flag_value="NA",
                        quality="NA_NO_PRICE_LIMIT",
                    )
                return _unknown_result(
                    source_symbol=source_symbol,
                    market=market,
                    board=board,
                    target_session=target_session,
                    rule_id="UNKNOWN_LEGACY_IPO_REGIME",
                    reason="IPO_SPECIAL_REGIME_PRE_REGISTRATION_UNVERIFIED",
                )
        elif (target_session - ipo_date).days < 14:
            return _unknown_result(
                source_symbol=source_symbol,
                market=market,
                board=board,
                target_session=target_session,
                rule_id="LEFT_CENSORED_HISTORY_UNKNOWN",
                reason="IPO_ORDINAL_REQUIRED_FOR_RECENT_LISTING",
            )
    elif type(history_present_count) is not int or history_present_count < 6:
        return _unknown_result(
            source_symbol=source_symbol,
            market=market,
            board=board,
            target_session=target_session,
            rule_id="LEFT_CENSORED_HISTORY_UNKNOWN",
            reason="IPO_DATE_NOT_STORED_AND_HISTORY_WINDOW_INSUFFICIENT",
        )

    preclose = _to_decimal(raw_preclose)
    if preclose is None:
        return _unknown_result(
            source_symbol=source_symbol,
            market=market,
            board=board,
            target_session=target_session,
            rule_id="UNKNOWN_PRECLOSE_MISSING",
            reason="RAW_PRECLOSE_MISSING_OR_NON_POSITIVE",
        )
    # 按用户决定与 R3 裁定：直接消费同日同捕获合格 RAW preclose（除权日为供应方调整后前收），
    # 不要求等于上一日真实收盘 prev_raw_close（prev_raw_close 仅保留作诊断参考）。
    _ = prev_raw_close

    upper_limit = (preclose * (Decimal("1") + limit_pct)).quantize(
        _TICK, rounding=ROUND_HALF_UP
    )
    lower_limit = (preclose * (Decimal("1") - limit_pct)).quantize(
        _TICK, rounding=ROUND_HALF_UP
    )
    open_p = _to_decimal(raw_open)
    high_p = _to_decimal(raw_high)
    low_p = _to_decimal(raw_low)
    close_p = _to_decimal(raw_close)

    def _flag(cond: bool | None) -> str:
        if cond is None:
            return "UNKNOWN"
        return "TRUE" if cond else "FALSE"

    close_up = _flag(None if close_p is None else close_p == upper_limit)
    close_down = _flag(None if close_p is None else close_p == lower_limit)
    touched_up = _flag(None if high_p is None else high_p >= upper_limit)
    touched_down = _flag(None if low_p is None else low_p <= lower_limit)
    ohlc_ok = (
        open_p is not None
        and high_p is not None
        and low_p is not None
        and close_p is not None
    )
    one_word_up = _flag(
        None
        if not ohlc_ok
        else (
            open_p == upper_limit
            and high_p == upper_limit
            and low_p == upper_limit
            and close_p == upper_limit
        )
    )
    one_word_down = _flag(
        None
        if not ohlc_ok
        else (
            open_p == lower_limit
            and high_p == lower_limit
            and low_p == lower_limit
            and close_p == lower_limit
        )
    )

    return {
        "target_session": target_session,
        "source_symbol": source_symbol,
        "market": market,
        "board": board,
        "research_eligible": board in {"MAIN_BOARD", "GEM"},
        "price_coordinate": "RAW_UNADJUSTED",
        "contract_version": PRICE_LIMIT_CONTRACT_VERSION,
        "rule_id": rule_id,
        "limit_status": "ASSESSABLE",
        "limit_pct": limit_pct,
        "reference_price": preclose,
        "upper_limit": upper_limit,
        "lower_limit": lower_limit,
        "close_limit_up": close_up,
        "close_limit_down": close_down,
        "touched_limit_up": touched_up,
        "touched_limit_down": touched_down,
        "one_word_limit_up": one_word_up,
        "one_word_limit_down": one_word_down,
        "quality": "VERIFIED_RAW_NON_PIT",
        "reason": "ASSESSABLE_RAW_UNADJUSTED",
    }
