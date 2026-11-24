"""MATH:ALG-26 + R3 多指数通用日级因子、主趋势与风格剪刀差纯算法引擎。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from fractions import Fraction
from typing import Any, Mapping, Sequence

MARKET_INDEX_CONTRACT_VERSION = "STOCK_A_MARKET_INDEX_FACTS_R1"


@dataclass(frozen=True)
class IndexDailyBar:
    """单个官方交易日、单条指数的日线点位输入（单位：INDEX_POINT）。"""

    session_date: date
    close_points: Fraction | Decimal | None
    preclose_points: Fraction | Decimal | None
    price_quality: str = "OBSERVED"
    bar_status: str = "TARGET_PRESENT"


def _pos_fraction(value: Any) -> Fraction | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, Fraction):
        return value if value > 0 else None
    if isinstance(value, Decimal):
        if not value.is_finite() or value <= 0:
            return None
        return Fraction(value)
    return None


def evaluate_index_day_factors(
    *,
    target_session: date,
    official_sessions: Sequence[date],
    bars_by_session: Mapping[date, IndexDailyBar | None],
) -> dict[str, Any]:
    """按独立窗口质量计算单条指数 5 项因子：ret1 / ret5 / ret20 / close_vs_ma20 / trend_state。

    单位约定：
    - ret1 / ret5 / ret20 / close_vs_ma20 均以精确 Fraction（fraction）返回；展示百分数时乘 100；
    - 短窗不因长窗（如趋势 25 会话）不足而连带 UNKNOWN。
    """
    if not official_sessions or target_session not in official_sessions:
        raise ValueError("TARGET_SESSION_NOT_IN_OFFICIAL_SESSIONS")
    sessions = tuple(official_sessions)
    for prev_s, cur_s in zip(sessions, sessions[1:]):
        if cur_s <= prev_s:
            raise ValueError("OFFICIAL_SESSIONS_MUST_BE_STRICTLY_INCREASING")
    target_idx = sessions.index(target_session)

    def _valid_close(idx: int) -> Fraction | None:
        if idx < 0:
            return None
        bar = bars_by_session.get(sessions[idx])
        if (
            bar is None
            or bar.bar_status != "TARGET_PRESENT"
            or bar.price_quality != "OBSERVED"
        ):
            return None
        return _pos_fraction(bar.close_points)

    target_bar = bars_by_session.get(target_session)
    close_t = _valid_close(target_idx)

    # 1. ret1 = close[t] / preclose[t] - 1
    ret1: Fraction | None = None
    ret1_q, ret1_r = "UNKNOWN_DATA", "TARGET_BAR_OR_PRECLOSE_MISSING"
    if close_t is not None and target_bar is not None:
        pre_t = _pos_fraction(target_bar.preclose_points)
        if pre_t is not None:
            ret1 = close_t / pre_t - Fraction(1)
            ret1_q, ret1_r = "OBSERVED", None

    # 2. ret5 (6 sessions) & 3. ret20 (21 sessions)
    def _ret_n(n: int) -> tuple[Fraction | None, str, str | None]:
        if close_t is None:
            return None, "UNKNOWN_DATA", "TARGET_CLOSE_MISSING"
        if target_idx < n:
            return None, "UNKNOWN_DATA", f"SHORT_HISTORY_NEED_{n + 1}_SESSIONS"
        base_c = _valid_close(target_idx - n)
        if base_c is None:
            return None, "UNKNOWN_DATA", f"START_ENDPOINT_T_MINUS_{n}_MISSING"
        return close_t / base_c - Fraction(1), "OBSERVED", None

    ret5, ret5_q, ret5_r = _ret_n(5)
    ret20, ret20_q, ret20_r = _ret_n(20)

    # 4. MA20[t] & close_vs_ma20 (20 sessions)
    def _ma20_at(end_idx: int) -> Fraction | None:
        if end_idx < 19:
            return None
        vals = [_valid_close(i) for i in range(end_idx - 19, end_idx + 1)]
        if any(v is None for v in vals):
            return None
        return sum((v for v in vals if v is not None), Fraction(0)) / Fraction(20)

    ma20_t = _ma20_at(target_idx)
    ma20_t5 = _ma20_at(target_idx - 5)

    close_vs_ma20: Fraction | None = None
    cvm_q, cvm_r = "UNKNOWN_DATA", "MA20_OR_CLOSE_UNAVAILABLE"
    if close_t is not None and ma20_t is not None:
        close_vs_ma20 = close_t / ma20_t - Fraction(1)
        cvm_q, cvm_r = "OBSERVED", None

    # 5. ALG-26 趋势轴（25 sessions）
    if close_t is None or ma20_t is None or ma20_t5 is None:
        trend_state = "UNKNOWN"
        trend_reason = (
            "SHORT_HISTORY_NEED_25_SESSIONS"
            if target_idx < 24
            else "MA20_OR_MA20_T_MINUS_5_MISSING"
        )
    elif close_t > ma20_t and ma20_t > ma20_t5:
        trend_state = "STRONG"
        trend_reason = "CLOSE_ABOVE_MA20_AND_MA20_RISING"
    elif close_t < ma20_t and ma20_t < ma20_t5:
        trend_state = "WEAK"
        trend_reason = "CLOSE_BELOW_MA20_AND_MA20_FALLING"
    else:
        trend_state = "NEUTRAL"
        trend_reason = "MIXED_CLOSE_AND_MA20_SLOPE"

    return {
        "target_session": target_session,
        "contract_version": MARKET_INDEX_CONTRACT_VERSION,
        "close_points": close_t,
        "ma20_points": ma20_t,
        "ma20_5d_ago_points": ma20_t5,
        "ret1_fraction": ret1,
        "ret1_quality": ret1_q,
        "ret1_reason": ret1_r,
        "ret5_fraction": ret5,
        "ret5_quality": ret5_q,
        "ret5_reason": ret5_r,
        "ret20_fraction": ret20,
        "ret20_quality": ret20_q,
        "ret20_reason": ret20_r,
        "close_vs_ma20_fraction": close_vs_ma20,
        "close_vs_ma20_quality": cvm_q,
        "close_vs_ma20_reason": cvm_r,
        "trend_state": trend_state,
        "trend_reason": trend_reason,
    }


def compute_style_spreads(
    *,
    index_facts_by_official_code: Mapping[str, Mapping[str, Any]],
    style_spread_pairs: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """计算同日同窗口多指数风格收益剪刀差（单位：百分点 pp，严禁点位相减）。"""
    spreads: list[dict[str, Any]] = []
    for pair in style_spread_pairs:
        left_code = str(pair["left_official_code"])
        right_code = str(pair["right_official_code"])
        left_fact = index_facts_by_official_code.get(left_code)
        right_fact = index_facts_by_official_code.get(right_code)
        item: dict[str, Any] = {
            "pair_id": pair["pair_id"],
            "label": pair["label"],
            "left_official_code": left_code,
            "right_official_code": right_code,
            "unit": "pp",
        }
        if left_fact is None or right_fact is None:
            item["status"] = "NOT_AVAILABLE"
            item["reason"] = "INDEX_OBSERVATION_UNAVAILABLE"
            for win in ("1d", "5d", "20d"):
                item[f"spread_{win}_pp"] = None
            spreads.append(item)
            continue

        any_known = False
        for win, key in (("1d", "ret1"), ("5d", "ret5"), ("20d", "ret20")):
            lq = left_fact.get(f"{key}_quality")
            rq = right_fact.get(f"{key}_quality")
            lv = left_fact.get(f"{key}_fraction")
            rv = right_fact.get(f"{key}_fraction")
            if (
                lq == "OBSERVED"
                and rq == "OBSERVED"
                and lv is not None
                and rv is not None
            ):
                diff_pp = (Fraction(lv) - Fraction(rv)) * Fraction(100)
                item[f"spread_{win}_pp"] = float(
                    round(Decimal(diff_pp.numerator) / Decimal(diff_pp.denominator), 4)
                )
                any_known = True
            else:
                item[f"spread_{win}_pp"] = None
        item["status"] = "AVAILABLE" if any_known else "UNKNOWN"
        item["reason"] = None if any_known else "INSUFFICIENT_WINDOW_FOR_SPREAD"
        spreads.append(item)
    return spreads
