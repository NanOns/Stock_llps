"""固定篮子基准、停牌估值、退市终值与 M5_R1 匹配对照纯算法核（ALG-35/36 / §11.9）。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from fractions import Fraction
from typing import Any, Mapping, Sequence


def compute_fixed_basket_path(
    *,
    member_weights: Mapping[str, Fraction],
    closes_by_symbol_session: Mapping[
        str, Mapping[date, tuple[Fraction | None, str | None, str]]
    ],
    t0_session: date,
    path_sessions: Sequence[date],
) -> dict[str, Any]:
    """按 T0 固定成员与原权重计算五期限篮子终点收益及每日收盘路径 MFE_CLOSE/MAE_CLOSE/MDD_CLOSE。

    closes_by_symbol_session[symbol][session] = (close_value, coordinate_id, quality)
    不因缺终值删成员重归一权重；任一成员缺失或坐标不可比时标 PARTIAL_UNVALUED/UNKNOWN。
    """
    if not member_weights or not path_sessions:
        return {
            "outcome_quality": "UNKNOWN",
            "basket_member_count": len(member_weights),
            "evaluable_member_count": 0,
            "endpoint_return": None,
            "mfe_close": None,
            "mae_close": None,
            "mdd_close": None,
            "reason_codes": ["EMPTY_FIXED_BASKET_OR_SESSIONS"],
        }
    total_weight = sum(member_weights.values(), Fraction(0))
    if total_weight != Fraction(1):
        raise ValueError("固定篮子权重之和必须等于1")

    t0_data: dict[str, tuple[Fraction, str]] = {}
    t0_reasons: list[str] = []
    for sym in sorted(member_weights):
        item = closes_by_symbol_session.get(sym, {}).get(t0_session)
        if (
            item is None
            or item[0] is None
            or item[0] <= 0
            or item[2] != "OBSERVED"
            or not item[1]
        ):
            t0_reasons.append(f"T0_MEMBER_UNUSABLE:{sym}")
        else:
            t0_data[sym] = (item[0], item[1])

    if len(t0_data) != len(member_weights):
        return {
            "outcome_quality": "UNKNOWN",
            "basket_member_count": len(member_weights),
            "evaluable_member_count": len(t0_data),
            "endpoint_return": None,
            "mfe_close": None,
            "mae_close": None,
            "mdd_close": None,
            "reason_codes": t0_reasons or ["T0_BASKET_NOT_READY"],
        }

    daily_returns: list[Fraction | None] = []
    endpoint_evaluable = 0
    reasons: list[str] = []
    end_session = path_sessions[-1]

    for day in path_sessions:
        day_sum = Fraction(0)
        day_evaluable = 0
        for sym, w in member_weights.items():
            c0, coord0 = t0_data[sym]
            item = closes_by_symbol_session.get(sym, {}).get(day)
            if (
                item is None
                or item[0] is None
                or item[0] <= 0
                or item[2] != "OBSERVED"
                or item[1] != coord0
            ):
                if day == end_session:
                    reasons.append(f"ENDPOINT_MEMBER_MISSING_OR_MISMATCH:{sym}")
                continue
            day_evaluable += 1
            day_sum += w * (item[0] / c0 - 1)
        if day == end_session:
            endpoint_evaluable = day_evaluable
        daily_returns.append(day_sum if day_evaluable == len(member_weights) else None)

    endpoint_r = daily_returns[-1]
    if endpoint_r is None:
        return {
            "outcome_quality": "PARTIAL_UNVALUED"
            if endpoint_evaluable > 0
            else "UNKNOWN",
            "basket_member_count": len(member_weights),
            "evaluable_member_count": endpoint_evaluable,
            "endpoint_return": None,
            "mfe_close": None,
            "mae_close": None,
            "mdd_close": None,
            "reason_codes": reasons or ["FIXED_BASKET_ENDPOINT_INCOMPLETE"],
        }

    if all(r is not None for r in daily_returns):
        valid_rs = [r for r in daily_returns if r is not None]
        mfe_close = max(Fraction(0), *valid_rs)
        mae_close = min(Fraction(0), *valid_rs)
        peak = Fraction(1)
        mdd_close = Fraction(0)
        for r in valid_rs:
            equity = Fraction(1) + r
            peak = max(peak, equity)
            mdd_close = min(mdd_close, equity / peak - 1)
        path_reasons: list[str] = []
    else:
        mfe_close = None
        mae_close = None
        mdd_close = None
        path_reasons = ["INTERMEDIATE_BASKET_SESSION_INCOMPLETE"]

    return {
        "outcome_quality": "OBSERVED",
        "basket_member_count": len(member_weights),
        "evaluable_member_count": endpoint_evaluable,
        "endpoint_return": endpoint_r,
        "mfe_close": mfe_close,
        "mae_close": mae_close,
        "mdd_close": mdd_close,
        "reason_codes": path_reasons,
    }


def evaluate_market_benchmark_eligibility(
    *,
    known_research_universe_count: int | None,
    eligible_basket_count: int,
) -> tuple[bool, Fraction | None, str | None]:
    """市场基准资格：|B| / |已知 Research Universe| >= 0.8 且 |B| >= 2。"""
    if known_research_universe_count is None or known_research_universe_count <= 0:
        return False, None, "RESEARCH_UNIVERSE_DENOMINATOR_UNKNOWN"
    ratio = Fraction(eligible_basket_count, known_research_universe_count)
    if eligible_basket_count < 2:
        return False, ratio, "MARKET_BENCHMARK_UNDER_MIN_MEMBERS_2"
    if ratio < Fraction(4, 5):
        return False, ratio, "MARKET_BENCHMARK_COVERAGE_BELOW_0_8"
    return True, ratio, None


def evaluate_suspension_marked_estimate(
    *,
    trade_status: str,
    t0_close: Fraction | None,
    last_actual_close: Fraction | None,
    quote_age_sessions: int | None,
    basis_verified: bool,
    adjustment_factor: Fraction = Fraction(1),
) -> dict[str, Any]:
    """确认停牌估值（MARKED_ESTIMATE）：仅当停牌已证、换基可核且 quote_age <= 20 会话时有效。"""
    if trade_status != "CONFIRMED_SUSPENSION":
        return {
            "quality": "UNKNOWN",
            "return_value": None,
            "reason": "NOT_CONFIRMED_SUSPENSION",
        }
    if not basis_verified or adjustment_factor <= 0:
        return {
            "quality": "UNKNOWN",
            "return_value": None,
            "reason": "SUSPENSION_BASIS_UNVERIFIED",
        }
    if quote_age_sessions is None or quote_age_sessions < 0 or quote_age_sessions > 20:
        return {
            "quality": "UNKNOWN",
            "return_value": None,
            "reason": "SUSPENSION_QUOTE_AGE_EXCEEDED_20",
        }
    if (
        t0_close is None
        or t0_close <= 0
        or last_actual_close is None
        or last_actual_close <= 0
    ):
        return {
            "quality": "UNKNOWN",
            "return_value": None,
            "reason": "SUSPENSION_PRICE_MISSING",
        }
    adjusted_close = last_actual_close * adjustment_factor
    return {
        "quality": "MARKED_ESTIMATE",
        "return_value": adjusted_close / t0_close - 1,
        "quote_age_sessions": quote_age_sessions,
        "reason": "CONFIRMED_SUSPENSION_MARKED_ESTIMATE",
    }


def evaluate_delisting_terminal_return(
    *,
    t0_close: Fraction | None,
    terminal_value: Fraction | None,
    terminal_evidence_verified: bool,
) -> dict[str, Any]:
    """退市终值收益：仅使用独立合法终值 >= 0（如 0 -> -1），缺终值不填 0。"""
    if not terminal_evidence_verified or terminal_value is None or terminal_value < 0:
        return {
            "quality": "UNKNOWN",
            "return_value": None,
            "reason": "DELISTING_TERMINAL_VALUE_MISSING",
        }
    if t0_close is None or t0_close <= 0:
        return {
            "quality": "UNKNOWN",
            "return_value": None,
            "reason": "T0_CLOSE_INVALID",
        }
    return {
        "quality": "OBSERVED",
        "return_value": terminal_value / t0_close - 1,
        "reason": "DELISTING_TERMINAL_VALUE_VERIFIED",
    }


@dataclass(frozen=True)
class M5Candidate:
    entity_id: str
    source_symbol: str
    rule_state: str
    safety_gate_passed: bool | None
    board_type: str | None
    is_st: bool | None
    industry_id: str | None
    p0: Fraction | None
    rps20: Fraction | None
    pos60: Fraction | None
    amt20: Fraction | None


def match_m5_controls_for_t0(
    target: M5Candidate,
    pool: Sequence[M5Candidate],
) -> dict[str, Any]:
    """确定性 M5_R1 匹配对照（§11.9）：同规则 NO_MATCH、安全门 TRUE、同板别/ST/主行业、固定卡尺、精确距离取前 5。"""
    if (
        target.safety_gate_passed is not True
        or not target.board_type
        or target.is_st is None
        or not target.industry_id
        or target.p0 is None
        or target.p0 <= 0
        or target.rps20 is None
        or target.pos60 is None
        or target.amt20 is None
        or target.amt20 <= 0
    ):
        return {
            "match_status": "UNKNOWN",
            "match_k": 0,
            "controls": [],
            "reason": "TARGET_M5_FEATURES_OR_INDUSTRY_UNKNOWN",
        }

    scored: list[tuple[Fraction, str, M5Candidate]] = []
    seen_entities: set[str] = {target.entity_id}
    for cand in pool:
        if cand.entity_id in seen_entities:
            continue
        if (
            cand.rule_state != "FALSE"
            or cand.safety_gate_passed is not True
            or cand.board_type != target.board_type
            or cand.is_st != target.is_st
            or cand.industry_id != target.industry_id
            or cand.p0 is None
            or cand.p0 <= 0
            or cand.rps20 is None
            or cand.pos60 is None
            or cand.amt20 is None
            or cand.amt20 <= 0
        ):
            continue
        d_rps = abs(cand.rps20 - target.rps20)
        d_pos = abs(cand.pos60 - target.pos60)
        amt_ratio = cand.amt20 / target.amt20
        if d_rps > Fraction(10) or d_pos > Fraction(1, 5):
            continue
        if amt_ratio < Fraction(1, 2) or amt_ratio > Fraction(2):
            continue
        dist = (
            d_rps / Fraction(10)
            + d_pos / Fraction(1, 5)
            + (abs(cand.amt20 - target.amt20) / max(cand.amt20, target.amt20))
            / Fraction(1, 2)
        )
        seen_entities.add(cand.entity_id)
        scored.append((dist, cand.entity_id, cand))

    scored.sort(key=lambda item: (item[0], item[1]))
    top = scored[:5]
    k = len(top)
    if k < 3:
        return {
            "match_status": "INSUFFICIENT_MATCH",
            "match_k": k,
            "controls": [
                {
                    "rank": idx,
                    "entity_id": item[1],
                    "source_symbol": item[2].source_symbol,
                    "distance": item[0],
                    "weight": None,
                }
                for idx, item in enumerate(top, start=1)
            ],
            "reason": "M5_CONTROLS_FEWER_THAN_3",
        }
    w = Fraction(1, k)
    return {
        "match_status": "MATCHED",
        "match_k": k,
        "controls": [
            {
                "rank": idx,
                "entity_id": item[1],
                "source_symbol": item[2].source_symbol,
                "distance": item[0],
                "weight": w,
            }
            for idx, item in enumerate(top, start=1)
        ],
        "reason": None,
    }
