"""R03 正式 Rotation 生命周期纯核算法（ALG-24 / §11.3 ROTATION_EXECUTION_R2）。

严格遵循 §11.3 有序 10 步状态机：
- 仅在纯内存合成/规范数据上运行，不执行磁盘或网络 I/O；
- 冻结篮子 B = M[t0-1]，权重 w_j = 1/|B|，不因后续成员增删改权重；
- OUT 必须绑定 t-1 已接受板块 D2（WARM/CONFIRMED），绝不读当日 D2 或上一日 raw_warm；
- H0 使用 t0-1 合法 RPS20 >= 80 集合。
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from fractions import Fraction
from typing import Any

from stock_a.research.sector_native import tri, tri_and


@dataclass(frozen=True)
class FrozenRotationBasket:
    """t0 出生时冻结的 Rotation 篮子与基准快照。"""

    episode_id: str
    board_id: str
    board_kind: str
    t0_session: str
    members: tuple[str, ...]  # B = M[t0-1]
    weights: Mapping[str, Fraction]  # 1/|B|
    b_prices: Mapping[str, Fraction]  # t0-1 合法 ACTUAL 正价格
    p_prices: Mapping[str, Fraction]  # t0 合法 ACTUAL 正价格
    s0_seed: frozenset[str]  # t0 已知 Seed TRUE 集合
    u0_up: frozenset[str]  # t0 正 ret1 集合
    h0_strong: frozenset[str]  # t0-1 RPS20 >= 80 集合
    s0_known: bool = True
    u0_known: bool = True
    h0_known: bool = True
    left_censored: bool = False


def evaluate_pulse_raw(
    *,
    dq5: Fraction | None,
    breadth_delta1: Fraction | None,
) -> dict[str, Any]:
    """PULSE raw = dq5 >= 10 AND breadth_delta1 >= 0.05。"""
    return tri_and(
        tri(dq5 >= Fraction(10), "DQ5_BELOW_10")
        if dq5 is not None
        else tri(None, "DQ5_UNKNOWN"),
        tri(breadth_delta1 >= Fraction(5, 100), "BREADTH_DELTA1_BELOW_0_05")
        if breadth_delta1 is not None
        else tri(None, "BREADTH_DELTA1_UNKNOWN"),
    )


def create_frozen_basket(
    *,
    episode_id: str,
    board_id: str,
    board_kind: str,
    t0_session: str,
    members_t0_minus_1: Sequence[str],
    prices_t0_minus_1: Mapping[str, Fraction | None],
    prices_t0: Mapping[str, Fraction | None],
    seed_t0: Mapping[str, str],
    ret1_t0: Mapping[str, Fraction | None],
    rps20_t0_minus_1: Mapping[str, Fraction | None],
    left_censored: bool = False,
) -> tuple[FrozenRotationBasket | None, str | None]:
    """冻结 B=M[t0-1]、双基准价格及 S0/U0/H0；缺必要出生价格时返回 (None, 原因)。"""
    unique_members = tuple(dict.fromkeys(members_t0_minus_1))
    if len(unique_members) < 5:
        return None, "BASKET_SIZE_BELOW_5"

    b_prices: dict[str, Fraction] = {}
    p_prices: dict[str, Fraction] = {}
    for eid in unique_members:
        bp = prices_t0_minus_1.get(eid)
        pp = prices_t0.get(eid)
        if bp is None or bp <= 0 or pp is None or pp <= 0:
            return None, "BIRTH_ACTUAL_PRICE_MISSING"
        b_prices[eid] = bp
        p_prices[eid] = pp

    weight = Fraction(1, len(unique_members))
    weights = {eid: weight for eid in unique_members}

    s0_known = all(seed_t0.get(eid) in {"TRUE", "FALSE"} for eid in unique_members)
    s0 = frozenset(eid for eid in unique_members if seed_t0.get(eid) == "TRUE")

    u0_known = all(ret1_t0.get(eid) is not None for eid in unique_members)
    u0 = frozenset(
        eid
        for eid in unique_members
        if ret1_t0.get(eid) is not None and ret1_t0[eid] > 0  # type: ignore[operator]
    )

    h0_known = all(rps20_t0_minus_1.get(eid) is not None for eid in unique_members)
    h0 = frozenset(
        eid
        for eid in unique_members
        if rps20_t0_minus_1.get(eid) is not None
        and rps20_t0_minus_1[eid] >= Fraction(80)  # type: ignore[operator]
    )

    return (
        FrozenRotationBasket(
            episode_id=episode_id,
            board_id=board_id,
            board_kind=board_kind,
            t0_session=t0_session,
            members=unique_members,
            weights=weights,
            b_prices=b_prices,
            p_prices=p_prices,
            s0_seed=s0,
            u0_up=u0,
            h0_strong=h0,
            s0_known=s0_known,
            u0_known=u0_known,
            h0_known=h0_known,
            left_censored=left_censored,
        ),
        None,
    )


def evaluate_basket_metrics(
    basket: FrozenRotationBasket,
    *,
    prices_u: Mapping[str, Fraction | None],
    seed_u: Mapping[str, str],
    seed_prev: Mapping[str, str],
    rps20_u: Mapping[str, Fraction | None],
    breadth_delta1: Fraction | None,
    top1_concentration: Fraction | None,
) -> dict[str, Any]:
    """计算后继日 u 的 formation_R、post_R、留存与扩散指标。"""
    form_r: Fraction | None = Fraction(0)
    post_r: Fraction | None = Fraction(0)
    for eid in basket.members:
        pu = prices_u.get(eid)
        if pu is None or pu <= 0:
            form_r = None
            post_r = None
            break
        w = basket.weights[eid]
        form_r += w * (pu / basket.b_prices[eid] - 1)
        post_r += w * (pu / basket.p_prices[eid] - 1)

    # Seed 留存
    if not basket.s0_known:
        seed_ret = {"state": "UNKNOWN", "value": None, "reason": "S0_UNKNOWN"}
    elif not basket.s0_seed:
        seed_ret = {"state": "NA", "value": None, "reason": "S0_EMPTY"}
    else:
        states = [seed_u.get(eid, "UNKNOWN") for eid in basket.s0_seed]
        if any(s not in {"TRUE", "FALSE"} for s in states):
            seed_ret = {"state": "UNKNOWN", "value": None, "reason": "SEED_U_UNKNOWN"}
        else:
            val = Fraction(sum(1 for s in states if s == "TRUE"), len(basket.s0_seed))
            seed_ret = {"state": "OBSERVED", "value": val, "reason": None}

    # Breadth 留存：U0 中当前 C >= 冻结 p_j 的比例（等号计保留）
    if not basket.u0_known:
        breadth_ret = {"state": "UNKNOWN", "value": None, "reason": "U0_UNKNOWN"}
    elif not basket.u0_up:
        breadth_ret = {"state": "NA", "value": None, "reason": "U0_EMPTY"}
    else:
        if any(prices_u.get(eid) is None for eid in basket.u0_up):
            breadth_ret = {
                "state": "UNKNOWN",
                "value": None,
                "reason": "PRICE_U_UNKNOWN",
            }
        else:
            kept = sum(
                1
                for eid in basket.u0_up
                if prices_u[eid] >= basket.p_prices[eid]  # type: ignore[operator]
            )
            breadth_ret = {
                "state": "OBSERVED",
                "value": Fraction(kept, len(basket.u0_up)),
                "reason": None,
            }

    # Strong 留存：H0 中当前 RPS20 >= 80
    if not basket.h0_known:
        strong_ret = {"state": "UNKNOWN", "value": None, "reason": "H0_UNKNOWN"}
    elif not basket.h0_strong:
        strong_ret = {"state": "NA", "value": None, "reason": "H0_EMPTY"}
    else:
        if any(rps20_u.get(eid) is None for eid in basket.h0_strong):
            strong_ret = {
                "state": "UNKNOWN",
                "value": None,
                "reason": "RPS20_U_UNKNOWN",
            }
        else:
            kept = sum(
                1
                for eid in basket.h0_strong
                if rps20_u[eid] >= Fraction(80)  # type: ignore[operator]
            )
            strong_ret = {
                "state": "OBSERVED",
                "value": Fraction(kept, len(basket.h0_strong)),
                "reason": None,
            }

    # 扩散：固定 B 中相邻可评估会话 Seed FALSE->TRUE entered, TRUE->FALSE exited
    entered = 0
    exited = 0
    diffusion_unknown = False
    for eid in basket.members:
        sp = seed_prev.get(eid, "UNKNOWN")
        su = seed_u.get(eid, "UNKNOWN")
        if sp not in {"TRUE", "FALSE"} or su not in {"TRUE", "FALSE"}:
            diffusion_unknown = True
            break
        if sp == "FALSE" and su == "TRUE":
            entered += 1
        elif sp == "TRUE" and su == "FALSE":
            exited += 1

    if diffusion_unknown or breadth_delta1 is None or top1_concentration is None:
        diffusion = tri(None, "DIFFUSION_INPUT_UNKNOWN")
    else:
        diffusion = tri(
            entered > exited
            and breadth_delta1 > Fraction(0)
            and top1_concentration <= Fraction(1, 2),
            "DIFFUSION_CRITERIA_UNMET",
        )

    # early 谓词（§11.3 三态 OR，两留存均 NA 时 early UNKNOWN）
    sr_branch = (
        tri(seed_ret["value"] >= Fraction(2, 3), "SEED_RETENTION_BELOW_2_3")
        if seed_ret["state"] == "OBSERVED"
        else None
        if seed_ret["state"] == "NA"
        else tri(None, seed_ret["reason"])
    )
    br_branch = (
        tri(breadth_ret["value"] >= Fraction(3, 5), "BREADTH_RETENTION_BELOW_0_6")
        if breadth_ret["state"] == "OBSERVED"
        else None
        if breadth_ret["state"] == "NA"
        else tri(None, breadth_ret["reason"])
    )
    if sr_branch is None and br_branch is None:
        retention_or = tri(None, "BOTH_RETENTIONS_NA")
    elif sr_branch is None:
        retention_or = br_branch  # type: ignore[assignment]
    elif br_branch is None:
        retention_or = sr_branch
    else:
        from stock_a.research.sector_native import tri_or

        retention_or = tri_or(sr_branch, br_branch)

    early = tri_and(
        tri(form_r > Fraction(0), "FORMATION_R_NOT_POSITIVE")
        if form_r is not None
        else tri(None, "FORMATION_R_UNKNOWN"),
        retention_or,
        tri(breadth_delta1 >= Fraction(0), "BREADTH_DELTA1_NEGATIVE")
        if breadth_delta1 is not None
        else tri(None, "BREADTH_DELTA1_UNKNOWN"),
        tri(top1_concentration <= Fraction(1, 3), "TOP1_ABOVE_1_3")
        if top1_concentration is not None
        else tri(None, "TOP1_UNKNOWN"),
    )

    if strong_ret["state"] == "NA":
        mature = {"state": "NA", "reason": "H0_EMPTY"}
    elif strong_ret["state"] == "UNKNOWN":
        mature = tri_and(early, tri(None, strong_ret["reason"]))
    else:
        mature = tri_and(
            early,
            tri(
                strong_ret["value"] >= Fraction(3, 4),
                "STRONG_RETENTION_BELOW_0_75",
            ),
        )

    return {
        "formation_r": form_r,
        "post_r": post_r,
        "seed_retention": seed_ret,
        "breadth_retention": breadth_ret,
        "strong_retention": strong_ret,
        "entered_count": entered,
        "exited_count": exited,
        "diffusion": diffusion,
        "early": early,
        "mature": mature,
    }


def step_rotation_state_machine(
    *,
    contract_valid: bool,
    previous_episode_state: Mapping[str, Any] | None,
    t_minus_1_d2_maturity: str | None,
    dq5: Fraction | None,
    breadth_delta1: Fraction | None,
    pulse_raw: Mapping[str, Any],
    basket_metrics: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """严格按 §11.3 10 步有序合同推进单个 Rotation 状态步。"""
    # 1. 精确合同/前态/坐标/日历错 -> 拒绝，不改旧终态
    if not contract_valid:
        return {
            "state": "REJECTED",
            "reason": "CONTRACT_OR_SCOPE_INVALID",
            "preserve_prior": True,
        }

    raw_prev_state = previous_episode_state.get("state") if previous_episode_state else None
    effective_prev_state = (
        previous_episode_state.get("last_known_state")
        if raw_prev_state == "UNKNOWN" and previous_episode_state is not None
        else raw_prev_state
    )
    has_active_basket = (
        previous_episode_state is not None
        and effective_prev_state not in {None, "NONE", "FAILED", "OUT"}
        and basket_metrics is not None
    )

    if not has_active_basket:
        # 终态后需 armed 才允许新 child；初次或已 armed 且满足 PULSE raw -> 出生 PULSE
        prev_market_age = (
            int(previous_episode_state.get("market_age", 0))
            if previous_episode_state is not None
            else 0
        )
        if effective_prev_state in {"FAILED", "OUT"}:
            armed = bool(previous_episode_state.get("armed", False))  # type: ignore[union-attr]
        else:
            armed = True
        if not armed and effective_prev_state in {"FAILED", "OUT"}:
            if pulse_raw["state"] == "FALSE":
                return {
                    "state": effective_prev_state,
                    "last_known_state": effective_prev_state,
                    "stale": False,
                    "armed": True,
                    "market_age": prev_market_age + 1,
                    "eval_age": previous_episode_state.get("eval_age", 0),  # type: ignore[union-attr]
                    "accepted_once": previous_episode_state.get("accepted_once", False),  # type: ignore[union-attr]
                    "out_neg_streak": 0,
                    "prev_mature_true": False,
                    "dq5": dq5,
                    "reason": "REARMED_BY_CONFIRMED_FALSE_PULSE",
                }
            return {
                "state": effective_prev_state,
                "last_known_state": effective_prev_state,
                "stale": False,
                "armed": False,
                "market_age": prev_market_age + 1,
                "eval_age": previous_episode_state.get("eval_age", 0),  # type: ignore[union-attr]
                "accepted_once": previous_episode_state.get("accepted_once", False),  # type: ignore[union-attr]
                "out_neg_streak": 0,
                "prev_mature_true": False,
                "dq5": dq5,
                "reason": "TERMINATED_WAITING_FOR_FALSE_PULSE_REARM",
            }

        if pulse_raw["state"] == "TRUE":
            return {
                "state": "PULSE",
                "last_known_state": "PULSE",
                "stale": False,
                "armed": False,
                "market_age": 0,
                "eval_age": 0,
                "accepted_once": False,
                "out_neg_streak": 0,
                "prev_mature_true": False,
                "dq5": dq5,
                "reason": None,
            }
        if pulse_raw["state"] == "FALSE":
            return {
                "state": "NONE",
                "last_known_state": "NONE",
                "stale": False,
                "armed": True,
                "market_age": 0,
                "eval_age": 0,
                "accepted_once": False,
                "out_neg_streak": 0,
                "prev_mature_true": False,
                "dq5": dq5,
                "reason": None,
            }
        return {
            "state": "UNKNOWN",
            "last_known_state": effective_prev_state,
            "stale": effective_prev_state is not None,
            "armed": armed,
            "market_age": (prev_market_age + 1) if previous_episode_state else 0,
            "eval_age": 0,
            "accepted_once": False,
            "out_neg_streak": 0,
            "prev_mature_true": False,
            "dq5": None,
            "reason": pulse_raw.get("reason"),
        }

    # 活动 episode 推进
    assert previous_episode_state is not None
    assert basket_metrics is not None
    prev_state = str(effective_prev_state)
    prev_was_unknown = raw_prev_state == "UNKNOWN"
    prev_age = int(previous_episode_state.get("eval_age", 0))
    prev_market_age = int(previous_episode_state.get("market_age", prev_age))
    next_age = prev_age + 1
    next_market_age = prev_market_age + 1
    accepted_once = bool(previous_episode_state.get("accepted_once", False))
    prev_out_streak = (
        0 if prev_was_unknown else int(previous_episode_state.get("out_neg_streak", 0))
    )
    prev_mature_true = (
        False
        if prev_was_unknown
        else bool(previous_episode_state.get("prev_mature_true", previous_episode_state.get("mature_true", False)))
    )
    prev_dq5 = None if prev_was_unknown else previous_episode_state.get("dq5")

    form_r = basket_metrics["formation_r"]
    early = basket_metrics["early"]
    mature = basket_metrics["mature"]
    diffusion = basket_metrics["diffusion"]

    def _stale_unknown(reason: str) -> dict[str, Any]:
        return {
            "state": "UNKNOWN",
            "last_known_state": prev_state,
            "stale": True,
            "armed": False,
            "market_age": next_market_age,
            "eval_age": prev_age,
            "accepted_once": accepted_once,
            "out_neg_streak": 0,
            "prev_mature_true": prev_mature_true,
            "dq5": prev_dq5,
            "reason": reason,
        }

    # 2. 后继第 1..5 可评估日：formation_R <= 0 且 breadth_delta1 <= -0.05 -> FAILED（Kleene 归约）
    if 1 <= next_age <= 5:
        fail_pred = tri_and(
            tri(form_r <= Fraction(0), "FORM_R_POSITIVE")
            if form_r is not None
            else tri(None, "FORM_R_UNKNOWN"),
            tri(breadth_delta1 <= Fraction(-5, 100), "BREADTH_DELTA1_ABOVE_NEG0_05")
            if breadth_delta1 is not None
            else tri(None, "BREADTH_DELTA1_UNKNOWN"),
        )
        if fail_pred["state"] == "TRUE":
            return {
                "state": "FAILED",
                "last_known_state": "FAILED",
                "stale": False,
                "armed": False,
                "market_age": next_market_age,
                "eval_age": next_age,
                "accepted_once": accepted_once,
                "out_neg_streak": 0,
                "prev_mature_true": False,
                "dq5": dq5,
                "reason": "EARLY_FAILURE_THRESHOLD_HIT",
            }
        if fail_pred["state"] == "UNKNOWN":
            return _stale_unknown("FAIL_CHECK_INPUT_UNKNOWN")

    # 3. OUT：t-1 已接受板块 D2 为 WARM/CONFIRMED 且 dq5 < 0 与 breadth_delta1 < 0 连续 2 个相邻可评估会话
    out_streak = prev_out_streak
    d2_is_warm_or_conf = (
        tri(True)
        if t_minus_1_d2_maturity in {"WARM", "CONFIRMED"}
        else tri(False, "D2_NOT_WARM_OR_CONFIRMED")
        if t_minus_1_d2_maturity in {"NONE", "PREWATCH"}
        else tri(None, "T_MINUS_1_D2_MATURITY_UNKNOWN")
    )
    out_neg_today = tri_and(
        d2_is_warm_or_conf,
        tri(dq5 < Fraction(0), "DQ5_NON_NEGATIVE")
        if dq5 is not None
        else tri(None, "DQ5_UNKNOWN"),
        tri(breadth_delta1 < Fraction(0), "BREADTH_DELTA1_NON_NEGATIVE")
        if breadth_delta1 is not None
        else tri(None, "BREADTH_DELTA1_UNKNOWN"),
    )
    if out_neg_today["state"] == "TRUE":
        out_streak = prev_out_streak + 1
        if out_streak >= 2:
            return {
                "state": "OUT",
                "last_known_state": "OUT",
                "stale": False,
                "armed": False,
                "market_age": next_market_age,
                "eval_age": next_age,
                "accepted_once": accepted_once,
                "out_neg_streak": out_streak,
                "prev_mature_true": False,
                "dq5": dq5,
                "reason": "TWO_SESSION_ROTATION_OUT",
            }
    elif out_neg_today["state"] == "FALSE":
        out_streak = 0
    else:
        return _stale_unknown(str(out_neg_today.get("reason") or "OUT_INPUT_UNKNOWN"))

    # 4. 尚未 accepted_once 且当前后继有效计数 >= 6 -> FAILED
    if not accepted_once and next_age >= 6:
        return {
            "state": "FAILED",
            "last_known_state": "FAILED",
            "stale": False,
            "armed": False,
            "market_age": next_market_age,
            "eval_age": next_age,
            "accepted_once": False,
            "out_neg_streak": out_streak,
            "prev_mature_true": False,
            "dq5": dq5,
            "reason": "NOT_ACCEPTED_BY_SESSION_6",
        }

    # 5. REACCELERATING：前状态在成熟集合、前相邻会话 mature TRUE 且 prev_dq5 <= 0，今 pulse 和 mature TRUE
    if (
        not prev_was_unknown
        and prev_state in {"ACCEPTED", "EXPANDING", "REACCELERATING"}
        and prev_mature_true
        and prev_dq5 is not None
        and prev_dq5 <= Fraction(0)
        and mature["state"] != "NA"
    ):
        reaccel_pred = tri_and(
            pulse_raw,
            tri(
                True
                if mature["state"] == "TRUE"
                else False
                if mature["state"] == "FALSE"
                else None,
                "MATURE_NOT_TRUE",
            ),
        )
        if reaccel_pred["state"] == "TRUE":
            return {
                "state": "REACCELERATING",
                "last_known_state": "REACCELERATING",
                "stale": False,
                "armed": False,
                "market_age": next_market_age,
                "eval_age": next_age,
                "accepted_once": True,
                "out_neg_streak": out_streak,
                "prev_mature_true": True,
                "dq5": dq5,
                "reason": None,
            }
        if reaccel_pred["state"] == "UNKNOWN":
            return _stale_unknown("REACCELERATING_INPUT_UNKNOWN")

    # 6. EXPANDING：后继计数 >= 2、前状态在成熟集合（IN 不可直接 EXPANDING）、今 mature 与 diffusion TRUE
    if (
        next_age >= 2
        and prev_state in {"ACCEPTED", "EXPANDING", "REACCELERATING"}
        and mature["state"] != "NA"
    ):
        exp_pred = tri_and(
            tri(
                True
                if mature["state"] == "TRUE"
                else False
                if mature["state"] == "FALSE"
                else None,
                "MATURE_NOT_TRUE",
            ),
            diffusion,
        )
        if exp_pred["state"] == "TRUE":
            return {
                "state": "EXPANDING",
                "last_known_state": "EXPANDING",
                "stale": False,
                "armed": False,
                "market_age": next_market_age,
                "eval_age": next_age,
                "accepted_once": True,
                "out_neg_streak": out_streak,
                "prev_mature_true": True,
                "dq5": dq5,
                "reason": None,
            }
        if exp_pred["state"] == "UNKNOWN":
            return _stale_unknown("EXPANDING_INPUT_UNKNOWN")

    # 7. ACCEPTED：后继计数 >= 2，early TRUE 且 breadth_delta1 >= -0.05
    if next_age >= 2:
        acc_pred = tri_and(
            early,
            tri(breadth_delta1 >= Fraction(-5, 100), "BREADTH_DELTA1_BELOW_NEG0_05")
            if breadth_delta1 is not None
            else tri(None, "BREADTH_DELTA1_UNKNOWN"),
        )
        if acc_pred["state"] == "TRUE":
            return {
                "state": "ACCEPTED",
                "last_known_state": "ACCEPTED",
                "stale": False,
                "armed": False,
                "market_age": next_market_age,
                "eval_age": next_age,
                "accepted_once": True,
                "out_neg_streak": out_streak,
                "prev_mature_true": mature["state"] == "TRUE",
                "dq5": dq5,
                "reason": None,
            }
        if acc_pred["state"] == "UNKNOWN":
            return _stale_unknown("ACCEPTED_INPUT_UNKNOWN")

    # 8. IN：后继计数 >= 1 且 early TRUE
    if next_age >= 1:
        if early["state"] == "TRUE":
            return {
                "state": "IN",
                "last_known_state": "IN",
                "stale": False,
                "armed": False,
                "market_age": next_market_age,
                "eval_age": next_age,
                "accepted_once": accepted_once,
                "out_neg_streak": out_streak,
                "prev_mature_true": mature["state"] == "TRUE",
                "dq5": dq5,
                "reason": None,
            }
        if early["state"] == "UNKNOWN":
            return _stale_unknown("IN_INPUT_UNKNOWN")

    # 10. 保原状态
    return {
        "state": prev_state,
        "last_known_state": prev_state,
        "stale": False,
        "armed": False,
        "market_age": next_market_age,
        "eval_age": next_age,
        "accepted_once": accepted_once,
        "out_neg_streak": out_streak,
        "prev_mature_true": mature["state"] == "TRUE",
        "dq5": dq5,
        "reason": None,
    }
