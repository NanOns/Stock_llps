"""R03 正式板块 Native、Wilson 宽度、三档资格、前态健康、LOO、风险与 Priority 纯核算法。

严格遵循 ALG-21/22/23/25/30/31/32 与 §9.2/9.4/9.9/10.2/11.2/11.7：
- 绝不在本模块内执行任何磁盘或网络 I/O；
- 所有三态均显式区分 TRUE / FALSE / UNKNOWN / NA；
- Wilson 区间采用 Decimal 高精度计算，不引入约等于 epsilon。
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, localcontext
from fractions import Fraction
from statistics import median
from typing import Any

from stock_a.domain.market_presentation import classify_market_breadth


def tri(state: bool | None, reason: str | None = None) -> dict[str, Any]:
    """构造标准三态字典。"""
    if state is True:
        return {"state": "TRUE", "reason": None, "reasons": ()}
    if state is False:
        reasons = (reason,) if reason else ()
        return {"state": "FALSE", "reason": reason, "reasons": reasons}
    eff_reason = reason or "INPUT_UNKNOWN"
    return {"state": "UNKNOWN", "reason": eff_reason, "reasons": (eff_reason,)}


def _collect_item_reasons(item: Mapping[str, Any], default_reason: str) -> list[str]:
    """从三态子项提取全部原因码，兼容仅含 reason 的字典。"""
    raw_reasons = item.get("reasons")
    if isinstance(raw_reasons, (list, tuple)) and raw_reasons:
        return [str(r) for r in raw_reasons if r]
    r = item.get("reason")
    return [str(r or default_reason)]


def tri_and(*items: Mapping[str, Any]) -> dict[str, Any]:
    """Kleene 三态逻辑与：任一 FALSE 支配 UNKNOWN，并收集全部失败/未知原因。"""
    false_reasons: list[str] = []
    unknown_reasons: list[str] = []
    for item in items:
        state = item["state"]
        if state == "FALSE":
            false_reasons.extend(_collect_item_reasons(item, "CONDITION_FALSE"))
        elif state != "TRUE":
            unknown_reasons.extend(_collect_item_reasons(item, "INPUT_UNKNOWN"))
    if false_reasons:
        dedup = tuple(dict.fromkeys(false_reasons))
        return {"state": "FALSE", "reason": dedup[0], "reasons": dedup}
    if unknown_reasons:
        dedup = tuple(dict.fromkeys(unknown_reasons))
        return {"state": "UNKNOWN", "reason": dedup[0], "reasons": dedup}
    return {"state": "TRUE", "reason": None, "reasons": ()}


def tri_or(*items: Mapping[str, Any]) -> dict[str, Any]:
    """Kleene 三态逻辑或：任一 TRUE 支配 UNKNOWN，并收集全部失败/未知原因。"""
    false_reasons: list[str] = []
    unknown_reasons: list[str] = []
    has_true = False
    for item in items:
        state = item["state"]
        if state == "TRUE":
            has_true = True
        elif state == "FALSE":
            false_reasons.extend(_collect_item_reasons(item, "CONDITION_FALSE"))
        else:
            unknown_reasons.extend(_collect_item_reasons(item, "INPUT_UNKNOWN"))
    if has_true:
        return {"state": "TRUE", "reason": None, "reasons": ()}
    if unknown_reasons:
        dedup = tuple(dict.fromkeys(unknown_reasons))
        return {"state": "UNKNOWN", "reason": dedup[0], "reasons": dedup}
    dedup_false = tuple(dict.fromkeys(false_reasons))
    return {
        "state": "FALSE",
        "reason": dedup_false[0] if dedup_false else None,
        "reasons": dedup_false,
    }


def _join_reasons(tri_result: Mapping[str, Any]) -> str | None:
    """将三态结果中的全部原因码以 ';' 去重拼接为可读原因串。"""
    reasons = tri_result.get("reasons")
    if isinstance(reasons, (list, tuple)) and reasons:
        dedup = list(dict.fromkeys(str(r) for r in reasons if r))
        return ";".join(dedup) if dedup else None
    r = tri_result.get("reason")
    return str(r) if r else None


def _to_fraction(value: Any) -> Fraction | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, Fraction):
        return value
    if isinstance(value, (int, str, Decimal)):
        try:
            f = Fraction(value)
            return f
        except (ValueError, ZeroDivisionError):
            return None
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            return None
        return Fraction(str(value))
    return None


@dataclass(frozen=True)
class MemberSlot:
    """日期有效板块声明成员槽位（保留未映射 slot，同实体别名去重）。"""

    slot_id: str
    entity_id: str | None  # 已映射稳定证券身份，未映射为 None
    symbol: str | None


def normalize_member_slots(slots: Iterable[MemberSlot]) -> tuple[MemberSlot, ...]:
    """按实体身份去重（同实体仅保留一次），未映射 slot 各自保留。"""
    seen_entities: set[str] = set()
    normalized: list[MemberSlot] = []
    for slot in sorted(slots, key=lambda s: (s.entity_id or "", s.slot_id)):
        if slot.entity_id is not None:
            if slot.entity_id in seen_entities:
                continue
            seen_entities.add(slot.entity_id)
        normalized.append(slot)
    return tuple(normalized)


def compute_membership_delta(
    members_t: Sequence[MemberSlot],
    members_u: Sequence[MemberSlot],
) -> dict[str, Any]:
    """比较两日完整成员集合，区分共同成员、加入、退出与未映射槽位。"""
    norm_t = normalize_member_slots(members_t)
    norm_u = normalize_member_slots(members_u)
    map_t = {s.entity_id: s for s in norm_t if s.entity_id is not None}
    map_u = {s.entity_id: s for s in norm_u if s.entity_id is not None}
    unmapped_t = [s.slot_id for s in norm_t if s.entity_id is None]
    unmapped_u = [s.slot_id for s in norm_u if s.entity_id is None]
    common_ids = sorted(set(map_t) & set(map_u))
    joined_ids = sorted(set(map_t) - set(map_u))
    exited_ids = sorted(set(map_u) - set(map_t))
    return {
        "n_t": len(norm_t),
        "n_u": len(norm_u),
        "common_entity_ids": common_ids,
        "joined_entity_ids": joined_ids,
        "exited_entity_ids": exited_ids,
        "unmapped_t": unmapped_t,
        "unmapped_u": unmapped_u,
    }


def _midrank_percentile(values: Sequence[Fraction], own: Fraction) -> Fraction | None:
    """同类型合格全集按中秩计算 0..100 百分位；peer_n < 2 时返回 None。"""
    if len(values) < 2:
        return None
    less = sum(1 for v in values if v < own)
    equal = sum(1 for v in values if v == own)
    return (
        Fraction(100)
        * (Fraction(less) + Fraction(equal - 1, 2))
        / Fraction(len(values) - 1)
    )


def compute_wilson_interval(
    k: int,
    f: int,
    u: int,
    *,
    threshold: Fraction = Fraction(1, 20),
    z_str: str = "1.96",
) -> dict[str, Any]:
    """计算 ALG-22 Wilson 区间，采用 Decimal 高精度避免浮点误差改变等号边界。"""
    n = k + f + u
    if n <= 0 or k < 0 or f < 0 or u < 0:
        return {
            "n": n,
            "k": k,
            "f": f,
            "u": u,
            "point_value": None,
            "lower": None,
            "upper": None,
            "meets_threshold": tri(None, "WILSON_EMPTY_DENOMINATOR"),
        }

    def _wilson_bound(success: int, total: int) -> Decimal:
        with localcontext() as ctx:
            ctx.prec = 50
            z = Decimal(z_str)
            z2 = z * z
            n_dec = Decimal(total)
            p_dec = Decimal(success) / n_dec
            term = (p_dec * (Decimal(1) - p_dec) / n_dec) + (
                z2 / (Decimal(4) * n_dec * n_dec)
            )
            num = p_dec + (z2 / (Decimal(2) * n_dec)) - z * term.sqrt()
            den = Decimal(1) + (z2 / n_dec)
            return num / den

    lower = _wilson_bound(k, n)
    upper = _wilson_bound(k + u, n) if u > 0 else lower
    point_value = lower if u == 0 else None
    thresh_dec = Decimal(threshold.numerator) / Decimal(threshold.denominator)

    if lower >= thresh_dec:
        meets = tri(True)
    elif upper < thresh_dec:
        meets = tri(False, "WILSON_BELOW_THRESHOLD")
    else:
        meets = tri(None, "WILSON_INTERVAL_SPANS_THRESHOLD")

    return {
        "n": n,
        "k": k,
        "f": f,
        "u": u,
        "point_value": str(point_value) if point_value is not None else None,
        "lower": str(lower),
        "upper": str(upper),
        "meets_threshold": meets,
    }


def compute_concentration(
    members: Sequence[MemberSlot],
    raw_amounts_by_entity: Mapping[str, Any],
) -> dict[str, Any]:
    """按完整 M 的规范 RAW 金额计算 top1 与 top3 集中度；缺一金额 UNKNOWN，全 0 NA。"""
    n = len(members)
    if n == 0:
        return {
            "status": "NA",
            "top1_concentration": None,
            "top3_concentration": None,
            "reason": "EMPTY_BOARD",
        }
    amounts: list[Fraction] = []
    for slot in members:
        if slot.entity_id is None:
            return {
                "status": "UNKNOWN",
                "top1_concentration": None,
                "top3_concentration": None,
                "reason": "UNMAPPED_MEMBER_AMOUNT_UNKNOWN",
            }
        amt = _to_fraction(raw_amounts_by_entity.get(slot.entity_id))
        if amt is None or amt < 0:
            return {
                "status": "UNKNOWN",
                "top1_concentration": None,
                "top3_concentration": None,
                "reason": "MEMBER_RAW_AMOUNT_UNKNOWN",
            }
        amounts.append(amt)

    total = sum(amounts, Fraction(0))
    if total <= 0:
        return {
            "status": "NA",
            "top1_concentration": None,
            "top3_concentration": None,
            "reason": "ZERO_TOTAL_RAW_AMOUNT",
        }
    ordered = sorted(amounts, reverse=True)
    top1 = ordered[0] / total
    top3 = sum(ordered[:3], Fraction(0)) / total
    return {
        "status": "OBSERVED",
        "top1_concentration": top1,
        "top3_concentration": top3,
        "reason": None,
    }


def _endpoint_primitive_gate(
    known_values: Sequence[Fraction], n: int
) -> tuple[bool, str | None]:
    """ALG-21/§11.2 独立原语质量门：至少 5 个已知且 |E_x| / n >= 0.8。"""
    if n < 5:
        return False, "MEMBER_COUNT_BELOW_5"
    if len(known_values) < 5:
        return False, "KNOWN_COUNT_BELOW_5"
    if 5 * len(known_values) < 4 * n:
        return False, "COVERAGE_BELOW_0_8"
    return True, None


def compute_endpoint_board_primitives(
    *,
    board_id: str,
    board_kind: str,
    members: Sequence[MemberSlot],
    observations: Mapping[str, Mapping[str, Any]],
    coordinate_ready: bool,
    membership_complete: bool = True,
) -> dict[str, Any]:
    """计算单日单板块的完整 Native 原语（不含同日 peer 排名与跨日差）。"""
    norm_members = normalize_member_slots(members)
    n = len(norm_members)
    actual_count = 0
    suspended_count = 0
    unknown_quote_count = 0
    isolated_count = 0

    ret1_vals: list[Fraction] = []
    ret5_vals: list[Fraction] = []
    ret20_vals: list[Fraction] = []
    above_ma20_flags: list[bool] = []
    amr20_vals: list[Fraction] = []
    seed_k = 0
    seed_f = 0
    seed_u = 0

    for slot in norm_members:
        obs = observations.get(slot.entity_id) if slot.entity_id is not None else None
        if obs is None:
            unknown_quote_count += 1
            seed_u += 1
            continue

        q_state = obs.get("actual_quote_state")
        if q_state == "TRUE":
            actual_count += 1
        elif q_state in {"CONFIRMED_SUSPENSION", "FALSE"}:
            suspended_count += 1
        elif q_state == "ISOLATED":
            isolated_count += 1
        else:
            unknown_quote_count += 1

        r1 = _to_fraction(obs.get("ret1"))
        if r1 is not None:
            ret1_vals.append(r1)
        r5 = _to_fraction(obs.get("ret5"))
        if r5 is not None:
            ret5_vals.append(r5)
        r20 = _to_fraction(obs.get("ret20"))
        if r20 is not None:
            ret20_vals.append(r20)
        abv = obs.get("above_ma20")
        if isinstance(abv, bool):
            above_ma20_flags.append(abv)
        amr = _to_fraction(obs.get("amr20"))
        if amr is not None:
            amr20_vals.append(amr)

        s_state = obs.get("seed_state")
        if s_state == "TRUE":
            seed_k += 1
        elif s_state == "FALSE":
            seed_f += 1
        else:
            seed_u += 1

    quote_cov = Fraction(actual_count, n) if n > 0 and membership_complete else None
    quote_gate = (
        tri(None, "MEMBERSHIP_INCOMPLETE")
        if not membership_complete
        else tri(
            n >= 5 and actual_count >= 5 and 5 * actual_count >= 4 * n,
            "QUOTE_COVERAGE_OR_SIZE_UNMET",
        )
    )
    safety = tri_and(
        quote_gate,
        tri(coordinate_ready, "COORDINATE_NOT_READY"),
    )

    if not membership_complete:
        rs5_ok, rs5_reason = False, "MEMBERSHIP_INCOMPLETE"
        rs20_ok, rs20_reason = False, "MEMBERSHIP_INCOMPLETE"
        b_ok, b_reason = False, "MEMBERSHIP_INCOMPLETE"
        ma_ok, ma_reason = False, "MEMBERSHIP_INCOMPLETE"
        amr_ok, amr_reason = False, "MEMBERSHIP_INCOMPLETE"
    else:
        rs5_ok, rs5_reason = _endpoint_primitive_gate(ret5_vals, n)
        rs20_ok, rs20_reason = _endpoint_primitive_gate(ret20_vals, n)
        b_ok, b_reason = _endpoint_primitive_gate(ret1_vals, n)
        ma_ok = (
            n >= 5 and len(above_ma20_flags) >= 5 and 5 * len(above_ma20_flags) >= 4 * n
        )
        ma_reason = None if ma_ok else "MA20_GATE_UNMET"
        amr_ok, amr_reason = _endpoint_primitive_gate(amr20_vals, n)

    rs5_val = Fraction(median(ret5_vals)) if rs5_ok else None
    rs20_val = Fraction(median(ret20_vals)) if rs20_ok else None
    breadth_val = (
        Fraction(sum(1 for v in ret1_vals if v > 0), len(ret1_vals)) if b_ok else None
    )
    ma20_val = (
        Fraction(sum(1 for v in above_ma20_flags if v), len(above_ma20_flags))
        if ma_ok
        else None
    )
    amr20_val = Fraction(median(amr20_vals)) if amr_ok else None

    if membership_complete:
        wilson = compute_wilson_interval(
            seed_k, seed_f, seed_u, threshold=Fraction(1, 20)
        )
        raw_amounts = {
            slot.entity_id: observations[slot.entity_id].get("raw_amount")
            for slot in norm_members
            if slot.entity_id is not None and slot.entity_id in observations
        }
        concentration = compute_concentration(norm_members, raw_amounts)
    else:
        wilson = {
            "n": n,
            "k": seed_k,
            "f": seed_f,
            "u": seed_u,
            "point_value": None,
            "lower": None,
            "upper": None,
            "meets_threshold": tri(None, "MEMBERSHIP_INCOMPLETE"),
        }
        concentration = {
            "status": "UNKNOWN",
            "top1_concentration": None,
            "top3_concentration": None,
            "reason": "MEMBERSHIP_INCOMPLETE",
        }

    unmapped_count = sum(1 for slot in norm_members if slot.entity_id is None)
    return {
        "board_id": board_id,
        "board_kind": board_kind,
        "members": norm_members,
        "membership_complete": membership_complete,
        "unmapped_count": unmapped_count,
        "n": n,
        "actual_quote_count": actual_count,
        "suspended_count": suspended_count,
        "unknown_quote_count": unknown_quote_count,
        "isolated_count": isolated_count,
        "quote_coverage": quote_cov,
        "safety": safety,
        "safety_reason": _join_reasons(safety),
        "rs5": {"value": rs5_val, "known": len(ret5_vals), "reason": rs5_reason},
        "rs5_reason": rs5_reason,
        "rs20": {"value": rs20_val, "known": len(ret20_vals), "reason": rs20_reason},
        "rs20_reason": rs20_reason,
        "breadth": {"value": breadth_val, "known": len(ret1_vals), "reason": b_reason},
        "breadth_reason": b_reason,
        "ma20_width": {
            "value": ma20_val,
            "known": len(above_ma20_flags),
            "reason": ma_reason,
        },
        "ma20_reason": ma_reason,
        "amr20_median": {
            "value": amr20_val,
            "known": len(amr20_vals),
            "reason": amr_reason,
        },
        "amr20_reason": amr_reason,
        "seed_k": seed_k,
        "seed_f": seed_f,
        "seed_u": seed_u,
        "seed_wilson": wilson,
        "concentration": concentration,
        "concentration_status": concentration["status"],
        "concentration_reason": concentration["reason"],
        "observations": observations,
    }


def rank_endpoint_boards(
    endpoint_boards: Mapping[Any, dict[str, Any]],
) -> dict[Any, dict[str, Any]]:
    """对同日同类型合格板块全集独立计算 RS5_pct 与 RS20_pct（ALG-21）。"""
    by_kind: dict[str, list[dict[str, Any]]] = {}
    for board in endpoint_boards.values():
        if board["safety"]["state"] == "TRUE":
            by_kind.setdefault(board["board_kind"], []).append(board)

    ranked: dict[Any, dict[str, Any]] = {}
    for board_key, board in endpoint_boards.items():
        copied = dict(board)
        if board["safety"]["state"] != "TRUE":
            copied["rs5_pct"] = None
            copied["rs20_pct"] = None
            copied["peer_count"] = 0
            copied["rs5_peer_count"] = 0
            copied["rs20_peer_count"] = 0
            ranked[board_key] = copied
            continue
        peers = by_kind.get(board["board_kind"], [])
        rs5_peers = [p["rs5"]["value"] for p in peers if p["rs5"]["value"] is not None]
        rs20_peers = [
            p["rs20"]["value"] for p in peers if p["rs20"]["value"] is not None
        ]
        own5 = board["rs5"]["value"]
        own20 = board["rs20"]["value"]
        copied["rs5_pct"] = (
            _midrank_percentile(rs5_peers, own5) if own5 is not None else None
        )
        copied["rs20_pct"] = (
            _midrank_percentile(rs20_peers, own20) if own20 is not None else None
        )
        copied["peer_count"] = len(peers)
        copied["rs5_peer_count"] = len(rs5_peers)
        copied["rs20_peer_count"] = len(rs20_peers)
        ranked[board_key] = copied
    return ranked


def _common_member_delta(
    members_t: Sequence[MemberSlot],
    obs_t: Mapping[str, Mapping[str, Any]],
    members_u: Sequence[MemberSlot],
    obs_u: Mapping[str, Mapping[str, Any]],
    field: str,
    *,
    membership_complete_t: bool = True,
    membership_complete_u: bool = True,
    unmapped_t: int = 0,
    unmapped_u: int = 0,
) -> dict[str, Any]:
    """在 M_t ∩ M_u 且两端 E_x 交集上重算共同成员宽度/MA 变化（ALG-21）。"""
    membership = compute_membership_delta(members_t, members_u)
    eff_unmapped_t = max(unmapped_t, len(membership["unmapped_t"]))
    eff_unmapped_u = max(unmapped_u, len(membership["unmapped_u"]))
    common_ids = membership["common_entity_ids"]
    vals_t: list[Any] = []
    vals_u: list[Any] = []
    for eid in common_ids:
        vt = obs_t.get(eid, {}).get(field)
        vu = obs_u.get(eid, {}).get(field)
        if field == "ret1":
            ft, fu = _to_fraction(vt), _to_fraction(vu)
            if ft is not None and fu is not None:
                vals_t.append(ft)
                vals_u.append(fu)
        elif field == "above_ma20":
            if isinstance(vt, bool) and isinstance(vu, bool):
                vals_t.append(vt)
                vals_u.append(vu)

    common_known_count = len(vals_t)
    if (
        not membership_complete_t
        or not membership_complete_u
        or eff_unmapped_t > 0
        or eff_unmapped_u > 0
    ):
        membership_incomplete = {
            **membership,
            "joined_entity_ids": (),
            "exited_entity_ids": (),
        }
        return {
            "delta": None,
            "common_eval_count": common_known_count,
            "common_known_count": common_known_count,
            "membership_delta": membership_incomplete,
            "membership_delta_status": "UNKNOWN",
            "joined_entity_ids": (),
            "exited_entity_ids": (),
            "reason": "MEMBERSHIP_INCOMPLETE",
        }

    n_t = membership["n_t"]
    n_u = membership["n_u"]
    if (
        n_t < 5
        or n_u < 5
        or common_known_count < 5
        or 5 * common_known_count < 4 * max(n_t, n_u)
    ):
        return {
            "delta": None,
            "common_eval_count": common_known_count,
            "common_known_count": common_known_count,
            "membership_delta": membership,
            "membership_delta_status": "OBSERVED",
            "joined_entity_ids": tuple(membership["joined_entity_ids"]),
            "exited_entity_ids": tuple(membership["exited_entity_ids"]),
            "reason": "COMMON_COVERAGE_UNMET",
        }

    if field == "ret1":
        rate_t = Fraction(sum(1 for v in vals_t if v > 0), len(vals_t))
        rate_u = Fraction(sum(1 for v in vals_u if v > 0), len(vals_u))
    else:
        rate_t = Fraction(sum(1 for v in vals_t if v), len(vals_t))
        rate_u = Fraction(sum(1 for v in vals_u if v), len(vals_u))

    return {
        "delta": rate_t - rate_u,
        "common_eval_count": common_known_count,
        "common_known_count": common_known_count,
        "membership_delta": membership,
        "membership_delta_status": "OBSERVED",
        "joined_entity_ids": tuple(membership["joined_entity_ids"]),
        "exited_entity_ids": tuple(membership["exited_entity_ids"]),
        "reason": None,
    }


def evaluate_sector_raw_qualification(
    native_t: Mapping[str, Any],
    *,
    dq5: Fraction | None,
    breadth_delta3: Fraction | None,
    ma20_delta3: Fraction | None,
) -> dict[str, Any]:
    """按 §11.2 精确条件与 Kleene 三态归约三档 raw 资格。"""
    safety = native_t["safety"]
    rs5_pct = native_t.get("rs5_pct")
    rs20_pct = native_t.get("rs20_pct")
    breadth = native_t["breadth"]["value"]
    ma20 = native_t["ma20_width"]["value"]
    seed_gate = native_t["seed_wilson"]["meets_threshold"]
    top1 = native_t["concentration"].get("top1_concentration")

    def ge(val: Fraction | None, thr: Fraction, reason: str) -> dict[str, Any]:
        return tri(val >= thr, reason) if val is not None else tri(None, reason)

    def gt(val: Fraction | None, thr: Fraction, reason: str) -> dict[str, Any]:
        return tri(val > thr, reason) if val is not None else tri(None, reason)

    def le(val: Fraction | None, thr: Fraction, reason: str) -> dict[str, Any]:
        return tri(val <= thr, reason) if val is not None else tri(None, reason)

    prewatch_raw = tri_and(
        safety,
        ge(dq5, Fraction(3), "DQ5_BELOW_3"),
        tri_or(
            gt(breadth_delta3, Fraction(0), "BREADTH_DELTA3_NOT_POSITIVE"),
            gt(ma20_delta3, Fraction(0), "MA20_DELTA3_NOT_POSITIVE"),
        ),
        seed_gate,
    )
    warm_raw = tri_and(
        safety,
        ge(rs5_pct, Fraction(60), "RS5_PCT_BELOW_60"),
        ge(rs20_pct, Fraction(50), "RS20_PCT_BELOW_50"),
        ge(breadth, Fraction(1, 2), "BREADTH_BELOW_0_50"),
        ge(ma20, Fraction(1, 2), "MA20_BELOW_0_50"),
        seed_gate,
    )
    confirmed_raw = tri_and(
        warm_raw,
        ge(rs5_pct, Fraction(70), "RS5_PCT_BELOW_70"),
        ge(rs20_pct, Fraction(60), "RS20_PCT_BELOW_60"),
        ge(breadth, Fraction(55, 100), "BREADTH_BELOW_0_55"),
        ge(ma20, Fraction(55, 100), "MA20_BELOW_0_55"),
        ge(breadth_delta3, Fraction(0), "BREADTH_DELTA3_NEGATIVE"),
        ge(ma20_delta3, Fraction(0), "MA20_DELTA3_NEGATIVE"),
        le(top1, Fraction(1, 2), "TOP1_CONCENTRATION_ABOVE_0_50"),
    )
    return {
        "safety": safety,
        "prewatch_raw": prewatch_raw,
        "warm_raw": warm_raw,
        "confirmed_raw": confirmed_raw,
    }


_MATURITY_ORDER = {"NONE": 0, "PREWATCH": 1, "WARM": 2, "CONFIRMED": 3}
_VALID_MATURITIES = frozenset(_MATURITY_ORDER)


def advance_sector_maturity_and_health(
    raw_qual: Mapping[str, Mapping[str, Any]],
    *,
    dq5: Fraction | None,
    previous_state: Mapping[str, Any] | None,
    hard_damaged: bool = False,
    previous_session_missing: bool = False,
    prev_trade_date: date | None = None,
    session_gap: int | None = None,
) -> dict[str, Any]:
    """按 §10.2 计算今日有效资格、2日滞后展示成熟、PREWATCH 10日退出与板块 health。"""
    c_state = raw_qual["confirmed_raw"]["state"]
    w_state = raw_qual["warm_raw"]["state"]
    p_state = raw_qual["prewatch_raw"]["state"]

    # 上下界推导
    if c_state == "TRUE":
        lower = "CONFIRMED"
    elif w_state == "TRUE":
        lower = "WARM"
    elif p_state == "TRUE":
        lower = "PREWATCH"
    else:
        lower = "NONE"

    if c_state != "FALSE":
        upper = "CONFIRMED"
    elif w_state != "FALSE":
        upper = "WARM"
    elif p_state != "FALSE":
        upper = "PREWATCH"
    else:
        upper = "NONE"

    effective_maturity = lower if lower == upper else "UNKNOWN"

    # 板块健康：仅用独立 dq5（>3 IMPROVING, <-3 WEAKENING, 其余 STABLE）
    if hard_damaged:
        health = "DAMAGED"
    elif dq5 is None:
        health = "UNKNOWN"
    elif dq5 > Fraction(3):
        health = "IMPROVING"
    elif dq5 < Fraction(-3):
        health = "WEAKENING"
    else:
        health = "STABLE"

    prev_display = previous_state.get("display_maturity") if previous_state else None
    prev_effective = (
        previous_state.get("effective_maturity") if previous_state else None
    )
    prev_display_quality = (
        previous_state.get("display_quality") if previous_state else None
    )
    raw_last_known_display = (
        (
            previous_state.get("last_known_display")
            or previous_state.get("last_known_maturity")
        )
        if previous_state is not None
        else None
    )
    prev_last_known_display = (
        raw_last_known_display
        if raw_last_known_display in _VALID_MATURITIES
        else (prev_display if prev_display in _VALID_MATURITIES else None)
    )

    raw_last_known_effective = (
        (
            previous_state.get("last_known_effective")
            or previous_state.get("last_known_maturity")
        )
        if previous_state is not None
        else None
    )
    prev_last_known_effective = (
        raw_last_known_effective
        if raw_last_known_effective in _VALID_MATURITIES
        else (prev_effective if prev_effective in _VALID_MATURITIES else None)
    )

    _valid_exhaustion = frozenset({"HIGH", "MEDIUM", "LOW"})
    prev_exhaustion_level = "UNKNOWN"
    if previous_state is not None:
        raw_ex = previous_state.get("exhaustion_level")
        raw_prev_ex = previous_state.get("prev_exhaustion_level")
        if raw_ex in _valid_exhaustion:
            prev_exhaustion_level = str(raw_ex)
        elif raw_prev_ex in _valid_exhaustion:
            prev_exhaustion_level = str(raw_prev_ex)

    prev_downgrade_streak = int(
        previous_state.get("downgrade_streak", 0) if previous_state else 0
    )
    prev_downgrade_target = (
        previous_state.get("downgrade_target") if previous_state else None
    )
    prev_prewatch_age = int(
        previous_state.get("prewatch_no_improve_count", 0) if previous_state else 0
    )
    prev_dq5_base = _to_fraction(
        previous_state.get("prewatch_dq5_baseline") if previous_state else None
    )
    prev_prewatch_exited = bool(
        previous_state.get("prewatch_exited", False) if previous_state else False
    )

    if effective_maturity == "UNKNOWN":
        held_display = (
            prev_display
            if prev_display in _VALID_MATURITIES
            else prev_last_known_display
        )
        return {
            "effective_maturity": "UNKNOWN",
            "maturity_lower": lower,
            "maturity_upper": upper,
            "display_maturity": held_display or "UNKNOWN",
            "last_known_display": prev_last_known_display,
            "last_known_effective": prev_last_known_effective,
            "last_known_maturity": prev_last_known_display,
            "display_quality": "STALE" if held_display is not None else "UNKNOWN",
            "downgrade_streak": 0,
            "downgrade_target": None,
            "prewatch_no_improve_count": prev_prewatch_age,
            "prewatch_dq5_baseline": prev_dq5_base,
            "prewatch_exited": prev_prewatch_exited,
            "health": health,
            "prev_trade_date": prev_trade_date,
            "session_gap": session_gap,
            "prev_exhaustion_level": prev_exhaustion_level,
        }

    # 若昨日因 PREWATCH 10日无改善退出且今日仍仅达 PREWATCH，当日不重入
    prewatch_exited = False
    if prev_prewatch_exited and effective_maturity == "PREWATCH":
        effective_maturity = "NONE"
        prewatch_exited = True

    recovered_from_unknown = (
        prev_effective == "UNKNOWN"
        or prev_display_quality == "STALE"
        or prev_display == "UNKNOWN"
    )
    gap_after_prev = previous_state is not None and previous_session_missing
    if prev_display is None or prev_display == "UNKNOWN" or recovered_from_unknown:
        anchor = (
            prev_display
            if prev_display in _VALID_MATURITIES
            else prev_last_known_display
        )
        if (
            anchor is not None
            and _MATURITY_ORDER[effective_maturity] < _MATURITY_ORDER[anchor]
        ):
            display_maturity = anchor
            if gap_after_prev:
                # 中间缺日后恢复，禁止推进 downgrade_streak
                downgrade_streak = 0
                downgrade_target = None
            else:
                # 从 UNKNOWN 恢复后降级重新从 1 计数
                downgrade_streak = 1
                downgrade_target = effective_maturity
            display_quality = "STALE" if previous_session_missing else "OBSERVED"
        else:
            display_maturity = effective_maturity
            downgrade_streak = 0
            downgrade_target = None
            if previous_session_missing:
                display_quality = "STALE"
            elif anchor is not None or prev_last_known_effective is not None:
                display_quality = "OBSERVED"
            else:
                display_quality = "FIRST_OBSERVED"
    elif _MATURITY_ORDER[effective_maturity] > _MATURITY_ORDER[prev_display]:
        display_maturity = effective_maturity
        downgrade_streak = 0
        downgrade_target = None
        display_quality = "STALE" if previous_session_missing else "OBSERVED"
    elif _MATURITY_ORDER[effective_maturity] < _MATURITY_ORDER[prev_display]:
        if gap_after_prev:
            # 中间缺日后恢复，禁止推进 downgrade_streak
            downgrade_streak = 0
            downgrade_target = None
            display_maturity = prev_display
        else:
            if (
                prev_downgrade_streak >= 1
                and prev_downgrade_target in _MATURITY_ORDER
                and _MATURITY_ORDER[effective_maturity]
                <= _MATURITY_ORDER[prev_downgrade_target]
            ):
                downgrade_streak = prev_downgrade_streak + 1
            else:
                downgrade_streak = 1
            if downgrade_streak >= 2:
                display_maturity = effective_maturity
                downgrade_streak = 0
                downgrade_target = None
            else:
                display_maturity = prev_display
                downgrade_target = effective_maturity
        display_quality = "STALE" if previous_session_missing else "OBSERVED"
    else:
        display_maturity = prev_display
        downgrade_streak = 0
        downgrade_target = None
        display_quality = "STALE" if previous_session_missing else "OBSERVED"

    # PREWATCH 10 日无改善（同版 dq5 相对归零基准 < 3pp）退出；dq5 未知或中间缺日时不累加
    prewatch_no_improve_count = 0
    prewatch_dq5_baseline = None
    if display_maturity == "PREWATCH" and not prewatch_exited:
        if prev_display != "PREWATCH" or prev_dq5_base is None:
            prewatch_dq5_baseline = dq5
            prewatch_no_improve_count = 0
        elif dq5 is None or gap_after_prev:
            prewatch_dq5_baseline = prev_dq5_base
            prewatch_no_improve_count = prev_prewatch_age
        elif (dq5 - prev_dq5_base) >= Fraction(3):
            prewatch_dq5_baseline = dq5
            prewatch_no_improve_count = 0
        else:
            prewatch_dq5_baseline = prev_dq5_base
            prewatch_no_improve_count = prev_prewatch_age + 1
            if prewatch_no_improve_count >= 10:
                display_maturity = "NONE"
                effective_maturity = "NONE"
                prewatch_no_improve_count = 0
                prewatch_dq5_baseline = None
                prewatch_exited = True

    return {
        "effective_maturity": effective_maturity,
        "maturity_lower": lower,
        "maturity_upper": upper,
        "display_maturity": display_maturity,
        "last_known_display": display_maturity,
        "last_known_effective": effective_maturity,
        "last_known_maturity": effective_maturity,
        "display_quality": display_quality,
        "downgrade_streak": downgrade_streak,
        "downgrade_target": downgrade_target,
        "prewatch_no_improve_count": prewatch_no_improve_count,
        "prewatch_dq5_baseline": prewatch_dq5_baseline,
        "prewatch_exited": prewatch_exited,
        "health": health,
        "prev_trade_date": prev_trade_date,
        "session_gap": session_gap,
        "prev_exhaustion_level": prev_exhaustion_level,
    }


def compute_sector_exhaustion_risk(
    *,
    rs20_pct: Fraction | None,
    dq5: Fraction | None,
    breadth_delta3: Fraction | None,
) -> dict[str, Any]:
    """按 §9.4 计算板块 exhaustion 风险，完整 Kleene 归约：高优先可能成立且未知时不降判，已排除则继续。"""
    rs20_ge_80 = (
        tri(rs20_pct >= Fraction(80), "RS20_PCT_BELOW_80")
        if rs20_pct is not None
        else tri(None, "RS20_PCT_UNKNOWN")
    )
    high = tri_and(
        rs20_ge_80,
        tri(dq5 <= Fraction(-5), "DQ5_ABOVE_NEG5")
        if dq5 is not None
        else tri(None, "DQ5_UNKNOWN"),
        tri(breadth_delta3 <= Fraction(-5, 100), "BREADTH_DELTA3_ABOVE_NEG0_05")
        if breadth_delta3 is not None
        else tri(None, "BREADTH_DELTA3_UNKNOWN"),
    )
    if high["state"] == "TRUE":
        return {"level": "HIGH", "reason": None}
    if high["state"] == "UNKNOWN":
        return {"level": "UNKNOWN", "reason": high["reason"]}

    med = tri_and(
        rs20_ge_80,
        tri(dq5 < Fraction(0), "DQ5_NON_NEGATIVE")
        if dq5 is not None
        else tri(None, "DQ5_UNKNOWN"),
    )
    if med["state"] == "TRUE":
        return {"level": "MEDIUM", "reason": None}
    if med["state"] == "UNKNOWN":
        return {"level": "UNKNOWN", "reason": med["reason"]}
    return {"level": "LOW", "reason": med.get("reason")}


def compute_sector_emergence(
    *,
    dq5: Fraction | None,
    breadth_delta3: Fraction | None,
    seed_wilson_point: Fraction | None,
) -> dict[str, Any]:
    """按 ALG-32 计算板块 emergence 等级（HIGH/MEDIUM/LOW/NONE/UNKNOWN）。"""
    high = tri_and(
        tri(dq5 >= Fraction(10), "DQ5_BELOW_10")
        if dq5 is not None
        else tri(None, "DQ5_UNKNOWN"),
        tri(breadth_delta3 >= Fraction(5, 100), "BREADTH_DELTA3_BELOW_0_05")
        if breadth_delta3 is not None
        else tri(None, "BREADTH_DELTA3_UNKNOWN"),
        tri(seed_wilson_point >= Fraction(1, 10), "SEED_WIDTH_BELOW_0_10")
        if seed_wilson_point is not None
        else tri(None, "SEED_WIDTH_POINT_UNKNOWN"),
    )
    if high["state"] == "TRUE":
        return {"level": "HIGH", "rank": 3, "reason": None}
    if high["state"] == "UNKNOWN":
        return {"level": "UNKNOWN", "rank": None, "reason": high["reason"]}

    med = tri_and(
        tri(dq5 >= Fraction(3), "DQ5_BELOW_3")
        if dq5 is not None
        else tri(None, "DQ5_UNKNOWN"),
        tri(breadth_delta3 > Fraction(0), "BREADTH_DELTA3_NOT_POSITIVE")
        if breadth_delta3 is not None
        else tri(None, "BREADTH_DELTA3_UNKNOWN"),
    )
    if med["state"] == "TRUE":
        return {"level": "MEDIUM", "rank": 2, "reason": None}
    if med["state"] == "UNKNOWN":
        return {"level": "UNKNOWN", "rank": None, "reason": med["reason"]}

    if dq5 is None:
        return {"level": "UNKNOWN", "rank": None, "reason": "DQ5_UNKNOWN"}
    if dq5 > Fraction(0):
        return {"level": "LOW", "rank": 1, "reason": None}
    return {"level": "NONE", "rank": 0, "reason": None}


_EXHAUSTION_RANK = {"LOW": 0, "MEDIUM": 1, "HIGH": 2}


def compute_sector_priority(
    boards: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """按 ALG-32 / §11.7 对板块进行确定性有序 Priority 排序（含 RANKABLE/EXCLUDED/UNKNOWN）。"""
    rankable: list[dict[str, Any]] = []
    non_rankable: list[dict[str, Any]] = []
    for board in boards:
        safety_state = board["raw_qualification"]["safety"]["state"]
        eff_mat = board.get("lifecycle", {}).get("effective_maturity")
        prewatch_exited = bool(board.get("lifecycle", {}).get("prewatch_exited", False))
        if safety_state == "FALSE" or prewatch_exited or eff_mat == "NONE":
            non_rankable.append(
                {
                    "board_kind": board["board_kind"],
                    "board_id": board["board_id"],
                    "priority_status": "EXCLUDED",
                    "reason": (
                        "PREWATCH_EXITED_TODAY"
                        if prewatch_exited
                        else (
                            "EFFECTIVE_MATURITY_NONE"
                            if eff_mat == "NONE"
                            else board["raw_qualification"]["safety"].get("reason")
                        )
                    ),
                    "sort_key": None,
                }
            )
            continue

        emg = board["emergence"]
        exh = board["exhaustion"]
        dq5 = board.get("dq5")
        bd3 = board.get("breadth_delta3")
        md3 = board.get("ma20_delta3")
        sw_point = _to_fraction(board["seed_wilson"].get("point_value"))

        if (
            safety_state != "TRUE"
            or eff_mat == "UNKNOWN"
            or emg["rank"] is None
            or exh["level"] not in _EXHAUSTION_RANK
            or dq5 is None
            or bd3 is None
            or md3 is None
            or sw_point is None
        ):
            non_rankable.append(
                {
                    "board_kind": board["board_kind"],
                    "board_id": board["board_id"],
                    "priority_status": "UNKNOWN",
                    "reason": "PRIORITY_KEY_INCOMPLETE",
                    "sort_key": None,
                }
            )
            continue

        sort_key = (
            -int(emg["rank"]),
            -dq5,
            -bd3,
            -md3,
            -sw_point,
            _EXHAUSTION_RANK[exh["level"]],
            (str(board["board_kind"]), str(board["board_id"])),
        )
        rankable.append(
            {
                "board_kind": board["board_kind"],
                "board_id": board["board_id"],
                "priority_status": "RANKABLE",
                "reason": None,
                "sort_key": sort_key,
                "emergence": emg["level"],
                "exhaustion": exh["level"],
            }
        )

    rankable.sort(key=lambda item: item["sort_key"])
    for idx, item in enumerate(rankable, start=1):
        item["rank_position"] = idx
    return rankable + non_rankable


def _board_key(board: Mapping[str, Any]) -> tuple[str, str]:
    return (str(board["board_kind"]), str(board["board_id"]))


def compute_sector_native_day(
    *,
    boards_t: Sequence[Mapping[str, Any]],
    boards_t1: Mapping[Any, Mapping[str, Any]],
    boards_t3: Mapping[Any, Mapping[str, Any]],
    previous_states: Mapping[Any, Mapping[str, Any]] | None = None,
    prev_trade_date: date | None = None,
    session_gap: int | None = None,
    previous_session_missing: bool = False,
) -> list[dict[str, Any]]:
    """串联 ALG-21/22/23/30/31/32 计算单日全板块正式 Native、资格、前态、风险与 Priority。"""
    ranked_t = rank_endpoint_boards(
        {_board_key(b): compute_endpoint_board_primitives(**b) for b in boards_t}
    )
    ranked_t3 = rank_endpoint_boards(
        {
            _board_key(b): compute_endpoint_board_primitives(**b)
            for b in boards_t3.values()
        }
    )
    t1_by_key = {_board_key(b): b for b in boards_t1.values()}

    results: list[dict[str, Any]] = []
    for bkey, nt in ranked_t.items():
        board_kind, board_id = bkey
        nt3 = ranked_t3.get(bkey)
        nt1 = t1_by_key.get(bkey)
        dq5 = (
            nt["rs5_pct"] - nt3["rs5_pct"]
            if nt["rs5_pct"] is not None
            and nt3 is not None
            and nt3["rs5_pct"] is not None
            else None
        )
        bd3_info = (
            _common_member_delta(
                nt["members"],
                nt["observations"],
                nt3["members"],
                nt3["observations"],
                "ret1",
                membership_complete_t=bool(nt.get("membership_complete", True)),
                membership_complete_u=bool(nt3.get("membership_complete", True)),
            )
            if nt3 is not None
            else {
                "delta": None,
                "common_known_count": None,
                "membership_delta_status": "UNKNOWN",
                "joined_entity_ids": (),
                "exited_entity_ids": (),
                "reason": "PRIOR_T3_MISSING",
            }
        )
        md3_info = (
            _common_member_delta(
                nt["members"],
                nt["observations"],
                nt3["members"],
                nt3["observations"],
                "above_ma20",
                membership_complete_t=bool(nt.get("membership_complete", True)),
                membership_complete_u=bool(nt3.get("membership_complete", True)),
            )
            if nt3 is not None
            else {
                "delta": None,
                "common_known_count": None,
                "membership_delta_status": "UNKNOWN",
                "joined_entity_ids": (),
                "exited_entity_ids": (),
                "reason": "PRIOR_T3_MISSING",
            }
        )
        bd1_info = (
            _common_member_delta(
                nt["members"],
                nt["observations"],
                normalize_member_slots(nt1["members"]),
                nt1["observations"],
                "ret1",
                membership_complete_t=bool(nt.get("membership_complete", True)),
                membership_complete_u=bool(nt1.get("membership_complete", True)),
            )
            if nt1 is not None
            else {
                "delta": None,
                "common_known_count": None,
                "membership_delta_status": "UNKNOWN",
                "joined_entity_ids": (),
                "exited_entity_ids": (),
                "reason": "PRIOR_T1_MISSING",
            }
        )
        raw_qual = evaluate_sector_raw_qualification(
            nt,
            dq5=dq5,
            breadth_delta3=bd3_info["delta"],
            ma20_delta3=md3_info["delta"],
        )
        prev_state = None
        if previous_states:
            prev_state = previous_states.get(bkey) or previous_states.get(board_id)
        lifecycle = advance_sector_maturity_and_health(
            raw_qual,
            dq5=dq5,
            previous_state=prev_state,
            previous_session_missing=previous_session_missing,
            prev_trade_date=prev_trade_date,
            session_gap=session_gap,
        )
        exhaustion = compute_sector_exhaustion_risk(
            rs20_pct=nt["rs20_pct"],
            dq5=dq5,
            breadth_delta3=bd3_info["delta"],
        )
        sw_point = _to_fraction(nt["seed_wilson"].get("point_value"))
        emergence = compute_sector_emergence(
            dq5=dq5,
            breadth_delta3=bd3_info["delta"],
            seed_wilson_point=sw_point,
        )
        m_delta_status = (
            str(bd3_info.get("membership_delta_status", "UNKNOWN"))
            if nt3 is not None
            else "UNKNOWN"
        )
        joined_count = (
            len(bd3_info.get("joined_entity_ids", ()))
            if m_delta_status == "OBSERVED"
            else None
        )
        exited_count = (
            len(bd3_info.get("exited_entity_ids", ()))
            if m_delta_status == "OBSERVED"
            else None
        )
        prev_exhaustion_level = None
        if prev_state is not None:
            prev_exhaustion_level = prev_state.get("exhaustion_level") or (
                prev_state.get("exhaustion") or {}
            ).get("level")
        results.append(
            {
                **nt,
                "rs5_pct_t3": nt3["rs5_pct"] if nt3 is not None else None,
                "dq5": dq5,
                "breadth_delta1": bd1_info["delta"],
                "breadth_delta3": bd3_info["delta"],
                "ma20_delta3": md3_info["delta"],
                "breadth_delta1_common_count": bd1_info.get("common_known_count"),
                "breadth_delta3_common_count": bd3_info.get("common_known_count"),
                "ma20_delta3_common_count": md3_info.get("common_known_count"),
                "joined_count": joined_count,
                "exited_count": exited_count,
                "membership_delta_status": m_delta_status,
                "prewatch_raw_reason": _join_reasons(raw_qual["prewatch_raw"]),
                "warm_raw_reason": _join_reasons(raw_qual["warm_raw"]),
                "confirmed_raw_reason": _join_reasons(raw_qual["confirmed_raw"]),
                "raw_qualification": raw_qual,
                "lifecycle": lifecycle,
                "exhaustion": exhaustion,
                "exhaustion_reason": exhaustion.get("reason"),
                "emergence": emergence,
                "emergence_reason": emergence.get("reason"),
                "prev_trade_date": prev_trade_date,
                "session_gap": session_gap,
                "prev_exhaustion_level": prev_exhaustion_level,
            }
        )

    priority_map = {
        (p["board_kind"], p["board_id"]): p for p in compute_sector_priority(results)
    }
    for row in results:
        row["priority"] = priority_map[(row["board_kind"], row["board_id"])]
    return results


def compute_sector_loo_and_support(
    *,
    target_entity_id: str,
    boards_t: Sequence[Mapping[str, Any]],
    boards_t1: Mapping[Any, Mapping[str, Any]],
    boards_t3: Mapping[Any, Mapping[str, Any]],
    primary_industry_id: str | None = None,
    board_kind: str | None = None,
    membership_known: bool = True,
) -> dict[str, Any]:
    """ALG-25 LOO：从所有同类型 peer 剔除目标证券后完整重算并确定算法支持板块。"""
    scoped_t = [
        b for b in boards_t if board_kind is None or str(b["board_kind"]) == board_kind
    ]
    scoped_t1 = {
        k: b
        for k, b in boards_t1.items()
        if board_kind is None or str(b["board_kind"]) == board_kind
    }
    scoped_t3 = {
        k: b
        for k, b in boards_t3.items()
        if board_kind is None or str(b["board_kind"]) == board_kind
    }
    if not scoped_t:
        return {
            "target_entity_id": target_entity_id,
            "primary_industry_id": primary_industry_id,
            "algorithm_support_board_id": None,
            "support_status": "UNKNOWN",
            "reason": (
                "LOO_MEMBERSHIP_OR_SLOT_UNKNOWN"
                if not membership_known
                else "BOARD_KIND_SOURCE_MISSING"
            ),
            "candidate_count": 0,
            "qualified_candidate_count": 0,
            "loo_boards": [],
        }

    reduced_t = []
    member_keys: set[tuple[str, str]] = set()
    for b in scoped_t:
        slots = [s for s in b["members"] if s.entity_id != target_entity_id]
        if len(slots) != len(b["members"]):
            member_keys.add(_board_key(b))
        reduced_t.append({**b, "members": slots})

    if member_keys:
        reduced_t1 = {
            k: {
                **b,
                "members": [s for s in b["members"] if s.entity_id != target_entity_id],
            }
            for k, b in scoped_t1.items()
        }
        reduced_t3 = {
            k: {
                **b,
                "members": [s for s in b["members"] if s.entity_id != target_entity_id],
            }
            for k, b in scoped_t3.items()
        }
        recomputed = compute_sector_native_day(
            boards_t=reduced_t,
            boards_t1=reduced_t1,
            boards_t3=reduced_t3,
        )
        candidates = [
            {
                **r,
                "is_selected_support": False,
                "breadth_delta3": r.get("breadth_delta3"),
                "ma20_delta3": r.get("ma20_delta3"),
                "top1_concentration": r["concentration"].get("top1_concentration"),
            }
            for r in recomputed
            if _board_key(r) in member_keys
        ]
    else:
        candidates = []

    def _qual_bounds(row: Mapping[str, Any]) -> tuple[int, int]:
        rq = row["raw_qualification"]
        c_st = rq["confirmed_raw"]["state"]
        w_st = rq["warm_raw"]["state"]
        p_st = rq["prewatch_raw"]["state"]
        lower = (
            3 if c_st == "TRUE" else 2 if w_st == "TRUE" else 1 if p_st == "TRUE" else 0
        )
        upper = (
            3
            if c_st != "FALSE"
            else 2
            if w_st != "FALSE"
            else 1
            if p_st != "FALSE"
            else 0
        )
        return lower, upper

    qualified_candidates = []
    max_known_lower = 0
    for c in candidates:
        lo, up = _qual_bounds(c)
        if lo > max_known_lower:
            max_known_lower = lo
        if lo == up and lo > 0:
            qualified_candidates.append((lo, c))

    all_scoped_endpoints = [*scoped_t, *scoped_t1.values(), *scoped_t3.values()]
    has_unmapped_slot = any(
        any(s.entity_id is None for s in b["members"]) for b in all_scoped_endpoints
    )
    has_incomplete_membership = any(
        not b.get("membership_complete", True) for b in all_scoped_endpoints
    )
    if not membership_known or has_unmapped_slot or has_incomplete_membership:
        return {
            "target_entity_id": target_entity_id,
            "primary_industry_id": primary_industry_id,
            "algorithm_support_board_id": None,
            "support_status": "UNKNOWN",
            "reason": "LOO_MEMBERSHIP_OR_SLOT_UNKNOWN",
            "candidate_count": len(candidates),
            "qualified_candidate_count": len(qualified_candidates),
            "loo_boards": candidates,
        }

    if not member_keys:
        return {
            "target_entity_id": target_entity_id,
            "primary_industry_id": primary_industry_id,
            "algorithm_support_board_id": None,
            "support_status": "NA",
            "reason": "NO_APPLICABLE_BOARD",
            "candidate_count": 0,
            "qualified_candidate_count": 0,
            "loo_boards": [],
        }

    # 若存在可能达到或超过已知最高档的未知候选，禁止用低档覆盖
    ambiguous_higher = any(
        lo != up and up >= max(1, max_known_lower)
        for lo, up in (_qual_bounds(c) for c in candidates)
    )
    if ambiguous_higher:
        return {
            "target_entity_id": target_entity_id,
            "primary_industry_id": primary_industry_id,
            "algorithm_support_board_id": None,
            "support_status": "UNKNOWN",
            "reason": "LOO_QUALIFICATION_UNKNOWN",
            "candidate_count": len(candidates),
            "qualified_candidate_count": len(qualified_candidates),
            "loo_boards": candidates,
        }

    if not qualified_candidates:
        return {
            "target_entity_id": target_entity_id,
            "primary_industry_id": primary_industry_id,
            "algorithm_support_board_id": None,
            "support_status": "NA",
            "reason": "NO_QUALIFIED_LOO_BOARD",
            "candidate_count": len(candidates),
            "qualified_candidate_count": 0,
            "loo_boards": candidates,
        }

    top_tier = [c for qr, c in qualified_candidates if qr == max_known_lower]
    if len(top_tier) > 1:
        # 多个最高档候选并列时，若任一候选的 emergence 或 Seed 点值未知，无法确定唯一赢家 -> UNKNOWN
        if any(
            c["emergence"]["rank"] is None
            or _to_fraction(c["seed_wilson"].get("point_value")) is None
            for c in top_tier
        ):
            return {
                "target_entity_id": target_entity_id,
                "primary_industry_id": primary_industry_id,
                "algorithm_support_board_id": None,
                "support_status": "UNKNOWN",
                "reason": "LOO_TIEBREAK_KEY_UNKNOWN",
                "candidate_count": len(candidates),
                "qualified_candidate_count": len(qualified_candidates),
                "loo_boards": candidates,
            }
        top_tier.sort(
            key=lambda c: (
                -int(c["emergence"]["rank"]),
                -_to_fraction(c["seed_wilson"].get("point_value")),  # type: ignore[operator]
                str(c["board_id"]),
            )
        )

    best = top_tier[0]
    best_key = _board_key(best)
    for c in candidates:
        c["is_selected_support"] = _board_key(c) == best_key
    return {
        "target_entity_id": target_entity_id,
        "primary_industry_id": primary_industry_id,
        "algorithm_support_board_id": best["board_id"],
        "support_status": "SELECTED",
        "reason": None,
        "candidate_count": len(candidates),
        "qualified_candidate_count": len(qualified_candidates),
        "loo_boards": candidates,
    }


def compute_market_risk_projection(
    *,
    pressure_axis: Mapping[str, Any] | None,
    breadth_axis: Mapping[str, Any] | None,
    participation_axis: Mapping[str, Any] | None,
    formal_breadth: Mapping[str, Any] | None = None,
    breadth_change: Mapping[str, Any] | None = None,
    environment_display: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """MKT-06 / ALG-31：独立投影市场压力、宽度、参与三轴风险，不合成未采用总分。"""

    def _extract_known_count(axis: Mapping[str, Any]) -> Any:
        if axis.get("known_count") is not None:
            return axis.get("known_count")
        if axis.get("assessable_count") is not None:
            return axis.get("assessable_count")
        return axis.get("n_eligible")

    def _project_axis(name: str, axis: Mapping[str, Any] | None) -> dict[str, Any]:
        if not axis:
            return {"axis": name, "status": "NOT_COMPUTED", "classification": "UNKNOWN"}
        projected: dict[str, Any] = {
            "axis": name,
            "status": axis.get("status", "UNKNOWN"),
            "classification": axis.get("classification", "UNKNOWN"),
            "coverage": axis.get("coverage"),
            "known_count": _extract_known_count(axis),
            "unknown_count": axis.get("unknown_count"),
        }
        if name == "breadth":
            projected["n_t"] = axis.get("n_t")
            projected["n_t3"] = axis.get("n_t3")
            projected["n_common"] = axis.get("n_common")
            projected["n_eligible"] = axis.get("n_eligible")
        return projected

    effective_breadth = formal_breadth if formal_breadth is not None else breadth_axis
    breadth_projected = _project_axis("breadth", effective_breadth)
    if formal_breadth is not None:
        fb_status = formal_breadth.get("status", "UNKNOWN")
        breadth_projected["status"] = fb_status
        if fb_status == "OBSERVED":
            fb_cls = formal_breadth.get("classification")
            if not fb_cls:
                bv = _to_fraction(formal_breadth.get("breadth_value"))
                fb_cls = classify_market_breadth(bv) if bv is not None else "UNKNOWN"
            fb_cov = formal_breadth.get("coverage")
            if fb_cov is None:
                n_common = formal_breadth.get("n_common")
                n_eligible = formal_breadth.get("n_eligible")
                if (
                    n_common is not None
                    and n_eligible is not None
                    and int(n_common) > 0
                ):
                    fb_cov = Fraction(int(n_eligible), int(n_common))
            breadth_projected["classification"] = fb_cls
            breadth_projected["coverage"] = fb_cov
        else:
            breadth_projected["classification"] = "UNKNOWN"
            breadth_projected["coverage"] = None
    breadth_denominators = {
        "n_t": effective_breadth.get("n_t") if effective_breadth else None,
        "n_t3": effective_breadth.get("n_t3") if effective_breadth else None,
        "n_common": effective_breadth.get("n_common") if effective_breadth else None,
        "n_eligible": effective_breadth.get("n_eligible")
        if effective_breadth
        else None,
        "unknown_count": effective_breadth.get("unknown_count")
        if effective_breadth
        else None,
    }
    breadth_change_out = (
        {
            "change_status": breadth_change.get("change_status", "UNKNOWN"),
            "delta": breadth_change.get("delta"),
        }
        if breadth_change is not None and "change_status" in breadth_change
        else {"change_status": "UNKNOWN", "delta": None}
    )
    env_display_status = "UNKNOWN"
    if environment_display is not None:
        env_display_status = str(
            environment_display.get("display_status")
            or environment_display.get("display_quality")
            or environment_display.get("status")
            or "UNKNOWN"
        )

    return {
        "axes": {
            "pressure": _project_axis("pressure", pressure_axis),
            "breadth": breadth_projected,
            "participation": _project_axis("participation", participation_axis),
        },
        "breadth_denominators": breadth_denominators,
        "breadth_change": breadth_change_out,
        "environment_display_status": env_display_status,
        "composite_risk_score": None,
    }
