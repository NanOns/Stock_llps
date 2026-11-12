"""为真实 D0 变化对象建立确定、可解释的首页观察顺序。"""

from __future__ import annotations

from collections import Counter
from typing import Any, Mapping, Sequence


_STATE_ORDER = {"TRUE": 0, "UNKNOWN": 1, "FALSE": 2}
_STATE_LABEL = {
    "TRUE": "TRUE 已确认",
    "UNKNOWN": "UNKNOWN 未知",
    "FALSE": "FALSE 未确认",
}
_LAYER = {
    "TRUE": (0, "A", "Seed规则 TRUE"),
    "UNKNOWN": (1, "B", "Seed规则 UNKNOWN"),
    "FALSE": (2, "C", "Seed规则 FALSE"),
    "MISSING": (3, "D", "Seed规则缺少已存事实"),
}
_BOARD_KIND_ORDER = {"INDUSTRY": 0, "CONCEPT": 1}


def _safe_state(value: Any) -> str:
    return value if isinstance(value, str) and value in _STATE_ORDER else "UNKNOWN"


def _rule_state(evidence: Mapping[str, Any], rule_id: str) -> tuple[str, bool]:
    rules = evidence.get("rules")
    facts = rules.get("facts") if isinstance(rules, Mapping) else None
    fact = facts.get(rule_id) if isinstance(facts, Mapping) else None
    missing = not isinstance(fact, Mapping) or rule_id in (
        rules.get("missing_rule_ids") or []
    )
    return (
        _safe_state(fact.get("state")) if isinstance(fact, Mapping) else "UNKNOWN",
        missing,
    )


def _rule_reasons(evidence: Mapping[str, Any], rule_id: str) -> list[str]:
    rules = evidence.get("rules")
    facts = rules.get("facts") if isinstance(rules, Mapping) else None
    fact = facts.get(rule_id) if isinstance(facts, Mapping) else None
    reasons = fact.get("reasons") if isinstance(fact, Mapping) else None
    if not isinstance(reasons, (tuple, list)):
        return []
    return [reason for reason in reasons if isinstance(reason, str) and reason]


def _pulse_rank(status: str, available_count: int, membership_count: int) -> int:
    if available_count:
        return 0
    if membership_count and status == "NOT_COMPUTED":
        return 1
    if membership_count:
        return 2
    return 3


def order_source_launch_watch(
    stocks: Sequence[Mapping[str, Any]],
    boards: Sequence[Mapping[str, Any]],
    evidence_by_symbol: Mapping[str, Mapping[str, Any]],
    *,
    evidence_status: str,
    home_stock_limit: int = 30,
    home_board_limit: int = 15,
) -> dict[str, Any]:
    """排序已证 D0 新增对象；只使用事实状态与可读性，不计算分数。"""
    board_index = {
        (str(board.get("board_kind")), str(board.get("board_native_id"))): board
        for board in boards
    }
    ordered_boards = sorted(
        (dict(board) for board in boards),
        key=lambda board: (
            -int(board.get("new_member_count") or 0),
            _BOARD_KIND_ORDER.get(str(board.get("board_kind")), 2),
            str(board.get("board_name") or "").casefold(),
            str(board.get("board_native_id") or ""),
        ),
    )
    for index, board in enumerate(ordered_boards, start=1):
        board["observation_rank"] = index
        board["home_featured"] = index <= home_board_limit
        board["home_selection_reason"] = (
            "首页按已证 D0 新增成员关系数降序展示；关系相同时按类型、中文名和板块编号稳定排序。"
            if index <= home_board_limit
            else f"首页最多展示 {home_board_limit} 个板块；完整列表仍可搜索和分页，本板块排序第 {index} 位。"
        )
        board["observation_basis"] = (
            "按当日 D0 新增成员关系数降序；PULSE 仅作解释上下文，不参与板块顺序。"
        )

    prepared: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    layer_counts: Counter[str] = Counter()
    for stock_value in stocks:
        stock = dict(stock_value)
        symbol = str(stock.get("source_symbol") or "")
        source_evidence = evidence_by_symbol.get(symbol, {})
        seed = source_evidence.get("seed", {})
        d1 = source_evidence.get("d1", {})
        rules = source_evidence.get("rules", {})
        seed_state, seed_missing = _rule_state(source_evidence, "seed")
        seed_reasons = _rule_reasons(source_evidence, "seed")
        d1_anchor_state, d1_anchor_missing = _rule_state(
            source_evidence, "D1_ANCHOR_WINDOW"
        )
        d1_anchor_reasons = _rule_reasons(source_evidence, "D1_ANCHOR_WINDOW")
        seed_layer_state = "MISSING" if seed_missing else seed_state
        seed_rank, layer_code, layer_label = _LAYER[seed_layer_state]

        memberships: list[dict[str, Any]] = []
        pulse_available_count = 0
        pulse_not_computed_count = 0
        for membership_value in stock.get("also_in_sectors", ()):
            membership = dict(membership_value)
            board = board_index.get(
                (
                    str(membership.get("board_kind")),
                    str(membership.get("board_native_id")),
                ),
                {},
            )
            pulse = board.get("cross_period_pulse")
            pulse = pulse if isinstance(pulse, Mapping) else {}
            pulse_transition = board.get("cross_period_pulse_transition")
            pulse_transition = (
                pulse_transition if isinstance(pulse_transition, Mapping) else {}
            )
            pulse_status = str(pulse.get("status") or "UNKNOWN")
            if pulse_status == "AVAILABLE":
                pulse_available_count += 1
            elif pulse_status == "NOT_COMPUTED":
                pulse_not_computed_count += 1
            membership["pulse_context"] = {
                "status": pulse_status,
                "state": pulse.get("state") or "UNKNOWN",
                "reason_codes": list(pulse.get("reason_codes") or []),
                "transition_status": (pulse_transition.get("status") or "NOT_COMPUTED"),
                "transition_state": pulse_transition.get("transition_state")
                or "UNKNOWN",
                "transition_reason_codes": list(
                    pulse_transition.get("reason_codes") or []
                ),
                "membership_mode": board.get("membership_mode")
                or "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
            }
            memberships.append(membership)

        membership_count = len(memberships)
        pulse_status = (
            "AVAILABLE"
            if pulse_available_count
            else "NOT_COMPUTED"
            if pulse_not_computed_count
            else "UNKNOWN"
        )
        known_rule_count = int(rules.get("known_state_count") or 0)
        expected_rule_count = int(rules.get("expected_rule_count") or 9)
        missing_rule_count = len(rules.get("missing_rule_ids") or [])
        rule_completeness_rank = (
            0
            if known_rule_count == expected_rule_count and missing_rule_count == 0
            else 1
            if known_rule_count > 0
            else 2
            if not missing_rule_count
            else 3
        )
        d1_price_quality = str(d1.get("target_price_quality") or "UNKNOWN")
        d1_price_reasons = list(d1.get("target_price_reasons") or [])
        price_quality_rank = 0 if d1_price_quality == "OBSERVED" else 1
        pulse_rank = _pulse_rank(pulse_status, pulse_available_count, membership_count)
        membership_rank = 0 if membership_count else 1
        order_key = (
            seed_rank,
            _STATE_ORDER.get(d1_anchor_state, 1) if not d1_anchor_missing else 3,
            rule_completeness_rank,
            price_quality_rank,
            pulse_rank,
            membership_rank,
            symbol,
        )

        layer_counts[layer_code] += 1
        seed_display = (
            "缺少已存事实"
            if seed_missing
            else _STATE_LABEL.get(seed_state, "UNKNOWN 未知")
        )
        anchor_display = (
            "缺少已存事实"
            if d1_anchor_missing
            else _STATE_LABEL.get(d1_anchor_state, "UNKNOWN 未知")
        )
        membership_mode = stock.get("membership_mode") or (
            "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT"
        )
        evidence_view = {
            "status": source_evidence.get("status") or "UNKNOWN",
            "seed": seed,
            "d1": d1,
            "rules": rules,
            "sector_context": {
                "status": pulse_status if membership_count else "UNKNOWN",
                "membership_mode": membership_mode,
                "membership_count": membership_count,
                "pulse_available_count": pulse_available_count,
                "pulse_not_computed_count": pulse_not_computed_count,
                "memberships": memberships,
            },
        }
        stock["also_in_sectors"] = memberships
        stock["research_evidence"] = evidence_view
        stock["observation_order"] = {
            "layer": layer_code,
            "layer_label": layer_label,
            "seed_rule_state": seed_state if not seed_missing else "NOT_COMPUTED",
            "d1_anchor_state": (
                d1_anchor_state if not d1_anchor_missing else "NOT_COMPUTED"
            ),
            "known_rule_count": known_rule_count,
            "expected_rule_count": expected_rule_count,
            "d1_target_price_quality": d1_price_quality,
            "seed_rule_reasons": seed_reasons,
            "d1_anchor_reasons": d1_anchor_reasons,
            "d1_target_price_reasons": d1_price_reasons,
            "membership_count": membership_count,
            "pulse_available_count": pulse_available_count,
            "pulse_status": pulse_status,
            "tie_break": "SOURCE_SYMBOL_ASCENDING",
        }
        stock["observation_reason"] = (
            f"{layer_label}（{'；'.join(seed_reasons[:3]) or '无原因码'}）；D1 锚规则 "
            f"{anchor_display}（{'；'.join(d1_anchor_reasons[:3]) or '无原因码'}）；九规则中 "
            f"{known_rule_count}/{expected_rule_count} 条为 TRUE/FALSE；D1 目标价 "
            f"{d1_price_quality}（{'；'.join(d1_price_reasons[:3]) or '无原因码'}）；"
            f"当前成员关系 {membership_count} 条，"
            f"可读 PULSE {pulse_available_count} 条。成员回看 NON_PIT，PULSE 状态不影响排序。"
        )
        stock["research_evidence_status"] = source_evidence.get("status") or "UNKNOWN"
        prepared.append((order_key, stock))

    prepared.sort(key=lambda item: item[0])
    ordered_stocks = [stock for _, stock in prepared]
    for index, stock in enumerate(ordered_stocks, start=1):
        stock["observation_order"]["rank"] = index
        stock["observation_order"]["home_featured"] = index <= home_stock_limit
        if index <= home_stock_limit:
            stock["home_selection_reason"] = (
                f"进入首页前 {home_stock_limit} 个观察位；{stock['observation_reason']}"
            )
        else:
            stock["home_selection_reason"] = (
                f"首页最多展示 {home_stock_limit} 只；按上述观察顺序排第 {index} 位，"
                f"完整结果仍可分页查看。{stock['observation_reason']}"
            )

    stock_summary = {
        "contract": "STOCK_A_D0_EVIDENCE_ORDER_R1",
        "candidate_count": len(ordered_stocks),
        "home_limit": home_stock_limit,
        "home_featured_count": min(len(ordered_stocks), home_stock_limit),
        "not_featured_count": max(0, len(ordered_stocks) - home_stock_limit),
        "layer_counts": dict(sorted(layer_counts.items())),
        "evidence_status": evidence_status,
        "precedence": [
            "已存冻结 seed 规则：TRUE → UNKNOWN → FALSE → 缺少事实",
            "已存 D1_ANCHOR_WINDOW：TRUE → UNKNOWN → FALSE → 缺少事实",
            "九规则确定状态完整度：9/9 → 部分 → 全 UNKNOWN → 缺少规则事实",
            "已存 D1 目标价格质量：OBSERVED → UNKNOWN",
            "当前成员关系中有可读 PULSE 的代码优先；PULSE TRUE/FALSE 不参与顺序",
            "同层按 source_symbol 升序稳定收口",
        ],
        "meaning": "仅为研究观察次序；不修改冻结公式或阈值，不表示正式候选、Priority 或收益预测。",
    }
    board_summary = {
        "contract": "STOCK_A_D0_BOARD_MEMBER_ORDER_R1",
        "candidate_count": len(ordered_boards),
        "home_limit": home_board_limit,
        "home_featured_count": min(len(ordered_boards), home_board_limit),
        "not_featured_count": max(0, len(ordered_boards) - home_board_limit),
        "precedence": [
            "已证 D0 FALSE→TRUE 新增成员关系数降序",
            "关系数相同时按行业、概念、板块中文名和原生编号稳定排序",
            "成员可重复归属；PULSE 只作解释上下文，不代表行业强弱",
        ],
    }
    return {
        "stocks": ordered_stocks,
        "boards": ordered_boards,
        "stock_summary": stock_summary,
        "board_summary": board_summary,
    }


__all__ = ["order_source_launch_watch"]
