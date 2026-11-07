"""来源观察的板内相对位置：仅复用本次请求已读事实。"""

from bisect import bisect_left, bisect_right
from fractions import Fraction
from statistics import median

from stock_a.application.source_pool_seed import _board_native, _loo_differences


def _number(fact, field):
    if field.startswith("rps"):
        item = fact.get("rps", {}).get(field, {})
        if item.get("quality") != "OBSERVED" or item.get("value") is None:
            return None
        return Fraction(item["value"])
    item = fact.get("native_price", {}).get("fields", {}).get("core." + field, {})
    if item.get("quality") != "OBSERVED" or item.get("value") is None:
        return None
    return Fraction(item["value"]["exact"])


def _change(before, after):
    if before is None or after is None:
        return None
    return str(Fraction(after) - Fraction(before))


def _direction(delta):
    if delta is None:
        return "UNKNOWN"
    value = Fraction(delta)
    return "UP" if value > 0 else "DOWN" if value < 0 else "UNCHANGED"


def _positive_breadth(market):
    denominator = market["denominator"]
    counts = denominator.get("counts", {})
    required = (
        "COMPARABLE_GAIN",
        "COMPARABLE_LOSS",
        "COMPARABLE_BOTH_POSITIVE",
        "COMPARABLE_BOTH_NONPOSITIVE",
    )
    if any(name not in counts for name in required):
        return None
    positive = counts["COMPARABLE_GAIN"] + counts["COMPARABLE_BOTH_POSITIVE"]
    nonpositive = counts["COMPARABLE_LOSS"] + counts["COMPARABLE_BOTH_NONPOSITIVE"]
    known = positive + nonpositive
    return {
        "positive_count": positive,
        "nonpositive_count": nonpositive,
        "known_count": known,
        "unknown_count": denominator.get("UNKNOWN"),
        "positive_fraction": str(Fraction(positive, known)) if known else None,
    }


def market_changes(days):
    """输出相邻来源宽度；分母与未知项并列，不暗示正式市场总体。"""
    result = []
    for index, day in enumerate(days):
        current = day["market"]
        prior = days[index - 1]["market"] if index else None
        value = current["metric"].get("exact")
        earlier = prior["metric"].get("exact") if prior else None
        positive = _positive_breadth(current)
        previous_positive = _positive_breadth(prior) if prior else None
        result.append(
            {
                "session": day["session"],
                "prior_session": days[index - 1]["session"] if prior else None,
                "breadth_before": earlier,
                "breadth_now": value,
                "breadth_delta": _change(
                    earlier["fraction"] if earlier else None,
                    value["fraction"] if value else None,
                ),
                "breadth_direction": _direction(
                    _change(
                        earlier["fraction"] if earlier else None,
                        value["fraction"] if value else None,
                    )
                ),
                "denominator_before": prior["denominator"] if prior else None,
                "denominator_now": current["denominator"],
                "positive_breadth_before": previous_positive,
                "positive_breadth_now": positive,
                "positive_breadth_delta": _change(
                    previous_positive["positive_fraction"]
                    if previous_positive
                    else None,
                    positive["positive_fraction"] if positive else None,
                ),
                "comparison_note": "当日涨跌计数限于宽度原语的双时点共同可评价集合；相邻日各自归约，分母可能不同。",
                "formal_axes": {
                    "trend": "UNKNOWN",
                    "breadth": "UNKNOWN",
                    "participation": "UNKNOWN",
                    "pressure": "UNKNOWN",
                    "label": "UNKNOWN",
                },
                "reason": "来源池宽度不是正式历史总体；趋势、参与度及制度压力依赖未证实",
            }
        )
    return result


def board_changes(days, boards):
    """列出同类型板块已证原语和相邻值；不归约为阶段。"""
    by_key = {(b["session"], b["board_kind"], b["board_native_id"]): b for b in boards}
    output = []
    for index, day in enumerate(days):
        for (session, kind, board_id), board in by_key.items():
            if session != day["session"]:
                continue
            prior = (
                by_key.get((days[index - 1]["session"], kind, board_id))
                if index
                else None
            )
            native = board.get("native_detail")
            previous = prior.get("native_detail") if prior else None
            native = native if isinstance(native, dict) else {}
            previous = previous if isinstance(previous, dict) else {}
            metrics = {}
            for name, field in (
                ("ret5", "median"),
                ("ret20", "median"),
                ("breadth", "value"),
                ("ma20_width", "value"),
            ):
                current = native.get(name, {})
                before = previous.get(name, {})
                metrics[name] = {
                    "before": before.get(field),
                    "now": current.get(field),
                    "delta": _change(before.get(field), current.get(field)),
                    "before_denominator": before.get("known"),
                    "now_denominator": current.get("known"),
                    "before_percentile": before.get("percentile"),
                    "now_percentile": current.get("percentile"),
                    "percentile_delta_pp": _change(
                        before.get("percentile"), current.get("percentile")
                    ),
                    "peer_count": current.get("peer_count"),
                    "peer_total": current.get("peer_total"),
                }
            output.append(
                {
                    "session": session,
                    "board_kind": kind,
                    "board_native_id": board_id,
                    "prior_session": prior["session"] if prior else None,
                    "metrics": metrics,
                    "evidence_directions": {
                        name: _direction(metric["delta"])
                        for name, metric in metrics.items()
                    },
                    "cross_period": native.get("cross_period"),
                    "rotation": board.get("rotation_detail"),
                    "signals": board.get("signals"),
                    "membership_status": board.get("membership_status"),
                    "comparison_note": "相邻单日原语各有已知分母；共同成员双端重算见cross_period，不把变化归为正式阶段。",
                }
            )
    return output


def relative_position_view(
    days, observation, *, board_kind, board_id, symbol=None, d1_by_session=None
):
    """按固定当前成员、各日各指标已知分母计算中秩，未使用后继。"""
    members = sorted(
        s
        for s, boards in observation["memberships_by_symbol"].items()
        if any(
            b["board_kind"] == board_kind and b["board_native_id"] == board_id
            for b in boards
        )
    )
    member_set = set(members)
    peers = {
        board["board_native_id"]: []
        for board in observation["boards"]
        if board["board_kind"] == board_kind
    }
    for stock, memberships in observation["memberships_by_symbol"].items():
        for membership in memberships:
            if membership["board_kind"] == board_kind:
                peers.setdefault(membership["board_native_id"], []).append(stock)
    board_rows = {
        (r["session"], r["board_kind"], r["board_native_id"]): r
        for r in observation["boards"]
    }
    events = {}
    for event in observation["events"]:
        if event["source_symbol"] in member_set:
            events.setdefault((event["session"], event["source_symbol"]), []).append(
                event
            )
    output = []
    previous = {}
    for day in days:
        facts = day["seed"]["facts"]
        d0 = {
            name: {row["source_symbol"]: row for row in source["results"]}
            for name, source in day.get("scenarios", {}).items()
        }
        d1 = {
            row["source_symbol"]: row
            for row in (d1_by_session or {})
            .get(day["session"], {})
            .get("complete_symbol_details", [])
        }
        values = {
            field: sorted(
                v
                for s in members
                if (v := _number(facts.get(s, {}), field)) is not None
            )
            for field in ("ret5", "ret20", "rps5", "rps20")
        }
        medians = {
            key: median(items) if items else None for key, items in values.items()
        }
        positions = {}
        for stock in members:
            fact = facts.get(stock, {})
            metrics = {}
            for field, population in values.items():
                value = _number(fact, field)
                source_field = (
                    fact.get("rps", {}).get(field, {})
                    if field.startswith("rps")
                    else fact.get("native_price", {})
                    .get("fields", {})
                    .get("core." + field, {})
                )
                left = bisect_left(population, value) if value is not None else None
                right = bisect_right(population, value) if value is not None else None
                percentile = (
                    Fraction(100)
                    * (left + Fraction(right - left - 1, 2))
                    / (len(population) - 1)
                    if value is not None and len(population) > 1
                    else None
                )
                metrics[field] = {
                    "value": str(value) if value is not None else None,
                    "quality": source_field.get("quality", "UNKNOWN"),
                    "reason": source_field.get("reason")
                    if source_field
                    else "SOURCE_SYMBOL_OR_FIELD_MISSING",
                    "member_median": str(medians[field])
                    if medians[field] is not None
                    else None,
                    "difference_from_median": str(value - medians[field])
                    if value is not None and medians[field] is not None
                    else None,
                    "known_count": len(population),
                    "unknown_count": len(members) - len(population),
                    "descending_rank_min": len(population) - right + 1
                    if value is not None
                    else None,
                    "tie_count": right - left if value is not None else None,
                    "midrank_percentile": str(percentile)
                    if percentile is not None
                    else None,
                    "percentile_delta_pp": _change(
                        previous.get(stock, {})
                        .get(field, {})
                        .get("midrank_percentile"),
                        str(percentile) if percentile is not None else None,
                    ),
                    "value_delta": _change(
                        previous.get(stock, {}).get(field, {}).get("value"),
                        str(value) if value is not None else None,
                    ),
                    "rank_delta": _change(
                        previous.get(stock, {})
                        .get(field, {})
                        .get("descending_rank_min"),
                        len(population) - right + 1 if value is not None else None,
                    ),
                }
            positions[stock] = metrics
        selected = (
            [symbol]
            if symbol
            else sorted(s for (session, s) in events if session == day["session"])
        )
        rows = []
        for stock in selected:
            if stock not in member_set:
                continue
            fact = facts.get(stock, {})
            stock_events = events.get((day["session"], stock), [])
            rows.append(
                {
                    "source_symbol": stock,
                    "relative_position": positions[stock],
                    "rps_full_source_pool": fact.get("rps"),
                    "seed": {key: fact.get(key) for key in ("s1", "s2", "seed")},
                    "d0": {name: rows.get(stock) for name, rows in d0.items()},
                    "d1": d1.get(stock),
                    "states": day["states"].get(stock),
                    "observation_reason": (
                        "UNKNOWN_SOURCE_SYMBOL_NOT_IN_POOL"
                        if stock not in day["states"]
                        else "DISCOVERED_EVENT"
                        if stock_events
                        else "NO_PROVED_SELECTED_TRANSITION_OR_D1_CHANGE"
                    ),
                    "events": stock_events,
                    "formal_eligibility": "UNKNOWN",
                    "formal_authorization": "FORMAL_NOT_AUTHORIZED",
                }
            )
        board = board_rows.get((day["session"], board_kind, board_id))
        loo = {"state": "NOT_REQUESTED"}
        if symbol:
            if symbol not in member_set:
                loo = {"state": "NOT_APPLICABLE", "source_symbol": symbol}
            elif day["seed"].get("status") == "UNKNOWN_DATA":
                loo = {"state": "UNKNOWN", "reason": day["seed"].get("reason")}
            else:
                reduced = {
                    key: [s for s in group if s != symbol]
                    for key, group in peers.items()
                }
                before = board.get("native_detail") if board else None
                after = _board_native(day["seed"], reduced[board_id], reduced, board_id)
                loo = {
                    "state": "APPLIED",
                    "source_symbol": symbol,
                    "source_mapping": "MAPPED" if symbol in facts else "UNMAPPED",
                    "scope": "当日Native同类完整重算；跨期LOO未计算",
                    "affected_board_count": sum(
                        symbol in group for group in peers.values()
                    ),
                    "before": before,
                    "after": after,
                    "differences": _loo_differences(before, after)
                    if isinstance(before, dict) and "ret5" in before
                    else None,
                }
        output.append(
            {
                "session": day["session"],
                "board": board,
                "rps_source_pool_counts": day["seed"].get("endpoint_counts"),
                "loo": loo,
                "stocks": rows,
            }
        )
        previous = positions
    return {
        "membership_status": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
        "price_coordinate": "RAW_UNADJUSTED",
        "member_count": len(members),
        "selection_scope": "指定股票时逐日显示该成员；否则显示该板块全部已发现成员，不受事件分页和信号筛选影响",
        "rank_method": "同指标已知成员降序最小名次；百分位采用等值中秩，0最弱100最强",
        "sessions": output,
    }
