"""同日同源的显式探索对象比较，不构造正式对照或观察入组。"""

from collections import Counter
from fractions import Fraction
from statistics import median

from stock_a.research.d0_launch_raw import fraction_payload


def compare_shape_observations(results):
    """分别统计父候选事件和去重股票，未知前态不作为无匹配。"""
    if not results:
        raise ValueError("需要至少一个来源观察对象")
    first = results[0]
    identity = (
        first["target_session"],
        first["research_cutoff"],
        first["source_version"]["source_package_id"],
    )
    groups = {
        state: {"symbols": [], "event_counts": Counter(), "returns": []}
        for state in ("TRUE", "FALSE", "UNKNOWN")
    }
    event_returns = {state: [] for state in groups}
    event_counts = Counter()
    seen = set()
    endpoint = None
    for result in results:
        actual = (
            result["target_session"],
            result["research_cutoff"],
            result["source_version"]["source_package_id"],
        )
        if actual != identity or len(result["results"]) != 1:
            raise ValueError("比较对象须绑定相同目标日、截止日及来源版本")
        item = result["results"][0]
        symbol = item["source_symbol"]
        if symbol in seen:
            raise ValueError("比较对象来源代码重复")
        seen.add(symbol)
        obs = item["prior_shape_observation"]
        candidates = obs["diagnostic_parent_candidates"]
        states = [
            candidate.get("source_shape_state", "UNKNOWN") for candidate in candidates
        ]
        event_counts.update(states)
        history = obs["historical_states"]
        incomplete = len(history) != 20 or any(
            row.get("launch") == "UNKNOWN" or row.get("recovery") == "UNKNOWN"
            for row in history
        )
        state = (
            "TRUE"
            if "TRUE" in states
            else "UNKNOWN"
            if incomplete or not states or "UNKNOWN" in states
            else "FALSE"
        )
        group = groups[state]
        group["symbols"].append(symbol)
        group["event_counts"].update(states)
        facts = obs["post_signal_price_facts"]["facts"]
        points = facts["points"]
        signature = tuple(point["session"] for point in points)
        if endpoint is None:
            endpoint = signature
        elif signature != endpoint:
            raise ValueError("比较对象后继官方会话窗口不一致")
        point = points[-1] if points else None
        value = point["raw_close_relative_change"] if point else {}
        exact = value.get("value")
        if value.get("quality") == "OBSERVED" and isinstance(exact, dict):
            observed_return = Fraction(exact["exact"])
            group["returns"].append(observed_return)
            for event_state in states:
                event_returns[event_state].append(observed_return)
    return {
        "scenario": "STRONG_PULLBACK",
        "scope": "EXPLICIT_SOURCE_SAMPLE_NON_PIT_NOT_MATCHED_CONTROLS",
        "selection": "EXPLICIT_INPUT_SELECTION_TIME_NOT_VERIFIED",
        "target_session": identity[0],
        "followup_sessions": endpoint,
        "events": {
            state: {
                "count": event_counts[state],
                "valued_count": len(event_returns[state]),
                "unvalued_count": event_counts[state] - len(event_returns[state]),
                "median_return": fraction_payload(median(event_returns[state]))
                if event_returns[state]
                else None,
            }
            for state in groups
        },
        "groups": {
            state: {
                "unique_stock_count": len(group["symbols"]),
                "symbols": group["symbols"],
                "candidate_event_counts": dict(group["event_counts"]),
                "valued_stock_count": len(group["returns"]),
                "unvalued_stock_count": len(group["symbols"]) - len(group["returns"]),
                "median_stock_return": fraction_payload(median(group["returns"]))
                if group["returns"]
                else None,
            }
            for state, group in groups.items()
        },
    }
