"""市场宽度、成交参与和复合解释标签的纯数学规则。"""

from __future__ import annotations

from fractions import Fraction
from itertools import product
from typing import Any, Iterable


_TREND = ("STRONG", "NEUTRAL", "WEAK")
_BREADTH = ("STRONG", "NEUTRAL", "WEAK")
_PARTICIPATION = ("EXPANDING", "NORMAL", "THIN")
_PRESSURE = ("HIGH", "ELEVATED", "LOW")


def exact_median(values: Iterable[Fraction]) -> Fraction | None:
    """求有效精确有理数中位数，偶数样本取中间两数的精确算术平均。"""
    ordered = sorted(values)
    if not ordered:
        return None
    if any(not isinstance(value, Fraction) for value in ordered):
        raise TypeError("成交参与中位数只接受精确Fraction输入")
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2


def classify_market_breadth(value: Fraction | None) -> str:
    """按冻结B_fixed_source门槛产生市场宽度分类。"""
    if value is None:
        return "UNKNOWN"
    if not isinstance(value, Fraction):
        raise TypeError("市场宽度必须是精确Fraction或None")
    if value >= Fraction(1, 20):
        return "STRONG"
    if value <= Fraction(-1, 20):
        return "WEAK"
    return "NEUTRAL"


def classify_market_participation(value: Fraction | None) -> str:
    """按冻结amr20中位数门槛产生成交参与分类。"""
    if value is None:
        return "UNKNOWN"
    if not isinstance(value, Fraction) or value < 0:
        raise ValueError("成交参与必须是非负精确Fraction或None")
    if value >= Fraction(6, 5):
        return "EXPANDING"
    if value < Fraction(4, 5):
        return "THIN"
    return "NORMAL"


def market_environment_label(
    trend: str | None,
    breadth: str | None,
    participation: str | None,
    pressure: str | None,
) -> dict[str, Any]:
    """枚举未知轴的合法补全；仅在全部补全得到同一标签时报告已知。"""
    axes = (
        (trend, _TREND),
        (breadth, _BREADTH),
        (participation, _PARTICIPATION),
        (pressure, _PRESSURE),
    )
    candidates: list[tuple[str, ...]] = []
    missing_axes: list[str] = []
    for name, (value, allowed) in zip(
        ("trend", "breadth", "participation", "pressure"), axes, strict=True
    ):
        if value is None or value == "UNKNOWN" or value == "NOT_COMPUTED":
            candidates.append(allowed)
            missing_axes.append(name)
        elif value in allowed:
            candidates.append((value,))
        else:
            raise ValueError(f"市场{name}轴标签非法：{value}")

    labels = {_raw_market_label(*values) for values in product(*candidates)}
    ordered = sorted(labels)
    return {
        "status": "KNOWN" if len(ordered) == 1 else "UNKNOWN",
        "label": ordered[0] if len(ordered) == 1 else None,
        "possible_labels": ordered,
        "unknown_axes": missing_axes,
        "completion_count": 1 if not missing_axes else _completion_count(candidates),
    }


def _completion_count(candidates: list[tuple[str, ...]]) -> int:
    count = 1
    for values in candidates:
        count *= len(values)
    return count


def _raw_market_label(
    trend: str, breadth: str, participation: str, pressure: str
) -> str:
    if pressure == "HIGH" or (trend == "WEAK" and breadth == "WEAK"):
        return "DEFENSIVE"
    if (
        trend == "STRONG"
        and breadth == "STRONG"
        and participation == "EXPANDING"
        and pressure == "LOW"
    ):
        return "EXPANSIVE"
    if (
        trend != "WEAK"
        and breadth == "STRONG"
        and participation != "THIN"
        and pressure == "LOW"
    ):
        return "IMPROVING"
    return "MIXED"


def reduce_market_environment_display_series(
    observations: Iterable[dict[str, Any]],
    *,
    official_sessions: Iterable[Any] | None = None,
    initial_state: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """按 §11.4 规则将有序官方会话上的 raw 复合研究标签归约为显示状态序列。

    规则：
    - 传入 official_sessions 时，强制校验官方会话严格递增无重复；缺失会话自动以 UNKNOWN 插入
      （触发 STALE 并清空滞后转换计数），杜绝跨缺日误确认；
    - 首次已知 raw 标签直接显示并标记 FIRST_OBSERVED；
    - 规则版本或主参考指数切换单独标记 RULE_VERSION_CHANGED 并重起滞后计数；
    - DEFENSIVE 立即显示，无需 2 日确认；
    - 其余标签转换需连续 2 个相邻已知相同 raw 会话确认；
    - 遇到 UNKNOWN 暂停切换、保留 last_known 并标 STALE，同时清空转换计数。
    """
    obs_list = list(observations)
    if official_sessions is not None:
        ordered_sessions = [
            s.isoformat() if hasattr(s, "isoformat") else str(s)
            for s in official_sessions
        ]
        if not ordered_sessions:
            raise ValueError("OFFICIAL_SESSIONS_EMPTY")
        for prev_s, cur_s in zip(ordered_sessions, ordered_sessions[1:]):
            if cur_s <= prev_s:
                raise ValueError("OFFICIAL_SESSIONS_MUST_BE_STRICTLY_INCREASING")
        by_session: dict[str, dict[str, Any]] = {}
        for item in obs_list:
            raw_s = item.get("session")
            key = raw_s.isoformat() if hasattr(raw_s, "isoformat") else str(raw_s)
            if key in by_session:
                raise ValueError(f"DUPLICATE_MARKET_ENVIRONMENT_SESSION:{key}")
            if key not in set(ordered_sessions):
                raise ValueError(f"SESSION_NOT_IN_OFFICIAL_CALENDAR:{key}")
            by_session[key] = item
        default_rule = (
            str(obs_list[0].get("rule_version") or "MARKET_PRESENTATION_R1")
            if obs_list
            else "MARKET_PRESENTATION_R1"
        )
        default_primary = obs_list[0].get("primary_index_symbol") if obs_list else None
        aligned_obs: list[dict[str, Any]] = []
        last_rule = default_rule
        last_primary = default_primary
        for sess in ordered_sessions:
            found = by_session.get(sess)
            if found is not None:
                last_rule = str(found.get("rule_version") or last_rule)
                if "primary_index_symbol" in found:
                    last_primary = found.get("primary_index_symbol")
                aligned_obs.append(found)
            else:
                aligned_obs.append(
                    {
                        "session": sess,
                        "raw_status": "UNKNOWN",
                        "raw_label": None,
                        "rule_version": last_rule,
                        "primary_index_symbol": last_primary,
                    }
                )
        obs_list = aligned_obs

    displayed_label: str | None = None
    active_rule_key: tuple[str, str | None] | None = None
    pending_label: str | None = None
    pending_count = 0
    if isinstance(initial_state, dict):
        displayed_label = initial_state.get("displayed_label")
        pending_label = initial_state.get("pending_label")
        pending_count = int(initial_state.get("pending_count") or 0)
        init_rv = initial_state.get("rule_version")
        init_pi = initial_state.get("primary_index_symbol")
        if init_rv is not None:
            active_rule_key = (
                str(init_rv),
                None if init_pi is None else str(init_pi),
            )
    results: list[dict[str, Any]] = []

    for item in obs_list:
        raw_sess = item.get("session")
        session = raw_sess.isoformat() if hasattr(raw_sess, "isoformat") else raw_sess
        raw_status = item.get("raw_status")
        raw_label = item.get("raw_label")
        rule_version = str(item.get("rule_version") or "MARKET_PRESENTATION_R1")
        primary_index = item.get("primary_index_symbol")
        rule_key = (
            rule_version,
            None if primary_index is None else str(primary_index),
        )

        # 规则版本或主参考指数切换单独列示并独立起算
        if active_rule_key is not None and rule_key != active_rule_key:
            active_rule_key = rule_key
            pending_label = None
            pending_count = 0
            if raw_status == "KNOWN" and isinstance(raw_label, str):
                displayed_label = raw_label
                results.append(
                    {
                        "session": session,
                        "rule_version": rule_version,
                        "primary_index_symbol": primary_index,
                        "raw_status": raw_status,
                        "raw_label": raw_label,
                        "displayed_label": displayed_label,
                        "display_status": "RULE_VERSION_CHANGED",
                        "pending_label": None,
                        "pending_count": 0,
                    }
                )
            else:
                displayed_label = None
                results.append(
                    {
                        "session": session,
                        "rule_version": rule_version,
                        "primary_index_symbol": primary_index,
                        "raw_status": raw_status or "UNKNOWN",
                        "raw_label": None,
                        "displayed_label": None,
                        "display_status": "RULE_VERSION_CHANGED",
                        "pending_label": None,
                        "pending_count": 0,
                    }
                )
            continue

        if active_rule_key is None:
            active_rule_key = rule_key

        # 当日 raw 未知：暂停切换、清转换计数；已有显示则保留并标 STALE
        if raw_status != "KNOWN" or not isinstance(raw_label, str):
            pending_label = None
            pending_count = 0
            results.append(
                {
                    "session": session,
                    "rule_version": rule_version,
                    "raw_status": raw_status or "UNKNOWN",
                    "raw_label": None,
                    "displayed_label": displayed_label,
                    "display_status": (
                        "STALE" if displayed_label is not None else "UNKNOWN"
                    ),
                    "pending_label": None,
                    "pending_count": 0,
                }
            )
            continue

        # 首次已知：直接显示并标记 FIRST_OBSERVED
        if displayed_label is None:
            displayed_label = raw_label
            pending_label = None
            pending_count = 0
            results.append(
                {
                    "session": session,
                    "rule_version": rule_version,
                    "raw_status": "KNOWN",
                    "raw_label": raw_label,
                    "displayed_label": displayed_label,
                    "display_status": "FIRST_OBSERVED",
                    "pending_label": None,
                    "pending_count": 0,
                }
            )
            continue

        # 与当前显示相同：保持并清转换计数
        if raw_label == displayed_label:
            pending_label = None
            pending_count = 0
            results.append(
                {
                    "session": session,
                    "rule_version": rule_version,
                    "raw_status": "KNOWN",
                    "raw_label": raw_label,
                    "displayed_label": displayed_label,
                    "display_status": "HELD",
                    "pending_label": None,
                    "pending_count": 0,
                }
            )
            continue

        # DEFENSIVE 立即显示
        if raw_label == "DEFENSIVE":
            displayed_label = "DEFENSIVE"
            pending_label = None
            pending_count = 0
            results.append(
                {
                    "session": session,
                    "rule_version": rule_version,
                    "raw_status": "KNOWN",
                    "raw_label": raw_label,
                    "displayed_label": displayed_label,
                    "display_status": "TRANSITIONED_IMMEDIATE_DEFENSIVE",
                    "pending_label": None,
                    "pending_count": 0,
                }
            )
            continue

        # 其余非 DEFENSIVE 转换：需连续 2 个相邻已知相同 raw 会话确认
        if pending_label == raw_label and pending_count >= 1:
            displayed_label = raw_label
            pending_label = None
            pending_count = 0
            results.append(
                {
                    "session": session,
                    "rule_version": rule_version,
                    "raw_status": "KNOWN",
                    "raw_label": raw_label,
                    "displayed_label": displayed_label,
                    "display_status": "TRANSITIONED_CONFIRMED",
                    "pending_label": None,
                    "pending_count": 0,
                }
            )
        else:
            pending_label = raw_label
            pending_count = 1
            results.append(
                {
                    "session": session,
                    "rule_version": rule_version,
                    "raw_status": "KNOWN",
                    "raw_label": raw_label,
                    "displayed_label": displayed_label,
                    "display_status": "HYSTERESIS_PENDING",
                    "pending_label": pending_label,
                    "pending_count": pending_count,
                }
            )

    return results
