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
