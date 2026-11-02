"""ALG-29来源级原始突破谓词；不创建正式Anchor或交易信号。"""

from __future__ import annotations

from fractions import Fraction
from typing import Any

from stock_a.domain import FieldValue, exact_number


def breakout_price_passes(
    close: Fraction, prior_high20: Fraction, atr20: Fraction
) -> bool:
    """复用ALG-29的严格价格条件，调用者负责字段质量和数值校验。"""
    return close > prior_high20 + atr20 / 10


def breakout_clv_passes(clv: Fraction) -> bool:
    """复用ALG-29的非严格CLV边界。"""
    return clv >= Fraction(7, 10)


def _observed_number(
    field: FieldValue, name: str
) -> tuple[Fraction | None, str | None]:
    if field.quality != "OBSERVED":
        return None, field.reason or "UNKNOWN_REASON_NOT_PROVIDED"
    try:
        return exact_number(field.value), None
    except (TypeError, ValueError, ZeroDivisionError):
        return None, f"DATA_INVALID_{name.upper()}"


def _part(
    state: str,
    *,
    reason: str | None = None,
    reasons: tuple[str, ...] = (),
    evidence: dict[str, Fraction | None] | None = None,
) -> dict[str, Any]:
    return {
        "state": state,
        "reason": reason,
        "dependency_reasons": list(reasons),
        "evidence": evidence or {},
    }


def evaluate_raw_breakout_predicate(
    close: FieldValue,
    prior_high20: FieldValue,
    atr20: FieldValue,
    clv: FieldValue,
) -> dict[str, Any]:
    """只消费单一Core生产者的精确字段并按Kleene规则合取P1/P2。"""
    fields = {
        "close": close,
        "prior_high20": prior_high20,
        "atr20": atr20,
        "clv": clv,
    }
    if any(not isinstance(field, FieldValue) for field in fields.values()):
        raise TypeError("ALG-29谓词输入必须使用携带质量的FieldValue")

    values: dict[str, Fraction | None] = {}
    field_reasons: dict[str, str | None] = {}
    for name, field in fields.items():
        value, reason = _observed_number(field, name)
        values[name] = value
        field_reasons[name] = reason

    p1_reasons = [
        field_reasons[name]
        for name in ("close", "prior_high20", "atr20")
        if field_reasons[name]
    ]
    p2_reasons = [field_reasons["clv"]] if field_reasons["clv"] else []

    close_value = values["close"]
    high_value = values["prior_high20"]
    atr_value = values["atr20"]
    clv_value = values["clv"]
    threshold: Fraction | None = None

    if not p1_reasons:
        assert (
            close_value is not None and high_value is not None and atr_value is not None
        )
        threshold = high_value + atr_value / 10

    if p1_reasons:
        p1 = _part(
            "UNKNOWN",
            reason=p1_reasons[0],
            reasons=tuple(dict.fromkeys(p1_reasons)),
            evidence={"close": close_value, "threshold": threshold},
        )
    else:
        assert close_value is not None and threshold is not None
        p1 = _part(
            "TRUE"
            if breakout_price_passes(close_value, high_value, atr_value)
            else "FALSE",
            evidence={"close": close_value, "threshold": threshold},
        )

    if p2_reasons:
        p2 = _part(
            "UNKNOWN",
            reason=p2_reasons[0],
            reasons=tuple(dict.fromkeys(p2_reasons)),
            evidence={"clv": clv_value, "threshold": Fraction(7, 10)},
        )
    else:
        assert clv_value is not None
        p2 = _part(
            "TRUE" if breakout_clv_passes(clv_value) else "FALSE",
            evidence={"clv": clv_value, "threshold": Fraction(7, 10)},
        )

    if "FALSE" in {p1["state"], p2["state"]}:
        state = "FALSE"
    elif p1["state"] == p2["state"] == "TRUE":
        state = "TRUE"
    else:
        state = "UNKNOWN"
    dependencies = tuple(
        dict.fromkeys((*p1["dependency_reasons"], *p2["dependency_reasons"]))
    )
    return {
        "p1": p1,
        "p2": p2,
        "result": {
            "state": state,
            "reason": dependencies[0] if state == "UNKNOWN" and dependencies else None,
            "dependency_reasons": list(dependencies),
        },
    }
