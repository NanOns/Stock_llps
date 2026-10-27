"""Core 纯计算因子；只接受显式会话、质量和坐标输入。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, localcontext
from fractions import Fraction
from statistics import mean
from typing import Sequence

from stock_a.domain import ContractError, FieldValue, exact_number, unknown


@dataclass(frozen=True)
class CoreObservation:
    """一个官方市场会话；状态未知与确认停牌必须由上游明确给出。"""

    session: date
    state: str
    coordinate_id: str | None = None
    open: FieldValue | None = None
    high: FieldValue | None = None
    low: FieldValue | None = None
    close: FieldValue | None = None
    amount: FieldValue | None = None
    volume: FieldValue | None = None


_STATES = {"ACTUAL", "CONFIRMED_SUSPENSION", "UNKNOWN", "PRE_LISTING",
           "ADJUSTMENT_UNKNOWN", "IDENTITY_UNKNOWN"}
_PRICE_FIELDS = ("open", "high", "low", "close")


def _validate_history(history: Sequence[CoreObservation]) -> None:
    if not history:
        raise ContractError("Core历史会话不能为空")
    for row in history:
        if not isinstance(row, CoreObservation) or type(row.session) is not date:
            raise ContractError("Core输入必须是含date会话的CoreObservation")
        if not isinstance(row.state, str) or row.state not in _STATES:
            raise ContractError("Core输入含未注册会话状态")
        if row.state == "ACTUAL":
            if not isinstance(row.coordinate_id, str) or not row.coordinate_id:
                raise ContractError("actual bar缺少价格坐标身份")
            if any(not isinstance(getattr(row, name), FieldValue)
                   for name in (*_PRICE_FIELDS, "amount", "volume")):
                raise ContractError("actual bar的价格、金额和数量必须显式携带质量")
    if any(left.session >= right.session for left, right in zip(history, history[1:])):
        raise ContractError("Core会话必须唯一且按日期严格递增")


def _actual_window(history: Sequence[CoreObservation], count: int,
                   *, exclude_current: bool = False) -> tuple[list[CoreObservation], str | None]:
    end = len(history) - (1 if exclude_current else 0)
    if end <= 0:
        return [], "SHORT_HISTORY"
    selected: list[CoreObservation] = []
    for row in reversed(history[:end]):
        if row.state == "CONFIRMED_SUSPENSION":
            continue
        if row.state == "PRE_LISTING":
            break
        if row.state != "ACTUAL":
            return list(reversed(selected)), {
                "UNKNOWN": "UNKNOWN_SESSION_GAP",
                "ADJUSTMENT_UNKNOWN": "PRICE_IDENTITY_UNKNOWN",
                "IDENTITY_UNKNOWN": "SECURITY_IDENTITY_UNKNOWN",
            }[row.state]
        selected.append(row)
        if len(selected) == count:
            return list(reversed(selected)), None
    return list(reversed(selected)), "SHORT_HISTORY"


def _number(field: FieldValue, reason: str) -> tuple[Fraction | None, str | None]:
    if field.quality != "OBSERVED":
        return None, field.reason or "UPSTREAM_UNKNOWN"
    try:
        return exact_number(field.value), None
    except (ValueError, TypeError, ZeroDivisionError):
        return None, reason


def _numeric_column(rows: Sequence[CoreObservation], name: str,
                    *, nonnegative: bool = False) -> tuple[list[Fraction] | None, str | None]:
    values: list[Fraction] = []
    for row in rows:
        value, reason = _number(getattr(row, name), f"DATA_INVALID_{name.upper()}")
        if reason:
            return None, reason
        if nonnegative and value < 0:
            return None, f"DATA_INVALID_{name.upper()}"
        values.append(value)
    return values, None


def _prices(rows: Sequence[CoreObservation]) -> tuple[list[tuple[Fraction, Fraction, Fraction, Fraction]] | None,
                                                     str | None]:
    if len({row.coordinate_id for row in rows}) != 1:
        return None, "MIXED_PRICE_COORDINATE"
    result = []
    for row in rows:
        vals = []
        for name in _PRICE_FIELDS:
            value, reason = _number(getattr(row, name), "DATA_INVALID_OHLC")
            if reason:
                return None, "DATA_INVALID_OHLC" if reason.startswith("DATA_INVALID") else reason
            vals.append(value)
        o, h, low, close = vals
        if min(vals) <= 0 or low > min(o, close) or max(o, close) > h or h < low:
            return None, "DATA_INVALID_OHLC"
        result.append((o, h, low, close))
    return result, None


def _known(value, reason: str | None = None) -> FieldValue:
    return unknown(reason) if reason else FieldValue(value)


def _fixed_session_window(history: Sequence[CoreObservation], periods: int):
    if len(history) < periods + 1:
        return list(history), "SHORT_SESSION_HISTORY"
    rows = list(history[-periods - 1:])
    for row in rows:
        if row.state != "ACTUAL":
            return rows, {
                "UNKNOWN": "UNKNOWN_SESSION_GAP",
                "CONFIRMED_SUSPENSION": "SUSPENDED_ENDPOINT_OR_WINDOW",
                "PRE_LISTING": "PRE_LISTING_ENDPOINT",
                "ADJUSTMENT_UNKNOWN": "PRICE_IDENTITY_UNKNOWN",
                "IDENTITY_UNKNOWN": "SECURITY_IDENTITY_UNKNOWN",
            }[row.state]
    return rows, None


def _decimal_fraction(value: Fraction) -> Decimal:
    return Decimal(value.numerator) / Decimal(value.denominator)


def _volatility(rows: Sequence[CoreObservation], periods: int):
    session_rows, reason = _fixed_session_window(rows, periods)
    if reason:
        return unknown(reason)
    bars, reason = _prices(session_rows)
    if reason:
        return unknown(reason)
    closes = [bar[3] for bar in bars]
    if any(close <= 0 for close in closes):
        return unknown("INVALID_PRICE_DENOMINATOR")
    with localcontext() as ctx:
        ctx.prec = 34
        returns = [(_decimal_fraction(b) / _decimal_fraction(a)).ln()
                   for a, b in zip(closes, closes[1:])]
        avg = sum(returns, Decimal(0)) / Decimal(periods)
        variance = sum(((r - avg) ** 2 for r in returns), Decimal(0)) / Decimal(periods)
        return FieldValue(variance.sqrt())


def calculate_core(history: Sequence[CoreObservation]) -> dict[str, FieldValue]:
    """计算最新actual会话的Core字段，不做IO、发布或隐式缺值回退。"""
    _validate_history(history)
    latest = history[-1]
    if latest.state != "ACTUAL":
        reason = {
            "UNKNOWN": "CURRENT_SESSION_UNKNOWN",
            "CONFIRMED_SUSPENSION": "CURRENT_SESSION_SUSPENDED",
            "PRE_LISTING": "CURRENT_SESSION_PRE_LISTING",
            "ADJUSTMENT_UNKNOWN": "PRICE_IDENTITY_UNKNOWN",
            "IDENTITY_UNKNOWN": "SECURITY_IDENTITY_UNKNOWN",
        }[latest.state]
        return {name: unknown(reason) for name in _OUTPUT_FIELDS}

    result: dict[str, FieldValue] = {}
    for period in (5, 10, 20, 60):
        rows, reason = _actual_window(history, period)
        values, value_reason = _prices(rows) if not reason else (None, reason)
        values = values or []
        result[f"core.ma{period}"] = _known(mean([x[3] for x in values]) if values else None,
                                               reason or value_reason)
        if period in (5, 20, 60):
            result[f"core.hhv{period}"] = _known(max((x[1] for x in values), default=None), reason or value_reason)
            result[f"core.llv{period}"] = _known(min((x[2] for x in values), default=None), reason or value_reason)
            prior_rows, prior_reason = _actual_window(history, period, exclude_current=True)
            prior_prices, price_reason = _prices(prior_rows) if not prior_reason else (None, prior_reason)
            prior_prices = prior_prices or []
            result[f"core.prior_high{period}"] = _known(max((x[1] for x in prior_prices), default=None),
                                                           prior_reason or price_reason)
            result[f"core.prior_low{period}"] = _known(min((x[2] for x in prior_prices), default=None),
                                                          prior_reason or price_reason)

    # True range uses consecutive selected actual bars; known suspensions may be skipped.
    tr_rows, tr_reason = _actual_window(history, 2)
    tr_prices, price_reason = _prices(tr_rows) if not tr_reason else (None, tr_reason)
    if tr_prices:
        prev, cur = tr_prices
        tr = max(cur[1] - cur[2], abs(cur[1] - prev[3]), abs(cur[2] - prev[3]))
        result["core.tr"] = _known(tr)
    else:
        result["core.tr"] = unknown(tr_reason or price_reason)

    for period in (5, 20):
        rows, reason = _actual_window(history, period + 1)
        values, value_reason = _prices(rows) if not reason else (None, reason)
        if values:
            true_ranges = [max(cur[1] - cur[2], abs(cur[1] - prev[3]), abs(cur[2] - prev[3]))
                           for prev, cur in zip(values, values[1:])]
            result[f"core.atr{period}"] = _known(mean(true_ranges))
        else:
            result[f"core.atr{period}"] = unknown(reason or value_reason)

    # MA slope windows compare current and lagged actual-bar moving averages.
    for period, offset in ((20, 5), (60, 10)):
        rows, reason = _actual_window(history, period + offset)
        values, value_reason = _prices(rows) if not reason else (None, reason)
        atr = result["core.atr20"]
        reason = reason or value_reason or (atr.reason if atr.quality != "OBSERVED" else None)
        if not reason and atr.value <= 0:
            reason = "ZERO_ATR"
        if reason:
            result[f"core.ma_slope{period}_atr"] = unknown(reason)
        else:
            earlier = values[-period-offset:-offset]
            current = values[-period:]
            result[f"core.ma_slope{period}_atr"] = FieldValue((mean(x[3] for x in current)
                                                                  - mean(x[3] for x in earlier)) / atr.value)

    # Ratios use current raw value over the strict prior-N actual-bar mean.
    for name in ("amount", "volume"):
        for period in (5, 20):
            prior, reason = _actual_window(history, period, exclude_current=True)
            prior_values, value_reason = _numeric_column(prior, name, nonnegative=True) if not reason else (None, reason)
            current, current_reason = _number(getattr(latest, name), f"DATA_INVALID_{name.upper()}")
            reason = reason or value_reason or current_reason
            if not reason and current < 0:
                reason = f"DATA_INVALID_{name.upper()}"
            denominator = mean(prior_values) if prior_values else None
            if not reason and (denominator is None or denominator <= 0):
                reason = "NONPOSITIVE_DENOMINATOR"
            key = "amount_ratio" if name == "amount" else "volume_ratio"
            result[f"core.{key}{period}"] = unknown(reason) if reason else FieldValue(current / denominator)
    prior_amounts, prior_reason = _actual_window(history, 20, exclude_current=True)
    prior_values, amount_reason = _numeric_column(prior_amounts, "amount", nonnegative=True) if not prior_reason else (None, prior_reason)
    result["core.liquidity_mean20"] = (unknown(prior_reason or amount_reason) if prior_reason or amount_reason
                                         else FieldValue(mean(prior_values)))

    current_prices, current_reason = _prices([latest])
    if current_prices:
        o, h, low, close = current_prices[0]
        result["core.clv"] = unknown("ZERO_PRICE_RANGE") if h == low else FieldValue((close - low) / (h - low))
    else:
        result["core.clv"] = unknown(current_reason)

    result["core.ret1"] = _return(history, 1)
    for period in (3, 5, 20):
        result[f"core.ret{period}"] = _return(history, period)
    for period in (5, 20):
        result[f"core.vol{period}"] = _volatility(history, period)

    # Price location, range/volatility ratios and prior-window diagnostics.
    hhv60, llv60 = result["core.hhv60"], result["core.llv60"]
    if hhv60.quality != "OBSERVED" or llv60.quality != "OBSERVED" or not current_prices:
        reason = hhv60.reason or llv60.reason or current_reason
        result["core.pos60"] = unknown(reason)
    elif hhv60.value == llv60.value:
        result["core.pos60"] = unknown("ZERO_PRICE_RANGE")
    else:
        result["core.pos60"] = FieldValue((current_prices[0][3] - llv60.value) / (hhv60.value - llv60.value))
    _derived_price_ratios(result, current_prices)
    _progress(result, history)
    _prior_percentile(result, history, current_prices)
    _price_damage(result, current_prices)
    return result


def _return(history: Sequence[CoreObservation], periods: int) -> FieldValue:
    rows, reason = _fixed_session_window(history, periods)
    if reason:
        return unknown(reason)
    values, price_reason = _prices(rows)
    if price_reason:
        return unknown(price_reason)
    start, end = values[0][3], values[-1][3]
    return unknown("INVALID_PRICE_DENOMINATOR") if start <= 0 else FieldValue(end / start - 1)


def _derived_price_ratios(result, current_prices):
    current = current_prices[0][3] if current_prices else None
    for target, numerator, denominator in (
        ("core.bias20_atr", "core.ma20", "core.atr20"),
        ("core.dist_high20_atr", "core.prior_high20", "core.atr20"),
    ):
        a, b = result[numerator], result[denominator]
        reason = a.reason if a.quality != "OBSERVED" else b.reason if b.quality != "OBSERVED" else None
        if not current_prices:
            reason = reason or "CURRENT_PRICE_UNKNOWN"
        elif not reason and b.value <= 0:
            reason = "ZERO_ATR"
        if reason:
            result[target] = unknown(reason)
        else:
            value = (current - a.value if target.endswith("bias20_atr") else a.value - current) / b.value
            result[target] = FieldValue(value)

    for target, num_key, den_key in (
        ("core.range_ratio", "core.hhv5", "core.hhv20"),
        ("core.atr_ratio", "core.atr5", "core.atr20"),
        ("core.vol_ratio", "core.vol5", "core.vol20"),
    ):
        if target == "core.range_ratio":
            numerator = result[num_key].value - result["core.llv5"].value if result[num_key].quality == result["core.llv5"].quality == "OBSERVED" else None
            denominator = result[den_key].value - result["core.llv20"].value if result[den_key].quality == result["core.llv20"].quality == "OBSERVED" else None
            deps = [result[num_key], result["core.llv5"], result[den_key], result["core.llv20"]]
        else:
            numerator, denominator = result[num_key].value, result[den_key].value
            deps = [result[num_key], result[den_key]]
        reason = next((x.reason for x in deps if x.quality != "OBSERVED"), None)
        if not reason and (denominator is None or denominator <= 0):
            reason = "NONPOSITIVE_DENOMINATOR"
        result[target] = unknown(reason) if reason else FieldValue(numerator / denominator)


def _progress(result, history):
    rows, reason = _actual_window(history, 10)
    values, value_reason = _prices(rows) if not reason else (None, reason)
    if reason or value_reason:
        result["core.hh_progress"] = unknown(reason or value_reason)
        result["core.ll_progress"] = unknown(reason or value_reason)
        return
    result["core.hh_progress"] = FieldValue(max(x[1] for x in values[-5:]) > max(x[1] for x in values[:5]))
    result["core.ll_progress"] = FieldValue(min(x[2] for x in values[-5:]) < min(x[2] for x in values[:5]))


def _prior_percentile(result, history, current_prices):
    rows, reason = _actual_window(history, 60, exclude_current=True)
    values, value_reason = _prices(rows) if not reason else (None, reason)
    if reason or value_reason or not current_prices:
        result["core.prior60_percentile"] = unknown(reason or value_reason or "CURRENT_PRICE_UNKNOWN")
        return
    current = current_prices[0][3]
    closes = [x[3] for x in values]
    less = sum(x < current for x in closes)
    equal = sum(x == current for x in closes)
    result["core.prior60_percentile"] = FieldValue(Fraction(100 * (less + Fraction(equal, 2)), 60))


def _price_damage(result, current_prices):
    prior, atr, ret = result["core.prior_low20"], result["core.atr20"], result["core.ret1"]
    reason = next((x.reason for x in (prior, atr, ret) if x.quality != "OBSERVED"), None)
    if not current_prices:
        reason = reason or "CURRENT_PRICE_UNKNOWN"
    if reason:
        result["core.price_damage"] = unknown(reason)
    else:
        # Adopted research baseline from ALG-15: 0.5 ATR, strictly below and negative ret1.
        result["core.price_damage"] = FieldValue(
            current_prices[0][3] < prior.value - Fraction(1, 2) * atr.value and ret.value < 0
        )


_OUTPUT_FIELDS = (
    *(f"core.ma{n}" for n in (5, 10, 20, 60)),
    *(f"core.{kind}{n}" for kind in ("hhv", "llv", "prior_high", "prior_low") for n in (5, 20, 60)),
    "core.tr", "core.atr5", "core.atr20", "core.ma_slope20_atr", "core.ma_slope60_atr",
    "core.ret1", "core.ret3", "core.ret5", "core.ret20", "core.vol5", "core.vol20",
    "core.amount_ratio5", "core.amount_ratio20", "core.volume_ratio5", "core.volume_ratio20",
    "core.liquidity_mean20", "core.clv", "core.pos60", "core.bias20_atr", "core.dist_high20_atr",
    "core.range_ratio", "core.atr_ratio", "core.vol_ratio", "core.hh_progress", "core.ll_progress",
    "core.prior60_percentile", "core.price_damage",
)
