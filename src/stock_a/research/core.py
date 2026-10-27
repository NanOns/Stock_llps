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


@dataclass(frozen=True)
class MarketSessionCalendar:
    """由上游规范日历提供的官方市场会话闭合集合及其身份。"""

    calendar_id: str
    revision: str
    sessions: tuple[date, ...]

    def __post_init__(self) -> None:
        if not self.calendar_id or not self.revision:
            raise ContractError("官方市场日历必须有身份和版本")
        if type(self.sessions) is not tuple or not self.sessions or any(type(day) is not date for day in self.sessions):
            raise ContractError("官方市场日历必须包含date会话")
        if any(a >= b for a, b in zip(self.sessions, self.sessions[1:])):
            raise ContractError("官方市场日历会话必须唯一且递增")


_STATES = {"ACTUAL", "CONFIRMED_SUSPENSION", "UNKNOWN", "PRE_LISTING",
           "ADJUSTMENT_UNKNOWN", "IDENTITY_UNKNOWN"}
_PRICE_FIELDS = ("open", "high", "low", "close")


def _validate_history(history: Sequence[CoreObservation], calendar: MarketSessionCalendar) -> None:
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
    supplied = tuple(row.session for row in history)
    expected = tuple(day for day in calendar.sessions
                     if supplied[0] <= day <= supplied[-1])
    if supplied != expected:
        raise ContractError("Core输入未与官方市场会话日历闭合；不得压缩缺失会话")


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


def _coordinate_error(rows: Sequence[CoreObservation]) -> str | None:
    coordinates = {row.coordinate_id for row in rows if row.state == "ACTUAL"}
    return "MIXED_PRICE_COORDINATE" if len(coordinates) > 1 else None


def _known_ohlc_error(row: CoreObservation) -> str | None:
    known: dict[str, Fraction] = {}
    for name in _PRICE_FIELDS:
        field = getattr(row, name)
        if not isinstance(field, FieldValue) or field.quality != "OBSERVED":
            continue
        value, reason = _number(field, "DATA_INVALID_OHLC")
        if reason:
            return "DATA_INVALID_OHLC"
        if value <= 0:
            return "DATA_INVALID_OHLC"
        known[name] = value
    o, h, low, c = (known.get(k) for k in ("open", "high", "low", "close"))
    if h is not None and low is not None and low > h:
        return "DATA_INVALID_OHLC"
    if low is not None and any(value is not None and low > value for value in (o, c)):
        return "DATA_INVALID_OHLC"
    if h is not None and any(value is not None and value > h for value in (o, c)):
        return "DATA_INVALID_OHLC"
    return None


def _column(rows: Sequence[CoreObservation], name: str) -> tuple[list[Fraction] | None, str | None]:
    coordinate_error = _coordinate_error(rows)
    if coordinate_error:
        return None, coordinate_error
    values = []
    for row in rows:
        error = _known_ohlc_error(row)
        if error:
            return None, error
        value, reason = _number(getattr(row, name), f"DATA_INVALID_{name.upper()}")
        if reason:
            return None, reason
        if value <= 0:
            return None, f"DATA_INVALID_{name.upper()}"
        values.append(value)
    return values, None


def _extreme_column(rows: Sequence[CoreObservation], name: str) -> tuple[list[Fraction] | None, str | None]:
    coordinate_error = _coordinate_error(rows)
    if coordinate_error:
        return None, coordinate_error
    result = []
    for row in rows:
        error = _known_ohlc_error(row)
        if error:
            return None, error
        value, reason = _number(getattr(row, name), f"DATA_INVALID_{name.upper()}")
        if reason:
            return None, reason
        if value <= 0:
            return None, f"DATA_INVALID_{name.upper()}"
        result.append(value)
    return result, None


def _known(value, reason: str | None = None) -> FieldValue:
    return unknown(reason) if reason else FieldValue(value)


def _fixed_session_window(history: Sequence[CoreObservation], periods: int, *, require_all_actual: bool):
    if len(history) < periods + 1:
        return list(history), "SHORT_SESSION_HISTORY"
    rows = list(history[-periods - 1:])
    if rows[0].state != "ACTUAL" or rows[-1].state != "ACTUAL":
        endpoint = rows[0] if rows[0].state != "ACTUAL" else rows[-1]
        return rows, {
            "UNKNOWN": "UNKNOWN_SESSION_GAP",
            "CONFIRMED_SUSPENSION": "SUSPENDED_ENDPOINT_OR_WINDOW",
            "PRE_LISTING": "PRE_LISTING_ENDPOINT",
            "ADJUSTMENT_UNKNOWN": "PRICE_IDENTITY_UNKNOWN",
            "IDENTITY_UNKNOWN": "SECURITY_IDENTITY_UNKNOWN",
        }[endpoint.state]
    for row in rows[1:-1]:
        if row.state == "CONFIRMED_SUSPENSION" and not require_all_actual:
            continue
        if row.state != "ACTUAL":
            return rows, {
                "UNKNOWN": "UNKNOWN_SESSION_GAP",
                "CONFIRMED_SUSPENSION": "SUSPENDED_ENDPOINT_OR_WINDOW",
                "PRE_LISTING": "PRE_LISTING_SESSION_WINDOW",
                "ADJUSTMENT_UNKNOWN": "PRICE_IDENTITY_UNKNOWN",
                "IDENTITY_UNKNOWN": "SECURITY_IDENTITY_UNKNOWN",
            }[row.state]
    return rows, None


def _decimal_fraction(value: Fraction) -> Decimal:
    return Decimal(value.numerator) / Decimal(value.denominator)


def _volatility(rows: Sequence[CoreObservation], periods: int):
    session_rows, reason = _fixed_session_window(rows, periods, require_all_actual=True)
    if reason:
        return unknown(reason)
    bars, reason = _column(session_rows, "close")
    if reason:
        return unknown(reason)
    closes = bars
    if any(close <= 0 for close in closes):
        return unknown("INVALID_PRICE_DENOMINATOR")
    with localcontext() as ctx:
        ctx.prec = 34
        returns = [(_decimal_fraction(b) / _decimal_fraction(a)).ln()
                   for a, b in zip(closes, closes[1:])]
        avg = sum(returns, Decimal(0)) / Decimal(periods)
        variance = sum(((r - avg) ** 2 for r in returns), Decimal(0)) / Decimal(periods)
        return FieldValue(variance.sqrt())


def calculate_core(history: Sequence[CoreObservation], *,
                   official_calendar: MarketSessionCalendar | None = None) -> dict[str, FieldValue]:
    """计算最新actual会话的Core字段，不做IO、发布或隐式缺值回退。"""
    if not isinstance(official_calendar, MarketSessionCalendar):
        raise ContractError("Core计算必须传入版本化官方市场会话日历")
    _validate_history(history, official_calendar)
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
        values, value_reason = _column(rows, "close") if not reason else (None, reason)
        values = values or []
        result[f"core.ma{period}"] = _known(mean(values) if values else None,
                                               reason or value_reason)
        if period in (5, 20, 60):
            highs, high_reason = _extreme_column(rows, "high") if not reason else (None, reason)
            lows, low_reason = _extreme_column(rows, "low") if not reason else (None, reason)
            result[f"core.hhv{period}"] = _known(max(highs, default=None) if highs else None,
                                                    reason or high_reason)
            result[f"core.llv{period}"] = _known(min(lows, default=None) if lows else None,
                                                    reason or low_reason)
            prior_rows, prior_reason = _actual_window(history, period, exclude_current=True)
            prior_highs, high_reason = _extreme_column(prior_rows, "high") if not prior_reason else (None, prior_reason)
            prior_lows, low_reason = _extreme_column(prior_rows, "low") if not prior_reason else (None, prior_reason)
            result[f"core.prior_high{period}"] = _known(max(prior_highs, default=None) if prior_highs else None,
                                                           prior_reason or high_reason)
            result[f"core.prior_low{period}"] = _known(min(prior_lows, default=None) if prior_lows else None,
                                                          prior_reason or low_reason)

    # True range uses consecutive selected actual bars; known suspensions may be skipped.
    tr_rows, tr_reason = _actual_window(history, 2)
    if not tr_reason:
        tr_reason = _coordinate_error(tr_rows)
    if not tr_reason:
        tr_highs, high_reason = _extreme_column([tr_rows[-1]], "high")
        tr_lows, low_reason = _extreme_column([tr_rows[-1]], "low")
        tr_closes, close_reason = _column([tr_rows[-2]], "close")
    else:
        tr_highs, high_reason = None, tr_reason
        tr_lows, low_reason = None, tr_reason
        tr_closes, close_reason = None, tr_reason
    if tr_highs and tr_lows and tr_closes:
        cur_high, cur_low = tr_highs[0], tr_lows[0]
        prev_close = tr_closes[0]
        tr = max(cur_high - cur_low, abs(cur_high - prev_close), abs(cur_low - prev_close))
        result["core.tr"] = _known(tr)
    else:
        result["core.tr"] = unknown(tr_reason or high_reason or low_reason or close_reason)

    for period in (5, 20):
        rows, reason = _actual_window(history, period + 1)
        if not reason:
            # Each TR depends only on that day's high/low and the previous
            # actual session's close. The oldest bar's high/low and newest
            # bar's close are outside this formula's dependency set.
            if _coordinate_error(rows):
                reason = _coordinate_error(rows)
            else:
                reason = next((error for row in rows if (error := _known_ohlc_error(row))), None)
        if not reason:
            true_ranges = []
            for previous, current in zip(rows, rows[1:]):
                highs, high_reason = _extreme_column([current], "high")
                lows, low_reason = _extreme_column([current], "low")
                closes, close_reason = _column([previous], "close")
                dependency_error = high_reason or low_reason or close_reason
                if dependency_error:
                    reason = dependency_error
                    break
                cur_high, cur_low, prev_close = highs[0], lows[0], closes[0]
                true_ranges.append(max(cur_high - cur_low, abs(cur_high - prev_close), abs(cur_low - prev_close)))
        if not reason:
            result[f"core.atr{period}"] = _known(mean(true_ranges))
        else:
            result[f"core.atr{period}"] = unknown(reason)

    # MA slope windows compare current and lagged actual-bar moving averages.
    for period, offset in ((20, 5), (60, 10)):
        rows, reason = _actual_window(history, period + offset)
        values, value_reason = _column(rows, "close") if not reason else (None, reason)
        atr = result["core.atr20"]
        reason = reason or value_reason or (atr.reason if atr.quality != "OBSERVED" else None)
        if not reason and atr.value <= 0:
            reason = "ZERO_ATR"
        if reason:
            result[f"core.ma_slope{period}_atr"] = unknown(reason)
        else:
            earlier = values[-period-offset:-offset]
            current = values[-period:]
            result[f"core.ma_slope{period}_atr"] = FieldValue((mean(current) - mean(earlier)) / atr.value)

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

    current_close, current_reason = _column([latest], "close")
    current_highs, current_high_reason = _extreme_column([latest], "high")
    current_lows, current_low_reason = _extreme_column([latest], "low")
    current_prices = current_close
    if current_close and current_highs and current_lows:
        h, low = current_highs[0], current_lows[0]
        result["core.clv"] = unknown("ZERO_PRICE_RANGE") if h == low else FieldValue((current_close[0] - low) / (h - low))
    else:
        result["core.clv"] = unknown(current_reason or current_high_reason or current_low_reason)

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
        result["core.pos60"] = FieldValue((current_prices[0] - llv60.value) / (hhv60.value - llv60.value))
    _derived_price_ratios(result, current_prices)
    _progress(result, history)
    _prior_percentile(result, history, current_prices)
    _price_damage(result, current_prices)
    return result


def _return(history: Sequence[CoreObservation], periods: int) -> FieldValue:
    rows, reason = _fixed_session_window(history, periods, require_all_actual=False)
    if reason:
        return unknown(reason)
    for row in rows:
        error = _known_ohlc_error(row)
        if error:
            return unknown(error)
    values, price_reason = _column((rows[0], rows[-1]), "close")
    if price_reason:
        return unknown(price_reason)
    start, end = values[0], values[-1]
    return unknown("INVALID_PRICE_DENOMINATOR") if start <= 0 else FieldValue(end / start - 1)


def _derived_price_ratios(result, current_prices):
    current = current_prices[0] if current_prices else None
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
    highs, high_reason = _extreme_column(rows, "high") if not reason else (None, reason)
    lows, low_reason = _extreme_column(rows, "low") if not reason else (None, reason)
    result["core.hh_progress"] = (
        unknown(reason or high_reason) if reason or high_reason else
        FieldValue(max(highs[-5:]) > max(highs[:5]))
    )
    result["core.ll_progress"] = (
        unknown(reason or low_reason) if reason or low_reason else
        FieldValue(min(lows[-5:]) < min(lows[:5]))
    )


def _prior_percentile(result, history, current_prices):
    rows, reason = _actual_window(history, 60, exclude_current=True)
    values, value_reason = _column(rows, "close") if not reason else (None, reason)
    if reason or value_reason or not current_prices:
        result["core.prior60_percentile"] = unknown(reason or value_reason or "CURRENT_PRICE_UNKNOWN")
        return
    current = current_prices[0]
    closes = values
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
            current_prices[0] < prior.value - Fraction(1, 2) * atr.value and ret.value < 0
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
