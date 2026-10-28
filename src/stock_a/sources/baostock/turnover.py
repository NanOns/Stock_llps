"""Bao换手历史分位的纯计算；输入由调用方完成窗口及绑定核验。"""
from dataclasses import dataclass
from datetime import date, datetime
from fractions import Fraction

from stock_a.domain import ContractError, FieldValue, exact_number, unknown


@dataclass(frozen=True)
class TurnoverObservation:
    """交易日及规范fraction换手；身份与字段绑定由调用方负责。"""

    session_date: date
    turn: FieldValue


@dataclass(frozen=True)
class TurnoverPercentile:
    pct60: FieldValue
    band: FieldValue


def classify_turnover_percentile(value: object) -> str:
    """对0–100百分位按合同分档。"""
    try:
        pct = exact_number(value)
    except ValueError as error:
        raise ContractError("换手百分位必须是精确有限数") from error
    if not 0 <= pct <= 100:
        raise ContractError("换手百分位必须在0至100之间")
    return ("LOW" if pct < 20 else "NORMAL" if pct < 70 else
            "ELEVATED" if pct < 90 else "HIGH" if pct < 97 else "EXTREME")


def turnover_pct60(current: TurnoverObservation, prior: tuple[TurnoverObservation, ...],
                   *, binding_ready: bool, window_complete: bool) -> TurnoverPercentile:
    """前60个已确认有效观察按日期升序传入；完整性和绑定须显式声明。

    窗口完整性包括同一实体、无未知缺口以及恰为最近60个有效观察；
    本函数不从缺失记录推断停牌、绑定或来源准入。
    """
    if type(binding_ready) is not bool or type(window_complete) is not bool:
        raise ContractError("绑定与窗口完整性必须显式为布尔值")
    if not isinstance(current, TurnoverObservation) or not isinstance(prior, tuple):
        raise ContractError("换手观察及历史窗口类型非法")
    rows = (*prior, current)
    if any(not isinstance(row, TurnoverObservation) or not isinstance(row.session_date, date)
           or isinstance(row.session_date, datetime) or not isinstance(row.turn, FieldValue)
           for row in rows):
        raise ContractError("换手观察日期或字段类型非法")
    if any(left.session_date >= right.session_date for left, right in zip(rows, rows[1:])):
        raise ContractError("换手历史必须按日期严格递增且排除当前日")

    def unavailable(reason: str) -> TurnoverPercentile:
        return TurnoverPercentile(unknown(reason), unknown(reason))

    if not binding_ready:
        return unavailable("换手字段绑定不足")
    if not window_complete or len(prior) != 60:
        return unavailable("换手历史窗口不完整")
    values = []
    for row in rows:
        if row.turn.quality != "OBSERVED":
            return unavailable("换手观察质量未知或不适用")
        try:
            value = exact_number(row.turn.value)
        except ValueError:
            return unavailable("换手观察数值非法")
        if value < 0:
            return unavailable("换手观察数值非法")
        values.append(value)
    target = values[-1]
    less = sum(value < target for value in values[:-1])
    equal = sum(value == target for value in values[:-1])
    pct = Fraction(100 * (2 * less + equal), 120)
    band = classify_turnover_percentile(pct)
    return TurnoverPercentile(FieldValue(pct), FieldValue(band))
