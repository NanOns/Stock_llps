"""把显式提供的BaoStock日线响应映射为带字段质量的规范观察。"""
from __future__ import annotations

from collections.abc import Collection, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from fractions import Fraction

from stock_a.domain import ContractError, FieldValue, exact_number, unknown


_SEMANTIC_FIELDS = frozenset(
    {"session_date", "source_code", "open", "high", "low", "close", "volume", "amount", "turn", "tradestatus", "isST"}
)
_INVALID_FIELD_REASONS = {
    "open": "开盘价数据非法",
    "high": "最高价数据非法",
    "low": "最低价数据非法",
    "close": "收盘价数据非法",
    "volume": "成交量数据非法",
    "amount": "成交金额数据非法",
    "turn": "换手率数据非法",
}


@dataclass(frozen=True)
class BaoUnitContract:
    """调用方显式提供并版本化的单位约定；输出金额、价格和换手率使用人民币和fraction。"""

    version: str
    close_input_unit: str
    close_to_cny: object
    amount_to_cny: object
    volume_to_shares: object
    turn_input_unit: str

    def __post_init__(self) -> None:
        try:
            close_scale = exact_number(self.close_to_cny)
            amount_scale = exact_number(self.amount_to_cny)
            volume_scale = exact_number(self.volume_to_shares)
        except ValueError as error:
            raise ContractError("BaoStock价格、金额和数量倍率必须是精确有限数") from error
        if close_scale <= 0 or amount_scale <= 0 or volume_scale <= 0:
            raise ContractError("BaoStock价格、金额和数量倍率必须为正")
        if not isinstance(self.version, str) or not self.version.strip() or self.version != self.version.strip():
            raise ContractError("BaoStock单位合同必须有无首尾空白的版本")
        if (not isinstance(self.close_input_unit, str) or not self.close_input_unit.strip()
                or self.close_input_unit != self.close_input_unit.strip()):
            raise ContractError("BaoStock收盘价输入单位必须显式提供且不能含首尾空白")
        if self.turn_input_unit != "PERCENT":
            raise ContractError("BaoStock换手率输入单位必须明确为PERCENT")


@dataclass(frozen=True)
class BaoDailyObservation:
    """单条BaoStock日线规范投影，不代表实体身份绑定或准入结论。"""

    session_date: date
    source_code: str
    unit_contract_version: str
    open: FieldValue
    high: FieldValue
    low: FieldValue
    close: FieldValue
    volume: FieldValue
    amount: FieldValue
    turn: FieldValue
    tradestatus: FieldValue
    is_st: FieldValue
    returned_fields: tuple[str, ...]


def _numeric(value: object) -> Fraction:
    if isinstance(value, bool) or isinstance(value, float):
        raise ValueError("数值不得为布尔或二进制浮点")
    if isinstance(value, str):
        if not value.strip():
            raise ValueError("空数值")
        try:
            value = Decimal(value)
        except InvalidOperation as error:
            raise ValueError("数值格式非法") from error
    return exact_number(value)


def _required_key(row: Mapping[str, object], fields: Collection[str], column: str, label: str) -> object:
    if column not in fields:
        raise ContractError(f"BaoStock必需字段未返回：{label}")
    if column not in row:
        raise ContractError(f"BaoStock响应缺少已声明字段：{label}")
    value = row[column]
    if label == "交易日" and isinstance(value, date) and not isinstance(value, datetime):
        return value
    if value is None or not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ContractError(f"BaoStock必需字段值非法：{label}")
    return value


def _field_value(
    row: Mapping[str, object], fields: frozenset[str], column: str, field_name: str, scale: Fraction,
    *, minimum: Fraction | None = None, integer: bool = False,
) -> FieldValue:
    if column not in fields:
        return unknown("来源未返回该字段")
    if column not in row:
        return unknown("响应缺少已声明字段")
    value = row[column]
    if value is None or value == "":
        return unknown("来源值为空")
    try:
        normalized = _numeric(value) * scale
    except ValueError:
        return unknown(_INVALID_FIELD_REASONS[field_name])
    if minimum is not None and normalized < minimum:
        return unknown(_INVALID_FIELD_REASONS[field_name])
    if integer and normalized.denominator != 1:
        return unknown(_INVALID_FIELD_REASONS[field_name])
    return FieldValue(normalized)


def _passthrough_field(
    row: Mapping[str, object], fields: frozenset[str], column: str,
) -> FieldValue:
    if column not in fields:
        return unknown("来源未返回该字段")
    if column not in row:
        return unknown("响应缺少已声明字段")
    value = row[column]
    if value is None or value == "":
        return unknown("来源值为空")
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return unknown("状态字段原值类型非法")
    if isinstance(value, str) and not value.strip():
        return unknown("来源值为空")
    return FieldValue(value)


def normalize_daily_row(
    row: Mapping[str, object],
    returned_fields: Collection[str],
    field_map: Mapping[str, str],
    units: BaoUnitContract,
) -> BaoDailyObservation:
    """规范化单条日线；非法行情值局部隔离，非法日期/来源键拒绝整行。"""
    if not isinstance(row, Mapping):
        raise ContractError("BaoStock日线行必须是字段映射")
    if not isinstance(field_map, Mapping) or set(field_map) != _SEMANTIC_FIELDS:
        raise ContractError("BaoStock字段映射必须完整包含规范日期、证券、OHLCV、金额、换手及状态字段")
    if any(not isinstance(column, str) or not column.strip() or column != column.strip()
           for column in field_map.values()):
        raise ContractError("BaoStock字段映射中的来源列名必须是非空字符串")
    if len(set(field_map.values())) != len(field_map):
        raise ContractError("BaoStock字段映射不能将多个规范字段指向同一来源列")
    if not isinstance(returned_fields, Collection) or isinstance(returned_fields, (str, bytes)):
        raise ContractError("BaoStock实际字段清单必须是字符串集合")
    if any(not isinstance(name, str) or not name.strip() for name in returned_fields):
        raise ContractError("BaoStock实际字段清单包含非法列名")
    if len(set(returned_fields)) != len(returned_fields):
        raise ContractError("BaoStock实际字段清单不能重复声明来源列")
    if any(name != name.strip() for name in returned_fields):
        raise ContractError("BaoStock实际字段清单不能包含首尾空白")
    if not isinstance(units, BaoUnitContract):
        raise ContractError("缺少BaoStock单位合同")

    columns = frozenset(returned_fields)
    raw_date = _required_key(row, columns, field_map["session_date"], "交易日")
    if isinstance(raw_date, date):
        session_date = raw_date
    else:
        try:
            session_date = date.fromisoformat(raw_date)
        except (TypeError, ValueError) as error:
            raise ContractError("BaoStock交易日必须是有效ISO日期") from error
    source_code = _required_key(row, columns, field_map["source_code"], "来源证券代码")

    try:
        close_scale = exact_number(units.close_to_cny)
        amount_scale = exact_number(units.amount_to_cny)
        volume_scale = exact_number(units.volume_to_shares)
    except ValueError as error:
        raise ContractError("BaoStock价格、金额和数量倍率必须是精确有限数") from error

    prices: dict[str, FieldValue] = {}
    for price_name in ("open", "high", "low", "close"):
        value = _field_value(row, columns, field_map[price_name], price_name, close_scale, minimum=Fraction(0))
        if value.quality == "OBSERVED" and value.value == 0:
            value = unknown(_INVALID_FIELD_REASONS[price_name])
        prices[price_name] = value
    volume = _field_value(row, columns, field_map["volume"], "volume", volume_scale,
                          minimum=Fraction(0), integer=True)
    amount = _field_value(row, columns, field_map["amount"], "amount", amount_scale,
                          minimum=Fraction(0))
    turn = _field_value(row, columns, field_map["turn"], "turn", Fraction(1, 100),
                        minimum=Fraction(0))
    return BaoDailyObservation(
        session_date=session_date,
        source_code=source_code,
        unit_contract_version=units.version,
        open=prices["open"],
        high=prices["high"],
        low=prices["low"],
        close=prices["close"],
        volume=volume,
        amount=amount,
        turn=turn,
        tradestatus=_passthrough_field(row, columns, field_map["tradestatus"]),
        is_st=_passthrough_field(row, columns, field_map["isST"]),
        returned_fields=tuple(sorted(columns)),
    )
