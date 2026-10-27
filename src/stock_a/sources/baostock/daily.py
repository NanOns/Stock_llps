"""把显式提供的BaoStock日线响应映射为带字段质量的规范观察。"""
from __future__ import annotations

from collections.abc import Collection, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from fractions import Fraction

from stock_a.domain import ContractError, FieldValue, exact_number, unknown


_SEMANTIC_FIELDS = frozenset(
    {"session_date", "source_code", "close", "volume", "amount", "turn", "tradestatus", "isST"}
)


@dataclass(frozen=True)
class BaoUnitContract:
    """调用方显式提供的单位约定；turn输入固定为百分数。"""

    version: str
    amount_to_cny: object
    volume_to_shares: object
    turn_input_unit: str

    def __post_init__(self) -> None:
        try:
            amount_scale = exact_number(self.amount_to_cny)
            volume_scale = exact_number(self.volume_to_shares)
        except ValueError as error:
            raise ContractError("BaoStock金额和数量倍率必须是精确有限数") from error
        if amount_scale <= 0 or volume_scale <= 0:
            raise ContractError("BaoStock金额和数量倍率必须为正")
        if not isinstance(self.version, str) or not self.version.strip() or self.version != self.version.strip():
            raise ContractError("BaoStock单位合同必须有无首尾空白的版本")
        if self.turn_input_unit != "PERCENT":
            raise ContractError("BaoStock换手率输入单位必须明确为PERCENT")


@dataclass(frozen=True)
class BaoDailyObservation:
    """单条BaoStock日线规范投影，不代表实体身份绑定或准入结论。"""

    session_date: date
    source_code: str
    unit_contract_version: str
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
        return unknown("BAO_FIELD_NOT_RETURNED")
    if column not in row:
        return unknown("BAO_RESPONSE_FIELD_MISSING")
    value = row[column]
    if value is None or value == "":
        return unknown("BAO_VALUE_MISSING")
    try:
        normalized = _numeric(value) * scale
    except ValueError:
        return unknown(f"BAO_DATA_INVALID_{field_name.upper()}")
    if minimum is not None and normalized < minimum:
        return unknown(f"BAO_DATA_INVALID_{field_name.upper()}")
    if integer and normalized.denominator != 1:
        return unknown(f"BAO_DATA_INVALID_{field_name.upper()}")
    return FieldValue(normalized)


def _passthrough_field(
    row: Mapping[str, object], fields: frozenset[str], column: str,
) -> FieldValue:
    if column not in fields:
        return unknown("BAO_FIELD_NOT_RETURNED")
    if column not in row:
        return unknown("BAO_RESPONSE_FIELD_MISSING")
    value = row[column]
    if value is None or value == "":
        return unknown("BAO_VALUE_MISSING")
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return unknown("BAO_DATA_INVALID_ENUM_SOURCE_VALUE")
    if isinstance(value, str) and not value.strip():
        return unknown("BAO_VALUE_MISSING")
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
        raise ContractError("BaoStock字段映射必须完整包含八个规范字段")
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
        amount_scale = exact_number(units.amount_to_cny)
        volume_scale = exact_number(units.volume_to_shares)
    except ValueError as error:
        raise ContractError("BaoStock单位倍率必须是精确有限数") from error

    close = _field_value(row, columns, field_map["close"], "close", Fraction(1), minimum=Fraction(0))
    if close.quality == "OBSERVED" and close.value == 0:
        close = unknown("BAO_DATA_INVALID_CLOSE")
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
        close=close,
        volume=volume,
        amount=amount,
        turn=turn,
        tradestatus=_passthrough_field(row, columns, field_map["tradestatus"]),
        is_st=_passthrough_field(row, columns, field_map["isST"]),
        returned_fields=tuple(sorted(columns)),
    )
