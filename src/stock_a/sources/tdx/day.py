"""复用TDX 32字节布局及价格缩放，按价格/金额/数量独立校验。"""
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from fractions import Fraction
import math
import struct

from stock_a.domain import ContractError, FieldValue, exact_number, unknown

DAY_STRUCT = struct.Struct("<IIIIIfII")


@dataclass(frozen=True)
class UnitContract:
    version: str
    amount_to_cny: Decimal | None
    volume_to_shares: int | None

    def __post_init__(self):
        if self.amount_to_cny is None:
            amount_scale = None
        else:
            try:
                amount_scale = exact_number(self.amount_to_cny)
            except ValueError as error:
                raise ContractError("金额单位倍率必须为正有限精确数或未知") from error
        if not isinstance(self.version, str) or not self.version or (amount_scale is not None and amount_scale <= 0):
            raise ContractError("单位合同必须有版本，已提供倍率必须为正")
        if self.volume_to_shares is not None and (
                type(self.volume_to_shares) is not int or self.volume_to_shares <= 0):
            raise ContractError("数量单位倍率必须为正整数或未知")


@dataclass(frozen=True)
class DecodedDay:
    session_date: date
    prices: FieldValue  # 合法OHLC四元组，或整组隔离；不推断交易状态
    raw_open: FieldValue
    raw_high: FieldValue
    raw_low: FieldValue
    raw_close: FieldValue
    amount: FieldValue
    volume: FieldValue
    reserved: int
    unit_version: str
    raw_amount: FieldValue | None = None
    raw_volume: FieldValue | None = None


def project_ohlc_fields(prices: FieldValue) -> tuple[FieldValue, FieldValue, FieldValue, FieldValue]:
    """将OHLC组质量无损投影到raw.open/high/low/close四个字段。"""
    if not isinstance(prices, FieldValue):
        raise ContractError("OHLC组必须显式携带FieldValue质量")
    if prices.quality != "OBSERVED":
        return tuple(
            FieldValue(None, prices.quality, prices.reason)
            for _ in range(4)
        )
    if not isinstance(prices.value, tuple) or len(prices.value) != 4:
        raise ContractError("已知OHLC组必须包含按开高低收排列的四个值")
    return tuple(FieldValue(value, prices.quality, prices.reason) for value in prices.value)


def normalize_components(prices, amount, volume, units: UnitContract):
    """某个字段不合法只隔离该组；没有用零填未知的路径。"""
    if not isinstance(units, UnitContract):
        raise ContractError("缺少来源单位合同")
    try:
        o, h, l, c = tuple(exact_number(x) for x in prices)
        if o == h == l == c == 0:
            price_value = unknown("ZERO_PRICE_PLACEHOLDER")
        elif min(o, h, l, c) <= 0 or not l <= min(o, c) <= max(o, c) <= h:
            raise ValueError("OHLC非法")
        else:
            price_value = FieldValue((o, h, l, c))
    except (ValueError, TypeError):
        price_value = unknown("DATA_INVALID_OHLC")
    try:
        raw_amount = exact_number(amount)
        if raw_amount < 0:
            raise ValueError("金额为负")
        if units.amount_to_cny is None:
            amount_value = unknown("UNIT_AUTHORITY_NOT_VERIFIED")
        else:
            amount_value = FieldValue(raw_amount * exact_number(units.amount_to_cny))
    except ValueError:
        amount_value = unknown("DATA_INVALID_AMOUNT")
    try:
        if type(volume) is not int or volume < 0:
            raise ValueError("数量必须为非负整数")
        if units.volume_to_shares is None:
            volume_value = unknown("UNIT_AUTHORITY_NOT_VERIFIED")
            shares = None
        else:
            shares = volume * units.volume_to_shares
        if shares is not None and shares > 2**63 - 1:
            raise ValueError("数量超出bigint")
        if shares is not None:
            volume_value = FieldValue(shares)
    except ValueError:
        volume_value = unknown("DATA_INVALID_VOLUME")
    return price_value, amount_value, volume_value


def decode_day_record(raw32: bytes, units: UnitContract) -> DecodedDay:
    if not isinstance(units, UnitContract):
        raise ContractError("缺少来源单位合同")
    if not isinstance(raw32, bytes) or len(raw32) != DAY_STRUCT.size:
        raise ValueError("日线记录必须为32字节")
    day, o, h, l, c, amount, volume, reserved = DAY_STRUCT.unpack(raw32)
    session = date(day // 10000, day // 100 % 100, day % 100)
    prices = tuple(Fraction(x, 100) for x in (o, h, l, c))
    # float32解包后按实际二进制值精确转换；不把NaN/Infinity写成合法金额。
    amount_decimal = Decimal.from_float(amount) if math.isfinite(amount) else Decimal("NaN")
    raw_amount = FieldValue(amount_decimal) if amount_decimal.is_finite() else unknown("DATA_INVALID_AMOUNT")
    raw_volume = FieldValue(volume)
    fields = normalize_components(prices, amount_decimal, volume, units)
    price_value, amount_value, volume_value = fields
    raw_fields = project_ohlc_fields(price_value)
    return DecodedDay(
        session, price_value, *raw_fields, amount_value, volume_value, reserved,
        units.version, raw_amount, raw_volume,
    )
