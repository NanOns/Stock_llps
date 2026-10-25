"""字段值及合同错误；质量和能力就绪状态分开。"""
from dataclasses import dataclass
from decimal import Decimal
from fractions import Fraction


class ContractError(ValueError):
    """调用者未绑定必需合同，不能降级成缺数据。"""


@dataclass(frozen=True)
class FieldValue:
    value: object
    quality: str = "OBSERVED"
    reason: str | None = None

    def __post_init__(self):
        if self.quality not in {"OBSERVED", "UNKNOWN_DATA", "NOT_APPLICABLE"}:
            raise ContractError("不支持的字段质量")
        if self.quality == "OBSERVED":
            if self.value is None or self.reason is not None:
                raise ContractError("已知字段必须有值且不能带缺失原因")
        elif self.value is not None or not isinstance(self.reason, str) or not self.reason:
            raise ContractError("未知/不适用字段必须为空且有明确原因")


def unknown(reason: str) -> FieldValue:
    return FieldValue(None, "UNKNOWN_DATA", reason)


def exact_number(value: object) -> Fraction:
    """规范边界只接受有限的十进制/有理数/整数，拒bool和二进制float。"""
    if isinstance(value, bool) or not isinstance(value, (int, Decimal, Fraction)):
        raise ValueError("数值必须为整数、Decimal或Fraction，不能为bool/float")
    if isinstance(value, Decimal) and not value.is_finite():
        raise ValueError("数值必须有限")
    return Fraction(value)
