"""供PG事实写入复用的有限精确数值转换。"""

from __future__ import annotations

from decimal import Decimal, localcontext
from fractions import Fraction
from typing import Any


class TypedBridgeError(ValueError):
    """PG typed数值映射拒绝原因。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def _finite_decimal(value: Any) -> Decimal:
    """将精确有理数转换为有限PG numeric可接收的Decimal。"""
    if isinstance(value, bool):
        raise TypedBridgeError("FIELD_VALUE_INVALID", "布尔值不能作为数值字段")
    if isinstance(value, Decimal):
        result = value
    elif type(value) is int:
        result = Decimal(value)
    elif isinstance(value, Fraction):
        denominator = value.denominator
        for factor in (2, 5):
            while denominator % factor == 0:
                denominator //= factor
        if denominator != 1:
            raise TypedBridgeError(
                "FIELD_VALUE_NOT_DECIMAL", "精确数值不能无损表示为PostgreSQL numeric"
            )
        with localcontext() as context:
            context.prec = max(
                50, len(str(abs(value.numerator))) + len(str(value.denominator)) + 4
            )
            result = Decimal(value.numerator) / Decimal(value.denominator)
    else:
        raise TypedBridgeError(
            "FIELD_VALUE_INVALID", "数值字段必须是Decimal、Fraction或整数"
        )
    if not result.is_finite():
        raise TypedBridgeError(
            "FIELD_VALUE_INVALID", "PostgreSQL numeric字段必须是有限数"
        )
    return result
