"""研究内核的显式事实绑定；不授予来源、PIT或发布权限。"""

from dataclasses import dataclass
from datetime import date
from itertools import product

from stock_a.domain import ContractError, FieldValue, exact_number


class ExecutionError(RuntimeError):
    """已识别工程失败，不能包装成UNKNOWN_DATA。"""


def capability(state: str):
    if state == "EXECUTION_ERROR":
        raise ExecutionError("上游工程失败")
    if state != "READY":
        raise ContractError("所需研究能力未就绪：" + str(state))


def boolean(field: FieldValue):
    if not isinstance(field, FieldValue):
        raise ContractError("输入必须显式携带字段质量")
    if field.quality == "OBSERVED":
        if type(field.value) is not bool:
            raise ValueError("已知谓词必须为bool")
        return field.value
    if field.quality == "NOT_APPLICABLE":
        raise ValueError("此谓词不允许隐式NA")
    return None


def number(field: FieldValue, *, positive=False):
    if not isinstance(field, FieldValue):
        raise ContractError("输入必须显式携带字段质量")
    if field.quality != "OBSERVED":
        return None
    value = exact_number(field.value)
    if positive and value <= 0:
        raise ValueError("价格/尺度必须大于0")
    return value


def enum(field: FieldValue, domain):
    if not isinstance(field, FieldValue):
        raise ContractError("输入必须显式携带字段质量")
    if field.quality != "OBSERVED":
        return None
    if not isinstance(field.value, str) or field.value not in domain:
        raise ValueError("已知枚举超出合同值域")
    return field.value


@dataclass(frozen=True)
class Fact:
    field: FieldValue
    entity_id: str
    session: date
    coordinate_id: str
    source_ref: str
    episode_id: str | None = None

    def validate(self, entity_id, session, coordinate_id):
        if (
            not isinstance(self.field, FieldValue)
            or type(self.session) is not date
            or not all(
                isinstance(x, str) and x
                for x in (self.entity_id, self.coordinate_id, self.source_ref)
            )
            or (self.entity_id, self.session, self.coordinate_id)
            != (entity_id, session, coordinate_id)
        ):
            raise ValueError("事实实体/日期/坐标/来源绑定不一致")


def completions(values, domains):
    return product(
        *(
            domain if value is None else (value,)
            for value, domain in zip(values, domains)
        )
    )
