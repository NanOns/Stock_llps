"""唯一基础画像有序表达求值器，规则由调用者显式注入。"""
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from fractions import Fraction

from stock_a.domain import ContractError, FieldValue, exact_number, unknown


@dataclass(frozen=True)
class ClosedPrices:
    """来源层已选择的连续官方闭合周期；不能从价格条数推断日历连续。"""
    closes: tuple[FieldValue, ...]
    calendar_ordinals: tuple[int, ...]
    last_sessions: tuple[date, ...]
    complete: tuple[bool, ...]
    target_session: date
    coordinate_id: str


def _validate_rule(rule):
    """数据门之前拒绝坏合同；只允许固定操作集，不执行代码。"""
    inputs = rule.get("input_fields")
    domains = rule.get("input_domains")
    if (not isinstance(inputs, list) or len(inputs) != len(set(inputs))
            or not isinstance(domains, Mapping) or set(inputs) != set(domains)):
        raise ContractError("规则输入和类型域不一致")
    if rule.get("data_gate") not in {"ALL_INPUTS_KNOWN", "BRANCH_LOCAL_KLEENE"}:
        raise ContractError("缺少明确数据门")
    for domain in domains.values():
        kind = domain.get("type")
        if kind not in {"number", "boolean", "enum", "closed_prices"}:
            raise ContractError("合同含未注册输入类型")
        if kind == "enum" and (not domain.get("values") or
                               any(not isinstance(x, str) for x in domain["values"])):
            raise ContractError("合同枚举域非法")
        if kind == "closed_prices" and (type(domain.get("minimum_count")) is not int or domain["minimum_count"] < 2):
            raise ContractError("合同闭合周期窗口非法")
        if kind == "number":
            for bound in ("min", "exclusive_min", "max"):
                if bound in domain:
                    try:
                        exact_number(domain[bound])
                    except ValueError as error:
                        raise ContractError("合同数值域非法") from error
    names = set(inputs) | {"parameter." + name for name in rule["parameters"]}

    def check(node):
        if isinstance(node, Mapping):
            if set(node) == {"ref"}:
                if node["ref"] not in names:
                    raise ContractError("合同存在未定义引用：" + node["ref"])
                return
            if set(node) != {"op", "args"} or not isinstance(node["args"], list):
                raise ContractError("合同表达结构非法")
            op, args = node["op"], node["args"]
            arity = {"not": 1, "subtract": 2, "divide": 2, "multiply": 2,
                     "lt": 2, "lte": 2, "gt": 2, "gte": 2, "eq": 2}
            if op not in arity and op not in {"and", "or", "in"}:
                raise ContractError("未支持的合同操作")
            if ((op in arity and len(args) != arity[op]) or
                    (op in {"and", "or"} and not args) or (op == "in" and len(args) < 2)):
                raise ContractError("合同操作元数非法")
            for arg in args:
                check(arg)
        elif not isinstance(node, (bool, str)):
            try:
                exact_number(node)
            except ValueError as error:
                raise ContractError("合同数值必须精确有限") from error

    for name, operand in rule["operands"].items():
        if name in names:
            raise ContractError("操作数与输入/参数重名")
        if "expression" in operand:
            check(operand["expression"])
        elif (operand.get("field_id") not in inputs or operand.get("component") != "close"
              or domains[operand["field_id"]].get("type") != "closed_prices"
              or operand.get("endpoint_offset") not in (0, -1)
              or type(operand.get("count", 1)) is not int or operand.get("count", 1) <= 0):
            raise ContractError("闭合周期操作数非法")
        names.add(name)
    branches = rule["ordered_branches"]
    if not branches or branches[-1]["when"] is not True:
        raise ContractError("规则必须以显式穷尽分支结束")
    for branch in branches:
        check(branch["when"])
        if branch["value"] not in rule["output_values"]:
            raise ContractError("规则含未注册输出")


def _truth(value):
    if value is None or type(value) is bool:
        return value
    raise ContractError("谓词必须为三态布尔值")


def _evaluate(node, values):
    if isinstance(node, bool):
        return node
    if isinstance(node, str):
        return node
    if not isinstance(node, Mapping):
        return exact_number(node)
    if set(node) == {"ref"}:
        if node["ref"] not in values:
            raise ContractError("表达引用未绑定：" + node["ref"])
        return values[node["ref"]]
    if set(node) != {"op", "args"}:
        raise ContractError("表达节点结构非法")
    op, args = node["op"], node["args"]
    if not isinstance(args, list):
        raise ContractError("表达参数必须为列表")
    arity = {"not": 1, "subtract": 2, "divide": 2, "multiply": 2,
             "lt": 2, "lte": 2, "gt": 2, "gte": 2, "eq": 2}
    if op not in arity and op not in {"and", "or", "in"}:
        raise ContractError("未支持的表达操作：" + str(op))
    if (op in arity and len(args) != arity[op]) or (op in {"and", "or", "in"} and not args):
        raise ContractError("表达参数数量非法")
    evaluated = [_evaluate(arg, values) for arg in args]
    if op in {"and", "or"}:
        terms = [_truth(x) for x in evaluated]
        if op == "and":
            return False if False in terms else None if None in terms else True
        return True if True in terms else None if None in terms else False
    if op == "not":
        term = _truth(evaluated[0])
        return None if term is None else not term
    if any(item is None for item in evaluated):
        return None
    if op == "in":
        return evaluated[0] in evaluated[1:]
    left, right = evaluated
    if op == "eq":
        return left == right
    if any(type(x) is bool or not isinstance(x, Fraction) for x in evaluated):
        raise ContractError("比较/算术操作必须使用精确数值")
    if op == "subtract":
        return left - right
    if op == "multiply":
        return left * right
    if op == "divide":
        return left / right if right > 0 else None
    return {"lt": lambda: left < right, "lte": lambda: left <= right,
            "gt": lambda: left > right, "gte": lambda: left >= right}[op]()


def profile(field_id: str, inputs: Mapping[str, FieldValue], rules: Mapping) -> FieldValue:
    """求一个注册字段；未绑定实现/合同报错，真实未知保留为字段质量。"""
    matches = [rule for rule in rules.values() if rule["field_id"] == field_id]
    if len(matches) != 1:
        raise ContractError("画像字段必须有唯一规则：" + field_id)
    rule = matches[0]
    _validate_rule(rule)
    values, reasons = {}, []
    for name, parameter in rule["parameters"].items():
        if parameter.get("adoption_state") != "ADOPTED_RESEARCH_BASELINE" or parameter.get("value") is None:
            raise ContractError("参数尚未采用：" + name)
        try:
            value = exact_number(parameter["value"])
        except ValueError as error:
            raise ContractError("参数非法：" + name) from error
        if parameter.get("domain") == "FINITE_POSITIVE_NUMBER_REJECT_BOOL" and value <= 0:
            raise ContractError("参数必须为正有限数：" + name)
        values["parameter." + name] = value
    for key, domain in rule["input_domains"].items():
        if key not in inputs or not isinstance(inputs[key], FieldValue):
            raise ContractError("调用缺少带质量的输入：" + key)
        field = inputs[key]
        if field.quality != "OBSERVED":
            values[key] = None
            reasons.append(key + ":" + field.quality + ":" + field.reason)
            continue
        try:
            value = field.value
            if domain["type"] == "number":
                value = exact_number(value)
                for bound, test in (("min", lambda x, b: x >= b), ("exclusive_min", lambda x, b: x > b),
                                    ("max", lambda x, b: x <= b)):
                    if bound in domain and not test(value, exact_number(domain[bound])):
                        raise ValueError("数值超出字段值域")
            elif domain["type"] == "boolean":
                if type(value) is not bool:
                    raise ValueError("布尔字段类型非法")
            elif domain["type"] == "enum":
                if not isinstance(value, str) or value not in domain["values"]:
                    raise ValueError("枚举非法")
            elif domain["type"] == "closed_prices":
                count = domain["minimum_count"]
                if not isinstance(value, ClosedPrices) or not value.coordinate_id or type(value.target_session) is not date:
                    raise ValueError("缺少闭合周期或坐标")
                size = len(value.closes)
                if size < count or any(len(x) != size for x in
                                       (value.calendar_ordinals, value.last_sessions, value.complete)):
                    raise ValueError("闭合周期数量不齐")
                ordinals, sessions = value.calendar_ordinals[-count:], value.last_sessions[-count:]
                if (any(type(x) is not int for x in ordinals)
                        or tuple(ordinals) != tuple(range(ordinals[0], ordinals[0] + count))
                        or any(x is not True for x in value.complete[-count:])
                        or any(type(x) is not date or x > value.target_session for x in sessions)
                        or list(sessions) != sorted(set(sessions))):
                    raise ValueError("周期未闭合/不连续/包含未来")
                selected = value.closes[-count:]
                if any(not isinstance(x, FieldValue) or x.quality != "OBSERVED" for x in selected):
                    raise ValueError("周期收盘未知")
                value = tuple(exact_number(x.value) for x in selected)
                if any(x <= 0 for x in value):
                    raise ValueError("周期价格非法")
            else:
                raise ContractError("未注册输入类型")
            values[key] = value
        except ValueError:
            values[key] = None
            reasons.append(key + ":DATA_INVALID")
    if rule.get("data_gate") == "ALL_INPUTS_KNOWN" and reasons:
        return unknown(";".join(reasons))
    for name, operand in rule["operands"].items():
        if "expression" in operand:
            value = _evaluate(operand["expression"], values)
        else:
            series = values[operand["field_id"]]
            end = len(series) + operand["endpoint_offset"] if series is not None else 0
            count = operand.get("count", 1)
            value = sum(series[end-count:end], Fraction(0)) / count if series is not None else None
        if value is not None and "valid_range" in operand:
            low, high = map(exact_number, operand["valid_range"])
            if value < low or value > high or (operand.get("lower_exclusive") and value == low):
                value = None
                reasons.append(name + ":DATA_INVALID_RANGE")
        values[name] = value
    for branch in rule["ordered_branches"]:
        result = _truth(_evaluate(branch["when"], values))
        if result is None:
            return unknown(";".join(reasons) or "REQUIRED_INPUT_OR_DENOMINATOR_UNAVAILABLE")
        if result:
            if branch["value"] not in rule["output_values"]:
                raise ContractError("规则输出未注册")
            return FieldValue(branch["value"])
    raise ContractError("规则缺少穷尽分支")
