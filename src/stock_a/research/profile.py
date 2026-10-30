"""唯一基础画像有序表达求值器，规则由调用者显式注入。"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from fractions import Fraction
from hashlib import sha256

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


@dataclass(frozen=True)
class WindowIdentity:
    """显式窗口身份；不给算法使用latest或隐式端点的机会。"""

    contract: str
    first_endpoint: date | None
    last_endpoint: date | None
    selected_rows_digest: str
    calendar_revision: str
    status_revision: str


@dataclass(frozen=True)
class ProfileInput:
    """Core/上游字段值及其可核验的生产身份与上下文。"""

    field_id: str
    field: FieldValue
    unit: str
    producer_stage: str
    producer_symbol: str
    producer_source_sha256: str
    expression_ref: str
    algorithm_version: str | None
    parameter_version: str
    window: WindowIdentity | None
    coordinate_role: str
    coordinate_id: str | None
    context: Mapping[str, object]


@dataclass(frozen=True)
class RuntimeProducerBinding:
    """调用边界提供的当前producer身份；不来自合同中的历史源码摘要。"""

    symbol: str
    source_sha256: str
    algorithm_version: str | None


@dataclass(frozen=True)
class ProfileHandoff:
    """画像纯内核的输入集合及调用方声明的比较期望。

    此对象本身不证明来源可信。生产准入必须来自独立验证过的FOUNDATION选择器；
    该选择器尚未实现，故 profile() 生产入口当前一律阻断。
    """

    inputs: Mapping[str, ProfileInput]
    target_session: date
    source_cutoff: str
    target_coordinate_id: str | None
    expected_windows: Mapping[str, WindowIdentity]
    expected_context: Mapping[str, Mapping[str, object]]
    runtime_producers: Mapping[str, RuntimeProducerBinding] = field(
        default_factory=dict
    )


def _rule_for(field_id: str, rules: Mapping) -> Mapping:
    matches = [rule for rule in rules.values() if rule.get("field_id") == field_id]
    if len(matches) != 1:
        raise ContractError("画像字段必须有唯一规则：" + field_id)
    return matches[0]


def validate_profile_handoff(
    field_id: str,
    handoff: ProfileHandoff,
    rules: Mapping,
    field_registry: list[Mapping],
) -> dict[str, FieldValue]:
    """核验纯内核交接与机器合同的一致性，不认证来源、选择结果或期望身份可信性。"""
    if (
        not isinstance(handoff, ProfileHandoff)
        or type(handoff.target_session) is not date
    ):
        raise ContractError("Core→Profile交接缺少明确目标会话")
    if (
        not isinstance(handoff.source_cutoff, str)
        or not handoff.source_cutoff
        or handoff.source_cutoff.casefold() == "latest"
    ):
        raise ContractError("Core→Profile交接缺少真实source cutoff身份")
    rule = _rule_for(field_id, rules)
    required = rule.get("input_fields")
    if not isinstance(required, list) or set(required) != set(handoff.inputs):
        raise ContractError("Core→Profile输入字段集合与规则不完全一致")
    if not isinstance(field_registry, list):
        raise ContractError("Core→Profile field_registry必须是列表")
    registry_ids = [
        item.get("field_id") for item in field_registry if isinstance(item, Mapping)
    ]
    if (
        len(registry_ids) != len(field_registry)
        or any(not isinstance(name, str) or not name for name in registry_ids)
        or len(registry_ids) != len(set(registry_ids))
    ):
        raise ContractError("Core→Profile field_registry含重复或非法field_id")
    entries = [item for item in field_registry if item.get("field_id") in required]
    by_id = {item["field_id"]: item for item in entries}
    if len(by_id) != len(required):
        raise ContractError("Core→Profile输入字段缺少唯一field_registry绑定")
    window_fields = {
        name
        for name in required
        if by_id[name].get("window_contract", "NONE") != "NONE"
    }
    if (
        not isinstance(handoff.expected_windows, Mapping)
        or set(handoff.expected_windows) != window_fields
        or not isinstance(handoff.expected_context, Mapping)
        or set(handoff.expected_context) != set(required)
    ):
        raise ContractError("Core→Profile选择器期望身份集合缺失或含多余字段")

    values: dict[str, FieldValue] = {}
    target_coordinates: set[str] = set()
    context_values: dict[str, object] = {}
    expected_producer_symbols = {
        by_id[name].get("producer_symbol") for name in required
    }
    if (
        any(
            not isinstance(symbol, str) or not symbol
            for symbol in expected_producer_symbols
        )
        or not isinstance(handoff.runtime_producers, Mapping)
        or set(handoff.runtime_producers) != expected_producer_symbols
    ):
        raise ContractError("Core→Profile必须精确绑定所需的当前运行producer集合")
    for name in required:
        supplied = handoff.inputs[name]
        expected = by_id[name]
        if (
            not isinstance(supplied, ProfileInput)
            or supplied.field_id != name
            or not isinstance(supplied.field, FieldValue)
        ):
            raise ContractError("Core→Profile字段身份或值类型错误：" + name)
        producer_symbol = expected.get("producer_symbol")
        runtime_producer = handoff.runtime_producers.get(producer_symbol)
        if (
            not expected.get("producer_stage")
            or not isinstance(producer_symbol, str)
            or not isinstance(runtime_producer, RuntimeProducerBinding)
            or runtime_producer.symbol != producer_symbol
            or not isinstance(runtime_producer.source_sha256, str)
            or len(runtime_producer.source_sha256) != 64
            or any(
                ch not in "0123456789abcdef" for ch in runtime_producer.source_sha256
            )
            or runtime_producer.algorithm_version != expected.get("algorithm_version")
            or not expected.get("expression_ref")
            or not expected.get("parameter_version")
        ):
            raise ContractError("Core→Profile上游producer/算法合同未绑定：" + name)
        if expected.get("requiredness") in {"OPTIONAL", "optional", False}:
            raise ContractError(
                "Profile规则依赖字段不得在registry中降为optional：" + name
            )
        exact = {
            "unit": expected.get("unit"),
            "producer_stage": expected.get("producer_stage"),
            "producer_symbol": producer_symbol,
            "producer_source_sha256": runtime_producer.source_sha256,
            "expression_ref": expected.get("expression_ref"),
            "algorithm_version": expected.get("algorithm_version"),
            "parameter_version": expected.get("parameter_version"),
            "coordinate_role": expected.get("coordinate_role"),
        }
        if any(getattr(supplied, key) != value for key, value in exact.items()):
            raise ContractError(
                "Core→Profile字段单位/producer/算法/坐标绑定错配：" + name
            )
        window_contract = expected.get("window_contract", "NONE")
        expected_window = handoff.expected_windows.get(name)
        if (window_contract != "NONE") != isinstance(expected_window, WindowIdentity):
            raise ContractError("Core→Profile独立窗口期望集合与合同不一致：" + name)
        if supplied.window is None:
            if window_contract != "NONE" or expected_window is not None:
                raise ContractError("Core→Profile缺少精确窗口身份：" + name)
        else:
            window = supplied.window
            if not isinstance(expected_window, WindowIdentity):
                raise ContractError("Core→Profile缺少独立选择器窗口期望：" + name)
            empty_window = (
                window.first_endpoint is None and window.last_endpoint is None
            )
            empty_valid = (
                empty_window
                and supplied.field.quality != "OBSERVED"
                and window.selected_rows_digest == sha256(b"[]").hexdigest()
            )
            endpoints_valid = (
                type(window.first_endpoint) is date
                and type(window.last_endpoint) is date
                and window.first_endpoint <= window.last_endpoint
                and window.last_endpoint <= handoff.target_session
            )
            if (
                window.contract != window_contract
                or not (empty_valid or endpoints_valid)
                or len(window.selected_rows_digest) != 64
                or any(
                    ch not in "0123456789abcdef" for ch in window.selected_rows_digest
                )
                or not window.calendar_revision
                or not window.status_revision
                or window != expected_window
            ):
                raise ContractError(
                    "Core→Profile窗口端点/坐标时序/版本身份错配：" + name
                )
        if expected.get("coordinate_role") == "TARGET_SESSION":
            if (
                not handoff.target_coordinate_id
                or supplied.coordinate_id != handoff.target_coordinate_id
                or supplied.context.get("context.target_session")
                != handoff.target_session
            ):
                raise ContractError("Core→Profile目标会话坐标缺失或错日：" + name)
            target_coordinates.add(supplied.coordinate_id)
        dependencies = expected.get("context_dependencies", [])
        expected_context = handoff.expected_context.get(name)
        if not isinstance(expected_context, Mapping):
            raise ContractError("Core→Profile缺少独立期望上下文：" + name)
        for dependency in dependencies:
            if dependency not in supplied.context or dependency not in expected_context:
                raise ContractError("Core→Profile缺少上下文依赖：" + dependency)
            if supplied.context[dependency] != expected_context[dependency]:
                raise ContractError(
                    "Core→Profile字段上下文与独立期望不符：" + dependency
                )
            if (
                supplied.context[dependency] != handoff.target_session
                and dependency == "context.target_session"
            ):
                raise ContractError("Core→Profile目标会话上下文错配：" + name)
            if (
                dependency == "context.source_cutoff"
                and supplied.context[dependency] != handoff.source_cutoff
            ):
                raise ContractError("Core→Profile source cutoff上下文错配：" + name)
            if (
                dependency in context_values
                and context_values[dependency] != supplied.context[dependency]
            ):
                raise ContractError("Core→Profile输入上下文版本不一致：" + dependency)
            context_values[dependency] = supplied.context[dependency]
        values[name] = supplied.field
    if len(target_coordinates) > 1:
        raise ContractError("Core→Profile输入混用价格坐标")
    if target_coordinates and target_coordinates != {handoff.target_coordinate_id}:
        raise ContractError("Core→Profile目标价格坐标身份错配")
    return values


def _validate_rule(rule):
    """数据门之前拒绝坏合同；只允许固定操作集，不执行代码。"""
    inputs = rule.get("input_fields")
    domains = rule.get("input_domains")
    if (
        not isinstance(inputs, list)
        or len(inputs) != len(set(inputs))
        or not isinstance(domains, Mapping)
        or set(inputs) != set(domains)
    ):
        raise ContractError("规则输入和类型域不一致")
    if rule.get("data_gate") not in {"ALL_INPUTS_KNOWN", "BRANCH_LOCAL_KLEENE"}:
        raise ContractError("缺少明确数据门")
    for domain in domains.values():
        kind = domain.get("type")
        if kind not in {"number", "boolean", "enum", "closed_prices"}:
            raise ContractError("合同含未注册输入类型")
        if kind == "enum" and (
            not domain.get("values")
            or any(not isinstance(x, str) for x in domain["values"])
        ):
            raise ContractError("合同枚举域非法")
        if kind == "closed_prices" and (
            type(domain.get("minimum_count")) is not int or domain["minimum_count"] < 2
        ):
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
            arity = {
                "not": 1,
                "subtract": 2,
                "divide": 2,
                "multiply": 2,
                "lt": 2,
                "lte": 2,
                "gt": 2,
                "gte": 2,
                "eq": 2,
            }
            if op not in arity and op not in {"and", "or", "in"}:
                raise ContractError("未支持的合同操作")
            if (
                (op in arity and len(args) != arity[op])
                or (op in {"and", "or"} and not args)
                or (op == "in" and len(args) < 2)
            ):
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
        elif (
            operand.get("field_id") not in inputs
            or operand.get("component") != "close"
            or domains[operand["field_id"]].get("type") != "closed_prices"
            or operand.get("endpoint_offset") not in (0, -1)
            or type(operand.get("count", 1)) is not int
            or operand.get("count", 1) <= 0
        ):
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


def _evaluate_detail(node, values, missing_refs):
    if isinstance(node, bool):
        return node, set()
    if isinstance(node, str):
        return node, set()
    if not isinstance(node, Mapping):
        return exact_number(node), set()
    if set(node) == {"ref"}:
        if node["ref"] not in values:
            raise ContractError("表达引用未绑定：" + node["ref"])
        value = values[node["ref"]]
        return value, set(missing_refs.get(node["ref"], ())) if value is None else set()
    if set(node) != {"op", "args"}:
        raise ContractError("表达节点结构非法")
    op, args = node["op"], node["args"]
    if not isinstance(args, list):
        raise ContractError("表达参数必须为列表")
    arity = {
        "not": 1,
        "subtract": 2,
        "divide": 2,
        "multiply": 2,
        "lt": 2,
        "lte": 2,
        "gt": 2,
        "gte": 2,
        "eq": 2,
    }
    if op not in arity and op not in {"and", "or", "in"}:
        raise ContractError("未支持的表达操作：" + str(op))
    if (op in arity and len(args) != arity[op]) or (
        op in {"and", "or", "in"} and not args
    ):
        raise ContractError("表达参数数量非法")
    details = [_evaluate_detail(arg, values, missing_refs) for arg in args]
    evaluated = [value for value, _ in details]
    unavailable = [refs for value, refs in details if value is None]
    if op in {"and", "or"}:
        terms = [_truth(x) for x in evaluated]
        if op == "and":
            return (
                (False, set())
                if False in terms
                else (None, set().union(*unavailable))
                if None in terms
                else (True, set())
            )
        return (
            (True, set())
            if True in terms
            else (None, set().union(*unavailable))
            if None in terms
            else (False, set())
        )
    if op == "not":
        term = _truth(evaluated[0])
        return (None, unavailable[0]) if term is None else (not term, set())
    if any(item is None for item in evaluated):
        return None, set().union(*unavailable)
    if op == "in":
        return evaluated[0] in evaluated[1:], set()
    left, right = evaluated
    if op == "eq":
        return left == right, set()
    if any(type(x) is bool or not isinstance(x, Fraction) for x in evaluated):
        raise ContractError("比较/算术操作必须使用精确数值")
    if op == "subtract":
        return left - right, set()
    if op == "multiply":
        return left * right, set()
    if op == "divide":
        return (left / right if right > 0 else None), set()
    return {
        "lt": lambda: left < right,
        "lte": lambda: left <= right,
        "gt": lambda: left > right,
        "gte": lambda: left >= right,
    }[op](), set()


def _evaluate(node, values):
    return _evaluate_detail(node, values, {})[0]


def _derived_unavailable(qualities: list[str], reasons: list[str]) -> FieldValue:
    """UNKNOWN支配NA；只有NA来源时派生字段仍为NA。"""
    reason = ";".join(reasons) or "REQUIRED_INPUT_UNAVAILABLE"
    if qualities and all(quality == "NOT_APPLICABLE" for quality in qualities):
        return FieldValue(None, "NOT_APPLICABLE", reason)
    return unknown(reason)


def evaluate_profile_kernel(
    field_id: str,
    handoff: ProfileHandoff,
    rules: Mapping,
    field_registry: list[Mapping],
) -> FieldValue:
    """执行可独立验证的纯画像规则；不构成生产准入或可信身份证明。"""
    rule = _rule_for(field_id, rules)
    _validate_rule(rule)
    inputs = validate_profile_handoff(field_id, handoff, rules, field_registry)
    values, reasons, unavailable_qualities, missing_refs = {}, [], {}, {}
    for name, parameter in rule["parameters"].items():
        if (
            parameter.get("adoption_state") != "ADOPTED_RESEARCH_BASELINE"
            or parameter.get("value") is None
        ):
            raise ContractError("参数尚未采用：" + name)
        try:
            value = exact_number(parameter["value"])
        except ValueError as error:
            raise ContractError("参数非法：" + name) from error
        if (
            parameter.get("domain") == "FINITE_POSITIVE_NUMBER_REJECT_BOOL"
            and value <= 0
        ):
            raise ContractError("参数必须为正有限数：" + name)
        values["parameter." + name] = value
    for key, domain in rule["input_domains"].items():
        if key not in inputs or not isinstance(inputs[key], FieldValue):
            raise ContractError("调用缺少带质量的输入：" + key)
        field = inputs[key]
        if field.quality != "OBSERVED":
            values[key] = None
            reasons.append(key + ":" + field.quality + ":" + field.reason)
            unavailable_qualities[key] = field.quality
            missing_refs[key] = {key}
            continue
        try:
            value = field.value
            if domain["type"] == "number":
                value = exact_number(value)
                for bound, test in (
                    ("min", lambda x, b: x >= b),
                    ("exclusive_min", lambda x, b: x > b),
                    ("max", lambda x, b: x <= b),
                ):
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
                if (
                    not isinstance(value, ClosedPrices)
                    or not value.coordinate_id
                    or type(value.target_session) is not date
                ):
                    raise ValueError("缺少闭合周期或坐标")
                supplied = handoff.inputs[key]
                if (
                    value.target_session != handoff.target_session
                    or value.coordinate_id != handoff.target_coordinate_id
                    or value.coordinate_id != supplied.coordinate_id
                ):
                    raise ValueError("闭合周期目标会话或价格坐标与交接身份不一致")
                size = len(value.closes)
                if size < count or any(
                    len(x) != size
                    for x in (
                        value.calendar_ordinals,
                        value.last_sessions,
                        value.complete,
                    )
                ):
                    raise ValueError("闭合周期数量不齐")
                ordinals, sessions = (
                    value.calendar_ordinals[-count:],
                    value.last_sessions[-count:],
                )
                if (
                    any(type(x) is not int for x in ordinals)
                    or tuple(ordinals) != tuple(range(ordinals[0], ordinals[0] + count))
                    or any(x is not True for x in value.complete[-count:])
                    or any(
                        type(x) is not date or x > value.target_session
                        for x in sessions
                    )
                    or list(sessions) != sorted(set(sessions))
                ):
                    raise ValueError("周期未闭合/不连续/包含未来")
                selected = value.closes[-count:]
                if any(
                    not isinstance(x, FieldValue) or x.quality != "OBSERVED"
                    for x in selected
                ):
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
            unavailable_qualities[key] = "UNKNOWN_DATA"
            missing_refs[key] = {key}
    if rule.get("data_gate") == "ALL_INPUTS_KNOWN" and reasons:
        return _derived_unavailable(list(unavailable_qualities.values()), reasons)
    for name, operand in rule["operands"].items():
        if "expression" in operand:
            value, dependencies = _evaluate_detail(
                operand["expression"], values, missing_refs
            )
        else:
            series = values[operand["field_id"]]
            end = len(series) + operand["endpoint_offset"] if series is not None else 0
            count = operand.get("count", 1)
            value = (
                sum(series[end - count : end], Fraction(0)) / count
                if series is not None
                else None
            )
            dependencies = (
                set()
                if value is not None
                else set(missing_refs.get(operand["field_id"], {operand["field_id"]}))
            )
        if value is not None and "valid_range" in operand:
            low, high = map(exact_number, operand["valid_range"])
            if (
                value < low
                or value > high
                or (operand.get("lower_exclusive") and value == low)
            ):
                value = None
                reasons.append(name + ":DATA_INVALID_RANGE")
                unavailable_qualities[name] = "UNKNOWN_DATA"
                dependencies = {name}
        values[name] = value
        if value is None:
            missing_refs[name] = dependencies
    for branch in rule["ordered_branches"]:
        result, unresolved_refs = _evaluate_detail(branch["when"], values, missing_refs)
        result = _truth(result)
        if result is None:
            ordered_names = (*rule["input_fields"], *rule["operands"].keys())
            relevant = [
                name
                for name in ordered_names
                if name in unresolved_refs and name in unavailable_qualities
            ]
            qualities = [unavailable_qualities[name] for name in relevant]
            cause_by_name = {item.split(":", 1)[0]: item for item in reasons}
            relevant_reasons = [
                cause_by_name[name] for name in relevant if name in cause_by_name
            ]
            return _derived_unavailable(qualities, relevant_reasons)
        if result:
            if branch["value"] not in rule["output_values"]:
                raise ContractError("规则输出未注册")
            return FieldValue(branch["value"])
    raise ContractError("规则缺少穷尽分支")


def profile(
    field_id: str,
    handoff: ProfileHandoff,
    rules: Mapping,
    field_registry: list[Mapping],
) -> FieldValue:
    """生产画像入口；可信FOUNDATION选择器未实现时先拒绝全部生产求值。"""
    rule = _rule_for(field_id, rules)
    _validate_rule(rule)
    # 先执行可独立核验的合同拒绝，避免重复registry、缺producer等错误被遮蔽。
    # 这些比较只验证结构一致性；同一调用方仍可同时伪造输入和期望身份。
    validate_profile_handoff(field_id, handoff, rules, field_registry)
    raise ContractError(
        "生产画像准入阻断：可信FOUNDATION选择器及已验证行选择结果尚未实现"
    )


def closed_prices_from_period_bars(
    period_bars: Sequence[object],
    *,
    minimum_count: int,
    target_session: date,
    source_cutoff: str,
    coordinate_id: str,
) -> ClosedPrices:
    """把连续周期槽投影为Profile既有ClosedPrices输入，不跳过坏槽。"""
    if not isinstance(period_bars, Sequence) or isinstance(period_bars, (str, bytes)):
        raise ContractError("period_bars必须是有序序列")
    if type(minimum_count) is not int or minimum_count < 2:
        raise ContractError("minimum_count必须是至少2的整数")
    if (
        type(target_session) is not date
        or not isinstance(source_cutoff, str)
        or not source_cutoff
        or not isinstance(coordinate_id, str)
        or not coordinate_id
    ):
        raise ContractError("Profile周期投影缺少目标会话、来源截止或坐标")
    selected = tuple(period_bars[-minimum_count:])
    if not selected:
        if minimum_count not in {4, 6}:
            raise ContractError("Profile周/月窗口长度必须遵循已冻结的6周/4月规则")
        return ClosedPrices((), (), (), (), target_session, coordinate_id)
    required = (
        "period_kind",
        "calendar_ordinal",
        "planned_last_session",
        "eligible_closed_period",
        "close",
        "target_session",
        "source_cutoff",
        "coordinate_id",
        "calendar_closed",
        "calendar_revision",
        "status_revision",
        "action_revision",
        "aggregation_version",
    )
    if any(any(not hasattr(row, name) for name in required) for row in selected):
        raise ContractError("period_bar缺少Profile映射所需字段")
    kind = selected[0].period_kind
    if any(row.period_kind != kind for row in selected):
        raise ContractError("Profile周期窗口混合W/M")
    frozen_count = 6 if kind == "W" else 4 if kind == "M" else None
    if frozen_count is None or minimum_count != frozen_count:
        raise ContractError("Profile周/月窗口长度必须遵循已冻结的6周/4月规则")
    ordinals = tuple(row.calendar_ordinal for row in selected)
    if any(type(value) is not int for value in ordinals) or ordinals != tuple(
        range(ordinals[0], ordinals[0] + len(ordinals))
    ):
        raise ContractError("Profile周期窗口的官方日历序号不连续")
    if any(
        row.target_session != target_session or row.coordinate_id != coordinate_id
        for row in selected
    ):
        raise ContractError("Profile周期bar目标会话或坐标身份错配")
    if any(row.source_cutoff.isoformat() != source_cutoff for row in selected):
        raise ContractError("Profile周期bar来源截止错配")
    for attribute in (
        "calendar_revision",
        "status_revision",
        "action_revision",
        "aggregation_version",
    ):
        if len({getattr(row, attribute) for row in selected}) != 1:
            raise ContractError("Profile周期窗口混用版本：" + attribute)
    if any(
        row.calendar_closed is not True
        and getattr(row, "state", None) != "CALENDAR_COVERAGE_UNPROVEN"
        for row in selected
    ):
        raise ContractError("Profile周期窗口含as-of未闭合日历槽")
    if any(not isinstance(row.close, FieldValue) for row in selected):
        raise ContractError("Profile周期收盘缺少质量值")
    sessions = tuple(row.planned_last_session for row in selected)
    if any(type(session) is not date for session in sessions) or sessions != tuple(
        sorted(set(sessions))
    ):
        raise ContractError("Profile周期计划末会话非法或未递增")
    return ClosedPrices(
        tuple(row.close for row in selected),
        ordinals,
        sessions,
        tuple(
            row.eligible_closed_period is True
            and getattr(row, "calendar_coverage_proven", True)
            for row in selected
        ),
        target_session,
        coordinate_id,
    )
