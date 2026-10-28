"""同一DAG计划使用的纯增量模式判定。"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date
from hashlib import sha256
import json
from types import MappingProxyType

from stock_a.domain import ContractError


# 该摘要固定正式机器合同中的完整scope闭包目录；依赖键仅维护在合同文件。
_OFFICIAL_SCOPE_CLOSURE_CATALOG_DIGEST = "3cce5f595d838d988e422199df8346359bebe5910811d98f88c94dcfe013f265"
_OFFICIAL_DAY_CHAIN_CONTRACT_SHA256 = "89e4150e577196f95ef459a121308e191396bb0a991bc0d2e37829cddb4bb3eb"
_DAY_PLAN_KERNEL_SYMBOL = "stock_a.planning:assemble_day_plan"
_OMIT_DIGEST_VALUE = object()


class DayPlanError(ContractError):
    """DayPlan合同拒绝，带稳定机器原因码。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


@dataclass(frozen=True)
class AcceptedRunIdentity:
    """前一已接受研究的最小计划身份，不包含业务payload。"""

    target_session: date
    head_generation: int
    calendar_id: str
    calendar_revision: str
    scope_id: str
    scope_contract_version: str
    scope_contract_digest: str
    scope_catalog_digest: str
    semantic_revisions: Mapping[str, str]
    business_input_digest: str


@dataclass(frozen=True)
class IncrementalModeDecision:
    """纯分类结果；修订路径只标记，不在此执行。"""

    mode: str
    target_session: date
    expected_parent_generation: int | None
    sessions_to_append: tuple[date, ...]
    reason_codes: tuple[str, ...]


@dataclass(frozen=True)
class ScopeRevisionClosureContract:
    """从正式scope合同解析出的revision闭包声明及其内容摘要。"""

    scope_id: str
    contract_id: str
    contract_version: str
    required_keys: tuple[str, ...]
    binding_digest: str
    scope_catalog: Mapping[str, tuple[str, str, tuple[str, ...], str]]
    catalog_digest: str


def _scope_revision_binding_digest(
    *, scope_id: str, contract_id: str, contract_version: str,
    required_keys: tuple[str, ...],
) -> str:
    canonical = json.dumps(
        {
            "scope_id": scope_id,
            "contract_id": contract_id,
            "contract_version": contract_version,
            "required_keys": sorted(required_keys),
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(canonical).hexdigest()


def _validated_scope_contract(value: ScopeRevisionClosureContract) -> ScopeRevisionClosureContract:
    if not isinstance(value, ScopeRevisionClosureContract):
        raise ContractError("必须提供正式scope revision闭包合同对象")
    if any(
        not isinstance(item, str) or not item.strip()
        for item in (value.scope_id, value.contract_id, value.contract_version)
    ):
        raise ContractError("scope revision合同身份或版本缺失")
    keys = value.required_keys
    if (
        not isinstance(keys, tuple) or not keys
        or any(not isinstance(key, str) or not key.strip() for key in keys)
        or len(set(keys)) != len(keys)
    ):
        raise ContractError("正式scope合同必需revision键必须完整、非空且无重复")
    expected = _scope_revision_binding_digest(
        scope_id=value.scope_id,
        contract_id=value.contract_id,
        contract_version=value.contract_version,
        required_keys=keys,
    )
    if value.binding_digest != expected:
        raise ContractError("scope revision合同摘要与scope身份、版本或完整键集合不符")
    if not isinstance(value.scope_catalog, Mapping) or not value.scope_catalog:
        raise ContractError("正式scope revision合同目录缺失")
    normalized_catalog = {}
    for scope_id, entry in value.scope_catalog.items():
        if not isinstance(scope_id, str) or not scope_id.strip():
            raise ContractError("scope revision合同目录含非法scope身份")
        if not isinstance(entry, tuple) or len(entry) != 4:
            raise ContractError("scope revision合同目录项结构非法")
        contract_id, contract_version, required, entry_digest = entry
        if any(not isinstance(item, str) or not item.strip()
               for item in (contract_id, contract_version)):
            raise ContractError("scope revision合同目录项缺少合同身份或版本")
        if (not isinstance(required, tuple) or not required
                or any(not isinstance(key, str) or not key.strip() for key in required)
                or len(set(required)) != len(required)):
            raise ContractError("scope revision合同目录项必需键集合非法")
        entry_expected = _scope_revision_binding_digest(
            scope_id=scope_id,
            contract_id=contract_id,
            contract_version=contract_version,
            required_keys=required,
        )
        if entry_digest != entry_expected:
            raise ContractError("scope revision合同目录项摘要不匹配")
        normalized_catalog[scope_id] = {
            "contract_id": contract_id,
            "contract_version": contract_version,
            "required_keys": sorted(required),
            "binding_digest": entry_digest,
        }
    selected = normalized_catalog.get(value.scope_id)
    if selected is None or (
        selected["contract_id"] != value.contract_id
        or selected["contract_version"] != value.contract_version
        or tuple(selected["required_keys"]) != tuple(sorted(value.required_keys))
        or selected["binding_digest"] != value.binding_digest
    ):
        raise ContractError("所选scope与正式合同闭包目录不一致")
    catalog_payload = json.dumps(
        {
            "contract_id": value.contract_id,
            "contract_version": value.contract_version,
            "scope_contracts": normalized_catalog,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    calculated_catalog_digest = sha256(catalog_payload).hexdigest()
    if (value.catalog_digest != calculated_catalog_digest
            or value.catalog_digest != _OFFICIAL_SCOPE_CLOSURE_CATALOG_DIGEST):
        raise ContractError("scope revision合同目录摘要不是正式合同绑定摘要")
    return value


def _revision_map(value: Mapping[str, str], name: str) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise ContractError(name + "必须是revision映射")
    result = {}
    for key, revision in value.items():
        if not isinstance(key, str) or not key.strip():
            raise ContractError(name + "含空白或非法revision键")
        if not isinstance(revision, str):
            raise ContractError(name + "的revision值必须是字符串")
        result[key] = revision
    return result


def _business_digest(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ContractError(name + "必须是非空业务输入摘要")
    return value


def classify_incremental_mode(
    *,
    target_session: date,
    official_sessions: tuple[date, ...],
    calendar_id: str,
    calendar_revision: str,
    semantic_revisions: Mapping[str, str],
    scope_revision_contract: ScopeRevisionClosureContract,
    business_input_digest: str,
    previous: AcceptedRunIdentity | None,
    historical_repair_required: bool = False,
) -> IncrementalModeDecision:
    """按显式会话和revision身份区分首次运行、APPEND与修订需求。

    此纯函数校验正式scope合同绑定摘要，不读取配置/数据库，也不执行RANGE_REPAIR。
    """
    if type(target_session) is not date:
        raise ContractError("计划目标会话必须是date")
    if (not isinstance(calendar_id, str) or not calendar_id.strip()
            or not isinstance(calendar_revision, str) or not calendar_revision.strip()):
        raise ContractError("计划缺少官方日历身份")
    if (not isinstance(official_sessions, tuple) or not official_sessions
            or any(type(session) is not date for session in official_sessions)
            or tuple(sorted(set(official_sessions))) != official_sessions):
        raise ContractError("官方会话必须是非空、无重复且递增的date元组")
    if target_session not in official_sessions:
        raise ContractError("目标会话不在显式官方会话集合中")
    if type(historical_repair_required) is not bool:
        raise ContractError("historical_repair_required必须为bool")
    current_revisions = _revision_map(semantic_revisions, "当前语义revision")
    scope_contract = _validated_scope_contract(scope_revision_contract)
    required_keys = scope_contract.required_keys
    current_digest = _business_digest(business_input_digest, "当前业务输入摘要")

    if set(current_revisions) != set(required_keys):
        raise ContractError("当前语义revision闭包键集必须与scope输入合同精确一致")
    if any(not revision.strip() for revision in current_revisions.values()):
        raise ContractError("当前语义revision闭包含空值")

    if previous is None:
        return IncrementalModeDecision("INITIAL", target_session, None, (), ())
    if not isinstance(previous, AcceptedRunIdentity):
        raise ContractError("previous必须是AcceptedRunIdentity或None")
    if type(previous.target_session) is not date:
        raise ContractError("前一接受目标会话必须是date")
    if (type(previous.head_generation) is not int or previous.head_generation < 0):
        raise ContractError("前一Head generation必须是非负整数且不能是bool")
    if (not isinstance(previous.calendar_id, str) or not previous.calendar_id.strip()
            or not isinstance(previous.calendar_revision, str) or not previous.calendar_revision.strip()):
        raise ContractError("前一接受状态缺少官方日历身份")
    prior_revisions = _revision_map(previous.semantic_revisions, "前一语义revision")
    previous_digest = _business_digest(previous.business_input_digest, "前一业务输入摘要")

    if (
        previous.scope_id != scope_contract.scope_id
        or previous.scope_contract_version != scope_contract.contract_version
        or previous.scope_contract_digest != scope_contract.binding_digest
        or previous.scope_catalog_digest != scope_contract.catalog_digest
    ):
        return IncrementalModeDecision(
            "RANGE_REPAIR_REQUIRED", target_session, previous.head_generation, (),
            ("PRIOR_SCOPE_REVISION_CONTRACT_MISMATCH",),
        )
    if set(prior_revisions) != set(required_keys):
        return IncrementalModeDecision(
            "RANGE_REPAIR_REQUIRED", target_session, previous.head_generation, (),
            ("PRIOR_REVISION_CLOSURE_UNBOUND",),
        )
    if any(not revision.strip() for revision in prior_revisions.values()):
        return IncrementalModeDecision(
            "RANGE_REPAIR_REQUIRED", target_session, previous.head_generation, (),
            ("SEMANTIC_REVISION_MISSING",),
        )

    if (target_session == previous.target_session
            and previous.calendar_id == calendar_id
            and previous.calendar_revision == calendar_revision
            and prior_revisions == current_revisions
            and previous_digest == current_digest
            and not historical_repair_required):
        return IncrementalModeDecision(
            "NOOP", target_session, previous.head_generation, (), ("ALREADY_ACCEPTED",)
        )

    reasons = []
    if historical_repair_required:
        reasons.append("HISTORICAL_DEPENDENCY_REPAIR_REQUIRED")
    if (previous.calendar_id != calendar_id
            or previous.calendar_revision != calendar_revision):
        reasons.append("CALENDAR_IDENTITY_CHANGED")
    if target_session <= previous.target_session:
        reasons.append("TARGET_NOT_AFTER_ACCEPTED_SESSION")
    if set(prior_revisions) != set(current_revisions):
        reasons.append("SEMANTIC_REVISION_SET_CHANGED")
    elif prior_revisions != current_revisions:
        reasons.append("SEMANTIC_REVISION_CHANGED")
    if previous_digest != current_digest:
        reasons.append("BUSINESS_INPUT_IDENTITY_CHANGED")

    if reasons:
        return IncrementalModeDecision(
            "RANGE_REPAIR_REQUIRED", target_session, previous.head_generation, (), tuple(reasons)
        )
    if previous.target_session not in official_sessions:
        return IncrementalModeDecision(
            "RANGE_REPAIR_REQUIRED", target_session, previous.head_generation, (),
            ("ACCEPTED_SESSION_NOT_IN_CURRENT_CALENDAR",),
        )
    prior_index = official_sessions.index(previous.target_session)
    target_index = official_sessions.index(target_session)
    sessions_to_append = official_sessions[prior_index + 1:target_index + 1]
    if not sessions_to_append:
        return IncrementalModeDecision(
            "RANGE_REPAIR_REQUIRED", target_session, previous.head_generation, (),
            ("APPEND_SESSION_RANGE_EMPTY",),
        )
    return IncrementalModeDecision(
        "APPEND", target_session, previous.head_generation, sessions_to_append, ()
    )


@dataclass(frozen=True)
class CapabilityEvidence:
    """上游显式报告的能力状态；本类型不验证该声明的真实性。"""

    status: str
    evidence_ref: str | None
    metadata: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class DayPlanRequest:
    """P00纯计划输入；没有隐式I/O或默认业务身份。"""

    contract: Mapping[str, object]
    contract_sha256: str
    scope_id: str
    target_session: date
    official_sessions: tuple[date, ...]
    calendar_id: str
    calendar_revision: str
    scope_revision_contract: ScopeRevisionClosureContract
    semantic_revisions: Mapping[str, str]
    previous: AcceptedRunIdentity | None
    historical_repair_required: bool
    validated_inputs: Mapping[str, CapabilityEvidence]
    environment_capabilities: Mapping[str, CapabilityEvidence]
    frozen_context: Mapping[str, object]
    business_identity: Mapping[str, object]
    prior_context: Mapping[str, object]


@dataclass(frozen=True)
class DayPlan:
    """不可变计划候选；不表示P00真实生产核验或DAG已执行。"""

    scope_id: str
    contract_id: str
    contract_version: str
    contract_sha256: str
    plan_sha256: str
    target_session: date
    calendar_id: str
    calendar_revision: str
    mode: str
    decision_state: str
    expected_parent_generation: int | None
    sessions_to_append: tuple[date, ...]
    session_range: tuple[date, ...]
    required_stage_ids: tuple[str, ...]
    scheduled_stage_ids: tuple[str, ...]
    stage_kernel_roles: tuple[tuple[str, str], ...]
    required_owner_roles: tuple[str, ...]
    required_field_ids: tuple[str, ...]
    required_consumers: tuple[str, ...]
    history_policy: Mapping[str, object]
    frozen_context: Mapping[str, object]
    input_digest: str
    reason_codes: tuple[str, ...]
    candidate_ready: bool


@dataclass(frozen=True)
class EnvironmentGate:
    """按显式上游状态聚合的计划门，不是运行时部署证明。"""

    status: str
    capability_states: tuple[tuple[str, str, str | None, Mapping[str, object]], ...]
    reason_codes: tuple[str, ...]
    upstream_claims_authenticated: bool = False


@dataclass(frozen=True)
class DayPlanResult:
    day_plan: DayPlan
    environment_gate: EnvironmentGate
    prior_context: Mapping[str, object]


def _plan_error(code: str, message: str) -> DayPlanError:
    return DayPlanError(code, message)


def _json_identity(value: object, name: str, excluded_keys: frozenset[str] = frozenset()) -> object:
    """校验并复制稳定JSON身份，拒绝浮点、日期隐式转换及任意对象。"""
    if value is None or type(value) in (str, bool, int):
        return value
    if type(value) is float:
        raise _plan_error("BUSINESS_IDENTITY_INVALID", name + "不能含二进制浮点数")
    if isinstance(value, Mapping):
        result = {}
        for key, item in value.items():
            if not isinstance(key, str) or not key:
                raise _plan_error("BUSINESS_IDENTITY_INVALID", name + "含非法对象键")
            if key in excluded_keys:
                continue
            if key in result:
                raise _plan_error("BUSINESS_IDENTITY_INVALID", name + "含重复对象键")
            result[key] = _json_identity(item, name + "." + key, excluded_keys)
        return result
    if isinstance(value, (tuple, list)):
        return [_json_identity(item, name + "[]", excluded_keys) for item in value]
    raise _plan_error("BUSINESS_IDENTITY_INVALID", name + "含不支持的身份值")


def _freeze_value(value: object) -> object:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze_value(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze_value(item) for item in value)
    return value


def _freeze_mapping(value: Mapping[str, object]) -> Mapping[str, object]:
    return MappingProxyType({key: _freeze_value(item) for key, item in value.items()})


def _without_digest_exclusions(value: object, excluded_keys: frozenset[str]) -> object:
    """递归投影摘要输入，按合同键名移除所有运行时元数据。"""
    if isinstance(value, Mapping):
        projected = {}
        for key, item in value.items():
            if key in excluded_keys:
                continue
            child = _without_digest_exclusions(item, excluded_keys)
            if child is not _OMIT_DIGEST_VALUE:
                projected[key] = child
        if not projected and value:
            return _OMIT_DIGEST_VALUE
        return projected
    if isinstance(value, (tuple, list)):
        projected = [
            child for item in value
            if (child := _without_digest_exclusions(item, excluded_keys)) is not _OMIT_DIGEST_VALUE
        ]
        if not projected and value:
            return _OMIT_DIGEST_VALUE
        return projected
    return value


def _validated_contract(request: DayPlanRequest) -> tuple[Mapping[str, object], str]:
    if not isinstance(request.contract, Mapping):
        raise _plan_error("CONTRACT_INVALID", "必须显式提供正式机器合同映射")
    try:
        canonical = json.dumps(
            request.contract, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise _plan_error("CONTRACT_INVALID", "机器合同不是稳定JSON结构") from exc
    calculated = sha256(canonical).hexdigest()
    if request.contract_sha256 != calculated or calculated != _OFFICIAL_DAY_CHAIN_CONTRACT_SHA256:
        raise _plan_error("CONTRACT_SHA_MISMATCH", "机器合同规范化摘要不是当前正式合同绑定摘要")
    if (request.contract.get("contract_id") != "STOCK-A-DAY-CHAIN"
            or request.contract.get("version") != "R10"):
        raise _plan_error("CONTRACT_VERSION_MISMATCH", "机器合同身份或版本不匹配")
    binding = request.contract.get("scope_bindings")
    if not isinstance(binding, Mapping) or "FOUNDATION_EOD" not in binding:
        raise _plan_error("SCOPE_BINDING_MISSING", "正式合同缺少FOUNDATION_EOD绑定")
    day_plan_binding = binding["FOUNDATION_EOD"].get("day_plan_pure_kernel")
    if (not isinstance(day_plan_binding, Mapping)
            or day_plan_binding.get("contract_digest_algorithm") != "SHA-256"
            or day_plan_binding.get("contract_digest_canonicalization") != "UTF-8 JSON; sorted object keys; compact separators"):
        raise _plan_error("CONTRACT_DIGEST_CANONICALIZATION_UNSUPPORTED", "机器合同摘要算法与规范化规则不支持")
    return request.contract, calculated


def _validate_field_dependency_closure(
    scope: Mapping[str, object],
    included_stages: set[str],
    known_stages: set[str],
    binding_by_id: Mapping[str, Mapping[str, object]],
    artifacts: Mapping[str, object],
) -> None:
    field_registry = scope.get("field_registry")
    if not isinstance(field_registry, list) or not field_registry:
        raise _plan_error("DAG_FIELD_REGISTRY_INCOMPLETE", "scope缺少字段依赖登记")
    if scope.get("within_stage_field_toposort_required") is not True:
        raise _plan_error("DAG_FIELD_TOPOLOGY_NOT_REQUIRED", "scope未要求同stage字段依赖拓扑校验")

    fields_by_id: dict[str, Mapping[str, object]] = {}
    for field in field_registry:
        if not isinstance(field, Mapping):
            raise _plan_error("DAG_FIELD_INVALID", "字段依赖登记结构非法")
        field_id = field.get("field_id")
        producer_stage = field.get("producer_stage")
        output_artifact = field.get("output_artifact")
        consumers = field.get("consumers")
        input_fields = field.get("input_fields")
        if (not isinstance(field_id, str) or not field_id.strip() or field_id in fields_by_id
                or not isinstance(producer_stage, str) or producer_stage not in included_stages
                or not isinstance(output_artifact, str) or not output_artifact
                or not isinstance(consumers, list) or not consumers
                or any(not isinstance(item, str) or not item.strip() for item in consumers)
                or len(consumers) != len(set(consumers))
                or not isinstance(input_fields, list)
                or any(not isinstance(item, str) or not item.strip() for item in input_fields)
                or len(input_fields) != len(set(input_fields))):
            raise _plan_error("DAG_FIELD_INVALID", "字段身份、stage、输入或consumer登记非法")
        artifact = artifacts.get(output_artifact)
        stage_binding = binding_by_id.get(producer_stage)
        if (not isinstance(artifact, Mapping) or artifact.get("producer") != producer_stage
                or not isinstance(stage_binding, Mapping)
                or output_artifact not in stage_binding.get("outputs", [])):
            raise _plan_error("DAG_FIELD_OUTPUT_UNBOUND", "字段输出产物未由其producer stage正式产生: " + field_id)
        fields_by_id[field_id] = field

    for field_id, field in fields_by_id.items():
        producer_stage = field["producer_stage"]
        artifact = artifacts[field["output_artifact"]]
        artifact_consumers = artifact.get("consumers", [])
        for consumer in field["consumers"]:
            if consumer not in known_stages:
                continue
            if consumer == producer_stage:
                continue
            consumer_binding = binding_by_id.get(consumer)
            if (consumer not in included_stages
                    or consumer not in artifact_consumers
                    or not isinstance(consumer_binding, Mapping)
                    or field["output_artifact"] not in consumer_binding.get("required_inputs", [])):
                raise _plan_error(
                    "DAG_FIELD_CONSUMER_MISMATCH",
                    field_id + "声明的stage消费者未绑定对应required产物输入: " + consumer,
                )

    dependencies: dict[str, set[str]] = {field_id: set() for field_id in fields_by_id}
    for field_id, field in fields_by_id.items():
        producer_stage = field["producer_stage"]
        for input_field_id in field["input_fields"]:
            source = fields_by_id.get(input_field_id)
            if source is None:
                raise _plan_error("DAG_FIELD_INPUT_UNBOUND", field_id + "引用未登记字段: " + input_field_id)
            source_stage = source["producer_stage"]
            source_artifact = source["output_artifact"]
            if source_stage != producer_stage:
                stage_binding = binding_by_id[producer_stage]
                source_artifact_contract = artifacts[source_artifact]
                if (source_artifact not in stage_binding.get("required_inputs", [])
                        or producer_stage not in source["consumers"]
                        or producer_stage not in source_artifact_contract.get("consumers", [])):
                    raise _plan_error(
                        "DAG_FIELD_CONSUMER_MISMATCH",
                        field_id + "的跨stage字段输入未同时绑定required artifact及字段/产物consumer: " + input_field_id,
                    )
            dependencies[field_id].add(input_field_id)

    remaining = {field_id: set(inputs) for field_id, inputs in dependencies.items()}
    while remaining:
        ready = sorted(field_id for field_id, inputs in remaining.items() if not inputs)
        if not ready:
            raise _plan_error("DAG_FIELD_CYCLE", "scope字段依赖存在环")
        for field_id in ready:
            del remaining[field_id]
        for inputs in remaining.values():
            inputs.difference_update(ready)


def _validate_stage_dag(contract: Mapping[str, object], scope_id: str) -> tuple[tuple[str, ...], tuple[tuple[str, str], ...]]:
    bindings = contract.get("scope_bindings")
    scope = bindings.get(scope_id) if isinstance(bindings, Mapping) else None
    if not isinstance(scope, Mapping):
        raise _plan_error("SCOPE_BINDING_MISSING", "所选scope没有正式机器绑定")
    raw_bindings = scope.get("stage_bindings")
    raw_stages = contract.get("stages")
    artifacts = contract.get("artifact_groups")
    if not isinstance(raw_bindings, list) or not isinstance(raw_stages, list) or not isinstance(artifacts, Mapping):
        raise _plan_error("DAG_CONTRACT_INCOMPLETE", "机器合同缺少stage或artifact注册表")

    stage_by_id = {}
    for stage in raw_stages:
        if not isinstance(stage, Mapping) or not isinstance(stage.get("id"), str):
            raise _plan_error("DAG_STAGE_INVALID", "全局stage登记结构非法")
        stage_id = stage["id"]
        if stage_id in stage_by_id:
            raise _plan_error("DAG_DUPLICATE_STAGE_ID", "全局stage ID重复")
        stage_by_id[stage_id] = stage
    binding_by_id = {}
    for item in raw_bindings:
        if not isinstance(item, Mapping) or not isinstance(item.get("stage_id"), str):
            raise _plan_error("DAG_STAGE_BINDING_INVALID", "scope stage binding结构非法")
        stage_id = item["stage_id"]
        if stage_id in binding_by_id:
            raise _plan_error("DAG_DUPLICATE_STAGE_ID", "scope stage binding ID重复")
        if stage_id not in stage_by_id:
            raise _plan_error("DAG_STAGE_MISSING", "scope引用未登记stage: " + stage_id)
        role = item.get("kernel_role")
        if not isinstance(role, str) or not role.strip():
            raise _plan_error("DAG_KERNEL_ROLE_MISSING", "stage缺少具名kernel role: " + stage_id)
        if item.get("implementation_binding_required") is not True:
            raise _plan_error("DAG_KERNEL_BINDING_NOT_REQUIRED", "scope stage缺少必需kernel绑定: " + stage_id)
        global_stage = stage_by_id[stage_id]
        for binding_key, stage_key in (("required_inputs", "required_inputs"),
                                       ("optional_inputs", "optional_inputs"),
                                       ("outputs", "outputs")):
            projected = item.get(binding_key)
            declared = global_stage.get(stage_key)
            if (not isinstance(projected, list) or not isinstance(declared, list)
                    or any(not isinstance(value, str) for value in projected)
                    or len(projected) != len(set(projected))
                    or not set(projected).issubset(set(declared))):
                raise _plan_error("DAG_STAGE_BINDING_MISMATCH", stage_id + "的" + binding_key + "不是正式stage的合法scope投影")
        binding_by_id[stage_id] = item

    producer_for_artifact = {}
    for artifact_id, artifact in artifacts.items():
        if not isinstance(artifact_id, str) or not isinstance(artifact, Mapping):
            raise _plan_error("DAG_ARTIFACT_INVALID", "artifact登记结构非法")
        producer = artifact.get("producer")
        consumers = artifact.get("consumers")
        if not isinstance(producer, str) or not isinstance(consumers, list):
            raise _plan_error("DAG_ARTIFACT_INVALID", "artifact缺少producer或consumer: " + artifact_id)
        if artifact_id in producer_for_artifact:
            raise _plan_error("DAG_DUPLICATE_ARTIFACT_ID", "artifact ID重复")
        producer_for_artifact[artifact_id] = producer
        if producer not in stage_by_id:
            raise _plan_error("DAG_UNKNOWN_PRODUCER", "artifact producer未登记: " + artifact_id)
        if artifact_id not in stage_by_id[producer].get("outputs", []):
            raise _plan_error("DAG_PRODUCER_OUTPUT_MISMATCH", "artifact producer未声明输出: " + artifact_id)
        for consumer in consumers:
            if (consumer not in stage_by_id or artifact_id not in
                    stage_by_id[consumer].get("required_inputs", []) + stage_by_id[consumer].get("optional_inputs", [])):
                raise _plan_error("DAG_CONSUMER_INPUT_MISMATCH", "artifact consumer输入声明不匹配: " + artifact_id)
    for stage_id, binding in binding_by_id.items():
        for artifact_id in binding.get("required_inputs", []) + binding.get("optional_inputs", []):
            if artifact_id.startswith("ext."):
                continue
            artifact = artifacts.get(artifact_id)
            if not isinstance(artifact, Mapping) or stage_id not in artifact.get("consumers", []):
                raise _plan_error("DAG_CONSUMER_INPUT_MISMATCH", stage_id + "输入未登记为artifact消费者: " + artifact_id)

    included = set(binding_by_id)
    _validate_field_dependency_closure(scope, included, set(stage_by_id), binding_by_id, artifacts)

    for stage_id, binding in binding_by_id.items():
        for artifact_id in binding.get("required_inputs", []):
            if artifact_id.startswith("ext."):
                continue
            producer = producer_for_artifact.get(artifact_id)
            if producer is None:
                raise _plan_error("DAG_REQUIRED_INPUT_UNBOUND", stage_id + "必需输入无producer: " + artifact_id)
            if producer not in included:
                raise _plan_error("DAG_REQUIRED_PRODUCER_OUTSIDE_SCOPE", stage_id + "必需输入producer未纳入scope: " + artifact_id)
            if artifact_id not in binding_by_id[producer].get("outputs", []):
                raise _plan_error("DAG_REQUIRED_OUTPUT_UNBOUND", producer + "未在scope输出中绑定必需产物: " + artifact_id)
        for artifact_id in binding.get("outputs", []):
            if producer_for_artifact.get(artifact_id) != stage_id:
                raise _plan_error("DAG_REQUIRED_OUTPUT_UNBOUND", stage_id + "输出未绑定唯一producer: " + artifact_id)

    dependencies = {stage_id: set() for stage_id in included}
    for stage_id, binding in binding_by_id.items():
        for artifact_id in binding["required_inputs"]:
            producer = producer_for_artifact.get(artifact_id)
            if producer in included and producer != stage_id:
                dependencies[stage_id].add(producer)
            elif producer == stage_id:
                raise _plan_error("DAG_CYCLE", "stage自依赖: " + stage_id)
    order = []
    remaining = {stage_id: set(deps) for stage_id, deps in dependencies.items()}
    while remaining:
        ready = sorted(stage_id for stage_id, deps in remaining.items() if not deps)
        if not ready:
            raise _plan_error("DAG_CYCLE", "scope stage依赖存在环")
        for stage_id in ready:
            order.append(stage_id)
            del remaining[stage_id]
        for deps in remaining.values():
            deps.difference_update(ready)
    roles = tuple((stage_id, binding_by_id[stage_id]["kernel_role"]) for stage_id in order)
    return tuple(order), roles


def _validate_evidence_map(
    supplied: Mapping[str, CapabilityEvidence], required: tuple[str, ...], name: str,
    excluded_keys: frozenset[str],
) -> tuple[tuple[str, str, str | None, Mapping[str, object]], ...]:
    if not isinstance(supplied, Mapping):
        raise _plan_error("EVIDENCE_MAP_MISSING", name + "必须显式提供")
    states = []
    for key in required:
        evidence = supplied.get(key)
        if not isinstance(evidence, CapabilityEvidence):
            states.append((key, "UNKNOWN", None, MappingProxyType({})))
            continue
        status, ref = evidence.status, evidence.evidence_ref
        if not isinstance(status, str) or status not in {"READY", "DENY", "UNKNOWN"}:
            raise _plan_error("EVIDENCE_STATUS_INVALID", name + "状态必须为READY/DENY/UNKNOWN: " + key)
        if ref is not None and (not isinstance(ref, str) or not ref.strip()):
            raise _plan_error("EVIDENCE_REF_INVALID", name + "证据引用不能为空白字符串: " + key)
        if not isinstance(evidence.metadata, Mapping):
            raise _plan_error("EVIDENCE_METADATA_INVALID", name + "证据metadata必须是映射: " + key)
        projected_metadata = _without_digest_exclusions(evidence.metadata, excluded_keys)
        if projected_metadata is _OMIT_DIGEST_VALUE:
            projected_metadata = {}
        metadata = _json_identity(projected_metadata, name + "." + key + ".metadata")
        if not isinstance(metadata, dict):
            raise _plan_error("EVIDENCE_METADATA_INVALID", name + "证据metadata必须是对象: " + key)
        states.append((key, status, ref, _freeze_mapping(metadata)))
    return tuple(states)


def _required_context_fields(contract: Mapping[str, object], scope_id: str) -> tuple[str, ...]:
    stages = contract["scope_bindings"][scope_id]["stage_bindings"]
    p00 = [item for item in stages if item.get("stage_id") == "P00"]
    if len(p00) != 1:
        raise _plan_error("P00_BINDING_INVALID", "FOUNDATION_EOD必须恰有一个P00绑定")
    required = p00[0].get("required_inputs")
    if not isinstance(required, list) or not required:
        raise _plan_error("P00_INPUTS_INCOMPLETE", "P00必需输入登记缺失")
    return tuple(required)


def _scope_denominators(
    contract: Mapping[str, object], scope_id: str, stage_ids: tuple[str, ...], context: Mapping[str, object]
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    scope = contract["scope_bindings"][scope_id]
    owner_roles = scope.get("owner_roles")
    field_registry = scope.get("field_registry")
    consumers = scope.get("required_consumers")
    if not isinstance(owner_roles, Mapping) or not owner_roles:
        raise _plan_error("SCOPE_OWNER_REGISTRY_INCOMPLETE", "scope缺少完整owner角色登记")
    owner_ids = tuple(sorted(owner_roles))
    for owner_id, artifact_ids in owner_roles.items():
        if not isinstance(owner_id, str) or not owner_id or not isinstance(artifact_ids, list) or not artifact_ids:
            raise _plan_error("SCOPE_OWNER_REGISTRY_INVALID", "scope owner角色登记结构非法")
        if len(artifact_ids) != len(set(artifact_ids)):
            raise _plan_error("SCOPE_OWNER_REGISTRY_INVALID", "scope owner角色重复登记产物")
        for artifact_id in artifact_ids:
            if not isinstance(artifact_id, str) or artifact_id not in contract["artifact_groups"]:
                raise _plan_error("SCOPE_OWNER_ARTIFACT_UNBOUND", "owner引用未登记artifact")
            if contract["artifact_groups"][artifact_id].get("producer") not in stage_ids:
                raise _plan_error("SCOPE_OWNER_ARTIFACT_UNBOUND", "owner产物producer未纳入scope闭包")
    if not isinstance(field_registry, list) or not field_registry:
        raise _plan_error("SCOPE_FIELD_REGISTRY_INCOMPLETE", "scope缺少完整字段登记")
    field_ids = []
    for field in field_registry:
        if not isinstance(field, Mapping):
            raise _plan_error("SCOPE_FIELD_REGISTRY_INVALID", "scope字段登记结构非法")
        field_id, producer_stage, field_consumers = field.get("field_id"), field.get("producer_stage"), field.get("consumers")
        if (not isinstance(field_id, str) or not field_id.strip() or field_id in field_ids
                or producer_stage not in stage_ids or not isinstance(field_consumers, list)
                or not field_consumers):
            raise _plan_error("SCOPE_FIELD_REGISTRY_INVALID", "scope字段身份、producer或consumer不完整")
        field_ids.append(field_id)
        if any(not isinstance(consumer, str) or not consumer.strip() for consumer in field_consumers):
            raise _plan_error("SCOPE_FIELD_REGISTRY_INVALID", "scope字段含非法consumer")
    if not isinstance(consumers, list) or not consumers or any(not isinstance(value, str) or not value for value in consumers):
        raise _plan_error("SCOPE_CONSUMER_REGISTRY_INCOMPLETE", "scope缺少required consumer登记")
    if len(consumers) != len(set(consumers)):
        raise _plan_error("SCOPE_CONSUMER_REGISTRY_INVALID", "scope required consumer重复")

    expected = {
        "required_owner_roles": set(owner_ids),
        "required_field_contracts": set(field_ids),
        "required_consumers": set(consumers),
    }
    for key, identities in expected.items():
        actual = context.get(key)
        if not isinstance(actual, list) or any(not isinstance(item, str) for item in actual):
            raise _plan_error("SCOPE_DENOMINATOR_MISSING", "冻结上下文必须以ID列表完整声明" + key)
        if len(actual) != len(set(actual)) or set(actual) != identities:
            raise _plan_error("SCOPE_DENOMINATOR_MISMATCH", key + "必须与正式scope目录完整一致")
    return owner_ids, tuple(sorted(field_ids)), tuple(consumers)


def _business_input_digest(
    request: DayPlanRequest, contract_sha256: str, excluded_keys: frozenset[str]
) -> str:
    if not isinstance(request.business_identity, Mapping):
        raise _plan_error("BUSINESS_IDENTITY_INVALID", "业务值/质量/provenance/参数/producer身份不能为空")
    revision_policy = request.contract.get("revision_policy")
    closure_policy = revision_policy.get("semantic_revision_closure") if isinstance(revision_policy, Mapping) else None
    digest_fields = closure_policy.get("business_input_digest_fields") if isinstance(closure_policy, Mapping) else None
    if (not isinstance(digest_fields, list) or not digest_fields
            or any(not isinstance(key, str) or not key for key in digest_fields)
            or len(digest_fields) != len(set(digest_fields))):
        raise _plan_error("BUSINESS_DIGEST_CONTRACT_INVALID", "机器合同缺少正式业务摘要字段登记")
    required = set(digest_fields)
    if not required.issubset(request.business_identity):
        raise _plan_error("BUSINESS_IDENTITY_INCOMPLETE", "业务摘要必须包含正式合同登记的values/quality/provenance/parameters/producer字段")
    projected_identity = {}
    for key in digest_fields:
        projected = _without_digest_exclusions(request.business_identity[key], excluded_keys)
        if projected is _OMIT_DIGEST_VALUE:
            raise _plan_error("BUSINESS_IDENTITY_INCOMPLETE", "业务摘要字段不能只包含被排除的运行元数据: " + key)
        projected_identity[key] = _json_identity(projected, "business_identity." + key)
    if any(value is None or value == "" or value == {} or value == [] for value in projected_identity.values()):
        raise _plan_error("BUSINESS_IDENTITY_INCOMPLETE", "业务摘要的值/质量/provenance/参数/producer身份不能为空")
    if not isinstance(request.semantic_revisions, Mapping):
        raise _plan_error("SEMANTIC_REVISION_INVALID", "语义revision必须显式提供映射")
    payload = {
        "contract_id": "STOCK-A-DAY-CHAIN",
        "contract_version": "R10",
        "contract_sha256": contract_sha256,
        "scope_id": request.scope_id,
        "scope_revision_binding_digest": request.scope_revision_contract.binding_digest,
        "calendar_id": request.calendar_id,
        "calendar_revision": request.calendar_revision,
        "semantic_revisions": dict(request.semantic_revisions),
        "business_identity": projected_identity,
    }
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return sha256(canonical).hexdigest()


def assemble_day_plan(request: DayPlanRequest) -> DayPlanResult:
    """从显式上游上下文投影不可变P00候选；不认证输入、不执行stage。"""
    if not isinstance(request, DayPlanRequest):
        raise _plan_error("REQUEST_INVALID", "必须提供DayPlanRequest")
    contract, contract_sha = _validated_contract(request)
    try:
        closure = _validated_scope_contract(request.scope_revision_contract)
    except ContractError as exc:
        raise _plan_error("SCOPE_REVISION_CONTRACT_INVALID", str(exc)) from exc
    if closure.scope_id != request.scope_id:
        raise _plan_error("SCOPE_REVISION_MISMATCH", "revision闭包与所选scope不一致")
    scope_bindings = contract.get("scope_bindings")
    scope_binding = scope_bindings.get(request.scope_id) if isinstance(scope_bindings, Mapping) else None
    if not isinstance(scope_binding, Mapping):
        raise _plan_error("SCOPE_BINDING_MISSING", "所选scope没有正式P00/kernel绑定")
    kernel_binding = scope_binding.get("day_plan_pure_kernel")
    if (not isinstance(kernel_binding, Mapping)
            or kernel_binding.get("symbol") != _DAY_PLAN_KERNEL_SYMBOL
            or kernel_binding.get("state") != "IMPLEMENTED"
            or kernel_binding.get("business_digest_fields_ref") != "revision_policy.business_input_digest_fields"):
        raise _plan_error("P00_KERNEL_BINDING_MISMATCH", "正式scope未绑定当前DayPlan纯内核")
    raw_exclusions = kernel_binding.get("digest_excludes")
    if (kernel_binding.get("digest_exclusion_application") != "RECURSIVE_OBJECT_KEY_PROJECTION_BOTH_DIGESTS"
            or not isinstance(raw_exclusions, list)
            or not raw_exclusions
            or any(not isinstance(key, str) or not key for key in raw_exclusions)
            or len(raw_exclusions) != len(set(raw_exclusions))):
        raise _plan_error("DIGEST_EXCLUSION_CONTRACT_INVALID", "机器合同摘要排除规则缺失或不受支持")
    digest_exclusions = frozenset(raw_exclusions)
    stage_order, kernel_roles = _validate_stage_dag(contract, request.scope_id)
    if type(request.target_session) is not date:
        raise _plan_error("TARGET_SESSION_INVALID", "目标会话必须是date")
    if not isinstance(request.frozen_context, Mapping) or not request.frozen_context:
        raise _plan_error("FROZEN_CONTEXT_MISSING", "DayPlan冻结上下文不能为空")
    projected_context = _without_digest_exclusions(request.frozen_context, digest_exclusions)
    if projected_context is _OMIT_DIGEST_VALUE:
        raise _plan_error("FROZEN_CONTEXT_MISSING", "冻结上下文只包含被排除的运行元数据")
    context = _json_identity(projected_context, "frozen_context")
    if not isinstance(context, dict):
        raise _plan_error("FROZEN_CONTEXT_INVALID", "DayPlan冻结上下文必须是对象")
    freeze_keys = contract.get("scope_policy", {}).get("freeze_before_run", [])
    missing_freeze = [key for key in freeze_keys if key not in context]
    if missing_freeze:
        raise _plan_error("FROZEN_CONTEXT_INCOMPLETE", "冻结上下文缺少: " + ",".join(missing_freeze))
    owner_ids, field_ids, consumer_ids = _scope_denominators(
        contract, request.scope_id, stage_order, context
    )
    history_policy = contract["scope_bindings"][request.scope_id].get("history_policy")
    if not isinstance(history_policy, Mapping) or not history_policy:
        raise _plan_error("SCOPE_HISTORY_POLICY_MISSING", "正式scope缺少历史依赖策略")
    if context.get("target_session") != request.target_session.isoformat():
        raise _plan_error("FROZEN_TARGET_MISMATCH", "冻结目标会话与计划目标不一致")
    input_states = _validate_evidence_map(
        request.validated_inputs, _required_context_fields(contract, request.scope_id), "P00必需输入",
        digest_exclusions,
    )
    capability_keys = kernel_binding.get("required_environment_capabilities")
    if (not isinstance(capability_keys, list) or not capability_keys
            or any(not isinstance(key, str) or not key for key in capability_keys)
            or len(capability_keys) != len(set(capability_keys))):
        raise _plan_error("P00_ENVIRONMENT_CONTRACT_INVALID", "正式DayPlan合同缺少必需环境能力登记")
    capability_states = _validate_evidence_map(
        request.environment_capabilities, tuple(capability_keys), "P00环境能力", digest_exclusions
    )
    all_states = input_states + capability_states
    blocked = tuple(key for key, status, ref, _metadata in all_states if status != "READY" or not ref)
    gate = EnvironmentGate(
        "READY" if not blocked else "BLOCKED",
        all_states,
        () if not blocked else tuple("ENVIRONMENT_NOT_READY:" + key for key in blocked),
        False,
    )
    if not isinstance(request.prior_context, Mapping):
        raise _plan_error("PRIOR_CONTEXT_INVALID", "前态上下文必须显式为映射")
    projected_prior_context = _without_digest_exclusions(request.prior_context, digest_exclusions)
    if projected_prior_context is _OMIT_DIGEST_VALUE:
        raise _plan_error("PRIOR_CONTEXT_INVALID", "前态上下文只包含被排除的运行元数据")
    prior_context = _json_identity(projected_prior_context, "prior_context")
    if not isinstance(prior_context, dict):
        raise _plan_error("PRIOR_CONTEXT_INVALID", "前态上下文必须是对象")
    if request.previous is not None and not isinstance(request.previous, AcceptedRunIdentity):
        raise _plan_error("PRIOR_CONTEXT_INVALID", "前态必须是AcceptedRunIdentity或None")
    if request.previous is None and prior_context.get("first_run") is not True:
        raise _plan_error("FIRST_RUN_CONTEXT_MISSING", "无前态时必须显式声明first_run=true")
    if request.previous is not None and not isinstance(prior_context.get("accepted_identity_ref"), str):
        raise _plan_error("PRIOR_CONTEXT_UNBOUND", "有前态时缺已接受身份引用")
    expected_generation = None if request.previous is None else request.previous.head_generation
    if context.get("expected_head_generation") != expected_generation:
        raise _plan_error("FROZEN_PARENT_GENERATION_MISMATCH", "冻结父generation与已接受前态不一致")

    digest = _business_input_digest(request, contract_sha, digest_exclusions)
    try:
        decision = classify_incremental_mode(
            target_session=request.target_session,
            official_sessions=request.official_sessions,
            calendar_id=request.calendar_id,
            calendar_revision=request.calendar_revision,
            semantic_revisions=request.semantic_revisions,
            scope_revision_contract=request.scope_revision_contract,
            business_input_digest=digest,
            previous=request.previous,
            historical_repair_required=request.historical_repair_required,
        )
    except ContractError as exc:
        raise _plan_error("INCREMENTAL_CLASSIFICATION_INVALID", str(exc)) from exc
    if gate.status != "READY":
        scheduled, session_range, state, allowed = (), (), "BLOCKED", False
    elif decision.mode == "NOOP":
        scheduled, session_range, state, allowed = (), (), "ALREADY_ACCEPTED", False
    elif decision.mode == "RANGE_REPAIR_REQUIRED":
        scheduled, session_range, state, allowed = (), (), "REPAIR_REQUIRED", False
    else:
        scheduled = stage_order
        session_range = decision.sessions_to_append if decision.mode == "APPEND" else (request.target_session,)
        state, allowed = "PLANNED", True
    reasons = decision.reason_codes + gate.reason_codes
    plan_identity = {
        "contract_sha256": contract_sha,
        "scope_id": request.scope_id,
        "input_digest": digest,
        "target_session": request.target_session.isoformat(),
        "frozen_context": context,
        "prior_context": prior_context,
        "environment_capabilities": all_states,
        "mode": decision.mode,
        "decision_state": state,
        "expected_parent_generation": decision.expected_parent_generation,
        "sessions_to_append": [session.isoformat() for session in decision.sessions_to_append],
        "session_range": [session.isoformat() for session in session_range],
        "required_stage_ids": stage_order,
        "scheduled_stage_ids": scheduled,
        "reason_codes": reasons,
    }
    projected_plan_identity = _without_digest_exclusions(plan_identity, digest_exclusions)
    plan_sha256 = sha256(json.dumps(
        projected_plan_identity, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")).hexdigest()
    plan = DayPlan(
        scope_id=request.scope_id,
        contract_id=str(contract["contract_id"]),
        contract_version=str(contract["version"]),
        contract_sha256=contract_sha,
        plan_sha256=plan_sha256,
        target_session=request.target_session,
        calendar_id=request.calendar_id,
        calendar_revision=request.calendar_revision,
        mode=decision.mode,
        decision_state=state,
        expected_parent_generation=decision.expected_parent_generation,
        sessions_to_append=decision.sessions_to_append,
        session_range=session_range,
        required_stage_ids=stage_order,
        scheduled_stage_ids=scheduled,
        stage_kernel_roles=kernel_roles,
        required_owner_roles=owner_ids,
        required_field_ids=field_ids,
        required_consumers=consumer_ids,
        history_policy=_freeze_mapping(history_policy),
        frozen_context=_freeze_mapping(context),
        input_digest=digest,
        reason_codes=reasons,
        candidate_ready=allowed,
    )
    return DayPlanResult(plan, gate, _freeze_mapping(prior_context))
