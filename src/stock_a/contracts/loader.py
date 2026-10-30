"""day-chain正式机器合同的固定清单装配与校验。"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from hashlib import sha256
import json
import math
from pathlib import Path
from typing import Any


_MANIFEST_NAME = "contracts/day-chain.contract.json"
_FORMAT_ID = "STOCK-A-DAY-CHAIN-MODULES/R1"
_MAX_JSON_BYTES = 1_500_000
DAY_CHAIN_MODULES = (
    ("pipeline", "day-chain/pipeline.json"),
    ("fields-source", "day-chain/fields-source.json"),
    ("fields-core", "day-chain/fields-core.json"),
    ("fields-profile", "day-chain/fields-profile.json"),
    ("fields-control", "day-chain/fields-control.json"),
    ("research", "day-chain/research.json"),
    ("service", "day-chain/service.json"),
    ("policies", "day-chain/policies.json"),
)
_MODULES = DAY_CHAIN_MODULES
_ROOT_KEYS = {
    "pipeline": frozenset(
        {
            "contract_id",
            "version",
            "contract_kind",
            "requires_implementation_binding",
            "scope_policy",
            "stages",
            "artifact_groups",
            "algorithms",
            "time_roles",
            "forbidden_same_session_edges",
        }
    ),
    "research": frozenset(
        {
            "forward_policy",
            "research_execution_contracts",
            "research_field_bindings",
            "domain_decision_contracts",
        }
    ),
    "service": frozenset({"page_consumers", "read_policy", "service_product_contract"}),
    "policies": frozenset(
        {
            "language",
            "normative_documents",
            "external_inputs",
            "owner_policy",
            "identity_policy",
            "acceptance_policy",
            "completion_policy",
            "revision_policy",
            "storage_policy",
            "document_registry",
            "external_file_policy",
            "field_binding_policy",
            "algorithm_design_gates",
            "deployment_identity_policy",
            "scheduling_policy",
            "source_evidence_policy",
            "capacity_policy",
            "source_seal_validation",
        }
    ),
}
_SCOPE_KEYS = {
    "pipeline": frozenset(
        {
            "contract",
            "execution_mode",
            "stage_bindings",
            "within_stage_field_toposort_required",
            "history_policy",
            "source_seal_validation_ref",
            "day_plan_pure_kernel",
        }
    ),
    "fields-profile": frozenset(
        {"profile_expression_registry", "profile_expression_policy"}
    ),
    "fields-control": frozenset(
        {
            "owner_roles",
            "disabled_capabilities",
            "disabled_result_state",
            "control_dispatch_plan_source",
            "completion",
            "required_consumers",
        }
    ),
    "policies": frozenset(
        {
            "implementation_binding_required",
            "accepted_readback_identity_assertion",
            "context_input_registry",
            "context_identity_policy",
            "allowed_environments",
        }
    ),
}
_CONTENT_KEYS = {
    "pipeline": frozenset({"top_level", "scope_fields"}),
    "fields-source": frozenset({"fields"}),
    "fields-core": frozenset({"fields"}),
    "fields-profile": frozenset({"fields", "scope_fields"}),
    "fields-control": frozenset({"fields", "scope_fields"}),
    "research": frozenset({"top_level"}),
    "service": frozenset({"top_level"}),
    "policies": frozenset({"top_level", "scope_fields"}),
}
_FIELD_PREFIXES = {
    "fields-source": frozenset(
        {"identity", "raw", "adjusted", "coordinate", "period", "bao", "source_set"}
    ),
    "fields-core": frozenset({"core"}),
    "fields-profile": frozenset({"cross", "profile"}),
    "fields-control": frozenset(
        {
            "plan",
            "capture",
            "owner",
            "manifest",
            "read_index",
            "publication",
            "candidate",
            "head",
            "receipt",
            "dispatch",
            "readback",
            "completion",
        }
    ),
}
_PROGRESS_KEYS = frozenset(
    {
        "implementation_state",
        "implementation_batches",
        "producer_source_sha256",
        "source_sha256",
        "commit",
        "tree",
        "production_binding",
        "pipeline_state",
        "consumer_binding_state",
        "schema_binding_state",
        "canonical_typed_producer_state",
        "pg_read_write_state",
        "pure_kernel_binding_summary",
        "runtime_implementation_present",
        "runtime_pipeline_implemented",
        "production_entry_state",
        "production_entry_block_reason_zh",
        "pure_kernel_implemented",
        "pure_validation_kernel_present",
        "canonical_typed_producer_connected",
        "pg_read_write_attempted",
        "parameter_binding_state",
    }
)
_LOAD_PROOF = object()


class DayChainContractError(ValueError):
    """合同读取或装配失败，带稳定的机器错误码。"""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class LoadedDayChainContract(Mapping[str, Any]):
    """由固定模块清单装配并核验摘要的只读映射。"""

    document: dict[str, Any]
    assembled_sha256: str
    declared_sha256: str
    module_paths: tuple[str, ...]
    _proof: object = field(repr=False, compare=False)

    def __getitem__(self, key: str) -> Any:
        return self.document[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.document)

    def __len__(self) -> int:
        return len(self.document)

    @property
    def loader_verified(self) -> bool:
        return self._proof is _LOAD_PROOF


def canonical_contract_bytes(document: Mapping[str, Any]) -> bytes:
    """返回P00使用的稳定UTF-8规范化JSON字节。"""
    try:

        def json_numbers(value: Any) -> Any:
            if isinstance(value, Decimal):
                if not value.is_finite():
                    raise ValueError("合同数字必须有限")
                normalized = float(value)
                if not math.isfinite(normalized):
                    raise ValueError("合同数字超出规范JSON范围")
                return normalized
            if isinstance(value, dict):
                return {key: json_numbers(item) for key, item in value.items()}
            if isinstance(value, list):
                return [json_numbers(item) for item in value]
            return value

        return json.dumps(
            json_numbers(dict(document)),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise DayChainContractError(
            "DAY_CHAIN_JSON_INVALID", "装配合同不是有限且稳定的JSON结构"
        ) from error


def _duplicate_rejecting_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise DayChainContractError(
                "DAY_CHAIN_DUPLICATE_JSON_KEY", f"JSON对象含重复键：{key}"
            )
        result[key] = value
    return result


def _parse_float(value: str) -> Decimal:
    result = Decimal(value)
    if not result.is_finite():
        raise DayChainContractError(
            "DAY_CHAIN_NONFINITE_NUMBER", "JSON数字必须是有限值"
        )
    return result


def _parse_constant(value: str) -> Any:
    raise DayChainContractError(
        "DAY_CHAIN_NONFINITE_NUMBER", f"JSON不接受非有限常量：{value}"
    )


def _read_json(root: Path, relative_path: str) -> Any:
    relative = Path(relative_path)
    if (
        relative.is_absolute()
        or not relative.parts
        or any(part in {".", ".."} for part in relative.parts)
    ):
        raise DayChainContractError(
            "DAY_CHAIN_PATH_REJECTED", "合同模块路径必须是清单内的相对路径"
        )
    if relative.suffix.lower() != ".json":
        raise DayChainContractError("DAY_CHAIN_PATH_REJECTED", "合同模块只接受JSON文件")
    candidate = root / relative
    try:
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root)
    except FileNotFoundError as error:
        raise DayChainContractError(
            "DAY_CHAIN_FILE_MISSING", f"合同模块文件不存在：{relative_path}"
        ) from error
    except PermissionError as error:
        raise DayChainContractError(
            "DAY_CHAIN_ACCESS_DENIED", f"操作系统拒绝读取合同模块：{relative_path}"
        ) from error
    except ValueError as error:
        raise DayChainContractError(
            "DAY_CHAIN_PATH_REJECTED", f"合同模块最终路径离开配置根：{relative_path}"
        ) from error
    except OSError as error:
        raise DayChainContractError(
            "DAY_CHAIN_IO_ERROR", f"核验合同模块路径失败：{relative_path}"
        ) from error
    if not resolved.is_file():
        raise DayChainContractError(
            "DAY_CHAIN_FILE_INVALID", f"合同模块不是普通文件：{relative_path}"
        )
    try:
        raw = resolved.read_bytes()
    except PermissionError as error:
        raise DayChainContractError(
            "DAY_CHAIN_ACCESS_DENIED", f"操作系统拒绝读取合同模块：{relative_path}"
        ) from error
    except OSError as error:
        raise DayChainContractError(
            "DAY_CHAIN_IO_ERROR", f"读取合同模块失败：{relative_path}"
        ) from error
    if len(raw) > _MAX_JSON_BYTES:
        raise DayChainContractError(
            "DAY_CHAIN_FILE_TOO_LARGE", f"合同模块超过有界读取上限：{relative_path}"
        )
    try:
        return json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_duplicate_rejecting_object,
            parse_float=_parse_float,
            parse_constant=_parse_constant,
        )
    except DayChainContractError:
        raise
    except (UnicodeError, json.JSONDecodeError, ValueError) as error:
        raise DayChainContractError(
            "DAY_CHAIN_JSON_INVALID", f"合同模块不是合法UTF-8 JSON：{relative_path}"
        ) from error


def _require_mapping(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise DayChainContractError(
            "DAY_CHAIN_MODULE_INVALID", f"合同模块{label}必须是JSON对象"
        )
    return value


def _validate_progress_keys(document: Any) -> None:
    def visit(value: Any) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                if key in _PROGRESS_KEYS:
                    raise DayChainContractError(
                        "DAY_CHAIN_PROCESS_DATA_REJECTED",
                        f"正式合同不能包含研发状态或历史证明字段：{key}",
                    )
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(document)


def _validate_business_structure(document: dict[str, Any]) -> None:
    if (
        document.get("contract_id") != "STOCK-A-DAY-CHAIN"
        or document.get("version") != "R10"
    ):
        raise DayChainContractError(
            "DAY_CHAIN_IDENTITY_MISMATCH", "day-chain身份或版本不匹配"
        )
    stages = document.get("stages")
    artifacts = document.get("artifact_groups")
    scope_bindings = document.get("scope_bindings")
    if not isinstance(stages, list) or len(stages) != 26:
        raise DayChainContractError(
            "DAY_CHAIN_STAGE_SET_INVALID", "正式合同必须保留26个stage"
        )
    stage_ids = [
        stage.get("id") if isinstance(stage, dict) else None for stage in stages
    ]
    if any(not isinstance(item, str) or not item for item in stage_ids) or len(
        stage_ids
    ) != len(set(stage_ids)):
        raise DayChainContractError(
            "DAY_CHAIN_DUPLICATE_STAGE_ID", "stage ID缺失或重复"
        )
    if not isinstance(artifacts, dict) or len(artifacts) != 66:
        raise DayChainContractError(
            "DAY_CHAIN_ARTIFACT_SET_INVALID", "正式合同必须保留66组产物"
        )
    for artifact_id, artifact in artifacts.items():
        if not isinstance(artifact, dict) or artifact.get("producer") not in stage_ids:
            raise DayChainContractError(
                "DAY_CHAIN_ARTIFACT_PRODUCER_INVALID",
                f"产物producer未登记：{artifact_id}",
            )
    scopes = scope_bindings if isinstance(scope_bindings, dict) else {}
    scope = scopes.get("FOUNDATION_EOD")
    if not isinstance(scope, dict):
        raise DayChainContractError(
            "DAY_CHAIN_SCOPE_MISSING", "缺少FOUNDATION_EOD scope"
        )
    fields = scope.get("field_registry")
    if not isinstance(fields, list) or len(fields) != 102:
        raise DayChainContractError(
            "DAY_CHAIN_FIELD_SET_INVALID", "FOUNDATION_EOD必须保留102个字段"
        )
    field_ids: list[str] = []
    for item in fields:
        if not isinstance(item, dict):
            raise DayChainContractError("DAY_CHAIN_FIELD_INVALID", "字段登记必须是对象")
        field_id = item.get("field_id")
        if not isinstance(field_id, str) or not field_id:
            raise DayChainContractError(
                "DAY_CHAIN_FIELD_INVALID", "字段登记缺少field_id"
            )
        field_ids.append(field_id)
    if len(field_ids) != len(set(field_ids)):
        raise DayChainContractError("DAY_CHAIN_DUPLICATE_FIELD_ID", "字段ID重复")
    known_fields = set(field_ids)
    known_stages = set(stage_ids)
    for item in fields:
        field_id = item["field_id"]
        producer = item.get("producer_stage")
        output = item.get("output_artifact")
        if producer not in known_stages:
            raise DayChainContractError(
                "DAY_CHAIN_FIELD_STAGE_MISSING", f"字段producer stage未登记：{field_id}"
            )
        artifact = artifacts.get(output)
        if not isinstance(artifact, dict) or artifact.get("producer") != producer:
            raise DayChainContractError(
                "DAY_CHAIN_FIELD_ARTIFACT_MISSING", f"字段输出产物未绑定：{field_id}"
            )
        inputs = item.get("input_fields")
        if not isinstance(inputs, list) or any(
            value not in known_fields for value in inputs
        ):
            raise DayChainContractError(
                "DAY_CHAIN_FIELD_INPUT_MISSING", f"字段依赖未登记：{field_id}"
            )
    expression_registry = scope.get("profile_expression_registry")
    if not isinstance(expression_registry, dict) or len(expression_registry) != 11:
        raise DayChainContractError(
            "DAY_CHAIN_PROFILE_RULE_SET_INVALID", "Profile必须保留11条表达式规则"
        )
    for expression_id, expression in expression_registry.items():
        if (
            not isinstance(expression, dict)
            or expression.get("field_id") not in known_fields
        ):
            raise DayChainContractError(
                "DAY_CHAIN_PROFILE_RULE_INVALID",
                f"Profile表达式缺少字段绑定：{expression_id}",
            )
        inputs = expression.get("input_fields")
        if not isinstance(inputs, list) or any(
            value not in known_fields for value in inputs
        ):
            raise DayChainContractError(
                "DAY_CHAIN_PROFILE_INPUT_MISSING",
                f"Profile表达式输入未登记：{expression_id}",
            )


def load_day_chain_contract(configuration_root: Path | str) -> LoadedDayChainContract:
    """按唯一清单装配合同；绝不扫描目录或读取未登记文件。"""
    try:
        requested_root = Path(configuration_root)
        if not requested_root.is_absolute():
            raise DayChainContractError(
                "DAY_CHAIN_PATH_REJECTED", "正式配置根必须是绝对路径"
            )
        config_root = requested_root.resolve(strict=True)
        if not config_root.is_dir():
            raise DayChainContractError("DAY_CHAIN_PATH_REJECTED", "正式配置根不是目录")
        contracts_root = (config_root / "contracts").resolve(strict=True)
        contracts_root.relative_to(config_root)
    except DayChainContractError:
        raise
    except FileNotFoundError as error:
        raise DayChainContractError(
            "DAY_CHAIN_FILE_MISSING", "配置根或contracts目录不存在"
        ) from error
    except PermissionError as error:
        raise DayChainContractError(
            "DAY_CHAIN_ACCESS_DENIED", "操作系统拒绝核验合同配置目录"
        ) from error
    except ValueError as error:
        raise DayChainContractError(
            "DAY_CHAIN_PATH_REJECTED", "contracts目录最终路径离开配置根"
        ) from error
    except OSError as error:
        raise DayChainContractError(
            "DAY_CHAIN_IO_ERROR", "核验合同配置目录失败"
        ) from error
    if not contracts_root.is_dir():
        raise DayChainContractError("DAY_CHAIN_PATH_REJECTED", "contracts路径不是目录")

    manifest = _require_mapping(
        _read_json(contracts_root, "day-chain.contract.json"), "入口清单"
    )
    if set(manifest) != {
        "format",
        "contract_id",
        "version",
        "modules",
        "assembled_sha256",
    }:
        raise DayChainContractError(
            "DAY_CHAIN_MANIFEST_INVALID", "入口清单字段必须精确匹配正式模块格式"
        )
    if (
        manifest.get("format") != _FORMAT_ID
        or manifest.get("contract_id") != "STOCK-A-DAY-CHAIN"
        or manifest.get("version") != "R10"
    ):
        raise DayChainContractError(
            "DAY_CHAIN_MANIFEST_INVALID", "入口清单格式、合同身份或版本不匹配"
        )
    expected_modules = [{"module": module, "path": path} for module, path in _MODULES]
    if manifest.get("modules") != expected_modules:
        raise DayChainContractError(
            "DAY_CHAIN_MODULE_MANIFEST_MISMATCH",
            "入口模块必须等于代码与配置登记的固定清单",
        )
    declared_sha = manifest.get("assembled_sha256")
    if (
        not isinstance(declared_sha, str)
        or len(declared_sha) != 64
        or any(c not in "0123456789abcdef" for c in declared_sha)
    ):
        raise DayChainContractError(
            "DAY_CHAIN_MANIFEST_INVALID", "入口清单缺少有效assembled_sha256"
        )

    top_level: dict[str, Any] = {}
    scope_fields: dict[str, Any] = {}
    field_rows: list[tuple[int, str, dict[str, Any]]] = []
    loaded_paths: list[str] = []
    for module_id, relative_path in _MODULES:
        fragment = _require_mapping(
            _read_json(contracts_root, relative_path), module_id
        )
        if set(fragment) != _CONTENT_KEYS[module_id]:
            raise DayChainContractError(
                "DAY_CHAIN_MODULE_INVALID", f"模块{module_id}内容区不符合固定格式"
            )
        roots = fragment.get("top_level", {})
        if module_id in _ROOT_KEYS:
            roots = _require_mapping(roots, module_id + ".top_level")
            if set(roots) != _ROOT_KEYS[module_id]:
                raise DayChainContractError(
                    "DAY_CHAIN_MODULE_INVALID", f"模块{module_id}的顶层合同职责不匹配"
                )
            overlap = set(top_level).intersection(roots)
            if overlap:
                raise DayChainContractError(
                    "DAY_CHAIN_DUPLICATE_SECTION",
                    "重复合同节：" + ", ".join(sorted(overlap)),
                )
            top_level.update(roots)
        elif roots:
            raise DayChainContractError(
                "DAY_CHAIN_MODULE_INVALID", f"字段模块{module_id}不能含顶层合同节"
            )
        module_scope = fragment.get("scope_fields", {})
        if module_scope:
            module_scope = _require_mapping(module_scope, module_id + ".scope_fields")
            if set(module_scope) != _SCOPE_KEYS.get(module_id, frozenset()):
                raise DayChainContractError(
                    "DAY_CHAIN_MODULE_INVALID", f"模块{module_id}的scope职责不匹配"
                )
            overlap = set(scope_fields).intersection(module_scope)
            if overlap:
                raise DayChainContractError(
                    "DAY_CHAIN_DUPLICATE_SECTION",
                    "重复scope合同节：" + ", ".join(sorted(overlap)),
                )
            scope_fields.update(module_scope)
        elif module_id in _SCOPE_KEYS and _SCOPE_KEYS[module_id]:
            raise DayChainContractError(
                "DAY_CHAIN_MODULE_INVALID", f"模块{module_id}缺少scope合同节"
            )

        if "fields" in fragment:
            rows = fragment["fields"]
            if not isinstance(rows, list):
                raise DayChainContractError(
                    "DAY_CHAIN_MODULE_INVALID", f"模块{module_id}的fields必须是数组"
                )
            allowed_prefixes = _FIELD_PREFIXES[module_id]
            for row in rows:
                if not isinstance(row, dict) or set(row) != {"order", "definition"}:
                    raise DayChainContractError(
                        "DAY_CHAIN_FIELD_INVALID", f"模块{module_id}含非法字段封套"
                    )
                order = row["order"]
                definition = row["definition"]
                if (
                    type(order) is not int
                    or order < 0
                    or not isinstance(definition, dict)
                ):
                    raise DayChainContractError(
                        "DAY_CHAIN_FIELD_INVALID",
                        f"模块{module_id}含非法字段顺序或定义",
                    )
                field_id = definition.get("field_id")
                if (
                    not isinstance(field_id, str)
                    or field_id.split(".", 1)[0] not in allowed_prefixes
                ):
                    raise DayChainContractError(
                        "DAY_CHAIN_FIELD_INVALID",
                        f"字段不属于模块{module_id}：{field_id}",
                    )
                field_rows.append((order, module_id, definition))
        loaded_paths.append(relative_path)

    expected_root_keys = set().union(*_ROOT_KEYS.values())
    if set(top_level) != expected_root_keys:
        raise DayChainContractError(
            "DAY_CHAIN_ROOT_INCOMPLETE", "固定合同顶层节集合不完整"
        )
    expected_scope_keys = set().union(*_SCOPE_KEYS.values())
    if set(scope_fields) != expected_scope_keys:
        raise DayChainContractError(
            "DAY_CHAIN_SCOPE_INCOMPLETE", "固定FOUNDATION_EOD scope节集合不完整"
        )
    orders = [order for order, _, _ in field_rows]
    if len(field_rows) != 102 or sorted(orders) != list(range(102)):
        raise DayChainContractError(
            "DAY_CHAIN_FIELD_ORDER_INVALID", "102个字段必须按原顺序唯一覆盖0至101"
        )
    scope_fields["field_registry"] = [
        definition for _, _, definition in sorted(field_rows, key=lambda row: row[0])
    ]
    top_level["scope_bindings"] = {"FOUNDATION_EOD": scope_fields}
    document = top_level
    _validate_progress_keys(document)
    _validate_business_structure(document)
    actual_sha = sha256(canonical_contract_bytes(document)).hexdigest()
    if actual_sha != declared_sha:
        raise DayChainContractError(
            "DAY_CHAIN_DIGEST_MISMATCH",
            f"装配合同摘要与入口清单不一致：声明{declared_sha}，实算{actual_sha}",
        )
    return LoadedDayChainContract(
        document=document,
        assembled_sha256=actual_sha,
        declared_sha256=declared_sha,
        module_paths=tuple(loaded_paths),
        _proof=_LOAD_PROOF,
    )
