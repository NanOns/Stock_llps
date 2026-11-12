"""读取价格画像直接使用的字段、公式和窗口语义。"""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from decimal import Decimal
import json
import math
from pathlib import Path
from typing import Any


PROFILE_SEMANTIC_MODULES = (
    ("fields-source", "price-profile/fields-source.json"),
    ("fields-core", "price-profile/fields-core.json"),
    ("fields-profile", "price-profile/fields-profile.json"),
)
_MANIFEST_NAME = "price-profile.contract.json"
_FORMAT_ID = "STOCK-A-PRICE-PROFILE-SEMANTICS/R1"
_MAX_JSON_BYTES = 1_500_000


class PriceProfileContractError(ValueError):
    """价格画像字段合同读取失败，带稳定错误码。"""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class LoadedPriceProfileContract:
    """已核验字段依赖和算法版本的价格画像合同。"""

    document: dict[str, Any]
    module_paths: tuple[str, ...]


def canonical_contract_bytes(document: Mapping[str, Any]) -> bytes:
    """生成稳定UTF-8合同摘要输入。"""
    try:

        def normalize(value: Any) -> Any:
            if isinstance(value, Decimal):
                if not value.is_finite():
                    raise ValueError("合同数字必须有限")
                result = float(value)
                if not math.isfinite(result):
                    raise ValueError("合同数字超出JSON范围")
                return result
            if isinstance(value, dict):
                return {key: normalize(item) for key, item in value.items()}
            if isinstance(value, list):
                return [normalize(item) for item in value]
            return value

        return json.dumps(
            normalize(dict(document)),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise PriceProfileContractError(
            "PROFILE_CONTRACT_JSON_INVALID", "价格画像合同不是稳定的有限JSON结构"
        ) from error


def _read_json(root: Path, relative_path: str) -> dict[str, Any]:
    relative = Path(relative_path)
    if (
        relative.is_absolute()
        or not relative.parts
        or any(part in {".", ".."} for part in relative.parts)
        or relative.suffix.lower() != ".json"
    ):
        raise PriceProfileContractError(
            "PROFILE_CONTRACT_PATH_REJECTED", "合同路径必须是登记内的JSON相对路径"
        )
    candidate = root / relative
    try:
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root)
    except FileNotFoundError as error:
        raise PriceProfileContractError(
            "PROFILE_CONTRACT_FILE_MISSING", f"合同文件不存在：{relative_path}"
        ) from error
    except PermissionError as error:
        raise PriceProfileContractError(
            "PROFILE_CONTRACT_ACCESS_DENIED", f"系统拒绝读取合同文件：{relative_path}"
        ) from error
    except ValueError as error:
        raise PriceProfileContractError(
            "PROFILE_CONTRACT_PATH_REJECTED", f"合同路径离开配置根：{relative_path}"
        ) from error
    if not resolved.is_file():
        raise PriceProfileContractError(
            "PROFILE_CONTRACT_FILE_INVALID", f"合同路径不是普通文件：{relative_path}"
        )
    try:
        raw = resolved.read_bytes()
    except PermissionError as error:
        raise PriceProfileContractError(
            "PROFILE_CONTRACT_ACCESS_DENIED", f"系统拒绝读取合同文件：{relative_path}"
        ) from error
    except OSError as error:
        raise PriceProfileContractError(
            "PROFILE_CONTRACT_IO_ERROR", f"读取合同文件失败：{relative_path}"
        ) from error
    if len(raw) > _MAX_JSON_BYTES:
        raise PriceProfileContractError(
            "PROFILE_CONTRACT_FILE_TOO_LARGE", f"合同文件超过读取上限：{relative_path}"
        )

    def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise PriceProfileContractError(
                    "PROFILE_CONTRACT_DUPLICATE_KEY", f"合同JSON含重复键：{key}"
                )
            result[key] = value
        return result

    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=reject_duplicate_keys,
            parse_float=Decimal,
            parse_constant=lambda item: (_ for _ in ()).throw(
                ValueError(f"不接受非有限JSON数字：{item}")
            ),
        )
    except PriceProfileContractError:
        raise
    except (UnicodeError, json.JSONDecodeError, ValueError) as error:
        raise PriceProfileContractError(
            "PROFILE_CONTRACT_JSON_INVALID", f"合同不是合法UTF-8 JSON：{relative_path}"
        ) from error
    if not isinstance(value, dict):
        raise PriceProfileContractError(
            "PROFILE_CONTRACT_MODULE_INVALID", f"合同模块必须是对象：{relative_path}"
        )
    return value


def _validate_field_contract(document: dict[str, Any]) -> None:
    fields = document["field_registry"]
    rules = document["profile_expression_registry"]
    by_id: dict[str, dict[str, Any]] = {}
    for item in fields:
        field_id = item.get("field_id") if isinstance(item, dict) else None
        if not isinstance(field_id, str) or not field_id or field_id in by_id:
            raise PriceProfileContractError(
                "PROFILE_CONTRACT_FIELD_INVALID", "字段标识缺失或重复"
            )
        by_id[field_id] = item
    if sum(field_id.startswith("core.") for field_id in by_id) != 43:
        raise PriceProfileContractError(
            "PROFILE_CONTRACT_CORE_SET_INVALID", "价格画像合同必须保留Core43字段"
        )
    if len(rules) != 11:
        raise PriceProfileContractError(
            "PROFILE_CONTRACT_RULE_SET_INVALID", "价格画像合同必须保留Profile11规则"
        )
    for field_id, item in by_id.items():
        inputs = item.get("input_fields")
        if not isinstance(inputs, list) or any(value not in by_id for value in inputs):
            raise PriceProfileContractError(
                "PROFILE_CONTRACT_INPUT_MISSING", f"字段输入未登记：{field_id}"
            )
    for expression_id, rule in rules.items():
        if not isinstance(expression_id, str) or not isinstance(rule, dict):
            raise PriceProfileContractError(
                "PROFILE_CONTRACT_RULE_INVALID", "Profile规则标识或内容非法"
            )
        if rule.get("field_id") not in by_id:
            raise PriceProfileContractError(
                "PROFILE_CONTRACT_RULE_FIELD_MISSING",
                f"Profile规则没有对应输出字段：{expression_id}",
            )
        inputs = rule.get("input_fields")
        if not isinstance(inputs, list) or any(value not in by_id for value in inputs):
            raise PriceProfileContractError(
                "PROFILE_CONTRACT_RULE_INPUT_MISSING",
                f"Profile规则输入未登记：{expression_id}",
            )


def load_price_profile_contract(
    configuration_root: Path | str,
) -> LoadedPriceProfileContract:
    """按固定清单读取画像字段与公式，不读取执行图或发布控制内容。"""
    try:
        requested_root = Path(configuration_root)
        if not requested_root.is_absolute():
            raise PriceProfileContractError(
                "PROFILE_CONTRACT_PATH_REJECTED", "正式配置根必须是绝对路径"
            )
        config_root = requested_root.resolve(strict=True)
        if not config_root.is_dir():
            raise PriceProfileContractError(
                "PROFILE_CONTRACT_PATH_REJECTED", "正式配置根不是目录"
            )
        contracts_root = (config_root / "contracts").resolve(strict=True)
        contracts_root.relative_to(config_root)
    except PriceProfileContractError:
        raise
    except (FileNotFoundError, PermissionError, OSError, ValueError) as error:
        raise PriceProfileContractError(
            "PROFILE_CONTRACT_PATH_REJECTED", "无法核验正式合同配置根"
        ) from error

    manifest = _read_json(contracts_root, _MANIFEST_NAME)
    if set(manifest) != {
        "format",
        "contract_id",
        "version",
        "modules",
    }:
        raise PriceProfileContractError(
            "PROFILE_CONTRACT_MANIFEST_INVALID", "画像合同清单字段不完整或多余"
        )
    if (
        manifest.get("format") != _FORMAT_ID
        or manifest.get("contract_id") != "STOCK-A-PRICE-PROFILE-SEMANTICS"
        or manifest.get("version") != "R1"
    ):
        raise PriceProfileContractError(
            "PROFILE_CONTRACT_IDENTITY_MISMATCH", "画像合同身份或版本不匹配"
        )
    modules = manifest.get("modules")
    if not isinstance(modules, list) or len(modules) != len(PROFILE_SEMANTIC_MODULES):
        raise PriceProfileContractError(
            "PROFILE_CONTRACT_MODULE_SET_INVALID", "画像合同模块清单不完整"
        )
    module_documents: dict[str, dict[str, Any]] = {}
    loaded_paths: list[str] = []
    for expected, item in zip(PROFILE_SEMANTIC_MODULES, modules, strict=True):
        module_id, relative_path = expected
        if not isinstance(item, dict) or set(item) != {"id", "path"}:
            raise PriceProfileContractError(
                "PROFILE_CONTRACT_MODULE_INVALID", "画像合同模块登记字段无效"
            )
        if item.get("id") != module_id or item.get("path") != relative_path:
            raise PriceProfileContractError(
                "PROFILE_CONTRACT_MODULE_SET_INVALID", "画像合同模块顺序或路径不匹配"
            )
        module = _read_json(contracts_root, relative_path)
        module_documents[module_id] = module
        loaded_paths.append(relative_path)

    assembled_fields: list[tuple[int, dict[str, Any]]] = []
    for module_id, _relative_path in PROFILE_SEMANTIC_MODULES:
        module = module_documents[module_id]
        allowed = {"fields", "defaults"}
        if module_id == "fields-profile":
            allowed.add("scope_fields")
        if set(module) != allowed or not isinstance(module.get("fields"), list):
            raise PriceProfileContractError(
                "PROFILE_CONTRACT_MODULE_INVALID", f"模块内容不匹配：{module_id}"
            )
        defaults = module.get("defaults", {})
        if not isinstance(defaults, dict):
            raise PriceProfileContractError(
                "PROFILE_CONTRACT_DEFAULTS_INVALID", f"模块默认值非法：{module_id}"
            )
        for row in module["fields"]:
            if (
                not isinstance(row, dict)
                or set(row) != {"order", "definition"}
                or type(row.get("order")) is not int
                or not isinstance(row.get("definition"), dict)
            ):
                raise PriceProfileContractError(
                    "PROFILE_CONTRACT_FIELD_INVALID", f"模块字段记录非法：{module_id}"
                )
            assembled_fields.append(
                (row["order"], {**deepcopy(defaults), **deepcopy(row["definition"])})
            )
    orders = [order for order, _field in assembled_fields]
    if len(orders) != len(set(orders)):
        raise PriceProfileContractError(
            "PROFILE_CONTRACT_FIELD_ORDER_INVALID", "字段顺序值必须唯一"
        )
    profile_scope = module_documents["fields-profile"].get("scope_fields")
    if not isinstance(profile_scope, dict) or set(profile_scope) != {
        "profile_expression_registry",
        "profile_expression_policy",
    }:
        raise PriceProfileContractError(
            "PROFILE_CONTRACT_SCOPE_INVALID", "Profile公式登记或质量策略缺失"
        )
    document = {
        "contract_id": "STOCK-A-PRICE-PROFILE-SEMANTICS",
        "version": "R1",
        "field_registry": [
            field for _order, field in sorted(assembled_fields, key=lambda row: row[0])
        ],
        **deepcopy(profile_scope),
    }
    _validate_field_contract(document)
    return LoadedPriceProfileContract(
        document=document,
        module_paths=tuple(loaded_paths),
    )
