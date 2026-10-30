"""唯一运行配置槽位的路径核验、读取与合同校验。"""

from __future__ import annotations

import json
from pathlib import Path
import re
import uuid
from typing import Any

from stock_a.contracts import DAY_CHAIN_MODULES
from stock_a.sources.tdx.vipdata import VipdataError, VipdataSourceConfig

_WORKSPACE = Path("G:/codex work/Stock A")

_CONFIG_ROOT = _WORKSPACE / "config"

_CONFIG_FILE = _CONFIG_ROOT / "runtime.defaults.json"

_CONFIG_CONTRACT = "STOCK-A-RUNTIME-DEFAULTS/R9"

_EXIT_OK = 0

_EXIT_ARGUMENT = 2

_EXIT_UNAVAILABLE = 3

_EXIT_PERMISSION = 4

_EXIT_ENGINEERING = 5


class DiagnosticError(Exception):
    """可向用户显示的配置或能力不可用错误。"""

    def __init__(self, code: str, message: str, exit_code: int = _EXIT_UNAVAILABLE):
        super().__init__(message)
        self.code = code
        self.exit_code = exit_code


def _inside(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def load_runtime_config(
    explicit_path: str | None = None,
) -> tuple[dict[str, Any], Path]:
    if explicit_path is not None:
        try:
            requested = Path(explicit_path)
            approved = _CONFIG_FILE.resolve(strict=True)
            if not requested.is_absolute() or requested.resolve() != approved:
                raise DiagnosticError(
                    "CONFIG_PATH_REJECTED",
                    "仅允许读取唯一登记配置runtime.defaults.json",
                )
        except FileNotFoundError as error:
            raise DiagnosticError("CONFIG_MISSING", "唯一正式运行配置不存在") from error
        except PermissionError as error:
            raise DiagnosticError(
                "CONFIG_PERMISSION_DENIED",
                "操作系统拒绝核验正式配置路径",
                _EXIT_PERMISSION,
            ) from error
        except OSError as error:
            raise DiagnosticError(
                "CONFIG_IO_ERROR",
                "核验显式配置路径时发生工程I/O错误",
                _EXIT_ENGINEERING,
            ) from error
    try:
        config_root = _CONFIG_ROOT.resolve(strict=True)
        config_path = _CONFIG_FILE.resolve(strict=True)
        if config_root != _CONFIG_ROOT or not _inside(config_path, config_root):
            raise DiagnosticError(
                "CONFIG_PATH_REJECTED", "正式配置根或配置文件经链接离开唯一配置槽位"
            )
        if not config_path.is_file():
            raise DiagnosticError("CONFIG_MISSING", "唯一正式运行配置不是普通文件")
    except FileNotFoundError as error:
        raise DiagnosticError(
            "CONFIG_MISSING", "唯一正式运行配置或配置根不存在"
        ) from error
    except PermissionError as error:
        raise DiagnosticError(
            "CONFIG_PERMISSION_DENIED",
            "操作系统拒绝核验正式配置根及最终路径",
            _EXIT_PERMISSION,
        ) from error
    except OSError as error:
        raise DiagnosticError(
            "CONFIG_IO_ERROR",
            "核验正式配置根及最终路径时发生工程I/O错误",
            _EXIT_ENGINEERING,
        ) from error
    try:
        with config_path.open("r", encoding="utf-8") as stream:
            config = json.load(
                stream,
                parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
            )
    except PermissionError as error:
        raise DiagnosticError(
            "CONFIG_PERMISSION_DENIED", "操作系统拒绝读取正式配置", _EXIT_PERMISSION
        ) from error
    except OSError as error:
        raise DiagnosticError(
            "CONFIG_IO_ERROR", "读取正式配置时发生工程I/O错误", _EXIT_ENGINEERING
        ) from error
    except (UnicodeError, json.JSONDecodeError, ValueError) as error:
        raise DiagnosticError(
            "CONFIG_INVALID", "正式配置无法按UTF-8 JSON读取"
        ) from error
    _validate_config(config)
    return config, config_path


def _require_mapping(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise DiagnosticError("CONFIG_INVALID", f"配置字段{label}必须是对象")
    return value


def _validate_config(config: Any) -> None:
    root = _require_mapping(config, "根")
    if root.get("contract") != _CONFIG_CONTRACT:
        raise DiagnosticError("CONFIG_CONTRACT_MISMATCH", "正式配置合同版本不匹配")
    paths = _require_mapping(root.get("paths"), "paths")
    for key in ("workspace_root", "configuration_root", "temporary_root"):
        value = paths.get(key)
        if not isinstance(value, str) or not Path(value).is_absolute():
            raise DiagnosticError(
                "CONFIG_INVALID", f"配置字段paths.{key}必须是绝对路径"
            )
    if Path(paths["workspace_root"]).resolve() != _WORKSPACE.resolve():
        raise DiagnosticError("CONFIG_INVALID", "配置workspace_root与唯一源码根不一致")
    if Path(paths["configuration_root"]).resolve() != _CONFIG_ROOT.resolve():
        raise DiagnosticError(
            "CONFIG_INVALID", "配置configuration_root与唯一配置槽位不一致"
        )
    policy = _require_mapping(root.get("configuration_policy"), "configuration_policy")
    if (
        policy.get("contract") != "STOCK-A-CONFIGURATION-POLICY/R1"
        or policy.get("root_only") is not True
        or policy.get("recursive_autoload") is not False
        or policy.get("unknown_files") != "REJECT"
    ):
        raise DiagnosticError("CONFIG_INVALID", "配置目录安全策略缺失或类型错误")
    private_policy = _require_mapping(
        policy.get("private_files"), "configuration_policy.private_files"
    )
    expected_private_files = [
        {
            "path": "private/postgresql.credentials.json",
            "schema": "STOCK-A-PG-CREDENTIALS/R1",
            "allowed_keys": [
                "schema",
                "host",
                "port",
                "database",
                "username",
                "password",
            ],
            "consumer": "stock_a.storage.postgres.identity.inspect_postgres（仅pg-identity）",
            "lifecycle": "STOCK_A_PG_DSN优先；仅在其缺失时由pg-identity进程内存读取；不得输出、摘要、记录、复制或传入子进程参数",
        }
    ]
    if (
        private_policy.get("root") != "private"
        or private_policy.get("git_allowed") is not False
        or private_policy.get("allowed_role")
        != "SECRET_CONNECTION_KEYS_ONLY_NOT_BUSINESS_TEST_OR_RESULT_DATA"
        or private_policy.get("registered_files") != expected_private_files
    ):
        raise DiagnosticError(
            "CONFIG_INVALID", "私密配置登记必须精确匹配唯一PG凭据文件合同"
        )
    expected_deployment_connection = {
        "name": "STOCK_A_PG_DEPLOYMENT_DSN",
        "consumer": "stock_a.storage.postgres.identity._deployment_storage_checks",
        "role": "EXISTING_SUPERUSER_DEPLOYMENT_VERIFICATION_ONLY",
        "target": "127.0.0.1:5432/stock_a",
        "lifecycle": "可选安全环境变量；与stock_a业务连接分离；仅作只读实际簇、存储路径和业务后端绑定核验；缺失时拒绝写入、发布和迁移；不得输出、记录或复制秘密",
    }
    if (
        policy.get("deployment_connection_environment")
        != expected_deployment_connection
    ):
        raise DiagnosticError(
            "CONFIG_INVALID", "独立PG部署核验连接必须按唯一安全环境变量合同登记"
        )
    registered = policy.get("public_files")
    contract_revision = "STOCK-A-DAY-CHAIN/R10"
    expected = {
        (
            "runtime.defaults.json",
            "BUSINESS_RUNTIME_CONFIGURATION",
            _CONFIG_CONTRACT,
            "application startup / configuration resolver; source-vipdata reads source_policy.vipdata_official",
        ),
        (
            "contracts/day-chain.contract.json",
            "BUSINESS_PIPELINE_DESIGN_CONTRACT",
            contract_revision,
            "stock_a.contracts.load_day_chain_contract（唯一入口清单）",
        ),
    }
    module_labels = {
        "pipeline": "pipeline",
        "fields-source": "source fields",
        "fields-core": "Core fields",
        "fields-profile": "cross/Profile rules",
        "fields-control": "control fields",
        "research": "research rules",
        "service": "service contract",
        "policies": "policies",
    }
    expected.update(
        (
            f"contracts/{path}",
            "BUSINESS_PIPELINE_DESIGN_CONTRACT_MODULE",
            contract_revision,
            f"stock_a.contracts.load_day_chain_contract（{module_labels[module]}）",
        )
        for module, path in DAY_CHAIN_MODULES
    )
    if not isinstance(registered, list):
        raise DiagnosticError(
            "CONFIG_INVALID", "configuration_policy.public_files必须是登记清单"
        )
    actual = set()
    for item in registered:
        if not isinstance(item, dict) or not all(
            isinstance(item.get(key), str)
            for key in ("path", "role", "contract", "consumer")
        ):
            raise DiagnosticError("CONFIG_INVALID", "公共配置登记项字段不完整")
        actual.add((item["path"], item["role"], item["contract"], item["consumer"]))
    if actual != expected or len(actual) != len(registered):
        raise DiagnosticError(
            "CONFIG_INVALID", "公共配置登记清单与已批准配置槽位不一致"
        )
    sources = _require_mapping(root.get("source_policy"), "source_policy")
    roots = sources.get("configured_tdx_source_roots")
    if (
        not isinstance(roots, list)
        or any(
            not isinstance(item, str) or not item or not Path(item).is_absolute()
            for item in roots
        )
        or sources.get("all_source_roots_read_only") is not True
        or sources.get("require_phase0_before_dependent_operations") is not True
    ):
        raise DiagnosticError("CONFIG_INVALID", "TDX来源根必须是绝对路径字符串列表")
    try:
        VipdataSourceConfig.from_mapping(sources.get("vipdata_official"))
    except VipdataError as error:
        raise DiagnosticError(
            "CONFIG_INVALID", f"通达信官网端点配置无效：{error}"
        ) from error
    app = _require_mapping(root.get("application_identity"), "application_identity")
    project_uuid = app.get("project_uuid")
    try:
        valid_project_uuid = (
            isinstance(project_uuid, str)
            and str(uuid.UUID(project_uuid)) == project_uuid.lower()
        )
    except (ValueError, AttributeError):
        valid_project_uuid = False
    if app.get("application_id") != "STOCK_A" or not valid_project_uuid:
        raise DiagnosticError("CONFIG_INVALID", "应用身份声明不完整")
    if type(app.get("deployment_applied")) is not bool:
        raise DiagnosticError("CONFIG_INVALID", "deployment_applied必须是布尔值")
    if (
        not isinstance(app.get("migration_lineage"), str)
        or not app["migration_lineage"].strip()
    ):
        raise DiagnosticError("CONFIG_INVALID", "应用迁移谱系声明缺失")
    instance_id = app.get("deployment_instance_uuid")
    cluster_id = app.get("cluster_system_identifier")
    if app["deployment_applied"] is False:
        if (
            instance_id != "BIND_DURING_EXPLICIT_ENABLEMENT"
            or cluster_id != "BIND_DURING_EXPLICIT_ENABLEMENT"
            or app.get("migration_digest") != "BIND_DURING_EXPLICIT_ENABLEMENT"
        ):
            raise DiagnosticError(
                "CONFIG_INVALID", "未部署状态必须使用明确的实例与簇绑定占位值"
            )
    else:
        try:
            valid_instance = (
                isinstance(instance_id, str)
                and str(uuid.UUID(instance_id)) == instance_id.lower()
            )
        except (ValueError, AttributeError):
            valid_instance = False
        if (
            not valid_instance
            or not isinstance(cluster_id, str)
            or not cluster_id.isdecimal()
            or int(cluster_id) <= 0
        ):
            raise DiagnosticError("CONFIG_INVALID", "已部署状态的实例或簇身份无效")
        if not isinstance(app.get("migration_digest"), str) or not re.fullmatch(
            r"[0-9a-f]{64}", app["migration_digest"]
        ):
            raise DiagnosticError("CONFIG_INVALID", "已部署状态的迁移账本摘要无效")
    if app.get("business_login") != "stock_a":
        raise DiagnosticError(
            "CONFIG_INVALID", "PostgreSQL业务连接账号必须固定为stock_a"
        )
    database = _require_mapping(root.get("postgresql"), "postgresql")
    if database.get("database") != "stock_a":
        raise DiagnosticError("CONFIG_INVALID", "PostgreSQL逻辑数据库必须固定为stock_a")
    if database.get("business_schema") != "stock":
        raise DiagnosticError(
            "CONFIG_INVALID", "PostgreSQL唯一业务schema必须固定为stock"
        )
    expected_pg_paths = {
        "host": "127.0.0.1",
        "port": 5432,
        "data_directory": "G:/Postgres/data",
        "wal_directory": "G:/Postgres/data/pg_wal",
        "log_directory": "G:/Postgres/log",
        "manual_export_and_backup_directory": "G:/Postgres/backups",
        "temp_tablespaces": "",
    }
    for key, expected_value in expected_pg_paths.items():
        value = database.get(key)
        if type(value) is not type(expected_value) or value != expected_value:
            raise DiagnosticError(
                "CONFIG_INVALID", f"PostgreSQL配置字段{key}未匹配唯一G盘部署默认"
            )
    service_role = _require_mapping(root.get("startup_policy"), "startup_policy")
    expected_security_policy = {
        "require_explicit_validated_configuration": True,
        "require_pg_data_directory_match": True,
        "allow_legacy_postgres_writes": False,
        "allow_automatic_pg_initialization": False,
        "allow_database_creation": False,
        "allow_schema_creation": False,
        "allow_automatic_old_data_migration": False,
        "allow_port_takeover": False,
        "require_database_schema_application_instance_and_migration_match": True,
        "require_business_login_match": True,
        "allow_automatic_identity_bootstrap": False,
    }
    for key, expected_value in expected_security_policy.items():
        if (
            type(service_role.get(key)) is not bool
            or service_role[key] is not expected_value
        ):
            raise DiagnosticError(
                "CONFIG_INVALID", f"启动安全策略{key}缺失、类型错误或值违反固定策略"
            )
    if app.get("business_login") != "stock_a":
        raise DiagnosticError("CONFIG_INVALID", "应用运行账号必须固定为stock_a")
