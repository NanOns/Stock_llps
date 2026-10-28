"""新项目本地诊断与受控来源只读检查。"""
from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import re
import stat
import sys
import time
from typing import Any
import uuid

from stock_a import __version__
from stock_a.contracts import DAY_CHAIN_MODULES, DayChainContractError, load_day_chain_contract
from stock_a.domain import ContractError
from stock_a.sources.tdx.day import UnitContract
from stock_a.sources.tdx.actions import (
    GBBQ_PROTOCOL_VERSION,
    GBBQ_UNIT_VERSION,
    GbbqDecodeError,
    decode_gbbq_bytes,
)
from stock_a.sources.tdx.day_source import open_day_file, read_protected_bytes
from stock_a.sources.tdx.day_stream import DayReadError, ReadLimits
from stock_a.postgres_identity import inspect_postgres
from stock_a.postgres_binding import BindingRefused, bind_stock_namespace
from stock_a.research.entry import (
    CompareBudget,
    ResearchEntryError,
    UniverseDiscoveryBudget,
    research_compare,
    research_stock,
    research_universe_preview,
)


_WORKSPACE = Path("G:/codex work/Stock A")
_CONFIG_ROOT = _WORKSPACE / "config"
_CONFIG_FILE = _CONFIG_ROOT / "runtime.defaults.json"
_CONFIG_CONTRACT = "STOCK-A-RUNTIME-DEFAULTS/R8"
_EXIT_OK = 0
_EXIT_ARGUMENT = 2
_EXIT_UNAVAILABLE = 3
_EXIT_PERMISSION = 4
_EXIT_ENGINEERING = 5

_READ_ERROR_EXIT_CODES = {
    "SOURCE_NOT_CONFIGURED": _EXIT_UNAVAILABLE,
    "SOURCE_PATH_INVALID": _EXIT_UNAVAILABLE,
    "SOURCE_PATH_OUTSIDE_ROOT": _EXIT_UNAVAILABLE,
    "SOURCE_NOT_FOUND": _EXIT_UNAVAILABLE,
    "SOURCE_ACCESS_DENIED": _EXIT_PERMISSION,
    "SOURCE_HANDLE_UNVERIFIED": _EXIT_UNAVAILABLE,
    "SOURCE_IO_ERROR": _EXIT_ENGINEERING,
    "SOURCE_CHANGED": _EXIT_UNAVAILABLE,
    "READ_LIMIT_EXCEEDED": _EXIT_UNAVAILABLE,
    "READ_TIMEOUT": _EXIT_UNAVAILABLE,
    "STREAM_CONTRACT_ERROR": _EXIT_ENGINEERING,
    "EMPTY_FILE": _EXIT_UNAVAILABLE,
    "INCOMPLETE_TAIL_RECORD": _EXIT_UNAVAILABLE,
    "INVALID_DATE": _EXIT_UNAVAILABLE,
    "DUPLICATE_DATE": _EXIT_UNAVAILABLE,
    "SAME_DAY_CONFLICT": _EXIT_UNAVAILABLE,
    "NON_INCREASING_DATE": _EXIT_UNAVAILABLE,
    "SCAN_NOT_COMPLETE": _EXIT_ENGINEERING,
}


class DiagnosticError(Exception):
    """可向用户显示的配置或能力不可用错误。"""

    def __init__(self, code: str, message: str, exit_code: int = _EXIT_UNAVAILABLE):
        super().__init__(message)
        self.code = code
        self.exit_code = exit_code


def _module_mode() -> dict[str, str]:
    module_path = Path(__file__).resolve()
    workspace = _WORKSPACE.resolve()
    try:
        module_path.relative_to(workspace)
        mode = "DEV"
    except ValueError:
        mode = "INSTALLED"
    if mode == "INSTALLED":
        try:
            distribution = importlib.metadata.distribution("stock-a")
            files = distribution.files or ()
            installed = {Path(distribution.locate_file(item)).resolve() for item in files}
            if module_path not in installed:
                raise DiagnosticError("PACKAGE_IDENTITY_INVALID", "无法核验当前模块属于已安装的stock-a发行包")
        except importlib.metadata.PackageNotFoundError as error:
            raise DiagnosticError("PACKAGE_IDENTITY_INVALID", "当前模块不在开发源码根，也未登记为已安装包") from error
    return {"module_path": str(module_path), "version": __version__, "mode": mode}


def _inside(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _load_runtime_config(explicit_path: str | None = None) -> tuple[dict[str, Any], Path]:
    if explicit_path is not None:
        try:
            requested = Path(explicit_path)
            approved = _CONFIG_FILE.resolve(strict=True)
            if not requested.is_absolute() or requested.resolve() != approved:
                raise DiagnosticError("CONFIG_PATH_REJECTED", "仅允许读取唯一登记配置runtime.defaults.json")
        except FileNotFoundError as error:
            raise DiagnosticError("CONFIG_MISSING", "唯一正式运行配置不存在") from error
        except PermissionError as error:
            raise DiagnosticError("CONFIG_PERMISSION_DENIED", "操作系统拒绝核验正式配置路径", _EXIT_PERMISSION) from error
        except OSError as error:
            raise DiagnosticError("CONFIG_IO_ERROR", "核验显式配置路径时发生工程I/O错误", _EXIT_ENGINEERING) from error
    try:
        config_root = _CONFIG_ROOT.resolve(strict=True)
        config_path = _CONFIG_FILE.resolve(strict=True)
        if config_root != _CONFIG_ROOT or not _inside(config_path, config_root):
            raise DiagnosticError("CONFIG_PATH_REJECTED", "正式配置根或配置文件经链接离开唯一配置槽位")
        if not config_path.is_file():
            raise DiagnosticError("CONFIG_MISSING", "唯一正式运行配置不是普通文件")
    except FileNotFoundError as error:
        raise DiagnosticError("CONFIG_MISSING", "唯一正式运行配置或配置根不存在") from error
    except PermissionError as error:
        raise DiagnosticError("CONFIG_PERMISSION_DENIED", "操作系统拒绝核验正式配置根及最终路径", _EXIT_PERMISSION) from error
    except OSError as error:
        raise DiagnosticError("CONFIG_IO_ERROR", "核验正式配置根及最终路径时发生工程I/O错误", _EXIT_ENGINEERING) from error
    try:
        with config_path.open("r", encoding="utf-8") as stream:
            config = json.load(stream, parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
    except PermissionError as error:
        raise DiagnosticError("CONFIG_PERMISSION_DENIED", "操作系统拒绝读取正式配置", _EXIT_PERMISSION) from error
    except OSError as error:
        raise DiagnosticError("CONFIG_IO_ERROR", "读取正式配置时发生工程I/O错误", _EXIT_ENGINEERING) from error
    except (UnicodeError, json.JSONDecodeError, ValueError) as error:
        raise DiagnosticError("CONFIG_INVALID", "正式配置无法按UTF-8 JSON读取") from error
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
            raise DiagnosticError("CONFIG_INVALID", f"配置字段paths.{key}必须是绝对路径")
    if Path(paths["workspace_root"]).resolve() != _WORKSPACE.resolve():
        raise DiagnosticError("CONFIG_INVALID", "配置workspace_root与唯一源码根不一致")
    if Path(paths["configuration_root"]).resolve() != _CONFIG_ROOT.resolve():
        raise DiagnosticError("CONFIG_INVALID", "配置configuration_root与唯一配置槽位不一致")
    policy = _require_mapping(root.get("configuration_policy"), "configuration_policy")
    if (
        policy.get("contract") != "STOCK-A-CONFIGURATION-POLICY/R1"
        or policy.get("root_only") is not True
        or policy.get("recursive_autoload") is not False
        or policy.get("unknown_files") != "REJECT"
    ):
        raise DiagnosticError("CONFIG_INVALID", "配置目录安全策略缺失或类型错误")
    private_policy = _require_mapping(policy.get("private_files"), "configuration_policy.private_files")
    expected_private_files = [{
        "path": "private/postgresql.credentials.json",
        "schema": "STOCK-A-PG-CREDENTIALS/R1",
        "allowed_keys": ["schema", "host", "port", "database", "username", "password"],
        "consumer": "stock_a.postgres_identity.inspect_postgres（仅pg-identity）",
        "lifecycle": "STOCK_A_PG_DSN优先；仅在其缺失时由pg-identity进程内存读取；不得输出、摘要、记录、复制或传入子进程参数",
    }]
    if (private_policy.get("root") != "private" or private_policy.get("git_allowed") is not False
            or private_policy.get("allowed_role") != "SECRET_CONNECTION_KEYS_ONLY_NOT_BUSINESS_TEST_OR_RESULT_DATA"
            or private_policy.get("registered_files") != expected_private_files):
        raise DiagnosticError("CONFIG_INVALID", "私密配置登记必须精确匹配唯一PG凭据文件合同")
    expected_deployment_connection = {
        "name": "STOCK_A_PG_DEPLOYMENT_DSN",
        "consumer": "stock_a.postgres_identity._deployment_storage_checks",
        "role": "EXISTING_SUPERUSER_DEPLOYMENT_VERIFICATION_ONLY",
        "target": "127.0.0.1:5432/stock_a",
        "lifecycle": "可选安全环境变量；与stock_a业务连接分离；仅作只读实际簇、存储路径和业务后端绑定核验；缺失时拒绝写入、发布和迁移；不得输出、记录或复制秘密",
    }
    if policy.get("deployment_connection_environment") != expected_deployment_connection:
        raise DiagnosticError("CONFIG_INVALID", "独立PG部署核验连接必须按唯一安全环境变量合同登记")
    registered = policy.get("public_files")
    contract_revision = "STOCK-A-DAY-CHAIN/R10"
    expected = {
        ("runtime.defaults.json", "BUSINESS_RUNTIME_CONFIGURATION", _CONFIG_CONTRACT,
         "application startup / configuration resolver"),
        ("contracts/day-chain.contract.json", "BUSINESS_PIPELINE_DESIGN_CONTRACT", contract_revision,
         "stock_a.contracts.load_day_chain_contract（唯一入口清单）"),
    }
    module_labels = {
        "pipeline": "pipeline", "fields-source": "source fields", "fields-core": "Core fields",
        "fields-profile": "cross/Profile rules", "fields-control": "control fields",
        "research": "research rules", "service": "service contract", "policies": "policies",
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
        raise DiagnosticError("CONFIG_INVALID", "configuration_policy.public_files必须是登记清单")
    actual = set()
    for item in registered:
        if not isinstance(item, dict) or not all(isinstance(item.get(key), str) for key in ("path", "role", "contract", "consumer")):
            raise DiagnosticError("CONFIG_INVALID", "公共配置登记项字段不完整")
        actual.add((item["path"], item["role"], item["contract"], item["consumer"]))
    if actual != expected or len(actual) != len(registered):
        raise DiagnosticError("CONFIG_INVALID", "公共配置登记清单与已批准配置槽位不一致")
    sources = _require_mapping(root.get("source_policy"), "source_policy")
    roots = sources.get("configured_tdx_source_roots")
    if (
        not isinstance(roots, list)
        or any(not isinstance(item, str) or not item or not Path(item).is_absolute() for item in roots)
        or sources.get("all_source_roots_read_only") is not True
        or sources.get("require_phase0_before_dependent_operations") is not True
    ):
        raise DiagnosticError("CONFIG_INVALID", "TDX来源根必须是绝对路径字符串列表")
    app = _require_mapping(root.get("application_identity"), "application_identity")
    project_uuid = app.get("project_uuid")
    try:
        valid_project_uuid = isinstance(project_uuid, str) and str(uuid.UUID(project_uuid)) == project_uuid.lower()
    except (ValueError, AttributeError):
        valid_project_uuid = False
    if app.get("application_id") != "STOCK_A" or not valid_project_uuid:
        raise DiagnosticError("CONFIG_INVALID", "应用身份声明不完整")
    if type(app.get("deployment_applied")) is not bool:
        raise DiagnosticError("CONFIG_INVALID", "deployment_applied必须是布尔值")
    if not isinstance(app.get("migration_lineage"), str) or not app["migration_lineage"].strip():
        raise DiagnosticError("CONFIG_INVALID", "应用迁移谱系声明缺失")
    instance_id = app.get("deployment_instance_uuid")
    cluster_id = app.get("cluster_system_identifier")
    if app["deployment_applied"] is False:
        if (instance_id != "BIND_DURING_EXPLICIT_ENABLEMENT" or cluster_id != "BIND_DURING_EXPLICIT_ENABLEMENT"
                or app.get("migration_digest") != "BIND_DURING_EXPLICIT_ENABLEMENT"):
            raise DiagnosticError("CONFIG_INVALID", "未部署状态必须使用明确的实例与簇绑定占位值")
    else:
        try:
            valid_instance = isinstance(instance_id, str) and str(uuid.UUID(instance_id)) == instance_id.lower()
        except (ValueError, AttributeError):
            valid_instance = False
        if not valid_instance or not isinstance(cluster_id, str) or not cluster_id.isdecimal() or int(cluster_id) <= 0:
            raise DiagnosticError("CONFIG_INVALID", "已部署状态的实例或簇身份无效")
        if not isinstance(app.get("migration_digest"), str) or not re.fullmatch(r"[0-9a-f]{64}", app["migration_digest"]):
            raise DiagnosticError("CONFIG_INVALID", "已部署状态的迁移账本摘要无效")
    if app.get("business_login") != "stock_a":
        raise DiagnosticError("CONFIG_INVALID", "PostgreSQL业务连接账号必须固定为stock_a")
    database = _require_mapping(root.get("postgresql"), "postgresql")
    if database.get("database") != "stock_a":
        raise DiagnosticError("CONFIG_INVALID", "PostgreSQL逻辑数据库必须固定为stock_a")
    if database.get("business_schema") != "stock":
        raise DiagnosticError("CONFIG_INVALID", "PostgreSQL唯一业务schema必须固定为stock")
    expected_pg_paths = {
        "host": "127.0.0.1", "port": 5432,
        "data_directory": "G:/Postgres/data", "wal_directory": "G:/Postgres/data/pg_wal",
        "log_directory": "G:/Postgres/log", "manual_export_and_backup_directory": "G:/Postgres/backups",
        "temp_tablespaces": "",
    }
    for key, expected_value in expected_pg_paths.items():
        value = database.get(key)
        if type(value) is not type(expected_value) or value != expected_value:
            raise DiagnosticError("CONFIG_INVALID", f"PostgreSQL配置字段{key}未匹配唯一G盘部署默认")
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
        if type(service_role.get(key)) is not bool or service_role[key] is not expected_value:
            raise DiagnosticError("CONFIG_INVALID", f"启动安全策略{key}缺失、类型错误或值违反固定策略")
    if app.get("business_login") != "stock_a":
        raise DiagnosticError("CONFIG_INVALID", "应用运行账号必须固定为stock_a")


def _configuration_snapshot(config: dict[str, Any], config_path: Path) -> dict[str, Any]:
    app = config["application_identity"]
    paths = config["paths"]
    roots = [str(Path(item).resolve()) for item in config["source_policy"]["configured_tdx_source_roots"]]
    bound = (
        app["deployment_applied"] is True
        and app["deployment_instance_uuid"] != "BIND_DURING_EXPLICIT_ENABLEMENT"
        and app["cluster_system_identifier"] != "BIND_DURING_EXPLICIT_ENABLEMENT"
    )
    return {
        "configuration_path": str(config_path),
        "configuration_contract": config["contract"],
        "workspace_root": paths["workspace_root"],
        "service_role": {"business_login": app["business_login"], "actual_session_role": "NOT_CONNECTED_NOT_VERIFIED"},
        "source_roots": roots,
        "source_status": "NOT_CONFIGURED" if not roots else "DECLARED_NOT_VERIFIED",
        "database": {
            "application_id": app["application_id"],
            "project_uuid": app["project_uuid"],
            "database": config["postgresql"]["database"],
            "schema": config["postgresql"]["business_schema"],
            "deployment_applied": app["deployment_applied"],
            "instance_uuid": app["deployment_instance_uuid"],
            "cluster_system_identifier": app["cluster_system_identifier"],
            "identity_status": "DECLARED_BOUND_NOT_CONNECTED" if bound else "UNBOUND_NOT_VERIFIED",
            "connection_verified": False,
        },
    }


def _json_default(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, tuple):
        return list(value)
    raise TypeError(f"不支持的JSON类型：{type(value).__name__}")


def _emit(payload: dict[str, Any], *, error: bool = False) -> None:
    text = json.dumps(payload, ensure_ascii=False, allow_nan=False, separators=(",", ":"), default=_json_default)
    print(text, file=sys.stderr if error else sys.stdout)


class ChineseFormatter(argparse.HelpFormatter):
    def _format_usage(self, usage, actions, groups, prefix):
        return super()._format_usage(usage, actions, groups, "用法：")

    def start_section(self, heading):
        translated = {"positional arguments": "位置参数", "options": "选项"}.get(heading, heading)
        super().start_section(translated)


class ChineseParser(argparse.ArgumentParser):
    formatter_class = ChineseFormatter

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("add_help", False)
        super().__init__(*args, **kwargs)
        self.add_argument("-h", "--help", action="help", default=argparse.SUPPRESS, help="显示此帮助信息并退出")

    def format_usage(self) -> str:
        return super().format_usage().replace("usage:", "用法：", 1)

    def format_help(self) -> str:
        rendered = super().format_help().replace("usage:", "用法：", 1)
        for english, chinese in (("positional arguments:", "位置参数："), ("optional arguments:", "选项："), ("options:", "选项：")):
            rendered = rendered.replace(english, chinese)
        return rendered

    def error(self, message: str) -> None:
        _emit({"ok": False, "code": "ARGUMENT_ERROR", "message": "参数错误：请核对命令格式及必需参数"}, error=True)
        raise SystemExit(_EXIT_ARGUMENT)


def _parser() -> argparse.ArgumentParser:
    parser = ChineseParser(prog="stock-a", description="Stock A中文诊断与TDX只读来源检查")
    commands = parser.add_subparsers(dest="command", parser_class=ChineseParser)
    doctor = commands.add_parser("doctor", help="诊断本地配置、包身份和声明能力")
    doctor.add_argument("--config", help="仅接受唯一登记配置runtime.defaults.json的绝对路径")
    pg = commands.add_parser("pg-identity", help="以只读事务核验stock_a登录、固定数据库/schema及项目迁移身份")
    pg.add_argument("--timeout-seconds", type=float, default=5.0, help="连接与每条查询的秒数上限，默认5秒，最大30秒")
    bind = commands.add_parser("pg-bind", help="以stock_a账号应用待执行的Stock A集中前向迁移")
    bind.add_argument("--timeout-seconds", type=float, default=20.0, help="身份绑定总耗时上限，默认20秒，最大30秒")
    phase0 = commands.add_parser(
        "source-phase0",
        help="有界核验已绑定的本地TDX来源能力",
        description="按明示预算有界核验已绑定的本地TDX来源能力，只读不写",
    )
    phase0.add_argument("--max-entries", type=int, default=50000, help="每个行情目录最多枚举的目录项数，默认50000")
    phase0.add_argument("--max-file-bytes", type=int, default=16777216, help="单个来源文件最大读取字节数，默认16777216")
    phase0.add_argument("--max-total-bytes", type=int, default=33554432, help="本次来源文件总读取字节数上限，默认33554432")
    phase0.add_argument("--max-records", type=int, default=200000, help="每个日线文件及GBBQ最多解析记录数，默认200000")
    phase0.add_argument("--max-seconds", default="30", help="本次总核验耗时上限，默认30秒")
    inspect = commands.add_parser("source-inspect", help="只读检查单个TDX日线文件")
    inspect.add_argument("path", help="待检查日线文件的绝对路径")
    inspect.add_argument("--unit-version", required=True, help="显式日线单位合同版本")
    inspect.add_argument("--amount-to-cny", required=True, help="金额到元的精确十进制倍率")
    inspect.add_argument("--volume-to-shares", required=True, type=int, help="数量到股的正整数倍率")
    inspect.add_argument("--max-bytes", required=True, type=int, help="本次最大读取字节数")
    inspect.add_argument("--max-records", required=True, type=int, help="本次最大记录数")
    inspect.add_argument("--chunk-records", default=256, type=int, help="单次读取块记录数，默认256")
    inspect.add_argument("--max-seconds", default="30", help="本次最大读取秒数，默认30")
    stock = commands.add_parser("research-stock", help="只读查看单只股票的原始行情、Core字段和基础观察")
    stock.add_argument("symbol", help="来源代码，格式SH.六位代码或SZ.六位代码")
    _add_research_arguments(stock)
    stock.add_argument("--format", dest="output_format", choices=("summary", "json"), default="json", help="输出可读事实摘要或完整JSON，默认json")
    stock.add_argument("--include-profile-preview", action="store_true", help="在同一次Raw/Core只读结果上附加11项基础画像组件预览；非PIT且不开放生产准入")
    compare = commands.add_parser("research-compare", help="盘后只读输出有界多标的Core基础观察摘要")
    compare.add_argument("symbols", nargs="+", help="一个或多个来源代码，格式SH.六位代码或SZ.六位代码")
    _add_research_arguments(compare)
    compare.add_argument("--max-total-bytes", type=int, default=335544320, help="本次多股票扫描总字节上限，默认335544320")
    compare.add_argument("--max-total-records", type=int, default=4000000, help="本次多股票解析总记录上限，默认4000000")
    compare.add_argument("--max-total-seconds", type=float, default=120.0, help="本次多股票扫描总耗时秒数上限，默认120")
    universe = commands.add_parser(
        "research-universe-preview",
        help="发现沪深主板/创业板TDX来源候选并分页查看盘后基础观察",
    )
    _add_research_arguments(universe)
    universe.add_argument("--offset", type=int, default=0, help="来源候选分页起点，默认0")
    universe.add_argument("--limit", type=int, default=20, help="本页证券数，默认20，最多100")
    universe.add_argument("--market", choices=("ALL", "SH", "SZ"), default="ALL", help="市场过滤，默认ALL")
    universe.add_argument("--board", choices=("ALL", "MAIN_BOARD", "GEM"), default="ALL", help="板别过滤，默认ALL")
    universe.add_argument("--format", dest="output_format", choices=("summary", "json"), default="summary", help="输出短摘要或完整JSON，默认summary")
    universe.add_argument("--max-directory-entries", type=int, default=50000, help="来源目录累计枚举上限，默认50000")
    universe.add_argument("--max-discovery-seconds", type=float, default=30.0, help="来源目录发现时间上限秒，默认30")
    universe.add_argument("--max-total-bytes", type=int, default=335544320, help="当前页累计TDX读取字节上限，默认335544320")
    universe.add_argument("--max-total-records", type=int, default=4000000, help="当前页累计TDX记录上限，默认4000000")
    universe.add_argument("--max-total-seconds", type=float, default=120.0, help="当前页累计扫描秒数上限，默认120")
    return parser


def _add_research_arguments(command: argparse.ArgumentParser) -> None:
    command.add_argument("--target-session", required=True, help="必填目标交易会话，格式YYYY-MM-DD")
    command.add_argument("--unit-version", required=True, help="本次研究使用的显式TDX单位合同版本")
    command.add_argument("--amount-to-cny", required=True, help="金额到人民币元的精确十进制倍率")
    command.add_argument("--volume-to-shares", required=True, type=int, help="数量到股的正整数倍率")
    command.add_argument("--max-bytes", type=int, default=16777216, help="单文件最多读取字节数，默认16777216")
    command.add_argument("--max-records", type=int, default=200000, help="单文件最多读取记录数，默认200000")
    command.add_argument("--chunk-records", type=int, default=256, help="单次读取块记录数，默认256")
    command.add_argument("--max-seconds", default="30", help="单文件最大读取秒数，默认30")


def _safe_child(path: Path, root: Path) -> Path:
    try:
        resolved = path.resolve(strict=True)
        resolved.relative_to(root)
        mode = resolved.stat().st_mode
    except FileNotFoundError as error:
        raise DiagnosticError("SOURCE_PATH_INVALID", "来源目录不存在") from error
    except PermissionError as error:
        raise DiagnosticError("SOURCE_ACCESS_DENIED", "操作系统拒绝核验来源目录", _EXIT_PERMISSION) from error
    except OSError as error:
        raise DiagnosticError("SOURCE_IO_ERROR", "核验来源目录时发生工程I/O错误", _EXIT_ENGINEERING) from error
    except ValueError as error:
        raise DiagnosticError("SOURCE_PATH_INVALID", "来源目录不存在或最终路径离开保护根") from error
    if not stat.S_ISDIR(mode):
        raise DiagnosticError("SOURCE_PATH_INVALID", "来源目录结构不符合TDX目录合同")
    return resolved


def _run_source_phase0(args: argparse.Namespace) -> int:
    package = _module_mode()
    config, config_path = _load_runtime_config()
    roots = config["source_policy"]["configured_tdx_source_roots"]
    if not roots:
        raise DiagnosticError("SOURCE_NOT_CONFIGURED", "配置中没有绑定TDX来源保护根，阶段0在目录枚举前拒绝")
    if len(roots) != 1:
        raise DiagnosticError("SOURCE_ROOT_AMBIGUOUS", "阶段0要求一个明确绑定的本地TDX来源根")
    if type(args.max_entries) is not int or args.max_entries <= 0 or type(args.max_file_bytes) is not int or args.max_file_bytes <= 0 or type(args.max_total_bytes) is not int or args.max_total_bytes <= 0 or type(args.max_records) is not int or args.max_records <= 0:
        raise DiagnosticError("ARGUMENT_CONTRACT_INVALID", "阶段0目录、字节与记录预算必须是拒绝布尔值的正整数", _EXIT_ARGUMENT)
    try:
        seconds = Decimal(args.max_seconds)
        if not seconds.is_finite() or seconds <= 0:
            raise ValueError
    except (InvalidOperation, ValueError) as error:
        raise DiagnosticError("ARGUMENT_CONTRACT_INVALID", "阶段0耗时预算必须是有限正数", _EXIT_ARGUMENT) from error
    started = time.monotonic()
    try:
        root = Path(roots[0]).resolve(strict=True)
        root_mode = root.stat().st_mode
    except FileNotFoundError as error:
        raise DiagnosticError("SOURCE_PATH_INVALID", "已绑定TDX来源根不存在") from error
    except PermissionError as error:
        raise DiagnosticError("SOURCE_ACCESS_DENIED", "操作系统拒绝核验已绑定来源根", _EXIT_PERMISSION) from error
    except OSError as error:
        raise DiagnosticError("SOURCE_IO_ERROR", "核验已绑定来源根时发生工程I/O错误", _EXIT_ENGINEERING) from error
    if not stat.S_ISDIR(root_mode):
        raise DiagnosticError("SOURCE_PATH_INVALID", "已绑定TDX来源根不是目录")
    sample_rule = "对sh/sz/bj协议目录分别按文件名排序，完整只读扫描首个.day文件；每个目录最多枚举max_entries项"
    markets: dict[str, dict[str, Any]] = {}
    total_bytes = 0
    total_records = 0
    for market in ("sh", "sz", "bj"):
        directory = _safe_child(root / "vipdoc" / market / "lday", root)
        names: list[str] = []
        entry_count = 0
        excluded_links = 0
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    if time.monotonic() - started > float(seconds):
                        raise DiagnosticError("READ_TIMEOUT", "来源目录枚举超过耗时预算", _EXIT_UNAVAILABLE)
                    entry_count += 1
                    if entry_count > args.max_entries:
                        raise DiagnosticError("READ_LIMIT_EXCEEDED", "行情目录项数超过预算，不能将截断枚举报告为完整", _EXIT_UNAVAILABLE)
                    if entry.is_symlink():
                        excluded_links += 1
                        continue
                    if entry.is_file(follow_symlinks=False) and entry.name.lower().endswith(".day"):
                        names.append(entry.name)
        except PermissionError as error:
            raise DiagnosticError("SOURCE_ACCESS_DENIED", "操作系统拒绝枚举行情目录", _EXIT_PERMISSION) from error
        except OSError as error:
            raise DiagnosticError("SOURCE_IO_ERROR", "枚举行情目录时发生工程I/O错误", _EXIT_ENGINEERING) from error
        names.sort(key=str.casefold)
        market_result: dict[str, Any] = {
            "directory": str(directory),
            "day_file_count": len(names),
            "enumerated_entry_count": entry_count,
            "excluded_link_count": excluded_links,
            "scope_complete": True,
            "sample_rule": "字典序首个.day文件，读取整个文件",
            "sample": None,
        }
        if names:
            sample_path = _safe_file(root / "vipdoc" / market / "lday" / names[0], root)
            size = _source_file_size(sample_path)
            if size > args.max_file_bytes or total_bytes + size > args.max_total_bytes:
                raise DiagnosticError("READ_LIMIT_EXCEEDED", "日线样本超过本次文件或总字节预算", _EXIT_UNAVAILABLE)
            units = UnitContract("TDX_RAW_DIAGNOSTIC_ONLY", Decimal(1), 1)
            remaining_total = args.max_total_bytes - total_bytes
            if remaining_total <= 0:
                raise DiagnosticError("READ_LIMIT_EXCEEDED", "日线样本没有剩余总字节预算", _EXIT_UNAVAILABLE)
            limits = ReadLimits(min(args.max_file_bytes, remaining_total), args.max_records, 1024, seconds)
            try:
                with open_day_file(sample_path, roots, units, limits) as scan:
                    for _record in scan:
                        if time.monotonic() - started > float(seconds):
                            raise DayReadError("READ_TIMEOUT", "BUDGET")
                    summary = scan.summary
            except DayReadError as error:
                code = error.code
                exit_code = _READ_ERROR_EXIT_CODES.get(code, _EXIT_ENGINEERING)
                raise DiagnosticError(code, f"日线真实样本未能完整核验：{error}", exit_code) from error
            total_bytes += summary.scan.byte_count
            total_records += summary.scan.record_count
            market_result["sample"] = {
                "path": str(sample_path),
                "raw_byte_sha256": summary.scan.raw_byte_sha256,
                "byte_count": summary.scan.byte_count,
                "record_count": summary.scan.record_count,
                "first_date": summary.scan.first_date.isoformat(),
                "last_date": summary.scan.last_date.isoformat(),
                "source_identity": asdict(summary.source),
                "read_started_at": summary.scan.read_started_at.isoformat(),
                "read_completed_at": summary.scan.read_completed_at.isoformat(),
                "price_diagnostic_scale": "raw_price_integer/100 per existing decoder; candidate protocol observation only",
                "amount_unit": "UNVERIFIED_RAW_FLOAT32",
                "volume_unit": "UNVERIFIED_RAW_UINT32",
                "field_quality": {name: asdict(getattr(summary.scan, name)) for name in ("prices", "amount", "volume")},
            }
        markets[market] = market_result

    capabilities: list[dict[str, Any]] = [{
        "capability": "TDX_DAILY_RAW",
        "dataset_id": "TDX_DAILY_BARS",
        "version": "TDX_DAY_RECORD_32B_CANDIDATE",
        "source_authority": "UNVERIFIED_LOCAL_TDX_INSTALLATION",
        "producer": "stock_a.sources.tdx.day.decode_day_record + day_stream.DayRecordScan",
        "field_contract": [
            {"field": "trade_date", "unit": "YYYYMMDD calendar date", "time_role": "trade_date only; not publication or availability", "quality": "date validation in existing parser", "consumer_admission": "DENY_UNTIL_OFFICIAL_CALENDAR"},
            {"field": "open/high/low/close", "unit": "candidate raw price integer / 100", "time_role": "bar trade_date", "quality": "group validation counts included per sample", "consumer_admission": "DENY_NORMALIZED_CORE_PENDING_SOURCE_AND_UNIT_AUTHORITY"},
            {"field": "amount", "unit": "UNVERIFIED_RAW_FLOAT32", "time_role": "bar trade_date", "quality": "finite/nonnegative group validation counts included per sample", "consumer_admission": "DENY_NORMALIZED_CORE"},
            {"field": "volume", "unit": "UNVERIFIED_RAW_UINT32", "time_role": "bar trade_date", "quality": "nonnegative integer group validation counts included per sample", "consumer_admission": "DENY_NORMALIZED_CORE"},
        ],
        "clock_contract": {"observed_at": "read_started_at/read_completed_at with local timezone per sample", "available_at": "UNKNOWN", "published_at": "UNKNOWN", "captured_at": "this local CLI read observation only; not historical first capture", "sealed_at": "UNKNOWN"},
        "required": True,
        "status": "BLOCKED",
        "reason": "真实文件可读，但仅每市场一个样本；成交额/成交量单位及其来源权威未独立成立，未注册规范化消费者",
        "sample_scope": sample_rule,
        "consumer_admission": "DENY_NORMALIZED_CORE",
    }]

    action_path = _safe_file(root / "T0002" / "hq_cache" / "gbbq", root, optional=True)
    action_result: dict[str, Any] = {"status": "UNAVAILABLE", "reason": "来源文件不存在"}
    if action_path is not None:
        action_size = _source_file_size(action_path)
        remaining_total = args.max_total_bytes - total_bytes
        if remaining_total <= 0 or action_size > args.max_file_bytes or action_size > remaining_total:
            raise DiagnosticError("READ_LIMIT_EXCEEDED", "GBBQ超过本次文件或总字节预算，未生成完成摘要", _EXIT_UNAVAILABLE)
        try:
            raw, identity = read_protected_bytes(
                action_path,
                roots,
                max_bytes=min(args.max_file_bytes, remaining_total),
                max_seconds=float(seconds),
            )
        except DayReadError as error:
            exit_code = _READ_ERROR_EXIT_CODES.get(error.code, _EXIT_ENGINEERING)
            raise DiagnosticError(error.code, f"GBBQ真实文件未能完整核验：{error}", exit_code) from error
        total_bytes += len(raw)
        try:
            parsed = decode_gbbq_bytes(
                raw,
                source_ref="TDX_LOCAL_GBBQ_CANDIDATE",
                protocol_version=GBBQ_PROTOCOL_VERSION,
                unit_version=GBBQ_UNIT_VERSION,
                max_bytes=args.max_file_bytes,
                max_records=args.max_records,
            )
        except GbbqDecodeError as error:
            action_result = {"status": "BLOCKED", "reason": "GBBQ内容合同未闭合", "error_code": error.code, "raw_byte_sha256": hashlib.sha256(raw).hexdigest(), "byte_count": len(raw), "handle_path": identity.handle_path}
        else:
            categories: dict[str, int] = {}
            for record in parsed.records:
                categories[record.category_name] = categories.get(record.category_name, 0) + 1
            action_result = {
                "status": "DEGRADED_PASS",
                "path": str(action_path),
                "handle_path": identity.handle_path,
                "raw_byte_sha256": hashlib.sha256(raw).hexdigest(),
                "byte_count": len(raw),
                "record_count": parsed.parsed_count,
                "category_counts": categories,
                "protocol_version": parsed.protocol_version,
                "unit_version": parsed.unit_version,
                "producer": "stock_a.sources.tdx.actions.decode_gbbq_bytes",
                "source_authority": "UNVERIFIED_LOCAL_TDX_INSTALLATION",
                "identity_status": "SOURCE_SYMBOL_ONLY_NOT_STABLE_ENTITY",
                "pit_status": "NOT_ASSERTED_LOCAL_READ_CLOCK_ONLY",
                "consumer_admission": "CATEGORY_1_CANDIDATE_ONLY_NO_PIT_OR_OFFICIAL_IDENTITY",
            }
    capabilities.append({
        "capability": "TDX_GBBQ_CORPORATE_ACTIONS",
        "required": False,
        "status": action_result["status"],
        "reason": "本地字节可解析但不构成身份或PIT证明" if action_result["status"] == "DEGRADED_PASS" else action_result.get("reason"),
        "evidence": action_result,
    })

    classification_candidates = []
    for relative in ("T0002/hq_cache/tdxhy.cfg", "T0002/hq_cache/infoharbor_block.dat"):
        candidate = _safe_file(root.joinpath(*relative.split("/")), root, optional=True)
        if candidate is not None:
            classification_candidates.append({"path": str(candidate), "byte_count": _source_file_size(candidate)})
    identity_candidates = []
    for relative in ("T0002/hq_cache/ds_stk.dat", "T0002/hq_cache/base.dbf", "T0002/hq_cache/code2name.ini"):
        candidate = _safe_file(root.joinpath(*relative.split("/")), root, optional=True)
        if candidate is not None:
            identity_candidates.append({"path": str(candidate), "byte_count": _source_file_size(candidate)})
    capabilities.extend([
        {"capability": "TDX_INDUSTRY_CONCEPT_MEMBERSHIP", "required": False, "status": "BLOCKED", "reason": "分类候选文件存在但未有新项目解析/有效时钟合同；U03解析内核待后续身份与成员任务接入", "candidates": classification_candidates, "consumer_admission": "DENY"},
        {"capability": "SECURITY_ENTITY_AND_LIFECYCLE", "required": True, "status": "BLOCKED", "reason": "本地目录/代码/首条日线不是官方稳定实体、上市/退市或换码证明；U06候选未在本目标引入", "candidates": identity_candidates, "consumer_admission": "DENY_UNMAPPED"},
        {"capability": "OFFICIAL_TRADING_CALENDAR_AND_STATUS", "required": True, "status": "BLOCKED", "reason": "未识别并验证版本化交易所日历或日期有效状态来源；禁止由连续价格或缺bar推断", "consumer_admission": "DENY_UNKNOWN_GAPS"},
        {"capability": "BAOSTOCK_SUPPLEMENTAL", "required": False, "status": "UNAVAILABLE", "reason": "本任务禁止联网或调用第三方接口，未验证逐字段覆盖与单位", "consumer_admission": "DENY_SUPPLEMENTAL_ONLY"},
    ])
    required_blocked = any(item["required"] and item["status"] not in ("FULL_PASS", "DEGRADED_PASS") for item in capabilities)
    overall = "BLOCKED" if required_blocked else ("DEGRADED_PASS" if any(item["status"] != "FULL_PASS" for item in capabilities) else "FULL_PASS")
    elapsed = time.monotonic() - started
    if elapsed > float(seconds):
        raise DiagnosticError("READ_TIMEOUT", "本次阶段0核验整体超过耗时预算，不生成完成摘要", _EXIT_UNAVAILABLE)
    _emit({
        "ok": overall != "BLOCKED",
        "status": overall,
        "message": "已按声明预算完成本地只读阶段0核验；BLOCKED能力不进入scanner/消费者，观测时钟不代表available_at、published_at、封口或PIT",
        "package": package,
        "configuration_path": str(config_path),
        "source_root": str(root),
        "source_root_final_path": str(root),
        "observed_at": datetime.now().astimezone().isoformat(),
        "clock_role": "LOCAL_OBSERVED_AT_ONLY",
        "sample_rule": sample_rule,
        "market_directories": markets,
        "capabilities": capabilities,
        "coverage": {"day_file_count": sum(item["day_file_count"] for item in markets.values()), "sampled_day_files": sum(item["sample"] is not None for item in markets.values()), "unsampled_day_files": sum(item["day_file_count"] for item in markets.values()) - sum(item["sample"] is not None for item in markets.values()), "sampled_records": total_records, "sampled_source_bytes": total_bytes},
        "budgets": {"max_entries_per_market_directory": args.max_entries, "max_file_bytes": args.max_file_bytes, "max_total_bytes": args.max_total_bytes, "max_records_per_file": args.max_records, "max_seconds": str(seconds), "elapsed_seconds": round(elapsed, 6)},
        "business_admission": "DENY" if overall == "BLOCKED" else "SCOPED_ONLY",
        "database_connected": False,
        "production_switched": False,
    })
    return _EXIT_UNAVAILABLE if overall == "BLOCKED" else _EXIT_OK


def _safe_file(path: Path, root: Path, *, optional: bool = False) -> Path | None:
    try:
        resolved = path.resolve(strict=True)
        resolved.relative_to(root)
        mode = resolved.stat().st_mode
    except FileNotFoundError:
        if optional:
            return None
        raise DiagnosticError("SOURCE_PATH_INVALID", "所需TDX目录不存在")
    except PermissionError as error:
        raise DiagnosticError("SOURCE_ACCESS_DENIED", "操作系统拒绝核验来源文件", _EXIT_PERMISSION) from error
    except OSError as error:
        raise DiagnosticError("SOURCE_IO_ERROR", "核验来源文件时发生工程I/O错误", _EXIT_ENGINEERING) from error
    except ValueError as error:
        raise DiagnosticError("SOURCE_PATH_INVALID", "来源文件最终路径离开保护根") from error
    if not stat.S_ISREG(mode):
        raise DiagnosticError("SOURCE_PATH_INVALID", "来源目标不是普通文件")
    return resolved


def _source_file_size(path: Path) -> int:
    """读取来源大小元数据，并保留来源缺失、权限拒绝与工程I/O的区别。"""
    try:
        return path.stat().st_size
    except FileNotFoundError as error:
        raise DiagnosticError("SOURCE_NOT_FOUND", "来源文件在大小核验时已不存在", _EXIT_UNAVAILABLE) from error
    except PermissionError as error:
        raise DiagnosticError("SOURCE_ACCESS_DENIED", "操作系统拒绝读取来源文件大小", _EXIT_PERMISSION) from error
    except OSError as error:
        raise DiagnosticError("SOURCE_IO_ERROR", "读取来源文件大小时发生工程I/O错误", _EXIT_ENGINEERING) from error


def _run_doctor(args: argparse.Namespace) -> int:
    package = _module_mode()
    config, config_path = _load_runtime_config(args.config)
    snapshot = _configuration_snapshot(config, config_path)
    try:
        loaded_contract = load_day_chain_contract(config["paths"]["configuration_root"])
    except DayChainContractError as error:
        raise DiagnosticError(error.code, str(error)) from error
    contract_document = loaded_contract.document
    foundation = contract_document["scope_bindings"]["FOUNDATION_EOD"]
    _emit({
        "ok": False,
        "status": "LOCAL_DIAGNOSTIC_ONLY",
        "message": "本地包与配置结构已检查；未核验真实来源或数据库连接，不允许据此启动业务",
        "package": package,
        **snapshot,
        "day_chain_contract": {
            "contract_id": contract_document["contract_id"],
            "version": contract_document["version"],
            "assembled_sha256": loaded_contract.assembled_sha256,
            "module_count": len(loaded_contract.module_paths),
            "module_paths": list(loaded_contract.module_paths),
            "stage_count": len(contract_document["stages"]),
            "artifact_count": len(contract_document["artifact_groups"]),
            "foundation_field_count": len(foundation["field_registry"]),
            "profile_expression_count": len(foundation["profile_expression_registry"]),
            "production_readiness_claim": False,
        },
        "business_start_permitted": False,
        "database_connection_attempted": False,
    })
    return _EXIT_UNAVAILABLE


def _run_pg_identity(args: argparse.Namespace) -> int:
    if type(args.timeout_seconds) not in (int, float) or not math.isfinite(args.timeout_seconds) or not 0 < args.timeout_seconds <= 30:
        raise DiagnosticError("ARGUMENT_CONTRACT_INVALID", "PG核验超时必须大于0且不超过30秒", _EXIT_ARGUMENT)
    package = _module_mode()
    config, config_path = _load_runtime_config()
    result = inspect_postgres(config, timeout_seconds=args.timeout_seconds)
    result.update({
        "ok": result.get("status") == "DB_MIGRATION_READY",
        "message": "PG目标与账号核验结束；数据库状态不代表业务流水线已完成",
        "package": package,
        "configuration_path": str(config_path),
        "business_write_admission": "ALLOW_DATABASE_CAPABILITY" if result.get("status") == "DB_MIGRATION_READY" else "DENY",
        "production_switched": False,
    })
    _emit(result, error=result.get("status") != "DB_MIGRATION_READY")
    if result.get("status") == "INVALID_ARGUMENT":
        return _EXIT_ARGUMENT
    if result.get("status") == "DB_MIGRATION_READY":
        return _EXIT_OK
    if result.get("status") in ("PERMISSION_DENIED", "AUTHENTICATION_FAILED"):
        return _EXIT_PERMISSION
    if result.get("status") == "ENGINEERING_ERROR":
        return _EXIT_ENGINEERING
    return _EXIT_UNAVAILABLE


def _run_pg_bind(args: argparse.Namespace) -> int:
    if type(args.timeout_seconds) not in (int, float) or not math.isfinite(args.timeout_seconds) or not 0 < args.timeout_seconds <= 30:
        raise DiagnosticError("ARGUMENT_CONTRACT_INVALID", "PG绑定超时必须大于0且不超过30秒", _EXIT_ARGUMENT)
    package = _module_mode()
    if package.get("mode") != "DEV":
        raise DiagnosticError("BIND_MODE_REJECTED", "固定身份迁移仅允许从批准源码包的显式开发CLI执行", _EXIT_PERMISSION)
    config, config_path = _load_runtime_config()
    try:
        result = bind_stock_namespace(config, config_path, timeout_seconds=args.timeout_seconds)
    except BindingRefused as error:
        _emit({"ok": False, "status": "REJECTED", "code": "PG_BIND_REFUSED",
               "message": str(error), "business_write_admission": "DENY",
               "database_write_attempted": "UNKNOWN_IF_FAILURE_AFTER_COMMIT",
               "configuration_path": str(config_path), "production_switched": False}, error=True)
        return _EXIT_PERMISSION
    result.update({"ok": result.get("status") == "BOUND", "package": package,
                   "configuration_path": str(config_path), "production_switched": False})
    _emit(result)
    return _EXIT_OK


def _run_source_inspect(args: argparse.Namespace) -> int:
    if not Path(args.path).is_absolute():
        raise DiagnosticError("SOURCE_PATH_INVALID", "来源文件必须使用绝对路径")
    config, config_path = _load_runtime_config()
    roots = config["source_policy"]["configured_tdx_source_roots"]
    if not roots:
        raise DiagnosticError("SOURCE_NOT_CONFIGURED", "配置中没有绑定TDX来源保护根，拒绝打开任何来源文件")
    try:
        amount_scale = Decimal(args.amount_to_cny)
        seconds = Decimal(args.max_seconds)
        units = UnitContract(args.unit_version, amount_scale, args.volume_to_shares)
        limits = ReadLimits(args.max_bytes, args.max_records, args.chunk_records, seconds)
    except (InvalidOperation, ValueError, ContractError) as error:
        raise DiagnosticError("ARGUMENT_CONTRACT_INVALID", f"单位或读取预算无效：{error}", _EXIT_ARGUMENT) from error
    try:
        with open_day_file(args.path, roots, units, limits) as scan:
            for _located in scan:
                pass
            file_summary = scan.summary
            summary = file_summary.scan
            source = file_summary.source
    except DayReadError as error:
        exit_code = _READ_ERROR_EXIT_CODES.get(error.code, _EXIT_ENGINEERING)
        _emit({"ok": False, "code": error.code, "reason_category": error.reason_category, "cause_type": type(error.__cause__).__name__ if error.__cause__ is not None else None, "message": str(error), "record_index": error.record_index, "byte_offset": error.byte_offset}, error=True)
        return exit_code
    except PermissionError as error:
        raise DiagnosticError("SOURCE_ACCESS_DENIED", "操作系统拒绝读取来源文件", _EXIT_PERMISSION) from error
    except OSError as error:
        raise DiagnosticError("SOURCE_IO_ERROR", "读取来源时发生操作系统I/O错误", _EXIT_ENGINEERING) from error
    _emit({
        "ok": True,
        "status": "FILE_READ_COMPLETE_ONLY",
        "message": "指定日线文件已只读扫描至EOF并通过最终句柄核验；不代表来源封口、PIT或阶段0通过",
        "configuration_path": str(config_path),
        "complete": summary.complete,
        "record_count": summary.record_count,
        "byte_count": summary.byte_count,
        "first_date": summary.first_date.isoformat(),
        "last_date": summary.last_date.isoformat(),
        "raw_byte_sha256": summary.raw_byte_sha256,
        "unit_version": summary.unit_version,
        "groups": {name: asdict(getattr(summary, name)) for name in ("prices", "amount", "volume")},
        "read_started_at": summary.read_started_at.isoformat(),
        "read_completed_at": summary.read_completed_at.isoformat(),
        "read_clock_role": "LOCAL_FILE_READ_TIME_ONLY",
        "source": asdict(source),
        "budgets": {"max_bytes": limits.max_bytes, "max_records": limits.max_records, "chunk_records": limits.chunk_records, "max_seconds": limits.max_seconds},
    })
    return _EXIT_OK


def _run_research(args: argparse.Namespace, *, compare: bool) -> int:
    config, config_path = _load_runtime_config()
    roots = config["source_policy"]["configured_tdx_source_roots"]
    if not roots:
        raise DiagnosticError("SOURCE_NOT_CONFIGURED", "配置中没有绑定TDX来源保护根，拒绝读取行情")
    try:
        units = UnitContract(args.unit_version, Decimal(args.amount_to_cny), args.volume_to_shares)
        limits = ReadLimits(
            args.max_bytes,
            args.max_records,
            args.chunk_records,
            Decimal(args.max_seconds),
        )
    except (InvalidOperation, ValueError, ContractError) as error:
        raise DiagnosticError("ARGUMENT_CONTRACT_INVALID", f"单位或读取预算无效：{error}", _EXIT_ARGUMENT) from error
    profile_contract = None
    if not compare and getattr(args, "include_profile_preview", False):
        try:
            profile_contract = load_day_chain_contract(_CONFIG_ROOT).document
        except DayChainContractError as error:
            _emit({"ok": False, "code": error.code, "message": str(error), "database_io_attempted": False}, error=True)
            return _EXIT_UNAVAILABLE
    try:
        if compare:
            budget = CompareBudget(
                args.max_total_bytes,
                args.max_total_records,
                args.max_total_seconds,
            )
            result = research_compare(
                symbols=args.symbols,
                target_session=args.target_session,
                source_roots=roots,
                units=units,
                limits=limits,
                budget=budget,
            )
        else:
            result = research_stock(
                symbol=args.symbol,
                target_session=args.target_session,
                source_roots=roots,
                units=units,
                limits=limits,
                profile_contract=profile_contract,
            )
    except ResearchEntryError as error:
        if error.code == "SOURCE_ACCESS_DENIED":
            code = _EXIT_PERMISSION
        elif error.code == "SOURCE_IO_ERROR":
            code = _EXIT_ENGINEERING
        else:
            code = _EXIT_ARGUMENT if error.code in {
                "TARGET_DATE_INVALID", "SYMBOL_INVALID", "SYMBOL_OUT_OF_SCOPE",
                "COMPARE_SIZE_INVALID", "DUPLICATE_SYMBOL",
            } else _EXIT_UNAVAILABLE
        _emit({"ok": False, "code": error.code, "message": str(error), "database_io_attempted": False}, error=True)
        return code
    except DayReadError as error:
        exit_code = _READ_ERROR_EXIT_CODES.get(error.code, _EXIT_ENGINEERING)
        _emit({
            "ok": False,
            "code": error.code,
            "reason_category": error.reason_category,
            "message": str(error),
            "database_io_attempted": False,
        }, error=True)
        return exit_code
    result.update({
        "ok": result.get("status") in ("AVAILABLE", "COMPLETE_WITH_DISCLOSURES"),
        "configuration_path": str(config_path),
        "database_io_attempted": False,
        "database_write_attempted": False,
    })
    if not compare and getattr(args, "output_format", "json") == "summary":
        sys.stdout.write(_render_basic_fact_summary(result) + "\n")
    else:
        _emit(result)
    return _EXIT_OK if result["ok"] else _EXIT_UNAVAILABLE


def _summary_field(value: Any) -> str:
    if not isinstance(value, dict):
        return "—"
    quality = value.get("quality", "UNKNOWN_DATA")
    if quality != "OBSERVED":
        return f"未知:{value.get('reason') or '原因未明'}"
    rendered = value.get("value")
    if isinstance(rendered, dict):
        rendered = rendered.get("decimal", rendered.get("exact"))
    return f"{rendered} [{quality}]"


def _summary_fact(value: Any) -> str:
    if not isinstance(value, dict):
        return "—"
    value_payload = value.get("value", {})
    unit_trust = value.get("unit_trust")
    if not isinstance(unit_trust, dict) and isinstance(value_payload, dict):
        unit_trust = value_payload.get("unit_trust")
    unit_note = ""
    if isinstance(unit_trust, dict):
        interpretation = unit_trust.get("interpretation")
        if interpretation:
            unit_note = "；单位假设:" + str(interpretation)
        elif unit_trust.get("status") == "CALLER_DECLARED_ASSUMPTION":
            unit_note = "；单位假设:调用方声明"
    return (
        f"{value.get('field_id', 'unknown')}={_summary_field(value_payload)}"
        f" [公式/来源:{value.get('formula_source', '未登记')}"
        f"；坐标:{value.get('coordinate_role', '未登记')}"
        f"{'; 身份:' + value['coordinate_id'] if value.get('coordinate_id') else ''}"
        f"{unit_note}]"
    )


def _powershell_quote(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _render_basic_fact_summary(result: dict[str, Any]) -> str:
    dimensions = result.get("basic_fact_dimensions", {})
    groups = dimensions.get("dimensions", {})
    source_quality = dimensions.get("source_quality", {})
    raw = source_quality.get("raw_availability", {})
    core = source_quality.get("core_availability", {})
    lines = [
        f"基础量价研究解释 | {result.get('requested_symbol', '未知代码')} | 目标日 {result.get('target_session', '未知')} | NON_PIT_RETROSPECTIVE",
        (
            f"来源末日 {result.get('source_last_session', '未知')}；本机只读扫描 "
            f"{result.get('read_time', {}).get('started_at', '未知')} 至 "
            f"{result.get('read_time', {}).get('completed_at', '未知')}"
        ),
        (
            f"身份 {result.get('identity', {}).get('status', 'SOURCE_SYMBOL_ONLY')}；"
            f"价格坐标 {result.get('adjustment', {}).get('coordinate', 'RAW_UNADJUSTED')}；"
            "来源权威/首捕时钟未绑定；这是历史研究预览，不是实时/PIT数据或正式Profile。"
        ),
    ]
    group_labels = (
        ("price_trend", "价格与趋势"),
        ("position_volatility", "位置与波动"),
        ("participation", "量额参与"),
    )
    for key, label in group_labels:
        facts = groups.get(key, {})
        rendered = "；".join(_summary_fact(item) for item in facts.values()) or "无可显示字段"
        lines.append(f"{label}：{rendered}")
    lines.append(
        f"可用性：兼容状态={result.get('status')}；"
        f"Raw={raw.get('status')} {raw.get('observed_fields', 0)}/{raw.get('total_fields', 0)}；"
        f"Core={core.get('status')} {core.get('observed_fields', 0)}/{core.get('total_fields', 0)}"
    )
    window = source_quality.get("window_context", {})
    if window:
        lines.append(
            f"回看窗口：日历={window.get('calendar_revision', '未知')}，"
            f"会话={window.get('calendar_session_count', 0)}，"
            f"实际bar={window.get('actual_price_bars', 0)}，"
            f"缺bar会话={window.get('missing_bar_sessions', 0)}，"
            f"Core回看完整={window.get('lookback_complete')}，"
            f"原因={window.get('lookback_reason') or '无'}，"
            f"使用目标日后记录={window.get('future_rows_used')}"
        )
    unknowns = source_quality.get("unknown_facts_by_dimension", {})
    for key, label in group_labels:
        reasons = unknowns.get(key, {})
        if reasons:
            lines.append(f"{label}未知原因：" + "；".join(f"{field_id}={reason}" for field_id, reason in reasons.items()))
    explanation = result.get("availability_explanation", {})
    if explanation.get("missing_target_bar_explanation"):
        lines.append("目标日行情：" + explanation["missing_target_bar_explanation"])
    source = result.get("source", {})
    budgets = source.get("budgets", {})
    if budgets:
        lines.append(
            f"读取预算：{source.get('byte_count', 0)}/{budgets.get('max_bytes', 0)}字节，"
            f"{source.get('record_count', 0)}/{budgets.get('max_records', 0)}条；"
            "金额/数量单位为调用方声明假设。"
        )
    preview = result.get("profile_preview")
    if preview:
        lines.append(
            f"基础画像组件预览：{preview['counts']['observed']}项有值，"
            f"{preview['counts']['unknown_data']}项数据未知，"
            f"{preview['counts']['not_implemented'] + preview['counts']['contract_unbound']}项能力未接通；"
            "全部仅为RAW_UNADJUSTED/NON_PIT描述预览，正式profile()仍阻断。"
        )
        for component in preview["components"]:
            if component["status"] == "EVALUATED":
                result_text = component["value"] if component["quality"] == "OBSERVED" else component.get("reason")
                inputs = "；".join(
                    f"{item['rule_input_field']}←{item['source_field_id']}({item['value']['quality']})"
                    for item in component["inputs"]
                )
                lines.append(
                    f"{component['field_id']}：{component['quality']}={result_text}；"
                    f"规则={component['rule_id']}；输入={inputs or '无'}"
                )
            else:
                missing = "、".join(item["field_id"] for item in component.get("missing_inputs", []))
                lines.append(
                    f"{component['field_id']}：{component['status']}；缺少={missing or '未登记生产者'}；"
                    f"原因={component['reason']}"
                )
    return "\n".join(lines)


def _render_universe_summary(result: dict[str, Any]) -> str:
    discovery = result["discovery"]
    denominators = result["denominators"]
    page = result["page"]
    budget = result["budget"]
    lines = [
        f"TDX来源候选盘后观察 | {result['target_session']} | {result['candidate_contract']} | {result['price_coordinate']}",
        (
            f"来源目录项={discovery['directory_entries_scanned']}，.day文件={discovery['day_file_entries']}，"
            f"范围候选={denominators['source_candidate_total']}，越界类别排除={discovery['excluded_out_of_scope']}，"
            f"无效文件名={discovery['invalid_day_filenames']}"
        ),
        (
            f"过滤后分母={denominators['filtered_candidate_total']}；本页offset={page['offset']}、"
            f"limit={page['limit']}、返回={page['returned']}、页后剩余={page['remaining_after_page']}、"
            f"本页未评价={denominators['page_not_evaluated']}、全过滤集合未评价={denominators['not_evaluated_in_filtered_candidate_set']}"
        ),
        (
            f"本页分类 available={denominators['page_available']}、failed={denominators['page_failed']}、"
            f"partial_unknown={denominators['page_partial_unknown']}、not_evaluated={denominators['page_not_evaluated']}；"
            f"Raw/Core状态见逐行列"
        ),
        (
            f"读取={budget['used_bytes']}字节/{budget['used_records']}条，"
            f"发现={budget['discovery_elapsed_seconds']}秒，总耗时={budget['elapsed_seconds']}秒；"
            f"未复权、NON_PIT，不代表全候选池已读取或全部指标已知"
        ),
        "代码 | 市场/板别 | 来源末日 | Raw收盘 | MA5 | MA20 | MA60 | ATR20 | ret5 | ret20 | 收盘/MA20 | Amount | Volume | Raw/Core | 未知原因/错误 | 单股回查",
    ]
    for item in result["summary"]:
        raw = item.get("raw_availability") or {}
        core = item.get("core_availability") or {}
        raw_text = f"{raw.get('status', '未评价')} {raw.get('observed_fields', 0)}/{raw.get('total_fields', 0)}"
        core_text = f"{core.get('status', '未评价')} {core.get('observed_fields', 0)}/{core.get('total_fields', 0)}"
        reasons = dict(core.get("unknown_reasons", {}))
        for reason, count in (raw.get("unknown_reasons") or {}).items():
            reasons[f"Raw:{reason}"] = count
        reason_text = ";".join(f"{name}×{count}" for name, count in sorted(reasons.items()))
        if item.get("error_code"):
            reason_text = ";".join(filter(None, (reason_text, item["error_code"])))
        detail = item.get("detail_query", {})
        detail_command = "stock-a " + " ".join(
            _powershell_quote(value) for value in detail.get("arguments", [])
        ) if detail else "—"
        lines.append(" | ".join((
            item["symbol"],
            f"{item['market']}/{item['board']}",
            item.get("source_last_session") or "—",
            _summary_field(item.get("raw_close")),
            _summary_field(item.get("ma5")),
            _summary_field(item.get("ma20")),
            _summary_field(item.get("ma60")),
            _summary_field(item.get("atr20")),
            _summary_field(item.get("ret5")),
            _summary_field(item.get("ret20")),
            _summary_field(item.get("close_vs_ma20")),
            _summary_field(item.get("amount")),
            _summary_field(item.get("volume")),
            f"{raw_text}/{core_text}",
            reason_text or "—",
            detail_command,
        )))
    return "\n".join(lines)


def _run_research_universe(args: argparse.Namespace) -> int:
    config, config_path = _load_runtime_config()
    roots = config["source_policy"]["configured_tdx_source_roots"]
    if not roots:
        raise DiagnosticError("SOURCE_NOT_CONFIGURED", "配置中没有绑定TDX只读来源根，拒绝发现来源候选")
    try:
        units = UnitContract(args.unit_version, Decimal(args.amount_to_cny), args.volume_to_shares)
        limits = ReadLimits(
            args.max_bytes,
            args.max_records,
            args.chunk_records,
            Decimal(args.max_seconds),
        )
        discovery_budget = UniverseDiscoveryBudget(
            args.max_directory_entries,
            args.max_discovery_seconds,
        )
        budget = CompareBudget(
            args.max_total_bytes,
            args.max_total_records,
            args.max_total_seconds,
        )
    except (InvalidOperation, ValueError, ContractError, ResearchEntryError) as error:
        raise DiagnosticError("ARGUMENT_CONTRACT_INVALID", f"单位或预算合同无效：{error}", _EXIT_ARGUMENT) from error
    try:
        result = research_universe_preview(
            target_session=args.target_session,
            source_roots=roots,
            units=units,
            limits=limits,
            offset=args.offset,
            limit=args.limit,
            market_filter=args.market,
            board_filter=args.board,
            discovery_budget=discovery_budget,
            budget=budget,
        )
    except ResearchEntryError as error:
        if error.code == "SOURCE_ACCESS_DENIED":
            code = _EXIT_PERMISSION
        elif error.code == "SOURCE_IO_ERROR":
            code = _EXIT_ENGINEERING
        elif error.code in {
            "TARGET_DATE_INVALID", "UNIVERSE_PAGE_INVALID", "UNIVERSE_FILTER_INVALID",
            "UNIVERSE_BUDGET_INVALID", "COMPARE_BUDGET_INVALID",
        }:
            code = _EXIT_ARGUMENT
        else:
            code = _EXIT_UNAVAILABLE
        _emit({
            "ok": False,
            "code": error.code,
            "message": str(error),
            "context": error.context,
            "database_io_attempted": False,
            "database_write_attempted": False,
        }, error=True)
        return code
    result.update({
        "ok": result["status"] == "COMPLETE_WITH_DISCLOSURES",
        "configuration_path": str(config_path),
        "database_io_attempted": False,
        "database_write_attempted": False,
    })
    if args.output_format == "summary":
        sys.stdout.write(_render_universe_summary(result) + "\n")
    else:
        _emit(result)
    return _EXIT_OK if result["ok"] else _EXIT_UNAVAILABLE


def main(argv: list[str] | None = None) -> int:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")
    parser = _parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return _EXIT_OK
    try:
        if args.command == "doctor":
            return _run_doctor(args)
        if args.command == "pg-identity":
            return _run_pg_identity(args)
        if args.command == "pg-bind":
            return _run_pg_bind(args)
        if args.command == "source-phase0":
            return _run_source_phase0(args)
        if args.command == "source-inspect":
            return _run_source_inspect(args)
        if args.command == "research-stock":
            return _run_research(args, compare=False)
        if args.command == "research-compare":
            return _run_research(args, compare=True)
        if args.command == "research-universe-preview":
            return _run_research_universe(args)
    except DiagnosticError as error:
        _emit({"ok": False, "code": error.code, "cause_type": type(error.__cause__).__name__ if error.__cause__ is not None else None, "message": str(error)}, error=True)
        return error.exit_code
    except KeyboardInterrupt:
        _emit({"ok": False, "code": "OPERATION_INTERRUPTED", "message": "操作已中断，未生成完整来源摘要"}, error=True)
        return _EXIT_ENGINEERING
    except Exception as error:
        _emit({"ok": False, "code": "ENGINEERING_ERROR", "cause_type": type(error).__name__, "message": f"诊断执行失败：{type(error).__name__}"}, error=True)
        return _EXIT_ENGINEERING
    return _EXIT_ENGINEERING
