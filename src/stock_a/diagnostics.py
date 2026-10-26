"""新项目本地诊断与受控来源只读检查。"""
from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import importlib.metadata
import json
import os
from pathlib import Path
import sys
from typing import Any

from stock_a import __version__
from stock_a.domain import ContractError
from stock_a.sources.tdx.day import UnitContract
from stock_a.sources.tdx.day_source import open_day_file
from stock_a.sources.tdx.day_stream import DayReadError, ReadLimits


_WORKSPACE = Path("G:/codex work/Stock A")
_CONFIG_ROOT = _WORKSPACE / "config"
_CONFIG_FILE = _CONFIG_ROOT / "runtime.defaults.json"
_CONFIG_CONTRACT = "STOCK-A-RUNTIME-DEFAULTS/R8"
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
        except OSError as error:
            raise DiagnosticError("CONFIG_PATH_REJECTED", "无法核验显式配置路径") from error
    try:
        config_root = _CONFIG_ROOT.resolve(strict=True)
        config_path = _CONFIG_FILE.resolve(strict=True)
        if config_root != _CONFIG_ROOT or not _inside(config_path, config_root):
            raise DiagnosticError("CONFIG_PATH_REJECTED", "正式配置根或配置文件经链接离开唯一配置槽位")
        if not config_path.is_file():
            raise DiagnosticError("CONFIG_MISSING", "唯一正式运行配置不是普通文件")
    except FileNotFoundError as error:
        raise DiagnosticError("CONFIG_MISSING", "唯一正式运行配置或配置根不存在") from error
    except OSError as error:
        raise DiagnosticError("CONFIG_PATH_REJECTED", "无法核验正式配置根及最终路径") from error
    try:
        with config_path.open("r", encoding="utf-8") as stream:
            config = json.load(stream, parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
    except PermissionError as error:
        raise DiagnosticError("CONFIG_PERMISSION_DENIED", "操作系统拒绝读取正式配置", _EXIT_PERMISSION) from error
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as error:
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
    registered = policy.get("public_files")
    expected = {
        ("runtime.defaults.json", "BUSINESS_RUNTIME_CONFIGURATION", _CONFIG_CONTRACT),
        ("contracts/day-chain.contract.json", "BUSINESS_PIPELINE_DESIGN_CONTRACT", "STOCK-A-DAY-CHAIN/R10"),
    }
    if not isinstance(registered, list):
        raise DiagnosticError("CONFIG_INVALID", "configuration_policy.public_files必须是登记清单")
    actual = set()
    for item in registered:
        if not isinstance(item, dict) or not all(isinstance(item.get(key), str) for key in ("path", "role", "contract", "consumer")):
            raise DiagnosticError("CONFIG_INVALID", "公共配置登记项字段不完整")
        actual.add((item["path"], item["role"], item["contract"]))
    if actual != expected:
        raise DiagnosticError("CONFIG_INVALID", "公共配置登记清单与已批准配置槽位不一致")
    sources = _require_mapping(root.get("source_policy"), "source_policy")
    roots = sources.get("configured_tdx_source_roots")
    if not isinstance(roots, list) or any(not isinstance(item, str) or not Path(item).is_absolute() for item in roots):
        raise DiagnosticError("CONFIG_INVALID", "TDX来源根必须是绝对路径字符串列表")
    app = _require_mapping(root.get("application_identity"), "application_identity")
    if app.get("application_id") != "STOCK_A" or not isinstance(app.get("project_uuid"), str):
        raise DiagnosticError("CONFIG_INVALID", "应用身份声明不完整")
    if type(app.get("deployment_applied")) is not bool:
        raise DiagnosticError("CONFIG_INVALID", "deployment_applied必须是布尔值")
    for key in ("deployment_instance_uuid", "cluster_system_identifier"):
        if not isinstance(app.get(key), str):
            raise DiagnosticError("CONFIG_INVALID", f"应用身份{key}类型错误")
    roles = app.get("roles")
    if not isinstance(roles, list) or not roles or any(not isinstance(role, str) or not role for role in roles):
        raise DiagnosticError("CONFIG_INVALID", "应用角色声明必须是非空字符串列表")
    database = _require_mapping(root.get("postgresql"), "postgresql")
    for key in ("database", "business_schema"):
        if not isinstance(database.get(key), str) or not database[key]:
            raise DiagnosticError("CONFIG_INVALID", f"数据库声明{key}缺失或类型错误")
    service_role = _require_mapping(root.get("startup_policy"), "startup_policy")
    for key in ("require_explicit_validated_configuration", "require_pg_data_directory_match", "allow_legacy_postgres_writes"):
        if type(service_role.get(key)) is not bool:
            raise DiagnosticError("CONFIG_INVALID", f"启动策略{key}必须是布尔值")


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
        "service_role": {"configured_roles": app["roles"], "actual_session_role": "NOT_CONNECTED_NOT_VERIFIED"},
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
    text = json.dumps(payload, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    print(text, file=sys.stderr if error else sys.stdout)


def _parser() -> argparse.ArgumentParser:
    class ChineseParser(argparse.ArgumentParser):
        def error(self, message: str) -> None:
            _emit({"ok": False, "code": "ARGUMENT_ERROR", "message": "参数错误：请核对命令格式及必需参数"}, error=True)
            raise SystemExit(_EXIT_ARGUMENT)

    parser = ChineseParser(prog="stock-a", description="Stock A中文诊断与TDX只读来源检查")
    commands = parser.add_subparsers(dest="command", parser_class=ChineseParser)
    doctor = commands.add_parser("doctor", help="诊断本地配置、包身份和声明能力")
    doctor.add_argument("--config", help="仅接受唯一登记配置runtime.defaults.json的绝对路径")
    inspect = commands.add_parser("source-inspect", help="只读检查单个TDX日线文件")
    inspect.add_argument("path", help="待检查日线文件的绝对路径")
    inspect.add_argument("--unit-version", required=True, help="显式日线单位合同版本")
    inspect.add_argument("--amount-to-cny", required=True, help="金额到元的精确十进制倍率")
    inspect.add_argument("--volume-to-shares", required=True, type=int, help="数量到股的正整数倍率")
    inspect.add_argument("--max-bytes", required=True, type=int, help="本次最大读取字节数")
    inspect.add_argument("--max-records", required=True, type=int, help="本次最大记录数")
    inspect.add_argument("--chunk-records", default=256, type=int, help="单次读取块记录数，默认256")
    inspect.add_argument("--max-seconds", default="30", help="本次最大读取秒数，默认30")
    return parser


def _run_doctor(args: argparse.Namespace) -> int:
    package = _module_mode()
    config, config_path = _load_runtime_config(args.config)
    snapshot = _configuration_snapshot(config, config_path)
    _emit({
        "ok": False,
        "status": "LOCAL_DIAGNOSTIC_ONLY",
        "message": "本地包与配置结构已检查；未核验真实来源或数据库连接，不允许据此启动业务",
        "package": package,
        **snapshot,
        "business_start_permitted": False,
        "database_connection_attempted": False,
    })
    return _EXIT_UNAVAILABLE


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
        exit_code = _EXIT_PERMISSION if error.code == "SOURCE_ACCESS_DENIED" else _EXIT_UNAVAILABLE
        _emit({"ok": False, "code": error.code, "reason_category": error.reason_category, "message": str(error), "record_index": error.record_index, "byte_offset": error.byte_offset}, error=True)
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
        if args.command == "source-inspect":
            return _run_source_inspect(args)
    except DiagnosticError as error:
        _emit({"ok": False, "code": error.code, "message": str(error)}, error=True)
        return error.exit_code
    except Exception as error:
        _emit({"ok": False, "code": "ENGINEERING_ERROR", "message": f"诊断执行失败：{type(error).__name__}"}, error=True)
        return _EXIT_ENGINEERING
    return _EXIT_ENGINEERING
