"""Stock A命令入口共享的诊断、安全路径和JSON输出边界。"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation, localcontext
import importlib.metadata
import json
from pathlib import Path
import re
import stat
import sys
from typing import Any
from stock_a import __version__
from stock_a.application.runtime_config import (
    DiagnosticError,
    _CONFIG_ROOT,
    _EXIT_ARGUMENT,
    _EXIT_ENGINEERING,
    _EXIT_OK,
    _EXIT_PERMISSION,
    _EXIT_UNAVAILABLE,
    _WORKSPACE,
    load_runtime_config as _load_runtime_config,
)

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


def _module_mode() -> dict[str, str]:
    module_path = Path(__file__).resolve().parent.parent / "cli.py"
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
            installed = {
                Path(distribution.locate_file(item)).resolve() for item in files
            }
            if module_path not in installed:
                raise DiagnosticError(
                    "PACKAGE_IDENTITY_INVALID",
                    "无法核验当前模块属于已安装的stock-a发行包",
                )
        except importlib.metadata.PackageNotFoundError as error:
            raise DiagnosticError(
                "PACKAGE_IDENTITY_INVALID", "当前模块不在开发源码根，也未登记为已安装包"
            ) from error
    return {"module_path": str(module_path), "version": __version__, "mode": mode}


def _configuration_snapshot(
    config: dict[str, Any], config_path: Path
) -> dict[str, Any]:
    app = config["application_identity"]
    paths = config["paths"]
    roots = [
        str(Path(item).resolve())
        for item in config["source_policy"]["configured_tdx_source_roots"]
    ]
    bound = (
        app["deployment_applied"] is True
        and app["deployment_instance_uuid"] != "BIND_DURING_EXPLICIT_ENABLEMENT"
        and app["cluster_system_identifier"] != "BIND_DURING_EXPLICIT_ENABLEMENT"
    )
    return {
        "configuration_path": str(config_path),
        "configuration_contract": config["contract"],
        "workspace_root": paths["workspace_root"],
        "service_role": {
            "business_login": app["business_login"],
            "actual_session_role": "NOT_CONNECTED_NOT_VERIFIED",
        },
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
            "identity_status": "DECLARED_BOUND_NOT_CONNECTED"
            if bound
            else "UNBOUND_NOT_VERIFIED",
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
    text = json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        default=_json_default,
    )
    print(text, file=sys.stderr if error else sys.stdout)


def _safe_child(path: Path, root: Path, *, optional: bool = False) -> Path | None:
    try:
        resolved = path.resolve(strict=True)
        resolved.relative_to(root)
        mode = resolved.stat().st_mode
    except FileNotFoundError as error:
        if optional:
            return None
        raise DiagnosticError("SOURCE_PATH_INVALID", "来源目录不存在") from error
    except PermissionError as error:
        if optional:
            return None
        raise DiagnosticError(
            "SOURCE_ACCESS_DENIED", "操作系统拒绝核验来源目录", _EXIT_PERMISSION
        ) from error
    except OSError as error:
        if optional:
            return None
        raise DiagnosticError(
            "SOURCE_IO_ERROR", "核验来源目录时发生工程I/O错误", _EXIT_ENGINEERING
        ) from error
    except ValueError as error:
        if optional:
            return None
        raise DiagnosticError(
            "SOURCE_PATH_INVALID", "来源目录不存在或最终路径离开保护根"
        ) from error
    if not stat.S_ISDIR(mode):
        if optional:
            return None
        raise DiagnosticError("SOURCE_PATH_INVALID", "来源目录结构不符合TDX目录合同")
    return resolved


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
        raise DiagnosticError(
            "SOURCE_ACCESS_DENIED", "操作系统拒绝核验来源文件", _EXIT_PERMISSION
        ) from error
    except OSError as error:
        raise DiagnosticError(
            "SOURCE_IO_ERROR", "核验来源文件时发生工程I/O错误", _EXIT_ENGINEERING
        ) from error
    except ValueError as error:
        raise DiagnosticError(
            "SOURCE_PATH_INVALID", "来源文件最终路径离开保护根"
        ) from error
    if not stat.S_ISREG(mode):
        raise DiagnosticError("SOURCE_PATH_INVALID", "来源目标不是普通文件")
    return resolved


def _source_file_size(path: Path) -> int:
    """读取来源大小元数据，并保留来源缺失、权限拒绝与工程I/O的区别。"""
    try:
        return path.stat().st_size
    except FileNotFoundError as error:
        raise DiagnosticError(
            "SOURCE_NOT_FOUND", "来源文件在大小核验时已不存在", _EXIT_UNAVAILABLE
        ) from error
    except PermissionError as error:
        raise DiagnosticError(
            "SOURCE_ACCESS_DENIED", "操作系统拒绝读取来源文件大小", _EXIT_PERMISSION
        ) from error
    except OSError as error:
        raise DiagnosticError(
            "SOURCE_IO_ERROR", "读取来源文件大小时发生工程I/O错误", _EXIT_ENGINEERING
        ) from error


def _parse_source_set_date(value: str) -> date:
    if (
        not isinstance(value, str)
        or re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value) is None
    ):
        raise DiagnosticError(
            "TARGET_SESSION_INVALID",
            "目标交易会话必须使用YYYY-MM-DD格式",
            _EXIT_ARGUMENT,
        )
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise DiagnosticError(
            "TARGET_SESSION_INVALID", "目标交易会话不是有效日期", _EXIT_ARGUMENT
        ) from error
    if parsed.isoformat() != value:
        raise DiagnosticError(
            "TARGET_SESSION_INVALID",
            "目标交易会话必须使用标准ISO日期",
            _EXIT_ARGUMENT,
        )
    return parsed
