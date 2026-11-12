"""Stock A命令行source_display_names职责模块。"""

from __future__ import annotations

import argparse

from stock_a.application.source_symbol_display_names import (
    SourceSymbolDisplayNameApplicationError,
    capture_source_symbol_display_names,
)
from stock_a.application.runtime_config import (
    DiagnosticError,
    _EXIT_ARGUMENT,
    _EXIT_ENGINEERING,
    _EXIT_OK,
    _EXIT_UNAVAILABLE,
    load_runtime_config as _load_runtime_config,
)
from stock_a.application.cli_commands.common import _emit


def run_source_symbol_display_names(args: argparse.Namespace) -> int:
    if args.confirm_task_scope != "R06_SYMBOL_DISPLAY_NAMES_R1":
        raise DiagnosticError(
            "TASK_SCOPE_REQUIRED", "名称标签写入必须绑定当前R06任务范围", _EXIT_ARGUMENT
        )
    if args.persist is not True:
        raise DiagnosticError(
            "DISPLAY_NAME_PERSIST_REQUIRED",
            "数据库中文显示标签写入必须显式指定--persist",
            _EXIT_ARGUMENT,
        )
    config, config_path = _load_runtime_config()
    try:
        result = capture_source_symbol_display_names(config, persist=True)
    except SourceSymbolDisplayNameApplicationError as error:
        unavailable = error.code.startswith(
            "DISPLAY_NAME_SOURCE"
        ) or error.code.endswith("NOT_BOUND")
        _emit(
            {
                "ok": False,
                "code": error.code,
                "message": str(error),
                "database_write_attempted": False,
                "configuration_path": str(config_path),
            },
            error=True,
        )
        return _EXIT_UNAVAILABLE if unavailable else _EXIT_ENGINEERING
    result.update(
        {
            "ok": True,
            "configuration_path": str(config_path),
            "database_write_attempted": True,
        }
    )
    _emit(result)
    return _EXIT_OK


__all__ = ["run_source_symbol_display_names"]
