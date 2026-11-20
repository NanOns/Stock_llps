"""显式本地TDX日线文件只读检查命令。"""

from __future__ import annotations

import argparse
from dataclasses import asdict
from decimal import Decimal, InvalidOperation
from pathlib import Path

from stock_a.domain import ContractError
from stock_a.sources.tdx.day import UnitContract
from stock_a.sources.tdx.day_source import open_day_file
from stock_a.sources.tdx.day_stream import DayReadError, ReadLimits
from stock_a.application.runtime_config import (
    DiagnosticError,
    _EXIT_ARGUMENT,
    _EXIT_ENGINEERING,
    _EXIT_PERMISSION,
    _EXIT_OK,
    load_runtime_config as _load_runtime_config,
)
from stock_a.application.cli_commands.common import _READ_ERROR_EXIT_CODES, _emit


def _run_source_inspect(args: argparse.Namespace) -> int:
    if not Path(args.path).is_absolute():
        raise DiagnosticError("SOURCE_PATH_INVALID", "来源文件必须使用绝对路径")
    config, config_path = _load_runtime_config()
    roots = config["source_policy"]["configured_tdx_source_roots"]
    if not roots:
        raise DiagnosticError(
            "SOURCE_NOT_CONFIGURED", "配置中没有绑定TDX来源保护根，拒绝打开任何来源文件"
        )
    try:
        amount_scale = Decimal(args.amount_to_cny)
        seconds = Decimal(args.max_seconds)
        units = UnitContract(args.unit_version, amount_scale, args.volume_to_shares)
        limits = ReadLimits(
            args.max_bytes, args.max_records, args.chunk_records, seconds
        )
    except (InvalidOperation, ValueError, ContractError) as error:
        raise DiagnosticError(
            "ARGUMENT_CONTRACT_INVALID", f"单位或读取预算无效：{error}", _EXIT_ARGUMENT
        ) from error
    try:
        with open_day_file(args.path, roots, units, limits) as scan:
            for _located in scan:
                pass
            file_summary = scan.summary
            summary = file_summary.scan
            source = file_summary.source
    except DayReadError as error:
        exit_code = _READ_ERROR_EXIT_CODES.get(error.code, _EXIT_ENGINEERING)
        _emit(
            {
                "ok": False,
                "code": error.code,
                "reason_category": error.reason_category,
                "cause_type": type(error.__cause__).__name__
                if error.__cause__ is not None
                else None,
                "message": str(error),
                "record_index": error.record_index,
                "byte_offset": error.byte_offset,
            },
            error=True,
        )
        return exit_code
    except PermissionError as error:
        raise DiagnosticError(
            "SOURCE_ACCESS_DENIED", "操作系统拒绝读取来源文件", _EXIT_PERMISSION
        ) from error
    except OSError as error:
        raise DiagnosticError(
            "SOURCE_IO_ERROR", "读取来源时发生操作系统I/O错误", _EXIT_ENGINEERING
        ) from error
    _emit(
        {
            "ok": True,
            "status": "FILE_READ_COMPLETE_ONLY",
            "message": "指定日线文件已只读扫描至EOF并通过最终句柄核验；不代表来源封口、PIT或阶段0通过",
            "configuration_path": str(config_path),
            "complete": summary.complete,
            "record_count": summary.record_count,
            "byte_count": summary.byte_count,
            "first_date": summary.first_date.isoformat(),
            "last_date": summary.last_date.isoformat(),
            "unit_version": summary.unit_version,
            "groups": {
                name: asdict(getattr(summary, name))
                for name in ("prices", "amount", "volume")
            },
            "read_started_at": summary.read_started_at.isoformat(),
            "read_completed_at": summary.read_completed_at.isoformat(),
            "read_clock_role": "LOCAL_FILE_READ_TIME_ONLY",
            "source": asdict(source),
            "budgets": {
                "max_bytes": limits.max_bytes,
                "max_records": limits.max_records,
                "chunk_records": limits.chunk_records,
                "max_seconds": limits.max_seconds,
            },
        }
    )
    return _EXIT_OK


__all__ = ["_run_source_inspect"]
