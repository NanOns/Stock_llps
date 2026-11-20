"""仅承载显式本地TDX离线只读诊断CLI。"""

from __future__ import annotations

import argparse
from decimal import Decimal, InvalidOperation
import sys

from stock_a.contracts import PriceProfileContractError, load_price_profile_contract
from stock_a.domain import ContractError
from stock_a.sources.tdx.day import UnitContract
from stock_a.sources.tdx.day_stream import DayReadError, ReadLimits
from stock_a.research.entry import (
    CompareBudget,
    ResearchEntryError,
    research_compare,
    research_stock,
)
from stock_a.application.runtime_config import (
    DiagnosticError,
    _CONFIG_ROOT,
    _EXIT_ARGUMENT,
    _EXIT_ENGINEERING,
    _EXIT_OK,
    _EXIT_PERMISSION,
    _EXIT_UNAVAILABLE,
    load_runtime_config as _load_runtime_config,
)
from stock_a.application.cli_commands.common import _READ_ERROR_EXIT_CODES, _emit
from stock_a.application.cli_commands.research.research_output import (
    _render_basic_fact_summary,
)


def _run_research(args: argparse.Namespace, *, compare: bool) -> int:
    if not getattr(args, "local_tdx_diagnostic", False):
        raise DiagnosticError(
            "LOCAL_TDX_MODE_REQUIRED",
            "本命令仅支持显式本地TDX离线只读诊断；股票价格生产与PG查询由正式服务和DataJobCoordinator提供。",
            _EXIT_ARGUMENT,
        )
    local_limit = getattr(args, "local_tdx_max_sessions", 60)
    if not compare and (type(local_limit) is not int or not 1 <= local_limit <= 75):
        raise DiagnosticError(
            "LOCAL_TDX_SESSION_LIMIT_INVALID",
            "本地TDX诊断记录上限必须为1至75",
            _EXIT_ARGUMENT,
        )

    config, config_path = _load_runtime_config()
    roots = tuple(config["source_policy"]["configured_tdx_source_roots"])
    if not roots:
        raise DiagnosticError(
            "SOURCE_NOT_CONFIGURED", "配置中没有绑定TDX离线诊断根，拒绝打开来源文件"
        )
    try:
        units = UnitContract(
            args.unit_version,
            Decimal(args.amount_to_cny) if args.amount_to_cny is not None else None,
            args.volume_to_shares,
        )
        limits = ReadLimits(
            args.max_bytes,
            min(args.max_records, local_limit) if not compare else args.max_records,
            args.chunk_records,
            Decimal(args.max_seconds),
        )
    except (InvalidOperation, ValueError, ContractError) as error:
        raise DiagnosticError(
            "ARGUMENT_CONTRACT_INVALID", f"单位或读取预算无效：{error}", _EXIT_ARGUMENT
        ) from error

    profile_contract = None
    if not compare and getattr(args, "include_profile_preview", False):
        try:
            profile_contract = load_price_profile_contract(_CONFIG_ROOT).document
        except PriceProfileContractError as error:
            _emit(
                {
                    "ok": False,
                    "code": error.code,
                    "message": str(error),
                    "database_io_attempted": False,
                },
                error=True,
            )
            return _EXIT_UNAVAILABLE

    try:
        if compare:
            result = research_compare(
                symbols=args.symbols,
                target_session=args.target_session,
                source_roots=roots,
                units=units,
                limits=limits,
                budget=CompareBudget(
                    args.max_total_bytes,
                    args.max_total_records,
                    args.max_total_seconds,
                ),
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
        code = (
            _EXIT_PERMISSION
            if error.code == "SOURCE_ACCESS_DENIED"
            else _EXIT_ENGINEERING
            if error.code == "SOURCE_IO_ERROR"
            else _EXIT_ARGUMENT
            if error.code
            in {
                "TARGET_DATE_INVALID",
                "SYMBOL_INVALID",
                "SYMBOL_OUT_OF_SCOPE",
                "COMPARE_SIZE_INVALID",
                "DUPLICATE_SYMBOL",
            }
            else _EXIT_UNAVAILABLE
        )
        _emit(
            {
                "ok": False,
                "code": error.code,
                "message": str(error),
                "database_io_attempted": False,
                "database_write_attempted": False,
            },
            error=True,
        )
        return code
    except DayReadError as error:
        _emit(
            {
                "ok": False,
                "code": error.code,
                "reason_category": error.reason_category,
                "message": str(error),
                "database_io_attempted": False,
                "database_write_attempted": False,
            },
            error=True,
        )
        return _READ_ERROR_EXIT_CODES.get(error.code, _EXIT_ENGINEERING)

    result.update(
        {
            "ok": result.get("status") in ("AVAILABLE", "COMPLETE_WITH_DISCLOSURES"),
            "configuration_path": str(config_path),
            "source_access": {
                "mode": "EXPLICIT_LOCAL_TDX_DIAGNOSTIC_READ_ONLY",
                "configured_tdx_root_count": len(roots),
                "database_io_attempted": False,
                "database_write_attempted": False,
                "production_price_source": False,
            },
        }
    )
    if not compare and getattr(args, "output_format", "json") == "summary":
        sys.stdout.write(_render_basic_fact_summary(result) + "\n")
    else:
        _emit(result, error=not result["ok"])
    return _EXIT_OK if result["ok"] else _EXIT_UNAVAILABLE
