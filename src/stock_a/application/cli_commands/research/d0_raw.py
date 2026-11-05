"""来源股票D0原始场景只读CLI命令。"""

from __future__ import annotations

import argparse

from stock_a.application.cli_commands.common import (
    _emit,
    _parse_session_date,
)
from stock_a.application.runtime_config import (
    _EXIT_ARGUMENT,
    _EXIT_ENGINEERING,
    _EXIT_OK,
    _EXIT_UNAVAILABLE,
    DiagnosticError,
    load_runtime_config,
)
from stock_a.application.source_d0_raw import (
    SourceD0RawError,
    query_source_d0_raw_launch,
)


def _run_source_d0_raw(args: argparse.Namespace) -> int:
    try:
        config, config_path = load_runtime_config()
        result = query_source_d0_raw_launch(
            config,
            scenario=args.scenario,
            target_session=_parse_session_date(args.target_session),
            research_cutoff=_parse_session_date(args.research_cutoff),
            package_sha256=args.package_sha256,
            state=args.state,
            symbol=args.symbol,
            offset=args.offset,
            limit=args.limit,
            action=args.action,
            before_session=(
                _parse_session_date(args.before_session)
                if args.before_session is not None
                else None
            ),
            before_package_sha256=args.before_package_sha256,
            transition=args.transition,
        )
    except DiagnosticError as error:
        _emit(
            {"ok": False, "code": error.code, "message": str(error)},
            error=True,
        )
        return error.exit_code
    except (SourceD0RawError, ValueError) as error:
        category = getattr(error, "category", "ARGUMENT")
        _emit(
            {
                "ok": False,
                "code": getattr(error, "code", "D0_RAW_ARGUMENT_OR_DATA_ERROR"),
                "message": str(error),
                "database_write_attempted": False,
                "ddl_executed": False,
                "dml_executed": False,
            },
            error=True,
        )
        return _EXIT_ARGUMENT if category == "ARGUMENT" else _EXIT_UNAVAILABLE
    except Exception as error:
        _emit(
            {
                "ok": False,
                "code": "D0_RAW_ENGINEERING_ERROR",
                "message": f"D0 LAUNCH原始场景读取失败：{type(error).__name__}",
                "database_write_attempted": False,
                "ddl_executed": False,
                "dml_executed": False,
            },
            error=True,
        )
        return _EXIT_ENGINEERING
    result["configuration_path"] = str(config_path)
    _emit(result)
    return _EXIT_OK if result.get("ok") is True else _EXIT_UNAVAILABLE


__all__ = ["_run_source_d0_raw"]
