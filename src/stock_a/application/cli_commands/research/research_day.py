"""单日来源D0/D1事实计算及显式持久化CLI命令。"""

from __future__ import annotations

import argparse

from stock_a.application.cli_commands.common import _emit, _parse_session_date
from stock_a.application.runtime_config import (
    _EXIT_ARGUMENT,
    _EXIT_ENGINEERING,
    _EXIT_OK,
    _EXIT_UNAVAILABLE,
    DiagnosticError,
    load_runtime_config,
)
from stock_a.application.source_research_day import (
    SourceResearchDayError,
    run_source_research_day,
)


def _run_source_research_day(args: argparse.Namespace) -> int:
    try:
        config, config_path = load_runtime_config()
        report = run_source_research_day(
            config,
            target_session=_parse_session_date(args.target_session),
            research_cutoff=_parse_session_date(args.research_cutoff),
            package_sha256=args.package_sha256,
            d1_history_session_count=args.d1_history_session_count,
            persist=args.persist,
        )
    except DiagnosticError as error:
        _emit({"ok": False, "code": error.code, "message": str(error)}, error=True)
        return error.exit_code
    except (SourceResearchDayError, ValueError) as error:
        _emit(
            {
                "ok": False,
                "code": getattr(error, "code", "RESEARCH_DAY_ARGUMENT_OR_DATA_ERROR"),
                "message": str(error),
                "database_write_attempted": False,
                "ddl_executed": False,
                "dml_executed": False,
            },
            error=True,
        )
        return _EXIT_ARGUMENT
    except Exception as error:
        error_code = getattr(error, "code", None)
        _emit(
            {
                "ok": False,
                "code": error_code or "RESEARCH_DAY_ENGINEERING_ERROR",
                "cause_type": type(error).__name__,
                "message": str(error)
                if error_code
                else f"单日研究计算失败：{type(error).__name__}",
                "database_write_attempted": bool(args.persist),
            },
            error=True,
        )
        return _EXIT_ENGINEERING
    report["configuration_path"] = str(config_path)
    report["ok"] = True
    _emit(report)
    return _EXIT_OK if report.get("status") == "DRY_RUN_READY" else _EXIT_UNAVAILABLE


__all__ = ["_run_source_research_day"]
