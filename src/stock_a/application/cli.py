"""Stock A正式CLI入口，只负责UTF-8输出、命令分发和统一错误码。"""

from __future__ import annotations

import sys
from stock_a.application.runtime_config import (
    DiagnosticError,
    _EXIT_ENGINEERING,
    _EXIT_OK,
)

from stock_a.application.cli_commands.common import _emit, _parse_session_date
from stock_a.application.cli_commands.research.d0_raw import _run_source_d0_raw
from stock_a.application.cli_commands.research.fep_baseline import (
    _run_source_fep_baseline,
)
from stock_a.application.cli_commands.source.sector_members import (
    _run_source_tdx_sector_members,
)
from stock_a.application.cli_commands.source.source_diagnostics import (
    _run_doctor,
    _run_pg_bind,
    _run_pg_identity,
)


def _parser():
    from stock_a.application.cli_commands.parser import _parser as build_parser

    return build_parser()


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
        if args.command == "source-tdx-sector-members":
            return _run_source_tdx_sector_members(args)
        if args.command == "source-d0-raw":
            return _run_source_d0_raw(args)
        if args.command == "source-fep-baseline":
            return _run_source_fep_baseline(args)
        if args.command in ("product-serve", "preview-serve"):
            from stock_a.service.preview_http import run_preview_server

            return run_preview_server(
                host=args.host,
                port=args.port,
                research_cutoff=_parse_session_date(args.research_cutoff),
                data_jobs_authorized=args.authorize_data_jobs,
            )
    except DiagnosticError as error:
        _emit(
            {
                "ok": False,
                "code": error.code,
                "cause_type": type(error.__cause__).__name__
                if error.__cause__ is not None
                else None,
                "message": str(error),
            },
            error=True,
        )
        return error.exit_code
    except KeyboardInterrupt:
        _emit(
            {
                "ok": False,
                "code": "OPERATION_INTERRUPTED",
                "message": "操作已中断，未生成完整来源摘要",
            },
            error=True,
        )
        return _EXIT_ENGINEERING
    except Exception as error:
        _emit(
            {
                "ok": False,
                "code": "ENGINEERING_ERROR",
                "cause_type": type(error).__name__,
                "message": f"诊断执行失败：{type(error).__name__}",
            },
            error=True,
        )
        return _EXIT_ENGINEERING
    return _EXIT_ENGINEERING
