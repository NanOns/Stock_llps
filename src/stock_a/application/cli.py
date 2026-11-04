"""Stock A正式CLI入口，只负责UTF-8输出、命令分发和统一错误码。"""

from __future__ import annotations

import sys
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

from stock_a.application.cli_commands.common import _emit, _parse_source_set_date
from stock_a.application.cli_commands.research.d1_preview import (
    _run_source_d1_breakout_preview,
)
from stock_a.application.cli_commands.research.d0_raw import _run_source_d0_raw
from stock_a.application.cli_commands.research.market_research import (
    _run_source_market_breadth,
    _run_source_symbol_price_profile,
)
from stock_a.application.cli_commands.research.research_commands import (
    _run_research,
    _run_research_universe,
)
from stock_a.application.cli_commands.source.sector_members import (
    _run_source_raw_catalog,
    _run_source_tdx_sector_members,
)
from stock_a.application.cli_commands.source.source_diagnostics import (
    _run_doctor,
    _run_pg_bind,
    _run_pg_identity,
    _run_source_phase0,
)
from stock_a.application.cli_commands.source.source_ingress import (
    _run_source_ingress_plan,
    _run_source_p03_write,
    _run_source_p04_identity_coverage,
    _run_source_p04_read,
    _run_source_set_candidate,
)
from stock_a.application.cli_commands.source.vipdata import (
    _run_source_inspect,
    _run_source_vipdata,
    _run_source_vipdata_persist,
    _run_vipdata_revision_compare,
    _run_vipdata_target_quality,
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
        if args.command == "source-vipdata-target-quality":
            return _run_vipdata_target_quality(args)
        if args.command == "source-vipdata-revision-compare":
            return _run_vipdata_revision_compare(args)
        if args.command == "source-vipdata":
            return _run_source_vipdata(args)
        if args.command == "source-vipdata-persist":
            return _run_source_vipdata_persist(args)
        if args.command == "source-raw-catalog":
            return _run_source_raw_catalog(args)
        if args.command == "source-tdx-sector-members":
            return _run_source_tdx_sector_members(args)
        if args.command == "source-set-candidate":
            return _run_source_set_candidate(args, compute_candidate=True)
        if args.command == "source-set-catalog":
            return _run_source_set_candidate(args, compute_candidate=False)
        if args.command == "source-symbol-price-profile":
            return _run_source_symbol_price_profile(args)
        if args.command == "source-market-breadth":
            return _run_source_market_breadth(args)
        if args.command == "source-d0-raw":
            return _run_source_d0_raw(args)
        if args.command == "source-d1-breakout-preview":
            return _run_source_d1_breakout_preview(args)
        if args.command == "source-p04-identity-coverage":
            return _run_source_p04_identity_coverage(args)
        if args.command == "source-p04-read":
            return _run_source_p04_read(args)
        if args.command == "source-ingress-plan":
            return _run_source_ingress_plan(args)
        if args.command == "source-p03-write":
            return _run_source_p03_write(args)
        if args.command == "preview-serve":
            from stock_a.service.preview_http import run_preview_server

            return run_preview_server(
                host=args.host,
                port=args.port,
                research_cutoff=_parse_source_set_date(args.research_cutoff),
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
