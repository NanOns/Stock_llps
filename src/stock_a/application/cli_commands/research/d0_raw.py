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
    attach_recovery_failure_followup,
    attach_shape_state_followup,
    attach_stock_risk_readiness,
    query_source_d0_raw_launch,
)
from stock_a.research.d0_shape_comparison import compare_shape_observations


def _run_source_d0_raw(args: argparse.Namespace) -> int:
    try:
        config, config_path = load_runtime_config()
        result = query_source_d0_raw_launch(
            config,
            scenario=args.scenario,
            target_session=_parse_session_date(args.target_session),
            research_cutoff=_parse_session_date(args.research_cutoff),
            source_package_id=args.source_package_id,
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
            before_source_package_id=args.before_source_package_id,
            transition=args.transition,
            _include_shape_observation=(
                args.action == "current"
                and args.scenario == "RECOVERY_TURN"
                and bool(args.symbol)
            ),
        )
        if args.risk_readiness:
            if (
                args.action != "current"
                or not args.symbol
                or len(result["results"]) != 1
            ):
                raise SourceD0RawError(
                    "D0_RISK_SCOPE_INVALID",
                    "风险研究读取要求单股当前查询。",
                    category="ARGUMENT",
                )
            attach_stock_risk_readiness(
                config,
                result,
                target=_parse_session_date(args.target_session),
                cutoff=_parse_session_date(args.research_cutoff),
                source_package_id=args.source_package_id,
            )
        if getattr(args, "followup_session", None):
            if (
                args.action != "current"
                or not args.symbol
                or len(result["results"]) != 1
            ):
                raise SourceD0RawError(
                    "D0_FOLLOWUP_SCOPE_INVALID",
                    "后继观察要求单只股票的启动或恢复诊断当前查询。",
                    category="ARGUMENT",
                )
            attach_recovery_failure_followup(
                config,
                result["results"][0],
                target=_parse_session_date(args.target_session),
                followup=_parse_session_date(args.followup_session),
                cutoff=_parse_session_date(args.research_cutoff),
                source_package_id=args.source_package_id,
            )
        if getattr(args, "shape_followup_states", False):
            if (
                args.action != "current"
                or args.scenario != "RECOVERY_TURN"
                or not args.followup_session
                or len(result["results"]) != 1
                or not args.symbol
            ):
                raise SourceD0RawError(
                    "D0_SHAPE_FOLLOWUP_SCOPE_INVALID",
                    "形态后继需要单股恢复诊断及后继会话。",
                    category="ARGUMENT",
                )
            attach_shape_state_followup(
                config,
                result,
                target=_parse_session_date(args.target_session),
                followup=_parse_session_date(args.followup_session),
                cutoff=_parse_session_date(args.research_cutoff),
                source_package_id=args.source_package_id,
            )
        if getattr(args, "shape_control_symbol", None):
            if (
                args.action != "current"
                or args.scenario != "RECOVERY_TURN"
                or not args.symbol
                or not args.followup_session
                or len(result["results"]) != 1
                or len(args.shape_control_symbol) > 10
                or len(set([args.symbol, *args.shape_control_symbol]))
                != len(args.shape_control_symbol) + 1
            ):
                raise SourceD0RawError(
                    "D0_SHAPE_COMPARISON_SCOPE_INVALID",
                    "形态比较须指定单股恢复诊断、后继会话和最多十个不同的预选来源代码。",
                    category="ARGUMENT",
                )
            samples = [result]
            for comparison_symbol in args.shape_control_symbol:
                comparison = query_source_d0_raw_launch(
                    config,
                    scenario="RECOVERY_TURN",
                    target_session=_parse_session_date(args.target_session),
                    research_cutoff=_parse_session_date(args.research_cutoff),
                    source_package_id=args.source_package_id,
                    symbol=comparison_symbol,
                    limit=1,
                    _include_shape_observation=True,
                )
                if len(comparison["results"]) != 1:
                    raise SourceD0RawError(
                        "D0_SHAPE_COMPARISON_SYMBOL_MISSING", "来源池缺少比较股票。"
                    )
                attach_recovery_failure_followup(
                    config,
                    comparison["results"][0],
                    target=_parse_session_date(args.target_session),
                    followup=_parse_session_date(args.followup_session),
                    cutoff=_parse_session_date(args.research_cutoff),
                    source_package_id=args.source_package_id,
                )
                samples.append(comparison)
            result["shape_source_comparison"] = compare_shape_observations(samples)
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
                "message": f"D0原始场景读取失败：{type(error).__name__}",
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
