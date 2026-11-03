"""Stock A命令行market_research职责模块。"""

from __future__ import annotations

import argparse
import re
import sys
from typing import Any
from stock_a.contracts import (
    DAY_CHAIN_MODULES,
    DayChainContractError,
    load_day_chain_contract,
)
from stock_a.storage.postgres.source_raw import (
    RAW_DAILY_APPEND_TASK_SCOPES,
    RawSourcePgError,
    import_verified_vipdata_target,
    import_verified_vipdata_history,
    read_raw_daily_catalog,
    read_raw_daily_page,
    read_raw_daily_history_page,
    read_raw_daily_history_stock,
    read_raw_daily_stock,
)
from stock_a.application.source_set_candidate import (
    persist_source_set_candidate,
    prepare_source_set_candidate,
    read_source_set_candidate,
)
from stock_a.application.pg_price_profiles import (
    PgPriceProfileError,
    SOURCE_SYMBOL_PROFILE_SCOPE,
    query_source_symbol_price_profile_changes,
    query_source_symbol_price_profiles,
    run_source_symbol_price_profile,
    source_symbol_price_profile_snapshots,
)
from stock_a.application.source_market_context import (
    SourceMarketContextError,
    fraction_payload as _fraction_payload,
    read_source_market_context,
    source_market_context_payload,
)
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


def _run_source_market_breadth(args: argparse.Namespace) -> int:
    config, config_path = _load_runtime_config()
    target_session = _parse_source_set_date(args.target_session)
    research_cutoff = _parse_source_set_date(args.research_cutoff)
    if target_session > research_cutoff:
        raise DiagnosticError(
            "MARKET_BREADTH_DATE_AFTER_CUTOFF",
            "目标交易会话不得晚于显式研究截止日",
            _EXIT_ARGUMENT,
        )
    if re.fullmatch(r"[0-9a-f]{64}", args.package_sha256) is None:
        raise DiagnosticError(
            "MARKET_BREADTH_PACKAGE_SHA_INVALID",
            "完整TDX来源包SHA必须是64位小写十六进制值",
            _EXIT_ARGUMENT,
        )

    try:
        market_read = read_source_market_context(
            config,
            target_session=target_session,
            research_cutoff=research_cutoff,
            source_set_id=args.source_set_id,
            package_sha256=args.package_sha256,
        )
    except SourceMarketContextError as error:
        payload = {
            "ok": False,
            "status": "REJECTED",
            "code": error.code,
            "message": str(error),
            "database_write_attempted": False,
            "ddl_executed": False,
            "dml_executed": False,
        }
        if error.category not in {"ARGUMENT", "NOT_EVALUABLE"}:
            payload["configuration_path"] = str(config_path)
        _emit(payload, error=True)
        return (
            _EXIT_ARGUMENT
            if error.category in {"ARGUMENT", "NOT_EVALUABLE"}
            else _EXIT_UNAVAILABLE
        )
    except RawSourcePgError as error:
        _emit(
            {
                "ok": False,
                "status": "REJECTED",
                "code": error.code,
                "message": str(error),
                "configuration_path": str(config_path),
                "database_write_attempted": False,
                "ddl_executed": False,
                "dml_executed": False,
            },
            error=True,
        )
        return _EXIT_ARGUMENT if error.code.endswith("INVALID") else _EXIT_UNAVAILABLE

    calculation = market_read.calculation
    market_payload = source_market_context_payload(market_read)
    symbol_results = [
        {
            "source_symbol": item.source_symbol,
            "ret1_t": _fraction_payload(item.ret1_t),
            "ret1_t_minus_3": _fraction_payload(item.ret1_t_minus_3),
            "I_ret1_t_positive": item.positive_t,
            "I_ret1_t_minus_3_positive": item.positive_t_minus_3,
            "delta_indicator": item.delta_indicator,
            "classification": item.classification,
            "primary_unknown_reason": item.primary_unknown_reason,
            "unknown_reason_detail": item.unknown_reason_detail,
        }
        for item in calculation.symbols
    ]
    document = {
        "ok": True,
        "status": market_payload["status"],
        "contract_version": market_payload["contract_version"],
        "analysis_scope": market_payload["analysis_scope"],
        "identity_status": market_payload["identity_status"],
        "pit_status": market_payload["pit_status"],
        "price_coordinate": market_payload["price_coordinate"],
        "target_session": market_payload["target_session"],
        "research_cutoff": market_payload["research_cutoff"],
        "official_calendar": market_payload["official_calendar"],
        "source_cohort": market_payload["source_cohort"],
        "metric": market_payload["metric"],
        "denominator": market_payload["denominator"],
        "source_symbol_results": symbol_results,
        "formal_market_breadth": market_payload["formal_market_breadth"],
        "database_read": market_payload["database_read"],
        "configuration_path": str(config_path),
        "sidecar_output_written": False,
    }
    _emit(document)
    return _EXIT_OK


def _run_source_symbol_price_profile(args: argparse.Namespace) -> int:
    config, config_path = _load_runtime_config()
    try:
        max_target_session = _parse_source_set_date(args.research_cutoff)
        if args.action == "snapshots":
            result = source_symbol_price_profile_snapshots(
                config, max_target_session=max_target_session
            )
        elif args.action == "changes":
            if not args.before_session or not args.after_session:
                raise DiagnosticError(
                    "PROFILE_CHANGE_QUERY_REQUIRED",
                    "变化查询必须显式指定--before-session和--after-session",
                    _EXIT_ARGUMENT,
                )
            result = query_source_symbol_price_profile_changes(
                config,
                before_session=_parse_source_set_date(args.before_session),
                after_session=_parse_source_set_date(args.after_session),
                max_target_session=max_target_session,
                offset=args.offset,
                limit=args.limit,
                source_symbol=args.symbol,
                change_type=args.change_type,
            )
        elif args.action == "query":
            if not all(
                (
                    args.target_session,
                    args.source_set_id,
                    args.package_sha256,
                    args.profile_revision,
                )
            ):
                raise DiagnosticError(
                    "PROFILE_QUERY_KEY_REQUIRED",
                    "画像查询必须显式指定目标日期、source_set、包SHA和profile_revision",
                    _EXIT_ARGUMENT,
                )
            if args.core_field is None and args.core_quality is not None:
                raise DiagnosticError(
                    "PROFILE_CORE_FILTER_INVALID",
                    "Core质量筛选必须同时指定字段",
                    _EXIT_ARGUMENT,
                )
            result = query_source_symbol_price_profiles(
                config,
                target_session=_parse_source_set_date(args.target_session),
                max_target_session=max_target_session,
                source_set_id=args.source_set_id,
                package_sha256=args.package_sha256,
                profile_revision=args.profile_revision,
                offset=args.offset,
                limit=args.limit,
                market=args.market,
                board=args.board,
                target_bar_status=args.target_bar_status,
                price_window_status=args.price_window_status,
                source_symbol=args.symbol,
                core_field=args.core_field,
                core_quality=args.core_quality,
                price_damage_state=args.price_damage_state,
                ma_slope20_sign=args.ma_slope20_sign,
            )
        else:
            if not all(
                (
                    args.target_session,
                    args.source_set_id,
                    args.package_sha256,
                    args.scope,
                )
            ):
                raise DiagnosticError(
                    "PROFILE_COMPUTE_KEY_REQUIRED",
                    "画像计算必须显式指定目标日期、SEALED source_set、包SHA与计算scope",
                    _EXIT_ARGUMENT,
                )
            if re.fullmatch(r"[0-9a-f]{64}", args.package_sha256) is None:
                raise DiagnosticError(
                    "PROFILE_PACKAGE_SHA_INVALID",
                    "完整TDX包SHA必须是64位小写十六进制值",
                    _EXIT_ARGUMENT,
                )
            target_session = _parse_source_set_date(args.target_session)
            if target_session > max_target_session:
                raise PgPriceProfileError(
                    "PROFILE_DATE_AFTER_CUTOFF", "画像计算目标日超过显式研究截止日"
                )
            loaded_contract = load_day_chain_contract(_CONFIG_ROOT)
            result = run_source_symbol_price_profile(
                config,
                target_session=target_session,
                source_set_id=args.source_set_id,
                package_sha256=args.package_sha256,
                scope=args.scope,
                session_count=args.session_count,
                page_size=args.page_size,
                profile_contract=loaded_contract.document,
            )
    except (PgPriceProfileError, DayChainContractError) as error:
        _emit(
            {
                "ok": False,
                "status": "REJECTED",
                "code": getattr(error, "code", "PROFILE_CONTRACT_INVALID"),
                "message": str(error),
                "configuration_path": str(config_path),
                "database_write_attempted": args.action == "compute",
                "ddl_executed": False,
                "source_zip_opened": False,
                "baostock_called": False,
            },
            error=True,
        )
        return _EXIT_UNAVAILABLE
    result.update(
        {
            "ok": result.get("status") not in {"NO_MATCH", "EMPTY"},
            "configuration_path": str(config_path),
            "database_io_attempted": True,
            "database_write_attempted": args.action == "compute",
            "ddl_executed": False,
            "source_zip_opened": False,
            "baostock_called": False,
        }
    )
    if args.format == "summary":
        if args.action == "query":
            page = result.get("page", {})
            sys.stdout.write(
                f"PG画像查询 target={result.get('target_session')} "
                f"source_set={result.get('source_set_id')} offset={page.get('offset')} "
                f"limit={page.get('limit')} rows={page.get('count')} total={page.get('total')} "
                f"next={page.get('next_offset')} status={result.get('status')}\n"
            )
        elif args.action == "changes":
            page = result.get("page", {})
            summary = result.get("summary", {})
            sys.stdout.write(
                f"PG画像变化 {result.get('before_snapshot', {}).get('target_session')}→"
                f"{result.get('after_snapshot', {}).get('target_session')} "
                f"变更={summary.get('changed_source_symbol_count')} "
                f"本页={page.get('count')} / {page.get('total')} "
                f"偏移={page.get('offset')}\n"
            )
        elif args.action == "compute":
            sys.stdout.write(
                f"PG画像批次 target={result.get('target_session')} "
                f"source_set={result.get('source_set_id')} "
                f"scope={result.get('scope')} candidates={result.get('source_candidate_count')} "
                f"inserted={result.get('inserted_count')} "
                f"existing_same_content={result.get('existing_same_content_count')} "
                f"consumer_failures={result.get('consumer_failure_count')} "
                f"status={result.get('status')}\n"
            )
        else:
            for item in result.get("snapshots", ()):
                sys.stdout.write(
                    f"target={item['target_session']} source_set={item['source_set_id']} "
                    f"revision={item['profile_revision']} "
                    f"rows={item['source_symbol_count']}/{item['expected_source_symbol_count']} "
                    f"complete={item['complete']}\n"
                )
    else:
        _emit(result)
    return _EXIT_OK if result.get("ok") else _EXIT_UNAVAILABLE
