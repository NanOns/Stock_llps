"""Stock A命令行research_commands职责模块。"""

from __future__ import annotations

import argparse
from decimal import Decimal, InvalidOperation, localcontext
import json
import math
import sys
import time
from typing import Any
from stock_a.contracts import (
    DAY_CHAIN_MODULES,
    DayChainContractError,
    load_day_chain_contract,
)
from stock_a.domain import ContractError
from stock_a.sources.tdx.day import UnitContract
from stock_a.sources.tdx.day_stream import DayReadError, ReadLimits
from stock_a.sources.tdx.vipdata import (
    VipdataBudgets,
    VipdataError,
    VipdataSourceConfig,
    cleanup_vipdata_extraction,
    cleanup_vipdata_package,
    compare_vipdata_package_revisions,
    download_vipdata_package,
    extract_vipdata_day_entry,
    inspect_vipdata_page,
    observation_dict,
    package_dict,
    read_vipdata_candidate,
    scan_vipdata_target_quality,
    scan_vipdata_day_entry,
    VipdataTargetScanBudget,
    verify_existing_vipdata_package,
    vipdata_research_candidate_pool,
)
from stock_a.research.entry import (
    CompareBudget,
    ResearchEntryError,
    UniverseDiscoveryBudget,
    _normalize_symbol,
    _load_calendar,
    _parse_target,
    apply_identity_observation,
    research_compare,
    research_stock,
    research_stock_pg_history_preview,
    research_stock_pg_support_followup_preview,
    classify_source_support_change,
    select_source_support_change_page,
    research_universe_preview,
    research_universe_pg_history_preview,
)
from stock_a.sources.baostock.identity import project_identity_lifecycle
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

from stock_a.application.cli_commands.common import (
    _READ_ERROR_EXIT_CODES,
    _emit,
    _json_default,
)
from stock_a.application.cli_commands.research.pg_queries import (
    _run_pg_history_stock,
    _run_pg_history_universe,
    _run_pg_raw_stock,
    _run_pg_raw_universe,
)
from stock_a.application.cli_commands.research.research_core_profile import (
    _run_full_source_core_profile,
)
from stock_a.application.cli_commands.research.research_output import (
    _render_basic_fact_summary,
    _render_universe_summary,
)
from stock_a.application.cli_commands.source.vipdata import (
    _verify_explicit_vipdata_package,
)


def _run_research(args: argparse.Namespace, *, compare: bool) -> int:
    if getattr(args, "pg_source_history", False):
        if compare:
            raise DiagnosticError(
                "PG_HISTORY_COMMAND_UNSUPPORTED",
                "research-compare不支持PG历史画像模式",
                _EXIT_ARGUMENT,
            )
        return _run_pg_history_stock(args)
    if getattr(args, "pg_source_raw", False):
        if compare:
            raise DiagnosticError(
                "PG_SOURCE_COMMAND_UNSUPPORTED",
                "research-compare不支持本批PG Raw查询入口",
                _EXIT_ARGUMENT,
            )
        return _run_pg_raw_stock(args)
    if getattr(args, "pg_source_sha256", None) is not None:
        raise DiagnosticError(
            "PG_SOURCE_FLAG_REQUIRED",
            "提供来源SHA时必须同时显式启用--pg-source-raw",
            _EXIT_ARGUMENT,
        )
    cli_started = time.perf_counter()
    stage_timings: dict[str, Any] = {}
    config, config_path = _load_runtime_config()
    vipdata_validation_started = time.perf_counter()
    package = _verify_explicit_vipdata_package(args)
    if package is not None:
        stage_timings["vipdata_package_verify_elapsed_ms"] = round(
            (time.perf_counter() - vipdata_validation_started) * 1000,
            3,
        )
    roots = (
        ()
        if package is not None
        else config["source_policy"]["configured_tdx_source_roots"]
    )
    if package is None and not roots:
        raise DiagnosticError(
            "SOURCE_NOT_CONFIGURED", "配置中没有绑定TDX来源保护根，拒绝读取行情"
        )
    try:
        units = UnitContract(
            args.unit_version,
            Decimal(args.amount_to_cny) if args.amount_to_cny is not None else None,
            args.volume_to_shares,
        )
        limits = ReadLimits(
            args.max_bytes,
            args.max_records,
            args.chunk_records,
            Decimal(args.max_seconds),
        )
    except (InvalidOperation, ValueError, ContractError) as error:
        raise DiagnosticError(
            "ARGUMENT_CONTRACT_INVALID", f"单位或读取预算无效：{error}", _EXIT_ARGUMENT
        ) from error
    foundation_contract = None
    if not compare:
        try:
            foundation_contract = load_day_chain_contract(_CONFIG_ROOT).document
        except DayChainContractError as error:
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
    profile_contract = (
        foundation_contract
        if (not compare and getattr(args, "include_profile_preview", False))
        else None
    )
    identity_projection = None
    identity_snapshot: dict[str, Any] | None = None
    identity_query_status = "NOT_REQUESTED"
    include_identity = not compare and getattr(args, "include_baostock_identity", False)
    if include_identity and getattr(args, "include_baostock_evidence", False):
        _emit(
            {
                "ok": False,
                "code": "BAOSTOCK_OPTIONS_EXCLUSIVE",
                "message": "身份与日线证据请求会各自建立Bao会话；为保持本次单次登录预算，请分开执行。",
                "database_io_attempted": False,
            },
            error=True,
        )
        return _EXIT_ARGUMENT
    if include_identity:
        try:
            target_text = _parse_target(args.target_session).isoformat()
        except ResearchEntryError as error:
            _emit(
                {
                    "ok": False,
                    "code": error.code,
                    "message": str(error),
                    "database_io_attempted": False,
                },
                error=True,
            )
            return _EXIT_ARGUMENT
        timeout_seconds = getattr(args, "baostock_timeout_seconds", 8.0)
        max_total_seconds = getattr(args, "baostock_max_seconds", 25.0)
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not math.isfinite(timeout_seconds)
            or not 0 < timeout_seconds <= 30
            or isinstance(max_total_seconds, bool)
            or not isinstance(max_total_seconds, (int, float))
            or not math.isfinite(max_total_seconds)
            or not 0.5 <= max_total_seconds <= 30
        ):
            _emit(
                {
                    "ok": False,
                    "code": "BAOSTOCK_BUDGET_INVALID",
                    "message": "BaoStock socket超时须在0至30秒内，总预算须在0.5至30秒内。",
                    "database_io_attempted": False,
                },
                error=True,
            )
            return _EXIT_ARGUMENT
    else:
        target_text = None
    core_timings: dict[str, float] = {}
    try:
        if compare:
            budget = CompareBudget(
                args.max_total_bytes,
                args.max_total_records,
                args.max_total_seconds,
            )
            result = research_compare(
                symbols=args.symbols,
                target_session=args.target_session,
                source_roots=roots,
                units=units,
                limits=limits,
                budget=budget,
            )
        elif package is not None:
            if compare:
                raise DiagnosticError(
                    "VIPDATA_COMPARE_UNSUPPORTED",
                    "当前显式ZIP入口只接research-stock和research-universe-preview",
                )
            candidate_index_started = time.perf_counter()
            pool = vipdata_research_candidate_pool(package)
            stage_timings["vipdata_candidate_index_elapsed_ms"] = round(
                (time.perf_counter() - candidate_index_started) * 1000,
                3,
            )
            normalized, _directory, _board = _normalize_symbol(args.symbol)
            candidate = next(
                (item for item in pool.candidates if item["symbol"] == normalized), None
            )
            if candidate is None:
                raise VipdataError(
                    "ZIP_DAY_ENTRY_NOT_FOUND",
                    "已校验VIPDATA包中没有该沪深主板/创业板证券条目",
                )
            if candidate["empty_placeholder"]:
                raise VipdataError(
                    "ZIP_DAY_ENTRY_EMPTY",
                    "官方ZIP将此证券作为零字节占位；未送入Raw/Core",
                )
            entry_read_started = time.perf_counter()
            vipdata_capture = read_vipdata_candidate(
                package,
                candidate,
                target_session=_parse_target(args.target_session),
                units=units,
                limits=limits,
            )
            stage_timings["vipdata_entry_read_elapsed_ms"] = round(
                (time.perf_counter() - entry_read_started) * 1000,
                3,
            )
            result = research_stock(
                symbol=normalized,
                target_session=args.target_session,
                source_roots=(),
                units=units,
                limits=limits,
                profile_contract=profile_contract,
                foundation_contract=foundation_contract,
                include_baostock_evidence=getattr(
                    args, "include_baostock_evidence", False
                ),
                vipdata_capture=vipdata_capture,
                timing_sink=core_timings,
            )
        else:
            result = research_stock(
                symbol=args.symbol,
                target_session=args.target_session,
                source_roots=roots,
                units=units,
                limits=limits,
                profile_contract=profile_contract,
                foundation_contract=foundation_contract,
                include_baostock_evidence=getattr(
                    args, "include_baostock_evidence", False
                ),
                timing_sink=core_timings,
            )
    except ResearchEntryError as error:
        if error.code == "SOURCE_ACCESS_DENIED":
            code = _EXIT_PERMISSION
        elif error.code == "SOURCE_IO_ERROR":
            code = _EXIT_ENGINEERING
        else:
            code = (
                _EXIT_ARGUMENT
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
            },
            error=True,
        )
        return code
    except DayReadError as error:
        exit_code = _READ_ERROR_EXIT_CODES.get(error.code, _EXIT_ENGINEERING)
        _emit(
            {
                "ok": False,
                "code": error.code,
                "reason_category": error.reason_category,
                "message": str(error),
                "database_io_attempted": False,
            },
            error=True,
        )
        return exit_code
    except VipdataError as error:
        _emit(
            {
                "ok": False,
                "code": error.code,
                "message": str(error),
                "database_io_attempted": False,
                "database_write_attempted": False,
                "source_fallback_attempted": False,
            },
            error=True,
        )
        return _EXIT_UNAVAILABLE
    if include_identity and target_text is not None:
        identity_started = time.perf_counter()
        try:
            from stock_a.sources.baostock.client import query_identity_reference

            identity_snapshot = query_identity_reference(
                (target_text,),
                target_symbol=args.symbol,
                timeout_seconds=timeout_seconds,
                max_total_seconds=max_total_seconds,
            )
        except Exception as error:
            identity_snapshot = {
                "status": "ENGINEERING_ERROR",
                "source": "BAOSTOCK",
                "sdk_version": None,
                "target_symbol": args.symbol,
                "target_sessions": [target_text],
                "login_attempted": False,
                "logout_attempted": False,
                "query_calls": 0,
                "retry_count": 0,
                "verified_source_capture_id": None,
                "request_started_at": None,
                "request_completed_at": None,
                "capture_clock_role": "UNKNOWN_QUERY_DID_NOT_RETURN",
                "total_elapsed_ms": round(
                    (time.perf_counter() - identity_started) * 1000, 3
                ),
                "stage_elapsed_ms": {},
                "reason": f"BaoStock身份查询程序异常：{type(error).__name__}: {str(error)[:240]}",
                "error_type": type(error).__name__,
                "calls": {},
            }
        identity_query_status = str(
            identity_snapshot.get("status", "SOURCE_UNAVAILABLE")
        )
        calls = identity_snapshot.get("calls")
        if identity_query_status == "RESPONSE_RECEIVED" and isinstance(calls, dict):
            basic = calls.get("stock_basic")
            daily_by_date = calls.get("all_stock")
            session = (
                daily_by_date.get(target_text)
                if isinstance(daily_by_date, dict)
                else None
            )
            if isinstance(basic, dict) and isinstance(session, dict):
                try:
                    identity_projection = project_identity_lifecycle(
                        source_symbol=args.symbol,
                        target_session=_parse_target(target_text),
                        basic_snapshot=basic,
                        session_snapshot=session,
                    )
                except Exception as error:
                    identity_query_status = "IDENTITY_PROJECTION_ERROR"
                    identity_snapshot["reason"] = (
                        f"BaoStock响应已读取，但身份投影失败：{type(error).__name__}: {str(error)[:240]}"
                    )
            else:
                identity_query_status = "QUERY_FAILED"
        apply_identity_observation(
            result,
            identity_projection=identity_projection,
            identity_query_status=identity_query_status,
        )
    result.update(
        {
            "ok": result.get("status") in ("AVAILABLE", "COMPLETE_WITH_DISCLOSURES"),
            "configuration_path": str(config_path),
            "database_io_attempted": False,
            "database_write_attempted": False,
        }
    )
    if identity_snapshot is not None:
        calls = identity_snapshot.get("calls")
        basic = calls.get("stock_basic") if isinstance(calls, dict) else None
        daily_by_date = calls.get("all_stock") if isinstance(calls, dict) else None
        session = (
            daily_by_date.get(target_text) if isinstance(daily_by_date, dict) else None
        )
        result["bao_identity_capture"] = {
            "source": "BAOSTOCK",
            "target_symbol": identity_snapshot.get("target_symbol"),
            "target_session": target_text,
            "status": identity_snapshot.get("status"),
            "projection_status": identity_query_status,
            "sdk_version": identity_snapshot.get("sdk_version"),
            "login_attempted": identity_snapshot.get("login_attempted"),
            "login_error_code": identity_snapshot.get("login_error_code"),
            "login_error_message": identity_snapshot.get("login_error_message"),
            "logout_attempted": identity_snapshot.get("logout_attempted"),
            "logout_error_code": identity_snapshot.get("logout_error_code"),
            "logout_error_message": identity_snapshot.get("logout_error_message"),
            "query_calls": identity_snapshot.get("query_calls"),
            "request_started_at": identity_snapshot.get("request_started_at"),
            "request_completed_at": identity_snapshot.get("request_completed_at"),
            "capture_clock_role": identity_snapshot.get("capture_clock_role"),
            "verified_source_capture_id": identity_snapshot.get(
                "verified_source_capture_id"
            ),
            "basic_query": (
                {
                    key: basic.get(key)
                    for key in (
                        "status",
                        "requested_code",
                        "query_error_code",
                        "response_fields",
                        "rows_seen",
                        "max_rows_limit",
                        "request_started_at",
                        "request_completed_at",
                        "api_call_elapsed_ms",
                        "row_read_elapsed_ms",
                        "elapsed_ms",
                        "local_error",
                    )
                }
                if isinstance(basic, dict)
                else None
            ),
            "target_day_list_query": (
                {
                    key: session.get(key)
                    for key in (
                        "status",
                        "target_session",
                        "query_error_code",
                        "response_fields",
                        "rows_seen",
                        "max_rows_limit",
                        "request_started_at",
                        "request_completed_at",
                        "api_call_elapsed_ms",
                        "row_read_elapsed_ms",
                        "elapsed_ms",
                        "local_error",
                    )
                }
                if isinstance(session, dict)
                else None
            ),
            "stage_elapsed_ms": identity_snapshot.get("stage_elapsed_ms"),
            "max_total_seconds": identity_snapshot.get("max_total_seconds"),
            "total_elapsed_ms": identity_snapshot.get("total_elapsed_ms"),
            "retry_count": identity_snapshot.get("retry_count"),
            "error_type": identity_snapshot.get("error_type"),
            "reason": identity_snapshot.get("reason"),
        }
        result["bao_status"] = identity_query_status
    stage_timings.update(core_timings)
    stage_timings["cli_elapsed_before_output_ms"] = round(
        (time.perf_counter() - cli_started) * 1000, 3
    )
    stage_timings["clock"] = (
        "Python perf_counter monotonic; process wall-clock independently recorded by caller"
    )
    result["diagnostic_timing"] = stage_timings
    if (
        not compare
        and getattr(args, "output_format", "json") == "summary"
        and not include_identity
    ):
        summary = _render_basic_fact_summary(result)
        source_package = result.get("source_package")
        if isinstance(source_package, dict):
            summary = (
                f"显式VIPDATA来源包 {source_package['package_id']}；ZIP SHA-256={source_package['sha256']}；"
                f"条目={source_package['zip_entry_path']}；CRC={source_package['zip_crc']}\n"
                + summary
            )
        sys.stdout.write(summary + "\n")
    else:
        _emit(result)
    return _EXIT_OK if result["ok"] else _EXIT_UNAVAILABLE


def _run_research_universe(args: argparse.Namespace) -> int:
    if getattr(args, "pg_source_history", False):
        return _run_pg_history_universe(args)
    if getattr(args, "pg_source_raw", False):
        return _run_pg_raw_universe(args)
    if getattr(args, "pg_source_sha256", None) is not None:
        raise DiagnosticError(
            "PG_SOURCE_FLAG_REQUIRED",
            "提供来源SHA时必须同时显式启用--pg-source-raw",
            _EXIT_ARGUMENT,
        )
    command_started = time.monotonic()
    config, config_path = _load_runtime_config()
    package_verification_seconds = 0.0
    package_verification_started = time.monotonic()
    package = _verify_explicit_vipdata_package(args)
    if package is not None:
        package_verification_seconds = time.monotonic() - package_verification_started
    if args.full_source_core_profile and package is None:
        raise DiagnosticError(
            "VIPDATA_PACKAGE_REQUIRED",
            "全范围Core/Profile模式必须显式指定已校验的VIPDATA ZIP",
            _EXIT_ARGUMENT,
        )
    roots = (
        ()
        if package is not None
        else config["source_policy"]["configured_tdx_source_roots"]
    )
    if package is None and not roots:
        raise DiagnosticError(
            "SOURCE_NOT_CONFIGURED", "配置中没有绑定TDX只读来源根，拒绝发现来源候选"
        )
    try:
        units = UnitContract(
            args.unit_version,
            Decimal(args.amount_to_cny) if args.amount_to_cny is not None else None,
            args.volume_to_shares,
        )
        limits = ReadLimits(
            args.max_bytes,
            args.max_records,
            args.chunk_records,
            Decimal(args.max_seconds),
        )
        discovery_budget = UniverseDiscoveryBudget(
            args.max_directory_entries,
            args.max_discovery_seconds,
        )
        budget = CompareBudget(
            args.max_total_bytes,
            args.max_total_records,
            args.max_total_seconds,
        )
        source_quality_budget = VipdataTargetScanBudget(
            max_total_bytes=args.max_source_quality_bytes,
            max_total_records=args.max_source_quality_records,
            max_total_seconds=args.max_source_quality_seconds,
        )
    except (
        InvalidOperation,
        ValueError,
        ContractError,
        ResearchEntryError,
        VipdataError,
    ) as error:
        raise DiagnosticError(
            "ARGUMENT_CONTRACT_INVALID", f"单位或预算合同无效：{error}", _EXIT_ARGUMENT
        ) from error
    try:
        foundation_contract = load_day_chain_contract(_CONFIG_ROOT).document
    except DayChainContractError as error:
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
    profile_contract = (
        foundation_contract
        if args.include_profile_preview or args.full_source_core_profile
        else None
    )
    try:
        if args.full_source_core_profile:
            candidate_index_started = time.monotonic()
            pool = vipdata_research_candidate_pool(package)
            candidate_index_seconds = time.monotonic() - candidate_index_started
            result, target_quality_report, scan_wall_seconds, consumer_seconds = (
                _run_full_source_core_profile(
                    args,
                    package=package,
                    pool=pool,
                    units=units,
                    profile_contract=profile_contract,
                    scan_budget=source_quality_budget,
                )
            )
            result["performance"].update(
                {
                    "zip_sha_crc_validation_seconds": round(
                        package_verification_seconds, 6
                    ),
                    "candidate_pool_metadata_seconds": round(
                        candidate_index_seconds, 6
                    ),
                    "full_source_scan_seconds": round(scan_wall_seconds, 6),
                    "core_profile_consumer_seconds": round(consumer_seconds, 6),
                }
            )
        elif package is None:
            result = research_universe_preview(
                target_session=args.target_session,
                source_roots=roots,
                units=units,
                limits=limits,
                offset=args.offset,
                limit=args.limit,
                market_filter=args.market,
                board_filter=args.board,
                discovery_budget=discovery_budget,
                budget=budget,
                profile_contract=profile_contract,
                foundation_contract=foundation_contract,
            )
        else:
            candidate_index_started = time.monotonic()
            pool = vipdata_research_candidate_pool(package)
            candidate_index_seconds = time.monotonic() - candidate_index_started
            if (
                type(args.offset) is not int
                or args.offset < 0
                or type(args.limit) is not int
                or not 1 <= args.limit <= 100
                or args.market not in {"ALL", "SH", "SZ"}
                or args.board not in {"ALL", "MAIN_BOARD", "GEM"}
            ):
                raise ResearchEntryError(
                    "UNIVERSE_PAGE_INVALID", "ZIP来源分页或过滤条件无效"
                )
            target = _parse_target(args.target_session)
            _calendar, _metadata, _revision, sessions = _load_calendar(target)
            if target not in sessions:
                raise ResearchEntryError(
                    "TARGET_DATE_INVALID", "目标日期不在已核验的官方交易日历内"
                )
            filtered = tuple(
                item
                for item in pool.candidates
                if (args.market == "ALL" or item["market"] == args.market)
                and (args.board == "ALL" or item["board"] == args.board)
            )
            page_candidates = filtered[args.offset : args.offset + args.limit]
            quality_scan_started = time.monotonic()
            target_quality_report = scan_vipdata_target_quality(
                package,
                pool,
                target_session=target,
                units=units,
                budget=source_quality_budget,
                evaluation_symbols=(
                    None
                    if args.full_target_quality
                    else tuple(item["symbol"] for item in page_candidates)
                ),
                capture_symbols=tuple(
                    item["symbol"]
                    for item in page_candidates
                    if item.get("empty_placeholder") is not True
                ),
            )
            quality_scan_wall_seconds = time.monotonic() - quality_scan_started
            page_research_started = time.monotonic()
            result = research_universe_preview(
                target_session=args.target_session,
                source_roots=(),
                units=units,
                limits=limits,
                offset=args.offset,
                limit=args.limit,
                market_filter=args.market,
                board_filter=args.board,
                discovery_budget=discovery_budget,
                budget=budget,
                profile_contract=profile_contract,
                foundation_contract=foundation_contract,
                source_candidate_pool=pool.candidates,
                source_candidate_catalog=pool.catalog,
                source_target_quality_report=target_quality_report,
            )
            page_research_seconds = time.monotonic() - page_research_started
            source_quality = result.get("source_target_quality")
            if isinstance(source_quality, dict):
                source_quality["evaluation_mode"] = (
                    "FULL_TARGET_QUALITY"
                    if args.full_target_quality
                    else "FAST_PAGE_ONLY"
                )
                source_quality["full_pool_evaluation_complete"] = (
                    target_quality_report.not_evaluated == 0
                )
                source_quality["read_failures_present"] = (
                    target_quality_report.read_failed > 0
                )
                source_quality["not_evaluated_reason_counts"] = {
                    reason: sum(
                        item.status == "NOT_EVALUATED" and item.reason == reason
                        for item in target_quality_report.candidates
                    )
                    for reason in sorted(
                        {
                            item.reason
                            for item in target_quality_report.candidates
                            if item.status == "NOT_EVALUATED"
                            and item.reason is not None
                        }
                    )
                }
            page_symbols = {item["symbol"] for item in page_candidates}
            page_quality_rows = tuple(
                item
                for item in target_quality_report.candidates
                if item.symbol in page_symbols
            )
            result["performance"] = {
                "timing_contract": "STOCK_A_RESEARCH_UNIVERSE_SEGMENTS_R1",
                "zip_sha_crc_validation_seconds": round(
                    package_verification_seconds, 6
                ),
                "candidate_pool_metadata_seconds": round(candidate_index_seconds, 6),
                "target_quality_scan_wall_seconds": round(quality_scan_wall_seconds, 6),
                "target_quality_scan_report_seconds": target_quality_report.elapsed_seconds,
                "page_entry_decode_seconds": target_quality_report.consumer_capture_read_elapsed_seconds,
                "full_pool_entry_decode_seconds": (
                    target_quality_report.entry_read_elapsed_seconds
                    if args.full_target_quality
                    else None
                ),
                "full_pool_target_scan_seconds": (
                    target_quality_report.elapsed_seconds
                    if args.full_target_quality
                    else None
                ),
                "page_core_profile_seconds": round(page_research_seconds, 6),
                "page_source_bytes": sum(item.bytes_read for item in page_quality_rows),
                "page_source_records": sum(
                    item.record_count for item in page_quality_rows
                ),
                "quality_scan_bytes": target_quality_report.bytes_read,
                "quality_scan_records": target_quality_report.records_read,
                "quality_scan_candidate_total": target_quality_report.candidate_total,
            }
            result["source_package"] = {
                "package_id": f"TDX-HSJDAY-SHA256:{package.sha256}",
                "sha256": package.sha256,
                "file_name": package.file_name,
                "file_size": package.file_size,
                "zip_crc": package.zip_crc,
                "zip_entry_count": package.zip_entry_count,
                "entry_count_by_market": dict(package.entry_count_by_market),
                "official_package_url": "https://data.tdx.com.cn/vipdoc/hsjday.zip",
                "origin_evidence_status": "PREVIOUS_OFFICIAL_INGRESS_REPORTED; THIS_CALL_RECONFIRMED_LOCAL_SHA_AND_CRC",
                "verified_at": package.verified_at.isoformat(),
                "published_at": "UNKNOWN",
                "first_available_at": "UNKNOWN",
                "identity_status": "SOURCE_SYMBOL_ONLY",
                "membership_and_listing_status": "UNKNOWN_NOT_OFFICIALLY_BOUND",
                "price_coordinate": "RAW_UNADJUSTED",
                "amount_unit_authority": "CALLER_DECLARED_RESEARCH_ASSUMPTION",
                "volume_unit_authority": "CALLER_DECLARED_RESEARCH_ASSUMPTION",
                "acceptance_status": "NOT_ACCEPTED_NOT_PERSISTED",
            }
    except VipdataError as error:
        _emit(
            {
                "ok": False,
                "code": error.code,
                "message": str(error),
                "database_io_attempted": False,
                "database_write_attempted": False,
                "source_fallback_attempted": False,
            },
            error=True,
        )
        return _EXIT_UNAVAILABLE
    except ResearchEntryError as error:
        if error.code == "SOURCE_ACCESS_DENIED":
            code = _EXIT_PERMISSION
        elif error.code == "SOURCE_IO_ERROR":
            code = _EXIT_ENGINEERING
        elif error.code in {
            "TARGET_DATE_INVALID",
            "UNIVERSE_PAGE_INVALID",
            "UNIVERSE_FILTER_INVALID",
            "UNIVERSE_BUDGET_INVALID",
            "COMPARE_BUDGET_INVALID",
        }:
            code = _EXIT_ARGUMENT
        else:
            code = _EXIT_UNAVAILABLE
        _emit(
            {
                "ok": False,
                "code": error.code,
                "message": str(error),
                "context": error.context,
                "database_io_attempted": False,
                "database_write_attempted": False,
            },
            error=True,
        )
        return code
    result.update(
        {
            "ok": result["status"] == "COMPLETE_WITH_DISCLOSURES",
            "configuration_path": str(config_path),
            "database_io_attempted": False,
            "database_write_attempted": False,
        }
    )
    performance = result.get("performance")
    if isinstance(performance, dict):
        performance["total_command_seconds"] = round(
            time.monotonic() - command_started, 6
        )
    if args.output_format == "summary":
        full_observation = result.get("full_source_core_profile")
        if isinstance(full_observation, dict):
            source_package = result.get("source_package", {})
            lines = [
                f"全范围来源候选 Core/Profile：{full_observation['status']}；日期={full_observation['target_session']}；"
                f"ZIP SHA-256={source_package.get('sha256')}；坐标=RAW_UNADJUSTED；NON_PIT_RETROSPECTIVE",
                f"来源候选={full_observation['source_candidate_total']}；来源状态="
                f"{json.dumps(full_observation['source_status_counts'], ensure_ascii=False, sort_keys=True)}；"
                f"Core/Profile已生产={full_observation['core_profile_produced']}；未生产={full_observation['core_profile_not_produced']}；"
                f"消费者失败={full_observation['consumer_failed']}",
                f"来源分母闭合={full_observation['source_partition_complete']}；"
                f"Core字段闭合={full_observation['core_field_partitions_complete']}；"
                f"画像规则闭合={full_observation['profile_rule_partitions_complete']}；"
                "官方有效上市证券分母=UNKNOWN",
                "Core 43字段质量/原因：",
            ]
            for field_id, stats in sorted(full_observation["core_fields"].items()):
                lines.append(
                    f"  {field_id}: quality={json.dumps(stats['quality_counts'], ensure_ascii=False, sort_keys=True)}; "
                    f"未生产={json.dumps(stats['unavailable_counts'], ensure_ascii=False, sort_keys=True)}; "
                    f"原因={json.dumps(stats['reason_counts'], ensure_ascii=False, sort_keys=True)}"
                )
            lines.append("Profile 11规则质量/原因：")
            for field_id, stats in sorted(full_observation["profile_rules"].items()):
                lines.append(
                    f"  {field_id} ({stats['rule_id']}): status={json.dumps(stats['status_counts'], ensure_ascii=False, sort_keys=True)}; "
                    f"quality={json.dumps(stats['quality_counts'], ensure_ascii=False, sort_keys=True)}; "
                    f"原因={json.dumps(stats['reason_counts'], ensure_ascii=False, sort_keys=True)}"
                )
            lines.append("少量实际样本：")
            for sample in full_observation["samples"]:
                concise = {
                    "symbol": sample["symbol"],
                    "source_bar_status": sample["source_bar_status"],
                    "source_reason": sample["source_reason"],
                    "consumer_status": sample.get("consumer_status"),
                    "entry_sha256": sample.get("entry_sha256"),
                    "core": {
                        name: sample.get("core_fields", {}).get(name)
                        for name in ("core.ma20", "core.atr20", "core.ret5")
                        if name in sample.get("core_fields", {})
                    },
                    "profile": sample.get("profile_components"),
                }
                lines.append(
                    "  "
                    + json.dumps(
                        concise,
                        ensure_ascii=False,
                        default=_json_default,
                        separators=(",", ":"),
                    )
                )
            lines.append(
                "耗时/资源："
                + json.dumps(
                    full_observation["read_usage"],
                    ensure_ascii=False,
                    default=_json_default,
                    sort_keys=True,
                )
            )
            lines.append(
                "只读内存观察；不等于上市Universe、PIT、正式Profile、PG、Foundation accepted、API或网页发布。"
            )
            summary = "\n".join(lines)
        else:
            summary = _render_universe_summary(result)
        source_package = result.get("source_package")
        if isinstance(source_package, dict):
            summary = (
                f"显式VIPDATA来源包 {source_package['package_id']}；ZIP SHA-256={source_package['sha256']}；"
                f"CRC={source_package['zip_crc']}；沪深主板/GEM为代码池，不是官方上市总体\n"
                + summary
            )
        sys.stdout.write(summary + "\n")
    else:
        _emit(result)
    return _EXIT_OK if result["ok"] else _EXIT_UNAVAILABLE
