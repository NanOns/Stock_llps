"""Stock A命令行vipdata职责模块。"""

from __future__ import annotations

import argparse
from dataclasses import asdict
from decimal import Decimal, InvalidOperation, localcontext
import json
import math
from pathlib import Path
import re
import sys
import time
from typing import Any
from stock_a.domain import ContractError
from stock_a.sources.tdx.day import UnitContract
from stock_a.sources.tdx.day_source import open_day_file, read_protected_bytes
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
    read_sealed_source_market_breadth_history,
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


def _run_source_vipdata_persist(args: argparse.Namespace) -> int:
    command_started = time.monotonic()
    config, config_path = _load_runtime_config()
    if args.task_scope not in RAW_DAILY_APPEND_TASK_SCOPES:
        raise DiagnosticError(
            "TASK_SCOPE_REJECTED",
            "Raw APPEND只接受已登记的日更业务范围",
            _EXIT_PERMISSION,
        )
    if (
        not math.isfinite(args.max_vipdata_validation_seconds)
        or not 0 < args.max_vipdata_validation_seconds <= 600
    ):
        raise DiagnosticError(
            "VIPDATA_BUDGET_INVALID",
            "ZIP SHA/CRC验证预算须大于0且不超过600秒",
            _EXIT_ARGUMENT,
        )
    package = _verify_explicit_vipdata_package(args)
    if package is None:
        raise DiagnosticError(
            "VIPDATA_PACKAGE_REQUIRED",
            "Raw来源入库必须显式绑定已验证ZIP",
            _EXIT_ARGUMENT,
        )
    try:
        target = _parse_target(args.target_session)
    except ResearchEntryError as error:
        raise DiagnosticError(error.code, str(error), _EXIT_ARGUMENT) from error
    _calendar_payload, _calendar_metadata, _calendar_revision, official_sessions = (
        _load_calendar(target)
    )
    if target not in official_sessions:
        raise DiagnosticError(
            "TARGET_DATE_INVALID", "目标日期不在现行已核交易日历内", _EXIT_ARGUMENT
        )
    if (
        type(args.history_session_count) is not int
        or not 1 <= args.history_session_count <= 75
    ):
        raise DiagnosticError(
            "HISTORY_SESSION_WINDOW_INVALID", "历史扫描会话数须为1至75", _EXIT_ARGUMENT
        )
    available_sessions = tuple(
        session for session in official_sessions if session <= target
    )
    if len(available_sessions) < args.history_session_count:
        raise DiagnosticError(
            "UNKNOWN_CALENDAR",
            "官方日历覆盖不足以形成请求的历史会话窗口",
            _EXIT_UNAVAILABLE,
        )
    selected_sessions = available_sessions[-args.history_session_count :]
    try:
        budget = VipdataTargetScanBudget(
            max_total_bytes=args.max_source_quality_bytes,
            max_total_records=args.max_source_quality_records,
            max_total_seconds=args.max_source_quality_seconds,
        )
        if args.history_session_count == 1:
            result = import_verified_vipdata_target(
                config,
                package,
                target,
                task_scope=args.task_scope,
                scan_budget=budget,
                chunk_size=args.chunk_size,
            )
        else:
            result = import_verified_vipdata_history(
                config,
                package,
                selected_sessions,
                task_scope=args.task_scope,
                scan_budget=budget,
                chunk_size=args.chunk_size,
                max_raw_rows=750_000,
            )
    except VipdataError as error:
        raise DiagnosticError(
            "VIPDATA_SOURCE_SCAN_FAILED",
            f"VIPDATA来源单遍扫描失败：{error.code}",
            _EXIT_UNAVAILABLE,
        ) from error
    except (ValueError, ContractError) as error:
        raise DiagnosticError(
            "SOURCE_RAW_ARGUMENT_INVALID",
            f"来源Raw扫描参数无效：{error}",
            _EXIT_ARGUMENT,
        ) from error
    except RawSourcePgError as error:
        payload = {
            "ok": False,
            "status": "PARTIAL" if error.committed_rows is not None else "REJECTED",
            "code": error.code,
            "message": str(error),
            "committed_row_count": error.committed_rows,
            "target_session": target.isoformat(),
            "session_count": args.history_session_count,
            "package_sha256": package.sha256,
            "zip_crc": package.zip_crc,
            "package_file_size": package.file_size,
            "task_scope": args.task_scope,
            "storage_location_status": "USER_CONFIRMED_NOT_INDEPENDENTLY_VERIFIED",
            "database_write_attempted": error.metadata_committed
            or error.committed_rows not in (None, 0),
            "source_fallback_attempted": False,
            "production_switched": False,
        }
        _emit(payload, error=True)
        return _EXIT_UNAVAILABLE
    result.update(
        {
            "ok": result.get("status") in {"COMPLETE", "ALREADY_COMPLETE"},
            "mode": "verified_local_source_raw_ingest",
            "package_file_name": package.file_name,
            "package_file_size": package.file_size,
            "zip_crc": package.zip_crc,
            "zip_entry_count": package.zip_entry_count,
            "entry_count_by_market": dict(package.entry_count_by_market),
            "official_package_url": "https://data.tdx.com.cn/vipdoc/hsjday.zip",
            "package_verified_at": package.verified_at.isoformat(),
            "task_scope": args.task_scope,
            "configuration_path": str(config_path),
            "database_io_attempted": True,
            "database_write_attempted": (
                result.get("new_rows_committed", 0) > 0
                or result.get("database_committed_rows", 0) > 0
                or any(
                    item.get("new_rows_committed", 0) > 0
                    for item in result.get("session_results", [])
                )
            ),
            "production_switched": False,
            "command_elapsed_seconds": round(time.monotonic() - command_started, 6),
        }
    )
    _emit(result, error=not result["ok"])
    return _EXIT_OK if result["ok"] else _EXIT_UNAVAILABLE


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
            "raw_byte_sha256": summary.raw_byte_sha256,
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


def _verify_explicit_vipdata_package(args: argparse.Namespace):
    package_path = getattr(args, "vipdata_package", None)
    expected_sha256 = getattr(args, "vipdata_package_sha256", None)
    if (package_path is None) != (expected_sha256 is None):
        raise DiagnosticError(
            "VIPDATA_PACKAGE_BINDING_REQUIRED",
            "必须同时提供--vipdata-package和--vipdata-package-sha256",
            _EXIT_ARGUMENT,
        )
    if package_path is None:
        return None
    try:
        budgets = VipdataBudgets(max_total_seconds=args.max_vipdata_validation_seconds)
        return verify_existing_vipdata_package(
            package_path,
            expected_sha256=expected_sha256,
            budgets=budgets,
        )
    except VipdataError as error:
        raise DiagnosticError(error.code, str(error), _EXIT_UNAVAILABLE) from error


def _run_vipdata_target_quality(args: argparse.Namespace) -> int:
    command_started = time.monotonic()
    package_verification_started = time.monotonic()
    package = _verify_explicit_vipdata_package(args)
    package_verification_seconds = time.monotonic() - package_verification_started
    target = _parse_target(args.target_session)
    _calendar_payload, _calendar_metadata, _calendar_revision, sessions = (
        _load_calendar(target)
    )
    if target not in sessions:
        raise DiagnosticError(
            "TARGET_DATE_INVALID", "目标日期不在已核验的官方交易日历内", _EXIT_ARGUMENT
        )
    try:
        units = UnitContract(
            args.unit_version,
            Decimal(args.amount_to_cny) if args.amount_to_cny is not None else None,
            args.volume_to_shares,
        )
        budget = VipdataTargetScanBudget(
            page_size=args.page_size,
            max_total_bytes=args.max_total_bytes,
            max_total_records=args.max_total_records,
            max_total_seconds=args.max_total_seconds,
            max_candidate_bytes=args.max_candidate_bytes,
            max_candidate_records=args.max_candidate_records,
            max_candidate_seconds=args.max_candidate_seconds,
            chunk_records=args.chunk_records,
        )
    except (InvalidOperation, ValueError, ContractError, VipdataError) as error:
        raise DiagnosticError(
            "ARGUMENT_CONTRACT_INVALID",
            f"目标日质量评价合同或预算无效：{error}",
            _EXIT_ARGUMENT,
        ) from error

    def show_page(page) -> None:
        sys.stderr.write(
            json.dumps(
                {"page_progress": asdict(page)},
                ensure_ascii=False,
                default=_json_default,
            )
            + "\n"
        )

    try:
        candidate_index_started = time.monotonic()
        pool = vipdata_research_candidate_pool(package)
        candidate_index_seconds = time.monotonic() - candidate_index_started
        quality_scan_started = time.monotonic()
        report = scan_vipdata_target_quality(
            package,
            pool,
            target_session=target,
            units=units,
            budget=budget,
            on_page=show_page,
        )
        quality_scan_wall_seconds = time.monotonic() - quality_scan_started
    except VipdataError as error:
        _emit(
            {
                "ok": False,
                "code": error.code,
                "message": str(error),
                "target_session": target.isoformat(),
                "database_io_attempted": False,
                "database_write_attempted": False,
                "source_fallback_attempted": False,
            },
            error=True,
        )
        return _EXIT_UNAVAILABLE
    payload = {
        "ok": report.not_evaluated == 0 and report.read_failed == 0,
        "contract": "TDX_VIPDATA_TARGET_SESSION_SOURCE_QUALITY_NON_PIT_R1",
        "target_session": report.target_session.isoformat(),
        "scope": "DYNAMIC_SH_SZ_MAIN_BOARD_AND_GEM_ZIP_SOURCE_CODE_CANDIDATES",
        "official_valid_listing_denominator": "UNKNOWN_NOT_PROVED_BY_ZIP_FILENAMES",
        "package": {
            "package_id": report.package_id,
            "sha256": report.package_sha256,
            "file_size": package.file_size,
            "zip_crc": package.zip_crc,
            "zip_entry_count": package.zip_entry_count,
            "entry_count_by_market": dict(package.entry_count_by_market),
            "verified_at": report.package_verified_at.isoformat(),
            "official_package_url": "https://data.tdx.com.cn/vipdoc/hsjday.zip",
        },
        "denominators": {
            "candidate_total": report.candidate_total,
            "evaluated": report.evaluated,
            "target_bar_present": report.target_present,
            "target_bar_missing": report.target_missing,
            "read_failed": report.read_failed,
            "not_evaluated": report.not_evaluated,
            "candidate_partition_closed": report.candidate_total
            == report.evaluated + report.not_evaluated,
            "evaluation_partition_closed": report.evaluated
            == report.target_present + report.target_missing + report.read_failed,
        },
        "by_market": report.by_market,
        "by_board": report.by_board,
        "target_field_group_quality": report.field_quality,
        "pages": [asdict(page) for page in report.pages],
        "candidates": [asdict(item) for item in report.candidates],
        "read_usage": {
            "bytes": report.bytes_read,
            "records": report.records_read,
            "elapsed_seconds": report.elapsed_seconds,
            "entry_read_elapsed_seconds": report.entry_read_elapsed_seconds,
            "budget": asdict(report.budget),
            "package_sha_recalculated_per_candidate": False,
            "official_network_requested": False,
            "database_io_attempted": False,
            "business_write_attempted": False,
        },
        "performance": {
            "timing_contract": "STOCK_A_RESEARCH_UNIVERSE_SEGMENTS_R1",
            "evaluation_mode": "FULL_TARGET_QUALITY",
            "zip_sha_crc_validation_seconds": round(package_verification_seconds, 6),
            "candidate_pool_metadata_seconds": round(candidate_index_seconds, 6),
            "full_pool_entry_decode_seconds": report.entry_read_elapsed_seconds,
            "full_pool_target_scan_wall_seconds": round(quality_scan_wall_seconds, 6),
            "full_pool_target_scan_report_seconds": report.elapsed_seconds,
            "full_pool_source_bytes": report.bytes_read,
            "full_pool_source_records": report.records_read,
            "total_command_seconds": round(time.monotonic() - command_started, 6),
        },
        "identity_and_clock_disclosure": {
            "source_symbol_is_stable_security_id": False,
            "published_at": "UNKNOWN",
            "first_available_at": "UNKNOWN",
            "observed_at_role": "LOCAL_REVALIDATED_ZIP_ENTRY_READ_NON_PIT",
            "missing_target_bar_means": "UNKNOWN_NO_BAR; 不推断停牌、上市前或退市",
            "amount_and_volume_units": "CALLER_DECLARED_RESEARCH_ASSUMPTION_NOT_AUTHORITY",
        },
    }
    _emit(payload, error=not payload["ok"])
    return _EXIT_OK if payload["ok"] else _EXIT_UNAVAILABLE


def _run_vipdata_revision_compare(args: argparse.Namespace) -> int:
    started = time.monotonic()
    try:
        target = _parse_target(args.target_session)
        budgets = VipdataBudgets(max_total_seconds=args.max_validation_seconds)
        old_package = verify_existing_vipdata_package(
            args.old_package,
            expected_sha256=args.old_package_sha256,
            budgets=budgets,
        )
        new_package = verify_existing_vipdata_package(
            args.new_package,
            expected_sha256=args.new_package_sha256,
            budgets=budgets,
        )
        comparison = compare_vipdata_package_revisions(
            old_package,
            new_package,
            target_session=target,
            max_total_bytes=args.max_total_bytes,
            max_total_records=args.max_total_records,
            max_total_seconds=args.max_total_seconds,
            sample_limit=args.sample_limit,
        )
    except (VipdataError, ResearchEntryError, ValueError) as error:
        _emit(
            {
                "ok": False,
                "code": getattr(error, "code", "VIPDATA_REVISION_COMPARE_INVALID"),
                "message": str(error),
                "target_session": args.target_session,
                "source_fallback_attempted": False,
                "database_io_attempted": False,
                "database_write_attempted": False,
            },
            error=True,
        )
        return _EXIT_UNAVAILABLE
    payload = {
        "ok": comparison.comparison_complete and comparison.not_evaluated == 0,
        "contract": "TDX_VIPDATA_SAME_SESSION_ENTRY_REVISION_COMPARISON_R1",
        "target_session": comparison.target_session.isoformat(),
        "old_package": {
            "path": str(old_package.file_path),
            "sha256": old_package.sha256,
            "file_size": old_package.file_size,
            "zip_crc": old_package.zip_crc,
            "verified_at": old_package.verified_at.isoformat(),
            "published_at": "UNKNOWN",
            "first_available_at": "UNKNOWN",
        },
        "new_package": {
            "path": str(new_package.file_path),
            "sha256": new_package.sha256,
            "file_size": new_package.file_size,
            "zip_crc": new_package.zip_crc,
            "verified_at": new_package.verified_at.isoformat(),
            "published_at": "UNKNOWN",
            "first_available_at": "UNKNOWN",
        },
        "scope": "DYNAMIC_SH_SZ_MAIN_BOARD_AND_GEM_SOURCE_SYMBOL_CANDIDATES",
        "official_listing_denominator": "UNKNOWN_NOT_PROVED_BY_ZIP_FILENAMES",
        "comparison": asdict(comparison),
        "operation": {
            "elapsed_seconds": round(time.monotonic() - started, 6),
            "core_profile_recomputed": False,
            "source_fallback_attempted": False,
            "database_io_attempted": False,
            "database_write_attempted": False,
            "persistent_business_write_attempted": False,
            "clock_disclosure": comparison.capture_clock_role,
        },
    }
    _emit(payload, error=not payload["ok"])
    return _EXIT_OK if payload["ok"] else _EXIT_UNAVAILABLE


def _run_source_vipdata(args: argparse.Namespace) -> int:
    runtime_config, _config_path = _load_runtime_config()
    try:
        source_policy = runtime_config["source_policy"]
        source_config = VipdataSourceConfig.from_mapping(
            source_policy["vipdata_official"]
        )
        budgets = VipdataBudgets(
            max_page_bytes=args.max_page_bytes,
            max_package_bytes=args.max_package_bytes,
            max_zip_entries=args.max_zip_entries,
            max_uncompressed_bytes=args.max_uncompressed_bytes,
            max_entry_bytes=args.max_entry_bytes,
            max_compression_ratio=args.max_compression_ratio,
            timeout_seconds=args.timeout_seconds,
            max_total_seconds=args.max_total_seconds,
            max_redirects=args.max_redirects,
            reserve_free_bytes=args.reserve_free_bytes,
        )
        observation = inspect_vipdata_page(
            args.target_session,
            budgets=budgets,
            source_config=source_config,
        )
    except VipdataError as error:
        _emit(
            {
                "ok": False,
                "code": error.code,
                "message": str(error),
                "network_action": "EXPLICIT_SOURCE_VIPDATA_ONLY",
                "database_io_attempted": False,
                "database_write_attempted": False,
            },
            error=True,
        )
        return (
            _EXIT_ARGUMENT
            if error.code in {"TARGET_SESSION_INVALID", "BUDGET_INVALID"}
            else _EXIT_UNAVAILABLE
        )
    result: dict[str, Any] = {
        "ok": False,
        "mode": args.mode,
        "target_session": args.target_session,
        "observation": observation_dict(observation),
        "network_action": "EXPLICIT_SOURCE_VIPDATA_ONLY",
        "database_io_attempted": False,
        "database_write_attempted": False,
        "persistent_business_write_attempted": False,
    }
    if args.mode == "inspect":
        result["ok"] = observation.status == "PACKAGE_CANDIDATE_READY"
        _emit(result, error=not result["ok"])
        return _EXIT_OK if result["ok"] else _EXIT_UNAVAILABLE
    if observation.status != "PACKAGE_CANDIDATE_READY":
        result["download"] = {
            "status": "NOT_STARTED",
            "reason": observation.reason or observation.status,
        }
        _emit(result, error=True)
        return _EXIT_UNAVAILABLE
    try:
        package = download_vipdata_package(observation, budgets=budgets)
    except VipdataError as error:
        result["download"] = {"status": error.code, "reason": str(error)}
        _emit(result, error=True)
        return _EXIT_UNAVAILABLE

    return _consume_source_vipdata(args, package, result)


def _consume_source_vipdata(
    args: argparse.Namespace,
    package: Any,
    result: dict[str, Any],
) -> int:
    try:
        return _consume_source_vipdata_inner(args, package, result)
    finally:
        cleanup_vipdata_package(package)


def _consume_source_vipdata_inner(
    args: argparse.Namespace,
    package: Any,
    result: dict[str, Any],
) -> int:
    result["package"] = package_dict(package)
    result["validated_day_entry_count"] = len(package.day_entries)
    result["entry_count_by_market"] = dict(package.entry_count_by_market)
    result["core_consumer"] = {
        "status": "NOT_REQUESTED",
        "reason": "未显式提供--consume-symbol",
    }
    if args.consume_symbol:
        if not all(
            (args.unit_version, args.amount_to_cny, args.volume_to_shares is not None)
        ):
            _emit(
                {
                    **result,
                    "ok": False,
                    "core_consumer": {
                        "status": "BLOCKED",
                        "reason": "Core消费必须显式提供单位合同版本及倍率",
                    },
                },
                error=True,
            )
            return _EXIT_ARGUMENT
        try:
            symbol = args.consume_symbol.strip().upper()
            parts = symbol.split(".")
            if (
                len(parts) != 2
                or parts[0] not in {"SH", "SZ"}
                or re.fullmatch(r"[0-9]{6}", parts[1]) is None
            ):
                raise VipdataError(
                    "SYMBOL_INVALID", "Core样本必须使用SH.六位代码或SZ.六位代码"
                )
            units = UnitContract(
                args.unit_version, Decimal(args.amount_to_cny), args.volume_to_shares
            )
            limits = ReadLimits(
                args.max_symbol_bytes,
                args.max_symbol_records,
                256,
                Decimal(args.max_symbol_seconds),
            )
            entry_scan = scan_vipdata_day_entry(
                package,
                market=parts[0],
                symbol=parts[1],
                target_session=args.target_session,
                units=units,
                limits=limits,
            )
            extraction_root, _extracted_path = extract_vipdata_day_entry(
                package,
                market=parts[0],
                symbol=parts[1],
            )
            try:
                consumer = research_stock(
                    symbol=symbol,
                    target_session=args.target_session,
                    source_roots=(str(extraction_root),),
                    units=units,
                    limits=limits,
                )
            finally:
                cleanup_vipdata_extraction(extraction_root)
            result["core_consumer"] = {
                "status": "CORE_CONSUMED"
                if entry_scan.target_bar_present
                else "CORE_CONSUMED_TARGET_BAR_MISSING",
                "sample_symbol": symbol,
                "archive_entry": entry_scan.entry.path,
                "temporary_source_consumed": True,
                "source_sha256": consumer["source"]["raw_byte_sha256"],
                "zip_entry_first_date": entry_scan.first_date.isoformat(),
                "zip_entry_last_date": entry_scan.last_date.isoformat(),
                "zip_entry_record_count": entry_scan.record_count,
                "target_bar_present": entry_scan.target_bar_present,
                "target_session": consumer["target_session"],
                "raw_availability": consumer["raw_availability"],
                "core_availability": consumer["core_availability"],
                "canonical_selection": consumer.get("canonical_selection"),
                "status_semantics": "NON_PIT_RETROSPECTIVE；不代表官方身份、来源封口、PG接受或生产准入",
            }
        except (
            VipdataError,
            ContractError,
            InvalidOperation,
            ResearchEntryError,
            DayReadError,
            OSError,
        ) as error:
            code = (
                error.code
                if isinstance(error, (VipdataError, ResearchEntryError, DayReadError))
                else "CORE_CONSUMER_FAILED"
            )
            result["core_consumer"] = {"status": code, "reason": str(error)}
            _emit(result, error=True)
            return _EXIT_UNAVAILABLE
    result["ok"] = True
    _emit(result)
    return _EXIT_OK
