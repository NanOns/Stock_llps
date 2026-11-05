"""Stock A命令行pg_queries职责模块。"""

from __future__ import annotations

import argparse
from datetime import date, datetime
from pathlib import Path
import re
import sys
from typing import Any
from stock_a.contracts import (
    PriceProfileContractError,
    load_price_profile_contract,
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
from stock_a.application.pg_research import (
    PgResearchBindingError,
    read_source_version_context,
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
from stock_a.application.research_window import (
    resolve_pg_history_window as _pg_history_window,
)

from stock_a.application.cli_commands.common import _emit
from stock_a.application.cli_commands.research.research_output import _summary_field


def _validate_pg_raw_query_args(
    args: argparse.Namespace, *, universe: bool
) -> tuple[dict[str, Any], Path, date, str]:
    if not getattr(args, "pg_source_raw", False):
        if getattr(args, "pg_source_sha256", None) is not None:
            raise DiagnosticError(
                "PG_SOURCE_FLAG_REQUIRED",
                "提供来源SHA时必须同时显式启用--pg-source-raw",
                _EXIT_ARGUMENT,
            )
        raise DiagnosticError(
            "PG_SOURCE_FLAG_REQUIRED", "缺少显式PG Raw查询选项", _EXIT_ARGUMENT
        )
    source_sha = getattr(args, "pg_source_sha256", None)
    if (
        not isinstance(source_sha, str)
        or re.fullmatch(r"[0-9a-f]{64}", source_sha) is None
    ):
        raise DiagnosticError(
            "PG_SOURCE_REVISION_REQUIRED",
            "PG Raw读取必须提供64位小写来源包SHA-256",
            _EXIT_ARGUMENT,
        )
    if getattr(args, "vipdata_package", None) is not None:
        raise DiagnosticError(
            "PG_SOURCE_ZIP_CONFLICT",
            "PG Raw读取与重新打开VIPDATA ZIP不能在同一命令混用",
            _EXIT_ARGUMENT,
        )
    if (
        getattr(args, "amount_to_cny", None) is not None
        or getattr(args, "volume_to_shares", None) is not None
    ):
        raise DiagnosticError(
            "PG_SOURCE_UNIT_CONFLICT",
            "PG Raw读取保留TDX源单位，不接受金额或数量换算倍率",
            _EXIT_ARGUMENT,
        )
    for option in (
        "include_profile_preview",
        "include_baostock_evidence",
        "include_baostock_identity",
    ):
        if getattr(args, option, False):
            raise DiagnosticError(
                "PG_SOURCE_RECALCULATION_CONFLICT",
                "PG Raw读取不能同时请求Core/Profile重算或外部来源",
                _EXIT_ARGUMENT,
            )
    if universe and any(
        getattr(args, name, False)
        for name in ("full_target_quality", "full_source_core_profile")
    ):
        raise DiagnosticError(
            "PG_SOURCE_SCAN_CONFLICT",
            "PG Raw分页不能同时请求ZIP扫描或Core/Profile全范围计算",
            _EXIT_ARGUMENT,
        )
    config, config_path = _load_runtime_config()
    try:
        target = _parse_target(args.target_session)
    except ResearchEntryError as error:
        raise DiagnosticError(error.code, str(error), _EXIT_ARGUMENT) from error
    return config, config_path, target, source_sha


def _run_pg_raw_stock(args: argparse.Namespace) -> int:
    config, config_path, target, source_sha = _validate_pg_raw_query_args(
        args, universe=False
    )
    try:
        row = read_raw_daily_stock(
            config,
            source_symbol=args.symbol,
            target_session=target,
            package_sha256=source_sha,
        )
        source_context = read_source_version_context(
            config,
            target_session=target,
            package_sha256=source_sha,
            namespace_id=str(row["namespace_id"]) if row is not None else None,
            target_raw_rows=(row,) if row is not None else (),
        )
    except RawSourcePgError as error:
        _emit(
            {
                "ok": False,
                "status": "REJECTED",
                "code": error.code,
                "message": str(error),
                "database_write_attempted": False,
                "ddl_executed": False,
                "source_zip_opened": False,
            },
            error=True,
        )
        return (
            _EXIT_PERMISSION
            if "REJECTED" in error.code or error.code.endswith("INVALID")
            else _EXIT_UNAVAILABLE
        )
    except PgResearchBindingError as error:
        _emit(
            {
                "ok": False,
                "status": "REJECTED",
                "code": error.code,
                "message": str(error),
                "database_write_attempted": False,
                "dml_executed": False,
                "ddl_executed": False,
            },
            error=True,
        )
        return _EXIT_UNAVAILABLE
    result = {
        "ok": True,
        "status": "AVAILABLE" if row is not None else "NO_MATCH",
        "requested_symbol": args.symbol,
        "target_session": target.isoformat(),
        "package_sha256": source_sha,
        "analysis_scope": "TDX_SOURCE_SYMBOL_RAW_DAILY_FROM_POSTGRESQL",
        "row": row,
        "source_version": source_context,
        "fallback_attempted": False,
        "database_io_attempted": True,
        "database_write_attempted": False,
        "dml_executed": False,
        "ddl_executed": False,
        "source_zip_opened": False,
        "source_recalculated": False,
        "configuration_path": str(config_path),
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "amount_unit_status": "UNIT_UNVERIFIED",
        "volume_unit_status": "UNIT_UNVERIFIED",
        "canonical_or_foundation_status": "NOT_WRITTEN_OR_ACCEPTED",
    }
    _emit(result)
    return _EXIT_OK


def _run_pg_raw_universe(args: argparse.Namespace) -> int:
    config, config_path, target, source_sha = _validate_pg_raw_query_args(
        args, universe=True
    )
    if (
        type(args.offset) is not int
        or type(args.limit) is not int
        or not 1 <= args.limit <= 100
    ):
        raise DiagnosticError(
            "PG_PAGE_INVALID",
            "PG分页limit必须在1至100内，offset必须是非负整数",
            _EXIT_ARGUMENT,
        )
    try:
        result = read_raw_daily_page(
            config,
            target_session=target,
            package_sha256=source_sha,
            offset=args.offset,
            limit=args.limit,
            market=args.market,
            board=args.board,
        )
        source_context = read_source_version_context(
            config,
            target_session=target,
            package_sha256=source_sha,
            namespace_id=result["namespace_id"],
            target_raw_rows=result["rows"],
            expected_raw_row_count=result["version_row_count"],
        )
    except RawSourcePgError as error:
        _emit(
            {
                "ok": False,
                "status": "REJECTED",
                "code": error.code,
                "message": str(error),
                "database_write_attempted": False,
                "ddl_executed": False,
                "source_zip_opened": False,
            },
            error=True,
        )
        return (
            _EXIT_PERMISSION
            if "REJECTED" in error.code or error.code.endswith("INVALID")
            else _EXIT_UNAVAILABLE
        )
    except PgResearchBindingError as error:
        _emit(
            {
                "ok": False,
                "status": "REJECTED",
                "code": error.code,
                "message": str(error),
                "database_write_attempted": False,
                "dml_executed": False,
                "ddl_executed": False,
            },
            error=True,
        )
        return _EXIT_UNAVAILABLE
    result.update(
        {
            "source_version": source_context,
            "ok": True,
            "analysis_scope": "TDX_SOURCE_SYMBOL_RAW_DAILY_FROM_POSTGRESQL",
            "database_io_attempted": True,
            "database_write_attempted": False,
            "dml_executed": False,
            "configuration_path": str(config_path),
            "canonical_or_foundation_status": "NOT_WRITTEN_OR_ACCEPTED",
        }
    )
    _emit(result)
    return _EXIT_OK


def _validate_pg_history_args(
    args: argparse.Namespace,
) -> tuple[dict[str, Any], Path, date, tuple[date, ...], str]:
    if getattr(args, "pg_source_raw", False):
        raise DiagnosticError(
            "PG_SOURCE_MODES_EXCLUSIVE",
            "--pg-source-history不能与--pg-source-raw同时使用",
            _EXIT_ARGUMENT,
        )
    if (
        getattr(args, "vipdata_package", None) is not None
        or getattr(args, "vipdata_package_sha256", None) is not None
    ):
        raise DiagnosticError(
            "PG_HISTORY_ZIP_CONFLICT",
            "PG历史查询不得同时指定VIPDATA ZIP",
            _EXIT_ARGUMENT,
        )
    if (
        getattr(args, "amount_to_cny", None) is not None
        or getattr(args, "volume_to_shares", None) is not None
    ):
        raise DiagnosticError(
            "PG_HISTORY_UNIT_ASSUMPTION_REJECTED",
            "PG来源单位尚未核实，不能用调用方倍率生成量额Core",
            _EXIT_ARGUMENT,
        )
    if (
        getattr(args, "unit_version", None)
        != "TDX_RAW_SOURCE_VALUES_UNIT_UNVERIFIED_R1"
    ):
        raise DiagnosticError(
            "PG_HISTORY_UNIT_VERSION_REJECTED",
            "PG历史模式必须显式声明TDX原值单位未核实版本",
            _EXIT_ARGUMENT,
        )
    if getattr(args, "include_baostock_evidence", False) or getattr(
        args, "include_baostock_identity", False
    ):
        raise DiagnosticError(
            "PG_HISTORY_EXTERNAL_SOURCE_REJECTED",
            "PG历史查询是本地只读模式，不触发BaoStock网络请求",
            _EXIT_ARGUMENT,
        )
    source_sha = getattr(args, "pg_source_sha256", None)
    if (
        not isinstance(source_sha, str)
        or re.fullmatch(r"[0-9a-f]{64}", source_sha) is None
    ):
        raise DiagnosticError(
            "PG_HISTORY_PACKAGE_SHA_REQUIRED",
            "PG历史查询必须显式提供64位小写package SHA-256",
            _EXIT_ARGUMENT,
        )
    target, sessions, _revision = _pg_history_window(
        args.target_session,
        getattr(args, "pg_history_session_count", 75),
    )
    config, config_path = _load_runtime_config()
    return config, config_path, target, sessions, source_sha


def _run_pg_history_stock(args: argparse.Namespace) -> int:
    config, config_path, target, sessions, source_sha = _validate_pg_history_args(args)
    try:
        history = read_raw_daily_history_stock(
            config,
            source_symbol=args.symbol,
            target_session=target,
            package_sha256=source_sha,
            sessions=sessions,
        )
        source_context = read_source_version_context(
            config,
            target_session=target,
            package_sha256=source_sha,
            namespace_id=history["namespace_id"],
            target_raw_rows=history["history_rows"],
        )
        profile_contract = load_price_profile_contract(_CONFIG_ROOT).document
        result = research_stock_pg_history_preview(
            symbol=args.symbol,
            target_session=target.isoformat(),
            package_sha256=source_sha,
            session_dates=sessions,
            history_rows=history["history_rows"],
            missing_history_sessions=tuple(
                date.fromisoformat(item) for item in history["missing_history_sessions"]
            ),
            namespace_id=history["namespace_id"],
            profile_contract=profile_contract,
        )
    except RawSourcePgError as error:
        _emit(
            {
                "ok": False,
                "status": "REJECTED",
                "code": error.code,
                "message": str(error),
                "database_write_attempted": False,
                "ddl_executed": False,
                "source_zip_opened": False,
            },
            error=True,
        )
        return _EXIT_ARGUMENT if error.code.endswith("INVALID") else _EXIT_UNAVAILABLE
    except PgResearchBindingError as error:
        _emit(
            {
                "ok": False,
                "status": "REJECTED",
                "code": error.code,
                "message": str(error),
                "database_write_attempted": False,
                "ddl_executed": False,
                "source_zip_opened": False,
            },
            error=True,
        )
        return _EXIT_UNAVAILABLE
    except (ResearchEntryError, PriceProfileContractError) as error:
        code = getattr(error, "code", "PROFILE_CONTRACT_UNAVAILABLE")
        _emit(
            {
                "ok": False,
                "status": "REJECTED",
                "code": code,
                "message": str(error),
                "database_write_attempted": False,
                "ddl_executed": False,
                "source_zip_opened": False,
            },
            error=True,
        )
        return _EXIT_UNAVAILABLE
    result.update(
        {
            "database_io_attempted": True,
            "database_write_attempted": False,
            "dml_executed": False,
            "configuration_path": str(config_path),
            "source_version": source_context,
        }
    )
    if getattr(args, "output_format", "json") == "summary":
        fields = result.get("core", {}).get("fields", {})
        preview = result.get("profile_preview", {})
        core_labels = (
            "core.ma20",
            "core.ma60",
            "core.atr20",
            "core.ret20",
            "core.pos60",
        )
        line = [
            f"PG历史研究预览 {result.get('symbol')} {target.isoformat()} "
            f"窗口={result.get('window', {}).get('start_session')}..{result.get('window', {}).get('end_session')} "
            f"有效Bar={result.get('window', {}).get('real_bar_count')}/{result.get('window', {}).get('requested_sessions')} "
            f"包SHA={source_sha[:12]}",
        ]
        line.extend(
            f"{name}={_summary_field(fields.get(name))}" for name in core_labels
        )
        line.append(
            f"Core未知={result.get('core', {}).get('unknown_field_count')}/43 Profile规则={len(preview.get('components', []))}/11 NON_PIT"
        )
        sys.stdout.write("\n".join(line) + "\n")
    else:
        _emit(result)
    return _EXIT_OK if result.get("ok") else _EXIT_UNAVAILABLE


def _run_pg_history_universe(args: argparse.Namespace) -> int:
    if getattr(args, "full_target_quality", False) or getattr(
        args, "full_source_core_profile", False
    ):
        raise DiagnosticError(
            "PG_HISTORY_SCAN_OPTION_REJECTED",
            "PG历史模式只读取已入库Raw，不运行ZIP扫描模式",
            _EXIT_ARGUMENT,
        )
    config, config_path, target, sessions, source_sha = _validate_pg_history_args(args)
    if (
        type(args.offset) is not int
        or args.offset < 0
        or type(args.limit) is not int
        or not 1 <= args.limit <= 100
    ):
        raise DiagnosticError(
            "PG_PAGE_INVALID", "PG历史分页offset须非负且limit须为1至100", _EXIT_ARGUMENT
        )
    try:
        page = read_raw_daily_history_page(
            config,
            target_session=target,
            package_sha256=source_sha,
            sessions=sessions,
            offset=args.offset,
            limit=args.limit,
            market=args.market,
            board=args.board,
        )
        target_rows = [
            row
            for candidate in page["candidates"]
            for row in candidate["history_rows"]
            if row["session_date"] == target
        ]
        source_context = read_source_version_context(
            config,
            target_session=target,
            package_sha256=source_sha,
            namespace_id=page["namespace_id"],
            target_raw_rows=target_rows,
            expected_raw_row_count=page["version_row_count"],
        )
        profile_contract = load_price_profile_contract(_CONFIG_ROOT).document
        result = research_universe_pg_history_preview(
            target_session=target.isoformat(),
            package_sha256=source_sha,
            session_dates=sessions,
            page=page,
            profile_contract=profile_contract,
        )
    except RawSourcePgError as error:
        _emit(
            {
                "ok": False,
                "status": "REJECTED",
                "code": error.code,
                "message": str(error),
                "database_write_attempted": False,
                "ddl_executed": False,
                "source_zip_opened": False,
            },
            error=True,
        )
        return _EXIT_ARGUMENT if error.code.endswith("INVALID") else _EXIT_UNAVAILABLE
    except PgResearchBindingError as error:
        _emit(
            {
                "ok": False,
                "status": "REJECTED",
                "code": error.code,
                "message": str(error),
                "database_write_attempted": False,
                "ddl_executed": False,
            },
            error=True,
        )
        return _EXIT_UNAVAILABLE
    except PriceProfileContractError as error:
        _emit(
            {
                "ok": False,
                "status": "REJECTED",
                "code": error.code,
                "message": str(error),
                "database_write_attempted": False,
                "ddl_executed": False,
                "source_zip_opened": False,
            },
            error=True,
        )
        return _EXIT_UNAVAILABLE
    result.update(
        {
            "database_io_attempted": True,
            "database_write_attempted": False,
            "dml_executed": False,
            "configuration_path": str(config_path),
            "source_version": source_context,
        }
    )
    if getattr(args, "output_format", "summary") == "summary":
        page_info = result["page"]
        lines = [
            f"PG历史候选页 offset={page_info['offset']} limit={page_info['limit']} "
            f"rows={page_info['page_count']} filtered_total={page_info['total_source_candidates']} "
            f"next={page_info['next_offset']} window={result['session_start']}..{result['session_end']} "
            f"来源版本={result['source_version_status']} 包SHA={source_sha[:12]} "
            f"实际/期望={result['source_version_row_count']}/{result['source_version_expected_row_count']}",
        ]
        for item in result["results"]:
            core_fields = item.get("core_price_fields", {})
            lines.append(
                f"{item['symbol']} Core未知={item.get('core_unknown_field_count')} "
                + " ".join(
                    f"{field}={_summary_field(value)}"
                    for field, value in core_fields.items()
                )
                + f" Profile={item.get('profile_status_counts')}"
            )
        sys.stdout.write("\n".join(lines) + "\n")
    else:
        _emit(result)
    return _EXIT_OK
